"""Survival curves wrap — Kaplan-Meier 推定 + log-rank + CI band。

`role_progression.py` の KM 推定を統合し、CI band 付き time-to-event 分布を返す。
lifelines ベース、graceful (lifelines 不在時は ImportError)。

主用途:
- in_between → key_animator 等の役職進行 hazard
- visibility_loss 単一原因 hazard (Cox は別 module)
- gender × role × cohort の subgroup KM 比較

H1: anime.score 非依存。
H2: 「離職」「キャリア終了」frame NG → "credit 出現停止" のみ。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

import numpy as np
import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class KMCurve:
    """1 グループの KM 曲線 + 95% CI (Greenwood)."""

    label: str
    timeline: tuple[float, ...]
    survival: tuple[float, ...]
    ci_lower: tuple[float, ...]
    ci_upper: tuple[float, ...]
    n_at_risk_initial: int
    n_events: int
    median_survival: float | None


@dataclass(frozen=True)
class LogRankResult:
    """Multivariate log-rank test result."""

    chi2: float
    p_value: float
    df: int
    n_groups: int


@dataclass(frozen=True)
class SurvivalReport:
    """Per-group KM curves + multivariate log-rank summary."""

    curves: tuple[KMCurve, ...]
    log_rank: LogRankResult | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# KM fit (single group)
# ---------------------------------------------------------------------------


def fit_km(
    durations: np.ndarray,
    events: np.ndarray,
    *,
    label: str = "all",
) -> KMCurve:
    """1 グループ Kaplan-Meier 推定。

    Args:
        durations: time-to-event or censoring time.
        events: 1 = event observed, 0 = right-censored.
        label: ラベル (subgroup name).

    Returns:
        KMCurve with timeline / survival / CI / median.
    """
    try:
        from lifelines import KaplanMeierFitter
    except ImportError as exc:
        raise ImportError(
            "lifelines required for fit_km(). Add to pixi.toml."
        ) from exc

    if durations.shape[0] != events.shape[0]:
        raise ValueError("durations / events length mismatch")
    if durations.size == 0:
        raise ValueError("empty input")

    kmf = KaplanMeierFitter()
    kmf.fit(durations, event_observed=events.astype(int), label=label)
    timeline = tuple(float(v) for v in kmf.timeline)
    sf = tuple(float(v) for v in kmf.survival_function_[label])
    ci = kmf.confidence_interval_survival_function_
    ci_lo = tuple(float(v) for v in ci.iloc[:, 0])
    ci_hi = tuple(float(v) for v in ci.iloc[:, 1])

    n_events = int(events.sum())
    n_initial = int(durations.size)
    median = kmf.median_survival_time_
    median_val = (
        None if (median is None or median != median or median == float("inf"))
        else float(median)
    )

    return KMCurve(
        label=label,
        timeline=timeline,
        survival=sf,
        ci_lower=ci_lo,
        ci_upper=ci_hi,
        n_at_risk_initial=n_initial,
        n_events=n_events,
        median_survival=median_val,
    )


# ---------------------------------------------------------------------------
# Subgroup KM + multivariate log-rank
# ---------------------------------------------------------------------------


def fit_subgroups(
    durations: np.ndarray,
    events: np.ndarray,
    groups: Sequence,
    *,
    min_group_n: int = 5,
) -> SurvivalReport:
    """Subgroup 別 KM + multivariate log-rank。

    Args:
        durations / events / groups: 同長 array.
        min_group_n: subgroup obs 数下限。これ未満は除外。

    Returns:
        SurvivalReport with curves + log_rank。
    """
    if durations.shape[0] != events.shape[0] or durations.shape[0] != len(groups):
        raise ValueError("input length mismatch")

    groups_arr = np.asarray(groups)
    unique = sorted(set(groups_arr.tolist()), key=str)
    curves: list[KMCurve] = []
    notes: list[str] = []
    used_labels: list[str] = []
    used_durations: list[np.ndarray] = []
    used_events: list[np.ndarray] = []

    for grp in unique:
        mask = groups_arr == grp
        if int(mask.sum()) < min_group_n:
            notes.append(f"skipped {grp}: n={int(mask.sum())} < {min_group_n}")
            continue
        try:
            curve = fit_km(
                durations[mask], events[mask], label=str(grp),
            )
            curves.append(curve)
            used_labels.append(str(grp))
            used_durations.append(durations[mask])
            used_events.append(events[mask])
        except Exception as exc:
            notes.append(f"fit_km failed for {grp}: {exc}")

    # Log-rank multivariate (only if >= 2 groups)
    log_rank: LogRankResult | None = None
    if len(used_labels) >= 2:
        try:
            from lifelines.statistics import multivariate_logrank_test
            T = np.concatenate(used_durations)
            E = np.concatenate(used_events).astype(int)
            G = np.concatenate(
                [np.full(d.size, lbl, dtype=object) for d, lbl in zip(used_durations, used_labels)]
            )
            lr = multivariate_logrank_test(T, G, E)
            log_rank = LogRankResult(
                chi2=float(lr.test_statistic),
                p_value=float(lr.p_value),
                df=int(len(used_labels) - 1),
                n_groups=len(used_labels),
            )
        except Exception as exc:
            notes.append(f"log_rank failed: {exc}")

    return SurvivalReport(
        curves=tuple(curves),
        log_rank=log_rank,
        notes=tuple(notes),
    )


# ---------------------------------------------------------------------------
# Pairwise log-rank (e.g., each group vs reference)
# ---------------------------------------------------------------------------


def pairwise_logrank(
    durations: np.ndarray,
    events: np.ndarray,
    groups: Sequence,
    *,
    reference: str,
    min_group_n: int = 5,
) -> dict[str, LogRankResult]:
    """各 group vs reference の log-rank。

    Returns: {group_label: LogRankResult}, reference は除外。
    """
    try:
        from lifelines.statistics import logrank_test
    except ImportError as exc:
        raise ImportError("lifelines required") from exc

    groups_arr = np.asarray(groups)
    ref_mask = groups_arr == reference
    if int(ref_mask.sum()) < min_group_n:
        return {}

    ref_T = durations[ref_mask]
    ref_E = events[ref_mask].astype(int)

    out: dict[str, LogRankResult] = {}
    for grp in sorted(set(groups_arr.tolist()), key=str):
        if str(grp) == reference:
            continue
        mask = groups_arr == grp
        if int(mask.sum()) < min_group_n:
            continue
        try:
            lr = logrank_test(
                durations_A=ref_T,
                durations_B=durations[mask],
                event_observed_A=ref_E,
                event_observed_B=events[mask].astype(int),
            )
            out[str(grp)] = LogRankResult(
                chi2=float(lr.test_statistic),
                p_value=float(lr.p_value),
                df=1,
                n_groups=2,
            )
        except Exception as exc:
            log.debug("pairwise_logrank_failed", group=str(grp), error=str(exc))
    return out
