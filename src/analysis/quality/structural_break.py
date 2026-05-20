"""Structural break detection — CUSUM + sliding-window break point。

時系列 (e.g. annual Gini / fragility / theta_mean) で構造変化点を検出。
本格 Bai-Perron multiple break は scipy で容易でないため、本実装は:

1. **CUSUM (cumulative sum)** 検定: 構造一定 H0 を検定、p < α で break あり判定
2. **Sliding-window F-test**: 各候補 break t に対して
   pre vs post の mean / slope の F-test。最大 F の t を break candidate

`scipy` ベース。両指標が同方向に break flag → 構造変化点として採用。

H1: anime.score 非依存。
H2: 「劇的変化」frame NG → "観測された構造変化点" のみ。

References:
    - Bai & Perron (1998) "Estimating and testing linear models with multiple structural changes"
    - Brown, Durbin & Evans (1975) CUSUM
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class CUSUMResult:
    """CUSUM test statistic + 結論。"""

    statistic: float           # max CUSUM
    critical_value: float      # 5% bound
    p_approx: float            # approximate p (Brown-Durbin-Evans)
    has_break: bool
    n_obs: int


@dataclass(frozen=True)
class BreakCandidate:
    """1 候補 break point + F-stat."""

    index: int             # x index of candidate break
    x_value: float         # actual x at candidate (e.g. year)
    f_statistic: float
    p_value: float
    mean_pre: float
    mean_post: float
    delta: float           # mean_post - mean_pre


@dataclass(frozen=True)
class StructuralBreakReport:
    """CUSUM + sliding-window joint summary."""

    n_obs: int
    cusum: CUSUMResult
    top_candidates: tuple[BreakCandidate, ...]
    consensus_break_index: int | None     # 両指標が agree した index
    notes: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# CUSUM (Brown-Durbin-Evans approximation)
# ---------------------------------------------------------------------------


def cusum_test(
    values: Sequence[float], *, alpha: float = 0.05,
) -> CUSUMResult:
    """CUSUM 統計量 (cumulative sum of standardized residuals from mean)。

    H0: mean は時間で一定。
    CUSUM_t = sum_{i=1..t} (y_i - ȳ) / σ
    max |CUSUM_t| が approximate critical value (1.36 × sqrt(n) at 5%) 超えで reject。
    """
    arr = np.asarray(values, dtype=float)
    n = arr.size
    if n < 8:
        return CUSUMResult(
            statistic=0.0, critical_value=float("inf"),
            p_approx=1.0, has_break=False, n_obs=n,
        )
    mu = float(arr.mean())
    sigma = float(arr.std(ddof=1))
    if sigma == 0:
        return CUSUMResult(
            statistic=0.0, critical_value=float("inf"),
            p_approx=1.0, has_break=False, n_obs=n,
        )
    standardized = (arr - mu) / sigma
    cusum = np.cumsum(standardized)
    stat = float(np.max(np.abs(cusum)))
    # Brown-Durbin-Evans 5% critical: ~1.36 × sqrt(n)
    # alpha → c: 5% = 1.36, 1% = 1.63
    c_5 = 1.358 * np.sqrt(n)
    c_1 = 1.628 * np.sqrt(n)
    crit = c_1 if alpha <= 0.01 else c_5
    has_break = stat > crit
    # Approximate p via Kolmogorov-style: p ≈ 2 * exp(-2 * (stat / sqrt(n))^2)
    p_approx = float(min(1.0, 2.0 * np.exp(-2.0 * (stat / np.sqrt(n)) ** 2)))
    return CUSUMResult(
        statistic=stat,
        critical_value=float(crit),
        p_approx=p_approx,
        has_break=bool(has_break),
        n_obs=n,
    )


# ---------------------------------------------------------------------------
# Sliding-window F-test for mean shift
# ---------------------------------------------------------------------------


def sliding_window_break(
    values: Sequence[float],
    x: Sequence[float] | None = None,
    *,
    min_segment: int = 5,
    top_k: int = 3,
) -> list[BreakCandidate]:
    """各候補 break t に対し pre/post の mean shift F-test、上位 top_k を返す。

    F = (between-group SS / 1) / (within-group SS / (n - 2))
    p-value via F(1, n-2) distribution.
    """
    arr = np.asarray(values, dtype=float)
    n = arr.size
    if n < 2 * min_segment:
        return []
    if x is None:
        x_arr = np.arange(n, dtype=float)
    else:
        x_arr = np.asarray(x, dtype=float)

    candidates: list[BreakCandidate] = []
    eps = 1e-12  # for perfectly homogeneous segments (ss_w=0)
    for t in range(min_segment, n - min_segment + 1):
        pre = arr[:t]
        post = arr[t:]
        mu_pre = float(pre.mean())
        mu_post = float(post.mean())
        ss_w = float(np.sum((pre - mu_pre) ** 2) + np.sum((post - mu_post) ** 2))
        mu_all = float(arr.mean())
        ss_b = pre.size * (mu_pre - mu_all) ** 2 + post.size * (mu_post - mu_all) ** 2
        if ss_b == 0:
            # No between-group difference → skip
            continue
        ss_w_safe = max(ss_w, eps)
        f = (ss_b / 1.0) / (ss_w_safe / (n - 2))
        try:
            from scipy.stats import f as f_dist
            p = float(1.0 - f_dist.cdf(f, 1, n - 2))
        except ImportError:
            p = float("nan")
        candidates.append(
            BreakCandidate(
                index=int(t),
                x_value=float(x_arr[t]),
                f_statistic=float(f),
                p_value=p,
                mean_pre=mu_pre,
                mean_post=mu_post,
                delta=mu_post - mu_pre,
            )
        )

    candidates.sort(key=lambda c: -c.f_statistic)
    return candidates[:top_k]


# ---------------------------------------------------------------------------
# Combined report
# ---------------------------------------------------------------------------


def detect_break(
    values: Sequence[float],
    x: Sequence[float] | None = None,
    *,
    alpha: float = 0.05,
    min_segment: int = 5,
    top_k: int = 3,
    consensus_p_threshold: float = 0.05,
) -> StructuralBreakReport:
    """CUSUM + sliding-window 両方実行し、consensus break を判定。

    consensus = CUSUM has_break is True AND top-1 F-test p < threshold AND
    両者の break index が近い (sliding-window 上位 1 の index = consensus)。
    """
    cusum = cusum_test(values, alpha=alpha)
    cands = sliding_window_break(values, x=x, min_segment=min_segment, top_k=top_k)
    notes: list[str] = []
    consensus: int | None = None
    if cusum.has_break and cands and cands[0].p_value <= consensus_p_threshold:
        consensus = cands[0].index
        notes.append(
            f"consensus break at index {consensus} (x={cands[0].x_value:.2f}, "
            f"Δ={cands[0].delta:+.4f}, p={cands[0].p_value:.4g})"
        )
    elif cusum.has_break and not cands:
        notes.append("CUSUM detected break but no sliding-window candidate")
    elif not cusum.has_break and cands and cands[0].p_value <= consensus_p_threshold:
        notes.append(
            f"sliding-window p < {consensus_p_threshold} but CUSUM not rejected "
            "(local shift vs global stability)"
        )
    return StructuralBreakReport(
        n_obs=len(values),
        cusum=cusum,
        top_candidates=tuple(cands),
        consensus_break_index=consensus,
        notes=tuple(notes),
    )
