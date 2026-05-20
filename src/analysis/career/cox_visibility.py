"""Cox proportional hazards model for visibility loss (career exit) analysis.

既存 visibility_loss.py の LightGBM + isotonic calibration に対する補完。
Cox PH は HR (hazard ratio) + 95% CI を持つ統計推定で、academic venue
(労働経済学・労働社会学) の標準。LightGBM は予測精度、Cox は解釈性。

H1: anime.score 不参入 (構造的代理量のみ)。
H2: 「離職」「キャリア終了」frame NG → "visibility loss" / "credit 出現の途絶" のみ。

Output:
- per-feature HR + 95% CI
- Schoenfeld residual test (PH assumption test)
- concordance index (Harrell's C-index, holdout)
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class CoxFitResult:
    """Cox PH 推定結果。"""

    feature_names: tuple[str, ...]
    coefficients: tuple[float, ...]           # log(HR) per feature
    hazard_ratios: tuple[float, ...]          # exp(coef)
    hr_ci_low: tuple[float, ...]              # 95% CI lower bound on HR
    hr_ci_high: tuple[float, ...]
    p_values: tuple[float, ...]
    concordance_index: float                  # Harrell's C (training)
    n_subjects: int
    n_events: int


@dataclass(frozen=True)
class PHAssumptionTest:
    """Schoenfeld residual test for PH assumption."""

    feature_names: tuple[str, ...]
    test_statistic: tuple[float, ...]
    p_values: tuple[float, ...]
    global_p_value: float
    violators: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class HoldoutEvaluation:
    """Holdout (temporal split) evaluation."""

    train_n: int
    test_n: int
    train_concordance: float
    test_concordance: float
    test_year_threshold: int  # split year


# ---------------------------------------------------------------------------
# Cox fit (uses lifelines)
# ---------------------------------------------------------------------------


def fit_cox_ph(
    durations: np.ndarray,
    events: np.ndarray,
    covariates: np.ndarray,
    feature_names: list[str],
) -> CoxFitResult:
    """Fit Cox PH on (durations, events, X). lifelines wrapper。

    Args:
        durations: time until event-or-censoring per subject (years).
        events: bool / int 0-1 (event=1, censor=0).
        covariates: shape (n, k) feature matrix.
        feature_names: length k.

    Returns:
        CoxFitResult.

    Raises:
        ImportError: lifelines unavailable.
        ValueError: shape mismatch / empty.
    """
    try:
        from lifelines import CoxPHFitter
    except ImportError as exc:
        raise ImportError(
            "lifelines is required for fit_cox_ph(). "
            "Add lifelines>=0.30 to pixi.toml."
        ) from exc
    import pandas as pd

    if durations.shape[0] != events.shape[0] or durations.shape[0] != covariates.shape[0]:
        raise ValueError("durations / events / covariates row count mismatch")
    if durations.size == 0:
        raise ValueError("empty input")
    if covariates.shape[1] != len(feature_names):
        raise ValueError("feature_names length != covariate column count")

    df = pd.DataFrame(covariates, columns=feature_names)
    df["__duration"] = durations
    df["__event"] = events.astype(int)

    cph = CoxPHFitter()
    cph.fit(df, duration_col="__duration", event_col="__event")

    summary = cph.summary
    # Order rows by feature_names to keep API stable
    summary = summary.loc[feature_names]
    coefs = summary["coef"].to_numpy(dtype=float)
    hrs = summary["exp(coef)"].to_numpy(dtype=float)
    hr_lo = summary["exp(coef) lower 95%"].to_numpy(dtype=float)
    hr_hi = summary["exp(coef) upper 95%"].to_numpy(dtype=float)
    pvals = summary["p"].to_numpy(dtype=float)

    c_index = float(cph.concordance_index_)
    n_events = int(events.sum())

    return CoxFitResult(
        feature_names=tuple(feature_names),
        coefficients=tuple(float(c) for c in coefs),
        hazard_ratios=tuple(float(h) for h in hrs),
        hr_ci_low=tuple(float(h) for h in hr_lo),
        hr_ci_high=tuple(float(h) for h in hr_hi),
        p_values=tuple(float(p) for p in pvals),
        concordance_index=c_index,
        n_subjects=int(durations.size),
        n_events=n_events,
    )


# ---------------------------------------------------------------------------
# PH assumption test (Schoenfeld residuals)
# ---------------------------------------------------------------------------


def check_ph_assumption(
    durations: np.ndarray,
    events: np.ndarray,
    covariates: np.ndarray,
    feature_names: list[str],
    *,
    sig_level: float = 0.05,
) -> PHAssumptionTest:
    """Schoenfeld residual-based PH test (lifelines.check_assumptions)。

    H0: proportional hazards (PH) holds for the feature.
    p < sig_level → reject PH for that feature.
    """
    try:
        from lifelines import CoxPHFitter
        from lifelines.statistics import proportional_hazard_test
    except ImportError as exc:
        raise ImportError("lifelines required for check_ph_assumption") from exc
    import pandas as pd

    df = pd.DataFrame(covariates, columns=feature_names)
    df["__duration"] = durations
    df["__event"] = events.astype(int)

    cph = CoxPHFitter()
    cph.fit(df, duration_col="__duration", event_col="__event")
    ph_test = proportional_hazard_test(cph, df, time_transform="rank")
    summary = ph_test.summary
    # rows: per-feature
    feat_idx = [f for f in feature_names if f in summary.index]
    stats = []
    pvals = []
    violators = []
    for f in feat_idx:
        s = float(summary.loc[f, "test_statistic"])
        p = float(summary.loc[f, "p"])
        stats.append(s)
        pvals.append(p)
        if p < sig_level:
            violators.append(f)
    # Global p = minimum p (Bonferroni-like or simple min)
    global_p = float(min(pvals)) if pvals else 1.0
    return PHAssumptionTest(
        feature_names=tuple(feat_idx),
        test_statistic=tuple(stats),
        p_values=tuple(pvals),
        global_p_value=global_p,
        violators=tuple(violators),
    )


# ---------------------------------------------------------------------------
# Temporal holdout evaluation
# ---------------------------------------------------------------------------


def evaluate_temporal_holdout(
    durations: np.ndarray,
    events: np.ndarray,
    covariates: np.ndarray,
    feature_names: list[str],
    debut_years: np.ndarray,
    *,
    split_year: int,
) -> HoldoutEvaluation:
    """Train on debut_year < split_year, test on debut_year >= split_year。

    Returns concordance on both halves. Train/test mismatch signals temporal drift.
    """
    try:
        from lifelines import CoxPHFitter
        from lifelines.utils import concordance_index
    except ImportError as exc:
        raise ImportError("lifelines required for evaluate_temporal_holdout") from exc
    import pandas as pd

    if debut_years.shape[0] != durations.shape[0]:
        raise ValueError("debut_years length != durations length")

    train_mask = debut_years < split_year
    test_mask = ~train_mask
    n_train = int(train_mask.sum())
    n_test = int(test_mask.sum())
    if n_train == 0 or n_test == 0:
        raise ValueError(
            f"empty train ({n_train}) or test ({n_test}) split at year {split_year}"
        )

    df = pd.DataFrame(covariates, columns=feature_names)
    df["__duration"] = durations
    df["__event"] = events.astype(int)

    cph = CoxPHFitter()
    cph.fit(df.loc[train_mask], duration_col="__duration", event_col="__event")
    train_c = float(cph.concordance_index_)

    # Predict partial hazards on test set, compute concordance vs test durations/events
    test_df = df.loc[test_mask]
    test_partial = cph.predict_partial_hazard(test_df).to_numpy()
    test_c = float(
        concordance_index(
            test_df["__duration"].to_numpy(),
            -test_partial,  # higher partial hazard → shorter expected survival
            test_df["__event"].to_numpy(),
        )
    )

    return HoldoutEvaluation(
        train_n=n_train,
        test_n=n_test,
        train_concordance=train_c,
        test_concordance=test_c,
        test_year_threshold=int(split_year),
    )
