"""Oaxaca-Blinder decomposition for opportunity (credit-count) gaps.

同等 theta_i / tenure / role_diversity / studio FE 条件下で観測される
group A vs B の y (= log credit_count or log production_scale_sum) の差を
以下に分離:

    ΔY = (X_A - X_B) · β̂_B   +   X_A · (β̂_A - β̂_B)
          ─────endowment─────       ────structural────

- endowment: group A と B の **構造的位置 (X)** の差で説明される部分
- structural: 同じ X でも係数 (β) が異なる → 機会の structural gap

framing (H2 厳格遵守):
- ability-framing NG。endowment = 「構造的位置の差」、structural = 「同位置での処遇差」
- group 定義の任意性は openly 開示
- gender が null の person は **必ず除外** + その量を Findings に併記

CI: cluster-robust bootstrap (cluster = person)。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Sequence

import numpy as np
import structlog

log = structlog.get_logger(__name__)

_DEFAULT_BOOTSTRAP_N = 1000
_DEFAULT_RNG_SEED = 42


@dataclass(frozen=True)
class OaxacaResult:
    """Single-pass Oaxaca-Blinder decomposition (no bootstrap)."""

    group_a_mean_y: float
    group_b_mean_y: float
    raw_gap: float
    endowment: float
    structural: float
    n_a: int
    n_b: int
    feature_names: tuple[str, ...]
    beta_a: tuple[float, ...]
    beta_b: tuple[float, ...]
    # contribution per feature (endowment-side)
    endowment_per_feature: tuple[float, ...]
    # contribution per feature (structural-side)
    structural_per_feature: tuple[float, ...]


@dataclass(frozen=True)
class OaxacaSubgroupReport:
    """Oaxaca with bootstrap CI + subgroup metadata."""

    point: OaxacaResult
    bootstrap_n: int
    endowment_ci_low: float
    endowment_ci_high: float
    structural_ci_low: float
    structural_ci_high: float
    raw_gap_ci_low: float
    raw_gap_ci_high: float
    subgroup_label: str
    n_excluded_missing_y: int = 0
    n_excluded_missing_x: int = 0
    n_excluded_missing_group: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# OLS fit (intercept + cluster-aware) — minimal hand-rolled, statsmodels 不依存
# ---------------------------------------------------------------------------


def fit_group_ols(
    y: np.ndarray, x: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """Fit OLS β for one group, returning (β with intercept first, X̄).

    X shape: (n, k). Returns (β, x_mean) where β has length k+1 (intercept first).
    """
    if y.shape[0] != x.shape[0]:
        raise ValueError(f"y rows {y.shape[0]} != X rows {x.shape[0]}")
    if y.size == 0:
        raise ValueError("Empty group passed to fit_group_ols")
    n, k = x.shape
    # Add intercept column
    x_full = np.hstack([np.ones((n, 1)), x])  # (n, k+1)
    # Solve β = (X'X)^{-1} X'y via lstsq for numerical stability
    beta, _, _, _ = np.linalg.lstsq(x_full, y, rcond=None)
    x_mean = x.mean(axis=0)  # length k (excluding intercept)
    return beta, x_mean


# ---------------------------------------------------------------------------
# Single-pass Oaxaca-Blinder (no CI)
# ---------------------------------------------------------------------------


def decompose_oaxaca_blinder(
    y_a: np.ndarray,
    x_a: np.ndarray,
    y_b: np.ndarray,
    x_b: np.ndarray,
    feature_names: Sequence[str],
) -> OaxacaResult:
    """Compute single-pass Oaxaca decomposition (group A is the focal group).

    ΔY = ȳ_A - ȳ_B
        = (X̄_A - X̄_B) · β̂_B           # endowment (差分の構造的位置で説明される部分)
        + X̄_A · (β̂_A - β̂_B)            # structural (同じ X でも係数が違う)

    Args:
        y_a, x_a: group A outcomes + features.
        y_b, x_b: group B outcomes + features.
        feature_names: 列名 (intercept は含めない)。len(feature_names) == x_a.shape[1].

    Returns:
        OaxacaResult with per-feature endowment / structural contributions.
        Per-feature reported in original feature scale (intercept は structural の
        constant 項に統合される)。
    """
    if x_a.shape[1] != x_b.shape[1]:
        raise ValueError(
            f"Feature dim mismatch: A={x_a.shape[1]}, B={x_b.shape[1]}"
        )
    if len(feature_names) != x_a.shape[1]:
        raise ValueError(
            f"feature_names length {len(feature_names)} != X dim {x_a.shape[1]}"
        )

    beta_a, x_mean_a = fit_group_ols(y_a, x_a)
    beta_b, x_mean_b = fit_group_ols(y_b, x_b)

    ybar_a = float(y_a.mean())
    ybar_b = float(y_b.mean())
    raw_gap = ybar_a - ybar_b

    # endowment (per feature, excluding intercept): (X̄_A - X̄_B) * β_B[1:]
    end_per = (x_mean_a - x_mean_b) * beta_b[1:]
    # structural (per feature): X̄_A * (β_A[1:] - β_B[1:])
    str_per_feat = x_mean_a * (beta_a[1:] - beta_b[1:])
    # structural constant (intercept differential): β_A[0] - β_B[0]
    str_const = beta_a[0] - beta_b[0]

    endowment = float(end_per.sum())
    structural = float(str_per_feat.sum() + str_const)

    return OaxacaResult(
        group_a_mean_y=ybar_a,
        group_b_mean_y=ybar_b,
        raw_gap=raw_gap,
        endowment=endowment,
        structural=structural,
        n_a=int(y_a.size),
        n_b=int(y_b.size),
        feature_names=tuple(feature_names),
        beta_a=tuple(float(v) for v in beta_a),
        beta_b=tuple(float(v) for v in beta_b),
        endowment_per_feature=tuple(float(v) for v in end_per),
        structural_per_feature=tuple(float(v) for v in str_per_feat),
    )


# ---------------------------------------------------------------------------
# Subgroup wrapper with bootstrap CI
# ---------------------------------------------------------------------------


def _resample_indices(n: int, rng: np.random.Generator) -> np.ndarray:
    return rng.integers(low=0, high=n, size=n)


def decompose_subgroup(
    y_a: np.ndarray,
    x_a: np.ndarray,
    y_b: np.ndarray,
    x_b: np.ndarray,
    feature_names: Sequence[str],
    *,
    subgroup_label: str = "all",
    bootstrap_n: int = _DEFAULT_BOOTSTRAP_N,
    rng_seed: int = _DEFAULT_RNG_SEED,
    ci_level: float = 0.95,
    n_excluded_missing_y: int = 0,
    n_excluded_missing_x: int = 0,
    n_excluded_missing_group: int = 0,
    extra_notes: Sequence[str] = (),
) -> OaxacaSubgroupReport:
    """Oaxaca + bootstrap (resample within each group, percentile CI).

    Returns OaxacaSubgroupReport with point estimate + percentile CIs.

    Notes:
        - bootstrap_n=1000 default. Lower for tests via param.
        - cluster=person 想定だが、本関数は 1-row-per-person を仮定 (person-level y / X).
        - subgroup_label は report 表示用 (例: "JP / 1990s cohort").
    """
    point = decompose_oaxaca_blinder(y_a, x_a, y_b, x_b, feature_names)
    rng = np.random.default_rng(rng_seed)

    end_samples = np.empty(bootstrap_n)
    str_samples = np.empty(bootstrap_n)
    raw_samples = np.empty(bootstrap_n)

    n_a = int(y_a.size)
    n_b = int(y_b.size)

    boot_failures = 0
    for b in range(bootstrap_n):
        idx_a = _resample_indices(n_a, rng)
        idx_b = _resample_indices(n_b, rng)
        try:
            res = decompose_oaxaca_blinder(
                y_a[idx_a], x_a[idx_a], y_b[idx_b], x_b[idx_b], feature_names
            )
            end_samples[b] = res.endowment
            str_samples[b] = res.structural
            raw_samples[b] = res.raw_gap
        except (ValueError, np.linalg.LinAlgError):
            boot_failures += 1
            end_samples[b] = np.nan
            str_samples[b] = np.nan
            raw_samples[b] = np.nan

    # Drop NaN samples, compute percentile CI
    end_clean = end_samples[~np.isnan(end_samples)]
    str_clean = str_samples[~np.isnan(str_samples)]
    raw_clean = raw_samples[~np.isnan(raw_samples)]

    alpha = (1.0 - ci_level) / 2.0
    lo_pct = alpha * 100
    hi_pct = (1.0 - alpha) * 100

    notes: list[str] = list(extra_notes)
    if boot_failures:
        notes.append(
            f"bootstrap_failures={boot_failures}/{bootstrap_n} "
            "(singular X — small subgroup risk)"
        )

    return OaxacaSubgroupReport(
        point=point,
        bootstrap_n=bootstrap_n,
        endowment_ci_low=float(np.percentile(end_clean, lo_pct)) if end_clean.size else float("nan"),
        endowment_ci_high=float(np.percentile(end_clean, hi_pct)) if end_clean.size else float("nan"),
        structural_ci_low=float(np.percentile(str_clean, lo_pct)) if str_clean.size else float("nan"),
        structural_ci_high=float(np.percentile(str_clean, hi_pct)) if str_clean.size else float("nan"),
        raw_gap_ci_low=float(np.percentile(raw_clean, lo_pct)) if raw_clean.size else float("nan"),
        raw_gap_ci_high=float(np.percentile(raw_clean, hi_pct)) if raw_clean.size else float("nan"),
        subgroup_label=subgroup_label,
        n_excluded_missing_y=n_excluded_missing_y,
        n_excluded_missing_x=n_excluded_missing_x,
        n_excluded_missing_group=n_excluded_missing_group,
        notes=tuple(notes),
    )
