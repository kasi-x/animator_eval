"""Cohort-level inequality measures (Gini / Theil / Atkinson).

同年デビュー (or 同年代) cohort 内の構造的位置 (theta_i / credit_count) 分布の
不平等を測定。年次推移 = 構造的格差の拡大 / 縮小トラックを可視化。

H1: anime.score 不参入 (構造的代理量のみ — credit_count / theta_i)。
H2: 主観的評価 frame NG → "structural position inequality" として扱う。

References:
    - Gini (1912): mean absolute difference based concentration.
    - Theil (1967): entropy-based decomposable inequality.
    - Atkinson (1970): welfare-loss interpretation, ε parameter.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Single-cohort inequality measures
# ---------------------------------------------------------------------------


def gini_coefficient(values: Sequence[float]) -> float:
    """Gini coefficient (0 = perfect equality, 1 = max concentration).

    Mean absolute difference / (2 * mean). NaN / negative protected: drop negatives,
    return 0 for empty / all-zero.
    """
    arr = np.asarray([v for v in values if v is not None and v == v], dtype=float)
    if arr.size == 0:
        return 0.0
    # Negative values not meaningful for credit_count; clip to 0.
    arr = np.clip(arr, 0.0, None)
    if arr.sum() == 0.0:
        return 0.0
    arr = np.sort(arr)
    n = arr.size
    # Reference formula: G = (sum_{i=1..n} (2i - n - 1) x_i) / (n * sum(x))
    i = np.arange(1, n + 1, dtype=float)
    weighted = (2.0 * i - n - 1.0) * arr
    return float(weighted.sum() / (n * arr.sum()))


def theil_t_index(values: Sequence[float]) -> float:
    """Theil-T entropy index. Always >= 0; higher = more inequality.

    T = (1/N) * sum_i (x_i / mean) * ln(x_i / mean).
    Zero-valued elements treated with epsilon to avoid log(0).
    """
    arr = np.asarray([v for v in values if v is not None and v == v], dtype=float)
    if arr.size == 0:
        return 0.0
    arr = np.clip(arr, 0.0, None)
    mu = arr.mean()
    if mu <= 0:
        return 0.0
    # Avoid log(0)
    eps = 1e-12
    ratio = (arr + eps) / mu
    return float(np.mean(ratio * np.log(ratio)))


def atkinson_index(values: Sequence[float], *, epsilon: float = 0.5) -> float:
    """Atkinson inequality index. 0 = perfect equality, 1 = max inequality.

    A_ε = 1 - (mean_{generalized,ε}(x) / mean(x)).
    ε = 0.5 (mild welfare loss assumption) by default.
    """
    if epsilon < 0:
        raise ValueError("epsilon must be >= 0")
    arr = np.asarray([v for v in values if v is not None and v == v], dtype=float)
    if arr.size == 0:
        return 0.0
    arr = np.clip(arr, 0.0, None)
    if arr.sum() == 0.0:
        return 0.0
    mu = arr.mean()
    eps = 1e-12
    if epsilon == 1.0:
        # Geometric mean form
        gm = np.exp(np.mean(np.log(arr + eps)))
        return float(1.0 - gm / mu)
    # General form
    transformed = np.power(arr + eps, 1.0 - epsilon)
    mean_t = transformed.mean()
    inv = np.power(mean_t, 1.0 / (1.0 - epsilon))
    return float(1.0 - inv / mu)


# ---------------------------------------------------------------------------
# Bootstrap CI for any single-cohort inequality
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InequalityWithCI:
    """Point estimate + percentile CI for a single inequality metric."""

    metric: str
    point: float
    ci_low: float
    ci_high: float
    n: int
    bootstrap_n: int


def bootstrap_inequality(
    values: Sequence[float],
    metric_fn,
    *,
    metric_name: str,
    bootstrap_n: int = 1000,
    rng_seed: int = 42,
    ci_level: float = 0.95,
) -> InequalityWithCI:
    """Compute point + bootstrap CI for an arbitrary inequality function."""
    arr = np.asarray([v for v in values if v is not None and v == v], dtype=float)
    if arr.size == 0:
        return InequalityWithCI(
            metric=metric_name, point=0.0, ci_low=0.0, ci_high=0.0, n=0,
            bootstrap_n=bootstrap_n,
        )
    point = float(metric_fn(arr))
    rng = np.random.default_rng(rng_seed)
    samples = np.empty(bootstrap_n)
    for b in range(bootstrap_n):
        idx = rng.integers(low=0, high=arr.size, size=arr.size)
        samples[b] = metric_fn(arr[idx])
    alpha = (1.0 - ci_level) / 2.0
    return InequalityWithCI(
        metric=metric_name,
        point=point,
        ci_low=float(np.percentile(samples, alpha * 100)),
        ci_high=float(np.percentile(samples, (1.0 - alpha) * 100)),
        n=int(arr.size),
        bootstrap_n=bootstrap_n,
    )


# ---------------------------------------------------------------------------
# Cohort × time trajectory
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CohortInequalityRow:
    cohort_year: int           # 例: 1990 (decade) or exact debut year
    n_persons: int
    gini: float
    theil_t: float
    atkinson_0_5: float
    mean_value: float
    sd_value: float


def compute_cohort_trajectory(
    records: list[tuple[int, float]],
    *,
    bin_width: int = 5,
    min_cohort_n: int = 30,
) -> list[CohortInequalityRow]:
    """records = [(debut_year, value)] → cohort 別不平等指標時系列。

    Args:
        records: 1 row per person。value = log credit_count or theta_i。
        bin_width: cohort 幅 (年)。5 = 5 年 cohort、10 = 世代。
        min_cohort_n: cohort 内 person 数の下限。これ未満は除外。

    Returns:
        cohort_year (bin の下端) でソート済 list。
    """
    by_bin: dict[int, list[float]] = {}
    for year, value in records:
        if year is None or value is None:
            continue
        try:
            yi = int(year)
            vi = float(value)
        except (TypeError, ValueError):
            continue
        if vi != vi:  # NaN
            continue
        bin_key = (yi // bin_width) * bin_width
        by_bin.setdefault(bin_key, []).append(vi)

    rows: list[CohortInequalityRow] = []
    for bin_key in sorted(by_bin):
        vals = np.asarray(by_bin[bin_key], dtype=float)
        if vals.size < min_cohort_n:
            log.debug(
                "cohort_skipped_low_n", cohort_year=bin_key, n=int(vals.size),
                threshold=min_cohort_n,
            )
            continue
        rows.append(
            CohortInequalityRow(
                cohort_year=bin_key,
                n_persons=int(vals.size),
                gini=gini_coefficient(vals),
                theil_t=theil_t_index(vals),
                atkinson_0_5=atkinson_index(vals, epsilon=0.5),
                mean_value=float(vals.mean()),
                sd_value=float(vals.std(ddof=1)) if vals.size > 1 else 0.0,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Inter-cohort comparison (e.g. 1990s vs 2010s)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CohortComparison:
    """二つの cohort 間の不平等差分。"""

    cohort_a_label: str
    cohort_b_label: str
    gini_a: InequalityWithCI
    gini_b: InequalityWithCI
    gini_diff: float
    # CI が重ならない = 有意な差
    ci_overlap: bool
    interpretation: str


def compare_cohorts(
    values_a: Sequence[float],
    values_b: Sequence[float],
    *,
    label_a: str,
    label_b: str,
    bootstrap_n: int = 1000,
    rng_seed: int = 42,
) -> CohortComparison:
    """二つの cohort 値を Gini で比較。CI 重複から有意性を判断。"""
    a = bootstrap_inequality(
        values_a, gini_coefficient,
        metric_name="gini", bootstrap_n=bootstrap_n, rng_seed=rng_seed,
    )
    b = bootstrap_inequality(
        values_b, gini_coefficient,
        metric_name="gini", bootstrap_n=bootstrap_n, rng_seed=rng_seed + 1,
    )
    diff = a.point - b.point
    overlap = not (a.ci_high < b.ci_low or b.ci_high < a.ci_low)
    if overlap:
        interp = "区別不能 (CI 重複)"
    elif diff > 0:
        interp = f"{label_a} の不平等が {label_b} より大きい (CI 非重複)"
    else:
        interp = f"{label_b} の不平等が {label_a} より大きい (CI 非重複)"

    return CohortComparison(
        cohort_a_label=label_a,
        cohort_b_label=label_b,
        gini_a=a,
        gini_b=b,
        gini_diff=float(diff),
        ci_overlap=overlap,
        interpretation=interp,
    )
