"""Uncertainty estimation: analytic, bootstrap, and delta-method CIs.

These functions produce ``UncertaintyInfo`` instances that plug directly
into ``FindingSpec`` objects.  Each function is statistic-agnostic: you
supply a point estimate (or the raw sample) and get back a CI.

Usage example (bootstrap CI on Gini)::

    from src.analysis.uncertainty import bootstrap_ci

    ui = bootstrap_ci(
        sample=incomes,
        statistic=gini,        # (array) -> float
        n_bootstrap=2000,
        seed=42,
    )
    # ui.estimate = Gini of the full sample
    # ui.ci_lower, ui.ci_upper = percentile CI
"""

from __future__ import annotations

import math
from typing import Callable, Sequence

import numpy as np

from dataclasses import dataclass, field as _field


@dataclass
class UncertaintyInfo:
    """Uncertainty bounds attached to a statistic."""

    estimate: float | None = None
    ci_lower: float | None = None
    ci_upper: float | None = None
    ci_level: float = 0.95
    standard_error: float | None = None
    p_value: float | None = None
    n: int | None = None
    n_bootstrap: int | None = None
    method: str = ""
    source_code_ref: str = ""

    def has_interval(self) -> bool:
        return self.ci_lower is not None and self.ci_upper is not None


# ---------------------------------------------------------------------------
# Analytic (normal approximation)
# ---------------------------------------------------------------------------


def analytic_ci(
    estimate: float,
    standard_error: float,
    n: int,
    *,
    ci_level: float = 0.95,
    method: str = "analytic_normal",
    source_code_ref: str = "",
) -> UncertaintyInfo:
    """Compute a CI from a point estimate and its standard error.

    Uses the normal quantile ``z_{alpha/2}`` for the given ``ci_level``.
    """
    if ci_level <= 0 or ci_level >= 1:
        raise ValueError(f"ci_level must be in (0, 1), got {ci_level}")
    if standard_error < 0:
        raise ValueError(f"standard_error must be >= 0, got {standard_error}")

    # Normal quantile via the probit approximation (good to 4 decimal places)
    alpha = 1 - ci_level
    z = _normal_quantile(1 - alpha / 2)
    half_width = z * standard_error

    return UncertaintyInfo(
        estimate=estimate,
        ci_lower=estimate - half_width,
        ci_upper=estimate + half_width,
        ci_level=ci_level,
        standard_error=standard_error,
        n=n,
        method=method,
        source_code_ref=source_code_ref,
    )


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


def bootstrap_ci(
    sample: Sequence[float] | np.ndarray,
    statistic: Callable[[np.ndarray], float],
    *,
    n_bootstrap: int = 2000,
    ci_level: float = 0.95,
    seed: int | None = None,
    method: str = "bootstrap",
    source_code_ref: str = "",
) -> UncertaintyInfo:
    """Compute a percentile bootstrap CI.

    Parameters
    ----------
    sample:
        The observed data (1-D).
    statistic:
        A function ``f(arr) -> float`` computing the statistic of interest.
    n_bootstrap:
        Number of resamples.
    ci_level:
        Confidence level (default 0.95).
    seed:
        RNG seed for reproducibility.
    """
    arr = np.asarray(sample, dtype=float)
    if arr.ndim != 1 or len(arr) == 0:
        raise ValueError("sample must be a non-empty 1-D array")

    rng = np.random.default_rng(seed)
    point_estimate = float(statistic(arr))

    # Generate all bootstrap index matrices at once (vectorized)
    idx_matrix = rng.integers(0, len(arr), size=(n_bootstrap, len(arr)))
    boot_samples = arr[idx_matrix]  # shape: (n_bootstrap, n)

    # Try vectorized path first (works for np.mean, np.median, etc.)
    try:
        boot_stats = np.apply_along_axis(statistic, 1, boot_samples)
    except Exception:
        # Fallback for statistics that don't work with apply_along_axis
        boot_stats = np.array([statistic(boot_samples[i]) for i in range(n_bootstrap)])

    alpha = 1 - ci_level
    lo = float(np.percentile(boot_stats, 100 * alpha / 2))
    hi = float(np.percentile(boot_stats, 100 * (1 - alpha / 2)))
    se = float(np.std(boot_stats, ddof=1))

    return UncertaintyInfo(
        estimate=point_estimate,
        ci_lower=lo,
        ci_upper=hi,
        ci_level=ci_level,
        standard_error=se,
        n=len(arr),
        n_bootstrap=n_bootstrap,
        method=method,
        source_code_ref=source_code_ref,
    )


# ---------------------------------------------------------------------------
# Delta method
# ---------------------------------------------------------------------------


def delta_method_ci(
    estimate: float,
    gradient: float,
    variance_of_input: float,
    n: int,
    *,
    ci_level: float = 0.95,
    method: str = "delta_method",
    source_code_ref: str = "",
) -> UncertaintyInfo:
    """Compute a CI via the delta method.

    For ``g(X)`` where ``Var(X)`` is known, the delta method gives::

        Var(g(X)) ≈ (g'(X))^2 * Var(X)
        SE(g(X)) ≈ |g'(X)| * sqrt(Var(X) / n)

    Parameters
    ----------
    estimate:
        The transformed point estimate ``g(X_bar)``.
    gradient:
        ``g'(X_bar)``, the derivative evaluated at the sample mean.
    variance_of_input:
        ``Var(X)`` (population or sample variance of the input variable).
    n:
        Sample size.
    """
    se = abs(gradient) * math.sqrt(variance_of_input / n)
    return analytic_ci(
        estimate=estimate,
        standard_error=se,
        n=n,
        ci_level=ci_level,
        method=method,
        source_code_ref=source_code_ref,
    )


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _normal_quantile(p: float) -> float:
    """Approximate the standard normal quantile (inverse CDF).

    Uses the rational approximation from Abramowitz & Stegun (26.2.23),
    accurate to ~4.5 × 10⁻⁴. For our CI purposes this is more than
    sufficient and avoids pulling in scipy.
    """
    if p <= 0 or p >= 1:
        raise ValueError(f"p must be in (0, 1), got {p}")

    # Work with p in (0.5, 1) and negate at the end if needed
    if p < 0.5:
        return -_normal_quantile(1 - p)

    # Rational approximation (A&S 26.2.23)
    t = math.sqrt(-2 * math.log(1 - p))
    c0, c1, c2 = 2.515517, 0.802853, 0.010328
    d1, d2, d3 = 1.432788, 0.189269, 0.001308
    return t - (c0 + c1 * t + c2 * t * t) / (1 + d1 * t + d2 * t * t + d3 * t * t * t)
