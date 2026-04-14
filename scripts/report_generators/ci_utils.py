"""Statistical utilities for REPORT_PHILOSOPHY v2 compliance.

Provides:
- Confidence intervals (analytical SE, bootstrap)
- Distribution summaries (mandatory: mean + median + percentiles + shape)
- Null model comparison
- Sensitivity analysis runner
"""

from __future__ import annotations

import math
from typing import Any, Callable

import numpy as np


def analytical_ci(
    values: np.ndarray | list[float],
    confidence: float = 0.95,
) -> tuple[float, float]:
    """Analytical confidence interval using SE = sigma / sqrt(n).

    Per CLAUDE.md: compensation-basis CIs must be analytically derived.

    Returns:
        (ci_lower, ci_upper)
    """
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    n = len(arr)
    if n < 2:
        return (float("nan"), float("nan"))

    mean = float(np.mean(arr))
    se = float(np.std(arr, ddof=1)) / math.sqrt(n)

    # z-value for given confidence (e.g. 1.96 for 95%)
    from scipy.stats import norm

    z = norm.ppf(1 - (1 - confidence) / 2)
    return (mean - z * se, mean + z * se)


def bootstrap_ci(
    values: np.ndarray | list[float],
    n_boot: int = 2000,
    confidence: float = 0.95,
    statistic: Callable = np.mean,
    seed: int = 42,
) -> tuple[float, float]:
    """Bootstrap confidence interval for non-normal distributions.

    Returns:
        (ci_lower, ci_upper)
    """
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    n = len(arr)
    if n < 2:
        return (float("nan"), float("nan"))

    rng = np.random.default_rng(seed)
    boot_stats = np.empty(n_boot)
    for i in range(n_boot):
        sample = rng.choice(arr, size=n, replace=True)
        boot_stats[i] = statistic(sample)

    alpha = (1 - confidence) / 2
    return (float(np.percentile(boot_stats, 100 * alpha)),
            float(np.percentile(boot_stats, 100 * (1 - alpha))))


def distribution_summary(
    values: np.ndarray | list[float],
    confidence: float = 0.95,
    label: str = "",
) -> dict[str, Any]:
    """Full distribution summary — mandatory per v2 (means-only is prohibited).

    Returns dict with: n, mean, median, std, p10, p25, p75, p90,
    ci_lower, ci_upper, skewness, shape_description.
    """
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    n = len(arr)

    if n == 0:
        return {
            "label": label, "n": 0,
            "mean": float("nan"), "median": float("nan"), "std": float("nan"),
            "p10": float("nan"), "p25": float("nan"),
            "p75": float("nan"), "p90": float("nan"),
            "ci_lower": float("nan"), "ci_upper": float("nan"),
            "skewness": float("nan"), "shape": "no data",
        }

    mean = float(np.mean(arr))
    median = float(np.median(arr))
    std = float(np.std(arr, ddof=1)) if n >= 2 else 0.0

    p10, p25, p75, p90 = (
        float(np.percentile(arr, q)) for q in [10, 25, 75, 90]
    )

    ci_lo, ci_hi = analytical_ci(arr, confidence) if n >= 2 else (mean, mean)

    # Skewness
    if n >= 3 and std > 0:
        skew = float(np.mean(((arr - mean) / std) ** 3))
    else:
        skew = 0.0

    # Shape description (plain language for v2 findings)
    if abs(skew) < 0.5:
        shape = "approximately symmetric"
    elif skew > 1.5:
        shape = "strongly right-skewed"
    elif skew > 0.5:
        shape = "moderately right-skewed"
    elif skew < -1.5:
        shape = "strongly left-skewed"
    else:
        shape = "moderately left-skewed"

    return {
        "label": label,
        "n": n,
        "mean": round(mean, 4),
        "median": round(median, 4),
        "std": round(std, 4),
        "p10": round(p10, 4),
        "p25": round(p25, 4),
        "p75": round(p75, 4),
        "p90": round(p90, 4),
        "ci_lower": round(ci_lo, 4),
        "ci_upper": round(ci_hi, 4),
        "skewness": round(skew, 4),
        "shape": shape,
    }


def null_model_comparison(
    observed: float,
    baseline_values: np.ndarray | list[float],
    label: str = "",
) -> dict[str, Any]:
    """Compare observed statistic against a null/baseline distribution.

    Per v2 Section 3.2: population claims must compare vs baseline.

    Returns:
        {observed, baseline_mean, baseline_ci, z_score, p_value, effect_size, label}
    """
    base = np.asarray(baseline_values, dtype=float)
    base = base[np.isfinite(base)]
    n = len(base)
    if n < 2:
        return {
            "label": label,
            "observed": observed,
            "baseline_mean": float("nan"),
            "baseline_ci": (float("nan"), float("nan")),
            "z_score": float("nan"),
            "p_value": float("nan"),
            "effect_size": float("nan"),
            "significant": False,
        }

    base_mean = float(np.mean(base))
    base_std = float(np.std(base, ddof=1))
    ci_lo, ci_hi = analytical_ci(base)

    z = (observed - base_mean) / base_std if base_std > 0 else 0.0

    from scipy.stats import norm

    p_value = 2 * (1 - norm.cdf(abs(z)))  # two-tailed
    effect_size = (observed - base_mean) / base_std if base_std > 0 else 0.0

    return {
        "label": label,
        "observed": round(observed, 4),
        "baseline_mean": round(base_mean, 4),
        "baseline_ci": (round(ci_lo, 4), round(ci_hi, 4)),
        "z_score": round(z, 4),
        "p_value": round(p_value, 6),
        "effect_size": round(effect_size, 4),
        "significant": p_value < 0.05,
    }


def sensitivity_check(
    compute_fn: Callable[..., float],
    param_name: str,
    param_values: list,
    **fixed_kwargs: Any,
) -> list[dict[str, Any]]:
    """Run computation under alternative parameter choices.

    Per v2 Section 3.4: results dependent on thresholds must show
    sensitivity to alternative choices.

    Args:
        compute_fn: Function that returns a scalar result
        param_name: Name of the parameter to vary
        param_values: List of alternative parameter values
        **fixed_kwargs: Fixed keyword arguments to compute_fn

    Returns:
        List of {param_value, result} dicts
    """
    results = []
    for pval in param_values:
        kwargs = {**fixed_kwargs, param_name: pval}
        try:
            result = compute_fn(**kwargs)
            results.append({"param_value": pval, "result": result})
        except Exception as e:
            results.append({"param_value": pval, "result": None, "error": str(e)})
    return results


def format_ci(ci: tuple[float, float], precision: int = 2) -> str:
    """Format a CI tuple as a string: '95% CI [1.23, 4.56]'."""
    if math.isnan(ci[0]) or math.isnan(ci[1]):
        return "CI: insufficient data"
    return f"95% CI [{ci[0]:.{precision}f}, {ci[1]:.{precision}f}]"


def format_distribution_inline(summary: dict[str, Any], precision: int = 2) -> str:
    """Format a distribution summary as inline text for Findings sections.

    Example output: 'median=1.23 (IQR 0.89-1.67, n=1,234, moderately right-skewed)'
    """
    if summary["n"] == 0:
        return "no observations"
    p = precision
    return (
        f"median={summary['median']:.{p}f} "
        f"(IQR {summary['p25']:.{p}f}\u2013{summary['p75']:.{p}f}, "
        f"n={summary['n']:,}, "
        f"{summary['shape']})"
    )
