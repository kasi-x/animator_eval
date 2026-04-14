"""Tests for src/analysis/uncertainty.py."""

from __future__ import annotations

import math

import numpy as np
import pytest

from src.analysis.uncertainty import analytic_ci, bootstrap_ci, delta_method_ci


# ---------------------------------------------------------------------------
# analytic_ci
# ---------------------------------------------------------------------------


def test_analytic_ci_symmetric() -> None:
    ui = analytic_ci(estimate=10.0, standard_error=2.0, n=100)
    # 95% CI with z ≈ 1.96: half-width ≈ 3.92
    assert ui.has_interval()
    assert abs((ui.ci_upper - ui.ci_lower) / 2 - 1.96 * 2.0) < 0.05
    assert ui.n == 100
    assert ui.method == "analytic_normal"


def test_analytic_ci_zero_se() -> None:
    ui = analytic_ci(estimate=5.0, standard_error=0.0, n=50)
    assert ui.ci_lower == 5.0
    assert ui.ci_upper == 5.0


def test_analytic_ci_negative_se_raises() -> None:
    with pytest.raises(ValueError, match="standard_error"):
        analytic_ci(estimate=0.0, standard_error=-1.0, n=10)


def test_analytic_ci_bad_level() -> None:
    with pytest.raises(ValueError, match="ci_level"):
        analytic_ci(estimate=0.0, standard_error=1.0, n=10, ci_level=1.5)


# ---------------------------------------------------------------------------
# bootstrap_ci
# ---------------------------------------------------------------------------


def test_bootstrap_ci_mean() -> None:
    rng = np.random.default_rng(0)
    sample = rng.normal(50, 10, size=200)
    ui = bootstrap_ci(sample, np.mean, seed=42, n_bootstrap=500)
    assert ui.has_interval()
    assert ui.ci_lower < 50 < ui.ci_upper
    assert ui.n == 200
    assert ui.n_bootstrap == 500
    assert ui.method == "bootstrap"


def test_bootstrap_ci_deterministic_with_seed() -> None:
    sample = np.arange(100, dtype=float)
    a = bootstrap_ci(sample, np.median, seed=123, n_bootstrap=100)
    b = bootstrap_ci(sample, np.median, seed=123, n_bootstrap=100)
    assert a.ci_lower == b.ci_lower
    assert a.ci_upper == b.ci_upper


def test_bootstrap_ci_empty_raises() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        bootstrap_ci([], np.mean)


def test_bootstrap_ci_custom_statistic() -> None:
    sample = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    ui = bootstrap_ci(sample, lambda x: float(np.max(x) - np.min(x)), seed=0)
    assert ui.estimate == 4.0  # range of the full sample


# ---------------------------------------------------------------------------
# delta_method_ci
# ---------------------------------------------------------------------------


def test_delta_method_log() -> None:
    """Delta method for g(X) = log(X): g'(X) = 1/X."""
    x_bar = 10.0
    var_x = 4.0
    n = 100
    ui = delta_method_ci(
        estimate=math.log(x_bar),
        gradient=1 / x_bar,
        variance_of_input=var_x,
        n=n,
    )
    assert ui.has_interval()
    # SE(log(X)) = (1/X)*sqrt(Var(X)/n) = 0.1*0.2 = 0.02
    assert abs(ui.standard_error - 0.02) < 0.005
    assert ui.method == "delta_method"
