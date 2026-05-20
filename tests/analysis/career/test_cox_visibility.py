"""Tests for src/analysis/career/cox_visibility — Cox PH wrapper."""

from __future__ import annotations

import numpy as np
import pytest

# lifelines is required for these tests
lifelines = pytest.importorskip("lifelines")

from src.analysis.career.cox_visibility import (  # noqa: E402 — import after importorskip
    CoxFitResult,
    evaluate_temporal_holdout,
    fit_cox_ph,
    check_ph_assumption,
)


def _make_synthetic_survival(n: int = 300, seed: int = 7) -> tuple[np.ndarray, np.ndarray, np.ndarray, list[str]]:
    """Generate a clean Cox-conformant dataset.

    feature[0] (theta_i) decreases hazard (negative coef).
    feature[1] (tenure)  increases hazard (positive coef).
    """
    rng = np.random.default_rng(seed)
    theta = rng.normal(0, 1, n)
    tenure = rng.uniform(0, 10, n)
    X = np.column_stack([theta, tenure])
    # True log-hazard = -0.7 * theta + 0.3 * tenure
    linear = -0.7 * theta + 0.3 * tenure
    # Baseline hazard exponential with rate 0.1
    u = rng.uniform(0, 1, n)
    durations = -np.log(u) / (0.1 * np.exp(linear))
    # Censoring at duration > 15
    events = (durations < 15).astype(int)
    durations = np.minimum(durations, 15)
    return durations, events, X, ["theta_i", "tenure"]


class TestCoxFit:
    def test_returns_result(self):
        d, e, X, names = _make_synthetic_survival()
        result = fit_cox_ph(d, e, X, names)
        assert isinstance(result, CoxFitResult)
        assert result.n_subjects == d.size

    def test_feature_names_preserved(self):
        d, e, X, names = _make_synthetic_survival()
        result = fit_cox_ph(d, e, X, names)
        assert result.feature_names == ("theta_i", "tenure")

    def test_correct_sign_of_coef(self):
        d, e, X, names = _make_synthetic_survival(n=600, seed=11)
        result = fit_cox_ph(d, e, X, names)
        # theta_i has negative true coef → HR < 1
        idx_theta = result.feature_names.index("theta_i")
        assert result.hazard_ratios[idx_theta] < 1.0
        idx_tenure = result.feature_names.index("tenure")
        assert result.hazard_ratios[idx_tenure] > 1.0

    def test_ci_brackets_hr(self):
        d, e, X, names = _make_synthetic_survival()
        result = fit_cox_ph(d, e, X, names)
        for hr, lo, hi in zip(result.hazard_ratios, result.hr_ci_low, result.hr_ci_high):
            assert lo <= hr <= hi

    def test_concordance_above_random(self):
        d, e, X, names = _make_synthetic_survival(n=500)
        result = fit_cox_ph(d, e, X, names)
        # Real signal → C-index > 0.55
        assert result.concordance_index > 0.55

    def test_shape_mismatch_raises(self):
        d = np.array([1.0, 2.0, 3.0])
        e = np.array([1, 0])
        X = np.zeros((3, 2))
        with pytest.raises(ValueError):
            fit_cox_ph(d, e, X, ["a", "b"])

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            fit_cox_ph(np.array([]), np.array([]), np.zeros((0, 2)), ["a", "b"])

    def test_feature_count_mismatch_raises(self):
        d, e, X, _ = _make_synthetic_survival()
        with pytest.raises(ValueError):
            fit_cox_ph(d, e, X, ["only_one"])


class TestPHAssumption:
    def test_returns_test_result(self):
        d, e, X, names = _make_synthetic_survival(n=300, seed=23)
        ph = check_ph_assumption(d, e, X, names)
        assert ph.feature_names == ("theta_i", "tenure")
        assert len(ph.p_values) == 2

    def test_global_p_is_min(self):
        d, e, X, names = _make_synthetic_survival(n=300, seed=29)
        ph = check_ph_assumption(d, e, X, names)
        assert ph.global_p_value == min(ph.p_values)


class TestTemporalHoldout:
    def test_returns_evaluation(self):
        d, e, X, names = _make_synthetic_survival(n=400, seed=31)
        debut = np.random.default_rng(31).integers(2000, 2020, d.size)
        ev = evaluate_temporal_holdout(d, e, X, names, debut, split_year=2010)
        assert ev.train_n > 0
        assert ev.test_n > 0
        assert ev.train_n + ev.test_n == d.size

    def test_concordance_finite(self):
        d, e, X, names = _make_synthetic_survival(n=600, seed=37)
        debut = np.random.default_rng(37).integers(2000, 2020, d.size)
        ev = evaluate_temporal_holdout(d, e, X, names, debut, split_year=2010)
        assert 0.0 <= ev.train_concordance <= 1.0
        assert 0.0 <= ev.test_concordance <= 1.0

    def test_empty_test_raises(self):
        d, e, X, names = _make_synthetic_survival(n=100, seed=41)
        debut = np.full(d.size, 2000)  # all before threshold
        with pytest.raises(ValueError):
            evaluate_temporal_holdout(d, e, X, names, debut, split_year=2010)
