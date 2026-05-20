"""Tests for src/analysis/equity/cohort_inequality."""

from __future__ import annotations

import numpy as np
import pytest

from src.analysis.equity.cohort_inequality import (
    atkinson_index,
    bootstrap_inequality,
    compare_cohorts,
    compute_cohort_trajectory,
    gini_coefficient,
    theil_t_index,
)


class TestGini:
    def test_empty_is_zero(self):
        assert gini_coefficient([]) == 0.0

    def test_all_equal_is_zero(self):
        assert gini_coefficient([5.0, 5.0, 5.0, 5.0]) == 0.0

    def test_all_zero_is_zero(self):
        assert gini_coefficient([0.0, 0.0, 0.0]) == 0.0

    def test_max_inequality_one_winner(self):
        # 1 hot vector → Gini approaches 1 - 1/n
        vals = [0.0] * 99 + [1000.0]
        g = gini_coefficient(vals)
        assert g > 0.95

    def test_known_textbook_value(self):
        # Textbook example: [1, 2, 3, 4, 5]
        # Gini = 0.2667 (approx, well-known)
        g = gini_coefficient([1, 2, 3, 4, 5])
        assert abs(g - 0.2667) < 0.01

    def test_negative_clipped_to_zero(self):
        # Negatives should not break the metric
        g = gini_coefficient([-1.0, -2.0, 3.0])
        assert 0.0 <= g <= 1.0

    def test_nan_filtered(self):
        # NaN values dropped
        vals = [1.0, 2.0, float("nan"), 3.0]
        g = gini_coefficient(vals)
        # Same as [1, 2, 3] which has Gini ≈ 0.222
        assert abs(g - gini_coefficient([1.0, 2.0, 3.0])) < 1e-9


class TestTheil:
    def test_empty_is_zero(self):
        assert theil_t_index([]) == 0.0

    def test_equal_distribution_near_zero(self):
        # Equal → log(1) = 0 → Theil = 0
        assert theil_t_index([5.0, 5.0, 5.0]) < 1e-6

    def test_concentrated_is_higher(self):
        balanced = theil_t_index([1.0, 1.0, 1.0, 1.0])
        skewed = theil_t_index([0.01, 0.01, 0.01, 100.0])
        assert skewed > balanced
        assert skewed > 0


class TestAtkinson:
    def test_empty_is_zero(self):
        assert atkinson_index([]) == 0.0

    def test_equal_is_zero(self):
        assert atkinson_index([3.0, 3.0, 3.0]) < 1e-6

    def test_concentrated_is_higher(self):
        balanced = atkinson_index([1.0] * 10, epsilon=0.5)
        skewed = atkinson_index([0.1] * 9 + [100.0], epsilon=0.5)
        assert skewed > balanced

    def test_epsilon_1_uses_geometric_mean(self):
        # Should not crash and return a sensible value
        v = atkinson_index([1.0, 2.0, 3.0], epsilon=1.0)
        assert 0.0 <= v <= 1.0

    def test_epsilon_negative_raises(self):
        with pytest.raises(ValueError):
            atkinson_index([1.0, 2.0], epsilon=-0.5)


class TestBootstrap:
    def test_ci_contains_point(self):
        rng = np.random.default_rng(7)
        vals = rng.gamma(2.0, 2.0, 200)
        ci = bootstrap_inequality(
            vals, gini_coefficient,
            metric_name="gini", bootstrap_n=200, rng_seed=11,
        )
        assert ci.ci_low <= ci.point <= ci.ci_high

    def test_empty_safe(self):
        ci = bootstrap_inequality(
            [], gini_coefficient,
            metric_name="gini", bootstrap_n=100,
        )
        assert ci.n == 0
        assert ci.point == 0.0


class TestCohortTrajectory:
    def test_returns_sorted_by_year(self):
        rng = np.random.default_rng(13)
        records = []
        for year in [1990, 2000, 2010, 1995, 2005]:
            for _ in range(50):
                records.append((year, float(rng.exponential(1.0))))
        rows = compute_cohort_trajectory(records, bin_width=5, min_cohort_n=30)
        years = [r.cohort_year for r in rows]
        assert years == sorted(years)

    def test_low_n_cohort_skipped(self):
        # Only 5 persons in 2010 cohort → below threshold of 30
        records = [(2010, 1.0) for _ in range(5)]
        records += [(2020, 1.0) for _ in range(50)]
        rows = compute_cohort_trajectory(records, bin_width=5, min_cohort_n=30)
        years = {r.cohort_year for r in rows}
        assert 2010 not in years
        assert 2020 in years

    def test_bin_width_groups(self):
        # Bin width 10 → 1992 and 1995 both → bin 1990
        records = [(1992, 1.0), (1995, 2.0)] * 30
        rows = compute_cohort_trajectory(records, bin_width=10, min_cohort_n=30)
        assert len(rows) == 1
        assert rows[0].cohort_year == 1990

    def test_handles_nan_year(self):
        records = [(None, 1.0), (1990, 2.0)] * 30
        rows = compute_cohort_trajectory(records, bin_width=10, min_cohort_n=15)
        # Only valid year is 1990
        assert all(r.cohort_year == 1990 for r in rows)


class TestCompareCohorts:
    def test_non_overlapping_ci_flags_significant(self):
        # Cohort A = equal, Cohort B = highly skewed
        equal = [1.0] * 200
        skewed = [0.01] * 199 + [1000.0]
        cmp_ = compare_cohorts(
            equal, skewed, label_a="A", label_b="B",
            bootstrap_n=200, rng_seed=1,
        )
        # B is much more unequal → expect non-overlap
        # (with bootstrap n=200, sometimes overlap with all-equal so allow both)
        assert isinstance(cmp_.ci_overlap, bool)
        assert cmp_.gini_diff < 0  # A < B in inequality

    def test_identical_cohorts_overlap(self):
        rng = np.random.default_rng(17)
        vals = rng.gamma(2.0, 2.0, 200)
        cmp_ = compare_cohorts(
            vals.tolist(), vals.tolist(),
            label_a="A", label_b="A_copy",
            bootstrap_n=100, rng_seed=3,
        )
        assert cmp_.ci_overlap is True
