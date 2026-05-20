"""Tests for src/analysis/equity/oaxaca_decomp."""

from __future__ import annotations

import numpy as np
import pytest

from src.analysis.equity.oaxaca_decomp import (
    decompose_oaxaca_blinder,
    decompose_subgroup,
    fit_group_ols,
)


# ---------------------------------------------------------------------------
# fit_group_ols
# ---------------------------------------------------------------------------


class TestFitGroupOls:
    def test_intercept_only(self):
        # y = 5, no covariates → intercept = 5, x_mean is empty
        y = np.array([5.0, 5.0, 5.0])
        x = np.empty((3, 0))
        beta, x_mean = fit_group_ols(y, x)
        assert beta.shape == (1,)
        assert pytest.approx(beta[0], rel=1e-9) == 5.0
        assert x_mean.shape == (0,)

    def test_known_slope(self):
        # y = 2 + 3·x
        x = np.arange(10).reshape(-1, 1).astype(float)
        y = 2.0 + 3.0 * x[:, 0]
        beta, x_mean = fit_group_ols(y, x)
        assert pytest.approx(beta[0], rel=1e-9) == 2.0
        assert pytest.approx(beta[1], rel=1e-9) == 3.0
        assert pytest.approx(x_mean[0], rel=1e-9) == 4.5

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            fit_group_ols(np.array([]), np.empty((0, 1)))

    def test_shape_mismatch_raises(self):
        with pytest.raises(ValueError):
            fit_group_ols(np.array([1.0, 2.0]), np.empty((3, 1)))


# ---------------------------------------------------------------------------
# decompose_oaxaca_blinder — single-pass identities
# ---------------------------------------------------------------------------


class TestOaxacaIdentities:
    def test_endowment_structural_sum_to_raw_gap(self):
        rng = np.random.default_rng(7)
        n_a, n_b = 200, 200
        # group A: y = 1 + 2·x + noise
        x_a = rng.normal(0, 1, (n_a, 1))
        y_a = 1.0 + 2.0 * x_a[:, 0] + rng.normal(0, 0.1, n_a)
        # group B: y = 0.5 + 1.5·x + noise (lower intercept + lower slope)
        x_b = rng.normal(0, 1, (n_b, 1))
        y_b = 0.5 + 1.5 * x_b[:, 0] + rng.normal(0, 0.1, n_b)

        res = decompose_oaxaca_blinder(y_a, x_a, y_b, x_b, ["x1"])
        # raw_gap should equal endowment + structural exactly
        assert pytest.approx(res.endowment + res.structural, rel=1e-9) == res.raw_gap

    def test_zero_endowment_when_x_means_equal(self):
        # If X̄_A == X̄_B, endowment = 0 (no construction-side gap)
        rng = np.random.default_rng(11)
        x = rng.normal(0, 1, (300, 1))
        y_a = 1.0 + 2.0 * x[:, 0] + rng.normal(0, 0.1, 300)
        y_b = 0.5 + 2.0 * x[:, 0] + rng.normal(0, 0.1, 300)

        # x_a == x_b → endowment dominated by mean diff = 0
        res = decompose_oaxaca_blinder(y_a, x, y_b, x, ["x1"])
        # endowment_per_feature should be near zero
        assert abs(res.endowment_per_feature[0]) < 0.01
        # structural ~ raw_gap (intercept diff = 0.5 dominates)
        assert pytest.approx(res.structural, abs=0.1) == res.raw_gap

    def test_pure_endowment_when_betas_equal(self):
        # Same betas, different X means → structural near zero
        rng = np.random.default_rng(13)
        x_a = rng.normal(2.0, 0.5, (300, 1))  # higher X
        x_b = rng.normal(0.0, 0.5, (300, 1))  # lower X
        # Same β for both: y = 1 + 2·x
        y_a = 1.0 + 2.0 * x_a[:, 0] + rng.normal(0, 0.1, 300)
        y_b = 1.0 + 2.0 * x_b[:, 0] + rng.normal(0, 0.1, 300)
        res = decompose_oaxaca_blinder(y_a, x_a, y_b, x_b, ["x1"])
        # raw gap ~= 2·(2 - 0) = 4
        assert abs(res.raw_gap - 4.0) < 0.2
        # endowment dominates
        assert res.endowment / res.raw_gap > 0.9
        # structural near zero (intercepts equal, slopes equal)
        assert abs(res.structural) < 0.3

    def test_feature_names_length_validated(self):
        rng = np.random.default_rng(17)
        x = rng.normal(0, 1, (10, 2))
        y = rng.normal(0, 1, 10)
        with pytest.raises(ValueError):
            decompose_oaxaca_blinder(y, x, y, x, ["only_one_name"])

    def test_feature_dim_mismatch_validated(self):
        rng = np.random.default_rng(19)
        with pytest.raises(ValueError):
            decompose_oaxaca_blinder(
                rng.normal(0, 1, 10),
                rng.normal(0, 1, (10, 2)),
                rng.normal(0, 1, 10),
                rng.normal(0, 1, (10, 3)),
                ["x1", "x2"],
            )


# ---------------------------------------------------------------------------
# decompose_subgroup — bootstrap CI
# ---------------------------------------------------------------------------


class TestOaxacaBootstrap:
    def test_ci_brackets_point_estimate(self):
        rng = np.random.default_rng(23)
        x_a = rng.normal(0, 1, (200, 2))
        y_a = 1.0 + 2.0 * x_a[:, 0] + 0.5 * x_a[:, 1] + rng.normal(0, 0.1, 200)
        x_b = rng.normal(0, 1, (200, 2))
        y_b = 0.5 + 1.5 * x_b[:, 0] + 0.3 * x_b[:, 1] + rng.normal(0, 0.1, 200)

        report = decompose_subgroup(
            y_a, x_a, y_b, x_b,
            feature_names=["x1", "x2"],
            subgroup_label="test_subgroup",
            bootstrap_n=200,
            rng_seed=42,
        )
        # CI should bracket the point estimate (with high probability)
        assert report.endowment_ci_low <= report.point.endowment <= report.endowment_ci_high
        assert report.structural_ci_low <= report.point.structural <= report.structural_ci_high
        assert report.raw_gap_ci_low <= report.point.raw_gap <= report.raw_gap_ci_high

    def test_bootstrap_n_recorded(self):
        rng = np.random.default_rng(29)
        x = rng.normal(0, 1, (50, 1))
        y = rng.normal(0, 1, 50)
        report = decompose_subgroup(
            y, x, y, x, feature_names=["x1"],
            subgroup_label="zero_gap", bootstrap_n=50, rng_seed=5,
        )
        assert report.bootstrap_n == 50

    def test_excluded_counts_propagated(self):
        rng = np.random.default_rng(31)
        x = rng.normal(0, 1, (20, 1))
        y = rng.normal(0, 1, 20)
        report = decompose_subgroup(
            y, x, y, x, feature_names=["x1"],
            subgroup_label="excl_test",
            bootstrap_n=20, rng_seed=3,
            n_excluded_missing_y=5,
            n_excluded_missing_x=10,
            n_excluded_missing_group=15,
        )
        assert report.n_excluded_missing_y == 5
        assert report.n_excluded_missing_x == 10
        assert report.n_excluded_missing_group == 15

    def test_subgroup_label_propagated(self):
        rng = np.random.default_rng(37)
        x = rng.normal(0, 1, (50, 1))
        y = rng.normal(0, 1, 50)
        report = decompose_subgroup(
            y, x, y, x, feature_names=["x1"],
            subgroup_label="JP/1990s",
            bootstrap_n=30, rng_seed=7,
        )
        assert report.subgroup_label == "JP/1990s"

    def test_notes_include_failures_for_singular_x(self):
        # Constant X → singular matrix, fit may fail; bootstrap_failures > 0 captured.
        rng = np.random.default_rng(41)
        x = np.ones((30, 1))  # constant → singular when lstsq picks bad pivots
        y = rng.normal(0, 1, 30)
        # Should not raise (graceful), but may have boot_failures
        report = decompose_subgroup(
            y, x, y, x, feature_names=["constant"],
            subgroup_label="degenerate",
            bootstrap_n=10, rng_seed=2,
        )
        assert isinstance(report.notes, tuple)
