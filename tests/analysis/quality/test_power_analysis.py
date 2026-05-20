"""Tests for src/analysis/quality/power_analysis."""

from __future__ import annotations


from src.analysis.quality.power_analysis import (
    PowerEstimate,
    audit_report_power,
    mde_correlation,
    mde_t_test_two_sample,
    power_correlation,
    power_regression_coefficient,
    power_t_test_two_sample,
)


# ---------------------------------------------------------------------------
# t-test power
# ---------------------------------------------------------------------------


class TestTTestPower:
    def test_returns_estimate(self):
        est = power_t_test_two_sample(100, 100, effect_size_d=0.5)
        assert isinstance(est, PowerEstimate)
        assert est.test_family == "t_test"

    def test_zero_effect_low_power(self):
        est = power_t_test_two_sample(50, 50, effect_size_d=0.0)
        # power ~ α (Type I rate) → ~0.05
        assert est.power < 0.1

    def test_large_n_large_effect_high_power(self):
        est = power_t_test_two_sample(500, 500, effect_size_d=0.5)
        assert est.power > 0.9

    def test_small_n_returns_low_power(self):
        est = power_t_test_two_sample(5, 5, effect_size_d=0.3)
        assert est.power < 0.8

    def test_n_below_2_returns_zero_power(self):
        est = power_t_test_two_sample(1, 100, effect_size_d=0.5)
        assert est.power == 0.0

    def test_can_detect_flag(self):
        est = power_t_test_two_sample(1000, 1000, effect_size_d=0.5)
        assert est.can_detect is True
        weak = power_t_test_two_sample(20, 20, effect_size_d=0.1)
        assert weak.can_detect is False


# ---------------------------------------------------------------------------
# Regression power
# ---------------------------------------------------------------------------


class TestRegressionPower:
    def test_returns_estimate(self):
        est = power_regression_coefficient(100, beta=0.3, se_beta=0.1)
        assert isinstance(est, PowerEstimate)

    def test_large_t_stat_high_power(self):
        est = power_regression_coefficient(500, beta=0.5, se_beta=0.1)
        assert est.power > 0.95

    def test_se_zero_returns_zero_power(self):
        est = power_regression_coefficient(100, beta=0.3, se_beta=0.0)
        assert est.power == 0.0

    def test_small_n_returns_zero_power(self):
        est = power_regression_coefficient(2, beta=0.3, se_beta=0.1)
        assert est.power == 0.0


# ---------------------------------------------------------------------------
# Correlation power
# ---------------------------------------------------------------------------


class TestCorrelationPower:
    def test_returns_estimate(self):
        est = power_correlation(100, r=0.3)
        assert isinstance(est, PowerEstimate)

    def test_zero_correlation_low_power(self):
        est = power_correlation(100, r=0.0)
        assert est.power < 0.1

    def test_strong_correlation_high_power(self):
        est = power_correlation(200, r=0.3)
        assert est.power > 0.8

    def test_n_below_4_returns_zero(self):
        est = power_correlation(3, r=0.5)
        assert est.power == 0.0


# ---------------------------------------------------------------------------
# MDE inverse problem
# ---------------------------------------------------------------------------


class TestMDE:
    def test_mde_decreases_with_n(self):
        d_small = mde_t_test_two_sample(20, 20)
        d_big = mde_t_test_two_sample(1000, 1000)
        assert d_big < d_small

    def test_mde_correlation_decreases_with_n(self):
        r_small = mde_correlation(30)
        r_big = mde_correlation(1000)
        assert r_big < r_small

    def test_mde_with_huge_n_approaches_zero(self):
        d = mde_t_test_two_sample(10000, 10000)
        assert d < 0.1

    def test_mde_inf_for_invalid_n(self):
        d = mde_t_test_two_sample(1, 10)
        assert d == float("inf")


# ---------------------------------------------------------------------------
# Audit batch
# ---------------------------------------------------------------------------


class TestAudit:
    def test_audit_returns_rows(self):
        specs = [
            dict(
                report_name="gender_gap",
                test_label="female vs male credits",
                test_family="t_test",
                n1=300, n2=2000,
                observed_effect=0.3,
            ),
            dict(
                report_name="did_studio",
                test_label="ATE on theta_i",
                test_family="regression",
                n=500, beta=0.2, se_beta=0.08,
            ),
            dict(
                report_name="cohort_inequality",
                test_label="Gini vs year",
                test_family="correlation",
                n=20, observed_effect=0.5,
            ),
        ]
        rows = audit_report_power(specs)
        assert len(rows) == 3
        labels = {r.test_label for r in rows}
        assert "female vs male credits" in labels

    def test_verdict_classification(self):
        # high power: ok
        ok_spec = dict(
            report_name="r", test_label="t", test_family="t_test",
            n1=1000, n2=1000, observed_effect=0.5,
        )
        underp_spec = dict(
            report_name="r", test_label="t", test_family="t_test",
            n1=10, n2=10, observed_effect=0.05,
        )
        rows = audit_report_power([ok_spec, underp_spec])
        assert rows[0].verdict == "ok"
        assert rows[1].verdict in ("underpowered", "borderline")

    def test_unknown_family_skipped(self):
        specs = [
            dict(report_name="bad", test_label="x", test_family="chi_square", n=100),
        ]
        rows = audit_report_power(specs)
        assert rows == []
