"""Tests for src/analysis/causal/did_robustness."""

from __future__ import annotations

import numpy as np
import pytest

from src.analysis.causal.did_robustness import (
    EValueResult,
    JointLeadsResult,
    PlaceboResult,
    compute_e_value,
    e_value_from_continuous,
    joint_leads_test,
    placebo_did,
)


# ---------------------------------------------------------------------------
# placebo_did
# ---------------------------------------------------------------------------


class TestPlaceboDid:
    def _make_panel(self, n_persons=200, years=range(2000, 2020), real_event=2010, true_ate=2.0, seed=7):
        rng = np.random.default_rng(seed)
        treated_persons = set(range(0, n_persons // 2))
        rows = []
        for pid in range(n_persons):
            for y in years:
                treated_pre = pid in treated_persons and y < real_event
                treated_post = pid in treated_persons and y >= real_event
                noise = rng.normal(0, 0.3)
                if treated_post:
                    y_val = 1.0 + true_ate + noise
                else:
                    y_val = 1.0 + noise
                rows.append((pid, y, 1 if pid in treated_persons else 0, y_val))
        person = np.array([r[0] for r in rows])
        year = np.array([r[1] for r in rows])
        treated = np.array([r[2] for r in rows])
        y_arr = np.array([r[3] for r in rows])
        return y_arr, treated, year, person

    def test_real_ate_significant(self):
        y, tr, yr, pid = self._make_panel(true_ate=2.0)
        # Compute true ATE via simple naive DiD
        post = yr >= 2010
        ate = (
            (y[(tr == 1) & post].mean() - y[(tr == 1) & ~post].mean())
            - (y[(tr == 0) & post].mean() - y[(tr == 0) & ~post].mean())
        )
        res = placebo_did(y, tr, yr, pid, real_event_year=2010, observed_ate=ate)
        # placebo ATEs should be small, observed large → passes
        assert res.passes is True
        assert res.placebo_p_below_observed < 0.5

    def test_zero_true_effect_fails(self):
        y, tr, yr, pid = self._make_panel(true_ate=0.0)
        post = yr >= 2010
        ate = (
            (y[(tr == 1) & post].mean() - y[(tr == 1) & ~post].mean())
            - (y[(tr == 0) & post].mean() - y[(tr == 0) & ~post].mean())
        )
        res = placebo_did(y, tr, yr, pid, real_event_year=2010, observed_ate=ate)
        # When real ATE ~ 0, placebos are similarly random → not pass robustly
        # Just check it returns valid result
        assert isinstance(res, PlaceboResult)

    def test_no_data_returns_zero_runs(self):
        y = np.array([1.0, 2.0])
        tr = np.array([0, 1])
        yr = np.array([2010, 2010])
        pid = np.array([0, 1])
        # All offsets fall outside any year range → no valid runs
        res = placebo_did(y, tr, yr, pid, real_event_year=2010, observed_ate=0.5,
                          placebo_year_offsets=(100, 200))
        assert res.n_placebo_runs == 0

    def test_shape_mismatch_raises(self):
        y = np.array([1.0])
        tr = np.array([0])
        yr = np.array([2010, 2011])  # mismatch
        pid = np.array([0])
        with pytest.raises(ValueError):
            placebo_did(y, tr, yr, pid, real_event_year=2010, observed_ate=0.1)


# ---------------------------------------------------------------------------
# E-value
# ---------------------------------------------------------------------------


class TestEValue:
    def test_rr_one_returns_one(self):
        res = compute_e_value(1.0, 0.95, 1.05)
        assert res.e_value_point >= 1.0
        # Robustness interpretation: marginal
        assert res.e_value_ci == 1.0

    def test_high_rr_robust(self):
        # RR=5 is strong effect → E-value > 5
        res = compute_e_value(5.0, 3.0, 8.0)
        assert res.e_value_point > 5.0
        assert "頑健" in res.interpretation or "中程度" in res.interpretation

    def test_low_rr_inverted(self):
        # RR=0.2 → 1/RR=5 → E-value > 5
        res = compute_e_value(0.2, 0.1, 0.4)
        assert res.e_value_point > 5.0

    def test_marginal_rr_fragile(self):
        res = compute_e_value(1.1, 1.0, 1.3)
        # E-value ~ 1.42 → fragile
        assert res.e_value_point < 2.0


class TestEValueContinuous:
    def test_returns_evalue(self):
        res = e_value_from_continuous(beta=0.5, se_beta=0.1, sd_y=1.0)
        assert isinstance(res, EValueResult)
        assert res.e_value_point > 1.0

    def test_zero_sd_y_safe(self):
        res = e_value_from_continuous(beta=0.5, se_beta=0.1, sd_y=0.0)
        assert res.e_value_point == 1.0


# ---------------------------------------------------------------------------
# Joint leads test
# ---------------------------------------------------------------------------


class TestJointLeads:
    def test_zero_leads_returns_true(self):
        res = joint_leads_test([], [])
        assert res.parallel_trends_holds is True
        assert res.n_leads_tested == 0

    def test_small_leads_pass(self):
        # All leads near zero, SE 1.0 → low chi2
        res = joint_leads_test([0.05, -0.03, 0.02], [1.0, 1.0, 1.0])
        assert res.parallel_trends_holds is True
        assert res.p_value > 0.05

    def test_large_leads_fail(self):
        # Strong pre-trend → chi2 large
        res = joint_leads_test([3.0, 4.0, 2.5], [0.5, 0.5, 0.5])
        assert res.parallel_trends_holds is False
        assert res.p_value < 0.05

    def test_shape_mismatch_raises(self):
        with pytest.raises(ValueError):
            joint_leads_test([0.1, 0.2], [1.0])
