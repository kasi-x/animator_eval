"""Tests for src/analysis/career/survival_curves."""

from __future__ import annotations

import numpy as np
import pytest

pytest.importorskip("lifelines")

from src.analysis.career.survival_curves import (  # noqa: E402
    KMCurve,
    LogRankResult,
    SurvivalReport,
    fit_km,
    fit_subgroups,
    pairwise_logrank,
)


def _synth_km(n=200, hazard=0.1, t_max=20, seed=7):
    rng = np.random.default_rng(seed)
    u = rng.uniform(0, 1, n)
    durations = -np.log(u) / hazard
    events = (durations < t_max).astype(int)
    durations = np.minimum(durations, t_max)
    return durations, events


class TestFitKM:
    def test_returns_curve(self):
        d, e = _synth_km(seed=7)
        c = fit_km(d, e, label="g1")
        assert isinstance(c, KMCurve)
        assert c.label == "g1"
        assert len(c.timeline) > 0
        assert len(c.survival) == len(c.timeline)

    def test_survival_decreasing(self):
        d, e = _synth_km(seed=11)
        c = fit_km(d, e)
        sf = list(c.survival)
        # Survival decreases monotonically
        assert all(sf[i + 1] <= sf[i] + 1e-9 for i in range(len(sf) - 1))

    def test_ci_envelopes_survival(self):
        d, e = _synth_km(seed=13)
        c = fit_km(d, e)
        for s, lo, hi in zip(c.survival, c.ci_lower, c.ci_upper):
            # CI must envelope SF (within numerical tolerance)
            assert lo <= s + 1e-6
            assert s - 1e-6 <= hi

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            fit_km(np.array([]), np.array([]))

    def test_shape_mismatch_raises(self):
        with pytest.raises(ValueError):
            fit_km(np.array([1.0, 2.0]), np.array([1]))

    def test_n_events_matches_input(self):
        d, e = _synth_km(seed=17)
        c = fit_km(d, e)
        assert c.n_events == int(e.sum())
        assert c.n_at_risk_initial == d.size

    def test_median_survival_for_high_event_rate(self):
        # high hazard -> short median
        d, e = _synth_km(n=500, hazard=1.0, t_max=10, seed=19)
        c = fit_km(d, e)
        assert c.median_survival is not None
        assert c.median_survival < 5.0


class TestFitSubgroups:
    def test_returns_report(self):
        d1, e1 = _synth_km(n=100, hazard=0.1, seed=23)
        d2, e2 = _synth_km(n=100, hazard=0.2, seed=25)
        d = np.concatenate([d1, d2])
        e = np.concatenate([e1, e2])
        g = ["A"] * 100 + ["B"] * 100
        r = fit_subgroups(d, e, g)
        assert isinstance(r, SurvivalReport)
        assert len(r.curves) == 2

    def test_log_rank_detects_difference(self):
        d1, e1 = _synth_km(n=200, hazard=0.05, seed=29)
        d2, e2 = _synth_km(n=200, hazard=0.5, seed=31)
        d = np.concatenate([d1, d2])
        e = np.concatenate([e1, e2])
        g = ["A"] * 200 + ["B"] * 200
        r = fit_subgroups(d, e, g)
        assert r.log_rank is not None
        assert r.log_rank.p_value < 0.001  # huge hazard diff

    def test_small_group_skipped(self):
        d1, e1 = _synth_km(n=50, seed=37)
        d2, e2 = _synth_km(n=3, seed=41)
        d = np.concatenate([d1, d2])
        e = np.concatenate([e1, e2])
        g = ["A"] * 50 + ["B"] * 3
        r = fit_subgroups(d, e, g, min_group_n=10)
        labels = {c.label for c in r.curves}
        assert "A" in labels
        assert "B" not in labels  # skipped

    def test_shape_mismatch_raises(self):
        d = np.array([1.0, 2.0])
        e = np.array([1, 0])
        g = ["A"]  # mismatch
        with pytest.raises(ValueError):
            fit_subgroups(d, e, g)


class TestPairwiseLogRank:
    def test_pairs_excluding_reference(self):
        d_a, e_a = _synth_km(n=100, hazard=0.1, seed=43)
        d_b, e_b = _synth_km(n=100, hazard=0.5, seed=47)
        d_c, e_c = _synth_km(n=100, hazard=0.3, seed=53)
        d = np.concatenate([d_a, d_b, d_c])
        e = np.concatenate([e_a, e_b, e_c])
        g = ["A"] * 100 + ["B"] * 100 + ["C"] * 100
        result = pairwise_logrank(d, e, g, reference="A")
        assert "A" not in result
        assert "B" in result and "C" in result
        # B has biggest hazard diff with A -> smallest p
        assert result["B"].p_value < 0.001

    def test_reference_below_min_returns_empty(self):
        d = np.array([1.0, 2.0])
        e = np.array([1, 0])
        g = ["A", "B"]
        result = pairwise_logrank(d, e, g, reference="A", min_group_n=10)
        assert result == {}
