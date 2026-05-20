"""Tests for src/analysis/quality/credit_anomaly."""

from __future__ import annotations

import numpy as np
import pytest

from src.analysis.quality.credit_anomaly import (
    detect_poisson_outliers,
    detect_role_divergence,
    detect_source_disagreement,
)


# ---------------------------------------------------------------------------
# Poisson outlier
# ---------------------------------------------------------------------------


class TestPoissonOutliers:
    def test_empty_returns_empty(self):
        assert detect_poisson_outliers({}) == []

    def test_finds_high_outlier(self):
        # Cohort mean ~ 10, one person has 100 credits → way above
        d = {f"p{i}": 10 + (i % 5) for i in range(50)}
        d["bigshot"] = 100
        outs = detect_poisson_outliers(d, z_threshold=3.0)
        assert any(o.person_id == "bigshot" and o.direction == "high" for o in outs)

    def test_low_mean_cohort_skipped(self):
        # Cohort mean < min_expected → skipped
        d = {f"p{i}": 1 for i in range(100)}
        d["fluke"] = 50
        outs = detect_poisson_outliers(d, min_expected=5.0)
        assert outs == []

    def test_sorted_by_abs_z(self):
        rng = np.random.default_rng(7)
        d = {f"p{i}": int(rng.poisson(20)) for i in range(200)}
        d["mid_outlier"] = 60
        d["extreme_outlier"] = 200
        outs = detect_poisson_outliers(d, z_threshold=2.0)
        assert outs[0].person_id == "extreme_outlier"


# ---------------------------------------------------------------------------
# Role divergence
# ---------------------------------------------------------------------------


class TestRoleDivergence:
    def test_empty_returns_empty(self):
        assert detect_role_divergence({}) == []

    def test_normal_persons_not_flagged(self):
        # Everyone has same distribution → KL = 0
        d = {
            f"p{i}": {"animator": 10, "key_animator": 5, "director": 1}
            for i in range(50)
        }
        results = detect_role_divergence(d, kl_threshold=0.5, min_credits=5)
        assert results == []

    def test_role_concentrated_person_flagged(self):
        # Cohort: mostly animator, some key_animator, rare director
        d = {f"p{i}": {"animator": 10, "key_animator": 3} for i in range(30)}
        # Outlier: 100% director (very different from cohort)
        d["director_only"] = {"director": 20}
        results = detect_role_divergence(d, kl_threshold=1.0, min_credits=5)
        assert any(r.person_id == "director_only" for r in results)

    def test_min_credits_filters_noise(self):
        # Only 2 credits → noisy estimate, should be filtered
        d = {f"p{i}": {"animator": 10} for i in range(30)}
        d["lowdata"] = {"director": 2}
        results = detect_role_divergence(d, kl_threshold=0.5, min_credits=5)
        assert all(r.person_id != "lowdata" for r in results)

    def test_dominant_role_recorded(self):
        d = {f"p{i}": {"animator": 50} for i in range(30)}
        d["director_only"] = {"director": 30}
        results = detect_role_divergence(d, kl_threshold=0.5, min_credits=5)
        for r in results:
            if r.person_id == "director_only":
                assert r.dominant_role == "director"
                assert r.dominant_role_share == 1.0
                break
        else:
            pytest.fail("director_only not flagged")


# ---------------------------------------------------------------------------
# Source disagreement
# ---------------------------------------------------------------------------


class TestSourceDisagreement:
    def test_empty_returns_empty(self):
        assert detect_source_disagreement({}) == []

    def test_consistent_sources_not_flagged(self):
        # Different canonical ids each have similar count across sources
        d = {
            f"c{i}": {"anilist": 10, "mal": 11, "ann": 9}
            for i in range(20)
        }
        results = detect_source_disagreement(d, spread_threshold=4.0, min_total=10)
        assert results == []

    def test_high_spread_flagged(self):
        # Normal cohort
        d = {f"c{i}": {"anilist": 10, "mal": 11} for i in range(20)}
        # Outlier: anilist 50, mal 2 → spread 25 ≫ threshold
        d["c_bad"] = {"anilist": 50, "mal": 2}
        results = detect_source_disagreement(d, spread_threshold=5.0, min_total=10)
        assert any(s.canonical_id == "c_bad" for s in results)

    def test_low_total_skipped(self):
        d = {f"c{i}": {"anilist": 10, "mal": 10} for i in range(20)}
        d["low_total"] = {"anilist": 4, "mal": 1}
        results = detect_source_disagreement(d, spread_threshold=2.0, min_total=10)
        assert all(s.canonical_id != "low_total" for s in results)

    def test_sorted_by_spread(self):
        d = {f"c{i}": {"a": 10, "b": 11} for i in range(20)}
        d["mid"] = {"a": 30, "b": 5}        # spread 6
        d["extreme"] = {"a": 100, "b": 1}    # spread 100
        results = detect_source_disagreement(d, spread_threshold=3.0, min_total=10)
        assert results[0].canonical_id == "extreme"
