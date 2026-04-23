"""Tests for Phase 8 post_processing — percentiles, confidence intervals."""

from __future__ import annotations

import pytest

from src.runtime.models import Credit, Role
from src.pipeline_phases.post_processing import post_process_results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_AXES = ("iv_score", "person_fe", "birank", "patronage", "awcc", "dormancy")


def _result(pid: str, **kw) -> dict:
    base = {
        "person_id": pid,
        "iv_score": 0.0,
        "person_fe": 0.0,
        "birank": 0.0,
        "patronage": 0.0,
        "awcc": 0.0,
        "dormancy": 1.0,
    }
    base.update(kw)
    return base


def _credit(pid: str, src: str = "anilist") -> Credit:
    return Credit(person_id=pid, anime_id="a1", role=Role.DIRECTOR, source=src)


# ---------------------------------------------------------------------------
# Percentile calculation (extracted logic from post_process_results)
# ---------------------------------------------------------------------------


class TestPercentileCalculation:
    """Test the bisect-based percentile logic used in post_processing."""

    @staticmethod
    def _compute_percentiles(results: list[dict], axes: tuple[str, ...]) -> None:
        """Replicate the percentile logic from post_process_results."""
        import bisect

        n = len(results)
        if n > 1:
            for axis in axes:
                sorted_vals = sorted(r.get(axis, 0) for r in results)
                for r in results:
                    rank = bisect.bisect_right(sorted_vals, r.get(axis, 0))
                    pct_raw = rank / n * 100
                    r[f"{axis}_pct"] = (
                        100.0 if rank == n else min(round(pct_raw, 1), 99.9)
                    )
        elif n == 1:
            for r in results:
                for axis in axes:
                    r[f"{axis}_pct"] = 100.0

    def test_single_person_gets_100(self):
        results = [{"iv_score": 5.0}]
        self._compute_percentiles(results, ("iv_score",))
        assert results[0]["iv_score_pct"] == 100.0

    def test_three_distinct_values(self):
        results = [
            {"iv_score": 1.0},
            {"iv_score": 2.0},
            {"iv_score": 3.0},
        ]
        self._compute_percentiles(results, ("iv_score",))
        # bisect_right: rank 1/3=33.3, 2/3=66.7, 3/3=100.0
        assert results[0]["iv_score_pct"] == pytest.approx(33.3, abs=0.1)
        assert results[1]["iv_score_pct"] == pytest.approx(66.7, abs=0.1)
        assert results[2]["iv_score_pct"] == pytest.approx(100.0, abs=0.1)

    def test_tied_values_get_same_percentile(self):
        """D19: bisect_right gives all tied values the same (upper) percentile."""
        results = [
            {"iv_score": 5.0},
            {"iv_score": 5.0},
            {"iv_score": 5.0},
            {"iv_score": 10.0},
        ]
        self._compute_percentiles(results, ("iv_score",))
        # All 5.0s get bisect_right position 3 → 3/4*100 = 75.0
        assert results[0]["iv_score_pct"] == 75.0
        assert results[1]["iv_score_pct"] == 75.0
        assert results[2]["iv_score_pct"] == 75.0
        assert results[3]["iv_score_pct"] == 100.0

    def test_multiple_axes(self):
        results = [
            {"iv_score": 1.0, "person_fe": 10.0},
            {"iv_score": 2.0, "person_fe": 5.0},
        ]
        self._compute_percentiles(results, ("iv_score", "person_fe"))
        # iv_score: [1, 2] → 50%, 100%
        assert results[0]["iv_score_pct"] == 50.0
        assert results[1]["iv_score_pct"] == 100.0
        # person_fe: [5, 10] → person_fe=10 is 100%, person_fe=5 is 50%
        assert results[0]["person_fe_pct"] == 100.0
        assert results[1]["person_fe_pct"] == 50.0

    def test_missing_axis_defaults_to_zero(self):
        results = [
            {"iv_score": 3.0},
            {},  # missing iv_score → defaults to 0
        ]
        self._compute_percentiles(results, ("iv_score",))
        assert results[1]["iv_score_pct"] == 50.0  # 0 < 3 → rank 1/2
        assert results[0]["iv_score_pct"] == 100.0

    def test_empty_results(self):
        results = []
        self._compute_percentiles(results, ("iv_score",))
        assert results == []

    def test_negative_values(self):
        results = [
            {"iv_score": -5.0},
            {"iv_score": 0.0},
            {"iv_score": 5.0},
        ]
        self._compute_percentiles(results, ("iv_score",))
        assert results[0]["iv_score_pct"] == pytest.approx(33.3, abs=0.1)
        assert results[2]["iv_score_pct"] == 100.0


# ---------------------------------------------------------------------------
# Integration: post_process_results with typed inputs
# ---------------------------------------------------------------------------


class TestPostProcessResults:
    def test_single_person_all_axes_100(self):
        results = [_result("p1", iv_score=0.5)]
        post_process_results(results, [], None)
        for axis in _AXES:
            assert results[0][f"{axis}_pct"] == 100.0

    def test_top_scorer_gets_100(self):
        results = [
            _result("p1", iv_score=0.9),
            _result("p2", iv_score=0.5),
            _result("p3", iv_score=0.1),
        ]
        post_process_results(results, [], None)
        top = next(r for r in results if r["person_id"] == "p1")
        assert top["iv_score_pct"] == 100.0

    def test_all_pct_fields_added(self):
        results = [_result("p1"), _result("p2")]
        post_process_results(results, [], None)
        for r in results:
            for axis in _AXES:
                assert f"{axis}_pct" in r

    def test_pct_values_in_0_100(self):
        results = [_result(f"p{i}", iv_score=float(i)) for i in range(10)]
        post_process_results(results, [], None)
        for r in results:
            for axis in _AXES:
                pct = r[f"{axis}_pct"]
                assert 0.0 <= pct <= 100.0, f"{axis}_pct={pct}"

    def test_empty_results_no_crash(self):
        post_process_results([], [], None)

    def test_confidence_field_added(self):
        results = [_result("p1")]
        post_process_results(results, [_credit("p1")], None)
        assert "confidence" in results[0]

    def test_confidence_is_float(self):
        results = [_result("p1")]
        post_process_results(results, [_credit("p1")], None)
        assert isinstance(results[0]["confidence"], float)

    def test_confidence_non_negative(self):
        results = [_result("p1")]
        post_process_results(results, [_credit("p1")], None)
        assert results[0]["confidence"] >= 0.0

    def test_score_range_field_added(self):
        results = [_result("p1")]
        post_process_results(results, [_credit("p1")], None)
        assert "score_range" in results[0]

    def test_multi_source_lower_confidence_interval(self):
        # More sources → higher confidence → narrower interval (lower confidence float)
        results = [_result("p1"), _result("p2")]
        credits = [
            _credit("p1", "anilist"),
            _credit("p1", "ann"),
            _credit("p1", "mal"),
            _credit("p2", "anilist"),
        ]
        post_process_results(results, credits, None)
        p1 = next(r["confidence"] for r in results if r["person_id"] == "p1")
        p2 = next(r["confidence"] for r in results if r["person_id"] == "p2")
        assert p1 <= p2

    def test_no_credits_still_adds_confidence(self):
        results = [_result("p1")]
        post_process_results(results, [], None)
        assert "confidence" in results[0]
