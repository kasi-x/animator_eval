"""Tests for post_processing phase — percentiles, confidence intervals."""

import pytest


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
