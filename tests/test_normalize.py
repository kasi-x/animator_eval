"""normalize モジュールのテスト."""

import pytest

from src.analysis.normalize import (
    normalize_all_axes,
    normalize_minmax,
    normalize_percentile,
    normalize_scores,
    normalize_zscore,
)


class TestNormalizeScores:
    def test_empty(self):
        assert normalize_scores({}) == {}

    def test_single_value(self):
        result = normalize_scores({"p1": 50.0})
        assert result["p1"] == 50.0  # scale/2 when all same

    def test_two_values(self):
        result = normalize_scores({"p1": 10.0, "p2": 20.0})
        assert result["p1"] == 0.0
        assert result["p2"] == 100.0

    def test_three_values(self):
        result = normalize_scores({"p1": 0.0, "p2": 50.0, "p3": 100.0})
        assert result["p1"] == 0.0
        assert result["p2"] == 50.0
        assert result["p3"] == 100.0

    def test_custom_scale(self):
        result = normalize_scores({"p1": 0.0, "p2": 10.0}, target_maximum_value=50.0)
        assert result["p1"] == 0.0
        assert result["p2"] == 50.0

    def test_all_same(self):
        result = normalize_scores({"p1": 5.0, "p2": 5.0, "p3": 5.0})
        assert all(v == 50.0 for v in result.values())

    def test_negative_values(self):
        result = normalize_scores({"p1": -10.0, "p2": 0.0, "p3": 10.0})
        assert result["p1"] == 0.0
        assert result["p2"] == 50.0
        assert result["p3"] == 100.0

    def test_preserves_ordering(self):
        raw = {"p1": 3.0, "p2": 1.0, "p3": 7.0, "p4": 5.0}
        result = normalize_scores(raw)
        assert result["p2"] < result["p1"] < result["p4"] < result["p3"]


class TestNormalizeAllAxes:
    def test_normalizes_all_three(self):
        authority = {"p1": 0.001, "p2": 0.01}
        trust = {"p1": 5.0, "p2": 15.0}
        skill = {"p1": 20.0, "p2": 30.0}

        norm_a, norm_t, norm_s = normalize_all_axes(authority, trust, skill)
        assert norm_a["p1"] == 0.0
        assert norm_a["p2"] == 100.0
        assert norm_t["p1"] == 0.0
        assert norm_t["p2"] == 100.0
        assert norm_s["p1"] == 0.0
        assert norm_s["p2"] == 100.0

    def test_handles_empty(self):
        norm_a, norm_t, norm_s = normalize_all_axes({}, {}, {})
        assert norm_a == {}
        assert norm_t == {}
        assert norm_s == {}

    def test_with_method_param(self):
        authority = {"p1": 0.001, "p2": 0.01}
        trust = {"p1": 5.0, "p2": 15.0}
        skill = {"p1": 20.0, "p2": 30.0}
        norm_a, _, _ = normalize_all_axes(authority, trust, skill, method="percentile")
        assert norm_a["p1"] == 0.0
        assert norm_a["p2"] == 100.0


class TestNormalizePercentile:
    def test_empty(self):
        assert normalize_percentile({}) == {}

    def test_single(self):
        result = normalize_percentile({"p1": 50.0})
        assert result["p1"] == 50.0

    def test_two_values(self):
        result = normalize_percentile({"p1": 10.0, "p2": 20.0})
        assert result["p1"] == 0.0
        assert result["p2"] == 100.0

    def test_three_values_evenly_spaced(self):
        result = normalize_percentile({"p1": 1.0, "p2": 2.0, "p3": 3.0})
        assert result["p1"] == 0.0
        assert result["p2"] == 50.0
        assert result["p3"] == 100.0

    def test_preserves_ordering(self):
        raw = {"p1": 3.0, "p2": 1.0, "p3": 7.0, "p4": 5.0}
        result = normalize_percentile(raw)
        assert result["p2"] < result["p1"] < result["p4"] < result["p3"]

    def test_outlier_resistant(self):
        """Percentile is resistant to extreme outliers."""
        raw = {"p1": 1.0, "p2": 2.0, "p3": 3.0, "p4": 1000.0}
        result = normalize_percentile(raw)
        # p3 should be ~66.67, not squeezed near 0 like minmax
        assert result["p3"] == pytest.approx(66.67, abs=0.01)


class TestNormalizeZscore:
    def test_empty(self):
        assert normalize_zscore({}) == {}

    def test_single(self):
        result = normalize_zscore({"p1": 50.0})
        assert result["p1"] == 50.0

    def test_mean_maps_to_50(self):
        result = normalize_zscore({"p1": 10.0, "p2": 20.0, "p3": 30.0})
        assert result["p2"] == 50.0  # mean is 20

    def test_bounded_0_100(self):
        raw = {"p1": -100.0, "p2": 0.0, "p3": 100.0}
        result = normalize_zscore(raw)
        for v in result.values():
            assert 0 <= v <= 100

    def test_preserves_ordering(self):
        raw = {"p1": 3.0, "p2": 1.0, "p3": 7.0, "p4": 5.0}
        result = normalize_zscore(raw)
        assert result["p2"] < result["p1"] < result["p4"] < result["p3"]


class TestNormalizeScoresMethod:
    def test_default_is_minmax(self):
        raw = {"p1": 0.0, "p2": 100.0}
        result = normalize_scores(raw)
        assert result == normalize_minmax(raw)

    def test_explicit_percentile(self):
        raw = {"p1": 10.0, "p2": 20.0, "p3": 30.0}
        result = normalize_scores(raw, method="percentile")
        assert result == normalize_percentile(raw)

    def test_explicit_zscore(self):
        raw = {"p1": 10.0, "p2": 20.0, "p3": 30.0}
        result = normalize_scores(raw, method="zscore")
        assert result == normalize_zscore(raw)
