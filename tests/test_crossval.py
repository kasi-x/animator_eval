"""crossval モジュールのテスト."""

import pytest

from src.analysis.crossval import (
    _compute_composite,
    _rank_correlation,
    cross_validate_scores,
)
from src.models import Anime, Credit, Person, Role


def _make_test_data():
    """テスト用の人物・アニメ・クレジットデータ."""
    persons = [
        Person(id=f"p{i}", name_en=f"Person {i}") for i in range(1, 11)
    ]
    anime_list = [
        Anime(id="a1", title_en="Show A", year=2015, score=8.0),
        Anime(id="a2", title_en="Show B", year=2016, score=7.5),
        Anime(id="a3", title_en="Show C", year=2017, score=7.0),
        Anime(id="a4", title_en="Show D", year=2018, score=8.5),
        Anime(id="a5", title_en="Show E", year=2019, score=6.5),
    ]
    credits = []
    # p1 is a prolific director
    for a in anime_list:
        credits.append(Credit(person_id="p1", anime_id=a.id, role=Role.DIRECTOR, source="test"))
    # p2-p5 are key animators across multiple shows
    for pid in ["p2", "p3", "p4", "p5"]:
        for aid in ["a1", "a2", "a3"]:
            credits.append(Credit(person_id=pid, anime_id=aid, role=Role.KEY_ANIMATOR, source="test"))
    # p6-p10 are in-betweeners with fewer credits
    for pid in ["p6", "p7", "p8", "p9", "p10"]:
        credits.append(Credit(person_id=pid, anime_id="a1", role=Role.IN_BETWEEN, source="test"))
    return persons, anime_list, credits


class TestRankCorrelation:
    def test_perfect_correlation(self):
        ranks = {"a": 1, "b": 2, "c": 3}
        assert _rank_correlation(ranks, ranks) == 1.0

    def test_inverse_correlation(self):
        ranks_a = {"a": 1, "b": 2, "c": 3}
        ranks_b = {"a": 3, "b": 2, "c": 1}
        result = _rank_correlation(ranks_a, ranks_b)
        assert result == pytest.approx(-1.0)

    def test_no_common_keys(self):
        ranks_a = {"a": 1}
        ranks_b = {"b": 1}
        assert _rank_correlation(ranks_a, ranks_b) == 0.0

    def test_single_common_key(self):
        ranks_a = {"a": 1, "b": 2}
        ranks_b = {"a": 1, "c": 2}
        assert _rank_correlation(ranks_a, ranks_b) == 0.0

    def test_partial_overlap(self):
        ranks_a = {"a": 1, "b": 2, "c": 3, "d": 4}
        ranks_b = {"a": 1, "b": 2, "c": 3, "e": 4}
        # Perfect correlation on common keys
        assert _rank_correlation(ranks_a, ranks_b) == 1.0


class TestComputeComposite:
    def test_weighted_combination(self):
        authority = {"p1": 100.0}
        trust = {"p1": 100.0}
        skill = {"p1": 100.0}
        result = _compute_composite(authority, trust, skill)
        # 100*0.4 + 100*0.35 + 100*0.25 = 100.0
        assert result["p1"] == pytest.approx(100.0)

    def test_missing_scores_default_zero(self):
        authority = {"p1": 80.0}
        trust = {}
        skill = {}
        result = _compute_composite(authority, trust, skill)
        # 80*0.4 + 0 + 0 = 32.0
        assert result["p1"] == pytest.approx(32.0)

    def test_union_of_all_ids(self):
        authority = {"p1": 50.0}
        trust = {"p2": 50.0}
        skill = {"p3": 50.0}
        result = _compute_composite(authority, trust, skill)
        assert set(result.keys()) == {"p1", "p2", "p3"}


class TestCrossValidateScores:
    def test_returns_expected_keys(self):
        persons, anime_list, credits = _make_test_data()
        result = cross_validate_scores(persons, anime_list, credits, n_folds=3)
        assert "n_folds" in result
        assert "holdout_ratio" in result
        assert "avg_rank_correlation" in result
        assert "min_rank_correlation" in result
        assert "avg_top10_overlap" in result
        assert "fold_results" in result
        assert "total_credits" in result

    def test_fold_count(self):
        persons, anime_list, credits = _make_test_data()
        result = cross_validate_scores(persons, anime_list, credits, n_folds=3)
        assert len(result["fold_results"]) == 3

    def test_correlation_range(self):
        persons, anime_list, credits = _make_test_data()
        result = cross_validate_scores(persons, anime_list, credits, n_folds=5)
        # Correlations should be between -1 and 1
        assert -1.0 <= result["avg_rank_correlation"] <= 1.0
        assert -1.0 <= result["min_rank_correlation"] <= 1.0

    def test_top10_overlap_range(self):
        persons, anime_list, credits = _make_test_data()
        result = cross_validate_scores(persons, anime_list, credits, n_folds=5)
        assert 0.0 <= result["avg_top10_overlap"] <= 1.0

    def test_deterministic_with_seed(self):
        persons, anime_list, credits = _make_test_data()
        r1 = cross_validate_scores(persons, anime_list, credits, seed=123)
        r2 = cross_validate_scores(persons, anime_list, credits, seed=123)
        assert r1["avg_rank_correlation"] == r2["avg_rank_correlation"]
        assert r1["fold_results"] == r2["fold_results"]

    def test_different_seeds_differ(self):
        persons, anime_list, credits = _make_test_data()
        r1 = cross_validate_scores(persons, anime_list, credits, seed=1)
        r2 = cross_validate_scores(persons, anime_list, credits, seed=999)
        # With different seeds, fold results should differ
        assert r1["fold_results"] != r2["fold_results"]

    def test_high_holdout_ratio(self):
        persons, anime_list, credits = _make_test_data()
        result = cross_validate_scores(
            persons, anime_list, credits, holdout_ratio=0.5, n_folds=2
        )
        assert len(result["fold_results"]) == 2
        # With half credits removed, should still produce results
        for fold in result["fold_results"]:
            assert fold["credits_used"] < len(credits)

    def test_empty_credits(self):
        persons = [Person(id="p1", name_en="Test")]
        anime_list = [Anime(id="a1", title_en="Show")]
        result = cross_validate_scores(persons, anime_list, [], n_folds=3)
        assert result["avg_rank_correlation"] == 0
        assert result["fold_results"] == []

    def test_fold_results_structure(self):
        persons, anime_list, credits = _make_test_data()
        result = cross_validate_scores(persons, anime_list, credits, n_folds=2)
        for fold in result["fold_results"]:
            assert "fold" in fold
            assert "credits_used" in fold
            assert "correlation" in fold
            assert "top10_overlap" in fold
