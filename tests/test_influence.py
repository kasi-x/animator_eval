"""influence モジュールのテスト."""

from src.analysis.influence import (
    _find_mentor_mentee_pairs,
    _get_highest_stage,
    compute_influence_tree,
)
from src.models import BronzeAnime as Anime, Credit, Role


def _make_test_data():
    """メンター・メンティー関係のあるテストデータ."""
    anime_map = {
        "a1": Anime(id="a1", title_en="Show 2010", year=2010, score=8.0),
        "a2": Anime(id="a2", title_en="Show 2012", year=2012, score=7.5),
        "a3": Anime(id="a3", title_en="Show 2015", year=2015, score=8.5),
        "a4": Anime(id="a4", title_en="Show 2018", year=2018, score=7.0),
        "a5": Anime(id="a5", title_en="Show 2020", year=2020, score=9.0),
    }
    credits = [
        # p1 is a director on a1, a2, a3
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR, source="test"),
        # p2 worked under p1 on a1, a2 as key animator, then became director on a4, a5
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a4", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a5", role=Role.DIRECTOR, source="test"),
        # p3 worked under p1 on a1, a2, a3 as in-between (never advanced)
        Credit(person_id="p3", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p3", anime_id="a2", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p3", anime_id="a3", role=Role.IN_BETWEEN, source="test"),
        # p4 worked under p2 on a4, a5 as key animator (mentee of mentee)
        Credit(person_id="p4", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"),
        # p5 worked under p1 only on a1 (below threshold)
        Credit(person_id="p5", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
    ]
    return credits, anime_map


class TestFindMentorMenteePairs:
    def test_finds_pairs(self):
        credits, anime_map = _make_test_data()
        pairs = _find_mentor_mentee_pairs(credits, anime_map, min_shared_works=2)
        # p1 mentors p2 (2 shared) and p3 (3 shared)
        assert "p1" in pairs
        assert "p2" in pairs["p1"]
        assert "p3" in pairs["p1"]

    def test_min_shared_works_filter(self):
        credits, anime_map = _make_test_data()
        pairs = _find_mentor_mentee_pairs(credits, anime_map, min_shared_works=2)
        # p5 only shared 1 work with p1, should be excluded
        assert "p5" not in pairs.get("p1", {})

    def test_shared_count(self):
        credits, anime_map = _make_test_data()
        pairs = _find_mentor_mentee_pairs(credits, anime_map, min_shared_works=2)
        assert pairs["p1"]["p3"]["shared_count"] == 3

    def test_year_info(self):
        credits, anime_map = _make_test_data()
        pairs = _find_mentor_mentee_pairs(credits, anime_map, min_shared_works=2)
        assert pairs["p1"]["p2"]["first_year"] == 2010
        assert pairs["p1"]["p2"]["last_year"] == 2012

    def test_p2_mentors_p4(self):
        credits, anime_map = _make_test_data()
        pairs = _find_mentor_mentee_pairs(credits, anime_map, min_shared_works=2)
        # p2 is director on a4, a5 and p4 is key animator there
        assert "p2" in pairs
        assert "p4" in pairs["p2"]


class TestGetHighestStage:
    def test_director(self):
        credits, _ = _make_test_data()
        assert _get_highest_stage("p1", credits) == 6  # Director = stage 6

    def test_key_animator_then_director(self):
        credits, _ = _make_test_data()
        assert _get_highest_stage("p2", credits) == 6  # Also reached Director

    def test_in_between_only(self):
        credits, _ = _make_test_data()
        assert _get_highest_stage("p3", credits) == 1  # In-between = stage 1

    def test_nonexistent(self):
        credits, _ = _make_test_data()
        assert _get_highest_stage("nonexistent", credits) == 0


class TestComputeInfluenceTree:
    def test_returns_expected_keys(self):
        credits, anime_map = _make_test_data()
        result = compute_influence_tree(credits, anime_map)
        assert "mentors" in result
        assert "generation_chains" in result
        assert "total_mentors" in result
        assert "total_mentees" in result
        assert "avg_nurture_rate" in result

    def test_mentor_count(self):
        credits, anime_map = _make_test_data()
        result = compute_influence_tree(credits, anime_map)
        assert result["total_mentors"] == 2  # p1 and p2

    def test_mentee_count(self):
        credits, anime_map = _make_test_data()
        result = compute_influence_tree(credits, anime_map)
        assert result["total_mentees"] == 3  # p2, p3, p4

    def test_nurture_rate(self):
        credits, anime_map = _make_test_data()
        result = compute_influence_tree(credits, anime_map)
        # p1: p2 reached director (stage 6), p3 did not → 50%
        assert result["mentors"]["p1"]["nurture_rate"] == 50.0

    def test_with_scores(self):
        credits, anime_map = _make_test_data()
        scores = {"p2": 80.0, "p3": 30.0, "p4": 50.0}
        result = compute_influence_tree(credits, anime_map, person_scores=scores)
        # p1's influence = p2(80) + p3(30) = 110
        assert result["mentors"]["p1"]["influence_score"] == 110.0

    def test_generation_chains(self):
        credits, anime_map = _make_test_data()
        result = compute_influence_tree(credits, anime_map)
        # Should have chain: p1 → p2 (p2 also became mentor)
        chains = result["generation_chains"]
        assert len(chains) >= 1
        # At least one chain should contain p1 → p2
        has_p1_p2_chain = any("p1" in chain and "p2" in chain for chain in chains)
        assert has_p1_p2_chain

    def test_empty_credits(self):
        result = compute_influence_tree([], {})
        assert result["total_mentors"] == 0
        assert result["total_mentees"] == 0
        assert result["generation_chains"] == []

    def test_no_qualifying_pairs(self):
        """Pairs below min_shared_works threshold."""
        anime_map = {"a1": Anime(id="a1", title_en="Show", year=2020)}
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
        ]
        result = compute_influence_tree(credits, anime_map, min_shared_works=2)
        assert result["total_mentors"] == 0
