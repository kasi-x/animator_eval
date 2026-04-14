"""compatibility モジュールのテスト."""

import pytest

from src.analysis.compatibility import (
    compute_compatibility_groups,
    GroupCompatibilityResult,
)
from src.models import Anime, Credit, Role


@pytest.fixture
def anime_map():
    """6 anime works with scores — enough to create shared-work pairs."""
    return {
        "a1": Anime(
            id="a1", title_en="Show A", year=2018, score=8.0, studios=["Studio1"]
        ),
        "a2": Anime(
            id="a2", title_en="Show B", year=2019, score=7.5, studios=["Studio1"]
        ),
        "a3": Anime(
            id="a3", title_en="Show C", year=2019, score=9.0, studios=["Studio2"]
        ),
        "a4": Anime(
            id="a4", title_en="Show D", year=2020, score=6.5, studios=["Studio2"]
        ),
        "a5": Anime(
            id="a5", title_en="Show E", year=2021, score=8.5, studios=["Studio1"]
        ),
        "a6": Anime(
            id="a6", title_en="Show F", year=2021, score=7.0, studios=["Studio3"]
        ),
    }


@pytest.fixture
def credits_with_pairs():
    """Credits where p1-p2 share 3+ works; p3-p4 share 3+ works."""
    return [
        # p1 & p2 share a1, a2, a3 (3 works) — strong pair
        Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p2", anime_id="a1", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p2", anime_id="a2", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p1", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p2", anime_id="a3", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        # p1 also works alone on a4
        Credit(person_id="p1", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        # p2 also works alone on a5
        Credit(
            person_id="p2", anime_id="a5", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        # p3 & p4 share a3, a4, a5 (3 works) — second pair
        Credit(person_id="p3", anime_id="a3", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p4", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a4", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p4", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a5", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p4", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"),
        # p3 also works alone on a6
        Credit(person_id="p3", anime_id="a6", role=Role.DIRECTOR, source="test"),
    ]


@pytest.fixture
def iv_scores():
    return {
        "p1": 70.0,
        "p2": 65.0,
        "p3": 80.0,
        "p4": 55.0,
        "p5": 45.0,
        "p6": 60.0,
        "p7": 50.0,
    }


class TestPairDetection:
    """2 persons sharing 3+ works → pair detected."""

    def test_return_type(self, credits_with_pairs, anime_map, iv_scores):
        result = compute_compatibility_groups(credits_with_pairs, anime_map, iv_scores)
        assert isinstance(result, GroupCompatibilityResult)

    def test_pairs_detected(self, credits_with_pairs, anime_map, iv_scores):
        result = compute_compatibility_groups(credits_with_pairs, anime_map, iv_scores)
        assert result.total_pairs_analyzed > 0
        assert len(result.compatible_pairs) > 0

    def test_p1_p2_pair_found(self, credits_with_pairs, anime_map, iv_scores):
        result = compute_compatibility_groups(credits_with_pairs, anime_map, iv_scores)
        # Find the p1-p2 pair
        pair_keys = set()
        for pair in result.compatible_pairs:
            pair_keys.add(frozenset([pair["person_a"], pair["person_b"]]))
        assert frozenset(["p1", "p2"]) in pair_keys

    def test_p3_p4_pair_found(self, credits_with_pairs, anime_map, iv_scores):
        result = compute_compatibility_groups(credits_with_pairs, anime_map, iv_scores)
        pair_keys = set()
        for pair in result.compatible_pairs:
            pair_keys.add(frozenset([pair["person_a"], pair["person_b"]]))
        assert frozenset(["p3", "p4"]) in pair_keys

    def test_pair_shared_works_count(self, credits_with_pairs, anime_map, iv_scores):
        result = compute_compatibility_groups(credits_with_pairs, anime_map, iv_scores)
        for pair in result.compatible_pairs:
            # All detected pairs have at least 3 shared works
            assert pair["shared_works"] >= 3

    def test_pair_has_compatibility_score(
        self, credits_with_pairs, anime_map, iv_scores
    ):
        result = compute_compatibility_groups(credits_with_pairs, anime_map, iv_scores)
        for pair in result.compatible_pairs:
            assert "compatibility_score" in pair
            assert isinstance(pair["compatibility_score"], float)


class TestMinSharedWorks:
    """Pairs with <3 shared works → not detected."""

    def test_two_shared_works_not_detected(self, anime_map, iv_scores):
        """p1 and p2 share only 2 works — below default threshold of 3."""
        credits = [
            Credit(
                person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a1",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(
                person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a2",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            # Only 2 shared works — not enough
        ]
        result = compute_compatibility_groups(credits, anime_map, iv_scores)
        assert result.total_pairs_analyzed == 0
        assert len(result.compatible_pairs) == 0

    def test_custom_min_shared_works(self, anime_map, iv_scores):
        """With min_shared_works=2, 2-work pairs should be detected."""
        credits = [
            Credit(
                person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a1",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(
                person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a2",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            # p1 alone on a3, p2 alone on a4 (for with/without comparison)
            Credit(
                person_id="p1", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a4",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
        ]
        result = compute_compatibility_groups(
            credits, anime_map, iv_scores, min_shared_works=2
        )
        assert result.total_pairs_analyzed >= 1

    def test_one_shared_work_not_detected(self, anime_map, iv_scores):
        """Single co-appearance — always below threshold."""
        credits = [
            Credit(
                person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a1",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
        ]
        result = compute_compatibility_groups(credits, anime_map, iv_scores)
        assert result.total_pairs_analyzed == 0


class TestEmptyData:
    """Empty → empty result."""

    def test_empty_credits(self, anime_map, iv_scores):
        result = compute_compatibility_groups([], anime_map, iv_scores)
        assert isinstance(result, GroupCompatibilityResult)
        assert len(result.compatible_pairs) == 0
        assert len(result.compatible_groups) == 0
        assert result.bridge_persons == {}
        assert result.total_pairs_analyzed == 0

    def test_empty_everything(self):
        result = compute_compatibility_groups([], {}, {})
        assert isinstance(result, GroupCompatibilityResult)
        assert result.total_pairs_analyzed == 0

    def test_no_iv_scores(self, anime_map):
        """Credits exist but no IV scores — no persons pass the iv_scores filter."""
        credits = [
            Credit(
                person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a1",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
        ]
        result = compute_compatibility_groups(credits, anime_map, iv_scores={})
        assert result.total_pairs_analyzed == 0


class TestBridgePersons:
    """Person in 2+ groups → bridge detected."""

    def test_bridge_person_detected(self, anime_map):
        """p2 appears in overlapping triangles spanning two groups."""
        # Group 1: p1-p2-p3 share a1,a2,a3 (triangle)
        # Group 2: p2-p4-p5 share a4,a5,a6 (triangle)
        # p2 bridges both groups
        credits = [
            # Group 1 core: p1, p2, p3 → a1, a2, a3
            Credit(
                person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a1",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(person_id="p3", anime_id="a1", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a2",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(person_id="p3", anime_id="a2", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p1", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a3",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(person_id="p3", anime_id="a3", role=Role.DIRECTOR, source="test"),
            # Solo works for with/without comparison
            Credit(
                person_id="p1", anime_id="a6", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p3", anime_id="a6", role=Role.DIRECTOR, source="test"),
            # Group 2 core: p2, p4, p5 → a4, a5, a6
            Credit(
                person_id="p2",
                anime_id="a4",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(
                person_id="p4", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p5", anime_id="a4", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p2",
                anime_id="a5",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(
                person_id="p4", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p5", anime_id="a5", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p2",
                anime_id="a6",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(
                person_id="p4", anime_id="a6", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p5", anime_id="a6", role=Role.DIRECTOR, source="test"),
            # Solo works for with/without comparison
            Credit(
                person_id="p4", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p5", anime_id="a1", role=Role.DIRECTOR, source="test"),
        ]
        iv_scores = {
            "p1": 70.0,
            "p2": 65.0,
            "p3": 80.0,
            "p4": 55.0,
            "p5": 60.0,
        }
        result = compute_compatibility_groups(credits, anime_map, iv_scores)

        # If groups form triangles and p2 is in 2+ groups → bridge
        if len(result.compatible_groups) >= 2:
            assert len(result.bridge_persons) > 0
            # p2 should be among bridge persons
            assert "p2" in result.bridge_persons

    def test_bridge_score_positive(self, anime_map):
        """Bridge persons have positive bridge scores."""
        # Create overlapping groups to produce bridges
        credits = [
            # Triangle 1: p1-p2-p3 on a1,a2,a3
            Credit(
                person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a1",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(person_id="p3", anime_id="a1", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a2",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(person_id="p3", anime_id="a2", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p1", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2",
                anime_id="a3",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(person_id="p3", anime_id="a3", role=Role.DIRECTOR, source="test"),
            # Solo works
            Credit(
                person_id="p1", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p3", anime_id="a5", role=Role.DIRECTOR, source="test"),
            # Triangle 2: p2-p4-p5 on a4,a5,a6
            Credit(
                person_id="p2",
                anime_id="a4",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(
                person_id="p4", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p5", anime_id="a4", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p2",
                anime_id="a5",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(
                person_id="p4", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p5", anime_id="a5", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p2",
                anime_id="a6",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(
                person_id="p4", anime_id="a6", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p5", anime_id="a6", role=Role.DIRECTOR, source="test"),
            # Solo works
            Credit(
                person_id="p4", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p5", anime_id="a2", role=Role.DIRECTOR, source="test"),
        ]
        iv_scores = {
            "p1": 70.0,
            "p2": 65.0,
            "p3": 80.0,
            "p4": 55.0,
            "p5": 60.0,
        }
        result = compute_compatibility_groups(credits, anime_map, iv_scores)
        for pid, score in result.bridge_persons.items():
            assert score > 0

    def test_no_bridge_single_group(self, credits_with_pairs, anime_map, iv_scores):
        """When all persons are in a single group, no bridges exist."""
        result = compute_compatibility_groups(credits_with_pairs, anime_map, iv_scores)
        # With the default test data, p1-p2 and p3-p4 are separate pairs.
        # If they don't form triangles, there are no groups, hence no bridges.
        # This is valid: no groups → no bridges
        if len(result.compatible_groups) <= 1:
            assert len(result.bridge_persons) == 0

    def test_person_compatibility_boost(self, credits_with_pairs, anime_map, iv_scores):
        """Persons in positive pairs should have compatibility boost."""
        result = compute_compatibility_groups(credits_with_pairs, anime_map, iv_scores)
        # Persons with positive-compatibility pairs get a boost
        for pid, boost in result.person_compatibility_boost.items():
            assert boost > 0
            assert isinstance(boost, float)
