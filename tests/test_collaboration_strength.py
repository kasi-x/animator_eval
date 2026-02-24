"""collaboration_strength モジュールのテスト."""

from src.analysis.collaboration_strength import compute_collaboration_strength
from src.models import Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Work 1", year=2018, score=7.0),
        "a2": Anime(id="a2", title_en="Work 2", year=2020, score=8.0),
        "a3": Anime(id="a3", title_en="Work 3", year=2022, score=7.5),
        "a4": Anime(id="a4", title_en="Work 4", year=2024, score=8.5),
    }
    credits = [
        # p1 and p2 work together on a1, a2, a3 (strong pair)
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR, source="test"),
        Credit(
            person_id="p2", anime_id="a3", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        # p1 and p3 work together on a1 only (weak pair)
        Credit(person_id="p3", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        # p2 and p4 work together on a4 only
        Credit(person_id="p4", anime_id="a4", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
    ]
    return credits, anime_map


class TestComputeCollaborationStrength:
    def test_returns_pairs_above_min(self):
        credits, anime_map = _make_data()
        result = compute_collaboration_strength(credits, anime_map, min_shared=2)
        # Only p1-p2 pair has >= 2 shared works
        assert len(result) >= 1
        pair = result[0]
        assert {pair["person_a"], pair["person_b"]} == {"p1", "p2"}

    def test_shared_works_count(self):
        credits, anime_map = _make_data()
        result = compute_collaboration_strength(credits, anime_map, min_shared=2)
        pair = result[0]
        assert pair["shared_works"] == 3  # a1, a2, a3

    def test_longevity(self):
        credits, anime_map = _make_data()
        result = compute_collaboration_strength(credits, anime_map, min_shared=2)
        pair = result[0]
        # 2018 to 2022 = 5 years
        assert pair["first_year"] == 2018
        assert pair["latest_year"] == 2022
        assert pair["longevity"] == 5

    def test_strength_score_range(self):
        credits, anime_map = _make_data()
        result = compute_collaboration_strength(credits, anime_map, min_shared=2)
        for pair in result:
            assert 0 <= pair["strength_score"] <= 100

    def test_role_pairs(self):
        credits, anime_map = _make_data()
        result = compute_collaboration_strength(credits, anime_map, min_shared=2)
        pair = result[0]
        assert len(pair["top_role_pairs"]) > 0

    def test_with_person_scores(self):
        credits, anime_map = _make_data()
        scores = {"p1": 80.0, "p2": 60.0}
        result = compute_collaboration_strength(
            credits,
            anime_map,
            min_shared=2,
            person_scores=scores,
        )
        pair = result[0]
        assert pair["combined_score"] == 70.0  # (80+60)/2

    def test_min_shared_1(self):
        credits, anime_map = _make_data()
        result = compute_collaboration_strength(credits, anime_map, min_shared=1)
        # Multiple pairs should appear
        assert len(result) >= 3

    def test_empty(self):
        result = compute_collaboration_strength([], {})
        assert result == []

    def test_sorted_by_strength(self):
        credits, anime_map = _make_data()
        result = compute_collaboration_strength(credits, anime_map, min_shared=1)
        for i in range(len(result) - 1):
            assert result[i]["strength_score"] >= result[i + 1]["strength_score"]
