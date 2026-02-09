"""anime_stats モジュールのテスト."""

from src.analysis.anime_stats import compute_anime_stats
from src.models import Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_ja="作品A", year=2020, score=8.0),
        "a2": Anime(id="a2", title_en="Work B", year=2022, score=7.0),
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p3", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p4", anime_id="a2", role=Role.IN_BETWEEN),
    ]
    return credits, anime_map


class TestComputeAnimeStats:
    def test_basic_stats(self):
        credits, anime_map = _make_data()
        stats = compute_anime_stats(credits, anime_map)
        assert "a1" in stats
        assert stats["a1"]["credit_count"] == 3
        assert stats["a1"]["unique_persons"] == 3

    def test_role_distribution(self):
        credits, anime_map = _make_data()
        stats = compute_anime_stats(credits, anime_map)
        roles = stats["a1"]["role_distribution"]
        assert roles["key_animator"] == 2
        assert roles["director"] == 1

    def test_with_person_scores(self):
        credits, anime_map = _make_data()
        person_scores = {"p1": 90.0, "p2": 60.0, "p3": 70.0, "p4": 40.0}
        stats = compute_anime_stats(credits, anime_map, person_scores)
        # a1 has p1(90), p2(60), p3(70) → avg = 73.33
        assert stats["a1"]["avg_person_score"] > 70
        assert len(stats["a1"]["top_persons"]) == 3
        assert stats["a1"]["top_persons"][0]["person_id"] == "p1"

    def test_empty_credits(self):
        assert compute_anime_stats([], {}) == {}

    def test_title_fallback(self):
        credits, anime_map = _make_data()
        stats = compute_anime_stats(credits, anime_map)
        assert stats["a1"]["title"] == "作品A"
        assert stats["a2"]["title"] == "Work B"

    def test_no_person_scores(self):
        credits, anime_map = _make_data()
        stats = compute_anime_stats(credits, anime_map)
        assert "avg_person_score" not in stats["a1"]
