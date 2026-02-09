"""seasonal モジュールのテスト."""

from src.analysis.seasonal import compute_seasonal_trends
from src.models import Anime, Credit, Role


def _make_test_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Winter 2020", year=2020, season="winter", score=8.0),
        "a2": Anime(id="a2", title_en="Spring 2020", year=2020, season="spring", score=7.0),
        "a3": Anime(id="a3", title_en="Summer 2021", year=2021, season="summer", score=9.0),
        "a4": Anime(id="a4", title_en="Fall 2021", year=2021, season="fall", score=7.5),
        "a5": Anime(id="a5", title_en="No Season", year=2022),
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p3", anime_id="a3", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p4", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a4", role=Role.ANIMATION_DIRECTOR, source="test"),
        Credit(person_id="p5", anime_id="a5", role=Role.DIRECTOR, source="test"),
    ]
    return credits, anime_map


class TestComputeSeasonalTrends:
    def test_identifies_seasons(self):
        credits, anime_map = _make_test_data()
        result = compute_seasonal_trends(credits, anime_map)
        assert "winter" in result["by_season"]
        assert "spring" in result["by_season"]
        assert "summer" in result["by_season"]
        assert "fall" in result["by_season"]

    def test_anime_count(self):
        credits, anime_map = _make_test_data()
        result = compute_seasonal_trends(credits, anime_map)
        assert result["by_season"]["winter"]["anime_count"] == 1
        assert result["by_season"]["summer"]["anime_count"] == 1

    def test_credit_count(self):
        credits, anime_map = _make_test_data()
        result = compute_seasonal_trends(credits, anime_map)
        assert result["by_season"]["winter"]["credit_count"] == 2  # p1, p2

    def test_excludes_no_season(self):
        credits, anime_map = _make_test_data()
        result = compute_seasonal_trends(credits, anime_map)
        # a5 has no season, should not be in any season
        total = sum(s["credit_count"] for s in result["by_season"].values())
        assert total == 6  # excludes p5's credit on a5

    def test_role_by_season(self):
        credits, anime_map = _make_test_data()
        result = compute_seasonal_trends(credits, anime_map)
        assert result["role_by_season"]["winter"]["director"] == 1
        assert result["role_by_season"]["winter"]["key_animator"] == 1

    def test_with_scores(self):
        credits, anime_map = _make_test_data()
        scores = {"p1": 80.0, "p2": 60.0, "p3": 90.0, "p4": 50.0}
        result = compute_seasonal_trends(credits, anime_map, person_scores=scores)
        assert "avg_person_score" in result["by_season"]["winter"]

    def test_empty(self):
        result = compute_seasonal_trends([], {})
        assert result["by_season"] == {}
        assert result["total_with_season"] == 0

    def test_total_with_season(self):
        credits, anime_map = _make_test_data()
        result = compute_seasonal_trends(credits, anime_map)
        assert result["total_with_season"] == 6
