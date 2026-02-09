"""growth モジュールのテスト."""

from src.analysis.growth import compute_growth_trends
from src.models import Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Early", year=2015, score=6.0),
        "a2": Anime(id="a2", title_en="Mid", year=2018, score=7.0),
        "a3": Anime(id="a3", title_en="Recent 1", year=2023, score=8.0),
        "a4": Anime(id="a4", title_en="Recent 2", year=2024, score=8.5),
        "a5": Anime(id="a5", title_en="Recent 3", year=2024, score=7.5),
    }
    credits = [
        # p1: rising (few early, many recent)
        Credit(person_id="p1", anime_id="a1", role=Role.IN_BETWEEN),
        Credit(person_id="p1", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a4", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a5", role=Role.ANIMATION_DIRECTOR),
        # p2: declining (many early, none recent)
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR),
        # p3: stable
        Credit(person_id="p3", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a4", role=Role.DIRECTOR),
    ]
    return credits, anime_map


class TestComputeGrowthTrends:
    def test_returns_all_persons(self):
        credits, anime_map = _make_data()
        result = compute_growth_trends(credits, anime_map)
        assert "p1" in result
        assert "p2" in result
        assert "p3" in result

    def test_rising_trend(self):
        credits, anime_map = _make_data()
        result = compute_growth_trends(credits, anime_map, window=3)
        assert result["p1"]["trend"] == "rising"

    def test_inactive_trend(self):
        credits, anime_map = _make_data()
        result = compute_growth_trends(credits, anime_map, window=3)
        assert result["p2"]["trend"] == "inactive"

    def test_yearly_credits(self):
        credits, anime_map = _make_data()
        result = compute_growth_trends(credits, anime_map)
        assert result["p1"]["yearly_credits"][2024] == 2  # a4 + a5

    def test_total_credits(self):
        credits, anime_map = _make_data()
        result = compute_growth_trends(credits, anime_map)
        assert result["p1"]["total_credits"] == 4

    def test_activity_ratio(self):
        credits, anime_map = _make_data()
        result = compute_growth_trends(credits, anime_map, window=3)
        # p1 has 3 recent, 1 early → ratio = 0.75
        assert result["p1"]["activity_ratio"] == 0.75

    def test_with_person_scores(self):
        credits, anime_map = _make_data()
        scores = {"p1": 70.0, "p2": 50.0}
        result = compute_growth_trends(credits, anime_map, person_scores=scores)
        assert result["p1"]["current_score"] == 70.0

    def test_recent_avg_anime_score(self):
        credits, anime_map = _make_data()
        result = compute_growth_trends(credits, anime_map, window=3)
        # p1 recent: a3 (8.0), a4 (8.5), a5 (7.5)
        assert result["p1"]["recent_avg_anime_score"] == 8.0

    def test_empty(self):
        result = compute_growth_trends([], {})
        assert result == {}

    def test_career_span(self):
        credits, anime_map = _make_data()
        result = compute_growth_trends(credits, anime_map)
        # p1: 2015 to 2024 = 10
        assert result["p1"]["career_span"] == 10
