"""decade_analysis モジュールのテスト."""

from src.analysis.decade_analysis import compute_decade_analysis
from src.models import BronzeAnime as Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="2000s Show", year=2005),
        "a2": Anime(id="a2", title_en="2010s Show", year=2015),
        "a3": Anime(id="a3", title_en="2020s Show A", year=2022),
        "a4": Anime(id="a4", title_en="2020s Show B", year=2024),
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p2", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p1", anime_id="a2", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="p1", anime_id="a3", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="p2", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a4", role=Role.ANIMATION_DIRECTOR),
    ]
    return credits, anime_map


class TestComputeDecadeAnalysis:
    def test_returns_decades(self):
        credits, anime_map = _make_data()
        result = compute_decade_analysis(credits, anime_map)
        assert "2000s" in result["decades"]
        assert "2010s" in result["decades"]
        assert "2020s" in result["decades"]

    def test_credit_count(self):
        credits, anime_map = _make_data()
        result = compute_decade_analysis(credits, anime_map)
        assert result["decades"]["2000s"]["credit_count"] == 2
        assert result["decades"]["2020s"]["credit_count"] == 4

    def test_unique_persons(self):
        credits, anime_map = _make_data()
        result = compute_decade_analysis(credits, anime_map)
        assert result["decades"]["2020s"]["unique_persons"] == 3  # p1, p2, p3

    def test_avg_staff_count(self):
        credits, anime_map = _make_data()
        result = compute_decade_analysis(credits, anime_map)
        assert result["decades"]["2000s"]["avg_staff_count"] == 2.0

    def test_role_distribution(self):
        credits, anime_map = _make_data()
        result = compute_decade_analysis(credits, anime_map)
        roles = result["decades"]["2000s"]["role_distribution"]
        assert "key_animator" in roles
        assert "director" in roles

    def test_top_persons(self):
        credits, anime_map = _make_data()
        result = compute_decade_analysis(credits, anime_map)
        top = result["decades"]["2020s"]["top_persons"]
        assert len(top) > 0
        # p1 has 2 credits in 2020s
        p1_entry = next((t for t in top if t["person_id"] == "p1"), None)
        assert p1_entry is not None
        assert p1_entry["credits"] == 2

    def test_year_by_year(self):
        credits, anime_map = _make_data()
        result = compute_decade_analysis(credits, anime_map)
        assert 2022 in result["year_by_year"]
        assert result["year_by_year"][2022]["credits"] == 3

    def test_empty(self):
        result = compute_decade_analysis([], {})
        assert result["decades"] == {}
        assert result["year_by_year"] == {}
