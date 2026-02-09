"""productivity モジュールのテスト."""

from src.analysis.productivity import compute_productivity
from src.models import Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="A1", year=2018),
        "a2": Anime(id="a2", title_en="A2", year=2019),
        "a3": Anime(id="a3", title_en="A3", year=2020),
        "a4": Anime(id="a4", title_en="A4", year=2020),
    }
    credits = [
        # p1: 4 credits in 3 years
        Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a3", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="p1", anime_id="a4", role=Role.KEY_ANIMATOR),
        # p2: 1 credit
        Credit(person_id="p2", anime_id="a1", role=Role.DIRECTOR),
    ]
    return credits, anime_map


class TestComputeProductivity:
    def test_total_credits(self):
        credits, anime_map = _make_data()
        result = compute_productivity(credits, anime_map)
        assert result["p1"].total_credits == 4
        assert result["p2"].total_credits == 1

    def test_unique_anime(self):
        credits, anime_map = _make_data()
        result = compute_productivity(credits, anime_map)
        assert result["p1"].unique_anime == 4

    def test_active_years(self):
        credits, anime_map = _make_data()
        result = compute_productivity(credits, anime_map)
        assert result["p1"].active_years == 3  # 2018, 2019, 2020

    def test_career_span(self):
        credits, anime_map = _make_data()
        result = compute_productivity(credits, anime_map)
        assert result["p1"].career_span == 3  # 2020 - 2018 + 1

    def test_credits_per_year(self):
        credits, anime_map = _make_data()
        result = compute_productivity(credits, anime_map)
        assert result["p1"].credits_per_year > 1

    def test_peak_year(self):
        credits, anime_map = _make_data()
        result = compute_productivity(credits, anime_map)
        assert result["p1"].peak_year == 2020  # 2 credits
        assert result["p1"].peak_credits == 2

    def test_consistency_score(self):
        credits, anime_map = _make_data()
        result = compute_productivity(credits, anime_map)
        assert result["p1"].consistency_score == 1.0  # Active every year

    def test_no_year_data(self):
        anime_map = {"a1": Anime(id="a1", title_en="A1", year=None)}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR)]
        result = compute_productivity(credits, anime_map)
        assert result["p1"].active_years == 0
        assert result["p1"].credits_per_year == 0.0

    def test_empty(self):
        result = compute_productivity([], {})
        assert result == {}
