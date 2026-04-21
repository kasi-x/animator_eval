"""time_series モジュールのテスト."""

from src.analysis.time_series import compute_time_series
from src.models import BronzeAnime as Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Show 1", year=2018, score=7.0),
        "a2": Anime(id="a2", title_en="Show 2", year=2019, score=7.5),
        "a3": Anime(id="a3", title_en="Show 3", year=2020, score=8.0),
        "a4": Anime(id="a4", title_en="Show 4", year=2020, score=7.0),
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a3", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="p3", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p4", anime_id="a4", role=Role.DIRECTOR),
    ]
    return credits, anime_map


class TestComputeTimeSeries:
    def test_returns_years(self):
        credits, anime_map = _make_data()
        result = compute_time_series(credits, anime_map)
        assert result["years"] == [2018, 2019, 2020]

    def test_active_persons(self):
        credits, anime_map = _make_data()
        result = compute_time_series(credits, anime_map)
        assert result["series"]["active_persons"][2020] == 4  # p1,p2,p3,p4

    def test_new_entrants(self):
        credits, anime_map = _make_data()
        result = compute_time_series(credits, anime_map)
        # 2018: p1, p2 are new (2 new)
        assert result["series"]["new_entrants"][2018] == 2
        # 2019: p3 is new
        assert result["series"]["new_entrants"][2019] == 1
        # 2020: p4 is new
        assert result["series"]["new_entrants"][2020] == 1

    def test_credit_count(self):
        credits, anime_map = _make_data()
        result = compute_time_series(credits, anime_map)
        assert result["series"]["credit_count"][2020] == 4

    def test_avg_staff_count(self):
        credits, anime_map = _make_data()
        result = compute_time_series(credits, anime_map)
        assert result["series"]["avg_staff_count"][2018] == 2.0

    def test_unique_anime(self):
        credits, anime_map = _make_data()
        result = compute_time_series(credits, anime_map)
        assert result["series"]["unique_anime"][2020] == 2  # a3 + a4

    def test_peak_year(self):
        credits, anime_map = _make_data()
        result = compute_time_series(credits, anime_map)
        assert result["summary"]["peak_year"] == 2020
        assert result["summary"]["peak_credits"] == 4

    def test_total_unique_persons(self):
        credits, anime_map = _make_data()
        result = compute_time_series(credits, anime_map)
        assert result["summary"]["total_unique_persons"] == 4

    def test_empty(self):
        result = compute_time_series([], {})
        assert result["years"] == []
