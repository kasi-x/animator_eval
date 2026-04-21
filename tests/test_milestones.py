"""milestones モジュールのテスト."""

from src.analysis.milestones import compute_milestones
from src.models import BronzeAnime as Anime, Credit, Role


def _make_career():
    anime_map = {
        "a1": Anime(id="a1", title_en="First Show", year=2015, score=6.0),
        "a2": Anime(id="a2", title_en="Second Show", year=2017, score=7.5),
        "a3": Anime(id="a3", title_en="Third Show", year=2019, score=9.0),
        "a4": Anime(id="a4", title_en="Director Debut", year=2021, score=8.0),
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.IN_BETWEEN),
        Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a3", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="p1", anime_id="a4", role=Role.DIRECTOR),
    ]
    return credits, anime_map


class TestComputeMilestones:
    def test_empty(self):
        result = compute_milestones([], {})
        assert result == {}

    def test_career_start(self):
        credits, anime_map = _make_career()
        result = compute_milestones(credits, anime_map)
        types = [m["type"] for m in result["p1"]]
        assert "career_start" in types
        start = next(m for m in result["p1"] if m["type"] == "career_start")
        assert start["year"] == 2015

    def test_new_role_detected(self):
        credits, anime_map = _make_career()
        result = compute_milestones(credits, anime_map)
        new_roles = [m for m in result["p1"] if m["type"] == "new_role"]
        assert len(new_roles) >= 1

    def test_promotion_detected(self):
        credits, anime_map = _make_career()
        result = compute_milestones(credits, anime_map)
        promotions = [m for m in result["p1"] if m["type"] == "promotion"]
        assert len(promotions) >= 1
        # Stage should increase
        for p in promotions:
            assert p["to_stage"] > p["from_stage"]

    def test_first_director(self):
        credits, anime_map = _make_career()
        result = compute_milestones(credits, anime_map)
        types = [m["type"] for m in result["p1"]]
        assert "first_director" in types
        director = next(m for m in result["p1"] if m["type"] == "first_director")
        assert director["year"] == 2021

    def test_top_anime(self):
        # top_anime now triggers when staff_cnt >= 50 (large-scale production),
        # not based on anime.score. Build credits with 50 staff on a3.
        credits, anime_map = _make_career()
        # Add 49 more staff to a3 so that staff_cnt = 50
        for i in range(2, 51):
            credits.append(
                Credit(person_id=f"extra{i}", anime_id="a3", role=Role.KEY_ANIMATOR)
            )
        result = compute_milestones(credits, anime_map)
        types = [m["type"] for m in result["p1"]]
        assert "top_anime" in types
        top = next(m for m in result["p1"] if m["type"] == "top_anime")
        assert top["anime_id"] == "a3"
        assert top["staff_count"] >= 50

    def test_specific_person(self):
        credits, anime_map = _make_career()
        result = compute_milestones(credits, anime_map, person_id="p1")
        assert "p1" in result
        assert len(result) == 1

    def test_nonexistent_person(self):
        credits, anime_map = _make_career()
        result = compute_milestones(credits, anime_map, person_id="nonexistent")
        assert result == {}

    def test_milestones_sorted_by_year(self):
        credits, anime_map = _make_career()
        result = compute_milestones(credits, anime_map)
        years = [m.get("year", 9999) for m in result["p1"]]
        assert years == sorted(years)

    def test_no_year_data(self):
        anime_map = {"a1": Anime(id="a1", title_en="No Year")}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR)]
        result = compute_milestones(credits, anime_map)
        assert result == {}
