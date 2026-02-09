"""work_impact モジュールのテスト."""

from src.analysis.work_impact import compute_work_impact
from src.models import Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Hit Show", year=2023, score=9.0),
        "a2": Anime(id="a2", title_en="Old Show", year=2005, score=6.0),
        "a3": Anime(id="a3", title_en="Mid Show", year=2015, score=7.5),
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p1", anime_id="a2", role=Role.IN_BETWEEN),
        Credit(person_id="p1", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
    ]
    return credits, anime_map


class TestWorkImpact:
    def test_empty(self):
        result = compute_work_impact([], {})
        assert result == {}

    def test_returns_persons(self):
        credits, anime_map = _make_data()
        result = compute_work_impact(credits, anime_map)
        assert "p1" in result
        assert "p2" in result

    def test_impact_sorted_by_score(self):
        credits, anime_map = _make_data()
        result = compute_work_impact(credits, anime_map)
        impacts = result["p1"]
        scores = [i["impact_score"] for i in impacts]
        assert scores == sorted(scores, reverse=True)

    def test_high_score_anime_high_impact(self):
        credits, anime_map = _make_data()
        result = compute_work_impact(credits, anime_map)
        impacts = result["p1"]
        # The hit show (score 9.0, director role, 2023) should be top
        assert impacts[0]["anime_id"] == "a1"

    def test_impact_score_positive(self):
        credits, anime_map = _make_data()
        result = compute_work_impact(credits, anime_map)
        for impacts in result.values():
            for imp in impacts:
                assert imp["impact_score"] > 0

    def test_includes_metadata(self):
        credits, anime_map = _make_data()
        result = compute_work_impact(credits, anime_map)
        impact = result["p1"][0]
        assert "title" in impact
        assert "year" in impact
        assert "role" in impact
        assert "stage" in impact
        assert "team_size" in impact

    def test_max_20_per_person(self):
        """Per person, max 20 works returned."""
        anime_map = {}
        credits = []
        for i in range(30):
            aid = f"a{i}"
            anime_map[aid] = Anime(id=aid, title_en=f"Show {i}", year=2020, score=7.0)
            credits.append(Credit(person_id="p1", anime_id=aid, role=Role.KEY_ANIMATOR))

        result = compute_work_impact(credits, anime_map)
        assert len(result["p1"]) == 20
