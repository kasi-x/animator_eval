"""studio モジュールのテスト."""

from src.analysis.studio import compute_studio_analysis
from src.models import Anime, Credit, Role


def _make_test_data():
    anime_map = {
        "a1": Anime(
            id="a1", title_en="Show A", year=2020, score=8.0, studios=["Studio Ghibli"]
        ),
        "a2": Anime(
            id="a2", title_en="Show B", year=2021, score=7.5, studios=["Studio Ghibli"]
        ),
        "a3": Anime(
            id="a3", title_en="Show C", year=2022, score=9.0, studios=["MAPPA"]
        ),
        "a4": Anime(id="a4", title_en="Show D", year=2023, studios=["MAPPA"]),
        "a5": Anime(id="a5", title_en="Show E", year=2023),  # no studio
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a3", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p3", anime_id="a4", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p4", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p5", anime_id="a5", role=Role.DIRECTOR, source="test"),
    ]
    return credits, anime_map


class TestComputeStudioAnalysis:
    def test_identifies_studios(self):
        credits, anime_map = _make_test_data()
        result = compute_studio_analysis(credits, anime_map)
        assert "Studio Ghibli" in result
        assert "MAPPA" in result

    def test_excludes_no_studio(self):
        credits, anime_map = _make_test_data()
        result = compute_studio_analysis(credits, anime_map)
        # a5 has no studio, p5 should not appear
        assert len(result) == 2

    def test_anime_count(self):
        credits, anime_map = _make_test_data()
        result = compute_studio_analysis(credits, anime_map)
        assert result["Studio Ghibli"]["anime_count"] == 2
        assert result["MAPPA"]["anime_count"] == 2

    def test_person_count(self):
        credits, anime_map = _make_test_data()
        result = compute_studio_analysis(credits, anime_map)
        assert result["Studio Ghibli"]["person_count"] == 2  # p1, p2
        assert result["MAPPA"]["person_count"] == 2  # p3, p4

    def test_avg_anime_score(self):
        credits, anime_map = _make_test_data()
        result = compute_studio_analysis(credits, anime_map)
        assert result["Studio Ghibli"]["avg_anime_score"] == 7.75  # (8+7.5)/2
        assert result["MAPPA"]["avg_anime_score"] == 9.0  # only a3 has score

    def test_with_person_scores(self):
        credits, anime_map = _make_test_data()
        scores = {"p1": 80.0, "p2": 60.0, "p3": 90.0, "p4": 50.0}
        result = compute_studio_analysis(credits, anime_map, person_scores=scores)
        assert result["Studio Ghibli"]["avg_person_score"] == 70.0  # (80+60)/2

    def test_top_persons(self):
        credits, anime_map = _make_test_data()
        result = compute_studio_analysis(credits, anime_map)
        top = result["Studio Ghibli"]["top_persons"]
        assert len(top) == 2
        # Both p1 and p2 have 2 credits each
        assert all(p["credit_count"] == 2 for p in top)

    def test_year_range(self):
        credits, anime_map = _make_test_data()
        result = compute_studio_analysis(credits, anime_map)
        assert result["Studio Ghibli"]["year_range"] == [2020, 2021]
        assert result["MAPPA"]["year_range"] == [2022, 2023]

    def test_empty(self):
        result = compute_studio_analysis([], {})
        assert result == {}

    def test_no_studio_data(self):
        anime_map = {"a1": Anime(id="a1", title_en="No Studio")}
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test")
        ]
        result = compute_studio_analysis(credits, anime_map)
        assert result == {}
