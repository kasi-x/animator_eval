"""anime_prediction モジュールのテスト."""

from src.analysis.anime_prediction import predict_anime_score
from src.models import Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Hit 1", year=2020, score=8.5),
        "a2": Anime(id="a2", title_en="Hit 2", year=2021, score=8.0),
        "a3": Anime(id="a3", title_en="Avg", year=2022, score=6.0),
        "a4": Anime(id="a4", title_en="No Score", year=2023),
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a4", role=Role.DIRECTOR),
    ]
    return credits, anime_map


class TestPredictAnimeScore:
    def test_returns_prediction(self):
        credits, anime_map = _make_data()
        result = predict_anime_score(["p1", "p2"], credits, anime_map)
        assert result["predicted_score"] is not None
        assert result["predicted_score"] > 0

    def test_higher_overlap_weighted_more(self):
        credits, anime_map = _make_data()
        result = predict_anime_score(["p1", "p2"], credits, anime_map)
        # p1+p2 worked together on a1 (8.5) and a2 (8.0)
        # But p1 alone worked on a3 (6.0)
        # Prediction should be higher due to overlap weighting
        assert result["predicted_score"] > 7.0

    def test_basis_anime_count(self):
        credits, anime_map = _make_data()
        result = predict_anime_score(["p1"], credits, anime_map)
        assert result["basis_anime_count"] == 3  # a1, a2, a3 (not a4 - no score)

    def test_historical_range(self):
        credits, anime_map = _make_data()
        result = predict_anime_score(["p1"], credits, anime_map)
        assert result["historical_range"]["min"] == 6.0
        assert result["historical_range"]["max"] == 8.5

    def test_similar_teams(self):
        credits, anime_map = _make_data()
        result = predict_anime_score(["p1", "p2"], credits, anime_map)
        assert len(result["similar_teams"]) > 0

    def test_confidence_level(self):
        credits, anime_map = _make_data()
        result = predict_anime_score(["p1", "p2"], credits, anime_map)
        assert result["confidence"] in ("high", "medium", "low")

    def test_with_person_scores(self):
        credits, anime_map = _make_data()
        scores = {"p1": 80.0, "p2": 60.0}
        result = predict_anime_score(["p1", "p2"], credits, anime_map, person_scores=scores)
        assert result["team_avg_score"] == 70.0

    def test_empty_team(self):
        credits, anime_map = _make_data()
        result = predict_anime_score([], credits, anime_map)
        assert result["predicted_score"] is None

    def test_no_overlap(self):
        credits, anime_map = _make_data()
        result = predict_anime_score(["nonexistent"], credits, anime_map)
        assert result["predicted_score"] is None
        assert result["confidence"] == "none"
