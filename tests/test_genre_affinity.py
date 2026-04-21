"""genre_affinity モジュールのテスト."""

from src.analysis.genre.affinity import compute_genre_affinity, _score_tier, _era
from src.models import BronzeAnime as Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Old Classic", year=1995, score=9.0),
        "a2": Anime(id="a2", title_en="Mid Show", year=2015, score=7.0),
        "a3": Anime(id="a3", title_en="Modern Hit", year=2023, score=8.5),
        "a4": Anime(id="a4", title_en="Modern Mid", year=2022, score=6.0),
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p2", anime_id="a4", role=Role.KEY_ANIMATOR),
    ]
    return credits, anime_map


class TestScoreTier:
    def test_high(self):
        assert _score_tier(8.5) == "high_rated"

    def test_mid(self):
        assert _score_tier(7.0) == "mid_rated"

    def test_low(self):
        assert _score_tier(5.0) == "low_rated"

    def test_none(self):
        assert _score_tier(None) == "unknown"


class TestEra:
    def test_modern(self):
        assert _era(2023) == "modern"

    def test_2010s(self):
        assert _era(2015) == "2010s"

    def test_2000s(self):
        assert _era(2005) == "2000s"

    def test_classic(self):
        assert _era(1995) == "classic"

    def test_none(self):
        assert _era(None) == "unknown"


class TestGenreAffinity:
    def test_empty(self):
        result = compute_genre_affinity([], {})
        assert result == {}

    def test_returns_persons(self):
        credits, anime_map = _make_data()
        result = compute_genre_affinity(credits, anime_map)
        assert "p1" in result
        assert "p2" in result

    def test_score_tiers(self):
        credits, anime_map = _make_data()
        result = compute_genre_affinity(credits, anime_map)
        # p1: high (a1, a3), mid (a2) = 66.7% high, 33.3% mid
        assert "high_rated" in result["p1"]["score_tiers"]

    def test_eras(self):
        credits, anime_map = _make_data()
        result = compute_genre_affinity(credits, anime_map)
        assert "classic" in result["p1"]["eras"]
        assert "modern" in result["p1"]["eras"]

    def test_primary_tier(self):
        credits, anime_map = _make_data()
        result = compute_genre_affinity(credits, anime_map)
        assert result["p1"]["primary_tier"] in (
            "high_rated",
            "mid_rated",
            "low_rated",
            "unknown",
        )

    def test_avg_anime_score(self):
        credits, anime_map = _make_data()
        result = compute_genre_affinity(credits, anime_map)
        assert result["p1"]["avg_anime_score"] is not None
        assert result["p1"]["avg_anime_score"] > 0

    def test_total_credits(self):
        credits, anime_map = _make_data()
        result = compute_genre_affinity(credits, anime_map)
        assert result["p1"]["total_credits"] == 3
        assert result["p2"]["total_credits"] == 2
