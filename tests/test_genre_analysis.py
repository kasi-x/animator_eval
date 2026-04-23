"""Tests for genre ecosystem modules."""

# Fixtures (anime_map, anime_list, va_credits, production_credits, characters,
# person_fe) are provided by tests/conftest.py


class TestGenreEcosystem:
    def test_compute_genre_ecosystem(self, production_credits, anime_map):
        from src.analysis.genre.ecosystem import compute_genre_ecosystem

        result = compute_genre_ecosystem(production_credits, anime_map)
        assert isinstance(result.trends, dict)
        assert isinstance(result.staffing, dict)
        # Action appears in 3 anime -> should have a trend
        if "Action" in result.trends:
            assert result.trends["Action"].trend_class != ""


# ============================================================
# Genre Network Tests
# ============================================================


class TestGenreNetwork:
    def test_compute_pmi(self, anime_list):
        from src.analysis.genre.network import _compute_pmi

        pmi = _compute_pmi(anime_list, min_count=1)
        assert isinstance(pmi, dict)
        # Action and Adventure co-occur in a1
        for pair, val in pmi.items():
            assert isinstance(val, float)

    def test_compute_genre_network(self, anime_list):
        from src.analysis.genre.network import compute_genre_network

        result = compute_genre_network(anime_list)
        assert result.pmi_graph is not None
        assert isinstance(result.genre_families, dict)


# ============================================================
# Genre Quality Tests
# ============================================================


class TestGenreQuality:
    def test_compute_genre_quality(self, production_credits, anime_map, person_fe):
        from src.analysis.genre.quality import compute_genre_quality

        result = compute_genre_quality(production_credits, anime_map, person_fe)
        assert isinstance(result.quality, dict)
        assert isinstance(result.saturation, dict)
        assert isinstance(result.mobility, dict)


# ============================================================
# Synthetic VA Data Tests
# ============================================================
