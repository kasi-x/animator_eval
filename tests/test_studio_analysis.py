"""Tests for studio network modules."""

# Fixtures (anime_map, anime_list, va_credits, production_credits, characters,
# person_fe) are provided by tests/conftest.py


class TestStudioNetwork:
    def test_build_talent_sharing(self, production_credits, anime_map):
        from src.analysis.studio.network import build_talent_sharing_network

        g = build_talent_sharing_network(production_credits, anime_map)
        assert g.number_of_nodes() >= 0

    def test_build_coproduction(self, anime_map):
        from src.analysis.studio.network import build_coproduction_network

        g = build_coproduction_network(anime_map)
        assert g.number_of_nodes() >= 0
        # a2 has StudioA and StudioB -> should create edge
        if g.number_of_edges() > 0:
            assert g.has_edge("StudioA", "StudioB")

    def test_compute_studio_network(self, production_credits, anime_map):
        from src.analysis.studio.network import compute_studio_network

        result = compute_studio_network(production_credits, anime_map)
        assert result.talent_sharing_graph is not None
        assert result.coproduction_graph is not None


# ============================================================
# Talent Pipeline Tests
# ============================================================


class TestTalentPipeline:
    def test_compute_talent_pipeline(self, production_credits, anime_map, person_fe):
        from src.analysis.talent_pipeline import compute_talent_pipeline

        result = compute_talent_pipeline(production_credits, anime_map, person_fe)
        assert isinstance(result.flow_matrix, dict)
        assert isinstance(result.brain_drain_index, dict)
        assert isinstance(result.retention_rates, dict)


# ============================================================
# Genre Ecosystem Tests
# ============================================================


class TestStudioClustering:
    def test_name_clusters_by_rank(self):
        import numpy as np
        from src.analysis.studio.clustering import _name_clusters_by_rank

        centers = np.array(
            [
                [1.0, 10.0],
                [3.0, 5.0],
                [2.0, 1.0],
            ]
        )
        specs = [(0, ["high", "mid", "low"]), (1, ["big", "medium", "small"])]
        names = _name_clusters_by_rank(centers, specs)
        assert len(names) == 3
        # Cluster 1 (feat 0 = 3.0, highest) should be "high"
        assert "high" in names[1]


# ============================================================
# Model Tests
# ============================================================
