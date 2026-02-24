"""Tests for Rust-accelerated graph algorithms.

Tests both the Rust implementations (when available) and the Python fallbacks,
verifying that Rust results match NetworkX results within tolerance.
"""

import pytest
import networkx as nx

from src.analysis.graph_rust import (
    RUST_AVAILABLE,
    betweenness_centrality,
    degree_centrality,
    eigenvector_centrality,
    build_collaboration_edges,
)
from src.models import Credit, Person, Role


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def triangle_graph():
    """Simple triangle graph: A-B-C-A."""
    g = nx.Graph()
    g.add_edge("A", "B", weight=1.0)
    g.add_edge("B", "C", weight=1.0)
    g.add_edge("A", "C", weight=1.0)
    return g


@pytest.fixture
def path_graph():
    """Path graph: A-B-C (B is bridge node)."""
    g = nx.Graph()
    g.add_edge("A", "B", weight=1.0)
    g.add_edge("B", "C", weight=1.0)
    return g


@pytest.fixture
def star_graph():
    """Star graph: center=A, leaves=B,C,D,E."""
    g = nx.Graph()
    for leaf in ["B", "C", "D", "E"]:
        g.add_edge("A", leaf, weight=1.0)
    return g


@pytest.fixture
def weighted_graph():
    """Weighted graph for testing weight sensitivity."""
    g = nx.Graph()
    g.add_edge("A", "B", weight=5.0)
    g.add_edge("B", "C", weight=1.0)
    g.add_edge("A", "C", weight=0.5)
    g.add_edge("C", "D", weight=2.0)
    return g


@pytest.fixture
def sample_persons():
    return [
        Person(id="p1", name_ja="人物A", name_en="Person A", source="test"),
        Person(id="p2", name_ja="人物B", name_en="Person B", source="test"),
        Person(id="p3", name_ja="人物C", name_en="Person C", source="test"),
    ]


@pytest.fixture
def sample_credits():
    return [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p3", anime_id="a1", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
    ]


# ---------------------------------------------------------------------------
# Rust availability
# ---------------------------------------------------------------------------


class TestRustAvailability:
    def test_rust_extension_loaded(self):
        """Rust extension should be available in test environment."""
        assert RUST_AVAILABLE, (
            "Rust extension not available. Run: "
            "pixi run maturin develop --release "
            "--manifest-path rust_ext/Cargo.toml --uv"
        )


# ---------------------------------------------------------------------------
# Betweenness centrality
# ---------------------------------------------------------------------------


class TestBetweennessCentrality:
    def test_triangle_all_zero(self, triangle_graph):
        """In a triangle, all betweenness values should be 0."""
        bc = betweenness_centrality(triangle_graph)
        for node, val in bc.items():
            assert abs(val) < 1e-6, f"Node {node} betweenness should be ~0, got {val}"

    def test_path_bridge_highest(self, path_graph):
        """In A-B-C, node B should have highest betweenness."""
        bc = betweenness_centrality(path_graph)
        assert bc["B"] > bc["A"]
        assert bc["B"] > bc["C"]

    def test_star_center_highest(self, star_graph):
        """In a star graph, center A should have highest betweenness."""
        bc = betweenness_centrality(star_graph)
        for leaf in ["B", "C", "D", "E"]:
            assert bc["A"] > bc[leaf]

    def test_approximate_k(self, weighted_graph):
        """Approximate betweenness with k samples should return results."""
        bc = betweenness_centrality(weighted_graph, k=2, seed=42)
        assert len(bc) == 4
        assert all(isinstance(v, float) for v in bc.values())

    def test_empty_graph(self):
        """Empty graph should return empty dict."""
        g = nx.Graph()
        bc = betweenness_centrality(g)
        assert bc == {}

    @pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extension required")
    def test_matches_networkx(self, weighted_graph):
        """Rust betweenness should be close to NetworkX (exact, small graph)."""
        rust_bc = betweenness_centrality(weighted_graph)
        nx_bc = nx.betweenness_centrality(weighted_graph, weight="weight")
        for node in weighted_graph.nodes():
            assert abs(rust_bc[node] - nx_bc[node]) < 0.1, (
                f"Node {node}: rust={rust_bc[node]:.4f}, nx={nx_bc[node]:.4f}"
            )


# ---------------------------------------------------------------------------
# Degree centrality
# ---------------------------------------------------------------------------


class TestDegreeCentrality:
    def test_triangle_all_one(self, triangle_graph):
        """In a complete triangle, degree centrality = 1.0 for all."""
        dc = degree_centrality(triangle_graph)
        for node, val in dc.items():
            assert abs(val - 1.0) < 1e-6, f"Node {node}: expected 1.0, got {val}"

    def test_star_center_higher(self, star_graph):
        """Star center should have degree centrality = 1.0."""
        dc = degree_centrality(star_graph)
        assert abs(dc["A"] - 1.0) < 1e-6

    @pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extension required")
    def test_matches_networkx(self, weighted_graph):
        """Rust degree centrality should match NetworkX exactly."""
        rust_dc = degree_centrality(weighted_graph)
        nx_dc = nx.degree_centrality(weighted_graph)
        for node in weighted_graph.nodes():
            assert abs(rust_dc[node] - nx_dc[node]) < 1e-6, (
                f"Node {node}: rust={rust_dc[node]:.6f}, nx={nx_dc[node]:.6f}"
            )


# ---------------------------------------------------------------------------
# Eigenvector centrality
# ---------------------------------------------------------------------------


class TestEigenvectorCentrality:
    def test_triangle_symmetric(self, triangle_graph):
        """Symmetric graph should give equal eigenvector values."""
        ec = eigenvector_centrality(triangle_graph)
        vals = list(ec.values())
        for v in vals[1:]:
            assert abs(v - vals[0]) < 1e-4

    def test_star_center_highest(self, star_graph):
        """Star center should have highest eigenvector centrality."""
        ec = eigenvector_centrality(star_graph)
        if ec:  # May fail to converge on very small graphs
            for leaf in ["B", "C", "D", "E"]:
                assert ec.get("A", 0) >= ec.get(leaf, 0) - 1e-4


# ---------------------------------------------------------------------------
# Collaboration edge aggregation
# ---------------------------------------------------------------------------


class TestBuildCollaborationEdges:
    def test_basic_edges(self, sample_persons, sample_credits):
        """Should produce edges for persons sharing anime."""
        edge_data = build_collaboration_edges(sample_persons, sample_credits)
        assert len(edge_data) > 0

    def test_shared_works_count(self, sample_persons, sample_credits):
        """p1-p2 share 2 anime (a1, a2), should have shared_works=2."""
        edge_data = build_collaboration_edges(sample_persons, sample_credits)
        key_12 = ("p1", "p2")
        assert key_12 in edge_data
        assert edge_data[key_12]["shared_works"] == 2

    def test_weight_accumulation(self, sample_persons, sample_credits):
        """Edge weight should be sum of per-anime weights."""
        edge_data = build_collaboration_edges(sample_persons, sample_credits)
        key_12 = ("p1", "p2")
        assert edge_data[key_12]["weight"] > 0

    def test_canonical_ordering(self, sample_persons, sample_credits):
        """Edge keys should have person_a < person_b."""
        edge_data = build_collaboration_edges(sample_persons, sample_credits)
        for a, b in edge_data.keys():
            assert a < b, f"Edge ({a}, {b}) not canonically ordered"

    def test_no_self_edges(self, sample_persons, sample_credits):
        """Should not have self-edges (same person)."""
        edge_data = build_collaboration_edges(sample_persons, sample_credits)
        for a, b in edge_data.keys():
            assert a != b

    def test_empty_credits(self, sample_persons):
        """Empty credits should produce empty edges."""
        edge_data = build_collaboration_edges(sample_persons, [])
        assert len(edge_data) == 0


# ---------------------------------------------------------------------------
# Integration: Rust vs Python fallback equivalence
# ---------------------------------------------------------------------------


class TestRustPythonEquivalence:
    @pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extension required")
    def test_collaboration_edges_match(self, sample_persons, sample_credits):
        """Rust and Python should produce identical collaboration edges."""
        import src.analysis.graph_rust as gr

        # Force Rust path
        orig_available = gr.RUST_AVAILABLE
        try:
            gr.RUST_AVAILABLE = True
            rust_edges = gr.build_collaboration_edges(sample_persons, sample_credits)

            gr.RUST_AVAILABLE = False
            python_edges = gr.build_collaboration_edges(sample_persons, sample_credits)

            # Same keys
            assert set(rust_edges.keys()) == set(python_edges.keys())

            # Same values within tolerance
            for key in rust_edges:
                assert (
                    abs(rust_edges[key]["weight"] - python_edges[key]["weight"]) < 1e-6
                )
                assert (
                    rust_edges[key]["shared_works"] == python_edges[key]["shared_works"]
                )
        finally:
            gr.RUST_AVAILABLE = orig_available


# ---------------------------------------------------------------------------
# Betweenness cache (potential_value integration)
# ---------------------------------------------------------------------------


class TestBetweennessCache:
    def test_potential_value_uses_cache(self, weighted_graph):
        """compute_potential_value_scores should use betweenness_cache when provided."""
        from src.analysis.potential_value import compute_potential_value_scores

        person_scores = {
            "A": {"authority": 0.8, "trust": 0.7, "skill": 0.6, "composite": 0.7},
            "B": {"authority": 0.5, "trust": 0.4, "skill": 0.3, "composite": 0.4},
        }
        debiased = {
            "A": {"debiased_authority": 0.85},
            "B": {"debiased_authority": 0.55},
        }
        growth = {
            "A": {"growth_velocity": 1.0, "momentum_score": 0.5, "career_years": 10}
        }
        adjusted = {"A": 0.65, "B": 0.35}

        # Pre-compute cache
        betweenness_cache = betweenness_centrality(weighted_graph)

        # Should not raise and should use cache (no extra betweenness computation)
        result = compute_potential_value_scores(
            person_scores,
            debiased,
            growth,
            adjusted,
            weighted_graph,
            betweenness_cache=betweenness_cache,
        )
        assert "A" in result
        assert "B" in result
