"""Tests for BiRank bipartite PageRank."""

import networkx as nx
import pytest

from src.analysis.birank import BiRankResult, compute_birank


def _make_bipartite_graph(person_anime_edges):
    """Create a bipartite DiGraph with person and anime nodes.

    Args:
        person_anime_edges: list of (person_id, anime_id, weight) tuples
    """
    g = nx.DiGraph()
    for pid, aid, weight in person_anime_edges:
        g.add_node(pid, type="person")
        g.add_node(aid, type="anime")
        g.add_edge(pid, aid, weight=weight)
    return g


@pytest.fixture
def simple_graph():
    """Simple bipartite graph: 3 persons, 2 anime, varying connectivity."""
    edges = [
        ("p1", "a1", 3.0),
        ("p1", "a2", 2.0),
        ("p2", "a1", 1.0),
        ("p3", "a2", 1.0),
    ]
    return _make_bipartite_graph(edges)


@pytest.fixture
def single_edge_graph():
    """Minimal graph: one person, one anime, one edge."""
    return _make_bipartite_graph([("p1", "a1", 1.0)])


class TestBiRankConvergence:
    def test_birank_convergence(self, simple_graph):
        """BiRank converges on a simple well-connected graph."""
        result = compute_birank(simple_graph, max_iter=200, tol=1e-8)
        assert isinstance(result, BiRankResult)
        assert result.converged is True
        assert result.iterations > 0

    def test_birank_iterations_within_limit(self, simple_graph):
        result = compute_birank(simple_graph, max_iter=200)
        assert result.iterations <= 200


class TestBiRankScoreOrdering:
    def test_birank_score_ordering(self, simple_graph):
        """Person with more/higher-weight edges gets higher score."""
        result = compute_birank(simple_graph)
        # p1 connects to both anime with high weights (3.0 + 2.0 = 5.0)
        # p2 connects to one anime with weight 1.0
        # p3 connects to one anime with weight 1.0
        assert result.person_scores["p1"] > result.person_scores["p2"]
        assert result.person_scores["p1"] > result.person_scores["p3"]

    def test_all_scores_positive(self, simple_graph):
        result = compute_birank(simple_graph)
        for score in result.person_scores.values():
            assert score > 0
        for score in result.anime_scores.values():
            assert score > 0


class TestBiRankQueryVector:
    def test_birank_query_vector(self, simple_graph):
        """Personalized query boosts the specified person."""
        baseline = compute_birank(simple_graph)
        # Strongly boost p3
        query = {"p3": 10.0, "p1": 0.01, "p2": 0.01}
        boosted = compute_birank(simple_graph, query_vector=query)

        # p3's share should increase relative to baseline
        p3_baseline_share = baseline.person_scores["p3"] / sum(
            baseline.person_scores.values()
        )
        p3_boosted_share = boosted.person_scores["p3"] / sum(
            boosted.person_scores.values()
        )
        assert p3_boosted_share > p3_baseline_share


class TestBiRankEdgeCases:
    def test_birank_empty_graph(self):
        """Empty graph returns empty result."""
        g = nx.DiGraph()
        result = compute_birank(g)
        assert result.person_scores == {}
        assert result.anime_scores == {}
        assert result.converged is True
        assert result.iterations == 0

    def test_birank_no_edges(self):
        """Graph with nodes but no edges returns zero scores."""
        g = nx.DiGraph()
        g.add_node("p1", type="person")
        g.add_node("a1", type="anime")
        result = compute_birank(g)
        assert result.person_scores.get("p1", 0.0) == 0.0
        assert result.anime_scores.get("a1", 0.0) == 0.0

    def test_birank_single_edge(self, single_edge_graph):
        """Single person-anime edge works without error."""
        result = compute_birank(single_edge_graph)
        assert "p1" in result.person_scores
        assert "a1" in result.anime_scores
        assert result.person_scores["p1"] > 0
        assert result.converged is True

    def test_birank_persons_only_graph(self):
        """Graph with only person nodes (no anime) returns empty."""
        g = nx.DiGraph()
        g.add_node("p1", type="person")
        g.add_node("p2", type="person")
        result = compute_birank(g)
        assert result.person_scores == {}
        assert result.anime_scores == {}
