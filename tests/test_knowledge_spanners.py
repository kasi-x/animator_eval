"""Tests for Knowledge Spanners (AWCC + NDI)."""

import networkx as nx
import pytest

from src.analysis.network.knowledge_spanners import (
    KnowledgeSpannerMetrics,
    compute_awcc,
    compute_knowledge_spanners,
)


@pytest.fixture
def bridge_graph():
    """Two communities of 3 nodes each, with 1 bridge node (B).

    Community 0: A1 -- A2 -- A3 (all connected within)
    Community 1: C1 -- C2 -- C3 (all connected within)
    Bridge: B connects to A1 and C1
    """
    g = nx.Graph()
    # Community 0
    g.add_edges_from([("A1", "A2"), ("A2", "A3"), ("A1", "A3")])
    # Community 1
    g.add_edges_from([("C1", "C2"), ("C2", "C3"), ("C1", "C3")])
    # Bridge node B
    g.add_edges_from([("B", "A1"), ("B", "C1")])

    communities = {
        "A1": 0, "A2": 0, "A3": 0,
        "C1": 1, "C2": 1, "C3": 1,
        "B": 2,  # bridge in its own community or assigned to either
    }
    return g, communities


@pytest.fixture
def homogeneous_graph():
    """All nodes in the same community -- no bridging needed."""
    g = nx.Graph()
    g.add_edges_from([("n1", "n2"), ("n2", "n3"), ("n3", "n1")])
    communities = {"n1": 0, "n2": 0, "n3": 0}
    return g, communities


class TestAWCC:
    def test_awcc_bridge_node_high(self, bridge_graph):
        """Bridge node between 2 communities has high AWCC."""
        g, communities = bridge_graph
        awcc = compute_awcc(g, communities)
        # B has 2 neighbors (A1 in community 0, C1 in community 1) = 2 distinct communities
        # AWCC(B) = 2/2 = 1.0
        assert awcc["B"] == pytest.approx(1.0)

    def test_awcc_internal_node_low(self, homogeneous_graph):
        """Internal node has low AWCC (all neighbors in same community)."""
        g, communities = homogeneous_graph
        awcc = compute_awcc(g, communities)
        # Every node has 2 neighbors, all in community 0
        # AWCC = 1/2 = 0.5 (one distinct community / degree)
        for node in ["n1", "n2", "n3"]:
            assert awcc[node] == pytest.approx(1.0 / 2.0)

    def test_awcc_isolated_node(self):
        """Node with no neighbors gets AWCC = 0."""
        g = nx.Graph()
        g.add_node("isolated")
        communities = {"isolated": 0}
        awcc = compute_awcc(g, communities)
        assert awcc["isolated"] == 0.0


class TestKnowledgeSpanners:
    def test_knowledge_spanners_combined(self, bridge_graph):
        """Full function returns KnowledgeSpannerMetrics for each node."""
        g, communities = bridge_graph
        result = compute_knowledge_spanners(g, communities)
        assert isinstance(result, dict)
        assert "B" in result
        assert isinstance(result["B"], KnowledgeSpannerMetrics)
        # Bridge node should have high AWCC and reach multiple communities
        assert result["B"].awcc > 0
        assert result["B"].community_reach == 2
        assert result["B"].degree == 2

    def test_knowledge_spanners_internal_node_reach(self, bridge_graph):
        """Internal node A2 only reaches 1 community (its own neighbors are all comm 0)."""
        g, communities = bridge_graph
        result = compute_knowledge_spanners(g, communities)
        # A2's neighbors: A1 (comm 0) and A3 (comm 0) -> community_reach = 1
        assert result["A2"].community_reach == 1

    def test_empty_communities(self):
        """Handles empty communities dict."""
        g = nx.Graph()
        g.add_edges_from([("n1", "n2")])
        result = compute_knowledge_spanners(g, {})
        assert result == {}

    def test_empty_graph(self):
        """Handles empty graph."""
        g = nx.Graph()
        result = compute_knowledge_spanners(g, {"n1": 0})
        assert result == {}
