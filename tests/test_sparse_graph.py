"""Tests for SparseCollaborationGraph."""

from src.analysis.sparse_graph import SparseCollaborationGraph


def _make_graph():
    """Create a test graph with 5 nodes and 6 edges."""
    edge_data = {
        ("a", "b"): {"weight": 1.0, "shared_works": 2},
        ("a", "c"): {"weight": 2.0, "shared_works": 3},
        ("b", "c"): {"weight": 0.5, "shared_works": 1},
        ("b", "d"): {"weight": 1.5, "shared_works": 2},
        ("c", "d"): {"weight": 3.0, "shared_works": 4},
        ("d", "e"): {"weight": 0.8, "shared_works": 1},
    }
    node_attrs = {
        "a": {"name": "Alice"},
        "b": {"name": "Bob"},
        "c": {"name": "Charlie"},
        "d": {"name": "Diana"},
        "e": {"name": "Eve"},
    }
    return SparseCollaborationGraph(edge_data, node_attrs)


def test_basic_counts():
    g = _make_graph()
    assert g.number_of_nodes() == 5
    assert g.number_of_edges() == 6


def test_neighbors():
    g = _make_graph()
    assert set(g.neighbors("a")) == {"b", "c"}
    assert set(g.neighbors("d")) == {"b", "c", "e"}
    assert g.neighbors("nonexistent") == []


def test_degree():
    g = _make_graph()
    assert g.degree("a") == 2
    assert g.degree("d") == 3
    degrees = g.degree()
    assert degrees["a"] == 2
    assert degrees["d"] == 3


def test_edges_with_data():
    g = _make_graph()
    edges = list(g.edges(data=True))
    assert len(edges) == 6
    # Find a specific edge
    ab = [e for e in edges if {e[0], e[1]} == {"a", "b"}]
    assert len(ab) == 1
    assert ab[0][2]["weight"] == 1.0
    assert ab[0][2]["shared_works"] == 2


def test_get_edge_data():
    g = _make_graph()
    data = g.get_edge_data("a", "c")
    assert data is not None
    assert data["weight"] == 2.0
    assert data["shared_works"] == 3
    # Symmetric
    data2 = g.get_edge_data("c", "a")
    assert data2 is not None
    assert data2["weight"] == 2.0
    # Nonexistent edge
    assert g.get_edge_data("a", "e") is None


def test_subgraph():
    g = _make_graph()
    sg = g.subgraph(["a", "b", "c"])
    assert sg.number_of_nodes() == 3
    assert sg.number_of_edges() == 3
    assert set(sg.neighbors("a")) == {"b", "c"}


def test_getitem():
    g = _make_graph()
    assert g["a"]["b"]["weight"] == 1.0
    assert g["c"]["d"]["shared_works"] == 4


def test_contains():
    g = _make_graph()
    assert "a" in g
    assert "z" not in g


def test_has_edge():
    g = _make_graph()
    assert g.has_edge("a", "b")
    assert g.has_edge("b", "a")
    assert not g.has_edge("a", "e")


def test_community_detection():
    g = _make_graph()
    communities = g.community_detection_lpa(seed=42)
    assert len(communities) == 5
    # All nodes should have a community
    assert set(communities.keys()) == {"a", "b", "c", "d", "e"}


def test_empty_graph():
    g = SparseCollaborationGraph({}, {"a": {"name": "Alice"}})
    assert g.number_of_nodes() == 1
    assert g.number_of_edges() == 0
    assert g.neighbors("a") == []


def test_disconnected_components():
    edge_data = {
        ("a", "b"): {"weight": 1.0, "shared_works": 1},
        ("c", "d"): {"weight": 1.0, "shared_works": 1},
    }
    g = SparseCollaborationGraph(edge_data)
    assert g.number_of_nodes() == 4
    assert g.number_of_edges() == 2
    assert set(g.neighbors("a")) == {"b"}
    assert set(g.neighbors("c")) == {"d"}
