"""pagerank モジュールのテスト."""

import networkx as nx

from src.analysis.pagerank import compute_authority_scores, normalize_scores, weighted_pagerank


class TestWeightedPagerank:
    def test_empty_graph(self):
        g = nx.DiGraph()
        assert weighted_pagerank(g) == {}

    def test_simple_graph(self):
        g = nx.DiGraph()
        g.add_edge("a", "b", weight=1.0)
        g.add_edge("b", "a", weight=1.0)
        scores = weighted_pagerank(g)
        assert len(scores) == 2
        assert all(v > 0 for v in scores.values())

    def test_weighted_edges_affect_scores(self):
        g = nx.DiGraph()
        g.add_edge("a", "b", weight=10.0)
        g.add_edge("b", "a", weight=1.0)
        g.add_edge("a", "c", weight=1.0)
        g.add_edge("c", "a", weight=1.0)
        scores = weighted_pagerank(g)
        # b receives much more weight from a than c
        assert scores["b"] > scores["c"]


class TestNormalizeScores:
    def test_empty(self):
        assert normalize_scores({}) == {}

    def test_single_value(self):
        result = normalize_scores({"a": 0.5})
        assert result["a"] == 50.0

    def test_range(self):
        result = normalize_scores({"low": 0.1, "high": 0.9})
        assert result["low"] == 0.0
        assert result["high"] == 100.0


class TestComputeAuthorityScores:
    def test_person_only(self):
        g = nx.DiGraph()
        g.add_node("p1", type="person", name="Person 1")
        g.add_node("a1", type="anime", name="Anime 1")
        g.add_edge("p1", "a1", weight=2.0)
        g.add_edge("a1", "p1", weight=2.0)

        scores = compute_authority_scores(g, person_only=True)
        assert "p1" in scores
        assert "a1" not in scores
