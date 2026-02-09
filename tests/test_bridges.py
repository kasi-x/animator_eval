"""bridges モジュールのテスト."""

from src.analysis.bridges import detect_bridges, _compute_simple_communities
from src.models import Credit, Role


def _make_two_clusters():
    """2つのクラスターを持つデータ."""
    credits = [
        # Cluster A: p1, p2, p3 on a1
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p3", anime_id="a1", role=Role.KEY_ANIMATOR),
        # Cluster B: p4, p5, p6 on a2
        Credit(person_id="p4", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p5", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p6", anime_id="a2", role=Role.KEY_ANIMATOR),
        # Bridge: p2 also appears in a2
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR),
    ]
    return credits


class TestDetectBridges:
    def test_empty(self):
        result = detect_bridges([])
        assert result["bridge_persons"] == []

    def test_single_cluster_no_bridges(self):
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
            Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        # Single connected component → everyone same community
        result = detect_bridges(credits)
        assert result["bridge_persons"] == []
        assert result["cross_community_edges"] == []

    def test_two_clusters_with_bridge(self):
        credits = _make_two_clusters()
        # Provide explicit communities
        communities = {"p1": 0, "p2": 0, "p3": 0, "p4": 1, "p5": 1, "p6": 1}
        result = detect_bridges(credits, communities)
        # p2 is the bridge
        bridge_ids = [b["person_id"] for b in result["bridge_persons"]]
        assert "p2" in bridge_ids

    def test_bridge_score(self):
        credits = _make_two_clusters()
        communities = {"p1": 0, "p2": 0, "p3": 0, "p4": 1, "p5": 1, "p6": 1}
        result = detect_bridges(credits, communities)
        p2_bridge = next(b for b in result["bridge_persons"] if b["person_id"] == "p2")
        assert p2_bridge["bridge_score"] > 0
        assert p2_bridge["communities_connected"] == 2

    def test_cross_community_edges(self):
        credits = _make_two_clusters()
        communities = {"p1": 0, "p2": 0, "p3": 0, "p4": 1, "p5": 1, "p6": 1}
        result = detect_bridges(credits, communities)
        assert len(result["cross_community_edges"]) > 0
        for edge in result["cross_community_edges"]:
            assert edge["community_a"] != edge["community_b"]

    def test_stats(self):
        credits = _make_two_clusters()
        communities = {"p1": 0, "p2": 0, "p3": 0, "p4": 1, "p5": 1, "p6": 1}
        result = detect_bridges(credits, communities)
        assert result["stats"]["total_communities"] == 2
        assert result["stats"]["total_persons"] == 6

    def test_auto_communities(self):
        """communities=None の場合は内部で計算."""
        credits = _make_two_clusters()
        result = detect_bridges(credits)
        # p2 connects both clusters so it's all one component
        # → no bridges because everyone is in same community
        assert result["stats"]["total_persons"] == 6


class TestSimpleCommunities:
    def test_single_component(self):
        persons = {"p1", "p2", "p3"}
        edges = {("p1", "p2"): ["a1"], ("p2", "p3"): ["a1"]}
        communities = _compute_simple_communities(persons, edges)
        # All should be same community
        vals = set(communities.values())
        assert len(vals) == 1

    def test_two_components(self):
        persons = {"p1", "p2", "p3", "p4"}
        edges = {("p1", "p2"): ["a1"], ("p3", "p4"): ["a2"]}
        communities = _compute_simple_communities(persons, edges)
        assert communities["p1"] == communities["p2"]
        assert communities["p3"] == communities["p4"]
        assert communities["p1"] != communities["p3"]
