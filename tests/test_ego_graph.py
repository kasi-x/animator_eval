"""ego_graph モジュールのテスト."""

from src.analysis.network.ego_graph import extract_ego_graph
from src.models import Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Show 1", year=2020),
        "a2": Anime(id="a2", title_en="Show 2", year=2022),
        "a3": Anime(id="a3", title_en="Show 3", year=2023),
    }
    credits = [
        # p1 works with p2 on a1
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        # p2 works with p3 on a2 (p3 is 2 hops from p1)
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p3", anime_id="a2", role=Role.IN_BETWEEN),
        # p3 works with p4 on a3 (p4 is 3 hops from p1)
        Credit(person_id="p3", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p4", anime_id="a3", role=Role.DIRECTOR),
    ]
    return credits, anime_map


class TestExtractEgoGraph:
    def test_1hop_neighbors(self):
        credits, anime_map = _make_data()
        result = extract_ego_graph("p1", credits, anime_map, hops=1)
        node_ids = {n["id"] for n in result["nodes"]}
        assert "p1" in node_ids
        assert "p2" in node_ids
        assert "p3" not in node_ids  # 2 hops away

    def test_2hop_neighbors(self):
        credits, anime_map = _make_data()
        result = extract_ego_graph("p1", credits, anime_map, hops=2)
        node_ids = {n["id"] for n in result["nodes"]}
        assert "p3" in node_ids  # now reachable
        assert "p4" not in node_ids  # 3 hops

    def test_center_node_distance_0(self):
        credits, anime_map = _make_data()
        result = extract_ego_graph("p1", credits, anime_map, hops=1)
        center = next(n for n in result["nodes"] if n["id"] == "p1")
        assert center["distance"] == 0

    def test_neighbor_distance(self):
        credits, anime_map = _make_data()
        result = extract_ego_graph("p1", credits, anime_map, hops=2)
        p2 = next(n for n in result["nodes"] if n["id"] == "p2")
        p3 = next(n for n in result["nodes"] if n["id"] == "p3")
        assert p2["distance"] == 1
        assert p3["distance"] == 2

    def test_shared_works(self):
        credits, anime_map = _make_data()
        result = extract_ego_graph("p1", credits, anime_map, hops=1)
        p2 = next(n for n in result["nodes"] if n["id"] == "p2")
        assert p2["shared_works"] == 1  # a1

    def test_edges(self):
        credits, anime_map = _make_data()
        result = extract_ego_graph("p1", credits, anime_map, hops=1)
        assert result["total_edges"] >= 1

    def test_with_scores(self):
        credits, anime_map = _make_data()
        scores = {"p1": 80.0, "p2": 60.0}
        result = extract_ego_graph(
            "p1", credits, anime_map, hops=1, person_scores=scores
        )
        p2 = next(n for n in result["nodes"] if n["id"] == "p2")
        assert p2["score"] == 60.0

    def test_nonexistent_person(self):
        credits, anime_map = _make_data()
        result = extract_ego_graph("nonexistent", credits, anime_map)
        assert result["total_nodes"] == 0

    def test_edge_anime_ids(self):
        credits, anime_map = _make_data()
        result = extract_ego_graph("p1", credits, anime_map, hops=1)
        if result["edges"]:
            edge = result["edges"][0]
            assert "anime_ids" in edge
            assert len(edge["anime_ids"]) > 0
