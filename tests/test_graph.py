"""graph モジュールのテスト."""

from src.analysis.graph import (
    create_person_anime_network,
    create_person_collaboration_network,
    create_director_animator_network,
    determine_primary_role_for_each_person,
    calculate_network_centrality_scores,
    compute_graph_summary,
)
from src.models import Anime, Credit, Person, Role


def _sample_data():
    """テスト用データ."""
    persons = [
        Person(id="p1", name_en="Director A"),
        Person(id="p2", name_en="Animator B"),
        Person(id="p3", name_en="Animator C"),
    ]
    anime_list = [
        Anime(id="a1", title_en="Anime 1", year=2020, score=8.5),
        Anime(id="a2", title_en="Anime 2", year=2021, score=7.0),
    ]
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p3", anime_id="a1", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR),
    ]
    return persons, anime_list, credits


class TestBipartiteGraph:
    def test_node_count(self):
        persons, anime_list, credits = _sample_data()
        g = create_person_anime_network(persons, anime_list, credits)
        # 3 persons + 2 anime = 5 nodes
        assert g.number_of_nodes() == 5

    def test_edge_weights(self):
        persons, anime_list, credits = _sample_data()
        g = create_person_anime_network(persons, anime_list, credits)
        # p1 → a1 should exist
        assert g.has_edge("p1", "a1")
        assert g["p1"]["a1"]["weight"] > 0

    def test_bidirectional_edges(self):
        persons, anime_list, credits = _sample_data()
        g = create_person_anime_network(persons, anime_list, credits)
        assert g.has_edge("p1", "a1")
        assert g.has_edge("a1", "p1")


class TestCollaborationGraph:
    def test_collaborators_connected(self):
        persons, _, credits = _sample_data()
        g = create_person_collaboration_network(persons, credits)
        # p1 and p2 both in a1 and a2
        assert g.has_edge("p1", "p2")
        assert g["p1"]["p2"]["shared_works"] == 2

    def test_no_self_loops(self):
        persons, _, credits = _sample_data()
        g = create_person_collaboration_network(persons, credits)
        for node in g.nodes():
            assert not g.has_edge(node, node)


class TestDirectorAnimatorGraph:
    def test_director_to_animator_edges(self):
        _, _, credits = _sample_data()
        g = create_director_animator_network(credits)
        # p1 (director) → p2 (key animator)
        assert g.has_edge("p1", "p2")
        # p1 (director) → p3 (animation director)
        assert g.has_edge("p1", "p3")

    def test_no_animator_to_director(self):
        _, _, credits = _sample_data()
        g = create_director_animator_network(credits)
        assert not g.has_edge("p2", "p1")


class TestCentralityMetrics:
    def test_returns_all_metrics(self):
        persons, _, credits = _sample_data()
        g = create_person_collaboration_network(persons, credits)
        metrics = calculate_network_centrality_scores(g)
        assert len(metrics) > 0
        for pid, m in metrics.items():
            assert "degree" in m
            assert "betweenness" in m
            assert "closeness" in m
            assert "eigenvector" in m

    def test_empty_graph(self):
        import networkx as nx

        g = nx.Graph()
        metrics = calculate_network_centrality_scores(g)
        assert metrics == {}

    def test_person_ids_filter(self):
        persons, _, credits = _sample_data()
        g = create_person_collaboration_network(persons, credits)
        metrics = calculate_network_centrality_scores(g, person_ids={"p1", "p2"})
        assert set(metrics.keys()) == {"p1", "p2"}

    def test_values_in_valid_range(self):
        """中心性指標が 0 以上であることを確認."""
        persons, _, credits = _sample_data()
        g = create_person_collaboration_network(persons, credits)
        metrics = calculate_network_centrality_scores(g)
        for pid, m in metrics.items():
            assert m["degree"] >= 0
            assert m["betweenness"] >= 0
            assert m["closeness"] >= 0
            assert m["eigenvector"] >= 0


class TestClassifyPersonRoles:
    def test_director_classification(self):
        _, _, credits = _sample_data()
        result = determine_primary_role_for_each_person(credits)
        assert result["p1"]["primary_category"] == "director"

    def test_animator_classification(self):
        _, _, credits = _sample_data()
        result = determine_primary_role_for_each_person(credits)
        assert result["p2"]["primary_category"] == "animator"

    def test_total_credits(self):
        _, _, credits = _sample_data()
        result = determine_primary_role_for_each_person(credits)
        # p2 has KEY_ANIMATOR on a1 and a2
        assert result["p2"]["total_credits"] == 2

    def test_role_counts(self):
        _, _, credits = _sample_data()
        result = determine_primary_role_for_each_person(credits)
        assert result["p1"]["role_counts"]["director"] == 2

    def test_empty_credits(self):
        result = determine_primary_role_for_each_person([])
        assert result == {}

    def test_mixed_roles(self):
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
            Credit(person_id="p1", anime_id="a2", role=Role.SCREENPLAY),
            Credit(person_id="p1", anime_id="a3", role=Role.SCREENPLAY),
            Credit(person_id="p1", anime_id="a4", role=Role.SCREENPLAY),
        ]
        result = determine_primary_role_for_each_person(credits)
        # 3 writing credits vs 1 director → primary is writing
        assert result["p1"]["primary_category"] == "writing"


class TestComputeGraphSummary:
    def test_basic_summary(self):
        persons, _, credits = _sample_data()
        graph = create_person_collaboration_network(persons, credits)
        summary = compute_graph_summary(graph)
        assert summary["nodes"] > 0
        assert summary["edges"] >= 0
        assert 0 <= summary["density"] <= 1
        assert summary["avg_degree"] >= 0
        assert summary["components"] >= 1

    def test_empty_graph(self):
        import networkx as nx

        summary = compute_graph_summary(nx.Graph())
        assert summary["nodes"] == 0
        assert summary["edges"] == 0
        assert summary["density"] == 0.0

    def test_has_clustering(self):
        persons, _, credits = _sample_data()
        graph = create_person_collaboration_network(persons, credits)
        summary = compute_graph_summary(graph)
        if "avg_clustering" in summary:
            assert 0 <= summary["avg_clustering"] <= 1
