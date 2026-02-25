"""graph モジュールのテスト."""

from src.analysis.graph import (
    create_person_anime_network,
    create_person_collaboration_network,
    create_director_animator_network,
    determine_primary_role_for_each_person,
    calculate_network_centrality_scores,
    compute_graph_summary,
    _episode_weight_for_pair,
    _compute_anime_commitments,
    _work_importance,
    _episode_coverage,
)
from src.models import Anime, Credit, Person, Role
from src.utils.role_groups import generate_core_team_pairs


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


class TestEpisodeWeightForPair:
    def test_full_overlap(self):
        w = _episode_weight_for_pair(
            {1, 2, 3}, {1, 2, 3}, Role.KEY_ANIMATOR, Role.KEY_ANIMATOR, 12
        )
        assert w == 1.0

    def test_partial_overlap(self):
        w = _episode_weight_for_pair(
            {1, 2, 3}, {2, 3, 4}, Role.KEY_ANIMATOR, Role.KEY_ANIMATOR, 12
        )
        # overlap=2, union=4 → 0.5
        assert w == 0.5

    def test_no_overlap(self):
        w = _episode_weight_for_pair(
            {1, 2}, {3, 4}, Role.KEY_ANIMATOR, Role.KEY_ANIMATOR, 12
        )
        assert w == 0.0

    def test_no_episode_data_small_anime(self):
        w = _episode_weight_for_pair(
            set(), set(), Role.KEY_ANIMATOR, Role.KEY_ANIMATOR, 12
        )
        assert w == 1.0

    def test_no_episode_data_large_anime_through_roles(self):
        w = _episode_weight_for_pair(
            set(), set(), Role.DIRECTOR, Role.CHARACTER_DESIGNER, 200
        )
        assert w == 1.0

    def test_no_episode_data_large_anime_episodic_and_through(self):
        w = _episode_weight_for_pair(
            set(), set(), Role.DIRECTOR, Role.KEY_ANIMATOR, 200
        )
        # min(26/200, 1.0) = 0.13
        assert 0.12 < w < 0.14

    def test_no_episode_data_large_anime_both_episodic(self):
        w = _episode_weight_for_pair(
            set(), set(), Role.KEY_ANIMATOR, Role.IN_BETWEEN, 200
        )
        dilution = 26.0 / 200
        assert abs(w - dilution * dilution) < 0.001

    def test_one_has_episodes_other_doesnt_small_anime(self):
        """Small anime: one side missing → assume full overlap."""
        w = _episode_weight_for_pair(
            {1, 2, 3}, set(), Role.KEY_ANIMATOR, Role.KEY_ANIMATOR, 12
        )
        assert w == 1.0

    def test_one_has_episodes_other_is_through_role(self):
        """Through role without episodes → full overlap."""
        w = _episode_weight_for_pair(
            {1, 2, 3}, set(), Role.KEY_ANIMATOR, Role.DIRECTOR, 200
        )
        assert w == 1.0

    def test_one_has_episodes_other_episodic_large_anime(self):
        """Episodic role without episodes on large anime → estimated overlap."""
        w = _episode_weight_for_pair(
            {1, 2, 3}, set(), Role.KEY_ANIMATOR, Role.KEY_ANIMATOR, 200
        )
        # known_frac = 3/200, unknown_frac = 26/200
        # overlap_est / union_est formula should give a small positive value
        assert 0 < w < 1.0

    def test_no_total_episodes(self):
        """When anime has no episode count info, default to 1.0."""
        w = _episode_weight_for_pair(
            set(), set(), Role.KEY_ANIMATOR, Role.KEY_ANIMATOR, None
        )
        assert w == 1.0


class TestEpisodeAwareCollaborationGraph:
    def test_episode_overlap_reduces_weight(self):
        """Credits on different episodes should produce lower edge weight."""
        persons = [
            Person(id="p1", name_en="Animator A"),
            Person(id="p2", name_en="Animator B"),
        ]
        anime_map = {"a1": Anime(id="a1", title_en="Long Anime", episodes=100)}
        # p1 on eps 1-5, p2 on eps 6-10 → no overlap
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, episode=i)
            for i in range(1, 6)
        ] + [
            Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, episode=i)
            for i in range(6, 11)
        ]
        g = create_person_collaboration_network(persons, credits, anime_map=anime_map)
        # No episode overlap → no edge
        assert not g.has_edge("p1", "p2")

    def test_episode_overlap_creates_edge(self):
        """Credits on same episodes should create an edge."""
        persons = [
            Person(id="p1", name_en="Animator A"),
            Person(id="p2", name_en="Animator B"),
        ]
        anime_map = {"a1": Anime(id="a1", title_en="Anime 1", episodes=24)}
        # Both on eps 1-3
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, episode=i)
            for i in range(1, 4)
        ] + [
            Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, episode=i)
            for i in range(1, 4)
        ]
        g = create_person_collaboration_network(persons, credits, anime_map=anime_map)
        assert g.has_edge("p1", "p2")
        assert g["p1"]["p2"]["weight"] > 0

    def test_without_anime_map_still_works(self):
        """Backward compatibility: no anime_map → original behavior."""
        persons, _, credits = _sample_data()
        g = create_person_collaboration_network(persons, credits)
        assert g.has_edge("p1", "p2")
        assert g["p1"]["p2"]["shared_works"] == 2


class TestEpisodeCoverage:
    def test_with_episode_data(self):
        cov = _episode_coverage(Role.KEY_ANIMATOR, {1, 2, 3}, 12)
        assert abs(cov - 3.0 / 12) < 0.001

    def test_through_role_no_episodes(self):
        cov = _episode_coverage(Role.DIRECTOR, set(), 200)
        assert cov == 1.0

    def test_episodic_role_no_episodes_large_anime(self):
        cov = _episode_coverage(Role.KEY_ANIMATOR, set(), 200)
        assert abs(cov - 26.0 / 200) < 0.001

    def test_episodic_role_no_episodes_small_anime(self):
        cov = _episode_coverage(Role.KEY_ANIMATOR, set(), 12)
        assert cov == 1.0

    def test_no_total_episodes(self):
        cov = _episode_coverage(Role.KEY_ANIMATOR, set(), None)
        assert cov == 1.0


class TestComputeAnimeCommitments:
    def test_single_role(self):
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        ]
        commits = _compute_anime_commitments(credits, None)
        # Director weight = 3.0, no episode data → coverage 1.0
        assert abs(commits["a1"]["p1"] - 3.0) < 0.001

    def test_multi_role_sums(self):
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="p1", anime_id="a1", role=Role.ANIMATION_DIRECTOR),
        ]
        commits = _compute_anime_commitments(credits, None)
        # KEY_ANIMATOR=2.0 + ANIMATION_DIRECTOR≈2.49 ≈ 4.49
        assert abs(commits["a1"]["p1"] - 4.49) < 0.02

    def test_episode_coverage_reduces_commitment(self):
        anime_map = {"a1": Anime(id="a1", title_en="Long Anime", episodes=200)}
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, episode=5),
        ]
        commits = _compute_anime_commitments(credits, anime_map)
        # KEY_ANIMATOR=2.0 × (1/200) = 0.01
        assert abs(commits["a1"]["p1"] - 2.0 / 200) < 0.001

    def test_through_role_full_coverage(self):
        anime_map = {"a1": Anime(id="a1", title_en="Long Anime", episodes=200)}
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        ]
        commits = _compute_anime_commitments(credits, anime_map)
        # Director is through-role → coverage 1.0, weight 3.0
        assert abs(commits["a1"]["p1"] - 3.0) < 0.001


class TestWorkImportance:
    def test_high_score(self):
        anime = Anime(id="a1", title_en="Great Anime", score=9.0)
        assert abs(_work_importance(anime) - 0.9) < 0.001

    def test_low_score(self):
        anime = Anime(id="a1", title_en="Low Anime", score=3.0)
        assert abs(_work_importance(anime) - 0.3) < 0.001

    def test_very_low_score_clamped(self):
        anime = Anime(id="a1", title_en="Bad Anime", score=0.5)
        assert _work_importance(anime) == 0.1

    def test_none_score(self):
        anime = Anime(id="a1", title_en="Unknown Anime")
        assert _work_importance(anime) == 0.5

    def test_none_anime(self):
        assert _work_importance(None) == 0.5


class TestCommitmentWeighting:
    def test_multi_role_higher_weight_than_single_role(self):
        """Person with multiple roles should produce heavier edges."""
        persons = [
            Person(id="p1", name_en="Multi-Role"),
            Person(id="p2", name_en="Single-Role"),
            Person(id="p3", name_en="Reference"),
        ]
        anime_map = {"a1": Anime(id="a1", title_en="Anime 1", score=8.0)}
        credits = [
            # p1 has KEY_ANIMATOR + ANIMATION_DIRECTOR
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="p1", anime_id="a1", role=Role.ANIMATION_DIRECTOR),
            # p2 has only IN_BETWEEN
            Credit(person_id="p2", anime_id="a1", role=Role.IN_BETWEEN),
            # p3 is a reference collaborator
            Credit(person_id="p3", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        g = create_person_collaboration_network(persons, credits, anime_map=anime_map)
        # p1-p3 edge should be heavier than p2-p3 edge
        w_multi = g["p1"]["p3"]["weight"]
        w_single = g["p2"]["p3"]["weight"]
        assert w_multi > w_single

    def test_every_episode_higher_than_single_episode(self):
        """Person on all episodes should have heavier edges than 1-episode person."""
        persons = [
            Person(id="p1", name_en="Full Run"),
            Person(id="p2", name_en="One Episode"),
            Person(id="p3", name_en="Director"),
        ]
        anime_map = {
            "a1": Anime(id="a1", title_en="Long Anime", episodes=50, score=7.0)
        }
        credits = [
            # p3 is director (through-role, full coverage)
            Credit(person_id="p3", anime_id="a1", role=Role.DIRECTOR),
        ]
        # p1 on all 50 episodes
        credits += [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, episode=i)
            for i in range(1, 51)
        ]
        # p2 on only episode 5
        credits += [
            Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, episode=5),
        ]
        g = create_person_collaboration_network(persons, credits, anime_map=anime_map)
        # p1-p3 should be much heavier than p2-p3
        assert g.has_edge("p1", "p3")
        assert g.has_edge("p2", "p3")
        w_full = g["p1"]["p3"]["weight"]
        w_single = g["p2"]["p3"]["weight"]
        assert w_full > w_single * 5  # significantly heavier

    def test_high_score_anime_produces_heavier_edges(self):
        """Edges from high-score anime should be heavier than from low-score anime."""
        persons = [
            Person(id="p1", name_en="Animator A"),
            Person(id="p2", name_en="Animator B"),
        ]
        # High-score anime
        anime_map_high = {"a1": Anime(id="a1", title_en="Great", score=9.0)}
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        g_high = create_person_collaboration_network(
            persons, credits, anime_map=anime_map_high
        )
        w_high = g_high["p1"]["p2"]["weight"]

        # Low-score anime
        anime_map_low = {"a1": Anime(id="a1", title_en="Bad", score=3.0)}
        g_low = create_person_collaboration_network(
            persons, credits, anime_map=anime_map_low
        )
        w_low = g_low["p1"]["p2"]["weight"]

        assert w_high > w_low
        # 9.0/10 vs 3.0/10 → 3x ratio
        assert abs(w_high / w_low - 3.0) < 0.01

    def test_shared_works_unchanged(self):
        """shared_works should still count anime, not be affected by commitment."""
        persons, anime_list, credits = _sample_data()
        anime_map = {a.id: a for a in anime_list}
        g = create_person_collaboration_network(persons, credits, anime_map=anime_map)
        # p1 and p2 share a1 and a2
        assert g["p1"]["p2"]["shared_works"] == 2


class TestCoreTeamPairs:
    def test_core_team_pairs_basic(self):
        """CORE_TEAM members should all be connected to each other."""
        staff = {
            "p1": Role.DIRECTOR,
            "p2": Role.ANIMATION_DIRECTOR,
            "p3": Role.KEY_ANIMATOR,
        }
        pairs = generate_core_team_pairs(staff)
        pair_set = {tuple(sorted(p)) for p in pairs}
        # All three are CORE_TEAM → 3 pairs
        assert ("p1", "p2") in pair_set
        assert ("p1", "p3") in pair_set
        assert ("p2", "p3") in pair_set

    def test_non_core_connects_to_core(self):
        """Non-core staff should connect to all core members."""
        staff = {
            "p1": Role.DIRECTOR,           # core
            "p2": Role.KEY_ANIMATOR,        # core
            "p3": Role.IN_BETWEEN,          # non-core
        }
        pairs = generate_core_team_pairs(staff)
        pair_set = {tuple(sorted(p)) for p in pairs}
        # p3 (non-core) connects to p1 and p2 (core)
        assert ("p1", "p3") in pair_set
        assert ("p2", "p3") in pair_set
        # Core pair also exists
        assert ("p1", "p2") in pair_set

    def test_non_core_not_connected_to_each_other(self):
        """Non-core staff (e.g. two in-betweeners) should not connect."""
        staff = {
            "p1": Role.DIRECTOR,           # core
            "p2": Role.IN_BETWEEN,          # non-core
            "p3": Role.IN_BETWEEN,          # non-core
        }
        pairs = generate_core_team_pairs(staff)
        pair_set = {tuple(sorted(p)) for p in pairs}
        # p2 ↔ p3 should NOT exist
        assert ("p2", "p3") not in pair_set
        # But both connect to core
        assert ("p1", "p2") in pair_set
        assert ("p1", "p3") in pair_set

    def test_no_core_team_fallback(self):
        """When no CORE_TEAM members exist, all pairs are generated."""
        staff = {
            "p1": Role.IN_BETWEEN,
            "p2": Role.IN_BETWEEN,
            "p3": Role.PRODUCER,
        }
        pairs = generate_core_team_pairs(staff)
        pair_set = {tuple(sorted(p)) for p in pairs}
        # Fallback: all 3 pairs
        assert len(pair_set) == 3
        assert ("p1", "p2") in pair_set
        assert ("p1", "p3") in pair_set
        assert ("p2", "p3") in pair_set

    def test_large_anime_no_staff_dropped(self):
        """300-person anime: all staff should exist as nodes in graph."""
        # 20 core + 280 non-core
        persons = []
        credits = []
        for i in range(20):
            pid = f"core_{i}"
            persons.append(Person(id=pid, name_en=f"Core {i}"))
            credits.append(Credit(person_id=pid, anime_id="a1", role=Role.KEY_ANIMATOR))
        for i in range(280):
            pid = f"noncore_{i}"
            persons.append(Person(id=pid, name_en=f"NonCore {i}"))
            credits.append(Credit(person_id=pid, anime_id="a1", role=Role.IN_BETWEEN))

        g = create_person_collaboration_network(persons, credits)
        # ALL 300 staff should be nodes
        assert g.number_of_nodes() == 300
        # Verify some non-core are connected to core (not dropped)
        assert g.has_edge("noncore_0", "core_0")

    def test_edge_count_is_linear(self):
        """Edge count should be O(n×k), not O(n²)."""
        persons = []
        credits = []
        n_core = 10
        n_non_core = 200
        for i in range(n_core):
            pid = f"core_{i}"
            persons.append(Person(id=pid, name_en=f"Core {i}"))
            credits.append(Credit(person_id=pid, anime_id="a1", role=Role.DIRECTOR))
        for i in range(n_non_core):
            pid = f"noncore_{i}"
            persons.append(Person(id=pid, name_en=f"NonCore {i}"))
            credits.append(Credit(person_id=pid, anime_id="a1", role=Role.IN_BETWEEN))

        g = create_person_collaboration_network(persons, credits)
        # Expected: core pairs + star edges = k*(k-1)/2 + n_non_core*k
        expected = n_core * (n_core - 1) // 2 + n_non_core * n_core
        assert g.number_of_edges() == expected
        # Should be much less than all-pairs
        all_pairs = (n_core + n_non_core) * (n_core + n_non_core - 1) // 2
        assert g.number_of_edges() < all_pairs * 0.2  # <20% of all-pairs
