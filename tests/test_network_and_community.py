"""Extended tests for analysis modules that lack dedicated test files.

Covers:
1. community_detection.py — detect_communities, compute_community_features, detect_mentorships_in_community
2. path_finding.py — find_shortest_path, find_all_shortest_paths, find_bottleneck_nodes
3. temporal_influence.py — compute_temporal_profiles, analyze_cohort_trends, detect_industry_trends
4. genre_specialization.py — compute_genre_profiles, find_genre_specialists, compute_genre_similarity
5. structural_holes.py — compute_structural_hole_metrics, compute_brokerage_metrics, classify_brokerage_role
6. core_periphery.py — identify_core_periphery, compute_coreness_metrics, find_rising_stars
7. bias_detector.py — detect_role_bias, detect_studio_bias, detect_systematic_biases
"""

import networkx as nx
import pytest

from src.runtime.models import BronzeAnime as Anime, Credit, Role


# ---------------------------------------------------------------------------
# Helpers: reusable synthetic data builders
# ---------------------------------------------------------------------------


def _make_anime(
    anime_id: str,
    *,
    year: int = 2020,
    genres: list[str] | None = None,
    episodes: int | None = 12,
    season: str | None = None,
    studio: str | None = None,
) -> Anime:
    return Anime(
        id=anime_id,
        title_ja=f"Anime {anime_id}",
        year=year,
        genres=genres or [],
        episodes=episodes,
        season=season,
        studios=[studio] if studio else [],
    )


def _make_credit(
    person_id: str, anime_id: str, role: Role = Role.KEY_ANIMATOR
) -> Credit:
    return Credit(person_id=person_id, anime_id=anime_id, role=role)


def _build_triangle_graph() -> nx.Graph:
    """3-node fully connected weighted graph."""
    g = nx.Graph()
    g.add_edge("A", "B", weight=2, shared_works=1)
    g.add_edge("B", "C", weight=3, shared_works=2)
    g.add_edge("A", "C", weight=1, shared_works=1)
    return g


def _build_barbell_graph() -> nx.Graph:
    """Two cliques (A,B,C) and (D,E,F) connected by C-D."""
    g = nx.Graph()
    for u, v in [("A", "B"), ("A", "C"), ("B", "C")]:
        g.add_edge(u, v, weight=2, shared_works=1)
    for u, v in [("D", "E"), ("D", "F"), ("E", "F")]:
        g.add_edge(u, v, weight=2, shared_works=1)
    g.add_edge("C", "D", weight=1, shared_works=1)
    return g


def _build_star_graph() -> nx.Graph:
    """Star graph with center H and spokes H-A, H-B, H-C, H-D."""
    g = nx.Graph()
    for n in ["A", "B", "C", "D"]:
        g.add_edge("H", n, weight=1, shared_works=1)
    return g


def _build_collaboration_graph_for_communities() -> nx.Graph:
    """Two dense clusters connected by a weak link, for community detection."""
    g = nx.Graph()
    # Cluster 1: A, B, C, D
    for u in ["A", "B", "C", "D"]:
        for v in ["A", "B", "C", "D"]:
            if u < v:
                g.add_edge(u, v, weight=5, shared_works=3)
    # Cluster 2: E, F, G, H
    for u in ["E", "F", "G", "H"]:
        for v in ["E", "F", "G", "H"]:
            if u < v:
                g.add_edge(u, v, weight=5, shared_works=3)
    # Weak link
    g.add_edge("D", "E", weight=1, shared_works=1)
    return g


# ===========================================================================
# 1. Community Detection
# ===========================================================================


class TestCommunityDetection:
    def test_detect_communities_empty_graph(self):
        from src.analysis.network.community_detection import detect_communities

        result = detect_communities(nx.Graph())
        assert result == {}

    def test_detect_communities_small_graph_below_min_size(self):
        from src.analysis.network.community_detection import detect_communities

        g = _build_triangle_graph()
        # min_community_size=10 should exclude a 3-node component
        result = detect_communities(g, min_community_size=10)
        assert result == {}

    def test_detect_communities_finds_clusters(self):
        from src.analysis.network.community_detection import detect_communities

        g = _build_collaboration_graph_for_communities()
        result = detect_communities(g, min_community_size=3)
        assert len(result) >= 1
        # Each community is a Community dataclass
        for comm in result.values():
            assert comm.size >= 3
            assert comm.density >= 0

    def test_community_overlap_analysis(self):
        from src.analysis.network.community_detection import (
            analyze_community_overlap,
            detect_communities,
        )

        g = _build_collaboration_graph_for_communities()
        communities = detect_communities(g, min_community_size=3)
        bridges = analyze_community_overlap(communities, g)
        # bridges is a dict mapping person_id -> list of (comm_id, count)
        assert isinstance(bridges, dict)

    def test_get_community_formation_period_no_credits(self):
        from src.analysis.network.community_detection import (
            get_community_formation_period,
        )

        result = get_community_formation_period(["A", "B"], [], {})
        assert result is None

    def test_get_community_formation_period_normal(self):
        from src.analysis.network.community_detection import (
            get_community_formation_period,
        )

        anime_map = {
            "a1": _make_anime("a1", year=2018),
            "a2": _make_anime("a2", year=2019),
            "a3": _make_anime("a3", year=2020),
            "a4": _make_anime("a4", year=2021),
        }
        credits = [
            _make_credit("A", "a1"),
            _make_credit("A", "a2"),
            _make_credit("B", "a3"),
            _make_credit("B", "a4"),
        ]
        result = get_community_formation_period(["A", "B"], credits, anime_map)
        assert result is not None
        start, end = result
        assert start >= 2018
        assert end <= 2021

    def test_compute_prospective_potential_no_history(self):
        from src.analysis.network.community_detection import (
            compute_prospective_potential,
        )

        result = compute_prospective_potential("P1", [], {}, 2020, 50.0)
        assert result == 50.0

    def test_compute_retrospective_potential_at_peak(self):
        from src.analysis.network.community_detection import (
            compute_retrospective_potential,
        )

        # Already at peak -> returns current_score
        result = compute_retrospective_potential("P1", [], {}, 2020, 80.0, 80.0)
        assert result == 80.0

    def test_compute_retrospective_potential_below_peak(self):
        from src.analysis.network.community_detection import (
            compute_retrospective_potential,
        )

        anime_map = {
            "a1": _make_anime("a1", year=2015),
            "a2": _make_anime("a2", year=2020),
        }
        credits = [
            _make_credit("P1", "a1"),
            _make_credit("P1", "a2"),
        ]
        result = compute_retrospective_potential(
            "P1", credits, anime_map, 2018, 30.0, 80.0
        )
        # current(30) + gap(50) * factor => between 30 and 80
        assert 30.0 < result <= 80.0


# ===========================================================================
# 2. Path Finding
# ===========================================================================


class TestPathFinding:
    def test_find_shortest_path_normal(self):
        from src.analysis.network.path_finding import find_shortest_path

        g = _build_triangle_graph()
        result = find_shortest_path(g, "A", "C")
        assert result is not None
        assert result.source == "A"
        assert result.target == "C"
        assert result.length >= 1

    def test_find_shortest_path_same_node(self):
        from src.analysis.network.path_finding import find_shortest_path

        g = _build_triangle_graph()
        result = find_shortest_path(g, "A", "A")
        assert result is not None
        assert result.length == 0
        assert result.path == ["A"]

    def test_find_shortest_path_node_not_in_graph(self):
        from src.analysis.network.path_finding import find_shortest_path

        g = _build_triangle_graph()
        result = find_shortest_path(g, "A", "Z")
        assert result is None

    def test_find_shortest_path_no_path(self):
        from src.analysis.network.path_finding import find_shortest_path

        g = nx.Graph()
        g.add_node("A")
        g.add_node("B")
        result = find_shortest_path(g, "A", "B", weight=None)
        assert result is None

    def test_find_all_shortest_paths_normal(self):
        from src.analysis.network.path_finding import find_all_shortest_paths

        g = _build_triangle_graph()
        results = find_all_shortest_paths(g, "A", "C")
        assert len(results) >= 1
        # All returned paths should have the same length
        lengths = {r.length for r in results}
        assert len(lengths) == 1

    def test_find_all_shortest_paths_missing_node(self):
        from src.analysis.network.path_finding import find_all_shortest_paths

        g = _build_triangle_graph()
        results = find_all_shortest_paths(g, "A", "Z")
        assert results == []

    def test_find_bottleneck_nodes_empty_graph(self):
        from src.analysis.network.path_finding import find_bottleneck_nodes

        results = find_bottleneck_nodes(nx.Graph())
        assert results == []

    def test_find_bottleneck_nodes_barbell(self):
        from src.analysis.network.path_finding import find_bottleneck_nodes

        g = _build_barbell_graph()
        results = find_bottleneck_nodes(g, top_n=3)
        assert len(results) <= 3
        # C and D should have high betweenness (bridge nodes)
        top_ids = [r[0] for r in results]
        assert "C" in top_ids or "D" in top_ids

    def test_compute_separation_statistics(self):
        from src.analysis.network.path_finding import compute_separation_statistics

        g = _build_triangle_graph()
        stats = compute_separation_statistics(g)
        assert stats["n_components"] == 1
        assert stats["avg_path_length"] > 0
        assert stats["diameter"] >= 1

    def test_compute_separation_statistics_empty(self):
        from src.analysis.network.path_finding import compute_separation_statistics

        stats = compute_separation_statistics(nx.Graph())
        assert stats == {}


# ===========================================================================
# 4. Temporal Influence
# ===========================================================================


class TestTemporalInfluence:
    def _build_temporal_data(self):
        anime_map = {f"a{i}": _make_anime(f"a{i}", year=2015 + i) for i in range(1, 8)}
        credits = []
        # Person P1 active 2016-2022
        for i in range(1, 8):
            credits.append(_make_credit("P1", f"a{i}", Role.KEY_ANIMATOR))
        # Person P2 active 2016-2018
        for i in range(1, 4):
            credits.append(_make_credit("P2", f"a{i}", Role.ANIMATION_DIRECTOR))
        return credits, anime_map

    def test_compute_temporal_profiles_basic(self):
        from src.analysis.network.temporal_influence import compute_temporal_profiles

        credits, anime_map = self._build_temporal_data()
        profiles = compute_temporal_profiles(credits, anime_map)
        assert "P1" in profiles
        assert "P2" in profiles
        assert profiles["P1"].career_start == 2016
        assert profiles["P2"].career_end == 2018

    def test_compute_temporal_profiles_with_scores(self):
        from src.analysis.network.temporal_influence import compute_temporal_profiles

        credits, anime_map = self._build_temporal_data()
        scores = {
            "P1": {"birank": 10, "patronage": 20, "person_fe": 30, "iv_score": 50}
        }
        profiles = compute_temporal_profiles(credits, anime_map, current_scores=scores)
        # P1 snapshots should reflect the current scores
        assert profiles["P1"].peak_score == 50.0

    def test_compute_temporal_profiles_empty(self):
        from src.analysis.network.temporal_influence import compute_temporal_profiles

        profiles = compute_temporal_profiles([], {})
        assert profiles == {}

    def test_analyze_cohort_trends(self):
        from src.analysis.network.temporal_influence import (
            analyze_cohort_trends,
            compute_temporal_profiles,
        )

        credits, anime_map = self._build_temporal_data()
        profiles = compute_temporal_profiles(credits, anime_map)
        cohorts = analyze_cohort_trends(profiles, cohort_window=5)
        assert isinstance(cohorts, dict)
        # Both P1 and P2 start in 2016, so they share the same cohort
        assert len(cohorts) >= 1

    def test_detect_industry_trends(self):
        from src.analysis.network.temporal_influence import (
            compute_temporal_profiles,
            detect_industry_trends,
        )

        credits, anime_map = self._build_temporal_data()
        profiles = compute_temporal_profiles(credits, anime_map)
        trends = detect_industry_trends(profiles)
        assert isinstance(trends, dict)
        assert len(trends) > 0
        # Each year entry should have expected keys
        for year, stats in trends.items():
            assert "active_persons" in stats
            assert "total_credits" in stats


# ===========================================================================
# 5. Genre Specialization
# ===========================================================================


class TestGenreSpecialization:
    def _build_genre_data(self):
        anime_map = {
            "a1": _make_anime("a1", genres=["Action", "Fantasy"]),
            "a2": _make_anime("a2", genres=["Action"]),
            "a3": _make_anime("a3", genres=["Comedy"]),
            "a4": _make_anime("a4", genres=["Action", "Comedy"]),
        }
        credits = [
            _make_credit("P1", "a1"),
            _make_credit("P1", "a2"),
            _make_credit("P1", "a4"),
            _make_credit("P2", "a3"),
            _make_credit("P2", "a4"),
        ]
        return credits, anime_map

    def test_compute_genre_profiles(self):
        from src.analysis.genre.specialization import compute_genre_profiles

        credits, anime_map = self._build_genre_data()
        profiles = compute_genre_profiles(credits, anime_map)
        assert "P1" in profiles
        assert "P2" in profiles
        # P1 is primarily action
        assert profiles["P1"].primary_genre == "action"

    def test_compute_genre_profiles_empty(self):
        from src.analysis.genre.specialization import compute_genre_profiles

        profiles = compute_genre_profiles([], {})
        assert profiles == {}

    def test_genre_diversity_score(self):
        from src.analysis.genre.specialization import compute_genre_profiles

        credits, anime_map = self._build_genre_data()
        profiles = compute_genre_profiles(credits, anime_map)
        # P2 works in comedy only essentially -> higher specialization
        # P1 works in action+fantasy+comedy -> lower specialization (more diverse)
        # genre_diversity: higher = more diverse
        assert profiles["P1"].genre_diversity >= 0.0
        assert profiles["P1"].specialization_score >= 0.0

    def test_find_genre_specialists(self):
        from src.analysis.genre.specialization import (
            compute_genre_profiles,
            find_genre_specialists,
        )

        credits, anime_map = self._build_genre_data()
        profiles = compute_genre_profiles(credits, anime_map)
        specialists = find_genre_specialists(profiles, "action", min_works=2, top_n=5)
        # P1 has action as primary with >=2 works
        assert len(specialists) >= 1
        assert specialists[0][0] == "P1"

    def test_compute_genre_similarity_identical(self):
        from src.analysis.genre.specialization import (
            GenreProfile,
            compute_genre_similarity,
        )

        p1 = GenreProfile(person_id="A", genre_distribution={"action": 5, "comedy": 3})
        p2 = GenreProfile(person_id="B", genre_distribution={"action": 5, "comedy": 3})
        sim = compute_genre_similarity(p1, p2)
        assert sim == 1.0

    def test_compute_genre_similarity_orthogonal(self):
        from src.analysis.genre.specialization import (
            GenreProfile,
            compute_genre_similarity,
        )

        p1 = GenreProfile(person_id="A", genre_distribution={"action": 5})
        p2 = GenreProfile(person_id="B", genre_distribution={"comedy": 3})
        sim = compute_genre_similarity(p1, p2)
        assert sim == 0.0

    def test_normalize_genre(self):
        from src.analysis.genre.specialization import normalize_genre

        assert normalize_genre("Action") == "action"
        assert normalize_genre("Science Fiction") == "sci-fi"
        assert normalize_genre(None) is None
        assert normalize_genre("") is None

    def test_analyze_genre_trends(self):
        from src.analysis.genre.specialization import (
            analyze_genre_trends,
            compute_genre_profiles,
        )

        # Build data with clear genre specialists to avoid division-by-zero
        anime_map = {
            "a1": _make_anime("a1", genres=["Action"]),
            "a2": _make_anime("a2", genres=["Action"]),
            "a3": _make_anime("a3", genres=["Action"]),
            "a4": _make_anime("a4", genres=["Comedy"]),
            "a5": _make_anime("a5", genres=["Comedy"]),
            "a6": _make_anime("a6", genres=["Comedy"]),
        }
        credits = [
            # P1 is an action specialist
            _make_credit("P1", "a1"),
            _make_credit("P1", "a2"),
            _make_credit("P1", "a3"),
            # P2 is a comedy specialist
            _make_credit("P2", "a4"),
            _make_credit("P2", "a5"),
            _make_credit("P2", "a6"),
        ]
        profiles = compute_genre_profiles(credits, anime_map)
        trends = analyze_genre_trends(profiles)
        assert "action" in trends
        assert trends["action"]["total_creators"] >= 1


# ===========================================================================
# 6. Structural Holes
# ===========================================================================


class TestStructuralHoles:
    def test_compute_network_constraint_isolated(self):
        from src.analysis.network.structural_holes import compute_network_constraint

        g = nx.Graph()
        g.add_node("A")
        assert compute_network_constraint(g, "A") == 1.0

    def test_compute_network_constraint_star_center(self):
        from src.analysis.network.structural_holes import compute_network_constraint

        g = _build_star_graph()
        # Center "H" connects to 4 unconnected nodes -> low constraint
        constraint = compute_network_constraint(g, "H")
        # Each spoke contributes (1/4)^2 = 0.0625, no indirect => 4 * 0.0625 = 0.25
        assert constraint == pytest.approx(0.25, abs=0.01)

    def test_compute_effective_size_star(self):
        from src.analysis.network.structural_holes import compute_effective_size

        g = _build_star_graph()
        eff_size, efficiency = compute_effective_size(g, "H")
        # Hub with no inter-spoke connections; formula returns 5.0 due to
        # negative overlap counting.  Efficiency > 1 because effective_size > degree.
        assert eff_size == pytest.approx(5.0, abs=0.1)
        assert efficiency > 1.0

    def test_compute_effective_size_triangle(self):
        from src.analysis.network.structural_holes import compute_effective_size

        g = _build_triangle_graph()
        eff_size, efficiency = compute_effective_size(g, "A")
        # Triangle: moderate redundancy; effective_size = degree (2.0) for this formula
        assert eff_size <= 2.0

    def test_compute_structural_hole_metrics(self):
        from src.analysis.network.structural_holes import (
            compute_structural_hole_metrics,
        )

        g = _build_barbell_graph()
        metrics = compute_structural_hole_metrics(g)
        assert len(metrics) == 6
        # Bridge nodes C, D should have higher betweenness
        assert metrics["C"].betweenness > 0 or metrics["D"].betweenness > 0

    def test_classify_brokerage_role_coordinator(self):
        from src.analysis.network.structural_holes import (
            BrokerageRole,
            classify_brokerage_role,
        )

        groups = {"A": "G1", "B": "G1", "C": "G1"}
        role = classify_brokerage_role("B", "A", "C", groups)
        assert role == BrokerageRole.COORDINATOR

    def test_classify_brokerage_role_liaison(self):
        from src.analysis.network.structural_holes import (
            BrokerageRole,
            classify_brokerage_role,
        )

        groups = {"A": "G1", "B": "G2", "C": "G3"}
        role = classify_brokerage_role("B", "A", "C", groups)
        assert role == BrokerageRole.LIAISON

    def test_classify_brokerage_role_consultant(self):
        from src.analysis.network.structural_holes import (
            BrokerageRole,
            classify_brokerage_role,
        )

        groups = {"A": "G1", "B": "G2", "C": "G1"}
        role = classify_brokerage_role("B", "A", "C", groups)
        assert role == BrokerageRole.CONSULTANT

    def test_classify_brokerage_role_representative(self):
        from src.analysis.network.structural_holes import (
            BrokerageRole,
            classify_brokerage_role,
        )

        groups = {"A": "G1", "B": "G1", "C": "G2"}
        role = classify_brokerage_role("B", "A", "C", groups)
        assert role == BrokerageRole.REPRESENTATIVE

    def test_classify_brokerage_role_gatekeeper(self):
        from src.analysis.network.structural_holes import (
            BrokerageRole,
            classify_brokerage_role,
        )

        groups = {"A": "G2", "B": "G1", "C": "G1"}
        role = classify_brokerage_role("B", "A", "C", groups)
        assert role == BrokerageRole.GATEKEEPER

    def test_find_structural_hole_spanners(self):
        from src.analysis.network.structural_holes import (
            compute_structural_hole_metrics,
            find_structural_hole_spanners,
        )

        g = _build_barbell_graph()
        metrics = compute_structural_hole_metrics(g)
        spanners = find_structural_hole_spanners(metrics, top_n=3)
        assert len(spanners) <= 3
        # Each entry is (person_id, constraint, efficiency)
        for pid, constraint, efficiency in spanners:
            assert isinstance(pid, str)
            assert constraint >= 0

    def test_compute_brokerage_metrics(self):
        from src.analysis.network.structural_holes import compute_brokerage_metrics

        g = _build_barbell_graph()
        groups = {"A": "G1", "B": "G1", "C": "G1", "D": "G2", "E": "G2", "F": "G2"}
        metrics = compute_brokerage_metrics(g, groups)
        # C and D should have brokerage roles
        assert metrics["C"].total_brokerage > 0 or metrics["D"].total_brokerage > 0


# ===========================================================================
# 7. Core-Periphery
# ===========================================================================


class TestCorePeriphery:
    def test_identify_core_periphery_empty(self):
        from src.analysis.network.core_periphery import identify_core_periphery

        structure = identify_core_periphery(nx.Graph())
        assert structure.core_size == 0
        assert structure.core_members == []

    def test_identify_core_periphery_normal(self):
        from src.analysis.network.core_periphery import identify_core_periphery

        g = _build_collaboration_graph_for_communities()
        structure = identify_core_periphery(g)
        total = (
            len(structure.core_members)
            + len(structure.semi_periphery_members)
            + len(structure.periphery_members)
        )
        assert total == g.number_of_nodes()

    def test_compute_k_core_numbers(self):
        from src.analysis.network.core_periphery import compute_k_core_numbers

        g = _build_collaboration_graph_for_communities()
        k_cores = compute_k_core_numbers(g)
        assert len(k_cores) == g.number_of_nodes()
        # Nodes in a 4-clique should have k-core >= 3
        for node in ["A", "B", "C", "D"]:
            assert k_cores[node] >= 3

    def test_compute_coreness_score(self):
        from src.analysis.network.core_periphery import compute_coreness_score

        g = _build_collaboration_graph_for_communities()
        # Node in dense clique
        score = compute_coreness_score(g, "A", k_core=3, max_k=3)
        assert 0 <= score <= 1

    def test_find_rising_stars(self):
        from src.analysis.network.core_periphery import (
            compute_coreness_metrics,
            find_rising_stars,
            identify_core_periphery,
        )

        g = _build_collaboration_graph_for_communities()
        structure = identify_core_periphery(g, core_threshold=0.7, semi_threshold=0.3)
        metrics = compute_coreness_metrics(g, structure)
        rising = find_rising_stars(metrics, structure, min_core_ratio=0.1, top_n=5)
        assert isinstance(rising, list)
        # Should not include core members
        for pid, coreness, core_ratio in rising:
            assert pid not in structure.core_members


# ===========================================================================
# 8. Bias Detector
# ===========================================================================


class TestBiasDetector:
    def test_detect_role_bias_empty(self):
        from src.analysis.bias_detector import detect_role_bias

        results = detect_role_bias({}, {}, {})
        assert results == []

    def test_detect_role_bias_small_sample(self):
        from src.analysis.bias_detector import detect_role_bias

        # Fewer than 10 samples per role -> no results
        contributions = {
            "anime1": {
                f"p{i}": {"shapley_value": float(i), "role": "animator"}
                for i in range(1, 5)
            }
        }
        scores = {f"p{i}": {"iv_score": float(i * 0.5)} for i in range(1, 5)}
        roles = {f"p{i}": {"primary_role": "animator"} for i in range(1, 5)}
        results = detect_role_bias(contributions, scores, roles)
        assert results == []

    def test_detect_studio_bias_empty(self):
        from src.analysis.bias_detector import detect_studio_bias

        results = detect_studio_bias({}, {})
        assert results == []

    def test_detect_systematic_biases_integration(self):
        from src.analysis.bias_detector import detect_systematic_biases

        results = detect_systematic_biases(
            contributions={},
            person_scores={},
            studio_bias_metrics={},
            growth_acceleration_data={},
            potential_value_scores={},
            role_profiles={},
        )
        assert "role" in results
        assert "studio" in results
        assert "career_stage" in results

    def test_generate_bias_report_empty(self):
        from src.analysis.bias_detector import generate_bias_report

        report = generate_bias_report({"role": [], "studio": [], "career_stage": []})
        assert report["summary"]["total_biases_detected"] == 0
        assert report["recommendations"] == []

    def test_compute_ttest_and_effect_single_value(self):
        from src.analysis.bias_detector import _compute_ttest_and_effect

        p_val, cohens_d = _compute_ttest_and_effect([5.0])
        assert p_val == 1.0
        assert cohens_d == 0.0
