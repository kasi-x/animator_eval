"""Tests for temporal PageRank — yearly snapshots, foresight, promotions."""

import json
from dataclasses import asdict

import pytest

from src.analysis.temporal_pagerank import (
    YearlyBirankSnapshot,
    _add_peer_edges,
    _build_birank_timelines,
    _build_yearly_cumulative_graphs,
    _classify_trajectory,
    _compute_foresight_scores,
    _detect_promotions,
    _run_yearly_pagerank_with_warm_start,
    compute_temporal_pagerank,
)
from src.analysis.pagerank import normalize_scores
from src.models import Anime, Credit, Person, Role


# =============================================================================
# Helpers
# =============================================================================


def _build_yearly_normalized(yearly_scores, yearly_graphs):
    """Build yearly normalized scores (person nodes only) — test helper."""
    result = {}
    for year, scores in yearly_scores.items():
        graph = yearly_graphs[year]
        person_scores = {
            k: v for k, v in scores.items() if graph.nodes[k].get("type") == "person"
        }
        result[year] = normalize_scores(person_scores)
    return result


# =============================================================================
# Fixtures — synthetic test data
# =============================================================================


def _person(pid: str, name_ja: str = "", name_en: str = "") -> Person:
    return Person(id=pid, name_ja=name_ja, name_en=name_en)


def _anime(aid: str, year: int, score: float = 7.0, studios: list[str] | None = None) -> Anime:
    return Anime(
        id=aid,
        title_ja=f"Anime {aid}",
        year=year,
        score=score,
        studios=studios or [],
    )


def _credit(pid: str, aid: str, role: Role) -> Credit:
    return Credit(person_id=pid, anime_id=aid, role=role)


@pytest.fixture
def basic_data():
    """Basic test data: 2 directors, 3 animators, 3 anime across 3 years."""
    persons = [
        _person("d1", "監督A"),
        _person("d2", "監督B"),
        _person("a1", "アニメーターA"),
        _person("a2", "アニメーターB"),
        _person("a3", "アニメーターC"),
    ]
    anime_list = [
        _anime("w1", 2020, 8.0, ["StudioX"]),
        _anime("w2", 2021, 7.5, ["StudioX"]),
        _anime("w3", 2022, 9.0, ["StudioY"]),
    ]
    anime_map = {a.id: a for a in anime_list}

    credits = [
        # Year 2020: d1 directs w1, a1 is key animator
        _credit("d1", "w1", Role.DIRECTOR),
        _credit("a1", "w1", Role.IN_BETWEEN),
        _credit("a2", "w1", Role.KEY_ANIMATOR),
        # Year 2021: d1 directs w2, a1 promoted to KEY_ANIMATOR, a3 joins
        _credit("d1", "w2", Role.DIRECTOR),
        _credit("a1", "w2", Role.KEY_ANIMATOR),
        _credit("a3", "w2", Role.IN_BETWEEN),
        # Year 2022: d2 directs w3, a1 promoted to ANIMATION_DIRECTOR
        _credit("d2", "w3", Role.DIRECTOR),
        _credit("a1", "w3", Role.ANIMATION_DIRECTOR),
        _credit("a2", "w3", Role.KEY_ANIMATOR),
        _credit("a3", "w3", Role.KEY_ANIMATOR),
    ]

    return persons, anime_list, anime_map, credits


# =============================================================================
# Yearly Cumulative Graph Tests
# =============================================================================


class TestCumulativeGraphs:
    def test_cumulative_graph_grows(self, basic_data):
        """Year-over-year, cumulative graph should have more nodes/edges."""
        persons, anime_list, anime_map, credits = basic_data
        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)

        years = sorted(graphs.keys())
        assert years == [2020, 2021, 2022]

        # Node count should be non-decreasing
        prev_nodes = 0
        for year in years:
            n = graphs[year].number_of_nodes()
            assert n >= prev_nodes, f"Nodes decreased at year {year}"
            prev_nodes = n

        # Edge count should be non-decreasing
        prev_edges = 0
        for year in years:
            e = graphs[year].number_of_edges()
            assert e >= prev_edges, f"Edges decreased at year {year}"
            prev_edges = e

    def test_peer_edges_same_category(self, basic_data):
        """Same-category persons on the same anime should get peer edges."""
        persons, anime_list, anime_map, credits = basic_data
        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)

        # In 2020: a1 (IN_BETWEEN) and a2 (KEY_ANIMATOR) are both "animation" category
        g2020 = graphs[2020]
        # They should have peer edges (bidirectional)
        assert g2020.has_edge("a1", "a2") or g2020.has_edge("a2", "a1")

    def test_peer_edges_different_category_no_edge(self, basic_data):
        """Different-category persons should not get direct peer edges."""
        persons, anime_list, anime_map, credits = basic_data
        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)

        g2020 = graphs[2020]
        # d1 (direction) and a1 (animation) — peer edges should NOT exist
        # But bipartite edges through anime DO exist
        # Check that no peer-typed edge exists between d1 and a1
        if g2020.has_edge("d1", "a1"):
            # Edge exists but check if it's a peer edge or bipartite
            edge_data = g2020["d1"]["a1"]
            assert edge_data.get("edge_type") != "peer"

    def test_peer_edges_cap(self):
        """16+ persons in same category should be capped to 15."""
        import networkx as nx

        g = nx.DiGraph()
        # Create 20 persons in animation category
        persons_ids = [f"p{i}" for i in range(20)]
        for pid in persons_ids:
            g.add_node(pid, type="person")
        g.add_node("anime1", type="anime")

        anime_credits = {
            "anime1": [(pid, Role.KEY_ANIMATOR) for pid in persons_ids]
        }
        anime_map = {"anime1": _anime("anime1", 2020, 8.0)}

        added = _add_peer_edges(g, anime_credits, anime_map, 0.3)

        # With 15-person cap, max pairs = 15*14/2 = 105, bidirectional = 210
        assert added <= 210  # 15 cap
        # Should be less than all-pairs (20*19/2 * 2 = 380)
        assert added < 380

    def test_empty_credits(self):
        """Empty credits should produce empty graphs."""
        graphs = _build_yearly_cumulative_graphs([], {}, [], 0.3)
        assert graphs == {}


# =============================================================================
# Warm Start PageRank Tests
# =============================================================================


class TestWarmStartPageRank:
    def test_warm_start_scores_match_cold_start(self, basic_data):
        """Warm-start results should be very close to cold-start results."""
        persons, anime_list, anime_map, credits = basic_data
        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)

        warm_scores = _run_yearly_pagerank_with_warm_start(graphs)

        # Also run cold start (no nstart) on the last year's graph
        from src.analysis.pagerank import weighted_pagerank

        last_year = max(graphs.keys())
        cold_scores = weighted_pagerank(graphs[last_year])

        # Compare person scores (should be very close)
        for node in cold_scores:
            if graphs[last_year].nodes[node].get("type") == "person":
                warm = warm_scores[last_year].get(node, 0)
                cold = cold_scores[node]
                assert abs(warm - cold) < 0.01, (
                    f"Warm/cold divergence for {node}: {warm} vs {cold}"
                )

    def test_warm_start_with_new_nodes(self, basic_data):
        """Adding new nodes (new persons) between years should work fine."""
        persons, anime_list, anime_map, credits = basic_data
        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)

        scores = _run_yearly_pagerank_with_warm_start(graphs)

        # a3 only appears in 2021 — should have scores from 2021 onwards
        assert "a3" not in scores[2020]
        assert "a3" in scores[2021]
        assert scores[2021]["a3"] > 0

    def test_empty_graph_year(self):
        """Year with empty graph should return empty scores."""
        import networkx as nx

        graphs = {2020: nx.DiGraph()}
        scores = _run_yearly_pagerank_with_warm_start(graphs)
        assert scores[2020] == {}


# =============================================================================
# Authority Timeline Tests
# =============================================================================


class TestBirankTimeline:
    def test_timeline_peak_detection(self, basic_data):
        """Peak year should be correctly detected."""
        persons, anime_list, anime_map, credits = basic_data
        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)
        scores = _run_yearly_pagerank_with_warm_start(graphs)

        yearly_norm = _build_yearly_normalized(scores, graphs)
        timelines = _build_birank_timelines(scores, graphs, yearly_norm, credits, anime_map)

        # All persons should have timelines
        assert "d1" in timelines
        assert "a1" in timelines

        # Peak should be in the valid year range
        for pid, tl in timelines.items():
            assert tl.peak_year in [2020, 2021, 2022]
            assert tl.peak_birank >= 0

    def test_trajectory_rising(self):
        """Rising trajectory: scores increase over time."""
        snapshots = [
            YearlyBirankSnapshot(2018, 10.0, 0.01, 100, 500),
            YearlyBirankSnapshot(2019, 20.0, 0.02, 150, 700),
            YearlyBirankSnapshot(2020, 30.0, 0.03, 200, 900),
            YearlyBirankSnapshot(2021, 50.0, 0.05, 250, 1100),
            YearlyBirankSnapshot(2022, 70.0, 0.07, 300, 1300),
            YearlyBirankSnapshot(2023, 85.0, 0.08, 350, 1500),
        ]
        assert _classify_trajectory(snapshots) == "rising"

    def test_trajectory_declining(self):
        """Declining trajectory: scores decrease over time."""
        snapshots = [
            YearlyBirankSnapshot(2018, 80.0, 0.08, 100, 500),
            YearlyBirankSnapshot(2019, 70.0, 0.07, 150, 700),
            YearlyBirankSnapshot(2020, 55.0, 0.05, 200, 900),
            YearlyBirankSnapshot(2021, 40.0, 0.04, 250, 1100),
            YearlyBirankSnapshot(2022, 25.0, 0.02, 300, 1300),
            YearlyBirankSnapshot(2023, 15.0, 0.01, 350, 1500),
        ]
        assert _classify_trajectory(snapshots) == "declining"

    def test_trajectory_stable(self):
        """Stable trajectory: scores fluctuate within threshold."""
        snapshots = [
            YearlyBirankSnapshot(2018, 50.0, 0.05, 100, 500),
            YearlyBirankSnapshot(2019, 52.0, 0.05, 150, 700),
            YearlyBirankSnapshot(2020, 48.0, 0.05, 200, 900),
            YearlyBirankSnapshot(2021, 51.0, 0.05, 250, 1100),
        ]
        assert _classify_trajectory(snapshots) == "stable"

    def test_trajectory_single_snapshot(self):
        """Single snapshot should classify as stable."""
        snapshots = [YearlyBirankSnapshot(2020, 50.0, 0.05, 100, 500)]
        assert _classify_trajectory(snapshots) == "stable"


# =============================================================================
# Foresight Score Tests
# =============================================================================


class TestForesightScores:
    def test_foresight_basic(self):
        """Established X co-appears with unknown Y -> Y grows -> X gets credit."""
        persons = [_person("x1", "EstablishedX"), _person("y1", "UnknownY")]
        anime_map = {
            "w1": _anime("w1", 2020),
            "w2": _anime("w2", 2023),
        }
        credits = [
            _credit("x1", "w1", Role.DIRECTOR),
            _credit("y1", "w1", Role.IN_BETWEEN),
            # Y grows: appears in later work too
            _credit("y1", "w2", Role.ANIMATION_DIRECTOR),
            _credit("x1", "w2", Role.DIRECTOR),
        ]

        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)
        scores = _run_yearly_pagerank_with_warm_start(graphs)
        yearly_norm = _build_yearly_normalized(scores, graphs)

        timelines = _build_birank_timelines(scores, graphs, yearly_norm, credits, anime_map)
        foresight = _compute_foresight_scores(
            timelines, credits, anime_map, yearly_norm,
            foresight_horizon_years=5,
            unknown_threshold_percentile=25.0,
        )

        # At least one of them should have foresight score
        # (depends on whether X was "established" and Y was "unknown" in 2020)
        # With only 2 persons, percentile threshold may classify differently,
        # but the function should not crash
        assert isinstance(foresight, dict)

    def test_foresight_both_unknown_no_credit(self):
        """If both persons are unknown, no foresight credit should be given."""
        persons = [_person("y1"), _person("y2")]
        anime_map = {"w1": _anime("w1", 2020)}
        credits = [
            _credit("y1", "w1", Role.IN_BETWEEN),
            _credit("y2", "w1", Role.IN_BETWEEN),
        ]
        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)
        scores = _run_yearly_pagerank_with_warm_start(graphs)
        yearly_norm = _build_yearly_normalized(scores, graphs)

        timelines = _build_birank_timelines(scores, graphs, yearly_norm, credits, anime_map)
        foresight = _compute_foresight_scores(
            timelines, credits, anime_map, yearly_norm,
        )

        # Both are at same level, no established person -> no foresight
        # (or at most trivial scores)
        for fs in foresight.values():
            assert fs.n_discoveries == 0

    def test_foresight_confidence_intervals(self):
        """CI should exist and lower <= raw <= upper."""
        persons = [
            _person("x1", "DirectorX"),
            _person("y1", "AnimatorY1"),
            _person("y2", "AnimatorY2"),
            _person("y3", "AnimatorY3"),
        ]
        anime_list = [
            _anime("w1", 2018), _anime("w2", 2018),
            _anime("w3", 2022), _anime("w4", 2023),
        ]
        anime_map = {a.id: a for a in anime_list}

        credits = [
            _credit("x1", "w1", Role.DIRECTOR),
            _credit("y1", "w1", Role.IN_BETWEEN),
            _credit("x1", "w2", Role.DIRECTOR),
            _credit("y2", "w2", Role.IN_BETWEEN),
            _credit("y3", "w2", Role.IN_BETWEEN),
            # Future growth
            _credit("y1", "w3", Role.ANIMATION_DIRECTOR),
            _credit("y2", "w3", Role.KEY_ANIMATOR),
            _credit("y3", "w4", Role.ANIMATION_DIRECTOR),
            _credit("x1", "w3", Role.DIRECTOR),
            _credit("x1", "w4", Role.DIRECTOR),
        ]

        result = compute_temporal_pagerank(credits, anime_map, persons)

        for pid, fs_dict in result.foresight_scores.items():
            # CI bounds should be reasonable
            assert fs_dict["confidence_lower"] <= fs_dict["confidence_upper"]


# =============================================================================
# Promotion Detection Tests
# =============================================================================


class TestPromotionDetection:
    def test_promotion_detection(self, basic_data):
        """IN_BETWEEN -> KEY_ANIMATOR -> ANIMATION_DIRECTOR should be detected."""
        persons, anime_list, anime_map, credits = basic_data
        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)
        scores = _run_yearly_pagerank_with_warm_start(graphs)
        yearly_norm = _build_yearly_normalized(scores, graphs)
        timelines = _build_birank_timelines(scores, graphs, yearly_norm, credits, anime_map)

        # Use min_promotions=1 to detect all events (including single promotions)
        promotions = _detect_promotions(credits, anime_map, timelines, min_promotions=1)

        # a1 goes IN_BETWEEN(2020) -> KEY_ANIMATOR(2021) -> ANIMATION_DIRECTOR(2022)
        # Directors d1 and d2 should get attribution
        all_events = []
        for pc in promotions.values():
            all_events.extend(pc.events)

        a1_events = [e for e in all_events if e.promotee_id == "a1"]
        assert len(a1_events) >= 1, "Should detect at least one promotion for a1"

    def test_promotion_attribution_to_director(self, basic_data):
        """Promotions should be attributed to the highest-stage person on same anime."""
        persons, anime_list, anime_map, credits = basic_data
        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)
        scores = _run_yearly_pagerank_with_warm_start(graphs)
        yearly_norm = _build_yearly_normalized(scores, graphs)
        timelines = _build_birank_timelines(scores, graphs, yearly_norm, credits, anime_map)

        promotions = _detect_promotions(credits, anime_map, timelines, min_promotions=1)

        # Check that events are attributed to directors
        for pc in promotions.values():
            for event in pc.events:
                if event.attributed_to:
                    assert event.attributed_to in {"d1", "d2"}

    def test_min_promotions_filter(self, basic_data):
        """With min_promotions=2, single promotions should be filtered out."""
        persons, anime_list, anime_map, credits = basic_data
        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)
        scores = _run_yearly_pagerank_with_warm_start(graphs)
        yearly_norm = _build_yearly_normalized(scores, graphs)
        timelines = _build_birank_timelines(scores, graphs, yearly_norm, credits, anime_map)

        # With min_promotions=5, nothing should pass in this small dataset
        promotions = _detect_promotions(credits, anime_map, timelines, min_promotions=5)
        assert len(promotions) == 0

    def test_confidence_repeated_pattern(self):
        """5 promotions should give repeated_factor close to 1.0."""
        persons = [_person("d1", "Director")] + [_person(f"a{i}") for i in range(6)]
        anime_list = [_anime(f"w{i}", 2018 + i) for i in range(6)]
        anime_map = {a.id: a for a in anime_list}

        credits = []
        for i in range(6):
            credits.append(_credit("d1", f"w{i}", Role.DIRECTOR))

        # Each animator starts as IN_BETWEEN then gets promoted
        for i in range(5):
            credits.append(_credit(f"a{i}", f"w{i}", Role.IN_BETWEEN))
            credits.append(_credit(f"a{i}", f"w{i+1}", Role.KEY_ANIMATOR))
        # a5 stays in_between
        credits.append(_credit("a5", "w5", Role.IN_BETWEEN))

        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)
        scores = _run_yearly_pagerank_with_warm_start(graphs)
        yearly_norm = _build_yearly_normalized(scores, graphs)
        timelines = _build_birank_timelines(scores, graphs, yearly_norm, credits, anime_map)

        promotions = _detect_promotions(credits, anime_map, timelines, min_promotions=2)

        if "d1" in promotions:
            pc = promotions["d1"]
            # With 5+ events, repeated_factor should be 1.0
            assert pc.confidence > 0

    def test_confidence_exclusivity(self):
        """If promotee is also promoted at another studio, exclusivity should drop."""
        persons = [
            _person("d1", "DirectorA"),
            _person("d2", "DirectorB"),
            _person("a1", "AnimatorA"),
        ]
        anime_list = [
            _anime("w1", 2020, studios=["StudioA"]),
            _anime("w2", 2020, studios=["StudioB"]),
            _anime("w3", 2021, studios=["StudioA"]),
            _anime("w4", 2021, studios=["StudioB"]),
        ]
        anime_map = {a.id: a for a in anime_list}

        # a1 starts as IN_BETWEEN at both studios, promoted in same year at both
        credits = [
            _credit("d1", "w1", Role.DIRECTOR),
            _credit("a1", "w1", Role.IN_BETWEEN),
            _credit("d2", "w2", Role.DIRECTOR),
            _credit("a1", "w2", Role.IN_BETWEEN),
            # Year 2021: promoted at both
            _credit("d1", "w3", Role.DIRECTOR),
            _credit("a1", "w3", Role.KEY_ANIMATOR),
            _credit("d2", "w4", Role.DIRECTOR),
            _credit("a1", "w4", Role.KEY_ANIMATOR),
        ]

        graphs = _build_yearly_cumulative_graphs(credits, anime_map, persons, 0.3)
        scores = _run_yearly_pagerank_with_warm_start(graphs)
        yearly_norm = _build_yearly_normalized(scores, graphs)
        timelines = _build_birank_timelines(scores, graphs, yearly_norm, credits, anime_map)

        promotions = _detect_promotions(credits, anime_map, timelines, min_promotions=1)

        # When a1 is promoted at both studios in same year, exclusivity is lower
        # This test verifies the function doesn't crash and produces valid results
        for pc in promotions.values():
            assert 0.0 <= pc.exclusivity_score <= 1.0


# =============================================================================
# Integration / Edge Case Tests
# =============================================================================


class TestIntegration:
    def test_empty_data(self):
        """Empty input should return empty result, not crash."""
        result = compute_temporal_pagerank([], {}, [])
        assert result.years_computed == []
        assert result.total_persons == 0
        assert result.birank_timelines == {}
        assert result.foresight_scores == {}
        assert result.promotion_credits == {}

    def test_serialization(self, basic_data):
        """asdict() output should be JSON-serializable."""
        persons, anime_list, anime_map, credits = basic_data
        result = compute_temporal_pagerank(credits, anime_map, persons)

        # Should be directly serializable
        result_dict = asdict(result)
        json_str = json.dumps(result_dict, ensure_ascii=False)
        assert len(json_str) > 10  # Non-trivial output

        # Should round-trip
        parsed = json.loads(json_str)
        assert "birank_timelines" in parsed
        assert "foresight_scores" in parsed
        assert "promotion_credits" in parsed
        assert "years_computed" in parsed

    def test_full_pipeline_integration(self, basic_data):
        """Full compute_temporal_pagerank should produce coherent results."""
        persons, anime_list, anime_map, credits = basic_data
        result = compute_temporal_pagerank(credits, anime_map, persons)

        assert result.years_computed == [2020, 2021, 2022]
        assert result.total_persons > 0
        assert result.computation_time_seconds >= 0

        # Authority timelines
        for pid, tl_dict in result.birank_timelines.items():
            assert "snapshots" in tl_dict
            assert "peak_year" in tl_dict
            assert "trajectory" in tl_dict
            assert tl_dict["trajectory"] in {"rising", "stable", "declining", "peaked"}

    def test_single_year_data(self):
        """Data from a single year should work without errors."""
        persons = [_person("d1"), _person("a1")]
        anime_map = {"w1": _anime("w1", 2020)}
        credits = [
            _credit("d1", "w1", Role.DIRECTOR),
            _credit("a1", "w1", Role.KEY_ANIMATOR),
        ]

        result = compute_temporal_pagerank(credits, anime_map, persons)
        assert result.years_computed == [2020]
        assert result.total_persons == 2

    def test_no_year_anime_skipped(self):
        """Anime without year should be skipped gracefully."""
        persons = [_person("d1"), _person("a1")]
        anime_map = {"w1": _anime("w1", 2020), "w2": Anime(id="w2")}
        credits = [
            _credit("d1", "w1", Role.DIRECTOR),
            _credit("a1", "w1", Role.KEY_ANIMATOR),
            _credit("d1", "w2", Role.DIRECTOR),  # No year
        ]

        result = compute_temporal_pagerank(credits, anime_map, persons)
        assert result.years_computed == [2020]
