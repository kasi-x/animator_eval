"""Tests for src/analysis/network/resilience."""

from __future__ import annotations

import networkx as nx
import pytest

from src.analysis.network.resilience import (
    CriticalNode,
    ResilienceCurve,
    StrategyComparison,
    compare_strategies,
    find_critical_nodes,
    largest_connected_component_size,
    mean_eigenvector_authority,
    pair_connectivity,
    removal_order_by_attribute,
    removal_order_by_degree,
    removal_order_random,
    resilience_auc,
    simulate_resilience,
)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_lcc_empty(self):
        assert largest_connected_component_size(nx.Graph()) == 0

    def test_lcc_singleton(self):
        g = nx.Graph(); g.add_node(1)
        assert largest_connected_component_size(g) == 1

    def test_lcc_complete(self):
        g = nx.complete_graph(5)
        assert largest_connected_component_size(g) == 5

    def test_lcc_two_components(self):
        g = nx.Graph()
        g.add_edges_from([(0, 1), (2, 3), (3, 4)])
        assert largest_connected_component_size(g) == 3

    def test_pair_connectivity_complete(self):
        g = nx.complete_graph(5)
        assert pair_connectivity(g) == 10

    def test_pair_connectivity_split(self):
        g = nx.Graph()
        g.add_edges_from([(0, 1), (1, 2)])
        g.add_node(99)
        # component sizes 3 + 1 → 3*2/2 + 0 = 3
        assert pair_connectivity(g) == 3

    def test_pair_connectivity_empty(self):
        assert pair_connectivity(nx.Graph()) == 0

    def test_mean_authority_empty(self):
        assert mean_eigenvector_authority(nx.Graph()) == 0.0

    def test_mean_authority_returns_finite(self):
        g = nx.complete_graph(6)
        v = mean_eigenvector_authority(g)
        assert 0.0 < v < 1.0


# ---------------------------------------------------------------------------
# Removal orders
# ---------------------------------------------------------------------------


class TestRemovalOrders:
    def test_random_returns_all_nodes(self):
        g = nx.complete_graph(10)
        order = removal_order_random(g, rng_seed=1)
        assert len(order) == 10
        assert set(order) == set(g.nodes())

    def test_random_is_deterministic_with_seed(self):
        g = nx.complete_graph(10)
        a = removal_order_random(g, rng_seed=7)
        b = removal_order_random(g, rng_seed=7)
        assert a == b

    def test_random_k_limit(self):
        g = nx.complete_graph(10)
        order = removal_order_random(g, rng_seed=3, k=4)
        assert len(order) == 4

    def test_degree_order_descending(self):
        g = nx.Graph()
        g.add_edges_from([(0, 1), (0, 2), (0, 3), (1, 2)])
        order = removal_order_by_degree(g)
        # 0 has degree 3, 1 and 2 have 2, 3 has 1
        assert order[0] == 0
        assert order[-1] == 3

    def test_by_attribute_descending(self):
        g = nx.Graph()
        g.add_node("a", bridge_score=5.0)
        g.add_node("b", bridge_score=10.0)
        g.add_node("c", bridge_score=1.0)
        order = removal_order_by_attribute(g, "bridge_score")
        assert order == ["b", "a", "c"]

    def test_by_attribute_missing_to_last(self):
        g = nx.Graph()
        g.add_node("a", bridge_score=5.0)
        g.add_node("b")  # no attribute
        order = removal_order_by_attribute(g, "bridge_score")
        assert order == ["a", "b"]


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------


class TestSimulation:
    def test_empty_graph_returns_empty_curve(self):
        curve = simulate_resilience(nx.Graph(), [])
        assert curve.n_initial == 0
        assert curve.steps == ()

    def test_baseline_step_zero_included(self):
        g = nx.complete_graph(5)
        curve = simulate_resilience(g, [], strategy_name="empty")
        assert len(curve.steps) == 1
        assert curve.steps[0].step == 0
        assert curve.steps[0].n_remaining == 5

    def test_full_removal_terminates_at_zero(self):
        g = nx.path_graph(5)
        order = list(range(5))
        curve = simulate_resilience(g, order, strategy_name="full")
        last = curve.steps[-1]
        assert last.n_remaining == 0
        assert last.lcc_size == 0

    def test_targeted_attack_more_fragile_than_random(self):
        # Star graph: removing the hub disconnects everything
        g = nx.star_graph(20)  # node 0 is hub
        deg_order = removal_order_by_degree(g)
        rand_order = removal_order_random(g, rng_seed=1)
        deg_curve = simulate_resilience(g, deg_order, strategy_name="deg")
        rand_curve = simulate_resilience(g, rand_order, strategy_name="rand")
        # After 1 removal: targeted (hub) disconnects everything; random rarely picks hub.
        deg_after_1 = deg_curve.steps[1].lcc_size
        rand_after_1 = rand_curve.steps[1].lcc_size
        assert deg_after_1 < rand_after_1

    def test_ratio_curves_start_at_1(self):
        g = nx.complete_graph(5)
        curve = simulate_resilience(g, [0, 1, 2, 3, 4], strategy_name="all")
        assert curve.lcc_ratio_curve[0] == 1.0
        assert curve.pcc_ratio_curve[0] == 1.0


# ---------------------------------------------------------------------------
# AUC
# ---------------------------------------------------------------------------


class TestAUC:
    def test_empty_curve_auc_zero(self):
        curve = ResilienceCurve(strategy="empty", n_initial=0, steps=())
        assert resilience_auc(curve) == 0.0

    def test_unknown_metric_raises(self):
        g = nx.complete_graph(5)
        curve = simulate_resilience(g, [0, 1, 2], strategy_name="x")
        with pytest.raises(ValueError):
            resilience_auc(curve, metric="bogus")

    def test_robust_graph_high_auc_lcc(self):
        # Complete graph stays connected longer than star
        g_complete = nx.complete_graph(10)
        g_star = nx.star_graph(9)
        c_comp = simulate_resilience(g_complete, removal_order_by_degree(g_complete))
        c_star = simulate_resilience(g_star, removal_order_by_degree(g_star))
        assert resilience_auc(c_comp, metric="lcc") > resilience_auc(c_star, metric="lcc")


# ---------------------------------------------------------------------------
# Critical nodes
# ---------------------------------------------------------------------------


class TestCriticalNodes:
    def test_star_hub_is_most_critical(self):
        g = nx.star_graph(20)  # node 0 is hub
        results = find_critical_nodes(g, top_k=3, score_metric="pcc")
        assert results[0].node_id == "0"
        # Hub drop = full disconnection: large pcc_drop
        assert results[0].pcc_drop_ratio > 0.5

    def test_leaf_node_not_critical(self):
        g = nx.star_graph(20)
        results = find_critical_nodes(g, top_k=5, score_metric="lcc")
        # Hub (node 0) > leaves
        assert results[0].node_id == "0"
        # Leaves: lcc drop = 1 (just the removed node)
        leaf_results = [r for r in results if r.node_id != "0"]
        for r in leaf_results:
            assert r.lcc_drop <= 1.0

    def test_top_k_limit(self):
        g = nx.complete_graph(10)
        results = find_critical_nodes(g, top_k=3)
        assert len(results) == 3

    def test_candidates_subset(self):
        g = nx.complete_graph(10)
        results = find_critical_nodes(g, candidates=[0, 1], top_k=5)
        assert len(results) == 2
        assert {r.node_id for r in results} == {"0", "1"}


# ---------------------------------------------------------------------------
# Strategy comparison
# ---------------------------------------------------------------------------


class TestStrategyComparison:
    def test_complete_graph_low_fragility(self):
        # Complete graph: random ≈ targeted (all nodes equivalent)
        g = nx.complete_graph(10)
        cmp_ = compare_strategies(g, bridge_attribute=None, rng_seed=1)
        # rel_fragility small
        assert abs(cmp_.relative_fragility) < 0.2

    def test_star_graph_high_fragility(self):
        g = nx.star_graph(30)
        cmp_ = compare_strategies(g, bridge_attribute=None, rng_seed=2)
        # targeted (hub) >> random in destructiveness
        assert cmp_.relative_fragility > 0.2

    def test_bridge_auc_computed_when_attribute_present(self):
        g = nx.path_graph(5)
        for i, n in enumerate(g.nodes()):
            g.nodes[n]["bridge_score"] = float(i)
        cmp_ = compare_strategies(g, bridge_attribute="bridge_score", rng_seed=3)
        assert cmp_.bridge_auc is not None

    def test_bridge_auc_skipped_when_attribute_absent(self):
        g = nx.complete_graph(5)
        cmp_ = compare_strategies(g, bridge_attribute="bridge_score", rng_seed=4)
        assert cmp_.bridge_auc is None

    def test_interpretation_label(self):
        g = nx.complete_graph(10)
        cmp_ = compare_strategies(g, bridge_attribute=None, rng_seed=5)
        assert isinstance(cmp_.interpretation, str)
        assert cmp_.interpretation
