"""Hamilton H-3 tests — Phase 1-4 loading, validation, entity resolution, graph nodes.

Validates:
1. DAG builds from loading + resolution modules.
2. All declared node names exist in the DAG.
3. Node function signatures are correct (right input names for chaining).
4. Full Phase 1-9 DAG is buildable.
"""

from __future__ import annotations

import inspect


# ---------------------------------------------------------------------------
# DAG construction
# ---------------------------------------------------------------------------


class TestPhase14DagBuilds:
    def test_loading_dag_builds(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import loading

        dr = driver.Builder().with_modules(loading).build()
        assert dr is not None

    def test_resolution_dag_builds(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import resolution

        dr = driver.Builder().with_modules(resolution).build()
        assert dr is not None

    def test_full_phase1_9_dag_builds(self):
        """Full Phase 1-9 DAG: loading → resolution → scoring → metrics → assembly → analysis."""
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import (
            assembly, causal, core, genre, loading, metrics,
            network, resolution, scoring, studio,
        )

        dr = driver.Builder().with_modules(
            loading, resolution, scoring, metrics, assembly,
            core, studio, genre, network, causal,
        ).build()
        nodes = {v.name for v in dr.list_available_variables()}
        assert "raw_data_loaded" in nodes
        assert "graphs_built" in nodes
        assert "integrated_value_computation" in nodes
        assert "results_post_processed" in nodes

    def test_all_loading_node_names_in_dag(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import loading

        dr = driver.Builder().with_modules(loading).build()
        available = {v.name for v in dr.list_available_variables()}
        for name in loading.NODE_NAMES:
            assert name in available, f"Node '{name}' missing from loading DAG"

    def test_all_resolution_node_names_in_dag(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import resolution

        dr = driver.Builder().with_modules(resolution).build()
        available = {v.name for v in dr.list_available_variables()}
        for name in resolution.NODE_NAMES:
            assert name in available, f"Node '{name}' missing from resolution DAG"


# ---------------------------------------------------------------------------
# Node name exports
# ---------------------------------------------------------------------------


class TestLoadingResolutionNodeNames:
    def test_loading_node_names_count(self):
        from src.pipeline_phases.hamilton_modules.loading import NODE_NAMES
        # H-4: added "ctx" node (DAG entry point); was 2, now 3
        assert len(NODE_NAMES) == 3

    def test_resolution_node_names_count(self):
        from src.pipeline_phases.hamilton_modules.resolution import NODE_NAMES
        # H-4: added entity_resolved + graphs_result bridge nodes; was 2, now 4
        assert len(NODE_NAMES) == 4

    def test_loading_node_names_in_all_names(self):
        from src.pipeline_phases.hamilton_modules import ALL_NODE_NAMES, LOADING_NODE_NAMES
        for name in LOADING_NODE_NAMES:
            assert name in ALL_NODE_NAMES

    def test_loading_node_names_no_duplicates(self):
        from src.pipeline_phases.hamilton_modules import ALL_NODE_NAMES
        assert len(ALL_NODE_NAMES) == len(set(ALL_NODE_NAMES))


# ---------------------------------------------------------------------------
# Node chaining: verify dependency signatures
# ---------------------------------------------------------------------------


class TestNodeChaining:
    def test_data_validated_depends_on_raw_data_loaded(self):
        """data_validated must take raw_data_loaded as parameter to enforce ordering."""
        from src.pipeline_phases.hamilton_modules.loading import data_validated
        params = inspect.signature(data_validated).parameters
        assert "raw_data_loaded" in params

    def test_entity_resolution_depends_on_data_validated(self):
        from src.pipeline_phases.hamilton_modules.resolution import entity_resolution_run
        params = inspect.signature(entity_resolution_run).parameters
        assert "data_validated" in params

    def test_graphs_built_depends_on_entity_resolution(self):
        from src.pipeline_phases.hamilton_modules.resolution import graphs_built
        params = inspect.signature(graphs_built).parameters
        assert "entity_resolution_run" in params

    def test_akm_estimation_takes_typed_bags(self):
        # H-4: akm_estimation takes entity_resolved + graphs_result (no ctx)
        from src.pipeline_phases.hamilton_modules.scoring import akm_estimation
        params = inspect.signature(akm_estimation).parameters
        assert "entity_resolved" in params
        assert "graphs_result" in params
        assert "ctx" not in params

    def test_bipartite_enhanced_depends_on_akm(self):
        from src.pipeline_phases.hamilton_modules.scoring import bipartite_enhanced
        params = inspect.signature(bipartite_enhanced).parameters
        assert "akm_estimation" in params

    def test_integrated_value_depends_on_birank_rescaled(self):
        from src.pipeline_phases.hamilton_modules.scoring import integrated_value_computation
        params = inspect.signature(integrated_value_computation).parameters
        assert "birank_rescaled" in params

    def test_engagement_decay_depends_on_iv(self):
        from src.pipeline_phases.hamilton_modules.metrics import engagement_decay
        params = inspect.signature(engagement_decay).parameters
        assert "integrated_value_computation" in params

    def test_results_assembled_depends_on_metrics_bridge(self):
        # H-4: results_assembled depends on ctx_metrics_populated (final Phase 6 bridge)
        from src.pipeline_phases.hamilton_modules.assembly import results_assembled
        params = inspect.signature(results_assembled).parameters
        assert "ctx_metrics_populated" in params


# ---------------------------------------------------------------------------
# Total node count across all phases
# ---------------------------------------------------------------------------


class TestTotalNodeCount:
    def test_all_node_names_count(self):
        from src.pipeline_phases.hamilton_modules import ALL_NODE_NAMES
        # Phase 1-4: 4, Phase 5-8: 8+17+2=27, Phase 9: 12+5+5+10+8=40
        assert len(ALL_NODE_NAMES) >= 70

    def test_full_dag_node_count(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import (
            assembly, causal, core, genre, loading, metrics,
            network, resolution, scoring, studio,
        )

        dr = driver.Builder().with_modules(
            loading, resolution, scoring, metrics, assembly,
            core, studio, genre, network, causal,
        ).build()
        # ctx + all node functions
        n = len([v for v in dr.list_available_variables() if v.name != "ctx"])
        assert n >= 70
