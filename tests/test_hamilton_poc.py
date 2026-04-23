"""Hamilton H-1 PoC tests — analysis_modules Hamilton execution path.

Validates that:
1. The Hamilton DAG can be built from the hamilton_modules package.
2. All declared node names resolve to real functions.
3. run_analysis_modules_hamilton() runs without raising on synthetic context.
4. Individual nodes produce results of the expected type.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def minimal_context():
    """Minimal PipelineContext for testing Hamilton nodes."""
    from src.pipeline_phases.context import PipelineContext

    ctx = PipelineContext.__new__(PipelineContext)
    ctx.persons = []
    ctx.credits = []
    ctx.anime_map = {}
    ctx.anime_list = []
    ctx.results = []
    ctx.iv_scores = {}
    ctx.collaboration_graph = None
    ctx.anime_graphs = {}
    ctx.analysis_results = {}
    ctx.monitor = _DummyMonitor()
    return ctx


class _DummyMonitor:
    def measure(self, name):
        from contextlib import nullcontext
        return nullcontext()

    def get_summary(self):
        return {}


# ---------------------------------------------------------------------------
# DAG construction
# ---------------------------------------------------------------------------

class TestHamiltonDagConstruction:
    def test_dag_builds_without_error(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import core, studio, genre, network, causal

        dr = driver.Builder().with_modules(core, studio, genre, network, causal).build()
        assert dr is not None

    def test_all_node_names_are_in_dag(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import (
            ALL_NODE_NAMES,
            core, studio, genre, network, causal,
        )

        dr = driver.Builder().with_modules(core, studio, genre, network, causal).build()
        available = {v.name for v in dr.list_available_variables()}
        for name in ALL_NODE_NAMES:
            assert name in available, f"Node '{name}' not found in Hamilton DAG"

    def test_ctx_is_input_node(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import core

        dr = driver.Builder().with_modules(core).build()
        names = {v.name for v in dr.list_available_variables()}
        assert "ctx" in names

    def test_total_node_count(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import (
            ALL_NODE_NAMES,
            core, studio, genre, network, causal,
        )

        dr = driver.Builder().with_modules(core, studio, genre, network, causal).build()
        # ALL_NODE_NAMES + ctx input = total
        n_available = len([v for v in dr.list_available_variables() if v.name != "ctx"])
        assert n_available == len(ALL_NODE_NAMES)


# ---------------------------------------------------------------------------
# Node name exports
# ---------------------------------------------------------------------------

class TestNodeNameLists:
    def test_core_node_names_nonempty(self):
        from src.pipeline_phases.hamilton_modules.core import NODE_NAMES
        assert len(NODE_NAMES) > 0

    def test_studio_node_names_nonempty(self):
        from src.pipeline_phases.hamilton_modules.studio import NODE_NAMES
        assert len(NODE_NAMES) > 0

    def test_genre_node_names_nonempty(self):
        from src.pipeline_phases.hamilton_modules.genre import NODE_NAMES
        assert len(NODE_NAMES) > 0

    def test_network_node_names_nonempty(self):
        from src.pipeline_phases.hamilton_modules.network import NODE_NAMES
        assert len(NODE_NAMES) > 0

    def test_causal_node_names_nonempty(self):
        from src.pipeline_phases.hamilton_modules.causal import NODE_NAMES
        assert len(NODE_NAMES) > 0

    def test_all_node_names_no_duplicates(self):
        from src.pipeline_phases.hamilton_modules import ALL_NODE_NAMES
        assert len(ALL_NODE_NAMES) == len(set(ALL_NODE_NAMES))


# ---------------------------------------------------------------------------
# Individual node execution on empty context
# ---------------------------------------------------------------------------

class TestCoreNodesMinimal:
    def test_anime_stats_returns_something(self, minimal_context):
        from src.pipeline_phases.hamilton_modules.core import anime_stats
        result = anime_stats(minimal_context)
        assert result is not None or result == {}

    def test_outliers_on_empty_context(self, minimal_context):
        from src.pipeline_phases.hamilton_modules.core import outliers
        result = outliers(minimal_context)
        assert result is not None

    def test_crossval_on_empty_results(self, minimal_context):
        from src.pipeline_phases.hamilton_modules.core import crossval
        result = crossval(minimal_context)
        assert result is not None

    def test_collaborations_skips_without_graph(self, minimal_context):
        from src.pipeline_phases.hamilton_modules.network import collaborations
        result = collaborations(minimal_context)
        assert result == []

    def test_bridges_skips_without_graph(self, minimal_context):
        from src.pipeline_phases.hamilton_modules.core import bridges
        result = bridges(minimal_context)
        assert result == {}


# ---------------------------------------------------------------------------
# run_analysis_modules_hamilton() end-to-end
# ---------------------------------------------------------------------------

class TestRunAnalysisModulesHamilton:
    def test_runs_without_exception(self, minimal_context):
        from src.pipeline_phases.analysis_modules import run_analysis_modules_hamilton
        results = run_analysis_modules_hamilton(minimal_context)
        assert isinstance(results, dict)

    def test_result_keys_match_node_names(self, minimal_context):
        from src.pipeline_phases.analysis_modules import run_analysis_modules_hamilton
        from src.pipeline_phases.hamilton_modules import ALL_NODE_NAMES

        results = run_analysis_modules_hamilton(minimal_context)
        for name in ALL_NODE_NAMES:
            assert name in results, f"Missing key '{name}' in Hamilton results"

    def test_does_not_raise(self, minimal_context):
        """run_analysis_modules_hamilton never propagates exceptions upward."""
        from src.pipeline_phases.analysis_modules import run_analysis_modules_hamilton
        # Should complete without raising even when individual nodes fail on empty context
        results = run_analysis_modules_hamilton(minimal_context)
        assert isinstance(results, dict)
