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
    """Minimal context namespace for testing Hamilton nodes."""
    import types

    ctx = types.SimpleNamespace(
        persons=[],
        credits=[],
        anime_map={},
        anime_list=[],
        results=[],
        iv_scores={},
        iv_scores_historical={},
        iv_lambda_weights={},
        person_fe={},
        studio_fe={},
        studio_assignments={},
        birank_person_scores={},
        birank_anime_scores={},
        community_map={},
        knowledge_spanner_scores={},
        patronage_scores={},
        dormancy_scores={},
        quality_calibration={},
        akm_result=None,
        birank_result=None,
        collaboration_graph=None,
        person_anime_graph=None,
        betweenness_cache={},
        centrality={},
        decay_results={},
        role_profiles={},
        career_data={},
        circles={},
        network_density={},
        growth_data={},
        versatility={},
        career_friction={},
        peer_effect_result=None,
        era_effects=None,
        studio_bias_metrics={},
        growth_acceleration_data={},
        anime_values={},
        contribution_data={},
        potential_value_scores={},
        career_tracks={},
        anime_graphs={},
        analysis_results={},
        monitor=_DummyMonitor(),
    )
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
            ANALYSIS_NODE_NAMES,
            core, studio, genre, network, causal,
        )

        dr = driver.Builder().with_modules(core, studio, genre, network, causal).build()
        available = {v.name for v in dr.list_available_variables()}
        for name in ANALYSIS_NODE_NAMES:
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
            ANALYSIS_NODE_NAMES,
            core, studio, genre, network, causal,
        )

        dr = driver.Builder().with_modules(core, studio, genre, network, causal).build()
        # ANALYSIS_NODE_NAMES (Phase 9) + ctx input = total
        n_available = len([v for v in dr.list_available_variables() if v.name != "ctx"])
        assert n_available == len(ANALYSIS_NODE_NAMES)


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

    def test_bridges_without_graph_returns_empty_result(self, minimal_context):
        from src.pipeline_phases.hamilton_modules.core import bridges
        result = bridges(minimal_context)
        # detect_bridges returns a valid empty structure even without a graph
        assert isinstance(result, dict)
        assert result.get("bridge_persons", []) == []


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
        from src.pipeline_phases.hamilton_modules import ANALYSIS_NODE_NAMES

        results = run_analysis_modules_hamilton(minimal_context)
        for name in ANALYSIS_NODE_NAMES:
            assert name in results, f"Missing key '{name}' in Hamilton results"

    def test_does_not_raise(self, minimal_context):
        """run_analysis_modules_hamilton never propagates exceptions upward."""
        from src.pipeline_phases.analysis_modules import run_analysis_modules_hamilton
        # Should complete without raising even when individual nodes fail on empty context
        results = run_analysis_modules_hamilton(minimal_context)
        assert isinstance(results, dict)


# ---------------------------------------------------------------------------
# run_analysis_modules_phase() — ThreadPoolExecutor parallel path
# ---------------------------------------------------------------------------

class TestRunAnalysisModulesPhase:
    def test_runs_without_exception(self, minimal_context, tmp_path, monkeypatch):
        from src.pipeline_phases.analysis_modules import run_analysis_modules_phase
        import src.utils.config as cfg

        monkeypatch.setattr(cfg, "JSON_DIR", tmp_path)
        run_analysis_modules_phase(minimal_context, max_workers=2)

    def test_populates_analysis_results(self, minimal_context, tmp_path, monkeypatch):
        from src.pipeline_phases.analysis_modules import run_analysis_modules_phase
        import src.utils.config as cfg

        monkeypatch.setattr(cfg, "JSON_DIR", tmp_path)
        run_analysis_modules_phase(minimal_context, max_workers=2)
        assert isinstance(minimal_context.analysis_results, dict)

    def test_max_workers_one(self, minimal_context, tmp_path, monkeypatch):
        """Single-worker path exercises the same code without parallelism."""
        from src.pipeline_phases.analysis_modules import run_analysis_modules_phase
        import src.utils.config as cfg

        monkeypatch.setattr(cfg, "JSON_DIR", tmp_path)
        run_analysis_modules_phase(minimal_context, max_workers=1)
