"""Hamilton H-2 tests — Phase 5-8 scoring, metrics, and assembly nodes.

Validates:
1. DAG builds from scoring + metrics + assembly modules.
2. All declared node names exist in the DAG.
3. Each node can execute on an empty/minimal context without hard errors.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _DummyMonitor:
    def measure(self, name):
        from contextlib import nullcontext
        return nullcontext()

    def record_memory(self, label):
        pass

    def increment_counter(self, key, value=1):
        pass

    def get_summary(self):
        return {}


@pytest.fixture()
def minimal_ctx():
    """Minimal PipelineContext for testing Phase 5-8 nodes on empty data."""
    from src.pipeline_phases.context import PipelineContext

    ctx = PipelineContext.__new__(PipelineContext)
    ctx.persons = []
    ctx.credits = []
    ctx.anime_list = []
    ctx.anime_map = {}
    ctx.results = []
    ctx.iv_scores = {}
    ctx.iv_scores_historical = {}
    ctx.iv_lambda_weights = {}
    ctx.iv_component_std = None
    ctx.iv_component_mean = None
    ctx.pca_variance_explained = 0.0
    ctx.person_fe = {}
    ctx.studio_fe = {}
    ctx.studio_assignments = {}
    ctx.birank_person_scores = {}
    ctx.birank_anime_scores = {}
    ctx.birank_result = None
    ctx.akm_result = None
    ctx.knowledge_spanner_scores = {}
    ctx.patronage_scores = {}
    ctx.dormancy_scores = {}
    ctx.quality_calibration = {}
    ctx.community_map = {}
    ctx.collaboration_graph = None
    ctx.person_anime_graph = None
    ctx.canonical_map = {}
    ctx.betweenness_cache = {}
    ctx.centrality = {}
    ctx.decay_results = {}
    ctx.role_profiles = {}
    ctx.career_data = {}
    ctx.circles = {}
    ctx.versatility = {}
    ctx.network_density = {}
    ctx.growth_data = {}
    ctx.peer_effect_result = None
    ctx.career_friction = {}
    ctx.era_effects = None
    ctx.growth_acceleration_data = {}
    ctx.anime_values = {}
    ctx.contribution_data = {}
    ctx.potential_value_scores = {}
    ctx.career_tracks = {}
    ctx.analysis_results = {}
    ctx.va_credits = []
    ctx.characters = []
    ctx.character_map = {}
    ctx.va_person_ids = set()
    ctx.current_year = 2025
    ctx.current_quarter = 2
    ctx.visualize = False
    ctx.dry_run = False
    ctx.monitor = _DummyMonitor()
    return ctx


# ---------------------------------------------------------------------------
# DAG construction
# ---------------------------------------------------------------------------


class TestPhase58DagBuilds:
    def test_scoring_dag_builds(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import scoring

        dr = driver.Builder().with_modules(scoring).build()
        assert dr is not None

    def test_metrics_dag_builds(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import metrics

        dr = driver.Builder().with_modules(metrics).build()
        assert dr is not None

    def test_assembly_dag_builds(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import assembly

        dr = driver.Builder().with_modules(assembly).build()
        assert dr is not None

    def test_combined_phase5_9_dag_builds(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import (
            assembly, causal, core, genre, metrics, network, scoring, studio,
        )

        dr = driver.Builder().with_modules(
            scoring, metrics, assembly, core, studio, genre, network, causal
        ).build()
        assert dr is not None

    def test_all_scoring_node_names_in_dag(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import scoring

        dr = driver.Builder().with_modules(scoring).build()
        available = {v.name for v in dr.list_available_variables()}
        for name in scoring.NODE_NAMES:
            assert name in available, f"Node '{name}' missing from scoring DAG"

    def test_all_metrics_node_names_in_dag(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import metrics

        dr = driver.Builder().with_modules(metrics).build()
        available = {v.name for v in dr.list_available_variables()}
        for name in metrics.NODE_NAMES:
            assert name in available, f"Node '{name}' missing from metrics DAG"

    def test_all_assembly_node_names_in_dag(self):
        from hamilton import driver
        from src.pipeline_phases.hamilton_modules import assembly

        dr = driver.Builder().with_modules(assembly).build()
        available = {v.name for v in dr.list_available_variables()}
        for name in assembly.NODE_NAMES:
            assert name in available, f"Node '{name}' missing from assembly DAG"


# ---------------------------------------------------------------------------
# Node name exports
# ---------------------------------------------------------------------------


class TestNodeNameLists:
    def test_scoring_node_names_nonempty(self):
        from src.pipeline_phases.hamilton_modules.scoring import NODE_NAMES
        assert len(NODE_NAMES) == 8

    def test_metrics_node_names_nonempty(self):
        from src.pipeline_phases.hamilton_modules.metrics import NODE_NAMES
        assert len(NODE_NAMES) == 17

    def test_assembly_node_names_nonempty(self):
        from src.pipeline_phases.hamilton_modules.assembly import NODE_NAMES
        assert len(NODE_NAMES) == 2

    def test_scoring_node_names_no_duplicates(self):
        from src.pipeline_phases.hamilton_modules.scoring import NODE_NAMES
        assert len(NODE_NAMES) == len(set(NODE_NAMES))

    def test_metrics_node_names_no_duplicates(self):
        from src.pipeline_phases.hamilton_modules.metrics import NODE_NAMES
        assert len(NODE_NAMES) == len(set(NODE_NAMES))

    def test_scoring_node_names_export(self):
        from src.pipeline_phases.hamilton_modules import SCORING_NODE_NAMES
        assert "akm_estimation" in SCORING_NODE_NAMES
        assert "integrated_value_computation" in SCORING_NODE_NAMES
        assert "results_post_processed" in SCORING_NODE_NAMES


# ---------------------------------------------------------------------------
# Individual node execution on empty / minimal context
# ---------------------------------------------------------------------------


class TestScoringNodesMinimal:
    def test_akm_estimation_on_empty_credits(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.scoring import akm_estimation
        result = akm_estimation(minimal_ctx)
        assert result is not None

    def test_birank_rescaled_on_empty_scores(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.scoring import birank_rescaled
        result = birank_rescaled(minimal_ctx, dormancy_penalty_computation={})
        assert isinstance(result, dict)

    def test_knowledge_spanners_without_graph(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.scoring import knowledge_spanners_computation
        result = knowledge_spanners_computation(minimal_ctx, birank_computation=None)
        assert result is not None

    def test_patronage_premium_on_empty_credits(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.scoring import patronage_premium_computation
        result = patronage_premium_computation(minimal_ctx, birank_computation=None)
        assert isinstance(result, dict)

    def test_dormancy_penalty_on_empty_credits(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.scoring import dormancy_penalty_computation
        result = dormancy_penalty_computation(minimal_ctx, patronage_premium_computation={})
        assert isinstance(result, dict)


class TestMetricsNodesMinimal:
    def test_role_classification_on_empty_credits(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.metrics import role_classification
        result = role_classification(minimal_ctx, engagement_decay={})
        assert isinstance(result, dict)

    def test_career_analysis_on_empty_credits(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.metrics import career_analysis
        result = career_analysis(minimal_ctx, role_classification={})
        assert isinstance(result, dict)

    def test_centrality_without_graph(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.metrics import centrality_metrics
        result = centrality_metrics(minimal_ctx, versatility_computed={})
        assert isinstance(result, dict)

    def test_career_tracks_on_empty_credits(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.metrics import career_tracks_inferred
        result = career_tracks_inferred(minimal_ctx, potential_value_computed={})
        assert isinstance(result, dict)


class TestAssemblyNodesMinimal:
    def test_results_assembled_on_empty_context(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.assembly import results_assembled
        result = results_assembled(minimal_ctx, career_tracks_inferred={})
        assert isinstance(result, list)

    def test_results_post_processed_on_empty_results(self, minimal_ctx):
        from src.pipeline_phases.hamilton_modules.assembly import results_post_processed
        result = results_post_processed(minimal_ctx, results_assembled=[])
        assert isinstance(result, list)
