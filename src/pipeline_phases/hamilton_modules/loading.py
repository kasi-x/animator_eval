"""Phase 1+2: Data Loading and Validation nodes for Hamilton DAG (H-5).

Nodes:
  - ctx: creates mutable pipeline state from primitive inputs (DAG entry point)
  - raw_data_loaded: loads persons, anime, credits from silver.duckdb (Phase 1)
  - loaded_data: pass-through alias — tests inject directly, prod uses raw_data_loaded
  - data_validated: runs quality checks (Phase 2)

H-5: pipeline.py passes {"visualize": bool, "dry_run": bool} as Driver inputs.
     Tests can override ctx: dr.execute(..., inputs={"ctx": ctx_fixture}).
     Tests can inject loaded data: dr.execute(..., inputs={"loaded_data": fixture}).
"""

from __future__ import annotations

import datetime
import types
from typing import Any

from hamilton.function_modifiers import tag

from src.pipeline_phases.pipeline_types import LoadedData

NODE_NAMES: list[str] = [
    "ctx",
    "raw_data_loaded",
    "loaded_data",
    "data_validated",
]


@tag(stage="init", cost="cheap", domain="loading")
def ctx(visualize: bool, dry_run: bool) -> Any:
    """Create mutable pipeline state from primitive inputs (DAG entry point).

    Returns a SimpleNamespace that accumulates state as downstream nodes run.
    Tests override via inputs={"ctx": fixture} which bypasses this node.
    """
    from src.utils.performance import PerformanceMonitor

    now = datetime.datetime.now()
    return types.SimpleNamespace(
        # Config
        visualize=visualize,
        dry_run=dry_run,
        current_year=now.year,
        current_quarter=(now.month - 1) // 3 + 1,
        monitor=PerformanceMonitor(),
        # Phase 1 data (populated by entity_resolution_run)
        persons=[],
        anime_list=[],
        credits=[],
        anime_map={},
        canonical_map={},
        # Phase 4 graphs
        person_anime_graph=None,
        collaboration_graph=None,
        community_map={},
        # Phase 5 scoring
        akm_result=None,
        birank_result=None,
        person_fe={},
        studio_fe={},
        studio_assignments={},
        birank_person_scores={},
        birank_anime_scores={},
        knowledge_spanner_scores={},
        patronage_scores={},
        dormancy_scores={},
        quality_calibration={},
        iv_scores={},
        iv_scores_historical={},
        iv_lambda_weights={},
        iv_component_std=None,
        iv_component_mean=None,
        pca_variance_explained=0.0,
        # Phase 6 supplementary
        decay_results={},
        role_profiles={},
        career_data={},
        circles={},
        centrality={},
        network_density={},
        growth_data={},
        versatility={},
        betweenness_cache={},
        peer_effect_result=None,
        career_friction={},
        era_effects=None,
        studio_bias_metrics={},
        growth_acceleration_data={},
        anime_values={},
        contribution_data={},
        potential_value_scores={},
        career_tracks={},
        # Phase 7-8 results
        results=[],
        # Phase 9 analysis
        analysis_results={},
        # VA sub-pipeline
        va_credits=[],
        characters=[],
        character_map={},
        va_person_ids=set(),
        va_person_anime_graph=None,
        va_sd_graph=None,
        va_person_fe={},
        va_sd_fe={},
        va_birank_scores={},
        va_trust_scores={},
        va_patronage_scores={},
        va_dormancy_scores={},
        va_awcc_scores={},
        va_iv_scores={},
        va_character_diversity={},
        va_ensemble_synergy={},
        va_results=[],
    )


@tag(stage="phase1", cost="moderate", domain="loading")
def raw_data_loaded(visualize: bool, dry_run: bool) -> LoadedData:
    """Load persons, anime, credits — Phase 3: Resolved 層優先 + Conformed fallback."""
    from src.pipeline_phases.data_loading import load_pipeline_data_resolved

    return load_pipeline_data_resolved(visualize, dry_run)


@tag(stage="phase1", cost="cheap", domain="loading")
def loaded_data(raw_data_loaded: LoadedData) -> LoadedData:
    """Pass-through alias for raw_data_loaded.

    Allows tests to inject synthetic data via inputs={"loaded_data": fixture}
    without triggering raw_data_loaded (which needs a live DB).
    """
    return raw_data_loaded


@tag(stage="phase2", cost="cheap", domain="loading")
def data_validated(loaded_data: LoadedData, ctx: Any) -> Any:
    """Run data quality checks against silver.duckdb (Phase 2).

    Returns a ValidationResult (passed, errors, warnings).
    Depends on loaded_data (Phase 1) and ctx to run after loading.
    """
    from src.pipeline_phases.validation import run_validation_phase

    return run_validation_phase(loaded_data)
