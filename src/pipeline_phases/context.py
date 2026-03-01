"""Pipeline context — shared state for all pipeline phases.

This module defines the PipelineContext dataclass that carries state
through the entire pipeline execution, eliminating the need for global
variables and making dependencies explicit.

Also provides PipelineCheckpoint for crash resume support.
"""

import datetime
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import networkx as nx
import structlog

from src.models import Anime, Character, CharacterVoiceActor, Credit, Person
from src.utils.performance import PerformanceMonitor, get_monitor

checkpoint_logger = structlog.get_logger()


@dataclass
class PipelineContext:
    """Shared state for the entire pipeline execution.

    This context object is passed through all pipeline phases, accumulating
    data and results as it progresses. Each phase reads from and writes to
    specific fields, making data flow explicit and testable.

    Phases update context in this order:
    1. data_loading: persons, anime_list, credits, anime_map
    2. validation: (no context updates, just checks)
    3. entity_resolution: canonical_map, credits (resolved)
    4. graph_construction: person_anime_graph, collaboration_graph, community_map
    5. core_scoring: person_fe, studio_fe, birank, patronage, dormancy, iv_scores
    6. supplementary_metrics: decay_results, role_profiles, career_data, etc.
    7. result_assembly: results
    8. post_processing: (updates results in-place)
    9. analysis_modules: analysis_results
    10. export_and_viz: (reads from context, writes to files)
    """

    # Configuration
    visualize: bool
    dry_run: bool
    current_year: int = field(default_factory=lambda: datetime.datetime.now().year)

    # Source data (Phase 1: data_loading)
    persons: list[Person] = field(default_factory=list)
    anime_list: list[Anime] = field(default_factory=list)
    credits: list[Credit] = field(default_factory=list)
    anime_map: dict[str, Anime] = field(default_factory=dict)

    # Entity resolution (Phase 3)
    canonical_map: dict[str, str] = field(default_factory=dict)

    # Graphs (Phase 4: graph_construction)
    person_anime_graph: nx.Graph | None = None
    collaboration_graph: nx.Graph | None = None

    # 8-component structural scoring (Phase 5: core_scoring)
    akm_result: Any = None  # AKMResult dataclass
    birank_result: Any = None  # BiRankResult dataclass
    person_fe: dict[str, float] = field(default_factory=dict)  # θ_i
    studio_fe: dict[str, float] = field(default_factory=dict)  # ψ_j
    birank_person_scores: dict[str, float] = field(default_factory=dict)
    birank_anime_scores: dict[str, float] = field(default_factory=dict)
    community_map: dict[str, int] = field(default_factory=dict)  # person_id → community_id
    studio_assignments: dict[str, dict] = field(default_factory=dict)  # person_id → {year → studio}

    # Phase 6 new components
    knowledge_spanner_scores: dict[str, Any] = field(default_factory=dict)
    peer_effect_result: Any = None
    career_friction: dict[str, float] = field(default_factory=dict)
    era_effects: Any = None
    patronage_scores: dict[str, float] = field(default_factory=dict)
    dormancy_scores: dict[str, float] = field(default_factory=dict)

    # Integrated Value (replaces composite_scores semantically)
    iv_scores: dict[str, float] = field(default_factory=dict)
    iv_scores_historical: dict[str, float] = field(default_factory=dict)  # dormancy-free
    iv_lambda_weights: dict[str, float] = field(default_factory=dict)
    iv_component_std: dict[str, float] | None = None  # for consistent normalization
    iv_component_mean: dict[str, float] | None = None

    # Supplementary metrics (Phase 6)
    decay_results: dict[str, list[dict]] = field(default_factory=dict)
    role_profiles: dict[str, dict] = field(default_factory=dict)
    career_data: dict[str, Any] = field(default_factory=dict)
    circles: dict[str, Any] = field(default_factory=dict)
    centrality: dict[str, dict] = field(default_factory=dict)
    network_density: dict[str, dict] = field(default_factory=dict)
    growth_data: dict[str, dict] = field(default_factory=dict)
    versatility: dict[str, dict] = field(default_factory=dict)
    # Cached centrality sub-results (avoid recomputation)
    betweenness_cache: dict[str, float] = field(default_factory=dict)
    # Advanced metrics (Phase 6 extensions)
    studio_bias_metrics: dict[str, Any] = field(default_factory=dict)
    growth_acceleration_data: dict[str, Any] = field(default_factory=dict)
    anime_values: dict[str, Any] = field(default_factory=dict)
    contribution_data: dict[str, Any] = field(default_factory=dict)
    potential_value_scores: dict[str, Any] = field(default_factory=dict)

    # =========================================================================
    # Voice Actor Pipeline (Phases 4B–7B, parallel with production pipeline)
    # =========================================================================
    # VA source data (Phase 1)
    va_credits: list[CharacterVoiceActor] = field(default_factory=list)
    characters: list[Character] = field(default_factory=list)
    character_map: dict[str, Character] = field(default_factory=dict)
    va_person_ids: set[str] = field(default_factory=set)  # person IDs that are VAs

    # VA graphs (Phase 4B)
    va_anime_graph: nx.Graph | None = None  # VA ↔ anime bipartite
    va_collaboration_graph: nx.Graph | None = None  # VA ↔ VA
    va_sd_graph: nx.Graph | None = None  # VA ↔ sound_director

    # VA core scores (Phase 5B)
    va_person_fe: dict[str, float] = field(default_factory=dict)
    va_sd_fe: dict[str, float] = field(default_factory=dict)
    va_birank_scores: dict[str, float] = field(default_factory=dict)
    va_trust_scores: dict[str, float] = field(default_factory=dict)
    va_patronage_scores: dict[str, float] = field(default_factory=dict)
    va_dormancy_scores: dict[str, float] = field(default_factory=dict)
    va_awcc_scores: dict[str, float] = field(default_factory=dict)
    va_iv_scores: dict[str, float] = field(default_factory=dict)

    # VA supplementary metrics (Phase 6B)
    va_character_diversity: dict[str, Any] = field(default_factory=dict)
    va_ensemble_synergy: dict[str, Any] = field(default_factory=dict)
    va_replacement_difficulty: dict[str, float] = field(default_factory=dict)

    # VA results (Phase 7B)
    va_results: list[dict] = field(default_factory=list)

    # Results (Phase 7: result_assembly)
    results: list[dict] = field(default_factory=list)

    # Analysis outputs (Phase 9: analysis_modules)
    analysis_results: dict[str, Any] = field(default_factory=dict)

    # Performance monitoring
    monitor: PerformanceMonitor = field(default_factory=get_monitor)

    def __post_init__(self):
        """Initialize computed fields after dataclass init."""
        # Build anime_map if anime_list is provided but anime_map is empty
        if self.anime_list and not self.anime_map:
            self.anime_map = {a.id: a for a in self.anime_list}


class PipelineCheckpoint:
    """Manages pipeline crash resume checkpoints.

    Saves intermediate results after expensive phases so the pipeline can
    resume from the last completed phase instead of starting over.

    Strategy:
    - Phases 1-4 (data/graph): Always re-run (fast, reads from DB)
    - Phase 5-6 (scoring/metrics): Checkpoint core scores
    - Phase 7 (results): Checkpoint assembled results
    - Phase 9 (analysis): Checkpoint analysis results
    - Phase 10 (export): Delete checkpoint on success
    """

    CHECKPOINT_FILENAME = "pipeline_checkpoint.json"

    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_path = checkpoint_dir / self.CHECKPOINT_FILENAME

    def save(self, phase: int, context: PipelineContext) -> None:
        """Save checkpoint after a completed phase."""
        data: dict[str, Any] = {
            "last_completed_phase": phase,
            "timestamp": time.time(),
            "credit_count": len(context.credits),
            "person_count": len(context.persons),
        }

        # Save score data after phase 5 (core scoring)
        if phase >= 5:
            data["person_fe"] = context.person_fe
            data["studio_fe"] = context.studio_fe
            data["birank_person_scores"] = context.birank_person_scores
            data["birank_anime_scores"] = context.birank_anime_scores
            data["iv_scores"] = context.iv_scores
            data["iv_scores_historical"] = context.iv_scores_historical
            data["iv_lambda_weights"] = context.iv_lambda_weights
            data["patronage_scores"] = context.patronage_scores
            data["dormancy_scores"] = context.dormancy_scores

        # Save results after phase 7 (result assembly)
        if phase >= 7:
            data["results"] = context.results

        # Save analysis after phase 9
        if phase >= 9:
            # Filter to JSON-serializable analysis results
            serializable = {}
            for k, v in context.analysis_results.items():
                try:
                    json.dumps(v)
                    serializable[k] = v
                except (TypeError, ValueError):
                    pass
            data["analysis_results"] = serializable

        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        with open(self.checkpoint_path, "w") as f:
            json.dump(data, f, ensure_ascii=False)

        checkpoint_logger.debug(
            "checkpoint_saved", phase=phase, path=str(self.checkpoint_path)
        )

    def load(self) -> dict[str, Any] | None:
        """Load checkpoint if it exists and is valid."""
        if not self.checkpoint_path.exists():
            return None
        try:
            with open(self.checkpoint_path) as f:
                data = json.load(f)
            checkpoint_logger.info(
                "checkpoint_loaded",
                phase=data.get("last_completed_phase"),
                timestamp=data.get("timestamp"),
            )
            return data
        except (json.JSONDecodeError, OSError) as e:
            checkpoint_logger.warning("checkpoint_load_failed", error=str(e))
            return None

    def restore_to_context(self, checkpoint: dict, context: PipelineContext) -> int:
        """Restore checkpoint data into a PipelineContext.

        Returns the last completed phase number.
        """
        phase = checkpoint["last_completed_phase"]

        if phase >= 5:
            context.person_fe = checkpoint.get("person_fe", {})
            context.studio_fe = checkpoint.get("studio_fe", {})
            context.birank_person_scores = checkpoint.get("birank_person_scores", {})
            context.birank_anime_scores = checkpoint.get("birank_anime_scores", {})
            context.iv_scores = checkpoint.get("iv_scores", {})
            context.iv_scores_historical = checkpoint.get("iv_scores_historical", {})
            context.iv_lambda_weights = checkpoint.get("iv_lambda_weights", {})
            context.patronage_scores = checkpoint.get("patronage_scores", {})
            context.dormancy_scores = checkpoint.get("dormancy_scores", {})

        if phase >= 7:
            context.results = checkpoint.get("results", [])

        if phase >= 9:
            context.analysis_results = checkpoint.get("analysis_results", {})

        return phase

    def is_compatible(self, checkpoint: dict, context: PipelineContext) -> bool:
        """Check if checkpoint is compatible with current data."""
        return checkpoint.get("credit_count") == len(
            context.credits
        ) and checkpoint.get("person_count") == len(context.persons)

    def delete(self) -> None:
        """Delete checkpoint on successful pipeline completion."""
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()
            checkpoint_logger.debug("checkpoint_deleted")
