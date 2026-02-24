"""Pipeline context — shared state for all pipeline phases.

This module defines the PipelineContext dataclass that carries state
through the entire pipeline execution, eliminating the need for global
variables and making dependencies explicit.

Also provides PipelineCheckpoint for crash resume support.
"""

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import networkx as nx
import structlog

from src.models import Anime, Credit, Person
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
    4. graph_construction: person_anime_graph, collaboration_graph
    5. core_scoring: authority_scores, trust_scores, skill_scores
    6. supplementary_metrics: decay_results, role_profiles, career_data, etc.
    7. result_assembly: results, composite_scores
    8. post_processing: (updates results in-place)
    9. analysis_modules: analysis_results
    10. export_and_viz: (reads from context, writes to files)
    """

    # Configuration
    visualize: bool
    dry_run: bool
    current_year: int = 2026

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

    # Core scores (Phase 5: core_scoring)
    authority_scores: dict[str, float] = field(default_factory=dict)
    trust_scores: dict[str, float] = field(default_factory=dict)
    skill_scores: dict[str, float] = field(default_factory=dict)

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

    # Results (Phase 7: result_assembly)
    results: list[dict] = field(default_factory=list)
    composite_scores: dict[str, float] = field(default_factory=dict)

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
            data["authority_scores"] = context.authority_scores
            data["trust_scores"] = context.trust_scores
            data["skill_scores"] = context.skill_scores

        # Save results after phase 7 (result assembly)
        if phase >= 7:
            data["results"] = context.results
            data["composite_scores"] = context.composite_scores

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
            context.authority_scores = checkpoint.get("authority_scores", {})
            context.trust_scores = checkpoint.get("trust_scores", {})
            context.skill_scores = checkpoint.get("skill_scores", {})

        if phase >= 7:
            context.results = checkpoint.get("results", [])
            context.composite_scores = checkpoint.get("composite_scores", {})

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
