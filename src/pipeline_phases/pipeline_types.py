"""Typed data bags for the scoring pipeline (H-4).

Replaces PipelineContext with explicit typed dataclasses per phase.
Each bag carries only the fields produced or consumed by that phase.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LoadedData:
    """Phase 1 output: raw data from silver.duckdb."""

    persons: list = field(default_factory=list)
    anime_list: list = field(default_factory=list)
    credits: list = field(default_factory=list)
    anime_map: dict = field(default_factory=dict)
    va_credits: list = field(default_factory=list)
    characters: list = field(default_factory=list)
    character_map: dict = field(default_factory=dict)
    va_person_ids: set = field(default_factory=set)
    current_year: int = field(default_factory=lambda: datetime.datetime.now().year)
    current_quarter: int = field(
        default_factory=lambda: (datetime.datetime.now().month - 1) // 3 + 1
    )
    visualize: bool = False
    dry_run: bool = False


@dataclass
class EntityResolutionResult:
    """Phase 3 output: resolved data (credits, persons, anime all deduplicated)."""

    resolved_credits: list = field(default_factory=list)
    canonical_map: dict = field(default_factory=dict)
    # Updated after entity resolution (non-canonical entries removed)
    persons: list = field(default_factory=list)
    anime_list: list = field(default_factory=list)
    anime_map: dict = field(default_factory=dict)


@dataclass
class GraphsResult:
    """Phase 4 output: bipartite graph, collaboration graph, community map."""

    person_anime_graph: Any = None
    collaboration_graph: Any = None
    community_map: dict = field(default_factory=dict)


@dataclass
class CoreScoresResult:
    """Phase 5 output: all core scoring components."""

    # AKM
    akm_result: Any = None
    person_fe: dict = field(default_factory=dict)
    studio_fe: dict = field(default_factory=dict)
    studio_assignments: dict = field(default_factory=dict)
    quality_calibration: dict = field(default_factory=dict)
    # BiRank
    birank_result: Any = None
    birank_person_scores: dict = field(default_factory=dict)
    birank_anime_scores: dict = field(default_factory=dict)
    # Community (updated from graph phase)
    community_map: dict = field(default_factory=dict)
    # Knowledge spanners
    knowledge_spanner_scores: dict = field(default_factory=dict)
    # Patronage / dormancy
    patronage_scores: dict = field(default_factory=dict)
    dormancy_scores: dict = field(default_factory=dict)
    # Integrated Value
    iv_scores: dict = field(default_factory=dict)
    iv_scores_historical: dict = field(default_factory=dict)
    iv_lambda_weights: dict = field(default_factory=dict)
    iv_component_std: dict | None = None
    iv_component_mean: dict | None = None
    pca_variance_explained: float = 0.0


@dataclass
class SupplementaryMetricsResult:
    """Phase 6 output: supplementary metrics and career-aware score updates."""

    # Career / role
    decay_results: dict = field(default_factory=dict)
    role_profiles: dict = field(default_factory=dict)
    career_data: dict = field(default_factory=dict)
    career_tracks: dict = field(default_factory=dict)
    career_friction: dict = field(default_factory=dict)
    # Network
    circles: dict = field(default_factory=dict)
    centrality: dict = field(default_factory=dict)
    network_density: dict = field(default_factory=dict)
    betweenness_cache: dict = field(default_factory=dict)
    # Growth / value
    versatility: dict = field(default_factory=dict)
    growth_data: dict = field(default_factory=dict)
    growth_acceleration_data: dict = field(default_factory=dict)
    anime_values: dict = field(default_factory=dict)
    contribution_data: dict = field(default_factory=dict)
    potential_value_scores: dict = field(default_factory=dict)
    studio_bias_metrics: dict = field(default_factory=dict)
    # Causal
    era_effects: Any = None
    peer_effect_result: Any = None
    # Updated from Phase 5 (career-aware dormancy)
    dormancy_scores: dict = field(default_factory=dict)
    iv_scores: dict = field(default_factory=dict)
    knowledge_spanner_scores: dict = field(default_factory=dict)


@dataclass
class VAScoresResult:
    """VA pipeline output: voice-actor-specific scores."""

    va_person_fe: dict = field(default_factory=dict)
    va_sd_fe: dict = field(default_factory=dict)
    va_birank_scores: dict = field(default_factory=dict)
    va_trust_scores: dict = field(default_factory=dict)
    va_patronage_scores: dict = field(default_factory=dict)
    va_dormancy_scores: dict = field(default_factory=dict)
    va_awcc_scores: dict = field(default_factory=dict)
    va_iv_scores: dict = field(default_factory=dict)
    va_character_diversity: dict = field(default_factory=dict)
    va_ensemble_synergy: dict = field(default_factory=dict)
    va_replacement_difficulty: dict = field(default_factory=dict)
    va_results: list = field(default_factory=list)
    # VA graphs
    va_anime_graph: Any = None
    va_collaboration_graph: Any = None
    va_sd_graph: Any = None


@dataclass
class ExportContext:
    """Lightweight context for Phase 10 export/visualization (ctx 軽量版)."""

    results: list = field(default_factory=list)
    circles: dict = field(default_factory=dict)
    growth_data: dict = field(default_factory=dict)
    studio_bias_metrics: dict = field(default_factory=dict)
    anime_values: dict = field(default_factory=dict)
    contribution_data: dict = field(default_factory=dict)
    potential_value_scores: dict = field(default_factory=dict)
    analysis_results: dict = field(default_factory=dict)
    persons: list = field(default_factory=list)
    anime_list: list = field(default_factory=list)
    credits: list = field(default_factory=list)
    collaboration_graph: Any = None
    betweenness_cache: dict = field(default_factory=dict)
    visualize: bool = False
    community_map: dict = field(default_factory=dict)
    career_tracks: dict = field(default_factory=dict)
    peer_effect_result: Any = None
    career_friction: dict = field(default_factory=dict)
    iv_scores: dict = field(default_factory=dict)
    centrality: dict = field(default_factory=dict)
    network_density: dict = field(default_factory=dict)
    growth_acceleration_data: dict = field(default_factory=dict)
    versatility: dict = field(default_factory=dict)
    role_profiles: dict = field(default_factory=dict)
    anime_map: dict = field(default_factory=dict)
