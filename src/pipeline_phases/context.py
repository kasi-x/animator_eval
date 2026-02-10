"""Pipeline context — shared state for all pipeline phases.

This module defines the PipelineContext dataclass that carries state
through the entire pipeline execution, eliminating the need for global
variables and making dependencies explicit.
"""
from dataclasses import dataclass, field
from typing import Any

import networkx as nx

from src.models import Anime, Credit, Person
from src.utils.performance import PerformanceMonitor, get_monitor


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
