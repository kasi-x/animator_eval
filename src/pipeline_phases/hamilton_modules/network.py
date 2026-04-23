"""Network analysis nodes.

Hamilton nodes for collaboration, community detection, path finding,
network evolution, influence, and trust.
"""

from __future__ import annotations

from typing import Any

from src.pipeline_phases.context import PipelineContext


NODE_NAMES: list[str] = [
    "collaborations",
    "network_evolution",
    "influence",
    "productivity",
    "individual_profiles",
    "person_tags",
    "person_parameters",
    "synergy_scores",
    "trust_entry",
    "independent_units",
]


def collaborations(ctx: PipelineContext) -> Any:
    """Compute strongest collaboration pairs."""
    if ctx.collaboration_graph is None:
        return []
    from src.analysis.collaboration_strength import compute_collaboration_strength
    pairs = compute_collaboration_strength(
        ctx.credits,
        ctx.anime_map,
        min_shared=2,
        person_scores=ctx.iv_scores,
        collaboration_graph=ctx.collaboration_graph,
    )
    return pairs[:500] if pairs else []


def network_evolution(ctx: PipelineContext) -> Any:
    """Compute how the collaboration network evolved over time."""
    from src.analysis.network.network_evolution import compute_network_evolution
    return compute_network_evolution(ctx.credits, ctx.anime_map)


def influence(ctx: PipelineContext) -> Any:
    """Compute influence propagation tree."""
    from src.analysis.influence import compute_influence_tree
    return compute_influence_tree(ctx.results, ctx.credits)


def productivity(ctx: PipelineContext) -> Any:
    """Compute per-person productivity metrics."""
    from src.analysis.productivity import compute_productivity
    return compute_productivity(ctx.credits, ctx.anime_map)


def individual_profiles(ctx: PipelineContext) -> Any:
    """Compute individual contribution profiles (two-layer model)."""
    from src.analysis.scoring.individual_contribution import compute_individual_profiles
    return compute_individual_profiles(
        ctx.results, ctx.credits, ctx.anime_map,
        ctx.collaboration_graph, ctx.anime_graphs
    )


def person_tags(ctx: PipelineContext) -> Any:
    """Compute descriptive tags per person."""
    from src.analysis.person_tags import compute_person_tags
    return compute_person_tags(ctx.results, ctx.credits, ctx.anime_map)


def person_parameters(ctx: PipelineContext) -> Any:
    """Compute meta_common_person_parameters for Gold layer."""
    from src.analysis.person_parameters import compute_person_parameters
    return compute_person_parameters(ctx.results, ctx.credits, ctx.anime_map)


def synergy_scores(ctx: PipelineContext) -> Any:
    """Compute team synergy scores."""
    from src.analysis.synergy_score import compute_synergy_scores
    if ctx.collaboration_graph is None:
        return {}
    return compute_synergy_scores(
        ctx.credits, ctx.anime_map, ctx.results, ctx.collaboration_graph
    )


def trust_entry(ctx: PipelineContext) -> Any:
    """Compute trust-network entry analysis (policy brief input)."""
    from src.analysis.network.trust_entry import run_trust_entry_analysis
    return run_trust_entry_analysis(ctx.credits, ctx.anime_map, ctx.results)


def independent_units(ctx: PipelineContext) -> Any:
    """Detect independent production units in the collaboration graph."""
    from src.analysis.network.independent_unit import run_independent_units
    if ctx.collaboration_graph is None:
        return {}
    return run_independent_units(ctx.collaboration_graph, ctx.results)
