"""Network analysis nodes.

Hamilton nodes for collaboration, community detection, path finding,
network evolution, influence, and trust.
"""

from __future__ import annotations

from hamilton.function_modifiers import tag

from typing import Any



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


@tag(stage="phase9", cost="moderate", domain="analysis")
def collaborations(ctx: dict) -> Any:
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


@tag(stage="phase9", cost="moderate", domain="analysis")
def network_evolution(ctx: dict) -> Any:
    """Compute how the collaboration network evolved over time."""
    from src.analysis.network.network_evolution import compute_network_evolution
    return compute_network_evolution(ctx.credits, ctx.anime_map)


@tag(stage="phase9", cost="moderate", domain="analysis")
def influence(ctx: dict) -> Any:
    """Compute influence propagation tree."""
    from src.analysis.influence import compute_influence_tree
    return compute_influence_tree(ctx.credits, ctx.anime_map, person_scores=ctx.iv_scores)


@tag(stage="phase9", cost="moderate", domain="analysis")
def productivity(ctx: dict) -> Any:
    """Compute per-person productivity metrics."""
    from src.analysis.productivity import compute_productivity
    return compute_productivity(ctx.credits, ctx.anime_map)


@tag(stage="phase9", cost="moderate", domain="analysis")
def individual_profiles(ctx: dict) -> Any:
    """Compute individual contribution profiles (two-layer model)."""
    from src.analysis.scoring.individual_contribution import compute_individual_profiles
    return compute_individual_profiles(
        ctx.results, ctx.credits, ctx.anime_map, ctx.role_profiles,
        ctx.career_data, collaboration_graph=ctx.collaboration_graph,
    )


@tag(stage="phase9", cost="moderate", domain="analysis")
def person_tags(ctx: dict) -> Any:
    """Compute descriptive tags per person."""
    from src.analysis.person_tags import compute_person_tags
    return compute_person_tags(ctx.results)


@tag(stage="phase9", cost="moderate", domain="analysis")
def person_parameters(ctx: dict) -> Any:
    """Compute meta_common_person_parameters for Gold layer."""
    from src.analysis.person_parameters import compute_person_parameters
    return compute_person_parameters(ctx.results)


@tag(stage="phase9", cost="moderate", domain="analysis")
def synergy_scores(ctx: dict) -> Any:
    """Compute team synergy scores."""
    from src.analysis.synergy_score import compute_synergy_scores
    return compute_synergy_scores(ctx.credits, ctx.anime_map)


@tag(stage="phase9", cost="moderate", domain="analysis")
def trust_entry(ctx: dict) -> Any:
    """Compute trust-network entry analysis (policy brief input).

    bridges_result is not available in ctx for H-1; passes empty dict.
    H-2 will wire bridges → trust_entry as an explicit DAG edge.
    """
    from src.analysis.network.trust_entry import run_trust_entry_analysis
    return run_trust_entry_analysis(
        {}, ctx.person_fe, ctx.birank_person_scores,
        collaboration_graph=ctx.collaboration_graph,
    )


@tag(stage="phase9", cost="moderate", domain="analysis")
def independent_units(ctx: dict) -> Any:
    """Detect independent production units in the collaboration graph."""
    from src.analysis.network.independent_unit import run_independent_units
    return run_independent_units(
        ctx.community_map, ctx.credits, ctx.anime_map, ctx.person_fe
    )
