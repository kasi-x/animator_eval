"""Studio analysis nodes.

Hamilton nodes for studio profiling, benchmarking, clustering, and timeseries.
"""

from __future__ import annotations

from hamilton.function_modifiers import tag

from typing import Any



NODE_NAMES: list[str] = [
    "studios",
    "studio_timeseries",
    "studio_network",
    "studio_clustering",
    "studio_talent_density",
    "studio_benchmark_cards",
    "talent_pipeline",
]


@tag(stage="phase9", cost="moderate", domain="analysis")
def studios(ctx: dict) -> Any:
    """Compute studio performance analysis."""
    from src.analysis.studio.profile import compute_studio_analysis
    return compute_studio_analysis(ctx.credits, ctx.anime_map, ctx.iv_scores)


@tag(stage="phase9", cost="moderate", domain="analysis")
def studio_timeseries(ctx: dict) -> Any:
    """Compute per-studio time-series metrics."""
    from src.analysis.studio.timeseries import compute_studio_timeseries
    return compute_studio_timeseries(
        ctx.credits, ctx.anime_map, ctx.iv_scores, ctx.studio_assignments
    )


@tag(stage="phase9", cost="moderate", domain="analysis")
def studio_network(ctx: dict) -> Any:
    """Compute inter-studio collaboration network."""
    from src.analysis.studio.network import compute_studio_network
    return compute_studio_network(ctx.credits, ctx.anime_map)


@tag(stage="phase9", cost="moderate", domain="analysis")
def studio_clustering(ctx: dict) -> Any:
    """Cluster studios by talent density and collaboration patterns.

    Depends on studio_talent_density; re-computed here for H-1 flat DAG.
    H-2 will wire explicit node dependencies.
    """
    from src.analysis.studio.clustering import compute_studio_clustering
    from src.analysis.production_analysis import compute_studio_talent_density
    density = compute_studio_talent_density(ctx.credits, ctx.anime_map, ctx.person_fe)
    return compute_studio_clustering(
        ctx.credits, ctx.anime_map, density,
        studio_fe=ctx.studio_fe,
        birank_scores=ctx.birank_person_scores,
    )


@tag(stage="phase9", cost="moderate", domain="analysis")
def studio_talent_density(ctx: dict) -> Any:
    """Compute talent density metrics per studio."""
    from src.analysis.production_analysis import compute_studio_talent_density
    return compute_studio_talent_density(ctx.credits, ctx.anime_map, ctx.person_fe)


@tag(stage="phase9", cost="moderate", domain="analysis")
def studio_benchmark_cards(ctx: dict) -> Any:
    """Compute studio benchmark cards (HR brief input)."""
    from src.analysis.studio.benchmark_card import compute_studio_benchmark_cards
    # expected_ability_gap comes from causal_identification; use empty dict in H-1.
    return compute_studio_benchmark_cards(
        ctx.studio_assignments, ctx.person_fe, {}, ctx.credits
    )


@tag(stage="phase9", cost="moderate", domain="analysis")
def talent_pipeline(ctx: dict) -> Any:
    """Compute talent pipeline metrics (succession + development)."""
    from src.analysis.talent_pipeline import compute_talent_pipeline
    return compute_talent_pipeline(ctx.credits, ctx.anime_map, ctx.person_fe)
