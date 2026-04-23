"""Studio analysis nodes.

Hamilton nodes for studio profiling, benchmarking, clustering, and timeseries.
"""

from __future__ import annotations

from typing import Any

from src.pipeline_phases.context import PipelineContext


NODE_NAMES: list[str] = [
    "studios",
    "studio_timeseries",
    "studio_network",
    "studio_clustering",
    "studio_talent_density",
    "studio_benchmark_cards",
    "talent_pipeline",
]


def studios(ctx: PipelineContext) -> Any:
    """Compute studio performance analysis."""
    from src.analysis.studio.profile import compute_studio_analysis
    return compute_studio_analysis(ctx.credits, ctx.anime_map, ctx.iv_scores)


def studio_timeseries(ctx: PipelineContext) -> Any:
    """Compute per-studio time-series metrics."""
    from src.analysis.studio.timeseries import compute_studio_timeseries
    return compute_studio_timeseries(ctx.credits, ctx.anime_map)


def studio_network(ctx: PipelineContext) -> Any:
    """Compute inter-studio collaboration network."""
    from src.analysis.studio.network import compute_studio_network
    return compute_studio_network(ctx.credits, ctx.anime_map)


def studio_clustering(ctx: PipelineContext) -> Any:
    """Cluster studios by talent density and collaboration patterns.

    Depends on studio_talent_density and studio_network results.
    For H-1 they are re-computed here; H-2 will wire explicit dependencies.
    """
    from src.analysis.studio.clustering import compute_studio_clustering
    from src.analysis.studio.network import compute_studio_network as _studio_net
    from src.analysis.production_analysis import compute_studio_talent_density
    density = compute_studio_talent_density(ctx.credits, ctx.anime_map, ctx.results)
    net = _studio_net(ctx.credits, ctx.anime_map)
    return compute_studio_clustering(ctx.credits, ctx.anime_map, density, net)


def studio_talent_density(ctx: PipelineContext) -> Any:
    """Compute talent density metrics per studio."""
    from src.analysis.production_analysis import compute_studio_talent_density
    return compute_studio_talent_density(ctx.credits, ctx.anime_map, ctx.results)


def studio_benchmark_cards(ctx: PipelineContext) -> Any:
    """Compute studio benchmark cards (HR brief input)."""
    from src.analysis.studio.benchmark_card import compute_studio_benchmark_cards
    return compute_studio_benchmark_cards(ctx.credits, ctx.anime_map, ctx.results)


def talent_pipeline(ctx: PipelineContext) -> Any:
    """Compute talent pipeline metrics (succession + development)."""
    from src.analysis.talent_pipeline import compute_talent_pipeline
    return compute_talent_pipeline(ctx.credits, ctx.anime_map, ctx.results)
