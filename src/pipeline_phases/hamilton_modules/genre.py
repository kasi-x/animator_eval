"""Genre analysis nodes.

Hamilton nodes for genre affinity, ecosystem, network, quality, and whitespace.
"""

from __future__ import annotations

from typing import Any

from src.pipeline_phases.context import PipelineContext


NODE_NAMES: list[str] = [
    "genre_affinity",
    "genre_ecosystem",
    "genre_network",
    "genre_quality",
    "genre_whitespace",
]


def genre_affinity(ctx: PipelineContext) -> Any:
    """Compute genre affinity scores per person."""
    from src.analysis.genre.affinity import compute_genre_affinity
    return compute_genre_affinity(ctx.credits, ctx.anime_map)


def genre_ecosystem(ctx: PipelineContext) -> Any:
    """Compute genre ecosystem health metrics."""
    from src.analysis.genre.ecosystem import compute_genre_ecosystem
    return compute_genre_ecosystem(ctx.credits, ctx.anime_map, ctx.results)


def genre_network(ctx: PipelineContext) -> Any:
    """Compute genre co-occurrence network.

    Uses list(anime_map.values()) to avoid depending on anime_list,
    which is cleared after Phase 9 batch 1.
    """
    from src.analysis.genre.network import compute_genre_network
    return compute_genre_network(list(ctx.anime_map.values()))


def genre_quality(ctx: PipelineContext) -> Any:
    """Compute genre quality metrics."""
    from src.analysis.genre.quality import compute_genre_quality
    return compute_genre_quality(ctx.credits, ctx.anime_map, ctx.results)


def genre_whitespace(ctx: PipelineContext) -> Any:
    """Identify underserved genre whitespace (business brief input).

    H-1 stub: run_genre_whitespace expects dict[genre, {cagr_5y, penetration, ...}]
    but compute_genre_ecosystem returns a GenreEcosystemResult dataclass with
    different field names. Proper conversion will be wired in H-2.
    """
    import structlog
    structlog.get_logger().debug("genre_whitespace_skipped", reason="h1_interface_mismatch")
    return {}
