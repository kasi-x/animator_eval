"""Phase 4B: VA Graph Construction — build VA bipartite, collaboration, and SD graphs."""

import structlog

from src.analysis.va.graph import (
    build_va_anime_graph,
    build_va_collaboration_graph,
    build_va_sound_director_graph,
)
from src.pipeline_phases.pipeline_types import VAScoresResult

logger = structlog.get_logger()


def build_va_graphs_phase(
    va_credits: list,
    credits: list,
    anime_map: dict,
) -> VAScoresResult:
    """Build VA-specific graphs for BiRank, collaboration, and sound director analysis.

    Args:
        va_credits: VA credit records
        credits: All credits
        anime_map: Anime metadata lookup

    Returns:
        VAScoresResult with VA graphs populated.
    """
    result = VAScoresResult()

    if not va_credits:
        logger.info("va_graph_skipped", reason="no_va_credits")
        return result

    result.va_anime_graph = build_va_anime_graph(va_credits, anime_map)
    result.va_collaboration_graph = build_va_collaboration_graph(
        va_credits, anime_map
    )
    result.va_sd_graph = build_va_sound_director_graph(
        va_credits, credits, anime_map
    )

    logger.info(
        "va_graphs_built",
        va_anime_nodes=result.va_anime_graph.number_of_nodes()
        if result.va_anime_graph
        else 0,
        va_collab_edges=result.va_collaboration_graph.number_of_edges()
        if result.va_collaboration_graph
        else 0,
        va_sd_edges=result.va_sd_graph.number_of_edges() if result.va_sd_graph else 0,
    )
    return result
