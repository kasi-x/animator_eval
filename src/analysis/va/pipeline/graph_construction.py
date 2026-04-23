"""Phase 4B: VA Graph Construction — build VA bipartite, collaboration, and SD graphs."""

import structlog

from src.analysis.va.graph import (
    build_va_anime_graph,
    build_va_collaboration_graph,
    build_va_sound_director_graph,
)
from src.analysis.va.pipeline._common import skip_if_no_va_credits, va_step
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def build_va_graphs_phase(context: PipelineContext) -> None:
    """Build VA-specific graphs for BiRank, collaboration, and sound director analysis.

    Args:
        context: Pipeline context (must have va_credits loaded)

    Updates context fields:
        - va_anime_graph: VA-Anime bipartite graph
        - va_collaboration_graph: VA-VA collaboration graph
        - va_sd_graph: VA-Sound Director bipartite graph
    """
    if skip_if_no_va_credits(context, "va_graph_skipped"):
        return

    with va_step(context, "va_graph_construction"):
        context.va_anime_graph = build_va_anime_graph(
            context.va_credits, context.anime_map
        )
        context.va_collaboration_graph = build_va_collaboration_graph(
            context.va_credits, context.anime_map
        )
        context.va_sd_graph = build_va_sound_director_graph(
            context.va_credits, context.credits, context.anime_map
        )

    logger.info(
        "va_graphs_built",
        va_anime_nodes=context.va_anime_graph.number_of_nodes()
        if context.va_anime_graph
        else 0,
        va_collab_edges=context.va_collaboration_graph.number_of_edges()
        if context.va_collaboration_graph
        else 0,
        va_sd_edges=context.va_sd_graph.number_of_edges() if context.va_sd_graph else 0,
    )
