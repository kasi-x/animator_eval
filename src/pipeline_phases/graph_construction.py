"""Phase 4: Graph Construction — build person-anime and collaboration networks."""

import structlog

from src.analysis.graph import create_person_anime_network
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def build_graphs_phase(context: PipelineContext) -> None:
    """Build person-anime bipartite graph and collaboration graph.

    Args:
        context: Pipeline context

    Updates context fields:
        - person_anime_graph: NetworkX bipartite graph
        - collaboration_graph: NetworkX graph of person-person collaborations
    """
    with context.monitor.measure("graph_construction"):
        context.person_anime_graph = create_person_anime_network(
            context.persons, context.anime_list, context.credits
        )
        # We'll build collaboration graph later for supplementary metrics
        # but don't need it for core scoring

    context.monitor.record_memory("after_graph_build")
