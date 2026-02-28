"""Phase 4: Graph Construction — build person-anime and collaboration networks."""

import networkx as nx
import structlog

from src.analysis.graph import (
    create_person_anime_network,
    create_person_collaboration_network,
)
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def build_graphs_phase(context: PipelineContext) -> None:
    """Build person-anime bipartite graph, collaboration graph, and communities.

    Args:
        context: Pipeline context

    Updates context fields:
        - person_anime_graph: NetworkX bipartite graph
        - collaboration_graph: NetworkX graph of person-person collaborations
        - community_map: person_id → community_id (Louvain)
    """
    with context.monitor.measure("graph_construction"):
        # Bipartite graph (person ↔ anime)
        context.person_anime_graph = create_person_anime_network(
            context.persons, context.anime_list, context.credits
        )

        # Collaboration graph (person ↔ person)
        context.collaboration_graph = create_person_collaboration_network(
            context.persons, context.credits, anime_map=context.anime_map
        )

    # Community detection (needed for Knowledge Spanners in Phase 5)
    if (
        context.collaboration_graph is not None
        and context.collaboration_graph.number_of_edges() > 0
    ):
        with context.monitor.measure("community_detection"):
            try:
                n_edges = context.collaboration_graph.number_of_edges()
                if n_edges <= 1_000_000:
                    communities = nx.community.louvain_communities(
                        context.collaboration_graph, weight="weight", seed=42
                    )
                else:
                    communities = nx.community.label_propagation_communities(
                        context.collaboration_graph
                    )
                context.community_map = {
                    member: cid
                    for cid, members in enumerate(communities)
                    for member in members
                }
                logger.info(
                    "community_detection_complete",
                    communities=len(communities),
                    persons_mapped=len(context.community_map),
                )
            except Exception as e:
                logger.warning("community_detection_failed", error=str(e))

    context.monitor.record_memory("after_graph_build")
