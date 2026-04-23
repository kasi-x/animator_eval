"""Phase 4: Graph Construction — build person-anime and collaboration networks."""

import networkx as nx
import structlog

from src.analysis.graph import (
    create_person_anime_network,
    create_person_collaboration_network,
)
from src.pipeline_phases.pipeline_types import EntityResolutionResult, GraphsResult

logger = structlog.get_logger()


def build_graphs_phase(resolved: EntityResolutionResult) -> GraphsResult:
    """Build person-anime bipartite graph, collaboration graph, and communities.

    Args:
        resolved: Entity-resolved data (persons, resolved_credits, anime)

    Returns:
        GraphsResult with person_anime_graph, collaboration_graph, community_map
    """
    person_anime_graph = create_person_anime_network(
        resolved.persons, resolved.anime_list, resolved.resolved_credits
    )

    collaboration_graph = create_person_collaboration_network(
        resolved.persons, resolved.resolved_credits, anime_map=resolved.anime_map
    )

    community_map: dict = {}

    if collaboration_graph is not None and collaboration_graph.number_of_edges() > 0:
        try:
            from src.analysis.sparse_graph import SparseCollaborationGraph

            if isinstance(collaboration_graph, SparseCollaborationGraph):
                community_map = collaboration_graph.community_detection_lpa(seed=42)
                n_communities = len(set(community_map.values()))
            else:
                n_edges = collaboration_graph.number_of_edges()
                if n_edges <= 1_000_000:
                    communities = nx.community.louvain_communities(
                        collaboration_graph, weight="weight", seed=42
                    )
                else:
                    communities = nx.community.asyn_lpa_communities(
                        collaboration_graph, seed=42
                    )
                community_map = {
                    member: cid
                    for cid, members in enumerate(communities)
                    for member in members
                }
                n_communities = len(communities)
            logger.info(
                "community_detection_complete",
                communities=n_communities,
                persons_mapped=len(community_map),
            )
        except Exception as e:
            logger.warning("community_detection_failed", error=str(e))

    return GraphsResult(
        person_anime_graph=person_anime_graph,
        collaboration_graph=collaboration_graph,
        community_map=community_map,
    )
