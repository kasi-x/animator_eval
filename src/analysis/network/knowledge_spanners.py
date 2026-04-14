"""Knowledge Spanners — AWCC + NDI for structural hole analysis.

Measures how effectively a person bridges different communities
in the collaboration network:
- AWCC (Average Weighted Community Connectivity): fraction of distinct
  communities among neighbors
- NDI (Network Disruption Index): approximation of how much removing
  a person increases inter-community distances
"""

from dataclasses import dataclass

import numpy as np
import structlog

logger = structlog.get_logger()


@dataclass
class KnowledgeSpannerMetrics:
    """Per-person knowledge spanner metrics.

    Attributes:
        awcc: Average Weighted Community Connectivity (0-1)
        ndi: Network Disruption Index (approximated)
        community_reach: number of distinct communities among neighbors
        degree: node degree in collaboration graph
    """

    awcc: float
    ndi: float
    community_reach: int
    degree: int


def compute_awcc(
    collaboration_graph,
    communities: dict[str, int],
) -> dict[str, float]:
    """Compute Average Weighted Community Connectivity for each person.

    AWCC_i = |distinct communities among neighbors of i| / degree_i

    O(Σ degree) = O(2E) total.

    Args:
        collaboration_graph: NetworkX graph
        communities: node_id → community_id

    Returns:
        person_id → AWCC score (0-1)
    """
    awcc_scores: dict[str, float] = {}
    for node in collaboration_graph.nodes():
        if node not in communities:
            continue
        neighbors = list(collaboration_graph.neighbors(node))
        degree = len(neighbors)
        if degree == 0:
            awcc_scores[node] = 0.0
            continue
        neighbor_communities = {communities[n] for n in neighbors if n in communities}
        awcc_scores[node] = len(neighbor_communities) / degree

    return awcc_scores


def compute_ndi_approximate(
    collaboration_graph,
    communities: dict[str, int],
    betweenness_cache: dict[str, float] | None = None,
    sample_size: int = 200,
) -> dict[str, float]:
    """Approximate Network Disruption Index using regression.

    Full NDI (remove node, recompute inter-community distances) is O(V²)
    per removal — too expensive. Instead:
    1. Sample up to sample_size high-degree nodes
    2. Compute exact NDI for samples (simplified: count inter-community
       edges incident on the node)
    3. Fit regression NDI ~ f(degree, AWCC, betweenness) to predict for all

    Args:
        collaboration_graph: NetworkX graph
        communities: node_id → community_id
        betweenness_cache: optional pre-computed betweenness centrality
        sample_size: number of nodes for exact computation

    Returns:
        person_id → approximate NDI score
    """
    nodes = [n for n in collaboration_graph.nodes() if n in communities]
    if not nodes:
        return {}

    # Compute features for all nodes
    degrees = dict(collaboration_graph.degree())
    awcc = compute_awcc(collaboration_graph, communities)

    # Get or compute betweenness (use cache if available)
    if betweenness_cache:
        betweenness = betweenness_cache
    else:
        betweenness = {n: 0.0 for n in nodes}

    # Compute exact NDI for sample: count inter-community edges through node
    # Sort by degree (high degree nodes are most informative)
    sorted_nodes = sorted(nodes, key=lambda n: degrees.get(n, 0), reverse=True)
    sample_nodes = sorted_nodes[:sample_size]

    exact_ndi: dict[str, float] = {}
    for node in sample_nodes:
        my_comm = communities[node]
        inter_edges = 0
        total_edges = 0
        for neighbor in collaboration_graph.neighbors(node):
            if neighbor in communities:
                total_edges += 1
                if communities[neighbor] != my_comm:
                    inter_edges += 1
        exact_ndi[node] = inter_edges / max(total_edges, 1)

    if len(exact_ndi) < 3:
        # Not enough samples, just return exact values
        return exact_ndi

    # Build regression features for sample
    sample_ids = list(exact_ndi.keys())
    X_sample = np.array(
        [
            [
                degrees.get(n, 0),
                awcc.get(n, 0),
                betweenness.get(n, 0),
            ]
            for n in sample_ids
        ],
        dtype=np.float64,
    )
    y_sample = np.array([exact_ndi[n] for n in sample_ids], dtype=np.float64)

    # Add intercept
    X_sample_with_intercept = np.column_stack(
        [
            np.ones(len(sample_ids)),
            X_sample,
        ]
    )

    # Fit OLS
    try:
        coefs, _, _, _ = np.linalg.lstsq(X_sample_with_intercept, y_sample, rcond=None)
    except np.linalg.LinAlgError:
        # Fallback to exact values + zeros
        return {n: exact_ndi.get(n, 0.0) for n in nodes}

    # Predict for all nodes
    ndi_scores: dict[str, float] = {}
    for node in nodes:
        if node in exact_ndi:
            ndi_scores[node] = exact_ndi[node]
        else:
            features = np.array(
                [
                    1.0,  # intercept
                    degrees.get(node, 0),
                    awcc.get(node, 0),
                    betweenness.get(node, 0),
                ]
            )
            pred = float(np.dot(coefs, features))
            ndi_scores[node] = max(0.0, min(1.0, pred))  # clamp to [0, 1]

    return ndi_scores


def compute_knowledge_spanners(
    collaboration_graph,
    communities: dict[str, int],
    betweenness_cache: dict[str, float] | None = None,
) -> dict[str, KnowledgeSpannerMetrics]:
    """Compute combined knowledge spanner metrics for all persons.

    Args:
        collaboration_graph: NetworkX collaboration graph
        communities: node_id → community_id
        betweenness_cache: optional pre-computed betweenness centrality

    Returns:
        person_id → KnowledgeSpannerMetrics
    """
    if not collaboration_graph or collaboration_graph.number_of_nodes() == 0:
        return {}

    if not communities:
        logger.warning("knowledge_spanners_no_communities")
        return {}

    awcc_scores = compute_awcc(collaboration_graph, communities)
    ndi_scores = compute_ndi_approximate(
        collaboration_graph, communities, betweenness_cache=betweenness_cache
    )

    degrees = dict(collaboration_graph.degree())

    result: dict[str, KnowledgeSpannerMetrics] = {}
    for node in collaboration_graph.nodes():
        if node not in communities:
            continue
        neighbors = list(collaboration_graph.neighbors(node))
        community_reach = len({communities[n] for n in neighbors if n in communities})
        result[node] = KnowledgeSpannerMetrics(
            awcc=awcc_scores.get(node, 0.0),
            ndi=ndi_scores.get(node, 0.0),
            community_reach=community_reach,
            degree=degrees.get(node, 0),
        )

    logger.info("knowledge_spanners_computed", persons=len(result))
    return result
