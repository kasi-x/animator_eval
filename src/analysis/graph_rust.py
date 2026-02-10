"""Rust-accelerated graph algorithms with Python/NetworkX fallback.

This module provides a bridge to the Rust extension `animetor_eval_core`.
When the extension is available, algorithms run 50-100x faster.
When unavailable (e.g. CI without Rust toolchain), falls back to NetworkX.
"""

from collections import defaultdict

import networkx as nx
import structlog

from src.models import Credit, Person
from src.utils.config import ROLE_WEIGHTS

logger = structlog.get_logger()

try:
    from animetor_eval_core import (
        betweenness_centrality_rs,
        build_collaboration_edges_rs,
        degree_centrality_rs,
        eigenvector_centrality_rs,
    )

    RUST_AVAILABLE = True
    logger.debug("rust_extension_loaded")
except ImportError:
    RUST_AVAILABLE = False
    logger.debug("rust_extension_unavailable", fallback="networkx")


def _nx_graph_to_adjacency(graph: nx.Graph) -> dict[str, dict[str, float]]:
    """Convert a NetworkX graph to adjacency dict for Rust consumption."""
    adj: dict[str, dict[str, float]] = {}
    for node in graph.nodes():
        nbrs: dict[str, float] = {}
        for neighbor, data in graph[node].items():
            nbrs[str(neighbor)] = float(data.get("weight", 1.0))
        adj[str(node)] = nbrs
    return adj


def betweenness_centrality(
    graph: nx.Graph,
    k: int | None = None,
    weight: str = "weight",
    seed: int = 42,
) -> dict[str, float]:
    """Compute betweenness centrality, using Rust when available.

    Args:
        graph: NetworkX undirected graph
        k: Number of source samples (None = exact)
        weight: Edge weight attribute name
        seed: Random seed for reproducibility

    Returns:
        Dict mapping node_id to betweenness score
    """
    if RUST_AVAILABLE:
        adj = _nx_graph_to_adjacency(graph)
        n_nodes = len(adj)
        logger.info(
            "betweenness_rust",
            nodes=n_nodes,
            k=k,
            seed=seed,
        )
        return betweenness_centrality_rs(adj, k=k, seed=seed)

    # NetworkX fallback
    if k is not None:
        return nx.betweenness_centrality(graph, k=k, weight=weight, seed=seed)
    return nx.betweenness_centrality(graph, weight=weight)


def degree_centrality(graph: nx.Graph) -> dict[str, float]:
    """Compute degree centrality, using Rust when available."""
    if RUST_AVAILABLE:
        adj = _nx_graph_to_adjacency(graph)
        return degree_centrality_rs(adj)
    return nx.degree_centrality(graph)


def eigenvector_centrality(
    graph: nx.Graph,
    max_iter: int = 1000,
    weight: str = "weight",
) -> dict[str, float]:
    """Compute eigenvector centrality, using Rust when available."""
    if RUST_AVAILABLE:
        adj = _nx_graph_to_adjacency(graph)
        return eigenvector_centrality_rs(adj, max_iter=max_iter)

    try:
        return nx.eigenvector_centrality(graph, max_iter=max_iter, weight=weight)
    except (nx.PowerIterationFailedConvergence, nx.NetworkXError):
        logger.warning("eigenvector_centrality_failed")
        return {}


def build_collaboration_edges(
    persons: list[Person],
    credits: list[Credit],
) -> dict[tuple[str, str], dict[str, float]]:
    """Build collaboration edge data, using Rust when available.

    Returns:
        Dict mapping (person_a, person_b) to {"weight": float, "shared_works": int}
        with canonical ordering (person_a < person_b).
    """
    # Build anime_id → [(person_id, role, weight)] mapping
    anime_credits: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for c in credits:
        w = ROLE_WEIGHTS.get(c.role.value, 1.0)
        anime_credits[c.anime_id].append((c.person_id, w))

    if RUST_AVAILABLE:
        anime_staff_list = list(anime_credits.items())
        n_anime = len(anime_staff_list)
        total_staff = sum(len(s) for _, s in anime_staff_list)
        logger.info(
            "collaboration_edges_rust",
            anime=n_anime,
            total_staff=total_staff,
        )
        raw_edges = build_collaboration_edges_rs(anime_staff_list)
        # Convert to edge_data dict format
        edge_data: dict[tuple[str, str], dict[str, float]] = {}
        for pid_a, pid_b, weight, shared_works in raw_edges:
            edge_data[(pid_a, pid_b)] = {
                "weight": weight,
                "shared_works": shared_works,
            }
        return edge_data

    # Python fallback: same logic as original graph.py
    edge_data = defaultdict(lambda: {"weight": 0.0, "shared_works": 0})
    for _anime_id, staff in anime_credits.items():
        for i, (pid_a, w_a) in enumerate(staff):
            for pid_b, w_b in staff[i + 1 :]:
                if pid_a == pid_b:
                    continue
                edge_key = (pid_a, pid_b) if pid_a < pid_b else (pid_b, pid_a)
                edge_weight = (w_a + w_b) / 2.0
                edge_data[edge_key]["weight"] += edge_weight
                edge_data[edge_key]["shared_works"] += 1
    return dict(edge_data)
