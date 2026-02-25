"""Rust-accelerated graph algorithms with Python/NetworkX fallback.

This module provides a bridge to the Rust extension `animetor_eval_core`.
When the extension is available, algorithms run 50-100x faster.
When unavailable (e.g. CI without Rust toolchain), falls back to NetworkX.
"""

from collections import defaultdict

import networkx as nx
import structlog

from src.models import Credit, Person, Role
from src.utils.config import ROLE_WEIGHTS
from src.utils.role_groups import generate_core_team_pairs

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

    Uses CORE_TEAM star topology: core↔core all-pairs + core↔non-core star.
    No non-core↔non-core edges. All staff remain as nodes (nobody is dropped).

    Returns:
        Dict mapping (person_a, person_b) to {"weight": float, "shared_works": int}
        with canonical ordering (person_a < person_b).
    """
    # Build anime_id → {person_id: (role, weight)} mapping (deduplicated)
    anime_staff: dict[str, dict[str, tuple[Role, float]]] = defaultdict(dict)
    for c in credits:
        w = ROLE_WEIGHTS.get(c.role.value, 1.0)
        aid = c.anime_id
        pid = c.person_id
        if pid not in anime_staff[aid] or w > anime_staff[aid][pid][1]:
            anime_staff[aid][pid] = (c.role, w)

    if RUST_AVAILABLE:
        # Build CORE_TEAM-filtered staff lists for Rust
        # Only pass pairs that should have edges, as flat staff lists
        anime_staff_list: list[tuple[str, list[tuple[str, float]]]] = []
        for anime_id, staff in anime_staff.items():
            staff_roles = {pid: role for pid, (role, _w) in staff.items()}
            valid_pairs = generate_core_team_pairs(staff_roles)
            # Collect unique persons that appear in valid pairs
            pair_persons: set[str] = set()
            for a, b in valid_pairs:
                pair_persons.add(a)
                pair_persons.add(b)
            # Build staff list for Rust (only persons with edges)
            staff_for_rust = [(pid, staff[pid][1]) for pid in pair_persons if pid in staff]
            if staff_for_rust:
                anime_staff_list.append((anime_id, staff_for_rust))

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

        # Filter: Rust produces all-pairs within each anime's staff list,
        # but we only want CORE_TEAM topology edges.
        # Re-check each edge against the valid pairs.
        filtered_edge_data: dict[tuple[str, str], dict[str, float]] = {}
        valid_pair_set: set[tuple[str, str]] = set()
        for anime_id, staff_info in anime_staff.items():
            staff_roles = {pid: role for pid, (role, _w) in staff_info.items()}
            for pair in generate_core_team_pairs(staff_roles):
                canonical = (pair[0], pair[1]) if pair[0] < pair[1] else (pair[1], pair[0])
                valid_pair_set.add(canonical)

        for edge_key, attrs in edge_data.items():
            if edge_key in valid_pair_set:
                filtered_edge_data[edge_key] = attrs

        return filtered_edge_data

    # Python fallback: provides edge topology only.
    # Weights computed here are placeholder averages — graph.py's
    # _apply_commitment_adjustments() or _apply_episode_adjustments()
    # will overwrite them with commitment-based values.
    edge_data_default = defaultdict(lambda: {"weight": 0.0, "shared_works": 0})
    for anime_id, staff in anime_staff.items():
        staff_roles = {pid: role for pid, (role, _w) in staff.items()}
        valid_pairs = generate_core_team_pairs(staff_roles)
        for pid_a, pid_b in valid_pairs:
            if pid_a not in staff or pid_b not in staff:
                continue
            edge_key = (pid_a, pid_b) if pid_a < pid_b else (pid_b, pid_a)
            w_a = staff[pid_a][1]
            w_b = staff[pid_b][1]
            edge_weight = (w_a + w_b) / 2.0
            edge_data_default[edge_key]["weight"] += edge_weight
            edge_data_default[edge_key]["shared_works"] += 1
    return dict(edge_data_default)
