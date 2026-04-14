"""Genre Network Analysis — co-occurrence PMI, Louvain families, evolution.

Uses Pointwise Mutual Information (PMI) for edge weights instead of
raw co-occurrence counts to correct for frequency bias.
"""

import math
from collections import defaultdict
from dataclasses import dataclass, field

import networkx as nx
import structlog

from src.models import Anime

logger = structlog.get_logger()


@dataclass
class GenreNetworkResult:
    """Genre network analysis result.

    Attributes:
        pmi_graph: networkx Graph with PMI-weighted edges
        genre_families: genre → family_id (Louvain)
        family_names: family_id → list of genres
        pmi_matrix: (genre_a, genre_b) → PMI value
        evolution: decade → {(genre_a, genre_b) → PMI delta}
    """

    pmi_graph: nx.Graph | None = None
    genre_families: dict[str, int] = field(default_factory=dict)
    family_names: dict[int, list[str]] = field(default_factory=dict)
    pmi_matrix: dict[tuple[str, str], float] = field(default_factory=dict)
    evolution: dict[int, dict[tuple[str, str], float]] = field(default_factory=dict)


def _compute_pmi(
    anime_list: list[Anime],
    min_count: int = 5,
) -> dict[tuple[str, str], float]:
    """Compute Pointwise Mutual Information for genre pairs.

    PMI(g1, g2) = log2(P(g1,g2) / (P(g1) × P(g2)))

    Args:
        anime_list: all anime with genres
        min_count: minimum co-occurrence count to include

    Returns:
        (genre_a, genre_b) → PMI value (canonical order: a < b)
    """
    total = 0
    genre_count: dict[str, int] = defaultdict(int)
    pair_count: dict[tuple[str, str], int] = defaultdict(int)

    for anime in anime_list:
        if not anime.genres:
            continue
        total += 1
        genres = sorted(set(anime.genres))
        for g in genres:
            genre_count[g] += 1
        for i, g1 in enumerate(genres):
            for g2 in genres[i + 1 :]:
                pair_count[(g1, g2)] += 1

    if total == 0:
        return {}

    pmi: dict[tuple[str, str], float] = {}
    for (g1, g2), count in pair_count.items():
        if count < min_count:
            continue
        p_joint = count / total
        p_g1 = genre_count[g1] / total
        p_g2 = genre_count[g2] / total
        denom = p_g1 * p_g2
        if denom > 0:
            pmi[(g1, g2)] = math.log2(p_joint / denom)

    return pmi


def build_genre_pmi_network(
    anime_list: list[Anime],
    min_pmi: float = 0.0,
    min_count: int = 5,
) -> tuple[nx.Graph, dict[tuple[str, str], float]]:
    """Build genre co-occurrence network with PMI weights.

    Args:
        anime_list: all anime
        min_pmi: minimum PMI to include edge
        min_count: minimum co-occurrence count

    Returns:
        (graph, pmi_matrix)
    """
    pmi = _compute_pmi(anime_list, min_count=min_count)

    g = nx.Graph()
    for (g1, g2), value in pmi.items():
        if value >= min_pmi:
            g.add_edge(
                g1, g2, weight=max(value, 0.01)
            )  # positive weights for community detection

    logger.info(
        "genre_pmi_network_built",
        genres=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g, pmi


def compute_genre_evolution(
    anime_list: list[Anime],
    decade_range: tuple[int, int] = (1990, 2020),
) -> dict[int, dict[tuple[str, str], float]]:
    """Compute PMI changes across decades (delta-PMI).

    Args:
        anime_list: all anime
        decade_range: (start_decade, end_decade) inclusive

    Returns:
        decade → {(genre_a, genre_b) → PMI delta from previous decade}
    """
    # Split anime by decade
    decade_anime: dict[int, list[Anime]] = defaultdict(list)
    for anime in anime_list:
        if anime.year:
            decade = (anime.year // 10) * 10
            decade_anime[decade].append(anime)

    # Compute PMI per decade
    decade_pmi: dict[int, dict[tuple[str, str], float]] = {}
    for decade in range(decade_range[0], decade_range[1] + 10, 10):
        if decade in decade_anime:
            decade_pmi[decade] = _compute_pmi(decade_anime[decade], min_count=3)

    # Compute delta-PMI
    evolution: dict[int, dict[tuple[str, str], float]] = {}
    decades = sorted(decade_pmi.keys())
    for i in range(1, len(decades)):
        prev = decade_pmi[decades[i - 1]]
        curr = decade_pmi[decades[i]]
        all_pairs = set(prev.keys()) | set(curr.keys())
        delta: dict[tuple[str, str], float] = {}
        for pair in all_pairs:
            delta[pair] = curr.get(pair, 0.0) - prev.get(pair, 0.0)
        evolution[decades[i]] = delta

    return evolution


def compute_genre_network(
    anime_list: list[Anime],
) -> GenreNetworkResult:
    """Compute full genre network analysis.

    Args:
        anime_list: all anime with genres

    Returns:
        GenreNetworkResult with PMI graph, families, and evolution.
    """
    # Build PMI network
    pmi_graph, pmi_matrix = build_genre_pmi_network(anime_list)

    # Community detection (genre families)
    genre_families: dict[str, int] = {}
    family_names: dict[int, list[str]] = {}

    if pmi_graph.number_of_nodes() > 2:
        try:
            comms = nx.community.louvain_communities(
                pmi_graph, weight="weight", seed=42
            )
            for fam_id, members in enumerate(comms):
                family_names[fam_id] = sorted(members)
                for genre in members:
                    genre_families[genre] = fam_id
        except Exception:
            logger.debug("genre_community_detection_failed")

    # Genre evolution
    evolution = compute_genre_evolution(anime_list)

    logger.info(
        "genre_network_computed",
        genres=pmi_graph.number_of_nodes(),
        families=len(family_names),
        evolution_decades=len(evolution),
    )

    return GenreNetworkResult(
        pmi_graph=pmi_graph,
        genre_families=genre_families,
        family_names=family_names,
        pmi_matrix=pmi_matrix,
        evolution=evolution,
    )
