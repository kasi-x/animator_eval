"""Studio Network Analysis — talent sharing and co-production networks.

Two networks:
1. Talent sharing: studios connected by shared staff within 3-year windows
2. Co-production: studios connected by shared anime (from anime_studios)

Plus centrality metrics and Louvain community detection.
"""

from collections import defaultdict
from dataclasses import dataclass, field

import networkx as nx
import structlog

from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()

TALENT_WINDOW_YEARS = 3


@dataclass
class StudioNetworkResult:
    """Studio network analysis result.

    Attributes:
        talent_sharing_graph: networkx Graph of talent sharing
        coproduction_graph: networkx Graph of co-productions
        centrality: studio → {degree, weighted_degree, betweenness, eigenvector, closeness}
        communities: studio → community_id (Louvain)
        talent_flow_edges: list of {studio_a, studio_b, shared_persons, years}
    """

    talent_sharing_graph: nx.Graph | None = None
    coproduction_graph: nx.Graph | None = None
    centrality: dict[str, dict[str, float]] = field(default_factory=dict)
    communities: dict[str, int] = field(default_factory=dict)
    talent_flow_edges: list[dict] = field(default_factory=list)


def build_talent_sharing_network(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    window_years: int = TALENT_WINDOW_YEARS,
) -> nx.Graph:
    """Build studio talent sharing network.

    Two studios are connected when the same person has credits at both
    within a sliding window of `window_years`.

    Edge weight = number of shared persons.
    """
    # person → [(studio, year)]
    person_studio_years: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year or not anime.studios:
            continue
        for studio in anime.studios:
            person_studio_years[c.person_id].append((studio, anime.year))

    # Find shared persons between studios
    edge_weights: dict[tuple[str, str], set[str]] = defaultdict(set)
    for pid, studio_years in person_studio_years.items():
        # Unique studios per year for this person
        studios_by_year: dict[int, set[str]] = defaultdict(set)
        for studio, year in studio_years:
            studios_by_year[year].add(studio)

        # Check within-window transitions
        years = sorted(studios_by_year.keys())
        for i, y1 in enumerate(years):
            for y2 in years[i:]:
                if y2 - y1 > window_years:
                    break
                for s1 in studios_by_year[y1]:
                    for s2 in studios_by_year[y2]:
                        if s1 < s2:
                            edge_weights[(s1, s2)].add(pid)
                        elif s2 < s1:
                            edge_weights[(s2, s1)].add(pid)

    g = nx.Graph()
    for (s1, s2), persons in edge_weights.items():
        g.add_edge(s1, s2, weight=len(persons), shared_persons=len(persons))

    logger.info(
        "talent_sharing_network_built",
        studios=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g


def build_coproduction_network(
    anime_map: dict[str, Anime],
) -> nx.Graph:
    """Build studio co-production network.

    Studios are connected when they co-produce the same anime.
    Edge weight = number of co-produced anime.
    """
    edge_weights: dict[tuple[str, str], int] = defaultdict(int)

    for anime in anime_map.values():
        if not anime.studios or len(anime.studios) < 2:
            continue
        studios = sorted(anime.studios)
        for i, s1 in enumerate(studios):
            for s2 in studios[i + 1 :]:
                edge_weights[(s1, s2)] += 1

    g = nx.Graph()
    for (s1, s2), count in edge_weights.items():
        g.add_edge(s1, s2, weight=count, coproductions=count)

    logger.info(
        "coproduction_network_built",
        studios=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g


def compute_studio_centrality(
    graph: nx.Graph,
) -> dict[str, dict[str, float]]:
    """Compute centrality metrics for studios in a network."""
    if graph.number_of_nodes() == 0:
        return {}

    centrality: dict[str, dict[str, float]] = {}

    degree = dict(graph.degree(weight=None))
    w_degree = dict(graph.degree(weight="weight"))

    # Betweenness (approximate for large graphs)
    n = graph.number_of_nodes()
    k = min(n, 100) if n > 500 else None
    try:
        betw = nx.betweenness_centrality(graph, k=k, weight="weight")
    except Exception:
        betw = {node: 0.0 for node in graph.nodes()}

    # Eigenvector
    try:
        eigen = nx.eigenvector_centrality_numpy(graph, weight="weight")
    except Exception:
        eigen = {node: 0.0 for node in graph.nodes()}

    # Closeness
    try:
        close = nx.closeness_centrality(graph, distance=None)
    except Exception:
        close = {node: 0.0 for node in graph.nodes()}

    for node in graph.nodes():
        centrality[node] = {
            "degree": degree.get(node, 0),
            "weighted_degree": w_degree.get(node, 0),
            "betweenness": betw.get(node, 0.0),
            "eigenvector": eigen.get(node, 0.0),
            "closeness": close.get(node, 0.0),
        }

    return centrality


def compute_studio_network(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> StudioNetworkResult:
    """Compute full studio network analysis.

    Args:
        credits: all production credits
        anime_map: anime_id → Anime

    Returns:
        StudioNetworkResult with graphs, centrality, and communities.
    """
    talent_g = build_talent_sharing_network(credits, anime_map)
    coprod_g = build_coproduction_network(anime_map)

    # Centrality on talent sharing network (richer data)
    centrality = compute_studio_centrality(talent_g)

    # Community detection (Louvain)
    communities: dict[str, int] = {}
    if talent_g.number_of_nodes() > 2:
        try:
            comms = nx.community.louvain_communities(talent_g, weight="weight", seed=42)
            for comm_id, members in enumerate(comms):
                for member in members:
                    communities[member] = comm_id
        except Exception:
            logger.debug("studio_community_detection_failed")

    # Talent flow edges (for export)
    flow_edges = []
    for s1, s2, data in talent_g.edges(data=True):
        flow_edges.append(
            {
                "studio_a": s1,
                "studio_b": s2,
                "shared_persons": data.get("shared_persons", 0),
            }
        )

    logger.info(
        "studio_network_computed",
        talent_nodes=talent_g.number_of_nodes(),
        coprod_nodes=coprod_g.number_of_nodes(),
        communities=len(set(communities.values())) if communities else 0,
    )

    return StudioNetworkResult(
        talent_sharing_graph=talent_g,
        coproduction_graph=coprod_g,
        centrality=centrality,
        communities=communities,
        talent_flow_edges=flow_edges,
    )
