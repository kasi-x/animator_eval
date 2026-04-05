"""ネットワーク進化 — 人物ネットワークの時系列変化追跡.

年ごとにコラボレーションネットワークがどう変化したかを追跡する。
新規コラボの増加率、ネットワーク密度推移、コア人材の安定性などを計測。

Uses the pre-built collaboration graph (episode-aware) when available,
falling back to credit-based pair enumeration for backward compatibility.
"""

from collections import defaultdict

import networkx as nx
import structlog

from src.models import Anime, Credit
from src.utils.time_utils import get_year_quarter, yq_label

logger = structlog.get_logger()

# Threshold for switching from O(n²) pair enumeration to graph-based lookup
_LARGE_CAST_THRESHOLD = 100


def _edges_from_graph_for_cast(
    collab_graph: nx.Graph,
    persons_in_anime: set[str],
) -> set[tuple[str, str]]:
    """Efficiently find collaboration edges among a set of persons using the graph.

    For each person, iterate their graph neighbors and check if the neighbor
    is also in the cast. This is O(Σ degree(p)) instead of O(n²).
    """
    edges: set[tuple[str, str]] = set()
    for p in persons_in_anime:
        if p not in collab_graph:
            continue
        for neighbor in collab_graph.neighbors(p):
            if neighbor in persons_in_anime:
                edge = (p, neighbor) if p < neighbor else (neighbor, p)
                edges.add(edge)
    return edges


def compute_network_evolution(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    collaboration_graph: nx.Graph | None = None,
) -> dict:
    """年ごとのネットワーク進化を計算する.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime} マッピング
        collaboration_graph: Pre-built episode-aware collaboration graph.
            When provided, edges are derived from the graph instead of
            enumerating all O(n²) pairs per anime — faster and more accurate.

    Returns:
        {years: [...], snapshots: {year: {metrics}}, trends: {...}}
    """
    if not credits:
        return {"years": [], "snapshots": {}, "quarterly_snapshots": {}, "trends": {}}

    # Group credits by year AND by (year, quarter)
    credits_by_year: dict[int, list[Credit]] = defaultdict(list)
    credits_by_yq: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            credits_by_year[anime.year].append(c)
            yq = get_year_quarter(anime)
            if yq:
                credits_by_yq[yq_label(*yq)].append(c)

    if not credits_by_year:
        return {"years": [], "snapshots": {}, "quarterly_snapshots": {}, "trends": {}}

    years = sorted(credits_by_year.keys())
    cumulative_persons: set[str] = set()
    cumulative_edges: set[tuple[str, str]] = set()
    snapshots: dict[int, dict] = {}

    for year in years:
        year_credits = credits_by_year[year]

        # Persons active this year
        year_persons = {c.person_id for c in year_credits}

        # Group by anime
        anime_persons: dict[str, set[str]] = defaultdict(set)
        for c in year_credits:
            anime_persons[c.anime_id].add(c.person_id)

        new_edges_this_year = 0
        year_edges: set[tuple[str, str]] = set()

        for persons_in_anime in anime_persons.values():
            if len(persons_in_anime) < 2:
                continue

            if collaboration_graph is not None:
                # Graph-based: use neighbor lookup — O(Σ degree) per anime
                anime_edges = _edges_from_graph_for_cast(
                    collaboration_graph, persons_in_anime
                )
                for edge in anime_edges:
                    year_edges.add(edge)
                    if edge not in cumulative_edges:
                        new_edges_this_year += 1
            else:
                # No graph: enumerate pairs (skip very large casts)
                plist = sorted(persons_in_anime)
                if len(plist) > _LARGE_CAST_THRESHOLD:
                    continue
                for i, p1 in enumerate(plist):
                    for p2 in plist[i + 1 :]:
                        edge = (p1, p2)
                        year_edges.add(edge)
                        if edge not in cumulative_edges:
                            new_edges_this_year += 1

        # New persons this year
        new_persons = year_persons - cumulative_persons

        # Update cumulative state
        cumulative_persons |= year_persons
        cumulative_edges |= year_edges

        # Network density = actual_edges / possible_edges
        n = len(cumulative_persons)
        possible_edges = n * (n - 1) / 2 if n > 1 else 1
        density = len(cumulative_edges) / possible_edges if possible_edges > 0 else 0

        snapshots[year] = {
            "active_persons": len(year_persons),
            "new_persons": len(new_persons),
            "cumulative_persons": len(cumulative_persons),
            "year_edges": len(year_edges),
            "new_edges": new_edges_this_year,
            "cumulative_edges": len(cumulative_edges),
            "density": round(density, 6),
        }

    # Quarterly snapshots (lighter — person counts only, no edge enumeration)
    sorted_yq_labels = sorted(credits_by_yq.keys())
    quarterly_snapshots: dict[str, dict] = {}
    cumulative_q_persons: set[str] = set()
    for label in sorted_yq_labels:
        q_credits = credits_by_yq[label]
        q_persons = {c.person_id for c in q_credits}
        q_anime = {c.anime_id for c in q_credits}
        new_q_persons = q_persons - cumulative_q_persons
        cumulative_q_persons |= q_persons
        quarterly_snapshots[label] = {
            "active_persons": len(q_persons),
            "new_persons": len(new_q_persons),
            "cumulative_persons": len(cumulative_q_persons),
            "unique_anime": len(q_anime),
            "credit_count": len(q_credits),
        }

    # Compute trends
    if len(years) >= 2:
        first_snap = snapshots[years[0]]
        last_snap = snapshots[years[-1]]
        trends = {
            "person_growth": last_snap["cumulative_persons"]
            - first_snap["cumulative_persons"],
            "edge_growth": last_snap["cumulative_edges"]
            - first_snap["cumulative_edges"],
            "density_change": round(last_snap["density"] - first_snap["density"], 6),
            "avg_new_persons_per_year": round(
                sum(s["new_persons"] for s in snapshots.values()) / len(years), 1
            ),
            "avg_new_edges_per_year": round(
                sum(s["new_edges"] for s in snapshots.values()) / len(years), 1
            ),
        }
    else:
        trends = {}

    logger.info(
        "network_evolution_computed",
        years=len(years),
        quarters=len(sorted_yq_labels),
        graph_based=collaboration_graph is not None,
    )
    return {
        "years": years,
        "snapshots": snapshots,
        "quarterly_snapshots": quarterly_snapshots,
        "quarterly_labels": sorted_yq_labels,
        "trends": trends,
    }
