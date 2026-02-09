"""ネットワーク進化 — 人物ネットワークの時系列変化追跡.

年ごとにコラボレーションネットワークがどう変化したかを追跡する。
新規コラボの増加率、ネットワーク密度推移、コア人材の安定性などを計測。
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


def compute_network_evolution(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict:
    """年ごとのネットワーク進化を計算する.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime} マッピング

    Returns:
        {years: [...], snapshots: {year: {metrics}}, trends: {...}}
    """
    if not credits:
        return {"years": [], "snapshots": {}, "trends": {}}

    # Group credits by year
    credits_by_year: dict[int, list[Credit]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            credits_by_year[anime.year].append(c)

    if not credits_by_year:
        return {"years": [], "snapshots": {}, "trends": {}}

    years = sorted(credits_by_year.keys())
    cumulative_persons: set[str] = set()
    cumulative_edges: set[tuple[str, str]] = set()
    snapshots: dict[int, dict] = {}

    for year in years:
        year_credits = credits_by_year[year]

        # Persons active this year
        year_persons = {c.person_id for c in year_credits}

        # New collaborations this year
        anime_persons: dict[str, set[str]] = defaultdict(set)
        for c in year_credits:
            anime_persons[c.anime_id].add(c.person_id)

        new_edges_this_year = 0
        year_edges: set[tuple[str, str]] = set()
        for persons in anime_persons.values():
            plist = sorted(persons)
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

    # Compute trends
    if len(years) >= 2:
        first_snap = snapshots[years[0]]
        last_snap = snapshots[years[-1]]
        trends = {
            "person_growth": last_snap["cumulative_persons"] - first_snap["cumulative_persons"],
            "edge_growth": last_snap["cumulative_edges"] - first_snap["cumulative_edges"],
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

    logger.info("network_evolution_computed", years=len(years))
    return {"years": years, "snapshots": snapshots, "trends": trends}
