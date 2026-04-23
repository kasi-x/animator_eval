"""Seasonal trend analysis — analyse credit patterns by season.

アニメの放送シーズン（冬/春/夏/秋）ごとに、
人材の参加パターンと役職分布を計算する。
"""

from collections import defaultdict

import structlog

from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()

SEASON_ORDER = {"winter": 0, "spring": 1, "summer": 2, "fall": 3}


def _infer_season(anime: Anime) -> str | None:
    """Infer the season of an anime."""
    if anime.season:
        return anime.season.lower()
    return None


def compute_seasonal_trends(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
) -> dict:
    """Seasonal trend analysis.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime}
        person_scores: {person_id: composite_score}

    Returns:
        {
            "by_season": {season: {anime_count, credit_count, avg_score, ...}},
            "by_year_season": {year: {season: {anime_count, credit_count}}},
            "role_by_season": {season: {role: count}},
            "total_with_season": int,
        }
    """
    season_anime: dict[str, set[str]] = defaultdict(set)
    season_credits: dict[str, list[Credit]] = defaultdict(list)
    season_roles: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    year_season: dict[int, dict[str, dict]] = defaultdict(
        lambda: defaultdict(lambda: {"anime_count": 0, "credit_count": 0})
    )

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime:
            continue
        season = _infer_season(anime)
        if not season:
            continue

        season_anime[season].add(c.anime_id)
        season_credits[season].append(c)
        season_roles[season][c.role.value] += 1

        if anime.year:
            ys = year_season[anime.year][season]
            ys["credit_count"] += 1
            # anime_count updated separately below

    # Count anime per year-season (pre-aggregate to avoid O(n²))
    year_season_anime: dict[int, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        season = _infer_season(anime)
        if season:
            year_season_anime[anime.year][season].add(c.anime_id)

    for year, seasons in year_season_anime.items():
        for season, anime_ids in seasons.items():
            year_season[year][season]["anime_count"] = len(anime_ids)

    if not season_anime:
        return {
            "by_season": {},
            "by_year_season": {},
            "role_by_season": {},
            "total_with_season": 0,
        }

    by_season = {}
    for season in sorted(season_anime.keys(), key=lambda s: SEASON_ORDER.get(s, 99)):
        anime_ids = season_anime[season]
        creds = season_credits[season]
        persons = {c.person_id for c in creds}

        anime_scores = [
            anime_map[aid].score
            for aid in anime_ids
            if anime_map[aid].score is not None
        ]
        p_scores = []
        if person_scores:
            p_scores = [person_scores[pid] for pid in persons if pid in person_scores]

        entry: dict = {
            "anime_count": len(anime_ids),
            "credit_count": len(creds),
            "person_count": len(persons),
        }
        if anime_scores:
            entry["avg_anime_score"] = round(sum(anime_scores) / len(anime_scores), 2)
        if p_scores:
            entry["avg_person_score"] = round(sum(p_scores) / len(p_scores), 2)

        by_season[season] = entry

    # Year-season matrix
    by_year_season = {}
    for year in sorted(year_season.keys()):
        by_year_season[year] = {s: dict(v) for s, v in year_season[year].items()}

    total_with_season = sum(len(v) for v in season_credits.values())

    result = {
        "by_season": by_season,
        "by_year_season": by_year_season,
        "role_by_season": {s: dict(v) for s, v in season_roles.items()},
        "total_with_season": total_with_season,
    }

    logger.info("seasonal_trends_complete", seasons=len(by_season))
    return result


def compute_person_seasonal_activity(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, dict]:
    """Compute seasonal activity patterns per person.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime}

    Returns:
        {person_id: {
            primary_season: str | None,  # most active season
            season_distribution: {season: credit_count},
            works_per_season: {season: int},  # unique anime per season
            active_seasons: int,  # how many seasons they're active in
        }}
    """
    if not credits:
        return {}

    # aggregate credits by season per person
    person_season_credits: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    person_season_anime: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime:
            continue
        season = _infer_season(anime)
        if not season:
            continue
        person_season_credits[c.person_id][season] += 1
        person_season_anime[c.person_id][season].add(c.anime_id)

    results: dict[str, dict] = {}
    for person_id, season_dist in person_season_credits.items():
        # most active season
        primary_season = max(season_dist, key=season_dist.get) if season_dist else None  # type: ignore[arg-type]

        works_per_season = {
            s: len(aids) for s, aids in person_season_anime[person_id].items()
        }

        results[person_id] = {
            "primary_season": primary_season,
            "season_distribution": dict(season_dist),
            "works_per_season": works_per_season,
            "active_seasons": len(season_dist),
        }

    logger.info("person_seasonal_activity_computed", person_count=len(results))
    return results
