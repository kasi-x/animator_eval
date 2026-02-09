"""季節トレンド分析 — シーズンごとのクレジットパターンを分析する.

アニメの放送シーズン（冬/春/夏/秋）ごとに、
人材の参加パターンと役職分布を計算する。
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()

SEASON_ORDER = {"winter": 0, "spring": 1, "summer": 2, "fall": 3}


def _infer_season(anime: Anime) -> str | None:
    """アニメのシーズンを推定する."""
    if anime.season:
        return anime.season.lower()
    return None


def compute_seasonal_trends(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
) -> dict:
    """シーズンごとのトレンド分析.

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

    # Count anime per year-season
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        season = _infer_season(anime)
        if season:
            year_season[anime.year][season]["anime_count"] = len(
                {cr.anime_id for cr in season_credits[season]
                 if anime_map.get(cr.anime_id) and anime_map[cr.anime_id].year == anime.year}
            )

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
            anime_map[aid].score for aid in anime_ids
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
        by_year_season[year] = {
            s: dict(v) for s, v in year_season[year].items()
        }

    total_with_season = sum(len(v) for v in season_credits.values())

    result = {
        "by_season": by_season,
        "by_year_season": by_year_season,
        "role_by_season": {s: dict(v) for s, v in season_roles.items()},
        "total_with_season": total_with_season,
    }

    logger.info("seasonal_trends_complete", seasons=len(by_season))
    return result
