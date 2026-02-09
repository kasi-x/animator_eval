"""時系列分析 — 業界全体の年次統計推移を分析する.

年ごとの:
- アクティブ人物数
- 新規参入者数
- 平均スコアの推移
- 役職分布の変化
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


def compute_time_series(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict:
    """年次の時系列統計を算出する.

    Returns:
        {
            "years": [int],
            "series": {
                "active_persons": {year: count},
                "new_entrants": {year: count},
                "credit_count": {year: count},
                "avg_anime_score": {year: float},
                "unique_anime": {year: count},
            },
            "summary": {
                "peak_year": int,
                "peak_credits": int,
                "growth_rate": float,
            },
        }
    """
    # Build year → data
    year_persons: dict[int, set[str]] = defaultdict(set)
    year_credits: dict[int, int] = defaultdict(int)
    year_anime: dict[int, set[str]] = defaultdict(set)
    year_scores: dict[int, list[float]] = defaultdict(list)

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        year = anime.year
        year_persons[year].add(c.person_id)
        year_credits[year] += 1
        year_anime[year].add(c.anime_id)
        if anime.score:
            year_scores[year].append(anime.score)

    if not year_persons:
        return {"years": [], "series": {}, "summary": {}}

    years = sorted(year_persons.keys())

    # Track first appearance year for each person
    person_first_year: dict[str, int] = {}
    for year in years:
        for pid in year_persons[year]:
            if pid not in person_first_year:
                person_first_year[pid] = year

    # Build series
    active_persons = {y: len(year_persons[y]) for y in years}
    new_entrants = {
        y: sum(1 for pid in year_persons[y] if person_first_year.get(pid) == y)
        for y in years
    }
    credit_count = {y: year_credits[y] for y in years}
    avg_anime_score = {
        y: round(sum(year_scores[y]) / len(year_scores[y]), 2)
        for y in years if year_scores[y]
    }
    unique_anime = {y: len(year_anime[y]) for y in years}

    # Summary
    peak_year = max(years, key=lambda y: year_credits[y])
    peak_credits = year_credits[peak_year]

    # Growth rate (last 5 years vs previous 5 years)
    growth_rate = 0.0
    if len(years) >= 10:
        recent_5 = sum(year_credits.get(y, 0) for y in years[-5:])
        prev_5 = sum(year_credits.get(y, 0) for y in years[-10:-5])
        if prev_5 > 0:
            growth_rate = round((recent_5 - prev_5) / prev_5 * 100, 1)

    result = {
        "years": years,
        "series": {
            "active_persons": active_persons,
            "new_entrants": new_entrants,
            "credit_count": credit_count,
            "avg_anime_score": avg_anime_score,
            "unique_anime": unique_anime,
        },
        "summary": {
            "peak_year": peak_year,
            "peak_credits": peak_credits,
            "growth_rate": growth_rate,
            "total_years": len(years),
            "total_unique_persons": len(person_first_year),
        },
    }

    logger.info("time_series_computed", years=len(years))
    return result
