"""年代分析 — 10年単位でのアニメ業界トレンドを分析する.

各年代 (2000s, 2010s, 2020s 等) について:
- クレジット数・人物数の推移
- 役職分布の変化
- 平均作品スコアの推移
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


def compute_decade_analysis(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
) -> dict:
    """年代別のトレンド分析を実行する.

    Returns:
        {
            "decades": {
                "2000s": {
                    "credit_count": int,
                    "unique_persons": int,
                    "unique_anime": int,
                    "avg_anime_score": float,
                    "role_distribution": {role: count},
                    "top_persons": [{person_id, credits}],
                },
                ...
            },
            "year_by_year": {year: {credits, persons, anime}},
        }
    """
    # Group by year
    year_data: dict[int, dict] = defaultdict(
        lambda: {
            "credits": 0,
            "persons": set(),
            "anime": set(),
            "scores": [],
            "roles": defaultdict(int),
            "person_credits": defaultdict(int),
        }
    )

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue

        year = anime.year
        yd = year_data[year]
        yd["credits"] += 1
        yd["persons"].add(c.person_id)
        yd["anime"].add(c.anime_id)
        if anime.score:
            yd["scores"].append(anime.score)
        yd["roles"][c.role.value] += 1
        yd["person_credits"][c.person_id] += 1

    # Aggregate by decade
    decades: dict[str, dict] = {}
    for year, yd in sorted(year_data.items()):
        decade = f"{(year // 10) * 10}s"
        if decade not in decades:
            decades[decade] = {
                "credit_count": 0,
                "persons": set(),
                "anime": set(),
                "scores": [],
                "role_distribution": defaultdict(int),
                "person_credits": defaultdict(int),
            }
        dd = decades[decade]
        dd["credit_count"] += yd["credits"]
        dd["persons"].update(yd["persons"])
        dd["anime"].update(yd["anime"])
        dd["scores"].extend(yd["scores"])
        for role, cnt in yd["roles"].items():
            dd["role_distribution"][role] += cnt
        for pid, cnt in yd["person_credits"].items():
            dd["person_credits"][pid] += cnt

    # Format decades
    formatted_decades = {}
    for decade, dd in sorted(decades.items()):
        top_persons = sorted(dd["person_credits"].items(), key=lambda x: -x[1])[:10]

        formatted_decades[decade] = {
            "credit_count": dd["credit_count"],
            "unique_persons": len(dd["persons"]),
            "unique_anime": len(dd["anime"]),
            "avg_anime_score": round(sum(dd["scores"]) / len(dd["scores"]), 2)
            if dd["scores"]
            else None,
            "role_distribution": dict(
                sorted(dd["role_distribution"].items(), key=lambda x: -x[1])
            ),
            "top_persons": [
                {"person_id": pid, "credits": cnt} for pid, cnt in top_persons
            ],
        }

    # Year-by-year summary
    year_by_year = {}
    for year, yd in sorted(year_data.items()):
        year_by_year[year] = {
            "credits": yd["credits"],
            "persons": len(yd["persons"]),
            "anime": len(yd["anime"]),
            "avg_score": round(sum(yd["scores"]) / len(yd["scores"]), 2)
            if yd["scores"]
            else None,
        }

    logger.info("decade_analysis_computed", decades=len(formatted_decades))
    return {
        "decades": formatted_decades,
        "year_by_year": year_by_year,
    }
