"""Productivity metrics — analyse credit density and efficiency.

キャリア年数あたりのクレジット数（生産性指標）を計算する。
高い生産性 = 毎年多くの作品に参加。
"""

from collections import defaultdict

import structlog

from src.analysis.protocols import ProductivityMetrics
from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


def compute_productivity(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, ProductivityMetrics]:
    """Compute productivity metrics for each person.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime} マッピング

    Returns:
        Dict mapping person_id to ProductivityMetrics dataclass
    """
    if not credits:
        return {}

    # Group credits by person
    person_credits: dict[str, list[tuple[int | None, Credit]]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        year = anime.year if anime else None
        person_credits[c.person_id].append((year, c))

    result: dict[str, ProductivityMetrics] = {}

    for pid, year_credits in person_credits.items():
        total = len(year_credits)
        unique_anime = len({c.anime_id for _, c in year_credits})

        # Year-based stats
        years_with_credits: dict[int, int] = defaultdict(int)
        for year, _ in year_credits:
            if year is not None:
                years_with_credits[year] += 1

        if not years_with_credits:
            result[pid] = ProductivityMetrics(
                total_credits=total,
                unique_anime=unique_anime,
                active_years=0,
                career_span=0,
                credits_per_year=0.0,
                peak_year=None,
                peak_credits=0,
                consistency_score=0.0,
            )
            continue

        active_years = len(years_with_credits)
        career_span = max(years_with_credits) - min(years_with_credits) + 1
        credits_per_year = round(total / max(career_span, 1), 2)

        # Peak year
        peak_year = max(years_with_credits, key=years_with_credits.get)
        peak_credits = years_with_credits[peak_year]

        # Consistency: active_years / career_span (1.0 = active every year)
        consistency = round(active_years / career_span, 3) if career_span > 0 else 0

        result[pid] = ProductivityMetrics(
            total_credits=total,
            unique_anime=unique_anime,
            active_years=active_years,
            career_span=career_span,
            credits_per_year=credits_per_year,
            peak_year=peak_year,
            peak_credits=peak_credits,
            consistency_score=consistency,
        )

    logger.info("productivity_computed", persons=len(result))
    return result
