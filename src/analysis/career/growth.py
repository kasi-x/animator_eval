"""Growth prediction — analyse each person's growth tendency based on career trends.

    キャリアデータと過去クレジット密度から、
各人物の成長トレンドを推定する。
"""

import structlog

from src.analysis.protocols import GrowthMetrics
from src.runtime.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


def compute_growth_trends(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
    window: int = 3,
) -> dict[str, GrowthMetrics]:
    """Compute the growth trend for each person.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        person_scores: {person_id: composite_score}
        window: 直近何年分を「最近」とみなすか

    Returns:
        Dict mapping person_id to GrowthMetrics dataclass with trend analysis
    """
    from collections import defaultdict

    # Build year → credits per person
    person_years: dict[str, dict[int, list]] = defaultdict(lambda: defaultdict(list))

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            person_years[c.person_id][anime.year].append(
                {"anime_id": c.anime_id, "role": c.role.value}
            )

    if not person_years:
        return {}

    # Find global year range
    all_years = set()
    for yearly in person_years.values():
        all_years.update(yearly.keys())
    if not all_years:
        return {}

    max_year = max(all_years)
    recent_years = set(range(max_year - window + 1, max_year + 1))

    results = {}
    for pid, yearly in person_years.items():
        years = sorted(yearly.keys())
        if not years:
            continue

        yearly_counts = {y: len(entries) for y, entries in yearly.items()}

        total_credits = sum(yearly_counts.values())
        recent_credits = sum(
            cnt for y, cnt in yearly_counts.items() if y in recent_years
        )
        early_credits = sum(
            cnt for y, cnt in yearly_counts.items() if y not in recent_years
        )

        total_years = len(years)
        career_span = years[-1] - years[0] + 1 if len(years) > 1 else 1

        # Trend determination
        if total_years < 2:
            trend = "new"
        elif recent_credits == 0:
            trend = "inactive"
        else:
            recent_avg = recent_credits / min(window, career_span)
            early_avg = (
                early_credits / max(career_span - window, 1)
                if career_span > window
                else recent_avg
            )

            if recent_avg > early_avg * 1.3:
                trend = "rising"
            elif recent_avg < early_avg * 0.5:
                trend = "declining"
            else:
                trend = "stable"

        results[pid] = GrowthMetrics(
            yearly_credits=dict(sorted(yearly_counts.items())),
            trend=trend,
            total_credits=total_credits,
            recent_credits=recent_credits,
            total_years=total_years,
            career_span=career_span,
            activity_ratio=round(recent_credits / max(total_credits, 1), 3),
            recent_avg_anime_score=None,
            career_avg_anime_score=None,
            current_score=person_scores.get(pid) if person_scores else None,
        )

    # Compute summary stats
    trend_counts = defaultdict(int)
    for r in results.values():
        trend_counts[r.trend] += 1

    logger.info(
        "growth_trends_computed",
        persons=len(results),
        rising=trend_counts.get("rising", 0),
        stable=trend_counts.get("stable", 0),
        declining=trend_counts.get("declining", 0),
        inactive=trend_counts.get("inactive", 0),
    )
    return results
