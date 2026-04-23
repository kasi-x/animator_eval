"""Work impact — identify the works where a person had the most impact.

高評価作品への参加、チーム内での役職の高さ、その人の全キャリアに占める
その作品の重要度を総合的にスコアリングする。
"""

from collections import defaultdict

import structlog

from src.analysis.career import CAREER_STAGE
from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


def compute_work_impact(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
) -> dict[str, list[dict]]:
    """Compute per-person work impact.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime} マッピング
        person_scores: {person_id: composite_score}

    Returns:
        {person_id: [{anime_id, title, year, impact_score, ...}, ...]}
    """
    if not credits:
        return {}

    # Group credits by person
    person_credits: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        person_credits[c.person_id].append(c)

    # Count persons per anime (for relative importance)
    anime_person_count: dict[str, int] = defaultdict(int)
    for c in credits:
        anime_person_count[c.anime_id] += 1

    result: dict[str, list[dict]] = {}

    for pid, pcreds in person_credits.items():
        impacts = []

        for c in pcreds:
            anime = anime_map.get(c.anime_id)
            if not anime:
                continue

            # Factors for impact (structural only — no anime.score)
            # 1. Role stage (higher = more impact, 0-30 points)
            stage = CAREER_STAGE.get(c.role, 0)
            role_factor = min(30, stage * 6)

            # 2. Team share (smaller team = more individual impact, 0-30 points)
            team_size = anime_person_count.get(c.anime_id, 1)
            team_factor = min(30, 30 / max(1, team_size / 5))

            # 3. Production scale (episodes × duration, 0-20 points)
            eps = anime.episodes or 1
            dur = anime.duration or 24
            scale_factor = min(20, 5 * (eps * dur / 24) ** 0.3)

            # 4. Recency bonus (0-20 points)
            if anime.year and anime.year >= 2020:
                recency = 20
            elif anime.year and anime.year >= 2010:
                recency = 10
            else:
                recency = 5

            impact_score = round(role_factor + team_factor + scale_factor + recency, 1)

            impacts.append(
                {
                    "anime_id": c.anime_id,
                    "title": anime.title_en or anime.title_ja or c.anime_id,
                    "year": anime.year,
                    "anime_score": getattr(anime, "score", None),  # display-only
                    "role": c.role.value,
                    "stage": stage,
                    "team_size": team_size,
                    "impact_score": impact_score,
                }
            )

        # Sort by impact score
        impacts.sort(key=lambda x: -x["impact_score"])
        result[pid] = impacts[:20]  # Top 20 works

    logger.info("work_impact_computed", persons=len(result))
    return result
