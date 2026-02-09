"""キャリア分析 — 人物の経歴タイムラインと役職遷移を分析する.

個人のキャリアを時系列で追跡し、
- 活動開始・最新年
- 年ごとの活動量
- 役職の変遷（動画→原画→作画監督 等）
を算出する。
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit, Role

logger = structlog.get_logger()

# 役職のキャリアステージ順序（低→高）
CAREER_STAGE = {
    Role.IN_BETWEEN: 1,
    Role.SECOND_KEY_ANIMATOR: 2,
    Role.LAYOUT: 2,
    Role.KEY_ANIMATOR: 3,
    Role.EFFECTS: 3,
    Role.ANIMATION_DIRECTOR: 4,
    Role.CHARACTER_DESIGNER: 4,
    Role.STORYBOARD: 4,
    Role.CHIEF_ANIMATION_DIRECTOR: 5,
    Role.EPISODE_DIRECTOR: 5,
    Role.DIRECTOR: 6,
}


def analyze_career(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict:
    """特定人物のキャリアタイムラインを分析する.

    Returns:
        {
            "first_year": int | None,
            "latest_year": int | None,
            "active_years": int,
            "total_credits": int,
            "yearly_activity": {year: count},
            "role_progression": [{year: int, role: str, stage: int}],
            "highest_stage": int,
            "highest_roles": [str],
        }
    """
    person_credits = [c for c in credits if c.person_id == person_id]
    if not person_credits:
        return {
            "first_year": None, "latest_year": None, "active_years": 0,
            "total_credits": 0, "yearly_activity": {}, "role_progression": [],
            "highest_stage": 0, "highest_roles": [],
        }

    yearly_activity: dict[int, int] = defaultdict(int)
    role_by_year: dict[int, set[Role]] = defaultdict(set)
    years_seen = set()

    for c in person_credits:
        anime = anime_map.get(c.anime_id)
        year = anime.year if anime and anime.year else None
        if year:
            yearly_activity[year] += 1
            role_by_year[year].add(c.role)
            years_seen.add(year)

    first_year = min(years_seen) if years_seen else None
    latest_year = max(years_seen) if years_seen else None
    active_years = len(years_seen)

    # 役職遷移: 年ごとの最高ステージの役職を記録
    role_progression = []
    for year in sorted(role_by_year):
        roles = role_by_year[year]
        best_role = max(roles, key=lambda r: CAREER_STAGE.get(r, 0))
        role_progression.append({
            "year": year,
            "role": best_role.value,
            "stage": CAREER_STAGE.get(best_role, 0),
        })

    # 最高到達ステージ
    all_stages = [CAREER_STAGE.get(c.role, 0) for c in person_credits]
    highest_stage = max(all_stages) if all_stages else 0
    highest_roles = sorted({
        c.role.value for c in person_credits
        if CAREER_STAGE.get(c.role, 0) == highest_stage
    })

    # Peak year (most credits)
    peak_year = None
    peak_credits = 0
    for y, cnt in yearly_activity.items():
        if cnt > peak_credits:
            peak_credits = cnt
            peak_year = y

    return {
        "first_year": first_year,
        "latest_year": latest_year,
        "active_years": active_years,
        "total_credits": len(person_credits),
        "yearly_activity": dict(sorted(yearly_activity.items())),
        "role_progression": role_progression,
        "highest_stage": highest_stage,
        "highest_roles": highest_roles,
        "peak_year": peak_year,
        "peak_credits": peak_credits,
    }


def batch_career_analysis(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_ids: set[str] | None = None,
) -> dict[str, dict]:
    """複数人物のキャリア分析を一括実行する.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        person_ids: 対象人物ID (None = 全員)

    Returns:
        {person_id: career_analysis_result}
    """
    if person_ids is None:
        person_ids = {c.person_id for c in credits}

    results = {}
    for pid in person_ids:
        results[pid] = analyze_career(pid, credits, anime_map)

    logger.info("career_analysis_complete", persons=len(results))
    return results
