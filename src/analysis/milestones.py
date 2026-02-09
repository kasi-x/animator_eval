"""キャリアマイルストーン — 重要なキャリアイベントの検出.

クレジットデータから、役職昇進、初監督作品、最高評価作品への参加など、
キャリア上の重要マイルストーンを自動検出する。
"""

import structlog

from src.analysis.career import CAREER_STAGE
from src.models import Anime, Credit, Role

logger = structlog.get_logger()


def compute_milestones(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_id: str | None = None,
) -> dict[str, list[dict]]:
    """人物ごとのキャリアマイルストーンを計算する.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime} マッピング
        person_id: 特定人物のみ (None で全員)

    Returns:
        {person_id: [milestone_event, ...]}
    """
    if not credits:
        return {}

    # Filter credits
    target_credits = credits
    if person_id:
        target_credits = [c for c in credits if c.person_id == person_id]

    # Group credits by person and sort by year
    from collections import defaultdict

    person_credits: dict[str, list[tuple[int, Credit]]] = defaultdict(list)
    for c in target_credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            person_credits[c.person_id].append((anime.year, c))

    # Sort each person's credits by year
    for pid in person_credits:
        person_credits[pid].sort(key=lambda x: x[0])

    # Find the highest-scored anime
    best_score = 0.0
    best_anime_id = None
    for aid, anime in anime_map.items():
        if anime.score and anime.score > best_score:
            best_score = anime.score
            best_anime_id = aid

    all_milestones: dict[str, list[dict]] = {}

    for pid, year_credits in person_credits.items():
        milestones = []
        seen_roles: set[Role] = set()
        seen_stages: set[int] = set()
        highest_stage = 0

        for year, credit in year_credits:
            anime = anime_map.get(credit.anime_id)
            anime_title = anime.title_en if anime else credit.anime_id

            # First credit ever
            if len(milestones) == 0 and not seen_roles:
                milestones.append({
                    "type": "career_start",
                    "year": year,
                    "anime_id": credit.anime_id,
                    "anime_title": anime_title,
                    "description": f"初クレジット: {anime_title}",
                })

            # New role
            if credit.role not in seen_roles:
                seen_roles.add(credit.role)
                if len(seen_roles) > 1:  # Skip the very first role
                    milestones.append({
                        "type": "new_role",
                        "year": year,
                        "role": credit.role.value,
                        "anime_id": credit.anime_id,
                        "anime_title": anime_title,
                        "description": f"新役職 {credit.role.value}: {anime_title}",
                    })

            # Stage promotion
            stage = CAREER_STAGE.get(credit.role, 0)
            if stage > highest_stage:
                if highest_stage > 0:  # Skip the first stage
                    milestones.append({
                        "type": "promotion",
                        "year": year,
                        "from_stage": highest_stage,
                        "to_stage": stage,
                        "role": credit.role.value,
                        "anime_id": credit.anime_id,
                        "anime_title": anime_title,
                        "description": f"昇進 Stage {highest_stage}→{stage}: {credit.role.value}",
                    })
                highest_stage = stage
                seen_stages.add(stage)

            # Participation in highest-scored anime
            if credit.anime_id == best_anime_id and best_anime_id is not None:
                milestones.append({
                    "type": "top_anime",
                    "year": year,
                    "anime_id": credit.anime_id,
                    "anime_title": anime_title,
                    "score": best_score,
                    "description": f"最高評価作品参加: {anime_title} (score: {best_score})",
                })

            # First director credit
            if credit.role == Role.DIRECTOR and "first_director" not in {
                m["type"] for m in milestones
            }:
                milestones.append({
                    "type": "first_director",
                    "year": year,
                    "anime_id": credit.anime_id,
                    "anime_title": anime_title,
                    "description": f"初監督: {anime_title}",
                })

        # Prolific milestone: 10+ credits
        credit_count = len(year_credits)
        if credit_count >= 10:
            milestones.append({
                "type": "prolific",
                "total_credits": credit_count,
                "description": f"多数参加: {credit_count}作品",
            })

        all_milestones[pid] = sorted(
            milestones, key=lambda m: m.get("year", 9999)
        )

    logger.info("milestones_computed", persons=len(all_milestones))
    return all_milestones
