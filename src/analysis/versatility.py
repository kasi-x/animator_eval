"""役職多様性スコア — 複数の役割カテゴリでの活躍度を測定する.

異なる職種を経験している人物はより汎用的な能力を持つとみなし、
versatility_score で評価する。
"""

from collections import defaultdict

import structlog

from src.models import Credit, Role

logger = structlog.get_logger()

# 役職カテゴリマッピング
ROLE_CATEGORY: dict[Role, str] = {
    Role.DIRECTOR: "direction",
    Role.EPISODE_DIRECTOR: "direction",
    Role.STORYBOARD: "direction",
    Role.CHIEF_ANIMATION_DIRECTOR: "animation_supervision",
    Role.ANIMATION_DIRECTOR: "animation_supervision",
    Role.CHARACTER_DESIGNER: "design",
    Role.MECHANICAL_DESIGNER: "design",
    Role.ART_DIRECTOR: "design",
    Role.COLOR_DESIGNER: "design",
    Role.KEY_ANIMATOR: "animation",
    Role.SECOND_KEY_ANIMATOR: "animation",
    Role.IN_BETWEEN: "animation",
    Role.LAYOUT: "animation",
    Role.EFFECTS: "technical",
    Role.CGI_DIRECTOR: "technical",
    Role.PHOTOGRAPHY_DIRECTOR: "technical",
    Role.BACKGROUND_ART: "art",
    Role.SOUND_DIRECTOR: "sound",
    Role.MUSIC: "sound",
    Role.SERIES_COMPOSITION: "writing",
    Role.SCREENPLAY: "writing",
    Role.ORIGINAL_CREATOR: "writing",
    Role.PRODUCER: "production",
}


def compute_versatility(
    credits: list[Credit],
    person_ids: set[str] | None = None,
) -> dict[str, dict]:
    """人物ごとの役職多様性を計算する.

    Args:
        credits: クレジットリスト
        person_ids: 対象人物ID (None = 全員)

    Returns:
        {person_id: {
            "categories": [str],
            "category_count": int,
            "roles": [str],
            "role_count": int,
            "versatility_score": float,  # 0-100
            "category_credits": {category: count},
        }}
    """
    person_roles: dict[str, set[Role]] = defaultdict(set)
    person_categories: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for c in credits:
        if person_ids and c.person_id not in person_ids:
            continue
        person_roles[c.person_id].add(c.role)
        cat = ROLE_CATEGORY.get(c.role, "other")
        person_categories[c.person_id][cat] += 1

    results = {}
    for pid in person_roles:
        roles = person_roles[pid]
        categories = set(ROLE_CATEGORY.get(r, "other") for r in roles)
        cat_credits = dict(person_categories[pid])

        # Versatility score: based on unique categories (max ~8 categories)
        # Score = min(categories / 4, 1) * 100 → 4+ categories = 100
        n_cats = len(categories)
        versatility = min(n_cats / 4.0, 1.0) * 100

        results[pid] = {
            "categories": sorted(categories),
            "category_count": n_cats,
            "roles": sorted(r.value for r in roles),
            "role_count": len(roles),
            "versatility_score": round(versatility, 1),
            "category_credits": cat_credits,
        }

    logger.info("versatility_computed", persons=len(results))
    return results
