"""役職多様性スコア — 複数の役割カテゴリでの活躍度を測定する.

異なる職種を経験している人物はより汎用的な能力を持つとみなし、
versatility_score で評価する。
"""

from collections import defaultdict

import structlog

from src.analysis.protocols import VersatilityMetrics
from src.models import Credit, Role
from src.utils.role_groups import ROLE_CATEGORY

logger = structlog.get_logger()


def compute_versatility(
    credits: list[Credit],
    person_ids: set[str] | None = None,
) -> dict[str, VersatilityMetrics]:
    """人物ごとの役職多様性を計算する.

    Args:
        credits: クレジットリスト
        person_ids: 対象人物ID (None = 全員)

    Returns:
        Dict mapping person_id to VersatilityMetrics dataclass with:
        - categories: List of role categories worked in
        - category_count: Number of distinct categories
        - roles: List of specific roles worked
        - role_count: Number of distinct roles
        - versatility_score: 0-100 score (4+ categories = 100)
        - category_credits: Dict mapping category to credit count
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

        results[pid] = VersatilityMetrics(
            categories=sorted(categories),
            category_count=n_cats,
            roles=sorted(r.value for r in roles),
            role_count=len(roles),
            versatility_score=round(versatility, 1),
            category_credits=cat_credits,
        )

    logger.info("versatility_computed", persons=len(results))
    return results
