"""人物タグ — スコアとキャリアデータに基づく自動タグ付け.

各人物に対して、その特性を示す自動タグを付与する:
- "veteran" / "newcomer" — キャリア年数
- "rising_star" — 成長トレンド
- "hub" — コラボ数上位
- "specialist" / "generalist" — 役職多様性
- "high_authority" / "high_trust" / "high_skill" — 各軸上位
"""

import structlog

logger = structlog.get_logger()


def compute_person_tags(results: list[dict]) -> dict[str, list[str]]:
    """スコア結果から人物タグを自動生成する.

    Args:
        results: パイプライン結果 (scores.json の内容)

    Returns:
        {person_id: [tag1, tag2, ...]}
    """
    if not results:
        return {}

    # Compute thresholds (percentiles)
    n = len(results)
    if n < 3:
        return {r["person_id"]: [] for r in results}

    def percentile_threshold(axis: str, pct: float) -> float:
        vals = sorted(r.get(axis, 0) for r in results)
        idx = min(int(len(vals) * pct), len(vals) - 1)
        return vals[idx]

    high_auth = percentile_threshold("authority", 0.85)
    high_trust = percentile_threshold("trust", 0.85)
    high_skill = percentile_threshold("skill", 0.85)
    high_composite = percentile_threshold("composite", 0.90)

    tags: dict[str, list[str]] = {}
    for r in results:
        pid = r["person_id"]
        person_tags = []

        # Score-based tags (strict > so equal scores don't all qualify)
        if r.get("authority", 0) > high_auth:
            person_tags.append("high_authority")
        if r.get("trust", 0) > high_trust:
            person_tags.append("high_trust")
        if r.get("skill", 0) > high_skill:
            person_tags.append("high_skill")
        if r.get("composite", 0) > high_composite:
            person_tags.append("top_talent")

        # Career-based tags
        career = r.get("career", {})
        active_years = career.get("active_years", 0)
        if active_years >= 15:
            person_tags.append("veteran")
        elif active_years <= 3 and active_years > 0:
            person_tags.append("newcomer")

        # Growth trend
        growth = r.get("growth", {})
        if growth.get("trend") == "rising":
            person_tags.append("rising_star")
        elif growth.get("trend") == "inactive":
            person_tags.append("inactive")

        # Network density
        network = r.get("network", {})
        hub_score = network.get("hub_score", 0)
        if hub_score >= 80:
            person_tags.append("hub")

        # Versatility
        versatility = r.get("versatility", {})
        v_score = versatility.get("score", 0)
        if v_score >= 75:
            person_tags.append("generalist")
        elif v_score <= 25 and versatility.get("categories", 0) <= 1:
            person_tags.append("specialist")

        # Career progression
        highest_stage = career.get("highest_stage", 0)
        if highest_stage >= 6:
            person_tags.append("director_class")
        elif highest_stage >= 4:
            person_tags.append("senior_staff")

        tags[pid] = person_tags

    # Count tag distribution
    tag_counts: dict[str, int] = {}
    for person_tags in tags.values():
        for tag in person_tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1

    logger.info("person_tags_computed", persons=len(tags), unique_tags=len(tag_counts))
    return tags
