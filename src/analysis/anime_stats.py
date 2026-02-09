"""アニメ統計 — 作品ごとのスタッフ統計を算出する.

各作品について:
- クレジット数
- ユニーク人数
- 役職分布
- 参加者の平均スコア（パイプライン実行後に利用可能）
を算出する。
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


def compute_anime_stats(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
) -> dict[str, dict]:
    """作品ごとの統計情報を算出する.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        person_scores: {person_id: composite_score} (optional)

    Returns:
        {anime_id: {
            title, year, score,
            credit_count, unique_persons, role_distribution,
            avg_person_score, top_persons,
        }}
    """
    if not credits:
        return {}

    # クレジットをアニメIDでグループ化
    anime_credits: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        anime_credits[c.anime_id].append(c)

    results = {}
    for anime_id, acredits in anime_credits.items():
        anime = anime_map.get(anime_id)
        if not anime:
            continue

        person_ids = {c.person_id for c in acredits}
        role_dist: dict[str, int] = defaultdict(int)
        for c in acredits:
            role_dist[c.role.value] += 1

        entry: dict = {
            "title": anime.title_ja or anime.title_en or anime_id,
            "year": anime.year,
            "score": anime.score,
            "credit_count": len(acredits),
            "unique_persons": len(person_ids),
            "role_distribution": dict(sorted(role_dist.items(), key=lambda x: x[1], reverse=True)),
        }

        if person_scores:
            scored = [person_scores[pid] for pid in person_ids if pid in person_scores]
            if scored:
                entry["avg_person_score"] = round(sum(scored) / len(scored), 2)
                # Top 5 contributors by composite score
                top = sorted(
                    [(pid, person_scores[pid]) for pid in person_ids if pid in person_scores],
                    key=lambda x: x[1],
                    reverse=True,
                )[:5]
                entry["top_persons"] = [{"person_id": pid, "composite": s} for pid, s in top]

        results[anime_id] = entry

    logger.info("anime_stats_computed", anime_count=len(results))
    return results
