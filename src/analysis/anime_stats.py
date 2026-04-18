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
        person_scores: {person_id: iv_score} (optional)

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
            "score": getattr(anime, "score", None),  # display-only
            "credit_count": len(acredits),
            "unique_persons": len(person_ids),
            "role_distribution": dict(
                sorted(role_dist.items(), key=lambda x: x[1], reverse=True)
            ),
        }

        if person_scores:
            scored = [person_scores[pid] for pid in person_ids if pid in person_scores]
            if scored:
                entry["avg_person_score"] = round(sum(scored) / len(scored), 2)
                # Top 5 contributors by iv_score
                top = sorted(
                    [
                        (pid, person_scores[pid])
                        for pid in person_ids
                        if pid in person_scores
                    ],
                    key=lambda x: x[1],
                    reverse=True,
                )[:5]
                entry["top_persons"] = [
                    {"person_id": pid, "iv_score": s} for pid, s in top
                ]

        results[anime_id] = entry

    logger.info("anime_stats_computed", anime_count=len(results))
    return results


def compute_person_anime_stats(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, dict]:
    """人物ごとのアニメ参加統計を算出する.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime

    Returns:
        {person_id: {
            total_works: int,
            total_credits: int,
            role_distribution: {role: count},
            avg_anime_score: float | None,
            year_range: [first_year, latest_year],
            studios: list[str],  # unique studios
            top_works: list[{title, year, score, role}],  # top 5 by anime score
        }}
    """
    if not credits:
        return {}

    # クレジットを人物IDでグループ化
    person_credits: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        person_credits[c.person_id].append(c)

    results: dict[str, dict] = {}
    for person_id, pcredits in person_credits.items():
        anime_ids = {c.anime_id for c in pcredits}

        # 役職分布
        role_dist: dict[str, int] = defaultdict(int)
        for c in pcredits:
            role_dist[c.role.value] += 1

        # アニメスコアの平均
        scores = [
            anime_map[aid].score
            for aid in anime_ids
            if aid in anime_map and anime_map[aid].score is not None
        ]
        avg_score = round(sum(scores) / len(scores), 2) if scores else None

        # 年範囲
        years = [
            anime_map[aid].year
            for aid in anime_ids
            if aid in anime_map and anime_map[aid].year is not None
        ]
        year_range = [min(years), max(years)] if years else []

        # ユニークスタジオ
        studios = sorted(
            {
                anime_map[aid].studio
                for aid in anime_ids
                if aid in anime_map and anime_map[aid].studio
            }
        )

        # Top 5 works by anime score
        work_entries = []
        for c in pcredits:
            anime = anime_map.get(c.anime_id)
            if anime:
                work_entries.append(
                    {
                        "title": anime.title_ja or anime.title_en or c.anime_id,
                        "year": anime.year,
                        "score": getattr(anime, "score", None),  # display-only
                        "role": c.role.value,
                    }
                )
        # 重複排除（同一作品で複数役職の場合は最初のものを採用）
        seen_anime: set[str] = set()
        unique_works = []
        for w in work_entries:
            if w["title"] not in seen_anime:
                seen_anime.add(w["title"])
                unique_works.append(w)
        top_works = sorted(
            unique_works,
            key=lambda w: w["score"] if w["score"] is not None else -1,
            reverse=True,
        )[:5]

        results[person_id] = {
            "total_works": len(anime_ids),
            "total_credits": len(pcredits),
            "role_distribution": dict(
                sorted(role_dist.items(), key=lambda x: x[1], reverse=True)
            ),
            "avg_anime_score": avg_score,
            "year_range": year_range,
            "studios": studios,
            "top_works": top_works,
        }

    logger.info("person_anime_stats_computed", person_count=len(results))
    return results
