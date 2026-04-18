"""ジャンル親和性 — 人物のジャンル傾向分析.

各人物がどのジャンル（アニメのスコア帯・年代帯）で多く仕事をしているかを分析する。
（MAL/AniListのジャンルタグが利用可能になるまでは、スコア帯・年代帯で代用）
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


def _score_tier(score: float | None) -> str:
    """スコアティア — 表示用のみ（スコア計算には使わない）."""
    if score is None:
        return "unknown"
    if score >= 8.0:
        return "high_rated"
    if score >= 6.5:
        return "mid_rated"
    return "low_rated"


def _era(year: int | None) -> str:
    """年を時代に分類."""
    if year is None:
        return "unknown"
    if year >= 2020:
        return "modern"
    if year >= 2010:
        return "2010s"
    if year >= 2000:
        return "2000s"
    return "classic"


def compute_genre_affinity(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, dict]:
    """人物ごとのジャンル親和性を計算する.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime} マッピング

    Returns:
        {person_id: {score_tiers: {...}, eras: {...}, primary_tier, primary_era}}
    """
    if not credits:
        return {}

    # Group credits by person
    person_credits: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        person_credits[c.person_id].append(c)

    result: dict[str, dict] = {}

    for pid, pcreds in person_credits.items():
        tier_counts: dict[str, int] = defaultdict(int)
        era_counts: dict[str, int] = defaultdict(int)
        scores: list[float] = []

        for c in pcreds:
            anime = anime_map.get(c.anime_id)
            if anime:
                _disp = getattr(anime, "score", None)  # display-only
                tier = _score_tier(_disp)
                era = _era(anime.year)
                tier_counts[tier] += 1
                era_counts[era] += 1
                if _disp is not None:
                    scores.append(_disp)  # display-only — informational metadata

        total = len(pcreds)
        if total == 0:
            continue

        # Normalize to percentages
        tier_pct = {k: round(v / total * 100, 1) for k, v in tier_counts.items()}
        era_pct = {k: round(v / total * 100, 1) for k, v in era_counts.items()}

        # Primary affinity
        primary_tier = (
            max(tier_counts, key=tier_counts.get) if tier_counts else "unknown"
        )
        primary_era = max(era_counts, key=era_counts.get) if era_counts else "unknown"

        result[pid] = {
            "score_tiers": dict(tier_pct),
            "eras": dict(era_pct),
            "primary_tier": primary_tier,
            "primary_era": primary_era,
            "avg_anime_score": round(sum(scores) / len(scores), 2)
            if scores
            else None,  # display-only
            "total_credits": total,
        }

    logger.info("genre_affinity_computed", persons=len(result))
    return result
