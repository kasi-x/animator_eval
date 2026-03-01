"""アニメ予測 — チーム構成から制作規模を推定する.

過去の作品データを基に、特定のチーム構成での
期待される制作規模を推定する。

Note: anime.score (視聴者評価) は意図的に使用しない。
制作スタッフの貢献度と無関係な要因に左右されるため。
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


def predict_anime_score(
    team_person_ids: list[str],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
) -> dict:
    """チーム構成から期待される制作規模を推定する.

    予測手法: チームメンバーの過去作品の加重平均スタッフ数。
    メンバー全員が参加した作品はボーナス加重。

    Args:
        team_person_ids: チームメンバーID
        credits: 全クレジット
        anime_map: anime_id → Anime
        person_scores: {person_id: composite_score}

    Returns:
        {
            "predicted_score": float | None,  # 予測スタッフ数
            "confidence": str,
            "basis_anime_count": int,
            "team_avg_score": float | None,
            "historical_range": {min, max, avg},
            "similar_teams": [{anime_id, title, staff_count, overlap_count}],
        }
    """
    if not team_person_ids:
        return {
            "predicted_score": None,
            "confidence": "none",
            "basis_anime_count": 0,
        }

    team_set = set(team_person_ids)

    # Find anime where team members participated
    person_anime: dict[str, set[str]] = defaultdict(set)
    anime_persons: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        person_anime[c.person_id].add(c.anime_id)
        anime_persons[c.anime_id].add(c.person_id)

    # Use staff count as structural metric instead of anime.score
    anime_overlap: list[dict] = []
    for anime_id, participants in anime_persons.items():
        overlap = participants & team_set
        if not overlap:
            continue
        anime = anime_map.get(anime_id)
        if not anime:
            continue

        staff_count = len(participants)

        anime_overlap.append(
            {
                "anime_id": anime_id,
                "title": anime.display_title,
                "year": anime.year,
                "score": anime.score,  # kept for display only
                "staff_count": staff_count,
                "overlap_count": len(overlap),
                "overlap_ratio": len(overlap) / len(team_set),
            }
        )

    if not anime_overlap:
        return {
            "predicted_score": None,
            "confidence": "none",
            "basis_anime_count": 0,
        }

    # Weighted average: more overlap = more weight
    total_weight = 0
    weighted_sum = 0
    staff_counts = []
    for ao in anime_overlap:
        weight = ao["overlap_ratio"] ** 2  # Square for stronger weighting
        weighted_sum += ao["staff_count"] * weight
        total_weight += weight
        staff_counts.append(ao["staff_count"])

    predicted = round(weighted_sum / total_weight, 2) if total_weight > 0 else None

    # Confidence level
    if len(anime_overlap) >= 10 and any(
        ao["overlap_ratio"] > 0.5 for ao in anime_overlap
    ):
        confidence = "high"
    elif len(anime_overlap) >= 5:
        confidence = "medium"
    else:
        confidence = "low"

    # Team average composite score
    team_avg = None
    if person_scores:
        team_composite = [
            person_scores[pid] for pid in team_person_ids if pid in person_scores
        ]
        if team_composite:
            team_avg = round(sum(team_composite) / len(team_composite), 2)

    # Historical range
    historical = {
        "min": round(min(staff_counts), 2),
        "max": round(max(staff_counts), 2),
        "avg": round(sum(staff_counts) / len(staff_counts), 2),
    }

    # Top similar teams (highest overlap)
    similar_teams = sorted(
        anime_overlap, key=lambda x: (-x["overlap_count"], -x["staff_count"])
    )[:10]

    return {
        "predicted_score": predicted,
        "confidence": confidence,
        "basis_anime_count": len(anime_overlap),
        "team_avg_score": team_avg,
        "historical_range": historical,
        "similar_teams": similar_teams,
    }
