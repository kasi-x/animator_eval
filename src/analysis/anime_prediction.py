"""アニメ予測 — チーム構成からアニメスコアを推定する.

過去の作品データを基に、特定のチーム構成での
期待される作品スコアを推定する。
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
    """チーム構成から期待される作品スコアを推定する.

    予測手法: チームメンバーの過去作品の加重平均スコア。
    メンバー全員が参加した作品はボーナス加重。

    Args:
        team_person_ids: チームメンバーID
        credits: 全クレジット
        anime_map: anime_id → Anime
        person_scores: {person_id: composite_score}

    Returns:
        {
            "predicted_score": float | None,
            "confidence": str,
            "basis_anime_count": int,
            "team_avg_score": float | None,
            "historical_range": {min, max, avg},
            "similar_teams": [{anime_id, title, score, overlap_count}],
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

    # Score each relevant anime by overlap with team
    anime_overlap: list[dict] = []
    for anime_id, participants in anime_persons.items():
        overlap = participants & team_set
        if not overlap:
            continue
        anime = anime_map.get(anime_id)
        if not anime or not anime.score:
            continue

        anime_overlap.append({
            "anime_id": anime_id,
            "title": anime.display_title,
            "year": anime.year,
            "score": anime.score,
            "overlap_count": len(overlap),
            "overlap_ratio": len(overlap) / len(team_set),
        })

    if not anime_overlap:
        return {
            "predicted_score": None,
            "confidence": "none",
            "basis_anime_count": 0,
        }

    # Weighted average: more overlap = more weight
    total_weight = 0
    weighted_sum = 0
    scores = []
    for ao in anime_overlap:
        weight = ao["overlap_ratio"] ** 2  # Square for stronger weighting
        weighted_sum += ao["score"] * weight
        total_weight += weight
        scores.append(ao["score"])

    predicted = round(weighted_sum / total_weight, 2) if total_weight > 0 else None

    # Confidence level
    if len(anime_overlap) >= 10 and any(ao["overlap_ratio"] > 0.5 for ao in anime_overlap):
        confidence = "high"
    elif len(anime_overlap) >= 5:
        confidence = "medium"
    else:
        confidence = "low"

    # Team average composite score
    team_avg = None
    if person_scores:
        team_composite = [person_scores[pid] for pid in team_person_ids if pid in person_scores]
        if team_composite:
            team_avg = round(sum(team_composite) / len(team_composite), 2)

    # Historical range
    historical = {
        "min": round(min(scores), 2),
        "max": round(max(scores), 2),
        "avg": round(sum(scores) / len(scores), 2),
    }

    # Top similar teams (highest overlap)
    similar_teams = sorted(anime_overlap, key=lambda x: (-x["overlap_count"], -(x["score"] or 0)))[:10]

    return {
        "predicted_score": predicted,
        "confidence": confidence,
        "basis_anime_count": len(anime_overlap),
        "team_avg_score": team_avg,
        "historical_range": historical,
        "similar_teams": similar_teams,
    }
