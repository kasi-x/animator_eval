"""ネットワーク密度指標 — 個人のローカルネットワーク特性を計算する.

各人物について:
- collaborator_count: ユニーク共同作業者数
- avg_collaborator_score: 共同作業者の平均スコア
- collaboration_diversity: 異なるアニメ作品で共同した割合
- hub_score: (collaborator_count / max_collaborator_count) * 100
"""

from collections import defaultdict

import structlog

from src.models import Credit

logger = structlog.get_logger()


def compute_network_density(
    credits: list[Credit],
    person_scores: dict[str, float] | None = None,
) -> dict[str, dict]:
    """人物ごとのネットワーク密度を計算する.

    Args:
        credits: クレジットリスト
        person_scores: {person_id: composite_score}

    Returns:
        {person_id: {
            "collaborator_count": int,
            "avg_collaborator_score": float | None,
            "unique_anime": int,
            "hub_score": float,
        }}
    """
    # Build anime → persons mapping
    anime_persons: dict[str, set[str]] = defaultdict(set)
    person_anime: dict[str, set[str]] = defaultdict(set)

    for c in credits:
        anime_persons[c.anime_id].add(c.person_id)
        person_anime[c.person_id].add(c.anime_id)

    # For each person, find all unique collaborators
    person_collaborators: dict[str, set[str]] = defaultdict(set)
    for anime_id, persons in anime_persons.items():
        for pid in persons:
            person_collaborators[pid].update(persons - {pid})

    if not person_collaborators:
        return {}

    max_collabs = max(len(v) for v in person_collaborators.values())

    results = {}
    for pid in person_collaborators:
        collabs = person_collaborators[pid]
        unique_anime = len(person_anime[pid])

        entry: dict = {
            "collaborator_count": len(collabs),
            "unique_anime": unique_anime,
            "hub_score": round(len(collabs) / max_collabs * 100, 1) if max_collabs > 0 else 0,
        }

        if person_scores:
            collab_scores = [
                person_scores[c] for c in collabs if c in person_scores
            ]
            if collab_scores:
                entry["avg_collaborator_score"] = round(
                    sum(collab_scores) / len(collab_scores), 2
                )

        results[pid] = entry

    logger.info("network_density_computed", persons=len(results))
    return results
