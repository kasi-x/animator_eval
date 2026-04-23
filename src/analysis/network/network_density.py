"""Network density metrics — compute local network characteristics per person.

各人物について:
- collaborator_count: ユニーク共同作業者数
- avg_collaborator_score: 共同作業者の平均スコア
- collaboration_diversity: 異なるアニメ作品で共同した割合
- hub_score: (collaborator_count / max_collaborator_count) * 100
"""

from collections import defaultdict

import structlog

from src.analysis.protocols import NetworkDensityMetrics
from src.runtime.models import Credit

logger = structlog.get_logger()


def compute_network_density(
    credits: list[Credit],
    person_scores: dict[str, float] | None = None,
) -> dict[str, NetworkDensityMetrics]:
    """Compute network density for each person.

    Args:
        credits: クレジットリスト
        person_scores: {person_id: composite_score}

    Returns:
        Dict mapping person_id to NetworkDensityMetrics dataclass
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

        # Calculate average collaborator score
        avg_collab_score = None
        if person_scores:
            collab_scores = [person_scores[c] for c in collabs if c in person_scores]
            if collab_scores:
                avg_collab_score = round(sum(collab_scores) / len(collab_scores), 2)

        results[pid] = NetworkDensityMetrics(
            collaborator_count=len(collabs),
            unique_anime=unique_anime,
            hub_score=round(len(collabs) / max_collabs * 100, 1)
            if max_collabs > 0
            else 0,
            avg_collaborator_score=avg_collab_score,
        )

    logger.info("network_density_computed", persons=len(results))
    return results
