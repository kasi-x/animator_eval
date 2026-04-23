"""Collaboration diversity — quantify the diversity of a person's collaborators.

同じ人とばかり仕事をしている人 vs 多種多様な人と仕事をしている人を区別する。
Shannon entropy ベースの多様性指標を計算。
"""

import math
from collections import defaultdict

import structlog

from src.runtime.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


def compute_collab_diversity(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, dict]:
    """Compute per-person collaboration diversity.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime} マッピング

    Returns:
        {person_id: {unique_collaborators, diversity_index, repeat_rate, ...}}
    """
    if not credits:
        return {}

    # Build who worked with whom (per anime)
    anime_persons: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        anime_persons[c.anime_id].add(c.person_id)

    # Count collaborator frequency per person
    person_collab_freq: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for persons in anime_persons.values():
        plist = list(persons)
        for i, p1 in enumerate(plist):
            for j, p2 in enumerate(plist):
                if i != j:
                    person_collab_freq[p1][p2] += 1

    result: dict[str, dict] = {}

    for pid, collab_counts in person_collab_freq.items():
        if not collab_counts:
            continue

        unique_collabs = len(collab_counts)
        total_interactions = sum(collab_counts.values())

        # Shannon entropy for diversity
        entropy = 0.0
        for count in collab_counts.values():
            if count > 0:
                p = count / total_interactions
                entropy -= p * math.log2(p)

        # Normalize entropy (max entropy = log2(unique_collabs))
        max_entropy = math.log2(unique_collabs) if unique_collabs > 1 else 1
        normalized_entropy = round(entropy / max_entropy, 3) if max_entropy > 0 else 0

        # Repeat rate: % of collaborators worked with 2+ times
        repeat_collabs = sum(1 for c in collab_counts.values() if c >= 2)
        repeat_rate = (
            round(repeat_collabs / unique_collabs * 100, 1) if unique_collabs > 0 else 0
        )

        # Top collaborator concentration
        top_count = max(collab_counts.values())
        top_concentration = round(top_count / total_interactions * 100, 1)

        result[pid] = {
            "unique_collaborators": unique_collabs,
            "total_interactions": total_interactions,
            "diversity_index": normalized_entropy,
            "repeat_rate": repeat_rate,
            "top_collaborator_concentration": top_concentration,
            "diversity_score": round(normalized_entropy * 100, 1),
        }

    logger.info("collab_diversity_computed", persons=len(result))
    return result
