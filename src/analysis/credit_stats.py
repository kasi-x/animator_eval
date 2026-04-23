"""Credit-based statistical analysis — person_id level analysis without requiring full persons table.

This module provides analysis of credits data directly, useful when persons table is incomplete.
Analyzes roles, collaborations, and career patterns at the person_id level.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any

import structlog

from src.runtime.models import Credit

logger = structlog.get_logger()


@dataclass
class CreditStats:
    """Statistical summary of credits data.

    Attributes:
        total_credits: Total number of credit records
        unique_persons: Number of unique person_ids
        unique_anime: Number of unique anime_ids
        avg_credits_per_person: Average credits per person_id
        avg_staff_per_anime: Average staff per anime
        role_distribution: Dict of role -> count
        top_roles: Top 20 roles by frequency
        collaboration_stats: Dict with collaboration statistics
        timeline_stats: Dict with year -> credit counts
    """

    total_credits: int
    unique_persons: int
    unique_anime: int
    avg_credits_per_person: float
    avg_staff_per_anime: float
    role_distribution: dict[str, int]
    top_roles: list[dict[str, Any]]
    collaboration_stats: dict[str, Any]
    timeline_stats: dict[str, Any]
    person_id_stats: dict[str, Any]


def compute_credit_statistics(
    credits: list[Credit],
    anime_map: dict[str, Any],
) -> dict[str, Any]:
    """Compute comprehensive statistics from credits data.

    Args:
        credits: List of credit records
        anime_map: Dict mapping anime_id to anime data

    Returns:
        Dict with credit statistics including:
        - Basic counts (total_credits, unique_persons, unique_anime)
        - Role distribution
        - Top collaborators (person_id pairs)
        - Timeline analysis
        - Person-level statistics
    """
    if not credits:
        logger.warning("no_credits_for_analysis")
        return {}

    # Basic counts
    total_credits = len(credits)
    person_ids = {c.person_id for c in credits}
    anime_ids = {c.anime_id for c in credits}
    unique_persons = len(person_ids)
    unique_anime = len(anime_ids)

    logger.info(
        "credit_stats_start",
        total_credits=total_credits,
        unique_persons=unique_persons,
        unique_anime=unique_anime,
    )

    # Role distribution
    role_counter = Counter(c.role for c in credits if c.role)
    role_distribution = dict(role_counter.most_common())

    # Top roles with percentages
    top_roles = [
        {
            "role": role,
            "count": count,
            "percentage": round(count / total_credits * 100, 2),
        }
        for role, count in role_counter.most_common(20)
    ]

    # Collaboration analysis - person pairs who worked on same anime
    logger.info("computing_collaborations")

    # Group credits by anime_id for staff-per-anime stats
    anime_to_persons: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        anime_to_persons[c.anime_id].add(c.person_id)

    # Approximate total pairs from staff sizes (avoids O(n²) pair enumeration)
    # C(n,2) per anime, then estimate unique pairs
    total_pair_instances = sum(
        len(ps) * (len(ps) - 1) // 2 for ps in anime_to_persons.values()
    )

    collaboration_stats = {
        "total_pair_instances": total_pair_instances,
        "note": "Full pair enumeration deferred to collaboration_strength module",
    }

    # Timeline analysis - credits per year
    logger.info("computing_timeline")
    year_stats: dict[int, dict[str, Any]] = defaultdict(
        lambda: {"credits": 0, "anime_ids": set(), "person_ids": set()}
    )

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            year_stats[anime.year]["credits"] += 1
            year_stats[anime.year]["anime_ids"].add(c.anime_id)
            year_stats[anime.year]["person_ids"].add(c.person_id)

    # Convert to serializable format
    timeline_stats = {
        "by_year": [
            {
                "year": year,
                "credits": stats["credits"],
                "anime_count": len(stats["anime_ids"]),
                "person_count": len(stats["person_ids"]),
            }
            for year, stats in sorted(year_stats.items(), reverse=True)
        ],
        "total_years": len(year_stats),
        "year_range": (
            f"{min(year_stats.keys())}-{max(year_stats.keys())}"
            if year_stats
            else "N/A"
        ),
    }

    # Person-level statistics
    logger.info("computing_person_stats")
    person_credit_counts = Counter(c.person_id for c in credits)

    # Top persons by credit count
    top_persons = [
        {
            "person_id": person_id,
            "credit_count": count,
        }
        for person_id, count in person_credit_counts.most_common(100)
    ]

    # Person role diversity - how many distinct roles each person has
    person_roles: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        if c.role:
            person_roles[c.person_id].add(c.role)

    role_diversity_distribution = Counter(len(roles) for roles in person_roles.values())

    person_id_stats = {
        "top_persons_by_credits": top_persons,
        "role_diversity_distribution": dict(role_diversity_distribution),
        "avg_roles_per_person": (
            sum(len(roles) for roles in person_roles.values()) / len(person_roles)
            if person_roles
            else 0.0
        ),
        "max_roles_single_person": max(
            (len(roles) for roles in person_roles.values()),
            default=0,
        ),
    }

    # Build result
    result = {
        "summary": {
            "total_credits": total_credits,
            "unique_persons": unique_persons,
            "unique_anime": unique_anime,
            "avg_credits_per_person": round(total_credits / unique_persons, 1),
            "avg_staff_per_anime": round(total_credits / unique_anime, 1),
        },
        "role_distribution": role_distribution,
        "top_roles": top_roles,
        "collaboration_stats": collaboration_stats,
        "timeline_stats": timeline_stats,
        "person_id_stats": person_id_stats,
    }

    logger.info(
        "credit_stats_complete",
        total_pair_instances=collaboration_stats["total_pair_instances"],
        years_analyzed=timeline_stats["total_years"],
        top_person_credits=top_persons[0]["credit_count"] if top_persons else 0,
    )

    return result
