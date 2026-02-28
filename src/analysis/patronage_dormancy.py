"""Patronage Premium + Dormancy Penalty.

Patronage Premium: Measures the cumulative benefit of working with
high-prestige directors (based on their BiRank scores).

Dormancy Penalty: Penalizes persons who have been inactive for
extended periods, with a grace period before decay begins.
"""

from collections import defaultdict
from dataclasses import dataclass
from math import exp, log1p

import structlog

from src.models import Anime, Credit
from src.utils.role_groups import DIRECTOR_ROLES

logger = structlog.get_logger()


@dataclass
class PatronageDormancyResult:
    """Combined result of patronage and dormancy computation.

    Attributes:
        patronage_premium: per-person patronage score
        dormancy_penalty: per-person dormancy multiplier (0-1)
        patronage_details: per-person breakdown of director contributions
    """

    patronage_premium: dict[str, float]
    dormancy_penalty: dict[str, float]
    patronage_details: dict[str, list[dict]]


def compute_patronage_premium(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    director_birank_scores: dict[str, float],
) -> dict[str, float]:
    """Compute patronage premium for each person.

    Π_i = Σ_d (PR_d × log(1+N_id) × Quality_id)

    Where:
    - PR_d = BiRank score of director d
    - N_id = number of collaborations between person i and director d
    - Quality_id = avg anime score when working together

    Args:
        credits: all credits
        anime_map: anime_id → Anime
        director_birank_scores: director_id → BiRank score

    Returns:
        person_id → patronage premium
    """
    # Identify directors per anime
    anime_directors: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        if c.role in DIRECTOR_ROLES:
            anime_directors[c.anime_id].add(c.person_id)

    # For each non-director person, count collaborations with each director
    # and track quality
    person_director_collabs: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for c in credits:
        if c.role in DIRECTOR_ROLES:
            continue
        anime = anime_map.get(c.anime_id)
        if not anime:
            continue
        anime_score = anime.score if anime.score is not None else 0.0
        for dir_id in anime_directors.get(c.anime_id, set()):
            person_director_collabs[c.person_id][dir_id].append(anime_score)

    # Compute patronage premium
    patronage: dict[str, float] = {}
    for pid, director_works in person_director_collabs.items():
        total = 0.0
        for dir_id, scores in director_works.items():
            pr_d = director_birank_scores.get(dir_id, 0.0)
            n_collabs = len(scores)
            quality = sum(scores) / len(scores) if scores else 0.0
            total += pr_d * log1p(n_collabs) * quality
        patronage[pid] = total

    logger.info("patronage_premium_computed", persons=len(patronage))
    return patronage


def compute_dormancy_penalty(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    current_year: int = 2026,
    decay_rate: float = 0.5,
    grace_period: float = 2.0,
) -> dict[str, float]:
    """Compute dormancy penalty for each person.

    D(i,t) = exp(-δ × max(0, gap - τ_grace))

    Where:
    - gap = current_year - last_active_year
    - δ = decay_rate
    - τ_grace = grace_period (years of inactivity before penalty begins)

    Args:
        credits: all credits
        anime_map: anime_id → Anime
        current_year: reference year
        decay_rate: exponential decay rate
        grace_period: years before decay begins

    Returns:
        person_id → dormancy multiplier (0 to 1, higher = more active)
    """
    # Find last active year per person
    last_year: dict[str, int] = {}
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            if c.person_id not in last_year:
                last_year[c.person_id] = anime.year
            else:
                last_year[c.person_id] = max(last_year[c.person_id], anime.year)

    dormancy: dict[str, float] = {}
    for pid, ly in last_year.items():
        gap = current_year - ly
        effective_gap = max(0.0, gap - grace_period)
        dormancy[pid] = exp(-decay_rate * effective_gap)

    logger.info("dormancy_penalty_computed", persons=len(dormancy))
    return dormancy


def compute_patronage_and_dormancy(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    director_birank_scores: dict[str, float],
    current_year: int = 2026,
    decay_rate: float = 0.5,
    grace_period: float = 2.0,
) -> PatronageDormancyResult:
    """Compute both patronage premium and dormancy penalty.

    Args:
        credits: all credits
        anime_map: anime_id → Anime
        director_birank_scores: director_id → BiRank score
        current_year: reference year
        decay_rate: dormancy decay rate
        grace_period: dormancy grace period

    Returns:
        PatronageDormancyResult
    """
    patronage = compute_patronage_premium(credits, anime_map, director_birank_scores)
    dormancy = compute_dormancy_penalty(
        credits, anime_map, current_year, decay_rate, grace_period
    )

    # Build patronage details
    anime_directors: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        if c.role in DIRECTOR_ROLES:
            anime_directors[c.anime_id].add(c.person_id)

    patronage_details: dict[str, list[dict]] = defaultdict(list)
    for c in credits:
        if c.role in DIRECTOR_ROLES:
            continue
        for dir_id in anime_directors.get(c.anime_id, set()):
            pr_d = director_birank_scores.get(dir_id, 0.0)
            if pr_d > 0:
                patronage_details[c.person_id].append({
                    "director_id": dir_id,
                    "anime_id": c.anime_id,
                    "director_birank": round(pr_d, 6),
                })

    return PatronageDormancyResult(
        patronage_premium=patronage,
        dormancy_penalty=dormancy,
        patronage_details=dict(patronage_details),
    )
