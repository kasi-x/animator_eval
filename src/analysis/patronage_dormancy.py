"""Patronage Premium + Dormancy Penalty.

Patronage Premium: Measures the cumulative benefit of working with
high-prestige directors (based on their BiRank scores).

Dormancy Penalty: Penalizes persons who have been inactive for
extended periods, with a grace period before decay begins.
"""

import datetime
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

    Π_i = Σ_d (PR_d × log(1+N_id))

    Where:
    - PR_d = BiRank score of director d
    - N_id = number of collaborations between person i and director d

    The Quality term (avg anime.score) was removed because viewer ratings
    are independent of staff contribution — see CLAUDE.md for rationale.

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
    person_director_collabs: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    for c in credits:
        if c.role in DIRECTOR_ROLES:
            continue
        anime = anime_map.get(c.anime_id)
        if not anime:
            continue
        for dir_id in anime_directors.get(c.anime_id, set()):
            person_director_collabs[c.person_id][dir_id] += 1

    # Compute patronage premium: Π_i = Σ_d (PR_d × log(1+N_id))
    # Quality term removed — anime.score is independent of staff contribution
    patronage: dict[str, float] = {}
    for pid, director_works in person_director_collabs.items():
        total = 0.0
        for dir_id, n_collabs in director_works.items():
            pr_d = director_birank_scores.get(dir_id, 0.0)
            total += pr_d * log1p(n_collabs)
        patronage[pid] = total

    logger.info("patronage_premium_computed", persons=len(patronage))
    return patronage


def compute_dormancy_penalty(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    current_year: int | None = None,
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
    if current_year is None:
        current_year = datetime.datetime.now().year

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
    current_year: int | None = None,
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


def compute_career_aware_dormancy(
    raw_dormancy: dict[str, float],
    iv_scores_historical: dict[str, float],
    career_data: dict,
    career_capital_threshold: float = 0.7,
    dormancy_floor: float = 0.5,
) -> dict[str, float]:
    """Career-aware dormancy: protect veteran contributions from harsh dormancy.

    Veterans with significant career capital should not have their scores
    reduced below a floor. This prevents unfairly penalizing mid-career
    and veteran staff who may be between projects.

    career_capital = iv_percentile × years_norm × stage_norm
    If career_capital >= threshold, dormancy = max(raw_dormancy, floor)

    Args:
        raw_dormancy: person_id → raw dormancy multiplier (0-1)
        iv_scores_historical: person_id → iv_score without dormancy
        career_data: person_id → career snapshot (dict or dataclass)
        career_capital_threshold: threshold above which floor applies
        dormancy_floor: minimum dormancy for veterans

    Returns:
        person_id → career-aware dormancy multiplier
    """
    if not iv_scores_historical or not career_data:
        return raw_dormancy

    # Compute IV percentile ranks for career capital
    iv_values = sorted(iv_scores_historical.values())
    n_iv = len(iv_values)
    if n_iv == 0:
        return raw_dormancy

    import bisect

    result = {}
    for pid, raw_d in raw_dormancy.items():
        iv_hist = iv_scores_historical.get(pid, 0.0)
        iv_pctile = bisect.bisect_right(iv_values, iv_hist) / n_iv if n_iv > 0 else 0.0

        # Get career years and stage
        cd = career_data.get(pid)
        if cd is None:
            result[pid] = raw_d
            continue

        if isinstance(cd, dict):
            active_years = cd.get("active_years", 0)
            highest_stage = cd.get("highest_stage", 0)
        else:
            active_years = getattr(cd, "active_years", 0)
            highest_stage = getattr(cd, "highest_stage", 0)

        # Normalize: years (0-30 range), stage (0-6 range)
        years_norm = min(active_years / 30.0, 1.0)
        stage_norm = min(highest_stage / 6.0, 1.0)

        career_capital = iv_pctile * years_norm * stage_norm

        if career_capital >= career_capital_threshold:
            result[pid] = max(raw_d, dormancy_floor)
        else:
            result[pid] = raw_d

    logger.info(
        "career_aware_dormancy_computed",
        total=len(result),
        protected=sum(1 for pid in result if result[pid] > raw_dormancy.get(pid, 1.0)),
    )
    return result
