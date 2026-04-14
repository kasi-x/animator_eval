"""Career Friction — reduced-form DDCM estimating friction from observed transitions.

Compares observed career transitions (role upgrades) against expected
transitions given a person's score, to estimate how much friction
(barriers to advancement) each person faces.
"""

from collections import defaultdict
from dataclasses import dataclass

import numpy as np
import structlog

from src.models import Anime, Credit
from src.utils.role_groups import CAREER_STAGE_BY_VALUE

logger = structlog.get_logger()

# Re-export for backward compatibility
CAREER_STAGE = CAREER_STAGE_BY_VALUE


@dataclass
class CareerFrictionResult:
    """Result of career friction estimation.

    Attributes:
        friction_index: per-person (0=no friction, 1=maximal)
        role_friction: per role transition type → difficulty
        studio_tier_friction: studio tier → friction
        era_friction: decade → friction
        transition_matrix: from_stage → to_stage → count
    """

    friction_index: dict[str, float]
    role_friction: dict[str, float]
    studio_tier_friction: dict[str, float]
    era_friction: dict[int, float]
    transition_matrix: dict[int, dict[int, int]]


def _build_career_transitions(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, list[tuple[int, int]]]:
    """Build yearly career stage transitions per person.

    Returns:
        person_id → [(year, max_stage_in_year), ...]  sorted by year
    """
    # Track max stage per person per year
    person_year_stage: dict[str, dict[int, int]] = defaultdict(dict)

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        stage = CAREER_STAGE.get(c.role.value, 1)
        year = anime.year
        if year not in person_year_stage[c.person_id]:
            person_year_stage[c.person_id][year] = stage
        else:
            person_year_stage[c.person_id][year] = max(
                person_year_stage[c.person_id][year], stage
            )

    # Convert to sorted lists
    result: dict[str, list[tuple[int, int]]] = {}
    for pid, year_stages in person_year_stage.items():
        sorted_stages = sorted(year_stages.items())
        if len(sorted_stages) >= 2:
            result[pid] = sorted_stages

    return result


def estimate_career_friction(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
    studio_fe: dict[str, float] | None = None,
) -> CareerFrictionResult:
    """Estimate career friction from observed vs expected transitions.

    Friction = 1 - (actual_upgrades / expected_upgrades_given_score)

    Args:
        credits: all credits
        anime_map: anime_id → Anime
        person_scores: person_id → composite score (for expected upgrade rate)
        studio_fe: studio_name → fixed effect (for studio tier decomposition)

    Returns:
        CareerFrictionResult
    """
    person_scores = person_scores or {}
    studio_fe = studio_fe or {}

    # Build transitions
    transitions = _build_career_transitions(credits, anime_map)

    if not transitions:
        return CareerFrictionResult(
            friction_index={},
            role_friction={},
            studio_tier_friction={},
            era_friction={},
            transition_matrix={},
        )

    # Count transitions
    transition_matrix: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    person_upgrades: dict[str, int] = defaultdict(int)
    person_total_transitions: dict[str, int] = defaultdict(int)
    era_upgrades: dict[int, list[float]] = defaultdict(list)

    for pid, stages in transitions.items():
        for i in range(len(stages) - 1):
            year_from, stage_from = stages[i]
            year_to, stage_to = stages[i + 1]
            transition_matrix[stage_from][stage_to] += 1
            person_total_transitions[pid] += 1

            is_upgrade = 1.0 if stage_to > stage_from else 0.0
            if stage_to > stage_from:
                person_upgrades[pid] += 1

            # Track by era
            decade = (year_from // 10) * 10
            era_upgrades[decade].append(is_upgrade)

    # Compute population-level upgrade rates by score percentile
    # Group persons into quartiles by score
    scored_persons = [(pid, person_scores.get(pid, 0.0)) for pid in transitions]
    scored_persons.sort(key=lambda x: x[1])

    # Expected upgrade rate per quartile
    n_quartile = max(len(scored_persons) // 4, 1)
    quartile_upgrade_rates: list[float] = []
    for q in range(4):
        start = q * n_quartile
        end = start + n_quartile if q < 3 else len(scored_persons)
        quartile_pids = [pid for pid, _ in scored_persons[start:end]]
        total_trans = sum(person_total_transitions.get(pid, 0) for pid in quartile_pids)
        total_ups = sum(person_upgrades.get(pid, 0) for pid in quartile_pids)
        rate = total_ups / total_trans if total_trans > 0 else 0
        quartile_upgrade_rates.append(rate)

    # Assign expected upgrade rate per person based on their score quartile
    person_expected_rate: dict[str, float] = {}
    for i, (pid, score) in enumerate(scored_persons):
        q = min(i // max(n_quartile, 1), 3)
        person_expected_rate[pid] = quartile_upgrade_rates[q]

    # Compute friction index per person
    friction_index: dict[str, float] = {}
    for pid in transitions:
        total_trans = person_total_transitions.get(pid, 0)
        if total_trans == 0:
            friction_index[pid] = 0.0
            continue
        actual_rate = person_upgrades.get(pid, 0) / total_trans
        expected_rate = person_expected_rate.get(pid, 0)
        if expected_rate > 0:
            friction_index[pid] = max(0.0, min(1.0, 1.0 - actual_rate / expected_rate))
        else:
            friction_index[pid] = 0.0

    # Role transition friction: which transitions are hardest?
    role_friction: dict[str, float] = {}
    for stage_from, to_counts in transition_matrix.items():
        total = sum(to_counts.values())
        upgrades = sum(
            cnt for stage_to, cnt in to_counts.items() if stage_to > stage_from
        )
        key = f"stage_{stage_from}_up"
        role_friction[key] = 1.0 - (upgrades / total) if total > 0 else 0.0

    # Studio tier friction
    studio_tier_friction: dict[str, float] = {}
    if studio_fe:
        # Group studios into tiers by FE
        sorted_studios = sorted(studio_fe.items(), key=lambda x: x[1])
        n_tiers = min(3, len(sorted_studios))
        tier_size = max(len(sorted_studios) // n_tiers, 1)
        studio_tier_map: dict[str, str] = {}
        tier_labels = ["low", "mid", "high"]
        for i, (studio, _) in enumerate(sorted_studios):
            tier_idx = min(i // tier_size, n_tiers - 1)
            studio_tier_map[studio] = tier_labels[tier_idx]

        # Compute friction per tier (simplified: avg person friction in tier)
        tier_frictions: dict[str, list[float]] = defaultdict(list)
        for c in credits:
            anime = anime_map.get(c.anime_id)
            if anime and anime.studios:
                studio = anime.studios[0]
                tier = studio_tier_map.get(studio)
                if tier and c.person_id in friction_index:
                    tier_frictions[tier].append(friction_index[c.person_id])

        for tier, vals in tier_frictions.items():
            studio_tier_friction[tier] = float(np.mean(vals)) if vals else 0.0

    # Era friction
    era_friction: dict[int, float] = {}
    for decade, upgrade_flags in era_upgrades.items():
        era_friction[decade] = (
            1.0 - float(np.mean(upgrade_flags)) if upgrade_flags else 0.0
        )

    # Convert transition matrix to regular dict
    tm_dict = {k: dict(v) for k, v in transition_matrix.items()}

    logger.info(
        "career_friction_estimated",
        persons=len(friction_index),
        avg_friction=round(float(np.mean(list(friction_index.values()))), 3)
        if friction_index
        else 0,
    )

    return CareerFrictionResult(
        friction_index=friction_index,
        role_friction=role_friction,
        studio_tier_friction=studio_tier_friction,
        era_friction=era_friction,
        transition_matrix=tm_dict,
    )
