"""Era Effects — year fixed effects and project difficulty proxy.

Computes:
- Year fixed effects (γ_era): systematic differences across years
- Project difficulty scores: cast_size × role_entropy × team_score_variance
"""

from collections import defaultdict
from dataclasses import dataclass
from math import log2

import numpy as np
import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


@dataclass
class EraEffectResult:
    """Result of era effects estimation.

    Attributes:
        era_fe: γ_era per year — year fixed effects
        difficulty_scores: per-anime difficulty score
        difficulty_beta: coefficient on difficulty in the model
    """

    era_fe: dict[int, float]
    difficulty_scores: dict[str, float]
    difficulty_beta: float


def _compute_difficulty(
    anime_id: str,
    anime_credits: list[Credit],
    person_scores: dict[str, float],
) -> float:
    """Compute project difficulty for a single anime.

    difficulty = cast_size × Shannon_entropy(role_distribution) × variance(team_prior_scores)

    Args:
        anime_id: anime ID
        anime_credits: credits for this anime
        person_scores: person_id → prior score

    Returns:
        difficulty score (higher = more complex project)
    """
    if not anime_credits:
        return 0.0

    # Cast size: unique persons
    persons = {c.person_id for c in anime_credits}
    cast_size = len(persons)

    # Role distribution entropy
    role_counts: dict[str, int] = defaultdict(int)
    for c in anime_credits:
        role_counts[c.role.value] += 1
    total = sum(role_counts.values())
    if total > 0 and len(role_counts) > 1:
        entropy = -sum(
            (cnt / total) * log2(cnt / total) for cnt in role_counts.values() if cnt > 0
        )
    else:
        entropy = 0.0

    # Team prior score variance
    team_scores = [person_scores.get(pid, 0.0) for pid in persons]
    if len(team_scores) >= 2:
        score_var = float(np.var(team_scores))
    else:
        score_var = 0.0

    return cast_size * (1 + entropy) * (1 + score_var)


def compute_era_and_difficulty(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float],
) -> EraEffectResult:
    """Compute era fixed effects and project difficulty.

    Model: y_it = γ_year + β × difficulty + ε

    Args:
        credits: all credits
        anime_map: anime_id → Anime
        person_scores: person_id → score

    Returns:
        EraEffectResult with year FE and difficulty scores
    """
    # Group credits by anime
    anime_credits: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        anime_credits[c.anime_id].append(c)

    # Compute difficulty per anime
    difficulty_scores: dict[str, float] = {}
    for anime_id, creds in anime_credits.items():
        difficulty_scores[anime_id] = _compute_difficulty(
            anime_id, creds, person_scores
        )

    # Build panel: one observation per person-anime
    # y = person's score, x = difficulty, group = year
    obs_by_year: dict[int, list[tuple[float, float]]] = defaultdict(list)
    all_y = []
    all_difficulty = []
    all_years = []

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        if c.person_id not in person_scores:
            continue

        y_val = person_scores[c.person_id]
        d_val = difficulty_scores.get(c.anime_id, 0.0)
        obs_by_year[anime.year].append((y_val, d_val))
        all_y.append(y_val)
        all_difficulty.append(d_val)
        all_years.append(anime.year)

    if len(all_y) < 10:
        logger.warning("era_effects_too_few_obs", n=len(all_y))
        return EraEffectResult(
            era_fe={}, difficulty_scores=difficulty_scores, difficulty_beta=0.0
        )

    # Year demeaning: absorb year means
    year_means: dict[int, float] = {}
    for year, obs in obs_by_year.items():
        y_vals = [o[0] for o in obs]
        year_means[year] = float(np.mean(y_vals)) if y_vals else 0.0

    global_mean = float(np.mean(all_y))

    # Era fixed effects = year_mean - global_mean
    era_fe: dict[int, float] = {
        year: mean - global_mean for year, mean in year_means.items()
    }

    # Regress demeaned y on difficulty to get β
    y_demeaned = np.array(
        [
            all_y[i] - year_means.get(all_years[i], global_mean)
            for i in range(len(all_y))
        ],
        dtype=np.float64,
    )
    d_arr = np.array(all_difficulty, dtype=np.float64)

    # Normalize difficulty to prevent numerical issues
    d_std = np.std(d_arr)
    if d_std > 0:
        d_normalized = d_arr / d_std
    else:
        d_normalized = d_arr

    # OLS: y_demeaned = β × difficulty + ε
    if np.sum(d_normalized**2) > 0:
        difficulty_beta = float(
            np.sum(y_demeaned * d_normalized) / np.sum(d_normalized**2)
        )
        # Scale back
        if d_std > 0:
            difficulty_beta /= d_std
    else:
        difficulty_beta = 0.0

    logger.info(
        "era_effects_computed",
        years=len(era_fe),
        anime=len(difficulty_scores),
        difficulty_beta=round(difficulty_beta, 6),
    )

    return EraEffectResult(
        era_fe=era_fe,
        difficulty_scores=difficulty_scores,
        difficulty_beta=difficulty_beta,
    )
