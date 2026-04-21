"""Production (Studio) Analysis — talent density, capacity, and specialization.

Analyzes studios using structural data only (person FE, credit counts,
role distributions). No viewer ratings (anime.score) are used.
"""

from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import structlog

from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


@dataclass
class StudioTalentDensity:
    """Talent distribution metrics for a single studio.

    Attributes:
        studio: Studio name
        mean_fe: Mean person FE of studio staff
        median_fe: Median person FE
        std_fe: Standard deviation of person FE
        iqr_fe: Interquartile range of person FE
        top_10pct_mean_fe: Mean FE of top 10% of staff
        gini_coefficient: Gini coefficient (0=equal, 1=concentrated)
        talent_tiers: Count per tier (star/strong/average/developing)
        staff_count: Total unique staff
        anime_count: Total anime produced
        credit_count: Total credits
    """

    studio: str = ""
    mean_fe: float = 0.0
    median_fe: float = 0.0
    std_fe: float = 0.0
    iqr_fe: float = 0.0
    top_10pct_mean_fe: float = 0.0
    gini_coefficient: float = 0.0
    talent_tiers: dict[str, int] = field(default_factory=dict)
    staff_count: int = 0
    anime_count: int = 0
    credit_count: int = 0


def _compute_gini(values: np.ndarray) -> float:
    """Compute Gini coefficient for a 1-D array of non-negative values."""
    if len(values) < 2:
        return 0.0
    sorted_vals = np.sort(values)
    n = len(sorted_vals)
    index = np.arange(1, n + 1)
    return float(
        (2.0 * np.sum(index * sorted_vals) / (n * np.sum(sorted_vals))) - (n + 1) / n
    )


def compute_studio_talent_density(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_fe: dict[str, float],
    global_fe_percentiles: dict[str, float] | None = None,
) -> dict[str, StudioTalentDensity]:
    """Compute talent density metrics for each studio.

    Args:
        credits: all production credits
        anime_map: anime_id → Anime
        person_fe: person_id → person fixed effect
        global_fe_percentiles: optional precomputed thresholds
            {p10, p30, p70} for talent tier classification

    Returns:
        studio_name → StudioTalentDensity
    """
    # Build studio → set of person_ids and anime_ids
    studio_persons: dict[str, set[str]] = defaultdict(set)
    studio_anime: dict[str, set[str]] = defaultdict(set)
    studio_credits: dict[str, int] = defaultdict(int)

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.studios:
            continue
        for studio in anime.studios:
            studio_persons[studio].add(c.person_id)
            studio_anime[studio].add(c.anime_id)
            studio_credits[studio] += 1

    # Compute global FE percentiles for tier boundaries if not provided
    if global_fe_percentiles is None and person_fe:
        all_fe = np.array(list(person_fe.values()))
        global_fe_percentiles = {
            "p10": float(np.percentile(all_fe, 10)),
            "p30": float(np.percentile(all_fe, 30)),
            "p70": float(np.percentile(all_fe, 70)),
        }

    result: dict[str, StudioTalentDensity] = {}

    for studio, pids in studio_persons.items():
        fe_values = [person_fe[pid] for pid in pids if pid in person_fe]
        if not fe_values:
            continue

        fe_arr = np.array(fe_values)

        # Talent tiers
        tiers = {"star": 0, "strong": 0, "average": 0, "developing": 0}
        if global_fe_percentiles:
            p70 = global_fe_percentiles["p70"]
            p30 = global_fe_percentiles["p30"]
            for v in fe_values:
                if v >= p70:
                    tiers[
                        "star" if v >= float(np.percentile(fe_arr, 90)) else "strong"
                    ] += 1
                elif v >= p30:
                    tiers["average"] += 1
                else:
                    tiers["developing"] += 1

        # Top 10% mean
        n_top = max(1, len(fe_arr) // 10)
        top_fe = np.sort(fe_arr)[-n_top:]

        # Gini coefficient on shifted values (make non-negative)
        shifted = fe_arr - np.min(fe_arr) + 1e-10
        gini = _compute_gini(shifted)

        result[studio] = StudioTalentDensity(
            studio=studio,
            mean_fe=float(np.mean(fe_arr)),
            median_fe=float(np.median(fe_arr)),
            std_fe=float(np.std(fe_arr)),
            iqr_fe=float(np.percentile(fe_arr, 75) - np.percentile(fe_arr, 25)),
            top_10pct_mean_fe=float(np.mean(top_fe)),
            gini_coefficient=gini,
            talent_tiers=tiers,
            staff_count=len(pids),
            anime_count=len(studio_anime.get(studio, set())),
            credit_count=studio_credits.get(studio, 0),
        )

    logger.info("studio_talent_density_computed", studios=len(result))
    return result
