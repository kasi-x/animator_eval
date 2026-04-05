"""Voice Actor Integrated Value — weighted combination of VA components.

VA_IV = (0.30×θ_va + 0.15×va_birank + 0.15×sd_exposure + 0.15×va_awcc + 0.25×va_patronage) × dormancy

Patronage weight is higher (0.25) because sound directors' influence on
casting is more concentrated than directors' influence on animators.
"""

import numpy as np
import structlog

logger = structlog.get_logger()

# Fixed prior weights for VA components
VA_LAMBDA_WEIGHTS: dict[str, float] = {
    "person_fe": 0.30,
    "birank": 0.15,
    "sd_exposure": 0.15,
    "awcc": 0.15,
    "patronage": 0.25,
}


def compute_va_integrated_value(
    person_fe: dict[str, float],
    birank: dict[str, float],
    sd_exposure: dict[str, float],
    awcc: dict[str, float],
    patronage: dict[str, float],
    dormancy: dict[str, float],
) -> dict[str, float]:
    """Compute integrated value for voice actors.

    Components are z-score normalized before combination, then
    multiplied by dormancy.

    Args:
        person_fe: VA person fixed effects
        birank: VA BiRank scores
        sd_exposure: Sound director FE exposure
        awcc: VA AWCC scores
        patronage: VA patronage scores
        dormancy: VA dormancy multipliers (0-1)

    Returns:
        va_id → integrated value score
    """
    all_persons = set()
    all_persons.update(person_fe.keys())
    all_persons.update(birank.keys())

    if not all_persons:
        return {}

    components = {
        "person_fe": person_fe,
        "birank": birank,
        "sd_exposure": sd_exposure,
        "awcc": awcc,
        "patronage": patronage,
    }

    # Compute z-score parameters for each component
    comp_stats: dict[str, tuple[float, float]] = {}
    for name, scores in components.items():
        vals = list(scores.values())
        if vals:
            arr = np.array(vals)
            mean = float(np.mean(arr))
            std = float(np.std(arr))
            if std < 1e-10:
                std = 1.0
            comp_stats[name] = (mean, std)
        else:
            comp_stats[name] = (0.0, 1.0)

    # Compute IV
    iv_scores: dict[str, float] = {}
    for pid in all_persons:
        raw = 0.0
        for name, scores in components.items():
            val = scores.get(pid, 0.0)
            mean, std = comp_stats[name]
            z = (val - mean) / std
            raw += VA_LAMBDA_WEIGHTS[name] * z

        d = dormancy.get(pid, 1.0)
        iv_scores[pid] = raw * d

    logger.info("va_iv_computed", persons=len(iv_scores))
    return iv_scores


def compute_va_sd_exposure(
    va_sd_fe: dict[str, float],
    sd_assignments: dict[str, dict[int, str]],
) -> dict[str, float]:
    """Compute sound director FE exposure for each VA.

    Weighted average of sound director FEs the VA has worked with.

    Args:
        va_sd_fe: sd_id → sound director fixed effect
        sd_assignments: va_id → {year → sd_id}

    Returns:
        va_id → sd_exposure score
    """
    if not va_sd_fe or not sd_assignments:
        return {}

    exposure: dict[str, float] = {}
    for va_id, year_sd in sd_assignments.items():
        if not year_sd:
            continue
        sd_years: dict[str, int] = {}
        for year, sd_id in year_sd.items():
            sd_years[sd_id] = sd_years.get(sd_id, 0) + 1
        total = sum(sd_years.values())
        if total == 0:
            continue
        exp_val = sum(
            va_sd_fe.get(sd_id, 0.0) * (years / total)
            for sd_id, years in sd_years.items()
        )
        exposure[va_id] = exp_val

    return exposure
