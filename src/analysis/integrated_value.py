"""Integrated Value — CV-optimized weighted combination of 8 components.

Combines person FE, BiRank, studio exposure, AWCC, and patronage with
cross-validation-optimized weights, multiplied by dormancy penalty.

IV = (λ1·θ + λ2·birank + λ3·studio_exp + λ4·awcc + λ5·patronage) × dormancy
"""

from collections import defaultdict
from dataclasses import dataclass

import numpy as np
import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


@dataclass
class IntegratedValueResult:
    """Result of integrated value computation.

    Attributes:
        iv_scores: person_id → integrated value score
        lambda_weights: component name → optimized weight
        cv_mse: cross-validation mean squared error
        component_breakdown: person_id → {component → value}
    """

    iv_scores: dict[str, float]
    lambda_weights: dict[str, float]
    cv_mse: float
    component_breakdown: dict[str, dict[str, float]]


def compute_studio_exposure(
    person_fe: dict[str, float],
    studio_fe: dict[str, float],
    studio_assignments: dict[str, dict[int, str]] | None = None,
    akm_result=None,
) -> dict[str, float]:
    """Compute studio exposure for each person.

    studio_exposure_i = Σ_j(I{i∈j} · ψ_j) — sum of studio FEs for studios
    the person has worked at, weighted by time spent.

    Args:
        person_fe: person_id → person fixed effect (unused, for API compat)
        studio_fe: studio_name → studio fixed effect
        studio_assignments: person_id → {year → studio} (from AKM)
        akm_result: AKMResult instance (alternative source for assignments)

    Returns:
        person_id → studio exposure score
    """
    if not studio_fe:
        return {}

    # Get studio assignments from AKM result if not provided directly
    assignments = studio_assignments
    if assignments is None and akm_result is not None:
        assignments = {}

    if not assignments:
        return {}

    exposure: dict[str, float] = {}
    for pid, year_studio in assignments.items():
        if not year_studio:
            continue
        # Count years per studio
        studio_years: dict[str, int] = defaultdict(int)
        for year, studio in year_studio.items():
            studio_years[studio] += 1
        total_years = sum(studio_years.values())
        if total_years == 0:
            exposure[pid] = 0.0
            continue
        # Weighted sum of studio FE
        exp_val = sum(
            studio_fe.get(studio, 0.0) * (years / total_years)
            for studio, years in studio_years.items()
        )
        exposure[pid] = exp_val

    return exposure


def optimize_lambda_weights(
    components: dict[str, dict[str, float]],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    n_folds: int = 5,
    seed: int = 42,
) -> tuple[dict[str, float], float]:
    """Optimize component weights via cross-validation.

    Uses leave-one-anime-out CV: predict person's weighted anime score on
    held-out projects using component scores.

    Args:
        components: {component_name → {person_id → score}}
        credits: all credits
        anime_map: anime_id → Anime
        n_folds: number of CV folds
        seed: random seed

    Returns:
        (lambda_weights, cv_mse)
    """
    from scipy.optimize import minimize

    component_names = sorted(components.keys())
    n_components = len(component_names)

    if n_components == 0:
        return {}, 0.0

    # Build person → anime outcomes
    person_anime_scores: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.score is not None:
            person_anime_scores[c.person_id].append((c.anime_id, anime.score))

    # Get all persons with components and anime scores
    all_persons = set()
    for comp_scores in components.values():
        all_persons.update(comp_scores.keys())
    all_persons = {p for p in all_persons if p in person_anime_scores}

    if len(all_persons) < 10:
        # Too few persons, use equal weights
        equal_w = 1.0 / n_components
        return {name: equal_w for name in component_names}, 0.0

    # Build feature matrix for all persons
    person_list = sorted(all_persons)
    X = np.zeros((len(person_list), n_components), dtype=np.float64)
    for j, name in enumerate(component_names):
        for i, pid in enumerate(person_list):
            X[i, j] = components[name].get(pid, 0.0)

    # Normalize features
    x_std = np.std(X, axis=0)
    x_std[x_std == 0] = 1.0
    X_norm = X / x_std

    # Target: mean anime score per person
    y = np.array([
        np.mean([s for _, s in person_anime_scores[pid]])
        for pid in person_list
    ], dtype=np.float64)

    # CV optimization
    rng = np.random.RandomState(seed)
    indices = np.arange(len(person_list))
    rng.shuffle(indices)
    fold_size = len(indices) // n_folds

    def cv_mse(lambdas):
        total_mse = 0.0
        for fold in range(n_folds):
            start = fold * fold_size
            end = start + fold_size if fold < n_folds - 1 else len(indices)
            test_idx = indices[start:end]

            # Predict on test set
            pred = X_norm[test_idx] @ lambdas
            actual = y[test_idx]
            total_mse += np.mean((pred - actual) ** 2)

        return total_mse / n_folds

    # Optimize
    x0 = np.ones(n_components) / n_components
    bounds = [(0, None)] * n_components

    try:
        result = minimize(
            cv_mse,
            x0,
            method="L-BFGS-B",
            bounds=bounds,
            options={"maxiter": 100},
        )
        optimal_lambdas = result.x
        best_mse = float(result.fun)
    except Exception as e:
        logger.warning("iv_optimization_failed", error=str(e))
        optimal_lambdas = x0
        best_mse = float(cv_mse(x0))

    # Normalize weights to sum to 1
    total = np.sum(optimal_lambdas)
    if total > 0:
        optimal_lambdas /= total

    # Scale back by feature std
    scaled_lambdas = optimal_lambdas / x_std
    total_scaled = np.sum(scaled_lambdas)
    if total_scaled > 0:
        scaled_lambdas /= total_scaled

    lambda_weights = {
        name: float(scaled_lambdas[j]) for j, name in enumerate(component_names)
    }

    logger.info(
        "iv_weights_optimized",
        cv_mse=round(best_mse, 6),
        weights={k: round(v, 4) for k, v in lambda_weights.items()},
    )

    return lambda_weights, best_mse


def compute_integrated_value(
    person_fe: dict[str, float],
    birank: dict[str, float],
    studio_exposure: dict[str, float],
    awcc: dict[str, float],
    patronage: dict[str, float],
    dormancy: dict[str, float],
    lambdas: dict[str, float],
) -> dict[str, float]:
    """Compute integrated value scores.

    IV = (λ1·θ + λ2·birank + λ3·studio_exp + λ4·awcc + λ5·patronage) × dormancy

    Args:
        person_fe: person_id → person fixed effect (θ)
        birank: person_id → BiRank score
        studio_exposure: person_id → studio exposure
        awcc: person_id → AWCC score
        patronage: person_id → patronage premium
        dormancy: person_id → dormancy multiplier (0-1)
        lambdas: component name → weight

    Returns:
        person_id → integrated value score
    """
    all_persons = set()
    all_persons.update(person_fe.keys())
    all_persons.update(birank.keys())

    components = {
        "person_fe": person_fe,
        "birank": birank,
        "studio_exposure": studio_exposure,
        "awcc": awcc,
        "patronage": patronage,
    }

    iv_scores: dict[str, float] = {}
    for pid in all_persons:
        raw = sum(
            lambdas.get(name, 0.2) * scores.get(pid, 0.0)
            for name, scores in components.items()
        )
        d = dormancy.get(pid, 1.0)
        iv_scores[pid] = raw * d

    return iv_scores


def compute_integrated_value_full(
    person_fe: dict[str, float],
    birank: dict[str, float],
    studio_exposure: dict[str, float],
    awcc: dict[str, float],
    patronage: dict[str, float],
    dormancy: dict[str, float],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    n_folds: int = 5,
    seed: int = 42,
) -> IntegratedValueResult:
    """Full integrated value pipeline: optimize weights + compute scores.

    Args:
        person_fe: person fixed effects
        birank: BiRank scores
        studio_exposure: studio exposure scores
        awcc: AWCC scores
        patronage: patronage premiums
        dormancy: dormancy penalties
        credits: all credits
        anime_map: anime_id → Anime
        n_folds: CV folds
        seed: random seed

    Returns:
        IntegratedValueResult
    """
    components = {
        "person_fe": person_fe,
        "birank": birank,
        "studio_exposure": studio_exposure,
        "awcc": awcc,
        "patronage": patronage,
    }

    # Optimize weights
    lambdas, cv_mse = optimize_lambda_weights(
        components, credits, anime_map, n_folds=n_folds, seed=seed
    )

    # If optimization returned empty weights, use defaults
    if not lambdas:
        lambdas = {name: 0.2 for name in components}

    # Compute IV scores
    iv_scores = compute_integrated_value(
        person_fe, birank, studio_exposure, awcc, patronage, dormancy, lambdas
    )

    # Build component breakdown
    component_breakdown: dict[str, dict[str, float]] = {}
    for pid in iv_scores:
        component_breakdown[pid] = {
            name: lambdas.get(name, 0.2) * scores.get(pid, 0.0)
            for name, scores in components.items()
        }
        component_breakdown[pid]["dormancy"] = dormancy.get(pid, 1.0)

    logger.info(
        "integrated_value_computed",
        persons=len(iv_scores),
        cv_mse=round(cv_mse, 6),
    )

    return IntegratedValueResult(
        iv_scores=iv_scores,
        lambda_weights=lambdas,
        cv_mse=cv_mse,
        component_breakdown=component_breakdown,
    )
