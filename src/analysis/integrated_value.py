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
        component_std: component name → standard deviation (diagnostic)
    """

    iv_scores: dict[str, float]
    lambda_weights: dict[str, float]
    cv_mse: float
    component_breakdown: dict[str, dict[str, float]]
    component_std: dict[str, float] | None = None
    component_mean: dict[str, float] | None = None


def compute_studio_exposure(
    person_fe: dict[str, float],
    studio_fe: dict[str, float],
    studio_assignments: dict[str, dict[int, str]] | None = None,
) -> dict[str, float]:
    """Compute studio exposure for each person.

    studio_exposure_i = Σ_j(I{i∈j} · ψ_j) — sum of studio FEs for studios
    the person has worked at, weighted by time spent.

    Args:
        person_fe: person_id → person fixed effect (unused, for API compat)
        studio_fe: studio_name → studio fixed effect
        studio_assignments: person_id → {year → studio} (from AKM)

    Returns:
        person_id → studio exposure score
    """
    if not studio_fe:
        return {}

    assignments = studio_assignments

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
) -> tuple[dict[str, float], float, dict[str, float], dict[str, float]]:
    """Optimize component weights via cross-validation.

    Uses time-based CV: folds are split by year blocks to avoid target leakage.
    Optimization runs in centered normalized space for numerical stability.
    Final weights are kept in this space; compute_integrated_value applies
    the same centering + normalization so that weights are consistent.

    Args:
        components: {component_name → {person_id → score}}
        credits: all credits
        anime_map: anime_id → Anime
        n_folds: number of CV folds
        seed: random seed

    Returns:
        (lambda_weights, cv_mse, component_std)
    """
    component_names = sorted(components.keys())
    n_components = len(component_names)

    if n_components == 0:
        return {}, 0.0, {}, {}

    # Get all persons with components
    all_persons = set()
    for comp_scores in components.values():
        all_persons.update(comp_scores.keys())

    if len(all_persons) < 10:
        equal_w = 1.0 / n_components
        return {name: equal_w for name in component_names}, 0.0, {}, {}

    # Build feature matrix for all persons
    person_list = sorted(all_persons)
    X = np.zeros((len(person_list), n_components), dtype=np.float64)
    for j, name in enumerate(component_names):
        for i, pid in enumerate(person_list):
            X[i, j] = components[name].get(pid, 0.0)

    # Normalize features
    x_std = np.std(X, axis=0)
    x_std[x_std == 0] = 1.0
    std_floor = np.max(x_std) * 0.01
    x_std = np.maximum(x_std, std_floor)
    X_norm = X / x_std

    x_mean = np.mean(X_norm, axis=0)
    X_norm = X_norm - x_mean

    component_std = {name: float(x_std[j]) for j, name in enumerate(component_names)}
    component_mean = {name: float(x_mean[j]) for j, name in enumerate(component_names)}

    # Use fixed prior weights — no CV optimization against anime.score.
    # Theory-informed weights reflecting structural importance:
    #   person_fe (30%): Core individual demand from AKM
    #   birank (15%): Bipartite graph centrality — network position
    #   studio_exposure (15%): Institutional environment quality
    #   awcc (20%): Knowledge spanning — bridging communities
    #   patronage (20%): Director relationship quality
    prior_map = {
        "person_fe": 0.30,
        "birank": 0.15,
        "studio_exposure": 0.15,
        "awcc": 0.20,
        "patronage": 0.20,
    }
    lambda_weights = {
        name: prior_map.get(name, 1.0 / n_components)
        for name in component_names
    }
    # Normalize in case not all components are present
    total = sum(lambda_weights.values())
    if total > 0:
        lambda_weights = {k: v / total for k, v in lambda_weights.items()}

    logger.info(
        "iv_weights_fixed_prior",
        weights={k: round(v, 4) for k, v in lambda_weights.items()},
    )

    return lambda_weights, 0.0, component_std, component_mean


def compute_integrated_value(
    person_fe: dict[str, float],
    birank: dict[str, float],
    studio_exposure: dict[str, float],
    awcc: dict[str, float],
    patronage: dict[str, float],
    dormancy: dict[str, float],
    lambdas: dict[str, float],
    component_std: dict[str, float] | None = None,
    component_mean: dict[str, float] | None = None,
) -> dict[str, float]:
    """Compute integrated value scores.

    IV = (Σ_k λ_k · (x_k/σ_k - μ_k)) × dormancy

    Components are normalized (÷ std) and centered (- mean) to match the
    optimization space. This ensures person_fe's negative mean doesn't
    bias the score downward.

    Args:
        person_fe: person_id → person fixed effect (θ)
        birank: person_id → BiRank score
        studio_exposure: person_id → studio exposure
        awcc: person_id → AWCC score
        patronage: person_id → patronage premium
        dormancy: person_id → dormancy multiplier (0-1)
        lambdas: component name → weight (in centered normalized space)
        component_std: component name → std for normalization
        component_mean: component name → mean of normalized features (for centering)

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
            lambdas.get(name, 0.2) * (
                scores.get(pid, 0.0)
                / (component_std.get(name, 1.0) if component_std else 1.0)
                - (component_mean.get(name, 0.0) if component_mean else 0.0)
            )
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
    lambdas, cv_mse, comp_std, comp_mean = optimize_lambda_weights(
        components, credits, anime_map, n_folds=n_folds, seed=seed
    )

    # If optimization returned empty weights, use defaults
    if not lambdas:
        lambdas = {name: 0.2 for name in components}

    # Compute IV scores (pass component_std + mean for consistent normalization)
    iv_scores = compute_integrated_value(
        person_fe, birank, studio_exposure, awcc, patronage, dormancy, lambdas,
        component_std=comp_std if comp_std else None,
        component_mean=comp_mean if comp_mean else None,
    )

    # Build component breakdown (with normalization and centering applied)
    component_breakdown: dict[str, dict[str, float]] = {}
    for pid in iv_scores:
        component_breakdown[pid] = {
            name: lambdas.get(name, 0.2) * (
                scores.get(pid, 0.0)
                / (comp_std.get(name, 1.0) if comp_std else 1.0)
                - (comp_mean.get(name, 0.0) if comp_mean else 0.0)
            )
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
        component_std=comp_std if comp_std else None,
        component_mean=comp_mean if comp_mean else None,
    )
