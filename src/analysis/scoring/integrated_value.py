"""Integrated Value — PCA-weighted combination of 5 components.

Combines person FE, BiRank, studio exposure, AWCC, and patronage with
PCA PC1 loading-derived weights, multiplied by dormancy penalty.

IV = (λ1·θ + λ2·birank + λ3·studio_exp + λ4·awcc + λ5·patronage) × dormancy
"""

from collections import defaultdict
from dataclasses import dataclass

import numpy as np
import structlog

from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


@dataclass
class IntegratedValueResult:
    """Result of integrated value computation.

    Attributes:
        iv_scores: person_id → integrated value score
        lambda_weights: component name → optimized weight
        cv_mse: cross-validation mean squared error (legacy, always 0.0)
        component_breakdown: person_id → {component → value}
        component_std: component name → standard deviation (diagnostic)
        pca_variance_explained: PC1 variance explained ratio (0-1)
    """

    iv_scores: dict[str, float]
    lambda_weights: dict[str, float]
    cv_mse: float
    component_breakdown: dict[str, dict[str, float]]
    component_std: dict[str, float] | None = None
    component_mean: dict[str, float] | None = None
    pca_variance_explained: float = 0.0


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
) -> tuple[dict[str, float], float, dict[str, float], dict[str, float], float]:
    """Derive component weights from PCA PC1 loadings.

    PCA extracts the first principal component from the z-scored component
    matrix. The absolute loadings (with a 5% floor per component) are
    normalized to sum to 1.0 and used as lambda weights.

    Sign convention: PC1 is flipped so that the person_fe loading is positive,
    ensuring higher person_fe contributes positively to IV.

    Args:
        components: {component_name → {person_id → score}}
        credits: all credits (unused, kept for API compat)
        anime_map: anime_id → Anime (unused, kept for API compat)
        n_folds: unused (kept for API compat)
        seed: unused (kept for API compat)

    Returns:
        (lambda_weights, cv_mse=0.0, component_std, component_mean, variance_explained)
    """
    from sklearn.decomposition import PCA

    component_names = sorted(components.keys())
    n_components = len(component_names)

    if n_components == 0:
        return {}, 0.0, {}, {}, 0.0

    # Get all persons with components
    all_persons = set()
    for comp_scores in components.values():
        all_persons.update(comp_scores.keys())

    if len(all_persons) < 10:
        equal_w = 1.0 / n_components
        return {name: equal_w for name in component_names}, 0.0, {}, {}, 0.0

    # Build feature matrix for all persons
    person_list = sorted(all_persons)
    X = np.zeros((len(person_list), n_components), dtype=np.float64)
    for j, name in enumerate(component_names):
        for i, pid in enumerate(person_list):
            X[i, j] = components[name].get(pid, 0.0)

    # Normalize features: standard z-score (x - μ) / σ
    x_mean_raw = np.mean(X, axis=0)
    x_std = np.std(X, axis=0)
    x_std[x_std == 0] = 1.0
    # Floor: absolute minimum std of 1e-6 to avoid division explosion.
    # Unlike the old relative floor (1% of max std), this is independent of
    # other components — patronage's floor doesn't scale with birank's variance.
    x_std = np.maximum(x_std, 1e-6)
    X_norm = (X - x_mean_raw) / x_std

    # component_mean stores raw means, component_std stores raw stds.
    # To normalize a new value x: (x - mean) / std
    component_std = {name: float(x_std[j]) for j, name in enumerate(component_names)}
    component_mean = {
        name: float(x_mean_raw[j]) for j, name in enumerate(component_names)
    }

    # PCA: extract PC1 loadings as data-driven weights
    pca = PCA(n_components=1)
    pca.fit(X_norm)
    loadings = pca.components_[0]  # shape (n_components,)
    variance_explained = float(pca.explained_variance_ratio_[0])

    # Sign convention: flip so person_fe loading is positive
    pfe_idx = (
        component_names.index("person_fe") if "person_fe" in component_names else 0
    )
    if loadings[pfe_idx] < 0:
        loadings = -loadings

    # Absolute loadings → normalize to sum=1
    # No artificial floor: PCA loadings reflect true data structure.
    # A near-zero loading means the component is orthogonal to PC1 — forcing
    # 5% weight on it would inject noise into IV.
    raw_w = np.abs(loadings)
    raw_w /= raw_w.sum()

    lambda_weights = {name: float(raw_w[j]) for j, name in enumerate(component_names)}

    logger.info(
        "iv_weights_pca_pc1",
        weights={k: round(v, 4) for k, v in lambda_weights.items()},
        variance_explained=round(variance_explained, 4),
    )

    return lambda_weights, 0.0, component_std, component_mean, variance_explained


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

    IV = (Σ_k λ_k · ((x_k - μ_k) / σ_k)) × dormancy

    Components are z-score normalized: (x - mean) / std.
    Missing components (person has no value) contribute 0 to the sum,
    equivalent to "average person" — absence is not penalized.

    D07 note: λ weights come from PCA PC1 loadings (data-driven), not L2-penalized
    CV optimization. PCA loadings reflect the principal axis of variation.

    Args:
        person_fe: person_id → person fixed effect (θ)
        birank: person_id → BiRank score
        studio_exposure: person_id → studio exposure
        awcc: person_id → AWCC score
        patronage: person_id → patronage premium
        dormancy: person_id → dormancy multiplier (0-1)
        lambdas: component name → weight (in centered normalized space)
        component_std: component name → std for normalization
        component_mean: component name → raw mean (for centering)

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

    # Components where absence means "no data available" (not "zero contribution").
    # Missing studio_exposure = AKM couldn't estimate (non-mover), not "bad studio".
    # Missing patronage/awcc = genuinely no activity → use 0.0 (below-average is correct).
    _IMPUTE_MEAN_IF_MISSING = {"studio_exposure"}

    iv_scores: dict[str, float] = {}
    for pid in all_persons:
        raw = 0.0
        for name, scores in components.items():
            if pid not in scores:
                if name in _IMPUTE_MEAN_IF_MISSING:
                    # No data → contribute 0 in z-space (= population mean)
                    continue
                # Genuinely absent activity → use 0.0 raw value
                val = 0.0
            else:
                val = scores[pid]
            if component_std and component_mean:
                val = (val - component_mean.get(name, 0.0)) / component_std.get(
                    name, 1.0
                )
            raw += lambdas.get(name, 0.2) * val
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

    # Derive weights from PCA PC1 loadings
    lambdas, cv_mse, comp_std, comp_mean, variance_explained = optimize_lambda_weights(
        components, credits, anime_map, n_folds=n_folds, seed=seed
    )

    # If optimization returned empty weights, use defaults
    if not lambdas:
        lambdas = {name: 0.2 for name in components}

    # Compute IV scores (pass component_std + mean for consistent normalization)
    iv_scores = compute_integrated_value(
        person_fe,
        birank,
        studio_exposure,
        awcc,
        patronage,
        dormancy,
        lambdas,
        component_std=comp_std if comp_std else None,
        component_mean=comp_mean if comp_mean else None,
    )

    # Build component breakdown (with normalization and centering applied)
    _IMPUTE_MEAN = {"studio_exposure"}
    component_breakdown: dict[str, dict[str, float]] = {}
    for pid in iv_scores:
        bd: dict[str, float] = {}
        for name, scores in components.items():
            if pid not in scores and name in _IMPUTE_MEAN:
                bd[name] = 0.0  # mean-imputed → 0 in z-space
            elif comp_std and comp_mean:
                val = scores.get(pid, 0.0)
                bd[name] = lambdas.get(name, 0.2) * (
                    (val - comp_mean.get(name, 0.0)) / comp_std.get(name, 1.0)
                )
            else:
                bd[name] = lambdas.get(name, 0.2) * scores.get(pid, 0.0)
        bd["dormancy"] = dormancy.get(pid, 1.0)
        component_breakdown[pid] = bd

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
        pca_variance_explained=variance_explained,
    )
