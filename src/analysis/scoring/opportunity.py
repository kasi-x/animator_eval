"""Opportunity residual estimation via panel OLS + analytical CI + permutation null.

Replaces the prior cross-sectional heuristic with a panel regression that
controls for person fixed effects (theta_i from AKM), career tenure, role
diversity, studio, and year effects.  The residual represents structural
opportunity surplus or deficit *beyond* what those controls predict.

Method spec (CLAUDE.md H4 — compensation basis requires analytical CI):

    log(credit_count[i, year]) = β0 + β1·theta_i + β2·tenure_i
                                + β3·role_diversity_i
                                + α_studio[i] + γ_year + ε[i, year]

    opportunity_residual[i]  = mean over years( ε[i, year] )
    SE[i]                    = σ_ε / √n_years[i]    (analytical, ddof=1)
    CI95[i]                  = mean ± t_{n-1,0.975} · SE[i]

Permutation null (H0: opportunity is independent of person identity):
    - Permute person ids 1000 times
    - For each permutation, record the distribution of mean residuals
    - Empirical p-value = fraction of null draws ≥ |observed|
    - analytical CI and permutation null must both be stored

Hard constraints enforced here:
    - H1: anime.score NEVER enters any regression path — predictors are
      structural only (credit count, theta_i, tenure, role diversity, studio/year FE)
    - H4: SE = σ/√n only; heuristic CI is not accepted

Two entry points:

1. ``compute_opportunity_residual_panel(features, ...)`` — cross-sectional
   fallback (one aggregate row per person) used when only the pre-built
   features dict is available. Provides residuals but n_years=1, so no CI.

2. ``compute_opportunity_residual_from_credits(credits, anime_map, ...)`` —
   true per-(person, year) panel with studio FE (modal studio per person)
   and year FE (year dummies, reference = earliest observed year). This is
   the canonical path satisfying the full spec; it produces multi-year per-
   person residuals and therefore real analytical CIs.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from math import log
from typing import TYPE_CHECKING

import numpy as np
import structlog
from scipy.stats import t as t_dist

if TYPE_CHECKING:
    from src.runtime.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()

# Minimum observations (person-year cells) required to fit the panel
MIN_PANEL_OBS = 10
# Minimum years observed for a person to receive a CI (SE requires ≥2)
MIN_YEARS_FOR_CI = 2
# Number of permutation null draws
N_PERMUTATIONS = 1_000
# z-value for 95% confidence interval (large-n fallback)
Z_95 = 1.96


@dataclass
class OpportunityResidualResult:
    """Per-person opportunity residual with full inferential summary.

    Attributes:
        residual: mean OLS residual (ε̄_i across years). Positive = structural
            surplus of credited work beyond what controls predict.
            Negative = structural deficit (机会過少).
        se: analytical standard error σ_ε / √n_years.
        ci_lower: CI95 lower bound (residual - 1.96·SE).
        ci_upper: CI95 upper bound (residual + 1.96·SE).
        n_years: number of distinct calendar years observed for person i.
        p_value_permutation: fraction of |null draws| ≥ |observed| (two-sided).
            None when permutation was skipped (n < threshold).
    """

    residual: float | None
    se: float | None
    ci_lower: float | None
    ci_upper: float | None
    n_years: int
    p_value_permutation: float | None = None


@dataclass
class OpportunityModelSummary:
    """Summary statistics for the panel OLS fit."""

    r_squared: float | None
    n_persons: int
    n_panel_obs: int
    n_years_range: tuple[int, int] | None


def _build_panel_from_features(
    features: dict[str, dict],
    theta_map: dict[str, float] | None,
) -> tuple[
    list[tuple[str, int]],  # (person_id, year) row index
    np.ndarray,  # y: log(credit_count)
    np.ndarray,  # X: design matrix [theta_i, tenure_i, role_diversity_i, ...]
    list[str],  # person_ids ordered as rows
    dict[str, list[int]],  # person_id → list of row indices
]:
    """Construct panel from pre-built person features.

    Each row is one (person, year) observation.  When yearly granularity is
    unavailable the module falls back to a cross-section using total
    credit_count and career_years as the panel dimension.

    Args:
        features: person_id → feature dict from _build_person_features.
        theta_map: person_id → AKM theta_i (optional; zeros if None).

    Returns:
        (row_keys, y, X, ordered_person_ids, person_row_idx)
    """
    # We build the panel from the pre-aggregated features dict.
    # Each "person" contributes one aggregate row (cross-sectional fallback)
    # since the upstream features dict does not carry per-year credit counts.
    # A future upgrade can inject per-year panels when available.

    pids = sorted(features.keys())
    n = len(pids)

    if n < MIN_PANEL_OBS:
        return [], np.array([]), np.zeros((0, 0)), [], {}

    # --- Outcome: log(credit_count + 1) ---
    y = np.array(
        [np.log1p(features[pid].get("credit_count", 0)) for pid in pids],
        dtype=np.float64,
    )

    # --- Predictors (H1: structural only, no anime.score) ---
    theta_vec = np.array(
        [theta_map.get(pid, 0.0) if theta_map else 0.0 for pid in pids],
        dtype=np.float64,
    )
    tenure_vec = np.array(
        [float(features[pid].get("career_years", 0)) for pid in pids],
        dtype=np.float64,
    )
    # Role diversity: number of distinct primary_role values is a scalar per
    # person in the features dict.  We use the unique_studios count as a proxy
    # for structural breadth (H1-safe, structural count only).
    diversity_vec = np.array(
        [float(features[pid].get("unique_studios", 0)) for pid in pids],
        dtype=np.float64,
    )

    # Role dummy encoding (reference category = most frequent role)
    roles = sorted({features[pid].get("primary_role", "unknown") for pid in pids})
    role_dummies = _encode_role_dummies(pids, features, roles)

    # Design matrix: [theta_i, tenure_i, diversity_i, role_dummies...]
    X = np.column_stack([theta_vec, tenure_vec, diversity_vec] + list(role_dummies))

    # Row keys: (person_id, year=0) since we have one row per person
    row_keys = [(pid, 0) for pid in pids]

    # Person row index map: each person has exactly one row
    person_row_idx: dict[str, list[int]] = {pid: [i] for i, pid in enumerate(pids)}

    return row_keys, y, X, pids, person_row_idx


def _encode_role_dummies(
    pids: list[str],
    features: dict[str, dict],
    roles: list[str],
) -> list[np.ndarray]:
    """One-hot encode roles with reference category dropped."""
    if len(roles) <= 1:
        return []
    role_to_idx = {r: i for i, r in enumerate(roles)}
    n = len(pids)
    # Drop last category as reference
    n_dummies = len(roles) - 1
    dummies = []
    for d in range(n_dummies):
        col = np.zeros(n, dtype=np.float64)
        for i, pid in enumerate(pids):
            ridx = role_to_idx.get(features[pid].get("primary_role", roles[-1]), 0)
            if ridx == d:
                col[i] = 1.0
        dummies.append(col)
    return dummies


def _fit_ols(
    y: np.ndarray,
    X: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, float]:
    """Ordinary least squares with intercept.

    Args:
        y: outcome vector (n,)
        X: design matrix (n, p)

    Returns:
        (residuals, fitted, r_squared)
    """
    n = len(y)
    X_aug = np.column_stack([np.ones(n), X])
    try:
        beta, _, _, _ = np.linalg.lstsq(X_aug, y, rcond=None)
    except np.linalg.LinAlgError:
        return y - np.mean(y), np.full(n, np.mean(y)), 0.0

    fitted = X_aug @ beta
    residuals = y - fitted
    ss_res = float(np.sum(residuals**2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return residuals, fitted, r_squared


def _t_critical(n: int) -> float:
    """Two-tailed t critical value at 95% confidence for df = n - 1.

    Uses the t-distribution for all n (which converges to 1.96 as n → ∞).
    This is correct per analytical CI spec: SE = σ/√n with t_{n-1, 0.975}.

    Args:
        n: number of observations.

    Returns:
        t critical value (scalar ≥ 1.96).
    """
    return float(t_dist.ppf(0.975, df=n - 1))


def _compute_analytical_ci(
    residuals_by_person: dict[str, list[float]],
) -> dict[str, OpportunityResidualResult]:
    """Compute per-person mean residual and analytical SE = σ/√n.

    Uses t_{n-1, 0.975} (not z=1.96) for correct finite-sample coverage,
    per CLAUDE.md H4: analytical CI required for compensation claims.

    Args:
        residuals_by_person: person_id → list of per-row residuals.

    Returns:
        person_id → OpportunityResidualResult (p_value_permutation left None).
    """
    results: dict[str, OpportunityResidualResult] = {}
    for pid, resid_list in residuals_by_person.items():
        n = len(resid_list)
        arr = np.array(resid_list, dtype=np.float64)
        mean_resid = float(np.mean(arr))

        if n >= MIN_YEARS_FOR_CI:
            sigma = float(np.std(arr, ddof=1))  # sample std
            se = sigma / np.sqrt(n)
            t_crit = _t_critical(n)
            ci_lower = mean_resid - t_crit * se
            ci_upper = mean_resid + t_crit * se
        else:
            # Cannot compute CI with a single observation (ddof=1 undefined)
            se = None
            ci_lower = None
            ci_upper = None

        results[pid] = OpportunityResidualResult(
            residual=mean_resid,
            se=se,
            ci_lower=ci_lower,
            ci_upper=ci_upper,
            n_years=n,
        )
    return results


def _run_permutation_null(
    y: np.ndarray,
    X: np.ndarray,
    pids: list[str],
    observed_residuals: dict[str, float],
    n_permutations: int = N_PERMUTATIONS,
    rng: np.random.Generator | None = None,
) -> dict[str, float]:
    """Permutation null: shuffle person labels, refit OLS, record null distribution.

    H0: opportunity is independent of person identity.

    Empirical p-value (two-sided) for person i:
        p_i = #{|null_mean_i| >= |observed_mean_i|} / n_permutations

    Args:
        y: outcome vector.
        X: design matrix.
        pids: ordered list of person IDs.
        observed_residuals: person_id → mean residual from observed fit.
        n_permutations: number of permutation draws.
        rng: random number generator (for reproducibility in tests).

    Returns:
        person_id → empirical p-value.
    """
    if rng is None:
        rng = np.random.default_rng(seed=42)

    n = len(pids)
    obs_abs = np.array(
        [abs(observed_residuals.get(pid, 0.0)) for pid in pids], dtype=np.float64
    )

    # Count how many null draws exceed observed |residual| per person
    exceed_count = np.zeros(n, dtype=np.int64)

    for _ in range(n_permutations):
        # Permute outcome labels (not predictors) — tests person-identity independence
        y_perm = rng.permutation(y)
        residuals_perm, _, _ = _fit_ols(y_perm, X)
        null_abs = np.abs(residuals_perm)
        exceed_count += (null_abs >= obs_abs).astype(np.int64)

    p_values = {pids[i]: float(exceed_count[i]) / n_permutations for i in range(n)}
    return p_values


def compute_opportunity_residual_panel(
    features: dict[str, dict],
    theta_map: dict[str, float] | None = None,
    n_permutations: int = N_PERMUTATIONS,
    rng: np.random.Generator | None = None,
) -> tuple[dict[str, OpportunityResidualResult], OpportunityModelSummary]:
    """Compute opportunity residuals with analytical CI and permutation null.

    Panel OLS specification (H1-safe: no anime.score):

        log(credit_count[i]) = β0 + β1·theta_i + β2·tenure_i
                              + β3·role_diversity_i + role_dummies + ε[i]

        opportunity_residual[i] = mean(ε[i])
        SE[i]                   = σ_ε / √n_years[i]     (analytical CI)
        CI95[i]                 = ±1.96 · SE[i]

    Permutation null establishes whether the observed residual distribution
    departs from the null hypothesis of person-identity independence.

    Args:
        features: person_id → feature dict (from _build_person_features).
            Required keys: credit_count, career_years, unique_studios,
            primary_role.  Optional: iv_score (not used in regression).
        theta_map: person_id → AKM theta_i fixed effect.  Zeros if not
            provided.  H1: theta_i encodes production scale structural signal;
            it does NOT use anime.score.
        n_permutations: number of permutation draws for null model.
            Set to 0 to skip permutation (e.g., in unit tests).
        rng: numpy RNG for permutation reproducibility.

    Returns:
        (person_id → OpportunityResidualResult, OpportunityModelSummary)
    """
    pids_in = list(features.keys())
    if len(pids_in) < MIN_PANEL_OBS:
        logger.warning(
            "opportunity_residual_insufficient_data",
            n_persons=len(pids_in),
            min_required=MIN_PANEL_OBS,
        )
        empty: dict[str, OpportunityResidualResult] = {
            pid: OpportunityResidualResult(
                residual=None, se=None, ci_lower=None, ci_upper=None, n_years=0
            )
            for pid in pids_in
        }
        return empty, OpportunityModelSummary(
            r_squared=None, n_persons=len(pids_in), n_panel_obs=0, n_years_range=None
        )

    # --- Build panel ---
    row_keys, y, X, pids, person_row_idx = _build_panel_from_features(
        features, theta_map
    )
    n_obs = len(y)
    if n_obs == 0:
        logger.warning("opportunity_residual_no_panel_obs")
        empty = {
            pid: OpportunityResidualResult(
                residual=None, se=None, ci_lower=None, ci_upper=None, n_years=0
            )
            for pid in pids_in
        }
        return empty, OpportunityModelSummary(
            r_squared=None, n_persons=len(pids_in), n_panel_obs=0, n_years_range=None
        )

    # --- OLS fit ---
    residuals_arr, _, r_squared = _fit_ols(y, X)

    # --- Group residuals by person ---
    residuals_by_person: dict[str, list[float]] = defaultdict(list)
    for i, pid in enumerate(pids):
        row_indices = person_row_idx.get(pid, [])
        for ridx in row_indices:
            residuals_by_person[pid].append(float(residuals_arr[ridx]))

    # --- Analytical CI ---
    results = _compute_analytical_ci(residuals_by_person)

    # --- Permutation null ---
    if n_permutations > 0:
        observed_means = {
            pid: r.residual for pid, r in results.items() if r.residual is not None
        }
        if observed_means:
            p_values = _run_permutation_null(
                y, X, pids, observed_means, n_permutations=n_permutations, rng=rng
            )
            for pid, p_val in p_values.items():
                if pid in results:
                    results[pid].p_value_permutation = p_val

    # Persons in input but not in panel (filtered out) get None result
    for pid in pids_in:
        if pid not in results:
            results[pid] = OpportunityResidualResult(
                residual=None, se=None, ci_lower=None, ci_upper=None, n_years=0
            )

    n_years_list = [r.n_years for r in results.values() if r.n_years > 0]
    n_years_range = (min(n_years_list), max(n_years_list)) if n_years_list else None

    logger.info(
        "opportunity_residual_computed",
        n_persons=len(pids),
        n_panel_obs=n_obs,
        r_squared=round(r_squared, 3) if r_squared is not None else None,
        with_ci=sum(1 for r in results.values() if r.ci_lower is not None),
        with_p_value=sum(
            1 for r in results.values() if r.p_value_permutation is not None
        ),
        n_permutations=n_permutations,
    )

    summary = OpportunityModelSummary(
        r_squared=round(r_squared, 3) if r_squared is not None else None,
        n_persons=len(pids),
        n_panel_obs=n_obs,
        n_years_range=n_years_range,
    )
    return results, summary


# ---------------------------------------------------------------------------
# True per-(person, year) panel construction from credits + anime_map
# ---------------------------------------------------------------------------


def _role_diversity_entropy(role_counts: Counter) -> float:
    """Shannon entropy of a person's role-category distribution.

    Bounded to [0, 1] by dividing by log(K) where K is the number of role
    categories observed. Normalisation lets the predictor be compared across
    persons regardless of how many roles appear in the corpus.

    Args:
        role_counts: Counter of role labels (strings).

    Returns:
        normalised entropy in [0, 1] (0 = single role; 1 = uniform across roles).
    """
    total = sum(role_counts.values())
    if total == 0 or len(role_counts) <= 1:
        return 0.0
    probs = np.array([c / total for c in role_counts.values()], dtype=np.float64)
    h = -np.sum(probs * np.log(probs + 1e-12))
    h_max = log(len(role_counts))
    return float(h / h_max) if h_max > 0 else 0.0


@dataclass
class _PanelObservation:
    """One (person, year) cell of the panel."""

    person_id: str
    year: int
    log_credits: float
    theta: float
    tenure: float
    role_diversity: float
    studio: str


def _build_panel_from_credits(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    theta_map: dict[str, float] | None,
) -> list[_PanelObservation]:
    """Construct per-(person, year) panel observations from credits.

    Outcome: log(1 + credit_count[i, year]).
    Predictors: theta_i, tenure_i (years since first credit), role_diversity_i
    (Shannon entropy of role distribution over the entire career), studio_i
    (modal studio across career — used as fixed effect dummy).

    H1: anime.score is never read.  Only structural counts and roles.

    Args:
        credits: list of Credit objects.
        anime_map: anime_id → Anime (requires .year).
        theta_map: person_id → AKM theta_i (zeros when missing).

    Returns:
        List of _PanelObservation rows.  Empty if insufficient data.
    """
    # Group credits by (person, year)
    per_person_year: dict[tuple[str, int], int] = defaultdict(int)
    per_person_first_year: dict[str, int] = {}
    per_person_role_counts: dict[str, Counter] = defaultdict(Counter)
    per_person_studio_counts: dict[str, Counter] = defaultdict(Counter)

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime is None:
            continue
        # Prefer credit_year if set (per-episode attribution), else anime.year
        year = getattr(c, "credit_year", None) or anime.year
        if year is None:
            continue
        per_person_year[(c.person_id, int(year))] += 1
        prev_first = per_person_first_year.get(c.person_id)
        if prev_first is None or year < prev_first:
            per_person_first_year[c.person_id] = int(year)
        # Role label: use .value for Enum, or str() fallback
        role_label = getattr(c.role, "value", str(c.role))
        per_person_role_counts[c.person_id][role_label] += 1
        for s in anime.studios or []:
            per_person_studio_counts[c.person_id][s] += 1

    # Precompute per-person diversity and modal studio
    per_person_diversity = {
        pid: _role_diversity_entropy(rc) for pid, rc in per_person_role_counts.items()
    }
    per_person_studio = {
        pid: (sc.most_common(1)[0][0] if sc else "_unknown")
        for pid, sc in per_person_studio_counts.items()
    }

    observations: list[_PanelObservation] = []
    for (pid, year), n_credits in per_person_year.items():
        first_year = per_person_first_year[pid]
        tenure = float(year - first_year)
        observations.append(
            _PanelObservation(
                person_id=pid,
                year=year,
                log_credits=float(np.log1p(n_credits)),
                theta=float(theta_map.get(pid, 0.0)) if theta_map else 0.0,
                tenure=tenure,
                role_diversity=per_person_diversity.get(pid, 0.0),
                studio=per_person_studio.get(pid, "_unknown"),
            )
        )
    return observations


def _encode_categorical_fe(
    values: list[str], drop_reference: bool = True
) -> tuple[np.ndarray, list[str]]:
    """One-hot encode a categorical with optional reference-category drop.

    Args:
        values: list of category labels (length n).
        drop_reference: drop the most frequent category as reference.

    Returns:
        (design block of shape (n, K) or (n, K-1), list of category labels kept).
    """
    counts = Counter(values)
    if not counts:
        return np.zeros((len(values), 0)), []
    cats = [c for c, _ in counts.most_common()]
    if drop_reference and len(cats) > 1:
        cats_kept = cats[1:]  # drop the most frequent as reference
    else:
        cats_kept = cats
    if not cats_kept:
        return np.zeros((len(values), 0)), []
    idx = {c: j for j, c in enumerate(cats_kept)}
    M = np.zeros((len(values), len(cats_kept)), dtype=np.float64)
    for i, v in enumerate(values):
        j = idx.get(v)
        if j is not None:
            M[i, j] = 1.0
    return M, cats_kept


def compute_opportunity_residual_from_credits(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    theta_map: dict[str, float] | None = None,
    n_permutations: int = N_PERMUTATIONS,
    rng: np.random.Generator | None = None,
) -> tuple[dict[str, OpportunityResidualResult], OpportunityModelSummary]:
    """Compute opportunity residuals on a true per-(person, year) panel.

    Specification (full spec; H1-safe):

        log(credit_count[i, year]) = β0 + β1·theta_i + β2·tenure_i
                                    + β3·role_diversity_i
                                    + α_studio[i] + γ_year + ε[i, year]

        opportunity_residual[i] = mean over years ( ε[i, year] )
        SE[i] = σ_ε[i] / √n_years[i]  (analytical, within-person ddof=1)
        CI95[i] = mean ± t_{n-1, 0.975} · SE[i]

    Studio FE uses the person's **modal** studio (most-frequent across credits).
    This is the conservative choice — alternative (per-year studio) would
    explode dimensionality and overfit thin years. Year FE uses dummies with
    the earliest observed year as reference.

    Permutation null shuffles the outcome vector; same as the cross-sectional
    fallback. Empirical two-sided p-value per person.

    Args:
        credits: credit records with role + person_id + anime_id.
        anime_map: anime_id → Anime (must carry .year and .studios for FE).
        theta_map: person_id → AKM theta_i (zeros for missing).
        n_permutations: permutation draws (0 = skip).
        rng: numpy RNG (reproducibility).

    Returns:
        (person_id → OpportunityResidualResult, OpportunityModelSummary)
    """
    observations = _build_panel_from_credits(credits, anime_map, theta_map)
    pids_in = sorted({o.person_id for o in observations})

    n_obs = len(observations)
    if n_obs < MIN_PANEL_OBS or len(pids_in) < MIN_PANEL_OBS:
        logger.warning(
            "opportunity_residual_panel_insufficient",
            n_obs=n_obs,
            n_persons=len(pids_in),
            min_required=MIN_PANEL_OBS,
        )
        empty: dict[str, OpportunityResidualResult] = {
            pid: OpportunityResidualResult(
                residual=None, se=None, ci_lower=None, ci_upper=None, n_years=0
            )
            for pid in pids_in
        }
        return empty, OpportunityModelSummary(
            r_squared=None,
            n_persons=len(pids_in),
            n_panel_obs=n_obs,
            n_years_range=None,
        )

    # --- Outcome and continuous predictors ---
    y = np.array([o.log_credits for o in observations], dtype=np.float64)
    theta_vec = np.array([o.theta for o in observations], dtype=np.float64)
    tenure_vec = np.array([o.tenure for o in observations], dtype=np.float64)
    div_vec = np.array([o.role_diversity for o in observations], dtype=np.float64)

    # --- Studio FE (modal studio per person; reference = most-frequent) ---
    studio_block, _ = _encode_categorical_fe(
        [o.studio for o in observations], drop_reference=True
    )

    # --- Year FE (reference = most-frequent year) ---
    year_block, _ = _encode_categorical_fe(
        [str(o.year) for o in observations], drop_reference=True
    )

    # Continuous block — guard against degenerate columns (zero variance)
    cont_cols = []
    for col in (theta_vec, tenure_vec, div_vec):
        if np.std(col) > 1e-12:
            cont_cols.append(col)
    cont_block = np.column_stack(cont_cols) if cont_cols else np.zeros((n_obs, 0))

    X = (
        np.column_stack(
            [
                block
                for block in (cont_block, studio_block, year_block)
                if block.size > 0
            ]
        )
        if (cont_block.size + studio_block.size + year_block.size) > 0
        else np.zeros((n_obs, 0))
    )

    residuals_arr, _, r_squared = (
        _fit_ols(y, X)
        if X.size > 0
        else (
            y - float(np.mean(y)),
            np.full(n_obs, float(np.mean(y))),
            0.0,
        )
    )

    # Group residuals by person
    residuals_by_person: dict[str, list[float]] = defaultdict(list)
    for i, o in enumerate(observations):
        residuals_by_person[o.person_id].append(float(residuals_arr[i]))

    results = _compute_analytical_ci(residuals_by_person)

    # Permutation null on the full row-level outcome
    if n_permutations > 0 and X.size > 0:
        if rng is None:
            rng = np.random.default_rng(seed=42)
        # Build per-person observed mean residual vector to compare against
        # null shuffles. Null draw: shuffle y, refit, recompute per-person means.
        obs_means = np.array(
            [float(np.mean(residuals_by_person[pid])) for pid in pids_in],
            dtype=np.float64,
        )
        person_to_row_indices: dict[str, list[int]] = defaultdict(list)
        for i, o in enumerate(observations):
            person_to_row_indices[o.person_id].append(i)

        exceed_count = np.zeros(len(pids_in), dtype=np.int64)
        obs_abs = np.abs(obs_means)
        for _ in range(n_permutations):
            y_perm = rng.permutation(y)
            r_perm, _, _ = _fit_ols(y_perm, X)
            null_means = np.array(
                [float(np.mean(r_perm[person_to_row_indices[pid]])) for pid in pids_in],
                dtype=np.float64,
            )
            exceed_count += (np.abs(null_means) >= obs_abs).astype(np.int64)

        for j, pid in enumerate(pids_in):
            if pid in results:
                results[pid].p_value_permutation = (
                    float(exceed_count[j]) / n_permutations
                )

    n_years_list = [r.n_years for r in results.values() if r.n_years > 0]
    n_years_range = (min(n_years_list), max(n_years_list)) if n_years_list else None

    logger.info(
        "opportunity_residual_panel_from_credits",
        n_persons=len(pids_in),
        n_panel_obs=n_obs,
        r_squared=round(r_squared, 3),
        with_ci=sum(1 for r in results.values() if r.ci_lower is not None),
        with_p_value=sum(
            1 for r in results.values() if r.p_value_permutation is not None
        ),
        n_permutations=n_permutations,
    )

    summary = OpportunityModelSummary(
        r_squared=round(r_squared, 3),
        n_persons=len(pids_in),
        n_panel_obs=n_obs,
        n_years_range=n_years_range,
    )
    return results, summary


# ---------------------------------------------------------------------------
# Q-Q diagnostic for residual normality (Stop-if condition)
# ---------------------------------------------------------------------------


def residual_qq_deviation(residuals: list[float]) -> float:
    """Compute mean absolute deviation from the theoretical normal Q-Q line.

    Returns 0.0 for perfect normality, larger values indicate skewness or
    heavy tails. Used as a guard for the Stop-if condition: deviation > 0.5
    suggests log-link GLM may be more appropriate than OLS on log-counts.

    Args:
        residuals: flat list of OLS residuals (any source).

    Returns:
        Mean absolute deviation between empirical and theoretical quantiles
        after standardisation. Lower is better.
    """
    if len(residuals) < 5:
        return 0.0
    arr = np.array(residuals, dtype=np.float64)
    mu, sigma = float(np.mean(arr)), float(np.std(arr, ddof=1))
    if sigma <= 0:
        return 0.0
    standardised = np.sort((arr - mu) / sigma)
    n = len(standardised)
    # Theoretical normal quantiles for ranks 1..n
    from scipy.stats import norm

    theoretical = norm.ppf((np.arange(1, n + 1) - 0.5) / n)
    return float(np.mean(np.abs(standardised - theoretical)))


# ---------------------------------------------------------------------------
# CLI calibration check entry point
# ---------------------------------------------------------------------------


def _calibration_check(n_persons: int = 200, n_sim: int = 500) -> None:
    """Validate that empirical CI coverage meets 95% ±2pp nominal.

    Simulates a per-person multi-year residual draw under the null
    (ε ~ N(0, σ²) i.i.d. with true mean 0) and checks that the analytical
    95% CI captures the true mean in approximately 95% of replications.

    This is the direct test of CI calibration for the SE = σ/√n formula
    independent of the regression machinery (which is also exercised in
    tests/analysis/scoring/test_opportunity.py).

    Args:
        n_persons: synthetic dataset size per simulation.
        n_sim: number of simulation replications.
    """
    rng = np.random.default_rng(0)
    n_per_person = 6  # multi-year panel depth per person
    coverage_hits = 0
    total = 0

    for _ in range(n_sim):
        residuals_by_person = {
            f"p{i}": rng.normal(loc=0.0, scale=1.0, size=n_per_person).tolist()
            for i in range(n_persons)
        }
        results = _compute_analytical_ci(residuals_by_person)
        for r in results.values():
            if r.ci_lower is None:
                continue
            total += 1
            if r.ci_lower <= 0.0 <= r.ci_upper:
                coverage_hits += 1

    empirical_coverage = coverage_hits / total if total > 0 else 0.0
    target = 0.95
    tolerance = 0.02
    ok = abs(empirical_coverage - target) <= tolerance
    status = "PASS" if ok else "FAIL"
    print(
        f"[{status}] CI coverage calibration: empirical={empirical_coverage:.4f} "
        f"target={target:.3f} tolerance=±{tolerance:.3f} "
        f"(n_persons={n_persons}, n_sim={n_sim}, n_obs_per_person={n_per_person})"
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Opportunity residual module utilities"
    )
    parser.add_argument(
        "--calibration-check",
        action="store_true",
        help="Run CI coverage calibration check",
    )
    parser.add_argument("--n-persons", type=int, default=200)
    parser.add_argument("--n-sim", type=int, default=500)
    args = parser.parse_args()

    if args.calibration_check:
        _calibration_check(n_persons=args.n_persons, n_sim=args.n_sim)
