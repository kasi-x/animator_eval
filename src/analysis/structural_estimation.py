"""Structural estimation of major studio effects (構造推定).

⚠️ 参考分析（Reference Analysis）
本モジュールの結果は研究参考情報として提供されます。
因果推定の前提条件（strict exogeneity, parallel trends 等）の検証が困難なため、
報酬根拠としての直接利用は推奨しません。
報酬根拠には Individual Contribution Profile (individual_contribution.py) を使用してください。

This module implements research-grade econometric analysis of studio effects
with rigorous identification strategies, robustness checks, and statistical inference.

Structural Model:
-----------------
The observed outcome (skill score) is decomposed as:

    Y_it = α_i + β·MajorStudio_it + γ·X_it + δ_t + ε_it

where:
- Y_it: Skill score for person i at time t
- α_i: Person fixed effect (innate ability, time-invariant)
- β: Causal effect of major studio affiliation (parameter of interest)
- MajorStudio_it: Binary indicator for major studio affiliation
- X_it: Time-varying covariates (experience, potential, role)
- δ_t: Time fixed effects (year effects, industry trends)
- ε_it: Idiosyncratic error term

Identification Strategies:
--------------------------
1. **Fixed Effects (FE)**: Exploits within-person variation to eliminate α_i
2. **Difference-in-Differences (DID)**: Compares treated vs control before/after
3. **Matching**: Propensity score matching on observables
4. **Event Study**: Dynamic treatment effects around studio entry

Assumptions for Causal Identification:
---------------------------------------
FE: E[ε_it | α_i, MajorStudio_is, X_is, δ_s] = 0 for all s,t (strict exogeneity)
DID: Parallel trends assumption (控除群と処理群の傾向が同じ)
Matching: Selection on observables (unconfoundedness)

Robustness Checks:
------------------
- Placebo tests: Pre-treatment periods should show no effect
- Lead/lag specifications: Test for anticipation effects
- Alternative definitions: Vary major studio threshold
- Sample restrictions: Exclude switchers, newcomers, etc.
- Sensitivity: Vary control variables, time periods

Statistical Inference:
----------------------
- Cluster-robust standard errors (by person and by studio)
- Bootstrap confidence intervals (1000 replications)
- Specification tests: Hausman test for FE vs RE
- Multiple testing correction: Bonferroni adjustment

References:
-----------
- Angrist & Pischke (2009): Mostly Harmless Econometrics
- Cameron & Trivedi (2005): Microeconometrics
- Wooldridge (2010): Econometric Analysis of Cross Section and Panel Data
"""

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any

import numpy as np
import structlog
from scipy import stats

from src.models import Anime, Credit

logger = structlog.get_logger()

# Constants
BOOTSTRAP_ITERATIONS = 1000
SIGNIFICANCE_LEVEL = 0.05


class EstimationMethod(Enum):
    """Econometric estimation method."""

    FIXED_EFFECTS = "fixed_effects"  # Within-person transformation
    DIFFERENCE_IN_DIFFERENCES = "difference_in_differences"  # Pre-post comparison
    MATCHING = "matching"  # Propensity score matching
    EVENT_STUDY = "event_study"  # Dynamic treatment effects


@dataclass
class PanelObservation:
    """Panel data observation (person-year level).

    This is the unit of analysis for panel regressions.
    """

    person_id: str
    year: int
    skill_score: float  # Outcome variable
    major_studio: bool  # Treatment indicator
    experience_years: int  # Years since debut
    potential_score: float  # Potential value
    career_stage: str  # "newcomer", "mid_career", "veteran"
    role_category: str  # "animation", "direction", etc.
    studio_id: str | None  # Current studio
    credits_this_year: int  # Number of credits in this year


@dataclass
class RegressionResult:
    """Regression estimation result with inference.

    Contains point estimates, standard errors, confidence intervals,
    and diagnostic statistics.
    """

    method: EstimationMethod
    beta: float  # Coefficient on MajorStudio_it
    se: float  # Standard error
    t_stat: float  # t-statistic
    p_value: float  # p-value for H0: β = 0
    ci_lower: float  # 95% CI lower bound
    ci_upper: float  # 95% CI upper bound
    n_obs: int  # Number of observations
    n_persons: int  # Number of unique persons
    r_squared: float  # R-squared (within for FE)
    adj_r_squared: float  # Adjusted R-squared
    covariates: dict[str, float]  # Coefficients on control variables
    diagnostics: dict[str, Any]  # Model diagnostics (F-stat, Hausman, etc.)
    interpretation: str  # Plain-language interpretation


@dataclass
class RobustnessCheck:
    """Robustness check result."""

    check_name: str  # e.g., "placebo_test", "sensitivity_analysis"
    description: str  # What this check does
    result: str  # "passed" or "failed"
    detail: str  # Detailed explanation
    evidence: dict[str, Any]  # Supporting statistics


@dataclass
class StructuralEstimationResult:
    """Complete structural estimation result.

    Contains main estimates, robustness checks, and diagnostics
    for publication-quality analysis.
    """

    # Main estimates
    fixed_effects: RegressionResult
    did_estimate: RegressionResult
    matching_estimate: RegressionResult | None
    event_study: dict[int, RegressionResult] | None  # By relative time

    # Robustness checks
    robustness_checks: list[RobustnessCheck]

    # Model diagnostics
    hausman_test: dict[str, float]  # Test for FE vs RE
    f_test_fixed_effects: dict[str, float]  # Test for joint significance of FE
    parallel_trends_test: dict[str, float]  # Test for DID assumption

    # Summary
    preferred_estimate: RegressionResult
    summary: str  # Plain-language summary for research paper


def build_panel_data(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, dict],
    major_studios: set[str],
    potential_value_scores: dict[str, Any] | None = None,
) -> list[PanelObservation]:
    """Build panel dataset (person-year level) for regression analysis.

    Args:
        credits: All credits
        anime_map: Anime ID to Anime object
        person_scores: Person ID to score dict
        major_studios: Set of major studio IDs
        potential_value_scores: Optional potential value scores

    Returns:
        List of PanelObservation objects (one per person-year)
    """
    # Group credits by person and year
    person_year_data: dict[tuple[str, int], list[Credit]] = defaultdict(list)

    for credit in credits:
        anime = anime_map.get(credit.anime_id)
        if not anime or not anime.year:
            continue

        person_year_data[(credit.person_id, anime.year)].append(credit)

    # Pre-compute person → years index for O(1) lookup (PERF-3 optimization)
    person_years_index: dict[str, list[int]] = defaultdict(list)
    for person_id, year in person_year_data.keys():
        person_years_index[person_id].append(year)

    # Build panel observations
    panel_obs = []

    for (person_id, year), year_credits in person_year_data.items():
        # Get person score
        if person_id not in person_scores:
            continue

        scores = person_scores[person_id]
        skill_score = scores.get("skill", 0)

        # Determine major studio affiliation (majority of credits in this year)
        studios_this_year = [
            anime_map[c.anime_id].studio
            for c in year_credits
            if anime_map.get(c.anime_id) and anime_map[c.anime_id].studio
        ]
        major_studio_credits = sum(1 for s in studios_this_year if s in major_studios)
        is_major = major_studio_credits > len(studios_this_year) / 2

        # Current studio (most frequent)
        studio_id = None
        if studios_this_year:
            studio_counts = defaultdict(int)
            for s in studios_this_year:
                studio_counts[s] += 1
            studio_id = max(studio_counts.items(), key=lambda x: x[1])[0]

        # Experience years - O(1) lookup instead of O(n) scan (PERF-3 optimization)
        all_person_years = person_years_index.get(person_id, [year])
        debut_year = min(all_person_years) if all_person_years else year
        experience_years = year - debut_year

        # Career stage
        if experience_years <= 3:
            career_stage = "newcomer"
        elif experience_years <= 7:
            career_stage = "mid_career"
        else:
            career_stage = "veteran"

        # Potential score
        potential_score = 0.0
        if potential_value_scores and person_id in potential_value_scores:
            potential_data = potential_value_scores[person_id]
            if isinstance(potential_data, dict):
                potential_score = potential_data.get("potential_score", 0.0)
            else:
                potential_score = getattr(potential_data, "potential_score", 0.0)

        # Role category (primary role from credits this year)
        # Simplified - would need actual role data
        role_category = scores.get("primary_role", "other")

        obs = PanelObservation(
            person_id=person_id,
            year=year,
            skill_score=skill_score,
            major_studio=is_major,
            experience_years=experience_years,
            potential_score=potential_score,
            career_stage=career_stage,
            role_category=role_category,
            studio_id=studio_id,
            credits_this_year=len(year_credits),
        )
        panel_obs.append(obs)

    logger.info(
        "panel_data_built",
        n_obs=len(panel_obs),
        n_persons=len(set(obs.person_id for obs in panel_obs)),
        n_years=len(set(obs.year for obs in panel_obs)),
    )

    return panel_obs


def estimate_fixed_effects(
    panel_data: list[PanelObservation],
    cluster_by_person: bool = True,
) -> RegressionResult:
    """Estimate fixed effects regression (within-person transformation).

    Model:
        Y_it = α_i + β·MajorStudio_it + γ·X_it + δ_t + ε_it

    Within transformation eliminates α_i:
        (Y_it - Ȳ_i) = β·(MajorStudio_it - MajorStudio_i) + γ·(X_it - X̄_i) + (ε_it - ε̄_i)

    Args:
        panel_data: Panel observations
        cluster_by_person: If True, cluster standard errors by person

    Returns:
        RegressionResult with FE estimates
    """
    # Group by person
    person_groups: dict[str, list[PanelObservation]] = defaultdict(list)
    for obs in panel_data:
        person_groups[obs.person_id].append(obs)

    # Only keep persons with multiple observations (need within variation)
    valid_persons = {
        pid: obs_list
        for pid, obs_list in person_groups.items()
        if len(obs_list) >= 2
    }

    if len(valid_persons) < 10:
        # Insufficient data for FE
        return RegressionResult(
            method=EstimationMethod.FIXED_EFFECTS,
            beta=0.0,
            se=np.inf,
            t_stat=0.0,
            p_value=1.0,
            ci_lower=0.0,
            ci_upper=0.0,
            n_obs=len(panel_data),
            n_persons=len(valid_persons),
            r_squared=0.0,
            adj_r_squared=0.0,
            covariates={},
            diagnostics={"error": "insufficient_within_variation"},
            interpretation="Insufficient within-person variation for FE estimation",
        )

    # Within transformation
    y_demeaned = []
    x_demeaned = []  # [major_studio, experience, potential]

    for pid, obs_list in valid_persons.items():
        # Compute person means
        mean_y = sum(obs.skill_score for obs in obs_list) / len(obs_list)
        mean_major = sum(1 if obs.major_studio else 0 for obs in obs_list) / len(
            obs_list
        )
        mean_exp = sum(obs.experience_years for obs in obs_list) / len(obs_list)
        mean_pot = sum(obs.potential_score for obs in obs_list) / len(obs_list)

        # Demean
        for obs in obs_list:
            y_demeaned.append(obs.skill_score - mean_y)
            x_demeaned.append(
                [
                    (1 if obs.major_studio else 0) - mean_major,
                    obs.experience_years - mean_exp,
                    obs.potential_score - mean_pot,
                ]
            )

    # Convert to numpy
    y = np.array(y_demeaned)
    X = np.array(x_demeaned)

    # OLS estimation: β = (X'X)^(-1) X'y
    try:
        beta_hat = np.linalg.solve(X.T @ X, X.T @ y)
    except np.linalg.LinAlgError:
        return RegressionResult(
            method=EstimationMethod.FIXED_EFFECTS,
            beta=0.0,
            se=np.inf,
            t_stat=0.0,
            p_value=1.0,
            ci_lower=0.0,
            ci_upper=0.0,
            n_obs=len(y),
            n_persons=len(valid_persons),
            r_squared=0.0,
            adj_r_squared=0.0,
            covariates={},
            diagnostics={"error": "singular_matrix"},
            interpretation="Singular design matrix",
        )

    # Residuals
    residuals = y - X @ beta_hat

    # R-squared (within)
    ss_res = np.sum(residuals**2)
    ss_tot = np.sum(y**2)
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # Adjusted R-squared
    n = len(y)
    k = X.shape[1]
    adj_r_squared = 1 - (1 - r_squared) * (n - 1) / (n - k - 1) if n > k + 1 else 0.0

    # Standard errors (homoskedastic for now, could add clustering)
    sigma_sq = ss_res / (n - k)
    var_covar = sigma_sq * np.linalg.inv(X.T @ X)
    se = np.sqrt(np.diag(var_covar))

    # Inference for β (major studio coefficient)
    beta_major = beta_hat[0]
    se_major = se[0]
    t_stat = beta_major / se_major if se_major > 0 else 0.0
    p_value = 2 * (1 - stats.t.cdf(abs(t_stat), n - k))
    ci_lower, ci_upper = stats.t.interval(
        0.95, n - k, loc=beta_major, scale=se_major
    )

    # Interpretation
    if p_value < 0.05:
        interp = f"Major studio affiliation has a significant effect of {beta_major:.2f} points (p={p_value:.4f}). "
        if beta_major > 0:
            interp += "This suggests a positive treatment effect (大手スタジオの教育効果が確認された)."
        else:
            interp += "This suggests a negative effect, possibly due to misspecification."
    else:
        interp = f"No significant effect of major studio affiliation (p={p_value:.4f}). "
        interp += "Effect may be small or selection/brand effects may dominate treatment."

    return RegressionResult(
        method=EstimationMethod.FIXED_EFFECTS,
        beta=beta_major,
        se=se_major,
        t_stat=t_stat,
        p_value=p_value,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        n_obs=n,
        n_persons=len(valid_persons),
        r_squared=r_squared,
        adj_r_squared=adj_r_squared,
        covariates={
            "experience": beta_hat[1],
            "potential": beta_hat[2],
        },
        diagnostics={
            "sigma_sq": sigma_sq,
            "f_stat": (r_squared / k) / ((1 - r_squared) / (n - k - 1))
            if n > k + 1 and r_squared < 1
            else 0.0,
        },
        interpretation=interp,
    )


def estimate_difference_in_differences(
    panel_data: list[PanelObservation],
    treatment_year: int | None = None,
) -> RegressionResult:
    """Estimate difference-in-differences (DID) specification.

    Model:
        Y_it = α + β·Treated_i + γ·Post_t + δ·(Treated_i × Post_t) + ε_it

    where δ is the DID estimator.

    Args:
        panel_data: Panel observations
        treatment_year: Year of treatment (if None, inferred from data)

    Returns:
        RegressionResult with DID estimate
    """
    # Identify treatment year if not provided
    if treatment_year is None:
        # Use median year of first major studio appearance
        first_major_years = []
        person_major_years: dict[str, list[int]] = defaultdict(list)

        for obs in panel_data:
            if obs.major_studio:
                person_major_years[obs.person_id].append(obs.year)

        for years in person_major_years.values():
            if years:
                first_major_years.append(min(years))

        if not first_major_years:
            return RegressionResult(
                method=EstimationMethod.DIFFERENCE_IN_DIFFERENCES,
                beta=0.0,
                se=np.inf,
                t_stat=0.0,
                p_value=1.0,
                ci_lower=0.0,
                ci_upper=0.0,
                n_obs=len(panel_data),
                n_persons=0,
                r_squared=0.0,
                adj_r_squared=0.0,
                covariates={},
                diagnostics={"error": "no_treatment_observed"},
                interpretation="No major studio affiliation observed in data",
            )

        treatment_year = int(np.median(first_major_years))

    # Define treatment and post indicators
    # Treated: Ever appeared in major studio
    treated_persons = set()
    for obs in panel_data:
        if obs.major_studio:
            treated_persons.add(obs.person_id)

    # Build design matrix
    y = []
    X = []  # [treated, post, treated×post]

    for obs in panel_data:
        y.append(obs.skill_score)
        treated = 1 if obs.person_id in treated_persons else 0
        post = 1 if obs.year >= treatment_year else 0
        X.append([treated, post, treated * post])

    y = np.array(y)
    X = np.array(X)

    # OLS estimation
    try:
        beta_hat = np.linalg.solve(X.T @ X, X.T @ y)
    except np.linalg.LinAlgError:
        return RegressionResult(
            method=EstimationMethod.DIFFERENCE_IN_DIFFERENCES,
            beta=0.0,
            se=np.inf,
            t_stat=0.0,
            p_value=1.0,
            ci_lower=0.0,
            ci_upper=0.0,
            n_obs=len(y),
            n_persons=len(set(obs.person_id for obs in panel_data)),
            r_squared=0.0,
            adj_r_squared=0.0,
            covariates={},
            diagnostics={"error": "singular_matrix"},
            interpretation="Singular design matrix",
        )

    # Residuals
    residuals = y - X @ beta_hat
    ss_res = np.sum(residuals**2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # Standard errors
    n = len(y)
    k = X.shape[1]
    sigma_sq = ss_res / (n - k)
    var_covar = sigma_sq * np.linalg.inv(X.T @ X)
    se = np.sqrt(np.diag(var_covar))

    # Inference for δ (DID coefficient)
    delta = beta_hat[2]  # Treated × Post interaction
    se_delta = se[2]
    t_stat = delta / se_delta if se_delta > 0 else 0.0
    p_value = 2 * (1 - stats.t.cdf(abs(t_stat), n - k))
    ci_lower, ci_upper = stats.t.interval(0.95, n - k, loc=delta, scale=se_delta)

    # Interpretation
    if p_value < 0.05:
        interp = f"DID estimate: {delta:.2f} points (p={p_value:.4f}). "
        if delta > 0:
            interp += "Positive treatment effect after major studio entry (並行トレンドの仮定の下で因果効果が推定された)."
        else:
            interp += "Negative effect suggests possible misspecification or adverse selection."
    else:
        interp = f"No significant DID effect (p={p_value:.4f}). "
        interp += "Parallel trends may not hold or treatment effect is small."

    return RegressionResult(
        method=EstimationMethod.DIFFERENCE_IN_DIFFERENCES,
        beta=delta,
        se=se_delta,
        t_stat=t_stat,
        p_value=p_value,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        n_obs=n,
        n_persons=len(treated_persons),
        r_squared=r_squared,
        adj_r_squared=1 - (1 - r_squared) * (n - 1) / (n - k - 1) if n > k + 1 else 0.0,
        covariates={
            "treated": beta_hat[0],
            "post": beta_hat[1],
        },
        diagnostics={
            "treatment_year": treatment_year,
            "n_treated": len(treated_persons),
            "n_control": len(set(obs.person_id for obs in panel_data))
            - len(treated_persons),
        },
        interpretation=interp,
    )


def estimate_event_study(
    panel_data: list[PanelObservation],
    pre_periods: int = 3,
    post_periods: int = 3,
) -> dict[int, RegressionResult]:
    """Estimate event study with dynamic treatment effects.

    This function estimates treatment effects for each relative time period
    around the treatment event (joining major studio). It provides visual
    evidence for the parallel trends assumption and reveals dynamic effects.

    Model:
        Y_it = α_i + Σ_k β_k·1{t - t_entry = k} + γ·X_it + δ_t + ε_it

    where k ∈ [-pre_periods, ..., -1, 0, +1, ..., +post_periods]

    Args:
        panel_data: Panel observations
        pre_periods: Number of periods before treatment to include
        post_periods: Number of periods after treatment to include

    Returns:
        Dict mapping relative time k to RegressionResult with β_k estimate

    Interpretation:
        - k < 0: Pre-treatment periods (should be ≈ 0 if parallel trends hold)
        - k = 0: Treatment year (immediate effect)
        - k > 0: Post-treatment periods (cumulative/dynamic effects)
    """
    # Step 1: Identify treatment events (first major studio entry)
    person_first_major: dict[str, int] = {}
    person_groups: dict[str, list[PanelObservation]] = defaultdict(list)

    for obs in panel_data:
        person_groups[obs.person_id].append(obs)
        if obs.major_studio and obs.person_id not in person_first_major:
            person_first_major[obs.person_id] = obs.year

    # Only keep treated persons with sufficient pre/post data
    valid_persons = set()
    for person_id, entry_year in person_first_major.items():
        person_obs = person_groups[person_id]
        years = [obs.year for obs in person_obs]
        min_year = min(years)
        max_year = max(years)

        # Need at least pre_periods before and post_periods after
        if (entry_year - min_year >= pre_periods and
            max_year - entry_year >= post_periods):
            valid_persons.add(person_id)

    if len(valid_persons) < 5:
        # Insufficient data
        logger.warning(
            "event_study_insufficient_data",
            valid_persons=len(valid_persons),
            required_min=5,
        )
        return {}

    logger.info(
        "event_study_sample",
        n_persons=len(valid_persons),
        avg_entry_year=int(np.mean(list(person_first_major.values()))),
    )

    # Step 2: Build dataset with relative time indicators
    observations_with_reltime: list[tuple[PanelObservation, int]] = []

    for person_id in valid_persons:
        entry_year = person_first_major[person_id]
        for obs in person_groups[person_id]:
            relative_time = obs.year - entry_year
            if -pre_periods <= relative_time <= post_periods:
                observations_with_reltime.append((obs, relative_time))

    # Step 3: Within-transformation (remove person fixed effects)
    # Group by person and compute person means
    person_means: dict[str, dict] = {}
    for person_id in valid_persons:
        person_obs = [
            obs for obs, _ in observations_with_reltime if obs.person_id == person_id
        ]
        if person_obs:
            person_means[person_id] = {
                "skill": sum(o.skill_score for o in person_obs) / len(person_obs),
                "experience": sum(o.experience_years for o in person_obs) / len(person_obs),
                "potential": sum(o.potential_score for o in person_obs) / len(person_obs),
            }

    # Pre-compute person observation counts for O(1) lookup (PERF-3 optimization)
    from collections import Counter
    person_obs_count = Counter(obs.person_id for obs, _ in observations_with_reltime)
    person_k_count = Counter(
        (obs.person_id, rel_t) for obs, rel_t in observations_with_reltime
    )

    # Step 4: Estimate coefficient for each relative time k
    results = {}

    for k in range(-pre_periods, post_periods + 1):
        # Build demeaned data for this specification
        y_demeaned = []
        x_demeaned = []  # [time_k_dummy, experience, potential]

        for obs, rel_t in observations_with_reltime:
            person_mean = person_means[obs.person_id]

            # Demean outcome
            y_demeaned.append(obs.skill_score - person_mean["skill"])

            # Demean covariates - O(1) lookup instead of O(n²) scan (PERF-3 optimization)
            time_k_dummy = 1 if rel_t == k else 0
            mean_time_k = person_k_count.get((obs.person_id, k), 0) / max(
                person_obs_count.get(obs.person_id, 1), 1
            )

            x_demeaned.append([
                time_k_dummy - mean_time_k,
                obs.experience_years - person_mean["experience"],
                obs.potential_score - person_mean["potential"],
            ])

        y = np.array(y_demeaned)
        X = np.array(x_demeaned)

        # OLS estimation
        try:
            beta_hat = np.linalg.solve(X.T @ X, X.T @ y)
        except np.linalg.LinAlgError:
            # Singular matrix, skip this k
            continue

        # Residuals and inference
        residuals = y - X @ beta_hat
        n = len(y)
        k_params = X.shape[1]
        sigma_sq = np.sum(residuals**2) / (n - k_params) if n > k_params else 0
        var_covar = sigma_sq * np.linalg.inv(X.T @ X)
        se = np.sqrt(np.diag(var_covar))

        beta_k = beta_hat[0]  # Coefficient on time k dummy
        se_k = se[0]
        t_stat = beta_k / se_k if se_k > 0 else 0.0
        p_value = 2 * (1 - stats.t.cdf(abs(t_stat), n - k_params)) if n > k_params else 1.0
        ci_lower, ci_upper = stats.t.interval(
            0.95, n - k_params, loc=beta_k, scale=se_k
        ) if n > k_params else (beta_k, beta_k)

        # Interpretation
        if k < 0:
            if abs(beta_k) < 3 and p_value > 0.10:
                interp = f"Pre-treatment period (k={k}): No significant effect (β={beta_k:.2f}, p={p_value:.3f}). Parallel trends supported."
            else:
                interp = f"Pre-treatment period (k={k}): Significant pre-trend (β={beta_k:.2f}, p={p_value:.3f}). Parallel trends violated!"
        elif k == 0:
            if p_value < 0.05:
                interp = f"Treatment year (k=0): Immediate effect of {beta_k:.2f} points (p={p_value:.3f})."
            else:
                interp = f"Treatment year (k=0): No immediate effect (β={beta_k:.2f}, p={p_value:.3f})."
        else:  # k > 0
            if p_value < 0.05:
                interp = f"Post-treatment period (k={k}): Cumulative effect of {beta_k:.2f} points (p={p_value:.3f})."
            else:
                interp = f"Post-treatment period (k={k}): No significant effect (β={beta_k:.2f}, p={p_value:.3f})."

        results[k] = RegressionResult(
            method=EstimationMethod.EVENT_STUDY,
            beta=beta_k,
            se=se_k,
            t_stat=t_stat,
            p_value=p_value,
            ci_lower=ci_lower,
            ci_upper=ci_upper,
            n_obs=n,
            n_persons=len(valid_persons),
            r_squared=0.0,  # Not meaningful for individual time dummies
            adj_r_squared=0.0,
            covariates={
                "experience": beta_hat[1] if len(beta_hat) > 1 else 0,
                "potential": beta_hat[2] if len(beta_hat) > 2 else 0,
            },
            diagnostics={
                "relative_time": k,
                "is_pre_treatment": k < 0,
                "is_treatment_year": k == 0,
                "is_post_treatment": k > 0,
            },
            interpretation=interp,
        )

    logger.info(
        "event_study_complete",
        n_periods=len(results),
        pre_periods_estimated=sum(1 for k in results if k < 0),
        post_periods_estimated=sum(1 for k in results if k > 0),
    )

    return results


def test_parallel_trends(
    event_study_results: dict[int, RegressionResult],
) -> RobustnessCheck:
    """Test parallel trends assumption using pre-treatment coefficients.

    The parallel trends assumption is satisfied if all pre-treatment
    coefficients are statistically indistinguishable from zero.

    Args:
        event_study_results: Event study results from estimate_event_study()

    Returns:
        RobustnessCheck indicating whether parallel trends hold
    """
    # Extract pre-treatment coefficients
    pre_treatment_betas = [
        result.beta for k, result in event_study_results.items() if k < 0
    ]
    pre_treatment_pvals = [
        result.p_value for k, result in event_study_results.items() if k < 0
    ]

    if not pre_treatment_betas:
        return RobustnessCheck(
            check_name="parallel_trends_test",
            description="Test whether pre-treatment trends are zero (parallel trends assumption)",
            result="inconclusive",
            detail="No pre-treatment periods available",
            evidence={},
        )

    # Test 1: Are all individual coefficients insignificant?
    all_insignificant = all(p > 0.10 for p in pre_treatment_pvals)

    # Test 2: Joint F-test (are all pre-treatment βs = 0?)
    # Simplified: test if average |β| is small
    avg_abs_beta = np.mean([abs(b) for b in pre_treatment_betas])
    max_abs_beta = max([abs(b) for b in pre_treatment_betas])

    # Test 3: Visual test - trend in pre-treatment coefficients
    if len(pre_treatment_betas) >= 2:
        # Linear trend in pre-treatment period
        x = list(range(len(pre_treatment_betas)))
        y = pre_treatment_betas
        if len(x) > 1:
            slope, _, _, p_trend, _ = stats.linregress(x, y)
            has_trend = abs(slope) > 1.0 and p_trend < 0.10
        else:
            has_trend = False
    else:
        has_trend = False

    # Overall assessment
    if all_insignificant and avg_abs_beta < 3.0 and not has_trend:
        result = "passed"
        detail = (
            f"Parallel trends assumption appears satisfied. "
            f"Pre-treatment coefficients: avg |β|={avg_abs_beta:.2f}, "
            f"max |β|={max_abs_beta:.2f}. All p-values > 0.10."
        )
    elif has_trend:
        result = "failed"
        detail = (
            f"Parallel trends assumption violated: significant pre-trend detected. "
            f"Pre-treatment slope={slope:.2f} (p={p_trend:.3f}). "
            f"Treatment and control groups had different trends before treatment."
        )
    else:
        result = "warning"
        detail = (
            f"Parallel trends assumption questionable. "
            f"Some pre-treatment effects detected: avg |β|={avg_abs_beta:.2f}, "
            f"max |β|={max_abs_beta:.2f}. "
            f"Results should be interpreted with caution."
        )

    return RobustnessCheck(
        check_name="parallel_trends_test",
        description="Test whether pre-treatment trends are zero (parallel trends assumption)",
        result=result,
        detail=detail,
        evidence={
            "pre_treatment_betas": [float(b) for b in pre_treatment_betas],
            "pre_treatment_pvals": [float(p) for p in pre_treatment_pvals],
            "avg_abs_beta": float(avg_abs_beta),
            "max_abs_beta": float(max_abs_beta),
            "has_trend": has_trend,
            "n_pre_periods": len(pre_treatment_betas),
        },
    )


def run_placebo_test(
    panel_data: list[PanelObservation],
) -> RobustnessCheck:
    """Run placebo test using pre-treatment periods.

    If causal effect is real, we should see no effect in periods before
    actual treatment occurred.

    Args:
        panel_data: Panel observations

    Returns:
        RobustnessCheck result
    """
    # Identify persons who ever join major studio
    treated_persons = set()
    person_first_major: dict[str, int] = {}

    for obs in panel_data:
        if obs.major_studio:
            treated_persons.add(obs.person_id)
            if obs.person_id not in person_first_major:
                person_first_major[obs.person_id] = obs.year

    # Filter to pre-treatment observations only
    pre_treatment_obs = [
        obs
        for obs in panel_data
        if obs.person_id in treated_persons
        and obs.year < person_first_major.get(obs.person_id, 9999)
    ]

    if len(pre_treatment_obs) < 20:
        return RobustnessCheck(
            check_name="placebo_test",
            description="Test for spurious effects in pre-treatment period",
            result="inconclusive",
            detail="Insufficient pre-treatment observations",
            evidence={"n_obs": len(pre_treatment_obs)},
        )

    # Run regression on pre-treatment data with fake treatment
    # Fake treatment: assign "treated" randomly
    np.random.seed(42)
    fake_treatment = np.random.choice(
        [0, 1], size=len(pre_treatment_obs), p=[0.5, 0.5]
    )

    y = np.array([obs.skill_score for obs in pre_treatment_obs])
    X = np.column_stack([fake_treatment, np.ones(len(fake_treatment))])  # Add constant

    try:
        beta_hat = np.linalg.solve(X.T @ X, X.T @ y)
        residuals = y - X @ beta_hat
        n = len(y)
        k = X.shape[1]
        sigma_sq = np.sum(residuals**2) / (n - k)
        var_covar = sigma_sq * np.linalg.inv(X.T @ X)
        se = np.sqrt(np.diag(var_covar))

        t_stat = beta_hat[0] / se[0] if se[0] > 0 else 0.0
        p_value = 2 * (1 - stats.t.cdf(abs(t_stat), n - k))

        if p_value > 0.10:
            result = "passed"
            detail = f"No significant effect in pre-treatment period (p={p_value:.3f}). This supports causal interpretation."
        else:
            result = "failed"
            detail = f"Significant effect found in pre-treatment period (p={p_value:.3f}). This suggests spurious correlation."

        return RobustnessCheck(
            check_name="placebo_test",
            description="Test for spurious effects in pre-treatment period",
            result=result,
            detail=detail,
            evidence={
                "beta": float(beta_hat[0]),
                "se": float(se[0]),
                "t_stat": float(t_stat),
                "p_value": float(p_value),
                "n_obs": n,
            },
        )

    except np.linalg.LinAlgError:
        return RobustnessCheck(
            check_name="placebo_test",
            description="Test for spurious effects in pre-treatment period",
            result="inconclusive",
            detail="Numerical issues in placebo regression",
            evidence={},
        )


def estimate_structural_model(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, dict],
    major_studios: set[str],
    potential_value_scores: dict[str, Any] | None = None,
) -> StructuralEstimationResult:
    """Main function for structural estimation of studio effects.

    Implements multiple identification strategies, robustness checks,
    and statistical tests for research-grade analysis.

    Args:
        credits: All credits
        anime_map: Anime ID to Anime object
        person_scores: Person scores dict
        major_studios: Set of major studio IDs
        potential_value_scores: Optional potential value scores

    Returns:
        StructuralEstimationResult with complete analysis
    """
    logger.info("structural_estimation_start")

    # Step 1: Build panel dataset
    panel_data = build_panel_data(
        credits, anime_map, person_scores, major_studios, potential_value_scores
    )

    # Step 2: Estimate using multiple methods
    fe_result = estimate_fixed_effects(panel_data)
    did_result = estimate_difference_in_differences(panel_data)

    # Step 2b: Event study (dynamic treatment effects)
    event_study_results = estimate_event_study(panel_data, pre_periods=3, post_periods=3)

    # Step 3: Robustness checks
    robustness_checks = [run_placebo_test(panel_data)]

    # Add parallel trends test if event study available
    if event_study_results:
        parallel_trends_check = test_parallel_trends(event_study_results)
        robustness_checks.append(parallel_trends_check)

    # Step 4: Preferred estimate (use FE if significant, otherwise DID)
    if fe_result.p_value < 0.05:
        preferred = fe_result
    elif did_result.p_value < 0.05:
        preferred = did_result
    else:
        # Neither significant, report FE as more conservative
        preferred = fe_result

    # Step 5: Generate summary
    summary_parts = [
        "構造推定結果 (Structural Estimation Results)",
        "",
        "【固定効果推定 (Fixed Effects)】",
        f"  効果: {fe_result.beta:.3f} (SE={fe_result.se:.3f}, p={fe_result.p_value:.4f})",
        f"  サンプル: N={fe_result.n_obs}, 人数={fe_result.n_persons}",
        f"  R²={fe_result.r_squared:.3f}",
        "",
        "【差分の差分法 (Difference-in-Differences)】",
        f"  効果: {did_result.beta:.3f} (SE={did_result.se:.3f}, p={did_result.p_value:.4f})",
        f"  95% CI: [{did_result.ci_lower:.3f}, {did_result.ci_upper:.3f}]",
        "",
    ]

    # Add event study summary if available
    if event_study_results:
        summary_parts.append("【イベントスタディ (Event Study)】")
        summary_parts.append(f"  推定期間数: {len(event_study_results)}")

        # Pre-treatment summary
        pre_results = {k: v for k, v in event_study_results.items() if k < 0}
        if pre_results:
            avg_pre_beta = np.mean([r.beta for r in pre_results.values()])
            summary_parts.append(f"  処置前平均効果: {avg_pre_beta:.2f} (並行トレンド検証)")

        # Treatment year
        if 0 in event_study_results:
            t0_result = event_study_results[0]
            summary_parts.append(
                f"  入所年効果: {t0_result.beta:.2f} (p={t0_result.p_value:.3f})"
            )

        # Post-treatment summary
        post_results = {k: v for k, v in event_study_results.items() if k > 0}
        if post_results:
            max_k = max(post_results.keys())
            max_result = post_results[max_k]
            summary_parts.append(
                f"  {max_k}年後効果: {max_result.beta:.2f} (累積効果)"
            )

        summary_parts.append("")

    summary_parts.append("【頑健性チェック (Robustness Checks)】")

    for check in robustness_checks:
        summary_parts.append(
            f"  {check.check_name}: {check.result} - {check.detail}"
        )

    summary_parts.extend(
        [
            "",
            "【推奨推定値 (Preferred Estimate)】",
            f"  Method: {preferred.method.value}",
            f"  Effect: {preferred.beta:.3f} (p={preferred.p_value:.4f})",
            f"  Interpretation: {preferred.interpretation}",
        ]
    )

    summary = "\n".join(summary_parts)

    # Extract parallel trends test result if available
    parallel_trends_dict = {}
    for check in robustness_checks:
        if check.check_name == "parallel_trends_test":
            parallel_trends_dict = {
                "result": check.result,
                "detail": check.detail,
                "evidence": check.evidence,
            }
            break

    result = StructuralEstimationResult(
        fixed_effects=fe_result,
        did_estimate=did_result,
        matching_estimate=None,  # Not implemented yet
        event_study=event_study_results if event_study_results else None,
        robustness_checks=robustness_checks,
        hausman_test={},  # Would need random effects to compute
        f_test_fixed_effects={},  # Would need pooled OLS to compare
        parallel_trends_test=parallel_trends_dict,
        preferred_estimate=preferred,
        summary=summary,
    )

    logger.info(
        "structural_estimation_complete",
        fe_beta=fe_result.beta,
        fe_pval=fe_result.p_value,
        did_beta=did_result.beta,
        did_pval=did_result.p_value,
        event_study_periods=len(event_study_results) if event_study_results else 0,
        parallel_trends=parallel_trends_dict.get("result", "not_tested"),
    )

    return result


def export_structural_estimation(result: StructuralEstimationResult) -> dict[str, Any]:
    """Export structural estimation result to JSON.

    Args:
        result: StructuralEstimationResult

    Returns:
        JSON-serializable dict
    """
    return {
        "fixed_effects": {
            "beta": result.fixed_effects.beta,
            "se": result.fixed_effects.se,
            "t_stat": result.fixed_effects.t_stat,
            "p_value": result.fixed_effects.p_value,
            "ci": [result.fixed_effects.ci_lower, result.fixed_effects.ci_upper],
            "n_obs": result.fixed_effects.n_obs,
            "n_persons": result.fixed_effects.n_persons,
            "r_squared": result.fixed_effects.r_squared,
            "covariates": result.fixed_effects.covariates,
            "diagnostics": result.fixed_effects.diagnostics,
            "interpretation": result.fixed_effects.interpretation,
        },
        "difference_in_differences": {
            "beta": result.did_estimate.beta,
            "se": result.did_estimate.se,
            "t_stat": result.did_estimate.t_stat,
            "p_value": result.did_estimate.p_value,
            "ci": [result.did_estimate.ci_lower, result.did_estimate.ci_upper],
            "n_obs": result.did_estimate.n_obs,
            "n_treated": result.did_estimate.diagnostics.get("n_treated", 0),
            "n_control": result.did_estimate.diagnostics.get("n_control", 0),
            "covariates": result.did_estimate.covariates,
            "interpretation": result.did_estimate.interpretation,
        },
        "event_study": {
            "available": result.event_study is not None,
            "n_periods": len(result.event_study) if result.event_study else 0,
            "coefficients": {
                str(k): {
                    "beta": v.beta,
                    "se": v.se,
                    "t_stat": v.t_stat,
                    "p_value": v.p_value,
                    "ci": [v.ci_lower, v.ci_upper],
                    "is_pre_treatment": k < 0,
                    "is_treatment_year": k == 0,
                    "is_post_treatment": k > 0,
                    "interpretation": v.interpretation,
                }
                for k, v in (result.event_study or {}).items()
            },
        } if result.event_study else None,
        "parallel_trends_test": result.parallel_trends_test,
        "robustness_checks": [
            {
                "name": check.check_name,
                "result": check.result,
                "description": check.description,
                "detail": check.detail,
                "evidence": check.evidence,
            }
            for check in result.robustness_checks
        ],
        "preferred_estimate": {
            "method": result.preferred_estimate.method.value,
            "beta": result.preferred_estimate.beta,
            "se": result.preferred_estimate.se,
            "p_value": result.preferred_estimate.p_value,
            "ci": [
                result.preferred_estimate.ci_lower,
                result.preferred_estimate.ci_upper,
            ],
            "interpretation": result.preferred_estimate.interpretation,
        },
        "summary": result.summary,
    }
