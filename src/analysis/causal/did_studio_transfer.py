"""Difference-in-Differences analysis of studio transfer treatment.

Estimates causal effects of an inter-studio move on three structural outcomes:
  - theta_i (AKM person fixed effect)
  - opportunity_residual
  - credit_count (log-transformed)

Specification:

    y[i, t] = alpha_i + gamma_t + beta * post[i, t] * treated[i]
            + X[i, t] * delta + epsilon[i, t]

    alpha_i  : person fixed effect
    gamma_t  : year fixed effect
    treated[i]: 1 if person i makes a qualifying studio transfer during
                the observation window
    post[i, t]: 1 for t >= event_year[i]

Event-study extension (±5-year window):

    y[i, t] = alpha_i + gamma_t
            + Σ_{k=-5, k≠-1}^{+5} beta_k * 1[t - event_year[i] = k]
            + epsilon[i, t]

    k = -1 is the baseline period (omitted).
    Leads k ∈ {-5, -4, -3, -2} test parallel trends:
        H0: beta_k = 0 for all k < -1 (pre-treatment leads not different from 0)
    A joint F-test on pre-period leads (k ∈ {-3, -2}) is used as the primary
    parallel trends test.

Cluster-robust SE: clustered at person level (sandwich estimator).

Hard constraints:
    H1: anime.score NEVER enters any regression path.
    H4: Analytical CI required (cluster SE = sandwich estimator).
    H2: Results described as "structural position changes", not "improvement".
    H3: Studio assignment from Resolved layer data only.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import structlog
from scipy.stats import f as f_dist, t as t_dist

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Default event-study window: leads (-EVENT_WINDOW_YEARS .. -1) + lags (0 .. +EVENT_WINDOW_YEARS)
EVENT_WINDOW_YEARS: int = 5

#: Minimum credited works at new studio in transfer year to qualify (exclude brief visits)
MIN_CREDITS_NEW_STUDIO: int = 3

#: Minimum credited works at old studio in pre-transfer year to qualify
MIN_CREDITS_OLD_STUDIO: int = 3

#: Rolling window (years) for computing primary_studio assignment
PRIMARY_STUDIO_WINDOW: int = 3

#: Minimum panel observations for estimation to proceed
MIN_PANEL_OBS: int = 20

#: 5% significance level for parallel trends pre-period test
PARALLEL_TRENDS_ALPHA: float = 0.05


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class PersonYearObs:
    """One observation in the person × year panel.

    Attributes:
        person_id: canonical person identifier (Resolved layer)
        year: calendar year
        theta_i: AKM person fixed effect for this year (interpolated from nearest estimate)
        opportunity_residual: structural credit surplus/deficit (may be None)
        log_credit_count: log1p(annual credit count)
        tenure: years since first credited work
        role_diversity: distinct role group count in this year
        cohort_year: year of first credit (for cohort matching)
        primary_role_group: dominant role group in this year
    """

    person_id: str
    year: int
    theta_i: float
    opportunity_residual: float | None
    log_credit_count: float
    tenure: float
    role_diversity: float
    cohort_year: int
    primary_role_group: str


@dataclass
class TransferRecord:
    """Studio transfer event for a single person.

    Attributes:
        person_id: canonical person identifier
        event_year: year of first credit at the new studio (t=0)
        old_studio: studio identifier before transfer
        new_studio: studio identifier after transfer
        cohort_year: year of first credit (used for cohort matching)
        primary_role_group: dominant role group at event year
    """

    person_id: str
    event_year: int
    old_studio: str
    new_studio: str
    cohort_year: int
    primary_role_group: str


@dataclass
class DiDEstimate:
    """Two-way FE DiD point estimate with cluster-sandwich SE.

    Attributes:
        outcome: name of the dependent variable
        beta: ATT estimate (post × treated coefficient)
        se: cluster-sandwich standard error (person-clustered sandwich)
        ci_lower: 95% CI lower bound
        ci_upper: 95% CI upper bound
        t_stat: t-statistic (beta / se)
        p_value: two-sided p-value
        n_obs: total panel observations
        n_treated: number of treated persons
        n_control: number of control persons
        method: estimation description
    """

    outcome: str
    beta: float
    se: float
    ci_lower: float
    ci_upper: float
    t_stat: float
    p_value: float
    n_obs: int
    n_treated: int
    n_control: int
    method: str = "two-way FE DiD, person-clustered SE"


@dataclass
class EventStudyCoefficient:
    """Single event-study coefficient for relative-time period k.

    Attributes:
        k: relative time (negative = pre-treatment, 0 = event year, positive = post)
        beta: coefficient estimate
        se: cluster-sandwich standard error
        ci_lower: 95% CI lower bound
        ci_upper: 95% CI upper bound
        t_stat: t-statistic
        p_value: two-sided p-value
        is_baseline: True for k = -1 (omitted baseline, all zeros by construction)
    """

    k: int
    beta: float
    se: float
    ci_lower: float
    ci_upper: float
    t_stat: float
    p_value: float
    is_baseline: bool = False


@dataclass
class ParallelTrendsResult:
    """Joint pre-period parallel trends test (F-test on leads k ∈ {-3, -2}).

    Attributes:
        f_stat: F-statistic of joint test
        p_value: p-value of joint F-test
        df_num: numerator degrees of freedom (number of lead restrictions)
        df_denom: denominator degrees of freedom (panel dof)
        leads_tested: which k values were jointly tested
        pre_period_betas: beta estimates for the tested leads
        pre_period_ses: SE estimates for the tested leads
        trends_parallel: True if p_value >= PARALLEL_TRENDS_ALPHA (fail to reject H0)
    """

    f_stat: float
    p_value: float
    df_num: int
    df_denom: int
    leads_tested: list[int]
    pre_period_betas: list[float]
    pre_period_ses: list[float]
    trends_parallel: bool


@dataclass
class EventStudyResult:
    """Full event-study results for one outcome variable.

    Attributes:
        outcome: name of the dependent variable
        coefficients: one EventStudyCoefficient per k in the window (including baseline)
        parallel_trends: parallel trends test result
        n_obs: total panel observations
        n_treated: treated person count
        n_control: control person count
        window: (min_k, max_k) tuple
    """

    outcome: str
    coefficients: list[EventStudyCoefficient]
    parallel_trends: ParallelTrendsResult
    n_obs: int
    n_treated: int
    n_control: int
    window: tuple[int, int] = (-EVENT_WINDOW_YEARS, EVENT_WINDOW_YEARS)


@dataclass
class DiDResult:
    """Complete DiD analysis results (all three outcomes + event study).

    Attributes:
        did_estimates: ATT estimates for theta_i, opportunity_residual, log_credit_count
        event_study: event-study coefficients for all three outcomes
        n_treated: number of persons in treated group
        n_control: number of persons in control group
        sample_years: (first_year, last_year) of the panel
        control_match_method: description of control group construction
    """

    did_estimates: list[DiDEstimate]
    event_study: list[EventStudyResult]
    n_treated: int
    n_control: int
    sample_years: tuple[int, int]
    control_match_method: str = "cohort × role_group exact match"


# ---------------------------------------------------------------------------
# Panel construction helpers
# ---------------------------------------------------------------------------


def _compute_primary_studio(
    person_year_credits: dict[tuple[str, int, str], int],
    person_id: str,
    year: int,
    window: int = PRIMARY_STUDIO_WINDOW,
) -> str | None:
    """Determine the primary studio for person_id in a rolling window ending at year.

    Primary studio = studio with the most credits in [year - window + 1, year].
    Tie-break: most recent year.

    Args:
        person_year_credits: mapping (person_id, year, studio_id) -> credit count
        person_id: the person to evaluate
        year: the anchor year
        window: rolling window length in years

    Returns:
        Studio identifier or None if no credits in the window.
    """
    studio_counts: dict[str, int] = {}
    studio_last_year: dict[str, int] = {}

    for y in range(year - window + 1, year + 1):
        # Iterate over all (person_id, year, studio) keys matching this person and year
        for (pid, yr, studio), cnt in person_year_credits.items():
            if pid == person_id and yr == y:
                studio_counts[studio] = studio_counts.get(studio, 0) + cnt
                if studio not in studio_last_year or yr > studio_last_year[studio]:
                    studio_last_year[studio] = yr

    if not studio_counts:
        return None

    # Sort by count DESC, then last_year DESC for tie-break
    best = max(
        studio_counts.keys(),
        key=lambda s: (studio_counts[s], studio_last_year.get(s, 0)),
    )
    return best


def identify_transfer_events(
    person_year_credits: dict[tuple[str, int, str], int],
    person_cohort_year: dict[str, int],
    person_primary_role: dict[str, str],
) -> list[TransferRecord]:
    """Identify qualifying studio transfer events in the panel.

    Transfer criterion (from TASK_CARDS/25_compensation_fairness/01_did_studio_transfer.md):
        transfer[i, t] = 1 if:
          - primary_studio[i, t-1] != primary_studio[i, t]
          - credits_at_new_studio[i, t] >= MIN_CREDITS_NEW_STUDIO
          - credits_at_old_studio[i, t-1] >= MIN_CREDITS_OLD_STUDIO

    Only the *first* qualifying transfer per person is used.

    Args:
        person_year_credits: (person_id, year, studio_id) -> credit count
        person_cohort_year: person_id -> first credit year (cohort)
        person_primary_role: person_id -> primary role group label

    Returns:
        List of TransferRecord, one per qualifying person-transfer.
    """
    # Collect all person × year combos
    person_years: dict[str, set[int]] = {}
    for pid, yr, _studio in person_year_credits:
        person_years.setdefault(pid, set()).add(yr)

    transfers: list[TransferRecord] = []

    for pid, years in person_years.items():
        sorted_years = sorted(years)
        for t in sorted_years[1:]:
            prev_t = t - 1
            if prev_t not in years:
                continue

            ps_prev = _compute_primary_studio(person_year_credits, pid, prev_t)
            ps_curr = _compute_primary_studio(person_year_credits, pid, t)

            if ps_prev is None or ps_curr is None:
                continue
            if ps_prev == ps_curr:
                continue

            # Credits at new studio in year t
            new_credits = sum(
                cnt
                for (p, y, s), cnt in person_year_credits.items()
                if p == pid and y == t and s == ps_curr
            )
            # Credits at old studio in year t-1
            old_credits = sum(
                cnt
                for (p, y, s), cnt in person_year_credits.items()
                if p == pid and y == prev_t and s == ps_prev
            )

            if new_credits < MIN_CREDITS_NEW_STUDIO:
                continue
            if old_credits < MIN_CREDITS_OLD_STUDIO:
                continue

            transfers.append(
                TransferRecord(
                    person_id=pid,
                    event_year=t,
                    old_studio=ps_prev,
                    new_studio=ps_curr,
                    cohort_year=person_cohort_year.get(pid, t),
                    primary_role_group=person_primary_role.get(pid, "unknown"),
                )
            )
            break  # only first qualifying transfer per person

    log.info(
        "transfer_events_identified",
        n_persons=len(person_years),
        n_transfers=len(transfers),
        min_credits_new=MIN_CREDITS_NEW_STUDIO,
        min_credits_old=MIN_CREDITS_OLD_STUDIO,
    )
    return transfers


def select_control_group(
    treated_records: list[TransferRecord],
    all_persons: set[str],
    person_cohort_year: dict[str, int],
    person_primary_role: dict[str, str],
    treated_ids: set[str],
) -> set[str]:
    """Select a cohort × role-group matched control group.

    Control persons are those who:
      1. Never make a qualifying transfer (not in treated_ids)
      2. Share a cohort decade and primary role group with at least one treated person

    Cohort decade = (cohort_year // 5) * 5 (5-year cohort bins).

    Args:
        treated_records: list of TransferRecord from identify_transfer_events
        all_persons: universe of persons with panel data
        person_cohort_year: person_id -> first credit year
        person_primary_role: person_id -> primary role group
        treated_ids: set of treated person IDs (excluded from control)

    Returns:
        Set of control person IDs.
    """
    # Build (cohort_bin, role_group) combinations covered by treated persons
    treated_strata: set[tuple[int, str]] = set()
    for rec in treated_records:
        cohort_bin = (rec.cohort_year // 5) * 5
        treated_strata.add((cohort_bin, rec.primary_role_group))

    control_ids: set[str] = set()
    for pid in all_persons:
        if pid in treated_ids:
            continue
        cy = person_cohort_year.get(pid, 0)
        cohort_bin = (cy // 5) * 5
        role_grp = person_primary_role.get(pid, "unknown")
        if (cohort_bin, role_grp) in treated_strata:
            control_ids.add(pid)

    log.info(
        "control_group_selected",
        n_treated=len(treated_ids),
        n_control=len(control_ids),
        n_strata=len(treated_strata),
    )
    return control_ids


def build_panel(
    treated_records: list[TransferRecord],
    control_ids: set[str],
    person_year_outcomes: dict[tuple[str, int], dict[str, float | None]],
    window: int = EVENT_WINDOW_YEARS,
) -> list[PersonYearObs]:
    """Construct the person × year panel restricted to event window.

    For treated persons: include years [event_year - window, event_year + window].
    For control persons: include years [earliest_treated_event - window,
                                       latest_treated_event + window] to ensure
    overlap with treated period.

    Args:
        treated_records: list of TransferRecord
        control_ids: set of control person IDs
        person_year_outcomes: (person_id, year) -> outcome dict with keys:
            theta_i, opportunity_residual, log_credit_count, tenure,
            role_diversity, cohort_year, primary_role_group
        window: event-study window in years

    Returns:
        List of PersonYearObs ready for estimation.
    """
    treated_event_map: dict[str, int] = {
        rec.person_id: rec.event_year for rec in treated_records
    }

    # Determine control year range (union of treated windows)
    if treated_records:
        min_event = min(rec.event_year for rec in treated_records)
        max_event = max(rec.event_year for rec in treated_records)
        ctrl_year_min = min_event - window
        ctrl_year_max = max_event + window
    else:
        ctrl_year_min = 1990
        ctrl_year_max = 2025

    obs_list: list[PersonYearObs] = []

    for (pid, yr), outcomes in person_year_outcomes.items():
        if pid in treated_event_map:
            ev = treated_event_map[pid]
            if yr < ev - window or yr > ev + window:
                continue
        elif pid in control_ids:
            if yr < ctrl_year_min or yr > ctrl_year_max:
                continue
        else:
            continue

        obs_list.append(
            PersonYearObs(
                person_id=pid,
                year=yr,
                theta_i=float(outcomes.get("theta_i") or 0.0),
                opportunity_residual=outcomes.get("opportunity_residual"),
                log_credit_count=float(outcomes.get("log_credit_count") or 0.0),
                tenure=float(outcomes.get("tenure") or 0.0),
                role_diversity=float(outcomes.get("role_diversity") or 0.0),
                cohort_year=int(outcomes.get("cohort_year") or 0),
                primary_role_group=str(outcomes.get("primary_role_group") or "unknown"),
            )
        )

    log.info(
        "panel_built",
        n_obs=len(obs_list),
        n_treated=len(treated_event_map),
        n_control=len(control_ids),
        window=window,
    )
    return obs_list


# ---------------------------------------------------------------------------
# Fixed-effects demeaning (within-transformation)
# ---------------------------------------------------------------------------


def _within_demean(
    y: np.ndarray,
    X: np.ndarray,
    person_ind: np.ndarray,
    year_ind: np.ndarray,
    n_persons: int,
    n_years: int,
    max_iter: int = 100,
    tol: float = 1e-8,
) -> tuple[np.ndarray, np.ndarray]:
    """Demean y and X by removing person and year fixed effects.

    Uses iterative alternating projection (Gaure 2013): subtract person means,
    then year means, until convergence. This avoids the O(N^2) dummy matrix.

    Args:
        y: outcome vector (n_obs,)
        X: covariate matrix (n_obs, p) — already excludes FE indicators
        person_ind: person index per observation (0-based)
        year_ind: year index per observation (0-based)
        n_persons: total unique persons
        n_years: total unique years
        max_iter: maximum alternating iterations
        tol: convergence tolerance

    Returns:
        (y_demeaned, X_demeaned) after removing person + year means.
    """
    y_r = y.copy()
    X_r = X.copy()
    n_obs = len(y)

    for _ in range(max_iter):
        # Remove person means
        p_sum_y = np.zeros(n_persons)
        p_sum_x = np.zeros((n_persons, X.shape[1]))
        p_cnt = np.zeros(n_persons, dtype=np.int64)
        for k in range(n_obs):
            p = person_ind[k]
            p_sum_y[p] += y_r[k]
            p_sum_x[p] += X_r[k]
            p_cnt[p] += 1
        p_mean_y = np.where(p_cnt > 0, p_sum_y / np.maximum(p_cnt, 1), 0.0)
        p_mean_x = p_sum_x / np.maximum(p_cnt[:, None], 1)

        y_after_p = y_r - p_mean_y[person_ind]
        X_after_p = X_r - p_mean_x[person_ind]

        # Remove year means
        t_sum_y = np.zeros(n_years)
        t_sum_x = np.zeros((n_years, X.shape[1]))
        t_cnt = np.zeros(n_years, dtype=np.int64)
        for k in range(n_obs):
            t = year_ind[k]
            t_sum_y[t] += y_after_p[k]
            t_sum_x[t] += X_after_p[k]
            t_cnt[t] += 1
        t_mean_y = np.where(t_cnt > 0, t_sum_y / np.maximum(t_cnt, 1), 0.0)
        t_mean_x = t_sum_x / np.maximum(t_cnt[:, None], 1)

        y_new = y_after_p - t_mean_y[year_ind]
        X_new = X_after_p - t_mean_x[year_ind]

        # Check convergence
        diff = float(np.max(np.abs(y_new - y_r))) + float(np.max(np.abs(X_new - X_r)))
        y_r = y_new
        X_r = X_new
        if diff < tol:
            break

    return y_r, X_r


# ---------------------------------------------------------------------------
# Cluster-robust sandwich SE (person-level clusters)
# ---------------------------------------------------------------------------


def _cluster_se(
    y_dm: np.ndarray,
    X_dm: np.ndarray,
    beta: np.ndarray,
    person_ind: np.ndarray,
    n_persons: int,
) -> np.ndarray:
    """Compute cluster-sandwich (sandwich) standard errors clustered at person level.

    SE = sqrt(diag((X'X)^{-1} B (X'X)^{-1}))

    where B = Σ_i (X_i' e_i e_i' X_i),  X_i = rows for person i,
    e_i = residuals for person i.

    This is the HC1-style cluster estimator (finite-sample small-cluster correction
    g / (g - 1) × (n - 1) / (n - k) applied, where g = n_clusters).

    Args:
        y_dm: demeaned outcome vector
        X_dm: demeaned covariate matrix (n_obs, p)
        beta: OLS coefficient vector (p,)
        person_ind: person index per observation
        n_persons: number of unique persons (= number of clusters)

    Returns:
        cluster-sandwich standard errors (p,)
    """
    n_obs, p = X_dm.shape
    residuals = y_dm - X_dm @ beta

    XtX = X_dm.T @ X_dm
    try:
        XtX_inv = np.linalg.inv(XtX)
    except np.linalg.LinAlgError:
        XtX_inv = np.linalg.pinv(XtX)

    # Meat: B = Σ_i X_i' e_i e_i' X_i (cluster score outer products)
    B = np.zeros((p, p))
    for i in range(n_persons):
        mask = person_ind == i
        Xi = X_dm[mask]
        ei = residuals[mask]
        score_i = Xi.T @ ei  # (p,)
        B += np.outer(score_i, score_i)

    # Finite-sample correction: g / (g - 1) × (n - 1) / (n - p)
    g = n_persons
    if g > 1 and n_obs > p:
        correction = (g / (g - 1)) * ((n_obs - 1) / (n_obs - p))
    else:
        correction = 1.0

    V = correction * XtX_inv @ B @ XtX_inv
    se = np.sqrt(np.maximum(np.diag(V), 0.0))
    return se


# ---------------------------------------------------------------------------
# DiD estimation (two-way FE)
# ---------------------------------------------------------------------------


def estimate_did(
    panel: list[PersonYearObs],
    treated_event_map: dict[str, int],
    outcome_name: str = "theta_i",
    extra_controls: bool = True,
) -> DiDEstimate | None:
    """Estimate two-way FE DiD for one outcome variable.

    Model:
        y[i, t] = alpha_i + gamma_t + beta * post[i, t] * treated[i]
                + delta_1 * tenure[i, t] + delta_2 * role_diversity[i, t]
                + epsilon[i, t]

    Estimation via within-transformation (person + year FE demeaning)
    followed by OLS on the demeaned residuals.
    Cluster-robust SE at person level.

    Args:
        panel: list of PersonYearObs covering treated + control persons
        treated_event_map: person_id -> event_year for treated persons
        outcome_name: one of 'theta_i', 'opportunity_residual', 'log_credit_count'
        extra_controls: whether to include tenure + role_diversity as covariates

    Returns:
        DiDEstimate or None if estimation fails (insufficient data).
    """
    treated_ids = set(treated_event_map.keys())

    # Filter out observations where the outcome is unavailable
    valid_obs = []
    for obs in panel:
        y_val = _get_outcome(obs, outcome_name)
        if y_val is None:
            continue
        valid_obs.append(obs)

    n_obs = len(valid_obs)
    if n_obs < MIN_PANEL_OBS:
        log.warning(
            "did_insufficient_obs",
            outcome=outcome_name,
            n_obs=n_obs,
            min_required=MIN_PANEL_OBS,
        )
        return None

    # Build person and year index maps
    persons = sorted({obs.person_id for obs in valid_obs})
    years = sorted({obs.year for obs in valid_obs})
    person_to_idx = {p: i for i, p in enumerate(persons)}
    year_to_idx = {y: i for i, y in enumerate(years)}

    n_persons = len(persons)
    n_years = len(years)

    # Outcome vector
    y = np.array([_get_outcome(obs, outcome_name) for obs in valid_obs], dtype=np.float64)

    # Treatment indicator: post[i, t] * treated[i]
    did_treat = np.array(
        [
            1.0
            if (
                obs.person_id in treated_ids
                and obs.year >= treated_event_map[obs.person_id]
            )
            else 0.0
            for obs in valid_obs
        ],
        dtype=np.float64,
    )

    # Extra controls (tenure, role_diversity)
    if extra_controls:
        X = np.column_stack([
            did_treat,
            np.array([obs.tenure for obs in valid_obs], dtype=np.float64),
            np.array([obs.role_diversity for obs in valid_obs], dtype=np.float64),
        ])
    else:
        X = did_treat.reshape(-1, 1)

    person_ind = np.array([person_to_idx[obs.person_id] for obs in valid_obs], dtype=np.int64)
    year_ind = np.array([year_to_idx[obs.year] for obs in valid_obs], dtype=np.int64)

    # Within-transformation (remove person + year FE)
    y_dm, X_dm = _within_demean(y, X, person_ind, year_ind, n_persons, n_years)

    # OLS on demeaned data
    try:
        beta_all, _, _, _ = np.linalg.lstsq(X_dm, y_dm, rcond=None)
    except np.linalg.LinAlgError:
        log.warning("did_lstsq_failed", outcome=outcome_name)
        return None

    # Cluster-robust SE
    se_all = _cluster_se(y_dm, X_dm, beta_all, person_ind, n_persons)

    # DiD coefficient = first element (post × treated)
    beta_did = float(beta_all[0])
    se_did = float(se_all[0])

    # t-distribution critical value (df = n_persons - 1, conservative)
    df = max(n_persons - 1, 1)
    t_crit = float(t_dist.ppf(0.975, df=df))
    ci_lower = beta_did - t_crit * se_did
    ci_upper = beta_did + t_crit * se_did
    t_stat = beta_did / se_did if se_did > 0 else 0.0
    p_value = float(2 * t_dist.sf(abs(t_stat), df=df))

    n_treated = sum(1 for p in persons if p in treated_ids)
    n_control = n_persons - n_treated

    log.info(
        "did_estimated",
        outcome=outcome_name,
        beta=round(beta_did, 4),
        se=round(se_did, 4),
        ci=(round(ci_lower, 4), round(ci_upper, 4)),
        p_value=round(p_value, 4),
        n_obs=n_obs,
        n_persons=n_persons,
        n_treated=n_treated,
        n_control=n_control,
    )

    return DiDEstimate(
        outcome=outcome_name,
        beta=beta_did,
        se=se_did,
        ci_lower=ci_lower,
        ci_upper=ci_upper,
        t_stat=t_stat,
        p_value=p_value,
        n_obs=n_obs,
        n_treated=n_treated,
        n_control=n_control,
    )


def _get_outcome(obs: PersonYearObs, outcome_name: str) -> float | None:
    """Extract the named outcome from a PersonYearObs.

    Args:
        obs: the observation
        outcome_name: 'theta_i', 'opportunity_residual', or 'log_credit_count'

    Returns:
        Float value or None if unavailable.
    """
    if outcome_name == "theta_i":
        return obs.theta_i
    if outcome_name == "opportunity_residual":
        return obs.opportunity_residual
    if outcome_name == "log_credit_count":
        return obs.log_credit_count
    raise ValueError(f"Unknown outcome: {outcome_name!r}")


# ---------------------------------------------------------------------------
# Event-study estimation
# ---------------------------------------------------------------------------


def estimate_event_study(
    panel: list[PersonYearObs],
    treated_event_map: dict[str, int],
    outcome_name: str = "theta_i",
    window: int = EVENT_WINDOW_YEARS,
) -> EventStudyResult | None:
    """Estimate event-study specification for one outcome.

    Specification:
        y[i, t] = alpha_i + gamma_t
                + Σ_{k ≠ -1} beta_k * 1[t - event_year[i] = k]
                + epsilon[i, t]

    k = -1 is the omitted baseline (period immediately before transfer).
    For control persons, 1[t - event_year[i] = k] = 0 for all k (no event).

    Args:
        panel: list of PersonYearObs covering treated + control persons
        treated_event_map: person_id -> event_year
        outcome_name: 'theta_i', 'opportunity_residual', or 'log_credit_count'
        window: symmetric window size (leads: [-window, -1), lags: [0, window])

    Returns:
        EventStudyResult or None on failure.
    """
    treated_ids = set(treated_event_map.keys())

    # Filter observations with valid outcome
    valid_obs = [obs for obs in panel if _get_outcome(obs, outcome_name) is not None]
    n_obs = len(valid_obs)
    if n_obs < MIN_PANEL_OBS:
        return None

    persons = sorted({obs.person_id for obs in valid_obs})
    years = sorted({obs.year for obs in valid_obs})
    person_to_idx = {p: i for i, p in enumerate(persons)}
    year_to_idx = {y: i for i, y in enumerate(years)}
    n_persons = len(persons)
    n_years = len(years)

    # k values: [-window, ..., -2, 0, 1, ..., +window] (exclude k=-1 as baseline)
    k_values = [k for k in range(-window, window + 1) if k != -1]
    k_to_col = {k: col for col, k in enumerate(k_values)}
    n_k = len(k_values)

    # Outcome vector
    y = np.array([_get_outcome(obs, outcome_name) for obs in valid_obs], dtype=np.float64)

    # Build event-study indicators (n_obs × n_k)
    X = np.zeros((n_obs, n_k), dtype=np.float64)
    for row_idx, obs in enumerate(valid_obs):
        if obs.person_id not in treated_ids:
            continue  # control: all event-study indicators = 0
        ev = treated_event_map[obs.person_id]
        k = obs.year - ev
        if k == -1:
            continue  # baseline, omitted
        if k in k_to_col:
            X[row_idx, k_to_col[k]] = 1.0

    person_ind = np.array([person_to_idx[obs.person_id] for obs in valid_obs], dtype=np.int64)
    year_ind = np.array([year_to_idx[obs.year] for obs in valid_obs], dtype=np.int64)

    # Within-transformation
    y_dm, X_dm = _within_demean(y, X, person_ind, year_ind, n_persons, n_years)

    # OLS
    try:
        beta_all, _, _, _ = np.linalg.lstsq(X_dm, y_dm, rcond=None)
    except np.linalg.LinAlgError:
        return None

    # Cluster-robust SE
    se_all = _cluster_se(y_dm, X_dm, beta_all, person_ind, n_persons)

    df = max(n_persons - 1, 1)
    t_crit = float(t_dist.ppf(0.975, df=df))

    coefficients: list[EventStudyCoefficient] = []

    # Add baseline k=-1 (by construction zero)
    coefficients.append(
        EventStudyCoefficient(
            k=-1,
            beta=0.0,
            se=0.0,
            ci_lower=0.0,
            ci_upper=0.0,
            t_stat=0.0,
            p_value=1.0,
            is_baseline=True,
        )
    )

    # Add estimated k values
    for k in k_values:
        col = k_to_col[k]
        b = float(beta_all[col])
        se = float(se_all[col])
        ci_lo = b - t_crit * se
        ci_hi = b + t_crit * se
        t_stat = b / se if se > 0 else 0.0
        p_val = float(2 * t_dist.sf(abs(t_stat), df=df))
        coefficients.append(
            EventStudyCoefficient(
                k=k,
                beta=b,
                se=se,
                ci_lower=ci_lo,
                ci_upper=ci_hi,
                t_stat=t_stat,
                p_value=p_val,
            )
        )

    # Sort by k
    coefficients.sort(key=lambda c: c.k)

    # Parallel trends test
    pt = _test_parallel_trends(coefficients, beta_all, se_all, k_to_col, X_dm, y_dm, person_ind, n_persons, df)

    n_treated = sum(1 for p in persons if p in treated_ids)
    n_control = n_persons - n_treated

    log.info(
        "event_study_estimated",
        outcome=outcome_name,
        window=window,
        n_obs=n_obs,
        n_treated=n_treated,
        n_control=n_control,
        parallel_trends_p=round(pt.p_value, 4),
        trends_parallel=pt.trends_parallel,
    )

    return EventStudyResult(
        outcome=outcome_name,
        coefficients=coefficients,
        parallel_trends=pt,
        n_obs=n_obs,
        n_treated=n_treated,
        n_control=n_control,
        window=(-window, window),
    )


# ---------------------------------------------------------------------------
# Parallel trends test (joint F-test on pre-period leads)
# ---------------------------------------------------------------------------


def _test_parallel_trends(
    coefficients: list[EventStudyCoefficient],
    beta_all: np.ndarray,
    se_all: np.ndarray,
    k_to_col: dict[int, int],
    X_dm: np.ndarray,
    y_dm: np.ndarray,
    person_ind: np.ndarray,
    n_persons: int,
    df: int,
) -> ParallelTrendsResult:
    """Joint F-test on pre-period leads k ∈ {-3, -2} = 0 (parallel trends).

    Uses a Wald F-test: F = (R beta)' (R V R')^{-1} (R beta) / q

    where R selects the lead coefficients, V is the cluster-sandwich covariance matrix,
    and q is the number of restrictions.

    Args:
        coefficients: all EventStudyCoefficient objects (sorted by k)
        beta_all: full coefficient vector from OLS
        se_all: cluster-sandwich SE vector
        k_to_col: mapping k -> column index in beta_all
        X_dm: demeaned design matrix
        y_dm: demeaned outcome vector
        person_ind: person indices
        n_persons: number of persons
        df: denominator degrees of freedom

    Returns:
        ParallelTrendsResult with F-stat, p-value, and parallel_trends flag.
    """
    # Test leads k = -3, -2 (k = -1 is baseline, k < -3 may be sparse)
    leads_to_test = [k for k in [-3, -2] if k in k_to_col]

    if not leads_to_test:
        # Not enough pre-period variation; fall back to individual t-tests
        lead_betas = []
        lead_ses = []
        for coef in coefficients:
            if coef.k in {-3, -2} and not coef.is_baseline:
                lead_betas.append(coef.beta)
                lead_ses.append(coef.se)

        if not lead_betas:
            return ParallelTrendsResult(
                f_stat=0.0,
                p_value=1.0,
                df_num=0,
                df_denom=df,
                leads_tested=[],
                pre_period_betas=[],
                pre_period_ses=[],
                trends_parallel=True,
            )

        # Use max |t| as proxy for joint test
        t_stats = [abs(b / s) if s > 0 else 0.0 for b, s in zip(lead_betas, lead_ses)]
        p_vals = [float(2 * t_dist.sf(t, df=df)) for t in t_stats]
        # Joint: if ALL p > alpha, parallel trends hold
        all_non_sig = all(p >= PARALLEL_TRENDS_ALPHA for p in p_vals)
        f_proxy = max(t_stats) ** 2 if t_stats else 0.0
        return ParallelTrendsResult(
            f_stat=f_proxy,
            p_value=min(p_vals) if p_vals else 1.0,
            df_num=len(leads_to_test),
            df_denom=df,
            leads_tested=leads_to_test,
            pre_period_betas=lead_betas,
            pre_period_ses=lead_ses,
            trends_parallel=all_non_sig,
        )

    # Build restriction matrix R (q × p): picks the lead columns
    q = len(leads_to_test)
    p = beta_all.shape[0]
    R = np.zeros((q, p))
    for row, k in enumerate(leads_to_test):
        R[row, k_to_col[k]] = 1.0

    R_beta = R @ beta_all  # (q,)

    # Cluster-robust covariance of full beta vector
    n_obs = X_dm.shape[0]
    residuals = y_dm - X_dm @ beta_all
    XtX = X_dm.T @ X_dm
    try:
        XtX_inv = np.linalg.inv(XtX)
    except np.linalg.LinAlgError:
        XtX_inv = np.linalg.pinv(XtX)

    B = np.zeros((p, p))
    for i in range(n_persons):
        mask = person_ind == i
        Xi = X_dm[mask]
        ei = residuals[mask]
        score_i = Xi.T @ ei
        B += np.outer(score_i, score_i)

    g = n_persons
    if g > 1 and n_obs > p:
        correction = (g / (g - 1)) * ((n_obs - 1) / (n_obs - p))
    else:
        correction = 1.0

    V = correction * XtX_inv @ B @ XtX_inv

    # Wald F = (R beta)' (R V R')^{-1} (R beta) / q
    RVR = R @ V @ R.T
    try:
        RVR_inv = np.linalg.inv(RVR)
    except np.linalg.LinAlgError:
        RVR_inv = np.linalg.pinv(RVR)

    f_stat = float(R_beta @ RVR_inv @ R_beta / q)
    p_value = float(f_dist.sf(f_stat, dfn=q, dfd=df))

    lead_betas = [float(R_beta[i]) for i in range(q)]
    lead_ses = [float(np.sqrt(max(V[k_to_col[k], k_to_col[k]], 0.0))) for k in leads_to_test]

    trends_parallel = p_value >= PARALLEL_TRENDS_ALPHA

    log.info(
        "parallel_trends_test",
        leads_tested=leads_to_test,
        f_stat=round(f_stat, 3),
        p_value=round(p_value, 4),
        trends_parallel=trends_parallel,
    )

    return ParallelTrendsResult(
        f_stat=f_stat,
        p_value=p_value,
        df_num=q,
        df_denom=df,
        leads_tested=leads_to_test,
        pre_period_betas=lead_betas,
        pre_period_ses=lead_ses,
        trends_parallel=trends_parallel,
    )


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def run_did_analysis(
    person_year_credits: dict[tuple[str, int, str], int],
    person_year_outcomes: dict[tuple[str, int], dict[str, float | None]],
    person_cohort_year: dict[str, int],
    person_primary_role: dict[str, str],
    window: int = EVENT_WINDOW_YEARS,
    outcome_names: list[str] | None = None,
) -> DiDResult | None:
    """Run the full DiD analysis pipeline.

    Steps:
        1. Identify studio transfer events (treatment group)
        2. Select cohort × role-group matched control group
        3. Build person × year panel (event window)
        4. Estimate two-way FE DiD for each outcome
        5. Estimate event-study for each outcome
        6. Report parallel trends test results

    Args:
        person_year_credits: (person_id, year, studio_id) -> credit count,
            used for primary_studio assignment and transfer identification.
        person_year_outcomes: (person_id, year) -> outcome dict.
            Required keys: theta_i, opportunity_residual, log_credit_count,
            tenure, role_diversity, cohort_year, primary_role_group.
        person_cohort_year: person_id -> first credit year.
        person_primary_role: person_id -> primary role group.
        window: event-study window in years (default 5).
        outcome_names: list of outcome names to estimate; defaults to all three.

    Returns:
        DiDResult or None if insufficient treated persons.
    """
    if outcome_names is None:
        outcome_names = ["theta_i", "opportunity_residual", "log_credit_count"]

    # Step 1: Transfer events
    transfers = identify_transfer_events(
        person_year_credits, person_cohort_year, person_primary_role
    )

    if not transfers:
        log.warning("did_no_transfers_found")
        return None

    treated_ids = {rec.person_id for rec in transfers}
    treated_event_map = {rec.person_id: rec.event_year for rec in transfers}

    # Step 2: Control group
    all_persons = {pid for pid, _yr in person_year_outcomes}
    control_ids = select_control_group(
        transfers, all_persons, person_cohort_year, person_primary_role, treated_ids
    )

    # Step 3: Build panel
    panel = build_panel(transfers, control_ids, person_year_outcomes, window=window)

    if len(panel) < MIN_PANEL_OBS:
        log.warning("did_panel_too_small", n_obs=len(panel))
        return None

    # Panel year range
    all_years = [obs.year for obs in panel]
    sample_years = (min(all_years), max(all_years))

    # Step 4 + 5: DiD + event-study for each outcome
    did_estimates: list[DiDEstimate] = []
    event_study_results: list[EventStudyResult] = []

    for outcome in outcome_names:
        est = estimate_did(panel, treated_event_map, outcome_name=outcome)
        if est is not None:
            did_estimates.append(est)

        es = estimate_event_study(panel, treated_event_map, outcome_name=outcome, window=window)
        if es is not None:
            event_study_results.append(es)

    if not did_estimates:
        log.warning("did_no_estimates_produced")
        return None

    # Summary log
    n_treated = len(treated_ids)
    n_control = len(control_ids)

    log.info(
        "did_analysis_complete",
        n_treated=n_treated,
        n_control=n_control,
        n_outcomes=len(did_estimates),
        n_event_study=len(event_study_results),
        sample_years=sample_years,
    )

    return DiDResult(
        did_estimates=did_estimates,
        event_study=event_study_results,
        n_treated=n_treated,
        n_control=n_control,
        sample_years=sample_years,
    )


# ---------------------------------------------------------------------------
# CLI entry point (--dry-run)
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Studio transfer DiD analysis dry-run check"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run synthetic data smoke test",
    )
    args = parser.parse_args()

    if args.dry_run:
        rng = np.random.default_rng(42)
        n_persons = 100
        years = list(range(2005, 2021))

        # Build synthetic credits
        pyc: dict[tuple[str, int, str], int] = {}
        py_outcomes: dict[tuple[str, int], dict[str, float | None]] = {}
        cohorts: dict[str, int] = {}
        roles: dict[str, str] = {}

        studios = ["StudioA", "StudioB", "StudioC"]
        role_groups = ["key_animator", "animation_director", "director"]

        for i in range(n_persons):
            pid = f"p{i:04d}"
            first_year = int(rng.integers(2005, 2013))
            cohorts[pid] = first_year
            roles[pid] = role_groups[i % 3]
            primary_studio = studios[i % 3]

            for yr in years:
                if yr < first_year:
                    continue
                # Simulate potential transfer at year 2013 for first 30 persons
                if i < 30 and yr >= 2013:
                    studio = studios[(i % 3 + 1) % 3]
                    credits_val = int(rng.integers(3, 10))
                else:
                    studio = primary_studio
                    credits_val = int(rng.integers(1, 8))
                pyc[(pid, yr, studio)] = credits_val
                py_outcomes[(pid, yr)] = {
                    "theta_i": float(rng.normal(0, 1)),
                    "opportunity_residual": float(rng.normal(0, 0.5)),
                    "log_credit_count": float(np.log1p(credits_val)),
                    "tenure": float(yr - first_year),
                    "role_diversity": float(rng.integers(1, 4)),
                    "cohort_year": first_year,
                    "primary_role_group": roles[pid],
                }

        result = run_did_analysis(pyc, py_outcomes, cohorts, roles)
        if result is None:
            print("[dry-run] FAIL: run_did_analysis returned None")
        else:
            print(f"[dry-run] PASS: n_treated={result.n_treated}, n_control={result.n_control}")
            for est in result.did_estimates:
                print(
                    f"  {est.outcome}: beta={est.beta:.4f}, SE={est.se:.4f}, "
                    f"95% CI=[{est.ci_lower:.4f}, {est.ci_upper:.4f}], p={est.p_value:.4f}"
                )
