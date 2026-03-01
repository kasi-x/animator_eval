"""AKM Fixed Effects Decomposition — Abowd, Kramarz, Margolis (1999).

Decomposes observed outcomes (anime scores) into:
- Person fixed effects (θ_i): individual talent/ability
- Studio fixed effects (ψ_j): studio resources/environment
- Time-varying controls: experience, role level, credits per year

Uses iterative demeaning (Gaure 2013) to avoid massive dummy matrices.
Studio is inferred from anime credits when not directly available.

Observation weights (w_obs) account for role importance, involvement type,
episode coverage, and experience at time of work. This addresses the
"shared score problem" where all staff on an anime share the same outcome.
"""

import math
from collections import defaultdict
from dataclasses import dataclass

import numpy as np
import structlog

from src.analysis.contribution_attribution import ROLE_CONTRIBUTION_WEIGHTS
from src.models import Anime, Credit, Role
from src.utils.config import ROLE_WEIGHTS
from src.utils.episode_parser import parse_episodes
from src.utils.role_groups import EPISODIC_ROLES, THROUGH_ROLES

logger = structlog.get_logger()

# Normalize ROLE_CONTRIBUTION_WEIGHTS to [0, 1] by dividing by max value
_MAX_CONTRIB_WEIGHT = max(ROLE_CONTRIBUTION_WEIGHTS.values()) if ROLE_CONTRIBUTION_WEIGHTS else 1.0


@dataclass
class AKMResult:
    """Result of AKM estimation.

    Attributes:
        person_fe: θ_i — person fixed effects
        studio_fe: ψ_j — studio fixed effects
        beta: coefficients on person time-varying controls
        gamma: coefficients on firm time-varying controls
        residuals: (person_id, anime_id) → ε residuals
        connected_set_size: number of persons in connected set
        n_movers: number of persons who worked at 2+ studios
        n_observations: total person-anime observations
        r_squared: model R²
    """

    person_fe: dict[str, float]
    studio_fe: dict[str, float]
    beta: np.ndarray
    gamma: np.ndarray
    residuals: dict[tuple[str, str], float]
    connected_set_size: int
    n_movers: int
    n_observations: int
    r_squared: float


def infer_studio_assignment(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, dict[int, str]]:
    """Infer studio assignment for each person-year.

    For each person-year, pick the studio of the anime where they
    have the most credits (weighted by ROLE_WEIGHTS). Uses anime.studios[0].

    Args:
        credits: all credits
        anime_map: anime_id → Anime

    Returns:
        person_id → {year → studio_name}
    """
    # Accumulate role weights per (person, year, studio)
    weight_accum: dict[tuple[str, int, str], float] = defaultdict(float)

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year or not anime.studios:
            continue
        w = ROLE_WEIGHTS.get(c.role.value, 1.0)
        # Distribute credit weight equally across co-production studios
        per_studio_w = w / len(anime.studios)
        for studio in anime.studios:
            weight_accum[(c.person_id, anime.year, studio)] += per_studio_w

    # For each person-year, pick the studio with highest weight
    person_year_best: dict[tuple[str, int], tuple[str, float]] = {}
    for (pid, year, studio), w in weight_accum.items():
        key = (pid, year)
        if key not in person_year_best or w > person_year_best[key][1]:
            person_year_best[key] = (studio, w)

    # Build result
    result: dict[str, dict[int, str]] = defaultdict(dict)
    for (pid, year), (studio, _) in person_year_best.items():
        result[pid][year] = studio

    return dict(result)


def find_connected_set(
    studio_assignments: dict[str, dict[int, str]],
) -> tuple[set[str], set[str]]:
    """Find the largest connected set using Union-Find.

    Studios are connected when a person (mover) has worked at both.
    Only movers provide identification for studio FE.

    Args:
        studio_assignments: person_id → {year → studio}

    Returns:
        (connected_person_ids, connected_studio_ids)
    """
    # Union-Find
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # Collect all studios per person
    person_studios: dict[str, set[str]] = {}
    all_studios: set[str] = set()
    for pid, year_studio in studio_assignments.items():
        studios = set(year_studio.values())
        person_studios[pid] = studios
        all_studios.update(studios)
        # Initialize union-find
        for s in studios:
            if s not in parent:
                parent[s] = s

    # Union studios linked by movers
    movers = set()
    for pid, studios in person_studios.items():
        studio_list = list(studios)
        if len(studio_list) >= 2:
            movers.add(pid)
            for i in range(1, len(studio_list)):
                union(studio_list[0], studio_list[i])

    if not movers:
        # No movers — return all persons and studios anyway
        return set(studio_assignments.keys()), all_studios

    # Find largest connected component
    component_members: dict[str, set[str]] = defaultdict(set)
    for s in all_studios:
        component_members[find(s)].add(s)

    largest_root = max(component_members, key=lambda r: len(component_members[r]))
    connected_studios = component_members[largest_root]

    # Find persons in connected set (those who worked at connected studios)
    connected_persons = set()
    for pid, studios in person_studios.items():
        if studios & connected_studios:
            connected_persons.add(pid)

    return connected_persons, connected_studios


def _compute_credit_weight(
    role: Role,
    raw_role: str | None,
    anime: Anime,
    years_active: int,
) -> float:
    """Compute observation weight for a single credit.

    w_obs = w_role × w_involvement × w_coverage × w_experience

    Args:
        role: normalized role enum
        raw_role: original role string (may contain episode info)
        anime: the anime object
        years_active: years since person's first credit

    Returns:
        Observation weight (unnormalized).
    """
    # Factor 1: w_role — role importance (normalized to [0, 1])
    w_role = ROLE_CONTRIBUTION_WEIGHTS.get(role, 0.01) / _MAX_CONTRIB_WEIGHT

    # Factor 2: w_involvement — through vs episodic
    if role in THROUGH_ROLES:
        w_involvement = 1.5
    elif role in EPISODIC_ROLES:
        w_involvement = 0.7
    else:
        w_involvement = 1.0

    # Factor 3: w_coverage — episode coverage
    if role in THROUGH_ROLES:
        w_coverage = 1.0
    elif role in EPISODIC_ROLES:
        total_eps = anime.episodes or 1
        eps = parse_episodes(raw_role or "")
        if eps:
            w_coverage = len(eps) / max(total_eps, len(eps))
        else:
            w_coverage = 1.0 / math.sqrt(max(total_eps, 1))
    else:
        w_coverage = 1.0

    # Factor 4: w_experience — veteran reliability
    w_experience = min(1.0 + 0.3 * (1 - math.exp(-years_active / 5.0)), 1.3)

    return w_role * w_involvement * w_coverage * w_experience


def _build_panel(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    studio_assignments: dict[str, dict[int, str]],
    connected_persons: set[str],
    connected_studios: set[str],
) -> tuple[
    list[str],  # person_ids (ordered)
    list[tuple[str, str]],  # obs_keys: (person_id, anime_id)
    list[str],  # studio_ids (ordered)
    np.ndarray,  # y: outcomes (n_obs,)
    np.ndarray,  # person_indicators (n_obs,) int indices
    np.ndarray,  # studio_indicators (n_obs,) int indices
    np.ndarray,  # X: person controls (n_obs, n_x)
    np.ndarray,  # w_obs: observation weights (n_obs,)
]:
    """Build panel data for AKM estimation.

    Observation unit: person × anime (not person × year).
    This gives one observation per (person, anime) pair, so persons with
    more credits get more observations, aligning shrinkage with data density.

    Observation weights (w_obs) account for role importance, involvement type
    (through vs episodic), episode coverage, and experience at time of work.
    Multiple roles on the same anime accumulate weights (sum, not max).
    """
    # Track experience (first year) per person
    person_first_year: dict[str, int] = {}
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        if c.person_id not in connected_persons:
            continue
        year = anime.year
        if c.person_id not in person_first_year:
            person_first_year[c.person_id] = year
        else:
            person_first_year[c.person_id] = min(
                person_first_year[c.person_id], year
            )

    # Aggregate per (person, anime): collapse multiple roles on same anime.
    # role_w: max role weight (for controls); w_obs: sum of credit weights.
    pa_data: dict[tuple[str, str], tuple[float, float, int, str | None, float]] = {}
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year or anime.score is None:
            continue
        if c.person_id not in connected_persons:
            continue
        if not anime.studios:
            continue

        key = (c.person_id, c.anime_id)
        w = ROLE_WEIGHTS.get(c.role.value, 1.0)
        studio = anime.studios[0]

        # Compute credit-level observation weight
        years_active = anime.year - person_first_year.get(c.person_id, anime.year)
        cw = _compute_credit_weight(c.role, c.raw_role, anime, years_active)

        if key not in pa_data:
            pa_data[key] = (anime.score, w, anime.year, studio, cw)
        else:
            old_score, old_w, old_year, old_studio, old_cw = pa_data[key]
            # Max role weight for controls; accumulate w_obs for multi-role
            pa_data[key] = (anime.score, max(old_w, w), old_year, old_studio, old_cw + cw)

    # Cap accumulated w_obs at 95th percentile to limit multi-role outliers
    if pa_data:
        all_w_obs = [v[4] for v in pa_data.values()]
        cap = float(np.percentile(all_w_obs, 95))
        if cap > 0:
            pa_data = {
                k: (s, rw, yr, st, min(wo, cap))
                for k, (s, rw, yr, st, wo) in pa_data.items()
            }

    # Build ordered indices
    person_list = sorted(connected_persons)
    studio_list = sorted(connected_studios)
    person_to_idx = {pid: i for i, pid in enumerate(person_list)}
    studio_to_idx = {sid: i for i, sid in enumerate(studio_list)}

    # Build arrays
    obs_y = []
    obs_person = []
    obs_studio = []
    obs_x = []
    obs_w: list[float] = []
    obs_keys: list[tuple[str, str]] = []

    for (pid, anime_id), (score, role_w, year, studio, w_obs) in pa_data.items():
        if studio not in studio_to_idx:
            continue

        # Controls: experience_years, role_weight
        experience = year - person_first_year.get(pid, year)

        obs_y.append(score)
        obs_person.append(person_to_idx[pid])
        obs_studio.append(studio_to_idx[studio])
        obs_x.append([experience, role_w])
        obs_w.append(w_obs)
        obs_keys.append((pid, anime_id))

    if not obs_y:
        return (
            person_list, [], studio_list,
            np.array([]), np.array([]), np.array([]),
            np.array([]).reshape(0, 2), np.array([]),
        )

    y = np.array(obs_y, dtype=np.float64)
    person_ind = np.array(obs_person, dtype=np.int32)
    studio_ind = np.array(obs_studio, dtype=np.int32)
    X = np.array(obs_x, dtype=np.float64)
    w = np.array(obs_w, dtype=np.float64)

    # Median-normalize weights to preserve overall level
    med = float(np.median(w))
    if med > 0:
        w = w / med

    return person_list, obs_keys, studio_list, y, person_ind, studio_ind, X, w


def _debias_by_obs_count(
    person_fe_arr: np.ndarray,
    credit_counts: np.ndarray,
    n_persons: int,
    log: structlog.stdlib.BoundLogger,
) -> np.ndarray:
    """Remove systematic correlation between person FE and credit count.

    The AKM mechanically underestimates person_fe for persons with many
    credits: studios absorb much of the explained variance, so
    well-observed persons' FE converges toward the (negative) residual
    mean, while single-credit persons retain their noisy deviation.

    Fix: OLS regress person_fe on log(1 + credit_count), then subtract the
    slope component. This removes the mechanical bias while preserving
    the cross-person variation that truly reflects quality differences.

    Args:
        person_fe_arr: person FE estimates (n_persons,)
        credit_counts: total credits per person (n_persons,)
        n_persons: total persons
        log: logger

    Returns:
        Debiased person FE array.
    """
    if n_persons < 20:
        return person_fe_arr

    active = credit_counts > 0
    n_active = int(np.sum(active))
    if n_active < 20:
        return person_fe_arr

    log_credits = np.log1p(credit_counts[active].astype(np.float64))
    fe_active = person_fe_arr[active]

    # OLS: person_fe = a + b * log(1+credits) + residual
    X_debias = np.column_stack([np.ones(n_active), log_credits])
    try:
        b_debias, _, _, _ = np.linalg.lstsq(X_debias, fe_active, rcond=None)
    except np.linalg.LinAlgError:
        return person_fe_arr

    slope = float(b_debias[1])

    # Only debias if slope is meaningfully negative (the expected artifact)
    if slope >= 0:
        log.info("akm_debias_skipped", slope=round(slope, 4), reason="non_negative_slope")
        return person_fe_arr

    # Remove slope effect: shift each person's FE by -slope * (log_credits - mean)
    # This makes person_fe uncorrelated with credit count while preserving the mean
    debiased = person_fe_arr.copy()
    mean_log = float(np.mean(log_credits))
    debiased[active] = fe_active - slope * (log_credits - mean_log)

    # Report
    old_corr = float(np.corrcoef(fe_active, log_credits)[0, 1])
    new_corr = float(np.corrcoef(debiased[active], log_credits)[0, 1])
    log.info(
        "akm_debias_applied",
        slope=round(slope, 4),
        old_corr_log_credits=round(old_corr, 4),
        new_corr_log_credits=round(new_corr, 4),
        mean_shift_1credit=round(-slope * (0 - mean_log), 4),
        mean_shift_50credit=round(-slope * (np.log1p(50) - mean_log), 4),
    )

    return debiased


def _shrink_person_fe(
    person_fe_arr: np.ndarray,
    person_ind: np.ndarray,
    residuals: np.ndarray,
    n_obs: int,
    n_persons: int,
    log: structlog.stdlib.BoundLogger,
) -> np.ndarray:
    """Apply empirical Bayes shrinkage to person fixed effects.

    Shrinks noisy person FE estimates toward the grand mean based on
    raw observation counts. With n_i observations for person i:

        θ_shrunk[i] = (n_i / (n_i + κ)) · θ_raw[i] + (κ / (n_i + κ)) · μ

    Uses raw observation counts (not weighted effective counts) because
    shrinkage addresses the incidental parameters problem — which depends
    on the number of independent data points, not observation weights.
    A director on 1 anime is statistically 1 observation regardless of
    how much weight it receives in the WLS estimation.

    κ is estimated from the data as σ²_residual / σ²_signal, where
    σ²_signal = σ²_person_fe - σ²_residual/n̄ (variance decomposition).

    Args:
        person_fe_arr: raw person FE estimates (n_persons,)
        person_ind: person index for each observation (n_obs,)
        residuals: model residuals (n_obs,)
        n_obs: total observations
        n_persons: total persons
        log: logger

    Returns:
        Shrunk person FE array (same shape).
    """
    if n_persons == 0 or n_obs == 0:
        return person_fe_arr

    # Count raw observations per person (independent data points)
    obs_counts = np.zeros(n_persons, dtype=np.int64)
    for k in range(n_obs):
        obs_counts[person_ind[k]] += 1

    # Estimate κ from data
    sigma2_resid = float(np.mean(residuals ** 2)) if n_obs > 0 else 1.0

    active = obs_counts > 0
    if not np.any(active):
        return person_fe_arr

    sigma2_person_raw = float(np.var(person_fe_arr[active]))
    n_bar = float(np.mean(obs_counts[active]))

    # σ²_signal = σ²_raw - σ²_noise, where σ²_noise ≈ σ²_resid / n̄
    sigma2_signal = max(sigma2_person_raw - sigma2_resid / n_bar, sigma2_person_raw * 0.1)

    kappa = sigma2_resid / sigma2_signal if sigma2_signal > 0 else 10.0
    # Floor κ to prevent over-shrinkage when residual variance is very high,
    # cap to prevent under-shrinkage
    kappa = float(np.clip(kappa, 2.0, 50.0))

    # Grand mean of raw person FE
    mu = float(np.mean(person_fe_arr[active]))

    # Apply shrinkage
    shrunk = person_fe_arr.copy()
    for i in range(n_persons):
        n_i = obs_counts[i]
        if n_i == 0:
            continue
        reliability = n_i / (n_i + kappa)
        shrunk[i] = reliability * person_fe_arr[i] + (1 - reliability) * mu

    # Log diagnostics
    raw_std = float(np.std(person_fe_arr[active]))
    shrunk_std = float(np.std(shrunk[active]))
    log.info(
        "akm_shrinkage_applied",
        kappa=round(kappa, 2),
        sigma2_resid=round(sigma2_resid, 4),
        sigma2_signal=round(sigma2_signal, 4),
        raw_std=round(raw_std, 4),
        shrunk_std=round(shrunk_std, 4),
        grand_mean=round(mu, 4),
        shrinkage_1obs=round(1 / (1 + kappa), 3),
        shrinkage_5obs=round(5 / (5 + kappa), 3),
        shrinkage_20obs=round(20 / (20 + kappa), 3),
        shrinkage_50obs=round(50 / (50 + kappa), 3),
    )

    return shrunk


def estimate_akm(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    max_iter: int = 50,
    tol: float = 1e-8,
) -> AKMResult:
    """Estimate AKM fixed effects model.

    Uses iterative demeaning (Gaure 2013): alternately subtract person means
    and studio means until convergence, avoiding massive dummy matrices.

    Falls back to person FE only if movers < 10% of persons.

    Args:
        credits: all credits
        anime_map: anime_id → Anime
        max_iter: maximum demeaning iterations
        tol: convergence tolerance

    Returns:
        AKMResult with person and studio fixed effects
    """
    # Step 1: Infer studio assignments
    studio_assignments = infer_studio_assignment(credits, anime_map)

    if not studio_assignments:
        logger.warning("akm_no_studio_assignments")
        return AKMResult(
            person_fe={},
            studio_fe={},
            beta=np.array([]),
            gamma=np.array([]),
            residuals={},
            connected_set_size=0,
            n_movers=0,
            n_observations=0,
            r_squared=0.0,
        )

    # Step 2: Find connected set
    connected_persons, connected_studios = find_connected_set(studio_assignments)

    # Count movers
    movers = set()
    for pid in connected_persons:
        studios = set(studio_assignments.get(pid, {}).values())
        if len(studios) >= 2:
            movers.add(pid)

    n_movers = len(movers)
    mover_fraction = n_movers / len(connected_persons) if connected_persons else 0

    logger.info(
        "akm_connected_set",
        persons=len(connected_persons),
        studios=len(connected_studios),
        movers=n_movers,
        mover_fraction=round(mover_fraction, 3),
    )

    # Step 3: Build panel (with observation weights)
    person_list, obs_keys, studio_list, y, person_ind, studio_ind, X, w = _build_panel(
        credits, anime_map, studio_assignments, connected_persons, connected_studios
    )

    n_obs = len(y)
    if n_obs == 0:
        logger.warning("akm_no_observations")
        return AKMResult(
            person_fe={},
            studio_fe={},
            beta=np.array([]),
            gamma=np.array([]),
            residuals={},
            connected_set_size=len(connected_persons),
            n_movers=n_movers,
            n_observations=0,
            r_squared=0.0,
        )

    n_persons = len(person_list)
    n_studios = len(studio_list)

    # Log weight diagnostics
    n_through = 0
    n_episodic = 0
    for c in credits:
        if c.role in THROUGH_ROLES:
            n_through += 1
        elif c.role in EPISODIC_ROLES:
            n_episodic += 1
    n_total_credits = len(credits)
    logger.info(
        "akm_weights_summary",
        median_w=round(float(np.median(w)), 4),
        mean_w=round(float(np.mean(w)), 4),
        std_w=round(float(np.std(w)), 4),
        min_w=round(float(np.min(w)), 4),
        max_w=round(float(np.max(w)), 4),
        pct_through=round(n_through / max(n_total_credits, 1) * 100, 1),
        pct_episodic=round(n_episodic / max(n_total_credits, 1) * 100, 1),
    )

    # Step 4: Demean controls (partial out X) with WLS
    intercept = 0.0
    if X.shape[1] > 0 and n_obs > X.shape[1] + 1:
        # WLS: sqrt(w) transformation for heteroscedasticity correction
        try:
            sw = np.sqrt(w)
            X_aug = np.column_stack([np.ones(n_obs), X])
            X_w = X_aug * sw[:, None]
            y_w = y * sw
            beta_full, _, _, _ = np.linalg.lstsq(X_w, y_w, rcond=None)
            intercept = float(beta_full[0])
            beta = beta_full[1:]
            # Residuals in original scale
            y_resid = y - X_aug @ beta_full
        except np.linalg.LinAlgError:
            beta = np.zeros(X.shape[1])
            y_resid = y.copy()
    else:
        beta = np.array([])
        y_resid = y.copy()

    # Step 5: Weighted iterative demeaning for person and studio FE
    person_fe_arr = np.zeros(n_persons, dtype=np.float64)
    studio_fe_arr = np.zeros(n_studios, dtype=np.float64)

    if mover_fraction < 0.10:
        logger.warning(
            "akm_few_movers",
            mover_fraction=round(mover_fraction, 3),
            msg="Too few movers for studio FE identification; estimating person FE only",
        )
        # Person FE only: weighted mean of y_resid for person i
        person_sums = np.zeros(n_persons, dtype=np.float64)
        person_wsum = np.zeros(n_persons, dtype=np.float64)
        for k in range(n_obs):
            person_sums[person_ind[k]] += w[k] * y_resid[k]
            person_wsum[person_ind[k]] += w[k]
        mask = person_wsum > 0
        person_fe_arr[mask] = person_sums[mask] / person_wsum[mask]
    else:
        # Weighted iterative demeaning: alternate person and studio mean subtraction
        r = y_resid.copy()

        for iteration in range(max_iter):
            # Compute weighted person means
            person_sums = np.zeros(n_persons, dtype=np.float64)
            person_wsum = np.zeros(n_persons, dtype=np.float64)
            for k in range(n_obs):
                val = r[k] - studio_fe_arr[studio_ind[k]]
                person_sums[person_ind[k]] += w[k] * val
                person_wsum[person_ind[k]] += w[k]
            mask_p = person_wsum > 0
            new_person_fe = np.zeros(n_persons, dtype=np.float64)
            new_person_fe[mask_p] = person_sums[mask_p] / person_wsum[mask_p]

            # Compute weighted studio means
            studio_sums = np.zeros(n_studios, dtype=np.float64)
            studio_wsum = np.zeros(n_studios, dtype=np.float64)
            for k in range(n_obs):
                val = r[k] - new_person_fe[person_ind[k]]
                studio_sums[studio_ind[k]] += w[k] * val
                studio_wsum[studio_ind[k]] += w[k]
            mask_s = studio_wsum > 0
            new_studio_fe = np.zeros(n_studios, dtype=np.float64)
            new_studio_fe[mask_s] = studio_sums[mask_s] / studio_wsum[mask_s]

            # Check convergence
            person_diff = np.max(np.abs(new_person_fe - person_fe_arr))
            studio_diff = np.max(np.abs(new_studio_fe - studio_fe_arr))

            person_fe_arr = new_person_fe
            studio_fe_arr = new_studio_fe

            if max(person_diff, studio_diff) < tol:
                logger.debug("akm_converged", iteration=iteration + 1)
                break

        # Zero-sum constraint: normalize studio FE to zero mean (AKM identification)
        active_studios = studio_fe_arr != 0
        if np.any(active_studios):
            studio_mean = float(np.mean(studio_fe_arr[active_studios]))
        else:
            studio_mean = float(np.mean(studio_fe_arr)) if n_studios > 0 else 0.0
        studio_fe_arr -= studio_mean
        person_fe_arr += studio_mean  # absorb level shift into person FE

    # Step 6: Compute residuals and weighted R²
    fitted = np.full(n_obs, intercept, dtype=np.float64)
    for k in range(n_obs):
        fitted[k] += person_fe_arr[person_ind[k]] + studio_fe_arr[studio_ind[k]]
    if X.shape[1] > 0 and len(beta) > 0:
        fitted += X @ beta

    residuals_arr = y - fitted
    ss_res = float(np.sum(w * residuals_arr ** 2))
    y_wmean = float(np.sum(w * y) / np.sum(w))
    ss_tot = float(np.sum(w * (y - y_wmean) ** 2))
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    # Step 7: Empirical Bayes shrinkage of person FE (using effective obs counts)
    # With few observations, person FE estimates are noisy (incidental parameters
    # problem). Shrink toward the global mean proportionally to effective obs count:
    #   θ_shrunk[i] = (n_eff_i / (n_eff_i + κ)) · θ_raw[i] + (κ / (n_eff_i + κ)) · μ
    # where κ = σ²_residual / σ²_person (signal-to-noise ratio).
    # This pulls low-weight/few-obs estimates strongly toward the mean while
    # leaving well-observed persons nearly unchanged.
    person_fe_arr = _shrink_person_fe(
        person_fe_arr, person_ind, residuals_arr, n_obs, n_persons, logger
    )

    # Step 8: Credit-count debiasing
    # The AKM mechanically assigns lower person_fe to persons with many
    # credits because studios absorb work quality, leaving the person_fe
    # to converge toward the residual mean. This creates a spurious negative
    # correlation between person_fe and total credit count.
    # Fix: regress person_fe on log(total_credits) and subtract the slope,
    # preserving cross-person variation and overall mean.
    # Count total credits per person (not just person-year obs)
    person_credit_counts = np.zeros(n_persons, dtype=np.int64)
    person_idx_lookup = {pid: i for i, pid in enumerate(person_list)}
    for c in credits:
        idx = person_idx_lookup.get(c.person_id)
        if idx is not None:
            person_credit_counts[idx] += 1
    person_fe_arr = _debias_by_obs_count(
        person_fe_arr, person_credit_counts, n_persons, logger
    )

    # Build result dicts
    person_fe_dict = {pid: float(person_fe_arr[i]) for i, pid in enumerate(person_list)}
    studio_fe_dict = {sid: float(studio_fe_arr[i]) for i, sid in enumerate(studio_list)}
    residuals_dict = {}
    for k, key in enumerate(obs_keys):
        residuals_dict[key] = float(residuals_arr[k])

    logger.info(
        "akm_estimated",
        n_obs=n_obs,
        n_persons=n_persons,
        n_studios=n_studios,
        n_movers=n_movers,
        r_squared=round(r_squared, 4),
    )

    return AKMResult(
        person_fe=person_fe_dict,
        studio_fe=studio_fe_dict,
        beta=beta,
        gamma=np.array([]),  # not separately estimated
        residuals=residuals_dict,
        connected_set_size=len(connected_persons),
        n_movers=n_movers,
        n_observations=n_obs,
        r_squared=r_squared,
    )
