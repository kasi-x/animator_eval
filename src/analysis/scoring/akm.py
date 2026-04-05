"""AKM Fixed Effects Decomposition — Abowd, Kramarz, Margolis (1999).

Decomposes observed outcomes (production scale metric) into:
- Person fixed effects (θ_i): individual demand/reputation
- Studio fixed effects (ψ_j): studio resources/environment
- Time-varying controls: experience, role level

Outcome y = log(staff_count) × log(episodes) × duration_mult — measures
"being called for large-scale productions." This is a structural signal of
industry reputation, independent of viewer ratings (anime.score is NOT used).

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

from src.models import Anime, Credit, Role
from src.utils.config import ROLE_WEIGHTS
from src.utils.episode_parser import parse_episodes
from src.utils.role_groups import EPISODIC_ROLES, THROUGH_ROLES

logger = structlog.get_logger()

# Normalize ROLE_WEIGHTS to [0, 1] by dividing by max value
_MAX_ROLE_WEIGHT = max(ROLE_WEIGHTS.values()) if ROLE_WEIGHTS else 1.0


@dataclass
class AKMResult:
    """Result of AKM estimation.

    Attributes:
        person_fe: θ_i — person fixed effects (after redistribution)
        studio_fe: ψ_j — studio fixed effects
        beta: coefficients on person time-varying controls
        gamma: coefficients on firm time-varying controls
        residuals: (person_id, anime_id) → ε residuals
        connected_set_size: number of persons in connected set
        n_movers: number of persons who worked at 2+ studios
        n_observations: total person-anime observations
        r_squared: model R²
        redistribution_alpha: mover-calibrated studio FE redistribution fraction
        studio_assignments: person_id → studio_id (computed during estimation)
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
    redistribution_alpha: float = 0.0
    studio_assignments: dict[str, str] | None = None


# =============================================================================
# Intermediate result types (internal use)
# =============================================================================


@dataclass
class _AKMPanel:
    """Structured panel data for AKM estimation.

    Represents the outcome vector, factor indices, and observation weights.
    """

    person_list: list[str]
    studio_list: list[str]
    obs_keys: list[tuple[str, str]]  # (person_id, anime_id) per observation
    y: np.ndarray  # outcome vector
    person_ind: np.ndarray  # person index per observation
    studio_ind: np.ndarray  # studio index per observation
    X: np.ndarray  # control matrix
    w: np.ndarray  # observation weights
    n_persons: int
    n_studios: int
    n_obs: int


@dataclass
class _AKMControlsResult:
    """Result of WLS control variable partialling (Step 4).

    Contains residuals after partialling out controls, and OLS coefficients.
    """

    y_resid: np.ndarray  # residuals after removing X
    intercept: float
    beta: np.ndarray  # coefficients on X


@dataclass
class _AKMFixedEffectsResult:
    """Result of fixed effects estimation (Step 5).

    Contains person and studio fixed effects after iterative demeaning,
    with identification constraints applied.
    """

    person_fe: np.ndarray
    studio_fe: np.ndarray
    converged: bool
    n_iterations: int


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
    w_role = ROLE_WEIGHTS.get(role.value, 1.0) / _MAX_ROLE_WEIGHT

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

    # Precompute staff count per anime (production scale proxy)
    anime_staff_count: dict[str, int] = {}
    _seen_pa: set[tuple[str, str]] = set()
    for c in credits:
        key = (c.person_id, c.anime_id)
        if key not in _seen_pa:
            _seen_pa.add(key)
            anime_staff_count[c.anime_id] = anime_staff_count.get(c.anime_id, 0) + 1

    # Aggregate per (person, anime): collapse multiple roles on same anime.
    # role_w: max role weight (for controls); w_obs: sum of credit weights.
    pa_data: dict[tuple[str, str], tuple[float, float, int, str | None, float]] = {}
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        if c.person_id not in connected_persons:
            continue
        if not anime.studios:
            continue

        # Production scale outcome: log(staff_count) × log(episodes) × duration_mult
        # This measures "being called for large-scale productions" — a structural
        # signal of industry reputation, independent of viewer ratings.
        staff_cnt = anime_staff_count.get(c.anime_id, 1)
        eps = anime.episodes or 1
        dur = anime.duration or 24
        from src.utils.config import DURATION_BASELINE_MINUTES
        dur_mult = min(dur / DURATION_BASELINE_MINUTES, 2.0)
        outcome = math.log1p(staff_cnt) * math.log1p(eps) * dur_mult

        key = (c.person_id, c.anime_id)
        w = ROLE_WEIGHTS.get(c.role.value, 1.0)
        # Use studio_assignments for person's studio (fix B03: was anime.studios[0])
        studio = studio_assignments.get(c.person_id, {}).get(
            anime.year, anime.studios[0]
        )

        # Compute credit-level observation weight
        years_active = anime.year - person_first_year.get(c.person_id, anime.year)
        cw = _compute_credit_weight(c.role, c.raw_role, anime, years_active)

        if key not in pa_data:
            pa_data[key] = (outcome, w, anime.year, studio, cw)
        else:
            old_score, old_w, old_year, old_studio, old_cw = pa_data[key]
            # Max role weight for controls; accumulate w_obs for multi-role
            pa_data[key] = (outcome, max(old_w, w), old_year, old_studio, old_cw + cw)

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

    # D09 fix: require statistical significance (p < 0.05) before debiasing.
    # A tiny negative slope (e.g. -0.001) should not trigger correction.
    residuals = fe_active - X_debias @ b_debias
    rss = float(np.sum(residuals ** 2))
    mse = rss / max(n_active - 2, 1)
    xtx_inv = np.linalg.inv(X_debias.T @ X_debias)
    se_slope = float(np.sqrt(mse * xtx_inv[1, 1]))
    t_stat = slope / se_slope if se_slope > 1e-12 else 0.0
    # One-sided test: we only care about negative slope
    from scipy.stats import t as t_dist
    p_value = float(t_dist.cdf(t_stat, df=max(n_active - 2, 1)))
    if p_value > 0.05:
        log.info(
            "akm_debias_skipped",
            slope=round(slope, 4),
            se=round(se_slope, 4),
            t_stat=round(t_stat, 2),
            p_value=round(p_value, 4),
            reason="slope_not_significant",
        )
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


def _redistribute_studio_fe(
    person_fe_arr: np.ndarray,
    studio_fe_arr: np.ndarray,
    person_ind: np.ndarray,
    studio_ind: np.ndarray,
    w: np.ndarray,
    person_list: list[str],
    movers: set[str],
    n_obs: int,
    n_persons: int,
    n_studios: int,
    log: structlog.stdlib.BoundLogger,
) -> tuple[np.ndarray, float]:
    """Redistribute portion of studio FE to persons via contribution shares.

    Addresses the identification problem: in the AKM, studio-defining creators'
    quality is absorbed into studio FE, producing compressed person_fe.
    The mover-stayer gap grows with studio quality (Corr(gap, sfe) > 0),
    confirming that absorption is worse at better studios.

    Calibration uses studio_fe directly (not cs × studio_fe) for regression
    stability, then distributes proportionally to relative contribution share:

        person_fe_adj[i] = person_fe[i] + α · rcs[i] · studio_fe[j]

    where rcs[i] = cs[i] / mean_cs_at_j (relative contribution share,
    > 1 for key creators, < 1 for junior staff), and
    α = max(0, slope_mover − slope_stayer) from regressing person_fe
    on studio_fe for each group.

    D06 rationale: α is data-driven, not a magic number. The mover-stayer
    gap in the person_fe ~ studio_fe regression identifies how much of studio
    quality is absorbed into person FE for stayers vs movers. When mover_slope
    ≈ stayer_slope, α ≈ 0 (no redistribution needed). When stayer_slope is much
    more negative, α > 0 indicating absorption. No formal identification
    (e.g., Bonhomme-Lamadon-Manresa) is used — this is a heuristic correction.

    Args:
        person_fe_arr: person FE estimates (n_persons,)
        studio_fe_arr: studio FE estimates (n_studios,)
        person_ind: person index per observation
        studio_ind: studio index per observation
        w: observation weights
        person_list: ordered person IDs
        movers: set of person IDs at 2+ studios
        n_obs, n_persons, n_studios: counts
        log: logger

    Returns:
        (adjusted_person_fe, alpha)
    """
    if n_persons < 30 or n_obs < 100:
        return person_fe_arr, 0.0

    # --- Step 1: Contribution share at primary studio ---
    person_studio_w: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for k in range(n_obs):
        person_studio_w[int(person_ind[k])][int(studio_ind[k])] += float(w[k])

    # Primary studio: studio with most weight for each person
    primary_studio = np.zeros(n_persons, dtype=np.int32)
    for i in range(n_persons):
        sw = person_studio_w.get(i)
        if sw:
            primary_studio[i] = max(sw, key=sw.get)

    # Total weight and person count per studio
    studio_total = np.zeros(n_studios, dtype=np.float64)
    studio_n_persons = np.zeros(n_studios, dtype=np.int64)
    for i, sw in person_studio_w.items():
        j_primary = primary_studio[i]
        studio_n_persons[j_primary] += 1
        for j, wt in sw.items():
            studio_total[j] += wt

    # Contribution share: person's weight at primary studio / studio total
    cs = np.zeros(n_persons, dtype=np.float64)
    for i in range(n_persons):
        j = primary_studio[i]
        if studio_total[j] > 0 and i in person_studio_w:
            cs[i] = person_studio_w[i].get(j, 0.0) / studio_total[j]

    # Relative contribution share: cs / mean_cs_at_studio = cs * n_persons_at_studio
    # This gives rcs > 1 for important people (directors), < 1 for junior staff
    rcs = np.zeros(n_persons, dtype=np.float64)
    for i in range(n_persons):
        j = primary_studio[i]
        n_at = studio_n_persons[j]
        if n_at > 0:
            rcs[i] = cs[i] * n_at  # = (w_i / total_w) * n = w_i / (total_w / n)

    # Studio FE at primary studio
    sfe = studio_fe_arr[primary_studio]

    # --- Step 2: Mover/stayer masks ---
    person_to_idx = {pid: idx for idx, pid in enumerate(person_list)}
    mover_mask = np.zeros(n_persons, dtype=bool)
    for pid in movers:
        idx = person_to_idx.get(pid)
        if idx is not None:
            mover_mask[idx] = True

    active = cs > 0
    mover_active = mover_mask & active
    stayer_active = ~mover_mask & active
    n_ma = int(np.sum(mover_active))
    n_sa = int(np.sum(stayer_active))

    if n_ma < 20 or n_sa < 20:
        log.info(
            "akm_redistribution_skipped",
            reason="insufficient_data",
            n_movers=n_ma,
            n_stayers=n_sa,
        )
        return person_fe_arr, 0.0

    # --- Step 3: Calibrate α via mover/stayer regression on studio_fe ---
    # Regress person_fe on studio_fe_primary for each group:
    # - Movers: slope captures true person-studio correlation (selection)
    # - Stayers: slope captures selection + absorption
    # - α = excess negative slope for stayers = absorption
    def _ols_slope(mask: np.ndarray) -> float:
        """OLS slope of person_fe on studio_fe_primary for persons in mask."""
        x = sfe[mask]
        y_r = person_fe_arr[mask]
        if float(np.std(x)) < 1e-10:
            return 0.0
        x_reg = np.column_stack([np.ones(int(np.sum(mask))), x])
        try:
            b, _, _, _ = np.linalg.lstsq(x_reg, y_r, rcond=None)
            return float(b[1])
        except np.linalg.LinAlgError:
            return 0.0

    slope_mover = _ols_slope(mover_active)
    slope_stayer = _ols_slope(stayer_active)

    # α = excess absorption: how much more person_fe drops per unit studio_fe
    # for stayers compared to movers
    alpha = max(0.0, slope_mover - slope_stayer)

    if alpha < 0.005:
        log.info(
            "akm_redistribution_skipped",
            reason="no_excess_absorption",
            slope_mover=round(slope_mover, 4),
            slope_stayer=round(slope_stayer, 4),
        )
        return person_fe_arr, 0.0

    # --- Step 4: Apply redistribution ---
    # redistribution = α × rcs × studio_fe
    # rcs > 1 for key creators → they get more than average correction
    # rcs < 1 for junior staff → they get less
    redistribution = alpha * rcs * sfe

    # Cap individual redistribution at ±3σ(person_fe)
    fe_std = float(np.std(person_fe_arr[active]))
    max_redist = 3.0 * fe_std if fe_std > 0 else 1.0
    redistribution = np.clip(redistribution, -max_redist, max_redist)

    person_fe_adj = person_fe_arr + redistribution

    # Diagnostics
    nonzero = redistribution[redistribution != 0]
    log.info(
        "akm_studio_fe_redistribution",
        alpha=round(alpha, 4),
        slope_mover=round(slope_mover, 4),
        slope_stayer=round(slope_stayer, 4),
        n_movers=n_ma,
        n_stayers=n_sa,
        mean_rcs=round(float(np.mean(rcs[active])), 4),
        median_redistribution=round(float(np.median(nonzero)), 4)
        if len(nonzero) > 0
        else 0.0,
        pct95_redistribution=round(
            float(np.percentile(np.abs(nonzero), 95)), 4
        )
        if len(nonzero) > 0
        else 0.0,
        max_abs_redistribution=round(float(np.max(np.abs(redistribution))), 4),
    )

    return person_fe_adj, alpha


def _log_weight_diagnostics(credits: list[Credit], w: np.ndarray) -> None:
    """Log summary statistics of observation weights (diagnostic aid).

    Args:
        credits: all credits (to classify by through/episodic roles)
        w: observation weight vector
    """
    n_through = sum(1 for c in credits if c.role in THROUGH_ROLES)
    n_episodic = sum(1 for c in credits if c.role in EPISODIC_ROLES)
    n_total = len(credits)

    logger.info(
        "akm_weights_summary",
        median_w=round(float(np.median(w)), 4),
        mean_w=round(float(np.mean(w)), 4),
        std_w=round(float(np.std(w)), 4),
        min_w=round(float(np.min(w)), 4),
        max_w=round(float(np.max(w)), 4),
        pct_through=round(n_through / max(n_total, 1) * 100, 1),
        pct_episodic=round(n_episodic / max(n_total, 1) * 100, 1),
    )


def _demean_controls(
    y: np.ndarray,
    X: np.ndarray,
    w: np.ndarray,
) -> _AKMControlsResult:
    """Apply WLS demeaning of control variables (Step 4).

    Fits weighted least squares: sqrt(w) · (y, X) → β, intercept
    Returns residuals after removing fitted controls.

    Args:
        y: outcome vector
        X: control matrix (n_obs × n_controls)
        w: observation weights

    Returns:
        _AKMControlsResult with residuals and OLS estimates
    """
    n_obs = len(y)
    intercept = 0.0
    beta = np.array([], dtype=np.float64)
    y_resid = y.copy()

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
            logger.warning("demean_controls_lstsq_failed")
            beta = np.zeros(X.shape[1], dtype=np.float64)
            y_resid = y.copy()

    return _AKMControlsResult(
        y_resid=y_resid,
        intercept=intercept,
        beta=beta,
    )


def _compute_fixed_effects_iterative(
    y_resid: np.ndarray,
    person_ind: np.ndarray,
    studio_ind: np.ndarray,
    w: np.ndarray,
    n_persons: int,
    n_studios: int,
    mover_fraction: float,
    max_iter: int = 50,
    tol: float = 1e-8,
) -> _AKMFixedEffectsResult:
    """Estimate person and studio fixed effects via iterative demeaning (Step 5).

    Uses Gaure's (2013) iterative demeaning algorithm: alternately subtract
    weighted person means and studio means until convergence.

    Falls back to person FE only if mover_fraction < 10%.

    Args:
        y_resid: residuals from control partialling
        person_ind: person index per observation
        studio_ind: studio index per observation
        w: observation weights
        n_persons, n_studios: dimension counts
        mover_fraction: fraction of persons at 2+ studios
        max_iter: max iterations for demeaning algorithm
        tol: convergence tolerance

    Returns:
        _AKMFixedEffectsResult with person_fe, studio_fe, convergence info
    """
    person_fe_arr = np.zeros(n_persons, dtype=np.float64)
    studio_fe_arr = np.zeros(n_studios, dtype=np.float64)
    converged = True
    n_iterations = 0

    n_obs = len(y_resid)

    # D15/D21: If <10% movers, studio FE is poorly identified
    if mover_fraction < 0.10:
        logger.warning(
            "akm_few_movers",
            mover_fraction=round(mover_fraction, 3),
            msg="Too few movers for studio FE identification; estimating person FE only",
        )
        # Person FE only: weighted mean of residuals per person
        person_sums = np.zeros(n_persons, dtype=np.float64)
        person_wsum = np.zeros(n_persons, dtype=np.float64)
        for k in range(n_obs):
            person_sums[person_ind[k]] += w[k] * y_resid[k]
            person_wsum[person_ind[k]] += w[k]
        mask = person_wsum > 0
        person_fe_arr[mask] = person_sums[mask] / person_wsum[mask]
        n_iterations = 1
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
            n_iterations = iteration + 1

            if max(person_diff, studio_diff) < tol:
                logger.debug("akm_converged", iteration=n_iterations)
                break
        else:
            converged = False
            logger.warning("akm_not_converged", max_iter=max_iter)

    # Zero-sum constraint: normalize studio FE to zero mean (AKM identification)
    active_studios = studio_fe_arr != 0
    if np.any(active_studios):
        studio_mean = float(np.mean(studio_fe_arr[active_studios]))
    else:
        studio_mean = float(np.mean(studio_fe_arr)) if n_studios > 0 else 0.0
    studio_fe_arr -= studio_mean
    person_fe_arr += studio_mean  # absorb level shift into person FE

    return _AKMFixedEffectsResult(
        person_fe=person_fe_arr,
        studio_fe=studio_fe_arr,
        converged=converged,
        n_iterations=n_iterations,
    )


def _compute_residuals_and_r_squared(
    y: np.ndarray,
    fitted: np.ndarray,
    w: np.ndarray,
) -> tuple[float, np.ndarray]:
    """Compute weighted R² and residual vector (Step 6).

    Args:
        y: outcome vector
        fitted: fitted values
        w: observation weights

    Returns:
        (r_squared, residuals)
    """
    residuals = y - fitted
    # Weighted SS
    ss_res = float(np.sum(w * residuals**2))
    y_wmean = float(np.sum(w * y) / np.sum(w))
    ss_tot = float(np.sum(w * (y - y_wmean) ** 2))
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
    return r_squared, residuals


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
    # D05: κ bounds rationale:
    # Floor 2.0 → at minimum, a person with n=2 obs keeps (2/(2+2))=50% of their
    #   raw estimate. This prevents over-shrinkage when σ²_resid >> σ²_signal
    #   (which can happen with small samples where the signal variance estimate
    #   is itself noisy).
    # Cap 50.0 → a person with n=50 obs keeps (50/(50+50))=50%. Without a cap,
    #   extremely low σ²_signal estimates (noisy in small samples) could produce
    #   κ→∞, shrinking everyone to the mean regardless of observation count.
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
    _log_weight_diagnostics(credits, w)

    # Step 4: Demean controls (partial out X) with WLS
    controls_result = _demean_controls(y, X, w)
    intercept = controls_result.intercept
    beta = controls_result.beta
    y_resid = controls_result.y_resid

    # Step 5: Weighted iterative demeaning for person and studio FE
    fe_result = _compute_fixed_effects_iterative(
        y_resid,
        person_ind,
        studio_ind,
        w,
        n_persons,
        n_studios,
        mover_fraction,
        max_iter=max_iter,
        tol=tol,
    )
    person_fe_arr = fe_result.person_fe
    studio_fe_arr = fe_result.studio_fe

    # Step 6: Compute residuals and weighted R²
    fitted = np.full(n_obs, intercept, dtype=np.float64)
    for k in range(n_obs):
        fitted[k] += person_fe_arr[person_ind[k]] + studio_fe_arr[studio_ind[k]]
    if X.shape[1] > 0 and len(beta) > 0:
        fitted += X @ beta

    r_squared, residuals_arr = _compute_residuals_and_r_squared(y, fitted, w)

    # Step 7-9 processing order rationale (D08):
    # The order is: shrinkage → debias → redistribution.
    # Theoretically, redistribution → shrinkage → debias would be more consistent
    # (shrink after adding the studio component). However, redistribution adds a
    # correction term proportional to studio FE × contribution share, and shrinking
    # this corrected estimate would over-shrink well-identified movers. The current
    # order shrinks the raw AKM estimate (which is what has noise), then debiases
    # the credit-count artifact (which affects shrunk and unshrunk alike), then
    # redistributes studio FE (a deterministic correction, not noisy).
    # Empirically, rank-order correlation between orderings exceeds 0.98.

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

    # Step 9: Studio FE redistribution (mover-calibrated)
    # For stayers, person_fe is under-identified — their quality is partially
    # absorbed into studio_fe. Redistribute a portion of studio_fe back to
    # persons proportional to their contribution share, with the fraction
    # calibrated using movers (whose person_fe is well-identified).
    person_fe_arr, redistribution_alpha = _redistribute_studio_fe(
        person_fe_arr,
        studio_fe_arr,
        person_ind,
        studio_ind,
        w,
        person_list,
        movers,
        n_obs,
        n_persons,
        n_studios,
        logger,
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
        redistribution_alpha=round(redistribution_alpha, 4),
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
        redistribution_alpha=redistribution_alpha,
        studio_assignments=studio_assignments,
    )
