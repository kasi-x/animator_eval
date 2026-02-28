"""AKM Fixed Effects Decomposition — Abowd, Kramarz, Margolis (1999).

Decomposes observed outcomes (anime scores) into:
- Person fixed effects (θ_i): individual talent/ability
- Studio fixed effects (ψ_j): studio resources/environment
- Time-varying controls: experience, role level, credits per year

Uses iterative demeaning (Gaure 2013) to avoid massive dummy matrices.
Studio is inferred from anime credits when not directly available.
"""

from collections import defaultdict
from dataclasses import dataclass

import numpy as np
import structlog

from src.models import Anime, Credit
from src.utils.config import ROLE_WEIGHTS

logger = structlog.get_logger()


@dataclass
class AKMResult:
    """Result of AKM estimation.

    Attributes:
        person_fe: θ_i — person fixed effects
        studio_fe: ψ_j — studio fixed effects
        beta: coefficients on person time-varying controls
        gamma: coefficients on firm time-varying controls
        residuals: (person_id, year) → ε residuals
        connected_set_size: number of persons in connected set
        n_movers: number of persons who worked at 2+ studios
        n_observations: total person-year observations
        r_squared: model R²
    """

    person_fe: dict[str, float]
    studio_fe: dict[str, float]
    beta: np.ndarray
    gamma: np.ndarray
    residuals: dict[tuple[str, int], float]
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
        studio = anime.studios[0]
        w = ROLE_WEIGHTS.get(c.role.value, 1.0)
        weight_accum[(c.person_id, anime.year, studio)] += w

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


def _build_panel(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    studio_assignments: dict[str, dict[int, str]],
    connected_persons: set[str],
    connected_studios: set[str],
) -> tuple[
    list[str],  # person_ids (ordered)
    list[int],  # years (ordered)
    list[str],  # studio_ids (ordered)
    np.ndarray,  # y: outcomes (n_obs,)
    np.ndarray,  # person_indicators (n_obs,) int indices
    np.ndarray,  # studio_indicators (n_obs,) int indices
    np.ndarray,  # X: person controls (n_obs, n_x)
]:
    """Build panel data for AKM estimation."""
    # Aggregate outcomes: y_{it} = weighted avg anime score for person i in year t
    outcome_accum: dict[tuple[str, int], list[tuple[float, float]]] = defaultdict(list)
    credit_counts: dict[tuple[str, int], int] = defaultdict(int)

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

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year or anime.score is None:
            continue
        if c.person_id not in connected_persons:
            continue
        year = anime.year
        w = ROLE_WEIGHTS.get(c.role.value, 1.0)
        outcome_accum[(c.person_id, year)].append((anime.score, w))
        credit_counts[(c.person_id, year)] += 1

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
    obs_keys = []

    for (pid, year), score_weights in outcome_accum.items():
        studio = studio_assignments.get(pid, {}).get(year)
        if studio not in studio_to_idx:
            continue

        # Weighted mean score
        total_w = sum(w for _, w in score_weights)
        y = sum(s * w for s, w in score_weights) / total_w if total_w > 0 else 0

        # Controls: experience_years, n_credits_year
        experience = year - person_first_year.get(pid, year)
        n_credits = credit_counts[(pid, year)]

        obs_y.append(y)
        obs_person.append(person_to_idx[pid])
        obs_studio.append(studio_to_idx[studio])
        obs_x.append([experience, n_credits])
        obs_keys.append((pid, year))

    if not obs_y:
        return person_list, [], studio_list, np.array([]), np.array([]), np.array([]), np.array([]).reshape(0, 2)

    y = np.array(obs_y, dtype=np.float64)
    person_ind = np.array(obs_person, dtype=np.int32)
    studio_ind = np.array(obs_studio, dtype=np.int32)
    X = np.array(obs_x, dtype=np.float64)

    return person_list, obs_keys, studio_list, y, person_ind, studio_ind, X


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

    # Step 3: Build panel
    person_list, obs_keys, studio_list, y, person_ind, studio_ind, X = _build_panel(
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

    # Step 4: Demean controls (partial out X)
    if X.shape[1] > 0 and n_obs > X.shape[1]:
        # OLS: y = X β + residual
        try:
            beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
            y_resid = y - X @ beta
        except np.linalg.LinAlgError:
            beta = np.zeros(X.shape[1])
            y_resid = y.copy()
    else:
        beta = np.array([])
        y_resid = y.copy()

    # Step 5: Iterative demeaning for person and studio FE
    person_fe_arr = np.zeros(n_persons, dtype=np.float64)
    studio_fe_arr = np.zeros(n_studios, dtype=np.float64)

    if mover_fraction < 0.10:
        logger.warning(
            "akm_few_movers",
            mover_fraction=round(mover_fraction, 3),
            msg="Too few movers for studio FE identification; estimating person FE only",
        )
        # Person FE only: θ_i = mean(y_resid for person i)
        person_sums = np.zeros(n_persons, dtype=np.float64)
        person_counts = np.zeros(n_persons, dtype=np.float64)
        for k in range(n_obs):
            person_sums[person_ind[k]] += y_resid[k]
            person_counts[person_ind[k]] += 1
        mask = person_counts > 0
        person_fe_arr[mask] = person_sums[mask] / person_counts[mask]
    else:
        # Iterative demeaning: alternate person and studio mean subtraction
        r = y_resid.copy()

        for iteration in range(max_iter):
            # Compute person means
            person_sums = np.zeros(n_persons, dtype=np.float64)
            person_counts = np.zeros(n_persons, dtype=np.float64)
            for k in range(n_obs):
                val = r[k] - studio_fe_arr[studio_ind[k]]
                person_sums[person_ind[k]] += val
                person_counts[person_ind[k]] += 1
            mask_p = person_counts > 0
            new_person_fe = np.zeros(n_persons, dtype=np.float64)
            new_person_fe[mask_p] = person_sums[mask_p] / person_counts[mask_p]

            # Compute studio means
            studio_sums = np.zeros(n_studios, dtype=np.float64)
            studio_counts = np.zeros(n_studios, dtype=np.float64)
            for k in range(n_obs):
                val = r[k] - new_person_fe[person_ind[k]]
                studio_sums[studio_ind[k]] += val
                studio_counts[studio_ind[k]] += 1
            mask_s = studio_counts > 0
            new_studio_fe = np.zeros(n_studios, dtype=np.float64)
            new_studio_fe[mask_s] = studio_sums[mask_s] / studio_counts[mask_s]

            # Check convergence
            person_diff = np.max(np.abs(new_person_fe - person_fe_arr))
            studio_diff = np.max(np.abs(new_studio_fe - studio_fe_arr))

            person_fe_arr = new_person_fe
            studio_fe_arr = new_studio_fe

            if max(person_diff, studio_diff) < tol:
                logger.debug("akm_converged", iteration=iteration + 1)
                break

    # Step 6: Compute residuals and R²
    fitted = np.zeros(n_obs, dtype=np.float64)
    for k in range(n_obs):
        fitted[k] = person_fe_arr[person_ind[k]] + studio_fe_arr[studio_ind[k]]
    if X.shape[1] > 0 and len(beta) > 0:
        fitted += X @ beta

    residuals_arr = y - fitted
    ss_res = np.sum(residuals_arr ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

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
