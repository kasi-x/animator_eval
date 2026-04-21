"""Voice Actor AKM Fixed Effects — person FE + sound director FE.

Outcome: y_va(i,j) = char_role_weight × log(1+cast_size) × log(1+episodes) × duration_mult

Fixed effects:
    θ_va_i = voice actor fixed effect (individual demand/reputation)
    ψ_sd_k = sound director fixed effect (casting authority)

Uses the same iterative demeaning framework as the production AKM.
"""

import math
from collections import defaultdict
from dataclasses import dataclass

import numpy as np
import structlog

from src.analysis.va.graph import _char_role_weight
from src.models import AnimeAnalysis as Anime, CharacterVoiceActor, Credit, Role

logger = structlog.get_logger()


@dataclass
class VAAKMResult:
    """Result of VA AKM estimation.

    Attributes:
        person_fe: θ_va — VA person fixed effects
        sd_fe: ψ_sd — sound director fixed effects
        residuals: (va_id, anime_id) → residual
        connected_set_size: persons in connected set
        n_movers: VAs who worked with 2+ sound directors
        n_observations: total VA-anime observations
        r_squared: model R²
    """

    person_fe: dict[str, float]
    sd_fe: dict[str, float]
    residuals: dict[tuple[str, str], float]
    connected_set_size: int
    n_movers: int
    n_observations: int
    r_squared: float


def _infer_sd_assignment(
    va_credits: list[CharacterVoiceActor],
    production_credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, dict[int, str]]:
    """Infer sound director assignment for each VA per year.

    For each VA-year, pick the sound director of the anime where the VA
    had the most prominent role.
    """
    # Sound directors per anime
    anime_sd: dict[str, set[str]] = defaultdict(set)
    for c in production_credits:
        if c.role == Role.SOUND_DIRECTOR:
            anime_sd[c.anime_id].add(c.person_id)

    # VA best role weight per (va, year, sd)
    weight_accum: dict[tuple[str, int, str], float] = defaultdict(float)
    for cva in va_credits:
        anime = anime_map.get(cva.anime_id)
        if not anime or not anime.year:
            continue
        sds = anime_sd.get(cva.anime_id, set())
        if not sds:
            continue
        w = _char_role_weight(cva.character_role)
        per_sd_w = w / len(sds)
        for sd_id in sds:
            weight_accum[(cva.person_id, anime.year, sd_id)] += per_sd_w

    # Pick best SD per VA-year
    va_year_best: dict[tuple[str, int], tuple[str, float]] = {}
    for (va_id, year, sd_id), w in weight_accum.items():
        key = (va_id, year)
        if key not in va_year_best or w > va_year_best[key][1]:
            va_year_best[key] = (sd_id, w)

    result: dict[str, dict[int, str]] = defaultdict(dict)
    for (va_id, year), (sd_id, _) in va_year_best.items():
        result[va_id][year] = sd_id

    return dict(result)


def estimate_va_akm(
    va_credits: list[CharacterVoiceActor],
    production_credits: list[Credit],
    anime_map: dict[str, Anime],
    max_iter: int = 50,
    tol: float = 1e-8,
) -> VAAKMResult:
    """Estimate VA AKM fixed effects model.

    Outcome: char_role_weight × log(1+cast_size) × log(1+episodes) × duration_mult

    Args:
        va_credits: character_voice_actor records
        production_credits: production credits (for sound directors)
        anime_map: anime_id → Anime
        max_iter: maximum demeaning iterations
        tol: convergence tolerance

    Returns:
        VAAKMResult with VA and sound director fixed effects.
    """
    # Step 1: Infer sound director assignments
    sd_assignments = _infer_sd_assignment(va_credits, production_credits, anime_map)

    if not sd_assignments:
        logger.warning("va_akm_no_sd_assignments")
        return VAAKMResult({}, {}, {}, 0, 0, 0, 0.0)

    # Step 2: Find connected set (VAs connected through shared SDs)
    va_sds: dict[str, set[str]] = {}
    all_sds: set[str] = set()
    for va_id, year_sd in sd_assignments.items():
        sds = set(year_sd.values())
        va_sds[va_id] = sds
        all_sds.update(sds)

    # Union-Find for connected set
    parent: dict[str, str] = {s: s for s in all_sds}

    def find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    movers = set()
    for va_id, sds in va_sds.items():
        sd_list = list(sds)
        if len(sd_list) >= 2:
            movers.add(va_id)
            for i in range(1, len(sd_list)):
                union(sd_list[0], sd_list[i])

    # All VAs are in connected set (may not have movers)
    connected_vas = set(sd_assignments.keys())
    connected_sds = all_sds

    # Step 3: Build panel (observation = VA × anime)
    # Cast size per anime (number of unique VAs)
    anime_cast_size: dict[str, int] = defaultdict(int)
    _seen: set[tuple[str, str]] = set()
    for cva in va_credits:
        key = (cva.person_id, cva.anime_id)
        if key not in _seen:
            _seen.add(key)
            anime_cast_size[cva.anime_id] += 1

    # VA experience (first year)
    va_first_year: dict[str, int] = {}
    for cva in va_credits:
        anime = anime_map.get(cva.anime_id)
        if anime and anime.year:
            if cva.person_id not in va_first_year:
                va_first_year[cva.person_id] = anime.year
            else:
                va_first_year[cva.person_id] = min(
                    va_first_year[cva.person_id], anime.year
                )

    # Aggregate per (VA, anime): best role weight + outcome
    pa_data: dict[tuple[str, str], tuple[float, float, str]] = {}
    for cva in va_credits:
        anime = anime_map.get(cva.anime_id)
        if not anime or not anime.year:
            continue
        if cva.person_id not in connected_vas:
            continue

        # Outcome
        cast_size = anime_cast_size.get(cva.anime_id, 1)
        eps = anime.episodes or 1
        dur = anime.duration or 24
        dur_mult = min(dur / 30, 2.0)
        best_role_w = _char_role_weight(cva.character_role)
        outcome = best_role_w * math.log1p(cast_size) * math.log1p(eps) * dur_mult

        # SD assignment
        sd_id = sd_assignments.get(cva.person_id, {}).get(anime.year, "")
        if not sd_id:
            continue

        key = (cva.person_id, cva.anime_id)
        if key not in pa_data or outcome > pa_data[key][0]:
            pa_data[key] = (outcome, best_role_w, sd_id)

    if not pa_data:
        logger.warning("va_akm_no_observations")
        return VAAKMResult({}, {}, {}, len(connected_vas), len(movers), 0, 0.0)

    # Build ordered indices
    va_list = sorted(connected_vas)
    sd_list = sorted(connected_sds)
    va_to_idx = {vid: i for i, vid in enumerate(va_list)}
    sd_to_idx = {sid: i for i, sid in enumerate(sd_list)}

    # Build arrays
    obs_y, obs_va, obs_sd, obs_keys = [], [], [], []
    for (va_id, anime_id), (outcome, role_w, sd_id) in pa_data.items():
        if sd_id not in sd_to_idx:
            continue
        obs_y.append(outcome)
        obs_va.append(va_to_idx[va_id])
        obs_sd.append(sd_to_idx[sd_id])
        obs_keys.append((va_id, anime_id))

    n_obs = len(obs_y)
    if n_obs == 0:
        return VAAKMResult({}, {}, {}, len(connected_vas), len(movers), 0, 0.0)

    y = np.array(obs_y, dtype=np.float64)
    va_ind = np.array(obs_va, dtype=np.int32)
    sd_ind = np.array(obs_sd, dtype=np.int32)
    n_va = len(va_list)
    n_sd = len(sd_list)

    # Step 4: Iterative demeaning
    va_fe_arr = np.zeros(n_va, dtype=np.float64)
    sd_fe_arr = np.zeros(n_sd, dtype=np.float64)

    mover_fraction = len(movers) / len(connected_vas) if connected_vas else 0

    if mover_fraction < 0.05:
        # Too few movers — VA FE only
        logger.info("va_akm_few_movers", fraction=round(mover_fraction, 3))
        sums = np.zeros(n_va, dtype=np.float64)
        cnts = np.zeros(n_va, dtype=np.int64)
        for k in range(n_obs):
            sums[va_ind[k]] += y[k]
            cnts[va_ind[k]] += 1
        mask = cnts > 0
        va_fe_arr[mask] = sums[mask] / cnts[mask]
    else:
        r = y.copy()
        for iteration in range(max_iter):
            # VA means
            va_sums = np.zeros(n_va, dtype=np.float64)
            va_cnts = np.zeros(n_va, dtype=np.int64)
            for k in range(n_obs):
                va_sums[va_ind[k]] += r[k] - sd_fe_arr[sd_ind[k]]
                va_cnts[va_ind[k]] += 1
            mask_v = va_cnts > 0
            new_va = np.zeros(n_va, dtype=np.float64)
            new_va[mask_v] = va_sums[mask_v] / va_cnts[mask_v]

            # SD means
            sd_sums = np.zeros(n_sd, dtype=np.float64)
            sd_cnts = np.zeros(n_sd, dtype=np.int64)
            for k in range(n_obs):
                sd_sums[sd_ind[k]] += r[k] - new_va[va_ind[k]]
                sd_cnts[sd_ind[k]] += 1
            mask_s = sd_cnts > 0
            new_sd = np.zeros(n_sd, dtype=np.float64)
            new_sd[mask_s] = sd_sums[mask_s] / sd_cnts[mask_s]

            # Check convergence
            if (
                np.max(np.abs(new_va - va_fe_arr)) < tol
                and np.max(np.abs(new_sd - sd_fe_arr)) < tol
            ):
                break

            va_fe_arr = new_va
            sd_fe_arr = new_sd

        # Zero-sum normalization for SD FE
        active_sd = sd_fe_arr != 0
        if np.any(active_sd):
            sd_mean = float(np.mean(sd_fe_arr[active_sd]))
            sd_fe_arr -= sd_mean
            va_fe_arr += sd_mean

    # Step 5: Empirical Bayes shrinkage
    obs_counts = np.zeros(n_va, dtype=np.int64)
    for k in range(n_obs):
        obs_counts[va_ind[k]] += 1

    fitted = np.array(
        [va_fe_arr[va_ind[k]] + sd_fe_arr[sd_ind[k]] for k in range(n_obs)]
    )
    residuals_arr = y - fitted

    sigma2_resid = float(np.mean(residuals_arr**2)) if n_obs > 0 else 1.0
    active = obs_counts > 0
    if np.any(active):
        sigma2_raw = float(np.var(va_fe_arr[active]))
        n_bar = float(np.mean(obs_counts[active]))
        sigma2_signal = max(sigma2_raw - sigma2_resid / n_bar, sigma2_raw * 0.1)
        kappa = float(
            np.clip(
                sigma2_resid / sigma2_signal if sigma2_signal > 0 else 10.0, 2.0, 50.0
            )
        )
        mu = float(np.mean(va_fe_arr[active]))

        for i in range(n_va):
            n_i = obs_counts[i]
            if n_i == 0:
                continue
            reliability = n_i / (n_i + kappa)
            va_fe_arr[i] = reliability * va_fe_arr[i] + (1 - reliability) * mu

    # Compute R²
    ss_res = float(np.sum(residuals_arr**2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

    # Build result dicts
    person_fe = {vid: float(va_fe_arr[i]) for i, vid in enumerate(va_list)}
    sd_fe = {sid: float(sd_fe_arr[i]) for i, sid in enumerate(sd_list)}
    residuals = {key: float(residuals_arr[k]) for k, key in enumerate(obs_keys)}

    logger.info(
        "va_akm_estimated",
        n_obs=n_obs,
        n_va=n_va,
        n_sd=n_sd,
        n_movers=len(movers),
        r_squared=round(r_squared, 4),
    )

    return VAAKMResult(
        person_fe=person_fe,
        sd_fe=sd_fe,
        residuals=residuals,
        connected_set_size=len(connected_vas),
        n_movers=len(movers),
        n_observations=n_obs,
        r_squared=r_squared,
    )
