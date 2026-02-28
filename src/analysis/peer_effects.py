"""Peer Effects Estimation — 2SLS for solving Manski's Reflection Problem.

Uses instrumental variables (distance-2 neighbor characteristics) to
separate endogenous peer effects from correlated effects and exogenous
peer effects.
"""

from collections import defaultdict
from dataclasses import dataclass

import networkx as nx
import numpy as np
import structlog

from src.models import Anime, Credit
from src.utils.config import ROLE_WEIGHTS

logger = structlog.get_logger()


@dataclass
class PeerEffectResult:
    """Result of peer effects estimation.

    Attributes:
        endogenous_effect: b — effect of peer outcomes on own outcome
        exogenous_effect: a — effect of peer characteristics on own outcome
        own_effect: c — effect of own characteristics on own outcome
        first_stage_f_stat: F-statistic from first stage regression
        n_observations: number of person-anime observations used
        person_peer_boost: per-person boost from peers (b × mean_peer_score)
        identified: False if weak instruments (F < 10)
    """

    endogenous_effect: float
    exogenous_effect: float
    own_effect: float
    first_stage_f_stat: float
    n_observations: int
    person_peer_boost: dict[str, float]
    identified: bool


def _build_peer_data(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float],
    collaboration_graph,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, list[str]]:
    """Build data matrices for 2SLS estimation.

    Returns:
        y: person outcomes (n,)
        X: own characteristics (n, k) — [experience, role_level, n_credits]
        X_bar: peer mean characteristics (n, k)
        Y_bar: leave-one-out peer mean outcomes (n,)
        person_ids: corresponding person IDs (n,)
    """
    # Group credits by anime
    anime_credits: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        anime_credits[c.anime_id].append(c)

    # Track person first year for experience
    person_first_year: dict[str, int] = {}
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            if c.person_id not in person_first_year:
                person_first_year[c.person_id] = anime.year
            else:
                person_first_year[c.person_id] = min(
                    person_first_year[c.person_id], anime.year
                )

    # Build observations
    y_list = []
    x_list = []
    x_bar_list = []
    y_bar_list = []
    pid_list = []

    for anime_id, creds in anime_credits.items():
        anime = anime_map.get(anime_id)
        if not anime or not anime.year:
            continue

        # Unique persons in this anime
        persons_in_anime = {}
        for c in creds:
            if c.person_id not in persons_in_anime:
                persons_in_anime[c.person_id] = {
                    "max_role_weight": ROLE_WEIGHTS.get(c.role.value, 1.0),
                    "n_credits": 1,
                }
            else:
                persons_in_anime[c.person_id]["n_credits"] += 1
                w = ROLE_WEIGHTS.get(c.role.value, 1.0)
                if w > persons_in_anime[c.person_id]["max_role_weight"]:
                    persons_in_anime[c.person_id]["max_role_weight"] = w

        if len(persons_in_anime) < 2:
            continue

        # Compute person features
        person_features = {}
        person_outcomes = {}
        for pid, info in persons_in_anime.items():
            experience = anime.year - person_first_year.get(pid, anime.year)
            person_features[pid] = [
                experience,
                info["max_role_weight"],
                info["n_credits"],
            ]
            person_outcomes[pid] = person_scores.get(pid, 0.0)

        # For each person: leave-one-out peer mean
        for pid in persons_in_anime:
            if pid not in person_scores:
                continue

            peers = [p for p in persons_in_anime if p != pid]
            if not peers:
                continue

            # Leave-one-out peer mean outcome
            peer_scores = [person_outcomes.get(p, 0.0) for p in peers]
            y_bar = np.mean(peer_scores) if peer_scores else 0.0

            # Peer mean characteristics
            peer_feats = [person_features[p] for p in peers if p in person_features]
            x_bar = np.mean(peer_feats, axis=0) if peer_feats else np.zeros(3)

            y_list.append(person_outcomes[pid])
            x_list.append(person_features[pid])
            x_bar_list.append(x_bar)
            y_bar_list.append(y_bar)
            pid_list.append(pid)

    if not y_list:
        return np.array([]), np.array([]).reshape(0, 3), np.array([]).reshape(0, 3), np.array([]), []

    return (
        np.array(y_list, dtype=np.float64),
        np.array(x_list, dtype=np.float64),
        np.array(x_bar_list, dtype=np.float64),
        np.array(y_bar_list, dtype=np.float64),
        pid_list,
    )


def _build_instruments(
    person_ids: list[str],
    collaboration_graph,
    person_scores: dict[str, float],
) -> np.ndarray:
    """Build instruments: avg characteristics of distance-2 neighbors.

    Uses direct neighbor set operations instead of BFS for efficiency.
    Caches per unique PID.  Caps sampled distance-1 neighbors to keep
    runtime bounded on dense graphs.

    Args:
        person_ids: person IDs corresponding to observations
        collaboration_graph: NetworkX graph
        person_scores: person_id → score

    Returns:
        Z: instrument matrix (n, 1) — avg distance-2 neighbor scores
    """
    MAX_D1_SAMPLE = 30  # cap distance-1 neighbors sampled per person

    cache: dict[str, float] = {}

    for pid in person_ids:
        if pid in cache:
            continue

        if pid not in collaboration_graph:
            cache[pid] = 0.0
            continue

        try:
            neighbors_1 = set(collaboration_graph.neighbors(pid))
        except (nx.NetworkXError, nx.NodeNotFound):
            cache[pid] = 0.0
            continue

        if not neighbors_1:
            cache[pid] = 0.0
            continue

        # Sample distance-1 neighbors
        sampled = sorted(neighbors_1)[:MAX_D1_SAMPLE]

        # Collect distance-2 neighbor scores via set operations
        d2_total = 0.0
        d2_count = 0
        seen = set()
        for n1 in sampled:
            try:
                for n2 in collaboration_graph.neighbors(n1):
                    if n2 != pid and n2 not in neighbors_1 and n2 not in seen:
                        seen.add(n2)
                        d2_total += person_scores.get(n2, 0.0)
                        d2_count += 1
            except (nx.NetworkXError, nx.NodeNotFound):
                continue

        cache[pid] = (d2_total / d2_count) if d2_count else 0.0

    return np.array([[cache.get(pid, 0.0)] for pid in person_ids], dtype=np.float64)


def estimate_peer_effects_2sls(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float],
    collaboration_graph,
) -> PeerEffectResult:
    """Estimate peer effects using 2SLS.

    Uses distance-2 neighbor characteristics as instruments for
    peer outcomes to solve Manski's Reflection Problem.

    Args:
        credits: all credits
        anime_map: anime_id → Anime
        person_scores: person_id → score
        collaboration_graph: NetworkX collaboration graph

    Returns:
        PeerEffectResult with estimated effects
    """
    empty_result = PeerEffectResult(
        endogenous_effect=0.0,
        exogenous_effect=0.0,
        own_effect=0.0,
        first_stage_f_stat=0.0,
        n_observations=0,
        person_peer_boost={},
        identified=False,
    )

    # Build peer data
    y, X, X_bar, Y_bar, pid_list = _build_peer_data(
        credits, anime_map, person_scores, collaboration_graph
    )

    if len(y) < 20:
        logger.warning("peer_effects_too_few_obs", n=len(y))
        return empty_result

    # Subsample for instrument computation on large datasets
    MAX_OBS_FOR_2SLS = 10_000
    full_pid_list = pid_list
    full_Y_bar = Y_bar
    if len(y) > MAX_OBS_FOR_2SLS:
        rng = np.random.default_rng(42)
        idx = rng.choice(len(y), MAX_OBS_FOR_2SLS, replace=False)
        idx.sort()
        y = y[idx]
        X = X[idx]
        X_bar = X_bar[idx]
        Y_bar = Y_bar[idx]
        pid_list = [pid_list[i] for i in idx]
        logger.info("peer_effects_subsampled", original=len(full_pid_list), sampled=MAX_OBS_FOR_2SLS)

    # Build instruments
    Z = _build_instruments(pid_list, collaboration_graph, person_scores)

    n = len(y)
    k_x = X.shape[1]

    # ---- First stage: Y_bar = Z π + X δ + error ----
    W_first = np.column_stack([np.ones(n), Z, X, X_bar])
    try:
        pi_hat, residuals_1, _, _ = np.linalg.lstsq(W_first, Y_bar, rcond=None)
    except np.linalg.LinAlgError:
        logger.warning("peer_effects_first_stage_failed")
        return empty_result

    Y_bar_hat = W_first @ pi_hat

    # F-statistic for instrument relevance
    # Compare restricted (without Z) vs unrestricted
    W_restricted = np.column_stack([np.ones(n), X, X_bar])
    try:
        pi_r, _, _, _ = np.linalg.lstsq(W_restricted, Y_bar, rcond=None)
        Y_bar_hat_r = W_restricted @ pi_r
        ss_r = np.sum((Y_bar - Y_bar_hat_r) ** 2)
        ss_u = np.sum((Y_bar - Y_bar_hat) ** 2)
        q = Z.shape[1]  # number of instruments
        k_full = W_first.shape[1]
        f_stat = ((ss_r - ss_u) / q) / (ss_u / max(n - k_full, 1))
    except (np.linalg.LinAlgError, ZeroDivisionError):
        f_stat = 0.0

    identified = f_stat >= 10.0

    # ---- Second stage: y = X c + X_bar a + Y_bar_hat b + error ----
    W_second = np.column_stack([np.ones(n), X, X_bar, Y_bar_hat])
    try:
        coefs, _, _, _ = np.linalg.lstsq(W_second, y, rcond=None)
    except np.linalg.LinAlgError:
        logger.warning("peer_effects_second_stage_failed")
        return empty_result

    # Extract coefficients
    # coefs: [intercept, c1, c2, c3, a1, a2, a3, b]
    own_effect = float(np.mean(coefs[1:1 + k_x]))  # avg own effect
    exogenous_effect = float(np.mean(coefs[1 + k_x:1 + 2 * k_x]))
    endogenous_effect = float(coefs[-1])

    if not identified:
        # Fall back to OLS estimate
        logger.warning(
            "peer_effects_weak_instruments",
            f_stat=round(f_stat, 2),
            msg="Using OLS instead of 2SLS",
        )
        W_ols = np.column_stack([np.ones(n), X, X_bar, Y_bar])
        try:
            coefs_ols, _, _, _ = np.linalg.lstsq(W_ols, y, rcond=None)
            endogenous_effect = float(coefs_ols[-1])
            own_effect = float(np.mean(coefs_ols[1:1 + k_x]))
            exogenous_effect = float(np.mean(coefs_ols[1 + k_x:1 + 2 * k_x]))
        except np.linalg.LinAlgError:
            pass

    # Per-person peer boost — use full (non-subsampled) data
    person_peer_scores: dict[str, list[float]] = defaultdict(list)
    for i, pid in enumerate(full_pid_list):
        person_peer_scores[pid].append(full_Y_bar[i])

    person_peer_boost = {
        pid: endogenous_effect * np.mean(scores)
        for pid, scores in person_peer_scores.items()
    }

    logger.info(
        "peer_effects_estimated",
        n_obs=n,
        endogenous=round(endogenous_effect, 4),
        exogenous=round(exogenous_effect, 4),
        f_stat=round(f_stat, 2),
        identified=identified,
    )

    return PeerEffectResult(
        endogenous_effect=endogenous_effect,
        exogenous_effect=exogenous_effect,
        own_effect=own_effect,
        first_stage_f_stat=f_stat,
        n_observations=n,
        person_peer_boost=person_peer_boost,
        identified=identified,
    )
