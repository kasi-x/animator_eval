"""BiRank — bipartite PageRank for person-anime networks.

Replaces unidirectional PageRank with a bipartite formulation that
jointly ranks persons and anime works, propagating prestige in both
directions through the person-anime bipartite graph.
"""

from dataclasses import dataclass

import numpy as np
import structlog

logger = structlog.get_logger()


@dataclass
class BiRankResult:
    """Result of BiRank computation.

    Attributes:
        person_scores: person_id → BiRank score
        anime_scores: anime_id → BiRank score
        iterations: number of iterations to converge
        converged: whether the algorithm converged within max_iter
    """

    person_scores: dict[str, float]
    anime_scores: dict[str, float]
    iterations: int
    converged: bool


def compute_birank(
    person_anime_graph,
    alpha: float = 0.85,
    beta: float = 0.85,
    max_iter: int = 100,
    tol: float = 1e-6,
    query_vector: dict[str, float] | None = None,
) -> BiRankResult:
    """Compute BiRank scores for persons and anime in a bipartite graph.

    Args:
        person_anime_graph: NetworkX graph with node attribute type="person"|"anime"
        alpha: person damping factor (0-1)
        beta: anime damping factor (0-1)
        max_iter: maximum iterations
        tol: convergence tolerance
        query_vector: optional initial person scores (for personalized ranking)

    Returns:
        BiRankResult with person and anime scores
    """
    # Separate persons and anime nodes
    person_ids = []
    anime_ids = []
    for node, data in person_anime_graph.nodes(data=True):
        if data.get("type") == "person":
            person_ids.append(node)
        elif data.get("type") == "anime":
            anime_ids.append(node)

    n_persons = len(person_ids)
    n_anime = len(anime_ids)

    if n_persons == 0 or n_anime == 0:
        logger.warning("birank_empty_graph", persons=n_persons, anime=n_anime)
        return BiRankResult({}, {}, 0, True)

    # Build index maps
    person_idx = {pid: i for i, pid in enumerate(person_ids)}
    anime_idx = {aid: i for i, aid in enumerate(anime_ids)}

    # Build sparse transition matrix (person → anime weights)
    # W[i, j] = weight of edge from person_i to anime_j
    from scipy.sparse import csr_matrix

    rows, cols, vals = [], [], []
    for pid in person_ids:
        for _, aid, data in person_anime_graph.out_edges(pid, data=True):
            if aid in anime_idx:
                rows.append(person_idx[pid])
                cols.append(anime_idx[aid])
                vals.append(data.get("weight", 1.0))

    if not vals:
        logger.warning("birank_no_edges")
        return BiRankResult(
            {pid: 0.0 for pid in person_ids},
            {aid: 0.0 for aid in anime_ids},
            0,
            True,
        )

    W = csr_matrix(
        (np.array(vals, dtype=np.float64), (np.array(rows), np.array(cols))),
        shape=(n_persons, n_anime),
    )

    # Row-normalize: S = D_p^{-1} W  (person → anime transition)
    row_sums = np.array(W.sum(axis=1)).flatten()
    row_sums[row_sums == 0] = 1.0
    D_p_inv = csr_matrix(
        (1.0 / row_sums, (np.arange(n_persons), np.arange(n_persons))),
        shape=(n_persons, n_persons),
    )
    S = D_p_inv @ W

    # Column-normalize: T = W D_a^{-1}  (anime → person transition)
    col_sums = np.array(W.sum(axis=0)).flatten()
    col_sums[col_sums == 0] = 1.0
    D_a_inv = csr_matrix(
        (1.0 / col_sums, (np.arange(n_anime), np.arange(n_anime))),
        shape=(n_anime, n_anime),
    )
    T = W @ D_a_inv

    # Initial vectors
    if query_vector:
        p = np.array(
            [query_vector.get(pid, 1.0 / n_persons) for pid in person_ids],
            dtype=np.float64,
        )
        p_sum = p.sum()
        if p_sum > 0:
            p /= p_sum
    else:
        p = np.ones(n_persons, dtype=np.float64) / n_persons

    u = np.ones(n_anime, dtype=np.float64) / n_anime

    p_0 = p.copy()
    u_0 = u.copy()

    # Iterative BiRank
    converged = False
    iteration = 0
    for iteration in range(1, max_iter + 1):
        # Update person scores: p = α · T · u + (1-α) · p_0
        p_new = alpha * (T @ u) + (1 - alpha) * p_0
        # Update anime scores: u = β · S^T · p + (1-β) · u_0
        u_new = beta * (S.T @ p_new) + (1 - beta) * u_0

        # Normalize
        p_sum = p_new.sum()
        if p_sum > 0:
            p_new /= p_sum
        u_sum = u_new.sum()
        if u_sum > 0:
            u_new /= u_sum

        # Check convergence
        p_diff = np.max(np.abs(p_new - p))
        u_diff = np.max(np.abs(u_new - u))

        p = p_new
        u = u_new

        if max(p_diff, u_diff) < tol:
            converged = True
            break

    # Build result dicts
    person_scores = {pid: float(p[i]) for i, pid in enumerate(person_ids)}
    anime_scores = {aid: float(u[i]) for i, aid in enumerate(anime_ids)}

    logger.info(
        "birank_complete",
        persons=n_persons,
        anime=n_anime,
        iterations=iteration,
        converged=converged,
    )

    return BiRankResult(person_scores, anime_scores, iteration, converged)
