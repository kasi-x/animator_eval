"""Career trajectory typology via Optimal Matching + Ward hierarchical clustering.

Extracts canonical career trajectory types from person-level annual primary-role
sequences using:
  1. Sequence construction (annual primary role per person)
  2. Optimal Matching (OM) distance matrix (substitution cost from CAREER_STAGE)
  3. Ward hierarchical clustering (scipy)
  4. Silhouette evaluation (k=3..7) with stop-if guard (silhouette < 0.2)
  5. Markov transition matrix per cluster

Role sequences use CAREER_STAGE numeric ordering from role_groups.py.
No viewer ratings, no subjective framing.  All data from Resolved/Conformed credits.

Reference:
  Abbott & Forrest (1986) Optimal Matching Methods for Historical Sequences.
  Journal of Interdisciplinary History 16(3), 471-494.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import structlog

from src.utils.role_groups import CAREER_STAGE_BY_VALUE

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Minimum sequence length for a person to be included in analysis
MIN_SEQ_LENGTH: int = 3

#: Silhouette threshold below which typology is declared absent
SILHOUETTE_THRESHOLD: float = 0.2

#: Range of k values to evaluate
K_RANGE: tuple[int, int] = (3, 7)

#: Indel cost for Optimal Matching (relative to substitution cost scale)
OM_INDEL_COST: float = 1.0

#: Structural role names for the animation-direction pipeline
ANIMATION_PIPELINE_ROLES: list[str] = [
    "in_between",
    "second_key_animator",
    "key_animator",
    "layout",
    "animation_director",
    "episode_director",
    "director",
]

# Stage 0 roles are non-production and excluded from sequences
_NON_PRODUCTION_STAGE: int = 0


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class RoleSequence:
    """Annual primary-role sequence for a single person.

    Each entry in ``stages`` is the CAREER_STAGE integer for that year's
    primary role (highest-stage role held in that year).
    ``roles`` stores the corresponding role strings for labelling.
    """

    person_id: str
    debut_year: int
    final_year: int
    stages: list[int]
    roles: list[str]

    @property
    def length(self) -> int:
        """Number of years in the sequence."""
        return len(self.stages)


@dataclass
class TrajectoryCluster:
    """One canonical trajectory type produced by Ward clustering.

    Attributes:
        cluster_id: Integer cluster index (0-based).
        label: Structural description derived from sequence shape.
            e.g. ``"early-specialist-stable"``, ``"late-broad-mobility"``.
            NEVER evaluative framing.
        person_ids: Members of this cluster.
        medoid_person_id: Representative member (closest to cluster centroid
            in OM distance space).
        typical_stages: Stage sequence of the medoid.
        typical_roles: Role sequence of the medoid.
        transition_matrix: Markov transition probability matrix
            (rows = from-stage, cols = to-stage, values in [0,1]).
        stage_labels: Ordered stage labels corresponding to matrix axes.
        n: Number of members.
    """

    cluster_id: int
    label: str
    person_ids: list[str]
    medoid_person_id: str
    typical_stages: list[int]
    typical_roles: list[str]
    transition_matrix: list[list[float]] = field(default_factory=list)
    stage_labels: list[str] = field(default_factory=list)
    n: int = 0

    def __post_init__(self) -> None:
        if self.n == 0:
            self.n = len(self.person_ids)


@dataclass
class TypologyResult:
    """Full output of ``compute_trajectory_typology``.

    Attributes:
        clusters: Canonical trajectory types (empty if stop-if triggered).
        best_k: Number of clusters chosen.
        silhouette_scores: Silhouette score per k evaluated.
        best_silhouette: Silhouette score for best_k (None if stop-if).
        n_sequences: Total sequences analysed.
        stop_if_triggered: True when all k silhouettes < SILHOUETTE_THRESHOLD.
        stop_if_reason: Human-readable stop-if explanation.
    """

    clusters: list[TrajectoryCluster]
    best_k: int | None
    silhouette_scores: dict[int, float]
    best_silhouette: float | None
    n_sequences: int
    stop_if_triggered: bool = False
    stop_if_reason: str = ""


# ---------------------------------------------------------------------------
# Step 1: Build annual role sequences from credits
# ---------------------------------------------------------------------------


def build_role_sequences(
    conn: Any,
    *,
    min_year: int = 1975,
    max_year: int = 2025,
    min_seq_length: int = MIN_SEQ_LENGTH,
) -> list[RoleSequence]:
    """Build per-person annual primary-role sequences from credits.

    Primary role per year = highest CAREER_STAGE role held in that year.
    Non-production roles (stage 0) are excluded from sequences; a year with
    only stage-0 credits is treated as a gap (no entry for that year).

    Gaps (years without production credits) are not interpolated; the
    sequence covers only years with observed production credits.

    Args:
        conn: DuckDB or SQLite connection to Resolved/Conformed credits.
        min_year: Earliest credit year included.
        max_year: Latest credit year included.
        min_seq_length: Minimum sequence length; shorter are excluded.

    Returns:
        List of RoleSequence objects.
    """
    sql = """
        SELECT person_id, credit_year, role
        FROM credits
        WHERE credit_year IS NOT NULL
          AND credit_year >= ?
          AND credit_year <= ?
          AND role IS NOT NULL
        ORDER BY person_id, credit_year
    """
    try:
        rows = conn.execute(sql, (min_year, max_year)).fetchall()
    except Exception:
        # Try conformed schema (DuckDB)
        sql_duckdb = """
            SELECT person_id, credit_year, role
            FROM conformed.credits
            WHERE credit_year IS NOT NULL
              AND credit_year >= ?
              AND credit_year <= ?
              AND role IS NOT NULL
            ORDER BY person_id, credit_year
        """
        rows = conn.execute(sql_duckdb, (min_year, max_year)).fetchall()

    # Group credits by (person_id, credit_year) and pick highest stage
    person_year_stage: dict[str, dict[int, tuple[int, str]]] = {}
    for person_id, credit_year, role in rows:
        stage = CAREER_STAGE_BY_VALUE.get(str(role), 0)
        if stage == _NON_PRODUCTION_STAGE:
            continue
        if person_id not in person_year_stage:
            person_year_stage[person_id] = {}
        prev = person_year_stage[person_id].get(credit_year)
        if prev is None or stage > prev[0]:
            person_year_stage[person_id][credit_year] = (stage, str(role))

    sequences: list[RoleSequence] = []
    for person_id, year_map in person_year_stage.items():
        sorted_years = sorted(year_map.keys())
        if len(sorted_years) < min_seq_length:
            continue
        stages = [year_map[y][0] for y in sorted_years]
        roles = [year_map[y][1] for y in sorted_years]
        sequences.append(
            RoleSequence(
                person_id=person_id,
                debut_year=sorted_years[0],
                final_year=sorted_years[-1],
                stages=stages,
                roles=roles,
            )
        )

    log.info(
        "sequences_built",
        n_persons=len(person_year_stage),
        n_sequences=len(sequences),
        min_seq_length=min_seq_length,
    )
    return sequences


# ---------------------------------------------------------------------------
# Step 2: Optimal Matching distance matrix
# ---------------------------------------------------------------------------


def _substitution_cost(stage_a: int, stage_b: int) -> float:
    """Substitution cost = absolute difference in CAREER_STAGE values.

    Stages further apart cost more to substitute, reflecting structural
    distance in the production hierarchy.
    """
    return float(abs(stage_a - stage_b))


def compute_om_distance_matrix(
    sequences: list[RoleSequence],
    *,
    indel_cost: float = OM_INDEL_COST,
) -> np.ndarray:
    """Compute pairwise Optimal Matching distance matrix.

    Uses Needleman-Wunsch style dynamic programming with:
    - substitution cost = |stage_a - stage_b| (structural distance)
    - indel cost = constant (gap open + extend, symmetric)

    Returns square symmetric float64 matrix of shape (n, n).
    """
    n = len(sequences)
    dist = np.zeros((n, n), dtype=np.float64)

    for i in range(n):
        seq_i = sequences[i].stages
        for j in range(i + 1, n):
            seq_j = sequences[j].stages
            d = _om_distance(seq_i, seq_j, indel_cost)
            dist[i, j] = d
            dist[j, i] = d

    log.debug("om_distance_matrix_computed", shape=dist.shape)
    return dist


def _om_distance(
    seq_a: list[int],
    seq_b: list[int],
    indel_cost: float,
) -> float:
    """Compute Optimal Matching distance between two integer stage sequences.

    Implements Needleman-Wunsch alignment with affine-equivalent costs:
    - match/substitute: |stage_a - stage_b|
    - indel (insert or delete): indel_cost

    Returns the minimum edit cost to transform seq_a into seq_b.
    """
    m, n = len(seq_a), len(seq_b)
    # dp[i][j] = min cost to align seq_a[:i] with seq_b[:j]
    dp = np.full((m + 1, n + 1), np.inf, dtype=np.float64)
    dp[0, 0] = 0.0
    for i in range(1, m + 1):
        dp[i, 0] = dp[i - 1, 0] + indel_cost
    for j in range(1, n + 1):
        dp[0, j] = dp[0, j - 1] + indel_cost

    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost_sub = dp[i - 1, j - 1] + _substitution_cost(seq_a[i - 1], seq_b[j - 1])
            cost_del = dp[i - 1, j] + indel_cost
            cost_ins = dp[i, j - 1] + indel_cost
            dp[i, j] = min(cost_sub, cost_del, cost_ins)

    return float(dp[m, n])


# ---------------------------------------------------------------------------
# Step 3: Ward hierarchical clustering + silhouette evaluation
# ---------------------------------------------------------------------------


def _ward_cluster_labels(dist_matrix: np.ndarray, k: int) -> np.ndarray:
    """Run Ward linkage on a precomputed distance matrix and return cluster labels.

    Uses scipy.cluster.hierarchy.linkage with 'ward' method on squareform.
    """
    from scipy.cluster.hierarchy import fcluster, linkage
    from scipy.spatial.distance import squareform

    condensed = squareform(dist_matrix, checks=False)
    Z = linkage(condensed, method="ward")
    labels = fcluster(Z, k, criterion="maxclust")
    return labels - 1  # 0-based


def _silhouette(dist_matrix: np.ndarray, labels: np.ndarray) -> float:
    """Compute mean silhouette coefficient from a precomputed distance matrix."""
    from sklearn.metrics import silhouette_score

    n_unique = len(np.unique(labels))
    if n_unique < 2 or n_unique >= len(labels):
        return -1.0
    return float(silhouette_score(dist_matrix, labels, metric="precomputed"))


def select_best_k(
    dist_matrix: np.ndarray,
    *,
    k_min: int = K_RANGE[0],
    k_max: int = K_RANGE[1],
) -> tuple[dict[int, float], int | None]:
    """Evaluate Ward clustering for k in [k_min, k_max].

    Returns:
        silhouette_by_k: Dict of k → silhouette score.
        best_k: k with highest silhouette, or None if all < SILHOUETTE_THRESHOLD.
    """
    n = dist_matrix.shape[0]
    # Cannot evaluate if fewer data points than k_max
    k_max_actual = min(k_max, n - 1)

    silhouette_by_k: dict[int, float] = {}
    for k in range(k_min, k_max_actual + 1):
        if k >= n:
            break
        labels = _ward_cluster_labels(dist_matrix, k)
        sil = _silhouette(dist_matrix, labels)
        silhouette_by_k[k] = sil
        log.debug("silhouette_evaluated", k=k, silhouette=round(sil, 4))

    if not silhouette_by_k:
        return {}, None

    best_k = max(silhouette_by_k, key=silhouette_by_k.__getitem__)
    if silhouette_by_k[best_k] < SILHOUETTE_THRESHOLD:
        log.info(
            "stop_if_triggered",
            best_silhouette=silhouette_by_k[best_k],
            threshold=SILHOUETTE_THRESHOLD,
        )
        return silhouette_by_k, None

    return silhouette_by_k, best_k


# ---------------------------------------------------------------------------
# Step 4: Markov transition matrix per cluster
# ---------------------------------------------------------------------------


def compute_markov_transitions(
    sequences: list[RoleSequence],
    *,
    stage_max: int = 6,
) -> tuple[list[list[float]], list[str]]:
    """Compute Markov transition probability matrix from a set of sequences.

    Transition is year-to-year: stage at year t → stage at year t+1.
    Only consecutive-year pairs (no gap) are counted.

    Args:
        sequences: Subset of sequences for one cluster.
        stage_max: Maximum stage value (inclusive).

    Returns:
        transition_matrix: (stage_max+1) × (stage_max+1) probability matrix.
        stage_labels: Labels for axes.
    """
    stages = list(range(0, stage_max + 1))
    n_stages = len(stages)
    counts = np.zeros((n_stages, n_stages), dtype=float)

    for seq in sequences:
        for t in range(len(seq.stages) - 1):
            # Only count consecutive years (no gap years in between)
            s_from = seq.stages[t]
            s_to = seq.stages[t + 1]
            if 0 <= s_from <= stage_max and 0 <= s_to <= stage_max:
                counts[s_from, s_to] += 1.0

    # Normalize rows to probabilities
    row_sums = counts.sum(axis=1, keepdims=True)
    with np.errstate(invalid="ignore", divide="ignore"):
        probs = np.where(row_sums > 0, counts / row_sums, 0.0)

    stage_labels = [f"stage_{s}" for s in stages]
    return probs.tolist(), stage_labels


# ---------------------------------------------------------------------------
# Step 5: Cluster labelling (structural, not evaluative)
# ---------------------------------------------------------------------------

_LABEL_UNKNOWN = "unclassified"


def _derive_structural_label(typical_stages: list[int]) -> str:
    """Derive a structural label from a typical stage sequence.

    Labels describe the trajectory shape in structural terms only.
    No evaluative framing ("high-performer", "fast-tracker", etc.).

    Heuristics (non-exhaustive):
    - Monotone ascending: "progressive-ascent"
    - Monotone descending: "descending-role-shift"
    - Plateau (no change after early years): "early-specialist-stable"
    - Late rise: "delayed-advancement"
    - High initial then low: "broad-to-focused"
    - Oscillating: "multi-role-alternation"
    """
    if not typical_stages:
        return _LABEL_UNKNOWN

    n = len(typical_stages)
    if n == 1:
        return "single-year-observed"

    first = typical_stages[0]
    last = typical_stages[-1]
    mid_idx = n // 2
    mid_val = typical_stages[mid_idx]

    diffs = [typical_stages[i + 1] - typical_stages[i] for i in range(n - 1)]
    n_pos = sum(1 for d in diffs if d > 0)
    n_neg = sum(1 for d in diffs if d < 0)
    n_zero = sum(1 for d in diffs if d == 0)
    net_change = last - first
    peak = max(typical_stages)
    trough = min(typical_stages)

    # Strictly monotone ascending
    if n_neg == 0 and n_pos > 0:
        return "progressive-ascent"

    # Strictly monotone descending
    if n_pos == 0 and n_neg > 0:
        return "descending-role-shift"

    # Mostly stable (> 70% zero diffs)
    if n_zero / max(len(diffs), 1) >= 0.70:
        return "early-specialist-stable"

    # Net rise with late concentration
    if net_change > 0 and n_pos > n_neg and mid_val <= first + 1:
        return "delayed-advancement"

    # High peak in middle, lower at ends (inverted U)
    if peak == mid_val and last < peak and first < peak:
        return "mid-career-peak-role"

    # Net negative with oscillation
    if net_change < 0 and n_neg > n_pos:
        return "broad-to-focused"

    # Oscillating
    if n_pos >= 1 and n_neg >= 1 and peak - trough >= 2:
        return "multi-role-alternation"

    return _LABEL_UNKNOWN


# ---------------------------------------------------------------------------
# Step 6: Medoid selection
# ---------------------------------------------------------------------------


def _find_medoid(
    indices: list[int],
    dist_matrix: np.ndarray,
) -> int:
    """Return the index (in dist_matrix) of the medoid of the given subset."""
    sub = dist_matrix[np.ix_(indices, indices)]
    row_sums = sub.sum(axis=1)
    return indices[int(np.argmin(row_sums))]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def compute_trajectory_typology(
    conn: Any,
    *,
    min_year: int = 1975,
    max_year: int = 2025,
    min_seq_length: int = MIN_SEQ_LENGTH,
    k_min: int = K_RANGE[0],
    k_max: int = K_RANGE[1],
    indel_cost: float = OM_INDEL_COST,
    rng_seed: int = 42,
) -> TypologyResult:
    """Compute career trajectory typology from credit sequences.

    Pipeline:
      1. Build annual role sequences from credits.
      2. Compute OM distance matrix.
      3. Ward cluster for k=3..7; evaluate silhouette.
      4. If best silhouette < 0.2: return stop-if result.
      5. For best k: build TrajectoryCluster objects with Markov matrices.

    Args:
        conn: Database connection (SQLite or DuckDB conformed schema).
        min_year: Earliest credit year.
        max_year: Latest credit year.
        min_seq_length: Minimum sequence length to include.
        k_min: Minimum cluster count to evaluate.
        k_max: Maximum cluster count to evaluate.
        indel_cost: OM indel (gap) penalty.
        rng_seed: Unused directly; reserved for future stochastic extensions.

    Returns:
        TypologyResult with clusters, silhouette scores, and stop-if flag.
    """
    sequences = build_role_sequences(
        conn,
        min_year=min_year,
        max_year=max_year,
        min_seq_length=min_seq_length,
    )

    n = len(sequences)
    log.info("trajectory_typology_started", n_sequences=n)

    if n < max(k_max, 10):
        return TypologyResult(
            clusters=[],
            best_k=None,
            silhouette_scores={},
            best_silhouette=None,
            n_sequences=n,
            stop_if_triggered=True,
            stop_if_reason=(
                f"Insufficient sequences for clustering: {n} < {max(k_max, 10)}. "
                "At least 10 sequences required."
            ),
        )

    dist_matrix = compute_om_distance_matrix(sequences, indel_cost=indel_cost)

    silhouette_by_k, best_k = select_best_k(
        dist_matrix, k_min=k_min, k_max=k_max
    )

    if best_k is None:
        best_sil = max(silhouette_by_k.values()) if silhouette_by_k else None
        return TypologyResult(
            clusters=[],
            best_k=None,
            silhouette_scores=silhouette_by_k,
            best_silhouette=best_sil,
            n_sequences=n,
            stop_if_triggered=True,
            stop_if_reason=(
                f"All silhouette scores below threshold {SILHOUETTE_THRESHOLD}. "
                f"Best={best_sil:.4f}. Typology structure absent in this dataset."
            ),
        )

    best_labels = _ward_cluster_labels(dist_matrix, best_k)
    clusters = _build_clusters(sequences, best_labels, dist_matrix)

    log.info(
        "trajectory_typology_completed",
        n_clusters=len(clusters),
        best_k=best_k,
        best_silhouette=round(silhouette_by_k[best_k], 4),
    )

    return TypologyResult(
        clusters=clusters,
        best_k=best_k,
        silhouette_scores=silhouette_by_k,
        best_silhouette=silhouette_by_k[best_k],
        n_sequences=n,
        stop_if_triggered=False,
    )


def _build_clusters(
    sequences: list[RoleSequence],
    labels: np.ndarray,
    dist_matrix: np.ndarray,
) -> list[TrajectoryCluster]:
    """Construct TrajectoryCluster objects from cluster label assignments."""
    from collections import defaultdict

    cluster_indices: dict[int, list[int]] = defaultdict(list)
    for idx, lbl in enumerate(labels):
        cluster_indices[int(lbl)].append(idx)

    clusters: list[TrajectoryCluster] = []
    for cid in sorted(cluster_indices.keys()):
        idxs = cluster_indices[cid]
        medoid_idx = _find_medoid(idxs, dist_matrix)
        medoid_seq = sequences[medoid_idx]

        member_ids = [sequences[i].person_id for i in idxs]
        member_seqs = [sequences[i] for i in idxs]

        trans_matrix, stage_labels = compute_markov_transitions(member_seqs)

        label = _derive_structural_label(medoid_seq.stages)

        clusters.append(
            TrajectoryCluster(
                cluster_id=cid,
                label=label,
                person_ids=member_ids,
                medoid_person_id=medoid_seq.person_id,
                typical_stages=list(medoid_seq.stages),
                typical_roles=list(medoid_seq.roles),
                transition_matrix=trans_matrix,
                stage_labels=stage_labels,
                n=len(member_ids),
            )
        )

    return clusters
