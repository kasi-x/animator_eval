"""Individual Contribution Profile — 個人貢献指標の算出.

ネットワーク指標（Authority/Trust/Skill）とは別の測定器として、
機会を統制した上での個人の独自の貢献を定量化する。

指標:
1. peer_percentile: 同役職×同キャリア年数コホート内の順位
2. opportunity_residual: 機会要因を統制した後の残差（個人の独自貢献）
3. consistency: 作品間の貢献安定性
4. independent_value: コラボレーターへの波及効果
"""

import bisect
from collections import defaultdict
from dataclasses import asdict, dataclass

import networkx as nx
import numpy as np
import structlog

from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()

# width of each career-year band
CAREER_BAND_WIDTH = 5
# minimum cohort size (merge or null if below)
MIN_COHORT_SIZE = 5
# minimum works required to compute a consistency score
MIN_WORKS_FOR_CONSISTENCY = 5
# minimum collaborators required to compute independent contribution
MIN_COLLABORATORS = 3


@dataclass
class IndividualProfile:
    """Individual contribution profile."""

    person_id: str
    peer_percentile: float | None = None
    peer_cohort: dict | None = None
    opportunity_residual: float | None = None
    consistency: float | None = None
    independent_value: float | None = None


@dataclass
class IndividualContributionResult:
    """Overall result."""

    profiles: dict  # person_id -> IndividualProfile (as dict)
    model_r_squared: float | None = None
    total_persons: int = 0
    cohort_count: int = 0


def _get_career_band(years: int) -> str:
    """Convert career years to a band label."""
    lower = (years // CAREER_BAND_WIDTH) * CAREER_BAND_WIDTH
    upper = lower + CAREER_BAND_WIDTH - 1
    return f"{lower}-{upper}y"


def _build_person_features(
    persons_with_scores: list[dict],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    role_profiles: dict[str, dict],
    career_data: dict[str, dict],
) -> dict[str, dict]:
    """Build feature vectors for each person."""
    # person → クレジット一覧を事前構築
    person_credits: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        person_credits[c.person_id].append(c)

    # anime → staff count (precompute to avoid O(n²))
    anime_staff_counts: dict[str, int] = defaultdict(int)
    _seen_pa: set[tuple[str, str]] = set()
    for c in credits:
        key = (c.person_id, c.anime_id)
        if key not in _seen_pa:
            _seen_pa.add(key)
            anime_staff_counts[c.anime_id] += 1

    # person → スタジオ一覧
    person_studios: dict[str, list[str]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.studios:
            person_studios[c.person_id].extend(anime.studios)

    features = {}
    for r in persons_with_scores:
        pid = r["person_id"]
        iv_score = r.get("iv_score", 0)

        # role
        rp = role_profiles.get(pid, {})
        primary_role = rp.get("primary_role", "unknown")

        # キャリア年数 (career_data values may be CareerSnapshot or dict)
        cd = career_data.get(pid)
        if cd is None:
            career_years = 0
        elif isinstance(cd, dict):
            career_years = cd.get("active_years", 0)
        else:
            career_years = getattr(cd, "active_years", 0)
        if career_years == 0:
            # credits から計算
            pc = person_credits.get(pid, [])
            years = set()
            for c in pc:
                anime = anime_map.get(c.anime_id)
                if anime and anime.year:
                    years.add(anime.year)
            if years:
                career_years = max(years) - min(years) + 1

        # average staff count of participated works (opportunity indicator — anime.score not used)
        pc = person_credits.get(pid, [])
        staff_counts_list = []
        seen_anime = set()
        for c in pc:
            if c.anime_id in seen_anime:
                continue
            seen_anime.add(c.anime_id)
            staff_counts_list.append(anime_staff_counts.get(c.anime_id, 1))
        avg_staff_count = np.mean(staff_counts_list) if staff_counts_list else 0

        # studio scale (studio frequency)
        studios = person_studios.get(pid, [])
        # D20: unique_studios is an opportunity proxy. More studios = more diverse
        # experience. It's deliberately simple (count, not quality). In the OLS
        # residual, it controls for "breadth of opportunity" so the residual
        # reflects individual quality net of opportunity.
        unique_studios = len(set(studios))

        features[pid] = {
            "iv_score": iv_score,
            "primary_role": primary_role,
            "career_years": career_years,
            "career_band": _get_career_band(career_years),
            "avg_staff_count": avg_staff_count,
            "unique_studios": unique_studios,
            "work_count": len(seen_anime),
            "credit_count": len(pc),
        }

    return features


def compute_peer_percentile(
    features: dict[str, dict],
    community_map: dict[str, int] | None = None,
) -> dict[str, dict]:
    """Compute within-cohort percentiles.

    D14 known limitation: peer_percentile is a rank transform of iv_score within
    role × career_band cohorts. It is NOT an independent evaluation axis — it's
    a contextual reinterpretation of the same IV score. Its value is in answering
    "how does this person compare to peers in similar roles?" rather than providing
    new information. Consumers should not treat it as independent of iv_score.

    Args:
        features: person_id → 特徴量辞書
        community_map: person_id → community_id（クラスタベースのパーセンタイル追加用）

    Returns:
        person_id → {peer_percentile, peer_cohort, cluster_percentile?, cluster_id?, cluster_size?}
    """
    # form cohorts: role × career_band
    cohorts: dict[tuple[str, str], list[tuple[str, float]]] = defaultdict(list)
    for pid, f in features.items():
        key = (f["primary_role"], f["career_band"])
        cohorts[key].append((pid, f["iv_score"]))

    # Identify roles that need full merge (any band in that role is < MIN_COHORT_SIZE)
    role_needs_merge: set[str] = set()
    for (role, band), members in cohorts.items():
        if len(members) < MIN_COHORT_SIZE:
            role_needs_merge.add(role)

    # Pre-compute merged cohorts for roles that need it
    merged_cohorts: dict[str, list[tuple[str, float]]] = {}
    for role in role_needs_merge:
        merged = []
        for (r2, b2), m2 in cohorts.items():
            if r2 == role:
                merged.extend(m2)
        merged_cohorts[role] = merged

    result = {}
    seen: set[str] = set()  # Prevent duplicate processing

    for (role, band), members in cohorts.items():
        if role in role_needs_merge:
            # Skip if we already processed this role's merged cohort
            if role in seen:
                continue
            seen.add(role)

            merged = merged_cohorts[role]
            if len(merged) < MIN_COHORT_SIZE:
                for pid, _ in merged:
                    result[pid] = {"peer_percentile": None, "peer_cohort": None}
                continue
            cohort_members = merged
            cohort_label = {
                "role": role,
                "career_band": "all",
                "cohort_size": len(merged),
            }
        else:
            cohort_members = members
            cohort_label = {
                "role": role,
                "career_band": band,
                "cohort_size": len(members),
            }

        # compute percentile (bisect for O(n log n) instead of O(n²))
        scores = sorted([s for _, s in cohort_members])
        n_members = len(scores)
        for pid, score in cohort_members:
            # percentile: fraction of persons ranked below this person
            rank = bisect.bisect_right(scores, score)
            percentile = round(rank / n_members * 100, 1)
            result[pid] = {
                "peer_percentile": percentile,
                "peer_cohort": cohort_label,
            }

    # Cluster-based percentile (if community_map provided)
    if community_map:
        cluster_cohorts: dict[int, list[tuple[str, float]]] = defaultdict(list)
        for pid, f in features.items():
            cid = community_map.get(pid)
            if cid is not None:
                cluster_cohorts[cid].append((pid, f["iv_score"]))

        for cid, members in cluster_cohorts.items():
            if len(members) < MIN_COHORT_SIZE:
                continue
            scores = sorted([s for _, s in members])
            n_members = len(scores)
            for pid, score in members:
                if pid in result:
                    rank = bisect.bisect_right(scores, score)
                    result[pid]["cluster_percentile"] = round(rank / n_members * 100, 1)
                    result[pid]["cluster_id"] = cid
                    result[pid]["cluster_size"] = n_members

    logger.info("peer_percentile_computed", persons=len(result), cohorts=len(cohorts))
    return result


def compute_opportunity_residual(
    features: dict[str, dict],
) -> tuple[dict[str, float], float]:
    """Compute opportunity-controlled residuals.

    OLS: composite ~ career_years + avg_staff_count + unique_studios + role_dummies

    Args:
        features: person_id → 特徴量辞書

    Returns:
        (person_id → z-score残差, R²)
    """
    pids = list(features.keys())
    if len(pids) < 10:
        logger.warning("insufficient_data_for_regression", persons=len(pids))
        return {pid: None for pid in pids}, None

    # dummy-encode roles
    roles = sorted({f["primary_role"] for f in features.values()})
    role_to_idx = {r: i for i, r in enumerate(roles)}

    y = np.array([features[pid]["iv_score"] for pid in pids])

    # feature matrix: [career_years, avg_staff_count, unique_studios, role_dummies...]
    n = len(pids)
    n_roles = max(len(roles) - 1, 0)  # Fix B06: 1ロール時は0列（零列を避ける）
    X = np.zeros((n, 3 + n_roles))

    for i, pid in enumerate(pids):
        f = features[pid]
        X[i, 0] = f["career_years"]
        X[i, 1] = f["avg_staff_count"]
        X[i, 2] = f["unique_studios"]
        ridx = role_to_idx.get(f["primary_role"], 0)
        if ridx > 0 and ridx <= n_roles:
            X[i, 2 + ridx] = 1.0

    # add intercept
    X = np.column_stack([np.ones(n), X])

    # OLS: β = (X'X)^-1 X'y with leverage correction (studentized residuals)
    try:
        XtX = X.T @ X
        # regularisation (guard against singular matrix)
        XtX += np.eye(XtX.shape[0]) * 1e-8
        XtX_inv = np.linalg.inv(XtX)
        beta = XtX_inv @ (X.T @ y)
        y_hat = X @ beta
        residuals = y - y_hat

        # R²
        ss_res = np.sum(residuals**2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Studentized residuals with leverage correction
        p = X.shape[1]  # number of predictors
        H_diag = np.sum((X @ XtX_inv) * X, axis=1)  # hat matrix diagonal
        s = np.sqrt(ss_res / max(n - p, 1))
        z_residuals = np.zeros(n)
        for i in range(n):
            denom = s * np.sqrt(max(1.0 - H_diag[i], 1e-10))
            z_residuals[i] = residuals[i] / denom if denom > 0 else 0.0

        result = {pids[i]: round(float(z_residuals[i]), 3) for i in range(n)}

        logger.info(
            "opportunity_residual_computed",
            persons=n,
            r_squared=round(r_squared, 3),
            roles=len(roles),
        )
        return result, round(r_squared, 3)

    except np.linalg.LinAlgError:
        logger.warning("regression_failed")
        return {pid: None for pid in pids}, None


def compute_consistency(
    features: dict[str, dict],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    akm_residuals: dict[tuple[str, str], float] | None = None,
) -> dict[str, float | None]:
    """Compute consistency scores across works.

    AKM残差が利用可能な場合はそれを使用し、スタジオや年次効果を除いた
    個人の純粋な貢献度の安定性を計測する。残差がない場合はフォールバック
    として作品スコアを使用する。

    Args:
        features: person_id → 特徴量辞書
        credits: クレジットリスト
        anime_map: anime_id → Anime
        akm_residuals: (person_id, anime_id) → AKM残差

    Returns:
        person_id → consistency (0-1, 1=完全に安定)
    """
    # person → AKM残差リスト（スタジオ・年次効果を除いた個人の貢献安定性）
    person_work_values: dict[str, list[float]] = defaultdict(list)
    person_seen_anime: dict[str, set] = defaultdict(set)

    for c in credits:
        pid = c.person_id
        if pid not in features:
            continue
        if c.anime_id in person_seen_anime[pid]:
            continue
        person_seen_anime[pid].add(c.anime_id)
        anime = anime_map.get(c.anime_id)
        if not anime:
            continue

        # Use AKM residuals only (no anime.score fallback)
        if akm_residuals:
            resid = akm_residuals.get((pid, c.anime_id))
            if resid is not None:
                person_work_values[pid].append(resid)

    # Compute population reference scale for consistent normalization
    all_values = [v for vals in person_work_values.values() for v in vals]
    ref_scale = float(np.std(all_values)) if all_values else 1.0
    ref_scale = max(ref_scale, 0.01)  # prevent division by zero

    result = {}
    for pid in features:
        values = person_work_values.get(pid, [])
        if len(values) < MIN_WORKS_FOR_CONSISTENCY:
            result[pid] = None
            continue

        std = float(np.std(values))

        # Unified formula: consistency = max(0, 1 - std/ref_scale)
        # This normalizes by population std so the metric is always 0-1
        consistency = max(0.0, 1.0 - std / ref_scale)

        result[pid] = round(consistency, 3)

    computed = sum(1 for v in result.values() if v is not None)
    logger.info(
        "consistency_computed", persons=computed, skipped=len(result) - computed
    )
    return result


def _get_iv_numba_kernel():
    """Lazy-compile the Numba JIT kernel for independent value computation.

    Returns the compiled function. Compilation happens once on first call (~2s).
    Subsequent calls return the cached version instantly.
    """
    from numba import njit, prange

    @njit(parallel=True, cache=True)
    def _iv_kernel(
        P_indptr,
        P_indices,
        CC_indptr,
        CC_indices,
        iv_vec,
        anime_iv_sum,
        anime_count,
        n_collabs,
        min_collab,
        n_persons,
    ):
        """Numba-compiled independent value computation over CSR arrays.

        For each person X and each collaborator C:
          - Partition C's anime into shared (with X) and only_c (without X)
          - Compute mean residual in each partition
          - diff = mean_with - mean_without
        Result: mean of diffs across all collaborators.

        Returns (values, valid) arrays — valid[i]=True if person i has a result.
        """
        values = np.empty(n_persons, dtype=np.float64)
        valid = np.zeros(n_persons, dtype=np.bool_)

        for x_idx in prange(n_persons):
            if n_collabs[x_idx] < min_collab:
                continue

            pid_iv = iv_vec[x_idx]
            # X's anime indices (sorted by CSR construction)
            x_start = P_indptr[x_idx]
            x_end = P_indptr[x_idx + 1]
            x_n_anime = x_end - x_start

            # Build a lookup array for X's anime: mark anime IDs present in X
            # Use sorted array + binary search instead of hash set (numba-friendly)
            x_anime = P_indices[x_start:x_end]  # already sorted in CSR

            # Collaborators of X
            cc_start = CC_indptr[x_idx]
            cc_end = CC_indptr[x_idx + 1]
            n_collabs_x = cc_end - cc_start

            # Pre-allocate diffs array
            diffs = np.empty(n_collabs_x, dtype=np.float64)
            n_diffs = 0

            for cc_i in range(cc_start, cc_end):
                c_idx = CC_indices[cc_i]
                collab_iv = iv_vec[c_idx]

                # C's anime
                c_start = P_indptr[c_idx]
                c_end = P_indptr[c_idx + 1]
                c_n = c_end - c_start
                if c_n == 0:
                    continue

                # Partition C's anime into shared/only_c using binary search on x_anime
                # Accumulate residual sums directly (no intermediate arrays)
                sum_resid_wo = 0.0
                count_wo = 0
                sum_resid_w = 0.0
                count_w = 0

                for j in range(c_start, c_end):
                    a_idx = P_indices[j]

                    # Binary search for a_idx in x_anime
                    is_shared = False
                    lo = 0
                    hi = x_n_anime
                    while lo < hi:
                        mid = (lo + hi) >> 1
                        if x_anime[mid] < a_idx:
                            lo = mid + 1
                        elif x_anime[mid] > a_idx:
                            hi = mid
                        else:
                            is_shared = True
                            break

                    a_sum = anime_iv_sum[a_idx]
                    a_cnt = anime_count[a_idx]

                    if is_shared:
                        # With X: quality = (sum - iv[C] - iv[X]) / (count - 2)
                        denom = a_cnt - 2.0
                        if denom > 0:
                            pq = (a_sum - collab_iv - pid_iv) / denom
                        else:
                            pq = 0.0
                        sum_resid_w += collab_iv - pq
                        count_w += 1
                    else:
                        # Without X: quality = (sum - iv[C]) / (count - 1)
                        denom = a_cnt - 1.0
                        if denom > 0:
                            pq = (a_sum - collab_iv) / denom
                        else:
                            pq = 0.0
                        sum_resid_wo += collab_iv - pq
                        count_wo += 1

                if count_w > 0 and count_wo > 0:
                    mean_w = sum_resid_w / count_w
                    mean_wo = sum_resid_wo / count_wo
                    diffs[n_diffs] = mean_w - mean_wo
                    n_diffs += 1

            if n_diffs >= min_collab:
                total = 0.0
                for di in range(n_diffs):
                    total += diffs[di]
                values[x_idx] = total / n_diffs
                valid[x_idx] = True

        return values, valid

    return _iv_kernel


def _compute_iv_numba(
    P,
    collab_count,
    iv_vec,
    anime_iv_sum,
    anime_count,
    n_collabs,
    n_persons,
    feature_pids,
):
    """Compute independent values using Numba JIT-compiled kernel.

    Falls back to pure-Python implementation if Numba is unavailable.
    """
    try:
        kernel = _get_iv_numba_kernel()
    except ImportError:
        logger.warning("numba_unavailable", msg="falling back to Python implementation")
        return _compute_iv_python(
            P,
            collab_count,
            iv_vec,
            anime_iv_sum,
            anime_count,
            n_collabs,
            n_persons,
            feature_pids,
        )

    logger.info("independent_value_numba", n_persons=n_persons)

    values, valid = kernel(
        P.indptr,
        P.indices,
        collab_count.indptr,
        collab_count.indices,
        iv_vec,
        anime_iv_sum,
        anime_count,
        n_collabs,
        MIN_COLLABORATORS,
        n_persons,
    )

    result: dict[str, float | None] = {}
    for i, pid in enumerate(feature_pids):
        if valid[i]:
            result[pid] = round(float(values[i]), 2)
        else:
            result[pid] = None
    return result


def _compute_iv_python(
    P,
    collab_count,
    iv_vec,
    anime_iv_sum,
    anime_count,
    n_collabs,
    n_persons,
    feature_pids,
):
    """Pure-Python fallback for independent value computation (CSR-native)."""
    P_indptr = P.indptr
    P_indices = P.indices
    CC_indptr = collab_count.indptr
    CC_indices = collab_count.indices

    result: dict[str, float | None] = {}
    for x_idx in range(n_persons):
        pid = feature_pids[x_idx]
        if n_collabs[x_idx] < MIN_COLLABORATORS:
            result[pid] = None
            continue

        pid_iv = iv_vec[x_idx]
        x_anime_set = set(P_indices[P_indptr[x_idx] : P_indptr[x_idx + 1]].tolist())
        collab_indices = CC_indices[CC_indptr[x_idx] : CC_indptr[x_idx + 1]]

        diffs = []
        for c_idx in collab_indices:
            collab_iv = iv_vec[c_idx]
            c_anime = P_indices[P_indptr[c_idx] : P_indptr[c_idx + 1]]
            if len(c_anime) == 0:
                continue

            shared_idx = []
            only_c_idx = []
            for a in c_anime:
                if a in x_anime_set:
                    shared_idx.append(a)
                else:
                    only_c_idx.append(a)

            if not shared_idx or not only_c_idx:
                continue

            wo_sums = anime_iv_sum[only_c_idx] - collab_iv
            wo_counts = anime_count[only_c_idx] - 1
            wo_pq = np.where(wo_counts > 0, wo_sums / np.maximum(wo_counts, 1), 0.0)
            mean_wo = (collab_iv - wo_pq).mean()

            w_sums = anime_iv_sum[shared_idx] - collab_iv - pid_iv
            w_counts = anime_count[shared_idx] - 2
            w_pq = np.where(w_counts > 0, w_sums / np.maximum(w_counts, 1), 0.0)
            mean_w = (collab_iv - w_pq).mean()

            diffs.append(mean_w - mean_wo)

        if len(diffs) < MIN_COLLABORATORS:
            result[pid] = None
        else:
            result[pid] = round(float(np.mean(diffs)), 2)

    return result


def compute_independent_value(
    features: dict[str, dict],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    collaboration_graph: nx.Graph | None = None,
) -> dict[str, float | None]:
    """Compute independent contribution (scipy.sparse vectorised).

    対象者Xのコラボレーターについて、
    「Xと共演した作品での残差」vs「共演していない作品での残差」の差分平均。

    scipy.sparse行列演算で O(P × C × A) のPythonループを排除。

    Args:
        features: person_id → 特徴量辞書
        credits: クレジットリスト
        anime_map: anime_id → Anime
        collaboration_graph: コラボレーショングラフ (unused, kept for API compat)

    Returns:
        person_id → independent_value (float, 正=引き上げ効果)
    """
    from scipy.sparse import csr_matrix

    if not features or not credits:
        return {}

    # --- Step 1: Build index mappings ---
    feature_pids = sorted(features.keys())
    pid_to_idx = {p: i for i, p in enumerate(feature_pids)}
    n_persons = len(feature_pids)

    # Collect anime IDs that have at least one featured person
    anime_ids_set: set[str] = set()
    for c in credits:
        if c.person_id in pid_to_idx:
            anime_ids_set.add(c.anime_id)
    anime_ids = sorted(anime_ids_set)
    aid_to_idx = {a: i for i, a in enumerate(anime_ids)}
    n_anime = len(anime_ids)

    if n_anime == 0:
        return {pid: None for pid in feature_pids}

    # --- Step 2: Build sparse participation matrix P (person × anime) ---
    rows, cols = [], []
    for c in credits:
        pi = pid_to_idx.get(c.person_id)
        ai = aid_to_idx.get(c.anime_id)
        if pi is not None and ai is not None:
            rows.append(pi)
            cols.append(ai)

    # Deduplicate (person, anime) pairs
    pairs = set(zip(rows, cols))
    rows_u = [p[0] for p in pairs]
    cols_u = [p[1] for p in pairs]
    P = csr_matrix(
        (np.ones(len(rows_u), dtype=np.float32), (rows_u, cols_u)),
        shape=(n_persons, n_anime),
    )

    # --- Step 3: IV score vector and per-anime aggregates ---
    iv_vec = np.array(
        [features[pid].get("iv_score", 0.0) for pid in feature_pids],
        dtype=np.float64,
    )
    # anime_iv_sum[a] = sum of IV of all participants in anime a
    anime_iv_sum = np.asarray(P.T @ iv_vec).ravel()  # (n_anime,)
    # anime_count[a] = number of participants in anime a
    anime_count = np.asarray(P.sum(axis=0)).ravel()  # (n_anime,)

    # --- Step 4: Co-occurrence matrix (person × person) ---
    # collab_count[i, j] = number of shared anime between i and j
    collab_count = P @ P.T  # sparse (n_persons × n_persons)
    collab_count.setdiag(0)  # exclude self
    collab_count.eliminate_zeros()

    # Number of collaborators per person
    n_collabs = np.asarray((collab_count > 0).sum(axis=1)).ravel()

    # --- Step 5: Numba JIT-compiled independent value computation ---
    # Compiles the triple-nested loop (person × collaborator × anime) to native code.
    # Uses raw CSR arrays with binary search — avoids all Python object overhead.
    result = _compute_iv_numba(
        P,
        collab_count,
        iv_vec,
        anime_iv_sum,
        anime_count,
        n_collabs,
        n_persons,
        feature_pids,
    )

    computed = sum(1 for v in result.values() if v is not None)
    logger.info(
        "independent_value_computed", persons=computed, skipped=len(result) - computed
    )
    return result


def _assemble_individual_profiles(
    features: dict,
    peer_data: dict,
    residuals: dict,
    consistency_scores: dict,
    independent_values: dict,
) -> dict:
    """Integration step: aggregate four metrics into a single person profile.

    Args:
        features: person_id → 特徴量辞書
        peer_data: person_id → {peer_percentile, peer_cohort, ...}
        residuals: person_id → opportunity_residual
        consistency_scores: person_id → consistency
        independent_values: person_id → independent_value

    Returns:
        person_id → {peer_percentile, opportunity_residual, consistency, independent_value, ...}
    """
    profiles = {}
    for pid in features:
        peer = peer_data.get(pid, {})
        profile = IndividualProfile(
            person_id=pid,
            peer_percentile=peer.get("peer_percentile"),
            peer_cohort=peer.get("peer_cohort"),
            opportunity_residual=residuals.get(pid),
            consistency=consistency_scores.get(pid),
            independent_value=independent_values.get(pid),
        )
        profile_dict = asdict(profile)
        # Add cluster metrics if available
        if "cluster_percentile" in peer:
            profile_dict["cluster_percentile"] = peer["cluster_percentile"]
            profile_dict["cluster_id"] = peer["cluster_id"]
            profile_dict["cluster_size"] = peer["cluster_size"]
        profiles[pid] = profile_dict
    return profiles


def compute_individual_profiles(
    results: list[dict],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    role_profiles: dict[str, dict],
    career_data: dict[str, dict],
    collaboration_graph: nx.Graph | None = None,
    akm_residuals: dict[tuple[str, str], float] | None = None,
    community_map: dict[str, int] | None = None,
) -> IndividualContributionResult:
    """Integrate all metrics to compute the Individual Contribution Profile.

    Args:
        results: パイプラインの結果リスト（person_id, composite 等を含む）
        credits: クレジットリスト
        anime_map: anime_id → Anime
        role_profiles: person_id → 役職情報
        career_data: person_id → キャリア情報
        collaboration_graph: コラボレーショングラフ
        akm_residuals: (person_id, anime_id) → AKM残差（consistency改善用）
        community_map: person_id → community_id（クラスタパーセンタイル用）

    Returns:
        IndividualContributionResult
    """
    logger.info("computing_individual_profiles", persons=len(results))

    # build features
    features = _build_person_features(
        results, credits, anime_map, role_profiles, career_data
    )

    # 1. peer comparison percentile (+ cluster-based)
    peer_data = compute_peer_percentile(features, community_map=community_map)

    # 2. opportunity-controlled residual
    residuals, r_squared = compute_opportunity_residual(features)

    # 3. consistency score (can use AKM residuals)
    consistency_scores = compute_consistency(
        features, credits, anime_map, akm_residuals=akm_residuals
    )

    # 4. independent contribution (selection-bias-reduced version)
    independent_values = compute_independent_value(
        features, credits, anime_map, collaboration_graph
    )

    # integrate: combine four metrics into one profile
    profiles = _assemble_individual_profiles(
        features, peer_data, residuals, consistency_scores, independent_values
    )

    logger.info(
        "individual_profiles_computed",
        total=len(profiles),
        with_percentile=sum(
            1 for p in profiles.values() if p["peer_percentile"] is not None
        ),
        with_residual=sum(
            1 for p in profiles.values() if p["opportunity_residual"] is not None
        ),
        with_consistency=sum(
            1 for p in profiles.values() if p["consistency"] is not None
        ),
        with_independent=sum(
            1 for p in profiles.values() if p["independent_value"] is not None
        ),
        r_squared=r_squared,
    )

    return IndividualContributionResult(
        profiles=profiles,
        model_r_squared=r_squared,
        total_persons=len(profiles),
        cohort_count=len(
            {(f["primary_role"], f["career_band"]) for f in features.values()}
        ),
    )
