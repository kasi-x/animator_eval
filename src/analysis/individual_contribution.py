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

from src.models import Anime, Credit

logger = structlog.get_logger()

# キャリア年数バンドの幅
CAREER_BAND_WIDTH = 5
# コホートの最小人数（これ未満はマージまたは null）
MIN_COHORT_SIZE = 5
# 一貫性スコア算出に必要な最小作品数
MIN_WORKS_FOR_CONSISTENCY = 5
# 独立貢献度算出に必要な最小コラボレーター数
MIN_COLLABORATORS = 3


@dataclass
class IndividualProfile:
    """個人貢献プロファイル."""

    person_id: str
    peer_percentile: float | None = None
    peer_cohort: dict | None = None
    opportunity_residual: float | None = None
    consistency: float | None = None
    independent_value: float | None = None


@dataclass
class IndividualContributionResult:
    """全体の結果."""

    profiles: dict  # person_id -> IndividualProfile (as dict)
    model_r_squared: float | None = None
    total_persons: int = 0
    cohort_count: int = 0


def _get_career_band(years: int) -> str:
    """キャリア年数をバンドに変換."""
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
    """各人の特徴量を構築する."""
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

        # 役職
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

        # 参加作品の平均スタッフ数（機会の指標 — anime.score は使わない）
        pc = person_credits.get(pid, [])
        staff_counts_list = []
        seen_anime = set()
        for c in pc:
            if c.anime_id in seen_anime:
                continue
            seen_anime.add(c.anime_id)
            staff_counts_list.append(anime_staff_counts.get(c.anime_id, 1))
        avg_staff_count = np.mean(staff_counts_list) if staff_counts_list else 0

        # スタジオ規模（所属スタジオの頻度）
        studios = person_studios.get(pid, [])
        # スタジオ規模の代理指標: そのスタジオで何人が働いているか
        # → 簡易版: ユニークスタジオ数（多い = 大手ではなく複数経験）
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
    """コホート内パーセンタイルを算出.

    Args:
        features: person_id → 特徴量辞書
        community_map: person_id → community_id（クラスタベースのパーセンタイル追加用）

    Returns:
        person_id → {peer_percentile, peer_cohort, cluster_percentile?, cluster_id?, cluster_size?}
    """
    # コホートを構成: role × career_band
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

        # パーセンタイル算出 (bisect for O(n log n) instead of O(n²))
        scores = sorted([s for _, s in cohort_members])
        n_members = len(scores)
        for pid, score in cohort_members:
            # パーセンタイル: この人より低い人の割合
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
    """機会統制残差を算出.

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

    # 役職のダミー変数化
    roles = sorted({f["primary_role"] for f in features.values()})
    role_to_idx = {r: i for i, r in enumerate(roles)}

    y = np.array([features[pid]["iv_score"] for pid in pids])

    # 特徴量行列: [career_years, avg_staff_count, unique_studios, role_dummies...]
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

    # 切片を追加
    X = np.column_stack([np.ones(n), X])

    # OLS: β = (X'X)^-1 X'y with leverage correction (studentized residuals)
    try:
        XtX = X.T @ X
        # 正則化（特異行列対策）
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
    """作品間の一貫性スコアを算出.

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


def compute_independent_value(
    features: dict[str, dict],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    collaboration_graph: nx.Graph | None = None,
) -> dict[str, float | None]:
    """独立貢献度を算出.

    対象者Xのコラボレーターについて、
    「Xと共演した作品のスコア」vs「Xと共演していない作品のスコア」を比較。
    差分の平均がXの独立貢献度。

    Args:
        features: person_id → 特徴量辞書
        credits: クレジットリスト
        anime_map: anime_id → Anime
        collaboration_graph: コラボレーショングラフ

    Returns:
        person_id → independent_value (float, 正=引き上げ効果)
    """
    # anime → 参加者セット
    anime_persons: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        if c.person_id in features:
            anime_persons[c.anime_id].add(c.person_id)

    # person → {anime_id} (participation set — no anime.score)
    person_anime_set: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        if c.person_id in features:
            person_anime_set[c.person_id].add(c.anime_id)

    result = {}
    target_pids = list(features.keys())
    features_keys = set(features.keys())

    # Precompute project quality per anime: sum and count of participant IV scores
    anime_iv_sum: dict[str, float] = {}
    anime_iv_count: dict[str, int] = {}
    for aid, persons in anime_persons.items():
        total = 0.0
        count = 0
        for p in persons:
            if p in features:
                total += features[p].get("iv_score", 0.0)
                count += 1
        anime_iv_sum[aid] = total
        anime_iv_count[aid] = count

    for pid in target_pids:
        # コラボレーターを特定
        if collaboration_graph and pid in collaboration_graph:
            collaborators = set(collaboration_graph.neighbors(pid))
        else:
            collaborators = set()
            for aid in person_anime_set[pid]:
                collaborators.update(anime_persons.get(aid, set()))
            collaborators.discard(pid)

        collaborators = collaborators & features_keys

        if len(collaborators) < MIN_COLLABORATORS:
            result[pid] = None
            continue

        # 各コラボレーターについて: IV統制付きの比較
        # "pid がいるときのコラボレーターのIV残差" vs "いないとき"
        diffs = []
        pid_anime = person_anime_set[pid]
        pid_iv = features[pid].get("iv_score", 0.0)

        for collab_id in collaborators:
            collab_anime_ids = person_anime_set.get(collab_id, set())
            if not collab_anime_ids:
                continue

            with_x_resids = []
            without_x_resids = []
            collab_iv = features[collab_id].get("iv_score", 0.0)
            for aid in collab_anime_ids:
                # Fix B07: exclude both collab AND pid from project quality
                total = anime_iv_sum.get(aid, 0.0) - collab_iv
                count = anime_iv_count.get(aid, 0) - (1 if collab_id in features else 0)
                if aid in pid_anime:
                    total -= pid_iv  # B07 fix: exclude pid too
                    count -= 1
                proj_quality = total / count if count > 0 else 0.0
                # Use collab's IV as "work outcome" instead of anime.score
                resid = collab_iv - proj_quality

                if aid in pid_anime:
                    with_x_resids.append(resid)
                else:
                    without_x_resids.append(resid)

            if with_x_resids and without_x_resids:
                diff = np.mean(with_x_resids) - np.mean(without_x_resids)
                diffs.append(diff)

        if len(diffs) < MIN_COLLABORATORS:
            result[pid] = None
            continue

        result[pid] = round(float(np.mean(diffs)), 2)

    computed = sum(1 for v in result.values() if v is not None)
    logger.info(
        "independent_value_computed", persons=computed, skipped=len(result) - computed
    )
    return result


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
    """全指標を統合して Individual Contribution Profile を算出.

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

    # 特徴量構築
    features = _build_person_features(
        results, credits, anime_map, role_profiles, career_data
    )

    # 1. ピア比較パーセンタイル（+ クラスタベース）
    peer_data = compute_peer_percentile(features, community_map=community_map)

    # 2. 機会統制残差
    residuals, r_squared = compute_opportunity_residual(features)

    # 3. 一貫性スコア（AKM残差を使用可能）
    consistency_scores = compute_consistency(
        features, credits, anime_map, akm_residuals=akm_residuals
    )

    # 4. 独立貢献度（セレクションバイアス軽減版）
    independent_values = compute_independent_value(
        features, credits, anime_map, collaboration_graph
    )

    # 統合
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
        # Add cluster percentile if available
        if "cluster_percentile" in peer:
            profile_dict["cluster_percentile"] = peer["cluster_percentile"]
            profile_dict["cluster_id"] = peer["cluster_id"]
            profile_dict["cluster_size"] = peer["cluster_size"]
        profiles[pid] = profile_dict

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
