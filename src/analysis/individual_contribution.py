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

        # 参加作品の平均スコア（機会の指標）
        pc = person_credits.get(pid, [])
        anime_scores = []
        seen_anime = set()
        for c in pc:
            if c.anime_id in seen_anime:
                continue
            seen_anime.add(c.anime_id)
            anime = anime_map.get(c.anime_id)
            if anime and anime.score:
                anime_scores.append(anime.score)
        avg_anime_score = np.mean(anime_scores) if anime_scores else 0

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
            "avg_anime_score": avg_anime_score,
            "unique_studios": unique_studios,
            "work_count": len(seen_anime),
            "credit_count": len(pc),
        }

    return features


def compute_peer_percentile(
    features: dict[str, dict],
) -> dict[str, dict]:
    """コホート内パーセンタイルを算出.

    Args:
        features: person_id → 特徴量辞書

    Returns:
        person_id → {peer_percentile, peer_cohort}
    """
    # コホートを構成: role × career_band
    cohorts: dict[tuple[str, str], list[tuple[str, float]]] = defaultdict(list)
    for pid, f in features.items():
        key = (f["primary_role"], f["career_band"])
        cohorts[key].append((pid, f["iv_score"]))

    # 小さいコホートをマージ（同じ役職の隣接バンド）
    role_bands: dict[str, list[str]] = defaultdict(list)
    for role, band in cohorts:
        if role not in role_bands or band not in role_bands[role]:
            role_bands[role].append(band)

    result = {}
    for (role, band), members in cohorts.items():
        if len(members) < MIN_COHORT_SIZE:
            # 同じ役職の全バンドをマージ
            merged = []
            for (r2, b2), m2 in cohorts.items():
                if r2 == role:
                    merged.extend(m2)
            if len(merged) < MIN_COHORT_SIZE:
                for pid, _ in members:
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
            rank = bisect.bisect_left(scores, score)
            percentile = round(rank / n_members * 100, 1)
            result[pid] = {
                "peer_percentile": percentile,
                "peer_cohort": cohort_label,
            }

    logger.info("peer_percentile_computed", persons=len(result), cohorts=len(cohorts))
    return result


def compute_opportunity_residual(
    features: dict[str, dict],
) -> tuple[dict[str, float], float]:
    """機会統制残差を算出.

    OLS: composite ~ career_years + avg_anime_score + unique_studios + role_dummies

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

    # 特徴量行列: [career_years, avg_anime_score, unique_studios, role_dummies...]
    n = len(pids)
    n_roles = max(len(roles) - 1, 1)  # ダミー変数（基準役職を除く）
    X = np.zeros((n, 3 + n_roles))

    for i, pid in enumerate(pids):
        f = features[pid]
        X[i, 0] = f["career_years"]
        X[i, 1] = f["avg_anime_score"]
        X[i, 2] = f["unique_studios"]
        ridx = role_to_idx.get(f["primary_role"], 0)
        if ridx > 0 and ridx <= n_roles:
            X[i, 2 + ridx] = 1.0

    # 切片を追加
    X = np.column_stack([np.ones(n), X])

    # OLS: β = (X'X)^-1 X'y
    try:
        XtX = X.T @ X
        # 正則化（特異行列対策）
        XtX += np.eye(XtX.shape[0]) * 1e-8
        beta = np.linalg.solve(XtX, X.T @ y)
        y_hat = X @ beta
        residuals = y - y_hat

        # R²
        ss_res = np.sum(residuals**2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # 標準化残差（z-score）
        std = np.std(residuals)
        if std > 0:
            z_residuals = residuals / std
        else:
            z_residuals = np.zeros(n)

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
) -> dict[str, float | None]:
    """作品間の一貫性スコアを算出.

    各人が参加した作品のスコアの変動係数（CV）を計算し、
    CVが低い（安定している）ほど高い一貫性スコアを返す。

    Args:
        features: person_id → 特徴量辞書
        credits: クレジットリスト
        anime_map: anime_id → Anime

    Returns:
        person_id → consistency (0-1, 1=完全に安定)
    """
    # person → 作品スコアリスト
    person_work_scores: dict[str, list[float]] = defaultdict(list)
    person_seen_anime: dict[str, set] = defaultdict(set)

    for c in credits:
        pid = c.person_id
        if pid not in features:
            continue
        if c.anime_id in person_seen_anime[pid]:
            continue
        person_seen_anime[pid].add(c.anime_id)
        anime = anime_map.get(c.anime_id)
        if anime and anime.score:
            person_work_scores[pid].append(anime.score)

    result = {}
    for pid in features:
        scores = person_work_scores.get(pid, [])
        if len(scores) < MIN_WORKS_FOR_CONSISTENCY:
            result[pid] = None
            continue

        mean = np.mean(scores)
        if mean == 0:
            result[pid] = None
            continue

        cv = np.std(scores) / mean
        # CV を 0-1 の一貫性スコアに変換（CV=0 → 1.0, CV=1 → 0.0）
        consistency = max(0.0, 1.0 - cv)
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

    # person → {anime_id: score}
    person_anime_scores: dict[str, dict[str, float]] = defaultdict(dict)
    for c in credits:
        if c.person_id not in features:
            continue
        anime = anime_map.get(c.anime_id)
        if anime and anime.score:
            person_anime_scores[c.person_id][c.anime_id] = anime.score

    result = {}
    target_pids = list(features.keys())
    features_keys = set(features.keys())

    for pid in target_pids:
        # コラボレーターを特定
        if collaboration_graph and pid in collaboration_graph:
            collaborators = set(collaboration_graph.neighbors(pid))
        else:
            # グラフがなければ共演者から構築
            collaborators = set()
            for aid in person_anime_scores[pid]:
                collaborators.update(anime_persons.get(aid, set()))
            collaborators.discard(pid)

        collaborators = collaborators & features_keys

        if len(collaborators) < MIN_COLLABORATORS:
            result[pid] = None
            continue

        # 各コラボレーターについて: Xと共演時のスコア vs 非共演時のスコア
        diffs = []
        pid_anime = set(person_anime_scores[pid].keys())

        for collab_id in collaborators:
            collab_anime = person_anime_scores.get(collab_id, {})
            if not collab_anime:
                continue

            with_x = [s for aid, s in collab_anime.items() if aid in pid_anime]
            without_x = [s for aid, s in collab_anime.items() if aid not in pid_anime]

            if with_x and without_x:
                diff = np.mean(with_x) - np.mean(without_x)
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
) -> IndividualContributionResult:
    """全指標を統合して Individual Contribution Profile を算出.

    Args:
        results: パイプラインの結果リスト（person_id, composite 等を含む）
        credits: クレジットリスト
        anime_map: anime_id → Anime
        role_profiles: person_id → 役職情報
        career_data: person_id → キャリア情報
        collaboration_graph: コラボレーショングラフ

    Returns:
        IndividualContributionResult
    """
    logger.info("computing_individual_profiles", persons=len(results))

    # 特徴量構築
    features = _build_person_features(
        results, credits, anime_map, role_profiles, career_data
    )

    # 1. ピア比較パーセンタイル
    peer_data = compute_peer_percentile(features)

    # 2. 機会統制残差
    residuals, r_squared = compute_opportunity_residual(features)

    # 3. 一貫性スコア
    consistency_scores = compute_consistency(features, credits, anime_map)

    # 4. 独立貢献度
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
        profiles[pid] = asdict(profile)

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
