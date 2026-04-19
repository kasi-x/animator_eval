"""ML-based homonym disambiguation using credit-pattern clustering.

同名別人の分離に DBSCAN を使用する。
ANN の ann_id が付いた同名別人グループをキャリブレーション用 ground truth として使い、
閾値 (epsilon) を自動調整する。

設計原則:
- false positive（別人→同一人物）は厳禁（信用毀損リスク）
- false negative（同一人物→別人扱い）は許容（保守側に倒す）
- 3クレジット未満の人物は判定不能として「分割しない」

特徴量グループ（重み付き）:
  decade     活動decade分布 (1920s-2020s, 11次元)  weight=3.0  ← 最重要
  role       ロール種別分布 (24次元)                weight=1.5
  role_cat   ロールカテゴリ (4次元)                 weight=2.0  ← animator/director分離
  studio     スタジオ在籍 (binary, 可変)             weight=1.0
  anime      共演作品 (binary, 可変)                 weight=0.5
"""
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

import numpy as np
import structlog

if TYPE_CHECKING:
    pass

logger = structlog.get_logger()

# DBSCAN パラメータ
MIN_CREDITS_FOR_SPLIT = 3       # これ未満のクレジット数の人物は ML 判定をスキップ
MIN_SAMPLES = 1                  # 孤立点も1クラスタとして扱う
DEFAULT_EPSILON = 0.35           # キャリブレーション失敗時のフォールバック

# 特徴量グループの重み
_W_DECADE = 3.0
_W_ROLE = 1.5
_W_ROLE_CAT = 2.0
_W_STUDIO = 1.0
_W_ANIME = 0.5

# decade インデックス: 1920s〜2020s = 11バケット
_DECADE_START = 1920
_N_DECADES = 11  # 1920, 1930, ..., 2020

# ロールカテゴリ（4区分）
_ROLE_CATEGORIES = {
    "animator": {
        "key_animator", "second_key_animator", "in_between",
        "animation_director", "chief_animation_director",
        "character_designer",
    },
    "director": {
        "director", "episode_director", "series_director",
        "storyboard", "action_animation_director",
    },
    "producer_staff": {
        "producer", "series_composition", "screenplay",
        "music", "sound_director", "photography_director",
        "art_director", "color_design", "cgi_director",
        "editing", "original_creator", "settings",
        "production_manager",
    },
    "voice_other": {
        "voice_actor", "special", "other",
    },
}


def _role_to_category(role_value: str) -> str:
    for cat, roles in _ROLE_CATEGORIES.items():
        if role_value in roles:
            return cat
    return "voice_other"


def _decade_idx(year: int | None) -> int | None:
    if year is None:
        return None
    idx = (year - _DECADE_START) // 10
    return max(0, min(idx, _N_DECADES - 1))


def build_feature_matrix(
    person_ids: list[str],
    credits_by_person: dict[str, list],
    anime_meta: dict[str, dict],
) -> tuple[np.ndarray, list[str]]:
    """クレジット情報から特徴行列を構築する。

    Args:
        person_ids: 対象の person_id リスト
        credits_by_person: {person_id: [Credit, ...]}
        anime_meta: {anime_id: {"year": int|None, "studios": list[str]}}

    Returns:
        (X, valid_pids)
        X: shape (len(valid_pids), n_features), L2 正規化済み
        valid_pids: MIN_CREDITS_FOR_SPLIT 以上のクレジットを持つ person_id リスト
    """
    from src.models import Role

    all_roles = [r.value for r in Role]
    role_idx = {r: i for i, r in enumerate(all_roles)}
    role_cats = ["animator", "director", "producer_staff", "voice_other"]
    role_cat_idx = {c: i for i, c in enumerate(role_cats)}

    # クレジット数が十分な人物のみ使用
    valid_pids = [
        pid for pid in person_ids
        if len(credits_by_person.get(pid, [])) >= MIN_CREDITS_FOR_SPLIT
    ]
    if len(valid_pids) < 2:
        # 十分なデータがある人物が2人未満 → 分割不可
        valid_pids = person_ids

    # グループ内の全スタジオ・作品を収集（binary特徴のインデックス構築）
    all_studios: set[str] = set()
    all_anime: set[str] = set()
    for pid in valid_pids:
        for c in credits_by_person.get(pid, []):
            meta = anime_meta.get(c.anime_id, {})
            for s in meta.get("studios", []):
                all_studios.add(s)
            all_anime.add(c.anime_id)

    studio_list = sorted(all_studios)
    anime_list = sorted(all_anime)
    studio_idx = {s: i for i, s in enumerate(studio_list)}
    anime_idx_ = {a: i for i, a in enumerate(anime_list)}

    # 特徴次元数（重み付き）
    n_decade = _N_DECADES
    n_role = len(all_roles)
    n_role_cat = len(role_cats)
    n_studio = len(studio_list)
    n_anime = len(anime_list)
    # 重みは特徴ブロックを繰り返し/スケーリングで実現
    # 実装上は float 重み × 特徴ベクトルを連結
    n = len(valid_pids)
    X_decade = np.zeros((n, n_decade))
    X_role = np.zeros((n, n_role))
    X_role_cat = np.zeros((n, n_role_cat))
    X_studio = np.zeros((n, max(n_studio, 1)))
    X_anime = np.zeros((n, max(n_anime, 1)))

    for i, pid in enumerate(valid_pids):
        for c in credits_by_person.get(pid, []):
            meta = anime_meta.get(c.anime_id, {})
            year = meta.get("year")

            # decade
            d = _decade_idx(year)
            if d is not None:
                X_decade[i, d] += 1.0

            # role
            rv = c.role.value if hasattr(c.role, "value") else str(c.role)
            if rv in role_idx:
                X_role[i, role_idx[rv]] += 1.0

            # role category
            cat = _role_to_category(rv)
            X_role_cat[i, role_cat_idx[cat]] += 1.0

            # studio (binary)
            for s in meta.get("studios", []):
                if s in studio_idx:
                    X_studio[i, studio_idx[s]] = 1.0

            # anime (binary)
            if c.anime_id in anime_idx_:
                X_anime[i, anime_idx_[c.anime_id]] = 1.0

    # 各ブロックを L2 正規化してから重み適用
    def _normalize_block(M: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(M, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return M / norms

    parts = [
        _normalize_block(X_decade) * _W_DECADE,
        _normalize_block(X_role) * _W_ROLE,
        _normalize_block(X_role_cat) * _W_ROLE_CAT,
        _normalize_block(X_studio) * _W_STUDIO,
        _normalize_block(X_anime) * _W_ANIME,
    ]
    X = np.hstack(parts)

    # 最終 L2 正規化
    norms = np.linalg.norm(X, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return X / norms, valid_pids


def _cosine_dist_matrix(X: np.ndarray) -> np.ndarray:
    """L2 正規化済み行列のペアワイズ cosine 距離行列を返す。"""
    sim = X @ X.T
    sim = np.clip(sim, -1.0, 1.0)
    return 1.0 - sim


def calibrate_epsilon(
    persons: list,
    credits_by_person: dict[str, list],
    anime_meta: dict[str, dict],
    percentile: float = 20.0,
) -> float:
    """ANN の同名別人グループを ground truth としてキャリブレーションする。

    同じ name_en を持つが ann_id が異なる人物ペアは「確実に別人」。
    それらのペアワイズ cosine 距離の分布から epsilon を決定する。

    percentile: 使用するパーセンタイル（低いほど保守的 = 分割しやすい）
    """
    ann_by_name: dict[str, list] = defaultdict(list)
    for p in persons:
        if p.ann_id and p.id.startswith("ann-") and p.name_en:
            key = p.name_en.strip().lower()
            ann_by_name[key].append(p)

    homonym_groups = [
        (name, ps) for name, ps in ann_by_name.items() if len(ps) >= 2
    ]

    if not homonym_groups:
        logger.info("ml_calibration_skipped", reason="no_ann_homonym_groups")
        return DEFAULT_EPSILON

    between_dists: list[float] = []

    for _name, group_persons in homonym_groups[:300]:
        pids = [p.id for p in group_persons]
        X, valid_pids = build_feature_matrix(pids, credits_by_person, anime_meta)
        if len(valid_pids) < 2 or X.shape[0] < 2:
            continue

        dist = _cosine_dist_matrix(X)
        n = X.shape[0]
        for i in range(n):
            for j in range(i + 1, n):
                between_dists.append(float(dist[i, j]))

    if not between_dists:
        logger.warning("ml_calibration_no_pairs")
        return DEFAULT_EPSILON

    dists = np.array(between_dists)
    epsilon = float(np.percentile(dists, percentile))
    epsilon = max(0.10, min(epsilon, 0.85))

    logger.info(
        "ml_homonym_calibrated",
        n_ground_truth_groups=len(homonym_groups),
        n_pairs=len(between_dists),
        dist_p10=round(float(np.percentile(dists, 10)), 3),
        dist_p20=round(float(np.percentile(dists, 20)), 3),
        dist_p50=round(float(np.percentile(dists, 50)), 3),
        dist_p80=round(float(np.percentile(dists, 80)), 3),
        calibrated_epsilon=round(epsilon, 3),
        calibration_percentile=percentile,
    )
    return epsilon


def split_homonym_groups(
    persons: list,
    credits_by_person: dict[str, list],
    anime_meta: dict[str, dict],
    epsilon: float | None = None,
) -> dict[str, str]:
    """同名グループに DBSCAN を適用して別人を分離する。

    Returns:
        {person_id: cluster_representative_id}
        同じクラスタの人物は同じ代表 ID にマップされる。
        異なるクラスタの人物（同名でも別人と判定）は異なる代表 ID になる。
    """
    try:
        from sklearn.cluster import DBSCAN
    except ImportError:
        logger.warning("ml_homonym_split_skipped", reason="sklearn_not_available")
        return {}

    from src.analysis.entity_resolution import normalize_name

    if epsilon is None:
        epsilon = calibrate_epsilon(persons, credits_by_person, anime_meta, percentile=70.0)

    # 名前別グループ化（name_ja 優先、なければ name_en）
    name_groups: dict[str, list] = defaultdict(list)
    for p in persons:
        key = normalize_name(p.name_ja or p.name_en or "")
        if key and len(key) >= 3:
            name_groups[key].append(p)

    cluster_map: dict[str, str] = {}
    n_groups_split = 0
    n_persons_split = 0

    for name, group_persons in name_groups.items():
        if len(group_persons) < 2:
            continue

        pids = [p.id for p in group_persons]
        X, valid_pids = build_feature_matrix(pids, credits_by_person, anime_meta)

        if X.shape[0] < 2:
            continue

        dist_mat = _cosine_dist_matrix(X)

        # DBSCAN: min_samples=1 で孤立点も独立クラスタとして扱う
        db = DBSCAN(eps=epsilon, min_samples=MIN_SAMPLES, metric="precomputed")
        labels = db.fit_predict(dist_mat)

        # クラスタごとに代表 ID を決める（最初のメンバーを代表とする）
        cluster_reps: dict[int, str] = {}
        for i, pid in enumerate(valid_pids):
            label = labels[i]
            if label not in cluster_reps:
                cluster_reps[label] = pid
            cluster_map[pid] = cluster_reps[label]

        n_clusters = len(set(labels))
        if n_clusters > 1:
            n_groups_split += 1
            n_persons_split += len(valid_pids)
            logger.info(
                "ml_homonym_split",
                name=name,
                n_persons=len(valid_pids),
                n_clusters=n_clusters,
                pids=valid_pids,
            )

    logger.info(
        "ml_homonym_split_complete",
        epsilon=round(epsilon, 3),
        groups_split=n_groups_split,
        persons_affected=n_persons_split,
    )
    return cluster_map


def build_anime_meta(anime_list: list) -> dict[str, dict]:
    """anime_list から anime_meta dict を構築するヘルパー。"""
    return {
        a.id: {
            "year": a.year,
            "studios": a.studios if isinstance(a.studios, list) else [],
        }
        for a in anime_list
    }
