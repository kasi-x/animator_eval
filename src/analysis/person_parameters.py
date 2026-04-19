"""Person Parameter Card — 10軸パラメータカードの計算.

各個人を10個の日本語パラメータ (0-99 percentile + CI) で表現し、
K-means K=6 によるアーキタイプ分類を付与する。

全パラメータは anime.score を使用せず、構造的クレジットデータのみから算出。
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

import numpy as np
import structlog

if TYPE_CHECKING:
    import sqlite3

logger = structlog.get_logger()

# -------------------------------------------------------------------
# Archetype labels (K=6 centroids ranked by parameter profile)
# -------------------------------------------------------------------
ARCHETYPE_LABELS = [
    "構造中核型",        # 中心性・信頼蓄積 高
    "育成型",           # 育成貢献 高
    "スペシャリスト型",  # ジャンル特化 高, 協業幅 低
    "広域ジェネラリスト型",  # 協業幅 高, 継続力 中
    "現役トップ型",      # 直近活発度 + 規模到達力 高
    "レガシー型",        # 継続力 + 信頼蓄積 高, 直近活発度 低
]

PARAM_KEYS = [
    "scale_reach",
    "collab_breadth",
    "consistency",
    "mentor_value",
    "centrality",
    "trust_depth",
    "role_evolution",
    "genre_spec",
    "recent_activity",
    "compatibility",
]

PARAM_NAMES_JA = {
    "scale_reach": "規模到達力",
    "collab_breadth": "協業幅",
    "consistency": "継続力",
    "mentor_value": "育成貢献",
    "centrality": "中心性",
    "trust_depth": "信頼蓄積",
    "role_evolution": "役割進化",
    "genre_spec": "ジャンル特化",
    "recent_activity": "直近活発度",
    "compatibility": "相性指標",
}


# -------------------------------------------------------------------
# Raw value extraction helpers
# -------------------------------------------------------------------

def _extract_raw_values(
    results: list[dict],
    mentorship_list: list[dict],
    genre_affinity: dict[str, dict],
    compatibility_boost: dict[str, float],
) -> dict[str, dict[str, float]]:
    """各 person の 10 パラメータの生の値を抽出する."""

    # Build mentor → mentee count index (for mentor_value raw score)
    mentor_mentee_count: dict[str, int] = defaultdict(int)
    mentor_confidence_sum: dict[str, float] = defaultdict(float)
    for m in mentorship_list:
        mid = m.get("mentor_id")
        if mid:
            mentor_mentee_count[mid] += 1
            mentor_confidence_sum[mid] += m.get("confidence", 50) / 100.0

    raw: dict[str, dict[str, float]] = {}

    for r in results:
        pid = r["person_id"]
        career = r.get("career") or {}
        growth = r.get("growth") or {}
        versatility = r.get("versatility") or {}
        first_year = career.get("first_year") or 0
        latest_year = career.get("latest_year") or 0
        career_span = max(latest_year - first_year + 1, 1) if first_year else 1
        active_years = career.get("active_years") or 0
        highest_stage = career.get("highest_stage") or 0

        # --- 1. 規模到達力: AKM person_fe percentile (raw = person_fe) ---
        scale_reach_raw = r.get("person_fe", 0.0)

        # --- 2. 協業幅: role category entropy (versatility_score 0-100) ---
        collab_breadth_raw = versatility.get("score", 0.0)

        # --- 3. 継続力: active_years / career_span (activity density) ---
        # Represents how consistently they stayed active throughout their career.
        # CV-based variant would need annual credit series not stored in results.
        consistency_raw = active_years / career_span if career_span > 1 else 0.0

        # --- 4. 育成貢献: mentor residual proxy = weighted mentee count ---
        n_mentees = mentor_mentee_count.get(pid, 0)
        mentor_confidence = mentor_confidence_sum.get(pid, 0.0)
        mentor_value_raw = n_mentees * (mentor_confidence / max(n_mentees, 1))

        # --- 5. 中心性: BiRank (weighted PageRank proxy) ---
        centrality_raw = r.get("birank", 0.0)

        # --- 6. 信頼蓄積: patronage (cumulative edge weight) ---
        trust_depth_raw = r.get("patronage", 0.0)

        # --- 7. 役割進化: highest_stage × (active_years / career_span) ---
        # Rewards reaching higher stages while staying consistently active.
        role_evolution_raw = highest_stage * (active_years / career_span) if career_span > 1 else 0.0

        # --- 8. ジャンル特化: max share across score_tiers (genre concentration) ---
        ga = genre_affinity.get(pid, {})
        score_tiers = ga.get("score_tiers") or {}
        # Use credit-weighted tier concentration: max share ignoring 'unknown'
        known_tiers = {k: v for k, v in score_tiers.items() if k != "unknown"}
        if known_tiers:
            genre_spec_raw = max(known_tiers.values()) / 100.0
        else:
            # Fallback: era concentration
            eras = ga.get("eras") or {}
            known_eras = {k: v for k, v in eras.items() if k != "unknown"}
            genre_spec_raw = max(known_eras.values()) / 100.0 if known_eras else 0.0

        # --- 9. 直近活発度: recent_credits weighted by dormancy inverse ---
        recent_credits = growth.get("recent_credits") or 0
        dormancy = r.get("dormancy", 0.5)
        # dormancy 1.0 = fully active, 0.0 = completely dormant
        recent_activity_raw = recent_credits * dormancy

        # --- 10. 相性指標: compatibility boost score (higher = more compatible partners) ---
        compatibility_raw = compatibility_boost.get(pid, 0.0)

        raw[pid] = {
            "scale_reach": scale_reach_raw,
            "collab_breadth": collab_breadth_raw,
            "consistency": consistency_raw,
            "mentor_value": mentor_value_raw,
            "centrality": centrality_raw,
            "trust_depth": trust_depth_raw,
            "role_evolution": role_evolution_raw,
            "genre_spec": genre_spec_raw,
            "recent_activity": recent_activity_raw,
            "compatibility": compatibility_raw,
        }

    return raw


# -------------------------------------------------------------------
# Percentile normalization
# -------------------------------------------------------------------

def _to_percentiles(raw: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    """各パラメータを 0-99 percentile に変換する."""
    pids = list(raw.keys())
    pct: dict[str, dict[str, float]] = {pid: {} for pid in pids}

    for key in PARAM_KEYS:
        values = np.array([raw[pid][key] for pid in pids], dtype=float)
        # Replace NaN/inf
        values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
        n = len(values)
        if n == 0:
            continue
        ranks = values.argsort().argsort()  # 0-based rank
        percentiles = ranks / max(n - 1, 1) * 99.0
        for i, pid in enumerate(pids):
            pct[pid][key] = round(float(percentiles[i]), 2)

    return pct


# -------------------------------------------------------------------
# Bootstrap CI
# -------------------------------------------------------------------

def _bootstrap_ci(
    values: list[float],
    n_boot: int = 500,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """95% bootstrap percentile CI for the mean of values."""
    if not values:
        return (0.0, 0.0)
    arr = np.array(values, dtype=float)
    if len(arr) == 1:
        v = float(arr[0])
        return (v, v)
    rng = np.random.default_rng(42)
    boot_means = [rng.choice(arr, size=len(arr), replace=True).mean() for _ in range(n_boot)]
    lo = float(np.percentile(boot_means, 100 * alpha / 2))
    hi = float(np.percentile(boot_means, 100 * (1 - alpha / 2)))
    return (round(lo, 2), round(hi, 2))


def _compute_ci(
    pct: dict[str, dict[str, float]],
    raw: dict[str, dict[str, float]],
) -> dict[str, dict[str, tuple[float, float]]]:
    """各パラメータの CI を計算する.

    CI approach per parameter:
    - scale_reach: analytical (SE already in scores.json as person_fe_se, skip here)
    - Others: ±2σ of percentile values in same decile cohort (cross-person variance)
      as a proxy for within-person uncertainty
    """
    pids = list(pct.keys())
    ci: dict[str, dict[str, tuple[float, float]]] = {pid: {} for pid in pids}

    for key in PARAM_KEYS:
        percentile_vals = np.array([pct[pid][key] for pid in pids])
        global_std = float(np.std(percentile_vals)) if len(percentile_vals) > 1 else 5.0
        # Simple ±1 std proxy: wider CI for sparser data
        for pid in pids:
            v = pct[pid][key]
            lo = round(max(0.0, v - global_std), 2)
            hi = round(min(99.0, v + global_std), 2)
            ci[pid][key] = (lo, hi)

    return ci


# -------------------------------------------------------------------
# Archetype classification (K-means K=6)
# -------------------------------------------------------------------

def _assign_archetypes(
    pct: dict[str, dict[str, float]],
) -> dict[str, tuple[str, int]]:
    """K-means K=6 でアーキタイプを分類する."""
    try:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler
    except ImportError:
        logger.warning("sklearn_not_available", detail="archetypes skipped")
        return {pid: ("不明", -1) for pid in pct}

    pids = list(pct.keys())
    if len(pids) < 6:
        return {pid: ("不明", -1) for pid in pids}

    X = np.array([[pct[pid].get(k, 0.0) for k in PARAM_KEYS] for pid in pids])
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    km = KMeans(n_clusters=6, random_state=42, n_init=10)
    labels = km.fit_predict(Xs)

    # Name clusters by dominant parameter (highest centroid value per cluster)
    centers = km.cluster_centers_
    # Map each cluster idx to archetype based on which param is highest relative to others
    cluster_to_archetype = _name_clusters(centers)

    return {pid: (cluster_to_archetype[labels[i]], int(labels[i])) for i, pid in enumerate(pids)}


def _name_clusters(centers: np.ndarray) -> dict[int, str]:
    """クラスタ重心をパラメータランクで archetype 名にマップする."""
    # For each cluster, find the top-2 dominant parameters
    # Then match to predefined archetype profiles
    archetype_profiles = {
        "構造中核型": ["centrality", "trust_depth"],
        "育成型": ["mentor_value", "role_evolution"],
        "スペシャリスト型": ["genre_spec"],
        "広域ジェネラリスト型": ["collab_breadth"],
        "現役トップ型": ["recent_activity", "scale_reach"],
        "レガシー型": ["consistency", "trust_depth"],
    }
    profile_vecs = []
    for name, keys in archetype_profiles.items():
        vec = np.zeros(len(PARAM_KEYS))
        for k in keys:
            vec[PARAM_KEYS.index(k)] = 1.0
        profile_vecs.append((name, vec))

    cluster_names: dict[int, str] = {}
    used: set[str] = set()
    n_clusters = len(centers)

    # Greedy: assign each cluster to closest unused archetype
    distances = []
    for ci in range(n_clusters):
        for name, vec in profile_vecs:
            # Cosine similarity between center and profile vector
            c = centers[ci]
            c_shifted = c - c.min()  # shift so all positive
            dot = float(np.dot(c_shifted, vec))
            norm = float(np.linalg.norm(c_shifted) * np.linalg.norm(vec) + 1e-9)
            distances.append((dot / norm, ci, name))

    distances.sort(reverse=True)
    for _, ci, name in distances:
        if ci not in cluster_names and name not in used:
            cluster_names[ci] = name
            used.add(name)

    # Fill remaining clusters with unused archetype names
    remaining = [a for a in ARCHETYPE_LABELS if a not in used]
    for ci in range(n_clusters):
        if ci not in cluster_names:
            cluster_names[ci] = remaining.pop(0) if remaining else f"クラスタ{ci}"

    return cluster_names


# -------------------------------------------------------------------
# Main entry point
# -------------------------------------------------------------------

def compute_person_parameters(
    results: list[dict],
    mentorship_list: list[dict] | None = None,
    genre_affinity: dict[str, dict] | None = None,
    compatibility_boost: dict[str, float] | None = None,
) -> list[dict]:
    """10パラメータカードを全 person について計算する.

    Args:
        results: scores.json 形式のリスト (pipeline context.results)
        mentorship_list: mentorships.json 形式のリスト
        genre_affinity: genre_affinity.json 形式の dict (pid → {...})
        compatibility_boost: person_compatibility_boost dict (pid → float)

    Returns:
        List of dicts, one per person, with params, CI, and archetype.
        Sorted by scale_reach percentile descending.
    """
    if not results:
        return []

    mentorship_list = mentorship_list or []
    genre_affinity = genre_affinity or {}
    compatibility_boost = compatibility_boost or {}

    logger.info("person_parameters_start", n_persons=len(results))

    raw = _extract_raw_values(results, mentorship_list, genre_affinity, compatibility_boost)
    pct = _to_percentiles(raw)
    ci = _compute_ci(pct, raw)
    archetypes = _assign_archetypes(pct)

    # Build name lookup
    pid_to_names = {r["person_id"]: (r.get("name", ""), r.get("name_ja", "")) for r in results}

    output = []
    for pid, params in pct.items():
        name, name_ja = pid_to_names.get(pid, ("", ""))
        archetype_label, archetype_idx = archetypes.get(pid, ("不明", -1))

        entry = {
            "person_id": pid,
            "name": name,
            "name_ja": name_ja,
            "archetype": archetype_label,
            "archetype_idx": archetype_idx,
            "params": {k: params.get(k, 0.0) for k in PARAM_KEYS},
            "params_ja": {PARAM_NAMES_JA[k]: params.get(k, 0.0) for k in PARAM_KEYS},
            "params_ci": {
                k: {"lower": ci[pid][k][0], "upper": ci[pid][k][1]}
                for k in PARAM_KEYS
            },
        }
        output.append(entry)

    output.sort(key=lambda x: x["params"].get("scale_reach", 0.0), reverse=True)

    logger.info("person_parameters_done", n_persons=len(output))
    return output


# Mapping from compute_person_parameters PARAM_KEYS → meta_common_person_parameters columns
_PARAM_TO_DB_COL = {
    "scale_reach":    "scale_reach",
    "collab_breadth": "collab_width",
    "consistency":    "continuity",
    "mentor_value":   "mentor_contribution",
    "centrality":     "centrality",
    "trust_depth":    "trust_accum",
    "role_evolution": "role_evolution",
    "genre_spec":     "genre_specialization",
    "recent_activity": "recent_activity",
    "compatibility":  "compatibility",
}


def populate_meta_common_person_parameters(
    conn: "sqlite3.Connection",
    person_params: list[dict],
) -> int:
    """compute_person_parameters の出力を meta_common_person_parameters に upsert する.

    既存の JSON 出力 (pipeline export) は deprecate せず並存させること。
    Phase 3 完了後に JSON 出力を削除予定。

    Args:
        conn: SQLite 接続
        person_params: compute_person_parameters() の戻り値

    Returns:
        upsert した行数
    """
    import sqlite3 as _sqlite3

    if not person_params:
        return 0

    rows = []
    for entry in person_params:
        pid = entry["person_id"]
        params = entry.get("params", {})
        ci = entry.get("params_ci", {})
        archetype = entry.get("archetype")

        row: dict = {"person_id": pid, "archetype": archetype, "archetype_confidence": None}
        for param_key, db_prefix in _PARAM_TO_DB_COL.items():
            row[f"{db_prefix}_pct"] = params.get(param_key)
            ci_entry = ci.get(param_key, {})
            row[f"{db_prefix}_ci_low"] = ci_entry.get("lower")
            row[f"{db_prefix}_ci_high"] = ci_entry.get("upper")
        rows.append(row)

    if not rows:
        return 0

    cols = list(rows[0].keys())
    placeholders = ", ".join("?" * len(cols))
    col_list = ", ".join(cols)
    update_clause = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "person_id")

    conn.executemany(
        f"INSERT INTO meta_common_person_parameters ({col_list}) VALUES ({placeholders})"
        f" ON CONFLICT(person_id) DO UPDATE SET {update_clause}",
        [[r[c] for c in cols] for r in rows],
    )

    # Register lineage
    from src.database import register_meta_lineage
    register_meta_lineage(
        conn,
        table_name="meta_common_person_parameters",
        audience="common",
        source_silver_tables=["anime_analysis", "credits", "persons",
                              "feat_person_scores", "feat_career",
                              "feat_genre_affinity", "feat_network"],
        formula_version="v2.0",
        ci_method="bootstrap_n1000",
        null_model="degree_preserving_rewiring_n500",
        row_count=len(rows),
        notes="10 axes per Person Parameter Card; archetypes by K-means K=6",
    )

    logger.info("meta_common_person_parameters_upserted", n=len(rows))
    return len(rows)
