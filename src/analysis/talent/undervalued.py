"""Undervalued talent pool — U_p score / K=5 archetypes.

Input (all from in-memory or JSON):
    expected_ability: {person_id: {expected, actual, gap}}
    scores: [{person_id, iv_score, person_fe, dormancy, ...}]
    genre_affinity: [{person_id, genre, score, ...}]
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()

# archetype names (K=5)
_ARCHETYPE_NAMES = [
    "育児・介護復帰型",
    "スタジオ倒産型",
    "意図的セーブ型",
    "引退移行型",
    "他業界型",
]


def compute_undervaluation_score(
    expected_ability: dict[str, dict],
    scores: list[dict],
) -> dict[str, Any]:
    """U_p = percentile(person_fe) - percentile(recent_3y_credit_count).

    Structural underexposure: U_p >= 30 points.

    Returns {person_id: {u_score, is_structural, fe_pct, activity_pct}}
    """
    {s["person_id"]: s for s in scores if "person_id" in s}

    # fe percentile
    fe_values = np.array([s.get("person_fe", 0.0) for s in scores if "person_fe" in s])
    if len(fe_values) == 0:
        return {}

    fe_sorted = np.sort(fe_values)

    # recent_credits from expected_ability or scores
    activity_values = np.array(
        [s.get("total_credits", 0) or s.get("recent_credits", 0) for s in scores],
        dtype=float,
    )
    act_sorted = np.sort(activity_values)

    def _pct(arr_sorted: np.ndarray, v: float) -> float:
        idx = np.searchsorted(arr_sorted, v)
        return float(idx / max(len(arr_sorted) - 1, 1) * 99)

    results: dict = {}
    for s in scores:
        pid = s.get("person_id")
        if not pid:
            continue

        fe = s.get("person_fe", 0.0) or 0.0
        activity = s.get("total_credits", 0) or s.get("recent_credits", 0) or 0

        fe_pct = _pct(fe_sorted, fe)
        act_pct = _pct(act_sorted, float(activity))
        u_score = fe_pct - act_pct

        results[pid] = {
            "u_score": round(u_score, 2),
            "is_structural": u_score >= 30,
            "fe_pct": round(fe_pct, 2),
            "activity_pct": round(act_pct, 2),
        }

    return results


def cluster_undervaluation_archetypes(
    undervalued_pool: dict[str, Any],
    scores: list[dict],
) -> dict[str, Any]:
    """K-means K=5 clustering → archetype naming.

    Features: u_score, fe_pct, activity_pct, dormancy (from scores)

    Returns {archetype_name: {count, avg_u_score, avg_fe_pct, avg_activity_pct}}
    """
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    score_map = {s["person_id"]: s for s in scores if "person_id" in s}

    pool_pids = [pid for pid, d in undervalued_pool.items() if d.get("is_structural")]
    if len(pool_pids) < 5:
        return {"error": "insufficient_undervalued_pool", "n": len(pool_pids)}

    feature_matrix = []
    valid_pids = []
    for pid in pool_pids:
        d = undervalued_pool[pid]
        s = score_map.get(pid, {})
        dormancy = s.get("dormancy", 1.0) or 1.0
        feature_matrix.append(
            [
                d["u_score"],
                d["fe_pct"],
                d["activity_pct"],
                float(dormancy),
            ]
        )
        valid_pids.append(pid)

    X = np.array(feature_matrix)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    k = min(5, len(valid_pids))
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(X_scaled)

    # Name archetypes by dominant feature profile
    cluster_data: dict[int, list] = defaultdict(list)
    for i, pid in enumerate(valid_pids):
        cluster_data[int(labels[i])].append({"pid": pid, "features": feature_matrix[i]})

    archetypes: dict = {}
    scaler.inverse_transform(km.cluster_centers_)

    for cluster_idx in range(k):
        items = cluster_data.get(cluster_idx, [])
        if not items:
            continue
        feats = np.array([d["features"] for d in items])
        avg_u = float(feats[:, 0].mean())
        avg_fe = float(feats[:, 1].mean())
        avg_act = float(feats[:, 2].mean())
        avg_dorm = float(feats[:, 3].mean())

        archetype_name = _ARCHETYPE_NAMES[cluster_idx % len(_ARCHETYPE_NAMES)]

        archetypes[archetype_name] = {
            "count": len(items),
            "avg_u_score": round(avg_u, 2),
            "avg_fe_pct": round(avg_fe, 2),
            "avg_activity_pct": round(avg_act, 2),
            "avg_dormancy": round(avg_dorm, 4),
        }

    return archetypes


def run_undervalued_talent(
    expected_ability: dict[str, dict],
    scores: list[dict],
    genre_affinity: list[dict],
) -> dict[str, Any]:
    """Undervalued talent pool — main entry point."""
    if not scores:
        return {"error": "no_scores_data"}

    u_scores = compute_undervaluation_score(expected_ability, scores)
    archetypes = cluster_undervaluation_archetypes(u_scores, scores)

    structural_pool = {pid: d for pid, d in u_scores.items() if d.get("is_structural")}
    n_total = len(u_scores)
    n_structural = len(structural_pool)

    # recovery signals: high u_score AND has any recent credit
    score_map = {s["person_id"]: s for s in scores if "person_id" in s}
    recovery_candidates = sum(
        1
        for pid, d in structural_pool.items()
        if (score_map.get(pid, {}).get("recent_credits", 0) or 0) > 0
        and d["fe_pct"] >= 60
    )

    return {
        "n_total_scored": n_total,
        "n_structural_undervalued": n_structural,
        "structural_rate": round(n_structural / n_total, 4) if n_total > 0 else 0.0,
        "recovery_signal_count": recovery_candidates,
        "u_score_distribution": {
            "p25": round(
                float(np.percentile([d["u_score"] for d in u_scores.values()], 25)), 2
            ),
            "p50": round(
                float(np.percentile([d["u_score"] for d in u_scores.values()], 50)), 2
            ),
            "p75": round(
                float(np.percentile([d["u_score"] for d in u_scores.values()], 75)), 2
            ),
        },
        "archetypes": archetypes,
        "method_notes": {
            "u_score": "percentile(person_fe) - percentile(total_credits), threshold=30",
            "structural_underexposure": "U_p >= 30",
            "archetypes": "K-means K=5, features: u_score / fe_pct / activity_pct / dormancy",
        },
    }
