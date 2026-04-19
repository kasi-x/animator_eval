"""チーム組成テンプレート — K=5 クラスタリング.

成功定義: scale_tier >= 4 の作品で構成されるチーム。
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()

_MIN_TEAM_SIZE = 3
_SUCCESS_TIER = 4


def cluster_team_patterns(
    credits: list[Any],
    anime_map: dict[str, Any],
    iv_scores: dict[str, float],
) -> dict[str, Any]:
    """K-means K=5 on team feature vectors.

    Features: size, role_diversity, career_diversity, fe_mean, fe_std.
    Silhouette gate: > 0.3.

    Returns {archetype: {centroid, success_rate, n_teams, example_animes}}
    """
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    from sklearn.preprocessing import StandardScaler
    from collections import Counter

    # Build per-anime team
    anime_team: dict[str, dict] = defaultdict(lambda: {"persons": [], "roles": []})
    for c in credits:
        if hasattr(c, "anime_id"):
            aid = str(c.anime_id)
            pid = str(c.person_id)
            role = str(getattr(c, "role", "unknown"))
        elif isinstance(c, dict):
            aid = str(c.get("anime_id", ""))
            pid = str(c.get("person_id", ""))
            role = str(c.get("role", "unknown"))
        else:
            continue
        if aid and pid:
            anime_team[aid]["persons"].append(pid)
            anime_team[aid]["roles"].append(role)

    # fe percentile lookup
    fe_values = np.array(list(iv_scores.values())) if iv_scores else np.array([0.5])
    fe_sorted = np.sort(fe_values)
    def _fe_pct(pid: str) -> float:
        fe = iv_scores.get(pid, 0.0) or 0.0
        idx = np.searchsorted(fe_sorted, fe)
        return float(idx / max(len(fe_sorted) - 1, 1))

    # Build feature vectors
    feature_vecs: list[tuple[str, np.ndarray, bool]] = []

    for aid, team in anime_team.items():
        persons = team["persons"]
        roles = team["roles"]
        if len(persons) < _MIN_TEAM_SIZE:
            continue

        # Determine success
        anime_obj = anime_map.get(aid)
        if hasattr(anime_obj, "scale_tier"):
            scale_tier = anime_obj.scale_tier or 0
        elif isinstance(anime_obj, dict):
            scale_tier = anime_obj.get("scale_tier") or 0
        else:
            scale_tier = 0
        success = scale_tier >= _SUCCESS_TIER

        size = len(set(persons))
        role_counts = Counter(roles)
        len(role_counts)
        role_entropy = float(
            -np.sum([c / sum(role_counts.values()) * np.log(c / sum(role_counts.values()) + 1e-9)
                     for c in role_counts.values()])
        )

        fe_pcts = [_fe_pct(p) for p in set(persons)]
        fe_mean = float(np.mean(fe_pcts))
        fe_std = float(np.std(fe_pcts)) if len(fe_pcts) > 1 else 0.0

        feature_vecs.append((aid, np.array([size, role_entropy, fe_mean, fe_std]), success))

    if len(feature_vecs) < 5:
        return {"error": "insufficient_teams", "n_teams": len(feature_vecs)}

    aids = [f[0] for f in feature_vecs]
    X = np.array([f[1] for f in feature_vecs])
    successes = np.array([f[2] for f in feature_vecs])

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    k = min(5, len(feature_vecs))
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(X_scaled)

    silhouette = float(silhouette_score(X_scaled, labels)) if k > 1 else 0.0

    archetype_names = [
        "大規模精鋭チーム",
        "多様役割バランス型",
        "小規模専門チーム",
        "新人育成型",
        "ベテラン主導型",
    ]

    results: dict = {}
    for cluster_idx in range(k):
        mask = labels == cluster_idx
        cluster_animes = [aids[i] for i in range(len(aids)) if mask[i]]
        X[mask]
        cluster_success = successes[mask]

        centroid_raw = scaler.inverse_transform(km.cluster_centers_[[cluster_idx]])[0]
        name = archetype_names[cluster_idx % len(archetype_names)]

        results[name] = {
            "n_teams": int(mask.sum()),
            "success_rate": round(float(cluster_success.mean()), 4),
            "centroid": {
                "size": round(float(centroid_raw[0]), 1),
                "role_entropy": round(float(centroid_raw[1]), 3),
                "fe_mean": round(float(centroid_raw[2]), 3),
                "fe_std": round(float(centroid_raw[3]), 3),
            },
            "example_animes": cluster_animes[:5],
        }

    return {
        "archetypes": results,
        "silhouette_score": round(silhouette, 4),
        "silhouette_gate_passed": silhouette > 0.3,
        "n_teams_total": len(feature_vecs),
        "method_notes": {
            "features": "size, role_entropy, fe_mean, fe_std",
            "success_def": f"scale_tier >= {_SUCCESS_TIER}",
            "silhouette_gate": "0.3",
        },
    }
