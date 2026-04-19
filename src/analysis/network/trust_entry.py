"""信頼ネット参入経路 — ゲートキーパースコア / リーチ指標.

Input (from existing JSON + context):
    bridges_result: result of detect_bridges() — {bridge_persons: [...]}
    person_fe: {person_id: float}
    birank_person_scores: {person_id: float}
    collaboration_graph: networkx.Graph (optional)
"""

from __future__ import annotations

from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()


def compute_gatekeeper_scores(
    bridges_result: dict[str, Any],
    person_fe: dict[str, float],
    birank_person_scores: dict[str, float],
) -> dict[str, Any]:
    """G_p = z(betweenness) + z(distinct_studios) + z(person_fe_pct) + z(bridge_score).

    Returns {top_100: [{person_id, gatekeeper_score, components}], all_scores: {}}
    """
    bridge_persons = bridges_result.get("bridge_persons", [])
    if not bridge_persons:
        return {"error": "no_bridge_persons", "top_100": []}

    def _z_score(values: list[float]) -> list[float]:
        arr = np.array(values, dtype=float)
        mu, sigma = arr.mean(), arr.std()
        return [(v - mu) / sigma if sigma > 0 else 0.0 for v in values]

    betweenness_vals = [float(p.get("betweenness_centrality") or p.get("betweenness") or 0.0)
                        for p in bridge_persons]
    bridge_score_vals = [float(p.get("bridge_score") or 0.0) for p in bridge_persons]

    # fe percentile
    fe_values = np.array(list(person_fe.values()))
    fe_sorted = np.sort(fe_values)
    def _fe_pct(fe: float) -> float:
        idx = np.searchsorted(fe_sorted, fe)
        return float(idx / max(len(fe_sorted) - 1, 1) * 100)

    fe_pcts = [_fe_pct(person_fe.get(p.get("person_id", ""), 0.0)) for p in bridge_persons]
    birank_vals = [float(birank_person_scores.get(p.get("person_id", ""), 0.0))
                   for p in bridge_persons]

    z_between = _z_score(betweenness_vals)
    z_bridge = _z_score(bridge_score_vals)
    z_fe = _z_score(fe_pcts)
    z_birank = _z_score(birank_vals)

    all_scores: dict = {}
    for i, p in enumerate(bridge_persons):
        pid = p.get("person_id", "")
        g_score = z_between[i] + z_fe[i] + z_birank[i] + z_bridge[i]
        all_scores[pid] = {
            "gatekeeper_score": round(g_score, 4),
            "z_betweenness": round(z_between[i], 4),
            "z_fe": round(z_fe[i], 4),
            "z_birank": round(z_birank[i], 4),
            "z_bridge": round(z_bridge[i], 4),
        }

    top_100 = sorted(all_scores.items(), key=lambda x: x[1]["gatekeeper_score"], reverse=True)[:100]

    return {
        "top_100": [{"person_id": pid, **d} for pid, d in top_100],
        "n_candidates": len(all_scores),
    }


def compute_reach_metric(
    gatekeeper_pool: list[dict],
    collaboration_graph: Any = None,
    total_persons: int = 0,
) -> dict[str, Any]:
    """Reach_p = |2-hop neighbors| / |V| for each gatekeeper.

    Falls back to betweenness-based estimate if graph not available.

    Returns {person_id: reach_fraction}
    """
    if collaboration_graph is None or total_persons == 0:
        # Estimate from gatekeeper score rank
        n = len(gatekeeper_pool)
        if n == 0:
            return {}
        results = {}
        for i, gk in enumerate(gatekeeper_pool):
            pid = gk.get("person_id", "")
            # Rough estimate: top gatekeepers reach ~30% of network
            rank_fraction = 1.0 - i / n
            reach_est = 0.05 + 0.25 * rank_fraction
            results[pid] = {
                "reach_fraction": round(reach_est, 4),
                "method": "rank_estimate",
            }
        return results

    # Actual 2-hop neighborhood
    results = {}
    n_total = len(collaboration_graph.nodes()) or total_persons or 1
    for gk in gatekeeper_pool:
        pid = gk.get("person_id", "")
        if pid not in collaboration_graph:
            continue
        neighbors_1 = set(collaboration_graph.neighbors(pid))
        neighbors_2 = set()
        for nb in neighbors_1:
            neighbors_2.update(collaboration_graph.neighbors(nb))
        neighbors_2.discard(pid)
        reach = len(neighbors_1 | neighbors_2)
        results[pid] = {
            "reach_fraction": round(reach / n_total, 4),
            "n_1hop": len(neighbors_1),
            "n_2hop": len(neighbors_2 - neighbors_1),
            "method": "graph_bfs",
        }

    return results


def run_trust_entry_analysis(
    bridges_result: dict[str, Any],
    person_fe: dict[str, float],
    birank_person_scores: dict[str, float],
    collaboration_graph: Any = None,
) -> dict[str, Any]:
    """信頼ネット参入経路 — メインエントリポイント."""
    if not bridges_result or not person_fe:
        return {"error": "missing_inputs"}

    gatekeeper = compute_gatekeeper_scores(bridges_result, person_fe, birank_person_scores)
    top_100 = gatekeeper.get("top_100", [])

    reach = compute_reach_metric(
        top_100,
        collaboration_graph,
        total_persons=len(person_fe),
    )

    return {
        "gatekeepers": gatekeeper,
        "reach": reach,
        "n_gatekeepers": len(top_100),
        "method_notes": {
            "gatekeeper": "z(betweenness) + z(fe_pct) + z(birank) + z(bridge_score)",
            "reach": "2-hop neighbors / |V|, graph-based if available else rank estimate",
        },
    }
