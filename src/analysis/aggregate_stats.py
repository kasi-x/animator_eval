"""Aggregate statistics — generate a statistical summary over all pipeline results.

各軸のスコア分布、パーセンタイル、業界全体の健全性指標を算出。
"""

import structlog

logger = structlog.get_logger()


def compute_aggregate_stats(results: list[dict]) -> dict:
    """Compute aggregate statistics from pipeline results.

    Args:
        results: スコア結果リスト

    Returns:
        {
            "score_distribution": {axis: {min, max, mean, median, std, p25, p75}},
            "role_breakdown": {role: {count, avg_iv_score}},
            "career_stats": {avg_active_years, avg_highest_stage, ...},
            "network_stats": {avg_hub_score, avg_collaborators, ...},
        }
    """
    if not results:
        return {}

    n = len(results)

    # Score distributions
    axes = ("birank", "patronage", "person_fe", "iv_score")
    score_dist: dict[str, dict] = {}

    for axis in axes:
        vals = sorted(r.get(axis, 0) for r in results)
        if not vals:
            continue
        mean = sum(vals) / len(vals)
        variance = sum((v - mean) ** 2 for v in vals) / len(vals)
        std = variance**0.5

        score_dist[axis] = {
            "min": round(vals[0], 2),
            "max": round(vals[-1], 2),
            "mean": round(mean, 2),
            "median": round(vals[n // 2], 2),
            "std": round(std, 2),
            "p25": round(vals[n // 4], 2),
            "p75": round(vals[(3 * n) // 4], 2),
        }

    # Role breakdown
    role_groups: dict[str, list[float]] = {}
    for r in results:
        role = r.get("primary_role", "unknown")
        if role not in role_groups:
            role_groups[role] = []
        role_groups[role].append(r.get("iv_score", 0))

    role_breakdown = {}
    for role, iv_scores in role_groups.items():
        role_breakdown[role] = {
            "count": len(iv_scores),
            "avg_iv_score": round(sum(iv_scores) / len(iv_scores), 2),
            "max_iv_score": round(max(iv_scores), 2),
        }

    # Career stats
    career_active = [
        r["career"]["active_years"]
        for r in results
        if r.get("career", {}).get("active_years")
    ]
    career_stages = [
        r["career"]["highest_stage"]
        for r in results
        if r.get("career", {}).get("highest_stage")
    ]

    career_stats = {}
    if career_active:
        career_stats["avg_active_years"] = round(
            sum(career_active) / len(career_active), 1
        )
        career_stats["max_active_years"] = max(career_active)
    if career_stages:
        career_stats["avg_highest_stage"] = round(
            sum(career_stages) / len(career_stages), 1
        )

    # Network stats
    hub_scores = [
        r["network"]["hub_score"]
        for r in results
        if r.get("network", {}).get("hub_score") is not None
    ]
    collaborators = [
        r["network"]["collaborators"]
        for r in results
        if r.get("network", {}).get("collaborators") is not None
    ]

    network_stats = {}
    if hub_scores:
        network_stats["avg_hub_score"] = round(sum(hub_scores) / len(hub_scores), 1)
    if collaborators:
        network_stats["avg_collaborators"] = round(
            sum(collaborators) / len(collaborators), 1
        )
        network_stats["max_collaborators"] = max(collaborators)

    result = {
        "total_persons": n,
        "score_distribution": score_dist,
        "role_breakdown": role_breakdown,
        "career_stats": career_stats,
        "network_stats": network_stats,
    }

    logger.info("aggregate_stats_computed", persons=n)
    return result
