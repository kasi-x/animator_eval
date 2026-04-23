"""Independent unit formation potential — community viability score."""

from __future__ import annotations

from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()

_MIN_UNIT_SIZE = 10
_REQUIRED_ROLE_COVERAGE = 0.7
_ESSENTIAL_ROLES_COUNT = 12


def compute_unit_viability(
    community_map: dict[str, Any],
    credits: list[Any],
    anime_map: dict[str, Any],
    person_fe: dict[str, float],
    min_size: int = _MIN_UNIT_SIZE,
    required_role_coverage: float = _REQUIRED_ROLE_COVERAGE,
) -> dict[str, Any]:
    """V_G = coverage_G × density_G × mean(person_fe_pct_G).

    coverage = distinct essential roles covered / 12
    density = actual co-credit edges / possible (n*(n-1)/2)

    Returns {community_id: {viability, coverage, density, size, redundancy}}
    """
    from collections import defaultdict, Counter

    # fe percentile
    fe_values = np.array(list(person_fe.values())) if person_fe else np.array([0.5])
    fe_sorted = np.sort(fe_values)

    def _fe_pct(pid: str) -> float:
        fe = person_fe.get(pid, 0.0) or 0.0
        idx = np.searchsorted(fe_sorted, fe)
        return float(idx / max(len(fe_sorted) - 1, 1))

    # Person → roles and co-credits
    person_roles: dict[str, set] = defaultdict(set)
    anime_persons: dict[str, set] = defaultdict(set)

    for c in credits:
        if hasattr(c, "person_id"):
            pid = str(c.person_id)
            role = str(getattr(c, "role", "unknown"))
            aid = str(getattr(c, "anime_id", ""))
        elif isinstance(c, dict):
            pid = str(c.get("person_id", ""))
            role = str(c.get("role", "unknown"))
            aid = str(c.get("anime_id", ""))
        else:
            continue
        if pid:
            person_roles[pid].add(role)
        if aid and pid:
            anime_persons[aid].add(pid)

    # Co-credit edges
    co_credit_edges: dict[str, set] = defaultdict(set)
    for aid, persons in anime_persons.items():
        plist = sorted(persons)
        for i in range(len(plist)):
            for j in range(i + 1, len(plist)):
                co_credit_edges[plist[i]].add(plist[j])
                co_credit_edges[plist[j]].add(plist[i])

    # Communities
    communities = community_map.get("communities", {})
    if not communities:
        communities = {
            k: v for k, v in community_map.items() if isinstance(v, (list, dict))
        }

    results: dict = {}
    for comm_id, community in communities.items():
        if isinstance(community, dict):
            members = community.get("members", []) or community.get("persons", [])
        elif isinstance(community, list):
            members = community
        else:
            continue

        members = [str(m) for m in members]
        n = len(members)
        if n < min_size:
            continue

        # Role coverage
        all_roles = set()
        for pid in members:
            all_roles.update(person_roles.get(pid, set()))
        coverage = min(1.0, len(all_roles) / _ESSENTIAL_ROLES_COUNT)

        # Density = internal edges / possible
        member_set = set(members)
        internal_edges = (
            sum(len(co_credit_edges.get(pid, set()) & member_set) for pid in members)
            // 2
        )
        max_edges = n * (n - 1) // 2
        density = internal_edges / max_edges if max_edges > 0 else 0.0

        # Mean fe percentile
        fe_pcts = [_fe_pct(pid) for pid in members if pid in person_fe]
        mean_fe_pct = float(np.mean(fe_pcts)) if fe_pcts else 0.5

        viability = coverage * density * mean_fe_pct

        # Redundancy: per role, how many members have it
        role_counts = Counter()
        for pid in members:
            for role in person_roles.get(pid, set()):
                role_counts[role] += 1
        redundancy = {
            role: count for role, count in role_counts.most_common(10) if count >= 2
        }

        results[str(comm_id)] = {
            "viability": round(float(viability), 4),
            "coverage": round(float(coverage), 4),
            "density": round(float(density), 4),
            "mean_fe_pct": round(float(mean_fe_pct), 4),
            "size": n,
            "n_distinct_roles": len(all_roles),
            "redundancy_top_roles": redundancy,
        }

    return results


def run_independent_units(
    community_map: dict[str, Any],
    credits: list[Any],
    anime_map: dict[str, Any],
    person_fe: dict[str, float],
) -> dict[str, Any]:
    """Independent unit formation potential — main entry point."""
    if not community_map:
        return {"error": "no_community_map"}

    viability = compute_unit_viability(community_map, credits, anime_map, person_fe)

    if not viability:
        return {"error": "no_viable_communities", "n_evaluated": 0}

    viable = {
        cid: d
        for cid, d in viability.items()
        if d["coverage"] >= _REQUIRED_ROLE_COVERAGE
    }

    ranked = sorted(viable.items(), key=lambda x: x[1]["viability"], reverse=True)

    return {
        "n_communities_evaluated": len(viability),
        "n_viable_communities": len(viable),
        "top_10_viable": dict(ranked[:10]),
        "all_viability": viability,
        "method_notes": {
            "viability": "coverage × density × mean_fe_pct",
            "coverage": f"distinct roles / {_ESSENTIAL_ROLES_COUNT} essential roles",
            "density": "internal co-credit edges / possible edges",
            "min_size": _MIN_UNIT_SIZE,
        },
    }
