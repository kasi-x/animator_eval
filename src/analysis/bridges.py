"""ブリッジ検出 — コミュニティ間をつなぐキーパーソンの特定.

グラフ理論のブリッジ概念を応用し、異なるコラボレーション
クラスター間を橋渡しする人物を検出する。
"""

from collections import defaultdict

import structlog

from src.models import Credit

logger = structlog.get_logger()


def detect_bridges(
    credits: list[Credit],
    communities: dict[str, int] | None = None,
) -> dict:
    """コミュニティ間ブリッジを検出する.

    Args:
        credits: クレジットリスト
        communities: {person_id: community_id} マッピング (None の場合は内部で計算)

    Returns:
        bridge_persons, cross_community_edges, community_connectivity
    """
    if not credits:
        return {"bridge_persons": [], "cross_community_edges": [], "stats": {}}

    # Build collaboration adjacency
    anime_persons: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        anime_persons[c.anime_id].add(c.person_id)

    # Build person-person edges with anime_ids
    edges: dict[tuple[str, str], list[str]] = defaultdict(list)
    all_persons: set[str] = set()
    for anime_id, persons in anime_persons.items():
        plist = sorted(persons)
        for i, p1 in enumerate(plist):
            all_persons.add(p1)
            for p2 in plist[i + 1 :]:
                edges[(p1, p2)].append(anime_id)

    if not all_persons:
        return {"bridge_persons": [], "cross_community_edges": [], "stats": {}}

    # If no communities provided, assign simple connected-component-based communities
    if communities is None:
        communities = _compute_simple_communities(all_persons, edges)

    # Find cross-community edges
    cross_edges = []
    person_cross_count: dict[str, int] = defaultdict(int)
    person_communities_touched: dict[str, set[int]] = defaultdict(set)

    for (p1, p2), anime_ids in edges.items():
        c1 = communities.get(p1, -1)
        c2 = communities.get(p2, -1)
        if c1 != c2 and c1 >= 0 and c2 >= 0:
            cross_edges.append({
                "person_a": p1,
                "person_b": p2,
                "community_a": c1,
                "community_b": c2,
                "shared_works": len(anime_ids),
            })
            person_cross_count[p1] += 1
            person_cross_count[p2] += 1
            person_communities_touched[p1].add(c1)
            person_communities_touched[p1].add(c2)
            person_communities_touched[p2].add(c1)
            person_communities_touched[p2].add(c2)

    # Rank bridge persons by number of cross-community connections
    bridge_persons = []
    for pid, count in sorted(person_cross_count.items(), key=lambda x: -x[1]):
        bridge_persons.append({
            "person_id": pid,
            "cross_community_edges": count,
            "communities_connected": len(person_communities_touched[pid]),
            "bridge_score": min(100, count * 10 + len(person_communities_touched[pid]) * 15),
        })

    # Community connectivity matrix
    num_communities = max(communities.values(), default=-1) + 1
    connectivity: dict[str, int] = {}
    for edge in cross_edges:
        key = f"{edge['community_a']}-{edge['community_b']}"
        connectivity[key] = connectivity.get(key, 0) + 1

    stats = {
        "total_persons": len(all_persons),
        "total_communities": num_communities,
        "total_cross_edges": len(cross_edges),
        "bridge_person_count": len(bridge_persons),
    }

    logger.info(
        "bridges_detected",
        bridge_persons=len(bridge_persons),
        cross_edges=len(cross_edges),
    )

    return {
        "bridge_persons": bridge_persons,
        "cross_community_edges": cross_edges,
        "community_connectivity": connectivity,
        "stats": stats,
    }


def _compute_simple_communities(
    persons: set[str],
    edges: dict[tuple[str, str], list[str]],
) -> dict[str, int]:
    """Union-Find で簡易コミュニティを計算."""
    parent: dict[str, str] = {p: p for p in persons}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for p1, p2 in edges:
        union(p1, p2)

    # Assign community IDs
    root_to_id: dict[str, int] = {}
    communities: dict[str, int] = {}
    for p in persons:
        root = find(p)
        if root not in root_to_id:
            root_to_id[root] = len(root_to_id)
        communities[p] = root_to_id[root]

    return communities
