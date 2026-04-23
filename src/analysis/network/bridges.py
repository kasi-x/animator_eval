"""Bridge detection — identify key persons who connect communities.

グラフ理論のブリッジ概念を応用し、異なるコラボレーション
クラスター間を橋渡しする人物を検出する。

スコア設計の方針:
- コミュニティサイズを考慮: 小さなコミュニティ同士のブリッジは低スコア
- edge_value = sqrt(min(size_a, size_b)) × log1p(shared_works)
  → 「より小さい側のコミュニティサイズ」で価値を決める
  → 5人コミュニティ ↔ 5000人コミュニティ: sqrt(5) ≈ 2.2
  → 500人コミュニティ ↔ 500人コミュニティ: sqrt(500) ≈ 22.4
- 最終スコアは log スケールで 0-99 に正規化 (カンストなし)
"""

import math
from collections import Counter, defaultdict

import networkx as nx
import structlog

from src.models import Credit

logger = structlog.get_logger()


def detect_bridges(
    credits: list[Credit],
    communities: dict[str, int] | None = None,
    collaboration_graph: "nx.Graph | None" = None,
) -> dict:
    """Detect bridges between communities.

    Args:
        credits: クレジットリスト
        communities: {person_id: community_id} マッピング (None の場合は内部で計算)
        collaboration_graph: 既存のコラボグラフ (再利用で高速化)

    Returns:
        bridge_persons, cross_community_edges, community_connectivity
    """
    if not credits:
        return {"bridge_persons": [], "cross_community_edges": [], "stats": {}}

    if collaboration_graph is not None:
        # Fast path: use existing graph edges
        all_persons = set(collaboration_graph.nodes())
        edges: dict[tuple[str, str], int] = {}
        for u, v, data in collaboration_graph.edges(data=True):
            key = (u, v) if u < v else (v, u)
            edges[key] = int(data.get("shared_works", 1))
    else:
        # Slow path: build edges from credits
        anime_persons: dict[str, set[str]] = defaultdict(set)
        for c in credits:
            anime_persons[c.anime_id].add(c.person_id)

        import itertools

        edge_anime: dict[tuple[str, str], list[str]] = defaultdict(list)
        all_persons = set()
        for anime_id, persons in anime_persons.items():
            plist = sorted(persons)
            all_persons.update(plist)
            for p1, p2 in itertools.combinations(plist, 2):
                edge_anime[(p1, p2)].append(anime_id)
        edges = {k: len(v) for k, v in edge_anime.items()}

    if not all_persons:
        return {"bridge_persons": [], "cross_community_edges": [], "stats": {}}

    # If no communities provided, assign simple connected-component-based communities
    if communities is None:
        communities = _compute_simple_communities(all_persons, edges)

    # Community sizes — number of members per community ID
    community_sizes: dict[int, int] = Counter(communities.values())

    # Find cross-community edges + compute per-person weighted scores
    cross_edges = []
    person_cross_count: dict[str, int] = defaultdict(int)
    person_communities_touched: dict[str, set[int]] = defaultdict(set)
    # Weighted score: size-aware bridge value accumulator
    person_weighted_score: dict[str, float] = defaultdict(float)

    for (p1, p2), shared_count in edges.items():
        c1 = communities.get(p1, -1)
        c2 = communities.get(p2, -1)
        if c1 != c2 and c1 >= 0 and c2 >= 0:
            sw = shared_count if isinstance(shared_count, int) else len(shared_count)
            cross_edges.append(
                {
                    "person_a": p1,
                    "person_b": p2,
                    "community_a": c1,
                    "community_b": c2,
                    "shared_works": sw,
                }
            )
            person_cross_count[p1] += 1
            person_cross_count[p2] += 1
            person_communities_touched[p1].update({c1, c2})
            person_communities_touched[p2].update({c1, c2})

            # Community-size-weighted edge value:
            # Use the SMALLER of the two community sizes as the bottleneck.
            # A tiny community ↔ large community bridge is low value.
            # sqrt() dampens so 500-person community isn't 100x more than 5-person.
            size1 = community_sizes.get(c1, 1)
            size2 = community_sizes.get(c2, 1)
            size_weight = math.sqrt(min(size1, size2))
            # Stronger connections (more shared works) = higher value
            works_weight = math.log1p(sw)
            edge_value = size_weight * works_weight

            person_weighted_score[p1] += edge_value
            person_weighted_score[p2] += edge_value

    # Compute raw bridge score per person:
    # weighted cross edges + community diversity bonus
    raw_scores: dict[str, float] = {}
    for pid in person_cross_count:
        # Diversity bonus: reward touching many LARGE communities
        diversity = sum(
            math.log1p(community_sizes.get(c, 1))
            for c in person_communities_touched[pid]
        )
        raw_scores[pid] = person_weighted_score[pid] + diversity

    # Normalize to 0-99 using log scale.
    # log normalization gives natural spread without saturation:
    #   score = log1p(raw) / log1p(max_raw) * 99
    # The top person gets 99, and scores spread logarithmically below.
    max_raw = max(raw_scores.values(), default=1.0)
    log_max = math.log1p(max_raw)

    # Sort by raw score (community-size-aware) for ranking
    bridge_persons = []
    for pid in sorted(raw_scores, key=lambda p: -raw_scores[p]):
        raw = raw_scores[pid]
        bridge_score = round(math.log1p(raw) / log_max * 99) if log_max > 0 else 0
        bridge_persons.append(
            {
                "person_id": pid,
                "cross_community_edges": person_cross_count[pid],
                "communities_connected": len(person_communities_touched[pid]),
                "bridge_score": bridge_score,
                # Expose raw score for debugging / alternative sorting
                "raw_bridge_score": round(raw, 2),
            }
        )

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
    """Compute simple communities using Union-Find."""
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
