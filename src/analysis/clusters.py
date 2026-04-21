"""クラスター分析 — 人物をコラボレーションパターンでグループ化する.

制作現場はスタジオ単位ではなく、監督やプロデューサーを中心とした
チーム単位で動くことが多い。グラフコミュニティ検出で自然なクラスターを特定する。
"""

from collections import defaultdict

import networkx as nx
import structlog

from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


def detect_collaboration_clusters(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    min_shared_works: int = 2,
    resolution: float = 1.0,
) -> dict:
    """コラボレーションベースのクラスターを検出する.

    Louvain コミュニティ検出を使用して、頻繁に共同作業する人物グループを特定する。

    Args:
        credits: クレジットリスト
        anime_map: アニメID → Animeマップ
        min_shared_works: エッジ生成に必要な最小共有作品数
        resolution: Louvain の解像度パラメータ

    Returns:
        {
            "clusters": [{id, members: [person_id], size, avg_shared_works}],
            "person_to_cluster": {person_id: cluster_id},
            "total_clusters": int,
        }
    """
    # Build collaboration graph
    anime_persons: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        anime_persons[c.anime_id].add(c.person_id)

    # Count shared works between person pairs
    collab_counts: dict[tuple[str, str], int] = defaultdict(int)
    for anime_id, pids in anime_persons.items():
        pids_sorted = sorted(pids)
        for i in range(len(pids_sorted)):
            for j in range(i + 1, min(i + 50, len(pids_sorted))):  # Cap for large casts
                collab_counts[(pids_sorted[i], pids_sorted[j])] += 1

    # Build graph with edges >= min_shared_works
    G = nx.Graph()
    for (p1, p2), count in collab_counts.items():
        if count >= min_shared_works:
            G.add_edge(p1, p2, weight=count)

    if G.number_of_nodes() < 3:
        return {"clusters": [], "person_to_cluster": {}, "total_clusters": 0}

    # Louvain community detection
    communities = nx.community.louvain_communities(
        G, weight="weight", resolution=resolution, seed=42
    )

    clusters = []
    person_to_cluster: dict[str, int] = {}
    for idx, community in enumerate(communities):
        members = sorted(community)
        if len(members) < 2:
            continue

        # Calculate average shared works within cluster
        internal_edges = [
            G[u][v]["weight"]
            for u in members
            for v in members
            if u < v and G.has_edge(u, v)
        ]
        avg_shared = sum(internal_edges) / len(internal_edges) if internal_edges else 0

        cluster_id = idx
        clusters.append(
            {
                "id": cluster_id,
                "members": members,
                "size": len(members),
                "avg_shared_works": round(avg_shared, 1),
                "internal_edges": len(internal_edges),
            }
        )
        for pid in members:
            person_to_cluster[pid] = cluster_id

    clusters.sort(key=lambda c: c["size"], reverse=True)

    logger.info(
        "cluster_detection_complete",
        clusters=len(clusters),
        persons_in_clusters=len(person_to_cluster),
        total_persons=G.number_of_nodes(),
    )

    return {
        "clusters": clusters,
        "person_to_cluster": person_to_cluster,
        "total_clusters": len(clusters),
    }


def compute_cluster_stats(
    clusters: dict,
    person_scores: dict[str, float] | None = None,
) -> list[dict]:
    """各クラスターの統計情報を計算する.

    Args:
        clusters: detect_collaboration_clusters の戻り値
        person_scores: {person_id: composite_score}

    Returns:
        クラスター統計のリスト（サイズ降順）
    """
    if not clusters.get("clusters"):
        return []

    result = []
    for cluster in clusters["clusters"]:
        stats: dict = {
            "cluster_id": cluster["id"],
            "size": cluster["size"],
            "avg_shared_works": cluster["avg_shared_works"],
            "internal_edges": cluster["internal_edges"],
        }
        if person_scores:
            member_scores = [
                person_scores[pid] for pid in cluster["members"] if pid in person_scores
            ]
            if member_scores:
                stats["avg_score"] = round(sum(member_scores) / len(member_scores), 1)
                stats["max_score"] = round(max(member_scores), 1)
                stats["min_score"] = round(min(member_scores), 1)
        result.append(stats)

    return result
