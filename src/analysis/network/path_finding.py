"""Path Finding — collaboration path search between creators.

2人のクリエイター間の最短コラボ経路、全経路、ボトルネック検出を行う。
「6次の隔たり」理論の検証や、意外な繋がりの発見に活用。
"""

from dataclasses import dataclass, field

import networkx as nx
import structlog


logger = structlog.get_logger()


@dataclass
class CollaborationPath:
    """Collaboration path.

    Attributes:
        source: 始点のperson_id
        target: 終点のperson_id
        path: 経路上のperson_idリスト（source → target）
        length: 経路長（ホップ数）
        total_weight: 経路上の全エッジ重みの合計
        shared_works: 経路上の各エッジでの共同作品情報
    """

    source: str
    target: str
    path: list[str] = field(default_factory=list)
    length: int = 0
    total_weight: float = 0.0
    shared_works: list[dict] = field(default_factory=list)


def find_shortest_path(
    collaboration_graph: nx.Graph,
    source: str,
    target: str,
    weight: str | None = "weight",
) -> CollaborationPath | None:
    """Find the shortest path between two creators.

    Args:
        collaboration_graph: コラボレーショングラフ
        source: 始点のperson_id
        target: 終点のperson_id
        weight: エッジ重み属性名（Noneで重みなし）

    Returns:
        最短経路情報、見つからない場合None
    """
    if source not in collaboration_graph or target not in collaboration_graph:
        logger.warning(
            "path_finding_node_not_found",
            source=source,
            target=target,
            source_exists=source in collaboration_graph,
            target_exists=target in collaboration_graph,
        )
        return None

    if source == target:
        return CollaborationPath(
            source=source,
            target=target,
            path=[source],
            length=0,
            total_weight=0.0,
        )

    try:
        if weight:
            # weighted shortest path (Dijkstra)
            path = nx.dijkstra_path(collaboration_graph, source, target, weight=weight)
            path_weight = nx.dijkstra_path_length(
                collaboration_graph, source, target, weight=weight
            )
        else:
            # unweighted shortest path (BFS)
            path = nx.shortest_path(collaboration_graph, source, target)
            path_weight = len(path) - 1

        # collect edge info along the path
        shared_works_info = []
        for i in range(len(path) - 1):
            edge_data = collaboration_graph.get_edge_data(path[i], path[i + 1])
            shared_works_info.append(
                {
                    "from": path[i],
                    "to": path[i + 1],
                    "weight": edge_data.get("weight", 0),
                    "shared_works": edge_data.get("shared_works", 0),
                }
            )

        result = CollaborationPath(
            source=source,
            target=target,
            path=path,
            length=len(path) - 1,
            total_weight=round(path_weight, 2),
            shared_works=shared_works_info,
        )

        logger.info(
            "shortest_path_found",
            source=source,
            target=target,
            length=result.length,
            weight=result.total_weight,
        )

        return result

    except nx.NetworkXNoPath:
        logger.warning("path_finding_no_path", source=source, target=target)
        return None
    except Exception as e:
        logger.error("path_finding_error", source=source, target=target, error=str(e))
        return None


def find_all_shortest_paths(
    collaboration_graph: nx.Graph,
    source: str,
    target: str,
    cutoff: int | None = None,
) -> list[CollaborationPath]:
    """Find all shortest paths between two creators.

    同じ長さの最短経路が複数ある場合、全てを返す。

    Args:
        collaboration_graph: コラボレーショングラフ
        source: 始点のperson_id
        target: 終点のperson_id
        cutoff: 最大経路長（これより長い経路は探索しない）

    Returns:
        最短経路のリスト（全て同じ長さ）
    """
    if source not in collaboration_graph or target not in collaboration_graph:
        return []

    if source == target:
        return [
            CollaborationPath(source=source, target=target, path=[source], length=0)
        ]

    try:
        all_paths = nx.all_shortest_paths(collaboration_graph, source, target)

        results = []
        for path in all_paths:
            # check path length
            if cutoff and len(path) - 1 > cutoff:
                continue

            # collect edge info
            total_weight = 0.0
            shared_works_info = []
            for i in range(len(path) - 1):
                edge_data = collaboration_graph.get_edge_data(path[i], path[i + 1])
                weight = edge_data.get("weight", 0)
                total_weight += weight
                shared_works_info.append(
                    {
                        "from": path[i],
                        "to": path[i + 1],
                        "weight": weight,
                        "shared_works": edge_data.get("shared_works", 0),
                    }
                )

            results.append(
                CollaborationPath(
                    source=source,
                    target=target,
                    path=path,
                    length=len(path) - 1,
                    total_weight=round(total_weight, 2),
                    shared_works=shared_works_info,
                )
            )

        logger.info(
            "all_shortest_paths_found",
            source=source,
            target=target,
            count=len(results),
            length=results[0].length if results else None,
        )

        return results

    except nx.NetworkXNoPath:
        logger.warning("path_finding_no_paths", source=source, target=target)
        return []
    except Exception as e:
        logger.error(
            "path_finding_error_all", source=source, target=target, error=str(e)
        )
        return []


def find_all_simple_paths(
    collaboration_graph: nx.Graph,
    source: str,
    target: str,
    cutoff: int = 5,
) -> list[CollaborationPath]:
    """Find all simple paths between two creators.

    単純経路: 同じノードを2度通らない経路

    Args:
        collaboration_graph: コラボレーショングラフ
        source: 始点のperson_id
        target: 終点のperson_id
        cutoff: 最大経路長

    Returns:
        単純経路のリスト（長さ順）
    """
    if source not in collaboration_graph or target not in collaboration_graph:
        return []

    if source == target:
        return [
            CollaborationPath(source=source, target=target, path=[source], length=0)
        ]

    try:
        all_paths = nx.all_simple_paths(
            collaboration_graph, source, target, cutoff=cutoff
        )

        results = []
        for path in all_paths:
            # collect edge info
            total_weight = 0.0
            shared_works_info = []
            for i in range(len(path) - 1):
                edge_data = collaboration_graph.get_edge_data(path[i], path[i + 1])
                weight = edge_data.get("weight", 0)
                total_weight += weight
                shared_works_info.append(
                    {
                        "from": path[i],
                        "to": path[i + 1],
                        "weight": weight,
                        "shared_works": edge_data.get("shared_works", 0),
                    }
                )

            results.append(
                CollaborationPath(
                    source=source,
                    target=target,
                    path=path,
                    length=len(path) - 1,
                    total_weight=round(total_weight, 2),
                    shared_works=shared_works_info,
                )
            )

        # sort by length
        results.sort(key=lambda x: (x.length, -x.total_weight))

        logger.info(
            "all_simple_paths_found",
            source=source,
            target=target,
            count=len(results),
            cutoff=cutoff,
        )

        return results

    except Exception as e:
        logger.error(
            "path_finding_error_simple", source=source, target=target, error=str(e)
        )
        return []


def compute_separation_statistics(
    collaboration_graph: nx.Graph,
    sample_size: int = 100,
) -> dict:
    """Compute network-wide separation statistics.

    ランダムサンプリングで平均経路長、最大経路長、連結成分などを算出。

    Args:
        collaboration_graph: コラボレーショングラフ
        sample_size: サンプリング数

    Returns:
        統計情報の辞書
    """
    if collaboration_graph.number_of_nodes() == 0:
        return {}

    # connected component analysis
    if nx.is_connected(collaboration_graph):
        largest_cc = collaboration_graph
        n_components = 1
    else:
        components = list(nx.connected_components(collaboration_graph))
        n_components = len(components)
        largest_cc = collaboration_graph.subgraph(max(components, key=len)).copy()

    # compute path lengths only on the largest connected component
    if largest_cc.number_of_nodes() < 2:
        return {
            "n_components": n_components,
            "largest_component_size": largest_cc.number_of_nodes(),
            "avg_path_length": 0,
            "diameter": 0,
        }

    # average path length and diameter
    try:
        avg_path_length = nx.average_shortest_path_length(largest_cc, weight="weight")
        diameter = nx.diameter(largest_cc)
    except Exception:
        # sample if the graph is too large
        import random

        nodes = list(largest_cc.nodes())
        sampled_lengths = []

        for _ in range(min(sample_size, len(nodes))):
            source = random.choice(nodes)
            target = random.choice(nodes)
            if source != target:
                try:
                    length = nx.shortest_path_length(largest_cc, source, target)
                    sampled_lengths.append(length)
                except nx.NetworkXNoPath:
                    pass

        avg_path_length = (
            sum(sampled_lengths) / len(sampled_lengths) if sampled_lengths else 0
        )
        diameter = max(sampled_lengths) if sampled_lengths else 0

    stats = {
        "n_components": n_components,
        "largest_component_size": largest_cc.number_of_nodes(),
        "largest_component_pct": round(
            100 * largest_cc.number_of_nodes() / collaboration_graph.number_of_nodes(),
            1,
        ),
        "avg_path_length": round(avg_path_length, 2),
        "diameter": diameter,
        "avg_degree": round(
            sum(dict(collaboration_graph.degree()).values())
            / collaboration_graph.number_of_nodes(),
            1,
        ),
    }

    logger.info("separation_statistics_computed", **stats)
    return stats


def find_bottleneck_nodes(
    collaboration_graph: nx.Graph,
    top_n: int = 10,
) -> list[tuple[str, float]]:
    """Identify network bottlenecks (critical relay nodes).

    Betweenness Centrality が高いノード = 多くの経路が通過する重要人物

    Args:
        collaboration_graph: コラボレーショングラフ
        top_n: 上位何件を返すか

    Returns:
        [(person_id, betweenness_score), ...] のリスト
    """
    if collaboration_graph.number_of_nodes() == 0:
        return []

    # use approximation for large graphs
    if collaboration_graph.number_of_nodes() > 1000:
        betweenness = nx.betweenness_centrality(
            collaboration_graph,
            k=min(100, collaboration_graph.number_of_nodes()),
            weight="weight",
        )
    else:
        betweenness = nx.betweenness_centrality(collaboration_graph, weight="weight")

    # extract top-N
    top_bottlenecks = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[
        :top_n
    ]

    logger.info("bottleneck_nodes_found", count=len(top_bottlenecks))
    return [(node, round(score, 4)) for node, score in top_bottlenecks]


def main():
    """Standalone entry point."""
    from src.analysis.graph import create_person_collaboration_network
    from src.analysis.io.silver_reader import (
        load_anime_silver,
        load_credits_silver,
        load_persons_silver,
    )

    persons = load_persons_silver()
    anime_list = load_anime_silver()
    credits = load_credits_silver()

    # build lookup maps
    anime_map = {a.id: a for a in anime_list}
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}

    # build collaboration graph
    logger.info("building_collaboration_graph")
    collab_graph = create_person_collaboration_network(credits, anime_map)

    # compute statistics
    stats = compute_separation_statistics(collab_graph)
    print("\nネットワーク統計:")
    print(f"  連結成分数: {stats.get('n_components', 'N/A')}")
    print(
        f"  最大成分サイズ: {stats.get('largest_component_size', 'N/A')} ({stats.get('largest_component_pct', 'N/A')}%)"
    )
    print(f"  平均経路長: {stats.get('avg_path_length', 'N/A')}")
    print(f"  Diameter: {stats.get('diameter', 'N/A')}")

    # bottleneck detection
    bottlenecks = find_bottleneck_nodes(collab_graph, top_n=5)
    print("\nボトルネック人物（重要中継点）:")
    for person_id, score in bottlenecks:
        print(f"  - {person_names.get(person_id, person_id)}: {score:.4f}")

    # path search example (between top 2 persons)
    if len(persons) >= 2:
        source_id = persons[0].id
        target_id = persons[1].id

        print(f"\n経路探索例: {person_names[source_id]} → {person_names[target_id]}")

        shortest = find_shortest_path(collab_graph, source_id, target_id)
        if shortest:
            print(f"  最短経路長: {shortest.length} ホップ")
            print(
                f"  経路: {' → '.join(person_names.get(p, p) for p in shortest.path)}"
            )



if __name__ == "__main__":
    main()
