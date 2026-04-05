"""Core-Periphery Structure Analysis — コア-ペリフェリー構造分析.

業界ネットワークの「コア」（密に繋がった elite）と「ペリフェリー」（疎な周辺）を識別。
コアへの参入障壁、モビリティパターン、セミペリフェリーの役割を分析。

References:
    - Borgatti, S. P., & Everett, M. G. (2000). Models of core/periphery structures. Social Networks.
    - Cattani, G., & Ferriani, S. (2008). A Core/Periphery Perspective on Individual Creative Performance. Org Sci.
"""

from dataclasses import dataclass, field
from enum import Enum

import networkx as nx
import structlog


logger = structlog.get_logger()


class CoreStatus(Enum):
    """ネットワーク上の位置."""

    CORE = "core"  # 密に繋がったエリート層
    SEMI_PERIPHERY = "semi_periphery"  # 中間層（コアとペリフェリーの橋渡し）
    PERIPHERY = "periphery"  # 疎に繋がった周辺
    ISOLATE = "isolate"  # 孤立ノード


@dataclass
class CorenessMetrics:
    """コア性の指標.

    Attributes:
        person_id: person_id
        coreness: コア性スコア（0-1、1に近いほどコア）
        core_status: コア/セミペリフェリー/ペリフェリー
        k_core: k-コア番号（最大k）
        core_degree: コア内での次数
        periphery_degree: ペリフェリーへの次数
        core_ratio: 接続のうちコアへの割合
        closeness_to_core: コアメンバーへの平均距離
    """

    person_id: str
    coreness: float = 0.0
    core_status: CoreStatus = CoreStatus.ISOLATE
    k_core: int = 0
    core_degree: int = 0
    periphery_degree: int = 0
    core_ratio: float = 0.0
    closeness_to_core: float = float("inf")


@dataclass
class CorePeripheryStructure:
    """コア-ペリフェリー構造の全体像.

    Attributes:
        core_members: コアメンバーのIDリスト
        semi_periphery_members: セミペリフェリーのIDリスト
        periphery_members: ペリフェリーのIDリスト
        core_size: コアのサイズ
        core_density: コア内の密度
        periphery_density: ペリフェリー内の密度
        core_periphery_ratio: コアとペリフェリーの接続密度
        modularity: コア-ペリフェリー分割のモジュラリティ
    """

    core_members: list[str] = field(default_factory=list)
    semi_periphery_members: list[str] = field(default_factory=list)
    periphery_members: list[str] = field(default_factory=list)
    core_size: int = 0
    core_density: float = 0.0
    periphery_density: float = 0.0
    core_periphery_ratio: float = 0.0
    modularity: float = 0.0


def compute_k_core_numbers(
    graph: nx.Graph,
) -> dict[str, int]:
    """k-コア番号を計算.

    k-core: 次数≥kのノードのみで構成される最大部分グラフ

    Args:
        graph: ネットワークグラフ

    Returns:
        person_id → k-core番号
    """
    return nx.core_number(graph)


def compute_coreness_score(
    graph: nx.Graph,
    person_id: str,
    k_core: int,
    max_k: int,
) -> float:
    """コア性スコアを計算.

    複数の指標を統合:
    - k-core番号（正規化）
    - 次数中心性
    - クローズネス中心性

    Args:
        graph: ネットワークグラフ
        person_id: 対象のperson_id
        k_core: その人のk-core番号
        max_k: グラフ全体の最大k-core

    Returns:
        コア性スコア（0-1）
    """
    if person_id not in graph:
        return 0.0

    # k-core component (40%)
    k_score = k_core / max_k if max_k > 0 else 0

    # Degree centrality (30%)
    degree = graph.degree(person_id)
    max_degree = max(dict(graph.degree()).values())
    degree_score = degree / max_degree if max_degree > 0 else 0

    # Clustering coefficient (30%) - core members have high clustering
    try:
        clustering = nx.clustering(graph, person_id)
    except Exception:
        clustering = 0

    coreness = 0.4 * k_score + 0.3 * degree_score + 0.3 * clustering

    return round(coreness, 4)


def identify_core_periphery(
    graph: nx.Graph,
    core_threshold: float = 0.7,
    semi_threshold: float = 0.4,
) -> CorePeripheryStructure:
    """コア-ペリフェリー構造を識別.

    Args:
        graph: ネットワークグラフ
        core_threshold: コアの閾値（coreness ≥ この値）
        semi_threshold: セミペリフェリーの閾値

    Returns:
        CorePeripheryStructure
    """
    if graph.number_of_nodes() == 0:
        return CorePeripheryStructure()

    # Compute k-core numbers
    k_cores = compute_k_core_numbers(graph)
    max_k = max(k_cores.values()) if k_cores else 1

    # Compute coreness for each node
    coreness_scores = {}
    for person_id in graph.nodes():
        coreness_scores[person_id] = compute_coreness_score(
            graph, person_id, k_cores.get(person_id, 0), max_k
        )

    # Classify nodes
    core_members = [
        pid for pid, score in coreness_scores.items() if score >= core_threshold
    ]
    semi_members = [
        pid
        for pid, score in coreness_scores.items()
        if semi_threshold <= score < core_threshold
    ]
    periphery_members = [
        pid for pid, score in coreness_scores.items() if score < semi_threshold
    ]

    # Compute densities
    core_subgraph = graph.subgraph(core_members) if core_members else graph.subgraph([])
    core_density = nx.density(core_subgraph)

    periphery_subgraph = (
        graph.subgraph(periphery_members) if periphery_members else graph.subgraph([])
    )
    periphery_density = nx.density(periphery_subgraph)

    # Core-periphery connections
    core_periphery_edges = 0
    for core in core_members:
        for periphery in periphery_members:
            if graph.has_edge(core, periphery):
                core_periphery_edges += 1

    max_cp_edges = len(core_members) * len(periphery_members)
    cp_ratio = core_periphery_edges / max_cp_edges if max_cp_edges > 0 else 0

    structure = CorePeripheryStructure(
        core_members=core_members,
        semi_periphery_members=semi_members,
        periphery_members=periphery_members,
        core_size=len(core_members),
        core_density=round(core_density, 4),
        periphery_density=round(periphery_density, 4),
        core_periphery_ratio=round(cp_ratio, 4),
        modularity=0.0,  # Placeholder
    )

    logger.info(
        "core_periphery_identified",
        core=len(core_members),
        semi=len(semi_members),
        periphery=len(periphery_members),
        core_density=structure.core_density,
    )

    return structure


def compute_coreness_metrics(
    graph: nx.Graph,
    core_structure: CorePeripheryStructure,
) -> dict[str, CorenessMetrics]:
    """全ノードのコア性指標を計算.

    Args:
        graph: ネットワークグラフ
        core_structure: コア-ペリフェリー構造

    Returns:
        person_id → CorenessMetrics
    """
    k_cores = compute_k_core_numbers(graph)
    max_k = max(k_cores.values()) if k_cores else 1

    core_set = set(core_structure.core_members)
    semi_set = set(core_structure.semi_periphery_members)
    periphery_set = set(core_structure.periphery_members)

    metrics: dict[str, CorenessMetrics] = {}

    for person_id in graph.nodes():
        # Determine status
        if person_id in core_set:
            status = CoreStatus.CORE
        elif person_id in semi_set:
            status = CoreStatus.SEMI_PERIPHERY
        elif person_id in periphery_set:
            status = CoreStatus.PERIPHERY
        else:
            status = CoreStatus.ISOLATE

        # Compute coreness score
        k_core = k_cores.get(person_id, 0)
        coreness = compute_coreness_score(graph, person_id, k_core, max_k)

        # Count connections to core vs periphery
        neighbors = list(graph.neighbors(person_id))
        core_degree = sum(1 for n in neighbors if n in core_set)
        periphery_degree = sum(1 for n in neighbors if n in periphery_set)

        total_degree = len(neighbors)
        core_ratio = core_degree / total_degree if total_degree > 0 else 0

        # Average distance to core members
        if core_structure.core_members:
            try:
                distances = [
                    nx.shortest_path_length(graph, person_id, core)
                    for core in core_structure.core_members
                    if nx.has_path(graph, person_id, core)
                ]
                avg_distance = (
                    sum(distances) / len(distances) if distances else float("inf")
                )
            except Exception:
                avg_distance = float("inf")
        else:
            avg_distance = float("inf")

        metrics[person_id] = CorenessMetrics(
            person_id=person_id,
            coreness=coreness,
            core_status=status,
            k_core=k_core,
            core_degree=core_degree,
            periphery_degree=periphery_degree,
            core_ratio=round(core_ratio, 3),
            closeness_to_core=round(avg_distance, 2)
            if avg_distance != float("inf")
            else float("inf"),
        )

    logger.info("coreness_metrics_computed", persons=len(metrics))

    return metrics


def find_rising_stars(
    metrics: dict[str, CorenessMetrics],
    structure: CorePeripheryStructure,
    min_core_ratio: float = 0.5,
    top_n: int = 20,
) -> list[tuple[str, float, float]]:
    """「上昇中の新星」を特定.

    セミペリフェリーまたはペリフェリーだが、
    コアメンバーと多く繋がっている = コアへの候補

    Args:
        metrics: コア性指標
        structure: コア-ペリフェリー構造
        min_core_ratio: 最低コア接続割合
        top_n: 上位何人を返すか

    Returns:
        [(person_id, coreness, core_ratio), ...] のリスト
    """
    candidates = []

    for person_id, m in metrics.items():
        # Not already in core
        if m.core_status == CoreStatus.CORE:
            continue

        # Has significant connections to core
        if m.core_ratio >= min_core_ratio and m.coreness > 0.3:
            candidates.append((person_id, m.coreness, m.core_ratio))

    # Sort by combination of coreness and core_ratio
    candidates.sort(key=lambda x: (x[1] + x[2]) / 2, reverse=True)

    logger.info("rising_stars_found", count=len(candidates[:top_n]))

    return candidates[:top_n]


def analyze_core_accessibility(
    structure: CorePeripheryStructure,
    metrics: dict[str, CorenessMetrics],
) -> dict[str, float]:
    """コアへのアクセシビリティを分析.

    Args:
        structure: コア-ペリフェリー構造
        metrics: コア性指標

    Returns:
        統計情報の辞書
    """
    # Semi-periphery members' average closeness to core
    semi_members = [
        m for m in metrics.values() if m.core_status == CoreStatus.SEMI_PERIPHERY
    ]
    periphery_members = [
        m for m in metrics.values() if m.core_status == CoreStatus.PERIPHERY
    ]

    avg_semi_distance = (
        sum(
            m.closeness_to_core
            for m in semi_members
            if m.closeness_to_core != float("inf")
        )
        / len([m for m in semi_members if m.closeness_to_core != float("inf")])
        if semi_members
        else 0
    )

    avg_periphery_distance = (
        sum(
            m.closeness_to_core
            for m in periphery_members
            if m.closeness_to_core != float("inf")
        )
        / len([m for m in periphery_members if m.closeness_to_core != float("inf")])
        if periphery_members
        else 0
    )

    # Percentage with direct core connections
    semi_with_core_links = sum(1 for m in semi_members if m.core_degree > 0)
    periphery_with_core_links = sum(1 for m in periphery_members if m.core_degree > 0)

    stats = {
        "core_size": structure.core_size,
        "core_density": structure.core_density,
        "periphery_density": structure.periphery_density,
        "core_closure": structure.core_density / structure.periphery_density
        if structure.periphery_density > 0
        else 0,
        "avg_semi_distance_to_core": round(avg_semi_distance, 2),
        "avg_periphery_distance_to_core": round(avg_periphery_distance, 2),
        "semi_core_link_rate": round(
            semi_with_core_links / len(semi_members) if semi_members else 0, 3
        ),
        "periphery_core_link_rate": round(
            periphery_with_core_links / len(periphery_members)
            if periphery_members
            else 0,
            3,
        ),
    }

    logger.info("core_accessibility_analyzed", **stats)

    return stats


def main():
    """スタンドアロン実行用エントリーポイント."""
    from src.analysis.graph import create_person_collaboration_network
    from src.database import (
        get_all_anime,
        get_all_credits,
        get_all_persons,
        get_connection,
        init_db,
    )

    conn = get_connection()
    init_db(conn)

    persons = get_all_persons(conn)
    anime_list = get_all_anime(conn)
    credits = get_all_credits(conn)

    # マップ作成
    anime_map = {a.id: a for a in anime_list}
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}

    # コラボレーショングラフ構築
    logger.info("building_collaboration_graph")
    collab_graph = create_person_collaboration_network(credits, anime_map)

    # コア-ペリフェリー分析
    logger.info("identifying_core_periphery")
    structure = identify_core_periphery(
        collab_graph, core_threshold=0.7, semi_threshold=0.4
    )

    # コア性指標計算
    logger.info("computing_coreness_metrics")
    metrics = compute_coreness_metrics(collab_graph, structure)

    # 結果表示
    print("\n=== コア-ペリフェリー構造 ===\n")
    print(f"コア: {structure.core_size}人 (密度: {structure.core_density:.3f})")
    print(f"セミペリフェリー: {len(structure.semi_periphery_members)}人")
    print(f"ペリフェリー: {len(structure.periphery_members)}人")
    print(f"ペリフェリー密度: {structure.periphery_density:.3f}")
    print(f"コア閉鎖性: {structure.core_density / structure.periphery_density:.1f}x")

    print("\n=== コアメンバー（トップ10）===\n")
    core_with_names = [
        (pid, person_names.get(pid, pid), metrics[pid].coreness)
        for pid in structure.core_members
    ]
    core_with_names.sort(key=lambda x: x[2], reverse=True)

    for pid, name, coreness in core_with_names[:10]:
        print(f"{name}: coreness={coreness:.3f}")

    # 上昇中の新星
    print("\n=== 上昇中の新星（コア候補）===\n")
    rising = find_rising_stars(metrics, structure, min_core_ratio=0.5, top_n=10)

    for pid, coreness, core_ratio in rising:
        name = person_names.get(pid, pid)
        print(f"{name}:")
        print(f"  Coreness: {coreness:.3f}")
        print(f"  コア接続率: {core_ratio:.1%}")
        print()

    # アクセシビリティ分析
    print("\n=== コアへのアクセシビリティ ===\n")
    accessibility = analyze_core_accessibility(structure, metrics)

    print(
        f"セミペリフェリー → コア平均距離: {accessibility['avg_semi_distance_to_core']:.2f}"
    )
    print(
        f"ペリフェリー → コア平均距離: {accessibility['avg_periphery_distance_to_core']:.2f}"
    )
    print(
        f"セミペリフェリーのコア直接接続率: {accessibility['semi_core_link_rate']:.1%}"
    )
    print(
        f"ペリフェリーのコア直接接続率: {accessibility['periphery_core_link_rate']:.1%}"
    )

    conn.close()


if __name__ == "__main__":
    main()
