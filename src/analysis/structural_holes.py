"""Structural Holes Analysis — 構造的空隙とブローカー役割分析.

Ronald Burt の構造的空隙理論に基づき、ネットワーク内の「橋渡し役」を特定。
異なるグループを繋ぐクリエイターは、情報優位性と制御力を持つ。

References:
    - Burt, R. S. (1992). Structural Holes: The Social Structure of Competition.
    - Burt, R. S. (2004). Structural Holes and Good Ideas. AJS.
    - Gould, R. V., & Fernandez, R. M. (1989). Structures of Mediation. ASR.
"""

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum

import networkx as nx
import structlog


logger = structlog.get_logger()


class BrokerageRole(Enum):
    """Gould & Fernandez (1989) の5つのブローカー役割."""

    COORDINATOR = "coordinator"  # 同じグループ内の仲介（A-B-C 全員が同じグループ）
    CONSULTANT = "consultant"  # 外部コンサルタント（Bだけ外部、A-Cは同じ内部グループ）
    REPRESENTATIVE = "representative"  # 代表者（A-Bは内部、Cは外部）
    GATEKEEPER = "gatekeeper"  # 門番（Aは外部、B-Cは内部）
    LIAISON = "liaison"  # 完全な橋渡し（A, B, C 全員が異なるグループ）


@dataclass
class StructuralHoleMetrics:
    """構造的空隙の指標.

    Attributes:
        person_id: person_id
        constraint: ネットワーク制約指数（0-1、低いほど自律的）
        effective_size: 有効ネットワークサイズ（冗長性を除いた接続数）
        efficiency: ネットワーク効率（effective_size / actual_size）
        hierarchy: 階層性（制約が一人に集中する度合い）
        betweenness: 媒介中心性（橋渡しの頻度）
        bridges: 橋となっている辺の数
        redundancy: 冗長な接続の割合
    """

    person_id: str
    constraint: float = 1.0
    effective_size: float = 0.0
    efficiency: float = 0.0
    hierarchy: float = 0.0
    betweenness: float = 0.0
    bridges: int = 0
    redundancy: float = 0.0


@dataclass
class BrokerageMetrics:
    """ブローカー役割の指標.

    Attributes:
        person_id: person_id
        total_brokerage: 総ブローカー数
        coordinator: Coordinator役割の数
        consultant: Consultant役割の数
        representative: Representative役割の数
        gatekeeper: Gatekeeper役割の数
        liaison: Liaison役割の数
        dominant_role: 最も多い役割
    """

    person_id: str
    total_brokerage: int = 0
    coordinator: int = 0
    consultant: int = 0
    representative: int = 0
    gatekeeper: int = 0
    liaison: int = 0
    dominant_role: BrokerageRole | None = None


def compute_network_constraint(
    graph: nx.Graph,
    person_id: str,
) -> float:
    """ネットワーク制約指数を計算（Burt's Constraint）.

    制約が低い = 構造的空隙が多い = 自律的

    Args:
        graph: ネットワークグラフ
        person_id: 対象のperson_id

    Returns:
        制約指数（0-1）
    """
    if person_id not in graph:
        return 1.0

    neighbors = list(graph.neighbors(person_id))
    if not neighbors:
        return 1.0

    # Burt's constraint formula:
    # C_i = Σ_j (p_ij + Σ_q p_iq * p_qj)^2
    # p_ij = proportion of i's network invested in j

    constraint = 0.0

    for j in neighbors:
        # Direct investment in j
        direct = 1.0 / len(neighbors)

        # Indirect investment through mutual connections
        indirect = 0.0
        for q in neighbors:
            if q != j and graph.has_edge(j, q):
                indirect += (1.0 / len(neighbors)) * (
                    1.0 / len(list(graph.neighbors(q)))
                )

        constraint += (direct + indirect) ** 2

    return round(constraint, 4)


def compute_effective_size(
    graph: nx.Graph,
    person_id: str,
) -> tuple[float, float]:
    """有効ネットワークサイズを計算.

    冗長な接続を除外した「実質的な」接続数。

    Args:
        graph: ネットワークグラフ
        person_id: 対象のperson_id

    Returns:
        (effective_size, efficiency)
    """
    if person_id not in graph:
        return 0.0, 0.0

    neighbors = list(graph.neighbors(person_id))
    if not neighbors:
        return 0.0, 0.0

    # Effective size = N - (redundancy)
    # Redundancy = average number of ties between i's contacts
    redundancy = 0.0
    for j in neighbors:
        # How many of i's other contacts does j know?
        j_neighbors = set(graph.neighbors(j))
        overlap = len(j_neighbors & set(neighbors)) - 1  # Exclude j itself
        redundancy += overlap / len(neighbors)

    effective_size = len(neighbors) - redundancy
    efficiency = effective_size / len(neighbors) if neighbors else 0.0

    return round(effective_size, 2), round(efficiency, 3)


def identify_bridges(
    graph: nx.Graph,
    person_id: str,
) -> int:
    """橋となっている辺の数を数える.

    橋: 削除するとネットワークが分断される辺

    Args:
        graph: ネットワークグラフ
        person_id: 対象のperson_id

    Returns:
        橋の数
    """
    if person_id not in graph:
        return 0

    # Find bridges in the graph
    bridges = list(nx.bridges(graph))

    # Count how many bridges involve this person
    bridge_count = sum(1 for u, v in bridges if person_id in (u, v))

    return bridge_count


def compute_structural_hole_metrics(
    collaboration_graph: nx.Graph,
) -> dict[str, StructuralHoleMetrics]:
    """全員の構造的空隙指標を計算.

    Args:
        collaboration_graph: コラボレーショングラフ

    Returns:
        person_id → StructuralHoleMetrics
    """
    metrics: dict[str, StructuralHoleMetrics] = {}

    # Compute betweenness centrality for all nodes
    betweenness = nx.betweenness_centrality(collaboration_graph, weight="weight")

    # Identify all bridges once (expensive operation)
    all_bridges = set(nx.bridges(collaboration_graph))

    for person_id in collaboration_graph.nodes():
        # Network constraint
        constraint = compute_network_constraint(collaboration_graph, person_id)

        # Effective size
        effective_size, efficiency = compute_effective_size(
            collaboration_graph, person_id
        )

        # Bridges
        bridge_count = sum(1 for u, v in all_bridges if person_id in (u, v))

        # Redundancy
        neighbors = list(collaboration_graph.neighbors(person_id))
        redundancy = 1.0 - efficiency if neighbors else 0.0

        # Hierarchy (concentration of constraint in one contact)
        # Simplified: std of constraint contributions
        hierarchy = 0.0  # Placeholder (computationally expensive)

        metrics[person_id] = StructuralHoleMetrics(
            person_id=person_id,
            constraint=constraint,
            effective_size=effective_size,
            efficiency=efficiency,
            hierarchy=hierarchy,
            betweenness=round(betweenness.get(person_id, 0), 4),
            bridges=bridge_count,
            redundancy=round(redundancy, 3),
        )

    logger.info(
        "structural_hole_metrics_computed",
        persons=len(metrics),
        avg_constraint=round(
            sum(m.constraint for m in metrics.values()) / len(metrics), 3
        ),
    )

    return metrics


def classify_brokerage_role(
    person_id: str,
    source_id: str,
    target_id: str,
    groups: dict[str, str],
) -> BrokerageRole:
    """ブローカー役割を分類（Gould & Fernandez 1989）.

    A (source) → B (person) → C (target) の関係で、
    A, B, C の所属グループから役割を判定。

    Args:
        person_id: 仲介者B
        source_id: 始点A
        target_id: 終点C
        groups: person_id → group の辞書

    Returns:
        ブローカー役割
    """
    group_a = groups.get(source_id, "unknown")
    group_b = groups.get(person_id, "unknown")
    group_c = groups.get(target_id, "unknown")

    # Coordinator: A, B, C 全員が同じグループ
    if group_a == group_b == group_c:
        return BrokerageRole.COORDINATOR

    # Consultant: Bだけ外部、A-Cは同じ内部グループ
    if group_a == group_c != group_b:
        return BrokerageRole.CONSULTANT

    # Representative: A-Bは内部、Cは外部
    if group_a == group_b != group_c:
        return BrokerageRole.REPRESENTATIVE

    # Gatekeeper: Aは外部、B-Cは内部
    if group_b == group_c != group_a:
        return BrokerageRole.GATEKEEPER

    # Liaison: A, B, C 全員が異なるグループ
    if group_a != group_b != group_c != group_a:
        return BrokerageRole.LIAISON

    # Default (shouldn't happen if groups are well-defined)
    return BrokerageRole.LIAISON


def compute_brokerage_metrics(
    collaboration_graph: nx.Graph,
    groups: dict[str, str],
) -> dict[str, BrokerageMetrics]:
    """ブローカー役割の指標を計算.

    Args:
        collaboration_graph: コラボレーショングラフ
        groups: person_id → group（例: スタジオ、役職カテゴリ）

    Returns:
        person_id → BrokerageMetrics
    """
    brokerage_counts: dict[str, dict[BrokerageRole, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    # For each person, check all triads where they are in the middle
    for person_id in collaboration_graph.nodes():
        neighbors = list(collaboration_graph.neighbors(person_id))

        # Check all pairs of neighbors (potential A-B-C paths)
        for i, source in enumerate(neighbors):
            for target in neighbors[i + 1 :]:
                # Classify the brokerage role
                role = classify_brokerage_role(person_id, source, target, groups)
                brokerage_counts[person_id][role] += 1

    # Convert to BrokerageMetrics
    metrics: dict[str, BrokerageMetrics] = {}

    for person_id, role_counts in brokerage_counts.items():
        total = sum(role_counts.values())
        dominant_role = (
            max(role_counts.items(), key=lambda x: x[1])[0] if role_counts else None
        )

        metrics[person_id] = BrokerageMetrics(
            person_id=person_id,
            total_brokerage=total,
            coordinator=role_counts.get(BrokerageRole.COORDINATOR, 0),
            consultant=role_counts.get(BrokerageRole.CONSULTANT, 0),
            representative=role_counts.get(BrokerageRole.REPRESENTATIVE, 0),
            gatekeeper=role_counts.get(BrokerageRole.GATEKEEPER, 0),
            liaison=role_counts.get(BrokerageRole.LIAISON, 0),
            dominant_role=dominant_role,
        )

    logger.info(
        "brokerage_metrics_computed",
        persons=len(metrics),
        avg_brokerage=round(
            sum(m.total_brokerage for m in metrics.values()) / len(metrics), 1
        ),
    )

    return metrics


def find_structural_hole_spanners(
    metrics: dict[str, StructuralHoleMetrics],
    top_n: int = 20,
) -> list[tuple[str, float, float]]:
    """構造的空隙を最も活用している人物を特定.

    低い制約 + 高い効率 = 構造的空隙の活用

    Args:
        metrics: 構造的空隙指標
        top_n: 上位何人を返すか

    Returns:
        [(person_id, constraint, efficiency), ...] のリスト
    """
    # Score = efficiency / (constraint + 0.1)  # Avoid division by zero
    scored = [
        (
            person_id,
            m.constraint,
            m.efficiency,
            m.efficiency / (m.constraint + 0.1),
        )
        for person_id, m in metrics.items()
    ]

    # Sort by score (descending)
    scored.sort(key=lambda x: x[3], reverse=True)

    result = [
        (pid, constraint, efficiency)
        for pid, constraint, efficiency, _ in scored[:top_n]
    ]

    logger.info("structural_hole_spanners_found", count=len(result))
    return result


def find_key_brokers(
    brokerage_metrics: dict[str, BrokerageMetrics],
    role: BrokerageRole | None = None,
    top_n: int = 20,
) -> list[tuple[str, int, BrokerageRole]]:
    """主要なブローカーを特定.

    Args:
        brokerage_metrics: ブローカー指標
        role: 特定の役割に絞る（Noneで全役割）
        top_n: 上位何人を返すか

    Returns:
        [(person_id, count, dominant_role), ...] のリスト
    """
    if role:
        # Filter by specific role
        role_attr = role.value
        scored = [
            (person_id, getattr(m, role_attr), m.dominant_role)
            for person_id, m in brokerage_metrics.items()
            if getattr(m, role_attr, 0) > 0
        ]
    else:
        # All roles
        scored = [
            (person_id, m.total_brokerage, m.dominant_role)
            for person_id, m in brokerage_metrics.items()
        ]

    # Sort by count
    scored.sort(key=lambda x: x[1], reverse=True)

    result = scored[:top_n]

    logger.info(
        "key_brokers_found", count=len(result), role=role.value if role else "all"
    )
    return result


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

    # グループ定義（例: 役職カテゴリ）
    from src.utils.role_groups import get_role_category

    person_groups = {}
    for credit in credits:
        if credit.person_id not in person_groups:
            person_groups[credit.person_id] = get_role_category(credit.role)

    # コラボレーショングラフ構築
    logger.info("building_collaboration_graph")
    collab_graph = create_person_collaboration_network(credits, anime_map)

    # 構造的空隙分析
    logger.info("computing_structural_holes")
    sh_metrics = compute_structural_hole_metrics(collab_graph)

    # ブローカー分析
    logger.info("computing_brokerage")
    brokerage = compute_brokerage_metrics(collab_graph, person_groups)

    # 結果表示
    print("\n=== 構造的空隙トップ10 ===")
    print("（低制約 + 高効率 = 情報優位性）\n")

    spanners = find_structural_hole_spanners(sh_metrics, top_n=10)
    for person_id, constraint, efficiency in spanners:
        name = person_names.get(person_id, person_id)
        print(f"{name}:")
        print(f"  制約: {constraint:.3f} (低いほど自律的)")
        print(f"  効率: {efficiency:.3f} (高いほど非冗長)")
        print()

    print("\n=== 主要ブローカー（トップ10）===")
    print("（異なるグループを繋ぐ仲介者）\n")

    brokers = find_key_brokers(brokerage, top_n=10)
    for person_id, count, dominant_role in brokers:
        name = person_names.get(person_id, person_id)
        role_name = dominant_role.value if dominant_role else "unknown"
        print(f"{name}: {count}回のブローカー役割（主に {role_name}）")

    # 役割別トップ
    print("\n=== 役割別ブローカー ===\n")
    for role in BrokerageRole:
        top_in_role = find_key_brokers(brokerage, role=role, top_n=3)
        if top_in_role:
            print(f"{role.value.capitalize()}:")
            for person_id, count, _ in top_in_role:
                name = person_names.get(person_id, person_id)
                print(f"  - {name}: {count}回")
            print()

    conn.close()


if __name__ == "__main__":
    main()
