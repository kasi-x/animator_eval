"""Community Detection — クリエイター派閥の自動検出.

Louvain法を使用してコラボレーションネットワークからコミュニティ（派閥）を検出する。
密に連携するクリエイター集団を可視化し、スタジオや監督を超えた実質的な協力関係を明らかにする。
"""

from collections import defaultdict
from dataclasses import dataclass, field

import networkx as nx
import structlog

from src.models import Credit

logger = structlog.get_logger()


@dataclass
class Community:
    """コミュニティ（派閥）の情報.

    Attributes:
        community_id: コミュニティID
        members: メンバーのperson_idリスト
        size: メンバー数
        density: 内部密度（0-1）
        modularity_contribution: モジュラリティへの寄与度
        top_members: 中心的なメンバー（次数順）
        internal_edges: コミュニティ内エッジ数
        external_edges: コミュニティ外へのエッジ数
    """

    community_id: int
    members: list[str] = field(default_factory=list)
    size: int = 0
    density: float = 0.0
    modularity_contribution: float = 0.0
    top_members: list[tuple[str, int]] = field(default_factory=list)
    internal_edges: int = 0
    external_edges: int = 0


def detect_communities(
    collaboration_graph: nx.Graph,
    min_community_size: int = 3,
    resolution: float = 1.0,
) -> dict[int, Community]:
    """コラボレーショングラフからコミュニティを検出する.

    Louvain法でモジュラリティを最大化するコミュニティ分割を見つける。
    密に連携するクリエイター集団（派閥）を自動的に抽出。

    Args:
        collaboration_graph: Person間のコラボレーショングラフ
        min_community_size: 最小コミュニティサイズ（これより小さいものは除外）
        resolution: 解像度パラメータ（大きいほど小さいコミュニティに分割）

    Returns:
        コミュニティID → Community情報の辞書
    """
    if collaboration_graph.number_of_nodes() == 0:
        logger.warning("community_detection_skipped_empty_graph")
        return {}

    logger.info(
        "community_detection_start",
        nodes=collaboration_graph.number_of_nodes(),
        edges=collaboration_graph.number_of_edges(),
        resolution=resolution,
    )

    # Louvain法でコミュニティ検出
    # NetworkXのgreedy_modularity_communitiesを使用（Louvain相当）
    communities_list = nx.community.greedy_modularity_communities(
        collaboration_graph,
        weight="weight",
        resolution=resolution,
    )

    # Community オブジェクトに変換
    communities: dict[int, Community] = {}
    community_id = 0

    for comm_set in communities_list:
        members = list(comm_set)
        size = len(members)

        # 最小サイズフィルタ
        if size < min_community_size:
            continue

        # サブグラフ作成
        subgraph = collaboration_graph.subgraph(members)

        # 内部密度計算
        possible_edges = size * (size - 1) / 2
        actual_edges = subgraph.number_of_edges()
        density = actual_edges / possible_edges if possible_edges > 0 else 0

        # 外部エッジ数計算
        external_edges = 0
        for member in members:
            for neighbor in collaboration_graph.neighbors(member):
                if neighbor not in comm_set:
                    external_edges += 1

        # 中心的メンバー（次数順）
        degrees = {node: subgraph.degree(node, weight="weight") for node in members}
        top_members = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:5]

        communities[community_id] = Community(
            community_id=community_id,
            members=members,
            size=size,
            density=round(density, 4),
            modularity_contribution=0.0,  # 後で計算
            top_members=[(m, int(d)) for m, d in top_members],
            internal_edges=actual_edges,
            external_edges=external_edges,
        )

        community_id += 1

    # 全体のモジュラリティ計算
    partition = {}
    for comm_id, comm in communities.items():
        for member in comm.members:
            partition[member] = comm_id

    if partition:
        total_modularity = nx.community.modularity(
            collaboration_graph, communities_list, weight="weight"
        )
        logger.info(
            "community_detection_complete",
            communities=len(communities),
            total_modularity=round(total_modularity, 4),
            avg_size=round(sum(c.size for c in communities.values()) / len(communities), 1),
        )
    else:
        logger.warning("community_detection_no_communities_found")

    return communities


def analyze_community_overlap(
    communities: dict[int, Community],
    collaboration_graph: nx.Graph,
) -> dict[str, list[tuple[int, int]]]:
    """コミュニティ間の重複分析.

    複数のコミュニティにまたがる「ブリッジ」的な人物を特定。

    Args:
        communities: コミュニティ情報
        collaboration_graph: コラボレーショングラフ

    Returns:
        person_id → [(community_id, connections_count)] のマッピング
    """
    person_connections: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))

    # 各人物がどのコミュニティと何回繋がっているかカウント
    for person_id in collaboration_graph.nodes():
        for neighbor in collaboration_graph.neighbors(person_id):
            # 隣人が属するコミュニティを見つける
            for comm_id, comm in communities.items():
                if neighbor in comm.members:
                    person_connections[person_id][comm_id] += 1

    # 複数コミュニティに接続している人物のみ抽出
    bridges = {}
    for person_id, connections in person_connections.items():
        if len(connections) >= 2:
            # 接続数順にソート
            sorted_connections = sorted(
                connections.items(), key=lambda x: x[1], reverse=True
            )
            bridges[person_id] = sorted_connections

    logger.info("community_overlap_analyzed", bridge_persons=len(bridges))
    return bridges


def compute_community_features(
    communities: dict[int, Community],
    credits: list[Credit],
    person_scores: dict[str, dict] | None = None,
) -> dict[int, dict]:
    """コミュニティの特徴量を計算.

    各コミュニティの平均スコア、活動期間、役職分布などを算出。

    Args:
        communities: コミュニティ情報
        credits: 全クレジット
        person_scores: person_id → score dict（オプション）

    Returns:
        community_id → 特徴量辞書
    """
    # person_id → credits のマッピング
    person_credits: dict[str, list[Credit]] = defaultdict(list)
    for credit in credits:
        person_credits[credit.person_id].append(credit)

    features = {}

    for comm_id, comm in communities.items():
        # メンバーのクレジット集約
        all_years = []
        all_roles: dict[str, int] = defaultdict(int)
        total_credits = 0

        for member_id in comm.members:
            member_creds = person_credits.get(member_id, [])
            total_credits += len(member_creds)
            for cred in member_creds:
                if cred.year:
                    all_years.append(cred.year)
                all_roles[cred.role.value] += 1

        # 活動期間
        active_period = (min(all_years), max(all_years)) if all_years else (None, None)
        active_years = len(set(all_years)) if all_years else 0

        # 平均スコア
        avg_scores = {}
        if person_scores:
            scores_in_comm = [
                person_scores.get(m, {}) for m in comm.members if m in person_scores
            ]
            if scores_in_comm:
                for key in ["authority", "trust", "skill", "composite"]:
                    values = [s.get(key, 0) for s in scores_in_comm if key in s]
                    if values:
                        avg_scores[f"avg_{key}"] = round(sum(values) / len(values), 2)

        # トップ役職
        top_roles = sorted(all_roles.items(), key=lambda x: x[1], reverse=True)[:3]

        features[comm_id] = {
            "size": comm.size,
            "density": comm.density,
            "total_credits": total_credits,
            "credits_per_person": round(total_credits / comm.size, 1) if comm.size > 0 else 0,
            "active_period": active_period,
            "active_years": active_years,
            "top_roles": [(role, count) for role, count in top_roles],
            **avg_scores,
        }

    logger.info("community_features_computed", communities=len(features))
    return features


def export_communities_for_visualization(
    communities: dict[int, Community],
    features: dict[int, dict],
    person_names: dict[str, str] | None = None,
) -> dict:
    """可視化用にコミュニティデータをエクスポート.

    Args:
        communities: コミュニティ情報
        features: コミュニティ特徴量
        person_names: person_id → 名前のマッピング

    Returns:
        JSONエクスポート可能な辞書
    """
    export_data = {
        "total_communities": len(communities),
        "communities": [],
    }

    for comm_id, comm in sorted(communities.items(), key=lambda x: x[1].size, reverse=True):
        comm_features = features.get(comm_id, {})

        # メンバー名の取得
        members_with_names = []
        if person_names:
            for member_id in comm.members:
                name = person_names.get(member_id, member_id)
                members_with_names.append({"person_id": member_id, "name": name})
        else:
            members_with_names = [{"person_id": m, "name": m} for m in comm.members]

        export_data["communities"].append({
            "community_id": comm_id,
            "size": comm.size,
            "density": comm.density,
            "members": members_with_names,
            "top_members": [
                {"person_id": m, "name": person_names.get(m, m) if person_names else m, "degree": d}
                for m, d in comm.top_members
            ],
            "internal_edges": comm.internal_edges,
            "external_edges": comm.external_edges,
            **comm_features,
        })

    return export_data


def main():
    """スタンドアロン実行用エントリーポイント."""
    from src.analysis.graph import build_collaboration_graph
    from src.database import get_all_credits, get_all_persons, get_connection, init_db

    conn = get_connection()
    init_db(conn)

    persons = get_all_persons(conn)
    credits = get_all_credits(conn)

    # 名前マップ作成
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}

    # コラボレーショングラフ構築
    logger.info("building_collaboration_graph")
    collab_graph = build_collaboration_graph(credits)

    # コミュニティ検出
    communities = detect_communities(collab_graph, min_community_size=5)

    # 特徴量計算
    features = compute_community_features(communities, credits)

    # ブリッジ分析
    bridges = analyze_community_overlap(communities, collab_graph)

    # エクスポート
    export_data = export_communities_for_visualization(communities, features, person_names)

    # 結果表示
    print(f"\n検出されたコミュニティ数: {len(communities)}")
    print(f"ブリッジ人物数: {len(bridges)}")

    for comm_id, comm in sorted(communities.items(), key=lambda x: x[1].size, reverse=True)[:5]:
        print(f"\nコミュニティ {comm_id}:")
        print(f"  サイズ: {comm.size}人")
        print(f"  密度: {comm.density:.3f}")
        print(f"  中心メンバー:")
        for person_id, degree in comm.top_members[:3]:
            print(f"    - {person_names.get(person_id, person_id)} (次数: {degree})")

    conn.close()


if __name__ == "__main__":
    main()
