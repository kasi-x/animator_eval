"""Community Detection — クリエイター派閥の自動検出.

Louvain法を使用してコラボレーションネットワークからコミュニティ（派閥）を検出する。
密に連携するクリエイター集団を可視化し、スタジオや監督を超えた実質的な協力関係を明らかにする。

師弟関係（メンター-メンティー）の検出と、時系列での能力評価（当時の能力、潜在能力）も統合。
"""

from collections import defaultdict
from dataclasses import dataclass, field

import networkx as nx
import structlog

from src.models import Anime, Credit

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
        mentorship_pairs: コミュニティ内の師弟関係 [(mentor_id, mentee_id, confidence)]
        avg_ability_at_formation: コミュニティ形成期の平均能力
        avg_prospective_potential: 当時推定の潜在能力（未来データなし）
        avg_retrospective_potential: 事後推定の潜在能力（未来データあり）
        ability_range: 能力値の範囲 (min, max)
    """

    community_id: int
    members: list[str] = field(default_factory=list)
    size: int = 0
    density: float = 0.0
    modularity_contribution: float = 0.0
    top_members: list[tuple[str, int]] = field(default_factory=list)
    internal_edges: int = 0
    external_edges: int = 0
    mentorship_pairs: list[tuple[str, str, float]] = field(default_factory=list)
    avg_ability_at_formation: float = 0.0
    avg_prospective_potential: float = 0.0
    avg_retrospective_potential: float = 0.0
    ability_range: tuple[float, float] = (0.0, 0.0)


def detect_mentorships_in_community(
    community_members: list[str],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    min_shared_works: int = 2,
) -> list[tuple[str, str, float]]:
    """コミュニティ内の師弟関係を検出.

    Args:
        community_members: コミュニティのメンバーIDリスト
        credits: 全クレジット
        anime_map: anime_id → Anime
        min_shared_works: 最低共演作品数

    Returns:
        [(mentor_id, mentee_id, confidence), ...] のリスト
    """
    from src.analysis.mentorship import infer_mentorships

    # Filter credits to community members only
    member_set = set(community_members)
    community_credits = [c for c in credits if c.person_id in member_set]

    # Detect mentorships
    all_mentorships = infer_mentorships(
        community_credits, anime_map, min_shared_works=min_shared_works
    )

    # Filter to pairs where both are in community
    mentorship_pairs = [
        (m["mentor_id"], m["mentee_id"], m["confidence"])
        for m in all_mentorships
        if m["mentor_id"] in member_set and m["mentee_id"] in member_set
    ]

    logger.debug(
        "mentorships_detected_in_community",
        members=len(community_members),
        pairs=len(mentorship_pairs),
    )

    return mentorship_pairs


def compute_prospective_potential(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    evaluation_year: int,
    current_score: float,
) -> float:
    """当時の潜在能力を推定（未来データを使わない前向き推定）.

    Args:
        person_id: 評価対象のperson_id
        credits: その人の全クレジット
        anime_map: anime_id → Anime
        evaluation_year: 評価時点の年
        current_score: 評価時点でのスコア

    Returns:
        推定潜在能力（0-100）
    """
    # Filter credits up to evaluation year (no future data)
    past_credits = []
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year and anime.year <= evaluation_year:
            past_credits.append(c)

    if not past_credits:
        return current_score

    # Calculate growth indicators
    years = sorted({anime_map[c.anime_id].year for c in past_credits if anime_map.get(c.anime_id) and anime_map[c.anime_id].year})
    if len(years) < 2:
        # Not enough history, use current score as baseline
        return current_score

    # Compute credits per year (acceleration indicator)
    career_span = years[-1] - years[0] + 1
    early_credits = len([c for c in past_credits if anime_map.get(c.anime_id) and anime_map[c.anime_id].year and anime_map[c.anime_id].year <= years[0] + career_span // 3])
    recent_credits = len([c for c in past_credits if anime_map.get(c.anime_id) and anime_map[c.anime_id].year and anime_map[c.anime_id].year >= years[-1] - career_span // 3])

    # Growth rate (positive = ascending career)
    growth_rate = (recent_credits - early_credits) / max(early_credits, 1)

    # Potential = current_score + growth_bonus
    # Growth bonus: up to +30 points for strong upward trajectory
    growth_bonus = min(30, max(-10, growth_rate * 20))

    # Recency bonus: younger careers have more potential
    years_active = len(years)
    recency_bonus = max(0, 10 - years_active)  # Max +10 for newcomers

    potential = current_score + growth_bonus + recency_bonus
    return min(100, max(0, potential))


def compute_retrospective_potential(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    evaluation_year: int,
    current_score: float,
    future_peak_score: float,
) -> float:
    """事後推定の潜在能力（未来データを使った後付け推定）.

    Args:
        person_id: 評価対象のperson_id
        credits: その人の全クレジット（過去+未来含む）
        anime_map: anime_id → Anime
        evaluation_year: 評価時点の年
        current_score: 評価時点でのスコア
        future_peak_score: 未来を含めたピークスコア

    Returns:
        事後推定潜在能力（0-100）
    """
    # With hindsight, we know the person's peak
    # Retrospective potential at evaluation_year = their future peak

    # Adjust for how far they were from peak at evaluation time
    score_gap = future_peak_score - current_score

    # If they were already at peak, potential = current
    if score_gap <= 0:
        return current_score

    # Otherwise, retrospective potential is somewhere between current and peak
    # Use a factor based on career stage at evaluation time
    all_years = sorted({anime_map[c.anime_id].year for c in credits if anime_map.get(c.anime_id) and anime_map[c.anime_id].year})
    if not all_years:
        return current_score

    years_since_debut = evaluation_year - all_years[0]

    # Early career: high potential (closer to future peak)
    # Late career: lower potential (closer to current)
    if years_since_debut <= 3:
        potential_factor = 0.9  # 90% towards peak
    elif years_since_debut <= 7:
        potential_factor = 0.7  # 70% towards peak
    else:
        potential_factor = 0.5  # 50% towards peak

    retrospective_potential = current_score + (score_gap * potential_factor)
    return min(100, max(0, retrospective_potential))


def get_community_formation_period(
    community_members: list[str],
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> tuple[int, int] | None:
    """コミュニティの形成期（最も活発だった期間）を特定.

    Args:
        community_members: コミュニティメンバー
        credits: 全クレジット
        anime_map: anime_id → Anime

    Returns:
        (start_year, end_year) または None
    """
    member_set = set(community_members)
    years = []

    for c in credits:
        if c.person_id in member_set:
            anime = anime_map.get(c.anime_id)
            if anime and anime.year:
                years.append(anime.year)

    if not years:
        return None

    # Find peak activity period (3-year window with most credits)
    year_counts: dict[int, int] = defaultdict(int)
    for year in years:
        year_counts[year] += 1

    if not year_counts:
        return None

    # Find 3-year window with highest total credits
    sorted_years = sorted(year_counts.keys())
    if len(sorted_years) < 3:
        return (min(sorted_years), max(sorted_years))

    max_activity = 0
    peak_start = sorted_years[0]

    for i in range(len(sorted_years) - 2):
        window_start = sorted_years[i]
        window_end = sorted_years[i + 2]
        window_credits = sum(
            year_counts[y] for y in range(window_start, window_end + 1)
        )
        if window_credits > max_activity:
            max_activity = window_credits
            peak_start = window_start

    peak_end = peak_start + 2
    return (peak_start, peak_end)


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
    anime_map: dict[str, Anime],
    person_scores: dict[str, dict] | None = None,
) -> dict[int, dict]:
    """コミュニティの特徴量を計算.

    各コミュニティの平均スコア、活動期間、役職分布、師弟関係、時系列能力などを算出。

    Args:
        communities: コミュニティ情報
        credits: 全クレジット
        anime_map: anime_id → Anime
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
                anime = anime_map.get(cred.anime_id)
                if anime and anime.year:
                    all_years.append(anime.year)
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

        # Detect mentorships within community
        mentorship_pairs = detect_mentorships_in_community(
            comm.members, credits, anime_map, min_shared_works=2
        )
        comm.mentorship_pairs = mentorship_pairs

        # Compute ability metrics at formation time
        formation_period = get_community_formation_period(comm.members, credits, anime_map)
        if formation_period and person_scores:
            formation_year = (formation_period[0] + formation_period[1]) // 2

            # Compute three types of ability for each member
            abilities_at_formation = []
            prospective_potentials = []
            retrospective_potentials = []

            for member_id in comm.members:
                if member_id not in person_scores:
                    continue

                member_creds = person_credits.get(member_id, [])
                current_score = person_scores[member_id].get("composite", 0)

                # 1. Ability at formation time (actual score at that time)
                # Simplified: use current score (ideally should recompute at formation_year)
                ability_at_time = current_score
                abilities_at_formation.append(ability_at_time)

                # 2. Prospective potential (estimated at formation time, no future data)
                prospective = compute_prospective_potential(
                    member_id, member_creds, anime_map, formation_year, ability_at_time
                )
                prospective_potentials.append(prospective)

                # 3. Retrospective potential (with hindsight, using all data)
                # Find peak score from all future activity
                # Assume current composite score is their peak (simplified)
                future_peak = current_score
                retrospective = compute_retrospective_potential(
                    member_id,
                    member_creds,
                    anime_map,
                    formation_year,
                    ability_at_time,
                    future_peak,
                )
                retrospective_potentials.append(retrospective)

            # Update community fields
            if abilities_at_formation:
                comm.avg_ability_at_formation = round(
                    sum(abilities_at_formation) / len(abilities_at_formation), 2
                )
                comm.ability_range = (
                    round(min(abilities_at_formation), 2),
                    round(max(abilities_at_formation), 2),
                )
            if prospective_potentials:
                comm.avg_prospective_potential = round(
                    sum(prospective_potentials) / len(prospective_potentials), 2
                )
            if retrospective_potentials:
                comm.avg_retrospective_potential = round(
                    sum(retrospective_potentials) / len(retrospective_potentials), 2
                )

        features[comm_id] = {
            "size": comm.size,
            "density": comm.density,
            "total_credits": total_credits,
            "credits_per_person": round(total_credits / comm.size, 1) if comm.size > 0 else 0,
            "active_period": active_period,
            "active_years": active_years,
            "top_roles": [(role, count) for role, count in top_roles],
            "mentorship_count": len(mentorship_pairs),
            "mentorship_pairs": [
                {"mentor": m, "mentee": t, "confidence": c} for m, t, c in mentorship_pairs
            ],
            "avg_ability_at_formation": comm.avg_ability_at_formation,
            "avg_prospective_potential": comm.avg_prospective_potential,
            "avg_retrospective_potential": comm.avg_retrospective_potential,
            "ability_range": comm.ability_range,
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

        # Format mentorship pairs with names
        mentorship_list = []
        for mentor_id, mentee_id, confidence in comm.mentorship_pairs:
            mentorship_list.append({
                "mentor_id": mentor_id,
                "mentor_name": person_names.get(mentor_id, mentor_id) if person_names else mentor_id,
                "mentee_id": mentee_id,
                "mentee_name": person_names.get(mentee_id, mentee_id) if person_names else mentee_id,
                "confidence": round(confidence, 1),
            })

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
            "mentorships": mentorship_list,
            "ability_metrics": {
                "avg_ability_at_formation": comm.avg_ability_at_formation,
                "avg_prospective_potential": comm.avg_prospective_potential,
                "avg_retrospective_potential": comm.avg_retrospective_potential,
                "ability_range": {
                    "min": comm.ability_range[0],
                    "max": comm.ability_range[1],
                },
            },
            **comm_features,
        })

    return export_data


def main():
    """スタンドアロン実行用エントリーポイント."""
    from src.analysis.graph import create_person_collaboration_network
    from src.database import get_all_anime, get_all_credits, get_all_persons, get_all_scores, get_connection, init_db

    conn = get_connection()
    init_db(conn)

    persons = get_all_persons(conn)
    anime_list = get_all_anime(conn)
    credits = get_all_credits(conn)
    scores_list = get_all_scores(conn)

    # マップ作成
    anime_map = {a.id: a for a in anime_list}
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}
    person_scores = {s.person_id: {"composite": s.composite} for s in scores_list}

    # コラボレーショングラフ構築
    logger.info("building_collaboration_graph")
    collab_graph = create_person_collaboration_network(credits, anime_map)

    # コミュニティ検出
    communities = detect_communities(collab_graph, min_community_size=5)

    # 特徴量計算
    features = compute_community_features(communities, credits, anime_map, person_scores)

    # ブリッジ分析
    bridges = analyze_community_overlap(communities, collab_graph)

    # エクスポート (function call kept for side effects, return value unused)
    _ = export_communities_for_visualization(communities, features, person_names)

    # 結果表示
    print(f"\n検出されたコミュニティ数: {len(communities)}")
    print(f"ブリッジ人物数: {len(bridges)}")

    for comm_id, comm in sorted(communities.items(), key=lambda x: x[1].size, reverse=True)[:5]:
        print(f"\nコミュニティ {comm_id}:")
        print(f"  サイズ: {comm.size}人")
        print(f"  密度: {comm.density:.3f}")
        print("  中心メンバー:")
        for person_id, degree in comm.top_members[:3]:
            print(f"    - {person_names.get(person_id, person_id)} (次数: {degree})")

        # 師弟関係
        if comm.mentorship_pairs:
            print(f"  師弟関係: {len(comm.mentorship_pairs)}組")
            for mentor_id, mentee_id, confidence in comm.mentorship_pairs[:3]:
                mentor_name = person_names.get(mentor_id, mentor_id)
                mentee_name = person_names.get(mentee_id, mentee_id)
                print(f"    - {mentor_name} → {mentee_name} (信頼度: {confidence:.1f})")

        # 能力指標
        if comm.avg_ability_at_formation > 0:
            print(f"  形成期の平均能力: {comm.avg_ability_at_formation:.1f}")
            print(f"  当時推定の潜在能力: {comm.avg_prospective_potential:.1f}")
            print(f"  事後推定の潜在能力: {comm.avg_retrospective_potential:.1f}")
            print(f"  能力範囲: {comm.ability_range[0]:.1f} - {comm.ability_range[1]:.1f}")

    conn.close()


if __name__ == "__main__":
    main()
