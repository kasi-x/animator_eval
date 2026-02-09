"""コラボレーショングラフ構築 (NetworkX).

ノード種別:
  - person: アニメーター、監督等
  - anime: アニメ作品

エッジ:
  - person → anime: クレジット関係 (weight = 役職重み)
  - person → person: 共同クレジット関係 (weight = 共演回数 × 役職重み)
"""

from collections import defaultdict

import networkx as nx
import structlog

from src.models import Anime, Credit, Person, Role
from src.utils.config import ROLE_WEIGHTS

logger = structlog.get_logger()


def _role_weight(role: Role) -> float:
    """役職に応じたエッジ重みを返す."""
    return ROLE_WEIGHTS.get(role.value, 1.0)


def create_person_anime_network(
    persons: list[Person],
    anime_list: list[Anime],
    credits: list[Credit],
) -> nx.DiGraph:
    """二部グラフ (person ↔ anime) を構築する.

    Creates a bipartite network connecting people to the anime works they contributed to.
    """
    g = nx.DiGraph()

    # ノード追加
    for p in persons:
        g.add_node(p.id, type="person", name=p.display_name, **{"name_ja": p.name_ja, "name_en": p.name_en})
    for a in anime_list:
        g.add_node(a.id, type="anime", name=a.display_title, year=a.year, score=a.score)

    # クレジットエッジ
    for c in credits:
        weight = _role_weight(c.role)
        # person → anime
        if g.has_edge(c.person_id, c.anime_id):
            g[c.person_id][c.anime_id]["weight"] += weight
            g[c.person_id][c.anime_id]["roles"].append(c.role.value)
        else:
            g.add_edge(
                c.person_id, c.anime_id, weight=weight, roles=[c.role.value]
            )
        # anime → person (逆方向、PageRank 伝播用)
        if g.has_edge(c.anime_id, c.person_id):
            g[c.anime_id][c.person_id]["weight"] += weight
        else:
            g.add_edge(c.anime_id, c.person_id, weight=weight)

    logger.info(
        "bipartite_graph_built",
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g


def create_person_collaboration_network(
    persons: list[Person],
    credits: list[Credit],
) -> nx.Graph:
    """人物間コラボレーション無向グラフを構築する.

    Creates a network of people who worked together on the same anime.
    同じ作品に参加した人物同士にエッジを張る。
    エッジ重み = Σ(role_weight_a × role_weight_b) / max_weight で正規化。
    """
    g = nx.Graph()

    for p in persons:
        g.add_node(p.id, name=p.display_name, name_ja=p.name_ja, name_en=p.name_en)

    # anime_id → [(person_id, role, weight)]
    anime_credits: dict[str, list[tuple[str, Role, float]]] = defaultdict(list)
    for c in credits:
        w = _role_weight(c.role)
        anime_credits[c.anime_id].append((c.person_id, c.role, w))

    # 同一作品の全ペアにエッジを追加
    for anime_id, staff in anime_credits.items():
        for i, (pid_a, role_a, w_a) in enumerate(staff):
            for pid_b, role_b, w_b in staff[i + 1 :]:
                if pid_a == pid_b:
                    continue
                edge_weight = (w_a + w_b) / 2.0
                if g.has_edge(pid_a, pid_b):
                    g[pid_a][pid_b]["weight"] += edge_weight
                    g[pid_a][pid_b]["shared_works"] += 1
                else:
                    g.add_edge(
                        pid_a,
                        pid_b,
                        weight=edge_weight,
                        shared_works=1,
                    )

    logger.info(
        "collaboration_graph_built",
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g


def create_director_animator_network(
    credits: list[Credit],
) -> nx.DiGraph:
    """監督→アニメーター の有向グラフを構築する.

    Creates a directed network showing which directors worked with which animators.
    同一作品で監督/演出とアニメーターが共演した場合にエッジを張る。
    Trust スコアの算出に使用。
    """
    g = nx.DiGraph()

    director_roles = {
        Role.DIRECTOR,
        Role.EPISODE_DIRECTOR,
        Role.CHIEF_ANIMATION_DIRECTOR,
    }
    animator_roles = {
        Role.ANIMATION_DIRECTOR,
        Role.KEY_ANIMATOR,
        Role.SECOND_KEY_ANIMATOR,
        Role.IN_BETWEEN,
    }

    # anime_id → directors/animators
    anime_directors: dict[str, list[tuple[str, float]]] = defaultdict(list)
    anime_animators: dict[str, list[tuple[str, float]]] = defaultdict(list)

    for c in credits:
        w = _role_weight(c.role)
        if c.role in director_roles:
            anime_directors[c.anime_id].append((c.person_id, w))
        if c.role in animator_roles:
            anime_animators[c.anime_id].append((c.person_id, w))

    for anime_id in anime_directors:
        if anime_id not in anime_animators:
            continue
        for dir_id, dir_w in anime_directors[anime_id]:
            for anim_id, anim_w in anime_animators[anime_id]:
                if dir_id == anim_id:
                    continue
                edge_w = (dir_w + anim_w) / 2.0
                if g.has_edge(dir_id, anim_id):
                    g[dir_id][anim_id]["weight"] += edge_w
                    g[dir_id][anim_id]["works"].append(anime_id)
                else:
                    g.add_edge(
                        dir_id,
                        anim_id,
                        weight=edge_w,
                        works=[anime_id],
                    )

    logger.info(
        "director_animator_graph_built",
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return g


def determine_primary_role_for_each_person(
    credits: list[Credit],
) -> dict[str, dict[str, int | str]]:
    """各人物の役職分布と主要カテゴリを算出する.

    Determines each person's primary role category based on their credit distribution.
    Returns:
        {person_id: {"primary_category": "animator"|"director"|...,
                      "role_counts": {role: count}, "total_credits": int}}
    """
    CATEGORY_MAP = {
        Role.DIRECTOR: "director",
        Role.CHIEF_ANIMATION_DIRECTOR: "director",
        Role.EPISODE_DIRECTOR: "director",
        Role.STORYBOARD: "director",
        Role.ANIMATION_DIRECTOR: "animator",
        Role.KEY_ANIMATOR: "animator",
        Role.SECOND_KEY_ANIMATOR: "animator",
        Role.IN_BETWEEN: "animator",
        Role.LAYOUT: "animator",
        Role.EFFECTS: "animator",
        Role.CHARACTER_DESIGNER: "designer",
        Role.MECHANICAL_DESIGNER: "designer",
        Role.ART_DIRECTOR: "designer",
        Role.COLOR_DESIGNER: "designer",
        Role.BACKGROUND_ART: "designer",
        Role.CGI_DIRECTOR: "technical",
        Role.PHOTOGRAPHY_DIRECTOR: "technical",
        Role.PRODUCER: "production",
        Role.SOUND_DIRECTOR: "production",
        Role.MUSIC: "production",
        Role.SERIES_COMPOSITION: "writing",
        Role.SCREENPLAY: "writing",
        Role.ORIGINAL_CREATOR: "writing",
    }

    person_roles: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for c in credits:
        person_roles[c.person_id][c.role.value] += 1

    result: dict[str, dict[str, int | str]] = {}
    for pid, role_counts in person_roles.items():
        # カテゴリ別の集計
        category_counts: dict[str, int] = defaultdict(int)
        total = 0
        for role_str, count in role_counts.items():
            total += count
            try:
                role = Role(role_str)
                cat = CATEGORY_MAP.get(role, "other")
            except ValueError:
                cat = "other"
            category_counts[cat] += count

        primary = max(category_counts, key=category_counts.get) if category_counts else "other"

        result[pid] = {
            "primary_category": primary,
            "role_counts": dict(role_counts),
            "total_credits": total,
        }

    logger.info("role_classification_complete", persons=len(result))
    return result


LARGE_GRAPH_THRESHOLD = 500  # nodes


def calculate_network_centrality_scores(
    graph: nx.Graph,
    person_ids: set[str] | None = None,
) -> dict[str, dict[str, float]]:
    """各種中心性指標を算出する.

    Calculates how central each person is to the collaboration network.
    大規模グラフ (>500ノード) の場合は近似アルゴリズムを使用する。

    Args:
        graph: 無向コラボレーショングラフ
        person_ids: 対象ノードの限定（None の場合は全ノード）

    Returns:
        {person_id: {"betweenness": ..., "closeness": ..., "degree": ..., "eigenvector": ...}}
    """
    if graph.number_of_nodes() == 0:
        return {}

    n_nodes = graph.number_of_nodes()
    is_large = n_nodes > LARGE_GRAPH_THRESHOLD

    if is_large:
        logger.info(
            "large_graph_detected",
            nodes=n_nodes,
            edges=graph.number_of_edges(),
            using_approximation=True,
        )

    metrics: dict[str, dict[str, float]] = {}

    # 次数中心性 (always fast: O(V))
    degree = nx.degree_centrality(graph)

    # 媒介中心性 — 大規模グラフでは近似版を使用
    if is_large:
        # k=min(100, V) サンプルで近似 O(k*(V+E))
        k = min(100, n_nodes)
        betweenness = nx.betweenness_centrality(graph, k=k, weight="weight", seed=42)
    else:
        betweenness = nx.betweenness_centrality(graph, weight="weight")

    # 近接中心性 — 大規模グラフではスキップ（O(V*(V+E))で高コスト）
    closeness: dict = {}
    if not is_large:
        for component in nx.connected_components(graph):
            subg = graph.subgraph(component)
            if subg.number_of_nodes() > 1:
                c = nx.closeness_centrality(subg, distance="weight")
                closeness.update(c)
            else:
                for n in component:
                    closeness[n] = 0.0

    # 固有ベクトル中心性（最大連結成分のみ）
    eigenvector: dict = {}
    if n_nodes > 1:
        try:
            largest_cc = max(nx.connected_components(graph), key=len)
            subg = graph.subgraph(largest_cc)
            eigenvector = nx.eigenvector_centrality(
                subg, max_iter=1000, weight="weight"
            )
        except (nx.PowerIterationFailedConvergence, nx.NetworkXError):
            logger.warning("eigenvector_centrality_failed")

    target_nodes = person_ids if person_ids else set(graph.nodes())
    for node in target_nodes:
        if node not in graph:
            continue
        metrics[node] = {
            "degree": degree.get(node, 0.0),
            "betweenness": betweenness.get(node, 0.0),
            "closeness": closeness.get(node, 0.0),
            "eigenvector": eigenvector.get(node, 0.0),
        }

    logger.info("centrality_metrics_computed", nodes=len(metrics))
    return metrics


def compute_graph_summary(graph: nx.Graph) -> dict:
    """グラフレベルの統計サマリーを算出する.

    Returns:
        {nodes, edges, density, avg_degree, components, largest_component_size}
    """
    n_nodes = graph.number_of_nodes()
    n_edges = graph.number_of_edges()

    if n_nodes == 0:
        return {
            "nodes": 0, "edges": 0, "density": 0.0,
            "avg_degree": 0.0, "components": 0, "largest_component_size": 0,
        }

    density = nx.density(graph)
    degrees = [d for _, d in graph.degree()]
    avg_degree = sum(degrees) / len(degrees) if degrees else 0.0
    components = list(nx.connected_components(graph))
    largest = max(len(c) for c in components) if components else 0

    summary = {
        "nodes": n_nodes,
        "edges": n_edges,
        "density": round(density, 6),
        "avg_degree": round(avg_degree, 2),
        "components": len(components),
        "largest_component_size": largest,
    }

    # Clustering coefficient (skip for very large graphs)
    if n_nodes <= 5000:
        try:
            avg_clustering = nx.average_clustering(graph, weight="weight")
            summary["avg_clustering"] = round(avg_clustering, 4)
        except Exception:
            pass

    logger.info("graph_summary", **summary)
    return summary


def main() -> None:
    """エントリーポイント: DBからデータを読み込みグラフを構築して保存."""
    import json

    from src.database import (
        get_connection,
        init_db,
        load_all_anime,
        load_all_credits,
        load_all_persons,
    )
    from src.log import setup_logging
    from src.utils.config import JSON_DIR

    setup_logging()

    conn = get_connection()
    init_db(conn)

    persons = load_all_persons(conn)
    anime_list = load_all_anime(conn)
    credits = load_all_credits(conn)
    conn.close()

    if not credits:
        logger.warning("No credits found in DB. Run scraper first.")
        return

    # 二部グラフ
    bp_graph = create_person_anime_network(persons, anime_list, credits)

    # コラボレーショングラフ
    collab_graph = create_person_collaboration_network(persons, credits)

    # 監督→アニメーターグラフ
    da_graph = create_director_animator_network(credits)

    # 統計出力
    stats = {
        "bipartite": {
            "nodes": bp_graph.number_of_nodes(),
            "edges": bp_graph.number_of_edges(),
        },
        "collaboration": {
            "nodes": collab_graph.number_of_nodes(),
            "edges": collab_graph.number_of_edges(),
        },
        "director_animator": {
            "nodes": da_graph.number_of_nodes(),
            "edges": da_graph.number_of_edges(),
        },
    }

    JSON_DIR.mkdir(parents=True, exist_ok=True)
    stats_path = JSON_DIR / "graph_stats.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    logger.info("graph_stats_saved", path=str(stats_path))
    logger.info("graph_stats", stats=stats)


if __name__ == "__main__":
    main()
