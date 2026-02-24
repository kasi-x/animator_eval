"""GraphML エクスポート — Gephi 等の外部ツール向けグラフ出力.

コラボレーショングラフをGraphML形式でエクスポートし、
Gephi, Cytoscape, yEd 等のグラフ可視化ツールで読み込めるようにする。
"""

from pathlib import Path

import networkx as nx
import structlog

from src.models import Credit, Person

logger = structlog.get_logger()


def export_graphml(
    persons: list[Person],
    credits: list[Credit],
    person_scores: dict[str, dict] | None = None,
    output_path: Path | None = None,
    collaboration_graph: nx.Graph | None = None,
    prettyprint: bool = True,
    round_decimals: int = 2,
    max_edges: int = 1_000_000,
    top_n_persons: int = 5000,
) -> Path:
    """コラボレーショングラフをGraphML形式でエクスポートする.

    大規模グラフ (>max_edges) の場合はcompositeスコア上位top_n_personsの
    サブグラフのみエクスポートする（Gephi等でも62M辺は扱えないため）。

    Args:
        persons: 人物リスト
        credits: クレジットリスト
        person_scores: {person_id: {authority, trust, skill, composite, ...}}
        output_path: 出力パス (None の場合はデフォルト)
        collaboration_graph: 既存のコラボレーショングラフ (再利用で高速化)
        prettyprint: XMLを整形するか (False で高速化、デフォルト True)
        round_decimals: float属性の丸め桁数 (デフォルト 2)
        max_edges: これを超えるとサブグラフに縮小 (デフォルト 1M)
        top_n_persons: サブグラフに含める上位人数 (デフォルト 5000)

    Returns:
        出力ファイルパス
    """
    from collections import defaultdict

    from src.utils.config import JSON_DIR

    if output_path is None:
        output_path = JSON_DIR / "collaboration_graph.graphml"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if graph is too large and needs subgraph extraction
    total_edges = collaboration_graph.number_of_edges() if collaboration_graph else None
    use_subgraph = total_edges is not None and total_edges > max_edges

    # Determine which person IDs to include
    if use_subgraph and person_scores:
        # Select top N persons by composite score
        top_pids = {
            pid
            for pid, _ in sorted(
                ((pid, ps.get("composite", 0)) for pid, ps in person_scores.items()),
                key=lambda x: x[1],
                reverse=True,
            )[:top_n_persons]
        }
        logger.info(
            "graphml_subgraph_mode",
            total_edges=total_edges,
            max_edges=max_edges,
            top_n_persons=len(top_pids),
        )
    else:
        top_pids = None  # Include all

    g = nx.Graph()

    # Add person nodes with attributes
    person_map = {p.id: p for p in persons}
    node_ids = top_pids if top_pids is not None else {p.id for p in persons}
    for pid in node_ids:
        p = person_map.get(pid)
        if not p:
            continue
        attrs: dict = {
            "label": p.display_name,
            "name_ja": p.name_ja or "",
            "name_en": p.name_en or "",
        }
        if person_scores and p.id in person_scores:
            ps = person_scores[p.id]
            for key in ("authority", "trust", "skill", "composite"):
                if key in ps:
                    attrs[key] = round(float(ps[key]), round_decimals)
            if "primary_role" in ps:
                attrs["category"] = str(ps["primary_role"])
        g.add_node(p.id, **attrs)

    if collaboration_graph is not None:
        # Reuse existing collaboration graph edges (avoids O(n²) recomputation)
        # Only iterate edges of nodes in our subgraph, not all 11M+ edges
        for pid in node_ids:
            if pid not in collaboration_graph:
                continue
            for v, data in collaboration_graph[pid].items():
                if pid < v and g.has_node(v):
                    shared = int(data.get("shared_works", 1))
                    g.add_edge(pid, v, weight=shared, shared_works=shared)
    else:
        # Fallback: build edges from credits (slow path)
        anime_persons: dict[str, list[str]] = defaultdict(list)
        for c in credits:
            anime_persons[c.anime_id].append(c.person_id)

        # Pre-aggregate edges in memory (same pattern as graph.py optimization)
        edge_counts: dict[tuple[str, str], int] = defaultdict(int)
        for anime_id, pids in anime_persons.items():
            unique_pids = list(set(pids))
            if top_pids is not None:
                unique_pids = [p for p in unique_pids if p in top_pids]
            for i in range(len(unique_pids)):
                for j in range(i + 1, len(unique_pids)):
                    a, b = unique_pids[i], unique_pids[j]
                    key = (a, b) if a < b else (b, a)
                    edge_counts[key] += 1
        g.add_edges_from(
            (a, b, {"weight": cnt, "shared_works": cnt})
            for (a, b), cnt in edge_counts.items()
        )

    nx.write_graphml_lxml(g, str(output_path), prettyprint=prettyprint)

    logger.info(
        "graphml_exported",
        path=str(output_path),
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
        subgraph=use_subgraph,
    )
    return output_path
