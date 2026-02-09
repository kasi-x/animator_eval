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
) -> Path:
    """コラボレーショングラフをGraphML形式でエクスポートする.

    Args:
        persons: 人物リスト
        credits: クレジットリスト
        person_scores: {person_id: {authority, trust, skill, composite, ...}}
        output_path: 出力パス (None の場合はデフォルト)

    Returns:
        出力ファイルパス
    """
    from collections import defaultdict

    from src.utils.config import JSON_DIR

    if output_path is None:
        output_path = JSON_DIR / "collaboration_graph.graphml"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    g = nx.Graph()

    # Add person nodes with attributes
    for p in persons:
        attrs: dict = {
            "label": p.display_name,
            "name_ja": p.name_ja or "",
            "name_en": p.name_en or "",
        }
        if person_scores and p.id in person_scores:
            ps = person_scores[p.id]
            for key in ("authority", "trust", "skill", "composite"):
                if key in ps:
                    attrs[key] = float(ps[key])
            if "primary_role" in ps:
                attrs["category"] = str(ps["primary_role"])
        g.add_node(p.id, **attrs)

    # Build edges from shared credits
    anime_persons: dict[str, list[str]] = defaultdict(list)
    for c in credits:
        anime_persons[c.anime_id].append(c.person_id)

    for anime_id, pids in anime_persons.items():
        unique_pids = list(set(pids))
        for i, a in enumerate(unique_pids):
            for b in unique_pids[i + 1:]:
                if g.has_edge(a, b):
                    g[a][b]["weight"] += 1
                    g[a][b]["shared_works"] += 1
                else:
                    g.add_edge(a, b, weight=1, shared_works=1)

    nx.write_graphml(g, str(output_path))

    logger.info(
        "graphml_exported",
        path=str(output_path),
        nodes=g.number_of_nodes(),
        edges=g.number_of_edges(),
    )
    return output_path
