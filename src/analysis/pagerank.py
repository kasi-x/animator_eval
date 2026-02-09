"""重み付き PageRank による Authority スコア算出.

PR(u) = (1-d)/N + d * Σ [PR(v) * W(v,u) / L(v)]   for v in B_u

d = 0.85 (damping factor)
W(v,u) = edge weight from v to u
B_u = set of nodes linking to u
L(v) = sum of outgoing edge weights from v
"""

import networkx as nx
import numpy as np
import structlog

from src.utils.config import CONVERGENCE_THRESHOLD, DAMPING_FACTOR, MAX_ITERATIONS

logger = structlog.get_logger()


def weighted_pagerank(
    graph: nx.DiGraph,
    damping: float = DAMPING_FACTOR,
    max_iter: int = MAX_ITERATIONS,
    tol: float = CONVERGENCE_THRESHOLD,
) -> dict[str, float]:
    """重み付き PageRank を計算する.

    NetworkX の pagerank に weight パラメータを渡すことで
    重み付きバージョンを使用する。
    """
    if graph.number_of_nodes() == 0:
        return {}

    try:
        scores = nx.pagerank(
            graph,
            alpha=damping,
            max_iter=max_iter,
            tol=tol,
            weight="weight",
        )
    except nx.PowerIterationFailedConvergence:
        logger.warning(
            "pagerank_convergence_failed",
            max_iter=max_iter,
        )
        scores = nx.pagerank(
            graph,
            alpha=damping,
            max_iter=max_iter * 2,
            tol=tol * 10,
            weight="weight",
        )

    return scores


def normalize_scores(scores: dict[str, float]) -> dict[str, float]:
    """スコアを 0-100 の範囲に正規化する."""
    if not scores:
        return {}

    values = np.array(list(scores.values()))
    min_val = values.min()
    max_val = values.max()

    if max_val == min_val:
        return {k: 50.0 for k in scores}

    return {
        k: float((v - min_val) / (max_val - min_val) * 100.0)
        for k, v in scores.items()
    }


def compute_authority_scores(
    graph: nx.DiGraph,
    person_only: bool = True,
) -> dict[str, float]:
    """Authority スコアを算出する.

    Args:
        graph: 二部グラフ (person ↔ anime)
        person_only: True の場合、person ノードのみ返す
    """
    raw_scores = weighted_pagerank(graph)

    if person_only:
        raw_scores = {
            k: v
            for k, v in raw_scores.items()
            if graph.nodes[k].get("type") == "person"
        }

    normalized = normalize_scores(raw_scores)

    logger.info("authority_scores_computed", nodes=len(normalized))
    if normalized:
        top5 = sorted(normalized.items(), key=lambda x: x[1], reverse=True)[:5]
        for node_id, score in top5:
            name = graph.nodes[node_id].get("name", node_id)
            logger.info("top_authority", name=name, score=score)

    return normalized


def main() -> None:
    """エントリーポイント: グラフを構築し Authority スコアを算出."""
    import json

    from src.analysis.graph import build_bipartite_graph
    from src.database import (
        get_connection,
        init_db,
        load_all_anime,
        load_all_credits,
        load_all_persons,
        upsert_score,
    )
    from src.log import setup_logging
    from src.models import ScoreResult
    from src.utils.config import JSON_DIR

    setup_logging()

    conn = get_connection()
    init_db(conn)

    persons = load_all_persons(conn)
    anime_list = load_all_anime(conn)
    credits = load_all_credits(conn)

    if not credits:
        logger.warning("No credits found in DB.")
        conn.close()
        return

    graph = build_bipartite_graph(persons, anime_list, credits)
    authority = compute_authority_scores(graph)

    # DB に保存
    for person_id, score in authority.items():
        upsert_score(conn, ScoreResult(person_id=person_id, authority=score))
    conn.commit()

    # JSON 出力
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    for person_id, score in sorted(authority.items(), key=lambda x: x[1], reverse=True):
        node_data = graph.nodes[person_id]
        results.append({
            "person_id": person_id,
            "name": node_data.get("name", ""),
            "name_ja": node_data.get("name_ja", ""),
            "name_en": node_data.get("name_en", ""),
            "authority_score": round(score, 4),
        })

    output_path = JSON_DIR / "authority_scores.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    logger.info("authority_scores_saved", path=str(output_path), persons=len(results))
    conn.close()


if __name__ == "__main__":
    main()
