"""Authority score computation via weighted PageRank.

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
    nstart: dict[str, float] | None = None,
) -> dict[str, float]:
    """Compute weighted PageRank.

    NetworkX の pagerank に weight パラメータを渡すことで
    重み付きバージョンを使用する。

    Args:
        graph: 有向グラフ
        damping: ダンピングファクタ (default: 0.85)
        max_iter: 最大反復回数
        tol: 収束閾値
        nstart: 初期スコア分布 (warm start用、Noneなら均一分布)
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
            nstart=nstart,
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
            nstart=nstart,
        )

    return scores


def normalize_scores(scores: dict[str, float]) -> dict[str, float]:
    """Normalise scores to the 0-100 range."""
    if not scores:
        return {}

    values = np.array(list(scores.values()))
    min_val = values.min()
    max_val = values.max()

    if max_val == min_val:
        return {k: 50.0 for k in scores}

    return {
        k: float((v - min_val) / (max_val - min_val) * 100.0) for k, v in scores.items()
    }


def compute_authority_scores(
    graph: nx.DiGraph,
    person_only: bool = True,
) -> dict[str, float]:
    """Compute Authority scores.

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
    """Entry point: build the graph and compute Authority scores."""
    import json

    from src.analysis.graph import create_person_anime_network
    from src.analysis.io.silver_reader import (
        load_anime_silver,
        load_credits_silver,
        load_persons_silver,
    )
    from src.infra.logging import setup_logging
    from src.utils.config import JSON_DIR

    setup_logging()

    persons = load_persons_silver()
    anime_list = load_anime_silver()
    credits = load_credits_silver()

    if not credits:
        logger.warning("No credits found in silver DB.")
        return

    graph = create_person_anime_network(persons, anime_list, credits)
    authority = compute_authority_scores(graph)

    # JSON output
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    for person_id, score in sorted(authority.items(), key=lambda x: x[1], reverse=True):
        node_data = graph.nodes[person_id]
        results.append(
            {
                "person_id": person_id,
                "name": node_data.get("name", ""),
                "name_ja": node_data.get("name_ja", ""),
                "name_en": node_data.get("name_en", ""),
                "authority_score": round(score, 4),
            }
        )

    output_path = JSON_DIR / "authority_scores.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    logger.info("authority_scores_saved", path=str(output_path), persons=len(results))


if __name__ == "__main__":
    main()
