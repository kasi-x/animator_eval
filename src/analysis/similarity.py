"""類似人物検索 — スコアプロファイルの類似度を算出する.

3軸スコアベクトル [authority, trust, skill] のコサイン類似度で
類似プロファイルの人物を検索する。
"""

import math

import structlog

logger = structlog.get_logger()


def _cosine_similarity(a: tuple[float, ...], b: tuple[float, ...]) -> float:
    """2つのベクトルのコサイン類似度を算出する."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def find_similar_persons(
    target_id: str,
    results: list[dict],
    top_n: int = 10,
) -> list[dict]:
    """スコアプロファイルが類似した人物を検索する.

    Args:
        target_id: 対象人物ID
        results: パイプライン結果 (scores.json の内容)
        top_n: 返す件数

    Returns:
        [{person_id, name, similarity, authority, trust, skill, composite}]
    """
    target = None
    for r in results:
        if r["person_id"] == target_id:
            target = r
            break

    if target is None:
        return []

    target_vec = (
        target.get("authority", 0),
        target.get("trust", 0),
        target.get("skill", 0),
    )

    similarities = []
    for r in results:
        if r["person_id"] == target_id:
            continue
        vec = (
            r.get("authority", 0),
            r.get("trust", 0),
            r.get("skill", 0),
        )
        sim = _cosine_similarity(target_vec, vec)
        similarities.append(
            {
                "person_id": r["person_id"],
                "name": r.get("name", r["person_id"]),
                "similarity": round(sim, 4),
                "authority": r.get("authority", 0),
                "trust": r.get("trust", 0),
                "skill": r.get("skill", 0),
                "composite": r.get("composite", 0),
            }
        )

    similarities.sort(key=lambda x: x["similarity"], reverse=True)
    return similarities[:top_n]
