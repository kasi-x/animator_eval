"""類似人物検索 — スコアプロファイルの類似度を算出する.

3軸スコアベクトル [person_fe, birank, patronage] のコサイン類似度で
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
        [{person_id, name, similarity, person_fe, birank, patronage, iv_score}]
    """
    target = None
    for r in results:
        if r["person_id"] == target_id:
            target = r
            break

    if target is None:
        return []

    target_vec = (
        target.get("person_fe", 0),
        target.get("birank", 0),
        target.get("patronage", 0),
    )

    similarities = []
    for r in results:
        if r["person_id"] == target_id:
            continue
        vec = (
            r.get("person_fe", 0),
            r.get("birank", 0),
            r.get("patronage", 0),
        )
        sim = _cosine_similarity(target_vec, vec)
        similarities.append(
            {
                "person_id": r["person_id"],
                "name": r.get("name", r["person_id"]),
                "similarity": round(sim, 4),
                "person_fe": r.get("person_fe", 0),
                "birank": r.get("birank", 0),
                "patronage": r.get("patronage", 0),
                "iv_score": r.get("iv_score", 0),
            }
        )

    similarities.sort(key=lambda x: x["similarity"], reverse=True)
    return similarities[:top_n]
