"""スコア信頼度 — クレジット数とデータ品質に基づく信頼区間.

少数のクレジットしかない人物のスコアは信頼性が低い。
信頼度は 0.0〜1.0 の値で、スコアの確信度を表す。
"""

import math

import structlog

logger = structlog.get_logger()

# 信頼度計算のパラメータ
MIN_CREDITS_FOR_FULL_CONFIDENCE = 20  # この数以上で信頼度 ~1.0
SOURCE_DIVERSITY_BONUS = 0.1  # 複数ソースからのデータがある場合のボーナス
YEAR_SPAN_BONUS_THRESHOLD = 3  # 3年以上の活動期間でボーナス


def compute_confidence(
    credit_count: int,
    source_count: int = 1,
    year_span: int = 0,
) -> float:
    """個人スコアの信頼度を計算する.

    Args:
        credit_count: クレジット数
        source_count: データソース数（AniList, MAL など）
        year_span: 活動年数

    Returns:
        0.0〜1.0 の信頼度
    """
    if credit_count == 0:
        return 0.0

    # Base confidence from credit count (asymptotic approach to 1.0)
    # Using 1 - e^(-k*n) curve
    k = 3.0 / MIN_CREDITS_FOR_FULL_CONFIDENCE  # k such that f(20) ≈ 0.78
    base = 1.0 - math.exp(-k * credit_count)

    # Source diversity bonus
    source_bonus = min(source_count - 1, 2) * SOURCE_DIVERSITY_BONUS

    # Year span bonus (longer careers = more reliable)
    year_bonus = 0.0
    if year_span >= YEAR_SPAN_BONUS_THRESHOLD:
        year_bonus = min(year_span / 10.0, 0.1)  # Max 0.1 bonus for 10+ years

    confidence = min(base + source_bonus + year_bonus, 1.0)
    return round(confidence, 3)


def compute_score_range(
    score: float,
    confidence: float,
    scale: float = 100.0,
) -> tuple[float, float]:
    """スコアの信頼区間を計算する.

    信頼度が低いほど区間が広がる。

    Args:
        score: 元スコア (0-scale)
        confidence: 信頼度 (0-1)
        scale: スコア上限

    Returns:
        (lower_bound, upper_bound)
    """
    if confidence >= 0.99:
        return (score, score)

    # Uncertainty margin: inversely proportional to confidence
    # At confidence=0, margin = ±50; at confidence=1, margin = 0
    max_margin = scale * 0.5
    margin = max_margin * (1.0 - confidence)

    lower = max(0.0, score - margin)
    upper = min(scale, score + margin)
    return (round(lower, 1), round(upper, 1))


def batch_compute_confidence(
    results: list[dict],
    credits_per_person: dict[str, int] | None = None,
    sources_per_person: dict[str, int] | None = None,
) -> list[dict]:
    """結果リストに信頼度と信頼区間を付加する.

    Args:
        results: scores.json の形式
        credits_per_person: {person_id: credit_count}
        sources_per_person: {person_id: source_count}

    Returns:
        results with "confidence" and "score_range" added
    """
    for r in results:
        pid = r["person_id"]
        credit_count = r.get("total_credits", 0)
        if credits_per_person:
            credit_count = credits_per_person.get(pid, credit_count)

        source_count = 1
        if sources_per_person:
            source_count = sources_per_person.get(pid, 1)

        year_span = 0
        if r.get("career"):
            career = r["career"]
            year_span = career.get("active_years", 0)

        conf = compute_confidence(credit_count, source_count, year_span)
        r["confidence"] = conf
        r["score_range"] = {
            "iv_score": compute_score_range(r["iv_score"], conf),
            "person_fe": compute_score_range(r["person_fe"], conf),
            "birank": compute_score_range(r["birank"], conf),
            "patronage": compute_score_range(r["patronage"], conf),
        }

    logger.info("confidence_computed", persons=len(results))
    return results
