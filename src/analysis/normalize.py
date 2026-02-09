"""スコア正規化 — 各軸のスコアを 0-100 スケールに正規化する.

異なるアルゴリズム由来の Authority / Trust / Skill を比較可能にするため、
min-max / percentile / z-score 正規化で 0-100 に揃える。
"""

import math
from enum import Enum

import structlog

logger = structlog.get_logger()


class NormalizationMethod(str, Enum):
    """スコア正規化の戦略を定義する列挙型.

    Defines the strategy for normalizing scores to a common scale.
    """

    MIN_MAX = "minmax"  # Min-max normalization (linear scaling)
    PERCENTILE = "percentile"  # Percentile rank normalization
    Z_SCORE = "zscore"  # Z-score normalization (mean=50, ±2σ = 0 or 100)


def normalize_minmax(
    scores: dict[str, float],
    target_maximum_value: float = 100.0,
) -> dict[str, float]:
    """min-max 正規化: [0, target_maximum_value].

    Rescales scores linearly to range from 0 to target_maximum_value.
    """
    if not scores:
        return {}

    values = list(scores.values())
    min_val = min(values)
    max_val = max(values)
    spread = max_val - min_val

    if spread == 0:
        return {pid: target_maximum_value / 2 for pid in scores}

    return {
        pid: round((val - min_val) / spread * target_maximum_value, 2)
        for pid, val in scores.items()
    }


def normalize_percentile(
    scores: dict[str, float],
    target_maximum_value: float = 100.0,
) -> dict[str, float]:
    """パーセンタイル正規化: 順位ベースで [0, target_maximum_value].

    Assigns scores based on percentile rank.
    """
    if not scores:
        return {}

    n = len(scores)
    if n == 1:
        return {pid: target_maximum_value / 2 for pid in scores}

    sorted_pids = sorted(scores.keys(), key=lambda pid: scores[pid])
    return {
        pid: round(rank / (n - 1) * target_maximum_value, 2)
        for rank, pid in enumerate(sorted_pids)
    }


def normalize_zscore(
    scores: dict[str, float],
    target_maximum_value: float = 100.0,
) -> dict[str, float]:
    """z-score 正規化: 平均50, 標準偏差に基づきスケール.

    Normalizes using z-scores: mean maps to target_maximum_value/2, ±2σ map to 0 or target_maximum_value.
    z-score を [0, target_maximum_value] にクリップする。
    mean → target_maximum_value/2, ±2σ → 0 or target_maximum_value.
    """
    if not scores:
        return {}

    values = list(scores.values())
    n = len(values)
    if n <= 1:
        return {pid: target_maximum_value / 2 for pid in scores}

    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    std = math.sqrt(variance) if variance > 0 else 1.0

    return {
        pid: round(
            max(0, min(target_maximum_value, (((val - mean) / std) + 2) / 4 * target_maximum_value)), 2
        )
        for pid, val in scores.items()
    }


def normalize_scores(
    scores: dict[str, float],
    target_maximum_value: float = 100.0,
    method: str | NormalizationMethod | None = None,
) -> dict[str, float]:
    """スコア辞書を正規化する.

    Normalizes a dictionary of scores using the specified method.

    Args:
        scores: {person_id: raw_score}
        target_maximum_value: 最大値（デフォルト100） / Maximum value for normalized scores
        method: Normalization strategy - "minmax" | "percentile" | "zscore" (None = use config default)

    Returns:
        {person_id: normalized_score}
    """
    if method is None:
        from src.utils.config import NORMALIZATION_METHOD
        method = NORMALIZATION_METHOD

    # Convert string to enum if needed
    if isinstance(method, str):
        method_str = method
    else:
        method_str = method.value if isinstance(method, NormalizationMethod) else method

    if method_str == NormalizationMethod.PERCENTILE or method_str == "percentile":
        return normalize_percentile(scores, target_maximum_value)
    elif method_str == NormalizationMethod.Z_SCORE or method_str == "zscore":
        return normalize_zscore(scores, target_maximum_value)
    else:
        return normalize_minmax(scores, target_maximum_value)


def normalize_all_axes(
    authority_scores: dict[str, float],
    trust_scores: dict[str, float],
    skill_scores: dict[str, float],
    method: str | None = None,
) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    """3軸全てを正規化する.

    Returns:
        (normalized_authority, normalized_trust, normalized_skill)
    """
    norm_a = normalize_scores(authority_scores, method=method)
    norm_t = normalize_scores(trust_scores, method=method)
    norm_s = normalize_scores(skill_scores, method=method)

    logger.info(
        "scores_normalized",
        method=method or "config_default",
        authority_count=len(norm_a),
        trust_count=len(norm_t),
        skill_count=len(norm_s),
    )

    return norm_a, norm_t, norm_s
