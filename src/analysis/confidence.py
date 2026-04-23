"""Score confidence — confidence intervals based on credit count and data quality.

少数のクレジットしかない人物のスコアは信頼性が低い。
信頼度は 0.0〜1.0 の値で、スコアの確信度を表す。
"""

import math

import structlog

logger = structlog.get_logger()

# parameters for confidence calculation
MIN_CREDITS_FOR_FULL_CONFIDENCE = 20  # この数以上で信頼度 ~1.0
SOURCE_DIVERSITY_BONUS = 0.1  # 複数ソースからのデータがある場合のボーナス
YEAR_SPAN_BONUS_THRESHOLD = 3  # 3年以上の活動期間でボーナス


def compute_confidence(
    credit_count: int,
    source_count: int = 1,
    year_span: int = 0,
) -> float:
    """Compute the confidence level for an individual score.

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
    """Compute confidence intervals for scores.

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


def compute_person_fe_ci(
    person_fe: float,
    n_obs: int,
    sigma_resid: float,
    ci_level: float = 0.95,
) -> tuple[float, float]:
    """Compute analytical confidence intervals for person_fe.

    SE_i = σ_resid / √n_obs_i, CI = θ_i ± t_{n-1,α/2} × SE_i.

    Uses the global residual standard deviation (estimated once from all AKM
    residuals) rather than per-person residuals, which avoids noisy variance
    estimates when a person has very few observations.

    For n_obs >= 30, uses z-approximation; otherwise uses t-distribution.

    Args:
        person_fe: 推定された person fixed effect (θ_i)
        n_obs: その人物の観測数（作品数）
        sigma_resid: AKM残差の全体標準偏差 (ddof=1)
        ci_level: 信頼水準 (0-1)

    Returns:
        (lower, upper) 信頼区間
    """
    if n_obs < 2 or sigma_resid <= 0:
        return (person_fe, person_fe)

    se = sigma_resid / math.sqrt(n_obs)

    # Use t-distribution for small samples, z for large
    if n_obs >= 30:
        # z-approximation
        z = 1.96 if ci_level == 0.95 else 2.576 if ci_level == 0.99 else 1.96
        crit = z
    else:
        from scipy.stats import t

        alpha = 1.0 - ci_level
        crit = float(t.ppf(1.0 - alpha / 2.0, df=n_obs - 1))

    lower = person_fe - crit * se
    upper = person_fe + crit * se
    return (round(lower, 4), round(upper, 4))


def batch_compute_confidence(
    results: list[dict],
    credits_per_person: dict[str, int] | None = None,
    sources_per_person: dict[str, int] | None = None,
    akm_residuals: dict[tuple[str, str], float] | None = None,
) -> list[dict]:
    """Attach confidence levels and confidence intervals to a result list.

    Args:
        results: scores.json の形式
        credits_per_person: {person_id: credit_count}
        sources_per_person: {person_id: source_count}
        akm_residuals: (person_id, anime_id) → AKM残差（解析的CI用）

    Returns:
        results with "confidence" and "score_range" added
    """
    # Compute global residual sigma and per-person observation counts (B09 fix)
    global_sigma = 0.0
    person_n_obs: dict[str, int] = {}
    if akm_residuals:
        import numpy as np
        from collections import Counter

        all_resid_values = list(akm_residuals.values())
        if len(all_resid_values) >= 2:
            global_sigma = float(np.std(all_resid_values, ddof=1))

        pid_counter: Counter[str] = Counter()
        for pid, _aid in akm_residuals:
            pid_counter[pid] += 1
        person_n_obs = dict(pid_counter)

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

        # Use percentile-transformed scores (0-100) for score_range
        # so scale=100.0 is correct (B08 fix)
        score_range: dict[str, tuple[float, float]] = {
            "iv_score": compute_score_range(r.get("iv_score_pct", 50.0), conf),
            "birank": compute_score_range(r.get("birank_pct", 50.0), conf),
            "patronage": compute_score_range(r.get("patronage_pct", 50.0), conf),
        }

        # Analytical CI for person_fe if AKM residuals available (B09 fix)
        n_obs = person_n_obs.get(pid, 0)
        if n_obs >= 2 and global_sigma > 0:
            score_range["person_fe"] = compute_person_fe_ci(
                r.get("person_fe", 0.0), n_obs, global_sigma
            )
        else:
            score_range["person_fe"] = compute_score_range(
                r.get("person_fe_pct", 50.0), conf
            )

        r["score_range"] = score_range

    logger.info("confidence_computed", persons=len(results))
    return results
