"""Data quality score — quantify data completeness and reliability on a 0-100 scale.

データベース内のデータの品質を多面的に評価し、
改善すべきポイントを特定する。
"""

import structlog

logger = structlog.get_logger()


def compute_data_quality_score(
    stats: dict,
    credits_with_source: int = 0,
    total_credits: int = 0,
    persons_with_score: int = 0,
    total_persons: int = 0,
    anime_with_year: int = 0,
    total_anime: int = 0,
    anime_with_score: int = 0,
    source_count: int = 0,
) -> dict:
    """Compute the overall data quality score.

    Args:
        stats: get_db_stats() の出力
        credits_with_source: ソース情報付きクレジット数
        total_credits: 全クレジット数
        persons_with_score: スコア算出済み人物数
        total_persons: 全人物数
        anime_with_year: 年情報付きアニメ数
        total_anime: 全アニメ数
        anime_with_score: 評価スコア付きアニメ数
        source_count: データソース種類数

    Returns:
        {
            "overall_score": float (0-100),
            "dimensions": {dimension: {score, description, issues}},
            "recommendations": [str],
        }
    """
    dimensions: dict[str, dict] = {}
    recommendations: list[str] = []

    # 1. Completeness (30% weight)
    completeness_scores = []

    if total_credits > 0:
        src_ratio = credits_with_source / total_credits
        completeness_scores.append(src_ratio)
        if src_ratio < 0.9:
            recommendations.append(f"クレジットのソース情報が不足 ({src_ratio:.0%})")
    else:
        completeness_scores.append(0)
        recommendations.append("クレジットデータが存在しません")

    if total_anime > 0:
        year_ratio = anime_with_year / total_anime
        completeness_scores.append(year_ratio)
        if year_ratio < 0.8:
            recommendations.append(f"アニメの年情報が不足 ({year_ratio:.0%})")

        score_ratio = anime_with_score / total_anime
        completeness_scores.append(score_ratio)
        if score_ratio < 0.7:
            recommendations.append(f"アニメの評価スコアが不足 ({score_ratio:.0%})")
    else:
        completeness_scores.extend([0, 0])
        recommendations.append("アニメデータが存在しません")

    completeness = sum(completeness_scores) / max(len(completeness_scores), 1) * 100

    dimensions["completeness"] = {
        "score": round(completeness, 1),
        "description": "データの完全性（ソース・年・スコアの充填率）",
    }

    # 2. Coverage (25% weight)
    if total_persons > 0:
        scored_ratio = persons_with_score / total_persons
        coverage = scored_ratio * 100
        if scored_ratio < 0.8:
            recommendations.append(
                f"スコア未算出の人物が多い ({(1 - scored_ratio):.0%})"
            )
    else:
        coverage = 0

    dimensions["coverage"] = {
        "score": round(coverage, 1),
        "description": "スコアリングカバレッジ（評価対象の割合）",
    }

    # 3. Diversity (20% weight)
    diversity = min(source_count / 3, 1.0) * 100  # 3+ sources = 100
    if source_count < 2:
        recommendations.append("データソースを追加してクロスバリデーションを強化")

    dimensions["diversity"] = {
        "score": round(diversity, 1),
        "description": "データソースの多様性",
    }

    # 4. Volume (15% weight)
    volume_thresholds = [
        (10000, 100),
        (5000, 80),
        (1000, 60),
        (100, 40),
        (0, 20),
    ]
    volume = 0
    for threshold, vol_score in volume_thresholds:
        if total_credits >= threshold:
            volume = vol_score
            break

    dimensions["volume"] = {
        "score": volume,
        "description": "データ量（クレジット数）",
    }

    # 5. Freshness (10% weight)
    freshness = 50  # Default middle score
    if stats.get("latest_year"):
        from datetime import datetime

        current_year = datetime.now().year
        years_old = current_year - stats["latest_year"]
        if years_old <= 1:
            freshness = 100
        elif years_old <= 3:
            freshness = 80
        elif years_old <= 5:
            freshness = 60
        else:
            freshness = 30
            recommendations.append("データが古い — 最新のクレジットを追加してください")

    dimensions["freshness"] = {
        "score": freshness,
        "description": "データの鮮度（最新データの年）",
    }

    # Overall score (weighted)
    weights = {
        "completeness": 0.30,
        "coverage": 0.25,
        "diversity": 0.20,
        "volume": 0.15,
        "freshness": 0.10,
    }
    overall = sum(dimensions[dim]["score"] * weight for dim, weight in weights.items())

    result = {
        "overall_score": round(overall, 1),
        "dimensions": dimensions,
        "recommendations": recommendations,
    }

    logger.info("data_quality_computed", overall=round(overall, 1))
    return result
