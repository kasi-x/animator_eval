"""Systematic Bias Detector — 構造的バイアスの検出.

業界全体で特定のグループ（役職、スタジオ、キャリアステージ）が
系統的に過小/過大評価されているかを統計的に検出。

理論:
- t検定による統計的有意性の確認
- Cohen's d による効果量の測定
- 貢献度（Shapley値）vs. 現在スコアのギャップ分析
"""

import statistics
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import structlog


logger = structlog.get_logger()


@dataclass
class BiasDetectionResult:
    """バイアス検出結果.

    Attributes:
        group_name: グループ名（例: "key_animator", "newcomer"）
        bias_type: バイアスの種類（"role" | "studio" | "career_stage"）
        sample_size: サンプル数
        avg_contribution: 平均貢献度（Shapley値）
        avg_current_score: 平均現在スコア
        bias_gap: ギャップ（contribution - score）
        statistical_significance: p値（t検定）
        effect_size: 効果量（Cohen's d）
        is_significant: 統計的に有意か（p < 0.05 and |d| > 0.3）
        bias_direction: "undervalued" | "overvalued" | "fair"
        affected_persons: 影響を受けている人数
    """

    group_name: str
    bias_type: str
    sample_size: int
    avg_contribution: float
    avg_current_score: float
    bias_gap: float
    statistical_significance: float
    effect_size: float
    is_significant: bool
    bias_direction: str
    affected_persons: int


def _compute_ttest_and_effect(gaps: list[float]) -> tuple[float, float]:
    """t検定とCohen's dを計算.

    Args:
        gaps: contribution - score のギャップリスト

    Returns:
        (p_value, cohens_d)
    """
    if len(gaps) < 2:
        return 1.0, 0.0

    try:
        # scipy.statsがあれば使う
        import scipy.stats

        t_stat, p_value = scipy.stats.ttest_1samp(gaps, 0)
    except ImportError:
        # scipyがない場合は簡易計算
        mean_gap = statistics.mean(gaps)
        std_gap = statistics.stdev(gaps) if len(gaps) > 1 else 1.0
        n = len(gaps)
        t_stat = mean_gap / (std_gap / (n**0.5))

        # 自由度 n-1 のt分布で近似的にp値計算（簡易版）
        # 正確な計算にはscipyが必要
        p_value = 0.05 if abs(t_stat) > 2.0 else 0.5

    # Cohen's d（効果量）
    std_gap = statistics.stdev(gaps) if len(gaps) > 1 else 1.0
    mean_gap = statistics.mean(gaps)
    cohens_d = mean_gap / std_gap if std_gap > 0 else 0.0

    return p_value, cohens_d


def detect_role_bias(
    contributions: dict[str, dict[str, dict]],  # anime_id → {person_id → contrib_dict}
    person_scores: dict[str, dict],
    role_profiles: dict[str, dict],
) -> list[BiasDetectionResult]:
    """役職別のバイアスを検出.

    Args:
        contributions: anime_id → {person_id → contribution_dict}
        person_scores: person_id → scores
        role_profiles: person_id → role_info

    Returns:
        役職別バイアス検出結果リスト
    """
    # 役職ごとに (contribution, score, person_id) を集計
    role_data: dict[str, list[tuple[float, float, str]]] = defaultdict(list)

    for anime_id, anime_contribs in contributions.items():
        for person_id, contrib_dict in anime_contribs.items():
            # contribution_dict から必要な情報を取得
            shapley = contrib_dict.get("shapley_value", 0)
            role_str = contrib_dict.get("role", "other")

            # 現在スコアを取得
            score = person_scores.get(person_id, {}).get("composite", 0)

            role_data[role_str].append((shapley, score, person_id))

    results = []
    for role_str, data_points in role_data.items():
        if len(data_points) < 10:  # 最低10サンプル必要
            continue

        contributions_list = [p[0] for p in data_points]
        scores_list = [p[1] for p in data_points]
        person_ids = set(p[2] for p in data_points)

        # ギャップ計算
        gaps = [c - s for c, s in zip(contributions_list, scores_list)]
        avg_gap = statistics.mean(gaps)

        # 統計検定
        p_value, cohens_d = _compute_ttest_and_effect(gaps)

        # 有意性判定（p < 0.05 かつ 中程度以上の効果量）
        is_significant = p_value < 0.05 and abs(cohens_d) > 0.3

        if is_significant:
            direction = "undervalued" if avg_gap > 0 else "overvalued"

            results.append(
                BiasDetectionResult(
                    group_name=role_str,
                    bias_type="role",
                    sample_size=len(data_points),
                    avg_contribution=round(statistics.mean(contributions_list), 2),
                    avg_current_score=round(statistics.mean(scores_list), 2),
                    bias_gap=round(avg_gap, 2),
                    statistical_significance=round(p_value, 4),
                    effect_size=round(cohens_d, 3),
                    is_significant=True,
                    bias_direction=direction,
                    affected_persons=len(person_ids),
                )
            )

    logger.info("role_bias_detected", biases=len(results), total_roles=len(role_data))
    return results


def detect_studio_bias(
    studio_bias_metrics: dict[str, Any],
    person_scores: dict[str, dict],
    min_persons_per_studio: int = 5,
) -> list[BiasDetectionResult]:
    """スタジオ別バイアスを検出.

    Args:
        studio_bias_metrics: スタジオバイアスメトリクス
        person_scores: person_id → scores
        min_persons_per_studio: スタジオあたり最低人数

    Returns:
        スタジオ別バイアス検出結果リスト
    """
    debiased_scores = studio_bias_metrics.get("debiased_scores", {})
    bias_metrics = studio_bias_metrics.get("bias_metrics", {})

    # スタジオごとにデータ集計
    studio_data: dict[str, list[tuple[float, float, str]]] = defaultdict(list)

    for person_id, debiased_dict in debiased_scores.items():
        if person_id not in bias_metrics:
            continue

        # スタジオ情報取得
        bias_info = bias_metrics[person_id]
        primary_studio = bias_info.get("primary_studio", "unknown")

        # 補正前と補正後のAuthority
        original = debiased_dict.get("original_authority", 0)
        debiased = debiased_dict.get("debiased_authority", 0)

        studio_data[primary_studio].append((debiased, original, person_id))

    results = []
    for studio, data_points in studio_data.items():
        if len(data_points) < min_persons_per_studio:
            continue

        debiased_list = [p[0] for p in data_points]
        original_list = [p[1] for p in data_points]
        person_ids = set(p[2] for p in data_points)

        # ギャップ: debiased - original
        # 正 = 補正で上昇（元々過小評価）
        # 負 = 補正で下降（元々過大評価）
        gaps = [d - o for d, o in zip(debiased_list, original_list)]
        avg_gap = statistics.mean(gaps)

        p_value, cohens_d = _compute_ttest_and_effect(gaps)
        is_significant = p_value < 0.05 and abs(cohens_d) > 0.3

        if is_significant:
            direction = "undervalued" if avg_gap > 0 else "overvalued"

            results.append(
                BiasDetectionResult(
                    group_name=studio,
                    bias_type="studio",
                    sample_size=len(data_points),
                    avg_contribution=round(statistics.mean(debiased_list), 2),
                    avg_current_score=round(statistics.mean(original_list), 2),
                    bias_gap=round(avg_gap, 2),
                    statistical_significance=round(p_value, 4),
                    effect_size=round(cohens_d, 3),
                    is_significant=True,
                    bias_direction=direction,
                    affected_persons=len(person_ids),
                )
            )

    logger.info("studio_bias_detected", biases=len(results), total_studios=len(studio_data))
    return results


def detect_career_stage_bias(
    growth_acceleration_data: dict[str, Any],
    potential_value_scores: dict[str, dict],
    person_scores: dict[str, dict],
) -> list[BiasDetectionResult]:
    """キャリアステージ別バイアスを検出.

    Args:
        growth_acceleration_data: 成長率データ
        potential_value_scores: 潜在価値スコア
        person_scores: person_id → scores

    Returns:
        キャリアステージ別バイアス検出結果リスト
    """
    growth_metrics = growth_acceleration_data.get("growth_metrics", {})

    # キャリアステージごとにデータ集計
    stage_groups: dict[str, list[tuple[float, float, str]]] = {
        "newcomer": [],  # <= 3年
        "mid_career": [],  # 4-10年
        "veteran": [],  # 11年以上
    }

    for person_id, growth_dict in growth_metrics.items():
        years = growth_dict.get("career_years", 0)

        # 潜在価値を取得
        potential = potential_value_scores.get(person_id, {}).get("potential_value", 0)

        # 現在スコアを取得
        current = person_scores.get(person_id, {}).get("composite", 0)

        # キャリアステージ分類
        if years <= 3:
            stage_groups["newcomer"].append((potential, current, person_id))
        elif years <= 10:
            stage_groups["mid_career"].append((potential, current, person_id))
        else:
            stage_groups["veteran"].append((potential, current, person_id))

    results = []
    for stage_name, data_points in stage_groups.items():
        if len(data_points) < 10:
            continue

        potential_list = [p[0] for p in data_points]
        current_list = [p[1] for p in data_points]
        person_ids = set(p[2] for p in data_points)

        # ギャップ: potential - current
        # 正 = 潜在価値が現在評価を上回る（過小評価）
        gaps = [pot - cur for pot, cur in zip(potential_list, current_list)]
        avg_gap = statistics.mean(gaps)

        p_value, cohens_d = _compute_ttest_and_effect(gaps)
        is_significant = p_value < 0.05 and abs(cohens_d) > 0.3

        if is_significant:
            direction = "undervalued" if avg_gap > 0 else "overvalued"

            results.append(
                BiasDetectionResult(
                    group_name=stage_name,
                    bias_type="career_stage",
                    sample_size=len(data_points),
                    avg_contribution=round(statistics.mean(potential_list), 2),
                    avg_current_score=round(statistics.mean(current_list), 2),
                    bias_gap=round(avg_gap, 2),
                    statistical_significance=round(p_value, 4),
                    effect_size=round(cohens_d, 3),
                    is_significant=True,
                    bias_direction=direction,
                    affected_persons=len(person_ids),
                )
            )

    logger.info("career_stage_bias_detected", biases=len(results), stages=len(stage_groups))
    return results


def detect_systematic_biases(
    contributions: dict[str, dict[str, dict]],
    person_scores: dict[str, dict],
    studio_bias_metrics: dict[str, Any],
    growth_acceleration_data: dict[str, Any],
    potential_value_scores: dict[str, dict],
    role_profiles: dict[str, dict],
) -> dict[str, list[BiasDetectionResult]]:
    """全ての構造的バイアスを検出.

    Args:
        contributions: anime_id → {person_id → contrib_dict}
        person_scores: person_id → scores
        studio_bias_metrics: スタジオバイアスメトリクス
        growth_acceleration_data: 成長率データ
        potential_value_scores: 潜在価値スコア
        role_profiles: person_id → role_info

    Returns:
        bias_type → [BiasDetectionResult, ...]
    """
    logger.info("detecting_systematic_biases")

    results = {
        "role": detect_role_bias(contributions, person_scores, role_profiles),
        "studio": detect_studio_bias(studio_bias_metrics, person_scores),
        "career_stage": detect_career_stage_bias(
            growth_acceleration_data, potential_value_scores, person_scores
        ),
    }

    total_biases = sum(len(biases) for biases in results.values())
    logger.info("systematic_biases_detected", total=total_biases)

    return results


def generate_bias_report(
    bias_results: dict[str, list[BiasDetectionResult]],
) -> dict:
    """バイアス検出レポートを生成.

    Args:
        bias_results: bias_type → [BiasDetectionResult, ...]

    Returns:
        包括的なレポート辞書
    """
    # 全バイアスをフラット化
    all_biases = []
    for bias_type, biases in bias_results.items():
        all_biases.extend(biases)

    # 深刻度で分類
    severe_biases = [b for b in all_biases if abs(b.effect_size) > 0.8]
    moderate_biases = [b for b in all_biases if 0.5 < abs(b.effect_size) <= 0.8]
    mild_biases = [b for b in all_biases if 0.3 < abs(b.effect_size) <= 0.5]

    # 推奨事項を生成
    recommendations = []
    for bias in sorted(all_biases, key=lambda b: abs(b.effect_size), reverse=True)[:5]:
        if bias.bias_direction == "undervalued":
            recommendations.append(
                f"{bias.group_name} ({bias.bias_type}) is systematically undervalued "
                f"by {abs(bias.bias_gap):.1f} points on average (p={bias.statistical_significance:.4f})"
            )
        else:
            recommendations.append(
                f"{bias.group_name} ({bias.bias_type}) is systematically overvalued "
                f"by {abs(bias.bias_gap):.1f} points on average (p={bias.statistical_significance:.4f})"
            )

    report = {
        "summary": {
            "total_biases_detected": len(all_biases),
            "severe_biases": len(severe_biases),
            "moderate_biases": len(moderate_biases),
            "mild_biases": len(mild_biases),
        },
        "role_biases": [
            {
                "group": b.group_name,
                "bias_gap": b.bias_gap,
                "effect_size": b.effect_size,
                "p_value": b.statistical_significance,
                "direction": b.bias_direction,
                "sample_size": b.sample_size,
                "affected_persons": b.affected_persons,
            }
            for b in bias_results.get("role", [])
        ],
        "studio_biases": [
            {
                "studio": b.group_name,
                "bias_gap": b.bias_gap,
                "effect_size": b.effect_size,
                "p_value": b.statistical_significance,
                "direction": b.bias_direction,
                "sample_size": b.sample_size,
                "affected_persons": b.affected_persons,
            }
            for b in bias_results.get("studio", [])
        ],
        "career_stage_biases": [
            {
                "stage": b.group_name,
                "bias_gap": b.bias_gap,
                "effect_size": b.effect_size,
                "p_value": b.statistical_significance,
                "direction": b.bias_direction,
                "sample_size": b.sample_size,
                "affected_persons": b.affected_persons,
            }
            for b in bias_results.get("career_stage", [])
        ],
        "recommendations": recommendations,
    }

    logger.info("bias_report_generated", total_biases=len(all_biases))
    return report


def main():
    """スタンドアロン実行用エントリーポイント."""
    # テスト用のダミーデータでデモ
    print("Systematic Bias Detector - Standalone Demo")
    print("(Requires actual pipeline data to run)")


if __name__ == "__main__":
    main()
