"""Fair Compensation Analyzer — 公正報酬分析.

Shapley値に基づいた理論的に公正な報酬配分を提案。
作品タイプ（TV1クール、2クール、劇場版など）による調整を考慮。

理論:
- Shapley値による公正な価値配分
- 作品タイプによるスタッフ数・役職重要度の調整
- Gini係数による不平等度測定
- 最低保証額と報酬比制約の適用
"""

import statistics
from dataclasses import dataclass, field
from enum import Enum

import structlog

from src.models import AnimeAnalysis as Anime, Role

logger = structlog.get_logger()


class AnimeType(Enum):
    """Work type."""

    MOVIE = "movie"  # 劇場版
    TV_1COUR = "tv_1cour"  # TV1クール（12-13話）
    TV_2COUR = "tv_2cour"  # TV2クール（24-26話）
    TV_LONG = "tv_long"  # 長編TVシリーズ（27話以上）
    OVA = "ova"  # OVA/ONA
    UNKNOWN = "unknown"


def classify_anime_type(anime: Anime) -> AnimeType:
    """Classify the work type.

    Args:
        anime: Animeオブジェクト

    Returns:
        作品タイプ
    """
    episodes = anime.episodes or 1

    if episodes == 1:
        # seasonがあればOVA、なければ劇場版
        if anime.season:
            return AnimeType.OVA
        return AnimeType.MOVIE
    elif episodes <= 13:
        return AnimeType.TV_1COUR
    elif episodes <= 26:
        return AnimeType.TV_2COUR
    else:
        return AnimeType.TV_LONG


# role importance adjustment factor by work type
# TV series: more episodes raise the importance of animation directors and key animators
# theatrical: directors and character designers have higher importance
ANIME_TYPE_ROLE_ADJUSTMENTS = {
    AnimeType.MOVIE: {
        Role.DIRECTOR: 1.3,  # 監督の重要度 +30%
        Role.CHARACTER_DESIGNER: 1.2,  # キャラデザ +20%
        Role.BACKGROUND_ART: 1.2,  # 美術監督 +20%
        Role.KEY_ANIMATOR: 0.9,  # 原画 -10%（話数少ない）
        Role.ANIMATION_DIRECTOR: 0.95,
    },
    AnimeType.TV_1COUR: {
        Role.DIRECTOR: 1.0,
        Role.EPISODE_DIRECTOR: 1.1,  # 演出 +10%
        Role.KEY_ANIMATOR: 1.0,
    },
    AnimeType.TV_2COUR: {
        Role.DIRECTOR: 0.95,  # 監督 -5%（話数多いので分散）
        Role.ANIMATION_DIRECTOR: 1.1,  # 作監 +10%
        Role.KEY_ANIMATOR: 1.05,  # 原画 +5%
        Role.EPISODE_DIRECTOR: 1.15,  # 演出 +15%
    },
    AnimeType.TV_LONG: {
        Role.DIRECTOR: 0.9,  # 監督 -10%
        Role.ANIMATION_DIRECTOR: 1.15,  # 作監 +15%
        Role.KEY_ANIMATOR: 1.1,  # 原画 +10%
        Role.EPISODE_DIRECTOR: 1.2,  # 演出 +20%
    },
    AnimeType.OVA: {
        Role.DIRECTOR: 1.1,
        Role.KEY_ANIMATOR: 0.95,
    },
}


@dataclass
class CompensationAnalysisRequest:
    """Compensation analysis request.

    Attributes:
        anime_id: 対象作品ID
        total_budget: 総報酬予算（任意の単位）
        min_compensation: 役職別最低保証額
        max_ratio: 最高/最低 報酬比の上限（デフォルト10倍）
        fairness_mode: "shapley" | "equal" | "role_based"
        apply_anime_type_adjustment: 作品タイプ調整を適用するか
    """

    anime_id: str
    total_budget: float
    min_compensation: dict[Role, float] = field(default_factory=dict)
    max_ratio: float = 10.0
    fairness_mode: str = "shapley"
    apply_anime_type_adjustment: bool = True


@dataclass
class FairnessMetrics:
    """Fairness metrics.

    Attributes:
        gini_coefficient: Gini係数（0=完全平等, 1=完全不平等）
        shapley_correlation: 配分とShapley値の相関
        min_compensation: 最低報酬額
        max_compensation: 最高報酬額
        compensation_ratio: 最高/最低比
    """

    gini_coefficient: float
    shapley_correlation: float
    min_compensation: float
    max_compensation: float
    compensation_ratio: float


@dataclass
class CompensationAnalysis:
    """Compensation analysis result.

    Attributes:
        anime_id: 作品ID
        anime_title: 作品タイトル
        anime_type: 作品タイプ
        total_budget: 総予算
        allocations: person_id → 報酬額
        contributions: person_id → contribution_dict
        fairness_metrics: 公正度メトリクス
    """

    anime_id: str
    anime_title: str
    anime_type: str
    total_budget: float
    allocations: dict[str, float]
    contributions: dict[str, dict]
    fairness_metrics: FairnessMetrics


def compute_gini_coefficient(values: list[float]) -> float:
    """Compute the Gini coefficient.

    Args:
        values: 数値のリスト

    Returns:
        Gini係数（0-1）
    """
    if not values or len(values) < 2:
        return 0.0

    sorted_values = sorted(values)
    n = len(sorted_values)
    cumsum = 0

    for i, val in enumerate(sorted_values):
        cumsum += (i + 1) * val  # 1-indexed weight: smallest value gets weight 1

    total = sum(sorted_values)
    if total == 0:
        return 0.0

    gini = (2 * cumsum) / (n * total) - (n + 1) / n
    return round(gini, 3)


def compute_fairness_metrics(
    allocations: dict[str, float],
    contributions: dict[str, dict],
) -> FairnessMetrics:
    """Measure the fairness of compensation allocation.

    Args:
        allocations: person_id → 報酬額
        contributions: person_id → contribution_dict

    Returns:
        公正度メトリクス
    """
    if not allocations:
        return FairnessMetrics(
            gini_coefficient=0.0,
            shapley_correlation=0.0,
            min_compensation=0.0,
            max_compensation=0.0,
            compensation_ratio=0.0,
        )

    # Gini coefficient
    gini = compute_gini_coefficient(list(allocations.values()))

    # Shapley相関
    shapley_values = [contributions[pid].get("shapley_value", 0) for pid in allocations]
    alloc_values = list(allocations.values())

    if len(alloc_values) > 1 and statistics.stdev(shapley_values) > 0:
        try:
            import scipy.stats

            shapley_corr, _ = scipy.stats.pearsonr(shapley_values, alloc_values)
        except ImportError:
            # scipyなしの簡易相関計算
            mean_shapley = statistics.mean(shapley_values)
            mean_alloc = statistics.mean(alloc_values)
            std_shapley = statistics.stdev(shapley_values)
            std_alloc = statistics.stdev(alloc_values)

            covariance = sum(
                (s - mean_shapley) * (a - mean_alloc)
                for s, a in zip(shapley_values, alloc_values)
            ) / len(shapley_values)

            shapley_corr = (
                covariance / (std_shapley * std_alloc)
                if std_shapley * std_alloc > 0
                else 0
            )
    else:
        shapley_corr = 1.0

    # compensation range
    min_comp = min(allocations.values())
    max_comp = max(allocations.values())
    ratio = max_comp / min_comp if min_comp > 0 else 0

    return FairnessMetrics(
        gini_coefficient=gini,
        shapley_correlation=round(shapley_corr, 3),
        min_compensation=round(min_comp, 2),
        max_compensation=round(max_comp, 2),
        compensation_ratio=round(ratio, 2),
    )


def analyze_fair_compensation(
    request: CompensationAnalysisRequest,
    contributions: dict[str, dict],  # person_id → contrib_dict
    anime: Anime,
) -> CompensationAnalysis:
    """Run fair compensation analysis.

    Args:
        request: 分析リクエスト
        contributions: person_id → contribution_dict
        anime: Animeオブジェクト

    Returns:
        報酬分析結果
    """
    if not contributions:
        logger.warning("no_contributions", anime_id=request.anime_id)
        return CompensationAnalysis(
            anime_id=request.anime_id,
            anime_title=anime.title_ja or anime.title_en or anime.id,
            anime_type=classify_anime_type(anime).value,
            total_budget=request.total_budget,
            allocations={},
            contributions={},
            fairness_metrics=FairnessMetrics(
                gini_coefficient=0.0,
                shapley_correlation=0.0,
                min_compensation=0.0,
                max_compensation=0.0,
                compensation_ratio=0.0,
            ),
        )

    # determine work type
    anime_type = classify_anime_type(anime)

    # Shapley値を集計（作品タイプ調整を適用）
    adjusted_shapley = {}
    for person_id, contrib_dict in contributions.items():
        base_shapley = contrib_dict.get("shapley_value", 0)

        # adjustment by work type
        if request.apply_anime_type_adjustment:
            role_str = contrib_dict.get("role", "other")
            try:
                role = Role(role_str)
            except ValueError:
                role = Role.SPECIAL

            adjustment = ANIME_TYPE_ROLE_ADJUSTMENTS.get(anime_type, {}).get(role, 1.0)
            adjusted_shapley[person_id] = base_shapley * adjustment
        else:
            adjusted_shapley[person_id] = base_shapley

    total_shapley = sum(adjusted_shapley.values())

    # Shapley値ベースの基本配分
    base_allocations = {}
    for person_id, shapley in adjusted_shapley.items():
        share = shapley / total_shapley if total_shapley > 0 else 0
        base_allocations[person_id] = request.total_budget * share

    # apply minimum guaranteed amount
    adjusted_allocations = {}
    total_guaranteed = 0

    for person_id, contrib_dict in contributions.items():
        role_str = contrib_dict.get("role", "other")
        try:
            role = Role(role_str)
        except ValueError:
            role = Role.SPECIAL

        min_comp = request.min_compensation.get(role, 0)
        adjusted_allocations[person_id] = max(
            base_allocations.get(person_id, 0), min_comp
        )
        total_guaranteed += adjusted_allocations[person_id]

    # if budget exceeded, adjust by proportional allocation
    if total_guaranteed > request.total_budget:
        scale = request.total_budget / total_guaranteed
        adjusted_allocations = {
            pid: comp * scale for pid, comp in adjusted_allocations.items()
        }

    # apply max/min ratio constraint (set floor = max / max_ratio)
    # NOTE: cap = min * max_ratio は逆順を引き起こすため不使用。
    # instead, set floor = max / max_ratio as minimum guarantee,
    # then scale the budget excess by Shapley ratio.
    if len(adjusted_allocations) > 1:
        min_comp = min(adjusted_allocations.values())
        max_comp = max(adjusted_allocations.values())

        if min_comp > 0 and max_comp / min_comp > request.max_ratio:
            floor = max_comp / request.max_ratio

            # raise any allocation below floor to floor
            raised_allocations = {
                pid: max(comp, floor) for pid, comp in adjusted_allocations.items()
            }
            # rescale because total budget has changed
            total_raised = sum(raised_allocations.values())
            if total_raised > 0:
                scale = request.total_budget / total_raised
                adjusted_allocations = {
                    pid: comp * scale for pid, comp in raised_allocations.items()
                }

    # compute fairness metrics
    fairness = compute_fairness_metrics(adjusted_allocations, contributions)

    logger.info(
        "compensation_analyzed",
        anime_id=request.anime_id,
        anime_type=anime_type.value,
        persons=len(adjusted_allocations),
        gini=fairness.gini_coefficient,
        shapley_corr=fairness.shapley_correlation,
    )

    return CompensationAnalysis(
        anime_id=request.anime_id,
        anime_title=anime.title_ja or anime.title_en or anime.id,
        anime_type=anime_type.value,
        total_budget=request.total_budget,
        allocations=adjusted_allocations,
        contributions=contributions,
        fairness_metrics=fairness,
    )


def batch_analyze_compensation(
    anime_list: list[Anime],
    all_contributions: dict[str, dict[str, dict]],  # anime_id → {person_id → contrib}
    total_budget_per_anime: float = 100.0,
    min_compensation: dict[Role, float] | None = None,
) -> dict[str, CompensationAnalysis]:
    """Run compensation analysis for multiple works in batch.

    Args:
        anime_list: Animeリスト
        all_contributions: anime_id → {person_id → contrib_dict}
        total_budget_per_anime: 作品あたりの総予算
        min_compensation: 役職別最低保証額

    Returns:
        anime_id → CompensationAnalysis
    """
    if min_compensation is None:
        min_compensation = {}

    results = {}
    anime_map = {a.id: a for a in anime_list}

    for anime_id, contributions in all_contributions.items():
        if anime_id not in anime_map:
            continue

        anime = anime_map[anime_id]

        request = CompensationAnalysisRequest(
            anime_id=anime_id,
            total_budget=total_budget_per_anime,
            min_compensation=min_compensation,
            max_ratio=10.0,
            apply_anime_type_adjustment=True,
        )

        analysis = analyze_fair_compensation(request, contributions, anime)
        results[anime_id] = analysis

    logger.info("batch_compensation_analyzed", anime=len(results))
    return results


def export_compensation_report(
    analyses: dict[str, CompensationAnalysis],
    person_names: dict[str, str],
    anime_scores: dict[str, float] | None = None,
) -> dict:
    """Export a compensation analysis report.

    Args:
        analyses: anime_id → CompensationAnalysis
        person_names: person_id → 名前
        anime_scores: anime_id → score (optional, for scatter chart)

    Returns:
        JSONエクスポート可能な辞書
    """
    if anime_scores is None:
        anime_scores = {}

    anime_reports = []

    for anime_id, analysis in analyses.items():
        # person_id → allocation の配列に変換
        allocations_list = [
            {
                "person_id": pid,
                "name": person_names.get(pid, pid),
                "role": analysis.contributions[pid].get("role", "other"),
                "allocation": alloc,
                "shapley_value": analysis.contributions[pid].get("shapley_value", 0),
                "value_share": analysis.contributions[pid].get("value_share", 0),
            }
            for pid, alloc in sorted(
                analysis.allocations.items(), key=lambda x: x[1], reverse=True
            )
        ]

        entry: dict = {
            "anime_id": anime_id,
            "anime_title": analysis.anime_title,
            "anime_type": analysis.anime_type,
            "total_budget": analysis.total_budget,
            "staff_count": len(analysis.allocations),
            "fairness": {
                "gini_coefficient": analysis.fairness_metrics.gini_coefficient,
                "shapley_correlation": analysis.fairness_metrics.shapley_correlation,
                "min_compensation": analysis.fairness_metrics.min_compensation,
                "max_compensation": analysis.fairness_metrics.max_compensation,
                "compensation_ratio": analysis.fairness_metrics.compensation_ratio,
            },
            "allocations": allocations_list,
        }
        score = anime_scores.get(anime_id)
        if score is not None:
            entry["anime_score"] = score
        anime_reports.append(entry)

    # summary statistics
    all_gini = [a.fairness_metrics.gini_coefficient for a in analyses.values()]
    all_corr = [a.fairness_metrics.shapley_correlation for a in analyses.values()]

    report = {
        "total_anime": len(analyses),
        "summary": {
            "avg_gini_coefficient": round(statistics.mean(all_gini), 3)
            if all_gini
            else 0,
            "avg_shapley_correlation": round(statistics.mean(all_corr), 3)
            if all_corr
            else 0,
        },
        "anime_type_distribution": {
            atype: sum(1 for a in analyses.values() if a.anime_type == atype)
            for atype in set(a.anime_type for a in analyses.values())
        },
        "analyses": anime_reports,
    }

    logger.info("compensation_report_exported", anime=len(analyses))
    return report


def main():
    """Standalone entry point."""
    print("Fair Compensation Analyzer - Standalone Demo")
    print("(Requires actual pipeline data to run)")


if __name__ == "__main__":
    main()
