"""Insights Report Generator — 実践的洞察レポートの生成.

PageRankと各種補正分析から実務的なインプリケーションを抽出:
- PageRank分析: 上位者の特徴、ネットワーク構造
- スタジオバイアス補正: 順位変動、クロススタジオ活動の価値
- 成長率分析: Rising Stars、停滞者の発見
- 潜在価値分析: 過小評価人材、構造的優位性
- ブリッジ分析: ネットワークの橋渡し役
- 提言: 評価システム改善、人材発掘戦略
"""

import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from typing import Any

import structlog

logger = structlog.get_logger()


@dataclass
class PageRankInsights:
    """PageRank分析の洞察.

    Attributes:
        top_percentile_share: 上位10%が占めるスコアシェア
        concentration_ratio: Herfindahl集中度指数
        avg_score: 平均スコア
        median_score: 中央値スコア
        top_characteristics: 上位者の共通特徴
        network_structure: ネットワーク構造の特性
    """

    top_percentile_share: float
    concentration_ratio: float
    avg_score: float
    median_score: float
    top_characteristics: dict[str, Any]
    network_structure: dict[str, Any]


@dataclass
class BiasInsights:
    """バイアス補正の洞察.

    Attributes:
        total_persons_affected: 影響を受けた人数
        avg_correction: 平均補正値
        top_gainers: 補正で最も上昇した人（top 10）
        top_losers: 補正で最も下降した人（top 10）
        studio_effects: スタジオ別の効果
        cross_studio_value: クロススタジオ活動の価値
    """

    total_persons_affected: int
    avg_correction: float
    top_gainers: list[dict]
    top_losers: list[dict]
    studio_effects: dict[str, dict]
    cross_studio_value: float


@dataclass
class GrowthInsights:
    """成長率分析の洞察.

    Attributes:
        rising_stars_count: 急成長中の人材数
        stagnant_count: 停滞している人材数
        avg_velocity: 平均成長速度
        top_risers: トップ急成長者（top 10）
        early_career_impact: 早期キャリアボーナスの効果
    """

    rising_stars_count: int
    stagnant_count: int
    avg_velocity: float
    top_risers: list[dict]
    early_career_impact: float


@dataclass
class PotentialValueInsights:
    """潜在価値分析の洞察.

    Attributes:
        category_distribution: カテゴリ別分布
        hidden_gems_count: Hidden Gem（過小評価）の人数
        undervalued_talent: 最も過小評価されている人材（top 10）
        structural_advantage_impact: 構造的優位性の効果
        elite_vs_hidden: EliteとHidden Gemの特徴比較
    """

    category_distribution: dict[str, int]
    hidden_gems_count: int
    undervalued_talent: list[dict]
    structural_advantage_impact: float
    elite_vs_hidden: dict[str, dict]


@dataclass
class BridgeInsights:
    """ブリッジ分析の洞察.

    Attributes:
        bridge_persons_count: ブリッジ人材の数
        avg_betweenness: 平均媒介中心性
        top_bridges: トップブリッジ人材（top 10）
        circle_connections: サークル間接続の重要性
        information_brokerage: 情報仲介の価値
    """

    bridge_persons_count: int
    avg_betweenness: float
    top_bridges: list[dict]
    circle_connections: int
    information_brokerage: float


@dataclass
class ComprehensiveInsights:
    """包括的洞察レポート.

    Attributes:
        pagerank: PageRank分析
        bias: バイアス補正分析
        growth: 成長率分析
        potential: 潜在価値分析
        bridges: ブリッジ分析
        recommendations: 実務的提言
        key_findings: 主要な発見
    """

    pagerank: PageRankInsights
    bias: BiasInsights
    growth: GrowthInsights
    potential: PotentialValueInsights
    bridges: BridgeInsights
    recommendations: list[str]
    key_findings: list[str]


def analyze_pagerank_distribution(
    person_scores: dict[str, dict],
    centrality: dict[str, dict],
    role_profiles: dict[str, dict],
) -> PageRankInsights:
    """PageRank分布を分析.

    Args:
        person_scores: person_id → scores
        centrality: person_id → centrality_metrics
        role_profiles: person_id → role_info

    Returns:
        PageRank分析の洞察
    """
    if not person_scores:
        return PageRankInsights(
            top_percentile_share=0.0,
            concentration_ratio=0.0,
            avg_score=0.0,
            median_score=0.0,
            top_characteristics={},
            network_structure={},
        )

    # スコアリストを取得
    scores = [s.get("authority", 0) for s in person_scores.values()]
    sorted_scores = sorted(scores, reverse=True)

    # 上位10%のシェア
    top_10_count = max(1, len(scores) // 10)
    top_10_sum = sum(sorted_scores[:top_10_count])
    total_sum = sum(scores)
    top_percentile_share = (top_10_sum / total_sum * 100) if total_sum > 0 else 0

    # Herfindahl集中度指数
    if total_sum > 0:
        shares = [s / total_sum for s in scores]
        herfindahl = sum(share**2 for share in shares)
    else:
        herfindahl = 0

    # 統計量
    avg_score = statistics.mean(scores) if scores else 0
    median_score = statistics.median(scores) if scores else 0

    # 上位者の特徴分析
    top_10_persons = sorted(
        person_scores.items(), key=lambda x: x[1].get("authority", 0), reverse=True
    )[:10]

    # 役職分布
    top_roles = defaultdict(int)
    for pid, _ in top_10_persons:
        role_info = role_profiles.get(pid, {})
        primary_role = role_info.get("primary_role", "unknown")
        top_roles[primary_role] += 1

    # ネットワーク特性（中心性）
    top_betweenness = []
    for pid, _ in top_10_persons:
        cent = centrality.get(pid, {})
        betweenness = cent.get("betweenness", 0)
        top_betweenness.append(betweenness)

    avg_top_betweenness = statistics.mean(top_betweenness) if top_betweenness else 0

    top_characteristics = {
        "role_distribution": dict(top_roles),
        "avg_betweenness": round(avg_top_betweenness, 4),
        "score_range": {
            "highest": round(sorted_scores[0], 2) if sorted_scores else 0,
            "10th": round(sorted_scores[9], 2) if len(sorted_scores) > 9 else 0,
        },
    }

    # ネットワーク構造
    all_betweenness = [c.get("betweenness", 0) for c in centrality.values()]
    network_structure = {
        "avg_betweenness": round(statistics.mean(all_betweenness), 4)
        if all_betweenness
        else 0,
        "max_betweenness": round(max(all_betweenness), 4) if all_betweenness else 0,
    }

    logger.info(
        "pagerank_insights_analyzed",
        top_10_share=round(top_percentile_share, 1),
        concentration=round(herfindahl, 3),
    )

    return PageRankInsights(
        top_percentile_share=round(top_percentile_share, 2),
        concentration_ratio=round(herfindahl, 3),
        avg_score=round(avg_score, 2),
        median_score=round(median_score, 2),
        top_characteristics=top_characteristics,
        network_structure=network_structure,
    )


def analyze_bias_correction_impact(
    studio_bias_metrics: dict[str, Any],
    person_names: dict[str, str],
) -> BiasInsights:
    """スタジオバイアス補正の影響を分析.

    Args:
        studio_bias_metrics: スタジオバイアスメトリクス
        person_names: person_id → 名前

    Returns:
        バイアス補正の洞察
    """
    debiased_scores = studio_bias_metrics.get("debiased_scores", {})
    bias_metrics = studio_bias_metrics.get("bias_metrics", {})

    if not debiased_scores:
        return BiasInsights(
            total_persons_affected=0,
            avg_correction=0.0,
            top_gainers=[],
            top_losers=[],
            studio_effects={},
            cross_studio_value=0.0,
        )

    # 補正値を計算
    corrections = []
    for pid, debiased_dict in debiased_scores.items():
        original = debiased_dict.get("original_authority", 0)
        debiased = debiased_dict.get("debiased_authority", 0)
        correction = debiased - original
        corrections.append(
            {
                "person_id": pid,
                "name": person_names.get(pid, pid),
                "original": round(original, 2),
                "debiased": round(debiased, 2),
                "correction": round(correction, 2),
            }
        )

    # 上位/下位ランキング
    top_gainers = sorted(corrections, key=lambda x: x["correction"], reverse=True)[:10]
    top_losers = sorted(corrections, key=lambda x: x["correction"])[:10]

    # 平均補正値
    avg_correction = statistics.mean([c["correction"] for c in corrections])

    # スタジオ別効果
    studio_effects = {}
    studio_corrections = defaultdict(list)

    for pid, bias_info in bias_metrics.items():
        studio = bias_info.get("primary_studio", "unknown")
        if pid in debiased_scores:
            correction = (
                debiased_scores[pid].get("debiased_authority", 0)
                - debiased_scores[pid].get("original_authority", 0)
            )
            studio_corrections[studio].append(correction)

    for studio, corrs in studio_corrections.items():
        if len(corrs) >= 3:  # 最低3人
            studio_effects[studio] = {
                "persons": len(corrs),
                "avg_correction": round(statistics.mean(corrs), 2),
                "direction": "overvalued" if statistics.mean(corrs) < 0 else "undervalued",
            }

    # クロススタジオ活動の価値
    cross_studio_values = []
    for pid, bias_info in bias_metrics.items():
        cross_studio_works = bias_info.get("cross_studio_works", 0)
        if cross_studio_works > 0 and pid in debiased_scores:
            correction = (
                debiased_scores[pid].get("debiased_authority", 0)
                - debiased_scores[pid].get("original_authority", 0)
            )
            cross_studio_values.append(correction)

    cross_studio_avg = (
        statistics.mean(cross_studio_values) if cross_studio_values else 0
    )

    logger.info(
        "bias_insights_analyzed",
        persons=len(corrections),
        avg_correction=round(avg_correction, 2),
    )

    return BiasInsights(
        total_persons_affected=len(corrections),
        avg_correction=round(avg_correction, 2),
        top_gainers=top_gainers,
        top_losers=top_losers,
        studio_effects=studio_effects,
        cross_studio_value=round(cross_studio_avg, 2),
    )


def analyze_growth_patterns(
    growth_acceleration_data: dict[str, Any],
    person_names: dict[str, str],
) -> GrowthInsights:
    """成長パターンを分析.

    Args:
        growth_acceleration_data: 成長率データ
        person_names: person_id → 名前

    Returns:
        成長率分析の洞察
    """
    growth_metrics = growth_acceleration_data.get("growth_metrics", {})

    if not growth_metrics:
        return GrowthInsights(
            rising_stars_count=0,
            stagnant_count=0,
            avg_velocity=0.0,
            top_risers=[],
            early_career_impact=0.0,
        )

    # 成長トレンド集計
    rising_count = sum(1 for g in growth_metrics.values() if g.get("trend") == "rising")
    stagnant_count = sum(
        1 for g in growth_metrics.values() if g.get("trend") in ["stable", "declining"]
    )

    # 平均成長速度
    velocities = [g.get("growth_velocity", 0) for g in growth_metrics.values()]
    avg_velocity = statistics.mean(velocities) if velocities else 0

    # トップ急成長者
    top_risers_data = [
        {
            "person_id": pid,
            "name": person_names.get(pid, pid),
            "velocity": round(g.get("growth_velocity", 0), 2),
            "momentum": round(g.get("momentum_score", 0), 2),
            "career_years": g.get("career_years", 0),
        }
        for pid, g in growth_metrics.items()
    ]
    top_risers = sorted(top_risers_data, key=lambda x: x["momentum"], reverse=True)[:10]

    # 早期キャリアボーナスの効果
    early_bonuses = [
        g.get("early_career_bonus", 0)
        for g in growth_metrics.values()
        if g.get("career_years", 999) <= 5
    ]
    early_career_impact = statistics.mean(early_bonuses) if early_bonuses else 0

    logger.info(
        "growth_insights_analyzed",
        rising_stars=rising_count,
        avg_velocity=round(avg_velocity, 2),
    )

    return GrowthInsights(
        rising_stars_count=rising_count,
        stagnant_count=stagnant_count,
        avg_velocity=round(avg_velocity, 2),
        top_risers=top_risers,
        early_career_impact=round(early_career_impact, 3),
    )


def analyze_potential_value_categories(
    potential_value_scores: dict[str, dict],
    person_names: dict[str, str],
) -> PotentialValueInsights:
    """潜在価値カテゴリを分析.

    Args:
        potential_value_scores: 潜在価値スコア
        person_names: person_id → 名前

    Returns:
        潜在価値分析の洞察
    """
    if not potential_value_scores:
        return PotentialValueInsights(
            category_distribution={},
            hidden_gems_count=0,
            undervalued_talent=[],
            structural_advantage_impact=0.0,
            elite_vs_hidden={},
        )

    # カテゴリ分布
    category_dist = defaultdict(int)
    for p in potential_value_scores.values():
        category = p.get("category", "unknown")
        category_dist[category] += 1

    hidden_gems_count = category_dist.get("hidden_gem", 0)

    # Hidden Gemを抽出
    hidden_gems = [
        {
            "person_id": pid,
            "name": person_names.get(pid, pid),
            "potential_value": round(p.get("potential_value", 0), 2),
            "hidden_score": round(p.get("hidden_score", 0), 2),
            "current_composite": round(p.get("composite", 0), 2),
        }
        for pid, p in potential_value_scores.items()
        if p.get("category") == "hidden_gem"
    ]
    undervalued = sorted(hidden_gems, key=lambda x: x["hidden_score"], reverse=True)[:10]

    # 構造的優位性の効果
    structural_scores = [
        p.get("structural_score", 0) for p in potential_value_scores.values()
    ]
    structural_impact = statistics.mean(structural_scores) if structural_scores else 0

    # EliteとHidden Gemの比較
    elite_group = [
        p for p in potential_value_scores.values() if p.get("category") == "elite"
    ]
    hidden_group = [
        p for p in potential_value_scores.values() if p.get("category") == "hidden_gem"
    ]

    elite_vs_hidden = {
        "elite": {
            "count": len(elite_group),
            "avg_authority": round(
                statistics.mean([p.get("authority", 0) for p in elite_group]), 2
            )
            if elite_group
            else 0,
            "avg_trust": round(
                statistics.mean([p.get("trust", 0) for p in elite_group]), 2
            )
            if elite_group
            else 0,
        },
        "hidden_gem": {
            "count": len(hidden_group),
            "avg_authority": round(
                statistics.mean([p.get("authority", 0) for p in hidden_group]), 2
            )
            if hidden_group
            else 0,
            "avg_debiased_authority": round(
                statistics.mean([p.get("debiased_authority", 0) for p in hidden_group]), 2
            )
            if hidden_group
            else 0,
        },
    }

    logger.info(
        "potential_insights_analyzed",
        hidden_gems=hidden_gems_count,
        categories=len(category_dist),
    )

    return PotentialValueInsights(
        category_distribution=dict(category_dist),
        hidden_gems_count=hidden_gems_count,
        undervalued_talent=undervalued,
        structural_advantage_impact=round(structural_impact, 2),
        elite_vs_hidden=elite_vs_hidden,
    )


def analyze_bridge_importance(
    bridges_data: dict[str, Any],
    person_names: dict[str, str],
    centrality: dict[str, dict],
) -> BridgeInsights:
    """ブリッジの重要性を分析.

    Args:
        bridges_data: ブリッジデータ
        person_names: person_id → 名前
        centrality: person_id → centrality_metrics

    Returns:
        ブリッジ分析の洞察
    """
    bridge_persons = bridges_data.get("bridge_persons", [])

    if not bridge_persons:
        return BridgeInsights(
            bridge_persons_count=0,
            avg_betweenness=0.0,
            top_bridges=[],
            circle_connections=0,
            information_brokerage=0.0,
        )

    # 媒介中心性の平均
    betweenness_scores = []
    for pid in bridge_persons:
        cent = centrality.get(pid, {})
        betweenness = cent.get("betweenness", 0)
        betweenness_scores.append(betweenness)

    avg_betweenness = statistics.mean(betweenness_scores) if betweenness_scores else 0

    # トップブリッジ人材
    bridge_rankings = [
        {
            "person_id": pid,
            "name": person_names.get(pid, pid),
            "betweenness": round(centrality.get(pid, {}).get("betweenness", 0), 4),
            "degree": centrality.get(pid, {}).get("degree", 0),
        }
        for pid in bridge_persons
    ]
    top_bridges = sorted(
        bridge_rankings, key=lambda x: x["betweenness"], reverse=True
    )[:10]

    # サークル間接続数（エッジ数の近似）
    circle_connections = bridges_data.get("total_bridge_edges", 0)

    # 情報仲介の価値（betweenness × degree の平均）
    brokerage_values = [
        centrality.get(pid, {}).get("betweenness", 0)
        * centrality.get(pid, {}).get("degree", 0)
        for pid in bridge_persons
    ]
    info_brokerage = statistics.mean(brokerage_values) if brokerage_values else 0

    logger.info(
        "bridge_insights_analyzed",
        bridges=len(bridge_persons),
        avg_betweenness=round(avg_betweenness, 4),
    )

    return BridgeInsights(
        bridge_persons_count=len(bridge_persons),
        avg_betweenness=round(avg_betweenness, 4),
        top_bridges=top_bridges,
        circle_connections=circle_connections,
        information_brokerage=round(info_brokerage, 2),
    )


def generate_recommendations(
    pagerank: PageRankInsights,
    bias: BiasInsights,
    growth: GrowthInsights,
    potential: PotentialValueInsights,
    bridges: BridgeInsights,
) -> list[str]:
    """実務的提言を生成.

    Args:
        pagerank: PageRank分析
        bias: バイアス補正分析
        growth: 成長率分析
        potential: 潜在価値分析
        bridges: ブリッジ分析

    Returns:
        提言リスト
    """
    recommendations = []

    # PageRank集中度に基づく提言
    if pagerank.concentration_ratio > 0.1:
        recommendations.append(
            f"スコア分布が集中している（Herfindahl={pagerank.concentration_ratio:.3f}）。"
            "上位10%が全体の{:.1f}%を占める。多様な人材評価の仕組みが必要。".format(
                pagerank.top_percentile_share
            )
        )

    # スタジオバイアスに基づく提言
    if abs(bias.avg_correction) > 1.0:
        recommendations.append(
            f"スタジオバイアス補正で平均{abs(bias.avg_correction):.1f}点の変動。"
            f"{'大手スタジオ出身者が過大評価されている' if bias.avg_correction < 0 else '中小スタジオ人材が過小評価されている'}。"
        )

    if bias.cross_studio_value > 0:
        recommendations.append(
            f"クロススタジオ活動者は平均+{bias.cross_studio_value:.1f}点の価値。"
            "複数スタジオでの経験を評価に反映すべき。"
        )

    # 成長率に基づく提言
    if growth.rising_stars_count > 0:
        recommendations.append(
            f"{growth.rising_stars_count}名の急成長人材を発見。"
            f"平均成長速度{growth.avg_velocity:.1f}クレジット/年。"
            "早期発掘と育成支援が重要。"
        )

    if growth.early_career_impact > 0.1:
        recommendations.append(
            f"早期キャリアボーナスの効果は+{growth.early_career_impact:.1%}。"
            "新人の潜在能力評価を強化すべき。"
        )

    # 潜在価値に基づく提言
    if potential.hidden_gems_count > 0:
        recommendations.append(
            f"{potential.hidden_gems_count}名のHidden Gem（過小評価人材）を発見。"
            "スタジオバイアス補正により真の実力が明らかに。"
        )

    if potential.structural_advantage_impact > 10:
        recommendations.append(
            f"構造的優位性（ネットワーク位置）の効果は{potential.structural_advantage_impact:.1f}点。"
            "ブリッジ人材の戦略的配置が重要。"
        )

    # ブリッジに基づく提言
    if bridges.bridge_persons_count > 0:
        recommendations.append(
            f"{bridges.bridge_persons_count}名のブリッジ人材が{bridges.circle_connections}のサークル間接続を担う。"
            f"平均媒介中心性{bridges.avg_betweenness:.4f}。"
            "情報仲介者の育成・保持が業界全体の効率化につながる。"
        )

    return recommendations


def generate_key_findings(
    pagerank: PageRankInsights,
    bias: BiasInsights,
    growth: GrowthInsights,
    potential: PotentialValueInsights,
    bridges: BridgeInsights,
) -> list[str]:
    """主要な発見を生成.

    Args:
        pagerank: PageRank分析
        bias: バイアス補正分析
        growth: 成長率分析
        potential: 潜在価値分析
        bridges: ブリッジ分析

    Returns:
        主要な発見リスト
    """
    findings = []

    # PageRank
    top_role = max(
        pagerank.top_characteristics.get("role_distribution", {}).items(),
        key=lambda x: x[1],
        default=("unknown", 0),
    )[0]
    findings.append(
        f"上位者の共通点: {top_role}が多数、平均媒介中心性{pagerank.top_characteristics.get('avg_betweenness', 0):.4f}"
    )

    # バイアス
    if bias.top_gainers:
        top_gainer = bias.top_gainers[0]
        findings.append(
            f"最大の補正: {top_gainer['name']}が+{top_gainer['correction']:.1f}点上昇（スタジオバイアス補正）"
        )

    # 成長
    if growth.top_risers:
        top_riser = growth.top_risers[0]
        findings.append(
            f"最高成長: {top_riser['name']}がモメンタム{top_riser['momentum']:.1f}（キャリア{top_riser['career_years']}年）"
        )

    # 潜在価値
    category_names = {
        "elite": "エリート",
        "rising_star": "ライジングスター",
        "hidden_gem": "Hidden Gem",
        "structural_player": "構造的プレイヤー",
        "steady_performer": "安定型",
        "newcomer": "新人",
    }
    top_category = max(
        potential.category_distribution.items(), key=lambda x: x[1], default=("unknown", 0)
    )
    findings.append(
        f"潜在価値分布: {category_names.get(top_category[0], top_category[0])}が最多（{top_category[1]}名）"
    )

    # ブリッジ
    if bridges.top_bridges:
        top_bridge = bridges.top_bridges[0]
        findings.append(
            f"最重要ブリッジ: {top_bridge['name']}（媒介中心性{top_bridge['betweenness']:.4f}、次数{top_bridge['degree']}）"
        )

    return findings


def generate_comprehensive_insights(
    person_scores: dict[str, dict],
    studio_bias_metrics: dict[str, Any],
    growth_acceleration_data: dict[str, Any],
    potential_value_scores: dict[str, dict],
    centrality: dict[str, dict],
    role_profiles: dict[str, dict],
    bridges_data: dict[str, Any],
    person_names: dict[str, str],
) -> ComprehensiveInsights:
    """包括的洞察レポートを生成.

    Args:
        person_scores: person_id → scores
        studio_bias_metrics: スタジオバイアスメトリクス
        growth_acceleration_data: 成長率データ
        potential_value_scores: 潜在価値スコア
        centrality: person_id → centrality_metrics
        role_profiles: person_id → role_info
        bridges_data: ブリッジデータ
        person_names: person_id → 名前

    Returns:
        包括的洞察レポート
    """
    logger.info("generating_comprehensive_insights")

    # 各分析を実行
    pagerank = analyze_pagerank_distribution(person_scores, centrality, role_profiles)
    bias = analyze_bias_correction_impact(studio_bias_metrics, person_names)
    growth = analyze_growth_patterns(growth_acceleration_data, person_names)
    potential = analyze_potential_value_categories(potential_value_scores, person_names)
    bridges = analyze_bridge_importance(bridges_data, person_names, centrality)

    # 提言と発見を生成
    recommendations = generate_recommendations(pagerank, bias, growth, potential, bridges)
    key_findings = generate_key_findings(pagerank, bias, growth, potential, bridges)

    logger.info(
        "comprehensive_insights_generated",
        recommendations=len(recommendations),
        findings=len(key_findings),
    )

    return ComprehensiveInsights(
        pagerank=pagerank,
        bias=bias,
        growth=growth,
        potential=potential,
        bridges=bridges,
        recommendations=recommendations,
        key_findings=key_findings,
    )


def export_insights_report(
    insights: ComprehensiveInsights,
) -> dict:
    """洞察レポートをJSON形式でエクスポート.

    Args:
        insights: 包括的洞察

    Returns:
        JSON出力用辞書
    """
    return {
        "pagerank_analysis": asdict(insights.pagerank),
        "bias_correction_analysis": asdict(insights.bias),
        "growth_analysis": asdict(insights.growth),
        "potential_value_analysis": asdict(insights.potential),
        "bridge_analysis": asdict(insights.bridges),
        "recommendations": insights.recommendations,
        "key_findings": insights.key_findings,
    }


def main():
    """スタンドアロン実行用エントリーポイント."""
    print("Insights Report Generator - Standalone Demo")
    print("(Requires actual pipeline data to run)")


if __name__ == "__main__":
    main()
