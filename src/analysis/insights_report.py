"""Insights Report Generator — 個人評価に基づく洞察レポートの生成.

個人の貢献を可視化し、適正な報酬と業界の健全化に資する洞察を抽出:
- PageRank分析: 上位者の特徴、ネットワーク構造
- スタジオバイアス補正: 過小評価されている個人の発見
- 成長率分析: Rising Stars、成長中の人材
- 潜在価値分析: 過小評価人材、構造的優位性
- ブリッジ分析: ネットワークの橋渡し役
- 提言: 個人の適正評価、過小評価の是正
"""

import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
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
class UndervaluationAlert:
    """個人の過小評価アラート.

    Attributes:
        person_id: 対象者ID
        name: 名前
        current_iv_score: 現在のIV Score
        debiased_birank: バイアス補正後BiRank
        birank_gap: 補正前後の差分
        category: 潜在価値カテゴリ
        reason: 過小評価の推定原因
    """

    person_id: str
    name: str
    current_iv_score: float
    debiased_birank: float
    birank_gap: float
    category: str
    reason: str


@dataclass
class ComprehensiveInsights:
    """包括的洞察レポート.

    Attributes:
        pagerank: PageRank分析
        bias: バイアス補正分析
        growth: 成長率分析
        potential: 潜在価値分析
        bridges: ブリッジ分析
        recommendations: 個人の適正評価に向けた提言
        key_findings: 主要な発見
        undervaluation_alerts: 過小評価アラート（個人レベル）
    """

    pagerank: PageRankInsights
    bias: BiasInsights
    growth: GrowthInsights
    potential: PotentialValueInsights
    bridges: BridgeInsights
    recommendations: list[str]
    key_findings: list[str]
    undervaluation_alerts: list[UndervaluationAlert] | None = None


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
    scores = [s.get("birank", 0) for s in person_scores.values()]
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
        person_scores.items(), key=lambda x: x[1].get("birank", 0), reverse=True
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
        original = debiased_dict.get("original_birank", 0)
        debiased = debiased_dict.get("debiased_birank", 0)
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
            correction = debiased_scores[pid].get(
                "debiased_birank", 0
            ) - debiased_scores[pid].get("original_birank", 0)
            studio_corrections[studio].append(correction)

    for studio, corrs in studio_corrections.items():
        if len(corrs) >= 3:  # 最低3人
            studio_effects[studio] = {
                "persons": len(corrs),
                "avg_correction": round(statistics.mean(corrs), 2),
                "direction": "overvalued"
                if statistics.mean(corrs) < 0
                else "undervalued",
            }

    # クロススタジオ活動の価値
    cross_studio_values = []
    for pid, bias_info in bias_metrics.items():
        cross_studio_works = bias_info.get("cross_studio_works", 0)
        if cross_studio_works > 0 and pid in debiased_scores:
            correction = debiased_scores[pid].get(
                "debiased_birank", 0
            ) - debiased_scores[pid].get("original_birank", 0)
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
            "current_iv_score": round(p.get("iv_score", 0), 2),
        }
        for pid, p in potential_value_scores.items()
        if p.get("category") == "hidden_gem"
    ]
    undervalued = sorted(hidden_gems, key=lambda x: x["hidden_score"], reverse=True)[
        :10
    ]

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
            "avg_birank": round(
                statistics.mean([p.get("birank", 0) for p in elite_group]), 2
            )
            if elite_group
            else 0,
            "avg_patronage": round(
                statistics.mean([p.get("patronage", 0) for p in elite_group]), 2
            )
            if elite_group
            else 0,
        },
        "hidden_gem": {
            "count": len(hidden_group),
            "avg_birank": round(
                statistics.mean([p.get("birank", 0) for p in hidden_group]), 2
            )
            if hidden_group
            else 0,
            "avg_debiased_birank": round(
                statistics.mean([p.get("debiased_birank", 0) for p in hidden_group]),
                2,
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
    bridge_persons_raw = bridges_data.get("bridge_persons", [])

    # bridge_persons may be a list of dicts (from detect_bridges) or list of strings
    # Normalize to list of person_id strings
    bridge_pids: list[str] = []
    for bp in bridge_persons_raw:
        if isinstance(bp, dict):
            bridge_pids.append(bp.get("person_id", ""))
        else:
            bridge_pids.append(str(bp))
    bridge_pids = [p for p in bridge_pids if p]

    if not bridge_pids:
        return BridgeInsights(
            bridge_persons_count=0,
            avg_betweenness=0.0,
            top_bridges=[],
            circle_connections=0,
            information_brokerage=0.0,
        )

    # 媒介中心性の平均
    betweenness_scores = []
    for pid in bridge_pids:
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
        for pid in bridge_pids
    ]
    top_bridges = sorted(bridge_rankings, key=lambda x: x["betweenness"], reverse=True)[
        :10
    ]

    # サークル間接続数（エッジ数の近似）
    circle_connections = bridges_data.get("total_bridge_edges", 0)

    # 情報仲介の価値（betweenness × degree の平均）
    brokerage_values = [
        centrality.get(pid, {}).get("betweenness", 0)
        * centrality.get(pid, {}).get("degree", 0)
        for pid in bridge_pids
    ]
    info_brokerage = statistics.mean(brokerage_values) if brokerage_values else 0

    logger.info(
        "bridge_insights_analyzed",
        bridges=len(bridge_pids),
        avg_betweenness=round(avg_betweenness, 4),
    )

    return BridgeInsights(
        bridge_persons_count=len(bridge_pids),
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
    """個人の適正評価に向けた提言を生成.

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

    # スコア集中 → 評価が少数に偏っている
    if pagerank.concentration_ratio > 0.1:
        recommendations.append(
            f"評価が上位に集中している（上位10%が全体の{pagerank.top_percentile_share:.1f}%を占有）。"
            "中堅〜若手の貢献が可視化されていない可能性がある。"
        )

    # スタジオバイアス → 過小評価されている個人がいる
    if abs(bias.avg_correction) > 1.0:
        if bias.avg_correction > 0:
            recommendations.append(
                f"スタジオバイアス補正で平均+{bias.avg_correction:.1f}点の上方修正。"
                "中小スタジオ所属の個人が過小評価されている。適正な報酬のため補正後スコアを参照すべき。"
            )
        else:
            recommendations.append(
                f"スタジオバイアス補正で平均{bias.avg_correction:.1f}点の修正。"
                "大手スタジオのブランド効果が個人スコアに影響している。"
            )

    if bias.cross_studio_value > 0:
        recommendations.append(
            f"複数スタジオで活動する個人は平均+{bias.cross_studio_value:.1f}点高い。"
            "スタジオ横断的な経験は個人の市場価値を高める。"
        )

    # 成長中の個人 → 報酬が追いついていない可能性
    if growth.rising_stars_count > 0:
        recommendations.append(
            f"{growth.rising_stars_count}名が急成長中（平均成長速度{growth.avg_velocity:.1f}/年）。"
            "成長中の個人は現在の報酬が実力に追いついていない可能性がある。"
        )

    if growth.early_career_impact > 0.1:
        recommendations.append(
            "キャリア初期の人材は過小評価されやすい。"
            "クレジット数が少ないだけで貢献度が低いわけではない点に留意。"
        )

    # Hidden Gems → 最も過小評価されている個人
    if potential.hidden_gems_count > 0:
        recommendations.append(
            f"⚠️ {potential.hidden_gems_count}名が過小評価（Hidden Gem）と判定。"
            "スタジオバイアス補正後のスコアが大幅に上昇する個人は、"
            "現在の評価・報酬が不当に低い可能性がある。"
        )

    if potential.structural_advantage_impact > 10:
        recommendations.append(
            f"ネットワーク上の構造的貢献（ブリッジ役）は{potential.structural_advantage_impact:.1f}点相当の価値。"
            "この貢献は従来のスコアでは見えにくく、報酬に反映されにくい。"
        )

    # ブリッジ人材 → 業界に不可欠だが評価されにくい
    if bridges.bridge_persons_count > 0:
        recommendations.append(
            f"{bridges.bridge_persons_count}名がスタジオ間の橋渡し役を担い、"
            f"{bridges.circle_connections}のサークル間接続に貢献。"
            "ブリッジ人材は業界の協業効率に不可欠だが、個人としての評価・報酬に反映されにくい。"
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
        potential.category_distribution.items(),
        key=lambda x: x[1],
        default=("unknown", 0),
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


def identify_undervaluation_alerts(
    studio_bias_metrics: dict[str, Any],
    potential_value_scores: dict[str, dict],
    person_names: dict[str, str],
    top_n: int = 20,
) -> list[UndervaluationAlert]:
    """過小評価されている個人を特定する.

    スタジオバイアス補正と潜在価値分析を組み合わせて、
    現在の評価が不当に低い可能性のある個人を抽出する。

    Args:
        studio_bias_metrics: スタジオバイアスメトリクス
        potential_value_scores: 潜在価値スコア
        person_names: person_id → 名前
        top_n: 最大アラート件数

    Returns:
        過小評価アラートのリスト（gap の大きい順）
    """
    debiased_scores = studio_bias_metrics.get("debiased_scores", {})
    bias_metrics = studio_bias_metrics.get("bias_metrics", {})
    alerts = []

    for pid, debiased_dict in debiased_scores.items():
        original = debiased_dict.get("original_birank", 0)
        debiased = debiased_dict.get("debiased_birank", 0)
        gap = debiased - original

        if gap <= 0.03:
            continue

        # 潜在価値カテゴリ
        pv = potential_value_scores.get(pid, {})
        category = pv.get("category", "unknown")

        # 過小評価の推定原因
        bias_info = bias_metrics.get(pid, {})
        primary_studio = bias_info.get("primary_studio", "")
        cross_studio_works = bias_info.get("cross_studio_works", 0)

        if cross_studio_works == 0 and primary_studio:
            reason = f"単一スタジオ（{primary_studio}）所属による可視性の限定"
        elif gap > 0.1:
            reason = "スタジオ規模バイアスによる大幅な過小評価"
        else:
            reason = "スタジオバイアスによる軽度の過小評価"

        iv_score = pv.get("iv_score", 0)

        alerts.append(
            UndervaluationAlert(
                person_id=pid,
                name=person_names.get(pid, pid),
                current_iv_score=round(iv_score, 2),
                debiased_birank=round(debiased, 4),
                birank_gap=round(gap, 4),
                category=category
                if isinstance(category, str)
                else category.value
                if hasattr(category, "value")
                else str(category),
                reason=reason,
            )
        )

    alerts.sort(key=lambda a: a.birank_gap, reverse=True)

    logger.info("undervaluation_alerts_generated", alerts=len(alerts[:top_n]))
    return alerts[:top_n]


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
    recommendations = generate_recommendations(
        pagerank, bias, growth, potential, bridges
    )
    key_findings = generate_key_findings(pagerank, bias, growth, potential, bridges)

    # 過小評価アラート
    undervaluation_alerts = identify_undervaluation_alerts(
        studio_bias_metrics, potential_value_scores, person_names
    )

    logger.info(
        "comprehensive_insights_generated",
        recommendations=len(recommendations),
        findings=len(key_findings),
        undervaluation_alerts=len(undervaluation_alerts),
    )

    return ComprehensiveInsights(
        pagerank=pagerank,
        bias=bias,
        growth=growth,
        potential=potential,
        bridges=bridges,
        recommendations=recommendations,
        key_findings=key_findings,
        undervaluation_alerts=undervaluation_alerts,
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
    result = {
        "pagerank_analysis": asdict(insights.pagerank),
        "bias_correction_analysis": asdict(insights.bias),
        "growth_analysis": asdict(insights.growth),
        "potential_value_analysis": asdict(insights.potential),
        "bridge_analysis": asdict(insights.bridges),
        "recommendations": insights.recommendations,
        "key_findings": insights.key_findings,
    }
    if insights.undervaluation_alerts:
        result["undervaluation_alerts"] = [
            asdict(a) for a in insights.undervaluation_alerts
        ]
    return result


def main():
    """Standalone entry point."""
    print("Insights Report Generator - Standalone Demo")
    print("(Requires actual pipeline data to run)")


if __name__ == "__main__":
    main()
