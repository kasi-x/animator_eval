"""Potential Value Score — 潜在価値スコアの統合.

既存のAuthority/Trust/Skillに、新しい補正・拡張を統合:
1. Studio-debiased Authority（スタジオバイアス補正）
2. Growth-adjusted Skill（成長率考慮）
3. Structural Advantage（構造的優位性）

最終的な「潜在価値」を多角的に評価。
"""

from dataclasses import dataclass
from enum import Enum

import networkx as nx
import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


class ValueCategory(Enum):
    """潜在価値のカテゴリ."""

    ELITE = "elite"  # 既に確立されたエリート（高Authority + 高Trust）
    RISING_STAR = "rising_star"  # 急成長中（高Growth + 中Authority）
    HIDDEN_GEM = "hidden_gem"  # 過小評価（高Skill + 低Authority + Studio debiased上昇）
    STRUCTURAL_PLAYER = "structural_player"  # 構造的優位（高Betweenness + 高Diversity）
    STEADY_PERFORMER = "steady_performer"  # 安定型（中程度の全スコア）
    NEWCOMER = "newcomer"  # 新人（キャリア浅い + ポテンシャル）


@dataclass
class PotentialValueScore:
    """潜在価値スコアの統合.

    Attributes:
        person_id: person_id
        # 既存スコア
        authority: 元のAuthority（PageRank）
        trust: Trust
        skill: Skill
        composite: 元のComposite
        # 補正・拡張スコア
        debiased_authority: スタジオバイアス補正Authority
        adjusted_skill: 成長率考慮Skill
        structural_advantage: 構造的優位性スコア
        # 統合スコア
        potential_value: 総合潜在価値スコア（0-100）
        category: 価値カテゴリ
        # 内訳
        elite_score: エリート要素
        growth_score: 成長要素
        hidden_score: 過小評価要素
        structural_score: 構造的要素
    """

    person_id: str
    # Original scores
    authority: float = 0.0
    trust: float = 0.0
    skill: float = 0.0
    composite: float = 0.0
    # Adjusted scores
    debiased_authority: float = 0.0
    adjusted_skill: float = 0.0
    structural_advantage: float = 0.0
    # Integrated score
    potential_value: float = 0.0
    category: ValueCategory = ValueCategory.STEADY_PERFORMER
    # Breakdown
    elite_score: float = 0.0
    growth_score: float = 0.0
    hidden_score: float = 0.0
    structural_score: float = 0.0


def compute_structural_advantage(
    collaboration_graph: nx.Graph,
    person_id: str,
    betweenness_cache: dict[str, float] | None = None,
) -> float:
    """構造的優位性スコアを計算.

    Betweenness + Structural holes の組み合わせ

    Args:
        collaboration_graph: コラボレーショングラフ
        person_id: 対象のperson_id
        betweenness_cache: 事前計算されたbetweenness centrality辞書

    Returns:
        構造的優位性スコア（0-1）
    """
    if person_id not in collaboration_graph:
        return 0.0

    # Use pre-computed betweenness if available
    betweenness_score = betweenness_cache.get(person_id, 0) if betweenness_cache else 0

    # Degree diversity (connections to different groups)
    neighbors = list(collaboration_graph.neighbors(person_id))
    if not neighbors:
        return 0

    # Simple diversity: number of unique neighbors / total possible
    diversity_score = len(set(neighbors)) / len(neighbors) if neighbors else 0

    # Structural advantage = betweenness + diversity
    advantage = (betweenness_score * 0.6) + (diversity_score * 0.4)

    return round(advantage, 4)


def compute_potential_value_scores(
    person_scores: dict[str, dict],
    debiased_scores: dict[str, dict],
    growth_metrics: dict[str, dict],
    adjusted_skills: dict[str, float],
    collaboration_graph: nx.Graph,
    betweenness_cache: dict[str, float] | None = None,
) -> dict[str, PotentialValueScore]:
    """潜在価値スコアを計算.

    Args:
        person_scores: 元のスコア辞書
        debiased_scores: スタジオバイアス補正後のスコア
        growth_metrics: 成長指標
        adjusted_skills: 成長率考慮Skill
        collaboration_graph: コラボレーショングラフ
        betweenness_cache: 事前計算されたbetweenness centrality辞書
            (Noneの場合はここで計算)

    Returns:
        person_id → PotentialValueScore
    """
    potential_scores: dict[str, PotentialValueScore] = {}

    # Use pre-computed betweenness if available, otherwise compute here
    if betweenness_cache is None:
        betweenness_cache = {}
        n_nodes = collaboration_graph.number_of_nodes()
        n_edges = collaboration_graph.number_of_edges()
        if n_nodes > 0:
            try:
                if n_nodes > 500 or n_edges > 100_000:
                    k = min(100, n_nodes)
                    betweenness_cache = nx.betweenness_centrality(
                        collaboration_graph, k=k, weight="weight"
                    )
                    logger.info("betweenness_approximate", k=k, nodes=n_nodes, edges=n_edges)
                else:
                    betweenness_cache = nx.betweenness_centrality(
                        collaboration_graph, weight="weight"
                    )
            except Exception:
                logger.warning("betweenness_failed", nodes=n_nodes, edges=n_edges)
    else:
        logger.info("betweenness_using_cache", cached_nodes=len(betweenness_cache))

    for person_id, scores in person_scores.items():
        # Original scores
        authority = scores.get("authority", 0)
        trust = scores.get("trust", 0)
        skill = scores.get("skill", 0)
        composite = scores.get("composite", 0)

        # Debiased authority
        debiased = debiased_scores.get(person_id, {})
        debiased_authority = debiased.get("debiased_authority", authority)

        # Adjusted skill
        adjusted_skill = adjusted_skills.get(person_id, skill)

        # Structural advantage
        structural_advantage = compute_structural_advantage(
            collaboration_graph, person_id, betweenness_cache
        )

        # Growth metrics
        growth = growth_metrics.get(person_id, {})
        velocity = growth.get("growth_velocity", 0)
        momentum = growth.get("momentum_score", 0)
        career_years = growth.get("career_years", 0)

        # Component scores (0-100)
        # 1. Elite score: established reputation
        elite_score = (authority * 0.5 + trust * 0.3 + skill * 0.2) * 100

        # 2. Growth score: rising potential
        growth_score = min(100, momentum * 20 + (adjusted_skill - skill) * 10)

        # 3. Hidden score: undervalued talent
        authority_improvement = debiased_authority - authority
        hidden_score = max(0, authority_improvement * 100)

        # 4. Structural score: network position
        structural_score = structural_advantage * 100

        # Determine category
        if career_years <= 3 and momentum > 1.0:
            category = ValueCategory.NEWCOMER
        elif velocity > 2.0 and authority > 0.5:
            category = ValueCategory.RISING_STAR
        elif authority_improvement > 0.1 and skill > 0.6:
            category = ValueCategory.HIDDEN_GEM
        elif structural_advantage > 0.7:
            category = ValueCategory.STRUCTURAL_PLAYER
        elif authority > 0.8 and trust > 0.8:
            category = ValueCategory.ELITE
        else:
            category = ValueCategory.STEADY_PERFORMER

        # Integrated potential value score
        # Weighted combination of all components
        potential_value = (
            elite_score * 0.3
            + growth_score * 0.3
            + hidden_score * 0.2
            + structural_score * 0.2
        )

        potential_scores[person_id] = PotentialValueScore(
            person_id=person_id,
            authority=round(authority, 4),
            trust=round(trust, 4),
            skill=round(skill, 4),
            composite=round(composite, 4),
            debiased_authority=round(debiased_authority, 4),
            adjusted_skill=round(adjusted_skill, 4),
            structural_advantage=round(structural_advantage, 4),
            potential_value=round(potential_value, 2),
            category=category,
            elite_score=round(elite_score, 2),
            growth_score=round(growth_score, 2),
            hidden_score=round(hidden_score, 2),
            structural_score=round(structural_score, 2),
        )

    logger.info(
        "potential_value_scores_computed",
        persons=len(potential_scores),
        categories={
            cat.value: sum(1 for p in potential_scores.values() if p.category == cat)
            for cat in ValueCategory
        },
    )

    return potential_scores


def rank_by_potential_value(
    potential_scores: dict[str, PotentialValueScore],
    category: ValueCategory | None = None,
    top_n: int = 50,
) -> list[tuple[str, float, ValueCategory]]:
    """潜在価値スコアでランキング.

    Args:
        potential_scores: 潜在価値スコア
        category: 特定カテゴリに絞る（Noneで全体）
        top_n: 上位何人を返すか

    Returns:
        [(person_id, potential_value, category), ...] のリスト
    """
    if category:
        # Filter by category
        filtered = [
            (person_id, p.potential_value, p.category)
            for person_id, p in potential_scores.items()
            if p.category == category
        ]
    else:
        # All categories
        filtered = [
            (person_id, p.potential_value, p.category)
            for person_id, p in potential_scores.items()
        ]

    # Sort by potential value
    filtered.sort(key=lambda x: x[1], reverse=True)

    logger.info(
        "ranking_by_potential_value",
        category=category.value if category else "all",
        count=len(filtered[:top_n]),
    )

    return filtered[:top_n]


def export_potential_value_report(
    potential_scores: dict[str, PotentialValueScore],
    person_names: dict[str, str],
) -> dict:
    """潜在価値レポートをエクスポート.

    Args:
        potential_scores: 潜在価値スコア
        person_names: person_id → 名前

    Returns:
        JSONエクスポート可能な辞書
    """
    # Overall ranking
    overall_ranking = rank_by_potential_value(potential_scores, top_n=100)

    # Category-specific rankings
    category_rankings = {}
    for category in ValueCategory:
        category_ranking = rank_by_potential_value(
            potential_scores, category=category, top_n=20
        )
        category_rankings[category.value] = [
            {
                "person_id": pid,
                "name": person_names.get(pid, pid),
                "potential_value": value,
                "category": cat.value,
            }
            for pid, value, cat in category_ranking
        ]

    # Full details
    detailed_scores = []
    for person_id, score in sorted(
        potential_scores.items(), key=lambda x: x[1].potential_value, reverse=True
    )[:100]:
        detailed_scores.append(
            {
                "person_id": person_id,
                "name": person_names.get(person_id, person_id),
                "potential_value": score.potential_value,
                "category": score.category.value,
                "original_scores": {
                    "authority": score.authority,
                    "trust": score.trust,
                    "skill": score.skill,
                    "composite": score.composite,
                },
                "adjusted_scores": {
                    "debiased_authority": score.debiased_authority,
                    "adjusted_skill": score.adjusted_skill,
                    "structural_advantage": score.structural_advantage,
                },
                "breakdown": {
                    "elite_score": score.elite_score,
                    "growth_score": score.growth_score,
                    "hidden_score": score.hidden_score,
                    "structural_score": score.structural_score,
                },
            }
        )

    report = {
        "total_persons": len(potential_scores),
        "category_distribution": {
            cat.value: sum(1 for p in potential_scores.values() if p.category == cat)
            for cat in ValueCategory
        },
        "overall_ranking": [
            {
                "person_id": pid,
                "name": person_names.get(pid, pid),
                "potential_value": value,
                "category": cat.value,
            }
            for pid, value, cat in overall_ranking
        ],
        "category_rankings": category_rankings,
        "detailed_scores": detailed_scores,
    }

    logger.info("potential_value_report_exported", persons=len(detailed_scores))

    return report


def main():
    """スタンドアロン実行用エントリーポイント."""
    from src.analysis.graph import create_person_collaboration_network
    from src.analysis.studio_bias_correction import (
        compute_studio_bias_metrics,
        compute_studio_prestige,
        debias_authority_scores,
    )
    from src.analysis.growth_acceleration import (
        compute_growth_metrics,
        compute_adjusted_skill_with_growth,
    )
    from src.database import (
        get_all_anime,
        get_all_credits,
        get_all_persons,
        get_all_scores,
        get_connection,
        init_db,
    )

    conn = get_connection()
    init_db(conn)

    persons = get_all_persons(conn)
    anime_list = get_all_anime(conn)
    credits = get_all_credits(conn)
    scores_list = get_all_scores(conn)

    # マップ作成
    anime_map = {a.id: a for a in anime_list}
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}
    person_scores = {
        s.person_id: {
            "authority": s.authority,
            "trust": s.trust,
            "skill": s.skill,
            "composite": s.composite,
        }
        for s in scores_list
    }

    # コラボレーショングラフ構築
    logger.info("building_collaboration_graph")
    collab_graph = create_person_collaboration_network(credits, anime_map)

    # スタジオバイアス補正
    logger.info("computing_studio_bias_correction")
    bias_metrics = compute_studio_bias_metrics(credits, anime_map)
    studio_prestige = compute_studio_prestige(credits, anime_map, person_scores)
    debiased = debias_authority_scores(
        person_scores, bias_metrics, studio_prestige, debias_strength=0.3
    )

    debiased_dict = {
        pid: {"debiased_authority": d.debiased_authority} for pid, d in debiased.items()
    }

    # 成長率分析
    logger.info("computing_growth_metrics")
    growth_metrics = compute_growth_metrics(credits, anime_map)
    adjusted_skills = compute_adjusted_skill_with_growth(
        person_scores, growth_metrics, growth_weight=0.3
    )

    growth_dict = {
        pid: {
            "growth_velocity": m.growth_velocity,
            "momentum_score": m.momentum_score,
            "career_years": m.career_years,
        }
        for pid, m in growth_metrics.items()
    }

    # 潜在価値スコア計算
    logger.info("computing_potential_value_scores")
    potential_scores = compute_potential_value_scores(
        person_scores, debiased_dict, growth_dict, adjusted_skills, collab_graph
    )

    # レポート生成
    logger.info("exporting_report")
    report = export_potential_value_report(potential_scores, person_names)

    # 結果表示
    print("\n=== 潜在価値スコア総合ランキング（トップ20）===\n")

    for rank, entry in enumerate(report["overall_ranking"][:20], 1):
        name = entry["name"]
        value = entry["potential_value"]
        category = entry["category"]

        print(f"{rank}. {name} ({category})")
        print(f"   潜在価値: {value:.1f}")

        # Get detailed breakdown
        person_id = entry["person_id"]
        details = potential_scores[person_id]
        print(f"   内訳: Elite={details.elite_score:.1f}, Growth={details.growth_score:.1f}, "
              f"Hidden={details.hidden_score:.1f}, Structural={details.structural_score:.1f}")
        print()

    # カテゴリ別分布
    print("\n=== カテゴリ別分布 ===\n")
    for category, count in report["category_distribution"].items():
        percentage = count / report["total_persons"] * 100
        print(f"{category}: {count}人 ({percentage:.1f}%)")

    # カテゴリ別トップ
    print("\n=== カテゴリ別トップ5 ===\n")
    for category in ValueCategory:
        cat_name = category.value
        if cat_name in report["category_rankings"]:
            entries = report["category_rankings"][cat_name][:5]
            if entries:
                print(f"{cat_name.upper()}:")
                for entry in entries:
                    print(f"  - {entry['name']}: {entry['potential_value']:.1f}")
                print()

    conn.close()


if __name__ == "__main__":
    main()
