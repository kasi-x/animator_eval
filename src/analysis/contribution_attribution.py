"""Contribution Attribution — 貢献度の帰属分析.

作品の価値を各クリエイターにどう分配すべきかをShapley値で計算。
「この人がいなかったら作品価値はどう変わったか？」を定量化。

理論:
- Shapley Value: ゲーム理論的な公平な価値分配
- Marginal Contribution: 各人の限界貢献度
- Counterfactual Analysis: 反事実推論
"""

from collections import defaultdict
from dataclasses import dataclass, field
import itertools
import math

import structlog

from src.models import Credit, Role

logger = structlog.get_logger()


@dataclass
class ContributionMetrics:
    """貢献度指標.

    Attributes:
        person_id: person_id
        anime_id: anime_id
        role: 役職
        shapley_value: Shapley値（公平な貢献度）
        marginal_contribution: 限界貢献度
        role_importance: 役職重要度
        irreplaceability: 代替不可能性（この人固有の価値）
        value_share: 作品価値のうちこの人のシェア（%）
    """

    person_id: str
    anime_id: str
    role: Role
    shapley_value: float = 0.0
    marginal_contribution: float = 0.0
    role_importance: float = 0.0
    irreplaceability: float = 0.0
    value_share: float = 0.0


# Role importance weights (配分の基準)
ROLE_CONTRIBUTION_WEIGHTS = {
    Role.DIRECTOR: 0.20,  # 監督: 20%
    Role.EPISODE_DIRECTOR: 0.08,
    Role.CHIEF_ANIMATION_DIRECTOR: 0.15,  # 総作監: 15%
    Role.ANIMATION_DIRECTOR: 0.10,
    Role.CHARACTER_DESIGNER: 0.12,  # キャラデザ: 12%
    Role.KEY_ANIMATOR: 0.06,
    Role.STORYBOARD: 0.08,
    Role.SCREENPLAY: 0.10,
    Role.SERIES_COMPOSITION: 0.12,
    Role.ART_DIRECTOR: 0.05,
    Role.MUSIC: 0.08,
    Role.SECOND_KEY_ANIMATOR: 0.03,
    Role.IN_BETWEEN: 0.01,
    Role.PRODUCER: 0.05,
    Role.OTHER: 0.01,
}


def compute_role_importance(role: Role) -> float:
    """役職の重要度を取得.

    Args:
        role: 役職

    Returns:
        重要度（0-1）
    """
    return ROLE_CONTRIBUTION_WEIGHTS.get(role, 0.01)


def estimate_marginal_contribution(
    person_id: str,
    role: Role,
    anime_value: float,
    person_scores: dict[str, dict],
    staff_quality_avg: float,
) -> float:
    """限界貢献度を推定.

    「この人がいなかったら価値はどう変わるか？」

    Args:
        person_id: person_id
        role: 役職
        anime_value: 作品の総価値
        person_scores: person_id → scores
        staff_quality_avg: スタッフ平均品質

    Returns:
        限界貢献度
    """
    # Person's quality
    person_quality = person_scores.get(person_id, {}).get("composite", staff_quality_avg)

    # Role importance
    role_weight = compute_role_importance(role)

    # Quality premium (above/below average)
    quality_premium = (person_quality - staff_quality_avg) / (staff_quality_avg + 0.1)

    # Marginal contribution = role_weight × anime_value × (1 + quality_premium)
    marginal = role_weight * anime_value * (1 + quality_premium)

    return round(marginal, 3)


def compute_shapley_value_approximate(
    person_id: str,
    role: Role,
    all_staff: list[tuple[str, Role]],
    anime_value: float,
    person_scores: dict[str, dict],
    staff_quality_avg: float,
    sample_size: int = 100,
) -> float:
    """Shapley値を近似計算（サンプリング法）.

    完全計算はO(2^n)で不可能なので、ランダムサンプリングで近似。

    Args:
        person_id: person_id
        role: 役職
        all_staff: 全スタッフ [(person_id, role), ...]
        anime_value: 作品の総価値
        person_scores: person_id → scores
        staff_quality_avg: スタッフ平均品質
        sample_size: サンプリング数

    Returns:
        Shapley値（近似）
    """
    import random

    # For small teams (<10), use exact calculation
    if len(all_staff) <= 10:
        sample_size = 2 ** len(all_staff)

    # Pre-compute marginal contributions for all staff (PERF-4 optimization)
    # Eliminates redundant computation across sampling iterations
    marginal_cache: dict[str, float] = {
        pid: estimate_marginal_contribution(
            pid, r, anime_value, person_scores, staff_quality_avg
        )
        for pid, r in all_staff
    }

    marginal_contributions = []

    for _ in range(min(sample_size, 1000)):  # Cap at 1000 samples
        # Random permutation of staff
        staff_copy = list(all_staff)
        random.shuffle(staff_copy)

        # Find position of target person
        try:
            position = next(i for i, (pid, _) in enumerate(staff_copy) if pid == person_id)
        except StopIteration:
            continue

        # Staff before this person (coalition)
        coalition = staff_copy[:position]

        # O(1) lookup instead of redundant computation (PERF-4 optimization)
        value_with_coalition = sum(marginal_cache[pid] for pid, _ in coalition)
        value_with_person = value_with_coalition + marginal_cache[person_id]

        # Marginal contribution in this permutation
        marginal = value_with_person - value_with_coalition
        marginal_contributions.append(marginal)

    # Shapley value = average marginal contribution
    shapley = sum(marginal_contributions) / len(marginal_contributions) if marginal_contributions else 0

    return round(shapley, 3)


def compute_contribution_attribution(
    anime_id: str,
    anime_value: float,
    credits: list[Credit],
    person_scores: dict[str, dict],
) -> dict[str, ContributionMetrics]:
    """作品への貢献度を各人に帰属.

    Args:
        anime_id: anime_id
        anime_value: 作品の総価値
        credits: この作品のクレジット
        person_scores: person_id → scores

    Returns:
        person_id → ContributionMetrics
    """
    if not credits:
        return {}

    # Staff quality average
    staff_composites = [
        person_scores.get(c.person_id, {}).get("composite", 0.5) for c in credits
    ]
    staff_quality_avg = sum(staff_composites) / len(staff_composites) if staff_composites else 0.5

    # All staff
    all_staff = [(c.person_id, c.role) for c in credits]

    contributions: dict[str, ContributionMetrics] = {}

    for credit in credits:
        person_id = credit.person_id

        # Skip if already computed (same person, multiple roles)
        if person_id in contributions:
            # Accumulate if same person with different role
            existing = contributions[person_id]
            marginal = estimate_marginal_contribution(
                person_id, credit.role, anime_value, person_scores, staff_quality_avg
            )
            existing.marginal_contribution += marginal
            continue

        # Role importance
        role_importance = compute_role_importance(credit.role)

        # Marginal contribution
        marginal = estimate_marginal_contribution(
            person_id, credit.role, anime_value, person_scores, staff_quality_avg
        )

        # Shapley value (approximate)
        shapley = compute_shapley_value_approximate(
            person_id,
            credit.role,
            all_staff,
            anime_value,
            person_scores,
            staff_quality_avg,
            sample_size=50,  # Reduced for performance
        )

        # Irreplaceability: how much better is this person than average
        person_quality = person_scores.get(person_id, {}).get("composite", staff_quality_avg)
        irreplaceability = max(0, person_quality - staff_quality_avg)

        contributions[person_id] = ContributionMetrics(
            person_id=person_id,
            anime_id=anime_id,
            role=credit.role,
            shapley_value=shapley,
            marginal_contribution=marginal,
            role_importance=role_importance,
            irreplaceability=round(irreplaceability, 3),
            value_share=0.0,  # Computed below
        )

    # Compute value shares (normalize to 100%)
    total_shapley = sum(c.shapley_value for c in contributions.values())
    if total_shapley > 0:
        for contrib in contributions.values():
            contrib.value_share = round((contrib.shapley_value / total_shapley) * 100, 2)

    logger.info(
        "contribution_attributed",
        anime_id=anime_id,
        staff=len(contributions),
        total_shapley=round(total_shapley, 2),
    )

    return contributions


def aggregate_contributions_by_person(
    all_contributions: dict[str, dict[str, ContributionMetrics]],
) -> dict[str, dict]:
    """全作品の貢献度を人ごとに集約.

    Args:
        all_contributions: anime_id → {person_id → ContributionMetrics}

    Returns:
        person_id → 集約統計
    """
    person_aggregates: dict[str, dict] = defaultdict(
        lambda: {
            "total_shapley": 0.0,
            "total_marginal": 0.0,
            "avg_value_share": 0.0,
            "avg_irreplaceability": 0.0,
            "work_count": 0,
            "roles": defaultdict(int),
        }
    )

    for anime_id, contributions in all_contributions.items():
        for person_id, contrib in contributions.items():
            agg = person_aggregates[person_id]
            agg["total_shapley"] += contrib.shapley_value
            agg["total_marginal"] += contrib.marginal_contribution
            agg["avg_value_share"] += contrib.value_share
            agg["avg_irreplaceability"] += contrib.irreplaceability
            agg["work_count"] += 1
            agg["roles"][contrib.role.value] += 1

    # Compute averages
    for person_id, agg in person_aggregates.items():
        count = agg["work_count"]
        agg["avg_value_share"] = round(agg["avg_value_share"] / count, 2)
        agg["avg_irreplaceability"] = round(agg["avg_irreplaceability"] / count, 3)
        agg["primary_role"] = max(agg["roles"].items(), key=lambda x: x[1])[0]

    logger.info("contributions_aggregated", persons=len(person_aggregates))

    return dict(person_aggregates)


def find_undervalued_contributors(
    person_aggregates: dict[str, dict],
    person_scores: dict[str, dict],
    top_n: int = 20,
) -> list[tuple[str, float, float]]:
    """過小評価されている貢献者を発見.

    高い貢献度（Shapley値）だが、低いスコア = 過小評価

    Args:
        person_aggregates: 集約貢献度
        person_scores: person_id → scores
        top_n: 上位何人を返すか

    Returns:
        [(person_id, contribution, current_score), ...] のリスト
    """
    candidates = []

    for person_id, agg in person_aggregates.items():
        if person_id not in person_scores:
            continue

        total_contribution = agg["total_shapley"]
        current_score = person_scores[person_id].get("composite", 0)

        # Contribution per work
        contribution_per_work = total_contribution / agg["work_count"]

        # Undervalued if contribution >> score
        if contribution_per_work > current_score * 1.5:
            candidates.append((person_id, contribution_per_work, current_score))

    # Sort by gap (contribution - score)
    candidates.sort(key=lambda x: x[1] - x[2], reverse=True)

    logger.info("undervalued_contributors_found", count=len(candidates[:top_n]))

    return candidates[:top_n]


def find_mvp_by_role(
    person_aggregates: dict[str, dict],
    role: str,
    top_n: int = 10,
) -> list[tuple[str, float, int]]:
    """役職別MVPを発見.

    Args:
        person_aggregates: 集約貢献度
        role: 役職（文字列）
        top_n: 上位何人を返すか

    Returns:
        [(person_id, total_shapley, work_count), ...] のリスト
    """
    # Filter by primary role
    role_contributors = [
        (person_id, agg["total_shapley"], agg["work_count"])
        for person_id, agg in person_aggregates.items()
        if agg.get("primary_role") == role
    ]

    # Sort by total shapley
    role_contributors.sort(key=lambda x: x[1], reverse=True)

    logger.info("mvp_by_role_found", role=role, count=len(role_contributors[:top_n]))

    return role_contributors[:top_n]


def main():
    """スタンドアロン実行用エントリーポイント."""
    from src.analysis.anime_value import compute_anime_values
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

    # Group credits by anime
    anime_credits: dict[str, list[Credit]] = defaultdict(list)
    for credit in credits:
        anime_credits[credit.anime_id].append(credit)

    # Compute anime values
    logger.info("computing_anime_values")
    anime_values = compute_anime_values(anime_list, credits, person_scores)

    # Compute contributions for sample of anime (top 100 by value)
    top_anime = sorted(
        anime_values.items(), key=lambda x: x[1].composite_value, reverse=True
    )[:100]

    logger.info("computing_contributions")
    all_contributions: dict[str, dict[str, ContributionMetrics]] = {}

    for anime_id, anime_value_metrics in top_anime:
        anime_creds = anime_credits.get(anime_id, [])
        if not anime_creds:
            continue

        contributions = compute_contribution_attribution(
            anime_id, anime_value_metrics.composite_value, anime_creds, person_scores
        )
        all_contributions[anime_id] = contributions

    # Aggregate by person
    logger.info("aggregating_contributions")
    person_aggregates = aggregate_contributions_by_person(all_contributions)

    # Top contributors
    print("\n=== 総貢献度ランキング（トップ20）===\n")
    top_contributors = sorted(
        person_aggregates.items(), key=lambda x: x[1]["total_shapley"], reverse=True
    )[:20]

    for rank, (person_id, agg) in enumerate(top_contributors, 1):
        name = person_names.get(person_id, person_id)
        print(f"{rank}. {name} ({agg['primary_role']})")
        print(f"   総Shapley値: {agg['total_shapley']:.1f}")
        print(f"   平均価値シェア: {agg['avg_value_share']:.1f}%")
        print(f"   作品数: {agg['work_count']}")
        print()

    # Undervalued contributors
    print("\n=== 過小評価されている貢献者 ===\n")
    undervalued = find_undervalued_contributors(person_aggregates, person_scores, top_n=10)

    for person_id, contribution, current_score in undervalued:
        name = person_names.get(person_id, person_id)
        gap = contribution - current_score
        print(f"{name}:")
        print(f"  作品あたり貢献: {contribution:.2f}")
        print(f"  現在スコア: {current_score:.2f}")
        print(f"  ギャップ: +{gap:.2f} (過小評価)")
        print()

    # Role MVPs
    print("\n=== 役職別MVP ===\n")
    for role in ["director", "chief_animation_director", "animation_director", "key_animator"]:
        mvps = find_mvp_by_role(person_aggregates, role, top_n=3)
        if mvps:
            print(f"{role.upper()}:")
            for person_id, shapley, works in mvps:
                name = person_names.get(person_id, person_id)
                print(f"  - {name}: Shapley={shapley:.1f} ({works}作品)")
            print()

    conn.close()


if __name__ == "__main__":
    main()
