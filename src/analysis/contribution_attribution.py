"""Contribution Attribution — 貢献度の帰属分析.

作品の価値を各クリエイターにどう分配すべきかをShapley値で計算。
「この人がいなかったら作品価値はどう変わったか？」を定量化。

理論:
- Shapley Value: ゲーム理論的な公平な価値分配
- Marginal Contribution: 各人の限界貢献度
- Counterfactual Analysis: 反事実推論

役職重要度:
- OLS回帰で推定: log(production_scale) ~ role構成比
- データ駆動 — 主観的な重みを排除
"""

from collections import Counter, defaultdict
from dataclasses import dataclass, field

import numpy as np
import structlog

from src.models import Credit, Role

logger = structlog.get_logger()


@dataclass
class RoleWeightEstimation:
    """OLS推定による役職重要度の結果.

    Attributes:
        weights: role.value → normalized weight (sum=1.0)
        coefficients: role.value → raw OLS coefficient (β_k)
        r_squared: 回帰のR²
        n_anime: 推定に使用した作品数
        method: 推定方法 ("ols" or "uniform")
    """

    weights: dict[str, float] = field(default_factory=dict)
    coefficients: dict[str, float] = field(default_factory=dict)
    r_squared: float = 0.0
    n_anime: int = 0
    method: str = "uniform"


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


# =============================================================================
# Data-driven role weight estimation
# =============================================================================

# Minimum anime count to attempt OLS estimation
_MIN_ANIME_FOR_OLS = 30


def estimate_role_weights(
    credits: list[Credit],
    anime_staff_counts: dict[str, int] | None = None,
) -> RoleWeightEstimation:
    """OLS回帰でデータ駆動の役職重要度を推定.

    Model: log(staff_count_j) = Σ_k β_k × role_share_jk + ε_j

    各アニメ j の role k 構成比が、production scale (staff_count) をどの程度
    説明するかを推定。β_k > 0 なら、その役職の比率が高い作品ほど大規模。

    Args:
        credits: 全クレジットデータ
        anime_staff_counts: anime_id → staff_count (省略時はcreditsから計算)

    Returns:
        RoleWeightEstimation with normalized weights
    """
    result = RoleWeightEstimation()

    if not credits:
        return _uniform_weights(result)

    # Group credits by anime
    anime_credits: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        anime_credits[c.anime_id].append(c)

    if len(anime_credits) < _MIN_ANIME_FOR_OLS:
        logger.info(
            "role_weight_estimation_skipped",
            reason="insufficient_anime",
            n_anime=len(anime_credits),
            min_required=_MIN_ANIME_FOR_OLS,
        )
        return _uniform_weights(result)

    # Collect all roles that appear in data
    all_roles = sorted({c.role.value for c in credits})
    role_to_idx = {r: i for i, r in enumerate(all_roles)}
    n_roles = len(all_roles)

    # Build design matrix X (role shares) and target y (log staff_count)
    anime_ids = sorted(anime_credits.keys())
    n_anime = len(anime_ids)
    X = np.zeros((n_anime, n_roles))
    y = np.zeros(n_anime)

    for i, aid in enumerate(anime_ids):
        creds = anime_credits[aid]
        n_staff = (
            anime_staff_counts[aid]
            if anime_staff_counts and aid in anime_staff_counts
            else len(creds)
        )
        if n_staff < 2:
            continue

        # Role composition: fraction of staff in each role
        role_counts = Counter(c.role.value for c in creds)
        total = sum(role_counts.values())
        for role_val, count in role_counts.items():
            if role_val in role_to_idx:
                X[i, role_to_idx[role_val]] = count / total

        y[i] = np.log(n_staff)

    # Remove anime with y=0 (n_staff < 2 cases)
    mask = y > 0
    X = X[mask]
    y = y[mask]
    n_anime = int(mask.sum())

    if n_anime < _MIN_ANIME_FOR_OLS:
        return _uniform_weights(result)

    # OLS: β = (X'X)^{-1} X'y
    # Add small ridge penalty for numerical stability (roles with few observations)
    ridge = 1e-4 * np.eye(n_roles)
    try:
        XtX = X.T @ X + ridge
        Xty = X.T @ y
        beta = np.linalg.solve(XtX, Xty)
    except np.linalg.LinAlgError:
        logger.warning("role_weight_ols_failed", reason="singular_matrix")
        return _uniform_weights(result)

    # R²
    y_pred = X @ beta
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # Convert coefficients to weights:
    # - Take max(β_k, floor) to ensure all roles have positive weight
    # - Normalize to sum to 1.0
    floor = 0.01
    raw_weights = np.maximum(beta, floor)
    weight_sum = raw_weights.sum()
    normalized = (
        raw_weights / weight_sum if weight_sum > 0 else np.ones(n_roles) / n_roles
    )

    result.weights = {all_roles[i]: float(normalized[i]) for i in range(n_roles)}
    result.coefficients = {all_roles[i]: float(beta[i]) for i in range(n_roles)}
    result.r_squared = float(r_squared)
    result.n_anime = n_anime
    result.method = "ols"

    logger.info(
        "role_weights_estimated",
        method="ols",
        n_anime=n_anime,
        n_roles=n_roles,
        r_squared=round(r_squared, 4),
        top_3=[
            (r, round(w, 4))
            for r, w in sorted(result.weights.items(), key=lambda x: -x[1])[:3]
        ],
    )

    return result


def _uniform_weights(result: RoleWeightEstimation) -> RoleWeightEstimation:
    """Fallback: uniform weights across all Role enum values."""
    all_roles = [r.value for r in Role]
    n = len(all_roles)
    result.weights = {r: 1.0 / n for r in all_roles}
    result.method = "uniform"
    logger.info("role_weights_fallback", method="uniform", n_roles=n)
    return result


# Module-level cache for estimated weights (set by estimate_role_weights or externally)
_cached_role_weights: dict[str, float] | None = None


def set_role_weights(weights: dict[str, float] | None) -> None:
    """Set the module-level role weights cache (called after OLS estimation)."""
    global _cached_role_weights  # noqa: PLW0603
    _cached_role_weights = weights


def compute_role_importance(role: Role) -> float:
    """役職の重要度を取得.

    OLS推定済みなら推定値、未推定なら均一重みを返す。

    Args:
        role: 役職

    Returns:
        重要度（0-1）
    """
    if _cached_role_weights is not None:
        return _cached_role_weights.get(role.value, 0.01)
    # Fallback: uniform
    n = len(Role)
    return 1.0 / n


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
    person_quality = person_scores.get(person_id, {}).get("iv_score", staff_quality_avg)

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
    """Compute Shapley value for a person's contribution to a work.

    With an additive value function (each person's contribution is independent),
    Shapley values equal individual marginal contributions exactly.
    The sample_size parameter is kept for API compatibility but unused.

    Args:
        person_id: person_id
        role: 役職
        all_staff: 全スタッフ [(person_id, role), ...]
        anime_value: 作品の総価値
        person_scores: person_id → scores
        staff_quality_avg: スタッフ平均品質
        sample_size: unused (kept for API compatibility)

    Returns:
        Shapley value (= marginal contribution for additive games)
    """
    # With an additive value function (v(S) = sum of individual marginals),
    # Shapley values equal individual marginal contributions exactly.
    # No sampling needed — this is a mathematical identity for additive games.
    role = next((r for pid, r in all_staff if pid == person_id), None)
    if role is None:
        return 0.0

    shapley = estimate_marginal_contribution(
        person_id, role, anime_value, person_scores, staff_quality_avg
    )
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
    staff_iv_scores = [
        person_scores.get(c.person_id, {}).get("iv_score", 0.5) for c in credits
    ]
    staff_quality_avg = (
        sum(staff_iv_scores) / len(staff_iv_scores) if staff_iv_scores else 0.5
    )

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
        person_quality = person_scores.get(person_id, {}).get(
            "iv_score", staff_quality_avg
        )
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
            contrib.value_share = round(
                (contrib.shapley_value / total_shapley) * 100, 2
            )

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
        current_score = person_scores[person_id].get("iv_score", 0)

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
        load_all_anime,
        load_all_credits,
        load_all_persons,
        load_all_scores,
        get_connection,
        init_db,
    )

    conn = get_connection()
    init_db(conn)

    persons = load_all_persons(conn)
    anime_list = load_all_anime(conn)
    credits = load_all_credits(conn)
    scores_list = load_all_scores(conn)

    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}
    person_scores = {
        s.person_id: {
            "person_fe": s.person_fe,
            "birank": s.birank,
            "patronage": s.patronage,
            "iv_score": s.iv_score,
        }
        for s in scores_list
    }

    # Estimate role weights from data
    estimation = estimate_role_weights(credits)
    set_role_weights(estimation.weights)
    logger.info(
        "role_weights_set",
        method=estimation.method,
        r_squared=round(estimation.r_squared, 4),
    )

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
    undervalued = find_undervalued_contributors(
        person_aggregates, person_scores, top_n=10
    )

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
    for role in [
        "director",
        "animation_director",
        "key_animator",
    ]:
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
