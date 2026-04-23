"""Growth Acceleration Analysis — 成長率・加速度分析.

「今は無名だが急成長中」のクリエイターを発見。
時系列データから成長トレンド、加速度、early career potentialを推定。

アプローチ:
1. クレジット数の時系列推移（1階微分: velocity）
2. 成長の加速度（2階微分: acceleration）
3. トレンド検出（rising/stable/declining）
4. Early career bonus（新人の潜在能力）
"""

from collections import defaultdict
from dataclasses import dataclass, field
import statistics

import structlog

from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


@dataclass
class AccelerationMetrics:
    """Growth metrics.

    Attributes:
        person_id: person_id
        career_years: キャリア年数
        total_credits: 総クレジット数
        annual_credits: 年次クレジット数の履歴
        growth_velocity: 成長速度（クレジット/年）
        growth_acceleration: 成長加速度（変化率の変化）
        trend: トレンド（rising/stable/declining/early）
        momentum_score: モメンタムスコア（velocity + acceleration）
        early_career_bonus: 早期キャリアボーナス
        peak_year: ピーク年
        consistency: 一貫性（年次のばらつき逆数）
    """

    person_id: str
    career_years: int = 0
    total_credits: int = 0
    annual_credits: dict[int, int] = field(default_factory=dict)
    growth_velocity: float = 0.0
    growth_acceleration: float = 0.0
    trend: str = "stable"
    momentum_score: float = 0.0
    early_career_bonus: float = 0.0
    peak_year: int | None = None
    consistency: float = 0.0


def compute_growth_metrics(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    current_year: int = 2026,
) -> dict[str, AccelerationMetrics]:
    """Compute growth metrics.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        current_year: 現在年

    Returns:
        person_id → AccelerationMetrics
    """
    # person_id → year → count
    person_year_credits: dict[str, dict[int, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    for credit in credits:
        anime = anime_map.get(credit.anime_id)
        if anime and anime.year:
            person_year_credits[credit.person_id][anime.year] += 1

    metrics: dict[str, AccelerationMetrics] = {}

    for person_id, year_credits in person_year_credits.items():
        if not year_credits:
            continue

        # Sort years
        years = sorted(year_credits.keys())
        career_start = years[0]
        career_end = years[-1]
        career_years = career_end - career_start + 1

        # Total credits
        total_credits = sum(year_credits.values())

        # Compute velocity (1st derivative)
        # Simple: (recent avg - early avg) / time span
        if len(years) >= 2:
            # Recent 3 years
            recent_years = [y for y in years if y >= career_end - 2]
            recent_avg = sum(year_credits[y] for y in recent_years) / len(recent_years)

            # Early 3 years
            early_years = [y for y in years if y <= career_start + 2]
            early_avg = sum(year_credits[y] for y in early_years) / len(early_years)

            time_span = career_end - career_start
            velocity = (recent_avg - early_avg) / time_span if time_span > 0 else 0
        else:
            velocity = 0

        # Compute acceleration (2nd derivative)
        # Compare velocity in first half vs second half
        if len(years) >= 4:
            mid_year = years[len(years) // 2]

            # First half velocity
            first_half = [y for y in years if y <= mid_year]
            if len(first_half) >= 2:
                first_recent = [y for y in first_half if y >= first_half[-1] - 1]
                first_early = [y for y in first_half if y <= first_half[0] + 1]
                first_recent_avg = (
                    sum(year_credits[y] for y in first_recent) / len(first_recent)
                    if first_recent
                    else 0
                )
                first_early_avg = (
                    sum(year_credits[y] for y in first_early) / len(first_early)
                    if first_early
                    else 0
                )
                first_velocity = first_recent_avg - first_early_avg
            else:
                first_velocity = 0

            # Second half velocity
            second_half = [y for y in years if y > mid_year]
            if len(second_half) >= 2:
                second_recent = [y for y in second_half if y >= second_half[-1] - 1]
                second_early = [y for y in second_half if y <= second_half[0] + 1]
                second_recent_avg = (
                    sum(year_credits[y] for y in second_recent) / len(second_recent)
                    if second_recent
                    else 0
                )
                second_early_avg = (
                    sum(year_credits[y] for y in second_early) / len(second_early)
                    if second_early
                    else 0
                )
                second_velocity = second_recent_avg - second_early_avg
            else:
                second_velocity = 0

            acceleration = second_velocity - first_velocity
        else:
            acceleration = 0

        # Trend detection
        if career_years <= 3:
            trend = "early"  # Too early to judge
        elif velocity > 1.0:
            trend = "rising"
        elif velocity < -1.0:
            trend = "declining"
        else:
            trend = "stable"

        # Momentum score
        momentum_score = velocity + (acceleration * 0.5)

        # Early career bonus
        # Younger careers get bonus for potential
        years_since_debut = current_year - career_start
        if years_since_debut <= 5:
            early_bonus = (5 - years_since_debut) * 0.1  # Up to +50%
        else:
            early_bonus = 0

        # Peak year
        peak_year = max(year_credits.items(), key=lambda x: x[1])[0]

        # Consistency (inverse of std dev)
        if len(year_credits) >= 3:
            credit_values = list(year_credits.values())
            std_dev = statistics.stdev(credit_values)
            consistency = 1 / (std_dev + 1)  # Avoid division by zero
        else:
            consistency = 0

        metrics[person_id] = AccelerationMetrics(
            person_id=person_id,
            career_years=career_years,
            total_credits=total_credits,
            annual_credits=dict(year_credits),
            growth_velocity=round(velocity, 3),
            growth_acceleration=round(acceleration, 3),
            trend=trend,
            momentum_score=round(momentum_score, 3),
            early_career_bonus=round(early_bonus, 3),
            peak_year=peak_year,
            consistency=round(consistency, 3),
        )

    logger.info(
        "growth_metrics_computed",
        persons=len(metrics),
        rising=sum(1 for m in metrics.values() if m.trend == "rising"),
    )

    return metrics


def find_fast_risers(
    growth_metrics: dict[str, AccelerationMetrics],
    min_velocity: float = 2.0,
    top_n: int = 20,
) -> list[tuple[str, float, float]]:
    """Discover rapidly-growing creators.

    Args:
        growth_metrics: 成長指標
        min_velocity: 最低成長速度
        top_n: 上位何人を返すか

    Returns:
        [(person_id, velocity, acceleration), ...] のリスト
    """
    # Filter rising talents
    risers = [
        (person_id, m.growth_velocity, m.growth_acceleration)
        for person_id, m in growth_metrics.items()
        if m.growth_velocity >= min_velocity
    ]

    # Sort by momentum score
    risers.sort(
        key=lambda x: x[1] + x[2] * 0.5, reverse=True
    )  # velocity + 0.5*acceleration

    logger.info("fast_risers_found", count=len(risers[:top_n]))

    return risers[:top_n]


def find_early_potential(
    growth_metrics: dict[str, AccelerationMetrics],
    max_career_years: int = 5,
    min_momentum: float = 1.0,
    top_n: int = 20,
) -> list[tuple[str, int, float]]:
    """Discover high-potential early-career persons.

    Args:
        growth_metrics: 成長指標
        max_career_years: 最大キャリア年数
        min_momentum: 最低モメンタム
        top_n: 上位何人を返すか

    Returns:
        [(person_id, career_years, momentum), ...] のリスト
    """
    # Filter early career with momentum
    early_talents = [
        (person_id, m.career_years, m.momentum_score)
        for person_id, m in growth_metrics.items()
        if m.career_years <= max_career_years and m.momentum_score >= min_momentum
    ]

    # Sort by momentum score
    early_talents.sort(key=lambda x: x[2], reverse=True)

    logger.info("early_potential_found", count=len(early_talents[:top_n]))

    return early_talents[:top_n]


def compute_adjusted_person_fe_with_growth(
    person_scores: dict[str, dict],
    growth_metrics: dict[str, AccelerationMetrics],
    growth_weight: float = 0.3,
) -> dict[str, float]:
    """Compute a Skill score adjusted for growth rate.

    Adjusted Skill = Skill * (1 + growth_bonus)
    growth_bonus = (velocity + acceleration) * growth_weight

    Args:
        person_scores: person_id → scores dict（skillを含む）
        growth_metrics: 成長指標
        growth_weight: 成長の重み

    Returns:
        person_id → adjusted_skill
    """
    adjusted_skills: dict[str, float] = {}

    for person_id, scores in person_scores.items():
        original_skill = scores.get("person_fe", 0)

        if person_id not in growth_metrics:
            adjusted_skills[person_id] = original_skill
            continue

        metrics = growth_metrics[person_id]

        # Growth bonus
        growth_bonus = (
            metrics.growth_velocity + metrics.growth_acceleration * 0.5
        ) * growth_weight

        # Early career bonus
        growth_bonus += metrics.early_career_bonus

        # Adjusted skill
        adjusted_skill = original_skill * (1 + growth_bonus)

        adjusted_skills[person_id] = round(adjusted_skill, 4)

    logger.info(
        "adjusted_skills_computed",
        persons=len(adjusted_skills),
        avg_adjustment=round(
            sum(
                (adjusted_skills[pid] - person_scores[pid].get("person_fe", 0))
                / person_scores[pid].get("person_fe", 1)
                for pid in adjusted_skills
                if person_scores[pid].get("person_fe", 0) > 0
            )
            / len(adjusted_skills),
            3,
        ),
    )

    return adjusted_skills


def main():
    """Standalone entry point."""
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

    # build lookup maps
    anime_map = {a.id: a for a in anime_list}
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}
    person_scores = {
        s.person_id: {"person_fe": s.person_fe, "iv_score": s.iv_score}
        for s in scores_list
    }

    # compute growth metrics
    logger.info("computing_growth_metrics")
    growth_metrics = compute_growth_metrics(credits, anime_map)

    # rapidly-growing persons
    print("\n=== 急成長中のクリエイター（Fast Risers）===\n")
    fast_risers = find_fast_risers(growth_metrics, min_velocity=2.0, top_n=10)

    for person_id, velocity, acceleration in fast_risers:
        name = person_names.get(person_id, person_id)
        metrics = growth_metrics[person_id]

        print(f"{name}:")
        print(f"  成長速度: {velocity:.2f} クレジット/年")
        print(f"  加速度: {acceleration:.2f}")
        print(f"  モメンタム: {metrics.momentum_score:.2f}")
        print(f"  キャリア: {metrics.career_years}年")
        print()

    # early-career potential
    print("\n=== 早期キャリアのポテンシャル人材 ===\n")
    early_potential = find_early_potential(
        growth_metrics, max_career_years=5, min_momentum=1.0, top_n=10
    )

    for person_id, career_years, momentum in early_potential:
        name = person_names.get(person_id, person_id)
        metrics = growth_metrics[person_id]

        print(f"{name} (キャリア{career_years}年):")
        print(f"  モメンタム: {momentum:.2f}")
        print(f"  トレンド: {metrics.trend}")
        print(f"  総クレジット: {metrics.total_credits}")
        print()

    # growth-rate-adjusted Skill score
    logger.info("computing_adjusted_skills")
    adjusted_skills = compute_adjusted_person_fe_with_growth(
        person_scores, growth_metrics, growth_weight=0.3
    )

    print("\n=== Skillスコア改善トップ10（成長率考慮）===\n")

    improvements = [
        (
            person_id,
            person_scores[person_id].get("person_fe", 0),
            adjusted_skills[person_id],
            adjusted_skills[person_id] - person_scores[person_id].get("person_fe", 0),
        )
        for person_id in adjusted_skills
        if person_scores[person_id].get("person_fe", 0) > 0
    ]

    improvements.sort(key=lambda x: x[3], reverse=True)

    for person_id, original, adjusted, improvement in improvements[:10]:
        name = person_names.get(person_id, person_id)
        print(f"{name}:")
        print(f"  元Skill: {original:.3f}")
        print(f"  調整後: {adjusted:.3f} (+{improvement:.3f})")
        print()

    conn.close()


if __name__ == "__main__":
    main()
