"""Anime Value Assessment — 作品価値の多次元評価.

作品自体の価値を複数の次元で定量化:
1. 商業的価値（人気・評価）
2. 批評的価値（賞・業界評価）
3. 創造的価値（革新性・独創性）
4. 文化的価値（長期的影響力）
5. 技術的価値（制作品質）

これらを統合した「総合作品価値」を算出し、人材評価にフィードバック。
"""

from collections import defaultdict
from dataclasses import dataclass, field

import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


@dataclass
class AnimeValueMetrics:
    """作品価値の指標.

    Attributes:
        anime_id: anime_id
        title: タイトル
        year: 制作年
        # 5つの価値次元
        commercial_value: 商業的価値（人気・評価）
        critical_value: 批評的価値（賞・業界評価）
        creative_value: 創造的価値（革新性・独創性）
        cultural_value: 文化的価値（長期的影響力）
        technical_value: 技術的価値（制作品質）
        # 統合スコア
        composite_value: 総合作品価値（0-100）
        # メタデータ
        staff_quality: スタッフの平均スコア
        staff_count: スタッフ数
        key_contributors: キーコントリビューター（上位5人）
        value_per_staff: スタッフあたり価値
    """

    anime_id: str
    title: str
    year: int | None = None
    # Value dimensions
    commercial_value: float = 0.0
    critical_value: float = 0.0
    creative_value: float = 0.0
    cultural_value: float = 0.0
    technical_value: float = 0.0
    # Composite
    composite_value: float = 0.0
    # Metadata
    staff_quality: float = 0.0
    staff_count: int = 0
    key_contributors: list[tuple[str, float]] = field(default_factory=list)
    value_per_staff: float = 0.0


def compute_commercial_value(
    anime: Anime,
    credits: list[Credit],
) -> float:
    """商業的価値を推定.

    指標:
    - スタッフ数（大規模プロジェクト）
    - 役職の多様性
    - （将来的に）視聴率、円盤売上、配信数など

    Args:
        anime: Animeオブジェクト
        credits: この作品のクレジット

    Returns:
        商業的価値（0-1）
    """
    # Staff count proxy (larger projects = more investment)
    staff_count = len(set(c.person_id for c in credits))
    staff_score = min(1.0, staff_count / 50)  # Normalize to 50 staff

    # Role diversity (more roles = more complex production)
    unique_roles = len(set(c.role for c in credits))
    diversity_score = min(1.0, unique_roles / 20)  # Normalize to 20 roles

    # Placeholder for external data (ratings, sales)
    # In real implementation, integrate with MAL ratings, etc.
    external_score = 0.5  # Default

    commercial = (staff_score * 0.3 + diversity_score * 0.2 + external_score * 0.5)

    return round(commercial, 4)


def compute_critical_value(
    anime: Anime,
    credits: list[Credit],
) -> float:
    """批評的価値を推定.

    指標:
    - タグ/ジャンルの存在（メタデータの充実度）
    - （将来的に）受賞歴、批評家スコアなど

    Args:
        anime: Animeオブジェクト
        credits: この作品のクレジット

    Returns:
        批評的価値（0-1）
    """
    # Tag richness (well-documented = critically discussed)
    if hasattr(anime, "tags") and anime.tags:
        tag_score = min(1.0, len(anime.tags) / 10)
    else:
        tag_score = 0.3  # Default

    # Placeholder for awards, critic scores
    awards_score = 0.5  # Default

    critical = (tag_score * 0.4 + awards_score * 0.6)

    return round(critical, 4)


def compute_creative_value(
    anime: Anime,
    credits: list[Credit],
    person_scores: dict[str, dict],
) -> float:
    """創造的価値を推定.

    指標:
    - キークリエイターのSkillスコア
    - スタッフのジャンル多様性
    - 新しいジャンル/タグの組み合わせ

    Args:
        anime: Animeオブジェクト
        credits: この作品のクレジット
        person_scores: person_id → scores

    Returns:
        創造的価値（0-1）
    """
    from src.utils.role_groups import is_director_role, is_animator_role

    # Key creators' average skill
    director_skills = [
        person_scores.get(c.person_id, {}).get("skill", 0)
        for c in credits
        if is_director_role(c.role) and c.person_id in person_scores
    ]

    animator_skills = [
        person_scores.get(c.person_id, {}).get("skill", 0)
        for c in credits
        if is_animator_role(c.role) and c.person_id in person_scores
    ]

    avg_director_skill = sum(director_skills) / len(director_skills) if director_skills else 0.5
    avg_animator_skill = (
        sum(animator_skills) / len(animator_skills) if animator_skills else 0.5
    )

    skill_score = (avg_director_skill * 0.6 + avg_animator_skill * 0.4)

    # Genre/tag novelty (placeholder)
    novelty_score = 0.5

    creative = (skill_score * 0.7 + novelty_score * 0.3)

    return round(creative, 4)


def compute_cultural_value(
    anime: Anime,
    credits: list[Credit],
    current_year: int = 2026,
) -> float:
    """文化的価値を推定.

    指標:
    - 経過年数（古いが残っている = 文化的意義）
    - （将来的に）引用・オマージュ数、ミーム化など

    Args:
        anime: Animeオブジェクト
        credits: この作品のクレジット
        current_year: 現在年

    Returns:
        文化的価値（0-1）
    """
    if not anime.year:
        return 0.5

    # Age bonus (classics gain value over time)
    age = current_year - anime.year
    age_score = min(1.0, age / 20)  # 20 years = classic

    # Longevity (still discussed after many years)
    if age > 10:
        longevity_score = 0.8
    elif age > 5:
        longevity_score = 0.6
    else:
        longevity_score = 0.4

    cultural = (age_score * 0.5 + longevity_score * 0.5)

    return round(cultural, 4)


def compute_technical_value(
    anime: Anime,
    credits: list[Credit],
    person_scores: dict[str, dict],
) -> float:
    """技術的価値を推定.

    指標:
    - アニメーターの平均スコア
    - 技術スタッフの充実度
    - （将来的に）作画評価、撮影技術など

    Args:
        anime: Animeオブジェクト
        credits: この作品のクレジット
        person_scores: person_id → scores

    Returns:
        技術的価値（0-1）
    """
    from src.utils.role_groups import is_animator_role

    # Animator quality
    animator_composites = [
        person_scores.get(c.person_id, {}).get("composite", 0)
        for c in credits
        if is_animator_role(c.role) and c.person_id in person_scores
    ]

    avg_animator_quality = (
        sum(animator_composites) / len(animator_composites) if animator_composites else 0.5
    )

    # Technical staff count (art, photography, effects)
    technical_roles = ["art_director", "photography_director", "effects", "cgi_director"]
    technical_count = sum(
        1 for c in credits if c.role.value.lower() in technical_roles
    )
    technical_score = min(1.0, technical_count / 5)

    technical = (avg_animator_quality * 0.7 + technical_score * 0.3)

    return round(technical, 4)


def compute_anime_values(
    anime_list: list[Anime],
    credits: list[Credit],
    person_scores: dict[str, dict],
    current_year: int = 2026,
) -> dict[str, AnimeValueMetrics]:
    """全作品の価値を計算.

    Args:
        anime_list: 全アニメ
        credits: 全クレジット
        person_scores: person_id → scores
        current_year: 現在年

    Returns:
        anime_id → AnimeValueMetrics
    """
    # Group credits by anime
    anime_credits: dict[str, list[Credit]] = defaultdict(list)
    for credit in credits:
        anime_credits[credit.anime_id].append(credit)

    values: dict[str, AnimeValueMetrics] = {}

    for anime in anime_list:
        anime_creds = anime_credits.get(anime.id, [])
        if not anime_creds:
            continue

        # Compute 5 value dimensions
        commercial = compute_commercial_value(anime, anime_creds)
        critical = compute_critical_value(anime, anime_creds)
        creative = compute_creative_value(anime, anime_creds, person_scores)
        cultural = compute_cultural_value(anime, anime_creds, current_year)
        technical = compute_technical_value(anime, anime_creds, person_scores)

        # Composite value (weighted average)
        composite = (
            commercial * 0.25
            + critical * 0.15
            + creative * 0.30
            + cultural * 0.10
            + technical * 0.20
        ) * 100  # Scale to 0-100

        # Staff quality
        staff_composites = [
            person_scores.get(c.person_id, {}).get("composite", 0)
            for c in anime_creds
            if c.person_id in person_scores
        ]
        avg_staff_quality = (
            sum(staff_composites) / len(staff_composites) if staff_composites else 0
        )

        # Key contributors (top 5 by composite score)
        staff_with_scores = [
            (c.person_id, person_scores.get(c.person_id, {}).get("composite", 0))
            for c in anime_creds
            if c.person_id in person_scores
        ]
        staff_with_scores.sort(key=lambda x: x[1], reverse=True)
        key_contributors = staff_with_scores[:5]

        # Value per staff
        staff_count = len(set(c.person_id for c in anime_creds))
        value_per_staff = composite / staff_count if staff_count > 0 else 0

        values[anime.id] = AnimeValueMetrics(
            anime_id=anime.id,
            title=anime.title_ja or anime.title_en or anime.id,
            year=anime.year,
            commercial_value=commercial,
            critical_value=critical,
            creative_value=creative,
            cultural_value=cultural,
            technical_value=technical,
            composite_value=round(composite, 2),
            staff_quality=round(avg_staff_quality, 3),
            staff_count=staff_count,
            key_contributors=key_contributors,
            value_per_staff=round(value_per_staff, 2),
        )

    logger.info(
        "anime_values_computed",
        anime=len(values),
        avg_value=round(sum(v.composite_value for v in values.values()) / len(values), 2),
    )

    return values


def rank_anime_by_value(
    anime_values: dict[str, AnimeValueMetrics],
    dimension: str = "composite",
    top_n: int = 50,
) -> list[tuple[str, str, float]]:
    """作品価値でランキング.

    Args:
        anime_values: 作品価値指標
        dimension: ランキング基準（composite/commercial/creative/etc）
        top_n: 上位何件を返すか

    Returns:
        [(anime_id, title, value), ...] のリスト
    """
    # Get value by dimension
    if dimension == "composite":
        ranked = [
            (anime_id, v.title, v.composite_value) for anime_id, v in anime_values.items()
        ]
    elif dimension == "commercial":
        ranked = [
            (anime_id, v.title, v.commercial_value * 100)
            for anime_id, v in anime_values.items()
        ]
    elif dimension == "creative":
        ranked = [
            (anime_id, v.title, v.creative_value * 100) for anime_id, v in anime_values.items()
        ]
    elif dimension == "technical":
        ranked = [
            (anime_id, v.title, v.technical_value * 100)
            for anime_id, v in anime_values.items()
        ]
    elif dimension == "cultural":
        ranked = [
            (anime_id, v.title, v.cultural_value * 100) for anime_id, v in anime_values.items()
        ]
    elif dimension == "value_per_staff":
        ranked = [
            (anime_id, v.title, v.value_per_staff) for anime_id, v in anime_values.items()
        ]
    else:
        ranked = [
            (anime_id, v.title, v.composite_value) for anime_id, v in anime_values.items()
        ]

    # Sort descending
    ranked.sort(key=lambda x: x[2], reverse=True)

    logger.info("anime_ranked", dimension=dimension, count=len(ranked[:top_n]))

    return ranked[:top_n]


def find_undervalued_works(
    anime_values: dict[str, AnimeValueMetrics],
    min_staff_quality: float = 0.6,
    max_value: float = 50,
    top_n: int = 20,
) -> list[tuple[str, str, float, float]]:
    """過小評価作品を発見.

    高品質スタッフだが低価値 = 商業的に失敗 or 未評価

    Args:
        anime_values: 作品価値指標
        min_staff_quality: 最低スタッフ品質
        max_value: 最大作品価値（これ以下）
        top_n: 上位何件を返すか

    Returns:
        [(anime_id, title, staff_quality, value), ...] のリスト
    """
    undervalued = [
        (anime_id, v.title, v.staff_quality, v.composite_value)
        for anime_id, v in anime_values.items()
        if v.staff_quality >= min_staff_quality and v.composite_value <= max_value
    ]

    # Sort by staff quality (descending)
    undervalued.sort(key=lambda x: x[2], reverse=True)

    logger.info("undervalued_works_found", count=len(undervalued[:top_n]))

    return undervalued[:top_n]


def find_overperforming_works(
    anime_values: dict[str, AnimeValueMetrics],
    max_staff_quality: float = 0.4,
    min_value: float = 60,
    top_n: int = 20,
) -> list[tuple[str, str, float, float]]:
    """期待以上の作品を発見.

    低品質スタッフだが高価値 = 予想外のヒット

    Args:
        anime_values: 作品価値指標
        max_staff_quality: 最大スタッフ品質
        min_value: 最低作品価値（これ以上）
        top_n: 上位何件を返すか

    Returns:
        [(anime_id, title, staff_quality, value), ...] のリスト
    """
    overperforming = [
        (anime_id, v.title, v.staff_quality, v.composite_value)
        for anime_id, v in anime_values.items()
        if v.staff_quality <= max_staff_quality and v.composite_value >= min_value
    ]

    # Sort by value (descending)
    overperforming.sort(key=lambda x: x[3], reverse=True)

    logger.info("overperforming_works_found", count=len(overperforming[:top_n]))

    return overperforming[:top_n]


def main():
    """スタンドアロン実行用エントリーポイント."""
    from src.database import (
        get_all_anime,
        get_all_credits,
        get_all_scores,
        get_connection,
        init_db,
    )

    conn = get_connection()
    init_db(conn)

    anime_list = get_all_anime(conn)
    credits = get_all_credits(conn)
    scores_list = get_all_scores(conn)

    person_scores = {
        s.person_id: {
            "authority": s.authority,
            "trust": s.trust,
            "skill": s.skill,
            "composite": s.composite,
        }
        for s in scores_list
    }

    # 作品価値計算
    logger.info("computing_anime_values")
    anime_values = compute_anime_values(anime_list, credits, person_scores)

    # 総合ランキング
    print("\n=== 作品価値ランキング（トップ20）===\n")
    top_anime = rank_anime_by_value(anime_values, dimension="composite", top_n=20)

    for rank, (anime_id, title, value) in enumerate(top_anime, 1):
        metrics = anime_values[anime_id]
        print(f"{rank}. {title} ({metrics.year})")
        print(f"   総合価値: {value:.1f}")
        print(f"   内訳: 商業={metrics.commercial_value:.2f}, "
              f"創造={metrics.creative_value:.2f}, "
              f"技術={metrics.technical_value:.2f}")
        print(f"   スタッフ品質: {metrics.staff_quality:.2f} ({metrics.staff_count}人)")
        print()

    # 次元別トップ5
    print("\n=== 次元別トップ5 ===\n")

    for dimension, name in [
        ("commercial", "商業的価値"),
        ("creative", "創造的価値"),
        ("technical", "技術的価値"),
    ]:
        print(f"{name}:")
        top = rank_anime_by_value(anime_values, dimension=dimension, top_n=5)
        for anime_id, title, value in top:
            print(f"  - {title}: {value:.1f}")
        print()

    # 過小評価作品
    print("\n=== 過小評価作品（高品質スタッフ・低価値）===\n")
    undervalued = find_undervalued_works(
        anime_values, min_staff_quality=0.6, max_value=50, top_n=10
    )

    for anime_id, title, staff_quality, value in undervalued:
        print(f"{title}:")
        print(f"  スタッフ品質: {staff_quality:.2f}")
        print(f"  作品価値: {value:.1f}")
        print()

    conn.close()


if __name__ == "__main__":
    main()
