"""Studio Bias Correction — スタジオバイアスの補正.

PageRankは「有名スタジオの人」に偏りやすい問題を解決。
クロススタジオでの実績を重視し、真の実力を評価。

補正方法:
1. スタジオごとのPageRank分布を分析
2. 「大手スタジオ所属」による過大評価を検出
3. クロススタジオ実績（studio diversity）で補正
4. スタジオ外評価（external validation）を統合
"""

from collections import defaultdict
from dataclasses import dataclass
import math

import structlog

from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


@dataclass
class StudioBiasMetrics:
    """Studio bias metrics.

    Attributes:
        person_id: person_id
        primary_studio: 主要活動スタジオ
        studio_diversity: スタジオ多様性（Shannon entropy）
        cross_studio_works: クロススタジオ作品数
        studio_concentration: スタジオ集中度（1スタジオへの偏り）
        external_validation: スタジオ外での評価獲得度
        bias_score: バイアススコア（高いほど偏りが大きい）
    """

    person_id: str
    primary_studio: str | None = None
    studio_diversity: float = 0.0
    cross_studio_works: int = 0
    studio_concentration: float = 0.0
    external_validation: float = 0.0
    bias_score: float = 0.0


@dataclass
class DebiasedScore:
    """Bias-corrected score.

    Attributes:
        person_id: person_id
        original_birank: 元のBiRank
        studio_bias: 検出されたスタジオバイアス
        debiased_birank: 補正後BiRank
        cross_studio_bonus: クロススタジオボーナス
        diversity_factor: 多様性係数
    """

    person_id: str
    original_birank: float = 0.0
    studio_bias: float = 0.0
    debiased_birank: float = 0.0
    cross_studio_bonus: float = 0.0
    diversity_factor: float = 1.0


def extract_studio_from_anime(anime: Anime) -> str | None:
    """Extract studio names from an anime.

    Args:
        anime: Animeオブジェクト

    Returns:
        スタジオ名（不明の場合None）
    """
    if anime.studios:
        return anime.studios[0]
    return "unknown"


def extract_all_studios(anime: Anime) -> list[str]:
    """Extract all studios from an anime.

    Args:
        anime: Animeオブジェクト

    Returns:
        スタジオ名リスト
    """
    if anime.studios:
        return anime.studios
    return []


def compute_studio_bias_metrics(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, StudioBiasMetrics]:
    """Compute studio bias metrics.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime

    Returns:
        person_id → StudioBiasMetrics
    """
    # person_id → studio → count
    person_studio_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    for credit in credits:
        anime = anime_map.get(credit.anime_id)
        if not anime:
            continue

        studio = extract_studio_from_anime(anime)
        if studio:
            person_studio_counts[credit.person_id][studio] += 1

    metrics: dict[str, StudioBiasMetrics] = {}

    for person_id, studio_counts in person_studio_counts.items():
        total_works = sum(studio_counts.values())
        if total_works == 0:
            continue

        # Primary studio (most frequent)
        primary_studio = max(studio_counts.items(), key=lambda x: x[1])[0]
        primary_count = studio_counts[primary_studio]

        # Studio concentration (Herfindahl index)
        concentration = sum(
            (count / total_works) ** 2 for count in studio_counts.values()
        )

        # Studio diversity (Shannon entropy)
        diversity = 0.0
        for count in studio_counts.values():
            p = count / total_works
            if p > 0:
                diversity -= p * math.log2(p)

        # Normalize by max entropy
        max_entropy = math.log2(len(studio_counts)) if len(studio_counts) > 1 else 1
        normalized_diversity = diversity / max_entropy if max_entropy > 0 else 0

        # Cross-studio works (worked with multiple studios)
        cross_studio_works = len(studio_counts) - 1  # Exclude primary studio

        # External validation: works outside primary studio
        external_works = total_works - primary_count
        external_validation = external_works / total_works if total_works > 0 else 0

        # Bias score: high concentration = high bias
        bias_score = concentration * (1 - normalized_diversity)

        metrics[person_id] = StudioBiasMetrics(
            person_id=person_id,
            primary_studio=primary_studio,
            studio_diversity=round(normalized_diversity, 3),
            cross_studio_works=cross_studio_works,
            studio_concentration=round(concentration, 3),
            external_validation=round(external_validation, 3),
            bias_score=round(bias_score, 3),
        )

    logger.info(
        "studio_bias_metrics_computed",
        persons=len(metrics),
        avg_diversity=round(
            sum(m.studio_diversity for m in metrics.values()) / len(metrics), 3
        )
        if metrics
        else 0.0,
    )

    return metrics


def compute_studio_prestige(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, dict],
) -> dict[str, float]:
    """Compute "prestige" per studio.

    所属クリエイターの平均スコアから推定。

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        person_scores: person_id → scores dict

    Returns:
        studio → prestige score
    """
    studio_scores: dict[str, list[float]] = defaultdict(list)

    for credit in credits:
        anime = anime_map.get(credit.anime_id)
        if not anime:
            continue

        studio = extract_studio_from_anime(anime)
        if studio and credit.person_id in person_scores:
            score = person_scores[credit.person_id].get("birank", 0)
            studio_scores[studio].append(score)

    # Average score per studio
    studio_prestige = {
        studio: sum(scores) / len(scores) if scores else 0
        for studio, scores in studio_scores.items()
    }

    logger.info(
        "studio_prestige_computed",
        studios=len(studio_prestige),
        top_studio=max(studio_prestige.items(), key=lambda x: x[1])[0]
        if studio_prestige
        else None,
    )

    return studio_prestige


def debias_birank_scores(
    person_scores: dict[str, dict],
    bias_metrics: dict[str, StudioBiasMetrics],
    studio_prestige: dict[str, float],
    debias_strength: float = 0.3,
) -> dict[str, DebiasedScore]:
    """BiRankスコアからスタジオバイアスを除去.

    Args:
        person_scores: person_id → scores dict（birankを含む）
        bias_metrics: スタジオバイアス指標
        studio_prestige: スタジオの威信スコア
        debias_strength: 補正の強さ（0-1）

    Returns:
        person_id → DebiasedScore
    """
    # Normalize studio prestige (0-1)
    max_prestige = max(studio_prestige.values()) if studio_prestige else 1
    normalized_prestige = {
        studio: prestige / max_prestige if max_prestige > 0 else 0
        for studio, prestige in studio_prestige.items()
    }

    debiased: dict[str, DebiasedScore] = {}

    for person_id, scores in person_scores.items():
        original_birank = scores.get("birank", 0)

        if person_id not in bias_metrics:
            # No bias data, keep original
            debiased[person_id] = DebiasedScore(
                person_id=person_id,
                original_birank=original_birank,
                debiased_birank=original_birank,
            )
            continue

        metrics = bias_metrics[person_id]

        # Studio bias penalty
        # High prestige studio + high concentration = high bias
        studio_prestige_value = normalized_prestige.get(metrics.primary_studio, 0)
        studio_bias = (
            studio_prestige_value * metrics.studio_concentration * debias_strength
        )

        # Diversity bonus
        # High diversity = less bias, get bonus
        diversity_factor = 1.0 + (metrics.studio_diversity * 0.2)  # Up to +20%

        # Cross-studio bonus
        # More studios = more validation
        cross_studio_bonus = min(0.1, metrics.cross_studio_works * 0.02)  # Up to +10%

        # Debiased birank
        debiased_birank = original_birank * (1 - studio_bias) * diversity_factor + (
            original_birank * cross_studio_bonus
        )

        debiased[person_id] = DebiasedScore(
            person_id=person_id,
            original_birank=round(original_birank, 4),
            studio_bias=round(studio_bias, 4),
            debiased_birank=round(debiased_birank, 4),
            cross_studio_bonus=round(cross_studio_bonus, 4),
            diversity_factor=round(diversity_factor, 3),
        )

    logger.info(
        "birank_debiased",
        persons=len(debiased),
        avg_bias=round(
            sum(d.studio_bias for d in debiased.values()) / len(debiased), 4
        ),
    )

    return debiased


def find_undervalued_by_studio(
    debiased: dict[str, DebiasedScore],
    top_n: int = 20,
) -> list[tuple[str, float, float]]:
    """Discover persons undervalued due to studio bias.

    補正後のスコアが大きく上昇した人 = 実力が過小評価されていた

    Args:
        debiased: バイアス補正スコア
        top_n: 上位何人を返すか

    Returns:
        [(person_id, original, debiased), ...] のリスト
    """
    # Calculate improvement
    improvements = [
        (
            person_id,
            d.original_birank,
            d.debiased_birank,
            d.debiased_birank - d.original_birank,
        )
        for person_id, d in debiased.items()
    ]

    # Sort by improvement (descending)
    improvements.sort(key=lambda x: x[3], reverse=True)

    result = [
        (pid, original, debiased) for pid, original, debiased, _ in improvements[:top_n]
    ]

    logger.info("undervalued_talents_found", count=len(result))

    return result


def find_overvalued_by_studio(
    debiased: dict[str, DebiasedScore],
    top_n: int = 20,
) -> list[tuple[str, float, float]]:
    """Discover persons overvalued due to studio bias.

    補正後のスコアが大きく下降した人 = スタジオの威信で評価されていた

    Args:
        debiased: バイアス補正スコア
        top_n: 上位何人を返すか

    Returns:
        [(person_id, original, debiased), ...] のリスト
    """
    # Calculate decline
    declines = [
        (
            person_id,
            d.original_birank,
            d.debiased_birank,
            d.original_birank - d.debiased_birank,
        )
        for person_id, d in debiased.items()
    ]

    # Sort by decline (descending)
    declines.sort(key=lambda x: x[3], reverse=True)

    result = [
        (pid, original, debiased) for pid, original, debiased, _ in declines[:top_n]
    ]

    logger.info("overvalued_talents_found", count=len(result))

    return result


@dataclass
class StudioDisparityResult:
    """Cross-studio compensation gap analysis result.

    Attributes:
        studio: スタジオ名
        person_count: 所属人数
        mean_iv_score: 平均IV Scoreスコア
        mean_birank: 平均BiRankスコア
        mean_patronage: 平均Patronageスコア
        mean_person_fe: 平均Person FEスコア
        score_std: iv_scoreの標準偏差
    """

    studio: str
    person_count: int = 0
    mean_iv_score: float = 0.0
    mean_birank: float = 0.0
    mean_patronage: float = 0.0
    mean_person_fe: float = 0.0
    score_std: float = 0.0


def compute_studio_disparity(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, dict],
    min_persons: int = 5,
) -> dict[str, StudioDisparityResult]:
    """Analyse the compensation gap (score distribution) between studios.

    同程度のPerson FEを持つ人材がスタジオによって
    異なるBiRank/Patronage評価を受けているかを検出。

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        person_scores: person_id → {"birank", "patronage", "person_fe", "iv_score"}
        min_persons: 最低所属人数（統計的信頼性のため）

    Returns:
        studio → StudioDisparityResult
    """
    # Map person → primary studio (most credits)
    person_studio_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for credit in credits:
        anime = anime_map.get(credit.anime_id)
        if not anime:
            continue
        studios = extract_all_studios(anime)
        for studio in studios:
            person_studio_counts[credit.person_id][studio] += 1

    person_primary_studio: dict[str, str] = {}
    for person_id, studio_counts in person_studio_counts.items():
        if studio_counts:
            person_primary_studio[person_id] = max(
                studio_counts.items(), key=lambda x: x[1]
            )[0]

    # Group scores by studio
    studio_person_scores: dict[str, list[dict]] = defaultdict(list)
    for person_id, scores in person_scores.items():
        studio = person_primary_studio.get(person_id)
        if studio and studio != "unknown":
            studio_person_scores[studio].append(scores)

    # Compute per-studio statistics
    results: dict[str, StudioDisparityResult] = {}
    for studio, scores_list in studio_person_scores.items():
        if len(scores_list) < min_persons:
            continue

        iv_scores = [s.get("iv_score", 0) for s in scores_list]
        biranks = [s.get("birank", 0) for s in scores_list]
        patronages = [s.get("patronage", 0) for s in scores_list]
        person_fes = [s.get("person_fe", 0) for s in scores_list]

        n = len(iv_scores)
        mean_iv = sum(iv_scores) / n
        mean_birank = sum(biranks) / n
        mean_patronage = sum(patronages) / n
        mean_person_fe = sum(person_fes) / n

        # Standard deviation
        variance = sum((x - mean_iv) ** 2 for x in iv_scores) / n
        std_dev = math.sqrt(variance)

        results[studio] = StudioDisparityResult(
            studio=studio,
            person_count=n,
            mean_iv_score=round(mean_iv, 4),
            mean_birank=round(mean_birank, 4),
            mean_patronage=round(mean_patronage, 4),
            mean_person_fe=round(mean_person_fe, 4),
            score_std=round(std_dev, 4),
        )

    logger.info(
        "studio_disparity_computed",
        studios=len(results),
        max_gap=round(
            max(r.mean_iv_score for r in results.values())
            - min(r.mean_iv_score for r in results.values()),
            4,
        )
        if len(results) >= 2
        else 0,
    )

    return results


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
        s.person_id: {"birank": s.birank, "iv_score": s.iv_score} for s in scores_list
    }

    # studio bias analysis
    logger.info("computing_studio_bias_metrics")
    bias_metrics = compute_studio_bias_metrics(credits, anime_map)

    # studio prestige
    logger.info("computing_studio_prestige")
    studio_prestige = compute_studio_prestige(credits, anime_map, person_scores)

    # bias correction
    logger.info("debiasing_birank_scores")
    debiased = debias_birank_scores(
        person_scores, bias_metrics, studio_prestige, debias_strength=0.3
    )

    # display results
    print("\n=== スタジオ威信ランキング（トップ10）===\n")
    sorted_studios = sorted(studio_prestige.items(), key=lambda x: x[1], reverse=True)[
        :10
    ]
    for studio, prestige in sorted_studios:
        print(f"{studio}: {prestige:.3f}")

    print("\n=== 過小評価人材（スタジオバイアス補正後に上昇）===\n")
    undervalued = find_undervalued_by_studio(debiased, top_n=10)

    for person_id, original, debiased_score in undervalued:
        name = person_names.get(person_id, person_id)
        improvement = debiased_score - original
        primary = bias_metrics.get(person_id)
        studio_name = primary.primary_studio if primary else "unknown"

        print(f"{name} ({studio_name}):")
        print(f"  元BiRank: {original:.3f}")
        print(f"  補正後: {debiased_score:.3f} (+{improvement:.3f})")
        if primary:
            print(f"  スタジオ多様性: {primary.studio_diversity:.2f}")
        print()

    print("\n=== 過大評価人材（スタジオバイアス補正後に下降）===\n")
    overvalued = find_overvalued_by_studio(debiased, top_n=10)

    for person_id, original, debiased_score in overvalued:
        name = person_names.get(person_id, person_id)
        decline = original - debiased_score
        primary = bias_metrics.get(person_id)
        studio_name = primary.primary_studio if primary else "unknown"

        print(f"{name} ({studio_name}):")
        print(f"  元BiRank: {original:.3f}")
        print(f"  補正後: {debiased_score:.3f} (-{decline:.3f})")
        if primary:
            print(f"  スタジオ集中度: {primary.studio_concentration:.2f}")
        print()

    conn.close()


if __name__ == "__main__":
    main()
