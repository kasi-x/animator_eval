"""Score cross-validation — measure score stability.

クレジットデータの一部を除外してスコアを再計算し、
ランキングがどの程度安定しているかを評価する。

安定性が高い = スコアが個別のクレジットに過度に依存していない
"""

import random

import structlog

from src.analysis.scoring.birank import compute_birank
from src.analysis.graph import create_person_anime_network
from src.models import AnimeAnalysis as Anime, Credit, Person

logger = structlog.get_logger()


def _rank_correlation(ranks_a: dict[str, int], ranks_b: dict[str, int]) -> float:
    """Spearman の順位相関係数を計算する."""
    common = set(ranks_a) & set(ranks_b)
    if len(common) < 2:
        return 0.0

    n = len(common)
    d_squared = sum((ranks_a[pid] - ranks_b[pid]) ** 2 for pid in common)
    return 1.0 - (6.0 * d_squared) / (n * (n * n - 1))


def cross_validate_scores(
    persons: list[Person],
    anime_list: list[Anime],
    credits: list[Credit],
    n_folds: int = 5,
    holdout_ratio: float = 0.2,
    seed: int = 42,
) -> dict:
    """Evaluate score stability via cross-validation.

    BiRank スコアを使用してランキング安定性を測定する。

    Args:
        persons: 人物リスト
        anime_list: アニメリスト
        credits: クレジットリスト
        n_folds: フォールド数
        holdout_ratio: 除外するクレジットの割合
        seed: ランダムシード

    Returns:
        {
            "n_folds": int,
            "holdout_ratio": float,
            "avg_rank_correlation": float,  # 平均順位相関
            "min_rank_correlation": float,
            "avg_top10_overlap": float,     # Top10の平均重複率
            "fold_results": [{correlation, top10_overlap}],
        }
    """
    rng = random.Random(seed)

    # Full scores as baseline (BiRank person scores)
    full_graph = create_person_anime_network(persons, anime_list, credits)
    full_birank = compute_birank(full_graph).person_scores

    # Rank all persons
    full_ranking = {
        pid: rank
        for rank, (pid, _) in enumerate(
            sorted(full_birank.items(), key=lambda x: -x[1]), 1
        )
    }
    full_top10 = set(sorted(full_birank, key=full_birank.get, reverse=True)[:10])

    fold_results = []
    n_holdout = max(1, int(len(credits) * holdout_ratio))

    for fold in range(n_folds):
        # Random holdout
        shuffled = list(range(len(credits)))
        rng.shuffle(shuffled)
        holdout_indices = set(shuffled[:n_holdout])
        fold_credits = [c for i, c in enumerate(credits) if i not in holdout_indices]

        if not fold_credits:
            continue

        # Recompute scores with held-out data
        fold_graph = create_person_anime_network(persons, anime_list, fold_credits)
        fold_birank = compute_birank(fold_graph).person_scores

        # Rank
        fold_ranking = {
            pid: rank
            for rank, (pid, _) in enumerate(
                sorted(fold_birank.items(), key=lambda x: -x[1]), 1
            )
        }
        fold_top10 = set(sorted(fold_birank, key=fold_birank.get, reverse=True)[:10])

        # Correlation
        correlation = _rank_correlation(full_ranking, fold_ranking)
        top10_overlap = len(full_top10 & fold_top10) / max(len(full_top10), 1)

        fold_results.append(
            {
                "fold": fold + 1,
                "credits_used": len(fold_credits),
                "correlation": round(correlation, 4),
                "top10_overlap": round(top10_overlap, 2),
            }
        )

    correlations = [f["correlation"] for f in fold_results]
    overlaps = [f["top10_overlap"] for f in fold_results]

    result = {
        "n_folds": n_folds,
        "holdout_ratio": holdout_ratio,
        "total_credits": len(credits),
        "avg_rank_correlation": round(sum(correlations) / len(correlations), 4)
        if correlations
        else 0,
        "min_rank_correlation": round(min(correlations), 4) if correlations else 0,
        "avg_top10_overlap": round(sum(overlaps) / len(overlaps), 2) if overlaps else 0,
        "fold_results": fold_results,
    }

    logger.info(
        "cross_validation_complete",
        folds=n_folds,
        avg_correlation=result["avg_rank_correlation"],
        avg_top10=result["avg_top10_overlap"],
    )

    return result
