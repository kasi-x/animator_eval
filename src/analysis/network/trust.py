"""Trust score computation — cumulative edge weight + time decay.

Trust は「同じ監督/演出家から繰り返し起用されること」を測る。
エッジ重みは共演回数と役職重みに基づき、
最近の起用ほど高く、離脱後は指数関数的に減衰する。
"""

import functools
import math
from collections import defaultdict
from typing import NamedTuple

import numpy as np
import structlog

from src.models import AnimeAnalysis as Anime, Credit, Role
from src.utils.config import ROLE_WEIGHTS
from src.utils.role_groups import DIRECTOR_ROLES, NON_PRODUCTION_ROLES

logger = structlog.get_logger()


class DirectorEngagementRecord(NamedTuple):
    """Co-credit record with a director — represents role importance and elapsed time."""

    role_importance_weight: (
        float  # 役職の重み (animation_director=2.5, key_animator=1.0, etc.)
    )
    years_since_collaboration: float  # 共演からの経過年数


# decay parameters
DECAY_HALF_LIFE_YEARS = 3.0  # 3年で半減
DECAY_LAMBDA = math.log(2) / DECAY_HALF_LIFE_YEARS


def _role_importance(role: Role) -> float:
    """Dynamic role importance from ROLE_WEIGHTS (single source of truth)."""
    return ROLE_WEIGHTS.get(role.value, 1.0)


@functools.lru_cache(maxsize=100)
def _compute_time_weight_cached(years_ago: int) -> float:
    """Time-decay weight: exp(-λt) (cached for repeated lookups)."""
    return math.exp(-DECAY_LAMBDA * max(0, years_ago))


def _compute_time_weight(years_ago: float) -> float:
    """Time-decay weight: exp(-λt)."""
    # Use cached version for integer years (common case)
    if years_ago == int(years_ago):
        return _compute_time_weight_cached(int(years_ago))
    return math.exp(-DECAY_LAMBDA * max(0, years_ago))


def compute_trust_scores(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    current_year: int = 2026,
) -> dict[str, float]:
    """Compute Trust scores for all persons.

    Trust = Σ (各監督からの起用) において:
      - 起用回数が多いほど高い
      - 上位役職ほど高い
      - 最近の起用ほど高い（時間減衰）
    """
    # identify director nodes
    director_credits: dict[str, set[str]] = defaultdict(set)  # person_id → {anime_id}
    animator_credits: dict[str, list[Credit]] = defaultdict(list)

    for c in credits:
        if c.role in NON_PRODUCTION_ROLES:
            continue
        if c.role in DIRECTOR_ROLES:
            director_credits[c.person_id].add(c.anime_id)
        animator_credits[c.person_id].append(c)

    # anime → director mapping
    anime_directors: dict[str, set[str]] = defaultdict(set)
    for dir_id, anime_ids in director_credits.items():
        for anime_id in anime_ids:
            anime_directors[anime_id].add(dir_id)

    # compute Trust score for each person
    # Pre-compute anime years and director work counts (avoid repeated lookups)
    anime_years: dict[str, int] = {
        aid: a.year if a.year else current_year - 5 for aid, a in anime_map.items()
    }

    director_work_counts: dict[str, int] = {
        dir_id: len(anime_ids) for dir_id, anime_ids in director_credits.items()
    }

    trust_scores: dict[str, float] = {}

    for person_id, person_credits in animator_credits.items():
        # aggregate co-credits with each director
        collaborations_with_each_director: dict[str, list[DirectorEngagementRecord]] = (
            defaultdict(list)
        )

        for c in person_credits:
            years_since_this_work = current_year - anime_years.get(
                c.anime_id, current_year - 5
            )

            # identify the director(s) of this work
            for dir_id in anime_directors.get(c.anime_id, set()):
                if dir_id == person_id:
                    continue  # 自分自身は除外

                role_importance = _role_importance(c.role)

                collaborations_with_each_director[dir_id].append(
                    DirectorEngagementRecord(
                        role_importance_weight=role_importance,
                        years_since_collaboration=years_since_this_work,
                    )
                )

        # Trust = Σ_directors [ repeat_bonus × Σ_works(role_weight × time_decay) ]
        total_trust_score = 0.0
        for (
            dir_id,
            all_collaborations_with_this_director,
        ) in collaborations_with_each_director.items():
            how_many_times_worked_together = len(all_collaborations_with_this_director)
            # repeat engagement bonus: saturates at log(1 + n)
            repeat_engagement_boost = math.log1p(how_many_times_worked_together)

            weighted_collaboration_sum = sum(
                collab.role_importance_weight
                * _compute_time_weight(collab.years_since_collaboration)
                for collab in all_collaborations_with_this_director
            )
            total_trust_score += repeat_engagement_boost * weighted_collaboration_sum

            # director prominence bonus (based on director credit count, pre-computed)
            how_many_works_director_has_directed = director_work_counts.get(dir_id, 0)
            director_prominence_multiplier = math.log1p(
                how_many_works_director_has_directed
            ) / math.log(10)
            total_trust_score += (
                weighted_collaboration_sum * director_prominence_multiplier * 0.3
            )

        trust_scores[person_id] = total_trust_score

    # normalise (0-100)
    if trust_scores:
        values = np.array(list(trust_scores.values()))
        min_val = values.min()
        max_val = values.max()
        if max_val > min_val:
            trust_scores = {
                k: float((v - min_val) / (max_val - min_val) * 100.0)
                for k, v in trust_scores.items()
            }
        else:
            trust_scores = {k: 50.0 for k in trust_scores}

    logger.info("trust_scores_computed", persons=len(trust_scores))
    return trust_scores


def detect_engagement_decay(
    person_id: str,
    director_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    window_size: int = 5,
) -> dict:
    """Detect engagement decay for a specific animator × director pair.

    直近 window_size 作品での起用率と期待値を比較。
    """
    # get director's works in chronological order
    director_works = []
    for c in credits:
        if c.person_id == director_id and c.role in DIRECTOR_ROLES:
            anime = anime_map.get(c.anime_id)
            if anime and anime.year:
                director_works.append((anime.year, c.anime_id))

    director_works.sort()

    if len(director_works) < window_size:
        return {"status": "insufficient_data", "works": len(director_works)}

    # overall engagement rate
    animator_anime_ids = {c.anime_id for c in credits if c.person_id == person_id}
    total_appearances = sum(1 for _, aid in director_works if aid in animator_anime_ids)
    expected_rate = total_appearances / len(director_works) if director_works else 0

    # recent-window engagement rate
    recent_works = director_works[-window_size:]
    recent_appearances = sum(1 for _, aid in recent_works if aid in animator_anime_ids)
    recent_rate = recent_appearances / window_size

    return {
        "status": "decayed" if recent_rate < expected_rate * 0.5 else "active",
        "expected_rate": round(expected_rate, 3),
        "recent_rate": round(recent_rate, 3),
        "total_works": len(director_works),
        "total_appearances": total_appearances,
        "recent_appearances": recent_appearances,
    }


def batch_detect_engagement_decay(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    window_size: int = 5,
) -> dict[str, list[dict]]:
    """Batch-detect engagement decay for all animator × director pairs.

    最適化版: credits を1回スキャンして事前集計し、
    共演のあるペアのみを検査する。

    Returns:
        {person_id: [{"director_id": ..., "status": "decayed", ...}, ...]}
    """
    # Step 1: 事前集計（credits を1回だけスキャン）
    director_works: dict[str, list[tuple[int, str]]] = defaultdict(list)
    person_anime: dict[str, set[str]] = defaultdict(set)
    director_ids: set[str] = set()

    for c in credits:
        person_anime[c.person_id].add(c.anime_id)
        if c.role in DIRECTOR_ROLES:
            director_ids.add(c.person_id)
            anime = anime_map.get(c.anime_id)
            if anime and anime.year:
                director_works[c.person_id].append((anime.year, c.anime_id))

    # Sort director works by year
    for dir_id in director_works:
        director_works[dir_id].sort()

    # Step 2: 監督ごとにコラボレーターを特定（共演のあるペアのみ）
    director_anime_sets: dict[str, set[str]] = {
        dir_id: {aid for _, aid in works} for dir_id, works in director_works.items()
    }

    # Step 3: 共演ペアのみ検査
    decay_results: dict[str, list[dict]] = {}
    pairs_checked = 0

    for dir_id, works in director_works.items():
        if len(works) < window_size:
            continue

        dir_anime = director_anime_sets[dir_id]
        recent_works = works[-window_size:]

        for pid, p_anime in person_anime.items():
            if pid in director_ids:
                continue

            # skip if no co-credits
            shared = p_anime & dir_anime
            if not shared:
                continue

            pairs_checked += 1

            # overall engagement rate
            total_appearances = len(shared)
            expected_rate = total_appearances / len(works)

            # recent-window engagement rate
            recent_appearances = sum(1 for _, aid in recent_works if aid in p_anime)
            recent_rate = recent_appearances / window_size

            if recent_rate < expected_rate * 0.5:
                decay_results.setdefault(pid, []).append(
                    {
                        "director_id": dir_id,
                        "status": "decayed",
                        "expected_rate": round(expected_rate, 3),
                        "recent_rate": round(recent_rate, 3),
                        "total_works": len(works),
                        "total_appearances": total_appearances,
                        "recent_appearances": recent_appearances,
                    }
                )

    logger.info(
        "engagement_decay_batch_complete",
        directors=len(director_works),
        pairs_checked=pairs_checked,
        persons_with_decay=len(decay_results),
    )
    return decay_results
