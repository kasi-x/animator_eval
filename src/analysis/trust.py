"""継続起用 (Trust) スコア算出 — 累積エッジ重み + 時間減衰.

Trust は「同じ監督/演出家から繰り返し起用されること」を測る。
エッジ重みは共演回数と役職重みに基づき、
最近の起用ほど高く、離脱後は指数関数的に減衰する。
"""

import math
from collections import defaultdict

import numpy as np
import structlog

from src.models import Anime, Credit, Role

logger = structlog.get_logger()

# 減衰パラメータ
DECAY_HALF_LIFE_YEARS = 3.0  # 3年で半減
DECAY_LAMBDA = math.log(2) / DECAY_HALF_LIFE_YEARS

DIRECTOR_ROLES = {
    Role.DIRECTOR,
    Role.EPISODE_DIRECTOR,
    Role.CHIEF_ANIMATION_DIRECTOR,
}


def _compute_time_weight(years_ago: float) -> float:
    """時間減衰重み: exp(-λt)."""
    return math.exp(-DECAY_LAMBDA * max(0, years_ago))


def compute_trust_scores(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    current_year: int = 2026,
) -> dict[str, float]:
    """全人物の Trust スコアを算出する.

    Trust = Σ (各監督からの起用) において:
      - 起用回数が多いほど高い
      - 上位役職ほど高い
      - 最近の起用ほど高い（時間減衰）
    """
    # 監督ノードを特定
    director_credits: dict[str, set[str]] = defaultdict(set)  # person_id → {anime_id}
    animator_credits: dict[str, list[Credit]] = defaultdict(list)

    for c in credits:
        if c.role in DIRECTOR_ROLES:
            director_credits[c.person_id].add(c.anime_id)
        animator_credits[c.person_id].append(c)

    # anime → director mapping
    anime_directors: dict[str, set[str]] = defaultdict(set)
    for dir_id, anime_ids in director_credits.items():
        for anime_id in anime_ids:
            anime_directors[anime_id].add(dir_id)

    # 各人物の Trust スコアを算出
    trust_scores: dict[str, float] = {}

    for person_id, person_credits in animator_credits.items():
        # 監督との共演を集計
        director_engagement: dict[str, list[tuple[float, float]]] = defaultdict(
            list
        )  # director_id → [(weight, years_ago)]

        for c in person_credits:
            anime = anime_map.get(c.anime_id)
            years_ago = (current_year - anime.year) if anime and anime.year else 5.0

            # この作品の監督を特定
            for dir_id in anime_directors.get(c.anime_id, set()):
                if dir_id == person_id:
                    continue  # 自分自身は除外
                role_weight = {
                    Role.ANIMATION_DIRECTOR: 2.5,
                    Role.CHIEF_ANIMATION_DIRECTOR: 2.8,
                    Role.KEY_ANIMATOR: 2.0,
                    Role.SECOND_KEY_ANIMATOR: 1.5,
                    Role.IN_BETWEEN: 1.0,
                    Role.CHARACTER_DESIGNER: 2.3,
                    Role.STORYBOARD: 2.0,
                }.get(c.role, 1.0)

                director_engagement[dir_id].append((role_weight, years_ago))

        # Trust = Σ_directors [ repeat_bonus × Σ_works(role_weight × time_decay) ]
        total_trust = 0.0
        for dir_id, engagements in director_engagement.items():
            n_works = len(engagements)
            # 繰り返し起用ボーナス: log(1 + n) で飽和
            repeat_bonus = math.log1p(n_works)

            weighted_sum = sum(
                w * _compute_time_weight(t) for w, t in engagements
            )
            total_trust += repeat_bonus * weighted_sum

            # 監督自身の著名度ボーナス（監督クレジット数に基づく）
            dir_prominence = len(director_credits.get(dir_id, set()))
            prominence_bonus = math.log1p(dir_prominence) / math.log(10)
            total_trust += weighted_sum * prominence_bonus * 0.3

        trust_scores[person_id] = total_trust

    # 正規化 (0-100)
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
    """特定のアニメーター×監督ペアの起用減衰を検出する.

    直近 window_size 作品での起用率と期待値を比較。
    """
    # 監督の作品を年代順で取得
    director_works = []
    for c in credits:
        if c.person_id == director_id and c.role in DIRECTOR_ROLES:
            anime = anime_map.get(c.anime_id)
            if anime and anime.year:
                director_works.append((anime.year, c.anime_id))

    director_works.sort()

    if len(director_works) < window_size:
        return {"status": "insufficient_data", "works": len(director_works)}

    # 全体の起用率
    animator_anime_ids = {
        c.anime_id for c in credits if c.person_id == person_id
    }
    total_appearances = sum(
        1 for _, aid in director_works if aid in animator_anime_ids
    )
    expected_rate = total_appearances / len(director_works) if director_works else 0

    # 直近 window の起用率
    recent_works = director_works[-window_size:]
    recent_appearances = sum(
        1 for _, aid in recent_works if aid in animator_anime_ids
    )
    recent_rate = recent_appearances / window_size

    return {
        "status": "decayed" if recent_rate < expected_rate * 0.5 else "active",
        "expected_rate": round(expected_rate, 3),
        "recent_rate": round(recent_rate, 3),
        "total_works": len(director_works),
        "total_appearances": total_appearances,
        "recent_appearances": recent_appearances,
    }
