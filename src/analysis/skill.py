"""OpenSkill ベースの Skill スコア算出.

作品のスコア（MAL/AniList 評点）を「試合結果」と見立て、
各アニメーターのスキルレーティングを算出する。

高評価作品に多く参加しているアニメーターほど高スキルと推定。
直近の作品ほど重視される（OpenSkill の自然な性質）。
"""

import numpy as np
import structlog
from openskill.models import PlackettLuce

from src.models import Anime, Credit, Role

logger = structlog.get_logger()

# スキル評価対象の役職（制作スタッフ全般）
SKILL_ROLES = {
    Role.CHIEF_ANIMATION_DIRECTOR,
    Role.ANIMATION_DIRECTOR,
    Role.KEY_ANIMATOR,
    Role.SECOND_KEY_ANIMATOR,
    Role.CHARACTER_DESIGNER,
    Role.STORYBOARD,
    Role.EPISODE_DIRECTOR,
    Role.ART_DIRECTOR,
    Role.EFFECTS,
    Role.LAYOUT,
}


def compute_skill_scores(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, float]:
    """OpenSkill を用いて Skill スコアを算出する.

    各作品を「試合」として扱い、作品の評点で順位付けする。
    同じ作品に参加したスタッフはチームとして扱う。
    """
    # 作品をスコア順にソート（年代も考慮）
    scored_anime: list[tuple[str, float, int]] = []
    for anime_id, anime in anime_map.items():
        if anime.score and anime.score > 0:
            year = anime.year or 2000
            scored_anime.append((anime_id, anime.score, year))

    scored_anime.sort(key=lambda x: (x[2], x[1]))  # 年代順→スコア順

    if not scored_anime:
        logger.warning("No scored anime found")
        return {}

    # anime_id → [person_id] (対象役職のみ)
    anime_staff: dict[str, list[str]] = {}
    for c in credits:
        if c.role in SKILL_ROLES:
            anime_staff.setdefault(c.anime_id, [])
            if c.person_id not in anime_staff[c.anime_id]:
                anime_staff[c.anime_id].append(c.person_id)

    # OpenSkill モデル初期化
    model = PlackettLuce()

    # 全参加者のレーティング
    all_person_ids: set[str] = set()
    for staff in anime_staff.values():
        all_person_ids.update(staff)

    ratings: dict[str, object] = {
        pid: model.rating() for pid in all_person_ids
    }

    # 年代ごとにバッチ処理（同年の作品をスコア順にランク）
    yearly: dict[int, list[tuple[str, float]]] = {}
    for anime_id, score, year in scored_anime:
        yearly.setdefault(year, []).append((anime_id, score))

    for year in sorted(yearly.keys()):
        anime_in_year = yearly[year]
        # スコア降順でランク
        anime_in_year.sort(key=lambda x: x[1], reverse=True)

        # 各作品のスタッフをチームとして構成
        teams = []
        team_keys: list[list[str]] = []

        for anime_id, _score in anime_in_year:
            staff = anime_staff.get(anime_id, [])
            if not staff:
                continue
            team_ratings = [ratings[pid] for pid in staff]
            teams.append(team_ratings)
            team_keys.append(staff)

        if len(teams) < 2:
            continue

        # ランクは 1-indexed（1位 = 最高スコアの作品チーム）
        ranks = list(range(1, len(teams) + 1))

        try:
            new_ratings = model.rate(teams, ranks=ranks)
            for team_pids, team_new in zip(team_keys, new_ratings):
                for pid, new_r in zip(team_pids, team_new):
                    ratings[pid] = new_r
        except Exception:
            logger.debug("skipping_year_rating_update", year=year)

    # mu を抽出して正規化
    skill_scores: dict[str, float] = {}
    for pid, r in ratings.items():
        skill_scores[pid] = r.mu  # type: ignore[union-attr]

    if skill_scores:
        values = np.array(list(skill_scores.values()))
        min_val = values.min()
        max_val = values.max()
        if max_val > min_val:
            skill_scores = {
                k: float((v - min_val) / (max_val - min_val) * 100.0)
                for k, v in skill_scores.items()
            }
        else:
            skill_scores = {k: 50.0 for k in skill_scores}

    logger.info("skill_scores_computed", persons=len(skill_scores))
    return skill_scores
