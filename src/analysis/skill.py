"""OpenSkill ベースの Skill スコア算出.

作品のスコア（MAL/AniList 評点）を「試合結果」と見立て、
各アニメーターのスキルレーティングを算出する。

高評価作品に多く参加しているアニメーターほど高スキルと推定。
直近の作品ほど重視される（OpenSkill の自然な性質）。
"""

from typing import NamedTuple

import numpy as np
import structlog
from openskill.models import PlackettLuce

from src.models import Anime, Credit, Role

logger = structlog.get_logger()


class ScoredAnimeRecord(NamedTuple):
    """評点付き作品の記録.

    Records an anime with its score and year for skill rating calculation.
    """

    anime_id: str
    score: float
    year: int

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
    anime_by_year_and_score: list[ScoredAnimeRecord] = []
    for anime_id, anime in anime_map.items():
        if anime.score and anime.score > 0:
            year = anime.year or 2000
            record = ScoredAnimeRecord(anime_id=anime_id, score=anime.score, year=year)
            anime_by_year_and_score.append(record)

    anime_by_year_and_score.sort(key=lambda record: (record.year, record.score))  # 年代順→スコア順

    if not anime_by_year_and_score:
        logger.warning("No scored anime found")
        return {}

    # anime_id → [person_id] (対象役職のみ)
    staff_by_anime: dict[str, list[str]] = {}
    for credit in credits:
        if credit.role in SKILL_ROLES:
            staff_by_anime.setdefault(credit.anime_id, [])
            if credit.person_id not in staff_by_anime[credit.anime_id]:
                staff_by_anime[credit.anime_id].append(credit.person_id)

    # OpenSkill モデル初期化
    model = PlackettLuce()

    # 全参加者のレーティング
    all_staff_member_ids: set[str] = set()
    for staff_list in staff_by_anime.values():
        all_staff_member_ids.update(staff_list)

    person_ratings: dict[str, object] = {
        person_id: model.rating() for person_id in all_staff_member_ids
    }

    # 年代ごとにバッチ処理（同年の作品をスコア順にランク）
    anime_records_by_year: dict[int, list[ScoredAnimeRecord]] = {}
    for anime_record in anime_by_year_and_score:
        anime_records_by_year.setdefault(anime_record.year, []).append(anime_record)

    for year in sorted(anime_records_by_year.keys()):
        yearly_anime_records = anime_records_by_year[year]
        # スコア降順でランク
        yearly_anime_records.sort(key=lambda record: record.score, reverse=True)

        # 各作品のスタッフをチームとして構成
        team_rating_objects = []
        team_member_ids: list[list[str]] = []

        for anime_record in yearly_anime_records:
            staff_members = staff_by_anime.get(anime_record.anime_id, [])
            if not staff_members:
                continue
            team_ratings = [person_ratings[person_id] for person_id in staff_members]
            team_rating_objects.append(team_ratings)
            team_member_ids.append(staff_members)

        if len(team_rating_objects) < 2:
            continue

        # ランクは 1-indexed（1位 = 最高スコアの作品チーム）
        competition_ranks = list(range(1, len(team_rating_objects) + 1))

        try:
            updated_team_ratings = model.rate(team_rating_objects, ranks=competition_ranks)
            for team_person_ids, updated_ratings in zip(team_member_ids, updated_team_ratings):
                for person_id, updated_rating in zip(team_person_ids, updated_ratings):
                    person_ratings[person_id] = updated_rating
        except Exception:
            logger.debug("skipping_year_rating_update", year=year)

    # mu を抽出して正規化
    skill_scores: dict[str, float] = {}
    for person_id, rating_object in person_ratings.items():
        skill_scores[person_id] = rating_object.mu  # type: ignore[union-attr]

    if skill_scores:
        score_values = np.array(list(skill_scores.values()))
        minimum_score = score_values.min()
        maximum_score = score_values.max()
        if maximum_score > minimum_score:
            skill_scores = {
                person_id: float((score - minimum_score) / (maximum_score - minimum_score) * 100.0)
                for person_id, score in skill_scores.items()
            }
        else:
            skill_scores = {person_id: 50.0 for person_id in skill_scores}

    logger.info("skill_scores_computed", persons=len(skill_scores))
    return skill_scores
