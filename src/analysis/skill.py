"""OpenSkill ベースの Skill スコア算出.

制作規模（スタッフ数）を「試合結果」と見立て、
各アニメーターのスキルレーティングを算出する。

大規模制作に起用される人ほど高スキルと推定。
直近の作品ほど重視される（OpenSkill の自然な性質）。

Note: anime.score (視聴者評価) は意図的に使用しない。
制作スタッフの貢献度と無関係な要因に左右されるため。
"""

from collections import defaultdict
from typing import NamedTuple

import numpy as np
import structlog
from openskill.models import PlackettLuce

from src.models import Anime, Credit
from src.utils.role_groups import SKILL_EVALUATED_ROLES as SKILL_ROLES

logger = structlog.get_logger()


class ScoredAnimeRecord(NamedTuple):
    """制作規模付き作品の記録.

    Records an anime with its staff count and year for skill rating calculation.
    """

    anime_id: str
    score: float  # production scale metric (staff count)
    year: int


def compute_skill_scores(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict[str, float]:
    """OpenSkill を用いて Skill スコアを算出する.

    各作品を「試合」として扱い、スタッフ数（制作規模）で順位付けする。
    同じ作品に参加したスタッフはチームとして扱う。
    """
    # Precompute staff count per anime
    anime_staff_count: dict[str, int] = defaultdict(int)
    seen_pa: set[tuple[str, str]] = set()
    for c in credits:
        key = (c.person_id, c.anime_id)
        if key not in seen_pa:
            seen_pa.add(key)
            anime_staff_count[c.anime_id] += 1

    # Build records: use staff count as the ranking metric
    anime_by_year_and_scale: list[ScoredAnimeRecord] = []
    for anime_id, anime in anime_map.items():
        staff_cnt = anime_staff_count.get(anime_id, 0)
        if staff_cnt == 0:
            continue
        year = anime.year or 2000
        record = ScoredAnimeRecord(anime_id=anime_id, score=float(staff_cnt), year=year)
        anime_by_year_and_scale.append(record)

    anime_by_year_and_scale.sort(
        key=lambda record: (record.year, record.score)
    )  # 年代順→規模順

    if not anime_by_year_and_scale:
        logger.warning("No anime with staff found")
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

    # 年代ごとにバッチ処理（同年の作品をスタッフ数順にランク）
    anime_records_by_year: dict[int, list[ScoredAnimeRecord]] = {}
    for anime_record in anime_by_year_and_scale:
        anime_records_by_year.setdefault(anime_record.year, []).append(anime_record)

    for year in sorted(anime_records_by_year.keys()):
        yearly_anime_records = anime_records_by_year[year]
        # スタッフ数降順でランク（大規模制作 = 上位）
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

        # ランクは 1-indexed（1位 = 最大規模の作品チーム）
        competition_ranks = list(range(1, len(team_rating_objects) + 1))

        try:
            updated_team_ratings = model.rate(
                team_rating_objects, ranks=competition_ranks
            )
            for team_person_ids, updated_ratings in zip(
                team_member_ids, updated_team_ratings
            ):
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
                person_id: float(
                    (score - minimum_score) / (maximum_score - minimum_score) * 100.0
                )
                for person_id, score in skill_scores.items()
            }
        else:
            skill_scores = {person_id: 50.0 for person_id in skill_scores}

    logger.info("skill_scores_computed", persons=len(skill_scores))
    return skill_scores
