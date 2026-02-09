"""スコア説明 — なぜそのスコアになったかを説明する.

各軸のスコアに対して、主要な寄与要因を抽出する。
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit
from src.utils.role_groups import DIRECTOR_ROLES

logger = structlog.get_logger()


def explain_authority(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> list[dict]:
    """Authority スコアの主要寄与要因を返す.

    高評価/高プロフィール作品への参加が Authority に寄与する。
    """
    person_credits = [c for c in credits if c.person_id == person_id]
    if not person_credits:
        return []

    works = []
    for c in person_credits:
        anime = anime_map.get(c.anime_id)
        if anime:
            works.append({
                "anime_id": anime.id,
                "title": anime.title_ja or anime.title_en or anime.id,
                "year": anime.year,
                "score": anime.score or 0,
                "role": c.role.value,
            })

    # 高スコア作品順にソート
    works.sort(key=lambda w: w["score"], reverse=True)
    return works[:10]


def explain_trust(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> list[dict]:
    """Trust スコアの主要寄与要因を返す.

    同じ監督との繰り返し共演が Trust に寄与する。
    """
    person_credits = [c for c in credits if c.person_id == person_id]
    if not person_credits:
        return []

    # 監督IDを特定
    anime_directors: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        if c.role in DIRECTOR_ROLES:
            anime_directors[c.anime_id].add(c.person_id)

    # 各監督との共演回数を数える
    director_collabs: dict[str, list[str]] = defaultdict(list)
    for c in person_credits:
        for dir_id in anime_directors.get(c.anime_id, set()):
            if dir_id != person_id:
                anime = anime_map.get(c.anime_id)
                title = (anime.title_ja or anime.title_en or c.anime_id) if anime else c.anime_id
                director_collabs[dir_id].append(title)

    result = []
    for dir_id, works in sorted(director_collabs.items(), key=lambda x: len(x[1]), reverse=True):
        if len(works) >= 1:
            result.append({
                "director_id": dir_id,
                "shared_works": len(works),
                "works": works[:5],
            })

    return result[:10]


def explain_skill(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> list[dict]:
    """Skill スコアの主要寄与要因を返す.

    高評価作品への参加が Skill レーティングに影響する。
    年代順にソートして、最近の実績を強調する。
    """
    person_credits = [c for c in credits if c.person_id == person_id]
    if not person_credits:
        return []

    works = []
    seen = set()
    for c in person_credits:
        if c.anime_id in seen:
            continue
        seen.add(c.anime_id)
        anime = anime_map.get(c.anime_id)
        if anime and anime.score:
            works.append({
                "anime_id": anime.id,
                "title": anime.title_ja or anime.title_en or anime.id,
                "year": anime.year,
                "score": anime.score,
            })

    # 最新順
    works.sort(key=lambda w: w["year"] or 0, reverse=True)
    return works[:10]
