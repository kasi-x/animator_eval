"""スタジオ分析 — スタジオごとの人材評価傾向を分析する.

スタジオとスタッフの関係を分析し、
スタジオ間での評価・待遇差の可視化に活用する。
"""

from collections import defaultdict

import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


def compute_studio_analysis(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
) -> dict[str, dict]:
    """スタジオごとの分析を実行する.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime}
        person_scores: {person_id: composite_score}

    Returns:
        {studio_name: {
            "anime_count": int,
            "person_count": int,
            "credit_count": int,
            "avg_score": float | None,
            "avg_person_score": float | None,
            "top_persons": [{person_id, credit_count, score}],
            "anime_titles": [str],
            "year_range": [min_year, max_year],
        }}
    """
    # Map anime to studios
    studio_anime: dict[str, set[str]] = defaultdict(set)
    studio_credits: dict[str, list[Credit]] = defaultdict(list)

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.studio:
            continue
        studio_anime[anime.studio].add(c.anime_id)
        studio_credits[anime.studio].append(c)

    if not studio_anime:
        return {}

    results = {}
    for studio, anime_ids in studio_anime.items():
        creds = studio_credits[studio]
        persons = {c.person_id for c in creds}

        # Anime scores
        anime_scores = []
        years = []
        titles = []
        for aid in anime_ids:
            a = anime_map[aid]
            if a.score:
                anime_scores.append(a.score)
            if a.year:
                years.append(a.year)
            titles.append(a.display_title)

        # Person scores
        p_scores = []
        if person_scores:
            p_scores = [person_scores[pid] for pid in persons if pid in person_scores]

        # Top persons by credit count at this studio
        person_credit_count: dict[str, int] = defaultdict(int)
        for c in creds:
            person_credit_count[c.person_id] += 1

        top_persons = sorted(
            person_credit_count.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:10]

        top_person_list = []
        for pid, cnt in top_persons:
            entry = {"person_id": pid, "credit_count": cnt}
            if person_scores and pid in person_scores:
                entry["score"] = round(person_scores[pid], 2)
            top_person_list.append(entry)

        studio_data = {
            "anime_count": len(anime_ids),
            "person_count": len(persons),
            "credit_count": len(creds),
            "top_persons": top_person_list,
            "anime_titles": sorted(titles),
        }

        if anime_scores:
            studio_data["avg_anime_score"] = round(
                sum(anime_scores) / len(anime_scores), 2
            )
        if p_scores:
            studio_data["avg_person_score"] = round(sum(p_scores) / len(p_scores), 2)
        if years:
            studio_data["year_range"] = [min(years), max(years)]

        results[studio] = studio_data

    logger.info("studio_analysis_complete", studios=len(results))
    return results
