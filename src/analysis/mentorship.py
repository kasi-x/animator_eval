"""メンターシップ推定 — 師弟関係の自動推定.

上位役職者（監督・作画監督）と下位役職者（原画・動画）が同一作品に
繰り返し参加するパターンから、メンター-メンティー関係を推定する。
"""

from collections import defaultdict

import structlog

from src.analysis.career import CAREER_STAGE
from src.models import Anime, Credit

logger = structlog.get_logger()


def infer_mentorships(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    min_shared_works: int = 3,
    min_stage_gap: int = 2,
) -> list[dict]:
    """メンター-メンティー関係を推定する.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime} マッピング
        min_shared_works: 最低共演作品数
        min_stage_gap: 最低役職ステージ差

    Returns:
        推定メンターシップのリスト
    """
    if not credits:
        return []

    # Group credits by anime
    anime_credits: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        anime_credits[c.anime_id].append(c)

    # For each person, compute their typical stage (mode)
    person_stages: dict[str, list[int]] = defaultdict(list)
    person_years: dict[str, list[int]] = defaultdict(list)
    for c in credits:
        stage = CAREER_STAGE.get(c.role, 0)
        if stage > 0:
            person_stages[c.person_id].append(stage)
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            person_years[c.person_id].append(anime.year)

    # Compute modal stage for each person
    person_modal_stage: dict[str, int] = {}
    for pid, stages in person_stages.items():
        if stages:
            person_modal_stage[pid] = max(set(stages), key=stages.count)

    # Find potential mentor-mentee pairs (shared works with stage gap)
    pair_shared: dict[tuple[str, str], list[str]] = defaultdict(list)
    for anime_id, creds in anime_credits.items():
        persons = [(c.person_id, CAREER_STAGE.get(c.role, 0)) for c in creds]
        for i, (p1, s1) in enumerate(persons):
            for p2, s2 in persons[i + 1 :]:
                if s1 == 0 or s2 == 0:
                    continue
                if s1 > s2 and s1 - s2 >= min_stage_gap:
                    pair_shared[(p1, p2)].append(anime_id)
                elif s2 > s1 and s2 - s1 >= min_stage_gap:
                    pair_shared[(p2, p1)].append(anime_id)

    # Filter by minimum shared works
    mentorships = []
    for (mentor_id, mentee_id), anime_ids in pair_shared.items():
        if len(anime_ids) < min_shared_works:
            continue

        # Calculate mentorship span
        years = []
        for aid in anime_ids:
            anime = anime_map.get(aid)
            if anime and anime.year:
                years.append(anime.year)

        mentor_stage = person_modal_stage.get(mentor_id, 0)
        mentee_stage = person_modal_stage.get(mentee_id, 0)

        mentorships.append(
            {
                "mentor_id": mentor_id,
                "mentee_id": mentee_id,
                "shared_works": len(anime_ids),
                "shared_anime_ids": sorted(set(anime_ids)),
                "mentor_stage": mentor_stage,
                "mentee_stage": mentee_stage,
                "stage_gap": mentor_stage - mentee_stage,
                "year_span": (min(years), max(years)) if years else None,
                "confidence": _compute_confidence(
                    len(anime_ids),
                    mentor_stage - mentee_stage,
                    max(years) - min(years) + 1 if years else 0,
                ),
            }
        )

    # Sort by confidence
    mentorships.sort(key=lambda m: -m["confidence"])

    logger.info("mentorships_inferred", total=len(mentorships))
    return mentorships


def _compute_confidence(shared_works: int, stage_gap: int, year_span: int) -> float:
    """メンターシップの信頼度 (0-100)."""
    # More shared works = higher confidence
    work_score = min(40, shared_works * 8)
    # Larger stage gap = more likely real mentorship
    gap_score = min(30, stage_gap * 10)
    # Longer time span = more sustained relationship (B16 fix: span, not count)
    span_score = min(30, year_span * 6)
    return min(100, work_score + gap_score + span_score)


def build_mentorship_tree(mentorships: list[dict]) -> dict:
    """メンターシップの木構造を構築する.

    Returns:
        {mentor_id: [mentee_ids], "roots": [top-level mentor IDs]}
    """
    if not mentorships:
        return {"tree": {}, "roots": []}

    children: dict[str, list[str]] = defaultdict(list)
    all_mentors: set[str] = set()
    all_mentees: set[str] = set()

    for m in mentorships:
        children[m["mentor_id"]].append(m["mentee_id"])
        all_mentors.add(m["mentor_id"])
        all_mentees.add(m["mentee_id"])

    # Roots are mentors who are not mentees of anyone
    roots = sorted(all_mentors - all_mentees)

    return {
        "tree": dict(children),
        "roots": roots,
    }
