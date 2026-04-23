"""Influence propagation analysis — build mentor-mentee relationships and influence trees.

ディレクターとその下で働いたアニメーターの関係を追跡し、
「誰の門下から誰がディレクターに成長したか」を分析する。

指標:
- 育成率 (nurture_rate): 門下生がディレクターレベルに達した割合
- 影響スコア (influence_score): 門下生の composite スコア加重合計
- 世代チェーン (generation_chains): メンター→メンティー→メンティーの弟子...
"""

from collections import defaultdict

import structlog

from src.analysis.career import CAREER_STAGE
from src.runtime.models import AnimeAnalysis as Anime, Credit
from src.utils.role_groups import (
    DIRECTOR_ROLES as _DIRECTOR_ROLES,
    MENTEE_ROLES as _MENTEE_ROLES,
)

logger = structlog.get_logger()


def _find_mentor_mentee_pairs(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    min_shared_works: int = 2,
) -> dict[str, dict[str, dict]]:
    """Detect mentor-mentee pairs.

    条件:
    - メンター: 同作品でディレクター系の役職
    - メンティー: 同作品でキーアニメーター以下の役職
    - 共同作品が min_shared_works 以上

    Returns:
        {mentor_id: {mentee_id: {shared_works: [...], first_year, last_year}}}
    """
    # Pre-compute anime years for O(1) lookup (PERF-2 optimization)
    anime_years: dict[str, int] = {
        aid: a.year for aid, a in anime_map.items() if a.year
    }

    # Build per-anime role assignments
    anime_directors: dict[str, set[str]] = defaultdict(set)
    anime_staff: dict[str, set[str]] = defaultdict(set)

    for c in credits:
        if c.role in _DIRECTOR_ROLES:
            anime_directors[c.anime_id].add(c.person_id)
        elif c.role in _MENTEE_ROLES:
            anime_staff[c.anime_id].add(c.person_id)

    # Find co-occurrences
    pairs: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for anime_id, directors in anime_directors.items():
        staff = anime_staff.get(anime_id, set())
        for dir_id in directors:
            for staff_id in staff:
                if dir_id != staff_id:
                    pairs[dir_id][staff_id].append(anime_id)

    # Filter by min_shared_works and add year info
    result: dict[str, dict[str, dict]] = {}
    for mentor_id, mentees in pairs.items():
        mentor_mentees = {}
        for mentee_id, shared_anime_ids in mentees.items():
            if len(shared_anime_ids) < min_shared_works:
                continue
            # O(1) lookup per anime instead of O(n) scan (PERF-2 optimization)
            years = [anime_years[aid] for aid in shared_anime_ids if aid in anime_years]
            mentor_mentees[mentee_id] = {
                "shared_works": shared_anime_ids,
                "shared_count": len(shared_anime_ids),
                "first_year": min(years) if years else None,
                "last_year": max(years) if years else None,
            }
        if mentor_mentees:
            result[mentor_id] = mentor_mentees

    return result


def _get_highest_stage(person_id: str, credits: list[Credit]) -> int:
    """Return the highest career stage of a person."""
    stages = [CAREER_STAGE.get(c.role, 0) for c in credits if c.person_id == person_id]
    return max(stages) if stages else 0


def compute_influence_tree(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
    min_shared_works: int = 2,
) -> dict:
    """Build an influence tree.

    Args:
        credits: クレジットリスト
        anime_map: {anime_id: Anime}
        person_scores: {person_id: composite_score}
        min_shared_works: メンター認定に必要な共同作品数

    Returns:
        {
            "mentors": {
                mentor_id: {
                    "mentee_count": int,
                    "nurture_rate": float,   # メンティーがディレクターに達した割合
                    "influence_score": float, # メンティーのスコア合計
                    "mentees": [{mentee_id, shared_count, reached_director, score}],
                }
            },
            "generation_chains": [
                [mentor_id, mentee_id, mentee_of_mentee_id, ...]
            ],
            "total_mentors": int,
            "total_mentees": int,
            "avg_nurture_rate": float,
        }
    """
    # Pre-compute highest career stage for each person (PERF-2 optimization)
    # This eliminates O(mentees × all_credits) complexity
    person_highest_stage: dict[str, int] = {}
    for c in credits:
        stage = CAREER_STAGE.get(c.role, 0)
        if stage > person_highest_stage.get(c.person_id, 0):
            person_highest_stage[c.person_id] = stage

    mentor_pairs = _find_mentor_mentee_pairs(credits, anime_map, min_shared_works)

    if not mentor_pairs:
        return {
            "mentors": {},
            "generation_chains": [],
            "total_mentors": 0,
            "total_mentees": 0,
            "avg_nurture_rate": 0.0,
        }

    # Compute per-mentor stats
    mentors_data: dict[str, dict] = {}
    all_mentees: set[str] = set()

    for mentor_id, mentees in mentor_pairs.items():
        mentee_list = []
        director_count = 0

        for mentee_id, info in mentees.items():
            all_mentees.add(mentee_id)
            # O(1) lookup instead of O(n) scan (PERF-2 optimization)
            highest = person_highest_stage.get(mentee_id, 0)
            reached_director = highest >= 5  # chief AD or above
            if reached_director:
                director_count += 1

            mentee_entry = {
                "mentee_id": mentee_id,
                "shared_count": info["shared_count"],
                "first_year": info["first_year"],
                "last_year": info["last_year"],
                "highest_stage": highest,
                "reached_director": reached_director,
            }
            if person_scores and mentee_id in person_scores:
                mentee_entry["score"] = round(person_scores[mentee_id], 2)
            mentee_list.append(mentee_entry)

        mentee_list.sort(key=lambda x: x["shared_count"], reverse=True)

        nurture_rate = director_count / len(mentees) * 100 if mentees else 0.0
        influence = 0.0
        if person_scores:
            influence = sum(person_scores.get(m["mentee_id"], 0) for m in mentee_list)

        mentors_data[mentor_id] = {
            "mentee_count": len(mentees),
            "nurture_rate": round(nurture_rate, 1),
            "influence_score": round(influence, 2),
            "mentees": mentee_list,
        }

    # Build generation chains: find mentor→mentee lineages
    # Only follow edges where mentee also became a mentor (grew into leadership)
    mentor_set = set(mentor_pairs.keys())
    root_mentors = sorted(mentor_set - all_mentees) or sorted(mentor_set)

    final_chains: list[list[str]] = []
    visited_global: set[str] = set()
    for root in root_mentors:
        if root in visited_global:
            continue
        # Greedy longest chain: always pick the mentee with most shared works
        chain = [root]
        visited_global.add(root)
        current = root
        while current in mentor_pairs:
            candidates = [
                (mid, mentor_pairs[current][mid]["shared_count"])
                for mid in mentor_pairs[current]
                if mid in mentor_set and mid not in visited_global
            ]
            if not candidates:
                break
            best = max(candidates, key=lambda x: x[1])
            chain.append(best[0])
            visited_global.add(best[0])
            current = best[0]
        if len(chain) >= 2:
            final_chains.append(chain)

    final_chains.sort(key=len, reverse=True)

    # Summary
    nurture_rates = [m["nurture_rate"] for m in mentors_data.values()]
    avg_nurture = sum(nurture_rates) / len(nurture_rates) if nurture_rates else 0.0

    result = {
        "mentors": mentors_data,
        "generation_chains": final_chains,
        "total_mentors": len(mentors_data),
        "total_mentees": len(all_mentees),
        "avg_nurture_rate": round(avg_nurture, 1),
    }

    logger.info(
        "influence_tree_complete",
        mentors=result["total_mentors"],
        mentees=result["total_mentees"],
        chains=len(final_chains),
        avg_nurture=result["avg_nurture_rate"],
    )

    return result
