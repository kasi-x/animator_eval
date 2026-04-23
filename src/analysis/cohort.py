"""Generational cohort analysis — compare career patterns by debut period.

アニメーターをデビュー年代（2000s, 2010s, 2020s）でグループ化し、
世代ごとの特徴を分析する:
- キャリア進行速度
- 各ステージでの典型的な役職
- スコア分布
"""

from collections import defaultdict

import structlog

from src.analysis.career import CAREER_STAGE
from src.runtime.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


def _get_decade(year: int) -> str:
    """Convert a year to a decade label string."""
    decade = (year // 10) * 10
    return f"{decade}s"


def compute_cohort_analysis(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, float] | None = None,
) -> dict:
    """Run generational cohort analysis.

    Args:
        credits: クレジットリスト
        anime_map: アニメマップ
        person_scores: {person_id: composite_score}

    Returns:
        {
            "cohorts": {decade: {size, avg_score, avg_career_span, ...}},
            "stage_by_cohort": {decade: {stage: count}},
            "total_persons": int,
        }
    """
    # Determine each person's debut year (first credited year)
    person_debut: dict[str, int] = {}
    person_years: dict[str, set[int]] = defaultdict(set)
    person_stages: dict[str, list[tuple[int, int]]] = defaultdict(list)

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        pid = c.person_id
        year = anime.year
        person_years[pid].add(year)
        if pid not in person_debut or year < person_debut[pid]:
            person_debut[pid] = year

        stage = CAREER_STAGE.get(c.role, 0)
        if stage > 0:
            person_stages[pid].append((year, stage))

    if not person_debut:
        return {"cohorts": {}, "stage_by_cohort": {}, "total_persons": 0}

    # Group by debut decade
    cohort_members: dict[str, list[str]] = defaultdict(list)
    for pid, debut in person_debut.items():
        decade = _get_decade(debut)
        cohort_members[decade].append(pid)

    # Compute per-cohort statistics
    cohorts = {}
    stage_by_cohort: dict[str, dict[int, int]] = {}

    for decade in sorted(cohort_members.keys()):
        members = cohort_members[decade]
        scores = []
        career_spans = []
        max_stages = []
        stage_counts: dict[int, int] = defaultdict(int)

        for pid in members:
            years = person_years.get(pid, set())
            if years:
                career_spans.append(max(years) - min(years) + 1)

            if person_scores and pid in person_scores:
                scores.append(person_scores[pid])

            # Max stage achieved
            stages = person_stages.get(pid, [])
            if stages:
                max_stage = max(s for _, s in stages)
                max_stages.append(max_stage)
                stage_counts[max_stage] += 1

        cohort_data: dict = {
            "size": len(members),
            "debut_decade": decade,
        }

        if career_spans:
            cohort_data["avg_career_span"] = round(
                sum(career_spans) / len(career_spans), 1
            )
            cohort_data["max_career_span"] = max(career_spans)

        if scores:
            cohort_data["avg_score"] = round(sum(scores) / len(scores), 1)
            cohort_data["median_score"] = round(sorted(scores)[len(scores) // 2], 1)

        if max_stages:
            cohort_data["avg_max_stage"] = round(sum(max_stages) / len(max_stages), 1)
            # Percentage who reached director level (stage 6)
            director_count = sum(1 for s in max_stages if s >= 6)
            cohort_data["director_rate"] = round(
                director_count / len(max_stages) * 100, 1
            )

        cohorts[decade] = cohort_data
        stage_by_cohort[decade] = dict(stage_counts)

    logger.info(
        "cohort_analysis_complete",
        cohorts=len(cohorts),
        total_persons=len(person_debut),
    )

    return {
        "cohorts": cohorts,
        "stage_by_cohort": stage_by_cohort,
        "total_persons": len(person_debut),
    }
