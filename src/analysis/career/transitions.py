"""Role transition analysis — analyse career paths of animators.

業界全体の役職遷移パターンを算出:
- 遷移確率（動画→原画、原画→作画監督、etc.）
- 平均遷移年数
- 最頻出キャリアパス
"""

from collections import defaultdict
from dataclasses import dataclass
from typing import NamedTuple

import structlog

from src.analysis.career import CAREER_STAGE
from src.runtime.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()

# Stage → readable label
STAGE_LABELS = {
    1: "In-Between",
    2: "2nd Key/Layout",
    3: "Key Animator",
    4: "Anim. Director",
    5: "Chief AD/Ep.Dir",
    6: "Director",
}


class YearlyStageRecord(NamedTuple):
    """Per-year role stage record.

    Records the career stage for a given year.
    """

    year: int
    stage: int


@dataclass(frozen=True)
class TransitionStatistics:
    """Role transition statistics.

    Statistics about role transitions between career stages.
    """

    from_stage: int
    from_label: str
    to_stage: int
    to_label: str
    count: int
    avg_years: float


@dataclass(frozen=True)
class CareerPathRecord:
    """Career path record.

    Records a specific career progression path and its frequency.
    """

    path: list[int]
    path_labels: list[str]
    count: int


@dataclass(frozen=True)
class TimeToStageStatistics:
    """Time-to-stage statistics.

    Statistics about time taken to reach a career stage.
    """

    label: str
    avg_years: float
    median_years: float
    sample_size: int


def compute_role_transitions(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict:
    """Compute industry-wide role transition statistics.

    Returns:
        {
            "transitions": [{from_stage, to_stage, count, avg_years}],
            "career_paths": [{path: [stages], count}],
            "avg_time_to_stage": {stage: avg_years_from_start},
            "total_persons_analyzed": int,
        }
    """
    # Group credits by person, sorted by year
    annual_role_records_by_person: dict[str, list[YearlyStageRecord]] = defaultdict(
        list
    )
    for credit in credits:
        anime = anime_map.get(credit.anime_id)
        if not anime or not anime.year:
            continue
        stage = CAREER_STAGE.get(credit.role, 0)
        if stage > 0:
            record = YearlyStageRecord(year=anime.year, stage=stage)
            annual_role_records_by_person[credit.person_id].append(record)

    # Compute per-person stage progression (max stage per year)
    transition_gap_years: dict[tuple[int, int], list[int]] = defaultdict(
        list
    )  # (from, to) -> [years_gap]
    career_path_frequencies: dict[tuple[int, ...], int] = defaultdict(int)
    years_to_reach_each_stage: dict[int, list[int]] = defaultdict(list)

    analyzed_person_count = 0
    for person_id, annual_records in annual_role_records_by_person.items():
        if len(annual_records) < 2:
            continue

        # Get max stage per year
        max_stage_per_year: dict[int, int] = {}
        for record in annual_records:
            max_stage_per_year[record.year] = max(
                max_stage_per_year.get(record.year, 0), record.stage
            )

        years_sorted = sorted(max_stage_per_year.keys())
        if len(years_sorted) < 2:
            continue

        analyzed_person_count += 1
        career_start_year = years_sorted[0]

        # Track progression (only upward transitions)
        career_progression_path = []
        previous_stage = max_stage_per_year[years_sorted[0]]
        previous_transition_year = years_sorted[0]
        career_progression_path.append(previous_stage)

        for year in years_sorted[1:]:
            current_stage = max_stage_per_year[year]
            if current_stage > previous_stage:
                years_elapsed = year - previous_transition_year
                transition_gap_years[(previous_stage, current_stage)].append(
                    years_elapsed
                )
                previous_stage = current_stage
                previous_transition_year = year
                career_progression_path.append(current_stage)
            # Track time to reach each stage from career start
            years_since_start = year - career_start_year
            years_to_reach_each_stage[current_stage].append(years_since_start)

        if len(career_progression_path) >= 2:
            career_path_frequencies[tuple(career_progression_path)] += 1

    # Build transition results
    transition_statistics = []
    for (from_stage, to_stage), gap_years_list in sorted(transition_gap_years.items()):
        avg_gap = sum(gap_years_list) / len(gap_years_list) if gap_years_list else 0
        stat = TransitionStatistics(
            from_stage=from_stage,
            from_label=STAGE_LABELS.get(from_stage, "?"),
            to_stage=to_stage,
            to_label=STAGE_LABELS.get(to_stage, "?"),
            count=len(gap_years_list),
            avg_years=round(avg_gap, 1),
        )
        transition_statistics.append(stat)

    # Top career paths
    most_common_paths = sorted(
        career_path_frequencies.items(), key=lambda item: -item[1]
    )[:20]
    career_path_records = []
    for path_tuple, frequency in most_common_paths:
        record = CareerPathRecord(
            path=list(path_tuple),
            path_labels=[STAGE_LABELS.get(stage, "?") for stage in path_tuple],
            count=frequency,
        )
        career_path_records.append(record)

    # Average time to reach each stage
    time_to_stage_stats = {}
    for stage in sorted(years_to_reach_each_stage.keys()):
        years_samples = years_to_reach_each_stage[stage]
        stats = TimeToStageStatistics(
            label=STAGE_LABELS.get(stage, "?"),
            avg_years=round(sum(years_samples) / len(years_samples), 1),
            median_years=round(sorted(years_samples)[len(years_samples) // 2], 1),
            sample_size=len(years_samples),
        )
        time_to_stage_stats[stage] = stats

    logger.info(
        "transition_analysis_complete",
        persons=analyzed_person_count,
        unique_transitions=len(transition_statistics),
    )

    return {
        "transitions": transition_statistics,
        "career_paths": career_path_records,
        "avg_time_to_stage": time_to_stage_stats,
        "total_persons_analyzed": analyzed_person_count,
    }
