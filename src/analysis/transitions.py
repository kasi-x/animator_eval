"""役職遷移分析 — アニメーターのキャリアパスを分析する.

業界全体の役職遷移パターンを算出:
- 遷移確率（動画→原画、原画→作画監督、etc.）
- 平均遷移年数
- 最頻出キャリアパス
"""

from collections import defaultdict

import structlog

from src.analysis.career import CAREER_STAGE
from src.models import Anime, Credit

logger = structlog.get_logger()

# Stage → readable label
STAGE_LABEL = {
    1: "In-Between",
    2: "2nd Key/Layout",
    3: "Key Animator",
    4: "Anim. Director",
    5: "Chief AD/Ep.Dir",
    6: "Director",
}


def compute_role_transitions(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict:
    """業界全体の役職遷移統計を計算する.

    Returns:
        {
            "transitions": [{from_stage, to_stage, count, avg_years}],
            "career_paths": [{path: [stages], count}],
            "avg_time_to_stage": {stage: avg_years_from_start},
            "total_persons_analyzed": int,
        }
    """
    # Group credits by person, sorted by year
    person_credits: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year:
            continue
        stage = CAREER_STAGE.get(c.role, 0)
        if stage > 0:
            person_credits[c.person_id].append((anime.year, stage))

    # Compute per-person stage progression (max stage per year)
    transition_counts: dict[tuple[int, int], list[int]] = defaultdict(list)  # (from, to) -> [years_gap]
    career_paths: dict[tuple[int, ...], int] = defaultdict(int)
    time_to_stage: dict[int, list[int]] = defaultdict(list)

    analyzed = 0
    for pid, year_stages in person_credits.items():
        if len(year_stages) < 2:
            continue

        # Get max stage per year
        yearly_max: dict[int, int] = {}
        for year, stage in year_stages:
            yearly_max[year] = max(yearly_max.get(year, 0), stage)

        years_sorted = sorted(yearly_max.keys())
        if len(years_sorted) < 2:
            continue

        analyzed += 1
        first_year = years_sorted[0]

        # Track progression (only upward transitions)
        path = []
        prev_stage = yearly_max[years_sorted[0]]
        prev_year = years_sorted[0]
        path.append(prev_stage)

        for year in years_sorted[1:]:
            cur_stage = yearly_max[year]
            if cur_stage > prev_stage:
                transition_counts[(prev_stage, cur_stage)].append(year - prev_year)
                prev_stage = cur_stage
                prev_year = year
                path.append(cur_stage)
            # Track time to reach each stage from career start
            time_to_stage[cur_stage].append(year - first_year)

        if len(path) >= 2:
            career_paths[tuple(path)] += 1

    # Build transition results
    transitions = []
    for (from_s, to_s), years_list in sorted(transition_counts.items()):
        avg_years = sum(years_list) / len(years_list) if years_list else 0
        transitions.append({
            "from_stage": from_s,
            "from_label": STAGE_LABEL.get(from_s, "?"),
            "to_stage": to_s,
            "to_label": STAGE_LABEL.get(to_s, "?"),
            "count": len(years_list),
            "avg_years": round(avg_years, 1),
        })

    # Top career paths
    top_paths = sorted(career_paths.items(), key=lambda x: -x[1])[:20]
    paths_result = []
    for path, count in top_paths:
        paths_result.append({
            "path": list(path),
            "path_labels": [STAGE_LABEL.get(s, "?") for s in path],
            "count": count,
        })

    # Average time to reach each stage
    avg_time = {}
    for stage in sorted(time_to_stage.keys()):
        vals = time_to_stage[stage]
        avg_time[stage] = {
            "label": STAGE_LABEL.get(stage, "?"),
            "avg_years": round(sum(vals) / len(vals), 1),
            "median_years": round(sorted(vals)[len(vals) // 2], 1),
            "sample_size": len(vals),
        }

    logger.info(
        "transition_analysis_complete",
        persons=analyzed,
        unique_transitions=len(transitions),
    )

    return {
        "transitions": transitions,
        "career_paths": paths_result,
        "avg_time_to_stage": avg_time,
        "total_persons_analyzed": analyzed,
    }
