"""Time-series analysis — analyse annual and quarterly industry-wide statistics.

年ごと、四半期ごとの:
- アクティブ人物数
- 新規参入者数
- 制作規模指標の推移
- 役職分布の変化

credit_year / credit_quarter が設定されている場合はそちらを優先し、
長期作品のクレジットを各話の放送時期に帰属させる。
"""

from collections import defaultdict

import structlog

from src.models import AnimeAnalysis as Anime, Credit
from src.utils.time_utils import get_year_quarter, yq_label

logger = structlog.get_logger()


def compute_time_series(
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> dict:
    """Compute annual and quarterly time-series statistics.

    credit_year/credit_quarter があるクレジットはそちらを使用（長期作品の話数別帰属）。
    なければ anime の year/quarter にフォールバック。

    Returns:
        {
            "years": [int],
            "series": { ... per-year series ... },
            "quarterly": {
                "labels": ["2020-Q1", ...],
                "series": {
                    "active_persons": {"2020-Q1": count, ...},
                    "new_entrants": {...},
                    "credit_count": {...},
                    "unique_anime": {...},
                },
            },
            "summary": { ... },
        }
    """
    # Build year → data AND (year, quarter) → data
    year_persons: dict[int, set[str]] = defaultdict(set)
    year_credits: dict[int, int] = defaultdict(int)
    year_anime: dict[int, set[str]] = defaultdict(set)
    yq_persons: dict[str, set[str]] = defaultdict(set)
    yq_credits: dict[str, int] = defaultdict(int)
    yq_anime: dict[str, set[str]] = defaultdict(set)

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime:
            continue

        # Prefer credit_year/credit_quarter (episode-level attribution for long-running works)
        c_year = c.credit_year
        c_quarter = c.credit_quarter

        # fallback: use anime.year / anime.quarter
        if c_year is None:
            c_year = anime.year
        if c_year is None:
            continue

        if c_quarter is None:
            yq = get_year_quarter(anime)
            c_quarter = yq[1] if yq else None

        # Annual aggregation
        year_persons[c_year].add(c.person_id)
        year_credits[c_year] += 1
        year_anime[c_year].add(c.anime_id)
        # Quarterly aggregation
        if c_quarter is not None:
            label = yq_label(c_year, c_quarter)
            yq_persons[label].add(c.person_id)
            yq_credits[label] += 1
            yq_anime[label].add(c.anime_id)

    if not year_persons:
        return {"years": [], "series": {}, "quarterly": {}, "summary": {}}

    years = sorted(year_persons.keys())

    # Track first appearance year for each person
    person_first_year: dict[str, int] = {}
    for year in years:
        for pid in year_persons[year]:
            if pid not in person_first_year:
                person_first_year[pid] = year

    # Track first appearance quarter for each person
    sorted_yq_labels = sorted(yq_persons.keys())
    person_first_yq: dict[str, str] = {}
    for label in sorted_yq_labels:
        for pid in yq_persons[label]:
            if pid not in person_first_yq:
                person_first_yq[pid] = label

    # Build annual series
    anime_staff_count = {
        aid: len({c.person_id for c in credits if c.anime_id == aid})
        for aid in {c.anime_id for c in credits}
    }
    active_persons = {y: len(year_persons[y]) for y in years}
    new_entrants = {
        y: sum(1 for pid in year_persons[y] if person_first_year.get(pid) == y)
        for y in years
    }
    credit_count = {y: year_credits[y] for y in years}
    avg_staff_count = {
        y: round(
            sum(anime_staff_count.get(aid, 0) for aid in year_anime[y])
            / len(year_anime[y]),
            2,
        )
        for y in years
        if year_anime[y]
    }
    unique_anime = {y: len(year_anime[y]) for y in years}

    # Build quarterly series
    yq_active = {label: len(yq_persons[label]) for label in sorted_yq_labels}
    yq_new = {
        label: sum(1 for pid in yq_persons[label] if person_first_yq.get(pid) == label)
        for label in sorted_yq_labels
    }
    yq_credit_count = {label: yq_credits[label] for label in sorted_yq_labels}
    yq_unique_anime = {label: len(yq_anime[label]) for label in sorted_yq_labels}

    # Summary
    peak_year = max(years, key=lambda y: year_credits[y])
    peak_credits = year_credits[peak_year]

    # Growth rate (last 5 years vs previous 5 years)
    growth_rate = 0.0
    if len(years) >= 10:
        recent_5 = sum(year_credits.get(y, 0) for y in years[-5:])
        prev_5 = sum(year_credits.get(y, 0) for y in years[-10:-5])
        if prev_5 > 0:
            growth_rate = round((recent_5 - prev_5) / prev_5 * 100, 1)

    result = {
        "years": years,
        "series": {
            "active_persons": active_persons,
            "new_entrants": new_entrants,
            "credit_count": credit_count,
            "avg_staff_count": avg_staff_count,
            "unique_anime": unique_anime,
        },
        "quarterly": {
            "labels": sorted_yq_labels,
            "series": {
                "active_persons": yq_active,
                "new_entrants": yq_new,
                "credit_count": yq_credit_count,
                "unique_anime": yq_unique_anime,
            },
        },
        "summary": {
            "peak_year": peak_year,
            "peak_credits": peak_credits,
            "growth_rate": growth_rate,
            "total_years": len(years),
            "total_quarters": len(sorted_yq_labels),
            "total_unique_persons": len(person_first_year),
        },
    }

    logger.info(
        "time_series_computed", years=len(years), quarters=len(sorted_yq_labels)
    )
    return result
