"""Studio Time Series Evaluation — 年単位でスタジオを継続評価.

Tracks studio performance over time: staff metrics, retention rates,
talent quality, and year-specific studio fixed effects.
"""

from collections import defaultdict
from dataclasses import asdict, dataclass

import structlog

from src.models import AnimeAnalysis as Anime, Credit
from src.utils.time_utils import get_year_quarter, yq_label

logger = structlog.get_logger()


@dataclass
class StudioYearMetrics:
    """Metrics for a single studio-year."""

    year: int
    studio_fe: float  # year-specific studio FE (residual mean)
    avg_anime_score: float  # average work score
    staff_count: int  # unique staff count
    avg_staff_iv: float  # staff average IV
    talent_retention: (
        float  # retention rate from prior year (fraction of prev staff retained)
    )
    new_talent_ratio: float  # fraction of staff new this year


@dataclass
class StudioTimeSeriesResult:
    """Result of studio time series computation."""

    studio_metrics: dict[str, list[dict]]  # studio → [StudioYearMetrics as dict]
    quarterly_summary: dict[str, dict[str, int]]  # studio → {"2020-Q1": credit_count}
    studios_analyzed: int
    total_studio_years: int


def compute_studio_timeseries(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    iv_scores: dict[str, float],
    studio_assignments: dict[str, dict[int, str]],
    akm_residuals: dict[tuple[str, str], float] | None = None,
) -> StudioTimeSeriesResult:
    """Compute year-by-year studio evaluation metrics.

    Args:
        credits: all credits
        anime_map: anime_id → Anime
        iv_scores: person_id → integrated value score
        studio_assignments: person_id → {year → studio}
        akm_residuals: (person_id, anime_id) → AKM residual (for year-specific studio FE)

    Returns:
        StudioTimeSeriesResult
    """
    # Group (studio, year) → set of person_ids
    studio_year_staff: dict[tuple[str, int], set[str]] = defaultdict(set)
    for pid, year_studio in studio_assignments.items():
        for year, studio in year_studio.items():
            studio_year_staff[(studio, year)].add(pid)

    # Group studio → anime scores per year
    studio_year_anime_scores: dict[tuple[str, int], list[float]] = defaultdict(list)
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year or not anime.studios:
            continue
        _disp = getattr(anime, "score", None)  # display-only
        if _disp is not None:
            for studio in anime.studios:
                studio_year_anime_scores[(studio, anime.year)].append(_disp)

    # Compute year-specific studio FE from AKM residuals
    studio_year_resid: dict[tuple[str, int], list[float]] = defaultdict(list)
    if akm_residuals:
        for (pid, anime_id), resid in akm_residuals.items():
            anime = anime_map.get(anime_id)
            if not anime or not anime.year or not anime.studios:
                continue
            studio = studio_assignments.get(pid, {}).get(anime.year)
            if not studio:
                studio = anime.studios[0]
            studio_year_resid[(studio, anime.year)].append(resid)

    # Gather all studios and years
    all_studios: set[str] = set()
    all_years: set[int] = set()
    for studio, year in studio_year_staff:
        all_studios.add(studio)
        all_years.add(year)

    if not all_studios or not all_years:
        return StudioTimeSeriesResult(
            studio_metrics={},
            quarterly_summary={},
            studios_analyzed=0,
            total_studio_years=0,
        )

    sorted_years = sorted(all_years)

    # Build metrics per studio
    studio_metrics: dict[str, list[dict]] = {}
    total_sy = 0

    for studio in sorted(all_studios):
        yearly_metrics = []
        prev_staff: set[str] | None = None

        for year in sorted_years:
            staff = studio_year_staff.get((studio, year), set())
            if not staff:
                prev_staff = None
                continue

            # Staff IV average
            staff_ivs = [iv_scores.get(pid, 0.0) for pid in staff]
            avg_iv = sum(staff_ivs) / len(staff_ivs) if staff_ivs else 0.0

            # Anime scores
            a_scores = studio_year_anime_scores.get((studio, year), [])
            avg_anime = sum(a_scores) / len(a_scores) if a_scores else 0.0

            # Retention: fraction of previous year's staff still present (B12 fix)
            retention = 0.0
            new_ratio = 1.0
            if prev_staff and staff:
                intersection = staff & prev_staff
                retention = len(intersection) / len(prev_staff)
                new_ratio = 1.0 - (len(intersection) / len(staff)) if staff else 1.0

            # Year-specific studio FE
            resids = studio_year_resid.get((studio, year), [])
            studio_fe = sum(resids) / len(resids) if resids else 0.0

            metrics = StudioYearMetrics(
                year=year,
                studio_fe=round(studio_fe, 4),
                avg_anime_score=round(avg_anime, 2),
                staff_count=len(staff),
                avg_staff_iv=round(avg_iv, 4),
                talent_retention=round(retention, 3),
                new_talent_ratio=round(new_ratio, 3),
            )
            yearly_metrics.append(asdict(metrics))
            total_sy += 1
            prev_staff = staff

        if yearly_metrics:
            studio_metrics[studio] = yearly_metrics

    # Quarterly summary: studio → {yq_label → credit_count}
    studio_quarter_credits: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.year or not anime.studios:
            continue
        yq = get_year_quarter(anime)
        if yq:
            label = yq_label(*yq)
            for studio in anime.studios:
                studio_quarter_credits[studio][label] += 1

    quarterly_summary = {
        studio: dict(sorted(qmap.items()))
        for studio, qmap in sorted(studio_quarter_credits.items())
    }

    logger.info(
        "studio_timeseries_computed",
        studios=len(studio_metrics),
        total_studio_years=total_sy,
    )

    return StudioTimeSeriesResult(
        studio_metrics=studio_metrics,
        quarterly_summary=quarterly_summary,
        studios_analyzed=len(studio_metrics),
        total_studio_years=total_sy,
    )
