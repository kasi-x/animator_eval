"""Time utilities — year+quarter helpers for quarterly aggregation.

Anime broadcast seasons map to calendar quarters:
    winter (1月期) → Q1, spring (4月期) → Q2,
    summer (7月期) → Q3, fall (10月期) → Q4.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.models import Anime

SEASON_TO_QUARTER: dict[str, int] = {
    "winter": 1,
    "spring": 2,
    "summer": 3,
    "fall": 4,
}


def season_to_quarter(season: str | None) -> int | None:
    """Convert broadcast season string to quarter number (1-4)."""
    if season is None:
        return None
    return SEASON_TO_QUARTER.get(season.lower())


def _quarter_from_start_date(start_date: str | None) -> int | None:
    """Extract quarter from a YYYY-MM-DD start_date string."""
    if not start_date or len(start_date) < 7:
        return None
    try:
        month = int(start_date[5:7])
        if 1 <= month <= 12:
            return (month - 1) // 3 + 1
    except (ValueError, IndexError):
        pass
    return None


def get_year_quarter(anime: Anime) -> tuple[int, int] | None:
    """Extract (year, quarter) from an Anime object.

    Returns None if year is missing.
    Priority: DB quarter column → season field → start_date month → Q2 fallback.
    """
    if anime.year is None:
        return None
    # DB で算出済みの quarter を最優先
    if anime.quarter is not None and 1 <= anime.quarter <= 4:
        return (anime.year, anime.quarter)
    # フォールバック: season → start_date → Q2
    q = season_to_quarter(anime.season)
    if q is None:
        q = _quarter_from_start_date(anime.start_date)
    if q is None:
        q = 2  # last resort fallback
    return (anime.year, q)


def yq_to_float(year: int, quarter: int) -> float:
    """Convert (year, quarter) to a fractional year for continuous math.

    Q1 → year + 0.0, Q2 → year + 0.25, Q3 → year + 0.5, Q4 → year + 0.75
    """
    return year + (quarter - 1) * 0.25


def float_to_yq(f: float) -> tuple[int, int]:
    """Convert fractional year back to (year, quarter)."""
    year = int(f)
    quarter = min(4, max(1, int((f - year) * 4) + 1))
    return (year, quarter)


def yq_label(year: int, quarter: int) -> str:
    """Format (year, quarter) as a label like '2020-Q1'."""
    return f"{year}-Q{quarter}"


def yq_diff_quarters(y1: int, q1: int, y2: int, q2: int) -> int:
    """Number of quarters between two (year, quarter) pairs.

    Returns (y2,q2) - (y1,q1) in quarters. Positive if (y2,q2) is later.
    """
    return (y2 - y1) * 4 + (q2 - q1)
