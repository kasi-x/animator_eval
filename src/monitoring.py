"""Data freshness monitoring -- track scraper health and staleness."""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

import structlog

from src.database import get_data_sources

logger = structlog.get_logger()

# Freshness thresholds per source (hours)
FRESHNESS_THRESHOLDS: dict[str, int] = {
    "anilist": 168,  # 7 days
    "mal": 168,  # 7 days
    "mediaarts": 720,  # 30 days
    "wikidata": 720,  # 30 days
}
DEFAULT_THRESHOLD_HOURS = 168  # 7 days


@dataclass
class FreshnessReport:
    """Freshness status for a single data source."""

    source: str
    last_scraped_at: str | None
    item_count: int
    status: str
    is_stale: bool
    hours_since_scrape: float | None
    threshold_hours: int


def check_data_freshness(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
) -> list[FreshnessReport]:
    """Check all data sources against freshness thresholds.

    Args:
        conn: Database connection.
        now: Current time (for testing). Defaults to UTC now.

    Returns:
        List of FreshnessReport for each registered data source.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    sources = get_data_sources(conn)
    reports: list[FreshnessReport] = []

    for src in sources:
        source_name = src["source"]
        last_scraped = src["last_scraped_at"]
        item_count = src["item_count"] or 0
        status = src["status"] or "unknown"
        threshold = FRESHNESS_THRESHOLDS.get(source_name, DEFAULT_THRESHOLD_HOURS)

        if last_scraped is None:
            # Never scraped
            reports.append(
                FreshnessReport(
                    source=source_name,
                    last_scraped_at=None,
                    item_count=item_count,
                    status=status,
                    is_stale=True,
                    hours_since_scrape=None,
                    threshold_hours=threshold,
                )
            )
            logger.warning(
                "data_source_never_scraped",
                source=source_name,
            )
            continue

        # Parse the timestamp -- SQLite CURRENT_TIMESTAMP produces "YYYY-MM-DD HH:MM:SS"
        try:
            scraped_dt = datetime.fromisoformat(last_scraped).replace(
                tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            # Fallback: treat as never scraped
            reports.append(
                FreshnessReport(
                    source=source_name,
                    last_scraped_at=last_scraped,
                    item_count=item_count,
                    status=status,
                    is_stale=True,
                    hours_since_scrape=None,
                    threshold_hours=threshold,
                )
            )
            continue

        delta = now - scraped_dt
        hours_since = delta.total_seconds() / 3600.0
        is_stale = hours_since > threshold

        if is_stale:
            logger.warning(
                "data_source_stale",
                source=source_name,
                hours_since_scrape=round(hours_since, 1),
                threshold_hours=threshold,
            )

        reports.append(
            FreshnessReport(
                source=source_name,
                last_scraped_at=last_scraped,
                item_count=item_count,
                status=status,
                is_stale=is_stale,
                hours_since_scrape=round(hours_since, 1),
                threshold_hours=threshold,
            )
        )

    return reports


def get_freshness_summary(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
) -> dict:
    """Return a summary dict of data source freshness.

    Args:
        conn: Database connection.
        now: Current time (for testing). Defaults to UTC now.

    Returns:
        Dict with total_sources, stale_sources, fresh_sources,
        sources (list of dicts), and overall_status.
    """
    reports = check_data_freshness(conn, now=now)

    total = len(reports)
    stale = sum(1 for r in reports if r.is_stale)
    fresh = total - stale

    # Determine overall status
    if total == 0:
        overall_status = "warning"
    elif stale == 0:
        overall_status = "healthy"
    elif stale == total:
        overall_status = "critical"
    else:
        overall_status = "warning"

    sources_list = [
        {
            "source": r.source,
            "last_scraped_at": r.last_scraped_at,
            "item_count": r.item_count,
            "status": r.status,
            "is_stale": r.is_stale,
            "hours_since_scrape": r.hours_since_scrape,
            "threshold_hours": r.threshold_hours,
        }
        for r in reports
    ]

    return {
        "total_sources": total,
        "stale_sources": stale,
        "fresh_sources": fresh,
        "sources": sources_list,
        "overall_status": overall_status,
    }
