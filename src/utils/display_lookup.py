"""Display layer helper — safe access to viewer ratings and metadata.

All anime.score, popularity, and description access MUST go through these functions.
This enables auditing and prevents accidental contamination of analysis layer.

**IMPORTANT: src/analysis/** code cannot import this module. Use only from:
- Reports
- CLI commands
- External-facing APIs
"""

import sqlite3
from typing import Optional
import structlog

logger = structlog.get_logger()

__all__ = [
    "get_display_score",
    "get_display_popularity",
    "get_display_favourites",
    "get_display_description",
    "get_display_metadata",
]


def get_display_score(conn: sqlite3.Connection, anime_id: str) -> Optional[float]:
    """Get anime viewer score (anime.score from AniList).

    **DISPLAY ONLY** — NOT for analysis scoring. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: AniList anime ID (e.g., "anilist_123")

    Returns:
        Viewer score (0-100) or None if not found
    """
    try:
        cursor = conn.execute(
            "SELECT score FROM anime_display WHERE anime_id = ?", (anime_id,)
        )
        row = cursor.fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        logger.warning("anime_display_not_available", anime_id=anime_id)
        return None


def get_display_popularity(conn: sqlite3.Connection, anime_id: str) -> Optional[int]:
    """Get anime popularity rank from AniList.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: AniList anime ID

    Returns:
        Popularity rank (lower is more popular) or None
    """
    try:
        cursor = conn.execute(
            "SELECT popularity FROM anime_display WHERE anime_id = ?", (anime_id,)
        )
        row = cursor.fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        logger.warning("anime_display_not_available", anime_id=anime_id)
        return None


def get_display_favourites(conn: sqlite3.Connection, anime_id: str) -> Optional[int]:
    """Get anime favourites count from AniList.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: AniList anime ID

    Returns:
        Favourites count or None
    """
    try:
        cursor = conn.execute(
            "SELECT favourites FROM anime_display WHERE anime_id = ?", (anime_id,)
        )
        row = cursor.fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        logger.warning("anime_display_not_available", anime_id=anime_id)
        return None


def get_display_description(conn: sqlite3.Connection, anime_id: str) -> Optional[str]:
    """Get anime description/synopsis from source.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: AniList anime ID

    Returns:
        Description text or None
    """
    try:
        cursor = conn.execute(
            "SELECT description FROM anime_display WHERE anime_id = ?", (anime_id,)
        )
        row = cursor.fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        logger.warning("anime_display_not_available", anime_id=anime_id)
        return None


def get_display_metadata(
    conn: sqlite3.Connection, anime_id: str
) -> Optional[dict]:
    """Get all display metadata for an anime in one call.

    **DISPLAY ONLY** — NOT for analysis.

    Args:
        conn: Database connection
        anime_id: AniList anime ID

    Returns:
        Dict with keys: score, popularity, favourites, description, or None
    """
    try:
        cursor = conn.execute(
            """
            SELECT score, popularity, favourites, description
            FROM anime_display WHERE anime_id = ?
            """,
            (anime_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "score": row[0],
            "popularity": row[1],
            "favourites": row[2],
            "description": row[3],
        }
    except sqlite3.OperationalError:
        logger.warning("anime_display_not_available", anime_id=anime_id)
        return None


# Audit: Log whenever display data is accessed (for compliance)
def log_display_access(reason: str, anime_id: str, field: str):
    """Log display layer access for audit trail.

    Args:
        reason: Why we're accessing (e.g., "report_export", "cli_preview")
        anime_id: Which anime
        field: Which field (score, popularity, etc.)
    """
    logger.info(
        "display_layer_access",
        reason=reason,
        anime_id=anime_id,
        field=field,
    )
