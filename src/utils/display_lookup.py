"""Display layer helper — safe access to viewer ratings and metadata.

All anime.score, popularity, and description access MUST go through these functions.
This enables auditing and prevents accidental contamination of analysis layer.

**IMPORTANT: src/analysis/** code cannot import this module. Use only from:
- Reports
- CLI commands
- External-facing APIs
"""

import json
import sqlite3
from typing import Optional, Any
import structlog

logger = structlog.get_logger()

__all__ = [
    "get_display_score",
    "get_display_popularity",
    "get_display_favourites",
    "get_display_description",
    "get_display_cover_url",
    "get_display_genres",
    "get_display_tags",
    "get_display_synonyms",
    "get_display_metadata",
    "clear_cache",
]

# Module-level cache for display layer queries
_CACHE: dict = {}


def clear_cache():
    """Clear the display layer query cache.
    
    Use in tests after modifying data or between test cases.
    """
    global _CACHE
    _CACHE.clear()
    logger.debug("display_cache_cleared")


def _get_source_info(anime_id: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Parse anime_id to determine source and ID.
    
    Args:
        anime_id: Formatted ID like "anilist:123", "ann:456", "seesaawiki:slug", etc.
        
    Returns:
        (source, table_name, id_column, id_value) or (None, None, None, None) if invalid
    """
    if not anime_id or not isinstance(anime_id, str):
        return None, None, None, None
    
    # Seesaawiki uses text ID with explicit prefix handling
    if anime_id.startswith("seesaawiki:"):
        _, id_str = anime_id.split(":", 1)
        return "seesaawiki", "src_seesaawiki_anime", "id", id_str
    
    # Try colon separator first (anilist:, allcinema:, keyframe:)
    if ":" in anime_id:
        source, id_str = anime_id.split(":", 1)
        table = f"src_{source}_anime"
        id_column = f"{source}_id"
        return source, table, id_column, id_str
    
    # Try hyphen separator (ann-, mal-)
    if "-" in anime_id:
        source, id_str = anime_id.split("-", 1)
        table = f"src_{source}_anime"
        id_column = f"{source}_id"
        return source, table, id_column, id_str
    
    return None, None, None, None


def _query_bronze(
    conn: sqlite3.Connection,
    anime_id: str,
    field: str,
) -> Optional[Any]:
    """Query a field from the appropriate bronze table.
    
    Args:
        conn: Database connection
        anime_id: Formatted ID (e.g., "anilist:123", "ann:456")
        field: Field name to retrieve
        
    Returns:
        Field value or None if not found
    """
    source, table, id_col, id_val = _get_source_info(anime_id)
    
    if not source:
        return None
    
    try:
        query = f"SELECT {field} FROM {table} WHERE {id_col} = ?"
        # Try integer ID first (for most sources)
        try:
            id_val_typed = int(id_val)
        except (ValueError, TypeError):
            id_val_typed = id_val  # For seesaawiki which uses text
        
        cursor = conn.execute(query, (id_val_typed,))
        row = cursor.fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


def get_display_score(conn: sqlite3.Connection, anime_id: str) -> Optional[float]:
    """Get anime viewer score (anime.score from AniList).

    **DISPLAY ONLY** — NOT for analysis scoring. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: Anime ID (e.g., "anilist:123")

    Returns:
        Viewer score (0-100) or None if not found
    """
    cache_key = f"score:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    
    result = _query_bronze(conn, anime_id, "score")
    _CACHE[cache_key] = result
    return result


def get_display_popularity(conn: sqlite3.Connection, anime_id: str) -> Optional[int]:
    """Get anime popularity rank from AniList.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: Anime ID

    Returns:
        Popularity rank (lower is more popular) or None
    """
    cache_key = f"popularity:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    
    result = _query_bronze(conn, anime_id, "popularity")
    _CACHE[cache_key] = result
    return result


def get_display_favourites(conn: sqlite3.Connection, anime_id: str) -> Optional[int]:
    """Get anime favourites count from AniList.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: Anime ID

    Returns:
        Favourites count or None
    """
    cache_key = f"favourites:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    
    result = _query_bronze(conn, anime_id, "favourites")
    _CACHE[cache_key] = result
    return result


def get_display_description(conn: sqlite3.Connection, anime_id: str) -> Optional[str]:
    """Get anime description/synopsis from source.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.
    
    Falls back: anilist → allcinema (if available)

    Args:
        conn: Database connection
        anime_id: Anime ID

    Returns:
        Description text or None
    """
    cache_key = f"description:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    
    source, _, _, _ = _get_source_info(anime_id)
    
    # Try primary source
    if source == "anilist":
        result = _query_bronze(conn, anime_id, "description")
    elif source == "allcinema":
        result = _query_bronze(conn, anime_id, "synopsis")
    else:
        result = _query_bronze(conn, anime_id, "description")
    
    _CACHE[cache_key] = result
    return result


def get_display_cover_url(conn: sqlite3.Connection, anime_id: str) -> Optional[str]:
    """Get anime cover image URL.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: Anime ID

    Returns:
        URL to cover image or None
    """
    cache_key = f"cover_url:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    
    # Only anilist has cover images in bronze
    source, _, _, _ = _get_source_info(anime_id)
    if source != "anilist":
        _CACHE[cache_key] = None
        return None
    
    result = _query_bronze(conn, anime_id, "cover_large")
    _CACHE[cache_key] = result
    return result


def get_display_genres(conn: sqlite3.Connection, anime_id: str) -> Optional[list[str]]:
    """Get anime genres as a list.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: Anime ID

    Returns:
        List of genre strings or None
    """
    cache_key = f"genres:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    
    genres_json = _query_bronze(conn, anime_id, "genres")
    if not genres_json:
        _CACHE[cache_key] = None
        return None
    
    try:
        result = json.loads(genres_json) if isinstance(genres_json, str) else genres_json
    except (json.JSONDecodeError, TypeError):
        result = None
    
    _CACHE[cache_key] = result
    return result


def get_display_tags(conn: sqlite3.Connection, anime_id: str) -> Optional[list[dict]]:
    """Get anime tags with metadata.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: Anime ID

    Returns:
        List of tag dicts (with 'name', 'rank' fields) or None
    """
    cache_key = f"tags:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    
    tags_json = _query_bronze(conn, anime_id, "tags")
    if not tags_json:
        _CACHE[cache_key] = None
        return None
    
    try:
        result = json.loads(tags_json) if isinstance(tags_json, str) else tags_json
    except (json.JSONDecodeError, TypeError):
        result = None
    
    _CACHE[cache_key] = result
    return result


def get_display_synonyms(conn: sqlite3.Connection, anime_id: str) -> Optional[list[str]]:
    """Get anime title synonyms/alternative titles.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        conn: Database connection
        anime_id: Anime ID

    Returns:
        List of alternative titles or None
    """
    cache_key = f"synonyms:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    
    synonyms_json = _query_bronze(conn, anime_id, "synonyms")
    if not synonyms_json:
        _CACHE[cache_key] = None
        return None
    
    try:
        result = json.loads(synonyms_json) if isinstance(synonyms_json, str) else synonyms_json
    except (json.JSONDecodeError, TypeError):
        result = None
    
    _CACHE[cache_key] = result
    return result


def get_display_metadata(
    conn: sqlite3.Connection, anime_id: str
) -> Optional[dict]:
    """Get all display metadata for an anime in one call.

    **DISPLAY ONLY** — NOT for analysis.

    Args:
        conn: Database connection
        anime_id: Anime ID

    Returns:
        Dict with keys: score, popularity, favourites, description, or None
    """
    cache_key = f"metadata:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    
    result = {
        "score": get_display_score(conn, anime_id),
        "popularity": get_display_popularity(conn, anime_id),
        "favourites": get_display_favourites(conn, anime_id),
        "description": get_display_description(conn, anime_id),
        "cover_url": get_display_cover_url(conn, anime_id),
        "genres": get_display_genres(conn, anime_id),
        "tags": get_display_tags(conn, anime_id),
        "synonyms": get_display_synonyms(conn, anime_id),
    }
    
    _CACHE[cache_key] = result
    return result


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
