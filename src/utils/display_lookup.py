"""Display layer helper — safe access to viewer ratings and metadata.

All anime.score, popularity, and description access MUST go through these functions.
This enables auditing and prevents accidental contamination of analysis layer.

**IMPORTANT: src/analysis/** code cannot import this module. Use only from:
- Reports
- CLI commands
- External-facing APIs
"""

import json
import os
from pathlib import Path
from typing import Optional, Any

import duckdb
import structlog

logger = structlog.get_logger()

DEFAULT_BRONZE_PATH: Path = Path(
    os.environ.get("ANIMETOR_BRONZE_PATH", "result/anime.db")
)

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


def _get_source_info(
    anime_id: str,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
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


def _open_bronze(bronze_path: Path | str | None) -> Optional[duckdb.DuckDBPyConnection]:
    """Open an in-memory DuckDB connection with the bronze SQLite file attached.

    Returns None if the file does not exist, so callers can short-circuit.
    """
    bp = Path(bronze_path) if bronze_path is not None else DEFAULT_BRONZE_PATH
    if not bp.exists():
        return None
    conn = duckdb.connect(":memory:")
    conn.execute(f"ATTACH '{bp}' AS bronze (TYPE SQLITE, READ_ONLY TRUE)")
    return conn


def _query_bronze(
    anime_id: str,
    field: str,
    bronze_path: Path | str | None = None,
) -> Optional[Any]:
    """Query a field from the appropriate bronze table.

    Args:
        anime_id: Formatted ID (e.g., "anilist:123", "ann:456")
        field: Field name to retrieve
        bronze_path: Path to the SQLite file; defaults to DEFAULT_BRONZE_PATH

    Returns:
        Field value or None if not found
    """
    source, table, id_col, id_val = _get_source_info(anime_id)

    if not source:
        return None

    conn = _open_bronze(bronze_path)
    if conn is None:
        return None

    try:
        try:
            id_typed: int | str = int(id_val)  # type: ignore[arg-type]
        except (ValueError, TypeError):
            id_typed = id_val  # For seesaawiki which uses text

        row = conn.execute(
            f"SELECT {field} FROM bronze.{table} WHERE {id_col} = ?",
            [id_typed],
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None
    finally:
        conn.close()


def get_display_score(
    anime_id: str, bronze_path: Path | str | None = None
) -> Optional[float]:
    """Get anime viewer score (anime.score from AniList).

    **DISPLAY ONLY** — NOT for analysis scoring. Returns None if unavailable.

    Args:
        anime_id: Anime ID (e.g., "anilist:123")
        bronze_path: Path to the SQLite file; defaults to DEFAULT_BRONZE_PATH

    Returns:
        Viewer score (0-100) or None if not found
    """
    cache_key = f"score:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    result = _query_bronze(anime_id, "score", bronze_path)
    _CACHE[cache_key] = result
    return result


def get_display_popularity(
    anime_id: str, bronze_path: Path | str | None = None
) -> Optional[int]:
    """Get anime popularity rank from AniList.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        anime_id: Anime ID
        bronze_path: Path to the SQLite file; defaults to DEFAULT_BRONZE_PATH

    Returns:
        Popularity rank (lower is more popular) or None
    """
    cache_key = f"popularity:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    result = _query_bronze(anime_id, "popularity", bronze_path)
    _CACHE[cache_key] = result
    return result


def get_display_favourites(
    anime_id: str, bronze_path: Path | str | None = None
) -> Optional[int]:
    """Get anime favourites count from AniList.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        anime_id: Anime ID
        bronze_path: Path to the SQLite file; defaults to DEFAULT_BRONZE_PATH

    Returns:
        Favourites count or None
    """
    cache_key = f"favourites:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    result = _query_bronze(anime_id, "favourites", bronze_path)
    _CACHE[cache_key] = result
    return result


def _query_primary_description(
    anime_id: str,
    source: str,
    bronze_path: Path | str | None = None,
) -> Optional[str]:
    field = "synopsis" if source == "allcinema" else "description"
    return _query_bronze(anime_id, field, bronze_path)


def _query_fallback_description(
    anime_id: str,
    bronze_path: Path | str | None = None,
) -> Optional[str]:
    """Look up allcinema synopsis via anime_external_ids cross-reference.

    anime_external_ids is a SILVER table that lives in the same SQLite file
    as the bronze src_* tables.
    """
    conn = _open_bronze(bronze_path)
    if conn is None:
        return None

    try:
        row = conn.execute(
            "SELECT external_id FROM bronze.anime_external_ids "
            "WHERE anime_id = ? AND source = 'allcinema'",
            [anime_id],
        ).fetchone()
        if not row:
            return None

        allcinema_id_str = row[0]
        try:
            allcinema_id: int | str = int(allcinema_id_str)
        except (ValueError, TypeError):
            allcinema_id = allcinema_id_str

        result = conn.execute(
            "SELECT synopsis FROM bronze.src_allcinema_anime WHERE allcinema_id = ?",
            [allcinema_id],
        ).fetchone()
        return result[0] if result else None
    except Exception:
        return None
    finally:
        conn.close()


def get_display_description(
    anime_id: str, bronze_path: Path | str | None = None
) -> Optional[str]:
    """Get anime description/synopsis from source.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Falls back: anilist → allcinema (if available)
    """
    cache_key = f"description:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]
    source, _, _, _ = _get_source_info(anime_id)
    result = _query_primary_description(anime_id, source, bronze_path)
    if result is None:
        result = _query_fallback_description(anime_id, bronze_path)
    _CACHE[cache_key] = result
    return result


def get_display_cover_url(
    anime_id: str, bronze_path: Path | str | None = None
) -> Optional[str]:
    """Get anime cover image URL.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        anime_id: Anime ID
        bronze_path: Path to the SQLite file; defaults to DEFAULT_BRONZE_PATH

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

    result = _query_bronze(anime_id, "cover_large", bronze_path)
    _CACHE[cache_key] = result
    return result


def get_display_genres(
    anime_id: str, bronze_path: Path | str | None = None
) -> Optional[list[str]]:
    """Get anime genres as a list.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        anime_id: Anime ID
        bronze_path: Path to the SQLite file; defaults to DEFAULT_BRONZE_PATH

    Returns:
        List of genre strings or None
    """
    cache_key = f"genres:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    genres_json = _query_bronze(anime_id, "genres", bronze_path)
    if not genres_json:
        _CACHE[cache_key] = []
        return []

    try:
        result = json.loads(genres_json) if isinstance(genres_json, str) else genres_json
        if not isinstance(result, list):
            result = []
    except (json.JSONDecodeError, TypeError):
        result = []

    _CACHE[cache_key] = result
    return result


def get_display_tags(
    anime_id: str, bronze_path: Path | str | None = None
) -> Optional[list[dict]]:
    """Get anime tags with metadata.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        anime_id: Anime ID
        bronze_path: Path to the SQLite file; defaults to DEFAULT_BRONZE_PATH

    Returns:
        List of tag dicts (with 'name', 'rank' fields) or None
    """
    cache_key = f"tags:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    tags_json = _query_bronze(anime_id, "tags", bronze_path)
    if not tags_json:
        _CACHE[cache_key] = []
        return []

    try:
        result = json.loads(tags_json) if isinstance(tags_json, str) else tags_json
        if not isinstance(result, list):
            result = []
    except (json.JSONDecodeError, TypeError):
        result = []

    _CACHE[cache_key] = result
    return result


def get_display_synonyms(
    anime_id: str, bronze_path: Path | str | None = None
) -> Optional[list[str]]:
    """Get anime title synonyms/alternative titles.

    **DISPLAY ONLY** — NOT for analysis. Returns None if unavailable.

    Args:
        anime_id: Anime ID
        bronze_path: Path to the SQLite file; defaults to DEFAULT_BRONZE_PATH

    Returns:
        List of alternative titles or None
    """
    cache_key = f"synonyms:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    synonyms_json = _query_bronze(anime_id, "synonyms", bronze_path)
    if not synonyms_json:
        _CACHE[cache_key] = []
        return []

    try:
        result = json.loads(synonyms_json) if isinstance(synonyms_json, str) else synonyms_json
        if not isinstance(result, list):
            result = []
    except (json.JSONDecodeError, TypeError):
        result = []

    _CACHE[cache_key] = result
    return result


def get_display_metadata(
    anime_id: str,
    bronze_path: Path | str | None = None,
) -> Optional[dict]:
    """Get all display metadata for an anime in one call.

    **DISPLAY ONLY** — NOT for analysis.

    Args:
        anime_id: Anime ID
        bronze_path: Path to the SQLite file; defaults to DEFAULT_BRONZE_PATH

    Returns:
        Dict with keys: score, popularity, favourites, description,
        cover_url, genres, tags, synonyms
    """
    cache_key = f"metadata:{anime_id}"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    result = {
        "score": get_display_score(anime_id, bronze_path),
        "popularity": get_display_popularity(anime_id, bronze_path),
        "favourites": get_display_favourites(anime_id, bronze_path),
        "description": get_display_description(anime_id, bronze_path),
        "cover_url": get_display_cover_url(anime_id, bronze_path),
        "genres": get_display_genres(anime_id, bronze_path),
        "tags": get_display_tags(anime_id, bronze_path),
        "synonyms": get_display_synonyms(anime_id, bronze_path),
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
