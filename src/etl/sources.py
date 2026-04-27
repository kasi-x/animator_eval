"""ETL source management - reads from schema v55 sources lookup table."""

from __future__ import annotations

import sqlite3

import structlog

log = structlog.get_logger()

# Fallback constants (used if sources table not available)
_DEFAULT_SOURCES = {
    "anilist": {"name": "ANILIST", "prefix": "anilist:"},
    "ann": {"name": "ANN", "prefix": "ann-"},
    "seesaawiki": {"name": "SEESAAWIKI", "prefix": "seesaawiki:"},
    "keyframe": {"name": "KEYFRAME", "prefix": "keyframe:"},
    "mal": {"name": "MAL", "prefix": "mal:"},
}


def get_source_prefix(conn: sqlite3.Connection, source_code: str) -> str:
    """Get ID prefix for a source code.
    
    Falls back to _DEFAULT_SOURCES if sources table not available.
    
    Args:
        conn: Database connection
        source_code: Source code (e.g., 'anilist', 'ann')
        
    Returns:
        ID prefix (e.g., 'anilist:', 'ann-')
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM sources WHERE id = ?", (source_code,))
        row = cursor.fetchone()
        if row:
            # Map source code to prefix
            # Most sources use format: source_code:123 or source_code-123
            if source_code in ("ann", "mal"):
                return f"{source_code}-"
            return f"{source_code}:"
    except sqlite3.OperationalError:
        pass  # sources table doesn't exist
    
    # Fallback
    if source_code in _DEFAULT_SOURCES:
        return _DEFAULT_SOURCES[source_code]["prefix"]
    
    log.warning("unknown_source_code", source=source_code)
    return f"{source_code}:"


def get_all_sources(conn: sqlite3.Connection) -> list[str]:
    """Get list of all available sources.
    
    Args:
        conn: Database connection
        
    Returns:
        List of source codes (e.g., ['anilist', 'ann', 'allcinema', ...])
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM sources ORDER BY id")
        sources = [row[0] for row in cursor.fetchall()]
        if sources:
            return sources
    except sqlite3.OperationalError:
        pass  # sources table doesn't exist
    
    # Fallback
    return list(_DEFAULT_SOURCES.keys())


def validate_source(conn: sqlite3.Connection, source_code: str) -> bool:
    """Check if source is valid and available.
    
    Args:
        conn: Database connection
        source_code: Source code to validate
        
    Returns:
        True if valid, False otherwise
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM sources WHERE id = ?", (source_code,))
        return cursor.fetchone() is not None
    except sqlite3.OperationalError:
        pass  # sources table doesn't exist
    
    # Fallback
    return source_code in _DEFAULT_SOURCES
