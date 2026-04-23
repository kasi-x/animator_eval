"""DuckDB-backed read paths for heavy analytical queries (Phase A PoC).

All writes remain in SQLite. This module provides READ-ONLY alternatives
that leverage DuckDB's vectorized execution and query optimizer for
analytical queries over the SQLite database files.

Targets (vs SQLite baseline):
  1. load_credits_ddb     — full credits scan (large table, vectorized)
  2. load_anime_joined_ddb — anime + genres + tags + studios in one query
  3. agg_credits_per_person_ddb — GROUP BY person/year/role (AKM feed)

Usage:
    from src.analysis.duckdb_io import load_credits_ddb, load_anime_joined_ddb
    rows = load_credits_ddb(db_path)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from src.database import DEFAULT_DB_PATH


def _duck(db_path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with the SQLite DB attached read-only."""
    path = str(db_path or DEFAULT_DB_PATH)
    conn = duckdb.connect(":memory:")
    conn.execute(f"ATTACH '{path}' AS sl (TYPE SQLITE, READ_ONLY TRUE)")
    return conn


# ---------------------------------------------------------------------------
# 1. Credits — full-table scan
# ---------------------------------------------------------------------------

def load_credits_ddb(db_path: Path | str | None = None) -> list[dict[str, Any]]:
    """Load all credits rows using DuckDB's vectorized scan.

    Returns list of dicts with the same columns as the SQLite credits table.
    Drop-in data-level replacement for:
        conn.execute("SELECT * FROM credits").fetchall()
    """
    conn = _duck(db_path)
    try:
        rel = conn.execute("SELECT * FROM sl.credits")
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Anime with genres/tags/studios — 5 SQLite queries → 1 DuckDB query
# ---------------------------------------------------------------------------

def load_anime_joined_ddb(db_path: Path | str | None = None) -> list[dict[str, Any]]:
    """Load anime rows with genres, tags, and studios aggregated into lists.

    Replaces the 5-query Python-merge loop in load_all_anime():
        SELECT * FROM anime          (base)
        SELECT * FROM anime_genres   (merge in Python)
        SELECT * FROM anime_tags     (merge in Python)
        SELECT * FROM anime_studios  (merge in Python)
        SELECT * FROM studios        (join for names)

    Returns one dict per anime with extra keys:
        genres  : list[str]
        tags    : list[str]   (name only, ordered by rank desc)
        studios : list[str]   (studio names, main studios first)
    """
    conn = _duck(db_path)
    try:
        # Pre-aggregate lists in CTEs so the main query is a simple LEFT JOIN
        # (avoids GROUP BY on all anime columns, which DuckDB enforces strictly)
        sql = """
        WITH genre_agg AS (
            SELECT anime_id,
                   list(genre_name ORDER BY genre_name) AS genres
            FROM sl.anime_genres
            GROUP BY anime_id
        ),
        tag_agg AS (
            SELECT anime_id,
                   list(tag_name ORDER BY rank DESC, tag_name) AS tags
            FROM sl.anime_tags
            GROUP BY anime_id
        ),
        studio_agg AS (
            SELECT ast.anime_id,
                   list(s.name ORDER BY ast.is_main DESC, s.name) AS studios
            FROM sl.anime_studios ast
            JOIN sl.studios s ON s.id = ast.studio_id
            GROUP BY ast.anime_id
        )
        SELECT
            a.*,
            COALESCE(g.genres,  []) AS genres,
            COALESCE(t.tags,    []) AS tags,
            COALESCE(s.studios, []) AS studios
        FROM sl.anime a
        LEFT JOIN genre_agg  g ON g.anime_id = a.id
        LEFT JOIN tag_agg    t ON t.anime_id = a.id
        LEFT JOIN studio_agg s ON s.anime_id = a.id
        ORDER BY a.id
        """
        rel = conn.execute(sql)
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 3. Credit aggregation per person / year / role — AKM / feat_career feed
# ---------------------------------------------------------------------------

def agg_credits_per_person_ddb(
    db_path: Path | str | None = None,
) -> list[dict[str, Any]]:
    """Aggregate credits by (person_id, credit_year, role) using DuckDB.

    Replaces the SQLite GROUP BY query used in feat_career_annual:
        SELECT person_id, credit_year, role,
               COUNT(DISTINCT anime_id) AS n_works,
               COUNT(*) AS n_credits
        FROM credits
        WHERE credit_year IS NOT NULL
        GROUP BY person_id, credit_year, role
        ORDER BY person_id, credit_year

    Returns list of dicts with keys:
        person_id, credit_year, role, n_works, n_credits
    """
    conn = _duck(db_path)
    try:
        sql = """
        SELECT
            person_id,
            credit_year,
            role,
            COUNT(DISTINCT anime_id) AS n_works,
            COUNT(*)                 AS n_credits
        FROM sl.credits
        WHERE credit_year IS NOT NULL
        GROUP BY person_id, credit_year, role
        ORDER BY person_id, credit_year
        """
        rel = conn.execute(sql)
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]
    finally:
        conn.close()
