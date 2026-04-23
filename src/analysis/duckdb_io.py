"""DuckDB-backed read paths for analytical queries over the SILVER layer.

Replaces the old ATTACH-SQLite-from-DuckDB pattern with direct DuckDB
native access to silver.duckdb (Card 05 cutover).

All functions use per-query open/close via silver_connect() so the analysis
process never pins to a stale inode after an atomic swap of silver.duckdb.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.analysis.silver_reader import silver_connect


# ---------------------------------------------------------------------------
# 1. Credits — full-table scan
# ---------------------------------------------------------------------------

def load_credits_ddb(silver_path: Path | str | None = None) -> list[dict[str, Any]]:
    """Load all credits rows using DuckDB's vectorized scan.

    Returns list of dicts with columns from the silver credits table.
    Drop-in data-level replacement for the old SQLite credits scan.
    """
    with silver_connect(silver_path) as conn:
        rel = conn.execute("SELECT * FROM credits")
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]


# ---------------------------------------------------------------------------
# 2. Anime with genres/tags/studios — aggregated query
#
# Note: anime_genres, anime_tags, anime_studios are not yet in silver.duckdb
# (Card 06 ETL scope). Until those tables are migrated, genres/tags/studios
# return empty lists. The core anime metadata (format, episodes, duration,
# year, scale_class) is already present in silver.
# ---------------------------------------------------------------------------

def load_anime_joined_ddb(silver_path: Path | str | None = None) -> list[dict[str, Any]]:
    """Load anime rows, with genres/tags/studios aggregated where available.

    If the related tables (anime_genres, anime_tags, anime_studios, studios)
    are not yet present in silver, those fields default to empty lists.
    """
    with silver_connect(silver_path) as conn:
        # Check which related tables exist
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = 'main'"
            ).fetchall()
        }

        if {"anime_genres", "anime_tags", "anime_studios", "studios"} <= existing:
            sql = """
            WITH genre_agg AS (
                SELECT anime_id,
                       list(genre_name ORDER BY genre_name) AS genres
                FROM anime_genres
                GROUP BY anime_id
            ),
            tag_agg AS (
                SELECT anime_id,
                       list(tag_name ORDER BY rank DESC, tag_name) AS tags
                FROM anime_tags
                GROUP BY anime_id
            ),
            studio_agg AS (
                SELECT ast.anime_id,
                       list(s.name ORDER BY ast.is_main DESC, s.name) AS studios
                FROM anime_studios ast
                JOIN studios s ON s.id = ast.studio_id
                GROUP BY ast.anime_id
            )
            SELECT
                a.*,
                COALESCE(g.genres,  []) AS genres,
                COALESCE(t.tags,    []) AS tags,
                COALESCE(s.studios, []) AS studios
            FROM anime a
            LEFT JOIN genre_agg  g ON g.anime_id = a.id
            LEFT JOIN tag_agg    t ON t.anime_id = a.id
            LEFT JOIN studio_agg s ON s.anime_id = a.id
            ORDER BY a.id
            """
        else:
            # Related tables not yet in silver — return anime with empty lists
            sql = """
            SELECT a.*, [] AS genres, [] AS tags, [] AS studios
            FROM anime a
            ORDER BY a.id
            """

        rel = conn.execute(sql)
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]


# ---------------------------------------------------------------------------
# 3. Credit aggregation per person / year / role — AKM / feat_career feed
# ---------------------------------------------------------------------------

def agg_credits_per_person_ddb(
    silver_path: Path | str | None = None,
) -> list[dict[str, Any]]:
    """Aggregate credits by (person_id, credit_year, role) using DuckDB.

    Returns list of dicts with keys: person_id, credit_year, role,
    n_works, n_credits.

    Note: silver credits do not yet store credit_year (Card 06 scope).
    If credit_year is absent from silver, returns an empty list so callers
    can fall back to the SQLite path.
    """
    with silver_connect(silver_path) as conn:
        cols_info = conn.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'credits' AND table_schema = 'main'"
        ).fetchall()
        col_names = {row[0] for row in cols_info}

        if "credit_year" not in col_names:
            return []

        sql = """
        SELECT
            person_id,
            credit_year,
            role,
            COUNT(DISTINCT anime_id) AS n_works,
            COUNT(*)                 AS n_credits
        FROM credits
        WHERE credit_year IS NOT NULL
        GROUP BY person_id, credit_year, role
        ORDER BY person_id, credit_year
        """
        rel = conn.execute(sql)
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]
