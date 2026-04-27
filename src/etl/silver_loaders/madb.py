"""Mediaarts (madb) BRONZE → SILVER loaders.

Tables: anime_broadcasters / anime_broadcast_schedule /
anime_production_committee / anime_production_companies /
anime_video_releases / anime_original_work_links.

DDL reference: src/db/schema.py `_MADB_SILVER_DDL` (Card 14/02).
"""
from __future__ import annotations

from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# DDL — DuckDB SILVER テーブル 6 本
# ---------------------------------------------------------------------------

_DDL_STATEMENTS = [
    "CREATE SEQUENCE IF NOT EXISTS seq_anime_broadcasters_id",
    """CREATE TABLE IF NOT EXISTS anime_broadcasters (
    id                  INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_broadcasters_id'),
    anime_id            VARCHAR NOT NULL,
    broadcaster_name    VARCHAR NOT NULL,
    is_network_station  INTEGER,
    UNIQUE (anime_id, broadcaster_name)
)""",
    "CREATE INDEX IF NOT EXISTS idx_anime_broadcasters_anime ON anime_broadcasters(anime_id)",
    """CREATE TABLE IF NOT EXISTS anime_broadcast_schedule (
    anime_id  VARCHAR PRIMARY KEY,
    raw_text  VARCHAR NOT NULL
)""",
    "CREATE SEQUENCE IF NOT EXISTS seq_anime_production_committee_id",
    """CREATE TABLE IF NOT EXISTS anime_production_committee (
    id            INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_production_committee_id'),
    anime_id      VARCHAR NOT NULL,
    company_name  VARCHAR NOT NULL,
    role_label    VARCHAR NOT NULL DEFAULT '',
    UNIQUE (anime_id, company_name, role_label)
)""",
    "CREATE INDEX IF NOT EXISTS idx_apc_anime ON anime_production_committee(anime_id)",
    "CREATE SEQUENCE IF NOT EXISTS seq_anime_production_companies_id",
    """CREATE TABLE IF NOT EXISTS anime_production_companies (
    id            INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_production_companies_id'),
    anime_id      VARCHAR NOT NULL,
    company_name  VARCHAR NOT NULL,
    role_label    VARCHAR NOT NULL DEFAULT '',
    is_main       INTEGER NOT NULL DEFAULT 0,
    UNIQUE (anime_id, company_name, role_label)
)""",
    "CREATE INDEX IF NOT EXISTS idx_apco_anime ON anime_production_companies(anime_id)",
    "CREATE SEQUENCE IF NOT EXISTS seq_anime_video_releases_id",
    """CREATE TABLE IF NOT EXISTS anime_video_releases (
    id              INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_video_releases_id'),
    release_madb_id VARCHAR NOT NULL UNIQUE,
    anime_id        VARCHAR,
    media_format    VARCHAR,
    date_published  VARCHAR,
    publisher       VARCHAR,
    product_id      VARCHAR,
    gtin            VARCHAR,
    runtime_min     INTEGER,
    volume_number   VARCHAR,
    release_title   VARCHAR
)""",
    "CREATE INDEX IF NOT EXISTS idx_avr_anime ON anime_video_releases(anime_id)",
    "CREATE SEQUENCE IF NOT EXISTS seq_anime_original_work_links_id",
    """CREATE TABLE IF NOT EXISTS anime_original_work_links (
    id              INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_original_work_links_id'),
    anime_id        VARCHAR NOT NULL,
    work_name       VARCHAR,
    creator_text    VARCHAR,
    series_link_id  VARCHAR,
    UNIQUE (anime_id, work_name)
)""",
    "CREATE INDEX IF NOT EXISTS idx_aow_anime ON anime_original_work_links(anime_id)",
]

# ---------------------------------------------------------------------------
# INSERT SQL — BRONZE → SILVER
# ---------------------------------------------------------------------------

_BROADCASTERS_SQL = """
INSERT INTO anime_broadcasters (anime_id, broadcaster_name, is_network_station)
SELECT DISTINCT
    madb_id,
    name,
    TRY_CAST(is_network_station AS INTEGER)
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL AND name IS NOT NULL
ON CONFLICT (anime_id, broadcaster_name) DO NOTHING
"""

_BROADCAST_SCHEDULE_SQL = """
INSERT INTO anime_broadcast_schedule (anime_id, raw_text)
SELECT DISTINCT madb_id, raw_text
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL AND raw_text IS NOT NULL
ON CONFLICT (anime_id) DO NOTHING
"""

_PROD_COMMITTEE_SQL = """
INSERT INTO anime_production_committee (anime_id, company_name, role_label)
SELECT DISTINCT madb_id, company_name, COALESCE(role_label, '')
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL AND company_name IS NOT NULL
ON CONFLICT (anime_id, company_name, role_label) DO NOTHING
"""

_PROD_COMPANIES_SQL = """
INSERT INTO anime_production_companies (anime_id, company_name, role_label, is_main)
SELECT DISTINCT
    madb_id,
    company_name,
    COALESCE(role_label, ''),
    COALESCE(TRY_CAST(is_main AS INTEGER), 0)
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL AND company_name IS NOT NULL
ON CONFLICT (anime_id, company_name, role_label) DO NOTHING
"""

_VIDEO_RELEASES_SQL = """
INSERT INTO anime_video_releases
    (release_madb_id, anime_id, media_format, date_published, publisher,
     product_id, gtin, runtime_min, volume_number, release_title)
SELECT DISTINCT
    madb_id,
    series_madb_id,
    media_format,
    date_published,
    publisher,
    product_id,
    gtin,
    TRY_CAST(runtime_min AS INTEGER),
    volume_number,
    release_title
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL
ON CONFLICT (release_madb_id) DO NOTHING
"""

_ORIGINAL_WORK_LINKS_SQL = """
INSERT INTO anime_original_work_links (anime_id, work_name, creator_text, series_link_id)
SELECT DISTINCT madb_id, work_name, creator_text, series_link_id
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL
ON CONFLICT (anime_id, work_name) DO NOTHING
"""

# (label, insert_sql, bronze_table_name, silver_table_name)
_LOAD_PLAN: list[tuple[str, str, str, str]] = [
    ("broadcasters",         _BROADCASTERS_SQL,     "broadcasters",         "anime_broadcasters"),
    ("broadcast_schedule",   _BROADCAST_SCHEDULE_SQL, "broadcast_schedule", "anime_broadcast_schedule"),
    ("production_committee", _PROD_COMMITTEE_SQL,   "production_committee", "anime_production_committee"),
    ("production_companies", _PROD_COMPANIES_SQL,   "production_companies", "anime_production_companies"),
    ("video_releases",       _VIDEO_RELEASES_SQL,   "video_releases",       "anime_video_releases"),
    ("original_work_links",  _ORIGINAL_WORK_LINKS_SQL, "original_work_links", "anime_original_work_links"),
]

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all 6 madb SILVER tables (idempotent).

    Safe to call multiple times — all statements use IF NOT EXISTS.
    """
    for stmt in _DDL_STATEMENTS:
        conn.execute(stmt)


def _bronze_glob(bronze_root: Path, table: str) -> str:
    """Return glob path for a mediaarts BRONZE table."""
    return str(bronze_root / "source=mediaarts" / f"table={table}" / "date=*" / "*.parquet")


def integrate(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path | str,
) -> dict[str, int]:
    """Load mediaarts BRONZE parquet → 6 SILVER tables.

    Args:
        conn: Open DuckDB connection with SILVER tables already created
              (call create_tables(conn) first if needed).
        bronze_root: Root path of the BRONZE parquet store
                     (e.g. Path('result/bronze')).

    Returns:
        Dict mapping silver table name → row count after insert.
        Error entries appear as ``"<label>_error": "<message>"`` if any step fails.
    """
    bronze_root = Path(bronze_root)
    create_tables(conn)

    counts: dict[str, int | str] = {}
    for label, sql, bronze_table, _silver_table in _LOAD_PLAN:
        glob = _bronze_glob(bronze_root, bronze_table)
        try:
            conn.execute(sql, [glob])
        except Exception as exc:
            counts[f"{label}_error"] = str(exc)

    for _label, _sql, _bronze_table, silver_table in _LOAD_PLAN:
        counts[silver_table] = conn.execute(
            f"SELECT COUNT(*) FROM {silver_table}"
        ).fetchone()[0]

    return counts
