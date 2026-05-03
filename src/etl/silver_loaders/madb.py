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

# Insert animation studios into the shared studios table.
# ID format: 'madb:n:' || company_name  (name-based).
_STUDIOS_FROM_PROD_COMPANIES_SQL = """
INSERT OR IGNORE INTO studios (id, name, updated_at)
SELECT DISTINCT
    'madb:n:' || company_name  AS id,
    company_name,
    now()                      AS updated_at
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE company_name IS NOT NULL
  AND (role_label = 'アニメーション制作' OR is_main = true)
"""

# Link anime → animation studio via anime_studios.
# anime_id uses 'madb:' || madb_id (e.g. 'madb:C14591').
# All rows here are from production_companies where is_main=true or role='アニメーション制作'.
_ANIME_STUDIOS_FROM_PROD_COMPANIES_SQL = """
INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main, role, source)
SELECT DISTINCT
    'madb:' || madb_id             AS anime_id,
    'madb:n:' || company_name      AS studio_id,
    COALESCE(TRY_CAST(is_main AS INTEGER), 0) AS is_main,
    COALESCE(role_label, '')       AS role,
    'mediaarts'                    AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE madb_id IS NOT NULL
  AND company_name IS NOT NULL
  AND (role_label = 'アニメーション制作' OR is_main = true)
"""

# anime_studios DDL — ensure table exists (may already exist from integrate_duckdb
# or earlier loader).  CREATE TABLE IF NOT EXISTS is idempotent.
_DDL_ANIME_STUDIOS = """
CREATE TABLE IF NOT EXISTS anime_studios (
    anime_id  VARCHAR NOT NULL,
    studio_id VARCHAR NOT NULL,
    is_main   INTEGER NOT NULL DEFAULT 0,
    role      VARCHAR NOT NULL DEFAULT '',
    source    VARCHAR NOT NULL DEFAULT '',
    PRIMARY KEY (anime_id, studio_id, role, source)
);
CREATE INDEX IF NOT EXISTS idx_anime_studios_anime  ON anime_studios(anime_id);
CREATE INDEX IF NOT EXISTS idx_anime_studios_studio ON anime_studios(studio_id);
"""

_DDL_STUDIOS = """
CREATE TABLE IF NOT EXISTS studios (
    id                  VARCHAR PRIMARY KEY,
    name                VARCHAR NOT NULL DEFAULT '',
    anilist_id          INTEGER,
    is_animation_studio INTEGER,
    country_of_origin   VARCHAR,
    favourites          INTEGER,
    site_url            VARCHAR,
    updated_at          TIMESTAMP DEFAULT now()
);
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
    """Create all madb SILVER tables (idempotent).

    Creates the 6 madb-specific tables plus ensures studios / anime_studios
    exist for the animation-studio linkage added in Card 22/01.
    Safe to call multiple times — all statements use IF NOT EXISTS.
    """
    for stmt in _DDL_STATEMENTS:
        conn.execute(stmt)

    for ddl_block in (_DDL_STUDIOS, _DDL_ANIME_STUDIOS):
        for stmt in ddl_block.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)


def _bronze_glob(bronze_root: Path, table: str) -> str:
    """Return glob path for a mediaarts BRONZE table."""
    return str(bronze_root / "source=mediaarts" / f"table={table}" / "date=*" / "*.parquet")


def integrate(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path | str,
) -> dict[str, int]:
    """Load mediaarts BRONZE parquet → 6 SILVER tables + anime_studios.

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

    # anime_studios: insert studios + anime_studios from production_companies
    # (アニメーション制作 and is_main rows only).
    pc_glob = _bronze_glob(bronze_root, "production_companies")
    try:
        conn.execute(_STUDIOS_FROM_PROD_COMPANIES_SQL, [pc_glob])
    except Exception as exc:
        counts["studios_mediaarts_error"] = str(exc)

    try:
        conn.execute(_ANIME_STUDIOS_FROM_PROD_COMPANIES_SQL, [pc_glob])
    except Exception as exc:
        counts["anime_studios_mediaarts_error"] = str(exc)

    counts["anime_studios_mediaarts"] = conn.execute(
        "SELECT COUNT(DISTINCT anime_id) FROM anime_studios WHERE source = 'mediaarts'"
    ).fetchone()[0]

    return counts
