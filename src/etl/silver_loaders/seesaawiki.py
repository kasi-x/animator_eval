"""SeesaaWiki BRONZE → SILVER extras (Card 14/04).

Tables loaded:
- studios         (existing SILVER table — INSERT only, ON CONFLICT DO NOTHING)
- anime_studios   (existing SILVER table — INSERT only, ON CONFLICT DO NOTHING)
- anime_theme_songs       (new)
- anime_episode_titles    (new)
- anime_gross_studios     (new)
- anime_production_committee (shared with Card 14/02 — CREATE IF NOT EXISTS)
- anime_original_work_info   (new)
- persons extras  (ALTER + UPDATE for columns not in base SILVER DDL)

Hard constraints:
- H1: score / popularity columns NOT present in seesaawiki BRONZE → nothing to worry about.
- H4: credits.evidence_source untouched — credits already integrated in integrate_duckdb.py.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

# ─── DDL (DuckDB-compatible) ──────────────────────────────────────────────────

# Shared with Card 02 (madb) — CREATE IF NOT EXISTS absorbs duplicates.
_DDL_PRODUCTION_COMMITTEE = """
CREATE TABLE IF NOT EXISTS anime_production_committee (
    anime_id     VARCHAR NOT NULL,
    company_name VARCHAR NOT NULL,
    role_label   VARCHAR,
    UNIQUE (anime_id, company_name, role_label)
);
CREATE INDEX IF NOT EXISTS idx_apc_anime
    ON anime_production_committee(anime_id);
"""

_DDL_THEME_SONGS = """
CREATE TABLE IF NOT EXISTS anime_theme_songs (
    anime_id    VARCHAR NOT NULL,
    song_type   VARCHAR,
    song_title  VARCHAR,
    role        VARCHAR,
    name        VARCHAR,
    UNIQUE (anime_id, song_type, song_title, role, name)
);
CREATE INDEX IF NOT EXISTS idx_ats_anime
    ON anime_theme_songs(anime_id);
"""

_DDL_EPISODE_TITLES = """
CREATE TABLE IF NOT EXISTS anime_episode_titles (
    anime_id  VARCHAR NOT NULL,
    episode   INTEGER,
    title     VARCHAR,
    source    VARCHAR NOT NULL DEFAULT 'seesaawiki',
    UNIQUE (anime_id, episode, source)
);
"""

_DDL_GROSS_STUDIOS = """
CREATE TABLE IF NOT EXISTS anime_gross_studios (
    anime_id    VARCHAR NOT NULL,
    studio_name VARCHAR NOT NULL,
    episode     INTEGER,
    UNIQUE (anime_id, studio_name, episode)
);
CREATE INDEX IF NOT EXISTS idx_ags_anime
    ON anime_gross_studios(anime_id);
"""

_DDL_ORIGINAL_WORK_INFO = """
CREATE TABLE IF NOT EXISTS anime_original_work_info (
    anime_id           VARCHAR PRIMARY KEY,
    author             VARCHAR,
    publisher          VARCHAR,
    label              VARCHAR,
    magazine           VARCHAR,
    serialization_type VARCHAR
);
"""

# persons 拡張列: DuckDB の ADD COLUMN IF NOT EXISTS を使用。
# hometown は Card 03 が ALTER → 本カードは UPDATE のみ (ALTER なし)。
_DDL_PERSONS_EXTENSION = [
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS name_native_raw TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS aliases TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS nationality TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS primary_occupations TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS years_active TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS description TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS image_large TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS image_medium TEXT",
    # hometown: no ALTER — Card 03 owns it. UPDATE path handles NULL-safe fill.
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS hometown TEXT",
]

# ─── SQL ──────────────────────────────────────────────────────────────────────

_STUDIOS_SQL = """
INSERT OR IGNORE INTO studios (id, name, anilist_id, is_animation_studio,
                                country_of_origin, favourites, site_url, updated_at)
SELECT DISTINCT
    id,
    COALESCE(name, '')                       AS name,
    TRY_CAST(anilist_id AS INTEGER)          AS anilist_id,
    TRY_CAST(is_animation_studio AS INTEGER) AS is_animation_studio,
    country_of_origin,
    TRY_CAST(favourites AS INTEGER)          AS favourites,
    site_url,
    now()                                    AS updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
)
WHERE _rn = 1
"""

_ANIME_STUDIOS_SQL = """
INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main)
SELECT DISTINCT
    anime_id,
    studio_id,
    COALESCE(TRY_CAST(is_main AS BOOLEAN), FALSE) AS is_main
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND studio_id IS NOT NULL
"""

_THEME_SONGS_SQL = """
INSERT OR IGNORE INTO anime_theme_songs (anime_id, song_type, song_title, role, name)
SELECT DISTINCT
    anime_id,
    song_type,
    song_title,
    role,
    name
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL
"""

_EPISODE_TITLES_SQL = """
INSERT OR IGNORE INTO anime_episode_titles (anime_id, episode, title, source)
SELECT DISTINCT
    anime_id,
    TRY_CAST(episode AS INTEGER) AS episode,
    title,
    'seesaawiki'                 AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL
"""

_GROSS_STUDIOS_SQL = """
INSERT OR IGNORE INTO anime_gross_studios (anime_id, studio_name, episode)
SELECT DISTINCT
    anime_id,
    studio_name,
    TRY_CAST(episode AS INTEGER) AS episode
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND studio_name IS NOT NULL
"""

_PRODUCTION_COMMITTEE_SQL = """
INSERT OR IGNORE INTO anime_production_committee (anime_id, company_name, role_label)
SELECT DISTINCT
    anime_id,
    member_name AS company_name,
    NULL        AS role_label
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND member_name IS NOT NULL
"""

_ORIGINAL_WORK_INFO_SQL = """
INSERT OR IGNORE INTO anime_original_work_info
    (anime_id, author, publisher, label, magazine, serialization_type)
SELECT DISTINCT
    anime_id,
    author,
    publisher,
    label,
    magazine,
    serialization_type
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY anime_id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE anime_id IS NOT NULL
)
WHERE _rn = 1
"""

# NULL-safe fill: seesaawiki data fills SILVER only if the column is currently NULL.
# hometown は ALTER 不要 (Card 03 が事前に ADD する) — COALESCE で NULL-safe に更新。
_PERSONS_EXTRAS_SQL = """
UPDATE persons
SET
    name_native_raw     = COALESCE(persons.name_native_raw,     bronze.name_native_raw),
    aliases             = COALESCE(persons.aliases,             bronze.aliases),
    nationality         = COALESCE(persons.nationality,         bronze.nationality),
    primary_occupations = COALESCE(persons.primary_occupations, bronze.primary_occupations),
    years_active        = COALESCE(persons.years_active,        bronze.years_active),
    hometown            = COALESCE(persons.hometown,            bronze.hometown),
    description         = COALESCE(persons.description,         bronze.description),
    image_large         = COALESCE(persons.image_large,         bronze.image_large),
    image_medium        = COALESCE(persons.image_medium,        bronze.image_medium)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
) AS bronze
WHERE persons.id = bronze.id
  AND bronze._rn = 1
"""


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _glob(bronze_root: Path, table: str) -> str:
    """Return glob path for a seesaawiki BRONZE table."""
    return str(
        bronze_root / "source=seesaawiki" / f"table={table}" / "date=*" / "*.parquet"
    )


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Create new SILVER tables required by this loader."""
    for ddl_block in [
        _DDL_PRODUCTION_COMMITTEE,
        _DDL_THEME_SONGS,
        _DDL_EPISODE_TITLES,
        _DDL_GROSS_STUDIOS,
        _DDL_ORIGINAL_WORK_INFO,
    ]:
        for stmt in ddl_block.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

    for stmt in _DDL_PERSONS_EXTENSION:
        conn.execute(stmt)


# ─── Public API ──────────────────────────────────────────────────────────────


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    """Load SeesaaWiki BRONZE extras into SILVER.

    Args:
        conn: Open DuckDB connection pointing at the SILVER database.
              ``anime``, ``persons``, ``studios``, ``anime_studios`` tables must
              already exist (created by integrate_duckdb.integrate()).
        bronze_root: Root directory of BRONZE parquet partitions.

    Returns:
        Dict mapping table name → row count after load. On per-table errors,
        an ``<table>_error`` key is set to the exception string instead.
    """
    bronze_root = Path(bronze_root)

    _apply_ddl(conn)

    pairs: list[tuple[str, str, str]] = [
        ("studios",              _glob(bronze_root, "studios"),              _STUDIOS_SQL),
        ("anime_studios",        _glob(bronze_root, "anime_studios"),        _ANIME_STUDIOS_SQL),
        ("anime_theme_songs",    _glob(bronze_root, "theme_songs"),          _THEME_SONGS_SQL),
        ("anime_episode_titles", _glob(bronze_root, "episode_titles"),       _EPISODE_TITLES_SQL),
        ("anime_gross_studios",  _glob(bronze_root, "gross_studios"),        _GROSS_STUDIOS_SQL),
        ("anime_production_committee",
                                 _glob(bronze_root, "production_committee"), _PRODUCTION_COMMITTEE_SQL),
        ("anime_original_work_info",
                                 _glob(bronze_root, "original_work_info"),   _ORIGINAL_WORK_INFO_SQL),
        ("persons",              _glob(bronze_root, "persons"),              _PERSONS_EXTRAS_SQL),
    ]

    counts: dict[str, int] = {}
    for silver_table, glob_path, sql in pairs:
        try:
            conn.execute(sql, [glob_path])
        except Exception as exc:
            counts[f"{silver_table}_error"] = str(exc)  # type: ignore[assignment]
            continue
        counts[silver_table] = conn.execute(
            f"SELECT COUNT(*) FROM {silver_table}"
        ).fetchone()[0]

    return counts
