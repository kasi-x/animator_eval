"""Keyframe BRONZE → SILVER extras (Card 14/06).

Tables loaded / updated:
- person_jobs                  (NEW)  keyframe/table=person_jobs
- person_studio_affiliations   (NEW)  keyframe/table=person_studios
- studios                      existing, INSERT ON CONFLICT DO NOTHING
    - studios_master rows      prefix 'kf:s' + studio_id
    - anime_studios name rows  prefix 'kf:n:' + studio_name
- anime_studios                existing, INSERT ON CONFLICT DO NOTHING
- anime_settings_categories    (NEW)  keyframe/table=settings_categories
- anime                        UPDATE kf_* columns  keyframe/table=anime
- persons                      UPDATE description/image_large (COALESCE: existing wins)
                                keyframe/table=person_profile

Hard-rule compliance:
- H1: no score / popularity / favourites columns touched
- H4: credits table not touched (already loaded by integrate_duckdb)
"""
from __future__ import annotations

from pathlib import Path

import duckdb

# ─── DDL ────────────────────────────────────────────────────────────────────

_DDL_PERSON_JOBS = """
CREATE SEQUENCE IF NOT EXISTS seq_person_jobs;
CREATE TABLE IF NOT EXISTS person_jobs (
    id        BIGINT DEFAULT nextval('seq_person_jobs') PRIMARY KEY,
    person_id TEXT NOT NULL,
    job       TEXT NOT NULL,
    source    TEXT NOT NULL DEFAULT 'keyframe',
    UNIQUE(person_id, job, source)
);
CREATE INDEX IF NOT EXISTS idx_person_jobs_person ON person_jobs(person_id);
"""

_DDL_PERSON_STUDIO_AFFILIATIONS = """
CREATE SEQUENCE IF NOT EXISTS seq_person_studio_affiliations;
CREATE TABLE IF NOT EXISTS person_studio_affiliations (
    id          BIGINT DEFAULT nextval('seq_person_studio_affiliations') PRIMARY KEY,
    person_id   TEXT NOT NULL,
    studio_name TEXT NOT NULL,
    alt_names   TEXT,
    source      TEXT NOT NULL DEFAULT 'keyframe',
    UNIQUE(person_id, studio_name, source)
);
CREATE INDEX IF NOT EXISTS idx_psa_person ON person_studio_affiliations(person_id);
"""

_DDL_ANIME_SETTINGS_CATEGORIES = """
CREATE SEQUENCE IF NOT EXISTS seq_anime_settings_categories;
CREATE TABLE IF NOT EXISTS anime_settings_categories (
    id             BIGINT DEFAULT nextval('seq_anime_settings_categories') PRIMARY KEY,
    anime_id       TEXT NOT NULL,
    category_name  TEXT NOT NULL,
    category_order INTEGER,
    UNIQUE(anime_id, category_name)
);
"""

# anime kf_* ALTER columns — DuckDB supports ADD COLUMN IF NOT EXISTS.
_DDL_ANIME_EXTENSION = [
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_uuid               TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_status             TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_slug               TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_delimiters         TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_episode_delimiters TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_role_delimiters    TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS kf_staff_delimiters   TEXT",
]

# persons extension — description already declared in main schema (schema.py:46) but
# image_large is added here (shared with Card 04 seesaawiki); safe to run multiple times.
_DDL_PERSONS_EXTENSION = [
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS description  TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS image_large  TEXT",
]

# ─── SQL ─────────────────────────────────────────────────────────────────────

_PERSON_JOBS_SQL = """
INSERT OR IGNORE INTO person_jobs (person_id, job, source)
SELECT DISTINCT
    CAST(person_id AS VARCHAR),
    job,
    'keyframe'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE person_id IS NOT NULL
  AND job       IS NOT NULL
"""

_PERSON_STUDIO_AFFILIATIONS_SQL = """
INSERT OR IGNORE INTO person_studio_affiliations (person_id, studio_name, alt_names, source)
SELECT DISTINCT
    CAST(person_id AS VARCHAR),
    studio_name,
    alt_names,
    'keyframe'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE person_id   IS NOT NULL
  AND studio_name IS NOT NULL
"""

_STUDIOS_MASTER_SQL = """
INSERT OR IGNORE INTO studios (id, name)
SELECT DISTINCT
    'kf:s' || CAST(studio_id AS VARCHAR),
    COALESCE(name_ja, name_en, '')
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE studio_id IS NOT NULL
"""

_ANIME_STUDIOS_INSERT_STUDIO_SQL = """
INSERT OR IGNORE INTO studios (id, name)
SELECT DISTINCT
    'kf:n:' || studio_name,
    studio_name
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE studio_name IS NOT NULL
"""

_ANIME_STUDIOS_LINK_SQL = """
INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main, role, source)
SELECT DISTINCT
    anime_id,
    'kf:n:' || studio_name,
    COALESCE(TRY_CAST(is_main AS BOOLEAN), FALSE),
    ''          AS role,
    'keyframe'  AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id    IS NOT NULL
  AND studio_name IS NOT NULL
"""

_SETTINGS_CATEGORIES_SQL = """
INSERT OR IGNORE INTO anime_settings_categories (anime_id, category_name, category_order)
SELECT DISTINCT
    anime_id,
    category_name,
    TRY_CAST(category_order AS INTEGER)
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id      IS NOT NULL
  AND category_name IS NOT NULL
"""

_ANIME_EXTRAS_SQL = """
UPDATE anime
SET
    kf_uuid               = bronze.kf_uuid,
    kf_status             = bronze.kf_status,
    kf_slug               = bronze.slug,
    kf_delimiters         = bronze.delimiters,
    kf_episode_delimiters = bronze.episode_delimiters,
    kf_role_delimiters    = bronze.role_delimiters,
    kf_staff_delimiters   = bronze.staff_delimiters
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  id IS NOT NULL
) AS bronze
WHERE anime.id = bronze.id
  AND bronze._rn = 1
"""

_PERSONS_PROFILE_UPDATE_SQL = """
UPDATE persons
SET
    description = COALESCE(persons.description, bronze.bio),
    image_large = COALESCE(persons.image_large, bronze.avatar)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  person_id IS NOT NULL
      AND  COALESCE(is_studio, FALSE) = FALSE
) AS bronze
WHERE persons.id = CAST(bronze.person_id AS VARCHAR)
  AND bronze._rn = 1
"""


# ─── helpers ─────────────────────────────────────────────────────────────────


def _glob(bronze_root: Path, table: str) -> str:
    """Return glob pattern for keyframe BRONZE parquet files."""
    return str(bronze_root / "source=keyframe" / f"table={table}" / "date=*" / "*.parquet")


def _has_files(bronze_root: Path, table: str) -> bool:
    """Return True when at least one parquet file exists for the given table."""
    return any(
        (bronze_root / "source=keyframe" / f"table={table}").glob("date=*/*.parquet")
    )


def _run_if_files_exist(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    bronze_root: Path,
    table: str,
) -> None:
    """Execute *sql* only when BRONZE parquet files exist for *table*."""
    if _has_files(bronze_root, table):
        conn.execute(sql, [_glob(bronze_root, table)])


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Create new tables and add extension columns."""
    for ddl_block in (_DDL_PERSON_JOBS, _DDL_PERSON_STUDIO_AFFILIATIONS, _DDL_ANIME_SETTINGS_CATEGORIES):
        for stmt in ddl_block.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

    for stmt in _DDL_ANIME_EXTENSION:
        conn.execute(stmt)

    for stmt in _DDL_PERSONS_EXTENSION:
        conn.execute(stmt)


# ─── public API ──────────────────────────────────────────────────────────────


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    """Load keyframe BRONZE extras into SILVER.

    Args:
        conn: Open DuckDB connection pointing at the SILVER database.
              Must already contain anime / persons / studios / anime_studios tables.
        bronze_root: Root directory of BRONZE parquet partitions
                     (e.g. ``result/bronze``).

    Returns:
        Dict with counts for each affected table:
        ``person_jobs``, ``person_studio_affiliations``,
        ``anime_settings_categories``, ``anime_extras_updated``,
        ``persons_profile_updated``.
    """
    bronze_root = Path(bronze_root)

    _apply_ddl(conn)

    _run_if_files_exist(conn, _PERSON_JOBS_SQL,                 bronze_root, "person_jobs")
    _run_if_files_exist(conn, _PERSON_STUDIO_AFFILIATIONS_SQL,  bronze_root, "person_studios")
    _run_if_files_exist(conn, _STUDIOS_MASTER_SQL,              bronze_root, "studios_master")
    _run_if_files_exist(conn, _ANIME_STUDIOS_INSERT_STUDIO_SQL, bronze_root, "anime_studios")
    _run_if_files_exist(conn, _ANIME_STUDIOS_LINK_SQL,          bronze_root, "anime_studios")
    _run_if_files_exist(conn, _SETTINGS_CATEGORIES_SQL,         bronze_root, "settings_categories")
    _run_if_files_exist(conn, _ANIME_EXTRAS_SQL,                bronze_root, "anime")
    _run_if_files_exist(conn, _PERSONS_PROFILE_UPDATE_SQL,      bronze_root, "person_profile")

    return {
        "person_jobs": conn.execute(
            "SELECT COUNT(*) FROM person_jobs"
        ).fetchone()[0],
        "person_studio_affiliations": conn.execute(
            "SELECT COUNT(*) FROM person_studio_affiliations"
        ).fetchone()[0],
        "anime_settings_categories": conn.execute(
            "SELECT COUNT(*) FROM anime_settings_categories"
        ).fetchone()[0],
        "anime_extras_updated": conn.execute(
            "SELECT COUNT(*) FROM anime WHERE kf_uuid IS NOT NULL"
        ).fetchone()[0],
        "persons_profile_updated": conn.execute(
            "SELECT COUNT(*) FROM persons WHERE image_large IS NOT NULL"
        ).fetchone()[0],
    }
