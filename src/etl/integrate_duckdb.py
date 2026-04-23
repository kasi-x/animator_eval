"""ETL: BRONZE parquet → SILVER duckdb (with atomic swap).

Reads bronze/source=*/table=*/date=*/*.parquet, dedups (latest date wins
per primary key), and writes a new silver.duckdb. Atomic swap via os.replace()
means analysis processes holding the old file inode are never blocked.

Core tables (Card 03): anime, persons, credits.
Studio tables (Card 06): studios, anime_studios — loaded when parquet exists,
skipped gracefully otherwise.
Other tables (anime_genres, anime_tags, ...) remain in SQLite ETL.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import structlog

from src.etl.atomic_swap import atomic_duckdb_swap

logger = structlog.get_logger()

DEFAULT_SILVER_PATH = Path(
    os.environ.get("ANIMETOR_SILVER_PATH", "result/silver.duckdb")
)
DEFAULT_BRONZE_ROOT = Path(
    os.environ.get("ANIMETOR_BRONZE_ROOT", "result/bronze")
)

# Minimal DDL for the three core SILVER tables.
# Full BRONZE schema lives in src/db/schema.py.
# Additional SILVER tables (studios, anime_genres, anime_tags, …) are handled
# by separate integration modules.
_DDL = """
CREATE TABLE IF NOT EXISTS studios (
    id                  VARCHAR PRIMARY KEY,
    name                VARCHAR NOT NULL DEFAULT '',
    anilist_id          INTEGER,
    is_animation_studio BOOLEAN,
    country_of_origin   VARCHAR,
    favourites          INTEGER,
    site_url            VARCHAR,
    updated_at          TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS anime_studios (
    anime_id    VARCHAR NOT NULL,
    studio_id   VARCHAR NOT NULL,
    is_main     BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (anime_id, studio_id)
);

CREATE TABLE IF NOT EXISTS anime (
    id          VARCHAR PRIMARY KEY,
    title_ja    VARCHAR NOT NULL DEFAULT '',
    title_en    VARCHAR NOT NULL DEFAULT '',
    year        INTEGER,
    season      VARCHAR,
    quarter     INTEGER,
    episodes    INTEGER,
    format      VARCHAR,
    duration    INTEGER,
    start_date  VARCHAR,
    end_date    VARCHAR,
    status      VARCHAR,
    source_mat  VARCHAR,
    work_type   VARCHAR,
    scale_class VARCHAR,
    fetched_at  TIMESTAMP,
    content_hash VARCHAR,
    updated_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS persons (
    id          VARCHAR PRIMARY KEY,
    name_ja     VARCHAR NOT NULL DEFAULT '',
    name_en     VARCHAR NOT NULL DEFAULT '',
    birth_date  VARCHAR,
    death_date  VARCHAR,
    website_url VARCHAR,
    updated_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS credits (
    person_id       VARCHAR NOT NULL,
    anime_id        VARCHAR NOT NULL,
    role            VARCHAR NOT NULL,
    raw_role        VARCHAR,
    episode         INTEGER,
    evidence_source VARCHAR NOT NULL,
    affiliation     VARCHAR,
    position        INTEGER,
    updated_at      TIMESTAMP DEFAULT now()
);
"""

# BRONZE anime parquet → SILVER anime table.
# Excludes: score, popularity, favourites, description, cover_*, banner,
#           mal_id, anilist_id, ann_id, allcinema_id, madb_id (external IDs),
#           genres, tags, studios (→ separate tables in full ETL).
# Includes: content_hash, fetched_at for diff detection.
# The hive partition adds virtual 'date' column used for dedup.
_ANIME_SQL = """
INSERT INTO anime
SELECT
    id,
    COALESCE(title_ja, '')  AS title_ja,
    COALESCE(title_en, '')  AS title_en,
    year,
    season,
    quarter,
    episodes,
    format,
    duration,
    start_date,
    end_date,
    status,
    COALESCE(original_work_type, source) AS source_mat,
    work_type,
    scale_class,
    TRY_CAST(fetched_at AS TIMESTAMP) AS fetched_at,
    content_hash,
    now()                   AS updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  id IS NOT NULL
)
WHERE _rn = 1
"""

# BRONZE persons → SILVER persons.
# date_of_birth → birth_date, site_url → website_url.
# Template uses {birth_date} and {website_url} placeholders that are filled by
# _build_persons_sql() after inspecting the parquet schema (optional columns vary
# across scrapers; e.g. jvmg writes no date_of_birth).
_PERSONS_SQL_TMPL = """
INSERT INTO persons
SELECT
    id,
    COALESCE(name_ja, '')       AS name_ja,
    COALESCE(name_en, '')       AS name_en,
    {birth_date}                AS birth_date,
    NULL                        AS death_date,
    {website_url}               AS website_url,
    now()                       AS updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  id IS NOT NULL
)
WHERE _rn = 1
"""


def _parquet_columns(conn: duckdb.DuckDBPyConnection, glob: str) -> set[str]:
    """Return column names present in any parquet file matching glob."""
    try:
        rows = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{glob}', hive_partitioning=true, union_by_name=true) LIMIT 0"
        ).fetchall()
        return {row[0] for row in rows}
    except Exception:
        return set()


def _build_persons_sql(conn: duckdb.DuckDBPyConnection, glob: str) -> str:
    cols = _parquet_columns(conn, glob)
    return _PERSONS_SQL_TMPL.format(
        birth_date="date_of_birth" if "date_of_birth" in cols else "NULL::VARCHAR",
        website_url="site_url" if "site_url" in cols else "NULL::VARCHAR",
    )

# BRONZE credits → SILVER credits.
# All credit rows are kept as evidence (no key-level dedup; unique index
# on (person_id, anime_id, role, episode, evidence_source) handles exact dups).
# evidence_source uses the explicit column if present, falls back to 'source'.
# affiliation/position are optional columns — use NULL when absent from parquet
# to avoid DuckDB 1.4.3 binder error (alias self-reference when column missing).
_CREDITS_SQL_TMPL = """
INSERT INTO credits
SELECT person_id, anime_id, role, raw_role, episode,
       evidence_source, affiliation, position, updated_at
FROM (
    SELECT DISTINCT
        person_id,
        anime_id,
        role,
        raw_role,
        episode,
        COALESCE(evidence_source, source) AS evidence_source,
        {affiliation}                     AS affiliation,
        {position}                        AS position,
        now()                             AS updated_at
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  person_id IS NOT NULL
      AND  anime_id IS NOT NULL
      AND  role IS NOT NULL
      AND  COALESCE(evidence_source, source) IS NOT NULL
) sub
"""


def _build_credits_sql(conn: duckdb.DuckDBPyConnection, glob: str) -> str:
    cols = _parquet_columns(conn, glob)
    return _CREDITS_SQL_TMPL.format(
        affiliation="TRY_CAST(affiliation AS VARCHAR)" if "affiliation" in cols else "NULL::VARCHAR",
        position="TRY_CAST(position AS INTEGER)" if "position" in cols else "NULL::INTEGER",
    )


_STUDIOS_SQL = """
INSERT INTO studios
SELECT
    id,
    COALESCE(name, '')       AS name,
    anilist_id,
    is_animation_studio,
    country_of_origin,
    favourites,
    site_url,
    now()                    AS updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  id IS NOT NULL
)
WHERE _rn = 1
"""

_ANIME_STUDIOS_SQL = """
INSERT INTO anime_studios
SELECT DISTINCT
    anime_id,
    studio_id,
    COALESCE(TRY_CAST(is_main AS BOOLEAN), FALSE) AS is_main
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND studio_id IS NOT NULL
"""


def integrate(
    bronze_root: Path | str | None = None,
    silver_path: Path | str | None = None,
    *,
    memory_limit: str = "2GB",
) -> dict[str, int]:
    """Build a fresh SILVER duckdb from BRONZE parquet, then atomic swap.

    Returns row counts for all loaded tables.
    Core tables (anime, persons, credits) raise FileNotFoundError if missing.
    Studio tables (studios, anime_studios) are loaded when parquet exists;
    silently skipped otherwise (not all scrapers write them).
    """
    bronze_root = Path(bronze_root or DEFAULT_BRONZE_ROOT)
    silver_path = Path(silver_path or DEFAULT_SILVER_PATH)

    def _glob(table: str) -> str:
        return str(bronze_root / "source=*" / f"table={table}" / "date=*" / "*.parquet")

    anime_glob = _glob("anime")
    persons_glob = _glob("persons")
    credits_glob = _glob("credits")
    studios_glob = _glob("studios")
    anime_studios_glob = _glob("anime_studios")

    counts: dict[str, int] = {}

    with atomic_duckdb_swap(silver_path) as new_path:
        conn = duckdb.connect(str(new_path))
        try:
            conn.execute(f"SET memory_limit='{memory_limit}'")
            conn.execute("SET temp_directory='/tmp/duckdb_spill'")
            for stmt in _DDL.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(stmt)

            # anime
            conn.execute(_ANIME_SQL, [anime_glob])
            counts["anime"] = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
            logger.info("silver_anime", count=counts["anime"])

            # persons (column mapping is schema-dependent — see _build_persons_sql)
            conn.execute(_build_persons_sql(conn, persons_glob), [persons_glob])
            counts["persons"] = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
            logger.info("silver_persons", count=counts["persons"])

            # credits (column mapping is schema-dependent — see _build_credits_sql)
            conn.execute(_build_credits_sql(conn, credits_glob), [credits_glob])
            counts["credits"] = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
            logger.info("silver_credits", count=counts["credits"])

            # studios + anime_studios (optional — skip if no bronze parquet)
            if _parquet_columns(conn, studios_glob):
                try:
                    conn.execute(_STUDIOS_SQL, [studios_glob])
                    counts["studios"] = conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
                    logger.info("silver_studios", count=counts["studios"])
                except Exception as exc:
                    logger.warning("silver_studios_skip", error=str(exc))

            if _parquet_columns(conn, anime_studios_glob):
                try:
                    conn.execute(_ANIME_STUDIOS_SQL, [anime_studios_glob])
                    counts["anime_studios"] = conn.execute(
                        "SELECT COUNT(*) FROM anime_studios"
                    ).fetchone()[0]
                    logger.info("silver_anime_studios", count=counts["anime_studios"])
                except Exception as exc:
                    logger.warning("silver_anime_studios_skip", error=str(exc))

        finally:
            conn.close()

    return counts


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Integrate BRONZE parquet → SILVER duckdb")
    parser.add_argument("--bronze-root", default=None)
    parser.add_argument("--silver-path", default=None)
    parser.add_argument("--memory-limit", default="2GB")
    args = parser.parse_args()

    counts = integrate(
        bronze_root=args.bronze_root,
        silver_path=args.silver_path,
        memory_limit=args.memory_limit,
    )
    for table, n in counts.items():
        print(f"  {table}: {n:,} rows")


if __name__ == "__main__":
    main()
