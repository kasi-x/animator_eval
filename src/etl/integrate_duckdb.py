"""ETL: BRONZE parquet → SILVER duckdb (with atomic swap).

Reads bronze/source=*/table=*/date=*/*.parquet, dedups (latest date wins
per primary key), and writes a new silver.duckdb. Atomic swap via os.replace()
means analysis processes holding the old file inode are never blocked.

Scope (Card 03): anime, persons, credits tables only.
Other silver tables (studios, anime_studios, anime_genres, anime_tags, ...)
are populated by the existing SQLite ETL (src/etl/integrate.py) until
Card 06 removes SQLite entirely.
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
# Full schema (studios, anime_genres, anime_tags, …) lives in models_v2.py
# and is handled by the SQLite ETL until 06_sqlite_decommission.
_DDL = """
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
    updated_at      TIMESTAMP DEFAULT now()
);
"""

# BRONZE anime parquet → SILVER anime table.
# Excludes: score, popularity, favourites, description, cover_*, banner,
#           mal_id, anilist_id, ann_id, allcinema_id, madb_id (external IDs),
#           genres, tags, studios (→ separate tables in full ETL).
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
_CREDITS_SQL = """
INSERT INTO credits
SELECT DISTINCT
    person_id,
    anime_id,
    role,
    raw_role,
    episode,
    COALESCE(evidence_source, source) AS evidence_source,
    now()                             AS updated_at
FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE  person_id IS NOT NULL
  AND  anime_id IS NOT NULL
  AND  role IS NOT NULL
  AND  COALESCE(evidence_source, source) IS NOT NULL
"""


def integrate(
    bronze_root: Path | str | None = None,
    silver_path: Path | str | None = None,
    *,
    memory_limit: str = "2GB",
) -> dict[str, int]:
    """Build a fresh SILVER duckdb from BRONZE parquet, then atomic swap.

    Returns row counts: {"anime": N, "persons": N, "credits": N}.
    Raises FileNotFoundError if no parquet files exist for a table.
    """
    bronze_root = Path(bronze_root or DEFAULT_BRONZE_ROOT)
    silver_path = Path(silver_path or DEFAULT_SILVER_PATH)

    def _glob(table: str) -> str:
        return str(bronze_root / "source=*" / f"table={table}" / "date=*" / "*.parquet")

    anime_glob = _glob("anime")
    persons_glob = _glob("persons")
    credits_glob = _glob("credits")

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

            # credits
            conn.execute(_CREDITS_SQL, [credits_glob])
            counts["credits"] = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
            logger.info("silver_credits", count=counts["credits"])

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
