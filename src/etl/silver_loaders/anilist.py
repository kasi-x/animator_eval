"""AniList BRONZE → SILVER extra loaders.

Tables loaded:
- characters    (BRONZE: source=anilist/table=characters)
- character_voice_actors (BRONZE: source=anilist/table=character_voice_actors)
- anime extras  (ALTER 列群: synonyms / country_of_origin / display_*)

DDL for the anime extension columns is declared in src/db/schema.py
"anilist extension" section (_ANILIST_EXTENSION_COLUMNS).

H1 compliance: bare BRONZE columns are mapped exclusively to display_* prefixed
SILVER columns (display_score, display_mean_score, display_favourites, etc.).
"""
from __future__ import annotations

from pathlib import Path

import duckdb

# ─── DDL for SILVER tables that this loader owns ────────────────────────────

_DDL_CHARACTERS = """
CREATE TABLE IF NOT EXISTS characters (
    id            VARCHAR PRIMARY KEY,
    name_ja       VARCHAR NOT NULL DEFAULT '',
    name_en       VARCHAR NOT NULL DEFAULT '',
    aliases       VARCHAR NOT NULL DEFAULT '[]',
    anilist_id    INTEGER,
    image_large   VARCHAR,
    image_medium  VARCHAR,
    description   VARCHAR,
    gender        VARCHAR,
    date_of_birth VARCHAR,
    age           VARCHAR,
    blood_type    VARCHAR,
    favourites    INTEGER,
    site_url      VARCHAR,
    updated_at    TIMESTAMP DEFAULT now(),
    UNIQUE (anilist_id)
);
"""

_DDL_CVA = """
CREATE TABLE IF NOT EXISTS character_voice_actors (
    id             INTEGER,
    character_id   VARCHAR NOT NULL,
    person_id      VARCHAR NOT NULL,
    anime_id       VARCHAR NOT NULL,
    character_role VARCHAR NOT NULL DEFAULT '',
    source         VARCHAR NOT NULL DEFAULT '',
    updated_at     TIMESTAMP DEFAULT now(),
    PRIMARY KEY (character_id, person_id, anime_id)
);
CREATE INDEX IF NOT EXISTS idx_cva_character
    ON character_voice_actors(character_id);
CREATE INDEX IF NOT EXISTS idx_cva_person
    ON character_voice_actors(person_id);
CREATE INDEX IF NOT EXISTS idx_cva_anime
    ON character_voice_actors(anime_id);
"""

# anime 拡張列 — H1: display_* prefix for all subjective columns.
# DuckDB supports ADD COLUMN IF NOT EXISTS.
_DDL_ANIME_EXTENSION = [
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS synonyms TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS country_of_origin TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS is_licensed INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS is_adult INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS hashtag TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS site_url TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS trailer_url TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS trailer_site TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS description TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS cover_large TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS cover_extra_large TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS cover_medium TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS banner TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS external_links_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS airing_schedule_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS relations_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_score REAL",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_mean_score REAL",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_favourites INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_popularity_rank INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rankings_json TEXT",
]

# ─── SQL templates ───────────────────────────────────────────────────────────

_CHARACTERS_SQL = """
INSERT OR IGNORE INTO characters
SELECT
    id,
    COALESCE(name_ja, '')    AS name_ja,
    COALESCE(name_en, '')    AS name_en,
    COALESCE(CAST(aliases AS VARCHAR), '[]') AS aliases,
    TRY_CAST(anilist_id AS INTEGER) AS anilist_id,
    image_large,
    image_medium,
    description,
    gender,
    date_of_birth,
    CAST(age AS VARCHAR)     AS age,
    blood_type,
    TRY_CAST(favourites AS INTEGER) AS favourites,
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

_CVA_SQL = """
INSERT OR IGNORE INTO character_voice_actors
    (character_id, person_id, anime_id, character_role, source, updated_at)
SELECT DISTINCT
    character_id,
    person_id,
    anime_id,
    COALESCE(character_role, '') AS character_role,
    'anilist'                    AS source,
    now()                        AS updated_at
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE character_id IS NOT NULL
  AND person_id    IS NOT NULL
  AND anime_id     IS NOT NULL
"""

_ANIME_EXTRAS_SQL = """
UPDATE anime
SET
    synonyms                = bronze.synonyms,
    country_of_origin       = bronze.country_of_origin,
    is_licensed             = TRY_CAST(bronze.is_licensed AS INTEGER),
    is_adult                = TRY_CAST(bronze.is_adult AS INTEGER),
    hashtag                 = bronze.hashtag,
    site_url                = bronze.site_url,
    trailer_url             = bronze.trailer_url,
    trailer_site            = bronze.trailer_site,
    description             = bronze.description,
    cover_large             = bronze.cover_large,
    cover_extra_large       = bronze.cover_extra_large,
    cover_medium            = bronze.cover_medium,
    banner                  = bronze.banner,
    external_links_json     = bronze.external_links_json,
    airing_schedule_json    = bronze.airing_schedule_json,
    relations_json          = bronze.relations_json,
    display_score           = TRY_CAST(bronze.score AS REAL),
    display_mean_score      = TRY_CAST(bronze.mean_score AS REAL),
    display_favourites      = TRY_CAST(bronze.favourites AS INTEGER),
    display_popularity_rank = TRY_CAST(bronze.popularity_rank AS INTEGER),
    display_rankings_json   = bronze.rankings_json
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  id IS NOT NULL
) AS bronze
WHERE anime.id = bronze.id
  AND bronze._rn = 1
"""


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Create characters / CVA tables and add anime extension columns."""
    for stmt in _DDL_CHARACTERS.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)

    for stmt in _DDL_CVA.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)

    for stmt in _DDL_ANIME_EXTENSION:
        conn.execute(stmt)


def _glob_path(bronze_root: Path, table: str) -> str:
    return str(bronze_root / "source=anilist" / f"table={table}" / "date=*" / "*.parquet")


def _has_parquet(conn: duckdb.DuckDBPyConnection, glob: str) -> bool:
    """Return True if at least one parquet file matches glob."""
    try:
        conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{glob}', union_by_name=true) LIMIT 0"
        )
        return True
    except Exception:
        return False


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    """Load AniList characters / CVA / anime extras into SILVER.

    Args:
        conn: Open DuckDB connection pointing at the SILVER database.
              Must already contain the ``anime`` table (created by integrate_duckdb).
        bronze_root: Root directory of BRONZE parquet partitions.

    Returns:
        Dict with row counts: ``characters``, ``character_voice_actors``,
        ``anime_extras_updated`` (anime rows that now have description).
        Tables skipped when no BRONZE parquet exists will be absent from the dict.
    """
    bronze_root = Path(bronze_root)

    _apply_ddl(conn)

    chars_glob = _glob_path(bronze_root, "characters")
    cva_glob   = _glob_path(bronze_root, "character_voice_actors")
    anime_glob = _glob_path(bronze_root, "anime")

    if _has_parquet(conn, chars_glob):
        conn.execute(_CHARACTERS_SQL, [chars_glob])

    if _has_parquet(conn, cva_glob):
        conn.execute(_CVA_SQL, [cva_glob])

    if _has_parquet(conn, anime_glob):
        conn.execute(_ANIME_EXTRAS_SQL, [anime_glob])

    counts: dict[str, int] = {
        "characters": conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0],
        "character_voice_actors": conn.execute(
            "SELECT COUNT(*) FROM character_voice_actors"
        ).fetchone()[0],
        "anime_extras_updated": conn.execute(
            "SELECT COUNT(*) FROM anime WHERE description IS NOT NULL"
        ).fetchone()[0],
    }
    return counts
