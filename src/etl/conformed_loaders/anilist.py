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

# anime_relations table — owned here only when not created by a prior loader.
# Includes source column (H4) for cross-source tracking.
_DDL_ANIME_RELATIONS = """
CREATE TABLE IF NOT EXISTS anime_relations (
    id               INTEGER,
    anime_id         VARCHAR NOT NULL,
    related_anime_id VARCHAR NOT NULL,
    relation_type    VARCHAR NOT NULL DEFAULT '',
    related_title    VARCHAR NOT NULL DEFAULT '',
    related_format   VARCHAR,
    source           VARCHAR NOT NULL DEFAULT '',
    PRIMARY KEY (anime_id, related_anime_id, relation_type, source)
);
CREATE INDEX IF NOT EXISTS idx_anime_relations_anime
    ON anime_relations(anime_id);
CREATE INDEX IF NOT EXISTS idx_anime_relations_related
    ON anime_relations(related_anime_id);
"""

# ALTER for tables created by an earlier loader without the source column.
# DuckDB does not allow NOT NULL in ALTER TABLE ADD COLUMN — DEFAULT '' suffices
# because all loaders supply an explicit source value.
_DDL_ANIME_RELATIONS_SOURCE_COL = (
    "ALTER TABLE anime_relations ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT ''"
)

# studios + anime_studios — ensure tables exist (may already exist from integrate_duckdb
# or a prior loader).  All statements use IF NOT EXISTS so they are idempotent.
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

# persons 拡張列 — 22/04: ensure extra columns exist before anilist UPDATE path runs.
# These are also added by bangumi loader, but anilist may run first in integration order.
# DuckDB supports ADD COLUMN IF NOT EXISTS (idempotent).
_DDL_PERSONS_EXTENSION = [
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS gender VARCHAR",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS description TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS image_large VARCHAR",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS image_medium VARCHAR",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS hometown VARCHAR",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS blood_type VARCHAR",
]

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

# ─── SQL: studios + anime_studios ────────────────────────────────────────────

# Load AniList studios from BRONZE studios table.
# These have proper IDs (e.g. 'anilist:s7') already set by the scraper.
_STUDIOS_SQL = """
INSERT OR IGNORE INTO studios (id, name, anilist_id, is_animation_studio,
                                country_of_origin, favourites, site_url, updated_at)
SELECT DISTINCT
    id,
    COALESCE(name, '')                        AS name,
    TRY_CAST(anilist_id AS INTEGER)           AS anilist_id,
    TRY_CAST(is_animation_studio AS BOOLEAN)  AS is_animation_studio,
    country_of_origin,
    TRY_CAST(favourites AS INTEGER)           AS favourites,
    site_url,
    now()                                     AS updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  id IS NOT NULL
)
WHERE _rn = 1
"""

# Load anime_studios from the dedicated BRONZE anime_studios table.
# This is a supplement to what integrate_duckdb.py already loads via the
# cross-source glob; calling it here ensures the loader is self-contained.
_ANIME_STUDIOS_FROM_TABLE_SQL = """
INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main, role, source)
SELECT DISTINCT
    anime_id,
    studio_id,
    COALESCE(TRY_CAST(is_main AS BOOLEAN), FALSE) AS is_main,
    ''        AS role,
    'anilist' AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND studio_id IS NOT NULL
"""

# Extract anime_studios from the anime.studios[] array column.
# The 'studio' column holds the primary (main) studio name; every element of
# the studios[] array is treated as a collaborating studio.
# Studio IDs are name-based ('anilist:n:' || name) because the array contains
# names only (no IDs).  The main studio (anime.studio) gets is_main=1.
_ANIME_STUDIOS_FROM_ARRAY_SQL = """
INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main, role, source)
SELECT DISTINCT
    a.id                                             AS anime_id,
    'anilist:n:' || studio_name                      AS studio_id,
    CASE WHEN studio_name = a.studio THEN 1 ELSE 0 END AS is_main,
    ''                                               AS role,
    'anilist'                                        AS source
FROM (
    SELECT id, studio, unnest(studios) AS studio_name
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
        FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
        WHERE  id IS NOT NULL
          AND  studios IS NOT NULL
          AND  len(studios) > 0
    )
    WHERE _rn = 1
) a
WHERE studio_name IS NOT NULL
  AND studio_name <> ''
"""

# Insert name-based studio stubs for studios that only appear in the array
# (not in the dedicated BRONZE studios table with proper anilist:s<id> IDs).
_STUDIOS_FROM_ARRAY_SQL = """
INSERT OR IGNORE INTO studios (id, name, updated_at)
SELECT DISTINCT
    'anilist:n:' || studio_name  AS id,
    studio_name,
    now()                        AS updated_at
FROM (
    SELECT unnest(studios) AS studio_name
    FROM (
        SELECT studios,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
        FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
        WHERE  id IS NOT NULL
          AND  studios IS NOT NULL
          AND  len(studios) > 0
    )
    WHERE _rn = 1
) t
WHERE studio_name IS NOT NULL
  AND studio_name <> ''
"""

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

# Parse relations_json from the SILVER anime table and insert normalized rows
# into anime_relations with source='anilist'.
#
# relations_json is a JSON array of objects:
#   [{"id": <anilist_id>, "type": "SEQUEL", "title": "...", "format": "TV"}, ...]
#
# Only rows where the related id is an integer are inserted (nulls excluded).
# The PK (anime_id, related_anime_id, relation_type, source) prevents duplicates.
_ANIME_RELATIONS_FROM_JSON_SQL = """
INSERT OR IGNORE INTO anime_relations
    (anime_id, related_anime_id, relation_type, related_title, related_format, source)
WITH expanded AS (
    SELECT
        a.id AS anime_id,
        elem AS rel
    FROM anime a,
    LATERAL (
        SELECT unnest(
            from_json(
                a.relations_json::JSON,
                '[{"id": "BIGINT", "type": "VARCHAR", "title": "VARCHAR", "format": "VARCHAR"}]'
            )
        ) AS elem
    ) t
    WHERE a.relations_json IS NOT NULL
      AND a.relations_json NOT IN ('[]', 'null', '')
      AND a.id LIKE 'anilist:%'
)
SELECT DISTINCT
    anime_id,
    'anilist:' || CAST((rel).id AS VARCHAR) AS related_anime_id,
    COALESCE((rel).type, '')               AS relation_type,
    COALESCE((rel).title, '')              AS related_title,
    (rel).format                           AS related_format,
    'anilist'                              AS source
FROM expanded
WHERE (rel).id IS NOT NULL
"""


def _anime_glob_has_studios_column(
    conn: duckdb.DuckDBPyConnection, glob: str
) -> bool:
    """Return True if the anime BRONZE parquet has a 'studios' array column."""
    try:
        cols = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{glob}', union_by_name=true) LIMIT 0"
        ).fetchall()
        return any(c[0] == "studios" for c in cols)
    except Exception:
        return False


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Create characters / CVA / studios / anime_studios tables and add anime/persons extension columns."""
    for ddl_block in (_DDL_CHARACTERS, _DDL_CVA, _DDL_ANIME_RELATIONS,
                      _DDL_STUDIOS, _DDL_ANIME_STUDIOS):
        for stmt in ddl_block.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

    # Backfill source column on tables created by a prior loader (H4).
    conn.execute(_DDL_ANIME_RELATIONS_SOURCE_COL)

    for stmt in _DDL_ANIME_EXTENSION:
        conn.execute(stmt)

    # 22/04: ensure persons extra columns exist (idempotent — IF NOT EXISTS).
    # Silently skip if the persons table doesn't exist yet (e.g. in isolated test setups
    # that only create the anime table).  When integrate_duckdb runs first it creates
    # persons before calling this loader, so the ALTER never fails in production.
    for stmt in _DDL_PERSONS_EXTENSION:
        try:
            conn.execute(stmt)
        except Exception:
            pass  # persons table absent in isolated test environments


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
    """Load AniList characters / CVA / anime extras / anime_relations into SILVER.

    Args:
        conn: Open DuckDB connection pointing at the SILVER database.
              Must already contain the ``anime`` table (created by integrate_duckdb).
        bronze_root: Root directory of BRONZE parquet partitions.

    Returns:
        Dict with row counts: ``characters``, ``character_voice_actors``,
        ``anime_extras_updated`` (anime rows that now have description),
        ``anime_relations_anilist`` (rows inserted with source='anilist').
        Tables skipped when no BRONZE parquet exists will be absent from the dict.
    """
    bronze_root = Path(bronze_root)

    _apply_ddl(conn)

    chars_glob     = _glob_path(bronze_root, "characters")
    cva_glob       = _glob_path(bronze_root, "character_voice_actors")
    anime_glob     = _glob_path(bronze_root, "anime")
    studios_glob   = _glob_path(bronze_root, "studios")
    as_table_glob  = _glob_path(bronze_root, "anime_studios")

    if _has_parquet(conn, chars_glob):
        conn.execute(_CHARACTERS_SQL, [chars_glob])

    if _has_parquet(conn, cva_glob):
        conn.execute(_CVA_SQL, [cva_glob])

    if _has_parquet(conn, anime_glob):
        conn.execute(_ANIME_EXTRAS_SQL, [anime_glob])

    # Parse relations_json from the (now updated) anime table and insert into
    # anime_relations with source='anilist'.  This runs after _ANIME_EXTRAS_SQL
    # so relations_json is available even when the anime row was created by a
    # different loader and only enriched here.
    conn.execute(_ANIME_RELATIONS_FROM_JSON_SQL)

    # Load studios from the dedicated BRONZE studios table (proper anilist:s<id> IDs).
    if _has_parquet(conn, studios_glob):
        conn.execute(_STUDIOS_SQL, [studios_glob])

    # Load anime_studios from the dedicated BRONZE anime_studios table.
    if _has_parquet(conn, as_table_glob):
        conn.execute(_ANIME_STUDIOS_FROM_TABLE_SQL, [as_table_glob])

    # Extract studios and anime_studios from anime.studios[] array.
    # This covers the majority of anilist anime (15K+) that have studio info
    # embedded in the anime table rather than the anime_studios table.
    # Guard: only run when the BRONZE parquet actually has a 'studios' column
    # (older or synthetic fixtures may lack it).
    if _has_parquet(conn, anime_glob) and _anime_glob_has_studios_column(conn, anime_glob):
        conn.execute(_STUDIOS_FROM_ARRAY_SQL, [anime_glob])
        conn.execute(_ANIME_STUDIOS_FROM_ARRAY_SQL, [anime_glob])

    counts: dict[str, int] = {
        "characters": conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0],
        "character_voice_actors": conn.execute(
            "SELECT COUNT(*) FROM character_voice_actors"
        ).fetchone()[0],
        "anime_extras_updated": conn.execute(
            "SELECT COUNT(*) FROM anime WHERE description IS NOT NULL"
        ).fetchone()[0],
        "anime_relations_anilist": conn.execute(
            "SELECT COUNT(*) FROM anime_relations WHERE source = 'anilist'"
        ).fetchone()[0],
        "anime_studios_anilist": conn.execute(
            "SELECT COUNT(DISTINCT anime_id) FROM anime_studios WHERE source = 'anilist'"
        ).fetchone()[0],
    }
    return counts
