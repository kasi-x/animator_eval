"""MAL/Jikan BRONZE → SILVER loader (Card 14/08).

Tables loaded:
- anime        (mal:a<id> prefix + display_*_mal ALTER columns)
- persons      (mal:p<id> prefix — skipped gracefully when BRONZE absent)
- credits      (staff_credits BRONZE → credits, evidence_source='mal')
- characters   (anime_characters BRONZE, mal:c<id> prefix)
- character_voice_actors (va_credits BRONZE)
- anime_genres
- studios / anime_studios  (name-based ID 'mal:n:' || studio_name)
- anime_relations
- anime_recommendations  (new SILVER table)

H1 compliance:
  All MAL subjective columns use display_*_mal prefix (display_score_mal etc).
  Bare column names without display_ prefix are never written to SILVER.

  rg check:
    rg '\\bscore\\b|\\bpopularity\\b|\\bfavourites\\b|\\bmembers\\b|\\brank\\b'
       src/etl/conformed_loaders/mal.py
    | rg -v 'display_'
    | rg -v '^\\s*#'
  must return 0 lines.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from src.etl.role_mappers import map_role

# ─── DDL ─────────────────────────────────────────────────────────────────────

_DDL_ANIME_GENRES = """
CREATE TABLE IF NOT EXISTS anime_genres (
    anime_id   VARCHAR NOT NULL,
    genre_name VARCHAR NOT NULL,
    PRIMARY KEY (anime_id, genre_name)
);
CREATE INDEX IF NOT EXISTS idx_anime_genres_anime ON anime_genres(anime_id);
"""

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

# ALTER for existing tables that predate the source column.
# DuckDB ADD COLUMN IF NOT EXISTS is idempotent; NOT NULL constraints are not
# allowed in ALTER TABLE ADD COLUMN (DuckDB limitation) so we use DEFAULT ''
# only — loaders always supply an explicit value so NULL never occurs in practice.
_DDL_ANIME_RELATIONS_SOURCE_COL = (
    "ALTER TABLE anime_relations ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT ''"
)

_DDL_ANIME_RECOMMENDATIONS = """
CREATE SEQUENCE IF NOT EXISTS seq_anime_recommendations_id;
CREATE TABLE IF NOT EXISTS anime_recommendations (
    id                   INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_recommendations_id'),
    anime_id             VARCHAR NOT NULL,
    recommended_anime_id VARCHAR NOT NULL,
    votes                INTEGER,
    source               VARCHAR NOT NULL DEFAULT 'mal',
    UNIQUE(anime_id, recommended_anime_id, source)
);
CREATE INDEX IF NOT EXISTS idx_arec_anime ON anime_recommendations(anime_id);
"""

# anime ALTER columns — H1: all subjective/popularity columns use display_*_mal.
_DDL_ANIME_EXTENSION: list[str] = [
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS mal_id_int INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_score_mal REAL",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_popularity_mal INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_members_mal INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_favorites_mal INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rank_mal INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_scored_by_mal INTEGER",
]

# ─── SQL ─────────────────────────────────────────────────────────────────────

# Insert new anime rows from MAL BRONZE.
# ID format: 'mal:a' || mal_id.
# display_*_mal columns are updated in a second pass (_ANIME_UPDATE_SQL)
# after ALTER columns are confirmed to exist.
_ANIME_INSERT_SQL = """
INSERT OR IGNORE INTO anime (id, title_ja, title_en, year, season, episodes,
                              format, duration, start_date, end_date, status,
                              updated_at)
SELECT
    'mal:a' || CAST(b.mal_id AS VARCHAR)            AS id,
    COALESCE(b.title_japanese, b.title, '')          AS title_ja,
    COALESCE(b.title_english, b.title, '')           AS title_en,
    b.anime_year,
    b.season,
    b.ep_count                                       AS episodes,
    b.type                                           AS format,
    NULL                                             AS duration,
    CASE
        WHEN b.aired_from IS NOT NULL AND LENGTH(TRIM(b.aired_from)) >= 10
        THEN SUBSTRING(b.aired_from, 1, 10)
        ELSE NULL
    END                                              AS start_date,
    CASE
        WHEN b.aired_to IS NOT NULL AND LENGTH(TRIM(b.aired_to)) >= 10
        THEN SUBSTRING(b.aired_to, 1, 10)
        ELSE NULL
    END                                              AS end_date,
    b.status,
    now()                                            AS updated_at
FROM (
    SELECT *,
           TRY_CAST(year AS INTEGER)      AS anime_year,
           TRY_CAST(episodes AS INTEGER)  AS ep_count,
           ROW_NUMBER() OVER (PARTITION BY mal_id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  mal_id IS NOT NULL
) AS b
WHERE b._rn = 1
"""

# Update display_*_mal columns on existing anime rows (both newly inserted and
# already present from other sources).  Uses H1-compliant column names.
_ANIME_UPDATE_SQL = """
UPDATE anime
SET
    mal_id_int             = TRY_CAST(b.mal_id AS INTEGER),
    display_score_mal      = TRY_CAST(b.display_score AS REAL),
    display_popularity_mal = TRY_CAST(b.display_popularity AS INTEGER),
    display_members_mal    = TRY_CAST(b.display_members AS INTEGER),
    display_favorites_mal  = TRY_CAST(b.display_favorites AS INTEGER),
    display_rank_mal       = TRY_CAST(b.display_rank AS INTEGER),
    display_scored_by_mal  = TRY_CAST(b.display_scored_by AS INTEGER)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY mal_id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  mal_id IS NOT NULL
) AS b
WHERE anime.id = 'mal:a' || CAST(b.mal_id AS VARCHAR)
  AND b._rn = 1
"""

# persons — insert from MAL BRONZE persons table (absent → 0 rows, no error).
_PERSONS_INSERT_SQL = """
INSERT OR IGNORE INTO persons (id, name_en, name_ja, mal_id, updated_at)
SELECT
    'mal:p' || CAST(mal_id AS VARCHAR)    AS id,
    COALESCE(name, '')                    AS name_en,
    ''                                    AS name_ja,
    TRY_CAST(mal_id AS INTEGER)           AS mal_id,
    now()                                 AS updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY mal_id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  mal_id IS NOT NULL
)
WHERE _rn = 1
"""

# staff_credits → credits.  role mapping via map_role("mal", position).
# Uses a DuckDB Python UDF (map_role_mal) so role normalisation happens inside
# the single SQL INSERT rather than via Python executemany (O(n) queries).
# NOT EXISTS guard prevents duplicates when episode IS NULL (DuckDB treats
# NULL as distinct in UNIQUE constraints, so INSERT OR IGNORE is insufficient).
_STAFF_CREDITS_SQL = """
INSERT INTO credits
    (person_id, anime_id, raw_role, role, evidence_source, updated_at)
SELECT src.person_id, src.anime_id, src.raw_role,
       map_role_mal(src.raw_role) AS role,
       'mal'::VARCHAR             AS evidence_source,
       now()                      AS updated_at
FROM (
    SELECT DISTINCT
        'mal:p' || CAST(mal_person_id AS VARCHAR)  AS person_id,
        'mal:a' || CAST(mal_id AS VARCHAR)         AS anime_id,
        COALESCE(position, '')                     AS raw_role
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE mal_id IS NOT NULL AND mal_person_id IS NOT NULL
) AS src
WHERE NOT EXISTS (
    SELECT 1 FROM credits c
    WHERE c.person_id       = src.person_id
      AND c.anime_id        = src.anime_id
      AND c.raw_role        = src.raw_role
      AND c.evidence_source = 'mal'
      AND c.episode IS NULL
)
"""

_ANIME_GENRES_SQL = """
INSERT OR IGNORE INTO anime_genres (anime_id, genre_name)
SELECT DISTINCT
    'mal:a' || CAST(mal_id AS VARCHAR)  AS anime_id,
    COALESCE(name, '')                  AS genre_name
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE mal_id IS NOT NULL
  AND name IS NOT NULL
"""

_STUDIOS_INSERT_SQL = """
INSERT OR IGNORE INTO studios (id, name, updated_at)
SELECT DISTINCT
    'mal:n:' || name  AS id,
    name,
    now()             AS updated_at
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE name IS NOT NULL
"""

_ANIME_STUDIOS_SQL = """
INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main, role, source)
SELECT DISTINCT
    'mal:a' || CAST(mal_id AS VARCHAR)               AS anime_id,
    'mal:n:' || name                                 AS studio_id,
    CASE WHEN lower(kind) IN ('studio', 'studios') THEN 1
         ELSE 0 END                                  AS is_main,
    ''                                               AS role,
    'mal'                                            AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE mal_id IS NOT NULL
  AND name IS NOT NULL
  AND lower(kind) IN ('studio', 'studios')
"""

_ANIME_RELATIONS_SQL = """
INSERT OR IGNORE INTO anime_relations
    (anime_id, related_anime_id, relation_type, related_title, source)
SELECT DISTINCT
    'mal:a' || CAST(mal_id AS VARCHAR)         AS anime_id,
    'mal:a' || CAST(target_mal_id AS VARCHAR)  AS related_anime_id,
    COALESCE(relation_type, '')                AS relation_type,
    COALESCE(target_name, '')                  AS related_title,
    'mal'                                      AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE mal_id IS NOT NULL
  AND target_mal_id IS NOT NULL
  AND target_type = 'anime'
"""

_ANIME_RECOMMENDATIONS_SQL = """
INSERT OR IGNORE INTO anime_recommendations
    (anime_id, recommended_anime_id, votes, source)
SELECT DISTINCT
    'mal:a' || CAST(mal_id AS VARCHAR)              AS anime_id,
    'mal:a' || CAST(recommended_mal_id AS VARCHAR)  AS recommended_anime_id,
    TRY_CAST(votes AS INTEGER)                      AS votes,
    'mal'                                           AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE mal_id IS NOT NULL
  AND recommended_mal_id IS NOT NULL
"""

# ─── Helpers ─────────────────────────────────────────────────────────────────


def _glob(bronze_root: Path, table: str) -> str:
    """Return glob pattern for a MAL BRONZE table partition."""
    return str(
        bronze_root / "source=mal" / f"table={table}" / "date=*" / "*.parquet"
    )


def _parquet_exists(bronze_root: Path, table: str) -> bool:
    """Return True if at least one parquet file exists for this table."""
    import glob as _glob_mod
    return bool(_glob_mod.glob(_glob(bronze_root, table)))


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Create new SILVER tables and add anime extension columns.

    Idempotent — all CREATE TABLE / CREATE INDEX / CREATE SEQUENCE statements
    use IF NOT EXISTS.  ALTER TABLE … ADD COLUMN IF NOT EXISTS is safe to
    re-run on an existing schema.

    Tables created here (owned by this loader):
        anime_genres, anime_relations, anime_recommendations
    Extension columns added to existing tables:
        anime.mal_id_int, anime.display_*_mal (7 columns)
        anime_relations.source (cross-source tracking, H4)
    """
    for ddl_block in (_DDL_ANIME_GENRES, _DDL_ANIME_RELATIONS, _DDL_ANIME_RECOMMENDATIONS):
        for stmt in ddl_block.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)
    # Backfill source column on tables created before this column existed.
    conn.execute(_DDL_ANIME_RELATIONS_SOURCE_COL)
    for stmt in _DDL_ANIME_EXTENSION:
        conn.execute(stmt)


def _load_staff_credits(
    conn: duckdb.DuckDBPyConnection, bronze_root: Path
) -> int:
    """Load staff_credits from BRONZE → credits using a DuckDB UDF for role mapping.

    Registers map_role_mal UDF on first call (idempotent — silently skips if
    already registered).  Uses a single SQL INSERT with NOT EXISTS guard to
    prevent duplicates on repeated calls.

    Returns total credits rows with evidence_source='mal' after insert.
    """
    if not _parquet_exists(bronze_root, "staff_credits"):
        return 0

    try:
        conn.create_function(
            "map_role_mal",
            lambda r: map_role("mal", r) if r is not None else "other",
            ["VARCHAR"],
            "VARCHAR",
        )
    except Exception:
        pass  # already registered on a second integrate() call

    conn.execute(_STAFF_CREDITS_SQL, [_glob(bronze_root, "staff_credits")])
    return conn.execute(
        "SELECT COUNT(*) FROM credits WHERE evidence_source = 'mal'"
    ).fetchone()[0]


def _load_characters_and_cva(
    conn: duckdb.DuckDBPyConnection, bronze_root: Path
) -> tuple[int, int]:
    """Insert characters and character_voice_actors from MAL BRONZE.

    Uses pure SQL INSERT OR IGNORE — avoids the O(n) Python executemany
    pattern that is prohibitively slow for large datasets (~100k rows).

    Returns (characters_count, cva_count).
    """
    if _parquet_exists(bronze_root, "anime_characters"):
        conn.execute(
            """
            INSERT OR IGNORE INTO characters (id, name_ja, name_en, site_url, updated_at)
            SELECT DISTINCT
                'mal:c' || CAST(mal_character_id AS VARCHAR)  AS id,
                COALESCE(character_name, '')                  AS name_ja,
                ''                                            AS name_en,
                COALESCE(character_url, '')                   AS site_url,
                now()                                         AS updated_at
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY mal_id, mal_character_id ORDER BY date DESC
                       ) AS _rn
                FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
                WHERE  mal_character_id IS NOT NULL
            )
            WHERE _rn = 1
            """,
            [_glob(bronze_root, "anime_characters")],
        )

    if _parquet_exists(bronze_root, "va_credits"):
        conn.execute(
            """
            INSERT OR IGNORE INTO character_voice_actors
                (character_id, person_id, anime_id, character_role, source, updated_at)
            SELECT DISTINCT
                'mal:c' || CAST(mal_character_id AS VARCHAR)  AS character_id,
                'mal:p' || CAST(mal_person_id AS VARCHAR)     AS person_id,
                'mal:a' || CAST(mal_id AS VARCHAR)            AS anime_id,
                COALESCE(language, '')                        AS character_role,
                'mal'                                         AS source,
                now()                                         AS updated_at
            FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
            WHERE mal_id IS NOT NULL
              AND mal_character_id IS NOT NULL
              AND mal_person_id IS NOT NULL
            """,
            [_glob(bronze_root, "va_credits")],
        )

    chars_inserted = conn.execute(
        "SELECT COUNT(*) FROM characters WHERE id LIKE 'mal:c%'"
    ).fetchone()[0]
    cva_inserted = conn.execute(
        "SELECT COUNT(*) FROM character_voice_actors WHERE source = 'mal'"
    ).fetchone()[0]

    return chars_inserted, cva_inserted


# ─── Public API ──────────────────────────────────────────────────────────────


def integrate(
    conn: duckdb.DuckDBPyConnection, bronze_root: Path | str
) -> dict[str, int]:
    """Load MAL BRONZE data into SILVER.

    Args:
        conn: Open DuckDB connection pointing at the SILVER database.
              Must already contain the ``anime`` table (created by integrate_duckdb).
        bronze_root: Root directory of BRONZE parquet partitions.

    Returns:
        Dict with row counts for each loaded table/operation:
        ``anime_inserted``, ``anime_updated``, ``persons_inserted``,
        ``credits_inserted``, ``characters_inserted``,
        ``character_voice_actors_inserted``, ``anime_genres_inserted``,
        ``studios_inserted``, ``anime_studios_inserted``,
        ``anime_relations_inserted``, ``anime_recommendations_inserted``.
    """
    bronze_root = Path(bronze_root)
    _apply_ddl(conn)

    counts: dict[str, int] = {}

    # --- anime ---
    if _parquet_exists(bronze_root, "anime"):
        anime_glob = _glob(bronze_root, "anime")
        conn.execute(_ANIME_INSERT_SQL, [anime_glob])
        conn.execute(_ANIME_UPDATE_SQL, [anime_glob])

    counts["anime_inserted"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE id LIKE 'mal:a%'"
    ).fetchone()[0]
    counts["anime_updated"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE display_score_mal IS NOT NULL"
    ).fetchone()[0]

    # --- persons (gracefully skip when BRONZE absent) ---
    if _parquet_exists(bronze_root, "persons"):
        conn.execute(_PERSONS_INSERT_SQL, [_glob(bronze_root, "persons")])
    counts["persons_inserted"] = conn.execute(
        "SELECT COUNT(*) FROM persons WHERE id LIKE 'mal:p%'"
    ).fetchone()[0]

    # --- staff_credits → credits ---
    counts["credits_inserted"] = _load_staff_credits(conn, bronze_root)

    # --- characters + CVA ---
    chars, cva = _load_characters_and_cva(conn, bronze_root)
    counts["characters_inserted"] = chars
    counts["character_voice_actors_inserted"] = cva

    # --- anime_genres ---
    if _parquet_exists(bronze_root, "anime_genres"):
        conn.execute(_ANIME_GENRES_SQL, [_glob(bronze_root, "anime_genres")])
    counts["anime_genres_inserted"] = conn.execute(
        "SELECT COUNT(*) FROM anime_genres"
    ).fetchone()[0]

    # --- studios + anime_studios ---
    if _parquet_exists(bronze_root, "anime_studios"):
        studios_glob = _glob(bronze_root, "anime_studios")
        conn.execute(_STUDIOS_INSERT_SQL, [studios_glob])
        conn.execute(_ANIME_STUDIOS_SQL, [studios_glob])
    counts["studios_inserted"] = conn.execute(
        "SELECT COUNT(*) FROM studios WHERE id LIKE 'mal:n:%'"
    ).fetchone()[0]
    counts["anime_studios_inserted"] = conn.execute(
        "SELECT COUNT(*) FROM anime_studios"
    ).fetchone()[0]

    # --- anime_relations ---
    if _parquet_exists(bronze_root, "anime_relations"):
        conn.execute(_ANIME_RELATIONS_SQL, [_glob(bronze_root, "anime_relations")])
    counts["anime_relations_inserted"] = conn.execute(
        "SELECT COUNT(*) FROM anime_relations"
    ).fetchone()[0]

    # --- anime_recommendations ---
    if _parquet_exists(bronze_root, "anime_recommendations"):
        conn.execute(
            _ANIME_RECOMMENDATIONS_SQL,
            [_glob(bronze_root, "anime_recommendations")],
        )
    counts["anime_recommendations_inserted"] = conn.execute(
        "SELECT COUNT(*) FROM anime_recommendations"
    ).fetchone()[0]

    return counts
