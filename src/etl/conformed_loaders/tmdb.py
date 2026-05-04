"""TMDb BRONZE → SILVER conformed loader (Card 14/09).

Tables loaded:
- anime        (tmdb:m<media_type>:<id> prefix rows, e.g. tmdb:movie:12345 / tmdb:tv:67890)
- persons      (tmdb:p<id> prefix rows)
- credits      (evidence_source='tmdb')
- anime extension columns:
    alternative_titles_json / translations_json / imdb_id / tvdb_id (+ display_*_tmdb)

H1 compliance:
  vote_average / vote_count / popularity map exclusively to display_*_tmdb columns.
  Bare column names without display_ prefix are never written to SILVER scoring paths.

  Verification:
    rg '\\b(vote_average|popularity)\\b' src/etl/conformed_loaders/tmdb.py | rg -v 'display_'
  must return 0 lines.
"""
from __future__ import annotations

import glob as _glob_mod
from pathlib import Path

import duckdb

from src.etl.role_mappers import map_role

# ─── DDL — anime extension columns (Card 14/09) ───────────────────────────────

# New columns added to the shared anime SILVER table.
# All subjective TMDb columns carry the display_*_tmdb prefix (H1).
# imdb_id / tvdb_id are structural cross-source keys — no prefix needed.
_DDL_ANIME_EXTENSION: list[str] = [
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS alternative_titles_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS translations_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS imdb_id TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS tvdb_id INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_vote_avg_tmdb REAL",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_vote_count_tmdb INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_popularity_tmdb REAL",
]

# New columns for persons table.
# display_popularity_tmdb isolated from scoring (H1).
_DDL_PERSONS_EXTENSION: list[str] = [
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS tmdb_id INTEGER",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS display_popularity_tmdb REAL",
]

# ─── SQL — anime ──────────────────────────────────────────────────────────────

# Insert new anime rows with tmdb:m<media_type>:<id> primary key.
# Structural fields only; display_*_tmdb updated in a second pass.
# Uses INSERT OR IGNORE so repeated calls are idempotent.
_ANIME_INSERT_SQL = """
INSERT OR IGNORE INTO anime (
    id, title_ja, title_en, year, episodes, format, duration,
    start_date, end_date, status, updated_at
)
SELECT
    'tmdb:' || b.media_type || ':' || CAST(b.tmdb_id AS VARCHAR)  AS id,
    COALESCE(b.original_title, b.title, '')                        AS title_ja,
    COALESCE(b.title, '')                                          AS title_en,
    TRY_CAST(b.year AS INTEGER)                                    AS year,
    TRY_CAST(b.episodes AS INTEGER)                                AS episodes,
    CASE b.media_type
        WHEN 'tv'    THEN 'TV'
        WHEN 'movie' THEN 'Movie'
        ELSE b.media_type
    END                                                            AS format,
    TRY_CAST(b.runtime AS INTEGER)                                 AS duration,
    COALESCE(b.first_air_date, b.release_date)                     AS start_date,
    b.last_air_date                                                AS end_date,
    b.status,
    now()                                                          AS updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY media_type, tmdb_id ORDER BY date DESC
           ) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  tmdb_id IS NOT NULL
      AND  media_type IS NOT NULL
) AS b
WHERE b._rn = 1
"""

# Update extension columns on both newly inserted and pre-existing anime rows.
# Structural: imdb_id, tvdb_id, alternative_titles_json, translations_json.
# Display only: display_vote_avg_tmdb, display_vote_count_tmdb, display_popularity_tmdb.
_ANIME_UPDATE_SQL = """
UPDATE anime
SET
    imdb_id                  = b.imdb_id,
    tvdb_id                  = TRY_CAST(b.tvdb_id AS INTEGER),
    alternative_titles_json  = b.alternative_titles,
    translations_json        = b.translations,
    display_vote_avg_tmdb    = TRY_CAST(b.display_vote_avg AS REAL),
    display_vote_count_tmdb  = TRY_CAST(b.display_vote_count AS INTEGER),
    display_popularity_tmdb  = TRY_CAST(b.display_popularity AS REAL)
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY media_type, tmdb_id ORDER BY date DESC
           ) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  tmdb_id IS NOT NULL
      AND  media_type IS NOT NULL
) AS b
WHERE anime.id = 'tmdb:' || b.media_type || ':' || CAST(b.tmdb_id AS VARCHAR)
  AND b._rn = 1
"""

# ─── SQL — persons ────────────────────────────────────────────────────────────

# Insert persons from TMDb BRONZE.
# H1: display_popularity_tmdb is the only popularity column written here.
# tmdb_id INTEGER extension column stores the integer ID for cross-source join.
_PERSONS_INSERT_SQL = """
INSERT OR IGNORE INTO persons (
    id, name_en, name_ja, tmdb_id, display_popularity_tmdb, updated_at
)
SELECT
    'tmdb:p' || CAST(b.tmdb_id AS VARCHAR)   AS id,
    COALESCE(b.name, '')                      AS name_en,
    ''                                        AS name_ja,
    TRY_CAST(b.tmdb_id AS INTEGER)            AS tmdb_id,
    TRY_CAST(b.display_popularity AS REAL)    AS display_popularity_tmdb,
    now()                                     AS updated_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY tmdb_id ORDER BY date DESC) AS _rn
    FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE  tmdb_id IS NOT NULL
) AS b
WHERE b._rn = 1
"""

# ─── SQL — credits ────────────────────────────────────────────────────────────

# credits SQL is constructed dynamically (after UDF registration).
# Role mapper: map_role("tmdb", role_raw)  — falls back to identity if unmapped.
# person_id = tmdb:p<tmdb_person_id>,  anime_id = tmdb:<media_type>:<tmdb_anime_id>.
# NOT EXISTS guard prevents duplicates when integrate() is called more than once.
_CREDITS_SQL_TMPL = """
INSERT INTO credits
    (person_id, anime_id, role, raw_role, episode, evidence_source, updated_at)
SELECT src.person_id, src.anime_id, src.role, src.raw_role, src.episode,
       'tmdb'::VARCHAR AS evidence_source, now() AS updated_at
FROM (
    SELECT DISTINCT
        'tmdb:p' || CAST(c.tmdb_person_id AS VARCHAR)                      AS person_id,
        'tmdb:' || c.media_type || ':' || CAST(c.tmdb_anime_id AS VARCHAR) AS anime_id,
        map_role_tmdb(c.role_raw)                                           AS role,
        COALESCE(c.role_raw, c.role, '')                                    AS raw_role,
        TRY_CAST(c.episode_count AS INTEGER)                                AS episode
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true) AS c
    WHERE c.tmdb_person_id IS NOT NULL
      AND c.tmdb_anime_id  IS NOT NULL
      AND c.role_raw       IS NOT NULL
) AS src
WHERE NOT EXISTS (
    SELECT 1 FROM credits ex
    WHERE ex.person_id       = src.person_id
      AND ex.anime_id        = src.anime_id
      AND ex.raw_role        = src.raw_role
      AND ex.evidence_source = 'tmdb'
      AND (ex.episode IS NOT DISTINCT FROM src.episode)
)
"""

# ─── Helpers ─────────────────────────────────────────────────────────────────


def _glob(bronze_root: Path, table: str) -> str:
    """Return glob pattern for a TMDb BRONZE table partition."""
    return str(
        bronze_root / "source=tmdb" / f"table={table}" / "date=*" / "*.parquet"
    )


def _parquet_exists(bronze_root: Path, table: str) -> bool:
    """Return True if at least one parquet file exists for this TMDb table."""
    return bool(_glob_mod.glob(_glob(bronze_root, table)))


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Add TMDb extension columns to anime and persons tables.

    All ALTER TABLE … ADD COLUMN IF NOT EXISTS statements are idempotent.
    Silently skips persons extension when the persons table is absent
    (isolated test environments that only create anime).
    """
    for stmt in _DDL_ANIME_EXTENSION:
        conn.execute(stmt)

    for stmt in _DDL_PERSONS_EXTENSION:
        try:
            conn.execute(stmt)
        except Exception:
            pass  # persons table absent in isolated test setups


def _register_credits_udf(conn: duckdb.DuckDBPyConnection) -> None:
    """Register map_role_tmdb DuckDB UDF (idempotent — silently skips if present)."""
    try:
        conn.create_function(
            "map_role_tmdb",
            lambda r: map_role("tmdb", r) if r is not None else "other",
            ["VARCHAR"],
            "VARCHAR",
        )
    except Exception:
        pass  # already registered on repeated integrate() calls


# ─── Public API ──────────────────────────────────────────────────────────────


def integrate(
    conn: duckdb.DuckDBPyConnection, bronze_root: Path | str
) -> dict[str, int]:
    """Load TMDb BRONZE data into SILVER.

    Args:
        conn: Open DuckDB connection pointing at the SILVER database.
              Must already contain the ``anime`` table (created by integrate_duckdb).
        bronze_root: Root directory of BRONZE parquet partitions
                     (e.g. ``result/bronze``).

    Returns:
        Dict with row counts:
        ``anime_inserted`` — anime rows with tmdb: prefix after integrate.
        ``anime_updated``  — anime rows with imdb_id populated after update pass.
        ``persons_inserted`` — persons rows with tmdb:p prefix.
        ``credits_inserted`` — credits rows with evidence_source='tmdb'.
    """
    bronze_root = Path(bronze_root)
    _apply_ddl(conn)

    counts: dict[str, int] = {}

    # ── anime ──
    if _parquet_exists(bronze_root, "anime"):
        anime_glob = _glob(bronze_root, "anime")
        conn.execute(_ANIME_INSERT_SQL, [anime_glob])
        conn.execute(_ANIME_UPDATE_SQL, [anime_glob])

    counts["anime_inserted"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE id LIKE 'tmdb:%'"
    ).fetchone()[0]
    counts["anime_updated"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE imdb_id IS NOT NULL"
    ).fetchone()[0]

    # ── persons ──
    if _parquet_exists(bronze_root, "persons"):
        try:
            conn.execute(_PERSONS_INSERT_SQL, [_glob(bronze_root, "persons")])
        except Exception:
            pass  # persons table may be absent in isolated test environments

    counts["persons_inserted"] = conn.execute(
        "SELECT COUNT(*) FROM persons WHERE id LIKE 'tmdb:p%'"
    ).fetchone()[0]

    # ── credits ──
    if _parquet_exists(bronze_root, "credits"):
        _register_credits_udf(conn)
        conn.execute(_CREDITS_SQL_TMPL, [_glob(bronze_root, "credits")])

    counts["credits_inserted"] = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE evidence_source = 'tmdb'"
    ).fetchone()[0]

    return counts
