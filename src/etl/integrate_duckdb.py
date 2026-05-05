"""ETL: BRONZE parquet → Conformed schema in animetor.duckdb.

Reads bronze/source=*/table=*/date=*/*.parquet, dedups (latest date wins
per primary key), and rebuilds the `conformed` schema inside animetor.duckdb.

Phase 1c: writes go directly to animetor.duckdb (conformed schema). The
mart schema is left untouched. The legacy silver.duckdb file is no longer
written by this module — readers must use animetor.duckdb / conformed.

Core tables (Card 03): anime, persons, credits.
Studio tables (Card 06): studios, anime_studios — loaded when parquet exists,
skipped gracefully otherwise.

Role normalization is performed via src.etl.role_mappers at ETL time:
  credits.role     = normalized Role.value  (e.g. "animation_director")
  credits.raw_role = original string from bronze  (NOT NULL)
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import structlog

from src.etl.role_mappers import map_role
import src.etl.conformed_loaders.anilist as _sl_anilist
import src.etl.conformed_loaders.ann as _sl_ann
import src.etl.conformed_loaders.bangumi as _sl_bangumi
import src.etl.conformed_loaders.keyframe as _sl_keyframe
import src.etl.conformed_loaders.madb as _sl_madb
import src.etl.conformed_loaders.mal as _sl_mal
import src.etl.conformed_loaders.sakuga_atwiki as _sl_sakuga_atwiki
import src.etl.conformed_loaders.seesaawiki as _sl_seesaawiki
import src.etl.conformed_loaders.tmdb as _sl_tmdb

logger = structlog.get_logger()

# Phase 1c: integrate writes to animetor.duckdb (conformed schema).
# DEFAULT_SILVER_PATH kept as alias for backward-compat (tests / CLI).
DEFAULT_DB_PATH = Path(
    os.environ.get("ANIMETOR_DB_PATH", "result/animetor.duckdb")
)
DEFAULT_SILVER_PATH = DEFAULT_DB_PATH
CONFORMED_SCHEMA = "conformed"
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
    role        VARCHAR NOT NULL DEFAULT '',
    source      VARCHAR NOT NULL DEFAULT '',
    PRIMARY KEY (anime_id, studio_id, role, source)
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
    id                  VARCHAR PRIMARY KEY,
    name_ja             VARCHAR NOT NULL DEFAULT '',
    name_en             VARCHAR NOT NULL DEFAULT '',
    name_ko             VARCHAR NOT NULL DEFAULT '',
    name_zh             VARCHAR NOT NULL DEFAULT '',
    names_alt           VARCHAR NOT NULL DEFAULT '{}',
    birth_date          VARCHAR,
    death_date          VARCHAR,
    website_url         VARCHAR,
    gender              VARCHAR,
    language            VARCHAR,
    description         TEXT,
    image_large         VARCHAR,
    image_medium        VARCHAR,
    hometown            VARCHAR,
    blood_type          VARCHAR,
    updated_at          TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS credits (
    person_id       VARCHAR,
    anime_id        VARCHAR,
    role            VARCHAR NOT NULL,
    raw_role        VARCHAR NOT NULL,
    episode         INTEGER,
    evidence_source VARCHAR NOT NULL,
    credit_year     INTEGER,
    credit_quarter  INTEGER,
    affiliation     VARCHAR,
    position        INTEGER,
    updated_at      TIMESTAMP DEFAULT now()
);
"""

# BRONZE anime parquet → SILVER anime table with hash-based diff detection.
# Excludes: score, popularity, favourites, description, cover_*, banner,
#           mal_id, anilist_id, ann_id, madb_id (external IDs),
#           genres, tags, studios (→ separate tables in full ETL).
# Includes: content_hash, fetched_at for diff detection.
# Strategy: DELETE + INSERT (upsert) on primary key (id), skipping rows with unchanged hash.
_ANIME_SQL_DELETE = """
WITH bronze AS (
    SELECT
        id,
        content_hash
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
        FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
        WHERE  id IS NOT NULL
    )
    WHERE _rn = 1
),
to_delete AS (
    SELECT bronze.id FROM bronze
    LEFT JOIN anime old ON bronze.id = old.id
    WHERE bronze.content_hash != old.content_hash OR old.id IS NULL
)
DELETE FROM anime WHERE id IN (SELECT id FROM to_delete)
"""

_ANIME_SQL_INSERT_TMPL = """
WITH bronze AS (
    SELECT
        id,
        COALESCE(title_ja, '')  AS title_ja,
        COALESCE(title_en, '')  AS title_en,
        {year}                  AS year,
        {season}                AS season,
        {quarter}               AS quarter,
        {episodes}              AS episodes,
        {format}                AS format,
        {duration}              AS duration,
        {start_date}            AS start_date,
        {end_date}              AS end_date,
        {status}                AS status,
        {source_mat}            AS source_mat,
        {work_type}             AS work_type,
        {scale_class}           AS scale_class,
        {fetched_at}            AS fetched_at,
        {content_hash}          AS content_hash,
        now()                   AS updated_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
        FROM   read_parquet(?, hive_partitioning=true, union_by_name=true)
        WHERE  id IS NOT NULL
    )
    WHERE _rn = 1
),
filtered AS (
    SELECT bronze.* FROM bronze
    LEFT JOIN anime old ON bronze.id = old.id
    WHERE bronze.content_hash != old.content_hash OR old.id IS NULL
)
INSERT INTO anime
SELECT * FROM filtered
"""

# BRONZE persons → SILVER persons.
# date_of_birth → birth_date, site_url → website_url.
# Template uses placeholders that are filled by _build_persons_sql() after inspecting the parquet schema.
# Extra columns (gender, description, image_large, image_medium, hometown, blood_type) are included
# when present in the BRONZE parquet (anilist supplies all of them).
_PERSONS_SQL_TMPL = """
INSERT INTO persons
SELECT
    id,
    {name_ja}                   AS name_ja,
    {name_en}                   AS name_en,
    {name_ko}                   AS name_ko,
    {name_zh}                   AS name_zh,
    {names_alt}                 AS names_alt,
    {birth_date}                AS birth_date,
    {death_date}                AS death_date,
    {website_url}               AS website_url,
    {gender}                    AS gender,
    {language}                  AS language,
    {description}               AS description,
    {image_large}               AS image_large,
    {image_medium}              AS image_medium,
    {hometown}                  AS hometown,
    {blood_type}                AS blood_type,
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
        name_ja="COALESCE(name_ja, '')" if "name_ja" in cols else "''::VARCHAR",
        name_en="COALESCE(name_en, '')" if "name_en" in cols else "''::VARCHAR",
        name_ko="COALESCE(name_ko, '')" if "name_ko" in cols else "''::VARCHAR",
        name_zh="COALESCE(name_zh, '')" if "name_zh" in cols else "''::VARCHAR",
        names_alt="COALESCE(names_alt, '{}')" if "names_alt" in cols else "'{}'::VARCHAR",
        birth_date="date_of_birth" if "date_of_birth" in cols else "NULL::VARCHAR",
        death_date="date_of_death" if "date_of_death" in cols else "NULL::VARCHAR",
        website_url="site_url" if "site_url" in cols else "NULL::VARCHAR",
        gender="gender" if "gender" in cols else "NULL::VARCHAR",
        language="language" if "language" in cols else "NULL::VARCHAR",
        description="description" if "description" in cols else "NULL::VARCHAR",
        image_large="image_large" if "image_large" in cols else "NULL::VARCHAR",
        image_medium="image_medium" if "image_medium" in cols else "NULL::VARCHAR",
        hometown="hometown" if "hometown" in cols else "NULL::VARCHAR",
        blood_type="blood_type" if "blood_type" in cols else "NULL::VARCHAR",
    )

def _build_credits_insert_seesaawiki(conn: duckdb.DuckDBPyConnection, glob: str) -> str:
    """Load SeesaaWiki credits: (anime_id, role, name, position, episode, affiliation).

    Registers a DuckDB UDF that delegates to map_role("seesaawiki", raw).
    raw_role is set to the original role string from bronze; role is the
    normalized Role.value after mapper application.

    affiliation is optional in bronze (older parquets may not have it);
    checked via schema introspection and substituted with NULL if absent.
    """
    cols = _parquet_columns(conn, glob)
    affiliation_expr = "src.affiliation" if "affiliation" in cols else "NULL::VARCHAR"

    conn.create_function(
        "map_role_seesaawiki",
        lambda r: map_role("seesaawiki", r) if r is not None else "other",
        ["VARCHAR"],
        "VARCHAR",
    )

    return f"""
INSERT INTO credits
WITH src AS (
    SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE source = 'seesaawiki'
)
SELECT DISTINCT
    NULL::VARCHAR                                AS person_id,
    src.anime_id,
    map_role_seesaawiki(src.role)                AS role,
    COALESCE(src.role, '')                       AS raw_role,
    src.episode,
    'seesaawiki'::VARCHAR                        AS evidence_source,
    NULL::INTEGER                                AS credit_year,
    NULL::INTEGER                                AS credit_quarter,
    {affiliation_expr}                           AS affiliation,
    TRY_CAST(src.position AS INTEGER)            AS position,
    now()                                        AS updated_at
FROM src
WHERE src.anime_id IS NOT NULL AND src.role IS NOT NULL
"""


def _build_credits_insert_anilist(conn: duckdb.DuckDBPyConnection, glob: str) -> str:
    """Load AniList credits: (person_id, anime_id, role, raw_role, episode, affiliation, position).

    AniList bronze writes a normalized Role.value in the `role` column and the
    original English string in `raw_role`.  The mapper re-validates the value so
    any stale non-canonical strings are corrected at ETL time.

    affiliation and position are INTEGER in the AniList parquet schema but NULL
    in all known data; cast to VARCHAR / INTEGER defensively.
    """
    conn.create_function(
        "map_role_anilist",
        lambda r: map_role("anilist", r) if r is not None else "other",
        ["VARCHAR"],
        "VARCHAR",
    )

    return """
INSERT INTO credits
WITH src AS (
    SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE source = 'anilist'
)
SELECT DISTINCT
    src.person_id,
    src.anime_id,
    map_role_anilist(src.role)                              AS role,
    COALESCE(TRY_CAST(src.raw_role AS VARCHAR), src.role, '') AS raw_role,
    src.episode,
    'anilist'::VARCHAR                                      AS evidence_source,
    NULL::INTEGER                                           AS credit_year,
    NULL::INTEGER                                           AS credit_quarter,
    TRY_CAST(src.affiliation AS VARCHAR)                    AS affiliation,
    TRY_CAST(src.position AS INTEGER)                       AS position,
    now()                                                   AS updated_at
FROM src
WHERE src.person_id IS NOT NULL AND src.anime_id IS NOT NULL AND src.role IS NOT NULL
"""


def _build_credits_insert_ann(conn: duckdb.DuckDBPyConnection, glob: str) -> str:
    """Load ANN credits: (ann_person_id, ann_anime_id, role, task_raw, gid).

    ANN uses integer IDs for persons and anime.  Silver uses VARCHAR IDs
    throughout, so we prefix them: 'ann:p<id>' and 'ann:a<id>'.

    The `role` column already contains a normalized Role.value string (mapped
    at scrape time).  `task_raw` holds the original English task label and is
    used as raw_role to satisfy the NOT NULL constraint.

    episode, affiliation, and position are not available in ANN bronze.
    """
    conn.create_function(
        "map_role_ann",
        lambda r: map_role("ann", r) if r is not None else "other",
        ["VARCHAR"],
        "VARCHAR",
    )

    return """
INSERT INTO credits
WITH src AS (
    SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE source = 'ann'
)
SELECT DISTINCT
    'ann:p' || CAST(src.ann_person_id AS VARCHAR) AS person_id,
    'ann:a' || CAST(src.ann_anime_id  AS VARCHAR) AS anime_id,
    map_role_ann(src.role)                        AS role,
    COALESCE(src.task_raw, src.role, '')          AS raw_role,
    NULL::INTEGER                                 AS episode,
    'ann'::VARCHAR                                AS evidence_source,
    NULL::INTEGER                                 AS credit_year,
    NULL::INTEGER                                 AS credit_quarter,
    NULL::VARCHAR                                 AS affiliation,
    NULL::INTEGER                                 AS position,
    now()                                         AS updated_at
FROM src
WHERE src.ann_person_id IS NOT NULL AND src.ann_anime_id IS NOT NULL AND src.role IS NOT NULL
"""


def _build_credits_insert_keyframe(conn: duckdb.DuckDBPyConnection, glob: str) -> str:
    """Load Keyframe credits: person credits only (is_studio_role = FALSE or NULL).

    Keyframe bronze writes a normalized Role.value in the `role` column and the
    original Japanese string in `raw_role`.  The mapper re-validates via the
    shared ROLE_MAP.

    is_studio_role = TRUE rows are excluded: studio credits are tracked separately
    and should not pollute the person-level credits table.

    episode = -1 in bronze means 'unknown episode'; converted to NULL here.
    affiliation and position are INTEGER in the Keyframe schema but unused.
    """
    conn.create_function(
        "map_role_keyframe",
        lambda r: map_role("keyframe", r) if r is not None else "other",
        ["VARCHAR"],
        "VARCHAR",
    )

    return """
INSERT INTO credits
WITH src AS (
    SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE source = 'keyframe'
      AND (is_studio_role IS NULL OR is_studio_role = FALSE)
)
SELECT DISTINCT
    src.person_id,
    src.anime_id,
    map_role_keyframe(src.role)                                    AS role,
    COALESCE(src.raw_role, src.role, '')                           AS raw_role,
    CASE WHEN src.episode = -1 THEN NULL ELSE src.episode END      AS episode,
    'keyframe'::VARCHAR                                            AS evidence_source,
    NULL::INTEGER                                                  AS credit_year,
    NULL::INTEGER                                                  AS credit_quarter,
    NULL::VARCHAR                                                  AS affiliation,
    NULL::INTEGER                                                  AS position,
    now()                                                          AS updated_at
FROM src
WHERE src.person_id IS NOT NULL AND src.anime_id IS NOT NULL AND src.role IS NOT NULL
"""


def _build_credits_insert_sakuga_atwiki(conn: duckdb.DuckDBPyConnection, glob: str) -> str:
    """Load Sakuga@wiki credits: person_id prefixed, anime_id intentionally NULL.

    Sakuga@wiki bronze does not carry an anime_id — only a work_title string.
    Silver.credits has no NOT NULL constraint on anime_id in the DuckDB DDL,
    so these rows are inserted with anime_id = NULL.

    TODO: resolve work_title → silver anime_id via title-matching ETL step
    (entity resolution §5) so that sakuga_atwiki credits can be linked to anime.

    person_id is formed from person_page_id: 'sakuga:p<id>'.
    episode_num is used for episode; role_raw is both the mapper input and raw_role.
    """
    conn.create_function(
        "map_role_sakuga_atwiki",
        lambda r: map_role("sakuga_atwiki", r) if r is not None else "other",
        ["VARCHAR"],
        "VARCHAR",
    )

    return """
INSERT INTO credits
WITH src AS (
    SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE source = 'sakuga_atwiki'
)
SELECT DISTINCT
    'sakuga:p' || CAST(src.person_page_id AS VARCHAR) AS person_id,
    NULL::VARCHAR                                      AS anime_id,
    map_role_sakuga_atwiki(src.role_raw)               AS role,
    COALESCE(src.role_raw, '')                         AS raw_role,
    TRY_CAST(src.episode_num AS INTEGER)               AS episode,
    'sakuga_atwiki'::VARCHAR                           AS evidence_source,
    NULL::INTEGER                                      AS credit_year,
    NULL::INTEGER                                      AS credit_quarter,
    NULL::VARCHAR                                      AS affiliation,
    NULL::INTEGER                                      AS position,
    now()                                              AS updated_at
FROM src
WHERE src.person_page_id IS NOT NULL AND src.role_raw IS NOT NULL
"""


def _build_credits_insert_mediaarts(conn: duckdb.DuckDBPyConnection, glob: str) -> str:
    """Load MediaArts credits: (person_id, anime_id, role, raw_role).

    Registers a DuckDB UDF that delegates to map_role("mediaarts", raw).
    MediaArts bronze already provides raw_role; if missing, role is used
    as the fallback to satisfy the NOT NULL constraint.
    """
    cols = _parquet_columns(conn, glob)
    raw_role_expr = "COALESCE(src.raw_role, src.role)" if "raw_role" in cols else "src.role"

    conn.create_function(
        "map_role_mediaarts",
        lambda r: map_role("mediaarts", r) if r is not None else "other",
        ["VARCHAR"],
        "VARCHAR",
    )

    return f"""
INSERT INTO credits
WITH src AS (
    SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE source = 'mediaarts'
)
SELECT DISTINCT
    src.person_id,
    src.anime_id,
    map_role_mediaarts(src.role)          AS role,
    {raw_role_expr}                       AS raw_role,
    NULL::INTEGER                         AS episode,
    'mediaarts'::VARCHAR                  AS evidence_source,
    NULL::INTEGER                         AS credit_year,
    NULL::INTEGER                         AS credit_quarter,
    NULL::VARCHAR                         AS affiliation,
    NULL::INTEGER                         AS position,
    now()                                 AS updated_at
FROM src
WHERE src.person_id IS NOT NULL AND src.anime_id IS NOT NULL AND src.role IS NOT NULL
"""


def _build_anime_sql(conn: duckdb.DuckDBPyConnection, glob: str) -> str:
    """Build anime INSERT SQL with schema-dependent column mapping."""
    cols = _parquet_columns(conn, glob)
    return _ANIME_SQL_INSERT_TMPL.format(
        year="year" if "year" in cols else "NULL::INTEGER",
        season="season" if "season" in cols else "NULL::VARCHAR",
        quarter="quarter" if "quarter" in cols else "NULL::INTEGER",
        episodes="episodes" if "episodes" in cols else "NULL::INTEGER",
        format="format" if "format" in cols else "NULL::VARCHAR",
        duration="duration" if "duration" in cols else "NULL::INTEGER",
        start_date="start_date" if "start_date" in cols else "NULL::VARCHAR",
        end_date="end_date" if "end_date" in cols else "NULL::VARCHAR",
        status="status" if "status" in cols else "NULL::VARCHAR",
        source_mat="COALESCE(TRY_CAST(original_work_type AS VARCHAR), TRY_CAST(source AS VARCHAR))" if "original_work_type" in cols else "'unknown'::VARCHAR",
        work_type="work_type" if "work_type" in cols else "NULL::VARCHAR",
        scale_class="scale_class" if "scale_class" in cols else "NULL::VARCHAR",
        fetched_at="TRY_CAST(fetched_at AS TIMESTAMP)" if "fetched_at" in cols else "NULL::TIMESTAMP",
        content_hash="content_hash" if "content_hash" in cols else "NULL::VARCHAR",
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
INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main, role, source)
SELECT DISTINCT
    anime_id,
    studio_id,
    COALESCE(TRY_CAST(is_main AS BOOLEAN), FALSE) AS is_main,
    ''                                             AS role,
    COALESCE(source, '')                           AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE anime_id IS NOT NULL AND studio_id IS NOT NULL
"""


def integrate(
    bronze_root: Path | str | None = None,
    silver_path: Path | str | None = None,
    *,
    memory_limit: str = "2GB",
) -> dict[str, int]:
    """Rebuild conformed schema in animetor.duckdb from BRONZE parquet.

    Phase 1c: writes go directly to animetor.duckdb (conformed schema). The
    mart schema is preserved across rebuilds. Atomic file swap is no longer
    used because animetor.duckdb hosts both conformed and mart schemas.

    The conformed schema is dropped and recreated each run (full rebuild).

    Returns row counts for all loaded tables.
    Core tables (anime, persons, credits) raise FileNotFoundError if missing.
    Studio tables (studios, anime_studios) are loaded when parquet exists;
    silently skipped otherwise (not all scrapers write them).
    """
    bronze_root = Path(bronze_root or DEFAULT_BRONZE_ROOT)
    db_path = Path(silver_path or DEFAULT_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Replace stale/corrupt files (old atomic_swap behaviour preserved):
    # if the file exists but is not a readable duckdb, drop and recreate.
    if db_path.exists():
        try:
            _probe = duckdb.connect(str(db_path), read_only=True)
            _probe.close()
        except Exception:
            db_path.unlink()

    def _glob(table: str) -> str:
        return str(bronze_root / "source=*" / f"table={table}" / "date=*" / "*.parquet")

    anime_glob = _glob("anime")
    persons_glob = _glob("persons")
    credits_glob = _glob("credits")
    studios_glob = _glob("studios")
    anime_studios_glob = _glob("anime_studios")

    counts: dict[str, int] = {}

    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(f"SET memory_limit='{memory_limit}'")
        conn.execute("SET temp_directory='/tmp/duckdb_spill'")
        # Phase 1c: full rebuild of conformed schema. mart/main untouched.
        conn.execute(f"DROP SCHEMA IF EXISTS {CONFORMED_SCHEMA} CASCADE")
        conn.execute(f"CREATE SCHEMA {CONFORMED_SCHEMA}")
        conn.execute(f"SET schema='{CONFORMED_SCHEMA}'")

        for stmt in _DDL.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

        # anime (delete stale, then insert new)
        conn.execute(_ANIME_SQL_DELETE, [anime_glob])
        conn.execute(_build_anime_sql(conn, anime_glob), [anime_glob])
        counts["anime"] = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
        logger.info("silver_anime", count=counts["anime"])

        # persons (column mapping is schema-dependent — see _build_persons_sql)
        conn.execute(_build_persons_sql(conn, persons_glob), [persons_glob])
        counts["persons"] = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        logger.info("silver_persons", count=counts["persons"])

        # credits — one loader per source; failures are isolated so a bad
        # source does not prevent other sources from loading.
        _credits_loaders = [
            ("seesaawiki", _build_credits_insert_seesaawiki),
            ("mediaarts", _build_credits_insert_mediaarts),
            ("anilist", _build_credits_insert_anilist),
            ("ann", _build_credits_insert_ann),
            ("keyframe", _build_credits_insert_keyframe),
            ("sakuga_atwiki", _build_credits_insert_sakuga_atwiki),
        ]
        for source_name, builder in _credits_loaders:
            try:
                sql = builder(conn, credits_glob)
                conn.execute(sql, [credits_glob])
            except Exception as exc:
                logger.warning(
                    "silver_credits_source_skip",
                    source=source_name,
                    error=str(exc),
                )
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

        # ── Card 14: source-specific SILVER extras ──────────────────────
        # Each loader's integrate(conn, bronze_root) applies its own DDL
        # (IF NOT EXISTS) and data inserts.  Failures are isolated so a
        # single bad source does not abort the others.
        _source_loaders = [
            ("seesaawiki", _sl_seesaawiki),
            ("madb", _sl_madb),
            ("anilist", _sl_anilist),
            ("ann", _sl_ann),
            ("bangumi", _sl_bangumi),
            ("keyframe", _sl_keyframe),
            ("mal", _sl_mal),
            ("sakuga_atwiki", _sl_sakuga_atwiki),
            ("tmdb", _sl_tmdb),
        ]
        for source_name, loader_module in _source_loaders:
            try:
                source_counts = loader_module.integrate(conn, bronze_root)
                for key, val in source_counts.items():
                    counts[f"{source_name}.{key}"] = val
                logger.info(
                    "silver_source_loaded",
                    source=source_name,
                    tables=list(source_counts.keys()),
                )
            except Exception as exc:
                logger.warning(
                    "silver_source_skip",
                    source=source_name,
                    error=str(exc),
                )
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
        if isinstance(n, int):
            print(f"  {table}: {n:,} rows")
        else:
            print(f"  {table}: {n}")  # error string 等


if __name__ == "__main__":
    main()
