"""ANN BRONZE → SILVER extras.

Tables loaded:
- anime           (INSERT: ANN rows; UPDATE: ANN extra columns)
- persons         (INSERT: ANN rows; UPDATE: ANN extra columns)
- anime_episodes  (INSERT from BRONZE episodes)
- anime_companies (INSERT from BRONZE company)
- anime_releases  (INSERT from BRONZE releases)
- anime_news      (INSERT from BRONZE news)
- anime_relations (INSERT from BRONZE related)
- character_voice_actors (INSERT from BRONZE cast)

DDL for new tables is declared inline (_DDL_* constants) and applied by
_apply_ddl() before any data operations.

H1 compliance: display_rating_votes / display_rating_weighted /
display_rating_bayesian are prefixed display_rating_* — they do not appear
bare in SILVER and are excluded from all scoring paths.

ANN ID prefix convention (matches integrate_duckdb._build_credits_insert_ann):
  anime   → 'ann:a' || ann_anime_id
  persons → 'ann:p' || ann_id / ann_person_id
  cast    → character 'ann:c' || character_id
"""
from __future__ import annotations

from pathlib import Path

import duckdb

# ─── DDL: new SILVER tables owned by this loader ────────────────────────────

_DDL_ANIME_EPISODES = """
CREATE TABLE IF NOT EXISTS anime_episodes (
    id          INTEGER,
    anime_id    VARCHAR NOT NULL,
    episode_num VARCHAR NOT NULL DEFAULT '',
    lang        VARCHAR NOT NULL DEFAULT '',
    title       VARCHAR,
    aired_date  INTEGER,
    PRIMARY KEY (anime_id, episode_num, lang)
);
CREATE INDEX IF NOT EXISTS idx_anime_episodes_anime
    ON anime_episodes(anime_id);
"""

_DDL_ANIME_COMPANIES = """
CREATE TABLE IF NOT EXISTS anime_companies (
    id           INTEGER,
    anime_id     VARCHAR NOT NULL,
    company_name VARCHAR NOT NULL,
    task         VARCHAR,
    company_id   VARCHAR,
    source       VARCHAR NOT NULL DEFAULT 'ann',
    PRIMARY KEY (anime_id, company_name, task)
);
CREATE INDEX IF NOT EXISTS idx_anime_companies_anime
    ON anime_companies(anime_id);
"""

_DDL_ANIME_RELEASES = """
CREATE TABLE IF NOT EXISTS anime_releases (
    id            INTEGER,
    anime_id      VARCHAR NOT NULL,
    product_title VARCHAR,
    release_date  VARCHAR,
    href          VARCHAR,
    region        VARCHAR,
    source        VARCHAR NOT NULL DEFAULT 'ann',
    PRIMARY KEY (anime_id, product_title, release_date)
);
CREATE INDEX IF NOT EXISTS idx_anime_releases_anime
    ON anime_releases(anime_id);
"""

_DDL_ANIME_NEWS = """
CREATE TABLE IF NOT EXISTS anime_news (
    id       INTEGER,
    anime_id VARCHAR NOT NULL,
    datetime VARCHAR,
    title    VARCHAR,
    href     VARCHAR,
    source   VARCHAR NOT NULL DEFAULT 'ann',
    PRIMARY KEY (anime_id, href)
);
CREATE INDEX IF NOT EXISTS idx_anime_news_anime
    ON anime_news(anime_id);
"""

# anime_relations — ensure table exists in DuckDB (may already exist if
# anilist or madb loader ran first; CREATE TABLE IF NOT EXISTS is idempotent).
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

# ALTER for tables that predate the source column (idempotent via IF NOT EXISTS).
# DuckDB does not allow NOT NULL in ALTER TABLE ADD COLUMN — DEFAULT '' suffices
# because all loaders supply an explicit source value.
_DDL_ANIME_RELATIONS_SOURCE_COL = (
    "ALTER TABLE anime_relations ADD COLUMN IF NOT EXISTS source VARCHAR DEFAULT ''"
)

# character_voice_actors — ensure table exists (may already exist if anilist
# loader ran first).
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

# studios + anime_studios — ensure tables exist (may already exist from integrate_duckdb
# or an earlier loader).  CREATE TABLE IF NOT EXISTS is idempotent.
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

# anime 拡張列 — H1: display_rating_* prefix for all ANN rating columns.
# DuckDB supports ADD COLUMN IF NOT EXISTS.
_DDL_ANIME_EXTENSION = [
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS themes TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS plot_summary TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS running_time_raw TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS objectionable_content TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS opening_themes_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS ending_themes_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS insert_songs_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS official_websites_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS vintage_raw TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS image_url TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rating_votes INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rating_weighted REAL",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rating_bayesian REAL",
    # Card 20/03: _ann suffix aliases for cross-source disambiguation (H1).
    # display_rating_count_ann  ← display_rating_votes  (vote count)
    # display_rating_avg_ann    ← display_rating_weighted (weighted mean, best proxy for avg)
    # display_rating_weighted_ann ← display_rating_weighted
    # display_rating_bayesian_ann ← display_rating_bayesian
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rating_count_ann INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rating_avg_ann REAL",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rating_weighted_ann REAL",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rating_bayesian_ann REAL",
]

# persons 拡張列
_DDL_PERSONS_EXTENSION = [
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS gender TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS height_raw TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS family_name_ja TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS given_name_ja TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS hometown TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS image_url_ann TEXT",
]

# ─── SQL: data operations ───────────────────────────────────────────────────

# INSERT ANN anime rows / persons rows are built dynamically by
# _build_anime_insert_sql() and _build_persons_insert_sql() (see helpers section).
#
# Key design constraints that drive dynamic construction:
# 1. DuckDB binder quirk: an explicit INSERT column list clashes with SELECT
#    aliases that share the same identifier (e.g. 'year', 'format', 'start_date').
#    Solution: use `INSERT OR IGNORE ... BY NAME`.
# 2. `BY NAME` requires every alias in the SELECT to exist in the target table.
#    Solution: only emit aliases for columns present in the SILVER table
#    (introspected via PRAGMA table_info).
# 3. Test fixtures only write a subset of BRONZE columns.
#    Solution: only reference BRONZE columns that exist in the parquet
#    (introspected via DESCRIBE).
# 4. _apply_ddl extends the SILVER anime/persons tables with extra ANN columns
#    (themes, plot_summary, gender, height_raw, …).  These are NOT aliased in
#    the INSERT; their defaults (NULL) apply and they are filled later by the
#    UPDATE paths (_ANIME_EXTRAS_SQL / _PERSONS_EXTRAS_SQL).

_ANIME_EXTRAS_SQL = """
WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 'ann:a' || CAST(ann_id AS VARCHAR)
               ORDER BY date DESC
           ) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE ann_id IS NOT NULL
)
UPDATE anime SET
    themes                      = bronze.themes,
    plot_summary                = bronze.plot_summary,
    running_time_raw            = bronze.running_time_raw,
    objectionable_content       = bronze.objectionable_content,
    opening_themes_json         = bronze.opening_themes_json,
    ending_themes_json          = bronze.ending_themes_json,
    insert_songs_json           = bronze.insert_songs_json,
    official_websites_json      = bronze.official_websites_json,
    vintage_raw                 = bronze.vintage_raw,
    image_url                   = bronze.image_url,
    display_rating_votes        = TRY_CAST(bronze.display_rating_votes AS INTEGER),
    display_rating_weighted     = TRY_CAST(bronze.display_rating_weighted AS REAL),
    display_rating_bayesian     = TRY_CAST(bronze.display_rating_bayesian AS REAL),
    -- Card 20/03: _ann suffix aliases (cross-source disambiguation, H1).
    display_rating_count_ann    = TRY_CAST(bronze.display_rating_votes AS INTEGER),
    display_rating_avg_ann      = TRY_CAST(bronze.display_rating_weighted AS REAL),
    display_rating_weighted_ann = TRY_CAST(bronze.display_rating_weighted AS REAL),
    display_rating_bayesian_ann = TRY_CAST(bronze.display_rating_bayesian AS REAL)
FROM bronze
WHERE anime.id = ('ann:a' || CAST(bronze.ann_id AS VARCHAR))
  AND bronze._rn = 1
"""

_PERSONS_EXTRAS_SQL = """
WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 'ann:p' || CAST(ann_id AS VARCHAR)
               ORDER BY date DESC
           ) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE ann_id IS NOT NULL
)
UPDATE persons SET
    gender         = TRY_CAST(bronze.gender AS VARCHAR),
    height_raw     = bronze.height_raw,
    family_name_ja = bronze.family_name_ja,
    given_name_ja  = bronze.given_name_ja,
    hometown       = bronze.hometown,
    image_url_ann  = TRY_CAST(bronze.image_url AS VARCHAR)
FROM bronze
WHERE persons.id = ('ann:p' || CAST(bronze.ann_id AS VARCHAR))
  AND bronze._rn = 1
"""

_EPISODES_SQL = """
INSERT OR IGNORE INTO anime_episodes (anime_id, episode_num, lang, title, aired_date)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    COALESCE(CAST(episode_num AS VARCHAR), ''),
    COALESCE(lang, ''),
    title,
    TRY_CAST(aired_date AS INTEGER)
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id IS NOT NULL
"""

_COMPANIES_SQL = """
INSERT OR IGNORE INTO anime_companies (anime_id, company_name, task, company_id, source)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    company_name,
    task,
    CAST(company_id AS VARCHAR),
    'ann'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id IS NOT NULL
  AND company_name IS NOT NULL
"""

_RELEASES_SQL = """
INSERT OR IGNORE INTO anime_releases (anime_id, product_title, release_date, href, region, source)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    product_title,
    release_date,
    href,
    TRY_CAST(region AS VARCHAR),
    'ann'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id IS NOT NULL
"""

_NEWS_SQL = """
INSERT OR IGNORE INTO anime_news (anime_id, datetime, title, href, source)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    datetime,
    title,
    href,
    'ann'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id IS NOT NULL
  AND href IS NOT NULL
"""

_RELATED_SQL = """
INSERT OR IGNORE INTO anime_relations (anime_id, related_anime_id, relation_type, related_title, source)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    'ann:a' || CAST(target_ann_id AS VARCHAR),
    COALESCE(rel, ''),
    '',
    'ann'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id IS NOT NULL
  AND target_ann_id IS NOT NULL
"""

_CAST_SQL = """
INSERT OR IGNORE INTO character_voice_actors
    (character_id, person_id, anime_id, character_role, source)
SELECT DISTINCT
    'ann:c' || CAST(character_id AS VARCHAR),
    'ann:p' || CAST(ann_person_id AS VARCHAR),
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    COALESCE(cast_role, ''),
    'ann'
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_person_id  IS NOT NULL
  AND ann_anime_id   IS NOT NULL
  AND character_id   IS NOT NULL
"""

# Insert animation-studio names into the shared studios table.
# ID format: 'ann:n:' || company_name  (name-based, same pattern as MAL/Keyframe).
_STUDIOS_FROM_COMPANY_SQL = """
INSERT OR IGNORE INTO studios (id, name, updated_at)
SELECT DISTINCT
    'ann:n:' || company_name  AS id,
    company_name,
    now()                     AS updated_at
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE company_name IS NOT NULL
  AND task = 'Animation Production'
"""

# Link anime to their animation studios via anime_studios.
# Only task='Animation Production' rows are treated as primary studio credits;
# all such rows are marked is_main=1 (ANN does not distinguish main vs. co-studio
# within this task).
_ANIME_STUDIOS_FROM_COMPANY_SQL = """
INSERT OR IGNORE INTO anime_studios (anime_id, studio_id, is_main, role, source)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR)  AS anime_id,
    'ann:n:' || company_name                  AS studio_id,
    1                                         AS is_main,
    'Animation Production'                    AS role,
    'ann'                                     AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
WHERE ann_anime_id   IS NOT NULL
  AND company_name   IS NOT NULL
  AND task = 'Animation Production'
"""


# ─── helpers ────────────────────────────────────────────────────────────────

def _g(bronze_root: Path, table: str) -> str:
    """Return glob path for an ANN BRONZE table."""
    return str(bronze_root / "source=ann" / f"table={table}" / "date=*" / "*.parquet")


def _parquet_cols(conn: duckdb.DuckDBPyConnection, glob: str) -> set[str]:
    """Return column names present in the ANN BRONZE parquet at *glob*."""
    try:
        rows = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{glob}', "
            f"hive_partitioning=true, union_by_name=true) LIMIT 0"
        ).fetchall()
        return {row[0] for row in rows}
    except Exception:  # noqa: BLE001
        return set()


def _silver_table_cols(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    """Return column names present in the given SILVER table."""
    try:
        rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()  # noqa: S608
        return {row[1] for row in rows}
    except Exception:  # noqa: BLE001
        return set()


def _build_anime_insert_sql(
    bronze_cols: set[str],
    silver_cols: set[str],
) -> str:
    """Return an `INSERT OR IGNORE INTO anime BY NAME` SQL string.

    Dynamically constructs the SELECT alias list so that:
    - Every alias emitted exists in the SILVER anime table (BY NAME requirement).
    - Every BRONZE column reference exists in the parquet (avoids column-not-found).
    - Optional SILVER columns (fetched_at, content_hash) are only aliased when
      they exist in both the BRONZE parquet and the SILVER table.

    ANN extra columns added by _apply_ddl (themes, plot_summary, …) are NOT
    included; their NULL defaults apply at INSERT and they are then populated by
    the UPDATE path (_ANIME_EXTRAS_SQL).
    """

    def _col(silver_name: str, bronze_name: str, expr: str, fallback: str) -> str | None:
        """Return 'expr AS silver_name' when both sides exist; None to skip entirely."""
        if silver_name not in silver_cols:
            return None  # column absent from silver → omit alias (BY NAME strict)
        value = expr if bronze_name in bronze_cols else fallback
        return f"    {value:<50} AS {silver_name}"

    core_lines = [
        f"    'ann:a' || CAST(t.ann_id AS VARCHAR){'':18} AS id",
        _col("title_ja",    "title_ja",    "COALESCE(t.title_ja, '')",        "''::VARCHAR"),
        _col("title_en",    "title_en",    "COALESCE(t.title_en, '')",        "''::VARCHAR"),
        _col("year",        "year",        "TRY_CAST(t.year AS INTEGER)",     "NULL::INTEGER"),
        "    NULL::VARCHAR                                           AS season",
        "    NULL::INTEGER                                           AS quarter",
        _col("episodes",   "episodes",    "TRY_CAST(t.episodes AS INTEGER)", "NULL::INTEGER"),
        _col("format",     "format",      "t.format",                        "NULL::VARCHAR"),
        "    NULL::INTEGER                                           AS duration",
        _col("start_date", "start_date",  "TRY_CAST(t.start_date AS VARCHAR)", "NULL::VARCHAR"),
        _col("end_date",   "end_date",    "TRY_CAST(t.end_date AS VARCHAR)",   "NULL::VARCHAR"),
        "    NULL::VARCHAR                                           AS status",
        "    NULL::VARCHAR                                           AS source_mat",
        "    NULL::VARCHAR                                           AS work_type",
        "    NULL::VARCHAR                                           AS scale_class",
        _col("fetched_at",    "fetched_at",    "TRY_CAST(t.fetched_at AS TIMESTAMP)", "NULL::TIMESTAMP"),
        _col("content_hash",  "content_hash",  "t.content_hash",                      "NULL::VARCHAR"),
        "    now()                                                   AS updated_at",
    ]

    select_body = ",\n".join(line for line in core_lines if line is not None)

    return f"""
INSERT OR IGNORE INTO anime BY NAME
SELECT
{select_body}
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ann_id
               ORDER BY date DESC
           ) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE ann_id IS NOT NULL
) t
WHERE t._rn = 1
"""


def _build_persons_insert_sql(
    bronze_cols: set[str],
    silver_cols: set[str],
) -> str:
    """Return an `INSERT OR IGNORE INTO persons BY NAME` SQL string.

    Same dynamic construction strategy as _build_anime_insert_sql.
    Note: BRONZE column 'date_of_birth' maps to SILVER column 'birth_date',
    and BRONZE 'website' maps to SILVER 'website_url'.
    """

    def _col(silver_name: str, bronze_name: str, expr: str, fallback: str) -> str | None:
        if silver_name not in silver_cols:
            return None
        value = expr if bronze_name in bronze_cols else fallback
        return f"    {value:<50} AS {silver_name}"

    def _static(silver_name: str, expr: str) -> str | None:
        """Return 'expr AS silver_name' only when silver_name exists in silver table."""
        if silver_name not in silver_cols:
            return None
        return f"    {expr:<50} AS {silver_name}"

    core_lines = [
        f"    'ann:p' || CAST(t.ann_id AS VARCHAR){'':18} AS id",
        _col("name_ja",     "name_ja",      "COALESCE(t.name_ja, '')",        "''::VARCHAR"),
        _col("name_en",     "name_en",      "COALESCE(t.name_en, '')",        "''::VARCHAR"),
        _col("name_ko",     "name_ko",      "COALESCE(t.name_ko, '')",        "''::VARCHAR"),
        _col("name_zh",     "name_zh",      "COALESCE(t.name_zh, '')",        "''::VARCHAR"),
        _col("names_alt",   "names_alt",    "COALESCE(t.names_alt, '{}')",    "'{}'::VARCHAR"),
        _col("birth_date",  "date_of_birth", "t.date_of_birth",               "NULL::VARCHAR"),
        _static("death_date",   "NULL::VARCHAR"),
        _col("website_url", "website",      "t.website",                      "NULL::VARCHAR"),
        "    now()                                                   AS updated_at",
    ]

    select_body = ",\n".join(line for line in core_lines if line is not None)

    return f"""
INSERT OR IGNORE INTO persons BY NAME
SELECT
{select_body}
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ann_id
               ORDER BY date DESC
           ) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE ann_id IS NOT NULL
) t
WHERE t._rn = 1
"""


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Create/extend SILVER tables for ANN data."""
    for ddl_block in (
        _DDL_ANIME_EPISODES,
        _DDL_ANIME_COMPANIES,
        _DDL_ANIME_RELEASES,
        _DDL_ANIME_NEWS,
        _DDL_ANIME_RELATIONS,
        _DDL_CVA,
        _DDL_STUDIOS,
        _DDL_ANIME_STUDIOS,
    ):
        for stmt in ddl_block.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

    # Backfill source column on tables created before this column existed (H4).
    conn.execute(_DDL_ANIME_RELATIONS_SOURCE_COL)

    for stmt in _DDL_ANIME_EXTENSION:
        conn.execute(stmt)

    for stmt in _DDL_PERSONS_EXTENSION:
        conn.execute(stmt)


# ─── public API ─────────────────────────────────────────────────────────────

def integrate(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path | str,
) -> dict[str, int | str]:
    """Load ANN BRONZE into SILVER DuckDB.

    Applies DDL (idempotent), then runs data operations in two phases:

    Phase 1 — INSERT base rows (required to resolve orphan credits):
      anime_insert: INSERT 'ann:a<id>' rows into anime
      persons_insert: INSERT 'ann:p<id>' rows into persons

    Phase 2 — UPDATE extras + INSERT child tables:
      anime_extras, persons_extras, episodes, companies, releases, news,
      related, cast, studios (from company Animation Production), anime_studios.

    Background: ANN BRONZE uses ann_id (INTEGER) rather than an 'id' column,
    so the generic integrate_duckdb anime/persons loaders (which glob source=*
    and require an 'id' column) skip ANN entirely.  Without Phase 1, the
    305K+ ANN credits are orphaned: credits.anime_id has no parent in anime.

    Args:
        conn: Open DuckDB connection pointing at the SILVER database.
              Must already contain anime / persons / credits / anime_relations /
              character_voice_actors tables.
        bronze_root: Root directory containing BRONZE parquet partitions.

    Returns:
        Dict with row counts for new tables and error keys if any operation
        failed (key: ``<table>_error``, value: error message string).
    """
    bronze_root = Path(bronze_root)

    _apply_ddl(conn)

    counts: dict[str, int | str] = {}

    # Phase 1: INSERT ANN anime + persons rows.
    # These are invisible to the generic integrate_duckdb loaders because ANN
    # BRONZE uses ann_id (INTEGER) rather than an 'id' (VARCHAR) column.
    # Credits were already INSERTed with 'ann:a<id>' / 'ann:p<id>' keys;
    # without these INSERTs those credits are orphaned in the FK graph.
    anime_glob   = _g(bronze_root, "anime")
    persons_glob = _g(bronze_root, "persons")

    silver_anime_cols   = _silver_table_cols(conn, "anime")
    silver_persons_cols = _silver_table_cols(conn, "persons")

    try:
        bronze_anime_cols = _parquet_cols(conn, anime_glob)
        sql = _build_anime_insert_sql(bronze_anime_cols, silver_anime_cols)
        conn.execute(sql, [anime_glob])
    except Exception as exc:  # noqa: BLE001
        counts["anime_insert_error"] = str(exc)

    try:
        bronze_persons_cols = _parquet_cols(conn, persons_glob)
        sql = _build_persons_insert_sql(bronze_persons_cols, silver_persons_cols)
        conn.execute(sql, [persons_glob])
    except Exception as exc:  # noqa: BLE001
        counts["persons_insert_error"] = str(exc)

    # Phase 1b: backfill orphan persons from credits when BRONZE persons rows
    # are missing for some ann_person_id values referenced in credits.
    # Card 14/13 follow-up: 631 orphan credit person_ids fixed by id-only insert.
    credits_glob = _g(bronze_root, "credits")
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO persons (id, name_en, name_ja)
            SELECT DISTINCT
                'ann:p' || CAST(ann_person_id AS VARCHAR) AS id,
                COALESCE(name_en, '')                     AS name_en,
                ''                                        AS name_ja
            FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
            WHERE ann_person_id IS NOT NULL
            """,
            [credits_glob],
        )
    except Exception as exc:  # noqa: BLE001
        counts["persons_from_credits_error"] = str(exc)

    # Phase 2: UPDATE ANN extra columns onto now-existing anime + persons rows.
    pairs = [
        ("anime",    _ANIME_EXTRAS_SQL),
        ("persons",  _PERSONS_EXTRAS_SQL),
        ("episodes", _EPISODES_SQL),
        ("company",  _COMPANIES_SQL),
        ("releases", _RELEASES_SQL),
        ("news",     _NEWS_SQL),
        ("related",  _RELATED_SQL),
        ("cast",     _CAST_SQL),
    ]

    for table, sql in pairs:
        try:
            conn.execute(sql, [_g(bronze_root, table)])
        except Exception as exc:  # noqa: BLE001
            counts[f"{table}_error"] = str(exc)

    # studios + anime_studios from company table (Animation Production rows only)
    company_glob = _g(bronze_root, "company")
    try:
        conn.execute(_STUDIOS_FROM_COMPANY_SQL, [company_glob])
    except Exception as exc:  # noqa: BLE001
        counts["studios_ann_error"] = str(exc)

    try:
        conn.execute(_ANIME_STUDIOS_FROM_COMPANY_SQL, [company_glob])
    except Exception as exc:  # noqa: BLE001
        counts["anime_studios_ann_error"] = str(exc)

    for silver_table in ("anime_episodes", "anime_companies",
                         "anime_releases", "anime_news"):
        counts[silver_table] = conn.execute(
            f"SELECT COUNT(*) FROM {silver_table}"  # noqa: S608
        ).fetchone()[0]

    counts["anime_studios_ann"] = conn.execute(
        "SELECT COUNT(DISTINCT anime_id) FROM anime_studios WHERE source = 'ann'"
    ).fetchone()[0]

    counts["anime_ann"] = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE id LIKE 'ann:%'"
    ).fetchone()[0]

    counts["persons_ann"] = conn.execute(
        "SELECT COUNT(*) FROM persons WHERE id LIKE 'ann:%'"
    ).fetchone()[0]

    return counts
