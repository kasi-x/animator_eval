"""ANN BRONZE → SILVER extras.

Tables loaded:
- anime           (UPDATE: ANN extra columns)
- persons         (UPDATE: ANN extra columns)
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
    PRIMARY KEY (anime_id, related_anime_id, relation_type)
);
CREATE INDEX IF NOT EXISTS idx_anime_relations_anime
    ON anime_relations(anime_id);
CREATE INDEX IF NOT EXISTS idx_anime_relations_related
    ON anime_relations(related_anime_id);
"""

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
    themes                  = bronze.themes,
    plot_summary            = bronze.plot_summary,
    running_time_raw        = bronze.running_time_raw,
    objectionable_content   = bronze.objectionable_content,
    opening_themes_json     = bronze.opening_themes_json,
    ending_themes_json      = bronze.ending_themes_json,
    insert_songs_json       = bronze.insert_songs_json,
    official_websites_json  = bronze.official_websites_json,
    vintage_raw             = bronze.vintage_raw,
    image_url               = bronze.image_url,
    display_rating_votes    = TRY_CAST(bronze.display_rating_votes AS INTEGER),
    display_rating_weighted = TRY_CAST(bronze.display_rating_weighted AS REAL),
    display_rating_bayesian = TRY_CAST(bronze.display_rating_bayesian AS REAL)
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
INSERT OR IGNORE INTO anime_relations (anime_id, related_anime_id, relation_type, related_title)
SELECT DISTINCT
    'ann:a' || CAST(ann_anime_id AS VARCHAR),
    'ann:a' || CAST(target_ann_id AS VARCHAR),
    COALESCE(rel, ''),
    ''
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


# ─── helpers ────────────────────────────────────────────────────────────────

def _g(bronze_root: Path, table: str) -> str:
    """Return glob path for an ANN BRONZE table."""
    return str(bronze_root / "source=ann" / f"table={table}" / "date=*" / "*.parquet")


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Create/extend SILVER tables for ANN data."""
    for ddl_block in (
        _DDL_ANIME_EPISODES,
        _DDL_ANIME_COMPANIES,
        _DDL_ANIME_RELEASES,
        _DDL_ANIME_NEWS,
        _DDL_ANIME_RELATIONS,
        _DDL_CVA,
    ):
        for stmt in ddl_block.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

    for stmt in _DDL_ANIME_EXTENSION:
        conn.execute(stmt)

    for stmt in _DDL_PERSONS_EXTENSION:
        conn.execute(stmt)


# ─── public API ─────────────────────────────────────────────────────────────

def integrate(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path | str,
) -> dict[str, int | str]:
    """Load ANN BRONZE extras into SILVER DuckDB.

    Applies DDL (idempotent), then runs 8 data operations:
    anime_extras, persons_extras, episodes, companies, releases, news,
    related, cast.

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

    counts: dict[str, int | str] = {}

    for table, sql in pairs:
        try:
            conn.execute(sql, [_g(bronze_root, table)])
        except Exception as exc:  # noqa: BLE001
            counts[f"{table}_error"] = str(exc)

    for silver_table in ("anime_episodes", "anime_companies",
                         "anime_releases", "anime_news"):
        counts[silver_table] = conn.execute(
            f"SELECT COUNT(*) FROM {silver_table}"  # noqa: S608
        ).fetchone()[0]

    return counts
