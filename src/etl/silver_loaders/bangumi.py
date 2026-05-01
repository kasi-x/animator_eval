"""bangumi.tv BRONZE → SILVER loaders.

Tables loaded:
- anime          (subjects type=2  → id='bgm:s<id>')
- persons        (persons          → id='bgm:p<id>')
- characters     (characters       → id='bgm:c<id>')
- credits        (subject_persons  → evidence_source='bangumi')
- character_voice_actors (person_characters JOIN subject_characters)

H1 compliance: score / score_details / rank / favorite are mapped to
display_* prefixed columns only and never appear bare in SILVER.

ID prefix conventions:
  'bgm:s<subject_id>'   — anime (subjects)
  'bgm:p<person_id>'    — persons
  'bgm:c<character_id>' — characters
"""
from __future__ import annotations

from pathlib import Path

import duckdb

# ─── DDL for additional SILVER columns ──────────────────────────────────────
# DuckDB supports ADD COLUMN IF NOT EXISTS; safe to run multiple times.

_DDL_ANIME_EXTENSION = [
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS infobox_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS platform TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS meta_tags_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS series_flag INTEGER",
    # H1: display_* prefix isolates scoring-irrelevant popularity data.
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_score_bgm REAL",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_score_details_json TEXT",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_rank_bgm INTEGER",
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_favorite_bgm INTEGER",
    # Card 20/03: display_collect_count_bgm — sum of wish+done+doing+on_hold+dropped
    # from the bangumi collection JSON (stored as favorite in BRONZE).
    "ALTER TABLE anime ADD COLUMN IF NOT EXISTS display_collect_count_bgm INTEGER",
]

_DDL_PERSONS_EXTENSION = [
    # gender / blood_type are primarily owned by Card 14/03 (ann).
    # We add them here with IF NOT EXISTS so the UPDATE path works regardless
    # of Card execution order. COALESCE in the UPDATE preserves existing values.
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS gender TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS blood_type TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS career_json TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS infobox_json TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS summary_bgm TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS bgm_id INTEGER",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS person_type INTEGER",
]

_DDL_CHARACTERS_EXTENSION = [
    "ALTER TABLE characters ADD COLUMN IF NOT EXISTS infobox_json TEXT",
    "ALTER TABLE characters ADD COLUMN IF NOT EXISTS summary_bgm TEXT",
    "ALTER TABLE characters ADD COLUMN IF NOT EXISTS bgm_id INTEGER",
    "ALTER TABLE characters ADD COLUMN IF NOT EXISTS character_type INTEGER",
    "ALTER TABLE characters ADD COLUMN IF NOT EXISTS images_json TEXT",
]

# ─── SQL templates ───────────────────────────────────────────────────────────

# subjects (type=2 = anime) → anime table.
# name_cn goes into title_en slot (best available non-JA title).
# platform stored as TEXT (bangumi integer code preserved as string).
# H1: score/rank/favorite → display_* columns.
_SUBJECTS_SQL = """
INSERT INTO anime (
    id,
    title_ja,
    title_en,
    infobox_json,
    platform,
    meta_tags_json,
    series_flag,
    display_score_bgm,
    display_score_details_json,
    display_rank_bgm,
    display_favorite_bgm,
    display_collect_count_bgm
)
SELECT
    'bgm:s' || CAST(id AS VARCHAR)                       AS id,
    COALESCE(name, '')                                   AS title_ja,
    COALESCE(name_cn, '')                                AS title_en,
    infobox                                              AS infobox_json,
    CAST(platform AS VARCHAR)                            AS platform,
    meta_tags                                            AS meta_tags_json,
    CASE WHEN series = true THEN 1 ELSE 0 END            AS series_flag,
    TRY_CAST(score    AS REAL)                           AS display_score_bgm,
    score_details                                        AS display_score_details_json,
    TRY_CAST(rank     AS INTEGER)                        AS display_rank_bgm,
    TRY_CAST(favorite AS INTEGER)                        AS display_favorite_bgm,
    -- Card 20/03: sum all collection categories (wish+done+doing+on_hold+dropped).
    -- favorite column is a JSON dict; TRY_CAST guards against non-JSON rows.
    (
        COALESCE(TRY_CAST(json_extract_string(favorite, '$.wish')    AS INTEGER), 0)
      + COALESCE(TRY_CAST(json_extract_string(favorite, '$.done')    AS INTEGER), 0)
      + COALESCE(TRY_CAST(json_extract_string(favorite, '$.doing')   AS INTEGER), 0)
      + COALESCE(TRY_CAST(json_extract_string(favorite, '$.on_hold') AS INTEGER), 0)
      + COALESCE(TRY_CAST(json_extract_string(favorite, '$.dropped') AS INTEGER), 0)
    )                                                    AS display_collect_count_bgm
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
      AND type = 2
)
WHERE _rn = 1
ON CONFLICT (id) DO NOTHING
"""

# persons → INSERT new rows only (existing person rows from other sources kept).
_PERSONS_INSERT_SQL = """
INSERT INTO persons (
    id,
    name_ja,
    name_en,
    career_json,
    infobox_json,
    summary_bgm,
    bgm_id,
    person_type
)
SELECT
    'bgm:p' || CAST(id AS VARCHAR)  AS id,
    COALESCE(name, '')              AS name_ja,
    ''                              AS name_en,
    career                          AS career_json,
    infobox                         AS infobox_json,
    summary                         AS summary_bgm,
    TRY_CAST(id AS INTEGER)         AS bgm_id,
    TRY_CAST(type AS INTEGER)       AS person_type
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
)
WHERE _rn = 1
ON CONFLICT (id) DO NOTHING
"""

# persons → UPDATE optional columns that may have been added by Card 14/03.
# Uses COALESCE to preserve existing non-NULL values from prior sources.
# Explicit VARCHAR casts guard against BIGINT bronze columns vs TEXT silver columns.
_PERSONS_UPDATE_SQL = """
UPDATE persons
SET
    gender     = COALESCE(persons.gender,
                          TRY_CAST(bronze.gender AS VARCHAR)),
    blood_type = COALESCE(persons.blood_type,
                          TRY_CAST(bronze.blood_type AS VARCHAR)),
    birth_date = COALESCE(
        persons.birth_date,
        CASE
            WHEN bronze.birth_year IS NOT NULL
            THEN CAST(bronze.birth_year AS VARCHAR) || '-'
                 || LPAD(COALESCE(CAST(bronze.birth_mon AS VARCHAR), '01'), 2, '0') || '-'
                 || LPAD(COALESCE(CAST(bronze.birth_day AS VARCHAR), '01'), 2, '0')
        END
    )
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
) AS bronze
WHERE persons.id = ('bgm:p' || CAST(bronze.id AS VARCHAR))
  AND bronze._rn = 1
"""

# characters → INSERT.
# Characters have birth_year/birth_mon/birth_day (same as persons).
_CHARACTERS_SQL = """
INSERT INTO characters (
    id,
    name_ja,
    name_en,
    gender,
    blood_type,
    infobox_json,
    summary_bgm,
    bgm_id,
    character_type,
    images_json
)
SELECT
    'bgm:c' || CAST(id AS VARCHAR)  AS id,
    COALESCE(name, '')              AS name_ja,
    ''                              AS name_en,
    gender                          AS gender,
    CAST(blood_type AS VARCHAR)     AS blood_type,
    infobox                         AS infobox_json,
    summary                         AS summary_bgm,
    TRY_CAST(id AS INTEGER)         AS bgm_id,
    TRY_CAST(type AS INTEGER)       AS character_type,
    images                          AS images_json
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE id IS NOT NULL
)
WHERE _rn = 1
ON CONFLICT (id) DO NOTHING
"""

# subject_persons → credits.
# position column is a Chinese string (e.g. '原画') or numeric code string.
# map_role_bangumi() UDF resolves to normalized Role.value.
_CREDITS_SQL = """
INSERT INTO credits (
    person_id,
    anime_id,
    role,
    raw_role,
    episode,
    evidence_source,
    affiliation,
    position
)
SELECT DISTINCT
    'bgm:p' || CAST(sp.person_id  AS VARCHAR) AS person_id,
    'bgm:s' || CAST(sp.subject_id AS VARCHAR) AS anime_id,
    map_role_bangumi(COALESCE(CAST(sp.position AS VARCHAR), '')) AS role,
    COALESCE(CAST(sp.position AS VARCHAR), '')                   AS raw_role,
    NULL                                                          AS episode,
    'bangumi'                                                     AS evidence_source,
    NULL                                                          AS affiliation,
    NULL                                                          AS position
FROM read_parquet(?, hive_partitioning=true, union_by_name=true) AS sp
WHERE sp.person_id  IS NOT NULL
  AND sp.subject_id IS NOT NULL
"""

# person_characters + subject_characters → character_voice_actors.
# subject_characters provides the character's relation (主役/配役 etc.).
# person_characters provides the voice actor (person_id) per character/subject.
# _CVA_NO_SC_SQL is used when subject_characters parquet is absent.
_CVA_NO_SC_SQL = """
INSERT INTO character_voice_actors (
    character_id,
    person_id,
    anime_id,
    character_role,
    source
)
SELECT DISTINCT
    'bgm:c' || CAST(pc.character_id AS VARCHAR) AS character_id,
    'bgm:p' || CAST(pc.person_id    AS VARCHAR) AS person_id,
    'bgm:s' || CAST(pc.subject_id   AS VARCHAR) AS anime_id,
    ''                                           AS character_role,
    'bangumi'                                    AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true) AS pc
WHERE pc.character_id IS NOT NULL
  AND pc.person_id    IS NOT NULL
  AND pc.subject_id   IS NOT NULL
ON CONFLICT (character_id, person_id, anime_id) DO NOTHING
"""

_CVA_SQL = """
INSERT INTO character_voice_actors (
    character_id,
    person_id,
    anime_id,
    character_role,
    source
)
SELECT DISTINCT
    'bgm:c' || CAST(pc.character_id AS VARCHAR) AS character_id,
    'bgm:p' || CAST(pc.person_id    AS VARCHAR) AS person_id,
    'bgm:s' || CAST(pc.subject_id   AS VARCHAR) AS anime_id,
    COALESCE(sc.relation, '')                    AS character_role,
    'bangumi'                                    AS source
FROM read_parquet(?, hive_partitioning=true, union_by_name=true) AS pc
LEFT JOIN read_parquet(?, hive_partitioning=true, union_by_name=true) AS sc
       ON pc.subject_id   = sc.subject_id
      AND pc.character_id = sc.character_id
WHERE pc.character_id IS NOT NULL
  AND pc.person_id    IS NOT NULL
  AND pc.subject_id   IS NOT NULL
ON CONFLICT (character_id, person_id, anime_id) DO NOTHING
"""


def _glob(bronze_root: Path, table: str) -> str:
    """Return a glob pattern for a bangumi BRONZE table partition."""
    return str(bronze_root / "source=bangumi" / f"table={table}" / "date=*" / "*.parquet")


def _has_parquet(conn: duckdb.DuckDBPyConnection, glob: str) -> bool:
    """Return True if at least one parquet file matches glob."""
    try:
        conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{glob}', union_by_name=true) LIMIT 0"
        )
        return True
    except Exception:
        return False


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Add bangumi extension columns to anime / persons / characters.

    All ALTER TABLE statements use IF NOT EXISTS — safe to run multiple times.
    """
    for stmt in _DDL_ANIME_EXTENSION:
        conn.execute(stmt)
    for stmt in _DDL_PERSONS_EXTENSION:
        conn.execute(stmt)
    for stmt in _DDL_CHARACTERS_EXTENSION:
        conn.execute(stmt)


def _register_udf(conn: duckdb.DuckDBPyConnection) -> None:
    """Register map_role_bangumi() as a DuckDB scalar UDF for SQL use in credits.

    Safe to call multiple times on the same connection — drops any prior
    registration first to avoid 'already created' errors.
    """
    from src.etl.role_mappers import map_role

    try:
        conn.remove_function("map_role_bangumi")
    except Exception:
        pass  # not registered yet — fine

    conn.create_function(
        "map_role_bangumi",
        lambda r: map_role("bangumi", r) if r is not None else "other",
        ["VARCHAR"],
        "VARCHAR",
    )


def integrate(conn: duckdb.DuckDBPyConnection, bronze_root: Path | str) -> dict[str, int]:
    """Load bangumi.tv BRONZE data into SILVER tables.

    Args:
        conn: Open DuckDB connection pointing at the SILVER database.
              Must already contain anime / persons / characters / credits /
              character_voice_actors tables.
        bronze_root: Root directory of BRONZE parquet partitions.

    Returns:
        Dict with counts keyed by:
          bgm_anime, bgm_persons, bgm_characters, bgm_credits, bgm_cva
    """
    bronze_root = Path(bronze_root)

    _apply_ddl(conn)
    _register_udf(conn)

    subjects_glob    = _glob(bronze_root, "subjects")
    persons_glob     = _glob(bronze_root, "persons")
    characters_glob  = _glob(bronze_root, "characters")
    sp_glob          = _glob(bronze_root, "subject_persons")
    pc_glob          = _glob(bronze_root, "person_characters")
    sc_glob          = _glob(bronze_root, "subject_characters")

    if _has_parquet(conn, subjects_glob):
        conn.execute(_SUBJECTS_SQL, [subjects_glob])

    if _has_parquet(conn, persons_glob):
        conn.execute(_PERSONS_INSERT_SQL, [persons_glob])
        conn.execute(_PERSONS_UPDATE_SQL, [persons_glob])

    if _has_parquet(conn, characters_glob):
        conn.execute(_CHARACTERS_SQL, [characters_glob])

    if _has_parquet(conn, sp_glob):
        conn.execute("DELETE FROM credits WHERE evidence_source = 'bangumi'")
        conn.execute(_CREDITS_SQL, [sp_glob])

    if _has_parquet(conn, pc_glob):
        conn.execute("DELETE FROM character_voice_actors WHERE source = 'bangumi'")
        if _has_parquet(conn, sc_glob):
            conn.execute(_CVA_SQL, [pc_glob, sc_glob])
        else:
            conn.execute(_CVA_NO_SC_SQL, [pc_glob])

    return {
        "bgm_anime": conn.execute(
            "SELECT COUNT(*) FROM anime WHERE id LIKE 'bgm:s%'"
        ).fetchone()[0],
        "bgm_persons": conn.execute(
            "SELECT COUNT(*) FROM persons WHERE id LIKE 'bgm:p%'"
        ).fetchone()[0],
        "bgm_characters": conn.execute(
            "SELECT COUNT(*) FROM characters WHERE id LIKE 'bgm:c%'"
        ).fetchone()[0],
        "bgm_credits": conn.execute(
            "SELECT COUNT(*) FROM credits WHERE evidence_source = 'bangumi'"
        ).fetchone()[0],
        "bgm_cva": conn.execute(
            "SELECT COUNT(*) FROM character_voice_actors WHERE source = 'bangumi'"
        ).fetchone()[0],
    }
