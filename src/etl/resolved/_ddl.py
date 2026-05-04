"""DDL for result/resolved.duckdb — Resolved layer (Phase 2a).

One canonical row per entity (anime / person / studio).
All scoring reads this layer (Phase 3 migration target).

Design rules:
- canonical_id is the single stable primary key for downstream joins.
- source_ids_json records all contributing conformed IDs (lineage).
- _source columns record which source each field was drawn from (audit trail).
- No display_* columns — H1: score/popularity values must never enter this layer.

Schema note: tables live in the `main` schema of resolved.duckdb (no inner schema
prefix) because DuckDB's catalog name equals the file stem.  Wrapping everything in
a schema also called "resolved" would produce the ambiguous `resolved.resolved.*`
path that DuckDB rejects.  Callers use plain unqualified table names or the
`resolved_connect()` context manager.
"""

CREATE_ANIME = """
CREATE TABLE IF NOT EXISTS anime (
    canonical_id         VARCHAR PRIMARY KEY,
    title_ja             VARCHAR NOT NULL DEFAULT '',
    title_en             VARCHAR NOT NULL DEFAULT '',
    year                 INTEGER,
    season               VARCHAR,
    quarter              INTEGER,
    episodes             INTEGER,
    format               VARCHAR,
    duration             INTEGER,
    start_date           VARCHAR,
    end_date             VARCHAR,
    status               VARCHAR,
    source_mat           VARCHAR,
    work_type            VARCHAR,
    scale_class          VARCHAR,
    country_of_origin    VARCHAR,
    source_ids_json      VARCHAR NOT NULL DEFAULT '[]',
    source_count         INTEGER,
    title_ja_source      VARCHAR,
    title_en_source      VARCHAR,
    year_source          VARCHAR,
    episodes_source      VARCHAR,
    format_source        VARCHAR,
    duration_source      VARCHAR,
    source_mat_source    VARCHAR,
    built_at             TIMESTAMP DEFAULT now()
)
"""

CREATE_PERSONS = """
CREATE TABLE IF NOT EXISTS persons (
    canonical_id         VARCHAR PRIMARY KEY,
    name_ja              VARCHAR NOT NULL DEFAULT '',
    name_en              VARCHAR NOT NULL DEFAULT '',
    name_ko              VARCHAR NOT NULL DEFAULT '',
    name_zh              VARCHAR NOT NULL DEFAULT '',
    birth_date           VARCHAR,
    death_date           VARCHAR,
    gender               VARCHAR,
    nationality          VARCHAR,
    source_ids_json      VARCHAR NOT NULL DEFAULT '[]',
    name_ja_source       VARCHAR,
    name_en_source       VARCHAR,
    gender_source        VARCHAR,
    birth_date_source    VARCHAR,
    built_at             TIMESTAMP DEFAULT now()
)
"""

CREATE_STUDIOS = """
CREATE TABLE IF NOT EXISTS studios (
    canonical_id         VARCHAR PRIMARY KEY,
    name                 VARCHAR NOT NULL DEFAULT '',
    is_animation_studio  BOOLEAN,
    country_of_origin    VARCHAR,
    source_ids_json      VARCHAR NOT NULL DEFAULT '[]',
    name_source          VARCHAR,
    country_source       VARCHAR,
    built_at             TIMESTAMP DEFAULT now()
)
"""

_CREATE_AUDIT_SEQ = "CREATE SEQUENCE IF NOT EXISTS seq_audit_id START 1"

CREATE_META_RESOLUTION_AUDIT = """
CREATE TABLE IF NOT EXISTS meta_resolution_audit (
    id               BIGINT PRIMARY KEY DEFAULT nextval('seq_audit_id'),
    canonical_id     VARCHAR NOT NULL,
    entity_type      VARCHAR NOT NULL CHECK (entity_type IN ('anime','person','studio')),
    field_name       VARCHAR NOT NULL,
    field_value      VARCHAR,
    source_name      VARCHAR NOT NULL,
    selection_reason VARCHAR NOT NULL,
    built_at         TIMESTAMP DEFAULT now()
)
"""

CREATE_IDX_AUDIT_CANONICAL = """
CREATE INDEX IF NOT EXISTS idx_audit_canonical
    ON meta_resolution_audit(canonical_id)
"""

CREATE_CREDITS = """
CREATE TABLE IF NOT EXISTS credits (
    person_id        VARCHAR NOT NULL,
    anime_id         VARCHAR NOT NULL,
    role             VARCHAR NOT NULL,
    raw_role         VARCHAR NOT NULL DEFAULT '',
    episode          INTEGER,
    evidence_source  VARCHAR NOT NULL DEFAULT '',
    credit_year      INTEGER,
    credit_quarter   INTEGER,
    affiliation      VARCHAR,
    position         INTEGER
)
"""

CREATE_IDX_CREDITS_PERSON = """
CREATE INDEX IF NOT EXISTS idx_credits_person
    ON credits(person_id)
"""

CREATE_IDX_CREDITS_ANIME = """
CREATE INDEX IF NOT EXISTS idx_credits_anime
    ON credits(anime_id)
"""

CREATE_IDX_CREDITS_ROLE = """
CREATE INDEX IF NOT EXISTS idx_credits_role
    ON credits(role)
"""

ALL_DDL: list[str] = [
    CREATE_ANIME,
    CREATE_PERSONS,
    CREATE_STUDIOS,
    _CREATE_AUDIT_SEQ,
    CREATE_META_RESOLUTION_AUDIT,
    CREATE_IDX_AUDIT_CANONICAL,
    CREATE_CREDITS,
    CREATE_IDX_CREDITS_PERSON,
    CREATE_IDX_CREDITS_ANIME,
    CREATE_IDX_CREDITS_ROLE,
]
