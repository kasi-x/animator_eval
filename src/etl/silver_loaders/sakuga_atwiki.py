"""sakuga@wiki BRONZE → SILVER (work_title resolution + persons).

Tables written:
    sakuga_work_title_resolution  — work_title → anime_id cache (new table)
    persons                       — sakuga:p<id> rows inserted if not exists;
                                    aliases / years_active updated when present

Credits UPDATE:
    credits rows where evidence_source='sakuga_atwiki' and anime_id IS NULL
    are back-filled from sakuga_work_title_resolution.

H3: title matching logic lives in src.etl.sakuga_title_matcher (independent
module, no touch to src/analysis/entity_resolution.py).
H4: evidence_source='sakuga_atwiki' is preserved.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from src.etl.sakuga_title_matcher import match_title

# ─── DDL ────────────────────────────────────────────────────────────────────

_DDL = [
    # Resolution cache table (primary).
    # id is a BIGINT sequence — DuckDB does not support AUTOINCREMENT keyword;
    # using SEQUENCE + DEFAULT is the idiomatic approach.
    "CREATE SEQUENCE IF NOT EXISTS seq_swtr_id START 1",
    """CREATE TABLE IF NOT EXISTS sakuga_work_title_resolution (
        id                BIGINT DEFAULT nextval('seq_swtr_id') PRIMARY KEY,
        work_title        TEXT NOT NULL,
        work_year         INTEGER,
        work_format       TEXT,
        resolved_anime_id TEXT,
        match_method      TEXT,
        match_score       REAL,
        UNIQUE(work_title, work_year, work_format)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_swtr_anime
       ON sakuga_work_title_resolution(resolved_anime_id)""",
    # Ensure persons has the columns this loader may write.
    # Card 04 (seesaawiki) also adds these; IF NOT EXISTS makes it idempotent.
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS aliases TEXT",
    "ALTER TABLE persons ADD COLUMN IF NOT EXISTS years_active TEXT",
]

# ─── SQL ────────────────────────────────────────────────────────────────────

# Insert sakuga persons that do not yet exist in SILVER.
# - id:          'sakuga:p' || page_id
# - name_ja:     name from BRONZE
# - aliases:     aliases_json from BRONZE (JSON array string)
# - years_active: "YYYY-" format from active_since_year (open-ended range)
# Latest date partition wins via ROW_NUMBER.
_PERSONS_INSERT_SQL = """
INSERT INTO persons (id, name_ja, aliases, years_active)
SELECT
    'sakuga:p' || CAST(page_id AS VARCHAR),
    COALESCE(name, ''),
    COALESCE(aliases_json, '[]'),
    CASE
        WHEN active_since_year IS NOT NULL
             THEN CAST(active_since_year AS VARCHAR) || '-'
        ELSE NULL
    END
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY page_id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE page_id IS NOT NULL
)
WHERE _rn = 1
ON CONFLICT (id) DO NOTHING
"""

# Update aliases / years_active for already-existing sakuga persons.
# Only fills in NULL values — never overwrites non-NULL data from other sources.
_PERSONS_UPDATE_SQL = """
UPDATE persons
SET
    aliases      = CASE
                       WHEN aliases IS NULL AND bronze.aliases_json IS NOT NULL
                       THEN bronze.aliases_json
                       ELSE aliases
                   END,
    years_active = CASE
                       WHEN years_active IS NULL AND bronze.active_since_year IS NOT NULL
                       THEN CAST(bronze.active_since_year AS VARCHAR) || '-'
                       ELSE years_active
                   END
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY page_id ORDER BY date DESC) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE page_id IS NOT NULL
)  AS bronze
WHERE _rn = 1
  AND persons.id = 'sakuga:p' || CAST(bronze.page_id AS VARCHAR)
  AND (aliases IS NULL OR years_active IS NULL)
"""

# Update credits.anime_id for sakuga_atwiki rows that currently have NULL.
# Joins through the resolution cache using (work_title, work_year) key.
# Matches credits by (person_id, raw_role) from BRONZE credits table.
_CREDITS_UPDATE_SQL = """
WITH bronze_credits AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY person_page_id, work_title, role_raw, episode_num
               ORDER BY date DESC
           ) AS _rn
    FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
    WHERE person_page_id IS NOT NULL
)
UPDATE credits
SET anime_id = swtr.resolved_anime_id
FROM bronze_credits
JOIN sakuga_work_title_resolution swtr
    ON  swtr.work_title = bronze_credits.work_title
    AND COALESCE(swtr.work_year, -1) = COALESCE(bronze_credits.work_year, -1)
WHERE credits.evidence_source = 'sakuga_atwiki'
  AND credits.person_id       = 'sakuga:p' || CAST(bronze_credits.person_page_id AS VARCHAR)
  AND credits.raw_role        = bronze_credits.role_raw
  AND credits.anime_id        IS NULL
  AND swtr.resolved_anime_id  IS NOT NULL
  AND bronze_credits._rn      = 1
"""


# ─── Helpers ────────────────────────────────────────────────────────────────

def _glob(bronze_root: Path, table: str) -> str:
    """Return glob string for a sakuga_atwiki BRONZE table."""
    return str(
        bronze_root
        / "source=sakuga_atwiki"
        / f"table={table}"
        / "date=*"
        / "*.parquet"
    )


def _has_parquet(bronze_root: Path, table: str) -> bool:
    """Return True if at least one parquet file exists for the given table."""
    import glob as _glob_mod
    pattern = str(
        bronze_root
        / "source=sakuga_atwiki"
        / f"table={table}"
        / "date=*"
        / "*.parquet"
    )
    return bool(_glob_mod.glob(pattern))


def _apply_ddl(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sakuga_work_title_resolution table and ensure persons columns exist."""
    for stmt in _DDL:
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)


def _resolve_work_titles(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path,
) -> int:
    """Populate sakuga_work_title_resolution from BRONZE credits.

    Fetches distinct (work_title, work_year, work_format) from BRONZE,
    runs the conservative 2-stage matcher against SILVER anime,
    and inserts results (skipping already-cached rows via ON CONFLICT DO NOTHING).

    Returns:
        Total rows inserted into sakuga_work_title_resolution.
    """
    if not _has_parquet(bronze_root, "credits"):
        return 0

    distinct_titles = conn.execute(f"""
        SELECT DISTINCT
            work_title,
            TRY_CAST(work_year AS INTEGER) AS work_year,
            work_format
        FROM read_parquet(
            '{_glob(bronze_root, "credits")}',
            hive_partitioning=true,
            union_by_name=true
        )
        WHERE work_title IS NOT NULL
    """).fetchall()

    anime_rows: list[tuple[str, str | None, str | None, int | None]] = conn.execute(
        "SELECT id, title_ja, title_en, year FROM anime"
    ).fetchall()

    rows = []
    for work_title, work_year, work_format in distinct_titles:
        aid, method, score = match_title(work_title, work_year, anime_rows)
        rows.append((work_title, work_year, work_format, aid, method, score))

    if rows:
        conn.executemany(
            """INSERT INTO sakuga_work_title_resolution
               (work_title, work_year, work_format, resolved_anime_id, match_method, match_score)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT DO NOTHING""",
            rows,
        )
    return len(rows)


# ─── Public API ─────────────────────────────────────────────────────────────

def integrate(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path | str,
) -> dict[str, int]:
    """Load sakuga@wiki BRONZE data into SILVER.

    Steps:
        1. Ensure DDL (sakuga_work_title_resolution table, persons columns).
        2. Insert new sakuga persons (id='sakuga:p<page_id>').
        3. Resolve work_title → anime_id and populate the resolution cache.
        4. Back-fill credits.anime_id where currently NULL.

    Args:
        conn:        Open DuckDB connection to the SILVER database.
                     Must already contain the ``anime`` and ``credits`` tables.
        bronze_root: Root directory of BRONZE parquet partitions.

    Returns:
        Dict with counts:
        - ``sakuga_persons``: total persons with id like 'sakuga:p%' after insert
        - ``resolution_rows``: number of distinct work titles processed
        - ``resolved_anime_ids``: resolution cache rows with a non-NULL anime_id
        - ``credits_resolved``: credits rows with evidence_source='sakuga_atwiki'
          that now have a non-NULL anime_id
    """
    bronze_root = Path(bronze_root)
    counts: dict[str, int] = {}

    _apply_ddl(conn)

    if _has_parquet(bronze_root, "persons"):
        persons_glob = _glob(bronze_root, "persons")
        conn.execute(_PERSONS_INSERT_SQL, [persons_glob])
        conn.execute(_PERSONS_UPDATE_SQL, [persons_glob])
    counts["sakuga_persons"] = conn.execute(
        "SELECT COUNT(*) FROM persons WHERE id LIKE 'sakuga:p%'"
    ).fetchone()[0]

    counts["resolution_rows"] = _resolve_work_titles(conn, bronze_root)
    counts["resolved_anime_ids"] = conn.execute(
        "SELECT COUNT(*) FROM sakuga_work_title_resolution WHERE resolved_anime_id IS NOT NULL"
    ).fetchone()[0]

    if _has_parquet(bronze_root, "credits"):
        credits_glob = _glob(bronze_root, "credits")
        conn.execute(_CREDITS_UPDATE_SQL, [credits_glob])
    counts["credits_resolved"] = conn.execute(
        """SELECT COUNT(*) FROM credits
           WHERE evidence_source = 'sakuga_atwiki'
             AND anime_id IS NOT NULL"""
    ).fetchone()[0]

    return counts
