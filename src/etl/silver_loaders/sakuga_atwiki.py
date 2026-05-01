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

Performance note: _resolve_work_titles uses a DuckDB SQL bulk-match approach
(two-pass exact→normalized JOIN with COUNT=1 guard) instead of iterating over
all anime rows per title in Python.  With 562 K anime and 7.5 K distinct sakuga
work titles, the Python O(N×M) approach (~4 B iterations) was too slow and
caused the loader to be killed mid-run, leaving sakuga_work_title_resolution
empty.  The SQL path runs in seconds inside the DuckDB engine.
match_title() in sakuga_title_matcher is kept for unit tests; its logic is
faithfully replicated in the SQL query below.  sakuga_atwiki.py no longer
imports match_title directly — tests import it from sakuga_title_matcher.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from src.etl.sakuga_title_matcher import _normalize

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


def _register_normalize_udf(conn: duckdb.DuckDBPyConnection) -> None:
    """Register normalize_title scalar UDF in DuckDB.

    Exposes the same NFKC + whitespace/punctuation normalisation as
    sakuga_title_matcher._normalize() so the SQL bulk-matcher uses an
    identical algorithm to the Python unit-test path.

    The UDF name is ``normalize_title``; silently skips re-registration
    when the function already exists (DuckDB raises NotImplementedException
    on duplicate create_function calls; idempotency is handled here so
    callers can call integrate() more than once on the same connection).
    """
    try:
        conn.create_function(
            "normalize_title",
            _normalize,
            ["VARCHAR"],
            "VARCHAR",
        )
    except Exception:
        # Already registered on this connection — safe to proceed.
        pass


def _resolve_work_titles(
    conn: duckdb.DuckDBPyConnection,
    bronze_root: Path,
) -> int:
    """Populate sakuga_work_title_resolution from BRONZE credits via SQL bulk-match.

    Two-pass conservative strategy (exact → normalized) executed entirely
    inside DuckDB — avoids the O(N×M) Python loop that was timing out with
    ~562 K anime rows.

    Pass 1 — exact match:
        JOIN anime on title_ja = work_title OR title_en = work_title,
        year-guarded (|year_diff| ≤ 1 or either side NULL).
        Accept only titles with exactly 1 distinct anime_id hit.

    Pass 2 — normalized match (titles unresolved after pass 1):
        Same join using normalize_title() UDF on both sides.
        Accept only titles with exactly 1 distinct anime_id hit.

    All distinct (work_title, work_year, work_format) triples are written
    to sakuga_work_title_resolution; unresolved rows get NULL anime_id /
    'unresolved' method / 0.0 score.  ON CONFLICT DO NOTHING skips rows
    already cached from a prior run.

    Args:
        conn:        Open DuckDB connection with SILVER anime table and the
                     sakuga_work_title_resolution table already created.
        bronze_root: Root directory of BRONZE parquet partitions.

    Returns:
        Number of distinct work-title triples processed (inserted or skipped
        due to conflict).  This equals the BRONZE DISTINCT count.
    """
    if not _has_parquet(bronze_root, "credits"):
        return 0

    _register_normalize_udf(conn)

    credits_glob = _glob(bronze_root, "credits")

    # Two-step approach:
    #   Step A — build temp tables with pre-computed normalized forms so the
    #             UDF is called once per row (not once per join pair).
    #   Step B — pure-SQL JOINs on the pre-normalized values.
    #
    # Step A: bronze_titles with normalized form (UDF called ~7.5 K times)
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _swtr_bronze AS
        SELECT DISTINCT
            CAST(work_title AS VARCHAR)            AS work_title,
            normalize_title(CAST(work_title AS VARCHAR)) AS norm_title,
            TRY_CAST(work_year AS INTEGER)         AS work_year,
            TRY_CAST(work_format AS VARCHAR)       AS work_format
        FROM read_parquet(
            '{credits_glob}',
            hive_partitioning=true,
            union_by_name=true
        )
        WHERE work_title IS NOT NULL
    """)

    # Step A: anime with normalized forms (UDF called ~2 × 562 K times — once each)
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE _swtr_anime AS
        SELECT
            id,
            title_ja,
            title_en,
            normalize_title(title_ja) AS norm_ja,
            normalize_title(title_en) AS norm_en,
            year
        FROM anime
    """)

    # Step B: pure-SQL bulk match using pre-normalized temp tables.
    conn.execute("""
        INSERT INTO sakuga_work_title_resolution
            (work_title, work_year, work_format, resolved_anime_id, match_method, match_score)

        WITH
        -- Pass 1: exact matches (title_ja or title_en == work_title literally)
        exact_hits AS (
            SELECT
                bt.work_title,
                bt.work_year,
                bt.work_format,
                a.id                                          AS anime_id,
                COUNT(DISTINCT a.id) OVER (
                    PARTITION BY bt.work_title, bt.work_year, bt.work_format
                )                                             AS hit_count
            FROM _swtr_bronze bt
            JOIN _swtr_anime a
                ON  (a.title_ja = bt.work_title OR a.title_en = bt.work_title)
                AND (bt.work_year IS NULL OR a.year IS NULL
                     OR ABS(a.year - bt.work_year) <= 1)
        ),
        exact_unique AS (
            SELECT DISTINCT work_title, work_year, work_format, anime_id
            FROM exact_hits
            WHERE hit_count = 1
        ),

        -- Pass 2: normalized matches for titles unresolved after pass 1
        norm_hits AS (
            SELECT
                bt.work_title,
                bt.work_year,
                bt.work_format,
                a.id                                          AS anime_id,
                COUNT(DISTINCT a.id) OVER (
                    PARTITION BY bt.work_title, bt.work_year, bt.work_format
                )                                             AS hit_count
            FROM _swtr_bronze bt
            JOIN _swtr_anime a
                ON  (a.norm_ja = bt.norm_title OR a.norm_en = bt.norm_title)
                AND (bt.work_year IS NULL OR a.year IS NULL
                     OR ABS(a.year - bt.work_year) <= 1)
            -- skip titles already resolved by exact pass
            WHERE NOT EXISTS (
                SELECT 1 FROM exact_unique eu
                WHERE  eu.work_title  = bt.work_title
                  AND  COALESCE(eu.work_year, -1)   = COALESCE(bt.work_year, -1)
                  AND  COALESCE(eu.work_format, '') = COALESCE(bt.work_format, '')
            )
            -- exclude anime that already matched exactly to avoid recounting
            AND NOT (a.title_ja = bt.work_title OR a.title_en = bt.work_title)
        ),
        norm_unique AS (
            SELECT DISTINCT work_title, work_year, work_format, anime_id
            FROM norm_hits
            WHERE hit_count = 1
        ),

        -- Combine: exact wins > normalized > unresolved
        resolved AS (
            SELECT work_title, work_year, work_format, anime_id,
                   'exact_title' AS match_method, 1.0::FLOAT AS match_score
            FROM exact_unique
            UNION ALL
            SELECT work_title, work_year, work_format, anime_id,
                   'normalized'  AS match_method, 0.95::FLOAT AS match_score
            FROM norm_unique
        ),
        all_titles AS (
            SELECT
                bt.work_title, bt.work_year, bt.work_format,
                r.anime_id                                    AS resolved_anime_id,
                COALESCE(r.match_method, 'unresolved')        AS match_method,
                COALESCE(r.match_score,  0.0::FLOAT)          AS match_score
            FROM _swtr_bronze bt
            LEFT JOIN resolved r
                ON  r.work_title  = bt.work_title
                AND COALESCE(r.work_year,   -1)  = COALESCE(bt.work_year,   -1)
                AND COALESCE(r.work_format, '') = COALESCE(bt.work_format, '')
        )

        SELECT work_title, work_year, work_format,
               resolved_anime_id, match_method, match_score
        FROM all_titles

        ON CONFLICT DO NOTHING
    """)

    return conn.execute(
        "SELECT COUNT(*) FROM sakuga_work_title_resolution"
    ).fetchone()[0]


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
