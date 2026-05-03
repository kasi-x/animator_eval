"""v62_mart_pk_restore.py — One-shot migration: restore PRIMARY KEY constraints on mart schema.

Phase 1c used CTAS (CREATE TABLE AS SELECT) to copy tables from gld.main into mart schema,
which drops all PRIMARY KEY / UNIQUE constraints (DuckDB CTAS behaviour).

This script:
  1. Connects to animetor.duckdb
  2. Deduplicates any rows that would violate the PK (keeps most-recently-inserted row)
  3. Adds PRIMARY KEY constraint to each mart table via ALTER TABLE

Run once:
    pixi run python scripts/maintenance/v62_mart_pk_restore.py

Safety:
  - Reads ANIMETOR_DB_PATH env var (same as DEFAULT_GOLD_DB_PATH in mart_writer.py)
  - Skips tables with 0 rows silently
  - Reports per-table result
  - Does NOT touch score_history (no PK by design)
  - Does NOT touch schemas outside mart
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import structlog

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# PK map: table -> ordered list of PK columns
# Source of truth: _DDL in src/analysis/io/mart_writer.py
# Verified against gold.duckdb which has these PKs intact.
# ---------------------------------------------------------------------------

MART_PK_MAP: dict[str, list[str]] = {
    "agg_director_circles": ["person_id", "director_id"],
    "agg_milestones": ["person_id", "event_type", "year", "anime_id"],
    "agg_person_career": ["person_id"],
    "agg_person_network": ["person_id"],
    "corrections_credit_year": ["id"],
    "corrections_role": ["id"],
    "feat_birank_annual": ["person_id", "year"],
    "feat_career": ["person_id"],
    "feat_career_annual": ["person_id", "career_year"],
    "feat_career_gaps": ["person_id", "gap_start_year"],
    "feat_career_scores": ["person_id"],
    "feat_causal_estimates": ["person_id"],
    "feat_cluster_membership": ["person_id"],
    "feat_contribution": ["person_id"],
    "feat_credit_activity": ["person_id"],
    "feat_genre_affinity": ["person_id", "genre"],
    "feat_mentorships": ["mentor_id", "mentee_id"],
    "feat_network": ["person_id"],
    "feat_network_scores": ["person_id"],
    "feat_person_role_progression": ["person_id", "role_category"],
    "feat_studio_affiliation": ["person_id", "credit_year", "studio_id"],
    "meta_common_person_parameters": ["person_id"],
    "meta_lineage": ["table_name"],
    "ops_entity_resolution_audit": ["person_id"],
    "person_scores": ["person_id"],
    # test_scores: in mart schema but not in _DDL; restore its PK as found in gold.duckdb
    "test_scores": ["person_id"],
}

# Tables intentionally without PK (excluded from migration):
#   score_history  — append-only history log, no natural PK


def _get_db_path() -> Path:
    return Path(
        os.environ.get(
            "ANIMETOR_DB_PATH",
            str(Path(__file__).resolve().parent.parent.parent / "result" / "animetor.duckdb"),
        )
    )


def _dedup_table(conn: duckdb.DuckDBPyConnection, schema: str, table: str, pk_cols: list[str]) -> int:
    """Delete duplicate rows, keeping the last occurrence by rowid.

    Returns number of rows deleted.
    """
    cols_str = ", ".join(pk_cols)
    # Use rowid to keep the last row among duplicates
    n_before = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
    n_distinct = conn.execute(
        f"SELECT COUNT(*) FROM (SELECT DISTINCT {cols_str} FROM {schema}.{table})"
    ).fetchone()[0]
    dupes = n_before - n_distinct
    if dupes == 0:
        return 0

    # Keep last inserted row for each PK (using rowid)
    conn.execute(f"""
        DELETE FROM {schema}.{table}
        WHERE rowid NOT IN (
            SELECT MAX(rowid)
            FROM {schema}.{table}
            GROUP BY {cols_str}
        )
    """)
    n_after = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
    deleted = n_before - n_after
    logger.warning("deduped_rows", table=table, deleted=deleted)
    return deleted


def restore_mart_pks(db_path: Path | None = None) -> dict[str, str]:
    """Restore PRIMARY KEY constraints on all mart tables.

    Returns {table_name: status} where status is one of:
      "added" | "already_exists" | "skipped_empty" | "skipped_missing" | "error:<msg>"
    """
    path = db_path or _get_db_path()
    if not path.exists():
        raise FileNotFoundError(f"DB not found: {path}")

    conn = duckdb.connect(str(path))
    conn.execute("SET memory_limit='8GB'")

    results: dict[str, str] = {}

    try:
        # Ensure mart schema exists
        conn.execute("CREATE SCHEMA IF NOT EXISTS mart")

        # Get existing mart tables
        existing_tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='mart'"
            ).fetchall()
        }

        # Get existing PK constraints in mart
        existing_pks = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.table_constraints "
                "WHERE table_schema='mart' AND constraint_type='PRIMARY KEY'"
            ).fetchall()
        }

        for table, pk_cols in MART_PK_MAP.items():
            if table not in existing_tables:
                logger.info("table_missing_in_mart", table=table)
                results[table] = "skipped_missing"
                continue

            if table in existing_pks:
                logger.info("pk_already_exists", table=table)
                results[table] = "already_exists"
                continue

            # Check row count
            n_rows = conn.execute(f"SELECT COUNT(*) FROM mart.{table}").fetchone()[0]
            if n_rows == 0:
                # Still add PK (needed for future inserts)
                logger.debug("empty_table_adding_pk", table=table)

            # Dedup if needed
            try:
                _dedup_table(conn, "mart", table, pk_cols)
            except Exception as exc:
                logger.error("dedup_failed", table=table, error=str(exc))
                results[table] = f"error:dedup:{exc}"
                continue

            # Add PK constraint
            cols_str = ", ".join(pk_cols)
            try:
                conn.execute(f"ALTER TABLE mart.{table} ADD PRIMARY KEY ({cols_str})")
                logger.info("pk_added", table=table, cols=pk_cols)
                results[table] = "added"
            except Exception as exc:
                err_msg = str(exc)
                logger.error("pk_add_failed", table=table, error=err_msg)
                results[table] = f"error:alter:{err_msg}"

        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    return results


def verify_pk_count(db_path: Path | None = None) -> int:
    """Return current count of PK constraints in mart schema."""
    path = db_path or _get_db_path()
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM information_schema.table_constraints "
            "WHERE table_schema='mart' AND constraint_type='PRIMARY KEY'"
        ).fetchone()[0]
    finally:
        conn.close()


def main() -> int:
    """Entry point. Returns exit code (0=success, 1=failure)."""
    import structlog
    structlog.configure(
        processors=[
            structlog.dev.ConsoleRenderer(),
        ],
        cache_logger_on_first_use=False,
    )

    db_path = _get_db_path()
    print(f"Target DB: {db_path}")
    print(f"DB exists: {db_path.exists()}")

    pk_before = verify_pk_count(db_path)
    print(f"\nPK constraints before: {pk_before}")

    print("\nRestoring PK constraints...")
    results = restore_mart_pks(db_path)

    added = [t for t, s in results.items() if s == "added"]
    already = [t for t, s in results.items() if s == "already_exists"]
    skipped = [t for t, s in results.items() if s.startswith("skipped")]
    errors = [(t, s) for t, s in results.items() if s.startswith("error")]

    print(f"\nResults:")
    print(f"  Added: {len(added)}")
    if added:
        for t in sorted(added):
            print(f"    + {t}")
    print(f"  Already existed: {len(already)}")
    print(f"  Skipped: {len(skipped)}")
    if errors:
        print(f"  Errors: {len(errors)}")
        for t, msg in errors:
            print(f"    ! {t}: {msg}")

    pk_after = verify_pk_count(db_path)
    print(f"\nPK constraints after: {pk_after}")

    if errors:
        print(f"\nFAILED: {len(errors)} errors")
        return 1

    print(f"\nSUCCESS: {pk_after} PK constraints in mart schema")
    return 0


if __name__ == "__main__":
    sys.exit(main())
