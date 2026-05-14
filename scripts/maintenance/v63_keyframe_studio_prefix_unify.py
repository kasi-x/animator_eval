"""v63_keyframe_studio_prefix_unify.py — One-shot migration.

Rename keyframe studio IDs in silver from the inconsistent `kf:` prefix to the
unified `keyframe:` prefix, matching anime/persons rows.

Background:
  src/etl/conformed_loaders/keyframe.py historically wrote studio IDs with the
  `kf:s` / `kf:n:` prefix while anime/persons used `keyframe:`. The writer is
  now updated to emit `keyframe:` consistently, but existing silver rows must
  be migrated.

Run once (after backing up silver.duckdb):
    pixi run python scripts/maintenance/v63_keyframe_studio_prefix_unify.py

Safety:
  - Pre-flight: counts `kf:s%` / `kf:n:%` rows in silver.studios + anime_studios
  - Refuses to run if any target `keyframe:s%` / `keyframe:n:%` id already
    exists in silver.studios (would cause PK conflicts)
  - Transaction: all updates in one BEGIN/COMMIT
  - Idempotent: re-running after migration completes is a no-op
  - Does NOT touch resolved.duckdb / mart — those are rebuilt downstream
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import structlog

logger = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DB = REPO_ROOT / "result" / "silver.duckdb"


def _count(conn: duckdb.DuckDBPyConnection, sql: str) -> int:
    return conn.execute(sql).fetchone()[0]


def _preflight(conn: duckdb.DuckDBPyConnection) -> bool:
    """Return True when migration is safe to run."""
    studios_kf = _count(
        conn,
        "SELECT COUNT(*) FROM studios WHERE id LIKE 'kf:s%' OR id LIKE 'kf:n:%'",
    )
    as_kf = _count(
        conn,
        "SELECT COUNT(*) FROM anime_studios "
        "WHERE studio_id LIKE 'kf:s%' OR studio_id LIKE 'kf:n:%'",
    )
    print(f"studios rows with kf: prefix      : {studios_kf}")
    print(f"anime_studios rows with kf: prefix : {as_kf}")

    if studios_kf == 0 and as_kf == 0:
        print("Nothing to migrate — silver already unified.")
        return False

    # Collision check: any target keyframe:s% / keyframe:n:% IDs that already
    # exist would block the UPDATE.
    collision = conn.execute(
        """
        SELECT COUNT(*) FROM studios s1
        WHERE (s1.id LIKE 'kf:s%' OR s1.id LIKE 'kf:n:%')
          AND EXISTS (
            SELECT 1 FROM studios s2
            WHERE s2.id = 'keyframe:' || substr(s1.id, 4)
          )
        """
    ).fetchone()[0]
    if collision > 0:
        print(
            f"ERROR: {collision} target keyframe:* ids already exist — "
            "manual reconciliation required before migration."
        )
        return False
    return True


def _migrate(conn: duckdb.DuckDBPyConnection) -> None:
    """Rename kf:* → keyframe:* in silver.studios + anime_studios.studio_id."""
    conn.execute("BEGIN TRANSACTION")
    try:
        # studios: PK is id → must use UPDATE (no FK from credits to studios.id)
        conn.execute(
            "UPDATE studios SET id = 'keyframe:' || substr(id, 4) "
            "WHERE id LIKE 'kf:s%' OR id LIKE 'kf:n:%'"
        )
        # anime_studios: studio_id refers to studios.id by convention (no FK)
        conn.execute(
            "UPDATE anime_studios SET studio_id = 'keyframe:' || substr(studio_id, 4) "
            "WHERE studio_id LIKE 'kf:s%' OR studio_id LIKE 'kf:n:%'"
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def main() -> int:
    if not SILVER_DB.exists():
        print(f"ERROR: silver DB not found at {SILVER_DB}")
        return 1
    conn = duckdb.connect(str(SILVER_DB))
    try:
        if not _preflight(conn):
            return 0
        print("Running migration...")
        _migrate(conn)
        # Post-flight summary
        remaining = _count(
            conn,
            "SELECT COUNT(*) FROM studios WHERE id LIKE 'kf:s%' OR id LIKE 'kf:n:%'",
        )
        unified = _count(
            conn,
            "SELECT COUNT(*) FROM studios "
            "WHERE id LIKE 'keyframe:s%' OR id LIKE 'keyframe:n:%'",
        )
        print(f"remaining kf:* rows in studios     : {remaining}")
        print(f"new keyframe:* rows in studios     : {unified}")
        if remaining > 0:
            print("WARN: not all rows migrated — check logs")
            return 2
        print("Migration complete.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
