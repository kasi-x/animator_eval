"""v64_keyframe_studio_prefix_unify_conformed.py — One-shot migration.

Migrate `kf:*` → `keyframe:*` prefix in the production Conformed DB
(`result/animetor.duckdb`, schema=`conformed`). Companion to v63 which
covered the legacy `silver.duckdb`.

Background:
  v63 migrated the legacy `silver.duckdb` (used only by anilist scraper
  since-mode and tests). The production pipeline reads from
  `animetor.duckdb` `conformed` schema; that DB still carries the old
  `kf:` prefix on studio rows. After this migration:
    - conformed.studios.id `kf:s%` / `kf:n:%` → `keyframe:s%` / `keyframe:n:%`
    - conformed.anime_studios.studio_id similarly

Run once (after backing up animetor.duckdb):
    pixi run python scripts/maintenance/v64_keyframe_studio_prefix_unify_conformed.py

Safety:
  - Pre-flight: counts target rows + collision check
  - Transaction: all updates in one BEGIN/COMMIT
  - Idempotent
  - Resolved layer must be rebuilt afterwards (see build_resolved_studios)
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import structlog

logger = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CONFORMED_DB = REPO_ROOT / "result" / "animetor.duckdb"


def _count(conn: duckdb.DuckDBPyConnection, sql: str) -> int:
    return conn.execute(sql).fetchone()[0]


def _preflight(conn: duckdb.DuckDBPyConnection) -> bool:
    studios_kf = _count(
        conn,
        "SELECT COUNT(*) FROM conformed.studios "
        "WHERE id LIKE 'kf:s%' OR id LIKE 'kf:n:%'",
    )
    as_kf = _count(
        conn,
        "SELECT COUNT(*) FROM conformed.anime_studios "
        "WHERE studio_id LIKE 'kf:s%' OR studio_id LIKE 'kf:n:%'",
    )
    print(f"conformed.studios rows with kf: prefix      : {studios_kf}")
    print(f"conformed.anime_studios rows with kf: prefix : {as_kf}")

    if studios_kf == 0 and as_kf == 0:
        print("Nothing to migrate — already unified.")
        return False

    collision = conn.execute(
        """
        SELECT COUNT(*) FROM conformed.studios s1
        WHERE (s1.id LIKE 'kf:s%' OR s1.id LIKE 'kf:n:%')
          AND EXISTS (
            SELECT 1 FROM conformed.studios s2
            WHERE s2.id = 'keyframe:' || substr(s1.id, 4)
          )
        """
    ).fetchone()[0]
    if collision > 0:
        print(f"ERROR: {collision} target keyframe:* ids collide — manual fix required.")
        return False
    return True


def _migrate(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            "UPDATE conformed.studios SET id = 'keyframe:' || substr(id, 4) "
            "WHERE id LIKE 'kf:s%' OR id LIKE 'kf:n:%'"
        )
        conn.execute(
            "UPDATE conformed.anime_studios "
            "SET studio_id = 'keyframe:' || substr(studio_id, 4) "
            "WHERE studio_id LIKE 'kf:s%' OR studio_id LIKE 'kf:n:%'"
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def main() -> int:
    if not CONFORMED_DB.exists():
        print(f"ERROR: conformed DB not found at {CONFORMED_DB}")
        return 1
    conn = duckdb.connect(str(CONFORMED_DB))
    try:
        if not _preflight(conn):
            return 0
        print("Running migration...")
        _migrate(conn)
        remaining = _count(
            conn,
            "SELECT COUNT(*) FROM conformed.studios "
            "WHERE id LIKE 'kf:s%' OR id LIKE 'kf:n:%'",
        )
        unified = _count(
            conn,
            "SELECT COUNT(*) FROM conformed.studios "
            "WHERE id LIKE 'keyframe:s%' OR id LIKE 'keyframe:n:%'",
        )
        print(f"remaining kf:* rows in conformed.studios : {remaining}")
        print(f"new keyframe:* rows in conformed.studios : {unified}")
        if remaining > 0:
            print("WARN: not all rows migrated")
            return 2
        print("Migration complete. Rebuild resolved.studios next.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
