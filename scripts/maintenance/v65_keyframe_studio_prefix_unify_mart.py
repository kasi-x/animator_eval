"""v65_keyframe_studio_prefix_unify_mart.py — One-shot migration.

Migrate `kf:*` → `keyframe:*` in mart-layer feat_studio_affiliation.studio_id,
across both `gold.duckdb` (legacy) and `animetor.duckdb`.`mart` (current).

Companion to v63 (silver) + v64 (animetor.conformed). After v63/v64 the upstream
conformed `anime_studios` carry the unified `keyframe:*` IDs, but historical
mart features were materialized with `kf:*` and need direct UPDATE.

Run once (after backing up both DBs):
    pixi run python scripts/maintenance/v65_keyframe_studio_prefix_unify_mart.py

Safety:
  - Pre-flight: counts target rows in each DB/table
  - Transaction per DB: BEGIN/COMMIT
  - Idempotent
  - Does NOT touch resolved.duckdb (already rebuilt via build_resolved_studios)
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import structlog

logger = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TARGETS: list[tuple[Path, str]] = [
    (REPO_ROOT / "result" / "gold.duckdb", "main.feat_studio_affiliation"),
    (REPO_ROOT / "result" / "animetor.duckdb", "mart.feat_studio_affiliation"),
]


def _count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    return conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE studio_id LIKE 'kf:%'"
    ).fetchone()[0]


def _migrate_one(db: Path, table: str) -> tuple[int, int]:
    """Returns (pre_count, post_count)."""
    conn = duckdb.connect(str(db))
    try:
        pre = _count(conn, table)
        if pre == 0:
            return pre, 0
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                f"UPDATE {table} SET studio_id = 'keyframe:' || substr(studio_id, 4) "
                f"WHERE studio_id LIKE 'kf:%'"
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        post = _count(conn, table)
        return pre, post
    finally:
        conn.close()


def main() -> int:
    rc = 0
    for db, table in TARGETS:
        if not db.exists():
            print(f"skip (missing): {db}")
            continue
        pre, post = _migrate_one(db, table)
        print(f"{db.name}/{table}: pre={pre}  post_remaining={post}")
        if post > 0:
            rc = 2
    if rc == 0:
        print("Mart migration complete.")
    return rc


if __name__ == "__main__":
    sys.exit(main())
