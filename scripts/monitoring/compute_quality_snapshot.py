#!/usr/bin/env python3
"""Compute per-run data-quality metrics and persist to ``meta_quality_snapshot``.

Intended invocation is once per pipeline run (e.g. at the tail of
``pixi run pipeline``), or standalone for ad-hoc diagnostics::

    pixi run python scripts/monitoring/compute_quality_snapshot.py
    pixi run python scripts/monitoring/compute_quality_snapshot.py --db data/animetor.db

Metrics captured (per §1.4 V-3, extensible):

    anime.row_count
    anime.null_rate_year
    anime.distinct_formats
    credits.row_count
    credits.unique_persons
    credits.unique_anime
    credits.evidence_sources
    persons.row_count
    persons.null_rate_gender
    studios.row_count

Each metric is a single float; the primary key is
(computed_at, table_name, metric) so re-runs within the same second are
idempotent.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure repo root is importable for ``src.*`` modules.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import structlog  # noqa: E402

log = structlog.get_logger()

MetricSpec = tuple[str, str, str]  # (table_name, metric, SQL returning REAL)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    except sqlite3.Error:
        return False
    return column in cols


def _default_specs(conn: sqlite3.Connection) -> list[MetricSpec]:
    specs: list[MetricSpec] = []
    if _table_exists(conn, "anime"):
        specs.append(("anime", "row_count", "SELECT COUNT(*) FROM anime"))
        if _column_exists(conn, "anime", "year"):
            specs.append((
                "anime", "null_rate_year",
                "SELECT AVG(CASE WHEN year IS NULL THEN 1.0 ELSE 0.0 END) "
                "FROM anime",
            ))
        if _column_exists(conn, "anime", "format"):
            specs.append((
                "anime", "distinct_formats",
                "SELECT COUNT(DISTINCT format) FROM anime",
            ))
    if _table_exists(conn, "credits"):
        specs.extend([
            ("credits", "row_count", "SELECT COUNT(*) FROM credits"),
            ("credits", "unique_persons",
             "SELECT COUNT(DISTINCT person_id) FROM credits"),
            ("credits", "unique_anime",
             "SELECT COUNT(DISTINCT anime_id) FROM credits"),
        ])
        if _column_exists(conn, "credits", "evidence_source"):
            specs.append((
                "credits", "evidence_sources",
                "SELECT COUNT(DISTINCT evidence_source) FROM credits",
            ))
    if _table_exists(conn, "persons"):
        specs.append(("persons", "row_count", "SELECT COUNT(*) FROM persons"))
        if _column_exists(conn, "persons", "gender"):
            specs.append((
                "persons", "null_rate_gender",
                "SELECT AVG(CASE WHEN gender IS NULL OR gender='' "
                "THEN 1.0 ELSE 0.0 END) FROM persons",
            ))
    if _table_exists(conn, "studios"):
        specs.append(("studios", "row_count", "SELECT COUNT(*) FROM studios"))
    return specs


def compute_and_write(
    conn: sqlite3.Connection,
    *,
    computed_at: str | None = None,
) -> list[tuple[str, str, float]]:
    """Compute the default metric set and insert into ``meta_quality_snapshot``.

    Returns the list of rows written.
    """
    from src.database import ensure_meta_quality_snapshot

    ensure_meta_quality_snapshot(conn)
    computed_at = computed_at or datetime.now(timezone.utc).isoformat(
        timespec="seconds",
    )

    specs = _default_specs(conn)
    rows: list[tuple[str, str, float]] = []
    for table, metric, sql in specs:
        try:
            result = conn.execute(sql).fetchone()
            value = float(result[0]) if result and result[0] is not None else 0.0
        except sqlite3.Error as exc:
            log.warning("metric_compute_failed", table=table, metric=metric,
                        sql=sql, error=str(exc))
            continue
        rows.append((table, metric, value))

    if rows:
        conn.executemany(
            "INSERT OR REPLACE INTO meta_quality_snapshot "
            "(computed_at, table_name, metric, value) VALUES (?, ?, ?, ?)",
            [(computed_at, t, m, v) for (t, m, v) in rows],
        )
        conn.commit()

    log.info("quality_snapshot_written",
             computed_at=computed_at, count=len(rows))
    return rows


def _default_db_path() -> Path:
    try:
        from src.utils.config import DB_PATH  # type: ignore[import-not-found]
        return Path(DB_PATH)
    except Exception:
        return Path("data/animetor.db")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    db_path = args.db or _default_db_path()
    if not db_path.exists():
        print(
            f"compute_quality_snapshot: database {db_path} does not exist. "
            "Nothing to snapshot.",
            file=sys.stderr,
        )
        return 0

    with sqlite3.connect(str(db_path)) as conn:
        rows = compute_and_write(conn)

    if args.verbose:
        for table, metric, value in rows:
            print(f"  {table}.{metric} = {value}")
    print(f"compute_quality_snapshot: wrote {len(rows)} metric(s) to {db_path}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
