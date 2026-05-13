#!/usr/bin/env python3
"""Weekly entity-resolution disagreement snapshot + CUSUM drift detection.

# Architecture note (5-tier model)
# Reads: Conformed layer (animetor.duckdb `conformed` schema) — cross-source attribute
#        disagreements among persons covered by >=2 data sources.
# Writes: Mart layer (animetor.duckdb `mart` schema) — weekly snapshot table
#         `meta_resolution_audit_weekly`.
# This is an explicit two-layer operation. See CLAUDE.md §Five-Tier Database Model
# and src/analysis/quality/resolution_drift.py module docstring.

Intended to run weekly (e.g. via cron or CI scheduled job)::

    pixi run python scripts/monitoring/weekly_resolution_snapshot.py
    pixi run python scripts/monitoring/weekly_resolution_snapshot.py --db result/animetor.duckdb
    pixi run python scripts/monitoring/weekly_resolution_snapshot.py --dry-run

Attributes monitored (cross-source disagreement rate):

    gender        — same canonical person, two sources give different gender values
    hometown      — same canonical person, two sources give different hometowns
    birthday      — same canonical person, two sources give different birth dates
    role_label    — same (person, anime) pair, two sources assign different roles

CUSUM alert threshold: ``CUSUM_THRESHOLD`` (default 0.10).
Alert fires as a structured log WARNING; no automated data modification is performed
(H3 hard constraint: entity-resolution logic is immutable to this module).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

# Ensure repo root is importable for ``src.*`` modules.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import structlog  # noqa: E402

from src.analysis.quality.resolution_drift import (  # noqa: E402
    CUSUM_THRESHOLD,
    CUSUMResult,
    DisagreementRow,
    compute_disagreement_metrics,
    ensure_audit_weekly_table,
    load_history_rates,
    run_cusum,
    write_snapshot,
)

log = structlog.get_logger()


def _iso_week_start(reference: date | None = None) -> str:
    """Return ISO date string for the Monday starting the current (or given) week."""
    d = reference or date.today()
    monday = d - timedelta(days=d.weekday())
    return monday.isoformat()


def _default_db_path() -> Path:
    """Return animetor.duckdb path from environment or conventional location."""
    import os
    env = os.environ.get("ANIMETOR_DB_PATH")
    if env:
        return Path(env)
    try:
        from src.analysis.io.conformed_reader import DEFAULT_DB_PATH
        return DEFAULT_DB_PATH
    except Exception:
        return Path("result/animetor.duckdb")


def run_snapshot(
    db_path: Path,
    *,
    week_start: str | None = None,
    dry_run: bool = False,
    cusum_threshold: float = CUSUM_THRESHOLD,
) -> dict[str, object]:
    """Compute weekly disagreement snapshot and persist to Mart layer.

    Args:
        db_path: Path to animetor.duckdb (Conformed + Mart in one file).
        week_start: ISO date (YYYY-MM-DD) for snapshot key. Defaults to current week Monday.
        dry_run: When True, compute metrics but skip writing to Mart.
        cusum_threshold: CUSUM decision threshold (default CUSUM_THRESHOLD).

    Returns:
        Summary dict with keys: week_start, rows_written, alerts_fired, metrics.
    """
    import duckdb

    week = week_start or _iso_week_start()

    log.info(
        "weekly_snapshot_start",
        db_path=str(db_path),
        week_start=week,
        dry_run=dry_run,
    )

    # Step 1: Query Conformed layer for disagreement rates.
    disagreement_rows: list[DisagreementRow] = compute_disagreement_metrics(db_path)

    if not disagreement_rows:
        log.info("weekly_snapshot_no_data", reason="conformed_schema_absent_or_empty")
        return {"week_start": week, "rows_written": 0, "alerts_fired": 0, "metrics": []}

    # Step 2: Load history from Mart layer and run CUSUM for each attribute.
    cusum_results: dict[str, CUSUMResult] = {}
    alerts_fired = 0

    if not dry_run:
        try:
            mart_conn = duckdb.connect(str(db_path), read_only=False)
            mart_conn.execute("SET memory_limit='4GB'")
            try:
                mart_conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
                mart_conn.execute("SET schema='mart'")
            except Exception:
                pass
            ensure_audit_weekly_table(mart_conn)

            for row in disagreement_rows:
                history = load_history_rates(
                    mart_conn,
                    row.attribute,
                    source_pair=row.source_pair,
                )
                result = run_cusum(
                    history_rates=history,
                    new_rate=row.disagreement_rate,
                    threshold=cusum_threshold,
                )
                cusum_results[row.attribute] = result
                if result.alert:
                    alerts_fired += 1
                    log.warning(
                        "resolution_drift_alert",
                        attribute=row.attribute,
                        week=week,
                        cusum_value=result.cusum_value,
                        threshold=cusum_threshold,
                        disagreement_rate=row.disagreement_rate,
                        n_history=result.n_history,
                    )

            rows_written = write_snapshot(
                mart_conn,
                week_start=week,
                rows=disagreement_rows,
                cusum_results=cusum_results,
            )
            mart_conn.close()
        except Exception as exc:
            log.error("weekly_snapshot_write_failed", error=str(exc))
            rows_written = 0
    else:
        # dry_run: compute CUSUM without DB write
        rows_written = 0
        for row in disagreement_rows:
            result = run_cusum(history_rates=[], new_rate=row.disagreement_rate)
            cusum_results[row.attribute] = result

    metrics_summary = [
        {
            "attribute": r.attribute,
            "disagreement_rate": r.disagreement_rate,
            "n_comparable": r.n_comparable,
            "cusum_value": cusum_results.get(r.attribute, CUSUMResult(0.0, False, 0)).cusum_value,
            "alert": cusum_results.get(r.attribute, CUSUMResult(0.0, False, 0)).alert,
        }
        for r in disagreement_rows
    ]

    log.info(
        "weekly_snapshot_complete",
        week_start=week,
        rows_written=rows_written,
        alerts_fired=alerts_fired,
        dry_run=dry_run,
    )

    return {
        "week_start": week,
        "rows_written": rows_written,
        "alerts_fired": alerts_fired,
        "metrics": metrics_summary,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to animetor.duckdb (default: from ANIMETOR_DB_PATH env or result/animetor.duckdb)",
    )
    parser.add_argument(
        "--week-start",
        default=None,
        metavar="YYYY-MM-DD",
        help="Override week start date (default: current week Monday)",
    )
    parser.add_argument(
        "--cusum-threshold",
        type=float,
        default=CUSUM_THRESHOLD,
        metavar="H",
        help=f"CUSUM decision threshold (default: {CUSUM_THRESHOLD})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute metrics but do not write to Mart layer",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-attribute metric table to stdout",
    )
    args = parser.parse_args(argv)

    db_path = args.db or _default_db_path()
    if not db_path.exists():
        print(
            f"weekly_resolution_snapshot: {db_path} does not exist. Nothing to snapshot.",
            file=sys.stderr,
        )
        return 0

    summary = run_snapshot(
        db_path,
        week_start=args.week_start,
        dry_run=args.dry_run,
        cusum_threshold=args.cusum_threshold,
    )

    if args.verbose:
        print(f"\nWeek: {summary['week_start']}")
        print(f"{'Attribute':<18} {'Rate':>8} {'N':>8} {'CUSUM':>8} {'Alert':>6}")
        print("-" * 52)
        for m in summary["metrics"]:
            alert_mark = "***" if m["alert"] else ""
            print(
                f"{m['attribute']:<18} {m['disagreement_rate']:>8.4f} "
                f"{m['n_comparable']:>8} {m['cusum_value']:>8.4f} {alert_mark:>6}"
            )
        print()
        print(f"Rows written : {summary['rows_written']}")
        print(f"Alerts fired : {summary['alerts_fired']}")
        if args.dry_run:
            print("(dry-run — nothing written)")

    return 0 if summary["alerts_fired"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
