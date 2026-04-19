#!/usr/bin/env python3
"""Detect 3σ-level drift in ``meta_quality_snapshot``.

For every (table_name, metric), compute mean and sample std over the prior
N snapshots (default: 30). If the most recent snapshot deviates from that
mean by more than ``--sigma`` (default 3.0) standard deviations, flag it.

Exit codes:
    0 — no anomalies (or too few snapshots to judge)
    1 — at least one metric exceeded the drift threshold

Usage::

    pixi run python scripts/monitoring/check_quality_anomaly.py
    pixi run python scripts/monitoring/check_quality_anomaly.py --sigma 2.5 --window 60

Safety: if fewer than ``--min-history`` prior snapshots exist, the script
cannot judge drift and exits 0 with a note.
"""

from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

# Ensure repo root is importable for ``src.*`` modules.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import structlog  # noqa: E402

log = structlog.get_logger()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def detect_anomalies(
    conn: sqlite3.Connection,
    *,
    window: int = 30,
    sigma: float = 3.0,
    min_history: int = 3,
) -> list[tuple[str, str, float, float, float]]:
    """Return list of (table, metric, latest, mean, std) exceeding sigma.

    Uses only the latest snapshot per (table, metric) vs. the previous
    ``window`` snapshots for that metric.
    """
    if not _table_exists(conn, "meta_quality_snapshot"):
        log.info("meta_quality_snapshot_absent_skipping")
        return []

    rows = conn.execute(
        """
        SELECT table_name, metric, computed_at, value
        FROM meta_quality_snapshot
        ORDER BY table_name, metric, computed_at DESC
        """
    ).fetchall()

    # Group by (table, metric) preserving descending order.
    groups: dict[tuple[str, str], list[tuple[str, float]]] = defaultdict(list)
    for table, metric, computed_at, value in rows:
        groups[(table, metric)].append((computed_at, float(value)))

    anomalies: list[tuple[str, str, float, float, float]] = []
    for (table, metric), points in groups.items():
        if len(points) <= min_history:
            continue
        latest_ts, latest = points[0]
        history = [v for _, v in points[1:1 + window]]
        if len(history) < min_history:
            continue
        mean = sum(history) / len(history)
        if len(history) < 2:
            continue
        var = sum((v - mean) ** 2 for v in history) / (len(history) - 1)
        std = math.sqrt(var)
        if std == 0:
            # Flag if latest is different from the flat history.
            if latest != mean:
                anomalies.append((table, metric, latest, mean, std))
            continue
        if abs(latest - mean) > sigma * std:
            anomalies.append((table, metric, latest, mean, std))
    return anomalies


def _default_db_path() -> Path:
    try:
        from src.utils.config import DB_PATH  # type: ignore[import-not-found]
        return Path(DB_PATH)
    except Exception:
        return Path("data/animetor.db")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--window", type=int, default=30,
                        help="Number of prior snapshots to compare against.")
    parser.add_argument("--sigma", type=float, default=3.0,
                        help="Standard-deviation threshold.")
    parser.add_argument("--min-history", type=int, default=3,
                        help="Minimum prior points required to judge.")
    args = parser.parse_args(argv)

    db_path = args.db or _default_db_path()
    if not db_path.exists():
        print(
            f"check_quality_anomaly: database {db_path} does not exist. "
            "No history available; skipping.",
            file=sys.stderr,
        )
        return 0

    with sqlite3.connect(str(db_path)) as conn:
        anomalies = detect_anomalies(
            conn,
            window=args.window,
            sigma=args.sigma,
            min_history=args.min_history,
        )

    if not anomalies:
        print("check_quality_anomaly: OK — no drift detected.")
        return 0

    print(f"check_quality_anomaly: FAIL — {len(anomalies)} metric(s) drifted:")
    for table, metric, latest, mean, std in anomalies:
        print(
            f"  {table}.{metric}: latest={latest:.6g} "
            f"mean={mean:.6g} std={std:.6g} "
            f"(Δ={latest - mean:+.6g}, {args.sigma}σ={args.sigma * std:.6g})"
        )
    return 1


if __name__ == "__main__":
    sys.exit(main())
