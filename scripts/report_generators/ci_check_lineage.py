#!/usr/bin/env python3
"""CI check: bronze / anime.score leak detector + lineage quality validator.

Two checks are performed:

Check 1 — Bronze leak detection (original)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The canonical lineage table is ``meta_lineage`` (Phase 1 / §1.4). Each
row declares, for a gold/feat/meta table:

  * ``table_name``              — the dependent table
  * ``source_silver_tables``    — comma-separated list of silver tables
  * ``source_bronze_forbidden`` — 1 if the table obeys the "no bronze"
                                   rule (anime.score excluded), 0 if the
                                   row is allowed to pull bronze fields
                                   (technical appendix only).
  * ``audience``                — one of {'policy', 'hr', 'biz',
                                   'technical_appendix', ...}.

A public report must only read from tables where
``source_bronze_forbidden = 1`` OR where audience is
``'technical_appendix'``. Any other combination is a contamination leak:
non-appendix audience + bronze allowed.

Check 2 — Lineage quality validation (new)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For every row in meta_lineage (or ops_lineage), validates:

  * ``formula_version``  is semver-like: ``vX.Y[.Z]``
  * ``inputs_hash``      is a hex string of at least 16 characters
  * ``description``      is at least 50 characters
  * ``computed_at``      is an ISO-8601 timestamp not older than 30 days

Behaviour
---------
1. Opens the default SQLite database.
2. If the lineage table is missing, exits 0 with an advisory message.
3. Otherwise runs both checks and exits 1 if either fails.

Usage::

    pixi run python scripts/report_generators/ci_check_lineage.py
    pixi run python scripts/report_generators/ci_check_lineage.py --db path/to/db.sqlite
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure repo root is importable for ``src.*`` modules.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import structlog  # noqa: E402

log = structlog.get_logger()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def _column_names(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Check 1: bronze leak detection (original)
# ---------------------------------------------------------------------------

def _resolve_lineage_table(conn: sqlite3.Connection) -> str | None:
    """Return the available lineage table name, preferring meta_lineage."""
    available = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('meta_lineage','ops_lineage')"
        ).fetchall()
    }
    if not available:
        return None
    return "meta_lineage" if "meta_lineage" in available else "ops_lineage"


def check_lineage(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Return list of (table_name, audience) that violate the public rule.

    An empty list means the database is compliant.
    """
    ltable = _resolve_lineage_table(conn)
    if ltable is None:
        log.info("lineage_table_absent_skipping")
        return []

    cols = _column_names(conn, ltable)
    required = {"table_name", "source_bronze_forbidden", "audience"}
    missing = required - set(cols)
    if missing:
        log.warning(
            "lineage_schema_incomplete",
            table=ltable,
            missing=sorted(missing),
            available=cols,
        )
        return []

    rows = conn.execute(
        f"""
        SELECT table_name, audience
        FROM {ltable}
        WHERE COALESCE(source_bronze_forbidden, 1) = 0
          AND COALESCE(audience, '') != 'technical_appendix'
        ORDER BY table_name
        """
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


# ---------------------------------------------------------------------------
# Check 2: lineage quality validation (new)
# ---------------------------------------------------------------------------

_SEMVER_RE = re.compile(r"^v\d+\.\d+(\.\d+)?(-[a-z0-9]+)?$", re.IGNORECASE)
_HEX_RE = re.compile(r"^[0-9a-f]{16,}$", re.IGNORECASE)
_STALE_DAYS = 30
_MIN_DESC_LEN = 50


def _check_formula_version(value: str | None, rid: str) -> list[str]:
    if not value:
        return [f"{rid}: formula_version missing"]
    if not _SEMVER_RE.match(value):
        return [f"{rid}: formula_version {value!r} not semver-like (expected vX.Y[.Z])"]
    return []


def _check_inputs_hash(value: str | None, rid: str) -> list[str]:
    if not value:
        return [f"{rid}: inputs_hash missing"]
    if not _HEX_RE.match(value):
        return [f"{rid}: inputs_hash {value!r} is not hex (expected >=16 chars)"]
    return []


def _check_description(value: str | None, rid: str) -> list[str]:
    length = len((value or "").strip())
    if length < _MIN_DESC_LEN:
        return [f"{rid}: description too short ({length} chars, need ≥{_MIN_DESC_LEN})"]
    return []


def _check_staleness(computed_at: str | None, rid: str) -> list[str]:
    if not computed_at:
        return [f"{rid}: computed_at missing"]
    try:
        ts = datetime.fromisoformat(computed_at.replace("Z", "+00:00"))
    except ValueError:
        return [f"{rid}: computed_at {computed_at!r} is not ISO-8601"]
    now = datetime.now(tz=timezone.utc)
    # Make ts timezone-aware if naive
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    if now - ts > timedelta(days=_STALE_DAYS):
        return [f"{rid}: computed_at is stale (>{_STALE_DAYS} days old: {computed_at})"]
    return []


def validate_lineage_quality(conn: sqlite3.Connection) -> list[str]:
    """Validate all lineage rows for quality (semver, hex, description, staleness).

    Returns list of human-readable error messages; empty = OK.
    """
    ltable = _resolve_lineage_table(conn)
    if ltable is None:
        return []  # Nothing to validate; bronze check already handles the advisory

    cols = set(_column_names(conn, ltable))
    if not cols:
        return [f"{ltable} table has no columns"]

    conn_rf = conn
    conn_rf.row_factory = sqlite3.Row
    rows = conn_rf.execute(f"SELECT * FROM {ltable}").fetchall()

    if not rows:
        return []  # Empty table — nothing to validate

    errors: list[str] = []
    for row in rows:
        rid = row["table_name"] if "table_name" in row.keys() else "<unknown>"

        if "formula_version" in cols:
            errors.extend(_check_formula_version(row["formula_version"], rid))
        if "inputs_hash" in cols:
            errors.extend(_check_inputs_hash(row["inputs_hash"], rid))
        if "description" in cols:
            errors.extend(_check_description(row["description"], rid))
        if "computed_at" in cols:
            errors.extend(_check_staleness(row["computed_at"], rid))
        if "ci_method" in cols and not row["ci_method"]:
            errors.append(f"{rid}: ci_method missing")
        if "null_model" in cols and not row["null_model"]:
            errors.append(f"{rid}: null_model missing")

    return errors


def _default_db_path() -> Path:
    try:
        from src.utils.config import DB_PATH  # type: ignore[import-not-found]
        return Path(DB_PATH)
    except Exception:
        return Path("result/db/animetor_eval.db")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Path to SQLite database (defaults to src.utils.config.DB_PATH).",
    )
    args = parser.parse_args(argv)

    db_path = args.db or _default_db_path()
    if not db_path.exists():
        print(
            f"ci_check_lineage: database {db_path} does not exist. "
            "Skipping check (presumably a fresh checkout).",
            file=sys.stderr,
        )
        return 0

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        leaky = check_lineage(conn)
        quality_errors = validate_lineage_quality(conn)

    all_ok = not leaky and not quality_errors
    if all_ok:
        ltable = _resolve_lineage_table(sqlite3.connect(str(db_path)))
        if ltable:
            n = sqlite3.connect(str(db_path)).execute(
                f"SELECT COUNT(*) FROM {ltable}"
            ).fetchone()[0]
            print(f"ci_check_lineage: OK — {n} row(s) validated, no bronze leaks.")
        else:
            print("ci_check_lineage: OK — lineage table absent (advisory).")
        return 0

    if leaky:
        print(
            "ci_check_lineage: FAIL — public audiences must not include "
            "bronze-permitted tables (anime.score et al):",
            file=sys.stderr,
        )
        for name, audience in leaky:
            print(f"  - {name}  (audience={audience!r})", file=sys.stderr)
        print(
            "\nResolution: either set source_bronze_forbidden=1 for the table, "
            "or move it to audience='technical_appendix'.",
            file=sys.stderr,
        )

    if quality_errors:
        print("ci_check_lineage: FAIL — lineage quality issues:", file=sys.stderr)
        for e in quality_errors:
            print(f"  - {e}", file=sys.stderr)
        print(
            "\nResolution: re-run the report generator so it inserts a fresh lineage row "
            "with formula_version (vX.Y), inputs_hash (≥16 hex chars), "
            f"description (≥{_MIN_DESC_LEN} chars), and a recent computed_at.",
            file=sys.stderr,
        )

    return 1


if __name__ == "__main__":
    sys.exit(main())
