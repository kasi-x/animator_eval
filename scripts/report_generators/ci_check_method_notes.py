#!/usr/bin/env python3
"""CI gate: every required report must have a meta_lineage row with non-empty fields.

Checks that each of the 5 mandatory reports has a corresponding entry in
meta_lineage (or ops_lineage on a fresh v2 schema) with non-empty values for
formula_version, ci_method, null_model, and inputs_hash.

Usage::

    pixi run python scripts/report_generators/ci_check_method_notes.py
    pixi run python scripts/report_generators/ci_check_method_notes.py --db path/to/db
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Reports whose Method Notes are mandatory — keyed by meta_lineage.table_name
REQUIRED_TABLE_NAMES = [
    "meta_policy_attrition",
    "meta_policy_monopsony",
    "meta_policy_gender",
    "meta_hr_studio_benchmark",
    "meta_biz_whitespace",
]

REQUIRED_FIELDS = ["formula_version", "ci_method", "null_model", "inputs_hash"]


def _resolve_db(arg: Path | None) -> Path:
    if arg is not None:
        return arg
    try:
        from src.utils.config import DB_PATH  # type: ignore[import-not-found]
        return Path(DB_PATH)
    except Exception:
        return Path("result/db/animetor_eval.db")


def _lineage_table(conn: sqlite3.Connection) -> str | None:
    """Return the available lineage table name, preferring meta_lineage."""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name IN ('meta_lineage','ops_lineage') ORDER BY name"
    ).fetchone()
    if row is None:
        return None
    # Prefer meta_lineage (production legacy schema)
    all_names = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('meta_lineage','ops_lineage')"
        ).fetchall()
    }
    return "meta_lineage" if "meta_lineage" in all_names else "ops_lineage"


def check_method_notes(conn: sqlite3.Connection) -> list[str]:
    """Return list of error messages; empty list means all required notes are present."""
    ltable = _lineage_table(conn)
    if ltable is None:
        return ["neither meta_lineage nor ops_lineage table found in database"]

    errors: list[str] = []
    conn.row_factory = sqlite3.Row
    for table_name in REQUIRED_TABLE_NAMES:
        row = conn.execute(
            f"SELECT * FROM {ltable} WHERE table_name = ?", (table_name,)
        ).fetchone()
        if row is None:
            errors.append(f"missing lineage row: {table_name}")
            continue
        keys = row.keys()
        for field in REQUIRED_FIELDS:
            if field not in keys:
                errors.append(f"{table_name}.{field}: column not present in {ltable}")
            elif not row[field]:
                errors.append(f"{table_name}.{field}: empty/null")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=None)
    args = parser.parse_args(argv)

    db_path = _resolve_db(args.db)
    if not db_path.exists():
        print(
            f"ci_check_method_notes: database {db_path} not found — skipping "
            "(run 'pixi run pipeline' or generate reports first).",
            file=sys.stderr,
        )
        return 0

    with sqlite3.connect(str(db_path)) as conn:
        errors = check_method_notes(conn)

    if errors:
        print("ci_check_method_notes: FAIL", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        print(
            "\nResolution: run the 5 report generators (policy_attrition, "
            "policy_monopsony, policy_gender_bottleneck, mgmt_studio_benchmark, "
            "biz_genre_whitespace) so they insert lineage rows.",
            file=sys.stderr,
        )
        return 1

    print(
        f"ci_check_method_notes: OK — {len(REQUIRED_TABLE_NAMES)} report(s) validated"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
