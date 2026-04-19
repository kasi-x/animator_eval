#!/usr/bin/env python3
"""CI check: bronze / anime.score leak detector via ``meta_lineage``.

Design
------
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

Behaviour
---------
This script:

1. Opens the default SQLite database.
2. If the ``meta_lineage`` table is missing, exits 0 with an advisory
   message (the lineage schema belongs to the Phase 1 agent; we do not
   fail CI before it lands).
3. Otherwise counts rows where::

       source_bronze_forbidden = 0 AND audience != 'technical_appendix'

   Any such row makes the script exit 1 and print the offenders.

Usage::

    pixi run python scripts/report_generators/ci_check_lineage.py
    pixi run python scripts/report_generators/ci_check_lineage.py --db path/to/db.sqlite
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
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


def check_lineage(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Return list of (table_name, audience) that violate the public rule.

    An empty list means the database is compliant.
    """
    if not _table_exists(conn, "meta_lineage"):
        log.info("meta_lineage_absent_skipping")
        return []

    cols = _column_names(conn, "meta_lineage")
    required = {"table_name", "source_bronze_forbidden", "audience"}
    missing = required - set(cols)
    if missing:
        log.warning(
            "meta_lineage_schema_incomplete",
            missing=sorted(missing),
            available=cols,
        )
        return []

    rows = conn.execute(
        """
        SELECT table_name, audience
        FROM meta_lineage
        WHERE COALESCE(source_bronze_forbidden, 1) = 0
          AND COALESCE(audience, '') != 'technical_appendix'
        ORDER BY table_name
        """
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _default_db_path() -> Path:
    try:
        from src.utils.config import DB_PATH  # type: ignore[import-not-found]
        return Path(DB_PATH)
    except Exception:
        return Path("data/animetor.db")


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

    if not leaky:
        print("ci_check_lineage: OK — no bronze leaks in public audiences.")
        return 0

    print(
        "ci_check_lineage: FAIL — public audiences must not include "
        "bronze-permitted tables (anime.score et al):"
    )
    for name, audience in leaky:
        print(f"  - {name}  (audience={audience!r})")
    print(
        "\nResolution: either set source_bronze_forbidden=1 for the table, "
        "or move it to audience='technical_appendix'."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
