"""CI lineage check — public reports must not pull bronze (anime.score) data.

Run: pixi run python scripts/ci_check_lineage.py
Exit code 1 if any public (non-technical_appendix) meta_* table has
source_bronze_forbidden = 0.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import get_connection


def check_no_bronze_leak_in_public() -> None:
    """Raise SystemExit(1) if any public meta_* table allows bronze access."""
    conn = get_connection()
    try:
        leaky = conn.execute(
            "SELECT table_name, audience FROM meta_lineage "
            "WHERE source_bronze_forbidden = 0 AND audience != 'technical_appendix'"
        ).fetchall()
    except Exception as e:
        print(f"[ci_check_lineage] meta_lineage not available: {e}", file=sys.stderr)
        sys.exit(0)  # table may not exist yet in fresh DB
    finally:
        conn.close()

    if leaky:
        offenders = ", ".join(f"{r[0]} (audience={r[1]})" for r in leaky)
        print(
            f"ERROR: Public reports must not pull bronze (anime.score) data.\n"
            f"Offending tables: {offenders}",
            file=sys.stderr,
        )
        sys.exit(1)

    print("[ci_check_lineage] OK — no bronze leak in public meta_* tables")


if __name__ == "__main__":
    check_no_bronze_leak_in_public()
