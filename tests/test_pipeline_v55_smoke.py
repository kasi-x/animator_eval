"""Smoke test: fresh v55 schema init is consistent.

Verifies that after init_db() on an empty SQLite file:
- get_schema_version() returns SCHEMA_VERSION (55)
- The v2 canonical tables (ops_lineage, person_scores as TABLE) exist
- Deprecated legacy tables (meta_lineage, anime_display, scores) are absent
- sources table contains the required seed codes
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from src.database import SCHEMA_VERSION, get_schema_version, init_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _table_type(conn: sqlite3.Connection, name: str) -> str | None:
    """Return 'table' or 'view' for name, or None if absent."""
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name = ?", (name,)
    ).fetchone()
    return row[0] if row else None


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return _table_type(conn, name) == "table"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_fresh_init_reaches_schema_version_55(tmp_path: Path) -> None:
    """init_db must set schema_version to SCHEMA_VERSION (55)."""
    conn = sqlite3.connect(str(tmp_path / "fresh.db"))
    conn.row_factory = sqlite3.Row
    try:
        init_db(conn)
        version = get_schema_version(conn)
    finally:
        conn.close()

    assert version == SCHEMA_VERSION, (
        f"Expected schema_version {SCHEMA_VERSION}, got {version}. "
        "Check that init_db_v2 inserts the version into schema_meta."
    )
    assert version >= 55, "v55 baseline must be reached"


def test_fresh_init_has_v2_ops_lineage(tmp_path: Path) -> None:
    """ops_lineage (v2 lineage table) must exist as a TABLE after init_db."""
    conn = sqlite3.connect(str(tmp_path / "fresh.db"))
    try:
        init_db(conn)
        has_ops = _table_exists(conn, "ops_lineage")
        is_view = _table_type(conn, "ops_lineage") == "view"
    finally:
        conn.close()

    assert has_ops, "ops_lineage TABLE missing after init_db"
    assert not is_view, "ops_lineage must be a TABLE, not a VIEW"


def test_fresh_init_person_scores_is_table_not_view(tmp_path: Path) -> None:
    """person_scores must be a TABLE (not a VIEW) after v53 migration."""
    conn = sqlite3.connect(str(tmp_path / "fresh.db"))
    try:
        init_db(conn)
        t = _table_type(conn, "person_scores")
    finally:
        conn.close()

    assert t == "table", (
        f"person_scores must be a TABLE; got type={t!r}. "
        "Likely schema_fix/03 (person_scores VIEW→TABLE) is not applied."
    )


def test_fresh_init_no_deprecated_tables(tmp_path: Path) -> None:
    """Deprecated tables must not exist in a fresh v2 schema."""
    deprecated = {"meta_lineage", "anime_display", "scores", "data_sources", "va_scores"}
    conn = sqlite3.connect(str(tmp_path / "fresh.db"))
    try:
        init_db(conn)
        all_tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()

    leaked = deprecated & all_tables
    assert not leaked, (
        f"Deprecated tables must not exist in fresh v2 schema: {sorted(leaked)}"
    )


def test_fresh_init_sources_seeded(tmp_path: Path) -> None:
    """sources lookup must contain the canonical 5 seed codes."""
    conn = sqlite3.connect(str(tmp_path / "fresh.db"))
    conn.row_factory = sqlite3.Row
    try:
        init_db(conn)
        rows = conn.execute("SELECT code FROM sources ORDER BY code").fetchall()
    finally:
        conn.close()

    codes = {r["code"] for r in rows}
    expected = {"anilist", "ann", "allcinema", "seesaawiki", "keyframe"}
    missing = expected - codes
    assert not missing, f"sources seeds missing after init_db: {sorted(missing)}"
