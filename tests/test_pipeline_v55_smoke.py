"""Smoke test: fresh v55/v56 schema init is consistent and pipeline completes.

Schema checks (fast):
- get_schema_version() returns SCHEMA_VERSION
- The v2 canonical tables exist; deprecated tables are absent
- sources table is seeded

Pipeline check (integration, uses synthetic data):
- Full pipeline runs to completion on a fresh schema
- person_scores TABLE is populated
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


# ---------------------------------------------------------------------------
# Pipeline completion test (integration)
# ---------------------------------------------------------------------------

def test_pipeline_completes_on_fresh_schema(tmp_path, monkeypatch) -> None:
    """Full pipeline must complete on a fresh v56 schema and populate person_scores."""
    import src.database
    import src.pipeline
    import src.utils.config

    db_path = tmp_path / "smoke.db"
    json_dir = tmp_path / "json"
    json_dir.mkdir()

    monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)
    monkeypatch.setattr(src.pipeline, "JSON_DIR", json_dir)
    monkeypatch.setattr(src.utils.config, "JSON_DIR", json_dir)

    from src.database import get_connection, init_db, insert_credit, upsert_anime, upsert_person
    from src.synthetic import generate_synthetic_data
    from src.pipeline import run_scoring_pipeline

    persons, anime_list, credits = generate_synthetic_data(
        n_directors=5, n_animators=20, n_anime=10, seed=99
    )
    conn = get_connection()
    init_db(conn)
    for p in persons:
        upsert_person(conn, p)
    for a in anime_list:
        upsert_anime(conn, a)
    for c in credits:
        insert_credit(conn, c)
    conn.commit()
    conn.close()

    results = run_scoring_pipeline(enable_websocket=False)

    assert len(results) > 0, "Pipeline produced no results"
    assert all("person_id" in r for r in results), "Results missing person_id field"
    assert all("iv_score" in r for r in results), "Results missing iv_score field"

    conn2 = get_connection()
    n_scores = conn2.execute("SELECT COUNT(*) FROM person_scores").fetchone()[0]
    conn2.close()
    assert n_scores > 0, "person_scores TABLE not populated after pipeline run"
