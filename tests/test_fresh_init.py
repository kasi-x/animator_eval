"""Smoke test: fresh DB init produces the target schema."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from src.db import init_db

EXPECTED = {
    "anime", "persons", "credits",
    "sources", "roles",
    "anime_studios", "anime_genres", "anime_tags",
    "anime_external_ids", "person_external_ids",
    "person_scores", "voice_actor_scores",
    "ops_source_scrape_status", "ops_lineage",
    "schema_meta",
}

FORBIDDEN = {
    "anime_display", "anime_analysis",
    "va_scores", "scores",
    "data_sources",
    "meta_lineage",
    "source_scrape_status",
}


def test_fresh_init_creates_target_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "fresh.db"
    conn = sqlite3.connect(db_path)
    try:
        init_db(conn)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        tables = {r[0] for r in rows}
    finally:
        conn.close()

    missing = EXPECTED - tables
    leaked = tables & FORBIDDEN
    assert not missing, f"Missing canonical tables: {sorted(missing)}"
    assert not leaked, f"Deprecated tables leaked: {sorted(leaked)}"


def test_anime_original_work_type_column(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "fresh.db")
    try:
        init_db(conn)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(anime)").fetchall()]
    finally:
        conn.close()
    assert "original_work_type" in cols, "anime.original_work_type column missing"
    assert "source" not in cols, "anime.source (old name) still present"


def test_credits_evidence_source_column(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "fresh.db")
    try:
        init_db(conn)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(credits)").fetchall()]
    finally:
        conn.close()
    assert "evidence_source" in cols, "credits.evidence_source column missing"
    assert "source" not in cols, "credits.source (old name) still present"
