"""Scraper write-path smoke tests against the new v55 schema.

Verifies that upsert_canonical_anime and the scraper dual-write pattern
work correctly on a fresh schema:
  1. Silver (canonical anime table) is written correctly.
  2. Bronze (src_anilist_anime) is written when the anilist-specific path is used.
  3. A Credit inserted via insert_credit is deduplicated correctly (episode=None).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.database import init_db, insert_credit, upsert_person, upsert_src_anilist_anime
from src.etl.integrate import upsert_canonical_anime
from src.models import BronzeAnime, Credit, Person, Role


@pytest.fixture()
def fresh_conn(tmp_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(tmp_path / "smoke.db"))
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def test_upsert_canonical_anime_writes_silver(fresh_conn: sqlite3.Connection) -> None:
    """upsert_canonical_anime must persist the anime in the canonical silver table."""
    anime = BronzeAnime(
        id="anilist:1",
        title_ja="テスト作品",
        title_en="Test Work",
        year=2025,
    )
    upsert_canonical_anime(fresh_conn, anime, evidence_source="anilist")
    fresh_conn.commit()

    row = fresh_conn.execute("SELECT id, title_ja, title_en FROM anime WHERE id=?", ("anilist:1",)).fetchone()
    assert row is not None, "anime row missing in silver layer"
    assert row["title_ja"] == "テスト作品"
    assert row["title_en"] == "Test Work"


def test_anilist_dual_write_populates_bronze(fresh_conn: sqlite3.Connection) -> None:
    """The anilist scraper pattern (silver + bronze) must fill both layers."""
    anime = BronzeAnime(
        id="anilist:1",
        title_ja="テスト作品",
        title_en="Test Work",
        year=2025,
        anilist_id=1,
    )
    upsert_canonical_anime(fresh_conn, anime, evidence_source="anilist")
    upsert_src_anilist_anime(fresh_conn, anime)
    fresh_conn.commit()

    silver = fresh_conn.execute("SELECT id FROM anime WHERE id=?", ("anilist:1",)).fetchone()
    assert silver is not None, "anime missing in silver"

    bronze = fresh_conn.execute("SELECT anilist_id FROM src_anilist_anime WHERE anilist_id=?", (1,)).fetchone()
    assert bronze is not None, "anime missing in bronze (src_anilist_anime)"


def test_insert_credit_deduplicates_null_episode(fresh_conn: sqlite3.Connection) -> None:
    """episode=None credits must deduplicate (INSERT OR IGNORE-equivalent)."""
    upsert_person(fresh_conn, Person(id="anilist:p1", name_en="Test Person"))
    upsert_canonical_anime(
        fresh_conn, BronzeAnime(id="anilist:1", title_en="Test"), evidence_source="anilist"
    )
    fresh_conn.commit()

    credit = Credit(person_id="anilist:p1", anime_id="anilist:1", role=Role.KEY_ANIMATOR, source="anilist")
    insert_credit(fresh_conn, credit)
    insert_credit(fresh_conn, credit)
    fresh_conn.commit()

    count = fresh_conn.execute(
        "SELECT COUNT(*) FROM credits WHERE person_id='anilist:p1' AND anime_id='anilist:1'"
    ).fetchone()[0]
    assert count == 1, f"Expected 1 credit row, got {count}"


def test_upsert_canonical_anime_idempotent(fresh_conn: sqlite3.Connection) -> None:
    """Calling upsert_canonical_anime twice must not create duplicate rows."""
    anime = BronzeAnime(id="anilist:2", title_ja="重複テスト", year=2024)
    upsert_canonical_anime(fresh_conn, anime, evidence_source="anilist")
    upsert_canonical_anime(fresh_conn, anime, evidence_source="anilist")
    fresh_conn.commit()

    count = fresh_conn.execute("SELECT COUNT(*) FROM anime WHERE id='anilist:2'").fetchone()[0]
    assert count == 1, f"Expected 1 row, got {count}"
