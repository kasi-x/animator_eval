"""Tests for characters cross-source dedup audit and safe merge.

Uses synthetic in-memory DuckDB fixtures — no real silver.duckdb required.
Covers all three detection criteria and both dry-run / actual-merge paths.
"""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from src.etl.audit.characters_dedup import (
    audit,
    find_dup_by_anilist_id,
    find_dup_by_name_and_actor,
    find_dup_by_name_and_anime,
)
from src.etl.dedup.characters_safe_merge import (
    _canonical_id,
    merge,
)

# ---------------------------------------------------------------------------
# Minimal DDL for in-memory SILVER
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS characters (
    id           VARCHAR PRIMARY KEY,
    name_ja      VARCHAR NOT NULL DEFAULT '',
    name_en      VARCHAR NOT NULL DEFAULT '',
    aliases      VARCHAR NOT NULL DEFAULT '[]',
    anilist_id   INTEGER,
    favourites   INTEGER,
    description  VARCHAR,
    gender       VARCHAR,
    date_of_birth VARCHAR,
    age          VARCHAR,
    blood_type   VARCHAR,
    site_url     VARCHAR,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(anilist_id)
);

CREATE TABLE IF NOT EXISTS character_voice_actors (
    id             INTEGER PRIMARY KEY,
    character_id   VARCHAR NOT NULL,
    person_id      VARCHAR NOT NULL,
    anime_id       VARCHAR NOT NULL,
    character_role VARCHAR NOT NULL DEFAULT '',
    source         VARCHAR NOT NULL DEFAULT '',
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(character_id, person_id, anime_id)
);
"""


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------


def _write_dedup_csv(path: Path, rows: list[dict]) -> None:
    """Write a characters_dedup.csv with the expected columns."""
    fieldnames = [
        "id_a",
        "id_b",
        "criterion",
        "evidence_detail",
        "name_a",
        "name_b",
        "similarity",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mem_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with minimal SILVER character schema."""
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL)
    return conn


@pytest.fixture()
def anilist_dup_conn(mem_conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Two character rows sharing the same anilist_id (cross-source dup)."""
    mem_conn.execute(
        "INSERT INTO characters (id, name_ja, name_en, anilist_id) VALUES (?, ?, ?, ?)",
        ["anilist:c1", "アスナ", "Asuna", 100],
    )
    mem_conn.execute(
        "INSERT INTO characters (id, name_ja, name_en) VALUES (?, ?, ?)",
        ["bgm:c99", "アスナ", "Asuna"],
    )
    # Add anilist_id to bgm row with same value via separate update approach:
    # since UNIQUE(anilist_id) is on the table we insert without anilist_id first
    # and then need to bypass — use a different id pattern without anilist_id constraint.
    # Instead, drop the UNIQUE constraint for test purposes by using a fresh conn.
    return mem_conn


@pytest.fixture()
def anilist_dup_conn_no_unique() -> duckdb.DuckDBPyConnection:
    """Characters sharing anilist_id but without the UNIQUE constraint (for testing)."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE characters (
            id           VARCHAR PRIMARY KEY,
            name_ja      VARCHAR NOT NULL DEFAULT '',
            name_en      VARCHAR NOT NULL DEFAULT '',
            aliases      VARCHAR NOT NULL DEFAULT '[]',
            anilist_id   INTEGER,
            favourites   INTEGER
        );
        CREATE TABLE character_voice_actors (
            id             INTEGER PRIMARY KEY,
            character_id   VARCHAR NOT NULL,
            person_id      VARCHAR NOT NULL,
            anime_id       VARCHAR NOT NULL,
            character_role VARCHAR NOT NULL DEFAULT '',
            source         VARCHAR NOT NULL DEFAULT '',
            UNIQUE(character_id, person_id, anime_id)
        );
    """)
    # Two rows with same anilist_id from different sources
    conn.execute(
        "INSERT INTO characters (id, name_ja, name_en, anilist_id) VALUES (?, ?, ?, ?)",
        ["anilist:c1", "アスナ", "Asuna", 100],
    )
    conn.execute(
        "INSERT INTO characters (id, name_ja, name_en, anilist_id) VALUES (?, ?, ?, ?)",
        ["bgm:c99", "アスナ", "Asuna", 100],
    )
    # A unique character (no dup)
    conn.execute(
        "INSERT INTO characters (id, name_ja, name_en, anilist_id) VALUES (?, ?, ?, ?)",
        ["anilist:c2", "キリト", "Kirito", 200],
    )
    return conn


@pytest.fixture()
def actor_dup_conn(mem_conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Two characters with same name + shared voice actor."""
    # Character pair with same name
    mem_conn.execute(
        "INSERT INTO characters (id, name_ja, name_en) VALUES (?, ?, ?)",
        ["anilist:c10", "エレン", "Eren"],
    )
    mem_conn.execute(
        "INSERT INTO characters (id, name_ja, name_en) VALUES (?, ?, ?)",
        ["bgm:c20", "エレン", "Eren"],
    )
    # A unique character
    mem_conn.execute(
        "INSERT INTO characters (id, name_ja, name_en) VALUES (?, ?, ?)",
        ["anilist:c99", "ミカサ", "Mikasa"],
    )
    # CVA: same person_id voices both dup characters
    mem_conn.execute(
        "INSERT INTO character_voice_actors (id, character_id, person_id, anime_id) VALUES (?, ?, ?, ?)",
        [1, "anilist:c10", "person:p1", "anime:a1"],
    )
    mem_conn.execute(
        "INSERT INTO character_voice_actors (id, character_id, person_id, anime_id) VALUES (?, ?, ?, ?)",
        [2, "bgm:c20", "person:p1", "anime:a1"],
    )
    # Unique character voice
    mem_conn.execute(
        "INSERT INTO character_voice_actors (id, character_id, person_id, anime_id) VALUES (?, ?, ?, ?)",
        [3, "anilist:c99", "person:p2", "anime:a2"],
    )
    return mem_conn


@pytest.fixture()
def anime_dup_conn(mem_conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Two characters with same name + shared anime_id."""
    mem_conn.execute(
        "INSERT INTO characters (id, name_ja, name_en) VALUES (?, ?, ?)",
        ["anilist:c30", "レイ", "Rei"],
    )
    mem_conn.execute(
        "INSERT INTO characters (id, name_ja, name_en) VALUES (?, ?, ?)",
        ["ann:c55", "レイ", "Rei"],
    )
    # A unique character
    mem_conn.execute(
        "INSERT INTO characters (id, name_ja, name_en) VALUES (?, ?, ?)",
        ["anilist:c31", "アスカ", "Asuka"],
    )
    # CVA: both characters appear in the same anime, different voice actors
    mem_conn.execute(
        "INSERT INTO character_voice_actors (id, character_id, person_id, anime_id) VALUES (?, ?, ?, ?)",
        [1, "anilist:c30", "person:p3", "anime:a5"],
    )
    mem_conn.execute(
        "INSERT INTO character_voice_actors (id, character_id, person_id, anime_id) VALUES (?, ?, ?, ?)",
        [2, "ann:c55", "person:p4", "anime:a5"],
    )
    mem_conn.execute(
        "INSERT INTO character_voice_actors (id, character_id, person_id, anime_id) VALUES (?, ?, ?, ?)",
        [3, "anilist:c31", "person:p5", "anime:a5"],
    )
    return mem_conn


# ---------------------------------------------------------------------------
# Unit tests: _canonical_id
# ---------------------------------------------------------------------------


class TestCanonicalId:
    def test_lex_smallest_chosen(self) -> None:
        assert _canonical_id("anilist:c1", "bgm:c99") == "anilist:c1"

    def test_reversed_input_order(self) -> None:
        assert _canonical_id("bgm:c99", "anilist:c1") == "anilist:c1"

    def test_same_prefix(self) -> None:
        assert _canonical_id("anilist:c10", "anilist:c2") == "anilist:c10"


# ---------------------------------------------------------------------------
# Unit tests: find_dup_by_anilist_id
# ---------------------------------------------------------------------------


class TestFindDupByAnilistId:
    def test_detects_same_anilist_id(
        self, anilist_dup_conn_no_unique: duckdb.DuckDBPyConnection
    ) -> None:
        rows = find_dup_by_anilist_id(anilist_dup_conn_no_unique)
        assert len(rows) == 1
        assert rows[0]["criterion"] == "anilist_id"
        assert rows[0]["anilist_id"] == 100

    def test_id_ordering(
        self, anilist_dup_conn_no_unique: duckdb.DuckDBPyConnection
    ) -> None:
        rows = find_dup_by_anilist_id(anilist_dup_conn_no_unique)
        assert rows[0]["id_a"] < rows[0]["id_b"]

    def test_no_false_positive_unique_character(
        self, anilist_dup_conn_no_unique: duckdb.DuckDBPyConnection
    ) -> None:
        rows = find_dup_by_anilist_id(anilist_dup_conn_no_unique)
        ids = {r["id_a"] for r in rows} | {r["id_b"] for r in rows}
        # anilist:c2 has no duplicate → not in any pair
        assert "anilist:c2" not in ids

    def test_empty_when_no_dups(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        mem_conn.execute(
            "INSERT INTO characters (id, name_ja, anilist_id) VALUES (?, ?, ?)",
            ["anilist:c1", "テスト", 1],
        )
        rows = find_dup_by_anilist_id(mem_conn)
        assert rows == []


# ---------------------------------------------------------------------------
# Unit tests: find_dup_by_name_and_actor
# ---------------------------------------------------------------------------


class TestFindDupByNameAndActor:
    def test_detects_shared_voice_actor(
        self, actor_dup_conn: duckdb.DuckDBPyConnection
    ) -> None:
        rows = find_dup_by_name_and_actor(actor_dup_conn)
        assert len(rows) >= 1
        criteria = {r["criterion"] for r in rows}
        assert "name_and_actor" in criteria

    def test_id_ordering(
        self, actor_dup_conn: duckdb.DuckDBPyConnection
    ) -> None:
        rows = find_dup_by_name_and_actor(actor_dup_conn)
        for r in rows:
            assert r["id_a"] < r["id_b"]

    def test_no_result_without_shared_actor(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        # Two chars with same name but different voice actors
        mem_conn.execute(
            "INSERT INTO characters (id, name_ja) VALUES (?, ?)",
            ["a:c1", "テスト"],
        )
        mem_conn.execute(
            "INSERT INTO characters (id, name_ja) VALUES (?, ?)",
            ["b:c1", "テスト"],
        )
        mem_conn.execute(
            "INSERT INTO character_voice_actors (id, character_id, person_id, anime_id) VALUES (?, ?, ?, ?)",
            [1, "a:c1", "p:1", "anime:1"],
        )
        mem_conn.execute(
            "INSERT INTO character_voice_actors (id, character_id, person_id, anime_id) VALUES (?, ?, ?, ?)",
            [2, "b:c1", "p:2", "anime:1"],
        )
        rows = find_dup_by_name_and_actor(mem_conn)
        assert rows == []

    def test_unique_character_not_included(
        self, actor_dup_conn: duckdb.DuckDBPyConnection
    ) -> None:
        rows = find_dup_by_name_and_actor(actor_dup_conn)
        all_ids = {r["id_a"] for r in rows} | {r["id_b"] for r in rows}
        # anilist:c99 (Mikasa) has a different name → not in any pair
        assert "anilist:c99" not in all_ids


# ---------------------------------------------------------------------------
# Unit tests: find_dup_by_name_and_anime
# ---------------------------------------------------------------------------


class TestFindDupByNameAndAnime:
    def test_detects_same_anime(
        self, anime_dup_conn: duckdb.DuckDBPyConnection
    ) -> None:
        rows = find_dup_by_name_and_anime(anime_dup_conn)
        assert len(rows) >= 1
        criteria = {r["criterion"] for r in rows}
        assert "name_and_anime" in criteria

    def test_id_ordering(
        self, anime_dup_conn: duckdb.DuckDBPyConnection
    ) -> None:
        rows = find_dup_by_name_and_anime(anime_dup_conn)
        for r in rows:
            assert r["id_a"] < r["id_b"]

    def test_shared_anime_id_in_result(
        self, anime_dup_conn: duckdb.DuckDBPyConnection
    ) -> None:
        rows = find_dup_by_name_and_anime(anime_dup_conn)
        assert any(r["shared_anime_id"] == "anime:a5" for r in rows)

    def test_no_result_different_anime(
        self, mem_conn: duckdb.DuckDBPyConnection
    ) -> None:
        mem_conn.execute(
            "INSERT INTO characters (id, name_ja) VALUES (?, ?)",
            ["a:c1", "テスト"],
        )
        mem_conn.execute(
            "INSERT INTO characters (id, name_ja) VALUES (?, ?)",
            ["b:c1", "テスト"],
        )
        mem_conn.execute(
            "INSERT INTO character_voice_actors (id, character_id, person_id, anime_id) VALUES (?, ?, ?, ?)",
            [1, "a:c1", "p:1", "anime:1"],
        )
        mem_conn.execute(
            "INSERT INTO character_voice_actors (id, character_id, person_id, anime_id) VALUES (?, ?, ?, ?)",
            [2, "b:c1", "p:2", "anime:2"],
        )
        rows = find_dup_by_name_and_anime(mem_conn)
        assert rows == []


# ---------------------------------------------------------------------------
# Integration tests: audit()
# ---------------------------------------------------------------------------


class TestAudit:
    def test_returns_counts_dict(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        counts = audit(actor_dup_conn, tmp_path)
        assert "anilist_id" in counts
        assert "name_and_actor" in counts
        assert "name_and_anime" in counts
        assert "total_unique_pairs" in counts

    def test_csv_written(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        audit(actor_dup_conn, tmp_path)
        assert (tmp_path / "characters_dedup.csv").exists()

    def test_summary_md_written(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        audit(actor_dup_conn, tmp_path)
        assert (tmp_path / "characters_dedup_summary.md").exists()

    def test_stop_if_raises_on_high_rate(
        self, tmp_path: Path
    ) -> None:
        """Audit raises RuntimeError if dup rate > 5% on a realistic-size dataset."""
        conn = duckdb.connect(":memory:")
        # Table without UNIQUE(anilist_id) so we can insert duplicates freely
        conn.execute("""
            CREATE TABLE characters (
                id VARCHAR PRIMARY KEY,
                name_ja VARCHAR NOT NULL DEFAULT '',
                name_en VARCHAR NOT NULL DEFAULT '',
                aliases VARCHAR NOT NULL DEFAULT '[]',
                anilist_id INTEGER
            );
            CREATE TABLE character_voice_actors (
                id INTEGER PRIMARY KEY,
                character_id VARCHAR NOT NULL,
                person_id VARCHAR NOT NULL,
                anime_id VARCHAR NOT NULL,
                character_role VARCHAR NOT NULL DEFAULT '',
                source VARCHAR NOT NULL DEFAULT '',
                UNIQUE(character_id, person_id, anime_id)
            );
        """)
        # Insert 100 unique characters and 10 dup pairs (10% rate → triggers stop-if)
        # First 80 unique rows
        for i in range(80):
            conn.execute(
                f"INSERT INTO characters VALUES ('src:c{i}', 'Char{i}', '', '[]', {i})"
            )
        # 10 dup pairs sharing anilist_id (20 rows, 10 pairs → 10% of 100 > 5%)
        for i in range(10):
            conn.execute(
                f"INSERT INTO characters VALUES ('a:dup{i}', 'Dup{i}', '', '[]', {1000 + i})"
            )
            conn.execute(
                f"INSERT INTO characters VALUES ('b:dup{i}', 'Dup{i}', '', '[]', {1000 + i})"
            )
        with pytest.raises(RuntimeError, match="STOP-IF"):
            audit(conn, tmp_path)

    def test_empty_db_returns_zeros(
        self, mem_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        counts = audit(mem_conn, tmp_path)
        assert counts["total_unique_pairs"] == 0

    def test_favourites_not_in_csv_columns(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """H1: favourites must not appear in the audit CSV output."""
        audit(actor_dup_conn, tmp_path)
        csv_path = tmp_path / "characters_dedup.csv"
        if csv_path.stat().st_size > 0:
            with open(csv_path, newline="", encoding="utf-8") as fh:
                header_line = fh.readline()
            assert "favourites" not in header_line


# ---------------------------------------------------------------------------
# Integration tests: merge() — dry_run
# ---------------------------------------------------------------------------


class TestMergeDryRun:
    def _make_csv(self, tmp_path: Path, rows: list[dict]) -> Path:
        p = tmp_path / "characters_dedup.csv"
        _write_dedup_csv(p, rows)
        return p

    def test_dry_run_returns_counts(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = self._make_csv(
            tmp_path,
            [
                {
                    "id_a": "anilist:c10",
                    "id_b": "bgm:c20",
                    "criterion": "name_and_actor",
                    "evidence_detail": "person:p1",
                    "name_a": "エレン",
                    "name_b": "エレン",
                    "similarity": "1.0",
                }
            ],
        )
        result = merge(actor_dup_conn, csv_path, dry_run=True)
        assert result["merge_count"] == 1
        # DB must not have changed
        count = actor_dup_conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
        assert count == 3

    def test_dry_run_no_db_writes(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = self._make_csv(
            tmp_path,
            [
                {
                    "id_a": "anilist:c10",
                    "id_b": "bgm:c20",
                    "criterion": "name_and_actor",
                    "evidence_detail": "person:p1",
                    "name_a": "エレン",
                    "name_b": "エレン",
                    "similarity": "1.0",
                }
            ],
        )
        before_chars = actor_dup_conn.execute(
            "SELECT COUNT(*) FROM characters"
        ).fetchone()[0]
        before_cva = actor_dup_conn.execute(
            "SELECT COUNT(*) FROM character_voice_actors"
        ).fetchone()[0]
        merge(actor_dup_conn, csv_path, dry_run=True)
        assert actor_dup_conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0] == before_chars
        assert actor_dup_conn.execute("SELECT COUNT(*) FROM character_voice_actors").fetchone()[0] == before_cva

    def test_dry_run_zero_for_ghost_ids(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = self._make_csv(
            tmp_path,
            [
                {
                    "id_a": "anilist:c10",
                    "id_b": "missing:c999",
                    "criterion": "name_and_actor",
                    "evidence_detail": "person:p1",
                    "name_a": "エレン",
                    "name_b": "エレン",
                    "similarity": "1.0",
                }
            ],
        )
        result = merge(actor_dup_conn, csv_path, dry_run=True)
        assert result["merge_count"] == 0

    def test_empty_csv_dry_run(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = self._make_csv(tmp_path, [])
        result = merge(actor_dup_conn, csv_path, dry_run=True)
        assert result["merge_count"] == 0


# ---------------------------------------------------------------------------
# Integration tests: merge() — actual merge
# ---------------------------------------------------------------------------


class TestMergeActual:
    def _csv_actor(self, tmp_path: Path) -> Path:
        p = tmp_path / "characters_dedup.csv"
        _write_dedup_csv(
            p,
            [
                {
                    "id_a": "anilist:c10",
                    "id_b": "bgm:c20",
                    "criterion": "name_and_actor",
                    "evidence_detail": "person:p1",
                    "name_a": "エレン",
                    "name_b": "エレン",
                    "similarity": "1.0",
                }
            ],
        )
        return p

    def test_characters_row_decremented(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        before = actor_dup_conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
        merge(actor_dup_conn, self._csv_actor(tmp_path))
        after = actor_dup_conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
        assert after == before - 1

    def test_canonical_id_survives(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge(actor_dup_conn, self._csv_actor(tmp_path))
        # anilist:c10 < bgm:c20 → canonical
        ids = {r[0] for r in actor_dup_conn.execute("SELECT id FROM characters").fetchall()}
        assert "anilist:c10" in ids
        assert "bgm:c20" not in ids

    def test_deprecated_id_gone(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge(actor_dup_conn, self._csv_actor(tmp_path))
        count = actor_dup_conn.execute(
            "SELECT COUNT(*) FROM characters WHERE id = 'bgm:c20'"
        ).fetchone()[0]
        assert count == 0

    def test_cva_rows_repointed(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge(actor_dup_conn, self._csv_actor(tmp_path))
        # The CVA row that was under bgm:c20 should now point to anilist:c10
        # (or be deleted because it was a duplicate; either way bgm:c20 must not remain)
        count_deprecated = actor_dup_conn.execute(
            "SELECT COUNT(*) FROM character_voice_actors WHERE character_id = 'bgm:c20'"
        ).fetchone()[0]
        assert count_deprecated == 0

    def test_audit_table_populated(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge(actor_dup_conn, self._csv_actor(tmp_path))
        count = actor_dup_conn.execute(
            "SELECT COUNT(*) FROM meta_entity_resolution_audit WHERE table_name = 'characters'"
        ).fetchone()[0]
        assert count == 1

    def test_audit_redirect_ids_correct(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge(actor_dup_conn, self._csv_actor(tmp_path))
        row = actor_dup_conn.execute(
            "SELECT redirect_from_id, redirect_to_id FROM meta_entity_resolution_audit"
            " WHERE table_name = 'characters'"
        ).fetchone()
        assert row is not None
        assert row[0] == "bgm:c20"
        assert row[1] == "anilist:c10"

    def test_returns_correct_counts(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        result = merge(actor_dup_conn, self._csv_actor(tmp_path))
        assert result["merge_count"] == 1
        assert result["audit_logged"] == 1
        assert result["before"] > result["after"]

    def test_idempotent_second_run(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge(actor_dup_conn, self._csv_actor(tmp_path))
        result2 = merge(actor_dup_conn, self._csv_actor(tmp_path))
        assert result2["merge_count"] == 0
        count = actor_dup_conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
        assert count == 2

    def test_unique_character_not_affected(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge(actor_dup_conn, self._csv_actor(tmp_path))
        row = actor_dup_conn.execute(
            "SELECT id FROM characters WHERE id = 'anilist:c99'"
        ).fetchone()
        assert row is not None

    def test_empty_csv_no_op(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = tmp_path / "characters_dedup.csv"
        _write_dedup_csv(csv_path, [])
        before = actor_dup_conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
        result = merge(actor_dup_conn, csv_path)
        after = actor_dup_conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
        assert after == before
        assert result["merge_count"] == 0

    def test_name_and_anime_criterion(
        self, anime_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = tmp_path / "characters_dedup.csv"
        _write_dedup_csv(
            csv_path,
            [
                {
                    "id_a": "anilist:c30",
                    "id_b": "ann:c55",
                    "criterion": "name_and_anime",
                    "evidence_detail": "anime:a5",
                    "name_a": "レイ",
                    "name_b": "レイ",
                    "similarity": "1.0",
                }
            ],
        )
        before = anime_dup_conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
        result = merge(anime_dup_conn, csv_path)
        after = anime_dup_conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
        assert after == before - 1
        assert result["merge_count"] == 1

    def test_h1_favourites_not_in_audit_reason(
        self, actor_dup_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """H1: favourites must not appear in merge_reason stored in audit table."""
        merge(actor_dup_conn, self._csv_actor(tmp_path))
        rows = actor_dup_conn.execute(
            "SELECT merge_reason FROM meta_entity_resolution_audit WHERE table_name = 'characters'"
        ).fetchall()
        for (reason,) in rows:
            assert "favourites" not in reason.lower()
