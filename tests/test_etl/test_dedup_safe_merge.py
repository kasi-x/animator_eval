"""Tests for src/etl/dedup/safe_merge.py.

Uses synthetic in-memory DuckDB fixtures so no real silver.duckdb is needed.
All merge logic is exercised in both dry_run=True and dry_run=False modes.
"""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from src.etl.dedup.safe_merge import (
    _canonical_id,
    _is_high_confidence_person,
    _is_high_confidence_studio,
    _normalize_studio_name,
    _parse_date,
    merge_persons,
    merge_studios,
)

# ---------------------------------------------------------------------------
# Minimal DDL for silver tables used in merge tests
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS persons (
    id         VARCHAR PRIMARY KEY,
    name_ja    VARCHAR NOT NULL DEFAULT '',
    name_en    VARCHAR NOT NULL DEFAULT '',
    birth_date VARCHAR
);

CREATE TABLE IF NOT EXISTS studios (
    id                VARCHAR PRIMARY KEY,
    name              VARCHAR NOT NULL DEFAULT '',
    country_of_origin VARCHAR
);

CREATE TABLE IF NOT EXISTS anime_studios (
    anime_id  VARCHAR NOT NULL,
    studio_id VARCHAR NOT NULL,
    is_main   BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (anime_id, studio_id)
);

CREATE TABLE IF NOT EXISTS credits (
    person_id       VARCHAR,
    anime_id        VARCHAR NOT NULL,
    role            VARCHAR NOT NULL,
    evidence_source VARCHAR NOT NULL,
    episode         INTEGER,
    raw_role        VARCHAR NOT NULL DEFAULT ''
);
"""


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------


def _write_person_csv(path: Path, rows: list[dict]) -> None:
    """Write silver_dedup_persons.csv with the expected columns."""
    fieldnames = [
        "candidate_id_a",
        "candidate_id_b",
        "sources",
        "evidence_name",
        "evidence_birth_date",
        "row_cnt",
        "src_cnt",
        "similarity",
    ]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_studio_csv(path: Path, rows: list[dict]) -> None:
    """Write silver_dedup_studios.csv with the expected columns."""
    fieldnames = [
        "candidate_id_a",
        "candidate_id_b",
        "sources",
        "evidence_name",
        "country_of_origin",
        "row_cnt",
        "src_cnt",
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
    """In-memory DuckDB with minimal silver schema."""
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL)
    return conn


@pytest.fixture()
def person_conn(mem_conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """DB with two duplicate persons and one unique person."""
    # Dup pair: same name_ja + same birth_date, different sources
    mem_conn.execute(
        "INSERT INTO persons VALUES (?, ?, ?, ?)",
        ["anilist:p1", "山田太郎", "", "1985-04-01"],
    )
    mem_conn.execute(
        "INSERT INTO persons VALUES (?, ?, ?, ?)",
        ["bgm:p99", "山田太郎", "", "1985-04-01"],
    )
    # Unique person (no match in CSV)
    mem_conn.execute(
        "INSERT INTO persons VALUES (?, ?, ?, ?)",
        ["anilist:p2", "鈴木花子", "", "1990-07-15"],
    )
    # Credits for the dup person on both IDs
    mem_conn.execute(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source) VALUES (?, ?, ?, ?)",
        ["anilist:p1", "anilist:a1", "director", "anilist"],
    )
    mem_conn.execute(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source) VALUES (?, ?, ?, ?)",
        ["bgm:p99", "anilist:a2", "producer", "bangumi"],
    )
    return mem_conn


@pytest.fixture()
def studio_conn(mem_conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """DB with two duplicate studios and one unique studio."""
    # Dup pair: same normalized name, both country NULL
    mem_conn.execute(
        "INSERT INTO studios VALUES (?, ?, ?)",
        ["anilist:s1", "J.C.STAFF", None],
    )
    mem_conn.execute(
        "INSERT INTO studios VALUES (?, ?, ?)",
        ["kf:s1", "JC STAFF", None],
    )
    # Unique studio
    mem_conn.execute(
        "INSERT INTO studios VALUES (?, ?, ?)",
        ["anilist:s2", "BONES", "JP"],
    )
    # anime_studios references for the dup studio
    mem_conn.execute(
        "INSERT INTO anime_studios VALUES (?, ?, ?)",
        ["anilist:a10", "kf:s1", False],
    )
    mem_conn.execute(
        "INSERT INTO anime_studios VALUES (?, ?, ?)",
        ["anilist:a11", "anilist:s1", False],
    )
    return mem_conn


# ---------------------------------------------------------------------------
# Unit tests: pure helpers
# ---------------------------------------------------------------------------


class TestParseDate:
    def test_valid_iso(self) -> None:
        from datetime import date

        assert _parse_date("1985-04-01") == date(1985, 4, 1)

    def test_none_input(self) -> None:
        assert _parse_date(None) is None

    def test_empty_string(self) -> None:
        assert _parse_date("") is None

    def test_whitespace_only(self) -> None:
        assert _parse_date("   ") is None

    def test_invalid_format(self) -> None:
        assert _parse_date("not-a-date") is None


class TestNormalizeStudioName:
    def test_punctuation_removed(self) -> None:
        assert _normalize_studio_name("J.C.STAFF") == _normalize_studio_name("JC STAFF")

    def test_case_insensitive(self) -> None:
        assert _normalize_studio_name("Ufotable") == _normalize_studio_name("UFOTABLE")

    def test_nfkc(self) -> None:
        # Full-width vs half-width
        assert _normalize_studio_name("ＡＢＣ") == _normalize_studio_name("ABC")


class TestCanonicalId:
    def test_lex_smallest_chosen(self) -> None:
        assert _canonical_id("anilist:p1", "bgm:p99") == "anilist:p1"

    def test_reversed_order(self) -> None:
        assert _canonical_id("bgm:p99", "anilist:p1") == "anilist:p1"

    def test_same_prefix(self) -> None:
        assert _canonical_id("anilist:p10", "anilist:p2") == "anilist:p10"


class TestIsHighConfidencePerson:
    def _row(self, sim: str, bdate: str) -> dict:
        return {
            "similarity": sim,
            "evidence_birth_date": bdate,
        }

    def test_sim_1_with_bdate(self) -> None:
        assert _is_high_confidence_person(self._row("1.0", "1985-04-01")) is True

    def test_sim_exact_threshold_excluded(self) -> None:
        assert _is_high_confidence_person(self._row("0.99", "1985-04-01")) is False

    def test_sim_above_threshold_no_bdate(self) -> None:
        # No birth date → cannot verify → excluded
        assert _is_high_confidence_person(self._row("1.0", "")) is False

    def test_sim_below_threshold(self) -> None:
        assert _is_high_confidence_person(self._row("0.95", "1985-04-01")) is False


class TestIsHighConfidenceStudio:
    def test_matching_normed_name_null_country(self) -> None:
        row = {"similarity": "1.0"}
        assert _is_high_confidence_studio(row, "J.C.STAFF", "JC STAFF", None, None) is True

    def test_matching_normed_name_same_country(self) -> None:
        row = {"similarity": "1.0"}
        assert _is_high_confidence_studio(row, "ufotable", "ufotable", "JP", "JP") is True

    def test_different_country_excluded(self) -> None:
        row = {"similarity": "1.0"}
        assert _is_high_confidence_studio(row, "studio", "studio", "JP", "KR") is False

    def test_sim_below_threshold(self) -> None:
        row = {"similarity": "0.98"}
        assert _is_high_confidence_studio(row, "jcstaff", "jcstaff", None, None) is False

    def test_normed_names_differ_excluded(self) -> None:
        row = {"similarity": "1.0"}
        assert _is_high_confidence_studio(row, "BONES", "MAPPA", None, None) is False


# ---------------------------------------------------------------------------
# Integration tests: merge_persons
# ---------------------------------------------------------------------------


class TestMergePersonsDryRun:
    def test_returns_counts_without_db_write(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = tmp_path / "persons.csv"
        _write_person_csv(
            csv_path,
            [
                {
                    "candidate_id_a": "anilist:p1",
                    "candidate_id_b": "bgm:p99",
                    "sources": "anilist,bgm",
                    "evidence_name": "山田太郎",
                    "evidence_birth_date": "1985-04-01",
                    "row_cnt": "2",
                    "src_cnt": "2",
                    "similarity": "1.0",
                }
            ],
        )
        result = merge_persons(person_conn, csv_path, dry_run=True)
        assert result["merge_count"] == 1
        assert result["audit_logged"] == 1
        # DB must not change
        count = person_conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        assert count == 3  # original 3 rows

    def test_zero_when_no_high_confidence(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = tmp_path / "persons.csv"
        _write_person_csv(
            csv_path,
            [
                {
                    "candidate_id_a": "anilist:p1",
                    "candidate_id_b": "bgm:p99",
                    "sources": "anilist,bgm",
                    "evidence_name": "山田太郎",
                    "evidence_birth_date": "1985-04-01",
                    "row_cnt": "2",
                    "src_cnt": "2",
                    "similarity": "0.95",  # below threshold
                }
            ],
        )
        result = merge_persons(person_conn, csv_path, dry_run=True)
        assert result["merge_count"] == 0

    def test_empty_csv_returns_zero(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = tmp_path / "persons.csv"
        _write_person_csv(csv_path, [])
        result = merge_persons(person_conn, csv_path, dry_run=True)
        assert result == {"merge_count": 0, "audit_logged": 0}


class TestMergePersonsActual:
    def _csv(self, tmp_path: Path, sim: str = "1.0") -> Path:
        csv_path = tmp_path / "persons.csv"
        _write_person_csv(
            csv_path,
            [
                {
                    "candidate_id_a": "anilist:p1",
                    "candidate_id_b": "bgm:p99",
                    "sources": "anilist,bgm",
                    "evidence_name": "山田太郎",
                    "evidence_birth_date": "1985-04-01",
                    "row_cnt": "2",
                    "src_cnt": "2",
                    "similarity": sim,
                }
            ],
        )
        return csv_path

    def test_persons_row_decremented(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        before = person_conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        merge_persons(person_conn, self._csv(tmp_path))
        after = person_conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        assert after == before - 1

    def test_canonical_id_survives(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_persons(person_conn, self._csv(tmp_path))
        # anilist:p1 < bgm:p99 → canonical
        ids = {r[0] for r in person_conn.execute("SELECT id FROM persons").fetchall()}
        assert "anilist:p1" in ids
        assert "bgm:p99" not in ids

    def test_deprecated_id_gone(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_persons(person_conn, self._csv(tmp_path))
        count = person_conn.execute(
            "SELECT COUNT(*) FROM persons WHERE id = 'bgm:p99'"
        ).fetchone()[0]
        assert count == 0

    def test_credits_row_count_unchanged(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        before = person_conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
        merge_persons(person_conn, self._csv(tmp_path))
        after = person_conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
        assert after == before  # row count invariant

    def test_credits_person_id_updated(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_persons(person_conn, self._csv(tmp_path))
        # bgm:p99 credit should now point to anilist:p1
        count = person_conn.execute(
            "SELECT COUNT(*) FROM credits WHERE person_id = 'anilist:p1'"
        ).fetchone()[0]
        assert count == 2  # both original credits now under canonical

    def test_evidence_source_preserved(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_persons(person_conn, self._csv(tmp_path))
        sources = {
            r[0]
            for r in person_conn.execute(
                "SELECT DISTINCT evidence_source FROM credits"
            ).fetchall()
        }
        assert "anilist" in sources
        assert "bangumi" in sources

    def test_audit_table_populated(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_persons(person_conn, self._csv(tmp_path))
        count = person_conn.execute(
            "SELECT COUNT(*) FROM meta_entity_resolution_audit WHERE table_name = 'persons'"
        ).fetchone()[0]
        assert count == 1

    def test_audit_redirect_ids_correct(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_persons(person_conn, self._csv(tmp_path))
        row = person_conn.execute(
            "SELECT redirect_from_id, redirect_to_id FROM meta_entity_resolution_audit"
            " WHERE table_name = 'persons'"
        ).fetchone()
        assert row is not None
        assert row[0] == "bgm:p99"
        assert row[1] == "anilist:p1"

    def test_returns_merge_count(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        result = merge_persons(person_conn, self._csv(tmp_path))
        assert result["merge_count"] == 1
        assert result["audit_logged"] == 1

    def test_idempotent_second_run(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_persons(person_conn, self._csv(tmp_path))
        # Second run: deprecated ID already gone, should be a no-op
        result2 = merge_persons(person_conn, self._csv(tmp_path))
        assert result2["merge_count"] == 0
        count = person_conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        assert count == 2

    def test_low_similarity_not_merged(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        before = person_conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        result = merge_persons(person_conn, self._csv(tmp_path, sim="0.95"))
        after = person_conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        assert after == before
        assert result["merge_count"] == 0

    def test_unique_person_not_affected(
        self, person_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_persons(person_conn, self._csv(tmp_path))
        row = person_conn.execute(
            "SELECT id FROM persons WHERE id = 'anilist:p2'"
        ).fetchone()
        assert row is not None


# ---------------------------------------------------------------------------
# Integration tests: merge_studios
# ---------------------------------------------------------------------------


class TestMergeStudiosDryRun:
    def test_returns_counts_without_db_write(
        self, studio_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = tmp_path / "studios.csv"
        _write_studio_csv(
            csv_path,
            [
                {
                    "candidate_id_a": "anilist:s1",
                    "candidate_id_b": "kf:s1",
                    "sources": "anilist,kf",
                    "evidence_name": "jcstaff",
                    "country_of_origin": "",
                    "row_cnt": "2",
                    "src_cnt": "2",
                    "similarity": "1.0",
                }
            ],
        )
        result = merge_studios(studio_conn, csv_path, dry_run=True)
        assert result["merge_count"] == 1
        count = studio_conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
        assert count == 3  # unchanged

    def test_empty_csv_returns_zero(
        self, studio_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        csv_path = tmp_path / "studios.csv"
        _write_studio_csv(csv_path, [])
        result = merge_studios(studio_conn, csv_path, dry_run=True)
        assert result == {"merge_count": 0, "audit_logged": 0}


class TestMergeStudiosActual:
    def _csv(self, tmp_path: Path, sim: str = "1.0") -> Path:
        csv_path = tmp_path / "studios.csv"
        _write_studio_csv(
            csv_path,
            [
                {
                    "candidate_id_a": "anilist:s1",
                    "candidate_id_b": "kf:s1",
                    "sources": "anilist,kf",
                    "evidence_name": "jcstaff",
                    "country_of_origin": "",
                    "row_cnt": "2",
                    "src_cnt": "2",
                    "similarity": sim,
                }
            ],
        )
        return csv_path

    def test_studios_row_decremented(
        self, studio_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        before = studio_conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
        merge_studios(studio_conn, self._csv(tmp_path))
        after = studio_conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
        assert after == before - 1

    def test_canonical_id_survives(
        self, studio_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_studios(studio_conn, self._csv(tmp_path))
        # anilist:s1 < kf:s1
        ids = {r[0] for r in studio_conn.execute("SELECT id FROM studios").fetchall()}
        assert "anilist:s1" in ids
        assert "kf:s1" not in ids

    def test_anime_studios_re_pointed(
        self, studio_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_studios(studio_conn, self._csv(tmp_path))
        # anilist:a10 was pointing at kf:s1; should now point to anilist:s1
        row = studio_conn.execute(
            "SELECT studio_id FROM anime_studios WHERE anime_id = 'anilist:a10'"
        ).fetchone()
        assert row is not None
        assert row[0] == "anilist:s1"

    def test_audit_table_populated(
        self, studio_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_studios(studio_conn, self._csv(tmp_path))
        count = studio_conn.execute(
            "SELECT COUNT(*) FROM meta_entity_resolution_audit WHERE table_name = 'studios'"
        ).fetchone()[0]
        assert count == 1

    def test_returns_merge_count(
        self, studio_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        result = merge_studios(studio_conn, self._csv(tmp_path))
        assert result["merge_count"] == 1
        assert result["audit_logged"] == 1

    def test_idempotent_second_run(
        self, studio_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_studios(studio_conn, self._csv(tmp_path))
        result2 = merge_studios(studio_conn, self._csv(tmp_path))
        assert result2["merge_count"] == 0
        count = studio_conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
        assert count == 2

    def test_unique_studio_not_affected(
        self, studio_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        merge_studios(studio_conn, self._csv(tmp_path))
        row = studio_conn.execute(
            "SELECT id FROM studios WHERE id = 'anilist:s2'"
        ).fetchone()
        assert row is not None

    def test_different_country_not_merged(
        self, mem_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        # Two studios with same normalized name but different countries
        mem_conn.execute("INSERT INTO studios VALUES (?, ?, ?)", ["a:s1", "MAPPA", "JP"])
        mem_conn.execute("INSERT INTO studios VALUES (?, ?, ?)", ["b:s1", "MAPPA", "KR"])
        csv_path = tmp_path / "studios.csv"
        _write_studio_csv(
            csv_path,
            [
                {
                    "candidate_id_a": "a:s1",
                    "candidate_id_b": "b:s1",
                    "sources": "a,b",
                    "evidence_name": "mappa",
                    "country_of_origin": "JP",
                    "row_cnt": "2",
                    "src_cnt": "2",
                    "similarity": "1.0",
                }
            ],
        )
        before = mem_conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
        result = merge_studios(mem_conn, csv_path)
        after = mem_conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
        assert after == before
        assert result["merge_count"] == 0

    def test_low_similarity_not_merged(
        self, studio_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        before = studio_conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
        result = merge_studios(studio_conn, self._csv(tmp_path, sim="0.95"))
        after = studio_conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
        assert after == before
        assert result["merge_count"] == 0
