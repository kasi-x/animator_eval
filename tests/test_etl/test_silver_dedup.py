"""Tests for src/etl/audit/silver_dedup.py.

Uses synthetic in-memory DuckDB fixtures; no real silver.duckdb required.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.etl.audit.silver_dedup import (
    audit,
    find_anime_dup_candidates,
    find_credit_within_source_dup,
    find_person_dup_candidates,
    find_studio_dup_candidates,
)

# ---------------------------------------------------------------------------
# Minimal DDL for the SILVER tables used by the audit
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS persons (
    id          VARCHAR PRIMARY KEY,
    name_ja     VARCHAR DEFAULT '',
    name_en     VARCHAR DEFAULT '',
    birth_date  VARCHAR,
    gender      VARCHAR
);

CREATE TABLE IF NOT EXISTS anime (
    id          VARCHAR PRIMARY KEY,
    title_ja    VARCHAR DEFAULT '',
    title_en    VARCHAR DEFAULT '',
    year        INTEGER,
    format      VARCHAR,
    duration    INTEGER
);

CREATE TABLE IF NOT EXISTS studios (
    id                  VARCHAR PRIMARY KEY,
    name                VARCHAR DEFAULT '',
    country_of_origin   VARCHAR
);

CREATE TABLE IF NOT EXISTS credits (
    person_id       VARCHAR,
    anime_id        VARCHAR NOT NULL,
    role            VARCHAR NOT NULL,
    evidence_source VARCHAR NOT NULL,
    episode         INTEGER,
    affiliation     VARCHAR,
    position        INTEGER,
    updated_at      TIMESTAMP,
    credit_year     INTEGER,
    credit_quarter  INTEGER,
    raw_role        VARCHAR
);
"""


@pytest.fixture()
def empty_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with SILVER schema but no data."""
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL)
    return conn


@pytest.fixture()
def loaded_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB pre-populated with synthetic data for dedup tests."""
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL)

    # --- persons -----------------------------------------------------------
    # a1 + a2: same name_ja + birth_date, different sources → dup candidate
    conn.execute(
        "INSERT INTO persons VALUES (?, ?, ?, ?, ?)",
        ["anilist:p1", "山田太郎", "", "1985-04-01", "Male"],
    )
    conn.execute(
        "INSERT INTO persons VALUES (?, ?, ?, ?, ?)",
        ["bgm:p99", "山田太郎", "", "1985-04-01", "Male"],
    )
    # b1: unique person (no match)
    conn.execute(
        "INSERT INTO persons VALUES (?, ?, ?, ?, ?)",
        ["anilist:p2", "鈴木花子", "", "1990-07-15", "Female"],
    )
    # c1: same name_ja, different birth_date → NOT a dup candidate
    conn.execute(
        "INSERT INTO persons VALUES (?, ?, ?, ?, ?)",
        ["bgm:p100", "鈴木花子", "", "1991-07-15", "Female"],
    )
    # d1 + d2: same name_ja, same birth_date, SAME source → NOT cross-source
    conn.execute(
        "INSERT INTO persons VALUES (?, ?, ?, ?, ?)",
        ["bgm:p200", "佐藤一郎", "", "1970-01-01", "Male"],
    )
    conn.execute(
        "INSERT INTO persons VALUES (?, ?, ?, ?, ?)",
        ["bgm:p201", "佐藤一郎", "", "1970-01-01", "Male"],
    )

    # --- anime -------------------------------------------------------------
    # a1 + a2: same norm title + year + format, different sources → dup candidate
    conn.execute(
        "INSERT INTO anime (id, title_ja, year, format) VALUES (?, ?, ?, ?)",
        ["anilist:a1", "魔法少女まどか☆マギカ", 2011, "TV"],
    )
    conn.execute(
        "INSERT INTO anime (id, title_ja, year, format) VALUES (?, ?, ?, ?)",
        ["madb:a1", "魔法少女まどかマギカ", 2011, "TV"],
    )
    # b1: unique
    conn.execute(
        "INSERT INTO anime (id, title_ja, year, format) VALUES (?, ?, ?, ?)",
        ["anilist:a2", "進撃の巨人", 2013, "TV"],
    )
    # c1: same title, different year → NOT dup (cross-source, different year)
    conn.execute(
        "INSERT INTO anime (id, title_ja, year, format) VALUES (?, ?, ?, ?)",
        ["madb:a2", "進撃の巨人", 2014, "TV"],
    )
    # d1: same title same year SAME source as b1 → NOT cross-source (both madb)
    conn.execute(
        "INSERT INTO anime (id, title_ja, year, format) VALUES (?, ?, ?, ?)",
        ["madb:a3", "銀魂", 2006, "TV"],
    )
    conn.execute(
        "INSERT INTO anime (id, title_ja, year, format) VALUES (?, ?, ?, ?)",
        ["madb:a4", "銀魂", 2006, "TV"],
    )

    # --- studios -----------------------------------------------------------
    # a1 + a2: same norm name (punctuation differs), different sources → dup
    conn.execute(
        "INSERT INTO studios VALUES (?, ?, ?)",
        ["anilist:s1", "J.C.STAFF", None],
    )
    conn.execute(
        "INSERT INTO studios VALUES (?, ?, ?)",
        ["kf:s1", "JC STAFF", None],
    )
    # b1: unique
    conn.execute(
        "INSERT INTO studios VALUES (?, ?, ?)",
        ["anilist:s2", "BONES", None],
    )

    # --- credits -----------------------------------------------------------
    # within-source dup: same person+anime+role+source+episode x2
    conn.execute(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source, episode)"
        " VALUES (?, ?, ?, ?, ?)",
        ["anilist:p1", "anilist:a1", "director", "anilist", None],
    )
    conn.execute(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source, episode)"
        " VALUES (?, ?, ?, ?, ?)",
        ["anilist:p1", "anilist:a1", "director", "anilist", None],
    )
    # cross-source OK row (should NOT appear as within-source dup)
    conn.execute(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source, episode)"
        " VALUES (?, ?, ?, ?, ?)",
        ["bgm:p99", "anilist:a1", "director", "bangumi", None],
    )
    # episode-keyed within-source dup
    conn.execute(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source, episode)"
        " VALUES (?, ?, ?, ?, ?)",
        ["anilist:p2", "anilist:a2", "key_animator", "anilist", 3],
    )
    conn.execute(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source, episode)"
        " VALUES (?, ?, ?, ?, ?)",
        ["anilist:p2", "anilist:a2", "key_animator", "anilist", 3],
    )
    # unique credit (no dup)
    conn.execute(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source, episode)"
        " VALUES (?, ?, ?, ?, ?)",
        ["bgm:p99", "anilist:a2", "producer", "bangumi", None],
    )

    return conn


# ---------------------------------------------------------------------------
# persons tests
# ---------------------------------------------------------------------------


class TestFindPersonDupCandidates:
    def test_empty_db_returns_empty(self, empty_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_person_dup_candidates(empty_conn)
        assert result == []

    def test_cross_source_dup_detected(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_person_dup_candidates(loaded_conn)
        # Only anilist:p1 + bgm:p99 should appear
        assert len(result) == 1
        row = result[0]
        assert "anilist" in row["sources"]
        assert "bgm" in row["sources"]
        assert row["evidence_name"] == "山田太郎"
        assert row["evidence_birth_date"] == "1985-04-01"

    def test_same_source_not_returned(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        # bgm:p200 + bgm:p201 are same source, should not appear
        result = find_person_dup_candidates(loaded_conn)
        ids = [r["candidate_id_a"] + r["candidate_id_b"] for r in result]
        assert not any("p200" in s or "p201" in s for s in ids)

    def test_different_birth_date_not_returned(
        self, loaded_conn: duckdb.DuckDBPyConnection
    ) -> None:
        result = find_person_dup_candidates(loaded_conn)
        # 鈴木花子 has different birth dates between sources
        assert not any(r["evidence_name"] == "鈴木花子" for r in result)

    def test_return_type(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_person_dup_candidates(loaded_conn)
        assert isinstance(result, list)
        if result:
            assert isinstance(result[0], dict)
            assert "candidate_id_a" in result[0]
            assert "similarity" in result[0]


# ---------------------------------------------------------------------------
# anime tests
# ---------------------------------------------------------------------------


class TestFindAnimeDupCandidates:
    def test_empty_db_returns_empty(self, empty_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_anime_dup_candidates(empty_conn)
        assert result == []

    def test_cross_source_dup_detected(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_anime_dup_candidates(loaded_conn)
        assert len(result) >= 1
        # The madoka match should appear
        madoka = [r for r in result if "anilist:a1" in (r["candidate_id_a"], r["candidate_id_b"])]
        assert madoka, "madoka cross-source pair not detected"
        row = madoka[0]
        assert "anilist" in row["sources"]
        assert "madb" in row["sources"]
        assert row["evidence_year"] == 2011

    def test_different_year_not_returned(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        # 進撃の巨人: anilist 2013 vs madb 2014 → should NOT be dup candidate
        result = find_anime_dup_candidates(loaded_conn)
        shingeki_cross = [
            r
            for r in result
            if "anilist:a2" in (r["candidate_id_a"], r["candidate_id_b"])
        ]
        assert shingeki_cross == []

    def test_h1_no_score_in_query(self) -> None:
        """Verify that the SQL for anime dedup never references score columns."""
        from src.etl.audit.silver_dedup import _ANIME_DUP_SQL

        for forbidden in ("score", "popularity", "favourites", "display_"):
            assert forbidden not in _ANIME_DUP_SQL.lower(), (
                f"H1 violation: '{forbidden}' found in _ANIME_DUP_SQL"
            )

    def test_return_type(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_anime_dup_candidates(loaded_conn)
        assert isinstance(result, list)
        if result:
            r = result[0]
            assert "candidate_id_a" in r
            assert "evidence_title" in r
            assert "evidence_year" in r
            assert "evidence_format" in r


# ---------------------------------------------------------------------------
# studios tests
# ---------------------------------------------------------------------------


class TestFindStudioDupCandidates:
    def test_empty_db_returns_empty(self, empty_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_studio_dup_candidates(empty_conn)
        assert result == []

    def test_cross_source_dup_detected(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_studio_dup_candidates(loaded_conn)
        assert len(result) >= 1
        jcstaff = [
            r
            for r in result
            if "anilist:s1" in (r["candidate_id_a"], r["candidate_id_b"])
        ]
        assert jcstaff, "JC Staff cross-source pair not detected"
        row = jcstaff[0]
        assert "anilist" in row["sources"]
        assert "kf" in row["sources"]

    def test_unique_studio_not_returned(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_studio_dup_candidates(loaded_conn)
        bones = [r for r in result if "anilist:s2" in (r["candidate_id_a"], r["candidate_id_b"])]
        assert bones == []

    def test_return_type(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_studio_dup_candidates(loaded_conn)
        assert isinstance(result, list)
        if result:
            r = result[0]
            assert "candidate_id_a" in r
            assert "evidence_name" in r


# ---------------------------------------------------------------------------
# credits tests
# ---------------------------------------------------------------------------


class TestFindCreditWithinSourceDup:
    def test_empty_db_returns_empty(self, empty_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_credit_within_source_dup(empty_conn)
        assert result == []

    def test_within_source_dup_detected(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_credit_within_source_dup(loaded_conn)
        assert len(result) >= 2  # director dup + key_animator dup

    def test_director_dup_found(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_credit_within_source_dup(loaded_conn)
        director_dups = [
            r
            for r in result
            if r["role"] == "director" and r["evidence_source"] == "anilist"
        ]
        assert director_dups, "director within-source dup not detected"
        assert director_dups[0]["dup_count"] == 2

    def test_cross_source_not_counted(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        # bgm:p99/director/bangumi is unique → not in results
        result = find_credit_within_source_dup(loaded_conn)
        bangumi_director = [
            r for r in result if r["evidence_source"] == "bangumi" and r["role"] == "director"
        ]
        assert bangumi_director == []

    def test_return_type(self, loaded_conn: duckdb.DuckDBPyConnection) -> None:
        result = find_credit_within_source_dup(loaded_conn)
        assert isinstance(result, list)
        if result:
            r = result[0]
            assert "person_id" in r
            assert "dup_count" in r


# ---------------------------------------------------------------------------
# audit() orchestrator
# ---------------------------------------------------------------------------


class TestAudit:
    def test_smoke_empty(self, empty_conn: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
        counts = audit(empty_conn, tmp_path)
        assert set(counts.keys()) == {"persons", "anime", "studios", "credits_within_src"}
        assert all(v == 0 for v in counts.values())

    def test_smoke_loaded(self, loaded_conn: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
        counts = audit(loaded_conn, tmp_path)
        assert counts["persons"] >= 1
        assert counts["anime"] >= 1
        assert counts["studios"] >= 1
        assert counts["credits_within_src"] >= 2

    def test_csvs_created(self, loaded_conn: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
        audit(loaded_conn, tmp_path)
        for fname in (
            "silver_dedup_persons.csv",
            "silver_dedup_anime.csv",
            "silver_dedup_studios.csv",
            "silver_dedup_credits.csv",
        ):
            assert (tmp_path / fname).exists(), f"{fname} not created"

    def test_summary_created(self, loaded_conn: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
        audit(loaded_conn, tmp_path)
        summary = tmp_path / "silver_dedup_summary.md"
        assert summary.exists()
        content = summary.read_text(encoding="utf-8")
        assert "SILVER Cross-Source Dedup Audit" in content
        assert "H1" in content
        assert "| persons" in content

    def test_output_dir_created(
        self, loaded_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        nested = tmp_path / "deep" / "nested" / "dir"
        audit(loaded_conn, nested)
        assert nested.is_dir()

    def test_returns_dict(self, loaded_conn: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
        counts = audit(loaded_conn, tmp_path)
        assert isinstance(counts, dict)
        for v in counts.values():
            assert isinstance(v, int)
