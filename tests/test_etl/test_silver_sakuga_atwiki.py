"""Tests for src/etl/sakuga_title_matcher and src/etl/conformed_loaders/sakuga_atwiki.

Test structure:
    TestTitleMatcher    — unit tests for _normalize and match_title
    TestSakugaLoader    — integration tests using an in-memory DuckDB with synthetic data
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.etl.sakuga_title_matcher import _normalize, match_title
from src.etl.conformed_loaders.sakuga_atwiki import integrate, _apply_ddl


# ─── Helpers ────────────────────────────────────────────────────────────────

def _make_silver_db() -> duckdb.DuckDBPyConnection:
    """Create an in-memory SILVER DuckDB with minimal schema for testing."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE anime (
            id       VARCHAR PRIMARY KEY,
            title_ja VARCHAR NOT NULL DEFAULT '',
            title_en VARCHAR NOT NULL DEFAULT '',
            year     INTEGER,
            season   VARCHAR,
            quarter  INTEGER,
            episodes INTEGER,
            format   VARCHAR,
            duration INTEGER,
            start_date  VARCHAR,
            end_date    VARCHAR,
            status   VARCHAR,
            source_mat VARCHAR,
            work_type  VARCHAR,
            scale_class VARCHAR,
            fetched_at  TIMESTAMP,
            content_hash VARCHAR,
            updated_at  TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE persons (
            id          VARCHAR PRIMARY KEY,
            name_ja     VARCHAR NOT NULL DEFAULT '',
            name_en     VARCHAR NOT NULL DEFAULT '',
            name_ko     VARCHAR NOT NULL DEFAULT '',
            name_zh     VARCHAR NOT NULL DEFAULT '',
            names_alt   VARCHAR NOT NULL DEFAULT '{}',
            birth_date  VARCHAR,
            death_date  VARCHAR,
            website_url VARCHAR,
            updated_at  TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE credits (
            person_id       VARCHAR,
            anime_id        VARCHAR,
            role            VARCHAR NOT NULL,
            raw_role        VARCHAR NOT NULL,
            episode         INTEGER,
            evidence_source VARCHAR NOT NULL,
            affiliation     VARCHAR,
            position        INTEGER,
            updated_at      TIMESTAMP DEFAULT now()
        )
    """)
    return conn


_PERSONS_SCHEMA = {
    "page_id": pd.Series(dtype="Int64"),
    "name": pd.Series(dtype="object"),
    "aliases_json": pd.Series(dtype="object"),
    "active_since_year": pd.Series(dtype="Int64"),
    "html_sha256": pd.Series(dtype="object"),
    "raw_wikibody_text": pd.Series(dtype="object"),
    "parse_ok": pd.Series(dtype="boolean"),
    "date": pd.Series(dtype="Int64"),
    "source": pd.Series(dtype="object"),
    "table": pd.Series(dtype="object"),
}

_CREDITS_SCHEMA = {
    "person_page_id": pd.Series(dtype="Int64"),
    "work_title": pd.Series(dtype="object"),
    "work_year": pd.Series(dtype="Int64"),
    "work_format": pd.Series(dtype="object"),
    "role_raw": pd.Series(dtype="object"),
    "episode_raw": pd.Series(dtype="object"),
    "episode_num": pd.Series(dtype="Int64"),
    "evidence_source": pd.Series(dtype="object"),
    "date": pd.Series(dtype="Int64"),
    "source": pd.Series(dtype="object"),
    "table": pd.Series(dtype="object"),
}

_TABLE_SCHEMAS: dict[str, dict] = {
    "persons": _PERSONS_SCHEMA,
    "credits": _CREDITS_SCHEMA,
}


def _write_parquet(tmp_path: Path, table: str, rows: list[dict]) -> Path:
    """Write rows as a hive-partitioned BRONZE parquet file.

    When rows is empty, writes an empty-but-schema-complete parquet so that
    DuckDB can read the file metadata without error.
    """
    base = tmp_path / "source=sakuga_atwiki" / f"table={table}" / "date=20260426"
    base.mkdir(parents=True, exist_ok=True)
    if rows:
        df = pd.DataFrame(rows)
    else:
        schema = _TABLE_SCHEMAS.get(table)
        df = pd.DataFrame(schema) if schema else pd.DataFrame()
    out = base / "test.parquet"
    df.to_parquet(out, index=False)
    return tmp_path


# ─── TestTitleMatcher ────────────────────────────────────────────────────────

class TestNormalize:
    def test_nfkc_fullwidth(self) -> None:
        """Full-width ASCII chars are normalized to half-width."""
        assert _normalize("ＡＢＣ") == "abc"

    def test_whitespace_stripped(self) -> None:
        assert _normalize("進 撃 の 巨人") == "進撃の巨人"

    def test_punctuation_stripped(self) -> None:
        assert _normalize("Re：ゼロ") == "reゼロ"

    def test_lowercase(self) -> None:
        assert _normalize("Attack on Titan") == "attackontitan"

    def test_empty_string(self) -> None:
        assert _normalize("") == ""


class TestMatchTitle:
    _anime = [
        ("a1", "進撃の巨人", "Attack on Titan", 2013),
        ("a2", "ヴァイオレット・エヴァーガーデン", "Violet Evergarden", 2018),
        ("a3", "Re：ゼロから始める異世界生活", "Re:ZERO -Starting Life in Another World-", 2016),
    ]

    def test_exact_match_ja(self) -> None:
        aid, method, score = match_title("進撃の巨人", 2013, self._anime)
        assert aid == "a1"
        assert method == "exact_title"
        assert score == 1.0

    def test_exact_match_en(self) -> None:
        aid, method, score = match_title("Attack on Titan", 2013, self._anime)
        assert aid == "a1"
        assert method == "exact_title"
        assert score == 1.0

    def test_normalized_match(self) -> None:
        # Fullwidth colon → halfwidth, but otherwise same title
        aid, method, score = match_title("Re：ゼロから始める異世界生活", 2016, self._anime)
        # Should be exact (the strings match literally in this case)
        assert aid == "a3"
        assert score >= 0.95

    def test_normalized_match_whitespace(self) -> None:
        # Extra spaces should still normalize-match
        aid, method, score = match_title("進 撃 の 巨 人", 2013, self._anime)
        assert aid == "a1"
        assert method == "normalized"
        assert score == 0.95

    def test_year_guard_exact_mismatch(self) -> None:
        """Year differs by > 1 → no match."""
        aid, method, score = match_title("進撃の巨人", 2020, self._anime)
        assert aid is None
        assert method == "unresolved"

    def test_year_guard_within_tolerance(self) -> None:
        """Year differs by 1 → still matches."""
        aid, method, score = match_title("進撃の巨人", 2014, self._anime)
        assert aid == "a1"

    def test_none_title_returns_unresolved(self) -> None:
        aid, method, score = match_title(None, 2013, self._anime)
        assert aid is None
        assert method == "unresolved"
        assert score == 0.0

    def test_no_match_returns_unresolved(self) -> None:
        aid, method, score = match_title("Unknown Title XYZ", None, self._anime)
        assert aid is None
        assert method == "unresolved"

    def test_ambiguous_multiple_hits_returns_unresolved(self) -> None:
        """Two anime with same title → conservatively unresolved."""
        dupes = [
            ("x1", "同名作品", "Same Title", 2010),
            ("x2", "同名作品", "Same Title", 2010),
        ]
        aid, method, score = match_title("同名作品", 2010, dupes)
        assert aid is None
        assert method == "unresolved"

    def test_no_year_constraint_matches_all_years(self) -> None:
        """work_year=None removes the year guard."""
        aid, method, score = match_title("進撃の巨人", None, self._anime)
        assert aid == "a1"

    def test_anime_year_none_matches_regardless(self) -> None:
        """Anime with year=None is not filtered out."""
        anime = [("a9", "テスト作品", "Test Work", None)]
        aid, method, score = match_title("テスト作品", 2020, anime)
        assert aid == "a9"


# ─── TestSakugaLoader ────────────────────────────────────────────────────────

class TestApplyDDL:
    def test_creates_resolution_table(self) -> None:
        conn = _make_silver_db()
        _apply_ddl(conn)
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        assert "sakuga_work_title_resolution" in tables

    def test_idempotent(self) -> None:
        """Running _apply_ddl twice must not raise."""
        conn = _make_silver_db()
        _apply_ddl(conn)
        _apply_ddl(conn)  # second run should not raise

    def test_persons_gets_aliases_column(self) -> None:
        conn = _make_silver_db()
        _apply_ddl(conn)
        cols = {r[0] for r in conn.execute("DESCRIBE persons").fetchall()}
        assert "aliases" in cols
        assert "years_active" in cols


class TestIntegrate:
    def test_persons_inserted(self, tmp_path: Path) -> None:
        conn = _make_silver_db()
        bronze_root = _write_parquet(tmp_path, "persons", [
            {"page_id": 1, "name": "山田太郎", "aliases_json": '["ヤマダ"]',
             "active_since_year": 2000, "html_sha256": "abc", "raw_wikibody_text": "",
             "parse_ok": True, "date": 20260426, "source": "sakuga_atwiki",
             "table": "persons"},
        ])
        # Also write empty credits and work_staff parquets so the loader can proceed
        _write_parquet(tmp_path, "credits", [
            {"person_page_id": 1, "work_title": "ダミー作品", "work_year": 2000,
             "work_format": "TV", "role_raw": "原画", "episode_raw": "1",
             "episode_num": 1, "evidence_source": "sakuga_atwiki",
             "date": 20260426, "source": "sakuga_atwiki", "table": "credits"},
        ])
        counts = integrate(conn, bronze_root)
        assert counts["sakuga_persons"] >= 1
        row = conn.execute(
            "SELECT name_ja, aliases, years_active FROM persons WHERE id = 'sakuga:p1'"
        ).fetchone()
        assert row is not None
        assert row[0] == "山田太郎"
        assert row[1] == '["ヤマダ"]'
        assert row[2] == "2000-"

    def test_persons_no_duplicate_on_rerun(self, tmp_path: Path) -> None:
        conn = _make_silver_db()
        bronze_root = _write_parquet(tmp_path, "persons", [
            {"page_id": 2, "name": "鈴木花子", "aliases_json": "[]",
             "active_since_year": None, "html_sha256": "x", "raw_wikibody_text": "",
             "parse_ok": True, "date": 20260426, "source": "sakuga_atwiki",
             "table": "persons"},
        ])
        _write_parquet(tmp_path, "credits", [])
        integrate(conn, bronze_root)
        integrate(conn, bronze_root)  # second run — must not raise or duplicate
        cnt = conn.execute(
            "SELECT COUNT(*) FROM persons WHERE id = 'sakuga:p2'"
        ).fetchone()[0]
        assert cnt == 1

    def test_resolution_table_populated(self, tmp_path: Path) -> None:
        conn = _make_silver_db()
        # Seed SILVER anime
        conn.execute("""
            INSERT INTO anime (id, title_ja, title_en, year)
            VALUES ('anilist:1', '進撃の巨人', 'Attack on Titan', 2013)
        """)
        bronze_root = _write_parquet(tmp_path, "persons", [])
        _write_parquet(tmp_path, "credits", [
            {"person_page_id": 10, "work_title": "進撃の巨人", "work_year": 2013,
             "work_format": "TV", "role_raw": "原画", "episode_raw": "1",
             "episode_num": 1, "evidence_source": "sakuga_atwiki",
             "date": 20260426, "source": "sakuga_atwiki", "table": "credits"},
        ])
        counts = integrate(conn, bronze_root)
        assert counts["resolution_rows"] == 1
        assert counts["resolved_anime_ids"] == 1
        row = conn.execute(
            "SELECT resolved_anime_id, match_method, match_score "
            "FROM sakuga_work_title_resolution WHERE work_title = '進撃の巨人'"
        ).fetchone()
        assert row[0] == "anilist:1"
        assert row[1] == "exact_title"
        assert row[2] == 1.0

    def test_credits_anime_id_backfilled(self, tmp_path: Path) -> None:
        conn = _make_silver_db()
        conn.execute("""
            INSERT INTO anime (id, title_ja, title_en, year)
            VALUES ('anilist:2', 'ヴァイオレット・エヴァーガーデン', 'Violet Evergarden', 2018)
        """)
        # Pre-insert a credit with anime_id = NULL (as integrate_duckdb would)
        conn.execute("""
            INSERT INTO credits (person_id, anime_id, role, raw_role, episode, evidence_source)
            VALUES ('sakuga:p5', NULL, 'key_animator', '原画', 1, 'sakuga_atwiki')
        """)
        bronze_root = _write_parquet(tmp_path, "persons", [])
        _write_parquet(tmp_path, "credits", [
            {"person_page_id": 5, "work_title": "ヴァイオレット・エヴァーガーデン",
             "work_year": 2018, "work_format": "TV", "role_raw": "原画",
             "episode_raw": "1", "episode_num": 1,
             "evidence_source": "sakuga_atwiki",
             "date": 20260426, "source": "sakuga_atwiki", "table": "credits"},
        ])
        counts = integrate(conn, bronze_root)
        assert counts["credits_resolved"] >= 1
        row = conn.execute(
            "SELECT anime_id FROM credits "
            "WHERE person_id = 'sakuga:p5' AND evidence_source = 'sakuga_atwiki'"
        ).fetchone()
        assert row[0] == "anilist:2"

    def test_unresolved_credits_stay_null(self, tmp_path: Path) -> None:
        conn = _make_silver_db()
        # No matching anime in SILVER
        conn.execute("""
            INSERT INTO credits (person_id, anime_id, role, raw_role, episode, evidence_source)
            VALUES ('sakuga:p7', NULL, 'key_animator', 'unknown_role', 1, 'sakuga_atwiki')
        """)
        bronze_root = _write_parquet(tmp_path, "persons", [])
        _write_parquet(tmp_path, "credits", [
            {"person_page_id": 7, "work_title": "存在しない作品", "work_year": 2024,
             "work_format": "TV", "role_raw": "unknown_role",
             "episode_raw": "1", "episode_num": 1,
             "evidence_source": "sakuga_atwiki",
             "date": 20260426, "source": "sakuga_atwiki", "table": "credits"},
        ])
        integrate(conn, bronze_root)
        row = conn.execute(
            "SELECT anime_id FROM credits WHERE person_id = 'sakuga:p7'"
        ).fetchone()
        assert row[0] is None  # still NULL — no match

    def test_evidence_source_preserved(self, tmp_path: Path) -> None:
        """evidence_source must remain 'sakuga_atwiki' after credits UPDATE (H4)."""
        conn = _make_silver_db()
        conn.execute("""
            INSERT INTO anime (id, title_ja, title_en, year)
            VALUES ('anilist:3', 'テスト', 'Test', 2020)
        """)
        conn.execute("""
            INSERT INTO credits (person_id, anime_id, role, raw_role, episode, evidence_source)
            VALUES ('sakuga:p8', NULL, 'other', 'なにか', 1, 'sakuga_atwiki')
        """)
        bronze_root = _write_parquet(tmp_path, "persons", [])
        _write_parquet(tmp_path, "credits", [
            {"person_page_id": 8, "work_title": "テスト", "work_year": 2020,
             "work_format": "TV", "role_raw": "なにか",
             "episode_raw": "1", "episode_num": 1,
             "evidence_source": "sakuga_atwiki",
             "date": 20260426, "source": "sakuga_atwiki", "table": "credits"},
        ])
        integrate(conn, bronze_root)
        row = conn.execute(
            "SELECT evidence_source FROM credits WHERE person_id = 'sakuga:p8'"
        ).fetchone()
        assert row[0] == "sakuga_atwiki"

    def test_resolution_table_idempotent(self, tmp_path: Path) -> None:
        """Running integrate twice must not create duplicate resolution rows."""
        conn = _make_silver_db()
        conn.execute("""
            INSERT INTO anime (id, title_ja, title_en, year)
            VALUES ('anilist:4', '千と千尋の神隠し', 'Spirited Away', 2001)
        """)
        bronze_root = _write_parquet(tmp_path, "persons", [])
        _write_parquet(tmp_path, "credits", [
            {"person_page_id": 9, "work_title": "千と千尋の神隠し", "work_year": 2001,
             "work_format": "Movie", "role_raw": "作画監督",
             "episode_raw": None, "episode_num": None,
             "evidence_source": "sakuga_atwiki",
             "date": 20260426, "source": "sakuga_atwiki", "table": "credits"},
        ])
        integrate(conn, bronze_root)
        integrate(conn, bronze_root)  # second run
        cnt = conn.execute(
            "SELECT COUNT(*) FROM sakuga_work_title_resolution "
            "WHERE work_title = '千と千尋の神隠し'"
        ).fetchone()[0]
        assert cnt == 1  # ON CONFLICT DO NOTHING — no duplicates
