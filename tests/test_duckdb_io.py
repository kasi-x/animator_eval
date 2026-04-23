"""Tests for DuckDB read-path functions (Card 05: silver-native version)."""

from __future__ import annotations

import duckdb
import pytest

# Minimal silver DDL (mirrors integrate_duckdb.py)
_SILVER_DDL = """
CREATE TABLE IF NOT EXISTS anime (
    id          VARCHAR PRIMARY KEY,
    title_ja    VARCHAR NOT NULL DEFAULT '',
    title_en    VARCHAR NOT NULL DEFAULT '',
    year        INTEGER,
    season      VARCHAR,
    quarter     INTEGER,
    episodes    INTEGER,
    format      VARCHAR,
    duration    INTEGER,
    start_date  VARCHAR,
    end_date    VARCHAR,
    status      VARCHAR,
    source_mat  VARCHAR,
    work_type   VARCHAR,
    scale_class VARCHAR,
    updated_at  TIMESTAMP DEFAULT now()
);
CREATE TABLE IF NOT EXISTS persons (
    id      VARCHAR PRIMARY KEY,
    name_ja VARCHAR NOT NULL DEFAULT '',
    name_en VARCHAR NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS credits (
    person_id       VARCHAR NOT NULL,
    anime_id        VARCHAR NOT NULL,
    role            VARCHAR NOT NULL,
    raw_role        VARCHAR,
    episode         INTEGER,
    evidence_source VARCHAR NOT NULL,
    updated_at      TIMESTAMP DEFAULT now()
);
"""


@pytest.fixture()
def silver_path(tmp_path):
    """Silver DuckDB with minimal dataset (no genre/studio tables)."""
    path = tmp_path / "silver.duckdb"
    conn = duckdb.connect(str(path))
    for stmt in _SILVER_DDL.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.executemany(
        "INSERT INTO anime (id, title_ja, title_en, year, episodes) VALUES (?,?,?,?,?)",
        [("a1", "テストアニメ", "Test Anime", 2020, 12),
         ("a2", "別アニメ", "Another Anime", 2021, 24)],
    )
    conn.executemany(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source)"
        " VALUES (?,?,?,?)",
        [
            ("p0", "a1", "director", "test"),
            ("p1", "a1", "key_animator", "test"),
            ("p2", "a2", "director", "test"),
        ],
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture()
def silver_path_with_related(tmp_path):
    """Silver DuckDB that includes anime_genres, anime_studios, studios tables."""
    path = tmp_path / "silver_full.duckdb"
    conn = duckdb.connect(str(path))
    for stmt in _SILVER_DDL.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.execute(
        "CREATE TABLE anime_genres (anime_id VARCHAR, genre_name VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE anime_tags (anime_id VARCHAR, tag_name VARCHAR, rank INTEGER)"
    )
    conn.execute("CREATE TABLE studios (id INTEGER PRIMARY KEY, name VARCHAR)")
    conn.execute(
        "CREATE TABLE anime_studios (anime_id VARCHAR, studio_id INTEGER, is_main BOOLEAN)"
    )
    conn.executemany(
        "INSERT INTO anime (id, title_ja, title_en, year) VALUES (?,?,?,?)",
        [("a1", "テストアニメ", "Test Anime", 2020),
         ("a2", "別アニメ", "Another Anime", 2021)],
    )
    conn.executemany(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source) VALUES (?,?,?,?)",
        [("p0", "a1", "director", "test"), ("p2", "a2", "director", "test")],
    )
    conn.executemany(
        "INSERT INTO anime_genres VALUES (?,?)",
        [("a1", "Action"), ("a1", "Drama")],
    )
    conn.execute("INSERT INTO studios VALUES (1, 'Studio A')")
    conn.execute("INSERT INTO anime_studios VALUES ('a1', 1, true)")
    conn.commit()
    conn.close()
    return path


class TestLoadCreditsDdb:
    def test_returns_all_rows(self, silver_path):
        from src.analysis.duckdb_io import load_credits_ddb

        rows = load_credits_ddb(silver_path)
        assert len(rows) == 3

    def test_returns_dicts(self, silver_path):
        from src.analysis.duckdb_io import load_credits_ddb

        rows = load_credits_ddb(silver_path)
        assert all(isinstance(r, dict) for r in rows)

    def test_person_id_present(self, silver_path):
        from src.analysis.duckdb_io import load_credits_ddb

        rows = load_credits_ddb(silver_path)
        pids = {r["person_id"] for r in rows}
        assert pids == {"p0", "p1", "p2"}

    def test_evidence_source_column(self, silver_path):
        from src.analysis.duckdb_io import load_credits_ddb

        rows = load_credits_ddb(silver_path)
        assert all("evidence_source" in r for r in rows)


class TestLoadAnimeJoinedDdb:
    def test_returns_all_anime_without_related(self, silver_path):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(silver_path)
        assert len(rows) == 2

    def test_genres_empty_when_table_absent(self, silver_path):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(silver_path)
        for r in rows:
            assert r["genres"] == []

    def test_genres_aggregated_when_table_present(self, silver_path_with_related):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(silver_path_with_related)
        a1 = next(r for r in rows if r["id"] == "a1")
        assert set(a1["genres"]) == {"Action", "Drama"}

    def test_studios_aggregated_when_table_present(self, silver_path_with_related):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(silver_path_with_related)
        a1 = next(r for r in rows if r["id"] == "a1")
        assert "Studio A" in a1["studios"]

    def test_no_genres_returns_empty_list(self, silver_path_with_related):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(silver_path_with_related)
        a2 = next(r for r in rows if r["id"] == "a2")
        assert a2["genres"] == []

    def test_title_preserved(self, silver_path):
        from src.analysis.duckdb_io import load_anime_joined_ddb

        rows = load_anime_joined_ddb(silver_path)
        a1 = next(r for r in rows if r["id"] == "a1")
        assert a1["title_en"] == "Test Anime"


class TestAggCreditsPerPersonDdb:
    def test_returns_empty_when_no_credit_year(self, silver_path):
        """Silver credits do not yet include credit_year → returns []."""
        from src.analysis.duckdb_io import agg_credits_per_person_ddb

        rows = agg_credits_per_person_ddb(silver_path)
        assert rows == []

    def test_groups_when_credit_year_present(self, tmp_path):
        """When silver has credit_year, aggregation works correctly."""
        from src.analysis.duckdb_io import agg_credits_per_person_ddb

        path = tmp_path / "silver_cy.duckdb"
        conn = duckdb.connect(str(path))
        conn.execute("""
            CREATE TABLE credits (
                person_id VARCHAR, anime_id VARCHAR, role VARCHAR,
                evidence_source VARCHAR, credit_year INTEGER
            )
        """)
        conn.executemany(
            "INSERT INTO credits VALUES (?,?,?,?,?)",
            [
                ("p0", "a1", "director", "test", 2020),
                ("p1", "a1", "key_animator", "test", 2020),
                ("p0", "a2", "director", "test", 2021),
            ],
        )
        conn.close()

        rows = agg_credits_per_person_ddb(path)
        assert len(rows) == 3
        p0_rows = [r for r in rows if r["person_id"] == "p0"]
        assert any(r["credit_year"] == 2020 for r in p0_rows)
        assert any(r["credit_year"] == 2021 for r in p0_rows)
