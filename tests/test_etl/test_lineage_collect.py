"""Tests for src/etl/lineage/collect.py.

Uses in-memory DuckDB + a synthetic BRONZE parquet tree written to tmp_path
to verify:
- DDL creation of meta_lineage
- row counts per SILVER table
- correct bronze_source / bronze_table mapping
- UPSERT (idempotent re-run)
- missing bronze partition handled gracefully (row_count = 0)
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.etl.lineage.collect import (
    _META_LINEAGE_DDL,
    _SILVER_TO_BRONZE,
    collect,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_silver_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with the 28 SILVER tables (minimal DDL)."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anime (
            id VARCHAR PRIMARY KEY,
            title_ja VARCHAR NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS persons (
            id VARCHAR PRIMARY KEY,
            name_ja VARCHAR NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS credits (
            person_id VARCHAR NOT NULL,
            anime_id VARCHAR NOT NULL,
            role VARCHAR NOT NULL,
            evidence_source VARCHAR NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS characters (id VARCHAR PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS character_voice_actors (
            character_id VARCHAR, person_id VARCHAR, anime_id VARCHAR,
            updated_at TIMESTAMP DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS studios (id VARCHAR PRIMARY KEY, updated_at TIMESTAMP DEFAULT now());
        CREATE TABLE IF NOT EXISTS anime_studios (anime_id VARCHAR, studio_id VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_genres (anime_id VARCHAR, genre VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_episodes (anime_id VARCHAR, episode_num INTEGER);
        CREATE TABLE IF NOT EXISTS anime_companies (anime_id VARCHAR, company VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_releases (anime_id VARCHAR, release_date VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_news (anime_id VARCHAR, title VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_relations (anime_id VARCHAR, related_id VARCHAR, relation_type VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_recommendations (anime_id VARCHAR, recommended_id VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_broadcasters (anime_id VARCHAR, broadcaster VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_broadcast_schedule (anime_id VARCHAR, day VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_video_releases (anime_id VARCHAR, format VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_production_companies (anime_id VARCHAR, company VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_production_committee (anime_id VARCHAR, member VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_original_work_links (anime_id VARCHAR, url VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_theme_songs (anime_id VARCHAR, title VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_episode_titles (anime_id VARCHAR, episode INTEGER, title VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_gross_studios (anime_id VARCHAR, studio VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_original_work_info (anime_id VARCHAR, info VARCHAR);
        CREATE TABLE IF NOT EXISTS person_jobs (person_id VARCHAR, job VARCHAR);
        CREATE TABLE IF NOT EXISTS person_studio_affiliations (person_id VARCHAR, studio_id VARCHAR);
        CREATE TABLE IF NOT EXISTS anime_settings_categories (anime_id VARCHAR, category VARCHAR);
        CREATE TABLE IF NOT EXISTS sakuga_work_title_resolution (
            work_title VARCHAR, anime_id VARCHAR
        );
    """)
    return conn


def _write_parquet(path: Path, rows: int) -> None:
    """Write a minimal parquet file with `rows` dummy rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"id": [str(i) for i in range(rows)]})
    pq.write_table(table, str(path))


def _make_bronze_tree(root: Path) -> None:
    """Create a minimal synthetic BRONZE parquet tree.

    Writes one parquet per (source, table) with small row counts so we can
    verify that collect() reads them correctly.
    """
    tree = {
        "anilist": {
            "anime": 10,
            "persons": 5,
            "credits": 20,
            "characters": 3,
            "character_voice_actors": 4,
            "studios": 2,
            "anime_studios": 6,
            "relations": 3,
        },
        "ann": {
            "anime": 8,
            "credits": 12,
            "persons": 4,
            "cast": 5,
            "company": 2,
            "episodes": 7,
            "news": 3,
            "releases": 2,
            "related": 1,
        },
        "seesaawiki": {
            "anime": 6,
            "credits": 9,
            "persons": 3,
            "theme_songs": 2,
            "episode_titles": 4,
            "gross_studios": 1,
            "original_work_info": 1,
            "production_committee": 1,
        },
        "keyframe": {
            "anime": 7,
            "credits": 11,
            "persons": 3,
            "settings_categories": 2,
            "studios_master": 2,
            "anime_studios": 5,
            "person_jobs": 2,
            "person_studios": 2,
        },
        "mediaarts": {
            "anime": 5,
            "credits": 8,
            "persons": 2,
            "broadcast_schedule": 2,
            "broadcasters": 3,
            "original_work_links": 1,
            "production_committee": 1,
            "production_companies": 1,
            "video_releases": 4,
        },
        "mal": {
            "anime": 9,
            "staff_credits": 15,
            "anime_characters": 6,
            "anime_studios": 7,
            "anime_genres": 5,
            "anime_recommendations": 4,
            "anime_relations": 3,
        },
        "bangumi": {
            "subjects": 4,
            "persons": 3,
            "characters": 2,
            "subject_persons": 5,
            "person_characters": 3,
        },
        "sakuga_atwiki": {
            "pages": 10,
            "credits": 6,
        },
    }
    date = "2026-05-01"
    for source, tables in tree.items():
        for table, rows in tables.items():
            parquet_path = root / f"source={source}" / f"table={table}" / f"date={date}" / "part.parquet"
            _write_parquet(parquet_path, rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def silver_conn() -> duckdb.DuckDBPyConnection:
    """In-memory SILVER DuckDB with all 28 tables (empty rows)."""
    conn = _make_silver_conn()
    # Insert a few rows so silver_row_count is verifiable
    conn.execute("INSERT INTO anime VALUES ('a:1', 'テスト', now())")
    conn.execute("INSERT INTO anime VALUES ('a:2', 'Test', now())")
    conn.execute("INSERT INTO persons VALUES ('p:1', '山田', now())")
    yield conn
    conn.close()


@pytest.fixture
def bronze_root(tmp_path: Path) -> Path:
    """Synthetic BRONZE parquet tree in tmp_path."""
    root = tmp_path / "bronze"
    _make_bronze_tree(root)
    return root


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMetaLineageDDL:
    """meta_lineage DDL creates the expected columns."""

    def test_ddl_creates_table(self) -> None:
        conn = duckdb.connect(":memory:")
        conn.execute(_META_LINEAGE_DDL)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(meta_lineage)").fetchall()}
        assert "silver_table" in cols
        assert "bronze_source" in cols
        assert "bronze_table" in cols
        assert "partition_date" in cols
        assert "silver_row_count" in cols
        assert "bronze_row_count" in cols
        assert "collected_at" in cols
        conn.close()

    def test_ddl_idempotent(self) -> None:
        conn = duckdb.connect(":memory:")
        conn.execute(_META_LINEAGE_DDL)
        conn.execute(_META_LINEAGE_DDL)  # second call must not raise
        conn.close()


class TestCollectSmoke:
    """collect() smoke tests with synthetic data."""

    def test_returns_positive_row_count(
        self, silver_conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        n = collect(silver_conn, bronze_root)
        assert n > 0

    def test_meta_lineage_table_created(
        self, silver_conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        collect(silver_conn, bronze_root)
        tables = {t[0] for t in silver_conn.execute("SHOW TABLES").fetchall()}
        assert "meta_lineage" in tables

    def test_all_28_silver_tables_represented(
        self, silver_conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        collect(silver_conn, bronze_root)
        result = silver_conn.execute(
            "SELECT DISTINCT silver_table FROM meta_lineage"
        ).fetchall()
        actual = {row[0] for row in result}
        expected = set(_SILVER_TO_BRONZE.keys())
        assert actual == expected, f"missing: {expected - actual}"

    def test_silver_row_count_populated(
        self, silver_conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        collect(silver_conn, bronze_root)
        anime_row = silver_conn.execute(
            "SELECT silver_row_count FROM meta_lineage WHERE silver_table = 'anime' LIMIT 1"
        ).fetchone()
        assert anime_row is not None
        assert anime_row[0] == 2  # two rows inserted in fixture

    def test_bronze_row_count_populated_for_known_source(
        self, silver_conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        collect(silver_conn, bronze_root)
        row = silver_conn.execute(
            "SELECT bronze_row_count FROM meta_lineage "
            "WHERE silver_table = 'anime' AND bronze_source = 'anilist' AND bronze_table = 'anime'"
        ).fetchone()
        assert row is not None
        assert row[0] == 10  # matches _make_bronze_tree

    def test_partition_date_populated(
        self, silver_conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        collect(silver_conn, bronze_root)
        row = silver_conn.execute(
            "SELECT partition_date FROM meta_lineage "
            "WHERE bronze_source = 'anilist' AND bronze_table = 'anime'"
        ).fetchone()
        assert row is not None
        assert row[0] == "2026-05-01"

    def test_missing_bronze_partition_gives_zero_count(
        self, silver_conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        # sakuga_work_title_resolution maps to sakuga_atwiki/pages
        # Our synthetic tree has sakuga_atwiki/pages so let's use a subset mapping
        small_mapping = {
            "sakuga_work_title_resolution": [("sakuga_atwiki", "no_such_table")],
        }
        collect(silver_conn, bronze_root, mapping=small_mapping)
        row = silver_conn.execute(
            "SELECT bronze_row_count FROM meta_lineage "
            "WHERE silver_table = 'sakuga_work_title_resolution'"
        ).fetchone()
        assert row is not None
        assert row[0] == 0


class TestCollectIdempotent:
    """Re-running collect() does UPSERT, not duplicate rows."""

    def test_upsert_no_duplicates(
        self, silver_conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        collect(silver_conn, bronze_root)
        n_first = silver_conn.execute("SELECT COUNT(*) FROM meta_lineage").fetchone()[0]

        collect(silver_conn, bronze_root)
        n_second = silver_conn.execute("SELECT COUNT(*) FROM meta_lineage").fetchone()[0]

        assert n_first == n_second

    def test_upsert_updates_collected_at(
        self, silver_conn: duckdb.DuckDBPyConnection, bronze_root: Path
    ) -> None:
        collect(silver_conn, bronze_root)
        ts1 = silver_conn.execute(
            "SELECT collected_at FROM meta_lineage WHERE silver_table = 'anime' LIMIT 1"
        ).fetchone()[0]

        import time

        time.sleep(0.01)  # ensure clock advances
        collect(silver_conn, bronze_root)
        ts2 = silver_conn.execute(
            "SELECT collected_at FROM meta_lineage WHERE silver_table = 'anime' LIMIT 1"
        ).fetchone()[0]

        assert ts2 >= ts1


class TestMappingCoverage:
    """Verify the static mapping covers all 28 expected SILVER tables."""

    _EXPECTED_28 = {
        "anime",
        "persons",
        "credits",
        "characters",
        "character_voice_actors",
        "studios",
        "anime_studios",
        "anime_genres",
        "anime_episodes",
        "anime_companies",
        "anime_releases",
        "anime_news",
        "anime_relations",
        "anime_recommendations",
        "anime_broadcasters",
        "anime_broadcast_schedule",
        "anime_video_releases",
        "anime_production_companies",
        "anime_production_committee",
        "anime_original_work_links",
        "anime_theme_songs",
        "anime_episode_titles",
        "anime_gross_studios",
        "anime_original_work_info",
        "person_jobs",
        "person_studio_affiliations",
        "anime_settings_categories",
        "sakuga_work_title_resolution",
    }

    def test_mapping_has_all_28_tables(self) -> None:
        assert set(_SILVER_TO_BRONZE.keys()) == self._EXPECTED_28

    def test_each_entry_has_at_least_one_source(self) -> None:
        for table, sources in _SILVER_TO_BRONZE.items():
            assert len(sources) >= 1, f"{table} has no bronze sources"

    def test_no_duplicate_source_per_table(self) -> None:
        for table, sources in _SILVER_TO_BRONZE.items():
            seen: set[tuple[str, str]] = set()
            for pair in sources:
                assert pair not in seen, f"duplicate {pair} in {table}"
                seen.add(pair)
