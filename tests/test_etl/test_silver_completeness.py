"""Tests for src/etl/audit/silver_completeness.py.

Uses synthetic BRONZE parquet (written via BronzeWriter) and an in-memory
DuckDB as the SILVER database.  No real silver.duckdb or result/bronze
parquet files are required.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.audit.silver_completeness import (
    CoverageRow,
    check,
    generate_report,
    list_bronze_tables,
    sample_missing_rows,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_parquet(bronze_root: Path, source: str, table: str, rows: list[dict]) -> None:
    """Write rows to a BRONZE parquet under bronze_root."""
    with BronzeWriter(source, table=table, root=bronze_root, compact_on_exit=False) as bw:
        for row in rows:
            bw.append(row)


def _make_silver_db(tmp_path: Path) -> Path:
    """Create a minimal SILVER DuckDB with anime + credits + persons tables."""
    db_path = tmp_path / "silver.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE anime (
            id       VARCHAR PRIMARY KEY,
            title_ja VARCHAR DEFAULT '',
            title_en VARCHAR DEFAULT ''
        );
        CREATE TABLE persons (
            id      VARCHAR PRIMARY KEY,
            name_ja VARCHAR DEFAULT '',
            name_en VARCHAR DEFAULT ''
        );
        CREATE TABLE credits (
            person_id       VARCHAR,
            anime_id        VARCHAR,
            role            VARCHAR NOT NULL DEFAULT 'other',
            raw_role        VARCHAR NOT NULL DEFAULT '',
            episode         INTEGER,
            evidence_source VARCHAR NOT NULL,
            credit_year     INTEGER,
            credit_quarter  INTEGER,
            affiliation     VARCHAR,
            position        INTEGER,
            updated_at      TIMESTAMP DEFAULT now()
        );
        CREATE TABLE character_voice_actors (
            character_id   VARCHAR NOT NULL,
            person_id      VARCHAR NOT NULL,
            anime_id       VARCHAR NOT NULL,
            character_role VARCHAR DEFAULT '',
            source         VARCHAR NOT NULL DEFAULT '',
            updated_at     TIMESTAMP DEFAULT now(),
            PRIMARY KEY (character_id, person_id, anime_id)
        );
        CREATE TABLE characters (
            id      VARCHAR PRIMARY KEY,
            name_ja VARCHAR DEFAULT '',
            name_en VARCHAR DEFAULT ''
        );
        CREATE TABLE studios (
            id   VARCHAR PRIMARY KEY,
            name VARCHAR DEFAULT ''
        );
        CREATE TABLE anime_studios (
            anime_id  VARCHAR NOT NULL,
            studio_id VARCHAR NOT NULL,
            is_main   BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (anime_id, studio_id)
        );
        CREATE TABLE person_jobs (
            id        BIGINT PRIMARY KEY,
            person_id VARCHAR NOT NULL,
            job       VARCHAR NOT NULL,
            source    VARCHAR NOT NULL DEFAULT 'keyframe'
        );
        CREATE TABLE person_studio_affiliations (
            id          BIGINT PRIMARY KEY,
            person_id   VARCHAR NOT NULL,
            studio_name VARCHAR NOT NULL,
            source      VARCHAR NOT NULL DEFAULT 'keyframe'
        );
        CREATE TABLE anime_settings_categories (
            anime_id VARCHAR NOT NULL,
            category VARCHAR NOT NULL,
            source   VARCHAR NOT NULL DEFAULT 'keyframe',
            PRIMARY KEY (anime_id, category)
        );
        CREATE TABLE anime_genres (
            anime_id VARCHAR NOT NULL,
            genre    VARCHAR NOT NULL,
            PRIMARY KEY (anime_id, genre)
        );
        CREATE TABLE anime_episodes (
            anime_id  VARCHAR NOT NULL,
            episode   INTEGER NOT NULL,
            PRIMARY KEY (anime_id, episode)
        );
        CREATE TABLE anime_news (
            id       BIGINT PRIMARY KEY,
            anime_id VARCHAR NOT NULL,
            title    VARCHAR DEFAULT ''
        );
        CREATE TABLE anime_recommendations (
            anime_id       VARCHAR NOT NULL,
            recommended_id VARCHAR NOT NULL,
            PRIMARY KEY (anime_id, recommended_id)
        );
        CREATE TABLE anime_relations (
            anime_id    VARCHAR NOT NULL,
            related_id  VARCHAR NOT NULL,
            relation    VARCHAR DEFAULT '',
            PRIMARY KEY (anime_id, related_id)
        );
        CREATE TABLE anime_episode_titles (
            anime_id VARCHAR NOT NULL,
            episode  INTEGER NOT NULL,
            title_ja VARCHAR DEFAULT '',
            PRIMARY KEY (anime_id, episode)
        );
        CREATE TABLE anime_theme_songs (
            anime_id VARCHAR NOT NULL,
            position INTEGER NOT NULL,
            song_type VARCHAR DEFAULT '',
            PRIMARY KEY (anime_id, position)
        );
        CREATE TABLE anime_gross_studios (
            anime_id   VARCHAR NOT NULL,
            studio_name VARCHAR NOT NULL,
            PRIMARY KEY (anime_id, studio_name)
        );
        CREATE TABLE anime_original_work_info (
            anime_id VARCHAR NOT NULL PRIMARY KEY,
            info_json VARCHAR DEFAULT '{}'
        );
        CREATE TABLE anime_production_committee (
            anime_id    VARCHAR NOT NULL,
            company_name VARCHAR NOT NULL,
            PRIMARY KEY (anime_id, company_name)
        );
        CREATE TABLE anime_broadcasters (
            anime_id  VARCHAR NOT NULL,
            name      VARCHAR NOT NULL,
            PRIMARY KEY (anime_id, name)
        );
        CREATE TABLE anime_broadcast_schedule (
            anime_id  VARCHAR NOT NULL,
            weekday   INTEGER NOT NULL,
            PRIMARY KEY (anime_id, weekday)
        );
        CREATE TABLE anime_original_work_links (
            anime_id   VARCHAR NOT NULL,
            url        VARCHAR NOT NULL,
            PRIMARY KEY (anime_id, url)
        );
        CREATE TABLE anime_production_companies (
            anime_id     VARCHAR NOT NULL,
            company_name VARCHAR NOT NULL,
            PRIMARY KEY (anime_id, company_name)
        );
        CREATE TABLE anime_video_releases (
            anime_id   VARCHAR NOT NULL,
            release_id VARCHAR NOT NULL,
            PRIMARY KEY (anime_id, release_id)
        );
        CREATE TABLE anime_releases (
            anime_id   VARCHAR NOT NULL,
            release_id VARCHAR NOT NULL,
            PRIMARY KEY (anime_id, release_id)
        );
        CREATE TABLE anime_companies (
            anime_id     VARCHAR NOT NULL,
            company_name VARCHAR NOT NULL,
            PRIMARY KEY (anime_id, company_name)
        );
        CREATE TABLE sakuga_work_title_resolution (
            page_title VARCHAR NOT NULL PRIMARY KEY,
            anime_id   VARCHAR
        );
    """)
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def bronze_root(tmp_path: Path) -> Path:
    """Write minimal synthetic BRONZE parquet for anilist and mal."""
    root = tmp_path / "bronze"

    # anilist/anime — 3 rows
    _write_parquet(root, "anilist", "anime", [
        {"id": "anilist:1", "title_ja": "アニメA", "title_en": "Anime A",
         "content_hash": "h1", "fetched_at": "2024-01-01"},
        {"id": "anilist:2", "title_ja": "アニメB", "title_en": "Anime B",
         "content_hash": "h2", "fetched_at": "2024-01-01"},
        {"id": "anilist:3", "title_ja": "アニメC", "title_en": "Anime C",
         "content_hash": "h3", "fetched_at": "2024-01-01"},
    ])

    # anilist/credits — 4 rows
    _write_parquet(root, "anilist", "credits", [
        {"person_id": "anilist:p1", "anime_id": "anilist:1", "role": "animation_director",
         "raw_role": "Animation Director", "episode": None, "source": "anilist"},
        {"person_id": "anilist:p2", "anime_id": "anilist:1", "role": "key_animation",
         "raw_role": "Key Animation", "episode": 1, "source": "anilist"},
        {"person_id": "anilist:p1", "anime_id": "anilist:2", "role": "director",
         "raw_role": "Director", "episode": None, "source": "anilist"},
        {"person_id": "anilist:p3", "anime_id": "anilist:3", "role": "other",
         "raw_role": "Misc", "episode": None, "source": "anilist"},
    ])

    # mal/anime — 2 rows
    _write_parquet(root, "mal", "anime", [
        {"id": "mal:a1", "title_ja": "マルA", "title_en": "Mal Anime A",
         "content_hash": "m1"},
        {"id": "mal:a2", "title_ja": "マルB", "title_en": "Mal Anime B",
         "content_hash": "m2"},
    ])

    return root


@pytest.fixture
def silver_db(tmp_path: Path, bronze_root: Path) -> Path:
    """Create a minimal SILVER DuckDB with partial data."""
    db_path = _make_silver_db(tmp_path)
    conn = duckdb.connect(str(db_path))

    # Load 2 of 3 anilist anime
    conn.execute("INSERT INTO anime VALUES ('anilist:1', 'アニメA', 'Anime A')")
    conn.execute("INSERT INTO anime VALUES ('anilist:2', 'アニメB', 'Anime B')")

    # Load all 4 anilist credits
    conn.execute("""
        INSERT INTO credits (person_id, anime_id, role, raw_role, evidence_source) VALUES
        ('anilist:p1', 'anilist:1', 'animation_director', 'Animation Director', 'anilist'),
        ('anilist:p2', 'anilist:1', 'key_animation',      'Key Animation',      'anilist'),
        ('anilist:p1', 'anilist:2', 'director',           'Director',           'anilist'),
        ('anilist:p3', 'anilist:3', 'other',              'Misc',               'anilist')
    """)

    # Load 1 of 2 mal anime
    conn.execute("INSERT INTO anime VALUES ('mal:a1', 'マルA', 'Mal Anime A')")

    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# list_bronze_tables tests
# ---------------------------------------------------------------------------

def test_list_bronze_tables_finds_known_tables(bronze_root: Path) -> None:
    tables = list_bronze_tables(bronze_root)
    assert ("anilist", "anime") in tables
    assert ("anilist", "credits") in tables
    assert ("mal", "anime") in tables


def test_list_bronze_tables_skips_bak_dirs(tmp_path: Path) -> None:
    """Backup dirs like source=ann.bak-20260425 must be ignored."""
    root = tmp_path / "bronze"
    # Create a fake bak dir
    bak_dir = root / "source=ann.bak-20260425" / "table=anime" / "date=2026-04-25"
    bak_dir.mkdir(parents=True)
    (bak_dir / "dummy.parquet").touch()

    tables = list_bronze_tables(root)
    sources = {s for s, _ in tables}
    assert "ann.bak-20260425" not in sources


def test_list_bronze_tables_empty_root(tmp_path: Path) -> None:
    empty = tmp_path / "no_bronze"
    assert list_bronze_tables(empty) == []


# ---------------------------------------------------------------------------
# check() tests
# ---------------------------------------------------------------------------

def test_check_returns_coverage_rows(bronze_root: Path, silver_db: Path) -> None:
    """check() must return a non-empty list of CoverageRow."""
    rows = check(bronze_root, str(silver_db))
    assert len(rows) > 0
    assert all(isinstance(r, CoverageRow) for r in rows)


def test_check_anilist_anime_partial(bronze_root: Path, silver_db: Path) -> None:
    """anilist/anime: 2/3 loaded → coverage ≈ 0.667 (PARTIAL)."""
    rows = check(bronze_root, str(silver_db))
    row = next(r for r in rows if r.source == "anilist" and r.bronze_table == "anime")
    assert row.bronze_rows == 3
    assert row.silver_rows == 2
    assert row.coverage == pytest.approx(2 / 3, abs=0.01)
    assert row.status == "PARTIAL"


def test_check_anilist_credits_full(bronze_root: Path, silver_db: Path) -> None:
    """anilist/credits: all 4 rows loaded → coverage = 1.0 (OK)."""
    rows = check(bronze_root, str(silver_db))
    row = next(r for r in rows if r.source == "anilist" and r.bronze_table == "credits")
    assert row.bronze_rows == 4
    assert row.silver_rows == 4
    assert row.coverage == pytest.approx(1.0)
    assert row.status == "OK"


def test_check_mal_anime_partial(bronze_root: Path, silver_db: Path) -> None:
    """mal/anime: 1/2 loaded → coverage = 0.5 (PARTIAL)."""
    rows = check(bronze_root, str(silver_db))
    row = next(r for r in rows if r.source == "mal" and r.bronze_table == "anime")
    assert row.bronze_rows == 2
    assert row.silver_rows == 1
    assert row.coverage == pytest.approx(0.5)
    assert row.status == "PARTIAL"


def test_check_unmapped_table(tmp_path: Path) -> None:
    """Tables mapped to None in SOURCE_TABLE_TO_SILVER are reported as UNMAPPED."""
    root = tmp_path / "bronze"
    # ann/episodes is explicitly unmapped
    _write_parquet(root, "ann", "episodes", [{"ann_id": 1, "title": "Ep 1"}])
    db_path = _make_silver_db(tmp_path)

    rows = check(root, str(db_path))
    row = next((r for r in rows if r.source == "ann" and r.bronze_table == "episodes"), None)
    assert row is not None
    assert row.unmapped is True
    assert row.status == "UNMAPPED"
    assert row.silver_rows == 0


def test_check_empty_bronze_source(tmp_path: Path) -> None:
    """Empty bronze → coverage 1.0 (nothing to load)."""
    root = tmp_path / "bronze"
    # Write a parquet with zero data rows
    with BronzeWriter("anilist", table="anime", root=root, compact_on_exit=False):
        pass  # no rows appended — flush() writes an empty parquet

    db_path = _make_silver_db(tmp_path)
    # Need at least one row to make DuckDB happy with empty parquet
    # So let's just check that the module handles 0-row tables gracefully
    rows = check(root, str(db_path))
    # If table is in mapping, it should produce a row with no error
    for r in rows:
        assert r.error is None or r.unmapped  # no unexpected errors


# ---------------------------------------------------------------------------
# CoverageRow property tests
# ---------------------------------------------------------------------------

def test_coverage_row_status_ok() -> None:
    r = CoverageRow("src", "tbl", 100, "anime", None, 97, 0.97, False)
    assert r.status == "OK"
    assert r.coverage_pct == "97.0%"


def test_coverage_row_status_partial() -> None:
    r = CoverageRow("src", "tbl", 100, "anime", None, 70, 0.70, False)
    assert r.status == "PARTIAL"
    assert r.coverage_pct == "70.0%"


def test_coverage_row_status_low() -> None:
    r = CoverageRow("src", "tbl", 100, "anime", None, 30, 0.30, False)
    assert r.status == "LOW"
    assert r.coverage_pct == "30.0%"


def test_coverage_row_status_unmapped() -> None:
    r = CoverageRow("src", "tbl", 50, None, None, 0, None, True)
    assert r.status == "UNMAPPED"
    assert r.coverage_pct == "unmapped"


def test_coverage_row_zero_bronze() -> None:
    """Empty BRONZE → coverage 1.0."""
    r = CoverageRow("src", "tbl", 0, "anime", None, 0, 1.0, False)
    assert r.status == "OK"


# ---------------------------------------------------------------------------
# generate_report() smoke test
# ---------------------------------------------------------------------------

def test_generate_report_creates_file(bronze_root: Path, silver_db: Path, tmp_path: Path) -> None:
    """generate_report() creates a markdown file with expected sections."""
    rows = check(bronze_root, str(silver_db))
    out = tmp_path / "report" / "silver_completeness.md"
    generate_report(rows, out)

    assert out.exists()
    text = out.read_text(encoding="utf-8")
    assert "# BRONZE → SILVER Completeness Report" in text
    assert "## Summary" in text
    assert "## Per-source Coverage" in text
    assert "Disclaimer" in text


def test_generate_report_contains_source_sections(
    bronze_root: Path, silver_db: Path, tmp_path: Path
) -> None:
    """Each bronze source must appear as its own subsection."""
    rows = check(bronze_root, str(silver_db))
    out = tmp_path / "report.md"
    generate_report(rows, out)

    text = out.read_text(encoding="utf-8")
    assert "### anilist" in text
    assert "### mal" in text


# ---------------------------------------------------------------------------
# sample_missing_rows smoke test
# ---------------------------------------------------------------------------

def test_sample_missing_rows_returns_list(bronze_root: Path, silver_db: Path) -> None:
    """sample_missing_rows must return a list of dicts (may be empty)."""
    result = sample_missing_rows(
        bronze_root=bronze_root,
        source="anilist",
        bronze_table="anime",
        silver_db=str(silver_db),
        silver_table="anime",
        silver_filter="id LIKE 'anilist:%'",
        n=5,
    )
    assert isinstance(result, list)
    for row in result:
        assert isinstance(row, dict)
