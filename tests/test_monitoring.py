"""Data freshness monitoring tests."""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest
from typer.testing import CliRunner

from src.cli import app
from src.freshness import (
    DEFAULT_THRESHOLD_HOURS,
    FRESHNESS_THRESHOLDS,
    check_data_freshness,
    get_freshness_summary,
)

runner = CliRunner()


@pytest.fixture()
def db_conn(tmp_path):
    """Create an in-memory-like SQLite DB with ops_source_scrape_status table."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE ops_source_scrape_status (
            source TEXT PRIMARY KEY,
            last_scraped_at TIMESTAMP,
            item_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'ok'
        )
    """)
    conn.commit()
    return conn


@pytest.fixture()
def populated_freshness_db(monkeypatch, tmp_path):
    """DB with ops_source_scrape_status and full schema for CLI testing."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            name_ja TEXT DEFAULT '',
            name_en TEXT DEFAULT '',
            aliases TEXT DEFAULT '[]',
            mal_id INTEGER,
            anilist_id INTEGER,
            canonical_id TEXT,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );
        CREATE TABLE IF NOT EXISTS anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT DEFAULT '',
            title_en TEXT DEFAULT '',
            year INTEGER,
            season TEXT,
            episodes INTEGER,
            mal_id INTEGER,
            anilist_id INTEGER,
            score REAL,
            UNIQUE(mal_id),
            UNIQUE(anilist_id)
        );
        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            episode INTEGER DEFAULT -1,
            source TEXT DEFAULT '',
            UNIQUE(person_id, anime_id, role, episode)
        );
        CREATE TABLE IF NOT EXISTS scores (
            person_id TEXT PRIMARY KEY,
            iv_score REAL DEFAULT 0.0,
            person_fe REAL DEFAULT 0.0,
            birank REAL DEFAULT 0.0,
            patronage REAL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS ops_source_scrape_status (
            source TEXT PRIMARY KEY,
            last_scraped_at TIMESTAMP,
            item_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'ok'
        );
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    # Insert some data sources with mixed freshness
    now = datetime.now(timezone.utc)
    fresh_time = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    stale_time = (now - timedelta(hours=500)).strftime("%Y-%m-%d %H:%M:%S")

    conn.execute(
        "INSERT INTO ops_source_scrape_status (source, last_scraped_at, item_count, status) VALUES (?, ?, ?, ?)",
        ("anilist", fresh_time, 1500, "ok"),
    )
    conn.execute(
        "INSERT INTO ops_source_scrape_status (source, last_scraped_at, item_count, status) VALUES (?, ?, ?, ?)",
        ("mal", stale_time, 800, "ok"),
    )
    conn.execute(
        "INSERT INTO ops_source_scrape_status (source, last_scraped_at, item_count, status) VALUES (?, ?, ?, ?)",
        ("mediaarts", None, 0, "ok"),
    )
    conn.commit()
    conn.close()

    def patched_get(db_path=None):
        c = sqlite3.connect(str(db_path or tmp_path / "test.db"))
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr("src.database.get_connection", patched_get)
    return db_path


# --- check_data_freshness tests ---


class TestCheckDataFreshness:
    def test_fresh_source(self, db_conn):
        """Source scraped recently should not be stale."""
        now = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        scraped_at = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status (source, last_scraped_at, item_count, status) VALUES (?, ?, ?, ?)",
            ("anilist", scraped_at, 1000, "ok"),
        )
        db_conn.commit()

        reports = check_data_freshness(db_conn, now=now)
        assert len(reports) == 1
        r = reports[0]
        assert r.source == "anilist"
        assert r.is_stale is False
        assert r.hours_since_scrape is not None
        assert r.hours_since_scrape < FRESHNESS_THRESHOLDS["anilist"]
        assert r.item_count == 1000
        assert r.threshold_hours == FRESHNESS_THRESHOLDS["anilist"]

    def test_stale_source(self, db_conn):
        """Source scraped long ago should be stale."""
        now = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        scraped_at = (now - timedelta(hours=500)).strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status (source, last_scraped_at, item_count, status) VALUES (?, ?, ?, ?)",
            ("anilist", scraped_at, 500, "ok"),
        )
        db_conn.commit()

        reports = check_data_freshness(db_conn, now=now)
        assert len(reports) == 1
        r = reports[0]
        assert r.is_stale is True
        assert r.hours_since_scrape is not None
        assert r.hours_since_scrape > FRESHNESS_THRESHOLDS["anilist"]

    def test_never_scraped_source(self, db_conn):
        """Source with NULL last_scraped_at should be stale."""
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status (source, last_scraped_at, item_count, status) VALUES (?, ?, ?, ?)",
            ("wikidata", None, 0, "ok"),
        )
        db_conn.commit()

        reports = check_data_freshness(db_conn)
        assert len(reports) == 1
        r = reports[0]
        assert r.is_stale is True
        assert r.hours_since_scrape is None
        assert r.last_scraped_at is None

    def test_custom_threshold_for_mediaarts(self, db_conn):
        """mediaarts has a 30-day (720h) threshold."""
        now = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        # 20 days ago -- should be fresh for mediaarts (threshold=720h)
        scraped_at = (now - timedelta(hours=480)).strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status (source, last_scraped_at, item_count, status) VALUES (?, ?, ?, ?)",
            ("mediaarts", scraped_at, 200, "ok"),
        )
        db_conn.commit()

        reports = check_data_freshness(db_conn, now=now)
        assert len(reports) == 1
        r = reports[0]
        assert r.is_stale is False
        assert r.threshold_hours == 720

    def test_unknown_source_uses_default_threshold(self, db_conn):
        """Unknown source should use DEFAULT_THRESHOLD_HOURS."""
        now = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        scraped_at = (now - timedelta(hours=200)).strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status (source, last_scraped_at, item_count, status) VALUES (?, ?, ?, ?)",
            ("custom_source", scraped_at, 100, "ok"),
        )
        db_conn.commit()

        reports = check_data_freshness(db_conn, now=now)
        assert len(reports) == 1
        r = reports[0]
        assert r.threshold_hours == DEFAULT_THRESHOLD_HOURS
        assert r.is_stale is True  # 200h > 168h default

    def test_empty_database(self, db_conn):
        """No data sources should return empty list."""
        reports = check_data_freshness(db_conn)
        assert reports == []

    def test_multiple_sources(self, db_conn):
        """Multiple sources with mixed states."""
        now = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        fresh = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        stale = (now - timedelta(hours=500)).strftime("%Y-%m-%d %H:%M:%S")

        db_conn.execute(
            "INSERT INTO ops_source_scrape_status VALUES (?, ?, ?, ?)",
            ("anilist", fresh, 1000, "ok"),
        )
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status VALUES (?, ?, ?, ?)",
            ("mal", stale, 500, "ok"),
        )
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status VALUES (?, ?, ?, ?)",
            ("wikidata", None, 0, "pending"),
        )
        db_conn.commit()

        reports = check_data_freshness(db_conn, now=now)
        assert len(reports) == 3

        by_source = {r.source: r for r in reports}
        assert by_source["anilist"].is_stale is False
        assert by_source["mal"].is_stale is True
        assert by_source["wikidata"].is_stale is True
        assert by_source["wikidata"].hours_since_scrape is None


# --- get_freshness_summary tests ---


class TestGetFreshnessSummary:
    def test_all_fresh(self, db_conn):
        """All sources fresh -> healthy."""
        now = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        fresh = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status VALUES (?, ?, ?, ?)",
            ("anilist", fresh, 1000, "ok"),
        )
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status VALUES (?, ?, ?, ?)", ("mal", fresh, 800, "ok")
        )
        db_conn.commit()

        summary = get_freshness_summary(db_conn, now=now)
        assert summary["overall_status"] == "healthy"
        assert summary["total_sources"] == 2
        assert summary["stale_sources"] == 0
        assert summary["fresh_sources"] == 2
        assert len(summary["sources"]) == 2

    def test_all_stale(self, db_conn):
        """All sources stale -> critical."""
        now = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        stale = (now - timedelta(hours=500)).strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status VALUES (?, ?, ?, ?)",
            ("anilist", stale, 100, "ok"),
        )
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status VALUES (?, ?, ?, ?)", ("mal", None, 0, "ok")
        )
        db_conn.commit()

        summary = get_freshness_summary(db_conn, now=now)
        assert summary["overall_status"] == "critical"
        assert summary["stale_sources"] == 2
        assert summary["fresh_sources"] == 0

    def test_mixed_states(self, db_conn):
        """Mixed fresh and stale -> warning."""
        now = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        fresh = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        stale = (now - timedelta(hours=500)).strftime("%Y-%m-%d %H:%M:%S")

        db_conn.execute(
            "INSERT INTO ops_source_scrape_status VALUES (?, ?, ?, ?)",
            ("anilist", fresh, 1000, "ok"),
        )
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status VALUES (?, ?, ?, ?)", ("mal", stale, 500, "ok")
        )
        db_conn.commit()

        summary = get_freshness_summary(db_conn, now=now)
        assert summary["overall_status"] == "warning"
        assert summary["total_sources"] == 2
        assert summary["stale_sources"] == 1
        assert summary["fresh_sources"] == 1

    def test_empty_sources(self, db_conn):
        """No sources at all -> warning."""
        summary = get_freshness_summary(db_conn)
        assert summary["overall_status"] == "warning"
        assert summary["total_sources"] == 0

    def test_summary_source_dicts(self, db_conn):
        """Verify summary sources contain expected keys."""
        now = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        fresh = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        db_conn.execute(
            "INSERT INTO ops_source_scrape_status VALUES (?, ?, ?, ?)",
            ("anilist", fresh, 1000, "ok"),
        )
        db_conn.commit()

        summary = get_freshness_summary(db_conn, now=now)
        src = summary["sources"][0]
        assert "source" in src
        assert "last_scraped_at" in src
        assert "item_count" in src
        assert "status" in src
        assert "is_stale" in src
        assert "hours_since_scrape" in src
        assert "threshold_hours" in src


# --- CLI freshness command tests ---


class TestFreshnessCliCommand:
    def test_freshness_command_displays_table(self, populated_freshness_db):
        result = runner.invoke(app, ["freshness"])
        assert result.exit_code == 0
        assert "Data Source Freshness" in result.output
        assert "anilist" in result.output
        assert "mal" in result.output
        assert "mediaarts" in result.output

    def test_freshness_shows_status(self, populated_freshness_db):
        result = runner.invoke(app, ["freshness"])
        assert result.exit_code == 0
        assert "Fresh" in result.output
        assert "Stale" in result.output or "Never scraped" in result.output

    def test_freshness_empty_db(self, monkeypatch, tmp_path):
        """Empty ops_source_scrape_status table shows 'No data sources registered.'"""
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE persons (id TEXT PRIMARY KEY, name_ja TEXT DEFAULT '', name_en TEXT DEFAULT '', aliases TEXT DEFAULT '[]', mal_id INTEGER, anilist_id INTEGER, canonical_id TEXT, UNIQUE(mal_id), UNIQUE(anilist_id));
            CREATE TABLE anime (id TEXT PRIMARY KEY, title_ja TEXT DEFAULT '', title_en TEXT DEFAULT '', year INTEGER, season TEXT, episodes INTEGER, mal_id INTEGER, anilist_id INTEGER, score REAL, UNIQUE(mal_id), UNIQUE(anilist_id));
            CREATE TABLE credits (id INTEGER PRIMARY KEY AUTOINCREMENT, person_id TEXT NOT NULL, anime_id TEXT NOT NULL, role TEXT NOT NULL, episode INTEGER DEFAULT -1, source TEXT DEFAULT '', evidence_source TEXT, UNIQUE(person_id, anime_id, role, episode));
            CREATE TABLE person_scores (person_id TEXT PRIMARY KEY, iv_score REAL DEFAULT 0.0, person_fe REAL DEFAULT 0.0, birank REAL DEFAULT 0.0, patronage REAL DEFAULT 0.0, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE ops_source_scrape_status (source TEXT PRIMARY KEY, last_scraped_at TIMESTAMP, item_count INTEGER DEFAULT 0, status TEXT DEFAULT 'ok');
            CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        """)
        conn.commit()
        conn.close()

        def patched_get(db_path=None):
            c = sqlite3.connect(str(tmp_path / "empty.db"))
            c.row_factory = sqlite3.Row
            return c

        monkeypatch.setattr("src.database.get_connection", patched_get)

        result = runner.invoke(app, ["freshness"])
        assert result.exit_code == 0
        assert "No data sources registered" in result.output


# --- API freshness endpoint tests ---


class TestFreshnessApiEndpoint:
    def test_freshness_endpoint(self, monkeypatch, tmp_path):
        """GET /api/freshness returns empty dict (Bronze Parquet migration pending)."""
        from fastapi.testclient import TestClient

        from src.api import app as fastapi_app

        client = TestClient(fastapi_app)
        response = client.get("/api/freshness")
        assert response.status_code == 200
        assert response.json() == {}

    def test_freshness_endpoint_empty(self, monkeypatch, tmp_path):
        """GET /api/freshness returns empty dict regardless of DB state."""
        from fastapi.testclient import TestClient

        from src.api import app as fastapi_app

        client = TestClient(fastapi_app)
        response = client.get("/api/freshness")
        assert response.status_code == 200
        assert response.json() == {}
