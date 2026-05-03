"""Tests for src/etl/audit/anime_studios_coverage.py (Card 22/01).

Verifies:
- measure() returns CoverageRow objects for all sources
- generate_report() writes valid Markdown
- Per-loader anime_studios INSERT correctness:
  - AniList: studios[] array + anime_studios table
  - MAL: kind='studio' filter (fix for 'Studios' vs 'studio' bug)
  - ANN: Animation Production task → anime_studios
  - Mediaarts: production_companies is_main/アニメーション制作 → anime_studios
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.etl.audit.anime_studios_coverage import CoverageRow, generate_report, measure
from src.etl.silver_loaders import anilist as anilist_loader
from src.etl.silver_loaders import ann as ann_loader
from src.etl.silver_loaders import madb as madb_loader
from src.etl.silver_loaders import mal as mal_loader
from src.scrapers.bronze_writer import BronzeWriter


# ---------------------------------------------------------------------------
# Shared SILVER DDL helpers
# ---------------------------------------------------------------------------

_SILVER_DDL = """
CREATE TABLE IF NOT EXISTS anime (
    id           VARCHAR PRIMARY KEY,
    title_ja     VARCHAR NOT NULL DEFAULT '',
    title_en     VARCHAR NOT NULL DEFAULT '',
    year         INTEGER,
    season       VARCHAR,
    quarter      INTEGER,
    episodes     INTEGER,
    format       VARCHAR,
    duration     INTEGER,
    start_date   VARCHAR,
    end_date     VARCHAR,
    status       VARCHAR,
    source_mat   VARCHAR,
    work_type    VARCHAR,
    scale_class  VARCHAR,
    fetched_at   TIMESTAMP,
    content_hash VARCHAR,
    updated_at   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS persons (
    id               VARCHAR PRIMARY KEY,
    name_ja          VARCHAR NOT NULL DEFAULT '',
    name_en          VARCHAR NOT NULL DEFAULT '',
    name_ko          VARCHAR NOT NULL DEFAULT '',
    name_zh          VARCHAR NOT NULL DEFAULT '',
    mal_id           INTEGER,
    anilist_id       INTEGER,
    updated_at       TIMESTAMP DEFAULT now(),
    UNIQUE(mal_id)
);

CREATE SEQUENCE IF NOT EXISTS seq_credits_id;
CREATE TABLE IF NOT EXISTS credits (
    id              INTEGER PRIMARY KEY DEFAULT nextval('seq_credits_id'),
    person_id       VARCHAR NOT NULL,
    anime_id        VARCHAR NOT NULL,
    role            VARCHAR NOT NULL,
    raw_role        VARCHAR NOT NULL DEFAULT '',
    episode         INTEGER,
    evidence_source VARCHAR NOT NULL DEFAULT '',
    credit_year     INTEGER,
    credit_quarter  INTEGER,
    affiliation     VARCHAR,
    position        INTEGER,
    updated_at      TIMESTAMP DEFAULT now(),
    UNIQUE(person_id, anime_id, raw_role, episode)
);

CREATE TABLE IF NOT EXISTS characters (
    id            VARCHAR PRIMARY KEY,
    name_ja       VARCHAR NOT NULL DEFAULT '',
    name_en       VARCHAR NOT NULL DEFAULT '',
    aliases       VARCHAR NOT NULL DEFAULT '[]',
    anilist_id    INTEGER,
    image_large   VARCHAR,
    image_medium  VARCHAR,
    description   VARCHAR,
    gender        VARCHAR,
    date_of_birth VARCHAR,
    age           VARCHAR,
    blood_type    VARCHAR,
    favourites    INTEGER,
    site_url      VARCHAR,
    updated_at    TIMESTAMP DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS seq_cva_id;
CREATE TABLE IF NOT EXISTS character_voice_actors (
    id             INTEGER PRIMARY KEY DEFAULT nextval('seq_cva_id'),
    character_id   VARCHAR NOT NULL,
    person_id      VARCHAR NOT NULL,
    anime_id       VARCHAR NOT NULL,
    character_role VARCHAR NOT NULL DEFAULT '',
    source         VARCHAR NOT NULL DEFAULT '',
    updated_at     TIMESTAMP DEFAULT now(),
    UNIQUE(character_id, person_id, anime_id)
);

CREATE TABLE IF NOT EXISTS studios (
    id                  VARCHAR PRIMARY KEY,
    name                VARCHAR NOT NULL DEFAULT '',
    anilist_id          INTEGER,
    is_animation_studio INTEGER,
    country_of_origin   VARCHAR,
    favourites          INTEGER,
    site_url            VARCHAR,
    updated_at          TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS anime_studios (
    anime_id  VARCHAR NOT NULL,
    studio_id VARCHAR NOT NULL,
    is_main   INTEGER NOT NULL DEFAULT 0,
    role      VARCHAR NOT NULL DEFAULT '',
    source    VARCHAR NOT NULL DEFAULT '',
    PRIMARY KEY (anime_id, studio_id, role, source)
);

CREATE SEQUENCE IF NOT EXISTS seq_anime_relations_id;
CREATE TABLE IF NOT EXISTS anime_relations (
    id               INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_relations_id'),
    anime_id         VARCHAR NOT NULL,
    related_anime_id VARCHAR NOT NULL,
    relation_type    VARCHAR NOT NULL DEFAULT '',
    related_title    VARCHAR NOT NULL DEFAULT '',
    related_format   VARCHAR,
    UNIQUE(anime_id, related_anime_id, relation_type)
);
"""


def _make_silver_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(_SILVER_DDL)
    return conn


def _write_parquet(path: Path, schema: pa.Schema, data: dict) -> None:
    """Write a single parquet file from a dict of column arrays."""
    path.parent.mkdir(parents=True, exist_ok=True)
    arrays = [pa.array(v, type=schema.field(k).type) for k, v in data.items()]
    tbl = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(tbl, path)


# ---------------------------------------------------------------------------
# CoverageRow unit tests
# ---------------------------------------------------------------------------

class TestCoverageRow:
    def test_bronze_studio_pct(self) -> None:
        row = CoverageRow("test", bronze_anime=100, bronze_with_studio=60, silver_in_anime_studios=30)
        assert row.bronze_studio_pct == pytest.approx(0.60)

    def test_capture_rate(self) -> None:
        row = CoverageRow("test", bronze_anime=100, bronze_with_studio=60, silver_in_anime_studios=30)
        assert row.capture_rate == pytest.approx(0.50)

    def test_zero_bronze_anime(self) -> None:
        row = CoverageRow("test", bronze_anime=0, bronze_with_studio=0, silver_in_anime_studios=0)
        assert row.bronze_studio_pct == 0.0
        assert row.capture_rate == 0.0

    def test_capture_rate_capped_at_one(self) -> None:
        """capture_rate is capped at 1.0 even if SILVER > BRONZE."""
        row = CoverageRow("test", bronze_anime=10, bronze_with_studio=5, silver_in_anime_studios=10)
        assert row.capture_rate == 1.0


# ---------------------------------------------------------------------------
# generate_report
# ---------------------------------------------------------------------------

class TestGenerateReport:
    def test_writes_markdown(self, tmp_path: Path) -> None:
        rows = [
            CoverageRow("anilist", 100, 80, 20),
            CoverageRow("mal", 200, 150, 100),
        ]
        out = tmp_path / "report.md"
        generate_report(rows, out, silver_total_anime=500)
        text = out.read_text()
        assert "anime_studios Coverage Audit" in text
        assert "anilist" in text
        assert "mal" in text
        assert "Disclaimers" in text

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        rows = [CoverageRow("test", 0, 0, 0)]
        out = tmp_path / "nested" / "dir" / "report.md"
        generate_report(rows, out)
        assert out.exists()

    def test_overall_coverage_line(self, tmp_path: Path) -> None:
        rows = [CoverageRow("mal", 200, 150, 100)]
        out = tmp_path / "r.md"
        generate_report(rows, out, silver_total_anime=400)
        text = out.read_text()
        assert "100" in text and "400" in text


# ---------------------------------------------------------------------------
# AniList loader: studios[] array → anime_studios
# ---------------------------------------------------------------------------

class TestAnilistStudiosArray:
    """Verify the anilist loader extracts anime_studios from anime.studios[] array."""

    @pytest.fixture
    def bronze_dir(self, tmp_path: Path) -> Path:
        root = tmp_path / "bronze"

        # anime table with studios array
        with BronzeWriter("anilist", table="anime", root=root) as bw:
            bw.append({
                "id": "anilist:1001",
                "title_ja": "テストA",
                "title_en": "Test A",
                "year": 2023,
                "season": "WINTER",
                "quarter": 1,
                "episodes": 12,
                "format": "TV",
                "status": "FINISHED",
                "duration": 24,
                "start_date": "2023-01-07",
                "end_date": "2023-03-25",
                "original_work_type": "ORIGINAL",
                "source": "ORIGINAL",
                "work_type": "TV",
                "scale_class": "medium",
                "fetched_at": "2026-04-28T00:00:00",
                "content_hash": "abc",
                "score": None,
                "mean_score": None,
                "favourites": None,
                "popularity_rank": None,
                "synonyms": None,
                "country_of_origin": "JP",
                "is_licensed": True,
                "is_adult": False,
                "hashtag": None,
                "site_url": "https://anilist.co/anime/1001",
                "trailer_url": None,
                "trailer_site": None,
                "description": "Test anime",
                "cover_large": None,
                "cover_extra_large": None,
                "cover_medium": None,
                "banner": None,
                "external_links_json": None,
                "airing_schedule_json": None,
                "relations_json": "[]",
                "rankings_json": None,
                # Key columns for this test
                "studio": "Studio BONES",
                "studios": ["Studio BONES", "Aniplex"],
                "display_title": "Test A",
            })

        return root

    def test_anime_studios_from_array_inserted(self, bronze_dir: Path) -> None:
        """integrate() extracts anime_studios from anime.studios[] array."""
        conn = _make_silver_conn()
        # Pre-insert anime so UPDATE in _ANIME_EXTRAS_SQL works
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["anilist:1001", "テストA", "Test A"],
        )
        counts = anilist_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts.get("anime_studios_anilist", 0) >= 1

    def test_main_studio_is_main(self, bronze_dir: Path) -> None:
        """The primary studio (anime.studio) gets is_main=1 in anime_studios."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["anilist:1001", "テストA", "Test A"],
        )
        anilist_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT is_main FROM anime_studios "
            "WHERE anime_id = 'anilist:1001' AND studio_id = 'anilist:n:Studio BONES'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 1

    def test_co_studio_is_not_main(self, bronze_dir: Path) -> None:
        """Non-primary studios get is_main=0."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["anilist:1001", "テストA", "Test A"],
        )
        anilist_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT is_main FROM anime_studios "
            "WHERE anime_id = 'anilist:1001' AND studio_id = 'anilist:n:Aniplex'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0

    def test_studios_stub_inserted(self, bronze_dir: Path) -> None:
        """Name-based studio stubs are inserted into studios table."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["anilist:1001", "テストA", "Test A"],
        )
        anilist_loader.integrate(conn, bronze_dir)
        count = conn.execute(
            "SELECT COUNT(*) FROM studios WHERE id LIKE 'anilist:n:%'"
        ).fetchone()[0]
        conn.close()
        assert count >= 2

    def test_idempotent(self, bronze_dir: Path) -> None:
        """Calling integrate() twice does not duplicate anime_studios rows."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["anilist:1001", "テストA", "Test A"],
        )
        anilist_loader.integrate(conn, bronze_dir)
        c1 = conn.execute(
            "SELECT COUNT(*) FROM anime_studios WHERE source = 'anilist'"
        ).fetchone()[0]
        anilist_loader.integrate(conn, bronze_dir)
        c2 = conn.execute(
            "SELECT COUNT(*) FROM anime_studios WHERE source = 'anilist'"
        ).fetchone()[0]
        conn.close()
        assert c1 == c2


# ---------------------------------------------------------------------------
# MAL loader: kind='studio' fix
# ---------------------------------------------------------------------------

class TestMalStudiosKindFix:
    """Verify the is_main fix: kind='studio' (real-world) → is_main=1."""

    @pytest.fixture
    def bronze_dir(self, tmp_path: Path) -> Path:
        root = tmp_path / "bronze"

        with BronzeWriter("mal", table="anime", root=root) as bw:
            bw.append({
                "mal_id": 100,
                "title": "Test",
                "title_english": "Test",
                "title_japanese": "テスト",
                "type": "TV",
                "source": "Original",
                "episodes": 12,
                "status": "Finished Airing",
                "airing": False,
                "aired_from": "2020-01-01",
                "aired_to": "2020-03-31",
                "season": "winter",
                "year": 2020,
                "display_score": 7.5,
                "display_scored_by": 10000,
                "display_rank": 500,
                "display_popularity": 100,
                "display_members": 50000,
                "display_favorites": 1000,
            })

        # Use lowercase 'studio' — the real-world MAL value
        with BronzeWriter("mal", table="anime_studios", root=root) as bw:
            bw.append({
                "mal_id": 100,
                "mal_producer_id": 14,
                "name": "Madhouse",
                "kind": "studio",
                "url": "https://myanimelist.net/anime/producer/14",
            })
            # producer kind — should NOT appear in anime_studios
            bw.append({
                "mal_id": 100,
                "mal_producer_id": 99,
                "name": "Some Distributor",
                "kind": "producer",
                "url": "https://myanimelist.net/anime/producer/99",
            })

        # Minimal empty tables to avoid glob errors
        for tbl in ["persons", "staff_credits", "anime_characters", "va_credits",
                    "anime_genres", "anime_relations", "anime_recommendations"]:
            with BronzeWriter("mal", table=tbl, root=root) as bw:
                pass

        return root

    def test_studio_kind_is_main(self, bronze_dir: Path) -> None:
        """kind='studio' (real-world lowercase) → is_main=1."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT is_main FROM anime_studios "
            "WHERE anime_id = 'mal:a100' AND studio_id = 'mal:n:Madhouse'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 1

    def test_producer_kind_excluded(self, bronze_dir: Path) -> None:
        """kind='producer' rows are excluded from anime_studios."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT 1 FROM anime_studios WHERE studio_id = 'mal:n:Some Distributor'"
        ).fetchone()
        conn.close()
        assert row is None

    def test_studios_uppercase_still_works(self, tmp_path: Path) -> None:
        """kind='Studios' (legacy test fixture) still maps to is_main=1 (backward compat)."""
        root = tmp_path / "bronze_legacy"

        with BronzeWriter("mal", table="anime", root=root) as bw:
            bw.append({
                "mal_id": 200,
                "title": "Legacy",
                "title_english": "Legacy",
                "title_japanese": "レガシー",
                "type": "TV",
                "source": "Original",
                "episodes": 1,
                "status": "Finished Airing",
                "airing": False,
                "aired_from": "2019-01-01",
                "aired_to": "2019-01-01",
                "season": "winter",
                "year": 2019,
                "display_score": 6.0,
                "display_scored_by": 1000,
                "display_rank": 9999,
                "display_popularity": 9999,
                "display_members": 5000,
                "display_favorites": 100,
            })

        with BronzeWriter("mal", table="anime_studios", root=root) as bw:
            bw.append({
                "mal_id": 200,
                "mal_producer_id": 55,
                "name": "OldStudio",
                "kind": "Studios",  # capital S — legacy format
                "url": "",
            })

        for tbl in ["persons", "staff_credits", "anime_characters", "va_credits",
                    "anime_genres", "anime_relations", "anime_recommendations"]:
            with BronzeWriter("mal", table=tbl, root=root) as bw:
                pass

        conn = _make_silver_conn()
        mal_loader.integrate(conn, root)
        row = conn.execute(
            "SELECT is_main FROM anime_studios WHERE studio_id = 'mal:n:OldStudio'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 1  # backward compat: 'Studios' also counts as main


# ---------------------------------------------------------------------------
# ANN loader: anime_studios from Animation Production companies
# ---------------------------------------------------------------------------

class TestAnnAnimationProduction:
    """Verify ANN company task='Animation Production' → anime_studios."""

    @pytest.fixture
    def bronze_dir(self, tmp_path: Path) -> Path:
        root = tmp_path / "bronze"

        with BronzeWriter("ann", table="anime", root=root) as bw:
            bw.append({
                "ann_id": 5001,
                "title": "Test Anime ANN",
                "year": 2022,
                "vintage_raw": "2022",
                "running_time_raw": None,
                "type": "TV",
                "url": "https://www.animenewsnetwork.com/encyclopedia/anime.php?id=5001",
                "image_url": None,
                "plot_summary": None,
                "themes": None,
                "objectionable_content": None,
                "opening_themes_json": None,
                "ending_themes_json": None,
                "insert_songs_json": None,
                "official_websites_json": None,
                "display_rating_votes": 500,
                "display_rating_weighted": 7.8,
                "display_rating_bayesian": 7.5,
            })

        with BronzeWriter("ann", table="company", root=root) as bw:
            # Animation Production → should appear in anime_studios
            bw.append({
                "ann_anime_id": 5001,
                "company_name": "Studio Deen",
                "task": "Animation Production",
                "company_id": 35,
            })
            # Production only — should NOT appear in anime_studios
            bw.append({
                "ann_anime_id": 5001,
                "company_name": "Bandai Namco",
                "task": "Production",
                "company_id": 77,
            })

        # Write empty bronze tables for unused operations
        for tbl in ["persons", "episodes", "releases", "news", "related", "cast"]:
            with BronzeWriter("ann", table=tbl, root=root) as bw:
                pass

        return root

    def test_animation_production_in_anime_studios(self, bronze_dir: Path) -> None:
        """ANN company with task='Animation Production' is inserted into anime_studios."""
        conn = _make_silver_conn()
        # Pre-insert anime row (ANN loader does UPDATE, not INSERT for anime)
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["ann:a5001", "テスト", "Test Anime ANN"],
        )
        counts = ann_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts.get("anime_studios_ann", 0) >= 1

    def test_production_task_excluded(self, bronze_dir: Path) -> None:
        """task='Production' companies do NOT appear in anime_studios."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["ann:a5001", "テスト", "Test Anime ANN"],
        )
        ann_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT 1 FROM anime_studios WHERE studio_id = 'ann:n:Bandai Namco'"
        ).fetchone()
        conn.close()
        assert row is None

    def test_studio_stub_inserted(self, bronze_dir: Path) -> None:
        """Studio entry is added to studios table with ann:n: prefix."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["ann:a5001", "テスト", "Test Anime ANN"],
        )
        ann_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT name FROM studios WHERE id = 'ann:n:Studio Deen'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "Studio Deen"

    def test_is_main_set(self, bronze_dir: Path) -> None:
        """ANN Animation Production rows have is_main=1."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["ann:a5001", "テスト", "Test Anime ANN"],
        )
        ann_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT is_main FROM anime_studios "
            "WHERE anime_id = 'ann:a5001' AND studio_id = 'ann:n:Studio Deen'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 1

    def test_source_column_is_ann(self, bronze_dir: Path) -> None:
        """anime_studios rows from ANN have source='ann' (H4)."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["ann:a5001", "テスト", "Test Anime ANN"],
        )
        ann_loader.integrate(conn, bronze_dir)
        rows = conn.execute(
            "SELECT DISTINCT source FROM anime_studios WHERE anime_id = 'ann:a5001'"
        ).fetchall()
        conn.close()
        assert len(rows) > 0
        assert all(r[0] == "ann" for r in rows)

    def test_idempotent(self, bronze_dir: Path) -> None:
        """Running integrate() twice does not duplicate anime_studios rows."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
            ["ann:a5001", "テスト", "Test Anime ANN"],
        )
        ann_loader.integrate(conn, bronze_dir)
        c1 = conn.execute(
            "SELECT COUNT(*) FROM anime_studios WHERE source = 'ann'"
        ).fetchone()[0]
        ann_loader.integrate(conn, bronze_dir)
        c2 = conn.execute(
            "SELECT COUNT(*) FROM anime_studios WHERE source = 'ann'"
        ).fetchone()[0]
        conn.close()
        assert c1 == c2


# ---------------------------------------------------------------------------
# Mediaarts loader: production_companies → anime_studios
# ---------------------------------------------------------------------------

class TestMadbAnimationStudios:
    """Verify mediaarts production_companies → anime_studios."""

    @pytest.fixture
    def bronze_root(self, tmp_path: Path) -> Path:
        root = tmp_path

        def _write(table_name: str, schema: pa.Schema, data: dict) -> None:
            date_dir = root / "source=mediaarts" / f"table={table_name}" / "date=2026-04-27"
            date_dir.mkdir(parents=True, exist_ok=True)
            arrays = [pa.array(v, type=schema.field(k).type) for k, v in data.items()]
            tbl = pa.Table.from_arrays(arrays, schema=schema)
            pq.write_table(tbl, date_dir / "data.parquet")

        # Minimal tables required by the loader
        _write(
            "broadcasters",
            pa.schema([pa.field("madb_id", pa.string()), pa.field("name", pa.string()),
                       pa.field("is_network_station", pa.bool_())]),
            {"madb_id": [], "name": [], "is_network_station": []},
        )
        _write(
            "broadcast_schedule",
            pa.schema([pa.field("madb_id", pa.string()), pa.field("raw_text", pa.string())]),
            {"madb_id": [], "raw_text": []},
        )
        _write(
            "production_committee",
            pa.schema([pa.field("madb_id", pa.string()), pa.field("company_name", pa.string()),
                       pa.field("role_label", pa.string())]),
            {"madb_id": [], "company_name": [], "role_label": []},
        )

        # production_companies: アニメーション制作 and plain is_main
        _write(
            "production_companies",
            pa.schema([
                pa.field("madb_id", pa.string()),
                pa.field("company_name", pa.string()),
                pa.field("role_label", pa.string()),
                pa.field("is_main", pa.bool_()),
            ]),
            {
                "madb_id":      ["C3001", "C3001", "C3001"],
                "company_name": ["Trigger", "KlockWorx", "Aniplex"],
                "role_label":   ["アニメーション制作", "製作", "製作"],
                "is_main":      [True, False, True],
            },
        )
        _write(
            "video_releases",
            pa.schema([pa.field("madb_id", pa.string()), pa.field("series_madb_id", pa.string()),
                       pa.field("media_format", pa.string()), pa.field("date_published", pa.string()),
                       pa.field("publisher", pa.string()), pa.field("product_id", pa.string()),
                       pa.field("gtin", pa.string()), pa.field("runtime_min", pa.int64()),
                       pa.field("volume_number", pa.string()), pa.field("release_title", pa.string())]),
            {"madb_id": [], "series_madb_id": [], "media_format": [], "date_published": [],
             "publisher": [], "product_id": [], "gtin": [], "runtime_min": [],
             "volume_number": [], "release_title": []},
        )
        _write(
            "original_work_links",
            pa.schema([pa.field("madb_id", pa.string()), pa.field("work_name", pa.string()),
                       pa.field("creator_text", pa.string()), pa.field("series_link_id", pa.string())]),
            {"madb_id": [], "work_name": [], "creator_text": [], "series_link_id": []},
        )
        return root

    def test_animation_studio_in_anime_studios(
        self, bronze_root: Path
    ) -> None:
        """アニメーション制作 company → anime_studios."""
        conn = _make_silver_conn()
        # Pre-insert the anime
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES ('madb:C3001', 'テスト', 'Test')"
        )
        counts = madb_loader.integrate(conn, bronze_root)
        conn.close()
        assert counts.get("anime_studios_mediaarts", 0) >= 1

    def test_animation_studio_id_format(
        self, bronze_root: Path
    ) -> None:
        """Studio ID uses 'madb:n:' prefix; anime_id uses 'madb:C...' format."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES ('madb:C3001', 'テスト', 'Test')"
        )
        madb_loader.integrate(conn, bronze_root)
        row = conn.execute(
            "SELECT anime_id, studio_id, is_main FROM anime_studios "
            "WHERE anime_id = 'madb:C3001' AND studio_id = 'madb:n:Trigger'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[2] == 1  # is_main because role_label = アニメーション制作

    def test_producer_is_main_true_also_included(
        self, bronze_root: Path
    ) -> None:
        """is_main=True rows (even without アニメーション制作 role) are included."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES ('madb:C3001', 'テスト', 'Test')"
        )
        madb_loader.integrate(conn, bronze_root)
        # Aniplex has is_main=True but role_label='製作', not 'アニメーション制作'
        row = conn.execute(
            "SELECT 1 FROM anime_studios WHERE anime_id = 'madb:C3001' AND studio_id = 'madb:n:Aniplex'"
        ).fetchone()
        conn.close()
        assert row is not None

    def test_non_main_production_excluded(
        self, bronze_root: Path
    ) -> None:
        """is_main=False + role='製作' rows are excluded from anime_studios."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES ('madb:C3001', 'テスト', 'Test')"
        )
        madb_loader.integrate(conn, bronze_root)
        row = conn.execute(
            "SELECT 1 FROM anime_studios WHERE studio_id = 'madb:n:KlockWorx'"
        ).fetchone()
        conn.close()
        assert row is None

    def test_source_is_mediaarts(self, bronze_root: Path) -> None:
        """H4: source column is 'mediaarts' for all madb anime_studios rows."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES ('madb:C3001', 'テスト', 'Test')"
        )
        madb_loader.integrate(conn, bronze_root)
        rows = conn.execute(
            "SELECT DISTINCT source FROM anime_studios WHERE anime_id = 'madb:C3001'"
        ).fetchall()
        conn.close()
        assert len(rows) > 0
        assert all(r[0] == "mediaarts" for r in rows)

    def test_idempotent(self, bronze_root: Path) -> None:
        """Running integrate() twice does not duplicate anime_studios rows."""
        conn = _make_silver_conn()
        conn.execute(
            "INSERT INTO anime (id, title_ja, title_en) VALUES ('madb:C3001', 'テスト', 'Test')"
        )
        madb_loader.integrate(conn, bronze_root)
        c1 = conn.execute(
            "SELECT COUNT(*) FROM anime_studios WHERE source = 'mediaarts'"
        ).fetchone()[0]
        madb_loader.integrate(conn, bronze_root)
        c2 = conn.execute(
            "SELECT COUNT(*) FROM anime_studios WHERE source = 'mediaarts'"
        ).fetchone()[0]
        conn.close()
        assert c1 == c2


# ---------------------------------------------------------------------------
# measure() integration test (synthetic silver + bronze)
# ---------------------------------------------------------------------------

class TestMeasure:
    """measure() returns CoverageRow for all expected sources."""

    def test_returns_all_sources(self, tmp_path: Path) -> None:
        silver_path = tmp_path / "silver.duckdb"
        bronze_root = tmp_path / "bronze"

        # Create minimal silver DB
        conn = duckdb.connect(str(silver_path))
        conn.execute(_SILVER_DDL)
        conn.execute(
            "INSERT INTO anime_studios (anime_id, studio_id, is_main, role, source) "
            "VALUES ('mal:a1', 'mal:n:Sunrise', 1, '', 'mal')"
        )
        conn.close()

        rows = measure(silver_path, bronze_root)

        sources = {r.source for r in rows}
        assert "anilist" in sources
        assert "mal" in sources
        assert "ann" in sources
        assert "mediaarts" in sources
        assert "seesaawiki" in sources
        assert "keyframe" in sources
        assert "bangumi" in sources

    def test_mal_silver_counted(self, tmp_path: Path) -> None:
        """measure() correctly reads SILVER anime_studios count for MAL."""
        silver_path = tmp_path / "silver.duckdb"
        bronze_root = tmp_path / "bronze"

        conn = duckdb.connect(str(silver_path))
        conn.execute(_SILVER_DDL)
        conn.execute(
            "INSERT INTO anime_studios (anime_id, studio_id, is_main, role, source) "
            "VALUES ('mal:a1', 'mal:n:Sunrise', 1, '', 'mal')"
        )
        conn.execute(
            "INSERT INTO anime_studios (anime_id, studio_id, is_main, role, source) "
            "VALUES ('mal:a2', 'mal:n:MAPPA', 1, '', 'mal')"
        )
        conn.close()

        rows = measure(silver_path, bronze_root)
        mal_row = next(r for r in rows if r.source == "mal")
        assert mal_row.silver_in_anime_studios == 2
