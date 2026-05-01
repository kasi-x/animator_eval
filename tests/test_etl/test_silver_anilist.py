"""Tests for src/etl/silver_loaders/anilist.py.

Creates minimal synthetic BRONZE parquet in a temp dir, builds a minimal
SILVER duckdb (mirroring the DDL from integrate_duckdb._DDL), then calls
integrate() and checks row counts and H1 invariants.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

import duckdb
import pytest

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.silver_loaders import anilist as anilist_loader

# ─── Minimal SILVER DDL (subset of integrate_duckdb._DDL) ─────────────────

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
"""


def _make_silver_conn() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB with the minimal SILVER anime table."""
    conn = duckdb.connect(":memory:")
    conn.execute(_SILVER_DDL)
    return conn


def _insert_anime(conn: duckdb.DuckDBPyConnection, anime_id: str = "anilist:1") -> None:
    conn.execute(
        "INSERT INTO anime (id, title_ja, title_en) VALUES (?, ?, ?)",
        [anime_id, "テストアニメ", "Test Anime"],
    )


# ─── BRONZE fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    """Write minimal valid BRONZE parquet for characters, CVA, and anime."""
    root = tmp_path / "bronze"

    with BronzeWriter("anilist", table="characters", root=root) as bw:
        bw.append({
            "id": "anilist:c1",
            "name_ja": "テストキャラ",
            "name_en": "Test Char",
            "aliases": ["TC"],
            "anilist_id": 999,
            "image_large": "https://example.com/large.png",
            "image_medium": "https://example.com/medium.png",
            "description": "A test character.",
            "gender": "Female",
            "date_of_birth": None,
            "age": None,
            "blood_type": None,
            "favourites": 42,
            "site_url": "https://anilist.co/character/999",
            "display_name": "テストキャラ",
        })

    with BronzeWriter("anilist", table="character_voice_actors", root=root) as bw:
        bw.append({
            "character_id": "anilist:c1",
            "person_id":    "anilist:p1",
            "anime_id":     "anilist:1",
            "character_role": "MAIN",
            "source": "anilist",
        })

    with BronzeWriter("anilist", table="anime", root=root) as bw:
        bw.append({
            "id": "anilist:1",
            "title_ja": "テストアニメ",
            "title_en": "Test Anime",
            "year": 2024,
            "season": "WINTER",
            "quarter": 1,
            "episodes": 12,
            "format": "TV",
            "duration": 24,
            "start_date": "2024-01-01",
            "end_date": "2024-03-25",
            "status": "FINISHED",
            "original_work_type": "MANGA",
            "source": "MANGA",
            "work_type": None,
            "scale_class": None,
            "score": 7.5,
            "mean_score": 74.0,
            "favourites": 1234,
            "popularity_rank": 500,
            "rankings_json": "[]",
            "synonyms": '["Test"]',
            "country_of_origin": "JP",
            "is_licensed": 1,
            "is_adult": 0,
            "hashtag": "#TestAnime",
            "site_url": "https://anilist.co/anime/1",
            "trailer_url": "https://youtube.com/v/abc",
            "trailer_site": "youtube",
            "description": "A test anime.",
            "cover_large": "https://example.com/cover_large.jpg",
            "cover_extra_large": "https://example.com/cover_xl.jpg",
            "cover_medium": "https://example.com/cover_medium.jpg",
            "banner": "https://example.com/banner.jpg",
            "external_links_json": "[]",
            "airing_schedule_json": "[]",
            "relations_json": "[]",
            "fetched_at": "2024-04-24T12:00:00",
            "content_hash": "abc123",
        })

    return root


# ─── Tests ───────────────────────────────────────────────────────────────────

def test_characters_loaded(bronze_dir: Path) -> None:
    """integrate() inserts characters from BRONZE."""
    conn = _make_silver_conn()
    counts = anilist_loader.integrate(conn, bronze_dir)
    conn.close()

    assert counts["characters"] == 1


def test_character_voice_actors_loaded(bronze_dir: Path) -> None:
    """integrate() inserts character_voice_actors from BRONZE."""
    conn = _make_silver_conn()
    counts = anilist_loader.integrate(conn, bronze_dir)
    conn.close()

    assert counts["character_voice_actors"] == 1


def test_anime_extras_updated(bronze_dir: Path) -> None:
    """integrate() UPDATE sets description on existing anime rows."""
    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:1")

    counts = anilist_loader.integrate(conn, bronze_dir)
    conn.close()

    assert counts["anime_extras_updated"] >= 1


def test_anime_display_columns_populated(bronze_dir: Path) -> None:
    """H1: display_* columns are set; bare score/favourites columns absent."""
    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:1")
    anilist_loader.integrate(conn, bronze_dir)

    row = conn.execute(
        "SELECT display_score, display_mean_score, display_favourites, "
        "display_popularity_rank FROM anime WHERE id='anilist:1'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == pytest.approx(7.5)     # display_score
    assert row[1] == pytest.approx(74.0)    # display_mean_score
    assert row[2] == 1234                   # display_favourites
    assert row[3] == 500                    # display_popularity_rank


def test_h1_no_bare_score_column(bronze_dir: Path) -> None:
    """H1: SILVER anime table must NOT have bare 'score', 'popularity', 'favourites' columns."""
    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:1")
    anilist_loader.integrate(conn, bronze_dir)

    cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
    conn.close()

    assert "score" not in cols
    assert "popularity" not in cols
    assert "favourites" not in cols
    assert "mean_score" not in cols
    assert "popularity_rank" not in cols


def test_characters_idempotent(bronze_dir: Path) -> None:
    """Calling integrate() twice does not duplicate characters rows."""
    conn = _make_silver_conn()
    anilist_loader.integrate(conn, bronze_dir)
    anilist_loader.integrate(conn, bronze_dir)
    count = conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]
    conn.close()

    assert count == 1


def test_cva_idempotent(bronze_dir: Path) -> None:
    """Calling integrate() twice does not duplicate CVA rows."""
    conn = _make_silver_conn()
    anilist_loader.integrate(conn, bronze_dir)
    anilist_loader.integrate(conn, bronze_dir)
    count = conn.execute("SELECT COUNT(*) FROM character_voice_actors").fetchone()[0]
    conn.close()

    assert count == 1


def _full_character_row(char_id: str, name_ja: str, anilist_id: int) -> dict:
    """Return a full character dict with all expected BRONZE columns."""
    return {
        "id": char_id,
        "name_ja": name_ja,
        "name_en": name_ja + " EN",
        "aliases": [],
        "anilist_id": anilist_id,
        "image_large": None,
        "image_medium": None,
        "description": None,
        "gender": None,
        "date_of_birth": None,
        "age": None,
        "blood_type": None,
        "favourites": 0,
        "site_url": None,
        "display_name": name_ja,
    }


def test_characters_dedup_latest_date(tmp_path: Path) -> None:
    """When the same character id appears in two date partitions, keep the newest."""
    root = tmp_path / "bronze"

    with BronzeWriter("anilist", table="characters", root=root, date=_dt.date(2026, 4, 22)) as bw:
        bw.append(_full_character_row("anilist:c99", "OLD_NAME", 99))

    with BronzeWriter("anilist", table="characters", root=root, date=_dt.date(2026, 4, 23)) as bw:
        bw.append(_full_character_row("anilist:c99", "NEW_NAME", 99))

    # Provide empty CVA and anime parquets so the loader doesn't error
    with BronzeWriter("anilist", table="character_voice_actors", root=root) as bw:
        pass
    with BronzeWriter("anilist", table="anime", root=root) as bw:
        pass

    conn = _make_silver_conn()
    anilist_loader.integrate(conn, root)
    row = conn.execute("SELECT name_ja FROM characters WHERE id='anilist:c99'").fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "NEW_NAME"


def test_structural_columns_populated(bronze_dir: Path) -> None:
    """Structural columns (synonyms, country_of_origin, site_url, etc.) are set."""
    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:1")
    anilist_loader.integrate(conn, bronze_dir)

    row = conn.execute(
        "SELECT synonyms, country_of_origin, is_adult, hashtag, site_url "
        "FROM anime WHERE id='anilist:1'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[1] == "JP"      # country_of_origin
    assert row[2] == 0         # is_adult
    assert row[3] == "#TestAnime"  # hashtag
    assert "anilist.co" in row[4]  # site_url


def test_extras_external_links_json_populated(bronze_dir: Path) -> None:
    """external_links_json is copied from BRONZE to SILVER."""
    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:1")
    anilist_loader.integrate(conn, bronze_dir)

    row = conn.execute(
        "SELECT external_links_json FROM anime WHERE id='anilist:1'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] is not None, "external_links_json must not be NULL after extras update"


def test_extras_airing_schedule_json_populated(bronze_dir: Path) -> None:
    """airing_schedule_json is copied from BRONZE to SILVER."""
    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:1")
    anilist_loader.integrate(conn, bronze_dir)

    row = conn.execute(
        "SELECT airing_schedule_json FROM anime WHERE id='anilist:1'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] is not None, "airing_schedule_json must not be NULL after extras update"


def test_extras_trailer_columns_populated(bronze_dir: Path) -> None:
    """trailer_url and trailer_site are copied from BRONZE to SILVER."""
    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:1")
    anilist_loader.integrate(conn, bronze_dir)

    row = conn.execute(
        "SELECT trailer_url, trailer_site FROM anime WHERE id='anilist:1'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "https://youtube.com/v/abc"  # trailer_url
    assert row[1] == "youtube"                    # trailer_site


def test_extras_display_rankings_json_populated(bronze_dir: Path) -> None:
    """H1: display_rankings_json (not bare rankings_json) is set from BRONZE rankings_json."""
    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:1")
    anilist_loader.integrate(conn, bronze_dir)

    row = conn.execute(
        "SELECT display_rankings_json FROM anime WHERE id='anilist:1'"
    ).fetchone()
    # Verify bare rankings_json column does not exist in SILVER
    cols = {r[1] for r in conn.execute("PRAGMA table_info('anime')").fetchall()}
    conn.close()

    assert row is not None
    assert row[0] is not None, "display_rankings_json must not be NULL after extras update"
    assert "rankings_json" not in cols, "bare rankings_json must not exist in SILVER (H1)"
