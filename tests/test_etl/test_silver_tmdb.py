"""Tests for src/etl/conformed_loaders/tmdb.py.

Creates minimal synthetic BRONZE parquet in a temp dir, builds a minimal
SILVER duckdb in-memory, then calls integrate() and checks row counts
and H1 invariants.

H1 invariant: SILVER columns must NOT contain bare vote_average / popularity /
vote_count — only display_*_tmdb prefixed variants.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.conformed_loaders import tmdb as tmdb_loader

# ─── Minimal SILVER DDL ───────────────────────────────────────────────────────

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
    names_alt        VARCHAR NOT NULL DEFAULT '{}',
    birth_date       VARCHAR,
    death_date       VARCHAR,
    website_url      VARCHAR,
    gender           VARCHAR,
    description      TEXT,
    image_large      VARCHAR,
    image_medium     VARCHAR,
    hometown         VARCHAR,
    blood_type       VARCHAR,
    updated_at       TIMESTAMP DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS seq_credits_id;
CREATE TABLE IF NOT EXISTS credits (
    id              INTEGER PRIMARY KEY DEFAULT nextval('seq_credits_id'),
    person_id       VARCHAR,
    anime_id        VARCHAR,
    role            VARCHAR NOT NULL DEFAULT '',
    raw_role        VARCHAR NOT NULL DEFAULT '',
    episode         INTEGER,
    evidence_source VARCHAR NOT NULL DEFAULT '',
    credit_year     INTEGER,
    credit_quarter  INTEGER,
    affiliation     VARCHAR,
    position        INTEGER,
    updated_at      TIMESTAMP DEFAULT now()
);
"""


def _make_silver_conn() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB with minimal SILVER tables."""
    conn = duckdb.connect(":memory:")
    conn.execute(_SILVER_DDL)
    return conn


# ─── BRONZE fixtures ──────────────────────────────────────────────────────────

@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    """Write synthetic TMDb BRONZE parquet for anime, persons, and credits."""
    root = tmp_path / "bronze"

    # anime — two rows: one tv, one movie
    with BronzeWriter("tmdb", table="anime", root=root) as bw:
        bw.append({
            "tmdb_id": 10001,
            "media_type": "tv",
            "title": "Spirited Away TV",
            "original_title": "千と千尋の神隠し TV",
            "original_lang": "ja",
            "origin_countries": '["JP"]',
            "spoken_languages": '["ja"]',
            "year": 2002,
            "first_air_date": "2002-07-20",
            "last_air_date": "2002-07-20",
            "release_date": None,
            "episodes": 1,
            "seasons": 1,
            "runtime": 125,
            "status": "Ended",
            "type": "Scripted",
            "in_production": 0,
            "adult": 0,
            "genres": '["Animation", "Family"]',
            "production_companies": '[]',
            "production_countries": '[]',
            "networks": '[]',
            "created_by": '[]',
            "belongs_to_collection": None,
            "overview": "A classic.",
            "tagline": None,
            "homepage": None,
            "poster_path": "/abc.jpg",
            "backdrop_path": None,
            "imdb_id": "tt0245429",
            "tvdb_id": 98765,
            "wikidata_id": None,
            "facebook_id": None,
            "instagram_id": None,
            "twitter_id": None,
            "display_vote_avg": 8.5,
            "display_vote_count": 12000,
            "display_popularity": 120.5,
            "display_budget": None,
            "display_revenue": None,
            "keywords": '[]',
            "alternative_titles": '[{"iso_3166_1":"JP","title":"千と千尋","type":""}]',
            "translations": '[{"iso_3166_1":"US","iso_639_1":"en","name":"Spirited Away"}]',
            "release_dates": '[]',
            "content_ratings": '[]',
            "videos": '[]',
            "images": '{}',
            "watch_providers": '{}',
            "recommendation_ids": '[]',
            "fetched_at": "2026-05-02T00:00:00",
            "content_hash": "hash_tv_10001",
        })
        bw.append({
            "tmdb_id": 20002,
            "media_type": "movie",
            "title": "My Neighbor Totoro",
            "original_title": "となりのトトロ",
            "original_lang": "ja",
            "origin_countries": '["JP"]',
            "spoken_languages": '["ja"]',
            "year": 1988,
            "first_air_date": None,
            "last_air_date": None,
            "release_date": "1988-04-16",
            "episodes": None,
            "seasons": None,
            "runtime": 86,
            "status": "Released",
            "type": None,
            "in_production": None,
            "adult": 0,
            "genres": '["Animation", "Family"]',
            "production_companies": '[]',
            "production_countries": '[]',
            "networks": '[]',
            "created_by": '[]',
            "belongs_to_collection": None,
            "overview": "Another classic.",
            "tagline": None,
            "homepage": None,
            "poster_path": "/def.jpg",
            "backdrop_path": None,
            "imdb_id": "tt0096283",
            "tvdb_id": None,
            "wikidata_id": None,
            "facebook_id": None,
            "instagram_id": None,
            "twitter_id": None,
            "display_vote_avg": 8.7,
            "display_vote_count": 9500,
            "display_popularity": 85.3,
            "display_budget": 0,
            "display_revenue": 0,
            "keywords": '[]',
            "alternative_titles": '[]',
            "translations": '[]',
            "release_dates": '[]',
            "content_ratings": '[]',
            "videos": '[]',
            "images": '{}',
            "watch_providers": '{}',
            "recommendation_ids": '[]',
            "fetched_at": "2026-05-02T00:00:00",
            "content_hash": "hash_movie_20002",
        })

    # persons — two rows
    with BronzeWriter("tmdb", table="persons", root=root) as bw:
        bw.append({
            "tmdb_id": 5001,
            "name": "Hayao Miyazaki",
            "also_known_as": '["宮崎 駿"]',
            "gender": 2,
            "birthday": "1941-01-05",
            "deathday": None,
            "place_of_birth": "Tokyo, Japan",
            "biography": "Legendary animator.",
            "known_for_dept": "Directing",
            "profile_path": "/miyazaki.jpg",
            "homepage": None,
            "adult": 0,
            "imdb_id": "nm0594503",
            "facebook_id": None,
            "instagram_id": None,
            "twitter_id": None,
            "tiktok_id": None,
            "youtube_id": None,
            "wikidata_id": "Q55400",
            "images": '[]',
            "display_popularity": 15.2,
        })
        bw.append({
            "tmdb_id": 5002,
            "name": "Joe Hisaishi",
            "also_known_as": '["久石 譲"]',
            "gender": 2,
            "birthday": "1950-12-06",
            "deathday": None,
            "place_of_birth": "Nagano, Japan",
            "biography": "Composer.",
            "known_for_dept": "Sound",
            "profile_path": None,
            "homepage": None,
            "adult": 0,
            "imdb_id": "nm0386810",
            "facebook_id": None,
            "instagram_id": None,
            "twitter_id": None,
            "tiktok_id": None,
            "youtube_id": None,
            "wikidata_id": None,
            "images": '[]',
            "display_popularity": 8.1,
        })

    # credits — three rows (crew)
    with BronzeWriter("tmdb", table="credits", root=root) as bw:
        bw.append({
            "tmdb_anime_id": 10001,
            "media_type": "tv",
            "tmdb_person_id": 5001,
            "credit_type": "crew",
            "role": "director",
            "role_raw": "Director",
            "character": None,
            "department": "Directing",
            "job": "Director",
            "episode_count": 1,
        })
        bw.append({
            "tmdb_anime_id": 10001,
            "media_type": "tv",
            "tmdb_person_id": 5002,
            "credit_type": "crew",
            "role": "music",
            "role_raw": "Music",
            "character": None,
            "department": "Sound",
            "job": "Music",
            "episode_count": 1,
        })
        bw.append({
            "tmdb_anime_id": 20002,
            "media_type": "movie",
            "tmdb_person_id": 5001,
            "credit_type": "crew",
            "role": "director",
            "role_raw": "Director",
            "character": None,
            "department": "Directing",
            "job": "Director",
            "episode_count": None,
        })

    return root


@pytest.fixture
def bronze_dir_anime_only(tmp_path: Path) -> Path:
    """BRONZE fixture with anime only — no persons or credits."""
    root = tmp_path / "bronze_anime_only"
    with BronzeWriter("tmdb", table="anime", root=root) as bw:
        bw.append({
            "tmdb_id": 30003,
            "media_type": "tv",
            "title": "Test Anime",
            "original_title": "テスト",
            "original_lang": "ja",
            "origin_countries": '["JP"]',
            "spoken_languages": '["ja"]',
            "year": 2020,
            "first_air_date": "2020-01-01",
            "last_air_date": None,
            "release_date": None,
            "episodes": 12,
            "seasons": 1,
            "runtime": 24,
            "status": "Ended",
            "type": "Scripted",
            "in_production": 0,
            "adult": 0,
            "genres": '["Animation"]',
            "production_companies": '[]',
            "production_countries": '[]',
            "networks": '[]',
            "created_by": '[]',
            "belongs_to_collection": None,
            "overview": None,
            "tagline": None,
            "homepage": None,
            "poster_path": None,
            "backdrop_path": None,
            "imdb_id": None,
            "tvdb_id": None,
            "wikidata_id": None,
            "facebook_id": None,
            "instagram_id": None,
            "twitter_id": None,
            "display_vote_avg": 7.0,
            "display_vote_count": 100,
            "display_popularity": 10.0,
            "display_budget": None,
            "display_revenue": None,
            "keywords": '[]',
            "alternative_titles": '[]',
            "translations": '[]',
            "release_dates": '[]',
            "content_ratings": '[]',
            "videos": '[]',
            "images": '{}',
            "watch_providers": '{}',
            "recommendation_ids": '[]',
            "fetched_at": "2026-05-02T00:00:00",
            "content_hash": "hash_tv_30003",
        })
    return root


# ─── Tests ───────────────────────────────────────────────────────────────────

class TestAnime:
    def test_anime_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts anime rows with tmdb: prefix."""
        conn = _make_silver_conn()
        counts = tmdb_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["anime_inserted"] >= 2

    def test_anime_tv_id_prefix(self, bronze_dir: Path) -> None:
        """TV anime rows use 'tmdb:tv:<id>' format."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT id, title_en FROM anime WHERE id = 'tmdb:tv:10001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert "Spirited" in row[1]

    def test_anime_movie_id_prefix(self, bronze_dir: Path) -> None:
        """Movie anime rows use 'tmdb:movie:<id>' format."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT id, title_en FROM anime WHERE id = 'tmdb:movie:20002'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert "Totoro" in row[1]

    def test_anime_imdb_id_populated(self, bronze_dir: Path) -> None:
        """imdb_id column is populated from TMDb BRONZE."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT imdb_id FROM anime WHERE id = 'tmdb:tv:10001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "tt0245429"

    def test_anime_tvdb_id_populated(self, bronze_dir: Path) -> None:
        """tvdb_id column is populated from TMDb BRONZE."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT tvdb_id FROM anime WHERE id = 'tmdb:tv:10001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 98765

    def test_anime_alternative_titles_json(self, bronze_dir: Path) -> None:
        """alternative_titles_json column is populated for anime with alt titles."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT alternative_titles_json FROM anime WHERE id = 'tmdb:tv:10001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] is not None
        assert "千と千尋" in row[0]

    def test_anime_translations_json(self, bronze_dir: Path) -> None:
        """translations_json column is populated for anime with translations."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT translations_json FROM anime WHERE id = 'tmdb:tv:10001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] is not None
        assert "Spirited Away" in row[0]

    def test_anime_display_columns_populated(self, bronze_dir: Path) -> None:
        """display_*_tmdb columns are populated from BRONZE."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT display_vote_avg_tmdb, display_vote_count_tmdb, display_popularity_tmdb "
            "FROM anime WHERE id = 'tmdb:tv:10001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == pytest.approx(8.5)   # display_vote_avg_tmdb
        assert row[1] == 12000                 # display_vote_count_tmdb
        assert row[2] == pytest.approx(120.5)  # display_popularity_tmdb

    def test_anime_idempotent(self, bronze_dir: Path) -> None:
        """integrate() twice does not duplicate anime rows."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        tmdb_loader.integrate(conn, bronze_dir)
        count = conn.execute(
            "SELECT COUNT(*) FROM anime WHERE id LIKE 'tmdb:%'"
        ).fetchone()[0]
        conn.close()
        assert count == 2

    def test_anime_only_no_error(self, bronze_dir_anime_only: Path) -> None:
        """integrate() does not error when persons and credits BRONZE are absent."""
        conn = _make_silver_conn()
        counts = tmdb_loader.integrate(conn, bronze_dir_anime_only)
        conn.close()
        assert counts["anime_inserted"] >= 1
        assert counts["persons_inserted"] == 0
        assert counts["credits_inserted"] == 0


class TestPersons:
    def test_persons_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts persons with tmdb:p prefix."""
        conn = _make_silver_conn()
        counts = tmdb_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["persons_inserted"] >= 2

    def test_persons_id_prefix(self, bronze_dir: Path) -> None:
        """Person IDs use 'tmdb:p<id>' format."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT id, name_en FROM persons WHERE id = 'tmdb:p5001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert "Miyazaki" in row[1]

    def test_persons_tmdb_id_column(self, bronze_dir: Path) -> None:
        """tmdb_id INTEGER extension column is populated."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT tmdb_id FROM persons WHERE id = 'tmdb:p5001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 5001

    def test_persons_display_popularity_tmdb(self, bronze_dir: Path) -> None:
        """display_popularity_tmdb column is populated from BRONZE."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT display_popularity_tmdb FROM persons WHERE id = 'tmdb:p5001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == pytest.approx(15.2)

    def test_persons_idempotent(self, bronze_dir: Path) -> None:
        """Calling integrate() twice does not duplicate persons rows."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        tmdb_loader.integrate(conn, bronze_dir)
        count = conn.execute(
            "SELECT COUNT(*) FROM persons WHERE id LIKE 'tmdb:p%'"
        ).fetchone()[0]
        conn.close()
        assert count == 2


class TestCredits:
    def test_credits_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts credits rows from TMDb BRONZE."""
        conn = _make_silver_conn()
        counts = tmdb_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["credits_inserted"] >= 3

    def test_credits_evidence_source(self, bronze_dir: Path) -> None:
        """H4: credits rows use evidence_source='tmdb'."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        rows = conn.execute(
            "SELECT DISTINCT evidence_source FROM credits WHERE evidence_source = 'tmdb'"
        ).fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0][0] == "tmdb"

    def test_credits_person_id_prefix(self, bronze_dir: Path) -> None:
        """Credits rows use tmdb:p prefix for person_id."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT person_id FROM credits WHERE evidence_source = 'tmdb' LIMIT 1"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0].startswith("tmdb:p")

    def test_credits_anime_id_prefix(self, bronze_dir: Path) -> None:
        """Credits rows use tmdb:<media_type>: prefix for anime_id."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT anime_id FROM credits WHERE evidence_source = 'tmdb' "
            "AND person_id = 'tmdb:p5001' LIMIT 1"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0].startswith("tmdb:")

    def test_credits_idempotent(self, bronze_dir: Path) -> None:
        """Calling integrate() twice does not duplicate credits rows."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        c1 = conn.execute(
            "SELECT COUNT(*) FROM credits WHERE evidence_source = 'tmdb'"
        ).fetchone()[0]
        tmdb_loader.integrate(conn, bronze_dir)
        c2 = conn.execute(
            "SELECT COUNT(*) FROM credits WHERE evidence_source = 'tmdb'"
        ).fetchone()[0]
        conn.close()
        assert c1 == c2


class TestH1Invariant:
    def test_no_bare_vote_average_column(self, bronze_dir: Path) -> None:
        """H1: anime table must not have bare 'vote_average' column."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
        conn.close()
        assert "vote_average" not in cols

    def test_no_bare_popularity_column(self, bronze_dir: Path) -> None:
        """H1: anime table must not have bare 'popularity' column."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
        conn.close()
        assert "popularity" not in cols

    def test_no_bare_vote_count_column(self, bronze_dir: Path) -> None:
        """H1: anime table must not have bare 'vote_count' column."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
        conn.close()
        assert "vote_count" not in cols

    def test_display_tmdb_columns_present(self, bronze_dir: Path) -> None:
        """H1: display_*_tmdb columns exist after integrate."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
        conn.close()
        assert "display_vote_avg_tmdb" in cols
        assert "display_vote_count_tmdb" in cols
        assert "display_popularity_tmdb" in cols

    def test_persons_no_bare_popularity_column(self, bronze_dir: Path) -> None:
        """H1: persons table must not have bare 'popularity' column."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('persons')").fetchall()}
        conn.close()
        assert "popularity" not in cols

    def test_persons_display_popularity_tmdb_column_present(self, bronze_dir: Path) -> None:
        """H1: persons has display_popularity_tmdb column, not bare popularity."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('persons')").fetchall()}
        conn.close()
        assert "display_popularity_tmdb" in cols

    def test_source_code_no_bare_vote_average(self) -> None:
        """H1: non-display_ references to vote_average must not appear in SQL/code.

        Mirrors project's prescribed check:
          rg '\\b(vote_average|popularity)\\b' src/etl/conformed_loaders/tmdb.py | rg -v 'display_'
        Returns 0 lines.
        """
        import subprocess
        src_path = (
            Path(__file__).parent.parent.parent
            / "src" / "etl" / "conformed_loaders" / "tmdb.py"
        )
        rg1 = subprocess.run(
            ["rg", r"\b(vote_average|popularity)\b", str(src_path)],
            capture_output=True, text=True,
        )
        if not rg1.stdout.strip():
            return  # no matches at all — clean
        rg2 = subprocess.run(
            ["rg", "-v", "display_"],
            input=rg1.stdout, capture_output=True, text=True,
        )
        remaining = rg2.stdout.strip()
        assert not remaining, (
            f"vote_average/popularity found outside display_ prefix:\n{remaining}"
        )

    def test_source_code_no_bare_popularity(self) -> None:
        """H1: alias test — covered by test_source_code_no_bare_vote_average.

        Both vote_average and popularity are checked together in the sibling test.
        This test verifies that display_popularity_tmdb is the only popularity
        column present in the SILVER persons schema after integrate().
        """
        conn = _make_silver_conn()
        from src.etl.conformed_loaders import tmdb as _tmdb_ldr
        _tmdb_ldr._apply_ddl(conn)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('persons')").fetchall()}
        conn.close()
        assert "popularity" not in cols
        assert "display_popularity_tmdb" in cols


class TestImdbIdMapping:
    def test_imdb_id_url_constructable(self, bronze_dir: Path) -> None:
        """imdb_id values are in tt-prefixed format suitable for URL construction."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        rows = conn.execute(
            "SELECT imdb_id FROM anime WHERE id LIKE 'tmdb:%' AND imdb_id IS NOT NULL"
        ).fetchall()
        conn.close()
        assert len(rows) >= 1
        for (imdb_id,) in rows:
            assert imdb_id.startswith("tt"), f"Expected tt-prefix, got: {imdb_id!r}"

    def test_imdb_id_url_format(self, bronze_dir: Path) -> None:
        """Spot check: tt0245429 maps to correct IMDb URL format."""
        conn = _make_silver_conn()
        tmdb_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT imdb_id FROM anime WHERE id = 'tmdb:tv:10001'"
        ).fetchone()
        conn.close()
        assert row is not None
        imdb_id = row[0]
        imdb_url = f"https://www.imdb.com/title/{imdb_id}/"
        assert imdb_url == "https://www.imdb.com/title/tt0245429/"
