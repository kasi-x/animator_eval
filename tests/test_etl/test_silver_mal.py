"""Tests for src/etl/silver_loaders/mal.py.

Creates minimal synthetic BRONZE parquet in a temp dir, builds a minimal
SILVER duckdb, then calls integrate() and checks row counts and H1 invariants.

H1 invariant: SILVER columns must NOT contain bare score / popularity /
favourites / members / rank — only display_*_mal prefixed variants.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.silver_loaders import mal as mal_loader

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

CREATE TABLE IF NOT EXISTS anime_genres (
    anime_id   VARCHAR NOT NULL,
    genre_name VARCHAR NOT NULL,
    PRIMARY KEY (anime_id, genre_name)
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

CREATE SEQUENCE IF NOT EXISTS seq_anime_studios_id;
CREATE TABLE IF NOT EXISTS anime_studios (
    id        INTEGER PRIMARY KEY DEFAULT nextval('seq_anime_studios_id'),
    anime_id  VARCHAR NOT NULL,
    studio_id VARCHAR NOT NULL,
    is_main   INTEGER NOT NULL DEFAULT 0,
    UNIQUE(anime_id, studio_id)
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
    """Return an in-memory DuckDB with minimal SILVER tables."""
    conn = duckdb.connect(":memory:")
    conn.execute(_SILVER_DDL)
    return conn


# ─── BRONZE fixtures ──────────────────────────────────────────────────────────

@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    """Write synthetic MAL BRONZE parquet for all 9 required tables."""
    root = tmp_path / "bronze"

    # anime
    with BronzeWriter("mal", table="anime", root=root) as bw:
        bw.append({
            "mal_id": 1,
            "url": "https://myanimelist.net/anime/1",
            "title": "Cowboy Bebop",
            "title_english": "Cowboy Bebop",
            "title_japanese": "カウボーイビバップ",
            "titles_alt_json": "[]",
            "synonyms_json": "[]",
            "type": "TV",
            "source": "Original",
            "episodes": 26,
            "status": "Finished Airing",
            "airing": False,
            "aired_from": "1998-04-03",
            "aired_to": "1999-04-24",
            "aired_string": "Apr 3, 1998 to Apr 24, 1999",
            "duration_raw": "24 min per ep",
            "rating": "R - 17+",
            "season": "spring",
            "year": 1998,
            "broadcast_day": None,
            "broadcast_time": None,
            "broadcast_timezone": None,
            "broadcast_string": None,
            "synopsis": "A classic anime.",
            "background": None,
            "approved": True,
            # H1: these are already prefixed display_* in the BRONZE schema
            "display_score": 8.75,
            "display_scored_by": 900000,
            "display_rank": 28,
            "display_popularity": 39,
            "display_members": 1200000,
            "display_favorites": 75000,
            "image_url": "https://cdn.myanimelist.net/images/anime/4/19644.jpg",
            "image_url_large": None,
            "trailer_youtube_id": None,
            "fetched_at": "2026-04-25T00:00:00",
            "content_hash": "abc123",
        })

    # persons (optional — test the graceful-skip via absent table path,
    # but we also test the populated path with an explicit small fixture)
    with BronzeWriter("mal", table="persons", root=root) as bw:
        bw.append({
            "mal_id": 101,
            "name": "Shinichiro Watanabe",
            "url": "https://myanimelist.net/people/101",
            "image_url": None,
            "birthday": None,
            "about": None,
            "fetched_at": "2026-04-25T00:00:00",
            "content_hash": "p101",
        })

    # staff_credits
    with BronzeWriter("mal", table="staff_credits", root=root) as bw:
        bw.append({
            "mal_id": 1,
            "mal_person_id": 101,
            "person_name": "Shinichiro Watanabe",
            "position": "Director",
        })
        bw.append({
            "mal_id": 1,
            "mal_person_id": 102,
            "person_name": "Yoko Kanno",
            "position": "Music",
        })

    # anime_characters
    with BronzeWriter("mal", table="anime_characters", root=root) as bw:
        bw.append({
            "mal_id": 1,
            "mal_character_id": 201,
            "character_name": "Spike Spiegel",
            "character_url": "https://myanimelist.net/character/201",
            "role": "Main",
            "display_favorites": 50000,
            "image_url": None,
        })

    # va_credits
    with BronzeWriter("mal", table="va_credits", root=root) as bw:
        bw.append({
            "mal_id": 1,
            "mal_character_id": 201,
            "mal_person_id": 301,
            "person_name": "Koichi Yamadera",
            "language": "Japanese",
        })

    # anime_genres
    with BronzeWriter("mal", table="anime_genres", root=root) as bw:
        bw.append({
            "mal_id": 1,
            "genre_id": 1,
            "name": "Action",
            "kind": "genre",
        })
        bw.append({
            "mal_id": 1,
            "genre_id": 2,
            "name": "Sci-Fi",
            "kind": "genre",
        })

    # anime_studios
    with BronzeWriter("mal", table="anime_studios", root=root) as bw:
        bw.append({
            "mal_id": 1,
            "mal_producer_id": 14,
            "name": "Sunrise",
            "kind": "Studios",
            "url": "https://myanimelist.net/anime/producer/14",
        })

    # anime_relations
    with BronzeWriter("mal", table="anime_relations", root=root) as bw:
        bw.append({
            "mal_id": 1,
            "relation_type": "Side story",
            "target_type": "anime",
            "target_mal_id": 5,
            "target_name": "Cowboy Bebop: The Movie",
            "target_url": "https://myanimelist.net/anime/5",
        })

    # anime_recommendations
    with BronzeWriter("mal", table="anime_recommendations", root=root) as bw:
        bw.append({
            "mal_id": 1,
            "recommended_mal_id": 6,
            "recommended_url": "https://myanimelist.net/anime/6",
            "votes": 42,
        })

    return root


@pytest.fixture
def bronze_dir_no_persons(tmp_path: Path) -> Path:
    """BRONZE fixture without persons table — to test graceful skip."""
    root = tmp_path / "bronze_no_persons"

    with BronzeWriter("mal", table="anime", root=root) as bw:
        bw.append({
            "mal_id": 999,
            "title": "Ghost in the Shell",
            "title_english": "Ghost in the Shell",
            "title_japanese": "攻殻機動隊",
            "type": "Movie",
            "source": "Manga",
            "episodes": 1,
            "status": "Finished Airing",
            "airing": False,
            "aired_from": "1995-11-18",
            "aired_to": "1995-11-18",
            "season": None,
            "year": 1995,
            "display_score": 8.5,
            "display_scored_by": 200000,
            "display_rank": 50,
            "display_popularity": 100,
            "display_members": 500000,
            "display_favorites": 20000,
        })

    # Provide empty tables to avoid glob errors on optional tables
    for tbl in ["staff_credits", "anime_characters", "va_credits",
                "anime_genres", "anime_studios", "anime_relations",
                "anime_recommendations"]:
        with BronzeWriter("mal", table=tbl, root=root) as bw:
            pass  # empty — no rows

    return root


# ─── Tests ───────────────────────────────────────────────────────────────────

class TestAnime:
    def test_anime_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts anime rows with mal:a prefix."""
        conn = _make_silver_conn()
        counts = mal_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["anime_inserted"] >= 1

    def test_anime_id_prefix(self, bronze_dir: Path) -> None:
        """Anime rows use 'mal:a<id>' format."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT id FROM anime WHERE id = 'mal:a1'"
        ).fetchone()
        conn.close()
        assert row is not None

    def test_anime_display_columns_populated(self, bronze_dir: Path) -> None:
        """display_*_mal columns are populated from BRONZE."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT display_score_mal, display_popularity_mal, "
            "display_members_mal, display_favorites_mal, "
            "display_rank_mal, display_scored_by_mal "
            "FROM anime WHERE id = 'mal:a1'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == pytest.approx(8.75)   # display_score_mal
        assert row[1] == 39                     # display_popularity_mal
        assert row[2] == 1200000                # display_members_mal
        assert row[3] == 75000                  # display_favorites_mal
        assert row[4] == 28                     # display_rank_mal
        assert row[5] == 900000                 # display_scored_by_mal

    def test_h1_no_bare_score_columns(self, bronze_dir: Path) -> None:
        """H1: SILVER anime table must NOT have bare score/popularity/etc columns."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
        conn.close()
        assert "score" not in cols
        assert "popularity" not in cols
        assert "favourites" not in cols
        assert "members" not in cols
        assert "rank" not in cols

    def test_anime_idempotent(self, bronze_dir: Path) -> None:
        """integrate() twice does not duplicate anime rows."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        mal_loader.integrate(conn, bronze_dir)
        count = conn.execute(
            "SELECT COUNT(*) FROM anime WHERE id LIKE 'mal:a%'"
        ).fetchone()[0]
        conn.close()
        assert count == 1


class TestPersons:
    def test_persons_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts persons with mal:p prefix when BRONZE exists."""
        conn = _make_silver_conn()
        counts = mal_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["persons_inserted"] >= 1

    def test_persons_id_prefix(self, bronze_dir: Path) -> None:
        """Person IDs use 'mal:p<id>' format."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT id, name_en FROM persons WHERE id = 'mal:p101'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert "Watanabe" in row[1]

    def test_persons_graceful_skip_when_absent(
        self, bronze_dir_no_persons: Path
    ) -> None:
        """integrate() does not error when persons BRONZE is absent."""
        conn = _make_silver_conn()
        counts = mal_loader.integrate(conn, bronze_dir_no_persons)
        conn.close()
        assert counts["persons_inserted"] == 0

    def test_persons_idempotent(self, bronze_dir: Path) -> None:
        """Calling integrate() twice does not duplicate person rows."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        mal_loader.integrate(conn, bronze_dir)
        count = conn.execute(
            "SELECT COUNT(*) FROM persons WHERE id LIKE 'mal:p%'"
        ).fetchone()[0]
        conn.close()
        assert count == 1


class TestCredits:
    def test_credits_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts staff_credits into credits table."""
        conn = _make_silver_conn()
        counts = mal_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["credits_inserted"] >= 2  # Director + Music

    def test_credits_evidence_source(self, bronze_dir: Path) -> None:
        """H4: credits rows use evidence_source='mal'."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        rows = conn.execute(
            "SELECT DISTINCT evidence_source FROM credits WHERE evidence_source = 'mal'"
        ).fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0][0] == "mal"

    def test_credits_role_mapping(self, bronze_dir: Path) -> None:
        """Director position maps to 'director' role."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT role FROM credits WHERE person_id = 'mal:p101' AND anime_id = 'mal:a1'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "director"

    def test_credits_music_role_mapping(self, bronze_dir: Path) -> None:
        """Music position maps to 'music' role."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT role FROM credits WHERE person_id = 'mal:p102' AND anime_id = 'mal:a1'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "music"

    def test_credits_idempotent(self, bronze_dir: Path) -> None:
        """Calling integrate() twice does not duplicate credits rows."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        c1 = conn.execute(
            "SELECT COUNT(*) FROM credits WHERE evidence_source = 'mal'"
        ).fetchone()[0]
        mal_loader.integrate(conn, bronze_dir)
        c2 = conn.execute(
            "SELECT COUNT(*) FROM credits WHERE evidence_source = 'mal'"
        ).fetchone()[0]
        conn.close()
        assert c1 == c2


class TestCharacters:
    def test_characters_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts characters with mal:c prefix."""
        conn = _make_silver_conn()
        counts = mal_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["characters_inserted"] >= 1

    def test_characters_id_prefix(self, bronze_dir: Path) -> None:
        """Character IDs use 'mal:c<id>' format."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT id, name_ja FROM characters WHERE id = 'mal:c201'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert "Spike" in row[1]

    def test_characters_idempotent(self, bronze_dir: Path) -> None:
        """integrate() twice does not duplicate character rows."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        mal_loader.integrate(conn, bronze_dir)
        count = conn.execute(
            "SELECT COUNT(*) FROM characters WHERE id LIKE 'mal:c%'"
        ).fetchone()[0]
        conn.close()
        assert count == 1


class TestCVA:
    def test_cva_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts character_voice_actors from va_credits."""
        conn = _make_silver_conn()
        counts = mal_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["character_voice_actors_inserted"] >= 1

    def test_cva_source(self, bronze_dir: Path) -> None:
        """CVA rows have source='mal'."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        rows = conn.execute(
            "SELECT DISTINCT source FROM character_voice_actors WHERE source = 'mal'"
        ).fetchall()
        conn.close()
        assert len(rows) == 1

    def test_cva_idempotent(self, bronze_dir: Path) -> None:
        """integrate() twice does not duplicate CVA rows."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        c1 = conn.execute(
            "SELECT COUNT(*) FROM character_voice_actors WHERE source = 'mal'"
        ).fetchone()[0]
        mal_loader.integrate(conn, bronze_dir)
        c2 = conn.execute(
            "SELECT COUNT(*) FROM character_voice_actors WHERE source = 'mal'"
        ).fetchone()[0]
        conn.close()
        assert c1 == c2


class TestGenres:
    def test_genres_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts anime_genres rows."""
        conn = _make_silver_conn()
        counts = mal_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["anime_genres_inserted"] >= 2

    def test_genres_idempotent(self, bronze_dir: Path) -> None:
        """integrate() twice does not duplicate genre rows."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        c1 = conn.execute("SELECT COUNT(*) FROM anime_genres").fetchone()[0]
        mal_loader.integrate(conn, bronze_dir)
        c2 = conn.execute("SELECT COUNT(*) FROM anime_genres").fetchone()[0]
        conn.close()
        assert c1 == c2


class TestStudios:
    def test_studios_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts studios with 'mal:n:' prefix."""
        conn = _make_silver_conn()
        counts = mal_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["studios_inserted"] >= 1

    def test_studio_id_format(self, bronze_dir: Path) -> None:
        """Studio IDs use 'mal:n:' || studio_name format."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT id, name FROM studios WHERE id = 'mal:n:Sunrise'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[1] == "Sunrise"

    def test_anime_studios_linked(self, bronze_dir: Path) -> None:
        """anime_studios joins anime and studios correctly."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT anime_id, studio_id, is_main FROM anime_studios "
            "WHERE anime_id = 'mal:a1' AND studio_id = 'mal:n:Sunrise'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[2] == 1  # is_main because kind = 'Studios'

    def test_studios_idempotent(self, bronze_dir: Path) -> None:
        """integrate() twice does not duplicate studio rows."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        c1 = conn.execute(
            "SELECT COUNT(*) FROM studios WHERE id LIKE 'mal:n:%'"
        ).fetchone()[0]
        mal_loader.integrate(conn, bronze_dir)
        c2 = conn.execute(
            "SELECT COUNT(*) FROM studios WHERE id LIKE 'mal:n:%'"
        ).fetchone()[0]
        conn.close()
        assert c1 == c2


class TestRelations:
    def test_relations_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts anime_relations rows."""
        conn = _make_silver_conn()
        counts = mal_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["anime_relations_inserted"] >= 1

    def test_relations_prefix(self, bronze_dir: Path) -> None:
        """anime_relations uses mal:a prefix for both sides."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT anime_id, related_anime_id, relation_type "
            "FROM anime_relations WHERE anime_id = 'mal:a1'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0].startswith("mal:a")
        assert row[1].startswith("mal:a")
        assert row[2] == "Side story"

    def test_relations_idempotent(self, bronze_dir: Path) -> None:
        """integrate() twice does not duplicate relation rows."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        c1 = conn.execute("SELECT COUNT(*) FROM anime_relations").fetchone()[0]
        mal_loader.integrate(conn, bronze_dir)
        c2 = conn.execute("SELECT COUNT(*) FROM anime_relations").fetchone()[0]
        conn.close()
        assert c1 == c2

    def test_relations_source_is_mal(self, bronze_dir: Path) -> None:
        """H4: anime_relations rows from MAL loader have source='mal'."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        src = conn.execute(
            "SELECT DISTINCT source FROM anime_relations WHERE anime_id = 'mal:a1'"
        ).fetchone()
        conn.close()
        assert src is not None
        assert src[0] == "mal"

    def test_anime_relations_has_source_column(self, bronze_dir: Path) -> None:
        """anime_relations table must have a source column after DDL (H4)."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('anime_relations')").fetchall()}
        conn.close()
        assert "source" in cols


class TestRecommendations:
    def test_recommendations_inserted(self, bronze_dir: Path) -> None:
        """integrate() inserts anime_recommendations rows."""
        conn = _make_silver_conn()
        counts = mal_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["anime_recommendations_inserted"] >= 1

    def test_recommendations_prefix(self, bronze_dir: Path) -> None:
        """anime_recommendations uses mal:a prefix for both anime IDs."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT anime_id, recommended_anime_id, votes, source "
            "FROM anime_recommendations WHERE anime_id = 'mal:a1'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "mal:a1"
        assert row[1] == "mal:a6"
        assert row[2] == 42
        assert row[3] == "mal"

    def test_recommendations_idempotent(self, bronze_dir: Path) -> None:
        """integrate() twice does not duplicate recommendation rows."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        c1 = conn.execute(
            "SELECT COUNT(*) FROM anime_recommendations"
        ).fetchone()[0]
        mal_loader.integrate(conn, bronze_dir)
        c2 = conn.execute(
            "SELECT COUNT(*) FROM anime_recommendations"
        ).fetchone()[0]
        conn.close()
        assert c1 == c2


class TestH1Invariant:
    def test_no_bare_score_column(self, bronze_dir: Path) -> None:
        """H1: anime table has no bare 'score' column."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
        conn.close()
        assert "score" not in cols

    def test_no_bare_popularity_column(self, bronze_dir: Path) -> None:
        """H1: anime table has no bare 'popularity' column."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
        conn.close()
        assert "popularity" not in cols

    def test_no_bare_members_column(self, bronze_dir: Path) -> None:
        """H1: anime table has no bare 'members' column."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
        conn.close()
        assert "members" not in cols

    def test_no_bare_rank_column(self, bronze_dir: Path) -> None:
        """H1: anime table has no bare 'rank' column."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
        conn.close()
        assert "rank" not in cols

    def test_display_mal_columns_present(self, bronze_dir: Path) -> None:
        """H1: display_*_mal columns exist and are populated."""
        conn = _make_silver_conn()
        mal_loader.integrate(conn, bronze_dir)
        cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
        conn.close()
        assert "display_score_mal" in cols
        assert "display_popularity_mal" in cols
        assert "display_members_mal" in cols
        assert "display_favorites_mal" in cols
        assert "display_rank_mal" in cols
        assert "display_scored_by_mal" in cols
