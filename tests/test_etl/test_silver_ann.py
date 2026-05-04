"""Tests for src/etl/conformed_loaders/ann.py.

Synthetic ANN BRONZE parquet fixtures → in-memory DuckDB → verify SILVER rows.

Data operations tested:
  anime_insert, persons_insert (Phase 1 — new INSERT paths)
  anime_extras, persons_extras, episodes, companies, releases, news,
  related, cast (Phase 2 — existing UPDATE/INSERT paths)

All tests use :memory: DuckDB — no file I/O beyond tmp_path parquet writes.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.conformed_loaders.ann import integrate, _apply_ddl


# ─── fixtures ───────────────────────────────────────────────────────────────

def _make_base_silver(conn: duckdb.DuckDBPyConnection) -> None:
    """Create minimal SILVER anime / persons tables in the test DuckDB."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anime (
            id      VARCHAR PRIMARY KEY,
            title_ja VARCHAR NOT NULL DEFAULT '',
            title_en VARCHAR NOT NULL DEFAULT '',
            year    INTEGER,
            season  VARCHAR,
            quarter INTEGER,
            episodes INTEGER,
            format  VARCHAR,
            duration INTEGER,
            start_date VARCHAR,
            end_date VARCHAR,
            status  VARCHAR,
            source_mat VARCHAR,
            work_type VARCHAR,
            scale_class VARCHAR,
            updated_at TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS persons (
            id         VARCHAR PRIMARY KEY,
            name_ja    VARCHAR NOT NULL DEFAULT '',
            name_en    VARCHAR NOT NULL DEFAULT '',
            name_ko    VARCHAR NOT NULL DEFAULT '',
            name_zh    VARCHAR NOT NULL DEFAULT '',
            names_alt  VARCHAR NOT NULL DEFAULT '{}',
            birth_date VARCHAR,
            website_url VARCHAR,
            updated_at TIMESTAMP DEFAULT now()
        )
    """)


def _write_bronze(
    root: Path,
    source: str,
    table: str,
    rows: list[dict],
) -> None:
    with BronzeWriter(source, table=table, root=root, compact_on_exit=False) as bw:
        for row in rows:
            bw.append(row)


# ─── helpers ────────────────────────────────────────────────────────────────

def _silver_conn(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with base SILVER tables."""
    conn = duckdb.connect(":memory:")
    _make_base_silver(conn)
    return conn


# ─── DDL: idempotency ───────────────────────────────────────────────────────

class TestApplyDdl:
    def test_idempotent(self, tmp_path: Path) -> None:
        """_apply_ddl is safe to call twice (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS)."""
        conn = _silver_conn(tmp_path)
        _apply_ddl(conn)
        _apply_ddl(conn)  # must not raise
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        assert "anime_episodes" in tables
        assert "anime_companies" in tables
        assert "anime_releases" in tables
        assert "anime_news" in tables
        assert "anime_relations" in tables
        assert "character_voice_actors" in tables

    def test_anime_extension_columns_created(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _apply_ddl(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('anime')").fetchall()}
        for col in (
            "themes", "plot_summary", "running_time_raw", "objectionable_content",
            "opening_themes_json", "ending_themes_json", "insert_songs_json",
            "official_websites_json", "vintage_raw", "image_url",
            "display_rating_votes", "display_rating_weighted", "display_rating_bayesian",
        ):
            assert col in cols, f"Missing anime column: {col}"

    def test_persons_extension_columns_created(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _apply_ddl(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('persons')").fetchall()}
        for col in ("height_raw", "family_name_ja", "given_name_ja", "image_url_ann"):
            assert col in cols, f"Missing persons column: {col}"

    def test_no_bare_rating_column(self, tmp_path: Path) -> None:
        """H1: bare rating_votes / rating_weighted / rating_bayesian must not exist."""
        conn = _silver_conn(tmp_path)
        _apply_ddl(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('anime')").fetchall()}
        assert "rating_votes" not in cols
        assert "rating_weighted" not in cols
        assert "rating_bayesian" not in cols


# ─── anime INSERT (Phase 1) ─────────────────────────────────────────────────

class TestAnimeInsert:
    """Phase 1: ANN BRONZE anime rows → SILVER anime INSERT."""

    def test_inserts_anime_row(self, tmp_path: Path) -> None:
        """integrate() inserts 'ann:a<id>' rows into anime."""
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "anime", [{
            "ann_id": 1,
            "title_en": "Test Anime",
            "title_ja": "テストアニメ",
            "year": 2009,
            "episodes": 26,
            "format": "TV",
            "start_date": None,
            "end_date": None,
            "content_hash": "abc123",
            "fetched_at": "2026-04-24T21:49:17.785364+00:00",
        }])
        integrate(conn, tmp_path)

        row = conn.execute(
            "SELECT id, title_en, title_ja, year, episodes, format "
            "FROM anime WHERE id = 'ann:a1'"
        ).fetchone()
        assert row is not None, "anime row 'ann:a1' was not inserted"
        assert row[0] == "ann:a1"
        assert row[1] == "Test Anime"
        assert row[2] == "テストアニメ"
        assert row[3] == 2009
        assert row[4] == 26
        assert row[5] == "TV"

    def test_inserts_multiple_anime_rows(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "anime", [
            {"ann_id": 10, "title_en": "Anime A", "title_ja": "", "year": 2020,
             "episodes": 12, "format": "TV"},
            {"ann_id": 20, "title_en": "Anime B", "title_ja": "", "year": 2021,
             "episodes": None, "format": "MOVIE"},
        ])
        counts = integrate(conn, tmp_path)

        assert counts.get("anime_ann") == 2, f"Expected 2 ANN anime, got: {counts}"
        n = conn.execute("SELECT COUNT(*) FROM anime WHERE id LIKE 'ann:%'").fetchone()[0]
        assert n == 2

    def test_deduplicates_on_pk_conflict(self, tmp_path: Path) -> None:
        """OR IGNORE: inserting same ann_id twice does not raise."""
        conn = _silver_conn(tmp_path)
        row = {"ann_id": 5, "title_en": "Dup", "title_ja": ""}
        _write_bronze(tmp_path, "ann", "anime", [row, row])
        integrate(conn, tmp_path)
        n = conn.execute("SELECT COUNT(*) FROM anime WHERE id = 'ann:a5'").fetchone()[0]
        assert n == 1

    def test_id_prefix_is_ann_a(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "anime", [{"ann_id": 99, "title_en": "X", "title_ja": ""}])
        integrate(conn, tmp_path)
        row = conn.execute("SELECT id FROM anime WHERE id = 'ann:a99'").fetchone()
        assert row is not None

    def test_counts_key_anime_ann_returned(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "anime", [{"ann_id": 7, "title_en": "Z", "title_ja": ""}])
        counts = integrate(conn, tmp_path)
        assert "anime_ann" in counts
        assert counts["anime_ann"] >= 1

    def test_no_anime_insert_error_key(self, tmp_path: Path) -> None:
        """Phase 1 INSERT must not produce an anime_insert_error key on clean data."""
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "anime", [{"ann_id": 3, "title_en": "Clean", "title_ja": ""}])
        counts = integrate(conn, tmp_path)
        assert "anime_insert_error" not in counts, counts.get("anime_insert_error")

    def test_existing_row_not_overwritten(self, tmp_path: Path) -> None:
        """OR IGNORE: a pre-existing 'ann:a<id>' row is not replaced by the INSERT."""
        conn = _silver_conn(tmp_path)
        conn.execute("INSERT INTO anime (id, title_en, title_ja) VALUES ('ann:a1', 'Pre-existing', '')")
        _write_bronze(tmp_path, "ann", "anime", [
            {"ann_id": 1, "title_en": "From Bronze", "title_ja": ""},
        ])
        integrate(conn, tmp_path)
        row = conn.execute("SELECT title_en FROM anime WHERE id = 'ann:a1'").fetchone()
        assert row[0] == "Pre-existing"


# ─── persons INSERT (Phase 1) ────────────────────────────────────────────────

class TestPersonsInsert:
    """Phase 1: ANN BRONZE persons rows → SILVER persons INSERT."""

    def test_inserts_person_row(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "persons", [{
            "ann_id": 42,
            "name_en": "Jane Doe",
            "name_ja": "ジェーン",
            "name_ko": "",
            "name_zh": "",
            "names_alt": "{}",
            "date_of_birth": None,
            "hometown": "Tokyo",
            "blood_type": "A",
            "website": None,
            "description": None,
            "gender": None,
            "nickname": None,
            "family_name_ja": "ドゥ",
            "given_name_ja": "ジェーン",
            "height_raw": "165cm",
            "image_url": None,
            "description_raw": None,
            "credits_json": None,
            "alt_names_json": None,
        }])
        integrate(conn, tmp_path)

        row = conn.execute(
            "SELECT id, name_en, name_ja FROM persons WHERE id = 'ann:p42'"
        ).fetchone()
        assert row is not None, "persons row 'ann:p42' was not inserted"
        assert row[0] == "ann:p42"
        assert row[1] == "Jane Doe"
        assert row[2] == "ジェーン"

    def test_inserts_multiple_persons(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "persons", [
            {"ann_id": 1, "name_en": "Alice", "name_ja": "", "name_ko": "",
             "name_zh": "", "names_alt": "{}", "date_of_birth": None,
             "hometown": None, "blood_type": None, "website": None,
             "description": None, "gender": None, "nickname": None,
             "family_name_ja": None, "given_name_ja": None, "height_raw": None,
             "image_url": None, "description_raw": None, "credits_json": None,
             "alt_names_json": None},
            {"ann_id": 2, "name_en": "Bob", "name_ja": "", "name_ko": "",
             "name_zh": "", "names_alt": "{}", "date_of_birth": None,
             "hometown": None, "blood_type": None, "website": None,
             "description": None, "gender": None, "nickname": None,
             "family_name_ja": None, "given_name_ja": None, "height_raw": None,
             "image_url": None, "description_raw": None, "credits_json": None,
             "alt_names_json": None},
        ])
        counts = integrate(conn, tmp_path)
        assert counts.get("persons_ann") == 2, f"Expected 2, got: {counts}"

    def test_id_prefix_is_ann_p(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "persons", [
            {"ann_id": 99, "name_en": "P99", "name_ja": "", "name_ko": "",
             "name_zh": "", "names_alt": "{}", "date_of_birth": None,
             "hometown": None, "blood_type": None, "website": None,
             "description": None, "gender": None, "nickname": None,
             "family_name_ja": None, "given_name_ja": None, "height_raw": None,
             "image_url": None, "description_raw": None, "credits_json": None,
             "alt_names_json": None},
        ])
        integrate(conn, tmp_path)
        row = conn.execute("SELECT id FROM persons WHERE id = 'ann:p99'").fetchone()
        assert row is not None

    def test_no_persons_insert_error_key(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "persons", [
            {"ann_id": 10, "name_en": "Clean", "name_ja": "", "name_ko": "",
             "name_zh": "", "names_alt": "{}", "date_of_birth": None,
             "hometown": None, "blood_type": None, "website": None,
             "description": None, "gender": None, "nickname": None,
             "family_name_ja": None, "given_name_ja": None, "height_raw": None,
             "image_url": None, "description_raw": None, "credits_json": None,
             "alt_names_json": None},
        ])
        counts = integrate(conn, tmp_path)
        assert "persons_insert_error" not in counts, counts.get("persons_insert_error")

    def test_orphan_credits_resolved(self, tmp_path: Path) -> None:
        """Core bug regression: after insert, credits.anime_id has a parent in anime."""
        conn = _silver_conn(tmp_path)
        # Simulate a credits table with an ANN credit
        conn.execute("""
            CREATE TABLE IF NOT EXISTS credits (
                person_id VARCHAR, anime_id VARCHAR, role VARCHAR NOT NULL,
                raw_role VARCHAR NOT NULL, evidence_source VARCHAR NOT NULL,
                updated_at TIMESTAMP DEFAULT now()
            )
        """)
        conn.execute(
            "INSERT INTO credits VALUES ('ann:p1', 'ann:a1', 'key_animation', 'Key Animation', 'ann', now())"
        )
        _write_bronze(tmp_path, "ann", "anime", [
            {"ann_id": 1, "title_en": "Test", "title_ja": ""},
        ])
        integrate(conn, tmp_path)
        # After integrate, the credit's anime_id should have a parent
        hit = conn.execute(
            "SELECT COUNT(*) FROM credits c "
            "JOIN anime a ON c.anime_id = a.id "
            "WHERE c.evidence_source = 'ann'"
        ).fetchone()[0]
        assert hit == 1, "Credit is still orphaned after integrate()"


# ─── anime extras ───────────────────────────────────────────────────────────

class TestAnimeExtras:
    def test_updates_existing_anime_row(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        conn.execute("INSERT INTO anime (id, title_en) VALUES ('ann:a1', 'Test Anime')")

        _write_bronze(tmp_path, "ann", "anime", [{
            "ann_id": 1,
            "title_en": "Test Anime",
            "title_ja": "テスト",
            "themes": "Drama; Comedy",
            "plot_summary": "A test story.",
            "running_time_raw": "24 min",
            "objectionable_content": "None",
            "opening_themes_json": '["OP1"]',
            "ending_themes_json": '["ED1"]',
            "insert_songs_json": None,
            "official_websites_json": None,
            "vintage_raw": "2024 Spring",
            "image_url": "https://example.com/img.jpg",
            "display_rating_votes": 500,
            "display_rating_weighted": 7.5,
            "display_rating_bayesian": 7.3,
        }])

        counts = integrate(conn, tmp_path)
        assert "anime_error" not in counts

        row = conn.execute(
            "SELECT themes, plot_summary, display_rating_votes, display_rating_weighted "
            "FROM anime WHERE id = 'ann:a1'"
        ).fetchone()
        assert row is not None
        assert row[0] == "Drama; Comedy"
        assert row[1] == "A test story."
        assert row[2] == 500
        assert abs(row[3] - 7.5) < 0.01

    def test_no_match_for_unknown_ann_id(self, tmp_path: Path) -> None:
        """Anime rows not in SILVER should not cause errors — just 0 updates."""
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "anime", [{
            "ann_id": 9999,
            "themes": "Action",
            "plot_summary": None,
            "running_time_raw": None,
            "objectionable_content": None,
            "opening_themes_json": None,
            "ending_themes_json": None,
            "insert_songs_json": None,
            "official_websites_json": None,
            "vintage_raw": None,
            "image_url": None,
            "display_rating_votes": None,
            "display_rating_weighted": None,
            "display_rating_bayesian": None,
        }])
        counts = integrate(conn, tmp_path)
        assert "anime_error" not in counts


# ─── persons extras ─────────────────────────────────────────────────────────

class TestPersonsExtras:
    def test_updates_existing_person_row(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        conn.execute("INSERT INTO persons (id, name_en) VALUES ('ann:p42', 'Jane Doe')")

        _write_bronze(tmp_path, "ann", "persons", [{
            "ann_id": 42,
            "name_en": "Jane Doe",
            "name_ja": "ジェーン・ドゥ",
            "name_ko": "",
            "name_zh": "",
            "names_alt": "{}",
            "date_of_birth": None,
            "hometown": "Tokyo",
            "blood_type": "A",
            "website": None,
            "description": None,
            "gender": None,
            "nickname": None,
            "family_name_ja": "ドゥ",
            "given_name_ja": "ジェーン",
            "height_raw": "165cm",
            "image_url": None,
            "description_raw": None,
            "credits_json": None,
            "alt_names_json": None,
        }])

        counts = integrate(conn, tmp_path)
        assert "persons_error" not in counts

        row = conn.execute(
            "SELECT hometown, height_raw, family_name_ja, given_name_ja "
            "FROM persons WHERE id = 'ann:p42'"
        ).fetchone()
        assert row is not None
        assert row[0] == "Tokyo"
        assert row[1] == "165cm"
        assert row[2] == "ドゥ"
        assert row[3] == "ジェーン"


# ─── episodes ───────────────────────────────────────────────────────────────

class TestEpisodes:
    def test_inserts_episodes(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "episodes", [
            {"ann_anime_id": 1, "episode_num": "1", "lang": "ja", "title": "第一話", "aired_date": None},
            {"ann_anime_id": 1, "episode_num": "2", "lang": "ja", "title": "第二話", "aired_date": None},
            {"ann_anime_id": 1, "episode_num": "1", "lang": "en", "title": "Episode 1", "aired_date": None},
        ])

        counts = integrate(conn, tmp_path)
        assert counts["anime_episodes"] == 3

    def test_deduplicates_on_conflict(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        row = {"ann_anime_id": 5, "episode_num": "1", "lang": "ja", "title": "EP1", "aired_date": None}
        _write_bronze(tmp_path, "ann", "episodes", [row, row])  # duplicate

        counts = integrate(conn, tmp_path)
        assert counts["anime_episodes"] == 1


# ─── companies ──────────────────────────────────────────────────────────────

class TestCompanies:
    def test_inserts_companies(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "company", [
            {"ann_anime_id": 1, "company_name": "Studio A", "task": "Animation", "company_id": 10},
            {"ann_anime_id": 1, "company_name": "Studio B", "task": "Production", "company_id": 20},
        ])

        counts = integrate(conn, tmp_path)
        assert counts["anime_companies"] == 2

    def test_source_is_ann(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "company", [
            {"ann_anime_id": 1, "company_name": "Toei", "task": "Production", "company_id": 1},
        ])
        integrate(conn, tmp_path)
        src = conn.execute("SELECT source FROM anime_companies LIMIT 1").fetchone()
        assert src[0] == "ann"


# ─── releases ───────────────────────────────────────────────────────────────

class TestReleases:
    def test_inserts_releases(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "releases", [
            {
                "ann_anime_id": 1,
                "product_title": "Blu-ray Vol.1",
                "release_date": "2024-06-01",
                "href": "https://example.com/1",
                "region": None,
            },
        ])
        counts = integrate(conn, tmp_path)
        assert counts["anime_releases"] == 1


# ─── news ───────────────────────────────────────────────────────────────────

class TestNews:
    def test_inserts_news(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "news", [
            {"ann_anime_id": 1, "datetime": "2024-01-15", "title": "Announcement", "href": "https://ann.com/news/1"},
            {"ann_anime_id": 1, "datetime": "2024-02-01", "title": "Cast Reveal", "href": "https://ann.com/news/2"},
        ])
        counts = integrate(conn, tmp_path)
        assert counts["anime_news"] == 2

    def test_null_href_excluded(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "news", [
            {"ann_anime_id": 1, "datetime": None, "title": "No link", "href": None},
        ])
        counts = integrate(conn, tmp_path)
        assert counts["anime_news"] == 0


# ─── related ────────────────────────────────────────────────────────────────

class TestRelated:
    def test_inserts_anime_relations(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "related", [
            {"ann_anime_id": 1, "target_ann_id": 2, "rel": "sequel", "direction": "forward"},
            {"ann_anime_id": 1, "target_ann_id": 3, "rel": "prequel", "direction": "forward"},
        ])
        counts = integrate(conn, tmp_path)
        assert "related_error" not in counts

        n = conn.execute("SELECT COUNT(*) FROM anime_relations").fetchone()[0]
        assert n == 2

    def test_relation_type_filled(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "related", [
            {"ann_anime_id": 10, "target_ann_id": 20, "rel": "alternative version", "direction": "forward"},
        ])
        integrate(conn, tmp_path)
        row = conn.execute("SELECT relation_type FROM anime_relations LIMIT 1").fetchone()
        assert row[0] == "alternative version"

    def test_relation_source_is_ann(self, tmp_path: Path) -> None:
        """H4: anime_relations rows from ANN loader have source='ann'."""
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "related", [
            {"ann_anime_id": 5, "target_ann_id": 6, "rel": "sequel", "direction": "forward"},
        ])
        integrate(conn, tmp_path)
        row = conn.execute("SELECT source FROM anime_relations LIMIT 1").fetchone()
        assert row is not None
        assert row[0] == "ann"

    def test_anime_relations_has_source_column(self, tmp_path: Path) -> None:
        """anime_relations table must have a source column after DDL."""
        conn = _silver_conn(tmp_path)
        _apply_ddl(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('anime_relations')").fetchall()}
        assert "source" in cols


# ─── cast ───────────────────────────────────────────────────────────────────

class TestCast:
    def test_inserts_character_voice_actors(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "cast", [
            {
                "ann_anime_id": 1,
                "ann_person_id": 42,
                "voice_actor_name": "Jane Doe",
                "cast_role": "Main",
                "character_name": "Sakura",
                "character_id": 100,
            },
        ])
        counts = integrate(conn, tmp_path)
        assert "cast_error" not in counts

        row = conn.execute(
            "SELECT character_id, person_id, anime_id, character_role, source "
            "FROM character_voice_actors LIMIT 1"
        ).fetchone()
        assert row is not None
        assert row[0] == "ann:c100"
        assert row[1] == "ann:p42"
        assert row[2] == "ann:a1"
        assert row[3] == "Main"
        assert row[4] == "ann"

    def test_null_character_id_excluded(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        _write_bronze(tmp_path, "ann", "cast", [
            {
                "ann_anime_id": 1,
                "ann_person_id": 10,
                "voice_actor_name": "Bob",
                "cast_role": None,
                "character_name": None,
                "character_id": None,
            },
        ])
        integrate(conn, tmp_path)
        n = conn.execute("SELECT COUNT(*) FROM character_voice_actors").fetchone()[0]
        assert n == 0


# ─── H1 compliance ──────────────────────────────────────────────────────────

class TestH1Compliance:
    def test_no_bare_rating_in_anime_table(self, tmp_path: Path) -> None:
        """H1: bare rating_* columns must not be added to anime."""
        conn = _silver_conn(tmp_path)
        _apply_ddl(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('anime')").fetchall()}
        assert "rating_votes" not in cols
        assert "rating_weighted" not in cols
        assert "rating_bayesian" not in cols

    def test_display_rating_prefix_present(self, tmp_path: Path) -> None:
        """H1: display_rating_* columns are added (allowed prefix)."""
        conn = _silver_conn(tmp_path)
        _apply_ddl(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('anime')").fetchall()}
        assert "display_rating_votes" in cols
        assert "display_rating_weighted" in cols
        assert "display_rating_bayesian" in cols


# ─── integrate return value ─────────────────────────────────────────────────

class TestIntegrateReturnValue:
    def test_returns_all_count_keys(self, tmp_path: Path) -> None:
        conn = _silver_conn(tmp_path)
        # Write minimal stubs for all 8 tables so no parquet-not-found error
        for table, row in [
            ("anime", {"ann_id": 1, "themes": None, "plot_summary": None,
                       "running_time_raw": None, "objectionable_content": None,
                       "opening_themes_json": None, "ending_themes_json": None,
                       "insert_songs_json": None, "official_websites_json": None,
                       "vintage_raw": None, "image_url": None,
                       "display_rating_votes": None, "display_rating_weighted": None,
                       "display_rating_bayesian": None}),
            ("persons", {"ann_id": 1, "name_en": "X", "name_ja": "", "name_ko": "",
                         "name_zh": "", "names_alt": "{}", "date_of_birth": None,
                         "hometown": None, "blood_type": None, "website": None,
                         "description": None, "gender": None, "nickname": None,
                         "family_name_ja": None, "given_name_ja": None,
                         "height_raw": None, "image_url": None,
                         "description_raw": None, "credits_json": None,
                         "alt_names_json": None}),
            ("episodes", {"ann_anime_id": 1, "episode_num": "1", "lang": "ja",
                          "title": "E1", "aired_date": None}),
            ("company", {"ann_anime_id": 1, "company_name": "C", "task": "T",
                         "company_id": 1}),
            ("releases", {"ann_anime_id": 1, "product_title": "P", "release_date": "2024",
                          "href": "h", "region": None}),
            ("news", {"ann_anime_id": 1, "datetime": None, "title": "N",
                      "href": "https://x.com/1"}),
            ("related", {"ann_anime_id": 1, "target_ann_id": 2, "rel": "sequel",
                         "direction": "forward"}),
            ("cast", {"ann_anime_id": 1, "ann_person_id": 1, "voice_actor_name": "VA",
                      "cast_role": "Main", "character_name": "C", "character_id": 1}),
        ]:
            _write_bronze(tmp_path, "ann", table, [row])

        counts = integrate(conn, tmp_path)

        for key in ("anime_episodes", "anime_companies", "anime_releases", "anime_news"):
            assert key in counts, f"Missing key in counts: {key}"


# ─── Card 20/03: _ann suffix columns ────────────────────────────────────────

class TestAnnSuffixColumns:
    """Card 20/03: display_rating_*_ann columns for cross-source disambiguation (H1)."""

    def test_ann_suffix_columns_created_by_ddl(self, tmp_path: Path) -> None:
        """_apply_ddl creates the four _ann suffix display columns."""
        conn = _silver_conn(tmp_path)
        _apply_ddl(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('anime')").fetchall()}
        for col in (
            "display_rating_count_ann",
            "display_rating_avg_ann",
            "display_rating_weighted_ann",
            "display_rating_bayesian_ann",
        ):
            assert col in cols, f"Missing _ann suffix column: {col}"

    def test_ann_suffix_columns_populated_on_update(self, tmp_path: Path) -> None:
        """integrate() propagates BRONZE rating values to _ann suffix columns."""
        conn = _silver_conn(tmp_path)
        conn.execute("INSERT INTO anime (id, title_en) VALUES ('ann:a10', 'Suffix Test')")

        _write_bronze(tmp_path, "ann", "anime", [{
            "ann_id": 10,
            "title_en": "Suffix Test",
            "title_ja": "サフィックステスト",
            "themes": None,
            "plot_summary": None,
            "running_time_raw": None,
            "objectionable_content": None,
            "opening_themes_json": None,
            "ending_themes_json": None,
            "insert_songs_json": None,
            "official_websites_json": None,
            "vintage_raw": None,
            "image_url": None,
            "display_rating_votes": 250,
            "display_rating_weighted": 8.2,
            "display_rating_bayesian": 8.1,
        }])

        integrate(conn, tmp_path)
        row = conn.execute(
            "SELECT display_rating_count_ann, display_rating_avg_ann, "
            "       display_rating_weighted_ann, display_rating_bayesian_ann "
            "FROM anime WHERE id = 'ann:a10'"
        ).fetchone()
        assert row is not None
        assert row[0] == 250       # display_rating_count_ann ← display_rating_votes
        assert abs(row[1] - 8.2) < 0.001   # display_rating_avg_ann ← display_rating_weighted
        assert abs(row[2] - 8.2) < 0.001   # display_rating_weighted_ann
        assert abs(row[3] - 8.1) < 0.001   # display_rating_bayesian_ann

    def test_ann_suffix_columns_null_safe(self, tmp_path: Path) -> None:
        """NULL BRONZE rating values produce NULL _ann suffix columns (no crash)."""
        conn = _silver_conn(tmp_path)
        conn.execute("INSERT INTO anime (id, title_en) VALUES ('ann:a20', 'NullRating')")

        _write_bronze(tmp_path, "ann", "anime", [{
            "ann_id": 20,
            "themes": None,
            "plot_summary": None,
            "running_time_raw": None,
            "objectionable_content": None,
            "opening_themes_json": None,
            "ending_themes_json": None,
            "insert_songs_json": None,
            "official_websites_json": None,
            "vintage_raw": None,
            "image_url": None,
            "display_rating_votes": None,
            "display_rating_weighted": None,
            "display_rating_bayesian": None,
        }])

        integrate(conn, tmp_path)
        row = conn.execute(
            "SELECT display_rating_count_ann, display_rating_avg_ann "
            "FROM anime WHERE id = 'ann:a20'"
        ).fetchone()
        assert row is not None
        assert row[0] is None
        assert row[1] is None

    def test_h1_no_bare_ann_suffix(self, tmp_path: Path) -> None:
        """H1: _ann columns must not exist without display_ prefix."""
        conn = _silver_conn(tmp_path)
        _apply_ddl(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info('anime')").fetchall()}
        assert "rating_count_ann" not in cols
        assert "rating_avg_ann" not in cols
        assert "rating_weighted_ann" not in cols
        assert "rating_bayesian_ann" not in cols
