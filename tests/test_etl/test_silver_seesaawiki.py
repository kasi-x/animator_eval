"""Unit tests for src/etl/silver_loaders/seesaawiki.py (Card 14/04).

Creates a minimal in-memory DuckDB with the core SILVER tables (anime,
persons, studios, anime_studios) pre-populated from synthetic parquet,
then calls ``integrate()`` and asserts on the resulting SILVER tables.

No real BRONZE parquet is required — each test writes synthetic parquet
files to a temporary directory.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from src.etl.silver_loaders.seesaawiki import _apply_ddl, integrate

# ─── Helpers ─────────────────────────────────────────────────────────────────


def _make_conn() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB with core SILVER tables."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE studios (
            id                  VARCHAR PRIMARY KEY,
            name                VARCHAR NOT NULL DEFAULT '',
            anilist_id          INTEGER,
            is_animation_studio BOOLEAN,
            country_of_origin   VARCHAR,
            favourites          INTEGER,
            site_url            VARCHAR,
            updated_at          TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE anime_studios (
            anime_id  VARCHAR NOT NULL,
            studio_id VARCHAR NOT NULL,
            is_main   BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (anime_id, studio_id)
        )
    """)
    conn.execute("""
        CREATE TABLE anime (
            id         VARCHAR PRIMARY KEY,
            title_ja   VARCHAR NOT NULL DEFAULT '',
            title_en   VARCHAR NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE persons (
            id         VARCHAR PRIMARY KEY,
            name_ja    VARCHAR NOT NULL DEFAULT '',
            name_en    VARCHAR NOT NULL DEFAULT '',
            updated_at TIMESTAMP DEFAULT now()
        )
    """)
    return conn


# Minimum column sets for empty-row parquet writes (DuckDB requires a schema).
_TABLE_MIN_COLUMNS: dict[str, list[str]] = {
    "studios":              ["id", "name", "anilist_id", "is_animation_studio",
                             "country_of_origin", "favourites", "site_url"],
    "anime_studios":        ["anime_id", "studio_id", "is_main"],
    "theme_songs":          ["anime_id", "song_type", "song_title", "role", "name"],
    "episode_titles":       ["anime_id", "episode", "title"],
    "gross_studios":        ["anime_id", "studio_name", "episode"],
    "production_committee": ["anime_id", "member_name"],
    "original_work_info":   ["anime_id", "author", "publisher",
                             "label", "magazine", "serialization_type"],
    "persons":              ["id", "name_ja", "name_en", "name_ko", "name_zh",
                             "names_alt", "name_native_raw", "aliases", "nationality",
                             "mal_id", "anilist_id", "madb_id", "ann_id",
                             "image_large", "image_medium", "image_large_path",
                             "image_medium_path", "date_of_birth", "age", "gender",
                             "primary_occupations", "years_active", "hometown",
                             "blood_type", "description", "favourites", "site_url",
                             "name_priority", "display_name"],
}


def _write_parquet(
    tmp: Path,
    table: str,
    rows: list[dict],
    source: str = "seesaawiki",
) -> None:
    """Write a synthetic parquet file to the BRONZE layout.

    When rows is empty, a schema-only parquet is written using _TABLE_MIN_COLUMNS
    so that DuckDB can read the file without schema inference errors.
    """
    import pandas as pd  # pandas is available via pixi

    date_dir = tmp / f"source={source}" / f"table={table}" / "date=2026-01-01"
    date_dir.mkdir(parents=True, exist_ok=True)
    if rows:
        df = pd.DataFrame(rows)
    else:
        cols = _TABLE_MIN_COLUMNS.get(table, ["id"])
        df = pd.DataFrame(columns=cols)
    df["date"] = "2026-01-01" if not rows else df.get("date", "2026-01-01")
    df["source"] = source
    df["table"] = table
    df.to_parquet(date_dir / "data.parquet", index=False)


# ─── DDL / apply_ddl ─────────────────────────────────────────────────────────


class TestApplyDDL:
    def test_creates_all_new_tables(self) -> None:
        conn = _make_conn()
        _apply_ddl(conn)
        expected_tables = {
            "anime_production_committee",
            "anime_theme_songs",
            "anime_episode_titles",
            "anime_gross_studios",
            "anime_original_work_info",
        }
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert expected_tables.issubset(existing)

    def test_idempotent(self) -> None:
        """Calling _apply_ddl twice must not raise."""
        conn = _make_conn()
        _apply_ddl(conn)
        _apply_ddl(conn)

    def test_persons_extension_columns_added(self) -> None:
        conn = _make_conn()
        _apply_ddl(conn)
        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'persons'"
            ).fetchall()
        }
        expected_cols = {
            "name_native_raw", "aliases", "nationality",
            "primary_occupations", "years_active",
            "description", "image_large", "image_medium", "hometown",
        }
        assert expected_cols.issubset(cols)


# ─── integrate() ─────────────────────────────────────────────────────────────


class TestIntegrateStudios:
    def test_studios_loaded(self, tmp_path: Path) -> None:
        conn = _make_conn()
        _write_parquet(tmp_path, "studios", [
            {"id": "sw:s1", "name": "Studio A", "anilist_id": 1,
             "is_animation_studio": 1, "country_of_origin": "JP",
             "favourites": 100, "site_url": "https://example.com/a"},
        ])
        counts = integrate(conn, tmp_path)
        assert counts.get("studios") == 1
        assert conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0] == 1

    def test_studios_dedup(self, tmp_path: Path) -> None:
        """Inserting the same studio ID twice must not create duplicates."""
        conn = _make_conn()
        rows = [{"id": "sw:s1", "name": "Studio A", "anilist_id": None,
                 "is_animation_studio": None, "country_of_origin": None,
                 "favourites": None, "site_url": None}]
        _write_parquet(tmp_path, "studios", rows)
        integrate(conn, tmp_path)
        integrate(conn, tmp_path)
        assert conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0] == 1


class TestIntegrateAnimeStudios:
    def test_anime_studios_loaded(self, tmp_path: Path) -> None:
        conn = _make_conn()
        _write_parquet(tmp_path, "anime_studios", [
            {"anime_id": "seesaa:a1", "studio_id": "sw:s1", "is_main": 1},
        ])
        counts = integrate(conn, tmp_path)
        assert counts.get("anime_studios") == 1

    def test_anime_studios_dedup(self, tmp_path: Path) -> None:
        conn = _make_conn()
        rows = [{"anime_id": "seesaa:a1", "studio_id": "sw:s1", "is_main": 0}]
        _write_parquet(tmp_path, "anime_studios", rows)
        integrate(conn, tmp_path)
        integrate(conn, tmp_path)
        assert conn.execute("SELECT COUNT(*) FROM anime_studios").fetchone()[0] == 1


class TestIntegrateThemeSongs:
    def test_theme_songs_loaded(self, tmp_path: Path) -> None:
        conn = _make_conn()
        _write_parquet(tmp_path, "theme_songs", [
            {"anime_id": "seesaa:a1", "song_type": "OP", "song_title": "Song1",
             "role": "artist", "name": "ArtistA"},
            {"anime_id": "seesaa:a1", "song_type": "ED", "song_title": "Song2",
             "role": "composer", "name": "ComposerB"},
        ])
        counts = integrate(conn, tmp_path)
        assert counts.get("anime_theme_songs") == 2

    def test_theme_songs_unique_constraint(self, tmp_path: Path) -> None:
        """Duplicate (anime_id, song_type, song_title, role, name) is ignored."""
        conn = _make_conn()
        row = {"anime_id": "seesaa:a1", "song_type": "OP", "song_title": "S",
               "role": "artist", "name": "X"}
        _write_parquet(tmp_path, "theme_songs", [row, row])
        integrate(conn, tmp_path)
        assert conn.execute("SELECT COUNT(*) FROM anime_theme_songs").fetchone()[0] == 1


class TestIntegrateEpisodeTitles:
    def test_episode_titles_loaded(self, tmp_path: Path) -> None:
        conn = _make_conn()
        _write_parquet(tmp_path, "episode_titles", [
            {"anime_id": "seesaa:a1", "episode": 1, "title": "Episode 1 Title"},
            {"anime_id": "seesaa:a1", "episode": 2, "title": "Episode 2 Title"},
        ])
        counts = integrate(conn, tmp_path)
        assert counts.get("anime_episode_titles") == 2

    def test_episode_titles_source_default(self, tmp_path: Path) -> None:
        conn = _make_conn()
        _write_parquet(tmp_path, "episode_titles", [
            {"anime_id": "seesaa:a2", "episode": 1, "title": "First"},
        ])
        integrate(conn, tmp_path)
        source = conn.execute(
            "SELECT source FROM anime_episode_titles WHERE anime_id = 'seesaa:a2'"
        ).fetchone()[0]
        assert source == "seesaawiki"


class TestIntegrateGrossStudios:
    def test_gross_studios_loaded(self, tmp_path: Path) -> None:
        conn = _make_conn()
        _write_parquet(tmp_path, "gross_studios", [
            {"anime_id": "seesaa:a1", "studio_name": "Studio B", "episode": 1},
            {"anime_id": "seesaa:a1", "studio_name": "Studio C", "episode": None},
        ])
        counts = integrate(conn, tmp_path)
        assert counts.get("anime_gross_studios") == 2

    def test_gross_studios_dedup(self, tmp_path: Path) -> None:
        conn = _make_conn()
        rows = [{"anime_id": "seesaa:a1", "studio_name": "Studio B", "episode": 1}]
        _write_parquet(tmp_path, "gross_studios", rows)
        integrate(conn, tmp_path)
        integrate(conn, tmp_path)
        assert conn.execute("SELECT COUNT(*) FROM anime_gross_studios").fetchone()[0] == 1


class TestIntegrateProductionCommittee:
    def test_production_committee_loaded(self, tmp_path: Path) -> None:
        conn = _make_conn()
        _write_parquet(tmp_path, "production_committee", [
            {"anime_id": "seesaa:a1", "member_name": "CommitteeX"},
            {"anime_id": "seesaa:a1", "member_name": "CommitteeY"},
        ])
        counts = integrate(conn, tmp_path)
        assert counts.get("anime_production_committee") == 2

    def test_production_committee_role_label_null(self, tmp_path: Path) -> None:
        """role_label must be NULL for seesaawiki entries."""
        conn = _make_conn()
        _write_parquet(tmp_path, "production_committee", [
            {"anime_id": "seesaa:a1", "member_name": "CommitteeX"},
        ])
        integrate(conn, tmp_path)
        role = conn.execute(
            "SELECT role_label FROM anime_production_committee LIMIT 1"
        ).fetchone()[0]
        assert role is None


class TestIntegrateOriginalWorkInfo:
    def test_original_work_info_loaded(self, tmp_path: Path) -> None:
        conn = _make_conn()
        _write_parquet(tmp_path, "original_work_info", [
            {"anime_id": "seesaa:a1", "author": "AuthorA", "publisher": "PubB",
             "label": "LabelC", "magazine": "MagD", "serialization_type": "manga"},
        ])
        counts = integrate(conn, tmp_path)
        assert counts.get("anime_original_work_info") == 1

    def test_original_work_info_dedup(self, tmp_path: Path) -> None:
        """Primary key on anime_id — second insert is ignored."""
        conn = _make_conn()
        rows = [{"anime_id": "seesaa:a1", "author": "A", "publisher": None,
                 "label": None, "magazine": None, "serialization_type": None}]
        _write_parquet(tmp_path, "original_work_info", rows)
        integrate(conn, tmp_path)
        integrate(conn, tmp_path)
        assert conn.execute(
            "SELECT COUNT(*) FROM anime_original_work_info"
        ).fetchone()[0] == 1


class TestIntegratePersonsExtras:
    def test_persons_extras_filled(self, tmp_path: Path) -> None:
        """NULL-safe fill: SILVER column NULL → filled from BRONZE."""
        conn = _make_conn()
        # Pre-seed person with bare columns
        conn.execute("INSERT INTO persons (id, name_ja) VALUES ('seesaa:p1', '山田太郎')")
        _write_parquet(tmp_path, "persons", [
            {"id": "seesaa:p1", "name_ja": "山田太郎", "name_en": "Taro Yamada",
             "name_ko": "", "name_zh": "", "names_alt": "{}",
             "name_native_raw": "山田太郎", "aliases": '["alias1"]',
             "nationality": '["JP"]', "mal_id": None, "anilist_id": None,
             "madb_id": None, "ann_id": None,
             "image_large": "https://img.example.com/large.jpg",
             "image_medium": "https://img.example.com/medium.jpg",
             "image_large_path": None, "image_medium_path": None,
             "date_of_birth": None, "age": None, "gender": None,
             "primary_occupations": '["animator"]',
             "years_active": '["2010", "2020"]',
             "hometown": "Tokyo", "blood_type": None,
             "description": "A description",
             "favourites": None, "site_url": None,
             "name_priority": None, "display_name": None},
        ])
        integrate(conn, tmp_path)
        row = conn.execute(
            "SELECT name_native_raw, description, image_large, hometown, years_active "
            "FROM persons WHERE id = 'seesaa:p1'"
        ).fetchone()
        assert row[0] == "山田太郎"
        assert row[1] == "A description"
        assert row[2] == "https://img.example.com/large.jpg"
        assert row[3] == "Tokyo"
        assert row[4] == '["2010", "2020"]'

    def test_persons_extras_coalesce_preserves_existing(self, tmp_path: Path) -> None:
        """Existing non-NULL column must NOT be overwritten."""
        conn = _make_conn()
        conn.execute("INSERT INTO persons (id, name_ja) VALUES ('seesaa:p2', '佐藤花子')")
        # Manually set description
        conn.execute(
            "ALTER TABLE persons ADD COLUMN IF NOT EXISTS description TEXT"
        )
        conn.execute(
            "UPDATE persons SET description = 'existing desc' WHERE id = 'seesaa:p2'"
        )
        _write_parquet(tmp_path, "persons", [
            {"id": "seesaa:p2", "name_ja": "佐藤花子", "name_en": "",
             "name_ko": "", "name_zh": "", "names_alt": "{}",
             "name_native_raw": None, "aliases": None,
             "nationality": None, "mal_id": None, "anilist_id": None,
             "madb_id": None, "ann_id": None,
             "image_large": None, "image_medium": None,
             "image_large_path": None, "image_medium_path": None,
             "date_of_birth": None, "age": None, "gender": None,
             "primary_occupations": None,
             "years_active": None,
             "hometown": None, "blood_type": None,
             "description": "new desc should NOT overwrite",
             "favourites": None, "site_url": None,
             "name_priority": None, "display_name": None},
        ])
        integrate(conn, tmp_path)
        desc = conn.execute(
            "SELECT description FROM persons WHERE id = 'seesaa:p2'"
        ).fetchone()[0]
        assert desc == "existing desc"

    def test_persons_not_in_silver_are_skipped(self, tmp_path: Path) -> None:
        """UPDATE only affects rows that already exist in SILVER."""
        conn = _make_conn()
        # No persons seeded into SILVER
        _write_parquet(tmp_path, "persons", [
            {"id": "seesaa:p_ghost", "name_ja": "ゴースト", "name_en": "",
             "name_ko": "", "name_zh": "", "names_alt": "{}",
             "name_native_raw": "ghost", "aliases": None,
             "nationality": None, "mal_id": None, "anilist_id": None,
             "madb_id": None, "ann_id": None,
             "image_large": None, "image_medium": None,
             "image_large_path": None, "image_medium_path": None,
             "date_of_birth": None, "age": None, "gender": None,
             "primary_occupations": None,
             "years_active": None,
             "hometown": None, "blood_type": None,
             "description": None,
             "favourites": None, "site_url": None,
             "name_priority": None, "display_name": None},
        ])
        integrate(conn, tmp_path)
        count = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        assert count == 0


class TestIntegrateReturnCounts:
    def test_counts_dict_has_all_tables(self, tmp_path: Path) -> None:
        """integrate() must return a count for every table it touches."""
        conn = _make_conn()
        # Write empty parquet for all tables
        for table in [
            "studios", "anime_studios", "theme_songs", "episode_titles",
            "gross_studios", "production_committee", "original_work_info", "persons",
        ]:
            _write_parquet(tmp_path, table, [])

        counts = integrate(conn, tmp_path)
        silver_tables = {
            "studios", "anime_studios", "anime_theme_songs", "anime_episode_titles",
            "anime_gross_studios", "anime_production_committee",
            "anime_original_work_info", "persons",
        }
        # All tables must appear in counts (no _error keys expected for empty input)
        error_keys = [k for k in counts if k.endswith("_error")]
        assert not error_keys, f"Unexpected errors: {error_keys}"
        for t in silver_tables:
            assert t in counts, f"Missing count for {t}"

    def test_empty_parquet_yields_zero_rows(self, tmp_path: Path) -> None:
        conn = _make_conn()
        for table in [
            "studios", "anime_studios", "theme_songs", "episode_titles",
            "gross_studios", "production_committee", "original_work_info", "persons",
        ]:
            _write_parquet(tmp_path, table, [])
        counts = integrate(conn, tmp_path)
        assert counts["anime_theme_songs"] == 0
        assert counts["anime_episode_titles"] == 0
        assert counts["anime_gross_studios"] == 0
        assert counts["anime_original_work_info"] == 0


class TestSchemaExtension:
    def test_upgrade_seesaawiki_extension_importable(self) -> None:
        """_upgrade_seesaawiki_extension must be importable from schema.py."""
        from src.db.schema import _upgrade_seesaawiki_extension  # noqa: F401
