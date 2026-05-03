"""Tests for src/etl/conformed_loaders/keyframe.py (Card 14/06)."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.conformed_loaders.keyframe import integrate


# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_silver_conn(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    """Create a minimal in-memory SILVER DuckDB with anime / persons / studios / anime_studios."""
    conn = duckdb.connect(str(tmp_path / "silver.duckdb"))
    conn.execute("""
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
            fetched_at  TIMESTAMP,
            content_hash VARCHAR,
            updated_at  TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS persons (
            id          VARCHAR PRIMARY KEY,
            name_ja     VARCHAR NOT NULL DEFAULT '',
            name_en     VARCHAR NOT NULL DEFAULT '',
            name_ko     VARCHAR NOT NULL DEFAULT '',
            name_zh     VARCHAR NOT NULL DEFAULT '',
            names_alt   VARCHAR NOT NULL DEFAULT '{}',
            birth_date  VARCHAR,
            description TEXT,
            website_url VARCHAR,
            updated_at  TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS studios (
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
        CREATE TABLE IF NOT EXISTS anime_studios (
            anime_id  VARCHAR NOT NULL,
            studio_id VARCHAR NOT NULL,
            is_main   BOOLEAN NOT NULL DEFAULT FALSE,
            role      VARCHAR NOT NULL DEFAULT '',
            source    VARCHAR NOT NULL DEFAULT '',
            PRIMARY KEY (anime_id, studio_id, role, source)
        )
    """)
    return conn


def _write_bronze(root: Path) -> None:
    """Write minimal BRONZE parquet fixtures for all keyframe tables."""
    with BronzeWriter("keyframe", table="person_jobs", root=root, compact_on_exit=False) as bw:
        bw.append({"person_id": 101, "job": "原画"})
        bw.append({"person_id": 101, "job": "作画監督"})
        bw.append({"person_id": 102, "job": "演出"})

    with BronzeWriter("keyframe", table="person_studios", root=root, compact_on_exit=False) as bw:
        bw.append({"person_id": 101, "studio_name": "スタジオA", "alt_names": '["Studio A"]'})
        bw.append({"person_id": 102, "studio_name": "スタジオB", "alt_names": None})

    with BronzeWriter("keyframe", table="studios_master", root=root, compact_on_exit=False) as bw:
        bw.append({"studio_id": 1, "name_ja": "スタジオA", "name_en": "Studio A"})
        bw.append({"studio_id": 2, "name_ja": "スタジオB", "name_en": "Studio B"})

    with BronzeWriter("keyframe", table="anime_studios", root=root, compact_on_exit=False) as bw:
        bw.append({"anime_id": "anilist:1", "studio_name": "スタジオA", "is_main": True})

    with BronzeWriter("keyframe", table="settings_categories", root=root, compact_on_exit=False) as bw:
        bw.append({"anime_id": "anilist:1", "category_name": "キャラクター", "category_order": 1})
        bw.append({"anime_id": "anilist:1", "category_name": "メカ", "category_order": 2})

    with BronzeWriter("keyframe", table="anime", root=root, compact_on_exit=False) as bw:
        bw.append({
            "id": "anilist:1",
            "kf_uuid": "abc-123",
            "kf_status": "published",
            "slug": "test-anime",
            "delimiters": "{}",
            "episode_delimiters": "{}",
            "role_delimiters": "{}",
            "staff_delimiters": "{}",
        })

    with BronzeWriter("keyframe", table="person_profile", root=root, compact_on_exit=False) as bw:
        bw.append({
            "person_id": 101,
            "is_studio": False,
            "name_ja": "山田太郎",
            "name_en": "Taro Yamada",
            "avatar": "https://example.com/avatar.jpg",
            "bio": "アニメーター",
        })


# ─── fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    root = tmp_path / "bronze"
    _write_bronze(root)
    return root


@pytest.fixture
def silver_conn(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    conn = _make_silver_conn(tmp_path)
    # Seed anime + persons rows that keyframe will UPDATE.
    conn.execute("INSERT INTO anime (id, title_ja) VALUES ('anilist:1', 'テスト')")
    conn.execute("INSERT INTO persons (id, name_ja, name_en) VALUES ('101', '山田太郎', 'Taro Yamada')")
    return conn


# ─── tests ───────────────────────────────────────────────────────────────────


def test_integrate_returns_counts(silver_conn, bronze_dir):
    counts = integrate(silver_conn, bronze_dir)

    assert counts["person_jobs"] > 0
    assert counts["person_studio_affiliations"] > 0
    assert counts["anime_settings_categories"] > 0


def test_person_jobs_loaded(silver_conn, bronze_dir):
    integrate(silver_conn, bronze_dir)
    rows = silver_conn.execute("SELECT * FROM person_jobs").fetchall()
    assert len(rows) == 3
    jobs = {r[2] for r in rows}  # (id, person_id, job, source)
    assert "原画" in jobs
    assert "作画監督" in jobs
    assert "演出" in jobs


def test_person_jobs_dedup(silver_conn, bronze_dir):
    """Running integrate twice must not duplicate rows."""
    integrate(silver_conn, bronze_dir)
    integrate(silver_conn, bronze_dir)
    count = silver_conn.execute("SELECT COUNT(*) FROM person_jobs").fetchone()[0]
    assert count == 3


def test_person_studio_affiliations_loaded(silver_conn, bronze_dir):
    integrate(silver_conn, bronze_dir)
    count = silver_conn.execute("SELECT COUNT(*) FROM person_studio_affiliations").fetchone()[0]
    assert count == 2


def test_studios_master_loaded(silver_conn, bronze_dir):
    """studios_master rows use 'kf:s<id>' prefix."""
    integrate(silver_conn, bronze_dir)
    ids = {r[0] for r in silver_conn.execute("SELECT id FROM studios").fetchall()}
    assert "kf:s1" in ids
    assert "kf:s2" in ids


def test_anime_studios_name_based_loaded(silver_conn, bronze_dir):
    """anime_studios rows use 'kf:n:<name>' prefix for studio_id."""
    integrate(silver_conn, bronze_dir)
    studio_ids = {
        r[0]
        for r in silver_conn.execute("SELECT studio_id FROM anime_studios").fetchall()
    }
    assert "kf:n:スタジオA" in studio_ids


def test_anime_studios_no_anilist_collision(silver_conn, bronze_dir):
    """kf:n: prefix must not collide with hypothetical anilist-style IDs."""
    # Seed an anilist-style studio
    silver_conn.execute("INSERT INTO studios (id, name) VALUES ('anilist:s:999', 'Some Studio')")
    integrate(silver_conn, bronze_dir)
    all_ids = {r[0] for r in silver_conn.execute("SELECT id FROM studios").fetchall()}
    assert "anilist:s:999" in all_ids  # untouched
    # kf:n: prefixed rows are separate
    kf_ids = {i for i in all_ids if i.startswith("kf:")}
    assert len(kf_ids) > 0


def test_anime_studios_source_column_populated(silver_conn, bronze_dir):
    """anime_studios rows inserted by keyframe loader carry source='keyframe'."""
    integrate(silver_conn, bronze_dir)
    rows = silver_conn.execute(
        "SELECT source FROM anime_studios WHERE studio_id LIKE 'kf:n:%'"
    ).fetchall()
    assert len(rows) > 0
    assert all(r[0] == "keyframe" for r in rows)


def test_anime_studios_no_pk_collision_on_repeated_insert(silver_conn, bronze_dir):
    """Running integrate twice must not raise PK violation in anime_studios."""
    integrate(silver_conn, bronze_dir)
    count1 = silver_conn.execute("SELECT COUNT(*) FROM anime_studios").fetchone()[0]
    integrate(silver_conn, bronze_dir)
    count2 = silver_conn.execute("SELECT COUNT(*) FROM anime_studios").fetchone()[0]
    assert count1 == count2  # idempotent


def test_settings_categories_loaded(silver_conn, bronze_dir):
    integrate(silver_conn, bronze_dir)
    rows = silver_conn.execute("SELECT category_name, category_order FROM anime_settings_categories").fetchall()
    names = {r[0] for r in rows}
    assert "キャラクター" in names
    assert "メカ" in names


def test_anime_kf_columns_updated(silver_conn, bronze_dir):
    integrate(silver_conn, bronze_dir)
    row = silver_conn.execute(
        "SELECT kf_uuid, kf_status, kf_slug FROM anime WHERE id = 'anilist:1'"
    ).fetchone()
    assert row is not None
    kf_uuid, kf_status, kf_slug = row
    assert kf_uuid == "abc-123"
    assert kf_status == "published"
    assert kf_slug == "test-anime"


def test_persons_profile_image_large_updated(silver_conn, bronze_dir):
    integrate(silver_conn, bronze_dir)
    row = silver_conn.execute(
        "SELECT image_large, description FROM persons WHERE id = '101'"
    ).fetchone()
    assert row is not None
    image_large, description = row
    assert image_large == "https://example.com/avatar.jpg"
    assert description == "アニメーター"


def test_persons_profile_coalesce_existing_wins(silver_conn, bronze_dir):
    """Existing description/image_large must not be overwritten by keyframe."""
    silver_conn.execute(
        "ALTER TABLE persons ADD COLUMN IF NOT EXISTS description TEXT"
    )
    silver_conn.execute(
        "ALTER TABLE persons ADD COLUMN IF NOT EXISTS image_large TEXT"
    )
    silver_conn.execute(
        "UPDATE persons SET description='既存説明', image_large='https://existing.example/img.jpg' WHERE id='101'"
    )
    integrate(silver_conn, bronze_dir)
    row = silver_conn.execute(
        "SELECT image_large, description FROM persons WHERE id = '101'"
    ).fetchone()
    assert row[0] == "https://existing.example/img.jpg"
    assert row[1] == "既存説明"


def test_persons_profile_studio_rows_skipped(tmp_path):
    """is_studio=True rows in person_profile must not update persons."""
    root = tmp_path / "bronze_studio"
    with BronzeWriter("keyframe", table="person_profile", root=root, compact_on_exit=False) as bw:
        bw.append({
            "person_id": 999,
            "is_studio": True,
            "name_ja": "スタジオ名",
            "name_en": "Studio Name",
            "avatar": "https://studio.example/logo.jpg",
            "bio": "スタジオ法人",
        })
    silver_dir = tmp_path / "silver_studio"
    silver_dir.mkdir(parents=True, exist_ok=True)
    conn2 = _make_silver_conn(silver_dir)
    # No persons row for id '999'
    integrate(conn2, root)
    count = conn2.execute("SELECT COUNT(*) FROM persons WHERE id = '999'").fetchone()[0]
    assert count == 0


def test_h1_anime_score_not_in_schema(silver_conn, bronze_dir):
    """H1: score / popularity / favourites must not appear in SILVER anime."""
    integrate(silver_conn, bronze_dir)
    cols = {r[1] for r in silver_conn.execute("PRAGMA table_info('anime')").fetchall()}
    assert "score" not in cols
    assert "popularity" not in cols
    assert "favourites" not in cols


def test_idempotent_full_run(silver_conn, bronze_dir):
    """Running integrate three times must produce identical counts."""
    c1 = integrate(silver_conn, bronze_dir)
    c2 = integrate(silver_conn, bronze_dir)
    c3 = integrate(silver_conn, bronze_dir)
    assert c1 == c2 == c3
