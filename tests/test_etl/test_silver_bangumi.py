"""Tests for src/etl/silver_loaders/bangumi.py.

Creates minimal synthetic BRONZE parquet in a temp dir, builds an
in-memory SILVER duckdb (mirroring integrate_duckdb._DDL), then calls
integrate() and checks:
  - row counts for each of the 5 targets
  - H1 invariant (no bare score/rank/favorite columns in SILVER)
  - idempotency (double-run produces no duplicates)
  - role mapping (Chinese position string → normalized Role.value)
  - persons UPDATE path (birth_date / gender / blood_type COALESCE)
  - CVA JOIN (person_characters JOIN subject_characters)
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

import duckdb
import pytest

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.silver_loaders import bangumi as bangumi_loader
from src.runtime.models import Role

# ─── Minimal SILVER DDL ──────────────────────────────────────────────────────
# Mirrors the core tables from integrate_duckdb._DDL plus character tables
# from silver_loaders/anilist.py.

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
    id          VARCHAR PRIMARY KEY,
    name_ja     VARCHAR NOT NULL DEFAULT '',
    name_en     VARCHAR NOT NULL DEFAULT '',
    name_ko     VARCHAR NOT NULL DEFAULT '',
    name_zh     VARCHAR NOT NULL DEFAULT '',
    names_alt   VARCHAR NOT NULL DEFAULT '{}',
    birth_date  VARCHAR,
    death_date  VARCHAR,
    website_url VARCHAR,
    updated_at  TIMESTAMP DEFAULT now()
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
    updated_at    TIMESTAMP DEFAULT now(),
    UNIQUE (anilist_id)
);

CREATE TABLE IF NOT EXISTS credits (
    person_id       VARCHAR,
    anime_id        VARCHAR,
    role            VARCHAR NOT NULL,
    raw_role        VARCHAR NOT NULL,
    episode         INTEGER,
    evidence_source VARCHAR NOT NULL,
    affiliation     VARCHAR,
    position        INTEGER,
    updated_at      TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS character_voice_actors (
    id             INTEGER,
    character_id   VARCHAR NOT NULL,
    person_id      VARCHAR NOT NULL,
    anime_id       VARCHAR NOT NULL,
    character_role VARCHAR NOT NULL DEFAULT '',
    source         VARCHAR NOT NULL DEFAULT '',
    updated_at     TIMESTAMP DEFAULT now(),
    PRIMARY KEY (character_id, person_id, anime_id)
);
"""


def _make_silver_conn() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB with the minimal SILVER tables."""
    conn = duckdb.connect(":memory:")
    conn.execute(_SILVER_DDL)
    return conn


# ─── BRONZE fixture helpers ───────────────────────────────────────────────────

def _write_subjects(root: Path, rows: list[dict] | None = None) -> None:
    if rows is None:
        rows = [
        {
            "id": 1001,
            "type": 2,
            "name": "テストアニメ",
            "name_cn": "测试动画",
            "infobox": '{"key": "value"}',
            "platform": 1,
            "summary": "A test anime.",
            "nsfw": False,
            "tags": "[]",
            "meta_tags": "[]",
            "score": 8.5,
            "score_details": '{"1":10}',
            "rank": 42,
            "release_date": "2024-01",
            "favorite": "123",
            "series": True,
        }
    ]
    with BronzeWriter("bangumi", table="subjects", root=root) as bw:
        for r in rows:
            bw.append(r)


def _write_persons(root: Path, rows: list[dict] | None = None) -> None:
    if rows is None:
        rows = [
            {
                "id": 2001,
                "name": "山田太郎",
                "type": 1,
                "career": '["animation"]',
                "summary": "A test animator.",
                "infobox": '{}',
                "gender": "male",
                "blood_type": 1,
                "birth_year": 1985,
                "birth_mon": 3,
                "birth_day": 15,
                "images": None,
                "locked": False,
                "stat_comments": 0,
                "stat_collects": 0,
                "last_modified": "2024-01-01",
                "fetched_at": None,
            }
        ]
    with BronzeWriter("bangumi", table="persons", root=root) as bw:
        for r in rows:
            bw.append(r)


def _write_characters(root: Path, rows: list[dict] | None = None) -> None:
    if rows is None:
        rows = [
            {
                "id": 3001,
                "name": "テストキャラ",
                "type": 1,
                "locked": False,
                "nsfw": False,
                "summary": "A test character.",
                "infobox": '{}',
                "gender": "female",
                "blood_type": 2,
                "birth_year": None,
                "birth_mon": None,
                "birth_day": None,
                "images": '{"large": "https://example.com/img.png"}',
                "stat_comments": 0,
                "stat_collects": 0,
                "fetched_at": None,
            }
        ]
    with BronzeWriter("bangumi", table="characters", root=root) as bw:
        for r in rows:
            bw.append(r)


def _write_subject_persons(root: Path, rows: list[dict] | None = None) -> None:
    if rows is None:
        rows = [
            {
                "subject_id": 1001,
                "person_id": 2001,
                "position": "原画",
                "person_type": 1,
                "career": '["animation"]',
                "eps": "",
                "name_raw": "山田太郎",
                "fetched_at": None,
            }
        ]
    with BronzeWriter("bangumi", table="subject_persons", root=root) as bw:
        for r in rows:
            bw.append(r)


def _write_person_characters(root: Path, rows: list[dict] | None = None) -> None:
    if rows is None:
        rows = [
            {
                "subject_id": 1001,
                "character_id": 3001,
                "person_id": 2001,
                "actor_type": 1,
                "fetched_at": None,
                "actor_career": '["voice_actor"]',
            }
        ]
    with BronzeWriter("bangumi", table="person_characters", root=root) as bw:
        for r in rows:
            bw.append(r)


def _write_subject_characters(root: Path, rows: list[dict] | None = None) -> None:
    if rows is None:
        rows = [
            {
                "subject_id": 1001,
                "character_id": 3001,
                "relation": "主角",
                "type": 1,
                "name_raw": "テストキャラ",
                "fetched_at": None,
            }
        ]
    with BronzeWriter("bangumi", table="subject_characters", root=root) as bw:
        for r in rows:
            bw.append(r)


@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    """Write all 6 bangumi BRONZE tables with minimal valid rows."""
    root = tmp_path / "bronze"
    _write_subjects(root)
    _write_persons(root)
    _write_characters(root)
    _write_subject_persons(root)
    _write_person_characters(root)
    _write_subject_characters(root)
    return root


# ─── Tests ───────────────────────────────────────────────────────────────────

class TestIntegrateCounts:
    def test_anime_inserted(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        counts = bangumi_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["bgm_anime"] == 1

    def test_persons_inserted(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        counts = bangumi_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["bgm_persons"] == 1

    def test_characters_inserted(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        counts = bangumi_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["bgm_characters"] == 1

    def test_credits_inserted(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        counts = bangumi_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["bgm_credits"] == 1

    def test_cva_inserted(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        counts = bangumi_loader.integrate(conn, bronze_dir)
        conn.close()
        assert counts["bgm_cva"] == 1


class TestH1Invariants:
    """H1: score/rank/favorite must not appear as bare column names."""

    def test_anime_no_bare_score(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        cols = {row[0] for row in conn.execute("DESCRIBE anime").fetchall()}
        conn.close()
        assert "score" not in cols
        assert "rank" not in cols
        assert "favorite" not in cols

    def test_anime_display_columns_set(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT display_score_bgm, display_rank_bgm, display_favorite_bgm "
            "FROM anime WHERE id = 'bgm:s1001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == pytest.approx(8.5)   # display_score_bgm
        assert row[1] == 42                    # display_rank_bgm
        assert row[2] == 123                   # display_favorite_bgm


class TestIdValues:
    def test_anime_id_prefix(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        row = conn.execute("SELECT id FROM anime WHERE id LIKE 'bgm:s%'").fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "bgm:s1001"

    def test_person_id_prefix(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        row = conn.execute("SELECT id FROM persons WHERE id LIKE 'bgm:p%'").fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "bgm:p2001"

    def test_character_id_prefix(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        row = conn.execute("SELECT id FROM characters WHERE id LIKE 'bgm:c%'").fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "bgm:c3001"

    def test_credit_evidence_source(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT evidence_source FROM credits WHERE evidence_source = 'bangumi'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "bangumi"

    def test_cva_source(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT source FROM character_voice_actors WHERE source = 'bangumi'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "bangumi"


class TestRoleMapping:
    """Credits.role must be a normalized Role.value via map_role_bangumi() UDF."""

    def test_chinese_position_maps_to_role_value(self, tmp_path: Path) -> None:
        """position='原画' → role='key_animator'."""
        root = tmp_path / "bronze"
        _write_subjects(root)
        _write_persons(root)
        _write_characters(root)
        _write_subject_persons(root, rows=[
            {
                "subject_id": 1001,
                "person_id": 2001,
                "position": "原画",
                "person_type": 1,
                "career": None,
                "eps": None,
                "name_raw": "山田太郎",
                "fetched_at": None,
            }
        ])
        _write_person_characters(root)
        _write_subject_characters(root)

        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, root)
        row = conn.execute("SELECT role, raw_role FROM credits").fetchone()
        conn.close()

        assert row is not None
        assert row[0] == Role.KEY_ANIMATOR.value
        assert row[1] == "原画"

    def test_known_positions_map_correctly(self, tmp_path: Path) -> None:
        """Multiple Chinese positions all map to non-'other' Role values."""
        root = tmp_path / "bronze"
        _write_subjects(root)
        _write_characters(root)
        _write_person_characters(root, rows=[])
        _write_subject_characters(root, rows=[])

        positions_expected = [
            ("作画监督", Role.ANIMATION_DIRECTOR.value),
            ("演出",    Role.EPISODE_DIRECTOR.value),
            ("分镜",    Role.EPISODE_DIRECTOR.value),
            ("导演",    Role.DIRECTOR.value),
            ("脚本",    Role.SCREENPLAY.value),
        ]

        # Write one person per position code
        persons_rows = [
            {
                "id": 2000 + i,
                "name": f"Person{i}",
                "type": 1,
                "career": None,
                "summary": None,
                "infobox": None,
                "gender": None,
                "blood_type": None,
                "birth_year": None,
                "birth_mon": None,
                "birth_day": None,
                "images": None,
                "locked": False,
                "stat_comments": 0,
                "stat_collects": 0,
                "last_modified": None,
                "fetched_at": None,
            }
            for i in range(len(positions_expected))
        ]
        with BronzeWriter("bangumi", table="persons", root=root) as bw:
            for r in persons_rows:
                bw.append(r)

        sp_rows = [
            {
                "subject_id": 1001,
                "person_id": 2000 + i,
                "position": pos,
                "person_type": 1,
                "career": None,
                "eps": None,
                "name_raw": f"Person{i}",
                "fetched_at": None,
            }
            for i, (pos, _) in enumerate(positions_expected)
        ]
        with BronzeWriter("bangumi", table="subject_persons", root=root) as bw:
            for r in sp_rows:
                bw.append(r)

        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, root)
        credits_rows = conn.execute(
            "SELECT raw_role, role FROM credits ORDER BY raw_role"
        ).fetchall()
        conn.close()

        role_by_raw = {raw: role for raw, role in credits_rows}
        for pos, expected_role in positions_expected:
            assert role_by_raw.get(pos) == expected_role, (
                f"position={pos!r} expected {expected_role!r}, got {role_by_raw.get(pos)!r}"
            )


class TestPersonsUpdate:
    """COALESCE update path: existing values must not be overwritten."""

    def test_birth_date_constructed_from_parts(self, bronze_dir: Path) -> None:
        """birth_year/birth_mon/birth_day → persons.birth_date as YYYY-MM-DD."""
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT birth_date FROM persons WHERE id = 'bgm:p2001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "1985-03-15"

    def test_existing_birth_date_not_overwritten(self, tmp_path: Path) -> None:
        """COALESCE: if persons.birth_date already set, bangumi does not overwrite."""
        root = tmp_path / "bronze"
        _write_subjects(root)
        _write_persons(root)
        _write_characters(root)
        _write_subject_persons(root)
        _write_person_characters(root)
        _write_subject_characters(root)

        conn = _make_silver_conn()
        # Pre-insert person with existing birth_date
        conn.execute(
            "INSERT INTO persons (id, name_ja, birth_date) VALUES ('bgm:p2001', '山田', '1990-01-01')"
        )
        bangumi_loader.integrate(conn, root)
        row = conn.execute(
            "SELECT birth_date FROM persons WHERE id = 'bgm:p2001'"
        ).fetchone()
        conn.close()
        # Should still be the pre-existing value
        assert row[0] == "1990-01-01"


class TestIdempotency:
    """Calling integrate() twice must not duplicate any rows."""

    def test_anime_idempotent(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        bangumi_loader.integrate(conn, bronze_dir)
        count = conn.execute("SELECT COUNT(*) FROM anime WHERE id LIKE 'bgm:s%'").fetchone()[0]
        conn.close()
        assert count == 1

    def test_persons_idempotent(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        bangumi_loader.integrate(conn, bronze_dir)
        count = conn.execute("SELECT COUNT(*) FROM persons WHERE id LIKE 'bgm:p%'").fetchone()[0]
        conn.close()
        assert count == 1

    def test_characters_idempotent(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        bangumi_loader.integrate(conn, bronze_dir)
        count = conn.execute("SELECT COUNT(*) FROM characters WHERE id LIKE 'bgm:c%'").fetchone()[0]
        conn.close()
        assert count == 1

    def test_credits_idempotent(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        bangumi_loader.integrate(conn, bronze_dir)
        count = conn.execute(
            "SELECT COUNT(*) FROM credits WHERE evidence_source = 'bangumi'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_cva_idempotent(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        bangumi_loader.integrate(conn, bronze_dir)
        count = conn.execute(
            "SELECT COUNT(*) FROM character_voice_actors WHERE source = 'bangumi'"
        ).fetchone()[0]
        conn.close()
        assert count == 1


class TestSubjectsTypeFilter:
    """Only subjects with type=2 (anime) should be imported."""

    def test_non_anime_subjects_excluded(self, tmp_path: Path) -> None:
        root = tmp_path / "bronze"
        # type=1 is books, type=3 is music — neither should be imported
        _write_subjects(root, rows=[
            {"id": 1001, "type": 2, "name": "Anime A", "name_cn": "", "infobox": None,
             "platform": 1, "summary": None, "nsfw": False, "tags": None,
             "meta_tags": None, "score": 7.0, "score_details": None, "rank": 10,
             "release_date": "2024", "favorite": "5", "series": False},
            {"id": 9001, "type": 1, "name": "Manga B", "name_cn": "", "infobox": None,
             "platform": 0, "summary": None, "nsfw": False, "tags": None,
             "meta_tags": None, "score": None, "score_details": None, "rank": None,
             "release_date": None, "favorite": None, "series": False},
        ])
        _write_persons(root, rows=[])
        _write_characters(root, rows=[])
        _write_subject_persons(root, rows=[])
        _write_person_characters(root, rows=[])
        _write_subject_characters(root, rows=[])

        conn = _make_silver_conn()
        counts = bangumi_loader.integrate(conn, root)
        conn.close()

        assert counts["bgm_anime"] == 1  # only type=2 row

    def test_anime_dedup_latest_date(self, tmp_path: Path) -> None:
        """Two date partitions with same subject_id → keep newest name."""
        root = tmp_path / "bronze"

        with BronzeWriter("bangumi", table="subjects", root=root,
                          date=_dt.date(2026, 4, 22)) as bw:
            bw.append({"id": 5001, "type": 2, "name": "OLD_NAME", "name_cn": "",
                        "infobox": None, "platform": 1, "summary": None,
                        "nsfw": False, "tags": None, "meta_tags": None,
                        "score": None, "score_details": None, "rank": None,
                        "release_date": None, "favorite": None, "series": False})

        with BronzeWriter("bangumi", table="subjects", root=root,
                          date=_dt.date(2026, 4, 23)) as bw:
            bw.append({"id": 5001, "type": 2, "name": "NEW_NAME", "name_cn": "",
                        "infobox": None, "platform": 1, "summary": None,
                        "nsfw": False, "tags": None, "meta_tags": None,
                        "score": None, "score_details": None, "rank": None,
                        "release_date": None, "favorite": None, "series": False})

        _write_persons(root, rows=[])
        _write_characters(root, rows=[])
        _write_subject_persons(root, rows=[])
        _write_person_characters(root, rows=[])
        _write_subject_characters(root, rows=[])

        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, root)
        row = conn.execute("SELECT title_ja FROM anime WHERE id = 'bgm:s5001'").fetchone()
        conn.close()

        assert row is not None
        assert row[0] == "NEW_NAME"


class TestCVAJoin:
    """CVA: character_role comes from subject_characters via LEFT JOIN."""

    def test_character_role_from_subject_characters(self, bronze_dir: Path) -> None:
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        row = conn.execute(
            "SELECT character_role FROM character_voice_actors WHERE source = 'bangumi'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "主角"

    def test_cva_without_matching_subject_character(self, tmp_path: Path) -> None:
        """person_characters with no matching subject_characters row → character_role=''."""
        root = tmp_path / "bronze"
        _write_subjects(root)
        _write_persons(root)
        _write_characters(root)
        _write_subject_persons(root, rows=[])
        _write_person_characters(root)
        # Empty subject_characters — no relation data
        _write_subject_characters(root, rows=[])

        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, root)
        row = conn.execute(
            "SELECT character_role FROM character_voice_actors WHERE source = 'bangumi'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == ""


class TestBangumiRoleMapperExtension:
    """Unit tests for the zh-string extension of the bangumi role mapper."""

    def test_known_zh_positions_map_correctly(self) -> None:
        from src.etl.role_mappers import map_role
        cases = [
            ("原画",    Role.KEY_ANIMATOR.value),
            ("作画监督", Role.ANIMATION_DIRECTOR.value),
            ("演出",    Role.EPISODE_DIRECTOR.value),
            ("分镜",    Role.EPISODE_DIRECTOR.value),
            ("补间动画", Role.IN_BETWEEN.value),
            ("脚本",    Role.SCREENPLAY.value),
            ("导演",    Role.DIRECTOR.value),
            ("音响监督", Role.SOUND_DIRECTOR.value),
            ("音乐",    Role.MUSIC.value),
            ("背景美术", Role.BACKGROUND_ART.value),
        ]
        for zh, expected in cases:
            assert map_role("bangumi", zh) == expected, (
                f"{zh!r}: expected {expected!r}"
            )

    def test_integer_code_still_works(self) -> None:
        from src.etl.role_mappers import map_role
        assert map_role("bangumi", "2") == Role.DIRECTOR.value
        assert map_role("bangumi", "20") == Role.KEY_ANIMATOR.value

    def test_unknown_zh_returns_other(self) -> None:
        from src.etl.role_mappers import map_role
        assert map_role("bangumi", "完全に未知の役職XYZ") == Role.OTHER.value


class TestCollectCount:
    """Card 20/03: display_collect_count_bgm = sum of wish+done+doing+on_hold+dropped."""

    def test_collect_count_computed_from_json_favorite(self, tmp_path: Path) -> None:
        """favorite JSON with collection categories → display_collect_count_bgm = their sum."""
        root = tmp_path / "bronze"
        _write_subjects(root, rows=[
            {
                "id": 7001,
                "type": 2,
                "name": "CollectAnime",
                "name_cn": "",
                "infobox": None,
                "platform": 1,
                "summary": None,
                "nsfw": False,
                "tags": None,
                "meta_tags": None,
                "score": 7.0,
                "score_details": None,
                "rank": 100,
                "release_date": "2024",
                # favorite as JSON collection dict (bangumi API format)
                "favorite": '{"wish": 100, "done": 500, "doing": 50, "on_hold": 20, "dropped": 10}',
                "series": False,
            }
        ])
        _write_persons(root, rows=[])
        _write_characters(root, rows=[])
        _write_subject_persons(root, rows=[])
        _write_person_characters(root, rows=[])
        _write_subject_characters(root, rows=[])

        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, root)
        row = conn.execute(
            "SELECT display_collect_count_bgm FROM anime WHERE id = 'bgm:s7001'"
        ).fetchone()
        conn.close()

        assert row is not None
        # 100 + 500 + 50 + 20 + 10 = 680
        assert row[0] == 680

    def test_collect_count_column_exists(self, bronze_dir: Path) -> None:
        """display_collect_count_bgm column is created by the loader DDL."""
        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, bronze_dir)
        cols = {row[0] for row in conn.execute("DESCRIBE anime").fetchall()}
        conn.close()
        assert "display_collect_count_bgm" in cols

    def test_collect_count_null_safe_for_non_json_favorite(self, tmp_path: Path) -> None:
        """favorite='123' (plain integer string) → display_collect_count_bgm = 0 (JSON misses)."""
        root = tmp_path / "bronze"
        # The standard fixture has favorite="123" (plain integer string, not JSON dict)
        _write_subjects(root)  # uses default fixture with favorite="123"
        _write_persons(root, rows=[])
        _write_characters(root, rows=[])
        _write_subject_persons(root, rows=[])
        _write_person_characters(root, rows=[])
        _write_subject_characters(root, rows=[])

        conn = _make_silver_conn()
        bangumi_loader.integrate(conn, root)
        row = conn.execute(
            "SELECT display_collect_count_bgm FROM anime WHERE id = 'bgm:s1001'"
        ).fetchone()
        conn.close()

        assert row is not None
        # json_extract_string on non-JSON returns NULL → COALESCE to 0 → sum = 0
        assert row[0] == 0
