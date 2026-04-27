"""Tests for src/etl/integrate_duckdb.py."""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

import duckdb
import pytest

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.integrate_duckdb import integrate


@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    """Write minimal valid BRONZE parquet for all three tables."""
    root = tmp_path / "bronze"

    with BronzeWriter("anilist", table="anime", root=root) as bw:
        bw.append({
            "id": "anilist:1",
            "title_ja": "テスト",
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
            "score": 7.5,  # BRONZE keeps score; SILVER must exclude it
            "fetched_at": "2024-04-24T12:00:00",
            "content_hash": "abc123def456",
        })

    with BronzeWriter("anilist", table="persons", root=root) as bw:
        bw.append({
            "id": "anilist:p1",
            "name_ja": "山田太郎",
            "name_en": "Taro Yamada",
            "name_ko": "",
            "name_zh": "",
            "names_alt": "{}",
            "date_of_birth": "1985-03-15",
            "site_url": "https://example.com",
        })

    # Use seesaawiki source so the credits loader picks it up.
    # (anilist/ann/mal credits loaders are not yet implemented in this ETL module.)
    with BronzeWriter("seesaawiki", table="credits", root=root) as bw:
        bw.append({
            "anime_id": "anilist:1",
            "person_id": None,
            "role": "監督",
            "episode": None,
            "evidence_source": "seesaawiki",
            "source": "seesaawiki",
            "position": None,
        })

    return root


def test_integrate_creates_silver(bronze_dir: Path, tmp_path: Path) -> None:
    silver = tmp_path / "silver.duckdb"
    counts = integrate(bronze_root=bronze_dir, silver_path=silver)

    assert silver.exists()
    assert counts["anime"] == 1
    assert counts["persons"] == 1
    assert counts["credits"] == 1


def test_anime_score_excluded_from_silver(bronze_dir: Path, tmp_path: Path) -> None:
    """H1: anime.score must not appear in SILVER anime table.

    Note: description is a legitimate display column added by the anilist loader
    (display metadata, not a scoring signal), so it is not checked here.
    """
    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=bronze_dir, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    # DuckDB PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
    conn.close()

    # H1: bare scoring/popularity columns must not appear in SILVER anime.
    # display_* prefixed columns are permitted (display metadata only).
    assert "score" not in cols
    assert "popularity" not in cols
    assert "favourites" not in cols


def test_persons_birth_date_mapped(bronze_dir: Path, tmp_path: Path) -> None:
    """date_of_birth (BRONZE) → birth_date (SILVER)."""
    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=bronze_dir, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    row = conn.execute("SELECT birth_date, website_url FROM persons WHERE id='anilist:p1'").fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "1985-03-15"
    assert row[1] == "https://example.com"


def test_credits_evidence_source_preserved(bronze_dir: Path, tmp_path: Path) -> None:
    """H4: credits.evidence_source must be retained (seesaawiki loader)."""
    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=bronze_dir, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    row = conn.execute("SELECT evidence_source FROM credits").fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "seesaawiki"


def test_integrate_atomic_replaces_stale_file(bronze_dir: Path, tmp_path: Path) -> None:
    """An old / corrupt silver.duckdb is replaced atomically."""
    silver = tmp_path / "silver.duckdb"
    silver.write_bytes(b"OLD_GARBAGE")

    integrate(bronze_root=bronze_dir, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    assert conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0] == 1
    conn.close()


def test_dedup_keeps_latest_date(tmp_path: Path) -> None:
    """When the same id appears in two date partitions, keep the newer one."""
    root = tmp_path / "bronze"

    with BronzeWriter("anilist", table="anime", root=root, date=_dt.date(2026, 4, 22)) as bw:
        bw.append({
            "id": "anilist:1", "title_ja": "OLD_TITLE", "title_en": "",
            "year": 2024, "season": None, "quarter": None, "episodes": 12,
            "format": "TV", "duration": 24, "start_date": None, "end_date": None,
            "status": "FINISHED", "original_work_type": None, "source": None,
            "work_type": None, "scale_class": None,
            "fetched_at": "2026-04-22T12:00:00", "content_hash": "old_hash",
        })

    with BronzeWriter("anilist", table="anime", root=root, date=_dt.date(2026, 4, 23)) as bw:
        bw.append({
            "id": "anilist:1", "title_ja": "NEW_TITLE", "title_en": "",
            "year": 2024, "season": None, "quarter": None, "episodes": 13,
            "format": "TV", "duration": 24, "start_date": None, "end_date": None,
            "status": "FINISHED", "original_work_type": None, "source": None,
            "work_type": None, "scale_class": None,
            "fetched_at": "2026-04-23T12:00:00", "content_hash": "new_hash",
        })

    # Need at least empty persons and credits globs (integrate skips missing)
    with BronzeWriter("anilist", table="persons", root=root) as bw:
        bw.append({"id": "p1", "name_ja": "X", "name_en": "", "name_ko": "", "name_zh": "", "names_alt": "{}"})
    with BronzeWriter("seesaawiki", table="credits", root=root) as bw:
        bw.append({
            "anime_id": "anilist:1", "person_id": None, "role": "監督",
            "episode": None, "evidence_source": "seesaawiki", "source": "seesaawiki",
            "position": None,
        })

    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=root, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    row = conn.execute("SELECT title_ja, episodes FROM anime WHERE id='anilist:1'").fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "NEW_TITLE"
    assert row[1] == 13


def test_credits_dedup_on_unique_key(tmp_path: Path) -> None:
    """Duplicate (person_id, anime_id, role, episode, evidence_source) is inserted once."""
    root = tmp_path / "bronze"

    with BronzeWriter("anilist", table="anime", root=root) as bw:
        bw.append({"id": "a1", "title_ja": "A", "title_en": "", "year": 2024,
                   "season": None, "quarter": None, "episodes": 1, "format": "TV",
                   "duration": 24, "start_date": None, "end_date": None,
                   "status": None, "original_work_type": None, "source": None,
                   "work_type": None, "scale_class": None,
                   "fetched_at": "2026-04-24T12:00:00", "content_hash": "hash_a1"})

    with BronzeWriter("anilist", table="persons", root=root) as bw:
        bw.append({"id": "p1", "name_ja": "A", "name_en": "", "name_ko": "", "name_zh": "", "names_alt": "{}"})

    # Two identical credit rows using seesaawiki source so the loader picks them up.
    dup_credit = {
        "anime_id": "a1", "person_id": None, "role": "監督",
        "episode": None, "evidence_source": "seesaawiki", "source": "seesaawiki",
        "position": None,
    }
    with BronzeWriter("seesaawiki", table="credits", root=root, date=_dt.date(2026, 4, 22)) as bw:
        bw.append(dup_credit)
    with BronzeWriter("seesaawiki", table="credits", root=root, date=_dt.date(2026, 4, 23)) as bw:
        bw.append(dup_credit)

    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=root, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    count = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
    conn.close()

    assert count == 1


def test_multi_source_merge(tmp_path: Path) -> None:
    """Two sources (anilist + ann) writing the same tables are merged."""
    root = tmp_path / "bronze"

    with BronzeWriter("anilist", table="anime", root=root) as bw:
        bw.append({"id": "anilist:1", "title_ja": "A", "title_en": "", "year": 2024,
                   "season": None, "quarter": None, "episodes": 12, "format": "TV",
                   "duration": 24, "start_date": None, "end_date": None,
                   "status": None, "original_work_type": None, "source": None,
                   "work_type": None, "scale_class": None,
                   "fetched_at": "2026-04-24T12:00:00", "content_hash": "hash_anilist1"})

    with BronzeWriter("ann", table="anime", root=root) as bw:
        bw.append({"id": "ann:42", "title_ja": "B", "title_en": "", "year": 2023,
                   "season": None, "quarter": None, "episodes": 26, "format": None,
                   "duration": None, "start_date": None, "end_date": None,
                   "status": None, "original_work_type": None, "source": None,
                   "work_type": None, "scale_class": None,
                   "fetched_at": "2026-04-24T12:00:00", "content_hash": "hash_ann42"})

    with BronzeWriter("anilist", table="persons", root=root) as bw:
        bw.append({"id": "p1", "name_ja": "X", "name_en": "", "name_ko": "", "name_zh": "", "names_alt": "{}"})
    with BronzeWriter("seesaawiki", table="credits", root=root) as bw:
        bw.append({"anime_id": "anilist:1", "person_id": None, "role": "監督",
                   "episode": None, "evidence_source": "seesaawiki", "source": "seesaawiki",
                   "position": None})

    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=root, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    count = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
    conn.close()

    assert count == 2


def test_studios_loaded_when_parquet_exists(tmp_path: Path) -> None:
    """studios + anime_studios are populated when bronze parquet is present."""
    root = tmp_path / "bronze"

    with BronzeWriter("anilist", table="anime", root=root) as bw:
        bw.append({"id": "a1", "title_ja": "A", "title_en": "", "year": 2024,
                   "season": None, "quarter": None, "episodes": 12, "format": "TV",
                   "duration": 24, "start_date": None, "end_date": None,
                   "status": None, "original_work_type": None, "source": None,
                   "work_type": None, "scale_class": None,
                   "fetched_at": "2026-04-24T12:00:00", "content_hash": "hash_a1_studios"})
    with BronzeWriter("anilist", table="persons", root=root) as bw:
        bw.append({"id": "p1", "name_ja": "X", "name_en": "", "name_ko": "", "name_zh": "", "names_alt": "{}"})
    with BronzeWriter("seesaawiki", table="credits", root=root) as bw:
        bw.append({"anime_id": "a1", "person_id": None, "role": "監督",
                   "episode": None, "evidence_source": "seesaawiki", "source": "seesaawiki",
                   "position": None})
    with BronzeWriter("anilist", table="studios", root=root) as bw:
        bw.append({"id": "anilist:s1", "name": "スタジオA", "anilist_id": 1,
                   "is_animation_studio": True, "country_of_origin": "JP",
                   "favourites": 100, "site_url": None})
    with BronzeWriter("anilist", table="anime_studios", root=root) as bw:
        bw.append({"anime_id": "a1", "studio_id": "anilist:s1", "is_main": True})

    silver = tmp_path / "silver.duckdb"
    counts = integrate(bronze_root=root, silver_path=silver)

    assert counts["studios"] == 1
    assert counts["anime_studios"] == 1

    conn = duckdb.connect(str(silver), read_only=True)
    row = conn.execute("SELECT name, is_animation_studio FROM studios WHERE id='anilist:s1'").fetchone()
    link = conn.execute("SELECT is_main FROM anime_studios WHERE anime_id='a1'").fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "スタジオA"
    assert row[1] is True
    assert link is not None
    assert link[0] is True


def test_studios_skipped_gracefully_when_no_parquet(bronze_dir: Path, tmp_path: Path) -> None:
    """integrate() succeeds with no studio parquet — studios/anime_studios absent from counts."""
    silver = tmp_path / "silver.duckdb"
    counts = integrate(bronze_root=bronze_dir, silver_path=silver)

    assert "studios" not in counts
    assert "anime_studios" not in counts

    # Tables still exist with zero rows
    conn = duckdb.connect(str(silver), read_only=True)
    n = conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
    conn.close()
    assert n == 0


# ---------------------------------------------------------------------------
# New source credit loader tests
# ---------------------------------------------------------------------------

def _minimal_bronze(root: Path) -> None:
    """Write the minimal anime + persons bronze rows required by integrate()."""
    with BronzeWriter("anilist", table="anime", root=root) as bw:
        bw.append({
            "id": "anilist:99", "title_ja": "テスト", "title_en": "Test",
            "year": 2024, "season": None, "quarter": None, "episodes": 12,
            "format": "TV", "duration": 24, "start_date": None, "end_date": None,
            "status": None, "original_work_type": None, "source": None,
            "work_type": None, "scale_class": None,
            "fetched_at": "2026-04-26T00:00:00", "content_hash": "hash_99",
        })
    with BronzeWriter("anilist", table="persons", root=root) as bw:
        bw.append({
            "id": "anilist:p1", "name_ja": "A", "name_en": "", "name_ko": "",
            "name_zh": "", "names_alt": "{}",
        })


def test_anilist_credits_loaded(tmp_path: Path) -> None:
    """AniList credits are inserted into silver with correct role, raw_role, evidence_source."""
    root = tmp_path / "bronze"
    _minimal_bronze(root)

    with BronzeWriter("anilist", table="credits", root=root) as bw:
        bw.append({
            "person_id": "anilist:p1",
            "anime_id": "anilist:99",
            "role": "director",
            "raw_role": "Director",
            "episode": None,
            "source": "anilist",
            "affiliation": None,
            "position": None,
        })

    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=root, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    row = conn.execute(
        "SELECT role, raw_role, evidence_source FROM credits WHERE evidence_source = 'anilist'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "director"
    assert row[1] == "Director"
    assert row[2] == "anilist"


def test_anilist_credits_raw_role_not_null(tmp_path: Path) -> None:
    """AniList credit raw_role falls back to role when raw_role is absent in bronze."""
    root = tmp_path / "bronze"
    _minimal_bronze(root)

    # Write a credit with no raw_role column (simulate older parquet)
    with BronzeWriter("anilist", table="credits", root=root) as bw:
        bw.append({
            "person_id": "anilist:p1",
            "anime_id": "anilist:99",
            "role": "key_animator",
            "raw_role": None,  # NULL — fallback to role
            "episode": None,
            "source": "anilist",
            "affiliation": None,
            "position": None,
        })

    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=root, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    row = conn.execute(
        "SELECT raw_role FROM credits WHERE evidence_source = 'anilist'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] is not None  # COALESCE fallback ensures NOT NULL


def test_ann_credits_loaded_with_prefixed_ids(tmp_path: Path) -> None:
    """ANN credits get prefixed IDs ('ann:p<id>', 'ann:a<id>') and correct raw_role."""
    root = tmp_path / "bronze"
    _minimal_bronze(root)

    with BronzeWriter("ann", table="credits", root=root) as bw:
        bw.append({
            "ann_person_id": 37480,
            "ann_anime_id": 4557,
            "role": "director",
            "task_raw": "Director",
            "gid": 2684520308,
            "name_en": "Foo Bar",
            "source": "ann",
        })

    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=root, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    row = conn.execute(
        "SELECT person_id, anime_id, role, raw_role, evidence_source "
        "FROM credits WHERE evidence_source = 'ann'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "ann:p37480"
    assert row[1] == "ann:a4557"
    assert row[2] == "director"
    assert row[3] == "Director"
    assert row[4] == "ann"


def test_keyframe_credits_loaded_studio_roles_excluded(tmp_path: Path) -> None:
    """Keyframe credits: person rows inserted, is_studio_role=TRUE rows excluded."""
    root = tmp_path / "bronze"
    _minimal_bronze(root)

    with BronzeWriter("keyframe", table="credits", root=root) as bw:
        # Person credit — should be inserted
        bw.append({
            "person_id": "keyframe:p1",
            "anime_id": "keyframe:a1",
            "role": "key_animator",
            "raw_role": "原画",
            "episode": None,
            "is_studio_role": False,
            "source": "keyframe",
        })
        # Studio credit — must be excluded
        bw.append({
            "person_id": "keyframe:studio1",
            "anime_id": "keyframe:a1",
            "role": "other",
            "raw_role": "制作",
            "episode": None,
            "is_studio_role": True,
            "source": "keyframe",
        })

    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=root, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    rows = conn.execute(
        "SELECT person_id, role, raw_role FROM credits WHERE evidence_source = 'keyframe'"
    ).fetchall()
    conn.close()

    assert len(rows) == 1
    assert rows[0][0] == "keyframe:p1"
    assert rows[0][1] == "key_animator"
    assert rows[0][2] == "原画"


def test_keyframe_episode_minus_one_becomes_null(tmp_path: Path) -> None:
    """Keyframe episode=-1 (unknown) is converted to NULL in silver."""
    root = tmp_path / "bronze"
    _minimal_bronze(root)

    with BronzeWriter("keyframe", table="credits", root=root) as bw:
        bw.append({
            "person_id": "keyframe:p2",
            "anime_id": "keyframe:a1",
            "role": "director",
            "raw_role": "監督",
            "episode": -1,
            "is_studio_role": False,
            "source": "keyframe",
        })

    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=root, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    row = conn.execute(
        "SELECT episode FROM credits WHERE evidence_source = 'keyframe' AND person_id = 'keyframe:p2'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] is None


def test_sakuga_atwiki_credits_loaded_with_null_anime_id(tmp_path: Path) -> None:
    """Sakuga@wiki credits are inserted with prefixed person_id and NULL anime_id.

    anime_id is NULL because sakuga_atwiki bronze only has work_title, not an
    anime_id.  The silver.credits DDL does not enforce NOT NULL on anime_id, so
    these rows are accepted.
    """
    root = tmp_path / "bronze"
    _minimal_bronze(root)

    with BronzeWriter("sakuga_atwiki", table="credits", root=root) as bw:
        bw.append({
            "person_page_id": 42,
            "work_title": "AKIRA",
            "work_year": 1988,
            "work_format": "movie",
            "role_raw": "原画",
            "episode_raw": None,
            "episode_num": None,
            "evidence_source": "sakuga_atwiki",
            "source": "sakuga_atwiki",
        })

    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=root, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    row = conn.execute(
        "SELECT person_id, anime_id, role, raw_role, evidence_source "
        "FROM credits WHERE evidence_source = 'sakuga_atwiki'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "sakuga:p42"
    assert row[1] is None          # anime_id is NULL — pending title resolution
    assert row[2] == "key_animator"
    assert row[3] == "原画"
    assert row[4] == "sakuga_atwiki"
