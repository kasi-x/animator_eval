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

    with BronzeWriter("anilist", table="credits", root=root) as bw:
        bw.append({
            "anime_id": "anilist:1",
            "person_id": "anilist:p1",
            "role": "director",
            "raw_role": "Director",
            "episode": None,
            "evidence_source": "anilist",
            "source": "anilist",
            "credit_year": 2024,
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
    """H1: anime.score must not appear in SILVER anime table."""
    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=bronze_dir, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    # DuckDB PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
    cols = {row[1] for row in conn.execute("PRAGMA table_info('anime')").fetchall()}
    conn.close()

    assert "score" not in cols
    assert "popularity" not in cols
    assert "favourites" not in cols
    assert "description" not in cols


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
    """H4: credits.evidence_source must be retained."""
    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=bronze_dir, silver_path=silver)

    conn = duckdb.connect(str(silver), read_only=True)
    row = conn.execute("SELECT evidence_source FROM credits").fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "anilist"


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
    with BronzeWriter("anilist", table="credits", root=root) as bw:
        bw.append({
            "anime_id": "anilist:1", "person_id": "p1", "role": "director",
            "raw_role": None, "episode": None, "evidence_source": "anilist",
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

    # Two identical credit rows
    dup_credit = {
        "anime_id": "a1", "person_id": "p1", "role": "director",
        "raw_role": None, "episode": None, "evidence_source": "anilist",
    }
    with BronzeWriter("anilist", table="credits", root=root, date=_dt.date(2026, 4, 22)) as bw:
        bw.append(dup_credit)
    with BronzeWriter("anilist", table="credits", root=root, date=_dt.date(2026, 4, 23)) as bw:
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
    with BronzeWriter("anilist", table="credits", root=root) as bw:
        bw.append({"anime_id": "anilist:1", "person_id": "p1", "role": "director",
                   "raw_role": None, "episode": None, "evidence_source": "anilist"})

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
    with BronzeWriter("anilist", table="credits", root=root) as bw:
        bw.append({"anime_id": "a1", "person_id": "p1", "role": "director",
                   "raw_role": None, "episode": None, "evidence_source": "anilist"})
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
