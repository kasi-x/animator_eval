"""Tests for sakuga atwiki BRONZE parquet writer."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from src.runtime.models import ParsedSakugaCredit, ParsedSakugaPerson
from src.scrapers.bronze_writer import write_sakuga_atwiki_bronze


def _make_person(page_id: int, name: str, credits: list[ParsedSakugaCredit]) -> ParsedSakugaPerson:
    return ParsedSakugaPerson(
        page_id=page_id,
        name=name,
        aliases=["alias_en"],
        active_since_year=2015 if credits else None,
        credits=credits,
        source_html_sha256="a" * 64,
    )


def _make_credit(work: str = "作品A", role: str = "原画", ep: int = 3) -> ParsedSakugaCredit:
    return ParsedSakugaCredit(
        work_title=work,
        work_year=2020,
        work_format="TV",
        role_raw=role,
        episode_raw=f"第{ep}話",
        episode_num=ep,
    )


@pytest.fixture()
def sample_persons() -> list[ParsedSakugaPerson]:
    return [
        _make_person(101, "山田花子", [_make_credit("作品A", "原画", 3), _make_credit("作品A", "作画監督", 7)]),
        _make_person(102, "佐藤次郎", [_make_credit("作品B", "原画", 1)]),
        _make_person(103, "田中三郎", []),  # parse failed — no credits
    ]


@pytest.fixture()
def sample_pages_meta() -> list[dict]:
    return [
        {"id": 101, "url": "https://www18.atwiki.jp/sakuga/pages/101.html",
         "title": "山田花子", "page_kind": "person",
         "discovered_at": "2026-04-25T00:00:00+00:00", "last_hash": "a" * 64},
        {"id": 102, "url": "https://www18.atwiki.jp/sakuga/pages/102.html",
         "title": "佐藤次郎", "page_kind": "person",
         "discovered_at": "2026-04-25T00:00:00+00:00", "last_hash": "b" * 64},
        {"id": 103, "url": "https://www18.atwiki.jp/sakuga/pages/103.html",
         "title": "田中三郎", "page_kind": "person",
         "discovered_at": "2026-04-25T00:00:00+00:00", "last_hash": "c" * 64},
        {"id": 200, "url": "https://www18.atwiki.jp/sakuga/pages/200.html",
         "title": "一覧ページ", "page_kind": "index",
         "discovered_at": "2026-04-25T00:00:00+00:00", "last_hash": "d" * 64},
    ]


@pytest.fixture()
def sample_raw_texts() -> dict[int, str]:
    return {
        101: "フィルモグラフィ\n作品A 第3話 原画\n作品A 第7話 作画監督",
        102: "参加作品\n作品B 第1話 原画",
        103: "テキスト " * 100,  # parse failed — lots of text but no parse result
    }


# ---------------------------------------------------------------------------
# 3 parquet files written
# ---------------------------------------------------------------------------

def test_three_parquets_written(tmp_path, sample_persons, sample_pages_meta, sample_raw_texts):
    written = write_sakuga_atwiki_bronze(
        persons=sample_persons,
        pages_metadata=sample_pages_meta,
        output_dir=tmp_path,
        date_partition="20260425",
        raw_texts=sample_raw_texts,
    )
    assert "pages" in written
    assert "persons" in written
    assert "credits" in written
    for p in written.values():
        assert Path(p).exists()


# ---------------------------------------------------------------------------
# Partition path structure
# ---------------------------------------------------------------------------

def test_partition_path_structure(tmp_path, sample_persons, sample_pages_meta):
    write_sakuga_atwiki_bronze(
        persons=sample_persons,
        pages_metadata=sample_pages_meta,
        output_dir=tmp_path,
        date_partition="20260425",
    )
    pages_dir = tmp_path / "source=sakuga_atwiki" / "table=pages" / "date=20260425"
    credits_dir = tmp_path / "source=sakuga_atwiki" / "table=credits" / "date=20260425"
    assert pages_dir.exists()
    assert credits_dir.exists()


# ---------------------------------------------------------------------------
# Row counts
# ---------------------------------------------------------------------------

def test_pages_row_count(tmp_path, sample_persons, sample_pages_meta):
    write_sakuga_atwiki_bronze(
        persons=sample_persons, pages_metadata=sample_pages_meta,
        output_dir=tmp_path, date_partition="20260425",
    )
    con = duckdb.connect()
    glob = str(tmp_path / "source=sakuga_atwiki/table=pages/date=20260425/*.parquet")
    count = con.execute(f"SELECT COUNT(*) FROM '{glob}'").fetchone()[0]
    assert count == 4  # all pages including non-person

def test_persons_row_count(tmp_path, sample_persons, sample_pages_meta):
    write_sakuga_atwiki_bronze(
        persons=sample_persons, pages_metadata=sample_pages_meta,
        output_dir=tmp_path, date_partition="20260425",
    )
    con = duckdb.connect()
    glob = str(tmp_path / "source=sakuga_atwiki/table=persons/date=20260425/*.parquet")
    count = con.execute(f"SELECT COUNT(*) FROM '{glob}'").fetchone()[0]
    assert count == 3  # 3 person pages

def test_credits_row_count(tmp_path, sample_persons, sample_pages_meta):
    write_sakuga_atwiki_bronze(
        persons=sample_persons, pages_metadata=sample_pages_meta,
        output_dir=tmp_path, date_partition="20260425",
    )
    con = duckdb.connect()
    glob = str(tmp_path / "source=sakuga_atwiki/table=credits/date=20260425/*.parquet")
    count = con.execute(f"SELECT COUNT(*) FROM '{glob}'").fetchone()[0]
    assert count == 3  # person_101 has 2, person_102 has 1, person_103 has 0


# ---------------------------------------------------------------------------
# evidence_source = "sakuga_atwiki" (H4)
# ---------------------------------------------------------------------------

def test_evidence_source(tmp_path, sample_persons, sample_pages_meta):
    write_sakuga_atwiki_bronze(
        persons=sample_persons, pages_metadata=sample_pages_meta,
        output_dir=tmp_path, date_partition="20260425",
    )
    con = duckdb.connect()
    glob = str(tmp_path / "source=sakuga_atwiki/table=credits/date=20260425/*.parquet")
    rows = con.execute(f"SELECT DISTINCT evidence_source FROM '{glob}'").fetchall()
    assert rows == [("sakuga_atwiki",)]


# ---------------------------------------------------------------------------
# parse_ok flag
# ---------------------------------------------------------------------------

def test_parse_ok_flag(tmp_path, sample_persons, sample_pages_meta):
    write_sakuga_atwiki_bronze(
        persons=sample_persons, pages_metadata=sample_pages_meta,
        output_dir=tmp_path, date_partition="20260425",
    )
    con = duckdb.connect()
    glob = str(tmp_path / "source=sakuga_atwiki/table=persons/date=20260425/*.parquet")
    rows = con.execute(
        f"SELECT page_id, parse_ok FROM '{glob}' ORDER BY page_id"
    ).fetchall()
    assert (101, True) in rows
    assert (102, True) in rows
    assert (103, False) in rows  # no credits → parse failed


# ---------------------------------------------------------------------------
# raw_wikibody_text stored (生データ保存)
# ---------------------------------------------------------------------------

def test_raw_wikibody_text_stored(tmp_path, sample_persons, sample_pages_meta, sample_raw_texts):
    write_sakuga_atwiki_bronze(
        persons=sample_persons, pages_metadata=sample_pages_meta,
        output_dir=tmp_path, date_partition="20260425",
        raw_texts=sample_raw_texts,
    )
    con = duckdb.connect()
    glob = str(tmp_path / "source=sakuga_atwiki/table=persons/date=20260425/*.parquet")
    rows = con.execute(
        f"SELECT page_id, raw_wikibody_text FROM '{glob}' WHERE page_id = 103"
    ).fetchall()
    assert rows
    assert len(rows[0][1]) > 0  # raw text preserved for the failed-parse page


def test_raw_wikibody_text_empty_when_not_provided(tmp_path, sample_persons, sample_pages_meta):
    write_sakuga_atwiki_bronze(
        persons=sample_persons, pages_metadata=sample_pages_meta,
        output_dir=tmp_path, date_partition="20260425",
        raw_texts=None,
    )
    con = duckdb.connect()
    glob = str(tmp_path / "source=sakuga_atwiki/table=persons/date=20260425/*.parquet")
    rows = con.execute(f"SELECT raw_wikibody_text FROM '{glob}'").fetchall()
    # All empty strings when raw_texts not supplied
    assert all(r[0] == "" for r in rows)


# ---------------------------------------------------------------------------
# aliases stored as JSON
# ---------------------------------------------------------------------------

def test_aliases_json(tmp_path, sample_persons, sample_pages_meta):
    write_sakuga_atwiki_bronze(
        persons=sample_persons, pages_metadata=sample_pages_meta,
        output_dir=tmp_path, date_partition="20260425",
    )
    con = duckdb.connect()
    glob = str(tmp_path / "source=sakuga_atwiki/table=persons/date=20260425/*.parquet")
    row = con.execute(f"SELECT aliases_json FROM '{glob}' WHERE page_id = 101").fetchone()
    aliases = json.loads(row[0])
    assert "alias_en" in aliases


# ---------------------------------------------------------------------------
# No subjective evaluation columns exist
# ---------------------------------------------------------------------------

def test_no_subjective_columns(tmp_path, sample_persons, sample_pages_meta):
    write_sakuga_atwiki_bronze(
        persons=sample_persons, pages_metadata=sample_pages_meta,
        output_dir=tmp_path, date_partition="20260425",
    )
    con = duckdb.connect()
    glob = str(tmp_path / "source=sakuga_atwiki/table=credits/date=20260425/*.parquet")
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{glob}'").fetchall()]
    forbidden = {"score", "rating", "quality", "evaluation", "rank"}
    assert not forbidden.intersection(set(cols))
