"""Integration tests for scrape_bangumi_persons.py orchestrator.

Stubs:
- BangumiClient.fetch_person → returns canned dict (no httpx)
- _CHECKPOINT_PATH → tmp_path
- BronzeWriter root → tmp_path
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

import scripts.scrape_bangumi_persons as pers_mod
from scripts.scrape_bangumi_persons import (
    _build_person_row,
    _scrape,
    _to_int32,
)

# ---------------------------------------------------------------------------
# Canned person API response
# ---------------------------------------------------------------------------

_CANNED_PERSON = {
    "id": 0,  # will be overridden per call
    "name": "Test Person",
    "type": 1,
    "career": ["animator", "director"],
    "summary": "A detailed summary.",
    "infobox": [
        {"key": "生日", "value": [{"v": "1985-03-15"}]},
        {"key": "国籍", "value": [{"v": "Japan"}]},
    ],
    "gender": "male",
    "blood_type": 1,
    "birth_year": 1985,
    "birth_mon": 3,
    "birth_day": 15,
    "images": {"small": "https://img/s.jpg", "grid": "https://img/g.jpg",
               "large": "https://img/l.jpg", "medium": "https://img/m.jpg"},
    "locked": False,
    "stat": {"comments": 42, "collects": 300},
    "last_modified": "2026-01-01T00:00:00Z",
}


# ---------------------------------------------------------------------------
# Stub BangumiClient
# ---------------------------------------------------------------------------


class _StubBangumiClient:
    """Returns canned person data keyed by person_id (without HTTP)."""

    def __init__(self, *args, **kwargs):
        self.fetch_calls: list[int] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def fetch_person(self, person_id: int):
        self.fetch_calls.append(person_id)
        person = dict(_CANNED_PERSON)
        person["id"] = person_id
        return person


# ---------------------------------------------------------------------------
# Tests: _build_person_row (unit)
# ---------------------------------------------------------------------------


def test_build_person_row_infobox_is_json_string():
    """infobox must be serialised as a JSON string, not a Python list."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_person_row(dict(_CANNED_PERSON, id=1), fetched_at)
    assert isinstance(row["infobox"], str)
    parsed = json.loads(row["infobox"])
    assert isinstance(parsed, list)
    assert parsed[0]["key"] == "生日"


def test_build_person_row_stat_split():
    """stat dict is split into stat_comments / stat_collects."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_person_row(dict(_CANNED_PERSON, id=1), fetched_at)
    assert row["stat_comments"] == 42
    assert row["stat_collects"] == 300
    assert "stat" not in row


def test_build_person_row_career_is_json_string():
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_person_row(dict(_CANNED_PERSON, id=1), fetched_at)
    assert isinstance(row["career"], str)
    parsed = json.loads(row["career"])
    assert parsed == ["animator", "director"]


def test_build_person_row_images_is_json_string():
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_person_row(dict(_CANNED_PERSON, id=1), fetched_at)
    assert isinstance(row["images"], str)
    imgs = json.loads(row["images"])
    assert "small" in imgs


def test_build_person_row_locked_is_bool():
    """locked must be present as a bool."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_person_row(dict(_CANNED_PERSON, id=1), fetched_at)
    assert "locked" in row
    assert isinstance(row["locked"], bool)
    assert row["locked"] is False


def test_build_person_row_locked_defaults_to_false_when_absent():
    """locked must default to False when the API field is absent."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    person_no_locked = {k: v for k, v in _CANNED_PERSON.items() if k != "locked"}
    person_no_locked["id"] = 1
    row = _build_person_row(person_no_locked, fetched_at)
    assert row["locked"] is False


def test_to_int32_none_returns_none():
    assert _to_int32(None) is None


def test_to_int32_valid_int():
    assert _to_int32(5) == 5


def test_to_int32_invalid_string_returns_none():
    assert _to_int32("abc") is None


# ---------------------------------------------------------------------------
# Tests: _scrape (end-to-end with stub + tmp_path)
# ---------------------------------------------------------------------------


def test_scrape_persons_writes_parquet_with_3_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """_scrape writes a persons parquet containing exactly 3 rows."""
    monkeypatch.setattr(pers_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "checkpoint_persons.json"
    monkeypatch.setattr(pers_mod, "_CHECKPOINT_PATH", checkpoint_path)

    person_ids = [101, 102, 103]
    checkpoint: dict = {"completed_ids": [], "failed_ids": [], "last_run_at": None}

    asyncio.run(_scrape(person_ids, checkpoint, "2026-01-01", dry_run=False))

    files = list((tmp_path / "source=bangumi" / "table=persons").rglob("*.parquet"))
    assert files
    tbl = pq.read_table(files[0])
    assert tbl.num_rows == 3


def test_scrape_persons_schema(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Persons parquet must contain expected columns including locked."""
    monkeypatch.setattr(pers_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(pers_mod, "_CHECKPOINT_PATH", checkpoint_path)

    asyncio.run(_scrape([1, 2, 3], {"completed_ids": [], "failed_ids": [], "last_run_at": None},
                        "2026-01-01", dry_run=False))

    files = list((tmp_path / "source=bangumi" / "table=persons").rglob("*.parquet"))
    schema = pq.read_schema(files[0])
    col_names = set(schema.names)
    expected = {
        "id", "name", "type", "infobox", "stat_comments", "stat_collects",
        "career", "images", "last_modified", "locked",
    }
    assert expected <= col_names, f"Missing columns: {expected - col_names}"


def test_scrape_persons_infobox_stored_as_json_string(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Verify infobox column in parquet is a JSON string, not a native list."""
    import pyarrow as pa
    monkeypatch.setattr(pers_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(pers_mod, "_CHECKPOINT_PATH", checkpoint_path)

    asyncio.run(_scrape([1], {"completed_ids": [], "failed_ids": [], "last_run_at": None},
                        "2026-01-01", dry_run=False))

    files = list((tmp_path / "source=bangumi" / "table=persons").rglob("*.parquet"))
    tbl = pq.read_table(files[0])
    assert tbl.schema.field("infobox").type == pa.string()
    val = tbl.column("infobox")[0].as_py()
    assert isinstance(val, str)
    json.loads(val)  # should not raise


def test_scrape_persons_checkpoint_updated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """completed_ids in checkpoint must reflect all scrapped IDs."""
    monkeypatch.setattr(pers_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(pers_mod, "_CHECKPOINT_PATH", checkpoint_path)

    checkpoint = {"completed_ids": [], "failed_ids": [], "last_run_at": None}
    asyncio.run(_scrape([10, 20, 30], checkpoint, "2026-01-01", dry_run=False))

    assert sorted(checkpoint["completed_ids"]) == [10, 20, 30]
