"""Integration tests for scrape_bangumi_characters.py orchestrator.

Stubs:
- BangumiClient.fetch_character → returns canned dict (no httpx)
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

import scripts.scrape_bangumi_characters as char_mod
from scripts.scrape_bangumi_characters import (
    _build_character_row,
    _scrape,
)

# ---------------------------------------------------------------------------
# Canned character API response
# ---------------------------------------------------------------------------

_CANNED_CHARACTER = {
    "id": 0,  # overridden per call
    "name": "Test Character",
    # type: 1=角色 2=機体 3=組織
    "type": 1,
    "locked": False,
    "nsfw": False,
    "summary": "A fictional character summary.",
    "infobox": [
        {"key": "生日", "value": [{"v": "Jan 1"}]},
    ],
    "gender": "female",
    "blood_type": 2,
    "birth_year": 2000,
    "birth_mon": 1,
    "birth_day": 1,
    "images": {
        "small": "https://img/s.jpg",
        "grid": "https://img/g.jpg",
        "large": "https://img/l.jpg",
        "medium": "https://img/m.jpg",
    },
    "stat": {"comments": 10, "collects": 150},
    # NOTE: last_modified is intentionally absent (per API spec)
}


# ---------------------------------------------------------------------------
# Stub BangumiClient
# ---------------------------------------------------------------------------


class _StubBangumiClient:
    """Returns canned character data without HTTP."""

    def __init__(self, *args, **kwargs):
        self.fetch_calls: list[int] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def fetch_character(self, character_id: int):
        self.fetch_calls.append(character_id)
        char = dict(_CANNED_CHARACTER)
        char["id"] = character_id
        return char


# ---------------------------------------------------------------------------
# Tests: _build_character_row (unit)
# ---------------------------------------------------------------------------


def test_build_character_row_no_last_modified():
    """Characters row must NOT include a last_modified column."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_character_row(dict(_CANNED_CHARACTER, id=1), fetched_at)
    assert "last_modified" not in row


def test_build_character_row_type_column_present():
    """'type' column must be present (character category int)."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_character_row(dict(_CANNED_CHARACTER, id=1), fetched_at)
    assert "type" in row
    assert row["type"] == 1


def test_build_character_row_locked_column_present():
    """'locked' column must be present as bool."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_character_row(dict(_CANNED_CHARACTER, id=1), fetched_at)
    assert "locked" in row
    assert isinstance(row["locked"], bool)
    assert row["locked"] is False


def test_build_character_row_nsfw_column_present():
    """'nsfw' column must be present as bool."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_character_row(dict(_CANNED_CHARACTER, id=1), fetched_at)
    assert "nsfw" in row
    assert isinstance(row["nsfw"], bool)
    assert row["nsfw"] is False


def test_build_character_row_nsfw_defaults_to_false_when_absent():
    """nsfw must default to False when the API field is absent."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    char_no_nsfw = {k: v for k, v in _CANNED_CHARACTER.items() if k != "nsfw"}
    char_no_nsfw["id"] = 1
    row = _build_character_row(char_no_nsfw, fetched_at)
    assert row["nsfw"] is False


def test_build_character_row_infobox_is_json_string():
    """infobox must be serialised as JSON string."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_character_row(dict(_CANNED_CHARACTER, id=1), fetched_at)
    assert isinstance(row["infobox"], str)
    json.loads(row["infobox"])  # should not raise


def test_build_character_row_stat_split():
    """stat dict is split into stat_comments / stat_collects."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    row = _build_character_row(dict(_CANNED_CHARACTER, id=1), fetched_at)
    assert row["stat_comments"] == 10
    assert row["stat_collects"] == 150
    assert "stat" not in row


# ---------------------------------------------------------------------------
# Tests: _scrape (end-to-end with stub + tmp_path)
# ---------------------------------------------------------------------------


def test_scrape_characters_writes_parquet_with_3_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """_scrape writes a characters parquet containing exactly 3 rows."""
    monkeypatch.setattr(char_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "checkpoint_characters.json"
    monkeypatch.setattr(char_mod, "_CHECKPOINT_PATH", checkpoint_path)

    character_ids = [201, 202, 203]
    checkpoint: dict = {"completed_ids": [], "failed_ids": [], "last_run_at": None}

    asyncio.run(_scrape(character_ids, checkpoint, "2026-01-01", dry_run=False))

    files = list((tmp_path / "source=bangumi" / "table=characters").rglob("*.parquet"))
    assert files
    tbl = pq.read_table(files[0])
    assert tbl.num_rows == 3


def test_scrape_characters_no_last_modified_in_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """characters parquet must NOT contain a last_modified column."""
    monkeypatch.setattr(char_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(char_mod, "_CHECKPOINT_PATH", checkpoint_path)

    asyncio.run(_scrape([1], {"completed_ids": [], "failed_ids": [], "last_run_at": None},
                        "2026-01-01", dry_run=False))

    files = list((tmp_path / "source=bangumi" / "table=characters").rglob("*.parquet"))
    schema = pq.read_schema(files[0])
    assert "last_modified" not in schema.names, "last_modified should not appear in characters parquet"


def test_scrape_characters_type_column_in_parquet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """characters parquet must contain 'type', 'locked', and 'nsfw' columns."""
    monkeypatch.setattr(char_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(char_mod, "_CHECKPOINT_PATH", checkpoint_path)

    asyncio.run(_scrape([1], {"completed_ids": [], "failed_ids": [], "last_run_at": None},
                        "2026-01-01", dry_run=False))

    files = list((tmp_path / "source=bangumi" / "table=characters").rglob("*.parquet"))
    tbl = pq.read_table(files[0])
    col_names = set(tbl.schema.names)
    expected = {"type", "locked", "nsfw"}
    assert expected <= col_names, f"Missing columns: {expected - col_names}"


def test_scrape_characters_checkpoint_updated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """After _scrape, checkpoint.completed_ids includes all character_ids."""
    monkeypatch.setattr(char_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(char_mod, "_CHECKPOINT_PATH", checkpoint_path)

    checkpoint = {"completed_ids": [], "failed_ids": [], "last_run_at": None}
    asyncio.run(_scrape([11, 22, 33], checkpoint, "2026-01-01", dry_run=False))

    assert sorted(checkpoint["completed_ids"]) == [11, 22, 33]


def test_scrape_characters_resume_skips_completed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Characters already in completed_ids must not be fetched again."""
    stub = _StubBangumiClient()
    monkeypatch.setattr(char_mod, "BangumiClient", lambda: stub)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(char_mod, "_CHECKPOINT_PATH", checkpoint_path)

    checkpoint = {"completed_ids": [1, 2, 3], "failed_ids": [], "last_run_at": None}
    asyncio.run(_scrape([1, 2, 3], checkpoint, "2026-01-01", dry_run=False))

    # Stub should never have been called — all already completed
    assert stub.fetch_calls == []
