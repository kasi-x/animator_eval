"""Integration tests for scrape_bangumi_relations.py orchestrator.

Stubs:
- BangumiClient → AsyncMock stub (no httpx at all)
- _CHECKPOINT_PATH → tmp_path
- _SUBJECTS_PARQUET_GLOB → subjects parquet fixture in tmp_path
- BronzeWriter root → tmp_path (via DEFAULT_BRONZE_ROOT monkeypatch)
"""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# We import the module-level helpers and internal functions directly.
import scripts.scrape_bangumi_relations as rel_mod
from scripts.scrape_bangumi_relations import (
    _build_character_and_actor_rows,
    _build_person_rows,
    _scrape,
)

# ---------------------------------------------------------------------------
# Canned API responses
# ---------------------------------------------------------------------------

_CANNED_PERSONS = [
    {
        "id": 101, "name": "P1", "type": 1, "relation": "Director", "career": [], "eps": "",
        "images": {"small": "https://img/s.jpg", "grid": "https://img/g.jpg",
                   "large": "https://img/l.jpg", "medium": "https://img/m.jpg"},
    },
    {
        "id": 102, "name": "P2", "type": 1, "relation": "Producer", "career": [], "eps": "",
        "images": {},
    },
]

_CANNED_CHARACTERS = [
    {
        "id": 201, "name": "C1", "type": 1, "relation": "主角",
        "summary": "A main character.",
        "images": {"small": "https://img/cs.jpg", "grid": "", "large": "", "medium": ""},
        "actors": [{"id": 301, "name": "VA1", "type": 1, "career": ["voice_actor"]}],
    },
    {
        "id": 202, "name": "C2", "type": 2, "relation": "配角",
        "summary": None,
        "images": {},
        "actors": [
            {"id": 302, "name": "VA2", "type": 1, "career": []},
            {"id": 303, "name": "VA3", "type": 1, "career": ["voice_actor", "singer"]},
        ],
    },
]


# ---------------------------------------------------------------------------
# Stub BangumiClient
# ---------------------------------------------------------------------------


class _StubBangumiClient:
    """Minimal async context manager stub — returns canned data without HTTP."""

    def __init__(self, *args, **kwargs):
        self.fetch_calls: list[tuple[str, int]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def fetch_subject_persons(self, subject_id: int):
        self.fetch_calls.append(("persons", subject_id))
        return _CANNED_PERSONS

    async def fetch_subject_characters(self, subject_id: int):
        self.fetch_calls.append(("characters", subject_id))
        return _CANNED_CHARACTERS


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_subjects_parquet(tmp_path: Path, subject_ids: list[int]) -> Path:
    """Write a minimal subjects parquet with an 'id' column."""
    table = pa.table({"id": pa.array(subject_ids, type=pa.int64())})
    part_dir = tmp_path / "source=bangumi" / "table=subjects" / "date=20260101"
    part_dir.mkdir(parents=True)
    out = part_dir / "part-0.parquet"
    pq.write_table(table, out)
    return out


# ---------------------------------------------------------------------------
# Tests: row builders (unit — no I/O)
# ---------------------------------------------------------------------------


def test_build_person_rows_fields():
    """_build_person_rows produces correct keys and types including images."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    rows = _build_person_rows(42, _CANNED_PERSONS, fetched_at)
    assert len(rows) == 2
    assert rows[0]["subject_id"] == 42
    assert rows[0]["person_id"] == 101
    assert rows[0]["position"] == "Director"
    assert isinstance(rows[0]["career"], str)  # JSON string
    assert rows[0]["fetched_at"] == fetched_at
    # images must be present as a JSON string
    assert isinstance(rows[0]["images"], str)
    import json
    imgs = json.loads(rows[0]["images"])
    assert "small" in imgs
    # missing images → "{}"
    assert rows[1]["images"] == "{}"


def test_build_character_and_actor_rows_explodes_actors():
    """_build_character_and_actor_rows explodes the actors nest."""
    fetched_at = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    char_rows, actor_rows = _build_character_and_actor_rows(42, _CANNED_CHARACTERS, fetched_at)

    assert len(char_rows) == 2
    assert char_rows[0]["character_id"] == 201
    assert char_rows[0]["subject_id"] == 42
    # images column on character rows
    import json
    assert isinstance(char_rows[0]["images"], str)
    assert "small" in json.loads(char_rows[0]["images"])
    # summary column: populated string and None case
    assert char_rows[0]["summary"] == "A main character."
    assert char_rows[1]["summary"] is None

    # 1 actor from C1 + 2 actors from C2 = 3 total
    assert len(actor_rows) == 3
    assert actor_rows[0]["person_id"] == 301
    assert actor_rows[1]["person_id"] == 302
    assert actor_rows[2]["person_id"] == 303
    for row in actor_rows:
        assert "character_id" in row
        assert "subject_id" in row
        # actor_career must be a JSON string
        assert isinstance(row["actor_career"], str)
        json.loads(row["actor_career"])  # must not raise
    # specific career values
    assert json.loads(actor_rows[0]["actor_career"]) == ["voice_actor"]
    assert json.loads(actor_rows[1]["actor_career"]) == []
    assert json.loads(actor_rows[2]["actor_career"]) == ["voice_actor", "singer"]


# ---------------------------------------------------------------------------
# Tests: _scrape (end-to-end with stub client + tmp_path)
# ---------------------------------------------------------------------------


def test_scrape_writes_3_parquets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """_scrape writes subject_persons, subject_characters, person_characters parquet."""
    # Patch BangumiClient to stub
    monkeypatch.setattr(rel_mod, "BangumiClient", _StubBangumiClient)
    # Patch BronzeWriter root so files go to tmp_path
    monkeypatch.setattr(
        "src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path
    )
    # Patch checkpoint save to tmp_path
    checkpoint_path = tmp_path / "checkpoint_relations.json"
    monkeypatch.setattr(rel_mod, "_CHECKPOINT_PATH", checkpoint_path)

    subject_ids = [10, 20, 30]
    checkpoint: dict = {"completed_ids": [], "failed_ids": [], "last_run_at": None}
    date_str = "2026-01-01"

    asyncio.run(_scrape(subject_ids, checkpoint, date_str, dry_run=False))

    # All 3 subject IDs completed
    assert set(checkpoint["completed_ids"]) == {10, 20, 30}

    # Three tables should have parquet files
    for table in ("subject_persons", "subject_characters", "person_characters"):
        files = list((tmp_path / "source=bangumi" / f"table={table}").rglob("*.parquet"))
        assert files, f"No parquet files for table={table}"


def test_scrape_checkpoint_reflects_completed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """After _scrape, checkpoint.completed_ids == all subject_ids."""
    monkeypatch.setattr(rel_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(rel_mod, "_CHECKPOINT_PATH", checkpoint_path)

    subject_ids = [1, 2, 3]
    checkpoint: dict = {"completed_ids": [], "failed_ids": [], "last_run_at": None}

    asyncio.run(_scrape(subject_ids, checkpoint, "2026-01-01", dry_run=False))

    assert sorted(checkpoint["completed_ids"]) == [1, 2, 3]
    assert checkpoint["failed_ids"] == []


def test_scrape_person_characters_has_actor_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """person_characters table should contain rows derived from actors nests."""
    monkeypatch.setattr(rel_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(rel_mod, "_CHECKPOINT_PATH", checkpoint_path)

    asyncio.run(_scrape([42], {"completed_ids": [], "failed_ids": [], "last_run_at": None},
                        "2026-01-01", dry_run=False))

    files = list((tmp_path / "source=bangumi" / "table=person_characters").rglob("*.parquet"))
    assert files
    tbl = pq.read_table(files[0])
    # 3 actor rows from 2 characters (1 + 2)
    assert tbl.num_rows == 3
    col_names = set(tbl.schema.names)
    assert {"person_id", "character_id", "subject_id", "actor_career"} <= col_names


def test_scrape_subject_persons_has_images(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """subject_persons parquet must contain an 'images' column."""
    monkeypatch.setattr(rel_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(rel_mod, "_CHECKPOINT_PATH", checkpoint_path)

    asyncio.run(_scrape([42], {"completed_ids": [], "failed_ids": [], "last_run_at": None},
                        "2026-01-01", dry_run=False))

    files = list((tmp_path / "source=bangumi" / "table=subject_persons").rglob("*.parquet"))
    assert files
    tbl = pq.read_table(files[0])
    assert "images" in set(tbl.schema.names)


def test_scrape_subject_characters_has_images_and_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """subject_characters parquet must contain 'images' and 'summary' columns."""
    monkeypatch.setattr(rel_mod, "BangumiClient", _StubBangumiClient)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(rel_mod, "_CHECKPOINT_PATH", checkpoint_path)

    asyncio.run(_scrape([42], {"completed_ids": [], "failed_ids": [], "last_run_at": None},
                        "2026-01-01", dry_run=False))

    files = list((tmp_path / "source=bangumi" / "table=subject_characters").rglob("*.parquet"))
    assert files
    tbl = pq.read_table(files[0])
    col_names = set(tbl.schema.names)
    assert {"images", "summary"} <= col_names


def test_scrape_resume_skips_already_completed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Resume: subjects already in completed_ids must not be re-fetched."""
    stub = _StubBangumiClient()
    monkeypatch.setattr(rel_mod, "BangumiClient", lambda: stub)
    monkeypatch.setattr("src.scrapers.bronze_writer.DEFAULT_BRONZE_ROOT", tmp_path)
    checkpoint_path = tmp_path / "cp.json"
    monkeypatch.setattr(rel_mod, "_CHECKPOINT_PATH", checkpoint_path)

    # Pre-populate: subjects 1, 2, 3 all completed
    checkpoint = {"completed_ids": [1, 2, 3], "failed_ids": [], "last_run_at": None}

    asyncio.run(_scrape([1, 2, 3], checkpoint, "2026-01-01", dry_run=False))

    # Stub never called (pending list is empty)
    assert stub.fetch_calls == []
