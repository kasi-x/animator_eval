"""Tests for src/scrapers/pipeline.py."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from src.scrapers.bronze_writer import BronzeWriter, BronzeWriterGroup
from src.scrapers.pipeline import Fetcher, Parser, Normalizer, PipelineStats, run_pipeline
from src.scrapers.checkpoint import Checkpoint


class DummyFetcher:
    """Minimal Fetcher implementation for testing."""

    def __init__(self, data: dict[str, dict[str, Any]]):
        """Store test data: id → raw payload."""
        self.data = data

    async def fetch(self, id: str) -> dict[str, Any] | None:
        """Return test data or None."""
        return self.data.get(id)


class DummyParser:
    """Minimal Parser implementation for testing."""

    def parse(self, raw: dict[str, Any], id: str) -> dict[str, str] | None:
        """Parse raw data to a record."""
        if not raw.get("title"):
            return None
        return {"id": id, "title": raw["title"]}


class DummyNormalizer:
    """Minimal Normalizer implementation for testing."""

    def normalize(self, rec: dict[str, str]) -> dict[str, list[dict[str, Any]]]:
        """Convert record to Bronze table rows."""
        return {
            "anime": [
                {
                    "id": rec["id"],
                    "title_ja": rec["title"],
                    "title_en": "",
                }
            ]
        }


def test_pipeline_basic_run(tmp_path: Path) -> None:
    """Test run_pipeline with minimal Fetcher/Parser/Normalizer."""
    fetcher = DummyFetcher(
        {
            "1": {"title": "Test Anime 1"},
            "2": {"title": "Test Anime 2"},
        }
    )
    parser = DummyParser()
    normalizer = DummyNormalizer()

    bronze_dir = tmp_path / "bronze"
    with BronzeWriter("anilist", table="anime", root=bronze_dir) as bw:
        bw.append(
            {
                "id": "dummy",
                "title_ja": "dummy",
                "title_en": "dummy",
            }
        )

    checkpoint = Checkpoint(tmp_path / "checkpoint.json")
    group = BronzeWriterGroup("anilist", tables=["anime"], root=bronze_dir)

    stats = asyncio.run(
        run_pipeline(
            ids=["1", "2"],
            fetcher=fetcher,
            parser=parser,
            normalizer=normalizer,
            writer=group,
            checkpoint=checkpoint,
            limit=0,
            label="test_run",
        )
    )

    assert stats.fetched == 2
    assert stats.parsed == 2
    assert stats.written == 2
    assert stats.failed == 0
    assert stats.skipped == 0


def test_pipeline_with_limit(tmp_path: Path) -> None:
    """Test run_pipeline respects limit."""
    fetcher = DummyFetcher(
        {
            "1": {"title": "Test 1"},
            "2": {"title": "Test 2"},
            "3": {"title": "Test 3"},
        }
    )
    parser = DummyParser()
    normalizer = DummyNormalizer()

    bronze_dir = tmp_path / "bronze"
    checkpoint = Checkpoint(tmp_path / "checkpoint.json")
    group = BronzeWriterGroup("anilist", tables=["anime"], root=bronze_dir)

    stats = asyncio.run(
        run_pipeline(
            ids=["1", "2", "3"],
            fetcher=fetcher,
            parser=parser,
            normalizer=normalizer,
            writer=group,
            checkpoint=checkpoint,
            limit=2,
            label="test_limit",
        )
    )

    assert stats.fetched == 2
    assert stats.parsed == 2
    assert stats.written == 2
    # limit cutoff simply stops the loop; the un-attempted ID is not counted as
    # skipped (skipped = fetcher→None or parser→None, not limit-truncated)
    assert stats.skipped == 0


def test_pipeline_parse_failures(tmp_path: Path) -> None:
    """Test run_pipeline handles parse failures gracefully."""
    fetcher = DummyFetcher(
        {
            "1": {"title": "Valid"},
            "2": {},  # Will fail parsing
            "3": {"title": "Valid"},
        }
    )
    parser = DummyParser()
    normalizer = DummyNormalizer()

    bronze_dir = tmp_path / "bronze"
    checkpoint = Checkpoint(tmp_path / "checkpoint.json")
    group = BronzeWriterGroup("anilist", tables=["anime"], root=bronze_dir)

    stats = asyncio.run(
        run_pipeline(
            ids=["1", "2", "3"],
            fetcher=fetcher,
            parser=parser,
            normalizer=normalizer,
            writer=group,
            checkpoint=checkpoint,
            limit=0,
            label="test_failures",
        )
    )

    assert stats.fetched == 3
    # parsed = records where parser returned non-None (successful parses only)
    assert stats.parsed == 2
    assert stats.written == 2
    assert stats.failed == 0  # parse failures are not counted as runner errors
    # parser→None increments skipped, not failed
    assert stats.skipped == 1


def test_pipeline_stats_dataclass() -> None:
    """Test PipelineStats dataclass."""
    stats = PipelineStats(fetched=10, parsed=9, written=8, failed=1, skipped=0)

    assert stats.fetched == 10
    assert stats.parsed == 9
    assert stats.written == 8
    assert stats.failed == 1
    assert stats.skipped == 0
