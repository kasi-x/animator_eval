"""Smoke tests for src.scrapers.pipeline.run_pipeline."""
from __future__ import annotations

import asyncio
import dataclasses
from pathlib import Path

from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.checkpoint import Checkpoint
from src.scrapers.pipeline import (
    Fetcher,
    Normalizer,
    Parser,
    PipelineStats,
    run_pipeline,
)


# ── dummy triple ──────────────────────────────────────────────────────────────


@dataclasses.dataclass
class _DummyRaw:
    id: str
    payload: str


@dataclasses.dataclass
class _DummyRec:
    id: str
    title: str


class _DummyFetcher:
    def __init__(self, miss: set[str] | None = None) -> None:
        self.calls: list[str] = []
        self._miss = miss or set()

    async def fetch(self, id: str) -> _DummyRaw | None:
        self.calls.append(id)
        if id in self._miss:
            return None
        return _DummyRaw(id=id, payload=f"raw_{id}")


class _DummyParser:
    def __init__(self, drop: set[str] | None = None) -> None:
        self._drop = drop or set()

    def parse(self, raw: _DummyRaw, id: str) -> _DummyRec | None:
        if id in self._drop:
            return None
        return _DummyRec(id=id, title=f"title_{raw.payload}")


class _DummyNormalizer:
    def normalize(self, rec: _DummyRec) -> dict[str, list[dict]]:
        return {"anime": [{"id": rec.id, "title": rec.title}]}


def _new_group(tmp_path: Path) -> BronzeWriterGroup:
    return BronzeWriterGroup("anilist", tables=["anime"], root=tmp_path / "bronze")


def _new_cp(tmp_path: Path) -> Checkpoint:
    return Checkpoint(tmp_path / "cp.json")


def _run(coro):
    return asyncio.run(coro)


# ── protocol structural conformance ───────────────────────────────────────────


def test_dummy_triple_satisfies_protocols():
    assert isinstance(_DummyFetcher(), Fetcher)
    assert isinstance(_DummyParser(), Parser)
    assert isinstance(_DummyNormalizer(), Normalizer)


# ── basic run ────────────────────────────────────────────────────────────────


def test_run_pipeline_processes_all_ids(tmp_path):
    fetcher = _DummyFetcher()
    parser = _DummyParser()
    normalizer = _DummyNormalizer()
    cp = _new_cp(tmp_path)

    with _new_group(tmp_path) as group:
        stats = _run(
            run_pipeline(
                ["1", "2", "3"],
                fetcher,
                parser,
                normalizer,
                group,
                cp,
                progress_override=False,
                label="dummy",
            )
        )

    assert isinstance(stats, PipelineStats)
    assert stats.fetched == 3
    assert stats.parsed == 3
    assert stats.written == 3
    assert stats.failed == 0
    assert stats.skipped == 0
    assert fetcher.calls == ["1", "2", "3"]


def test_run_pipeline_fetch_miss_does_not_count_as_parsed(tmp_path):
    fetcher = _DummyFetcher(miss={"2"})
    parser = _DummyParser()
    normalizer = _DummyNormalizer()
    cp = _new_cp(tmp_path)

    with _new_group(tmp_path) as group:
        stats = _run(
            run_pipeline(
                ["1", "2", "3"],
                fetcher,
                parser,
                normalizer,
                group,
                cp,
                progress_override=False,
            )
        )

    assert stats.fetched == 2
    assert stats.parsed == 2
    assert stats.written == 2


def test_run_pipeline_parser_drop_increments_skipped(tmp_path):
    fetcher = _DummyFetcher()
    parser = _DummyParser(drop={"2"})
    normalizer = _DummyNormalizer()
    cp = _new_cp(tmp_path)

    with _new_group(tmp_path) as group:
        stats = _run(
            run_pipeline(
                ["1", "2", "3"],
                fetcher,
                parser,
                normalizer,
                group,
                cp,
                progress_override=False,
            )
        )

    assert stats.fetched == 3
    assert stats.parsed == 2
    assert stats.written == 2
    assert stats.skipped == 1


def test_run_pipeline_skips_completed_ids(tmp_path):
    fetcher = _DummyFetcher()
    parser = _DummyParser()
    normalizer = _DummyNormalizer()
    cp = _new_cp(tmp_path)
    cp.sync_completed({"1"})

    with _new_group(tmp_path) as group:
        stats = _run(
            run_pipeline(
                ["1", "2", "3"],
                fetcher,
                parser,
                normalizer,
                group,
                cp,
                progress_override=False,
            )
        )

    assert fetcher.calls == ["2", "3"]
    assert stats.fetched == 2
    assert stats.parsed == 2


def test_run_pipeline_limit_caps_pending(tmp_path):
    fetcher = _DummyFetcher()
    parser = _DummyParser()
    normalizer = _DummyNormalizer()
    cp = _new_cp(tmp_path)

    with _new_group(tmp_path) as group:
        stats = _run(
            run_pipeline(
                ["1", "2", "3", "4", "5"],
                fetcher,
                parser,
                normalizer,
                group,
                cp,
                limit=2,
                progress_override=False,
            )
        )

    assert len(fetcher.calls) == 2
    assert stats.fetched == 2
