"""Generic scrape-loop runner.

Separates the 5 concerns that every scraper duplicates:
  Source   — ID enumeration → list[ID]            (caller's responsibility)
  Fetcher  — ID → raw payload                     (src.scrapers.fetchers)
  Parser   — raw → Record   (pure function)        (src.scrapers.parsers.*)
  Sink     — Record → BRONZE rows                  (src.scrapers.sinks)
  Runner   — loop + checkpoint + progress + flush  (this module)

Usage::

    from src.scrapers.runner import ScrapeRunner, Stats
    from src.scrapers.fetchers import HtmlFetcher
    from src.scrapers.sinks import BronzeSink

    fetcher = HtmlFetcher(client, "https://example.com/item/{id}")
    sink = BronzeSink(group, lambda rec: {"anime": [dataclasses.asdict(rec)]})
    runner = ScrapeRunner(
        fetcher=fetcher,
        parser=parse_fn,
        sink=sink,
        checkpoint=cp,
        label="example_anime",
        flush=group.flush_all,
    )
    stats = await runner.run(ids, limit=0)
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable, Iterable
from typing import Generic, TypeVar

import structlog

from src.scrapers.checkpoint import Checkpoint
from src.scrapers.progress import scrape_progress

log = structlog.get_logger()

ID = TypeVar("ID")
Raw = TypeVar("Raw")
Rec = TypeVar("Rec")


@dataclasses.dataclass
class Stats:
    """Per-run scrape statistics returned by ScrapeRunner.run()."""

    processed: int = 0
    written: int = 0
    skipped: int = 0
    errors: int = 0


@dataclasses.dataclass
class ScrapeRunner(Generic[ID, Raw, Rec]):
    """Checkpoint-aware async scrape loop.

    Args:
        fetcher:       async callable ID → Raw | None.
                       Returns None to skip (404 or parse guard).
        parser:        pure callable (Raw, ID) → Rec | None.
                       Returns None to skip (not-anime, missing data, …).
        sink:          callable Rec → int (rows written).
        checkpoint:    Checkpoint instance; must already be loaded.
        label:         short name for log and progress events.
        flush:         called every flush_every items and at end.
        flush_every:   items between checkpoint+flush; default 100.
    """

    fetcher: Callable[[ID], Awaitable[Raw | None]]
    parser: Callable[[Raw, ID], Rec | None]
    sink: Callable[[Rec], int]
    checkpoint: Checkpoint
    label: str
    flush: Callable[[], None]
    flush_every: int = 100

    async def run(
        self,
        ids: Iterable[ID],
        *,
        limit: int = 0,
        progress_override: bool | None = None,
    ) -> Stats:
        """Iterate pending IDs, fetch-parse-sink each, flush at intervals.

        Args:
            ids:               Full candidate id list (e.g. sitemap or bronze IDs).
            limit:             Cap to first N pending items; 0 = no cap.
            progress_override: Pass True/False to force bar/log mode;
                               None = auto-detect from TTY.

        Returns:
            Stats dataclass with processed/written/skipped/errors counts.
        """
        cp = self.checkpoint
        stats = Stats()

        pending = cp.pending(ids, limit=limit)
        completed: set = cp.completed_set

        log.info(
            f"{self.label}_start",
            pending=len(pending),
            completed=len(completed),
        )

        with scrape_progress(
            total=len(pending),
            description=f"scraping {self.label}",
            enabled=progress_override,
        ) as progress:
            for i, item_id in enumerate(pending):
                raw = await self._fetch_one(item_id, stats)
                if raw is None:
                    completed.add(item_id)
                    stats.processed += 1
                    progress.advance()
                    continue

                rec = self._parse_one(raw, item_id, stats)
                if rec is not None:
                    written = self._sink_one(rec, item_id, stats)
                    stats.written += written

                completed.add(item_id)
                stats.processed += 1
                progress.advance()

                if (i + 1) % self.flush_every == 0:
                    self._checkpoint_flush(completed, stats, progress, i + 1, len(pending))

        self._final_flush(completed)
        self._log_done(stats)
        return stats

    # ── internal step helpers ──────────────────────────────────────────────

    async def _fetch_one(self, item_id: ID, stats: Stats) -> Raw | None:
        """Fetch raw payload; log and count errors; return None to skip."""
        try:
            return await self.fetcher(item_id)
        except Exception as exc:
            log.error(
                f"{self.label}_fetch_error",
                item_id=item_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            stats.errors += 1
            return None

    def _parse_one(self, raw: Raw, item_id: ID, stats: Stats) -> Rec | None:
        """Parse raw payload; return None to skip (not an error)."""
        try:
            rec = self.parser(raw, item_id)
        except Exception as exc:
            log.error(
                f"{self.label}_parse_error",
                item_id=item_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            stats.errors += 1
            return None
        if rec is None:
            stats.skipped += 1
        return rec

    def _sink_one(self, rec: Rec, item_id: ID, stats: Stats) -> int:
        """Write record to bronze; return rows written."""
        try:
            return self.sink(rec)
        except Exception as exc:
            log.error(
                f"{self.label}_sink_error",
                item_id=item_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            stats.errors += 1
            return 0

    def _checkpoint_flush(
        self,
        completed: set,
        stats: Stats,
        progress,
        done: int,
        total: int,
    ) -> None:
        """Flush buffer, sync checkpoint, and log intermediate progress."""
        self.flush()
        self.checkpoint.sync_completed(completed)
        self.checkpoint.save()
        progress.log(
            f"{self.label}_checkpoint",
            done=done,
            remaining=total - done,
            written=stats.written,
            errors=stats.errors,
        )

    def _final_flush(self, completed: set) -> None:
        """Final buffer flush and checkpoint save after loop ends."""
        self.flush()
        self.checkpoint.sync_completed(completed)
        self.checkpoint.save()

    def _log_done(self, stats: Stats) -> None:
        log.info(
            f"{self.label}_done",
            processed=stats.processed,
            written=stats.written,
            skipped=stats.skipped,
            errors=stats.errors,
        )
