"""Unified progress reporting for scrapers.

Replaces the mix of `log.info(...)` checkpoints and ad-hoc `rich.progress.Progress`
blocks scattered across scrapers. One context manager, two backends:

- **TTY / human**:  rich progress bar with bar + ETA + completion fraction
- **non-TTY / AI agent / quiet**: structured log lines via structlog only

Auto-detection prefers the bar when stdout is a TTY. Override with:
- env var `ANIMETOR_NO_PROGRESS=1` → force structured-log mode
- env var `ANIMETOR_PROGRESS=1`    → force progress-bar mode
- explicit `enabled=` argument

Usage::

    from src.scrapers.progress import scrape_progress

    with scrape_progress(total=len(pending), description="scraping persons") as p:
        for item_id in pending:
            ...
            p.advance()
            if (i + 1) % 100 == 0:
                p.log("checkpoint_flushed", completed=done, remaining=todo)
"""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from typing import Any, Iterator, Protocol

import structlog

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Decision: progress bar vs structured-log
# ---------------------------------------------------------------------------


def progress_enabled(override: bool | None = None) -> bool:
    """Return True if a progress bar should be shown.

    Resolution order:
    1. `override` arg (highest priority)
    2. `ANIMETOR_NO_PROGRESS=1` env → False
    3. `ANIMETOR_PROGRESS=1`    env → True
    4. stdout.isatty() (default heuristic — humans see TTYs, agents don't)
    """
    if override is not None:
        return override
    if os.environ.get("ANIMETOR_NO_PROGRESS"):
        return False
    if os.environ.get("ANIMETOR_PROGRESS"):
        return True
    try:
        return sys.stdout.isatty()
    except (AttributeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Reporter protocol — both backends conform
# ---------------------------------------------------------------------------


class _Reporter(Protocol):
    def advance(self, n: int = 1) -> None: ...
    def log(self, event: str, **fields: Any) -> None: ...
    def update_description(self, description: str) -> None: ...


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------


class _LogReporter:
    """Quiet backend: only emits structured log lines.

    Used for non-TTY (AI agent) execution and `ANIMETOR_NO_PROGRESS=1`.
    """

    def __init__(self, total: int | None, description: str, *, log_every: int) -> None:
        self._total = total
        self._description = description
        self._log_every = max(1, log_every)
        self._done = 0
        logger.info("scrape_progress_start", description=description, total=total)

    def advance(self, n: int = 1) -> None:
        self._done += n
        is_done = self._total is not None and self._done >= self._total
        if self._done % self._log_every == 0 or is_done:
            fields: dict[str, Any] = {"description": self._description, "done": self._done}
            if self._total is not None:
                fields["total"] = self._total
                fields["remaining"] = max(0, self._total - self._done)
            logger.info("scrape_progress", **fields)

    def log(self, event: str, **fields: Any) -> None:
        logger.info(event, **fields)

    def update_description(self, description: str) -> None:
        self._description = description


class _BarReporter:
    """Rich progress-bar backend."""

    def __init__(self, progress: Any, task_id: Any) -> None:
        self._progress = progress
        self._task_id = task_id

    def advance(self, n: int = 1) -> None:
        self._progress.advance(self._task_id, advance=n)

    def log(self, event: str, **fields: Any) -> None:
        # Render alongside the bar without disturbing it.
        # rich.progress.Progress.console.log() integrates properly with the live region.
        msg_parts = [event] + [f"{k}={v}" for k, v in fields.items()]
        self._progress.console.log(" ".join(msg_parts))

    def update_description(self, description: str) -> None:
        self._progress.update(self._task_id, description=description)


# ---------------------------------------------------------------------------
# Public context manager
# ---------------------------------------------------------------------------


@contextmanager
def scrape_progress(
    total: int | None,
    description: str = "scraping",
    *,
    enabled: bool | None = None,
    log_every: int = 100,
) -> Iterator[_Reporter]:
    """Context-managed progress reporter.

    Args:
        total: total expected items, or None for indeterminate progress.
        description: short label for the bar / log line.
        enabled: force bar (True) / log-only (False), or auto (None).
        log_every: in log-only mode, emit a progress line every N advances.
    """
    if progress_enabled(enabled):
        from rich.progress import (
            BarColumn,
            MofNCompleteColumn,
            Progress,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
        )

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            transient=False,
        ) as bar:
            task_id = bar.add_task(description, total=total)
            yield _BarReporter(bar, task_id)
    else:
        yield _LogReporter(total, description, log_every=log_every)
