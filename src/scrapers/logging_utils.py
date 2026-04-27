"""Structured logging utilities for scrapers.

Provides:
  - configure_file_logging(source): JSONL file sink under logs/scrapers/
  - scraper_timer context manager for operation timing
  - read_log_events(path): read JSONL log files for tests / inspection

File logs are written to logs/scrapers/{source}_{YYYY-MM-DD}.jsonl in
addition to stdout. Each line is one JSON event so they can be tail-grepped
or piped into jq / DuckDB read_json_auto for analysis.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path

import structlog

logger = structlog.get_logger()

DEFAULT_LOG_DIR = Path(
    os.environ.get(
        "ANIMETOR_SCRAPER_LOG_DIR",
        str(Path(__file__).resolve().parent.parent.parent / "logs" / "scrapers"),
    )
)


_active_file_paths: set[Path] = set()


def _file_writer_processor(logger, method_name, event_dict):
    """structlog processor: append serialized event to every active file path.

    Runs late in the chain so the event_dict already has timestamp / level /
    contextvars merged. Returns the event_dict unchanged so subsequent
    processors (console renderer, etc.) still run.
    """
    if not _active_file_paths:
        return event_dict
    line = json.dumps(event_dict, ensure_ascii=False, default=str)
    for p in _active_file_paths:
        try:
            with open(p, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            # never let logging crash the scraper
            pass
    return event_dict


_renderer_installed = False


def configure_file_logging(
    source: str,
    *,
    log_dir: Path | None = None,
    level: int = logging.INFO,
) -> Path:
    """Add a JSONL file sink so structlog events are persisted across crashes.

    Idempotent within a process: calling twice with the same source/log_dir
    returns the same path and does not double-write.

    Each line is one JSON object: {"event": ..., "level": ..., "timestamp": ..., ...kwargs}.
    """
    global _renderer_installed

    target_dir = Path(log_dir or DEFAULT_LOG_DIR)
    target_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.date.today().isoformat()
    log_path = target_dir / f"{source}_{today}.jsonl"

    _active_file_paths.add(log_path)

    if not _renderer_installed:
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                _file_writer_processor,
                structlog.dev.ConsoleRenderer(colors=False),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(level),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=False,
        )
        _renderer_installed = True

    logger.info("scraper_log_file_attached", source=source, path=str(log_path))
    return log_path


def read_log_events(log_path: Path) -> list[dict]:
    """Read all JSON events from a scraper log file (for tests / inspection)."""
    events: list[dict] = []
    if not log_path.exists():
        return events
    for line in log_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


@contextmanager
def scraper_timer(source: str, operation: str):
    """Context manager for timing scraper operations with structured logging.

    Args:
        source: Data source name (e.g., 'anilist', 'mal', 'jvmg')
        operation: Operation name (e.g., 'fetch_anime', 'fetch_staff')

    Yields:
        Dictionary to update with operation metrics
    """
    metrics = {}
    start = time.time()
    try:
        yield metrics
        elapsed_ms = int((time.time() - start) * 1000)
        metrics["elapsed_ms"] = elapsed_ms
        metrics["source"] = source
    finally:
        if "elapsed_ms" not in metrics:
            metrics["elapsed_ms"] = int((time.time() - start) * 1000)
        if "source" not in metrics:
            metrics["source"] = source


