"""Tests for scraper file logging."""

from __future__ import annotations

import structlog

from src.scrapers.logging_utils import configure_file_logging, read_log_events


def test_configure_creates_jsonl_file(tmp_path):
    log_path = configure_file_logging("test_src", log_dir=tmp_path)
    assert log_path.exists()
    assert log_path.suffix == ".jsonl"
    assert log_path.parent == tmp_path


def test_events_written_to_file(tmp_path):
    log_path = configure_file_logging("test_events", log_dir=tmp_path)
    log = structlog.get_logger()
    log.info("test_event", k="v", n=42)
    log.warning("test_warn", reason="x")

    events = read_log_events(log_path)
    by_event = {e["event"]: e for e in events}
    assert "test_event" in by_event
    assert by_event["test_event"]["k"] == "v"
    assert by_event["test_event"]["n"] == 42
    assert by_event["test_event"]["level"] == "info"
    assert by_event["test_warn"]["level"] == "warning"


def test_idempotent_multiple_calls(tmp_path):
    """Calling configure twice with the same source must not double-write."""
    p1 = configure_file_logging("idem", log_dir=tmp_path)
    p2 = configure_file_logging("idem", log_dir=tmp_path)
    assert p1 == p2

    log = structlog.get_logger()
    log.info("once_only", marker="x")

    events = read_log_events(p1)
    matches = [e for e in events if e.get("event") == "once_only"]
    assert len(matches) == 1, f"event written {len(matches)} times: {matches}"


def test_read_missing_file_returns_empty(tmp_path):
    assert read_log_events(tmp_path / "does_not_exist.jsonl") == []
