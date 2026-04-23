"""Comprehensive scraper tests for retry, error handling, checkpoint, and edge cases.

Targets 70%+ coverage across:
- anilist_scraper.py (AniListClient, parsers, edge cases)
- mal_scraper.py (JikanClient, parsers, checkpoint)
- mediaarts_scraper.py (JSON-LD dump parser, GitHub download)
- jvmg_fetcher.py (WikidataClient, parsers, checkpoint)
- image_downloader.py (download_image, content validation, retry)
- retry.py (retry_async utility)
- exceptions.py (exception hierarchy)

All async tests use asyncio.run() wrappers since pytest-asyncio is not available.
"""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.models import Role
from src.scrapers.exceptions import (
    AuthenticationError,
    ContentValidationError,
    DataParseError,
    EndpointUnreachableError,
    RateLimitError,
    ScraperError,
)


def _run(coro):
    """Helper to run async coroutines in sync tests."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Exception hierarchy tests
# ---------------------------------------------------------------------------


class TestExceptions:
    def test_scraper_error_attributes(self):
        err = ScraperError(
            "boom", source="anilist", url="http://x", metadata={"k": "v"}
        )
        assert str(err) == "boom"
        assert err.source == "anilist"
        assert err.url == "http://x"
        assert err.metadata == {"k": "v"}

    def test_scraper_error_defaults(self):
        err = ScraperError("default")
        assert err.source == ""
        assert err.url == ""
        assert err.metadata == {}

    def test_rate_limit_error(self):
        err = RateLimitError(source="mal", retry_after=30.0)
        assert err.retry_after == 30.0
        assert isinstance(err, ScraperError)

    def test_rate_limit_error_defaults(self):
        err = RateLimitError()
        assert err.retry_after == 60.0
        assert str(err) == "Rate limited"

    def test_authentication_error(self):
        err = AuthenticationError("bad token", source="anilist")
        assert isinstance(err, ScraperError)
        assert err.source == "anilist"

    def test_data_parse_error(self):
        err = DataParseError("parse fail", source="mal")
        assert isinstance(err, ScraperError)

    def test_endpoint_unreachable_error(self):
        err = EndpointUnreachableError("down", source="wikidata", url="http://wd")
        assert isinstance(err, ScraperError)
        assert err.url == "http://wd"

    def test_content_validation_error(self):
        err = ContentValidationError("bad content", source="anilist")
        assert isinstance(err, ScraperError)


# ---------------------------------------------------------------------------
# retry_async tests
# ---------------------------------------------------------------------------


class TestRetryAsync:
    def test_success_on_first_attempt(self):
        from src.scrapers.retry import retry_async

        async def ok():
            return 42

        result = _run(retry_async(ok, max_attempts=3, source="test"))
        assert result == 42

    def test_success_after_transient_failure(self):
        from src.scrapers.retry import retry_async

        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ScraperError("transient", source="test")
            return "ok"

        result = _run(
            retry_async(flaky, max_attempts=5, base_delay=0.01, source="test")
        )
        assert result == "ok"
        assert call_count == 3

    def test_all_retries_exhausted(self):
        from src.scrapers.retry import retry_async

        async def always_fail():
            raise ScraperError("permanent", source="test")

        with pytest.raises(EndpointUnreachableError) as exc_info:
            _run(
                retry_async(always_fail, max_attempts=2, base_delay=0.01, source="test")
            )
        assert "Failed after 2 attempts" in str(exc_info.value)

    def test_rate_limit_error_respects_retry_after(self):
        from src.scrapers.retry import retry_async

        call_count = 0

        async def rate_limited_then_ok():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RateLimitError(source="test", retry_after=0.01)
            return "success"

        result = _run(
            retry_async(
                rate_limited_then_ok, max_attempts=3, base_delay=0.01, source="test"
            )
        )
        assert result == "success"
        assert call_count == 2

    def test_passes_args_and_kwargs(self):
        from src.scrapers.retry import retry_async

        async def add(a, b, extra=0):
            return a + b + extra

        result = _run(retry_async(add, 1, 2, extra=10, max_attempts=1, source="test"))
        assert result == 13


