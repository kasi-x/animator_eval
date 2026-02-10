"""スクレイパー例外階層とリトライユーティリティのテスト."""

import asyncio

import pytest

from src.scrapers.exceptions import (
    AuthenticationError,
    ContentValidationError,
    DataParseError,
    EndpointUnreachableError,
    RateLimitError,
    ScraperError,
)
from src.scrapers.retry import retry_async


class TestExceptionHierarchy:
    """例外クラスの継承関係テスト."""

    def test_scraper_error_is_exception(self):
        assert issubclass(ScraperError, Exception)

    def test_rate_limit_is_scraper_error(self):
        assert issubclass(RateLimitError, ScraperError)

    def test_auth_error_is_scraper_error(self):
        assert issubclass(AuthenticationError, ScraperError)

    def test_parse_error_is_scraper_error(self):
        assert issubclass(DataParseError, ScraperError)

    def test_endpoint_unreachable_is_scraper_error(self):
        assert issubclass(EndpointUnreachableError, ScraperError)

    def test_content_validation_is_scraper_error(self):
        assert issubclass(ContentValidationError, ScraperError)

    def test_all_catchable_as_scraper_error(self):
        errors = [
            RateLimitError("test", source="test"),
            AuthenticationError("test", source="test"),
            DataParseError("test", source="test"),
            EndpointUnreachableError("test", source="test"),
            ContentValidationError("test", source="test"),
        ]
        for error in errors:
            with pytest.raises(ScraperError):
                raise error


class TestScraperErrorAttributes:
    """例外属性テスト."""

    def test_base_error_attributes(self):
        err = ScraperError("msg", source="anilist", url="https://example.com", metadata={"key": "val"})
        assert str(err) == "msg"
        assert err.source == "anilist"
        assert err.url == "https://example.com"
        assert err.metadata == {"key": "val"}

    def test_base_error_defaults(self):
        err = ScraperError("msg")
        assert err.source == ""
        assert err.url == ""
        assert err.metadata == {}

    def test_rate_limit_retry_after(self):
        err = RateLimitError(retry_after=30.0)
        assert err.retry_after == 30.0
        assert str(err) == "Rate limited"

    def test_rate_limit_default_retry_after(self):
        err = RateLimitError()
        assert err.retry_after == 60.0


class TestRetryAsync:
    """retry_async ユーティリティのテスト."""

    def test_success_on_first_attempt(self):
        call_count = 0

        async def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = asyncio.run(
            retry_async(succeed, max_attempts=3, base_delay=0.01, source="test")
        )
        assert result == "ok"
        assert call_count == 1

    def test_success_after_retries(self):
        call_count = 0

        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ScraperError("transient", source="test")
            return "ok"

        result = asyncio.run(
            retry_async(fail_then_succeed, max_attempts=5, base_delay=0.01, source="test")
        )
        assert result == "ok"
        assert call_count == 3

    def test_exhausted_retries_raises_endpoint_unreachable(self):
        async def always_fail():
            raise ScraperError("fail", source="test")

        with pytest.raises(EndpointUnreachableError, match="Failed after 3 attempts"):
            asyncio.run(
                retry_async(always_fail, max_attempts=3, base_delay=0.01, source="test")
            )

    def test_rate_limit_respects_retry_after(self):
        call_count = 0

        async def rate_limited_then_ok():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RateLimitError(source="test", retry_after=0.01)
            return "ok"

        result = asyncio.run(
            retry_async(rate_limited_then_ok, max_attempts=3, base_delay=0.01, source="test")
        )
        assert result == "ok"
        assert call_count == 2

    def test_passes_args_and_kwargs(self):
        async def fn(a, b, c=None):
            return (a, b, c)

        result = asyncio.run(
            retry_async(fn, 1, 2, c=3, max_attempts=1, base_delay=0.01, source="test")
        )
        assert result == (1, 2, 3)
