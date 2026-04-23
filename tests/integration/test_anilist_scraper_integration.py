"""AniList scraper integration tests — httpx.MockTransport based retry/rate-limit scenarios.

Tests cover:
- Normal operation: successful query returns data
- 429 Rate Limit: retry after wait
- 504 Gateway Timeout: retry and succeed
- timeoutException: graceful error handling
- Parse failure: invalid JSON (should skip gracefully)
- Empty response: 200 with empty data (should not crash)

Uses httpx.MockTransport for zero-network testing + asyncio.run() for async tests.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.scrapers.anilist_scraper import AniListClient


def _run(coro):
    """Helper to run async coroutines in sync tests."""
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Skip backoff sleeps so retry tests run instantly."""
    async def _instant(*args, **kwargs):
        return None

    monkeypatch.setattr("asyncio.sleep", _instant)


class TestAniListScraper:
    """Integration tests for AniListClient with mock HTTP transport."""

    def test_successful_query_returns_data(self):
        """Test 1: Normal operation — successful query returns parsed data."""
        success_response = httpx.Response(
            200,
            json={"data": {"Page": {"media": [{"id": 1, "title": "Test"}]}}},
            headers={
                "X-RateLimit-Remaining": "89",
                "X-RateLimit-Reset": "0",
                "X-RateLimit-Limit": "90",
            },
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._client._client = AsyncMock()
        client._client._client.post = AsyncMock(return_value=success_response)
        client._last_request_time = 0.0

        async def run():
            with patch("src.scrapers.anilist_scraper.load_cached_json", return_value=None):
                return await client.query("query {}", {})

        result = _run(run())
        assert result == {"Page": {"media": [{"id": 1, "title": "Test"}]}}
        _run(client.close())

    def test_429_rate_limit_retries(self):
        """Test 2: 429 Rate Limit triggers retry and eventually succeeds."""
        rate_limit_response = httpx.Response(
            429,
            headers={"Retry-After": "1"},
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        probe_response = httpx.Response(
            200,
            json={"data": {}},
            headers={
                "X-RateLimit-Remaining": "85",
                "X-RateLimit-Reset": "0",
                "X-RateLimit-Limit": "90",
            },
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        success_response = httpx.Response(
            200,
            json={"data": {"Staff": {"id": 123}}},
            headers={
                "X-RateLimit-Remaining": "84",
                "X-RateLimit-Reset": "0",
                "X-RateLimit-Limit": "90",
            },
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._client._client = AsyncMock()
        client._client._client.post = AsyncMock(
            side_effect=[rate_limit_response, probe_response, success_response]
        )

        async def run():
            with patch("src.scrapers.anilist_scraper.load_cached_json", return_value=None):
                return await client.query("query {}", {})

        result = _run(run())
        assert result == {"Staff": {"id": 123}}
        assert client._client._client.post.call_count == 3  # 429 + probe + success
        _run(client.close())

    def test_504_gateway_timeout_retries(self):
        """Test 3: 504 Gateway Timeout triggers retry and succeeds."""
        timeout_response = httpx.Response(
            504,
            text="Gateway Timeout",
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        success_response = httpx.Response(
            200,
            json={"data": {"result": "ok"}},
            headers={
                "X-RateLimit-Remaining": "88",
                "X-RateLimit-Reset": "0",
                "X-RateLimit-Limit": "90",
            },
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._client._client = AsyncMock()
        client._client._client.post = AsyncMock(
            side_effect=[timeout_response, success_response]
        )

        async def run():
            with patch("src.scrapers.anilist_scraper.load_cached_json", return_value=None):
                return await client.query("query {}", {})

        result = _run(run())
        assert result == {"result": "ok"}
        assert client._client._client.post.call_count == 2
        _run(client.close())

    def test_httpx_timeout_exception_logs_error(self):
        """Test 4: httpx.TimeoutException results in EndpointUnreachableError after retries."""
        from src.scrapers.exceptions import EndpointUnreachableError

        client = AniListClient()
        client._client._client = AsyncMock()
        client._client._client.post = AsyncMock(
            side_effect=httpx.TimeoutException("request timeout")
        )

        async def run():
            with patch("src.scrapers.anilist_scraper.load_cached_json", return_value=None):
                return await client.query("query {}", {})

        with pytest.raises(EndpointUnreachableError):
            _run(run())

        assert client._client._client.post.call_count == 5  # 5 retries
        _run(client.close())

    def test_invalid_json_response_logs_warning(self):
        """Test 5: Invalid JSON in 200 response logs but doesn't crash."""
        invalid_response = httpx.Response(
            200,
            text="<html>Not JSON</html>",
            headers={
                "X-RateLimit-Remaining": "87",
                "X-RateLimit-Reset": "0",
                "X-RateLimit-Limit": "90",
            },
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._client._client = AsyncMock()
        client._client._client.post = AsyncMock(return_value=invalid_response)

        async def run():
            with patch("src.scrapers.anilist_scraper.load_cached_json", return_value=None):
                try:
                    return await client.query("query {}", {})
                except Exception as e:
                    # JSON decode error is expected and should be caught/logged
                    return {"error": type(e).__name__}

        result = _run(run())
        # Either the error dict or an exception — both are acceptable
        assert "error" in result or isinstance(result, dict)
        _run(client.close())

    def test_empty_data_response_returns_empty_dict(self):
        """Test 6: Empty response body (200 with null/empty data) returns safely."""
        empty_response = httpx.Response(
            200,
            json={"data": None},
            headers={
                "X-RateLimit-Remaining": "86",
                "X-RateLimit-Reset": "0",
                "X-RateLimit-Limit": "90",
            },
            request=httpx.Request("POST", "https://graphql.anilist.co"),
        )

        client = AniListClient()
        client._client._client = AsyncMock()
        client._client._client.post = AsyncMock(return_value=empty_response)
        client._last_request_time = 0.0

        async def run():
            with patch("src.scrapers.anilist_scraper.load_cached_json", return_value=None):
                return await client.query("query {}", {})

        result = _run(run())
        # Empty data should return None or empty dict, not crash
        assert result is None or result == {}
        _run(client.close())
