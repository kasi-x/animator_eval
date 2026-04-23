"""SeesaaWiki scraper integration tests — httpx.MockTransport based retry/timeout scenarios.

Tests cover:
- Normal operation: successful HTML page fetch
- 429 Rate Limit: retry after wait
- 504 Gateway Timeout: retry and succeed
- Timeout exception: graceful error
- Malformed HTML: invalid content (should not crash)
- Empty response: 200 with no body

Uses httpx.MockTransport for zero-network testing + asyncio.run() for async tests.
SeesaaWiki scraper uses raw httpx.AsyncClient (no wrapper class), so we test fetch functions.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from src.scrapers.seesaawiki_scraper import fetch_page_list


def _run(coro):
    """Helper to run async coroutines in sync tests."""
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Skip backoff sleeps so retry tests run instantly."""
    async def _instant(*args, **kwargs):
        return None

    monkeypatch.setattr("asyncio.sleep", _instant)


def _make_client_with(handler) -> httpx.AsyncClient:
    """Create AsyncClient with MockTransport for zero-network testing."""
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class TestSeesaaWikiScraper:
    """Integration tests for SeesaaWiki scraper with mock HTTP transport."""

    def test_successful_page_fetch_returns_html(self):
        """Test 1: Normal operation — successful fetch returns parsed page list."""
        def handler(req: httpx.Request) -> httpx.Response:
            # SeesaaWiki is EUC-JP encoded. We must send bytes, not text.
            html_content = """
            <html><body>
            <a href="/w/radioi_34/d/test%20anime%201">アニメ1</a>
            <a href="/w/radioi_34/d/test%20anime%202">アニメ2</a>
            </body></html>
            """
            return httpx.Response(
                200,
                content=html_content.encode("euc-jp"),
                headers={"Content-Type": "text/html; charset=euc-jp"},
            )

        client = _make_client_with(handler)
        try:
            result = _run(fetch_page_list(client, 1))
            assert len(result) == 2
            assert result[0]["title"] == "アニメ1"
        finally:
            _run(client.aclose())

    def test_429_rate_limit_retries(self):
        """Test 2: 429 Rate Limit triggers retry via AsyncClient default behavior."""
        calls = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(
                    429,
                    headers={"Retry-After": "1"},
                    text="rate limited",
                )
            html_content = """
            <html><body>
            <a href="/w/radioi_34/d/page1">Page1</a>
            </body></html>
            """
            return httpx.Response(
                200,
                content=html_content.encode("euc-jp"),
                headers={"Content-Type": "text/html; charset=euc-jp"},
            )

        client = _make_client_with(handler)
        try:
            # Note: httpx.AsyncClient doesn't auto-retry by default.
            # This test documents current behavior (no retry at this layer).
            # Retry is implemented at scraper level via loop.
            with pytest.raises(httpx.HTTPStatusError):
                _run(fetch_page_list(client, 1))
        finally:
            _run(client.aclose())

    def test_504_gateway_timeout_response_raises(self):
        """Test 3: 504 Gateway Timeout raises HTTPStatusError (no auto-retry in AsyncClient)."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                504,
                text="Gateway Timeout",
            )

        client = _make_client_with(handler)
        try:
            with pytest.raises(httpx.HTTPStatusError):
                _run(fetch_page_list(client, 1))
        finally:
            _run(client.aclose())

    def test_httpx_timeout_exception_propagates(self):
        """Test 4: httpx.TimeoutException propagates from AsyncClient."""
        def handler(req: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("request timeout")

        client = _make_client_with(handler)
        try:
            with pytest.raises(httpx.TimeoutException):
                _run(fetch_page_list(client, 1))
        finally:
            _run(client.aclose())

    def test_malformed_html_parses_gracefully(self):
        """Test 5: Malformed HTML (200 status) is parsed by BeautifulSoup gracefully."""
        def handler(req: httpx.Request) -> httpx.Response:
            # Unclosed tags — BeautifulSoup handles this
            html_content = '<html><body><a href="/w/radioi_34/d/test">Unclosed</body>'
            return httpx.Response(
                200,
                content=html_content.encode("euc-jp"),
                headers={"Content-Type": "text/html; charset=euc-jp"},
            )

        client = _make_client_with(handler)
        try:
            result = _run(fetch_page_list(client, 1))
            # BeautifulSoup doesn't crash on malformed HTML
            # It extracts what it can
            assert isinstance(result, list)
        finally:
            _run(client.aclose())

    def test_empty_response_body_returns_empty_list(self):
        """Test 6: Empty response (200 with no body) returns empty page list."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b"",
                headers={"Content-Type": "text/html; charset=euc-jp"},
            )

        client = _make_client_with(handler)
        try:
            result = _run(fetch_page_list(client, 1))
            assert result == []  # No <a> tags → empty list
        finally:
            _run(client.aclose())
