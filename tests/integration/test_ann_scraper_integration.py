"""ANN scraper integration tests — httpx.MockTransport based retry/timeout scenarios.

Tests cover:
- Normal operation: successful XML fetch
- 429 Rate Limit: retry after wait
- 504 Gateway Timeout: retry and succeed
- Timeout exception: graceful error
- Invalid XML: malformed response
- Empty response: 200 with no content

Uses httpx.MockTransport for zero-network testing + asyncio.run() for async tests.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from src.scrapers import ann_scraper
from src.scrapers.ann_scraper import AnnClient


def _run(coro):
    """Helper to run async coroutines in sync tests."""
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Skip backoff sleeps so retry tests run instantly."""
    async def _instant(*args, **kwargs):
        return None

    monkeypatch.setattr(ann_scraper.asyncio, "sleep", _instant)


def _make_client_with(handler) -> AnnClient:
    """Create AnnClient with MockTransport-backed AsyncClient."""
    client = AnnClient.__new__(AnnClient)
    client._delay = 0.0
    client._last_request = 0.0
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


class TestAnnScraper:
    """Integration tests for AnnClient with mock HTTP transport."""

    def test_successful_fetch_returns_xml(self):
        """Test 1: Normal operation — successful fetch returns XML data."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                text='<ann><anime><title>Test Anime</title></anime></ann>',
            )

        client = _make_client_with(handler)
        try:
            resp = _run(client.get("https://example.com/anime.xml", max_attempts=3))
            assert resp.status_code == 200
            assert "Test Anime" in resp.text
        finally:
            _run(client.close())

    def test_429_rate_limit_retries(self):
        """Test 2: 429 Rate Limit triggers retry and eventually succeeds."""
        calls = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(
                    429,
                    headers={"Retry-After": "1"},
                    text="rate limited",
                )
            return httpx.Response(
                200,
                text='<ann><result>ok</result></ann>',
            )

        client = _make_client_with(handler)
        try:
            resp = _run(client.get("https://example.com/staff.xml", max_attempts=3))
            assert resp.status_code == 200
            assert calls["n"] == 2
        finally:
            _run(client.close())

    def test_504_gateway_timeout_retries(self):
        """Test 3: 504 Gateway Timeout triggers retry and succeeds."""
        calls = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(
                    504,
                    text="Gateway Timeout",
                )
            return httpx.Response(
                200,
                text='<ann><data><item id="1"/></data></ann>',
            )

        client = _make_client_with(handler)
        try:
            resp = _run(client.get("https://example.com/api.xml", max_attempts=5))
            assert resp.status_code == 200
            assert calls["n"] == 2
        finally:
            _run(client.close())

    def test_httpx_timeout_exception_raises(self):
        """Test 4: httpx.TimeoutException after max_attempts raises HTTPStatusError."""
        def handler(req: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("request timeout")

        client = _make_client_with(handler)
        try:
            with pytest.raises(httpx.TimeoutException):
                _run(client.get("https://example.com", max_attempts=3))
        finally:
            _run(client.close())

    def test_malformed_xml_response_returns_text(self):
        """Test 5: Malformed XML (200 status) is returned as-is, not parsed here."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                text="<ann><broken><unclosed>",
            )

        client = _make_client_with(handler)
        try:
            resp = _run(client.get("https://example.com/bad.xml", max_attempts=3))
            assert resp.status_code == 200
            # Malformed XML is returned but not crash — parsing happens at higher layer
            assert "<ann>" in resp.text
        finally:
            _run(client.close())

    def test_empty_response_body(self):
        """Test 6: Empty response (200 with no body) is handled gracefully."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text="")

        client = _make_client_with(handler)
        try:
            resp = _run(client.get("https://example.com/empty.xml", max_attempts=3))
            assert resp.status_code == 200
            assert resp.text == ""
        finally:
            _run(client.close())
