"""ANN HTTP client tests — retry behavior and rate limiting.

Uses httpx.MockTransport to simulate server responses. No network.
pytest-asyncio is not installed in this project; we wrap each async
test in asyncio.run() per the existing convention.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable

import httpx
import pytest

from src.scrapers import ann_scraper
from src.scrapers.ann_scraper import AnnClient


def _run(coro: Awaitable):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Skip backoff sleeps so retry tests run instantly."""
    async def _instant(*args, **kwargs):
        return None

    monkeypatch.setattr(ann_scraper.asyncio, "sleep", _instant)


def _make_client_with(handler) -> AnnClient:
    """Create an AnnClient whose AsyncClient is backed by a MockTransport.

    Bypasses __init__'s real httpx.AsyncClient construction entirely so that
    no real network ever opens.
    """
    client = AnnClient.__new__(AnnClient)
    client._delay = 0.0
    client._last_request = 0.0
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


def test_get_retries_500_then_succeeds():
    calls = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(500, text="server error")
        return httpx.Response(200, text="<ann><ok/></ann>")

    client = _make_client_with(handler)
    try:
        resp = _run(client.get("https://example.com/api.xml", max_attempts=5))
    finally:
        _run(client.close())

    assert resp.status_code == 200
    assert calls["n"] == 3


def test_get_retries_502_504():
    """500-504 must all be retryable (was missing 500/502/504 before fix)."""
    seq = [502, 504, 200]
    idx = {"i": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        status = seq[idx["i"]]
        idx["i"] += 1
        body = "<ann/>" if status == 200 else "err"
        return httpx.Response(status, text=body)

    client = _make_client_with(handler)
    try:
        resp = _run(client.get("https://example.com", max_attempts=5))
    finally:
        _run(client.close())

    assert resp.status_code == 200
    assert idx["i"] == 3


def test_get_respects_retry_after_header():
    n = {"i": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        n["i"] += 1
        if n["i"] == 1:
            return httpx.Response(429, headers={"Retry-After": "5"}, text="slow down")
        return httpx.Response(200, text="<ann/>")

    client = _make_client_with(handler)
    try:
        resp = _run(client.get("https://example.com", max_attempts=3))
    finally:
        _run(client.close())

    assert resp.status_code == 200
    assert n["i"] == 2


def test_get_retries_remote_protocol_error():
    """httpx.RemoteProtocolError must be retryable (was missing before)."""
    calls = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.RemoteProtocolError("connection broke")
        return httpx.Response(200, text="<ann/>")

    client = _make_client_with(handler)
    try:
        resp = _run(client.get("https://example.com", max_attempts=3))
    finally:
        _run(client.close())

    assert resp.status_code == 200
    assert calls["n"] == 2


def test_get_gives_up_after_max_attempts():
    """After max_attempts, the last error must propagate (not loop forever)."""
    calls = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503, text="unavailable")

    client = _make_client_with(handler)
    try:
        with pytest.raises(httpx.HTTPStatusError):
            _run(client.get("https://example.com", max_attempts=3))
    finally:
        _run(client.close())

    assert calls["n"] == 3


def test_get_does_not_retry_4xx_other_than_429():
    """404 should not retry — it's a permanent error."""
    calls = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404, text="not found")

    client = _make_client_with(handler)
    try:
        with pytest.raises(httpx.HTTPStatusError):
            _run(client.get("https://example.com", max_attempts=5))
    finally:
        _run(client.close())

    assert calls["n"] == 1
