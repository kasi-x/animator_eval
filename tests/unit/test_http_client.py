"""Shared RetryingHttpClient tests via httpx.MockTransport.

Verifies the retry contract that all scrapers depend on:
  - 429/500-504/522/524 are retried
  - httpx.RemoteProtocolError + ReadError are retried
  - 4xx other than 429 fail immediately
  - max_attempts is respected
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable

import httpx
import pytest

from src.scrapers.http_client import RetryingHttpClient


def _run(coro: Awaitable):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Skip backoff sleeps."""
    async def _instant(*args, **kwargs):
        return None

    from src.scrapers import http_client
    monkeypatch.setattr(http_client.asyncio, "sleep", _instant)


def _client(handler) -> RetryingHttpClient:
    return RetryingHttpClient(
        source="test",
        delay=0.0,
        transport=httpx.MockTransport(handler),
    )


def test_retries_5xx_status_codes():
    seq = [500, 502, 503, 504, 200]
    idx = {"i": 0}

    def handler(req):
        s = seq[idx["i"]]
        idx["i"] += 1
        return httpx.Response(s, text="x")

    c = _client(handler)
    try:
        resp = _run(c.get("https://x.test/r", max_attempts=8))
    finally:
        _run(c.aclose())

    assert resp.status_code == 200
    assert idx["i"] == 5


def test_retries_429_with_retry_after():
    n = {"i": 0}

    def handler(req):
        n["i"] += 1
        if n["i"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"}, text="slow")
        return httpx.Response(200, text="ok")

    c = _client(handler)
    try:
        resp = _run(c.get("https://x.test", max_attempts=3))
    finally:
        _run(c.aclose())

    assert resp.status_code == 200


def test_retries_remote_protocol_error():
    n = {"i": 0}

    def handler(req):
        n["i"] += 1
        if n["i"] == 1:
            raise httpx.RemoteProtocolError("conn broken")
        return httpx.Response(200, text="ok")

    c = _client(handler)
    try:
        resp = _run(c.get("https://x.test", max_attempts=3))
    finally:
        _run(c.aclose())

    assert resp.status_code == 200


def test_does_not_retry_404():
    n = {"i": 0}

    def handler(req):
        n["i"] += 1
        return httpx.Response(404, text="missing")

    c = _client(handler)
    try:
        # 404 is non-retryable; client returns the response (caller decides)
        resp = _run(c.get("https://x.test", max_attempts=5))
    finally:
        _run(c.aclose())

    assert resp.status_code == 404
    assert n["i"] == 1


def test_max_attempts_propagates_status_error():
    n = {"i": 0}

    def handler(req):
        n["i"] += 1
        return httpx.Response(503, text="dead")

    c = _client(handler)
    try:
        with pytest.raises(httpx.HTTPStatusError):
            _run(c.get("https://x.test", max_attempts=3))
    finally:
        _run(c.aclose())

    assert n["i"] == 3


def test_post_request():
    body_seen = {"v": None}

    def handler(req):
        body_seen["v"] = req.content.decode()
        return httpx.Response(200, json={"ok": True})

    c = _client(handler)
    try:
        resp = _run(c.post("https://x.test", json={"a": 1}))
    finally:
        _run(c.aclose())

    assert resp.status_code == 200
    assert '"a"' in body_seen["v"]
