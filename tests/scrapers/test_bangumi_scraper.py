"""Unit tests for BangumiClient (src/scrapers/bangumi_scraper.py).

All HTTP is mocked via httpx.MockTransport — no real network calls.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx
import pytest

from src.scrapers.bangumi_scraper import BangumiClient, _compute_backoff_sleep, _BASE_DELAY
from src.scrapers.queries.bangumi import DEFAULT_USER_AGENT


# ---------------------------------------------------------------------------
# MockTransport helpers
# ---------------------------------------------------------------------------


def _json_response(data: Any, status: int = 200, headers: dict | None = None) -> httpx.Response:
    content = json.dumps(data).encode()
    return httpx.Response(status, content=content, headers=headers or {})


def _404() -> httpx.Response:
    return httpx.Response(404, content=b'{"title":"Not Found"}')


class _SequenceTransport(httpx.AsyncBaseTransport):
    """Returns responses one by one from a queue."""

    def __init__(self, responses: list[httpx.Response]) -> None:
        self._responses = list(responses)
        self._index = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self._index >= len(self._responses):
            raise AssertionError("More requests than expected responses")
        resp = self._responses[self._index]
        self._index += 1
        # Attach request so httpx internals work
        resp.request = request
        return resp

    @property
    def call_count(self) -> int:
        return self._index


class _StaticTransport(httpx.AsyncBaseTransport):
    """Always returns the same response."""

    def __init__(self, response: httpx.Response) -> None:
        self._response = response
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self._response.request = request
        return self._response

    @property
    def call_count(self) -> int:
        return len(self.requests)


# ---------------------------------------------------------------------------
# Helpers to run client with custom transport
# ---------------------------------------------------------------------------


async def _run_with_transport(coro_fn, transport: httpx.AsyncBaseTransport):
    """Instantiate BangumiClient, replace its httpx client transport, then call coro_fn."""
    client = BangumiClient(rate_limit_per_sec=100.0)  # 100/s = ~10ms gap (fast for tests)
    async with client:
        # Swap transport after context manager creates the inner AsyncClient
        client._client = httpx.AsyncClient(
            transport=transport,
            headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"},
        )
        return await coro_fn(client)


# ---------------------------------------------------------------------------
# Tests: fetch_subject_persons
# ---------------------------------------------------------------------------


def test_fetch_subject_persons_parses_list():
    """fetch_subject_persons returns parsed list when API returns JSON array."""
    persons_data = [
        {"id": 1, "name": "Alice", "type": 1, "relation": "Director", "career": ["animator"], "eps": ""},
        {"id": 2, "name": "Bob", "type": 1, "relation": "Producer", "career": [], "eps": ""},
    ]
    transport = _StaticTransport(_json_response(persons_data))

    async def run(client: BangumiClient):
        return await client.fetch_subject_persons(42)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is not None
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["id"] == 2


def test_fetch_subject_persons_returns_empty_list_for_non_list_response():
    """fetch_subject_persons returns [] when API returns unexpected dict (graceful)."""
    transport = _StaticTransport(_json_response({"error": "weird"}))

    async def run(client: BangumiClient):
        return await client.fetch_subject_persons(99)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result == []


# ---------------------------------------------------------------------------
# Tests: fetch_subject_characters / actors nest
# ---------------------------------------------------------------------------


def test_fetch_subject_characters_extracts_actors_nest():
    """fetch_subject_characters returns full character dicts including actors list."""
    characters_data = [
        {
            "id": 10,
            "name": "CharA",
            "type": 1,
            "relation": "主角",
            "summary": "",
            "actors": [
                {"id": 100, "name": "VA1", "type": 1},
                {"id": 101, "name": "VA2", "type": 1},
            ],
        },
        {
            "id": 11,
            "name": "CharB",
            "type": 2,
            "relation": "配角",
            "summary": "",
            "actors": [],
        },
    ]
    transport = _StaticTransport(_json_response(characters_data))

    async def run(client: BangumiClient):
        return await client.fetch_subject_characters(42)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is not None
    assert len(result) == 2
    # actors nest preserved
    assert len(result[0]["actors"]) == 2
    assert result[0]["actors"][0]["name"] == "VA1"
    assert result[1]["actors"] == []


# ---------------------------------------------------------------------------
# Tests: fetch_person
# ---------------------------------------------------------------------------


def test_fetch_person_parses_single_object():
    """fetch_person returns parsed dict for a person detail."""
    person_data = {
        "id": 55,
        "name": "Test Person",
        "type": 1,
        "career": ["animator"],
        "summary": "A test person",
        "infobox": [],
        "stat": {"comments": 10, "collects": 200},
    }
    transport = _StaticTransport(_json_response(person_data))

    async def run(client: BangumiClient):
        return await client.fetch_person(55)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is not None
    assert result["id"] == 55
    assert result["name"] == "Test Person"
    assert result["stat"]["collects"] == 200


def test_fetch_person_returns_none_for_list_response():
    """fetch_person returns None if API unexpectedly returns a list."""
    transport = _StaticTransport(_json_response([{"id": 1}]))

    async def run(client: BangumiClient):
        return await client.fetch_person(1)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is None


# ---------------------------------------------------------------------------
# Tests: fetch_character
# ---------------------------------------------------------------------------


def test_fetch_character_parses_single_object():
    """fetch_character returns parsed dict for a character detail."""
    char_data = {
        "id": 77,
        "name": "Test Character",
        "type": 1,
        "locked": False,
        "summary": "A fictional character",
        "infobox": [],
        "stat": {"comments": 5, "collects": 50},
    }
    transport = _StaticTransport(_json_response(char_data))

    async def run(client: BangumiClient):
        return await client.fetch_character(77)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is not None
    assert result["id"] == 77
    assert result["locked"] is False


# ---------------------------------------------------------------------------
# Tests: 404 → None (no exception, no retry)
# ---------------------------------------------------------------------------


def test_fetch_subject_persons_404_returns_none():
    """404 response should return None without raising or retrying."""
    transport = _StaticTransport(_404())

    async def run(client: BangumiClient):
        return await client.fetch_subject_persons(9999)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is None
    # Only one request should have been made
    assert transport.call_count == 1


def test_fetch_person_404_returns_none():
    transport = _StaticTransport(_404())

    async def run(client: BangumiClient):
        return await client.fetch_person(9999)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is None
    assert transport.call_count == 1


def test_fetch_character_404_returns_none():
    transport = _StaticTransport(_404())

    async def run(client: BangumiClient):
        return await client.fetch_character(9999)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is None
    assert transport.call_count == 1


# ---------------------------------------------------------------------------
# Tests: 429 retries and eventually succeeds
# ---------------------------------------------------------------------------


def test_fetch_person_retries_on_429_then_succeeds():
    """A 429 response followed by 200 should yield the successful result.

    We patch asyncio.sleep so the test doesn't actually wait.
    """
    person_data = {"id": 7, "name": "Retry Person", "type": 1, "career": [], "stat": {}}
    responses = [
        httpx.Response(429, content=b'{"title":"Rate Limited"}', headers={"Retry-After": "1"}),
        _json_response(person_data),
    ]
    transport = _SequenceTransport(responses)

    async def run(client: BangumiClient):
        return await client.fetch_person(7)

    # Patch asyncio.sleep so test is instant
    async def fast_sleep(_n):
        pass

    import unittest.mock as mock
    with mock.patch("asyncio.sleep", side_effect=fast_sleep):
        result = asyncio.run(_run_with_transport(run, transport))

    assert result is not None
    assert result["id"] == 7
    assert transport.call_count == 2


# ---------------------------------------------------------------------------
# Tests: User-Agent header is sent
# ---------------------------------------------------------------------------


def test_user_agent_header_sent_on_all_requests():
    """All requests must include the expected User-Agent header."""
    transport = _StaticTransport(_json_response([]))

    async def run(client: BangumiClient):
        await client.fetch_subject_persons(1)
        await client.fetch_subject_characters(1)
        return None

    asyncio.run(_run_with_transport(run, transport))

    # Every captured request must carry User-Agent
    for req in transport.requests:
        assert "User-Agent" in req.headers
        assert req.headers["User-Agent"] == DEFAULT_USER_AGENT


# ---------------------------------------------------------------------------
# Tests: rate limiter (wall-clock timing)
# ---------------------------------------------------------------------------


def test_rate_limiter_enforces_minimum_interval():
    """Two consecutive fetch calls must take >= 1 second wall time with default rate."""
    responses = [_json_response([]), _json_response([])]
    transport = _SequenceTransport(responses)

    async def run_timed():
        client = BangumiClient(rate_limit_per_sec=1.0)  # real 1/s limit
        async with client:
            client._client = httpx.AsyncClient(
                transport=transport,
                headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"},
            )
            t0 = time.monotonic()
            await client.fetch_subject_persons(1)
            await client.fetch_subject_persons(2)
            elapsed = time.monotonic() - t0
        return elapsed

    elapsed = asyncio.run(run_timed())
    # Should take at least ~1 second (one interval between two calls)
    assert elapsed >= 0.9, f"Expected >= 1s, got {elapsed:.3f}s"


# ---------------------------------------------------------------------------
# Tests: _compute_backoff_sleep helper
# ---------------------------------------------------------------------------


def test_compute_backoff_sleep_no_retry_after():
    """Without Retry-After, returns exponential backoff."""
    assert _compute_backoff_sleep(1, 429, None) == pytest.approx(_BASE_DELAY * 1)
    assert _compute_backoff_sleep(2, 429, None) == pytest.approx(_BASE_DELAY * 2)
    assert _compute_backoff_sleep(3, 500, None) == pytest.approx(_BASE_DELAY * 4)


def test_compute_backoff_sleep_with_integer_retry_after():
    """Integer Retry-After is parsed and used as a floor."""
    sleep = _compute_backoff_sleep(1, 429, "30")
    assert sleep == pytest.approx(30.0)


def test_compute_backoff_sleep_retry_after_zero_falls_back_to_backoff():
    """Retry-After: 0 (or invalid) should fall through to exponential backoff."""
    sleep = _compute_backoff_sleep(1, 429, "0")
    assert sleep == pytest.approx(_BASE_DELAY)


def test_compute_backoff_sleep_caps_at_retry_after_cap():
    """Retry-After larger than cap is clamped."""
    from src.scrapers.bangumi_scraper import _RETRY_AFTER_CAP
    sleep = _compute_backoff_sleep(1, 429, str(int(_RETRY_AFTER_CAP) + 1000))
    assert sleep == pytest.approx(_RETRY_AFTER_CAP)
