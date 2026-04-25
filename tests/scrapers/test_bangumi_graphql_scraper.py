"""Unit tests for BangumiGraphQLClient (src/scrapers/bangumi_graphql_scraper.py).

All HTTP is mocked via httpx.MockTransport — no real network calls are made.
Tests verify:
    - Single subject fetch (canned GraphQL response → expected data shape)
    - Batched fetch (3 aliased queries → 3 subjects parsed out)
    - Error path: NOT_FOUND → returns None for that id
    - Error path: other GraphQL error → raises ScraperError
    - 429 retry with Retry-After → honors header, eventually succeeds
    - Shared rate limiter: two consecutive calls take >= 1 s wall time
    - User-Agent header is sent on all requests
    - Person fetch + adapter (camelCase → snake_case normalisation)
    - Character fetch + adapter
    - adapt_* helpers produce v0-compatible dicts
    - Batch with partial NOT_FOUND (some null, some data)
"""

from __future__ import annotations

import asyncio
import json
import time
import unittest.mock as mock
from typing import Any

import httpx
import pytest

from src.scrapers.bangumi_graphql_scraper import (
    BangumiGraphQLClient,
    adapt_character_gql_to_v0,
    adapt_person_gql_to_v0,
    adapt_subject_characters_gql,
    adapt_subject_persons_gql,
)
from src.scrapers.bangumi_graphql_scraper import _HOST_RATE_LIMITER
from src.scrapers.exceptions import ScraperError
from src.scrapers.queries.bangumi_graphql import DEFAULT_USER_AGENT


# ---------------------------------------------------------------------------
# MockTransport helpers (copied / adapted from test_bangumi_scraper.py pattern)
# ---------------------------------------------------------------------------


def _gql_response(data: dict[str, Any], status: int = 200, headers: dict | None = None) -> httpx.Response:
    """Wrap data in a standard GraphQL envelope {data: ...}."""
    body = json.dumps({"data": data}).encode()
    return httpx.Response(status, content=body, headers=headers or {})


def _gql_error_response(
    errors: list[dict[str, Any]], status: int = 200
) -> httpx.Response:
    """Return a GraphQL error envelope (HTTP 200 with errors array)."""
    body = json.dumps({"data": None, "errors": errors}).encode()
    return httpx.Response(status, content=body)


def _http_error(status: int, headers: dict | None = None) -> httpx.Response:
    return httpx.Response(status, content=b'{"title":"error"}', headers=headers or {})


class _SequenceTransport(httpx.AsyncBaseTransport):
    """Returns responses one by one from a queue."""

    def __init__(self, responses: list[httpx.Response]) -> None:
        self._responses = list(responses)
        self._index = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self._index >= len(self._responses):
            raise AssertionError("More requests made than expected responses")
        resp = self._responses[self._index]
        self._index += 1
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
# Helper to inject a mock transport into BangumiGraphQLClient
# ---------------------------------------------------------------------------


async def _run_with_transport(coro_fn, transport: httpx.AsyncBaseTransport):
    """Instantiate BangumiGraphQLClient, swap transport, then call coro_fn."""
    # Reset rate limiter so tests don't interfere with each other.
    _HOST_RATE_LIMITER.reset_for_test()
    client = BangumiGraphQLClient()
    async with client:
        client._client = httpx.AsyncClient(
            transport=transport,
            headers={
                "User-Agent": DEFAULT_USER_AGENT,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        return await coro_fn(client)


# ---------------------------------------------------------------------------
# Sample canned data
# ---------------------------------------------------------------------------

_SAMPLE_SUBJECT = {
    "id": 328,
    "name": "まどか☆マギカ",
    "nameCN": "魔法少女小圆",
    "summary": "Test summary",
    "infobox": [{"key": "放送开始", "values": [{"k": None, "v": "2011"}]}],
    "images": {"large": "/l/328.jpg", "medium": "/m/328.jpg", "small": "/s/328.jpg", "grid": "/g/328.jpg"},
    "airtime": {"date": "2011-01-07", "month": 1, "weekday": 5, "year": 2011},
    "rating": {"score": 9.0, "rank": 1, "total": 50000, "count": {}},
    "tags": [{"name": "魔法少女", "count": 100}],
    "persons": [
        {
            "person": {"id": 9527, "name": "新房昭之", "type": 1, "career": ["animator"], "images": None},
            "position": "导演",
        }
    ],
    "characters": [
        {
            "character": {
                "id": 4321,
                "name": "鹿目まどか",
                "images": {"large": "/cl.jpg", "medium": "/cm.jpg", "small": "/cs.jpg", "grid": "/cg.jpg"},
                "summary": "",
            },
            "type": 1,
            "order": "主角",
        }
    ],
}

_SAMPLE_PERSON_GQL = {
    "id": 9527,
    "name": "新房昭之",
    "type": 1,
    "career": ["animator", "director"],
    "summary": "Veteran anime director.",
    "infobox": [{"key": "生日", "values": [{"k": None, "v": "1961"}]}],
    "gender": "male",
    "bloodType": 2,
    "birthYear": 1961,
    "birthMon": 9,
    "birthDay": 27,
    "images": {"large": "/pl.jpg", "medium": "/pm.jpg", "small": "/ps.jpg", "grid": "/pg.jpg"},
    "stat": {"comments": 42, "collects": 3000},
    "locked": False,
    "lastModified": "2024-01-01T00:00:00Z",
}

_SAMPLE_CHARACTER_GQL = {
    "id": 4321,
    "name": "鹿目まどか",
    "type": 1,
    "summary": "The protagonist.",
    "infobox": [],
    "gender": "female",
    "bloodType": 0,
    "birthYear": None,
    "birthMon": None,
    "birthDay": None,
    "images": {"large": "/cl.jpg", "medium": "/cm.jpg", "small": "/cs.jpg", "grid": "/cg.jpg"},
    "stat": {"comments": 5, "collects": 1500},
    "locked": False,
}


# ---------------------------------------------------------------------------
# Test 1: single subject fetch — happy path
# ---------------------------------------------------------------------------


def test_fetch_subject_full_returns_parsed_dict():
    """fetch_subject_full returns the data.subject dict on success."""
    transport = _StaticTransport(_gql_response({"subject": _SAMPLE_SUBJECT}))

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_subject_full(328)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is not None
    assert result["id"] == 328
    assert result["name"] == "まどか☆マギカ"
    assert result["nameCN"] == "魔法少女小圆"
    assert len(result["persons"]) == 1
    assert len(result["characters"]) == 1


# ---------------------------------------------------------------------------
# Test 2: batched fetch — three aliased queries parsed correctly
# ---------------------------------------------------------------------------


def test_fetch_subjects_batched_parses_three_subjects():
    """fetch_subjects_batched correctly maps s{id} aliases back to integer keys."""
    data = {
        "s100": {**_SAMPLE_SUBJECT, "id": 100, "name": "Subject100"},
        "s200": {**_SAMPLE_SUBJECT, "id": 200, "name": "Subject200"},
        "s300": {**_SAMPLE_SUBJECT, "id": 300, "name": "Subject300"},
    }
    transport = _StaticTransport(_gql_response(data))

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_subjects_batched([100, 200, 300])

    result = asyncio.run(_run_with_transport(run, transport))
    assert len(result) == 3
    assert result[100]["name"] == "Subject100"
    assert result[200]["name"] == "Subject200"
    assert result[300]["name"] == "Subject300"


# ---------------------------------------------------------------------------
# Test 3: NOT_FOUND error → returns None for single fetch
# ---------------------------------------------------------------------------


def test_fetch_subject_full_not_found_returns_none():
    """A GraphQL NOT_FOUND error → fetch_subject_full returns None (no exception)."""
    error_resp = _gql_error_response(
        [{"message": "not found", "extensions": {"code": "NOT_FOUND"}}]
    )
    transport = _StaticTransport(error_resp)

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_subject_full(9999)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is None
    assert transport.call_count == 1  # no retry on NOT_FOUND


# ---------------------------------------------------------------------------
# Test 4: other GraphQL error → raises ScraperError
# ---------------------------------------------------------------------------


def test_other_graphql_error_raises_scraper_error():
    """A non-NOT_FOUND GraphQL error should raise ScraperError."""
    error_resp = _gql_error_response(
        [{"message": "internal error", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}]
    )
    transport = _StaticTransport(error_resp)

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_subject_full(328)

    with pytest.raises(ScraperError):
        asyncio.run(_run_with_transport(run, transport))


# ---------------------------------------------------------------------------
# Test 5: 429 retry with Retry-After → honors header, eventually succeeds
# ---------------------------------------------------------------------------


def test_fetch_subject_retries_on_429_then_succeeds():
    """A 429 response followed by a 200 should yield the successful result."""
    responses = [
        httpx.Response(429, content=b'{"title":"Rate Limited"}', headers={"Retry-After": "1"}),
        _gql_response({"subject": _SAMPLE_SUBJECT}),
    ]
    transport = _SequenceTransport(responses)

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_subject_full(328)

    async def fast_sleep(_n):
        pass

    with mock.patch("asyncio.sleep", side_effect=fast_sleep):
        result = asyncio.run(_run_with_transport(run, transport))

    assert result is not None
    assert result["id"] == 328
    assert transport.call_count == 2


# ---------------------------------------------------------------------------
# Test 6: shared rate limiter — two consecutive calls take >= 1 s wall time
# ---------------------------------------------------------------------------


def test_shared_rate_limiter_enforces_one_req_per_sec():
    """Two consecutive fetch_subject_full calls must span >= 1 s wall time.

    This verifies the shared _HOST_RATE_LIMITER works for the GraphQL client.
    """
    data = {"subject": _SAMPLE_SUBJECT}
    responses = [_gql_response(data), _gql_response(data)]
    transport = _SequenceTransport(responses)

    async def run_timed():
        # Use the real rate limiter (reset first so no carry-over).
        _HOST_RATE_LIMITER.reset_for_test()
        client = BangumiGraphQLClient()
        async with client:
            client._client = httpx.AsyncClient(
                transport=transport,
                headers={
                    "User-Agent": DEFAULT_USER_AGENT,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
            )
            t0 = time.monotonic()
            await client.fetch_subject_full(328)
            await client.fetch_subject_full(328)
            return time.monotonic() - t0

    elapsed = asyncio.run(run_timed())
    assert elapsed >= 0.9, f"Expected >= 1 s, got {elapsed:.3f} s"


# ---------------------------------------------------------------------------
# Test 7: User-Agent header is sent on all requests
# ---------------------------------------------------------------------------


def test_user_agent_header_sent():
    """All GraphQL POST requests must include the expected User-Agent header."""
    transport = _StaticTransport(_gql_response({"subject": _SAMPLE_SUBJECT}))

    async def run(client: BangumiGraphQLClient):
        await client.fetch_subject_full(328)

    asyncio.run(_run_with_transport(run, transport))

    for req in transport.requests:
        assert "User-Agent" in req.headers
        assert req.headers["User-Agent"] == DEFAULT_USER_AGENT


# ---------------------------------------------------------------------------
# Test 8: person fetch + adapt_person_gql_to_v0 normalisation
# ---------------------------------------------------------------------------


def test_fetch_person_returns_gql_dict():
    """fetch_person returns the raw GraphQL person dict (camelCase)."""
    transport = _StaticTransport(_gql_response({"person": _SAMPLE_PERSON_GQL}))

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_person(9527)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is not None
    assert result["id"] == 9527
    # Raw response has camelCase keys
    assert "bloodType" in result


def test_adapt_person_gql_to_v0_converts_camel_to_snake():
    """adapt_person_gql_to_v0 renames camelCase fields to v0 snake_case."""
    adapted = adapt_person_gql_to_v0(_SAMPLE_PERSON_GQL.copy())

    assert "blood_type" in adapted
    assert adapted["blood_type"] == 2
    assert "birth_year" in adapted
    assert adapted["birth_year"] == 1961
    assert "birth_mon" in adapted
    assert adapted["birth_mon"] == 9
    assert "birth_day" in adapted
    assert adapted["birth_day"] == 27
    assert "last_modified" in adapted
    assert adapted["last_modified"] == "2024-01-01T00:00:00Z"
    # Original camelCase keys should be gone
    assert "bloodType" not in adapted
    assert "birthYear" not in adapted


def test_adapt_person_gql_infobox_values_renamed_to_value():
    """adapt_person_gql_to_v0 renames infobox 'values' key to 'value'."""
    adapted = adapt_person_gql_to_v0(_SAMPLE_PERSON_GQL.copy())
    # infobox entry had "values" from GQL schema → must become "value"
    assert "value" in adapted["infobox"][0]
    assert "values" not in adapted["infobox"][0]


# ---------------------------------------------------------------------------
# Test 9: character fetch + adapt_character_gql_to_v0 normalisation
# ---------------------------------------------------------------------------


def test_fetch_character_returns_gql_dict():
    """fetch_character returns the raw GraphQL character dict."""
    transport = _StaticTransport(_gql_response({"character": _SAMPLE_CHARACTER_GQL}))

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_character(4321)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is not None
    assert result["id"] == 4321
    assert result["name"] == "鹿目まどか"


def test_adapt_character_gql_to_v0_converts_camel_to_snake():
    """adapt_character_gql_to_v0 renames camelCase fields to v0 snake_case."""
    adapted = adapt_character_gql_to_v0(_SAMPLE_CHARACTER_GQL.copy())

    assert "blood_type" in adapted
    assert adapted["blood_type"] == 0
    assert "birth_year" in adapted
    assert adapted["birth_year"] is None
    # Original camelCase keys gone
    assert "bloodType" not in adapted
    assert "birthYear" not in adapted


# ---------------------------------------------------------------------------
# Test 10: adapt_subject_persons_gql extracts person list
# ---------------------------------------------------------------------------


def test_adapt_subject_persons_gql_extracts_persons():
    """adapt_subject_persons_gql returns a v0-compatible person list."""
    persons = adapt_subject_persons_gql(328, _SAMPLE_SUBJECT)
    assert len(persons) == 1
    p = persons[0]
    assert p["id"] == 9527
    assert p["name"] == "新房昭之"
    assert p["relation"] == "导演"
    assert isinstance(p["career"], list)
    assert isinstance(p["images"], dict)


# ---------------------------------------------------------------------------
# Test 11: adapt_subject_characters_gql extracts characters + actors
# ---------------------------------------------------------------------------


def test_adapt_subject_characters_gql_extracts_characters_and_actors():
    """adapt_subject_characters_gql returns characters."""
    chars = adapt_subject_characters_gql(328, _SAMPLE_SUBJECT)
    assert len(chars) == 1
    c = chars[0]
    assert c["id"] == 4321
    assert c["name"] == "鹿目まどか"
    assert c["relation"] == "主角"


# ---------------------------------------------------------------------------
# Test 12: batched fetch with partial null (some subjects not found)
# ---------------------------------------------------------------------------


def test_fetch_subjects_batched_skips_null_subjects():
    """fetch_subjects_batched omits subjects that the server returned as null."""
    data = {
        "s100": {**_SAMPLE_SUBJECT, "id": 100, "name": "Found"},
        "s200": None,  # server returned null for this id
    }
    transport = _StaticTransport(_gql_response(data))

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_subjects_batched([100, 200])

    result = asyncio.run(_run_with_transport(run, transport))
    assert 100 in result
    assert 200 not in result  # null → omitted


# ---------------------------------------------------------------------------
# Test 13: HTTP 5xx retries and eventually succeeds
# ---------------------------------------------------------------------------


def test_fetch_subject_retries_on_500_then_succeeds():
    """A 500 response followed by a 200 should yield the successful result."""
    responses = [
        httpx.Response(500, content=b'{"message":"internal error"}'),
        _gql_response({"subject": _SAMPLE_SUBJECT}),
    ]
    transport = _SequenceTransport(responses)

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_subject_full(328)

    async def fast_sleep(_n):
        pass

    with mock.patch("asyncio.sleep", side_effect=fast_sleep):
        result = asyncio.run(_run_with_transport(run, transport))

    assert result is not None
    assert transport.call_count == 2


# ---------------------------------------------------------------------------
# Test 14: person NOT_FOUND → returns None
# ---------------------------------------------------------------------------


def test_fetch_person_not_found_returns_none():
    """GraphQL NOT_FOUND for person → fetch_person returns None."""
    error_resp = _gql_error_response(
        [{"message": "not found", "extensions": {"code": "NOT_FOUND"}}]
    )
    transport = _StaticTransport(error_resp)

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_person(99999)

    result = asyncio.run(_run_with_transport(run, transport))
    assert result is None


# ---------------------------------------------------------------------------
# Test 15: SUBJECT_BATCH_QUERY raises ValueError on empty list
# ---------------------------------------------------------------------------


def test_fetch_subjects_batched_empty_list_raises():
    """fetch_subjects_batched with empty list raises ValueError immediately."""

    async def run(client: BangumiGraphQLClient):
        return await client.fetch_subjects_batched([])

    transport = _StaticTransport(_gql_response({}))
    with pytest.raises(ValueError, match="non-empty"):
        asyncio.run(_run_with_transport(run, transport))
