"""Tests for KeyframeApiClient — mock HTTP, no real network calls.

pytest-asyncio is not installed in this project; async tests use asyncio.run().
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from src.scrapers.keyframe_api import KeyframeApiClient


def _run(coro):
    """Run a coroutine synchronously."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(status: int, json_data=None, headers=None):
    """Build a mock httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    resp.headers = httpx.Headers(headers or {})
    if json_data is not None:
        resp.json = MagicMock(return_value=json_data)
    return resp


# ---------------------------------------------------------------------------
# get_roles_master
# ---------------------------------------------------------------------------


class TestGetRolesMaster:
    def test_returns_list_on_200(self):
        payload = [{"id": 1, "name_en": "Director", "name_ja": "監督"}]
        resp = _make_response(200, payload)

        async def run():
            client = KeyframeApiClient(delay=0)
            with patch.object(client._client, "get", new_callable=AsyncMock, return_value=resp):
                result = await client.get_roles_master()
            await client.close()
            return result

        assert _run(run()) == payload

    def test_returns_none_on_404(self):
        resp = _make_response(404)

        async def run():
            client = KeyframeApiClient(delay=0)
            with patch.object(client._client, "get", new_callable=AsyncMock, return_value=resp):
                result = await client.get_roles_master()
            await client.close()
            return result

        assert _run(run()) is None

    def test_retries_on_429_and_succeeds(self):
        """Second attempt succeeds after 429."""
        payload = [{"id": 1}]
        resp_429 = _make_response(429, headers={"Retry-After": "1"})
        resp_200 = _make_response(200, payload)

        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            return resp_429 if call_count == 1 else resp_200

        async def run():
            client = KeyframeApiClient(delay=0)
            with (
                patch.object(client._client, "get", side_effect=mock_get),
                patch("asyncio.sleep", new_callable=AsyncMock),
            ):
                result = await client.get_roles_master()
            await client.close()
            return result, call_count

        result, calls = _run(run())
        assert result == payload
        assert calls == 2

    def test_returns_none_after_max_retries_429(self):
        """All attempts return 429 → None after MAX_RETRIES."""
        resp_429 = _make_response(429, headers={"Retry-After": "1"})

        async def run():
            client = KeyframeApiClient(delay=0)
            with (
                patch.object(client._client, "get", new_callable=AsyncMock, return_value=resp_429),
                patch("asyncio.sleep", new_callable=AsyncMock),
            ):
                result = await client.get_roles_master()
            await client.close()
            return result

        assert _run(run()) is None

    def test_retries_on_500(self):
        """500 triggers retry; second attempt succeeds."""
        payload = {"ok": True}
        resp_500 = _make_response(500)
        resp_200 = _make_response(200, payload)

        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            return resp_500 if call_count == 1 else resp_200

        async def run():
            client = KeyframeApiClient(delay=0)
            with (
                patch.object(client._client, "get", side_effect=mock_get),
                patch("asyncio.sleep", new_callable=AsyncMock),
            ):
                result = await client.get_roles_master()
            await client.close()
            return result

        assert _run(run()) == payload

    def test_retries_on_network_error(self):
        """RequestError triggers retry; second attempt succeeds."""
        payload = [{"id": 2}]
        resp_200 = _make_response(200, payload)
        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectError("timeout")
            return resp_200

        async def run():
            client = KeyframeApiClient(delay=0)
            with (
                patch.object(client._client, "get", side_effect=mock_get),
                patch("asyncio.sleep", new_callable=AsyncMock),
            ):
                result = await client.get_roles_master()
            await client.close()
            return result

        assert _run(run()) == payload

    def test_unhandled_status_returns_none(self):
        """Unrecognised status (e.g. 403) returns None without retry."""
        resp_403 = _make_response(403)

        async def run():
            client = KeyframeApiClient(delay=0)
            with patch.object(client._client, "get", new_callable=AsyncMock, return_value=resp_403):
                result = await client.get_roles_master()
            await client.close()
            return result

        assert _run(run()) is None


# ---------------------------------------------------------------------------
# Delay enforcement
# ---------------------------------------------------------------------------


class TestDelayEnforcement:
    def test_delay_is_respected_between_calls(self):
        """Back-to-back call: a throttle sleep is triggered."""
        payload = [{"id": 1}]
        resp = _make_response(200, payload)

        sleep_calls: list[float] = []

        async def fake_sleep(secs: float) -> None:
            sleep_calls.append(secs)

        async def run():
            client = KeyframeApiClient(delay=2.0)
            client._last_request_at = time.monotonic()  # simulate recent request
            with (
                patch.object(client._client, "get", new_callable=AsyncMock, return_value=resp),
                patch("asyncio.sleep", side_effect=fake_sleep),
            ):
                await client.get_roles_master()
            await client.close()
            return sleep_calls

        calls = _run(run())
        assert any(s >= 1.0 for s in calls), f"Expected throttle sleep, got: {calls}"


# ---------------------------------------------------------------------------
# get_person_show / get_preview — smoke routing tests
# ---------------------------------------------------------------------------


class TestPersonAndPreviewRouting:
    def test_get_person_show_calls_correct_url(self):
        payload = {"staff": {"id": 123}, "credits": []}
        resp = _make_response(200, payload)

        called_url = []

        async def mock_get(url, **kwargs):
            called_url.append(url)
            return resp

        async def run():
            client = KeyframeApiClient(delay=0)
            with patch.object(client._client, "get", side_effect=mock_get):
                result = await client.get_person_show(123)
            await client.close()
            return result, called_url[0]

        result, url = _run(run())
        assert result == payload
        assert "id=123" in url
        assert "type=person" in url

    def test_get_preview_calls_preview_endpoint(self):
        payload = {"total": 5512, "recent": [], "airing": [], "data": []}
        resp = _make_response(200, payload)

        called_url = []

        async def mock_get(url, **kwargs):
            called_url.append(url)
            return resp

        async def run():
            client = KeyframeApiClient(delay=0)
            with patch.object(client._client, "get", side_effect=mock_get):
                result = await client.get_preview()
            await client.close()
            return result, called_url[0]

        result, url = _run(run())
        assert result == payload
        assert "preview" in url
