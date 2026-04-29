"""Tests for KeyframeApiClient — mock HTTP, no real network calls.

KeyframeApiClient is a thin wrapper over RetryingHttpClient. These tests
cover the wrapper's own contracts:

  • 200 → returns parsed JSON
  • 404 → returns None
  • 429 → raises KeyframeQuotaExceeded (daily quota; do not retry)
  • URL routing for the named endpoint methods

Retry/throttle/Retry-After behaviour is owned by RetryingHttpClient and
covered by its own test module.

pytest-asyncio is not installed in this project; async tests use asyncio.run().
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.scrapers.keyframe_api import KeyframeApiClient, KeyframeQuotaExceeded


def _run(coro):
    """Run a coroutine synchronously."""
    return asyncio.run(coro)


def _make_response(status: int, json_data=None, text: str | None = None):
    """Build a mock httpx.Response with the given status / payload."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    resp.headers = httpx.Headers({})
    if json_data is not None:
        resp.json = MagicMock(return_value=json_data)
    if text is not None:
        resp.text = text
    resp.raise_for_status = MagicMock()
    return resp


def _patch_request(client: KeyframeApiClient, *, return_value=None, side_effect=None):
    """Patch the underlying RetryingHttpClient.request used by KeyframeApiClient."""
    kwargs: dict = {"new_callable": AsyncMock}
    if return_value is not None:
        kwargs["return_value"] = return_value
    if side_effect is not None:
        kwargs.pop("new_callable")
        kwargs["side_effect"] = side_effect
    return patch.object(client._client, "request", **kwargs)


# ---------------------------------------------------------------------------
# JSON status handling — 200 / 404 / 429
# ---------------------------------------------------------------------------


class TestGetJsonStatusHandling:
    def test_returns_parsed_payload_on_200(self):
        payload = [{"id": 1, "name_en": "Director", "name_ja": "監督"}]
        resp = _make_response(200, payload)

        async def run():
            client = KeyframeApiClient(delay=0)
            with _patch_request(client, return_value=resp):
                result = await client.get_roles_master()
            await client.close()
            return result

        assert _run(run()) == payload

    def test_returns_none_on_404(self):
        resp = _make_response(404)

        async def run():
            client = KeyframeApiClient(delay=0)
            with _patch_request(client, return_value=resp):
                result = await client.get_roles_master()
            await client.close()
            return result

        assert _run(run()) is None

    def test_raises_quota_exceeded_on_429(self):
        """429 means the daily quota is exhausted — never silently swallowed."""
        resp = _make_response(429)

        async def run():
            client = KeyframeApiClient(delay=0)
            with _patch_request(client, return_value=resp):
                with pytest.raises(KeyframeQuotaExceeded):
                    await client.get_roles_master()
            await client.close()

        _run(run())


# ---------------------------------------------------------------------------
# HTML status handling — 200 / 404 / 429
# ---------------------------------------------------------------------------


class TestGetHtmlStatusHandling:
    def test_returns_text_on_200(self):
        resp = _make_response(200, text="<html>ok</html>")

        async def run():
            client = KeyframeApiClient(delay=0)
            with _patch_request(client, return_value=resp):
                result = await client.get_anime_page("slug")
            await client.close()
            return result

        assert _run(run()) == "<html>ok</html>"

    def test_returns_none_on_404(self):
        resp = _make_response(404)

        async def run():
            client = KeyframeApiClient(delay=0)
            with _patch_request(client, return_value=resp):
                result = await client.get_anime_page("missing")
            await client.close()
            return result

        assert _run(run()) is None

    def test_raises_quota_exceeded_on_429(self):
        resp = _make_response(429)

        async def run():
            client = KeyframeApiClient(delay=0)
            with _patch_request(client, return_value=resp):
                with pytest.raises(KeyframeQuotaExceeded):
                    await client.get_sitemap()
            await client.close()

        _run(run())


# ---------------------------------------------------------------------------
# URL routing for the named endpoints
# ---------------------------------------------------------------------------


class TestEndpointUrlRouting:
    def _capture_first_url(self, client: KeyframeApiClient, payload):
        """Patch request to record the URL of the first call."""
        captured: list[str] = []
        resp = _make_response(200, payload)

        async def fake_request(method, url, **kwargs):
            captured.append(url)
            return resp

        return patch.object(client._client, "request", side_effect=fake_request), captured

    def test_get_person_show_routes_correctly(self):
        payload = {"staff": {"id": 123}, "credits": []}

        async def run():
            client = KeyframeApiClient(delay=0)
            ctx, captured = self._capture_first_url(client, payload)
            with ctx:
                result = await client.get_person_show(123)
            await client.close()
            return result, captured[0]

        result, url = _run(run())
        assert result == payload
        assert "/api/person/show.php" in url
        assert "id=123" in url
        assert "type=person" in url

    def test_get_preview_routes_correctly(self):
        payload = {"total": 5512, "recent": [], "airing": [], "data": []}

        async def run():
            client = KeyframeApiClient(delay=0)
            ctx, captured = self._capture_first_url(client, payload)
            with ctx:
                result = await client.get_preview()
            await client.close()
            return result, captured[0]

        result, url = _run(run())
        assert result == payload
        assert "/api/stafflists/preview.php" in url

    def test_search_staff_routes_with_query_and_offset(self):
        payload = {"total": 0, "results": []}

        async def run():
            client = KeyframeApiClient(delay=0)
            ctx, captured = self._capture_first_url(client, payload)
            with ctx:
                await client.search_staff("宮崎", offset=50)
            await client.close()
            return captured[0]

        url = _run(run())
        assert "/api/search/" in url
        assert "type=staff" in url
        assert "offset=50" in url

    def test_translate_name_uses_lang_param(self):
        payload = []

        async def run():
            client = KeyframeApiClient(delay=0)
            ctx, captured = self._capture_first_url(client, payload)
            with ctx:
                await client.translate_name("Hayao", lang="en")
            await client.close()
            return captured[0]

        url = _run(run())
        assert "/api/data/translate.v4.php" in url
        assert "en=Hayao" in url

    def test_get_sitemap_returns_text(self):
        resp = _make_response(200, text="<urlset/>")

        async def run():
            client = KeyframeApiClient(delay=0)
            with _patch_request(client, return_value=resp):
                result = await client.get_sitemap()
            await client.close()
            return result

        assert _run(run()) == "<urlset/>"
