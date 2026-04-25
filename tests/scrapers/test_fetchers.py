"""Tests for src.scrapers.fetchers."""
from __future__ import annotations

import asyncio

import pytest

from src.scrapers.fetchers import HtmlFetcher, JsonFetcher, XmlBatchFetcher


def _run(coro):
    return asyncio.run(coro)


class _Resp:
    def __init__(self, status: int, text: str = "", json_data=None):
        self.status_code = status
        self.text = text
        self._json = json_data or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json


class _MockClient:
    def __init__(self, responses: dict[str, _Resp]):
        self._resp = responses
        self.calls: list[str] = []

    async def get(self, url: str, **_kw) -> _Resp:
        self.calls.append(url)
        return self._resp.get(url, _Resp(200, "<default/>"))


@pytest.fixture(autouse=True)
def _no_cache(monkeypatch):
    monkeypatch.setenv("SCRAPER_CACHE_DISABLE", "1")


# ── HtmlFetcher ───────────────────────────────────────────────────────────────


def test_html_fetcher_returns_text():
    client = _MockClient({"https://x.com/42": _Resp(200, "<html>ok</html>")})
    fetcher = HtmlFetcher(client, "https://x.com/{id}", source="test")
    assert _run(fetcher(42)) == "<html>ok</html>"


def test_html_fetcher_404_returns_none():
    client = _MockClient({"https://x.com/99": _Resp(404)})
    fetcher = HtmlFetcher(client, "https://x.com/{id}", source="test")
    assert _run(fetcher(99)) is None


def test_html_fetcher_no_namespace_always_fetches(monkeypatch):
    """Without a namespace, every call hits the network (no caching)."""
    client = _MockClient({"https://x.com/1": _Resp(200, "<p>a</p>")})
    fetcher = HtmlFetcher(client, "https://x.com/{id}", namespace=None, source="test")
    _run(fetcher(1))
    _run(fetcher(1))
    assert len(client.calls) == 2


# ── XmlBatchFetcher ───────────────────────────────────────────────────────────


def test_xml_batch_fetcher_slash_ids():
    url = "https://ann.net/api.xml?anime=1/2/3"
    client = _MockClient({url: _Resp(200, "<ann/>")})
    fetcher = XmlBatchFetcher(client, "https://ann.net/api.xml", id_param_name="anime", source="test")
    assert _run(fetcher([1, 2, 3])) == "<ann/>"
    assert client.calls[0] == url


def test_xml_batch_fetcher_single_id():
    url = "https://ann.net/api.xml?anime=5"
    client = _MockClient({url: _Resp(200, "<root/>")})
    fetcher = XmlBatchFetcher(client, "https://ann.net/api.xml", id_param_name="anime", source="test")
    assert _run(fetcher(5)) == "<root/>"


def test_xml_batch_fetcher_404_none():
    url = "https://ann.net/api.xml?anime=999"
    client = _MockClient({url: _Resp(404)})
    fetcher = XmlBatchFetcher(client, "https://ann.net/api.xml", id_param_name="anime", source="test")
    assert _run(fetcher(999)) is None


# ── JsonFetcher ───────────────────────────────────────────────────────────────


def test_json_fetcher_returns_dict():
    url = "https://api.ex.com/42"
    client = _MockClient({url: _Resp(200, json_data={"id": 42})})
    fetcher = JsonFetcher(client, lambda id_: f"https://api.ex.com/{id_}", source="test")
    assert _run(fetcher(42)) == {"id": 42}


def test_json_fetcher_404_none():
    url = "https://api.ex.com/0"
    client = _MockClient({url: _Resp(404)})
    fetcher = JsonFetcher(client, lambda id_: f"https://api.ex.com/{id_}", source="test")
    assert _run(fetcher(0)) is None
