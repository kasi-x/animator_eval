"""Unit tests for fetch_anime_ids_from_search (Scenario β).

All HTTP calls are replaced by mock transports — no real network access.
Uses asyncio.run() wrapper (project convention, cf. test_http_client.py).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from unittest.mock import AsyncMock, MagicMock, patch

from src.scrapers.allcinema_scraper import (
    AllcinemaClient,
    _fetch_anime_ids_for_word,
    _search_anime_one_page,
    fetch_anime_ids_from_search,
)


def _run(coro: Awaitable):
    return asyncio.run(coro)


# ─── helpers ────────────────────────────────────────────────────────────────


def _make_search_response(movies: list[int], page: int, maxpage: int) -> MagicMock:
    """Build a mock httpx.Response for /ajax/search."""
    movie_list = [
        {"movie": {"cinemaid": cid, "animationflag": "アニメ"}}
        for cid in movies
    ]
    body = {
        "total_count": len(movies),
        "search": {
            "searchmovies": {
                "movies": movie_list,
                "page": {
                    "page": page,
                    "pagelimit": 100,
                    "maxpage": maxpage,
                    "allcount": len(movies),
                    "startcount": (page - 1) * 100 + 1,
                    "endcount": page * 100,
                },
                "result": {},
            }
        },
    }
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = body
    resp.raise_for_status = MagicMock()
    return resp


def _make_over_limit_response() -> MagicMock:
    """Build a mock response when results > 1000 (empty movies)."""
    body = {
        "total_count": 5000,
        "search": {
            "searchmovies": {
                "movies": [],
                "page": {
                    "page": 1,
                    "pagelimit": 100,
                    "maxpage": 50,
                    "allcount": 5000,
                },
                "result": {"message": "検索結果数が 1000 件を超えました。"},
            }
        },
    }
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = body
    resp.raise_for_status = MagicMock()
    return resp


def _make_client() -> MagicMock:
    """Build a mock AllcinemaClient with CSRF token pre-set."""
    client = MagicMock(spec=AllcinemaClient)
    client._csrf_token = "test_token"
    client._client = MagicMock()
    client._client.post = AsyncMock()
    client.get = AsyncMock()
    return client


# ─── _search_anime_one_page ──────────────────────────────────────────────────


def test_search_one_page_returns_ids():
    """Normal case: 3 IDs on page 1 of 1."""
    client = _make_client()
    client._client.post = AsyncMock(
        return_value=_make_search_response([111, 222, 333], page=1, maxpage=1)
    )

    ids, maxpage = _run(_search_anime_one_page(client, "テスト", page=1))

    assert ids == [111, 222, 333]
    assert maxpage == 1


def test_search_one_page_over_limit_returns_empty():
    """Server returns empty movies when >1000 results."""
    client = _make_client()
    client._client.post = AsyncMock(return_value=_make_over_limit_response())

    ids, maxpage = _run(_search_anime_one_page(client, "あ", page=1))

    assert ids == []
    assert maxpage == 0


def test_search_one_page_http_error_returns_empty():
    """Network error returns empty list gracefully (no exception raised)."""
    client = _make_client()
    client._client.post = AsyncMock(side_effect=Exception("timeout"))

    ids, maxpage = _run(_search_anime_one_page(client, "テスト", page=1))

    assert ids == []
    assert maxpage == 0


# ─── _fetch_anime_ids_for_word ───────────────────────────────────────────────


def test_fetch_word_paginates():
    """Collects IDs across multiple pages (maxpage=2)."""
    client = _make_client()

    page1_resp = _make_search_response(list(range(100, 200)), page=1, maxpage=2)
    page2_resp = _make_search_response(list(range(200, 250)), page=2, maxpage=2)
    client._client.post = AsyncMock(side_effect=[page1_resp, page2_resp])

    collected: set[int] = set()
    _run(_fetch_anime_ids_for_word(client, "テスト", collected, depth=0))

    assert len(collected) == 150
    assert 100 in collected
    assert 249 in collected


def test_fetch_word_deduplicates():
    """IDs that appear in multiple runs are deduplicated via set."""
    client = _make_client()
    resp1 = _make_search_response([10, 20, 30], page=1, maxpage=1)
    client._client.post = AsyncMock(return_value=resp1)

    collected: set[int] = {10, 20}  # pre-existing entries
    _run(_fetch_anime_ids_for_word(client, "テスト", collected, depth=0))

    assert collected == {10, 20, 30}


def test_fetch_word_recursion_depth_limit():
    """Recursion stops at _MAX_PREFIX_DEPTH even when still over limit."""
    from src.scrapers.allcinema_scraper import _MAX_PREFIX_DEPTH

    client = _make_client()
    # All responses are over-limit (empty movies)
    client._client.post = AsyncMock(return_value=_make_over_limit_response())

    collected: set[int] = set()
    # Start at max depth → should not recurse further, just return
    _run(
        _fetch_anime_ids_for_word(
            client,
            "あ" * _MAX_PREFIX_DEPTH,
            collected,
            depth=_MAX_PREFIX_DEPTH,
        )
    )

    assert len(collected) == 0


# ─── fetch_anime_ids_from_search ────────────────────────────────────────────


def test_fetch_all_deduplicates_across_seeds():
    """IDs returned for multiple seeds are globally deduplicated."""
    client = _make_client()

    # Every seed returns the same 2 IDs — final list should have only 2
    resp = _make_search_response([9001, 9002], page=1, maxpage=1)
    client._client.post = AsyncMock(return_value=resp)

    with patch("src.scrapers.allcinema_scraper._SEARCH_SEEDS", ("あ", "い")):
        ids = _run(fetch_anime_ids_from_search(client))

    assert ids == [9001, 9002]
    assert ids == sorted(ids)


def test_fetch_all_returns_sorted_list():
    """Result list is sorted ascending regardless of discovery order."""
    client = _make_client()

    resp_a = _make_search_response([300, 100], page=1, maxpage=1)
    resp_b = _make_search_response([200, 100], page=1, maxpage=1)
    client._client.post = AsyncMock(side_effect=[resp_a, resp_b])

    with patch("src.scrapers.allcinema_scraper._SEARCH_SEEDS", ("あ", "い")):
        ids = _run(fetch_anime_ids_from_search(client))

    assert ids == sorted(ids)
    assert set(ids) == {100, 200, 300}
