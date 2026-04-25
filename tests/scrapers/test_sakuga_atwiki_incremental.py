"""Unit tests for sakuga atwiki incremental update logic."""
from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from src.scrapers.sakuga_atwiki_scraper import (
    PageRecord,
    _html_hash,
    _incremental,
    _load_discovered,
    _save_discovered,
    _write_html_cache,
)

# ---------------------------------------------------------------------------
# HTML templates
# ---------------------------------------------------------------------------

_PERSON_HTML = """\
<!DOCTYPE html><html lang="ja"><head><title>{name} - 作画@wiki - atwiki（アットウィキ）</title></head>
<body><div id="wikibody">
<h2>フィルモグラフィ</h2>
<h3>作品A (2020) TV</h3>
<ul><li>第3話 原画</li><li>第7話 作画監督</li></ul>
</div></body></html>"""

_CHANGED_HTML = """\
<!DOCTYPE html><html lang="ja"><head><title>{name} - 作画@wiki - atwiki（アットウィキ）</title></head>
<body><div id="wikibody">
<h2>フィルモグラフィ</h2>
<h3>作品A (2020) TV</h3>
<ul><li>第3話 原画</li><li>第7話 作画監督</li></ul>
<h3>作品B (2022) TV</h3>
<ul><li>第5話 原画</li></ul>
</div></body></html>"""

_NEW_PAGE_HTML = """\
<!DOCTYPE html><html lang="ja"><head><title>新人アニメーター - 作画@wiki - atwiki（アットウィキ）</title></head>
<body><div id="wikibody">
<h2>フィルモグラフィ</h2>
<h3>新作品 (2025) TV</h3>
<ul><li>第1話 原画</li></ul>
</div></body></html>"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(pid: int, name: str, html: str) -> PageRecord:
    return {
        "id": pid,
        "url": f"https://www18.atwiki.jp/sakuga/pages/{pid}.html",
        "title": name,
        "page_kind": "person",
        "discovered_at": "2026-04-01T00:00:00+00:00",
        "last_hash": _html_hash(html),
    }


def _mock_fetcher(side_effects: list[str]) -> AsyncMock:
    m = AsyncMock()
    m.fetch = AsyncMock(side_effect=side_effects)
    m.__aenter__ = AsyncMock(return_value=m)
    m.__aexit__ = AsyncMock(return_value=False)
    return m


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture()
def cache_dir(tmp_path: Path) -> Path:
    return tmp_path / "sakuga"


@pytest.fixture()
def setup(cache_dir: Path):
    cache_dir.mkdir()
    html_101 = _PERSON_HTML.format(name="山田花子")
    html_102 = _PERSON_HTML.format(name="佐藤次郎")
    records = {
        101: _make_record(101, "山田花子", html_101),
        102: _make_record(102, "佐藤次郎", html_102),
    }
    _save_discovered(cache_dir / "discovered_pages.json", records)
    _write_html_cache(cache_dir, 101, html_101)
    _write_html_cache(cache_dir, 102, html_102)
    return records, html_101, html_102


# ---------------------------------------------------------------------------
# Hash helpers
# ---------------------------------------------------------------------------

def test_html_hash_deterministic():
    html = "<html><body>test</body></html>"
    assert _html_hash(html) == _html_hash(html)
    assert len(_html_hash(html)) == 64


def test_html_hash_differs():
    assert _html_hash("v1") != _html_hash("v2")


# ---------------------------------------------------------------------------
# Unchanged pages are skipped
# ---------------------------------------------------------------------------

def test_unchanged_pages_skipped(cache_dir, setup, tmp_path):
    records, html_101, html_102 = setup
    fetcher = _mock_fetcher([html_101, html_102])

    with patch("src.scrapers.sakuga_atwiki_scraper.PlaywrightFetcher", return_value=fetcher), \
         patch("src.scrapers.sakuga_atwiki_scraper.fetch_disallow_patterns", return_value=[]):
        stats = _run(_incremental(
            cache_dir=cache_dir,
            output=tmp_path / "bronze",
            date="20260425",
            delay=0.0,
            max_pages=0,
            headless=True,
        ))

    assert stats["fetched"] == 2
    assert stats["unchanged"] == 2
    assert stats["changed"] == 0


# ---------------------------------------------------------------------------
# Changed page triggers parse + write
# ---------------------------------------------------------------------------

def test_changed_page_triggers_write(cache_dir, setup, tmp_path):
    records, html_101, html_102 = setup
    changed_html = _CHANGED_HTML.format(name="山田花子")
    fetcher = _mock_fetcher([changed_html, html_102])

    with patch("src.scrapers.sakuga_atwiki_scraper.PlaywrightFetcher", return_value=fetcher), \
         patch("src.scrapers.sakuga_atwiki_scraper.fetch_disallow_patterns", return_value=[]):
        stats = _run(_incremental(
            cache_dir=cache_dir,
            output=tmp_path / "bronze",
            date="20260425",
            delay=0.0,
            max_pages=0,
            headless=True,
        ))

    assert stats["changed"] == 1
    assert stats["unchanged"] == 1
    credits_dir = tmp_path / "bronze" / "source=sakuga_atwiki" / "table=credits" / "date=20260425"
    assert len(list(credits_dir.glob("*.parquet"))) > 0


# ---------------------------------------------------------------------------
# discovered_pages.json updated with new hash
# ---------------------------------------------------------------------------

def test_hash_updated(cache_dir, setup, tmp_path):
    records, html_101, html_102 = setup
    changed_html = _CHANGED_HTML.format(name="山田花子")
    old_hash = records[101]["last_hash"]
    fetcher = _mock_fetcher([changed_html, html_102])

    with patch("src.scrapers.sakuga_atwiki_scraper.PlaywrightFetcher", return_value=fetcher), \
         patch("src.scrapers.sakuga_atwiki_scraper.fetch_disallow_patterns", return_value=[]):
        _run(_incremental(
            cache_dir=cache_dir,
            output=tmp_path / "bronze",
            date="20260425",
            delay=0.0,
            max_pages=0,
            headless=True,
        ))

    updated = _load_discovered(cache_dir / "discovered_pages.json")
    assert updated[101]["last_hash"] != old_hash
    assert updated[101]["last_hash"] == _html_hash(changed_html)
    assert updated[102]["last_hash"] == records[102]["last_hash"]


# ---------------------------------------------------------------------------
# New page discovered + added to discovered_pages.json
# ---------------------------------------------------------------------------

def test_new_page_discovered(cache_dir, setup, tmp_path):
    records, html_101, html_102 = setup
    changed_with_link = _CHANGED_HTML.format(name="山田花子").replace(
        "</body>",
        '<a href="/sakuga/pages/999.html">新ページ</a></body>',
    )
    fetcher = _mock_fetcher([changed_with_link, html_102, _NEW_PAGE_HTML])

    with patch("src.scrapers.sakuga_atwiki_scraper.PlaywrightFetcher", return_value=fetcher), \
         patch("src.scrapers.sakuga_atwiki_scraper.fetch_disallow_patterns", return_value=[]):
        stats = _run(_incremental(
            cache_dir=cache_dir,
            output=tmp_path / "bronze",
            date="20260425",
            delay=0.0,
            max_pages=100,
            headless=True,
        ))

    assert stats["new_pages"] == 1
    updated = _load_discovered(cache_dir / "discovered_pages.json")
    assert 999 in updated


# ---------------------------------------------------------------------------
# Existing parquet not overwritten (mtime unchanged)
# ---------------------------------------------------------------------------

def test_existing_parquet_not_overwritten(cache_dir, setup, tmp_path):
    old_dir = tmp_path / "bronze" / "source=sakuga_atwiki" / "table=credits" / "date=20260101"
    old_dir.mkdir(parents=True)
    old_parquet = old_dir / "old.parquet"
    old_parquet.write_bytes(b"fake")
    original_mtime = old_parquet.stat().st_mtime

    records, html_101, html_102 = setup
    changed_html = _CHANGED_HTML.format(name="山田花子")
    fetcher = _mock_fetcher([changed_html, html_102])

    with patch("src.scrapers.sakuga_atwiki_scraper.PlaywrightFetcher", return_value=fetcher), \
         patch("src.scrapers.sakuga_atwiki_scraper.fetch_disallow_patterns", return_value=[]):
        _run(_incremental(
            cache_dir=cache_dir,
            output=tmp_path / "bronze",
            date="20260425",
            delay=0.0,
            max_pages=0,
            headless=True,
        ))

    assert old_parquet.stat().st_mtime == original_mtime


# ---------------------------------------------------------------------------
# robots.txt disallow respected
# ---------------------------------------------------------------------------

def test_disallow_respected(cache_dir, setup, tmp_path):
    records, html_101, html_102 = setup
    fetcher = _mock_fetcher([html_101])

    with patch("src.scrapers.sakuga_atwiki_scraper.PlaywrightFetcher", return_value=fetcher), \
         patch("src.scrapers.sakuga_atwiki_scraper.fetch_disallow_patterns",
               return_value=["/sakuga/pages/"]):
        stats = _run(_incremental(
            cache_dir=cache_dir,
            output=tmp_path / "bronze",
            date="20260425",
            delay=0.0,
            max_pages=0,
            headless=True,
        ))

    assert stats["fetched"] == 0
    fetcher.fetch.assert_not_called()
