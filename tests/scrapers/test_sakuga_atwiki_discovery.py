"""Unit tests for sakuga atwiki page discovery (no network calls)."""
from __future__ import annotations

from pathlib import Path

from src.scrapers.parsers.sakuga_atwiki import classify_page_kind, extract_page_ids
from src.scrapers.parsers.sakuga_atwiki_robots import is_allowed

_FIXTURES = Path(__file__).parent.parent / "fixtures" / "scrapers" / "sakuga"


def _html(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# classify_page_kind — 5 kinds × 2 fixtures
# ---------------------------------------------------------------------------

class TestClassifyPageKind:
    def test_meta_menu(self):
        assert classify_page_kind(
            "メニュー - 作画@wiki - atwiki（アットウィキ）", _html("meta.html")
        ) == "meta"

    def test_meta_sitemap(self):
        assert classify_page_kind(
            "サイトマップ - 作画@wiki - atwiki（アットウィキ）", _html("meta2.html")
        ) == "meta"

    def test_index_list_title(self):
        assert classify_page_kind(
            "アニメーター一覧 - 作画@wiki", _html("index.html")
        ) == "index"

    def test_index_50on_heading(self):
        assert classify_page_kind(
            "さ行 - 作画@wiki", _html("index2.html")
        ) == "index"

    def test_person_filmography(self):
        assert classify_page_kind(
            "下谷智之 - 作画@wiki - atwiki（アットウィキ）", _html("person.html")
        ) == "person"

    def test_person_credits(self):
        assert classify_page_kind(
            "山田太郎 - 作画@wiki", _html("person2.html")
        ) == "person"

    def test_work_staff(self):
        assert classify_page_kind(
            "ある作品 - 作画@wiki", _html("work.html")
        ) == "work"

    def test_work_episodes(self):
        assert classify_page_kind(
            "別の作品 - 作画@wiki", _html("work2.html")
        ) == "work"

    def test_unknown_no_markers(self):
        assert classify_page_kind(
            "テストページ - 作画@wiki", _html("unknown.html")
        ) == "unknown"

    def test_unknown_inline(self):
        assert classify_page_kind(
            "編集中 - 作画@wiki", _html("unknown2.html")
        ) == "unknown"


# ---------------------------------------------------------------------------
# is_allowed — robots.txt patterns
# ---------------------------------------------------------------------------

class TestIsAllowed:
    _PATTERNS = ["/*/search", "/*/backup", "/*/edit*"]

    def test_pages_path_allowed(self):
        assert is_allowed("/sakuga/pages/123.html", self._PATTERNS) is True

    def test_search_disallowed(self):
        assert is_allowed("/sakuga/search", self._PATTERNS) is False

    def test_edit_wildcard_disallowed(self):
        assert is_allowed("/sakuga/editx/123", self._PATTERNS) is False

    def test_backup_disallowed(self):
        assert is_allowed("/sakuga/backup", self._PATTERNS) is False

    def test_empty_patterns_all_allowed(self):
        assert is_allowed("/sakuga/pages/1.html", []) is True


# ---------------------------------------------------------------------------
# extract_page_ids
# ---------------------------------------------------------------------------

class TestExtractPageIds:
    def test_relative_hrefs(self):
        html = '<a href="/sakuga/pages/42.html">x</a><a href="/sakuga/pages/100.html">y</a>'
        assert extract_page_ids(html) == [42, 100]

    def test_absolute_hrefs(self):
        html = '<a href="https://www18.atwiki.jp/sakuga/pages/7.html">z</a>'
        assert extract_page_ids(html) == [7]

    def test_deduplicates(self):
        html = '<a href="/sakuga/pages/5.html">a</a><a href="/sakuga/pages/5.html">b</a>'
        assert extract_page_ids(html) == [5]

    def test_no_links(self):
        assert extract_page_ids("<html><body><p>nothing</p></body></html>") == []

    def test_preserves_order(self):
        html = (
            '<a href="/sakuga/pages/10.html">a</a>'
            '<a href="/sakuga/pages/3.html">b</a>'
            '<a href="/sakuga/pages/7.html">c</a>'
        )
        assert extract_page_ids(html) == [10, 3, 7]
