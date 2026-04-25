"""Unit tests for sakuga atwiki person parser."""
from __future__ import annotations

import math
from pathlib import Path
from unittest.mock import patch

import pytest

from src.runtime.models import ParsedSakugaCredit
from src.scrapers.parsers.sakuga_atwiki import (
    _extract_format,
    _extract_year,
    _parse_episode,
    parse_person_page,
)

_PERSONS_DIR = Path(__file__).parent.parent / "fixtures" / "scrapers" / "sakuga" / "persons"


def _html(name: str) -> str:
    return (_PERSONS_DIR / name).read_text(encoding="utf-8")


def _expected(name: str) -> int:
    stem = name.replace(".html", "")
    return int((_PERSONS_DIR / f"{stem}.expected.txt").read_text().strip())


# ---------------------------------------------------------------------------
# Fixture parse: 30 persons — credit count within ±10% of expected
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "fixture",
    [f"person_{i:02d}.html" for i in range(1, 31)],
)
def test_fixture_credit_count(fixture: str) -> None:
    html = _html(fixture)
    expected = _expected(fixture)
    result = parse_person_page(html, page_id=int(fixture[7:9]))
    actual = len(result.credits)
    # Allow ±1 or ±10%, whichever is larger
    tolerance = max(1, math.ceil(expected * 0.1))
    assert abs(actual - expected) <= tolerance, (
        f"{fixture}: expected ~{expected} credits, got {actual}"
    )


# ---------------------------------------------------------------------------
# Name extraction
# ---------------------------------------------------------------------------

class TestNameExtraction:
    def test_basic_name(self) -> None:
        html = _html("person_01.html")
        result = parse_person_page(html, page_id=1)
        assert result.name == "山田花子"

    def test_page_id_preserved(self) -> None:
        html = _html("person_01.html")
        result = parse_person_page(html, page_id=42)
        assert result.page_id == 42


# ---------------------------------------------------------------------------
# Alias extraction
# ---------------------------------------------------------------------------

class TestAliasExtraction:
    def test_betsumei_alias(self) -> None:
        html = _html("person_26.html")  # has 別名: Roku Goto
        result = parse_person_page(html)
        assert "Roku Goto" in result.aliases

    def test_kyuumei_alias(self) -> None:
        html = _html("person_27.html")  # has 旧名: 二七 藤本
        result = parse_person_page(html)
        assert any("藤本" in a for a in result.aliases)

    def test_eiji_alias(self) -> None:
        html = _html("person_30.html")  # has 英字: Sanjuu Hashimoto
        result = parse_person_page(html)
        assert "Sanjuu Hashimoto" in result.aliases


# ---------------------------------------------------------------------------
# active_since_year
# ---------------------------------------------------------------------------

class TestActiveSinceYear:
    def test_min_year_taken(self) -> None:
        html = _html("person_10.html")  # works 2013, 2015, 2018
        result = parse_person_page(html)
        assert result.active_since_year == 2013

    def test_no_credits_no_year(self) -> None:
        html = "<html><body><div id='wikibody'><h2>フィルモグラフィ</h2></div></body></html>"
        result = parse_person_page(html)
        assert result.active_since_year is None


# ---------------------------------------------------------------------------
# source_html_sha256
# ---------------------------------------------------------------------------

def test_sha256_deterministic() -> None:
    html = _html("person_01.html")
    r1 = parse_person_page(html)
    r2 = parse_person_page(html)
    assert r1.source_html_sha256 == r2.source_html_sha256
    assert len(r1.source_html_sha256) == 64


# ---------------------------------------------------------------------------
# Credit field correctness
# ---------------------------------------------------------------------------

class TestCreditFields:
    def test_work_format_tv(self) -> None:
        html = _html("person_01.html")
        result = parse_person_page(html)
        tv_credits = [c for c in result.credits if c.work_format == "TV"]
        assert len(tv_credits) > 0

    def test_work_format_theater(self) -> None:
        html = _html("person_02.html")  # has 劇場版
        result = parse_person_page(html)
        theater = [c for c in result.credits if c.work_format == "劇場"]
        assert len(theater) > 0

    def test_episode_num_extracted(self) -> None:
        html = _html("person_01.html")
        result = parse_person_page(html)
        nums = [c.episode_num for c in result.credits if c.episode_num is not None]
        assert len(nums) > 0

    def test_role_raw_preserved(self) -> None:
        html = _html("person_01.html")
        result = parse_person_page(html)
        roles = {c.role_raw for c in result.credits}
        assert "原画" in roles or "作画監督" in roles


# ---------------------------------------------------------------------------
# No subjective words in output
# ---------------------------------------------------------------------------

def test_no_subjective_words_in_credits() -> None:
    for fixture in [f"person_{i:02d}.html" for i in range(1, 31)]:
        result = parse_person_page(_html(fixture))
        for c in result.credits:
            for field in (c.work_title, c.role_raw):
                assert "神作画" not in field
                assert "作画崩壊" not in field


# ---------------------------------------------------------------------------
# LLM fallback invocation
# ---------------------------------------------------------------------------

def test_llm_fallback_called_when_regex_empty() -> None:
    # Page with content but no parseable markers
    # "テキスト内容 " = 7 chars; need wikibody_text >= 500 chars → use * 80
    html = """<html><body><div id='wikibody'>
    <p>フィルモグラフィ</p>
    <p>""" + "テキスト内容 " * 80 + """</p>
    </div></body></html>"""

    mock_result = [
        ParsedSakugaCredit(
            work_title="モック作品",
            work_year=2020,
            work_format="TV",
            role_raw="原画",
            episode_raw="第3話",
            episode_num=3,
        )
    ]

    with patch(
        "src.scrapers.parsers.sakuga_atwiki._llm_fallback",
        return_value=mock_result,
    ) as mock_llm:
        result = parse_person_page(html)
        mock_llm.assert_called_once()
        assert len(result.credits) == 1
        assert result.credits[0].work_title == "モック作品"


def test_llm_fallback_not_called_when_credits_found() -> None:
    html = _html("person_01.html")
    with patch(
        "src.scrapers.parsers.sakuga_atwiki._llm_fallback",
        return_value=[],
    ) as mock_llm:
        parse_person_page(html)
        mock_llm.assert_not_called()


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_extract_year(self) -> None:
        assert _extract_year("作品A (2020) TV") == 2020
        assert _extract_year("no year here") is None

    def test_extract_format_tv(self) -> None:
        assert _extract_format("作品 TV") == "TV"

    def test_extract_format_theater(self) -> None:
        assert _extract_format("作品 劇場版") == "劇場"

    def test_extract_format_ova(self) -> None:
        assert _extract_format("作品 OVA") == "OVA"

    def test_extract_format_none(self) -> None:
        assert _extract_format("タイトルのみ") is None

    def test_parse_episode_single(self) -> None:
        raw, num = _parse_episode("第3話 原画")
        assert num == 3

    def test_parse_episode_hash(self) -> None:
        _, num = _parse_episode("#7 作画監督")
        assert num == 7

    def test_parse_episode_range_first(self) -> None:
        _, num = _parse_episode("第1話〜第3話 原画")
        assert num == 1

    def test_parse_episode_op(self) -> None:
        raw, num = _parse_episode("OP 原画")
        assert raw == "OP"
        assert num is None

    def test_parse_episode_none(self) -> None:
        raw, num = _parse_episode("原画のみ")
        assert raw is None
        assert num is None
