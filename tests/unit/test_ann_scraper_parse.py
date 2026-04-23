"""ANN scraper parser unit tests.

Driven by real ANN XML/HTML responses captured to tests/fixtures/scrapers/ann/.
No network required — these tests verify parse functions only.

Capture commands (for refresh):
    curl -sSL 'https://www.animenewsnetwork.com/encyclopedia/api.xml?anime=1/5/100/4658' \\
        > tests/fixtures/scrapers/ann/anime_batch.xml
    curl -sSL 'https://www.animenewsnetwork.com/encyclopedia/api.xml?people=260/261/431' \\
        > tests/fixtures/scrapers/ann/persons_batch.xml
    curl -sSL 'https://cdn.animenewsnetwork.com/encyclopedia/reports.xml?tag=masterlist&nlist=all' \\
        > tests/fixtures/scrapers/ann/masterlist_html_response.html
    curl -sSL 'https://www.animenewsnetwork.com/encyclopedia/people.php?id=260' \\
        > tests/fixtures/scrapers/ann/person_260.html
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from src.scrapers.ann_scraper import (
    _normalize_format,
    _parse_dob_html,
    _parse_vintage,
    parse_anime_xml,
    parse_person_html,
    parse_person_xml,
)

FIXTURES = Path(__file__).parent / "fixtures" / "scrapers" / "ann"


def _load(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# parse_anime_xml — real XML batch
# ---------------------------------------------------------------------------


@pytest.fixture
def anime_records():
    root = ET.fromstring(_load("anime_batch.xml"))
    return parse_anime_xml(root)


def test_parses_all_anime_in_batch(anime_records):
    assert len(anime_records) == 4
    assert {r.ann_id for r in anime_records} == {1, 5, 100, 4658}


def test_anime_basic_fields(anime_records):
    by_id = {r.ann_id: r for r in anime_records}
    assert by_id[1].title_en == "Angel Links"
    assert by_id[1].title_ja == "星方天使エンジェルリンクス"
    assert by_id[1].year == 1999
    assert by_id[1].episodes == 13


def test_oav_format_recognized(anime_records):
    """ANN returns type='OAV' (not 'OVA'). _ANN_TYPE_MAP must handle this."""
    by_id = {r.ann_id: r for r in anime_records}
    # ann_id=5 (Battle Skipper) and 100 (Adventures of Kotetsu) are OAV in real XML
    assert by_id[5].format == "OVA", (
        f"OAV type not mapped to OVA — got {by_id[5].format!r}. "
        "_ANN_TYPE_MAP needs 'OAV': 'OVA'"
    )
    assert by_id[100].format == "OVA"


def test_tv_format_recognized(anime_records):
    by_id = {r.ann_id: r for r in anime_records}
    assert by_id[1].format == "TV"
    assert by_id[4658].format == "TV"


def test_staff_extracted(anime_records):
    by_id = {r.ann_id: r for r in anime_records}
    assert len(by_id[1].staff) > 10
    assert any(s.ann_person_id == 260 and "Director" in s.task for s in by_id[1].staff)


def test_staff_no_duplicate_person_role(anime_records):
    """A (person, role) pair should appear at most once per anime."""
    for rec in anime_records:
        seen = set()
        dups = []
        for s in rec.staff:
            key = (s.ann_person_id, s.task)
            if key in seen:
                dups.append(key)
            seen.add(key)
        assert not dups, f"anime {rec.ann_id} has duplicate (pid, task): {dups[:3]}"


# ---------------------------------------------------------------------------
# _ANN_TYPE_MAP — known-good types coverage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ann_type,expected", [
    ("TV", "TV"),
    ("tv", "TV"),               # case-insensitive
    ("movie", "MOVIE"),
    ("Movie", "MOVIE"),
    ("OVA", "OVA"),
    ("OAV", "OVA"),             # ANN actually returns "OAV" not "OVA"
    ("oav", "OVA"),
    ("ONA", "ONA"),
    ("special", "SPECIAL"),
    ("Special", "SPECIAL"),
    ("TV Special", "SPECIAL"),
    ("tv special", "SPECIAL"),
    ("Web", "ONA"),
    (" TV ", "TV"),             # whitespace tolerance
    ("Music Video", "MUSIC_VIDEO"),
    ("Unknown", None),          # unmapped → None
    ("", None),
])
def test_ann_type_map_covers_known(ann_type, expected):
    assert _normalize_format(ann_type) == expected, (
        f"_normalize_format({ann_type!r}) → expected {expected!r}"
    )


# ---------------------------------------------------------------------------
# _parse_vintage — date string parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("vintage,expected", [
    ("Apr 3, 1998 to Apr 24, 1999", (1998, "1998-04-03", "1999-04-24")),
    ("Jan 5, 2001", (2001, "2001-01-05", None)),
    ("2001", (2001, None, None)),
    ("", (None, None, None)),
    ("Unknown", (None, None, None)),
])
def test_parse_vintage(vintage, expected):
    assert _parse_vintage(vintage) == expected


# ---------------------------------------------------------------------------
# parse_person_xml — current ANN API returns "ignored" for ?people=
# ---------------------------------------------------------------------------


def test_persons_endpoint_returns_warning():
    """Document the broken state: ANN's ?people=N endpoint returns
    <warning>ignored ...</warning> instead of person records.
    Until Phase 3 is rewritten to scrape HTML pages, parse_person_xml
    must return [] for these responses (no crash)."""
    root = ET.fromstring(_load("persons_batch.xml"))
    persons = parse_person_xml(root)
    assert persons == []


# ---------------------------------------------------------------------------
# Masterlist endpoint — currently returns HTML (broken)
# ---------------------------------------------------------------------------


def test_masterlist_returns_html_not_xml():
    """Document the broken state: ANN's CDN masterlist endpoint returns
    HTML even with the documented ?tag=masterlist&nlist=all params.
    fetch_masterlist must detect this and fall back to _probe_max_id."""
    text = _load("masterlist_html_response.html")
    stripped = text.lstrip()
    assert stripped.startswith("<!DOCTYPE") or stripped.startswith("<html")


# ---------------------------------------------------------------------------
# _parse_dob_html — date-of-birth format normalisation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("text,expected", [
    ("1941-01-05", "1941-01-05"),          # ISO: そのまま
    ("Jan 5, 1941", "1941-01-05"),          # 略月名
    ("January 5, 1941", "1941-01-05"),      # 完全月名
    ("1941", "1941"),                       # 年のみ
    ("born 1941 somewhere", "1941"),        # 年を含む文字列
    ("", None),                             # 空文字 → None
    ("unknown", None),                      # 数字なし → None
])
def test_parse_dob_html(text, expected):
    assert _parse_dob_html(text) == expected


# ---------------------------------------------------------------------------
# parse_person_html — real fixture: person_260.html (Yūji Yamaguchi)
# ---------------------------------------------------------------------------


@pytest.fixture
def person_260():
    return parse_person_html(_load("person_260.html"), ann_id=260)


def test_person_html_returns_detail(person_260):
    assert person_260 is not None


def test_person_html_ann_id(person_260):
    assert person_260.ann_id == 260


def test_person_html_english_name(person_260):
    assert "YAMAGUCHI" in person_260.name_en or "Yamaguchi" in person_260.name_en


def test_person_html_japanese_name(person_260):
    assert "山口" in person_260.name_ja


def test_person_html_no_birth_date(person_260):
    # Person 260 has no birthdate in the fixture — field should be None
    assert person_260.date_of_birth is None


def test_person_html_cloudflare_block_returns_none():
    """Cloudflare challenge page → None (not a crash)."""
    cf_html = (
        "<html><head><title>Just a moment...</title></head>"
        "<body>Please wait while we verify your browser.</body></html>"
    )
    result = parse_person_html(cf_html, ann_id=999)
    assert result is None


def test_person_html_minimal_fields():
    """Minimal valid page: only h1 present — returns detail with blank optional fields."""
    html = """
    <html><head><title>Test Person - Anime News Network</title></head>
    <body>
      <div id="page-title">
        <h1 id="page_header">Test Person</h1>
      </div>
    </body></html>
    """
    result = parse_person_html(html, ann_id=1)
    assert result is not None
    assert result.name_en == "Test Person"
    assert result.date_of_birth is None
    assert result.hometown is None


def test_person_html_with_birth_date():
    """infotype div containing birthdate is extracted correctly."""
    html = """
    <html><head><title>Born Person - Anime News Network</title></head>
    <body>
      <div id="page-title">
        <h1 id="page_header">Born Person</h1>
        宮崎 駿
      </div>
      <div id="infotype-5" class="encyc-info-type">
        <strong>Birthdate:</strong> <span>1941-01-05</span>
      </div>
      <div id="infotype-7" class="encyc-info-type">
        <strong>Hometown:</strong> <span>Tokyo</span>
      </div>
      <div id="infotype-8" class="encyc-info-type">
        <strong>Blood type:</strong> <span>A</span>
      </div>
    </body></html>
    """
    result = parse_person_html(html, ann_id=2)
    assert result is not None
    assert result.name_en == "Born Person"
    assert "宮崎" in result.name_ja
    assert result.date_of_birth == "1941-01-05"
    assert result.hometown == "Tokyo"
    assert result.blood_type == "A"


def test_person_html_dob_month_name():
    """Birth date in 'January 5, 1941' format is normalised to ISO."""
    html = """
    <html><head><title>X - Anime News Network</title></head>
    <body>
      <div id="page-title"><h1 id="page_header">X</h1></div>
      <div id="infotype-5" class="encyc-info-type">
        <strong>Birthdate:</strong> <span>January 5, 1941</span>
      </div>
    </body></html>
    """
    result = parse_person_html(html, ann_id=3)
    assert result is not None
    assert result.date_of_birth == "1941-01-05"
