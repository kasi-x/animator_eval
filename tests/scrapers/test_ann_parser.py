"""ANN parser unit tests.

Driven by real ANN XML/HTML fixtures — no network required.

Fixture capture commands (refresh when needed):
    curl -sL 'https://cdn.animenewsnetwork.com/encyclopedia/api.xml?anime=1/5/100/4658' \
        > tests/fixtures/scrapers/ann/anime_batch.xml
    curl -sL 'https://cdn.animenewsnetwork.com/encyclopedia/api.xml?people=260/261/431' \
        > tests/fixtures/scrapers/ann/persons_batch.xml
    curl -sL 'https://cdn.animenewsnetwork.com/encyclopedia/reports.xml?tag=masterlist&nlist=all' \
        > tests/fixtures/scrapers/ann/masterlist_html_response.html
    curl -sL 'https://www.animenewsnetwork.com/encyclopedia/people.php?id=260' \
        > tests/fixtures/scrapers/ann/person_260.html
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from src.scrapers.parsers.ann import (
    AnimeXmlParseResult,
    _normalize_format,
    _parse_dob_html,
    _parse_theme,
    _parse_vintage,
    parse_anime_xml,
    parse_person_html,
    parse_person_xml,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "scrapers" / "ann"


def _load(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# parse_anime_xml — result type
# ---------------------------------------------------------------------------


@pytest.fixture
def anime_result() -> AnimeXmlParseResult:
    root = ET.fromstring(_load("anime_batch.xml"))
    return parse_anime_xml(root)


@pytest.fixture
def anime_by_id(anime_result: AnimeXmlParseResult) -> dict:
    return {a.ann_id: a for a in anime_result.anime}


def test_parse_anime_xml_returns_result_type(anime_result):
    assert isinstance(anime_result, AnimeXmlParseResult)


def test_parses_all_anime_in_batch(anime_result):
    assert len(anime_result.anime) == 4
    assert {a.ann_id for a in anime_result.anime} == {1, 5, 100, 4658}


# ---------------------------------------------------------------------------
# Basic anime fields
# ---------------------------------------------------------------------------


def test_anime_basic_fields(anime_by_id):
    a = anime_by_id[1]
    assert a.title_en == "Angel Links"
    assert a.title_ja == "星方天使エンジェルリンクス"
    assert a.year == 1999
    assert a.episodes == 13


def test_oav_format_recognized(anime_by_id):
    assert anime_by_id[5].format == "OVA"
    assert anime_by_id[100].format == "OVA"


def test_tv_format_recognized(anime_by_id):
    assert anime_by_id[1].format == "TV"
    assert anime_by_id[4658].format == "TV"


# ---------------------------------------------------------------------------
# New info fields
# ---------------------------------------------------------------------------


def test_themes_extracted(anime_by_id):
    assert "pirates" in anime_by_id[1].themes
    assert "space" in anime_by_id[1].themes


def test_plot_summary_extracted(anime_by_id):
    summary = anime_by_id[1].plot_summary
    assert summary is not None
    assert len(summary) > 50


def test_opening_theme_parsed(anime_by_id):
    themes = json.loads(anime_by_id[1].opening_themes_json)
    assert len(themes) >= 1
    assert themes[0]["title"] == "All My Soul"
    assert themes[0]["artist"] == "Naw Naw"


def test_ending_theme_parsed(anime_by_id):
    themes = json.loads(anime_by_id[1].ending_themes_json)
    assert len(themes) >= 1
    assert themes[0]["title"] == "True Moon"


def test_image_url_extracted(anime_by_id):
    assert anime_by_id[1].image_url is not None
    assert "animenewsnetwork.com" in anime_by_id[1].image_url


def test_vintage_raw_extracted(anime_by_id):
    assert anime_by_id[1].vintage_raw is not None


def test_titles_alt_is_dict_json(anime_by_id):
    titles_alt = json.loads(anime_by_id[1].titles_alt)
    assert isinstance(titles_alt, dict)
    assert "JA" in titles_alt
    ja_titles = titles_alt["JA"]
    assert any("星方天使エンジェルリンクス" in t for t in ja_titles)


# ---------------------------------------------------------------------------
# Ratings (display only — Hard Rule)
# ---------------------------------------------------------------------------


def test_display_rating_votes(anime_by_id):
    assert anime_by_id[1].display_rating_votes == 352


def test_display_rating_weighted(anime_by_id):
    w = anime_by_id[1].display_rating_weighted
    assert w is not None
    assert 0 < w < 10


def test_anime_without_rating_has_none():
    xml_no_rating = "<ann><anime id=\"9999\" type=\"TV\" name=\"X\"></anime></ann>"
    result = parse_anime_xml(ET.fromstring(xml_no_rating))
    assert result.anime[0].display_rating_votes is None
    assert result.anime[0].display_rating_weighted is None


# ---------------------------------------------------------------------------
# Staff
# ---------------------------------------------------------------------------


def test_staff_extracted(anime_by_id):
    staff = anime_by_id[1].staff
    assert len(staff) > 10
    assert any(s.ann_person_id == 260 and "Director" in s.task for s in staff)


def test_staff_task_raw_equals_task(anime_by_id):
    for s in anime_by_id[1].staff:
        assert s.task_raw == s.task


def test_staff_gid_populated(anime_by_id):
    staff = anime_by_id[1].staff
    assert any(s.gid is not None for s in staff)


def test_staff_no_duplicate_person_role(anime_by_id):
    for rec in anime_by_id.values():
        seen: set = set()
        dups = []
        for s in rec.staff:
            key = (s.ann_person_id, s.task)
            if key in seen:
                dups.append(key)
            seen.add(key)
        assert not dups, f"anime {rec.ann_id} duplicate (pid, task): {dups[:3]}"


# ---------------------------------------------------------------------------
# Cast table
# ---------------------------------------------------------------------------


def test_cast_extracted(anime_result):
    cast_for_1 = [c for c in anime_result.cast if c.ann_anime_id == 1]
    assert len(cast_for_1) > 5


def test_cast_fields(anime_result):
    c = next(c for c in anime_result.cast if c.ann_anime_id == 1)
    assert c.ann_person_id > 0
    assert c.voice_actor_name
    assert c.character_name
    assert c.cast_role in ("EN", "JA", "")


# ---------------------------------------------------------------------------
# Company table (from <credit> elements)
# ---------------------------------------------------------------------------


def test_company_extracted(anime_result):
    co = [c for c in anime_result.company if c.ann_anime_id == 1]
    assert len(co) >= 1


def test_company_has_animation_production(anime_result):
    co = [c for c in anime_result.company if c.ann_anime_id == 1]
    tasks = {c.task for c in co}
    assert "Animation Production" in tasks


# ---------------------------------------------------------------------------
# Episodes table
# ---------------------------------------------------------------------------


def test_episodes_extracted(anime_result):
    eps = [e for e in anime_result.episodes if e.ann_anime_id == 1]
    assert len(eps) >= 13


def test_episode_fields(anime_result):
    ep = next(e for e in anime_result.episodes if e.ann_anime_id == 1)
    assert ep.episode_num
    assert ep.lang
    assert ep.title


# ---------------------------------------------------------------------------
# Releases table
# ---------------------------------------------------------------------------


def test_releases_extracted(anime_result):
    rels = [r for r in anime_result.releases if r.ann_anime_id == 1]
    assert len(rels) >= 1


def test_release_has_date(anime_result):
    rels = [r for r in anime_result.releases if r.ann_anime_id == 1]
    assert any(r.release_date is not None for r in rels)


# ---------------------------------------------------------------------------
# News table
# ---------------------------------------------------------------------------


def test_news_extracted(anime_result):
    news = [n for n in anime_result.news if n.ann_anime_id == 1]
    assert len(news) >= 1


def test_news_fields(anime_result):
    n = next(x for x in anime_result.news if x.ann_anime_id == 1)
    assert n.datetime
    assert n.title


# ---------------------------------------------------------------------------
# Related table
# ---------------------------------------------------------------------------


def test_related_extracted(anime_result):
    rel = [r for r in anime_result.related if r.ann_anime_id == 1]
    assert len(rel) >= 1


def test_related_fields(anime_result):
    rel = next(r for r in anime_result.related if r.ann_anime_id == 1)
    assert rel.target_ann_id > 0
    assert rel.rel
    assert rel.direction in ("prev", "next")


# ---------------------------------------------------------------------------
# _ANN_TYPE_MAP coverage
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ann_type,expected", [
    ("TV", "TV"),
    ("tv", "TV"),
    ("movie", "MOVIE"),
    ("Movie", "MOVIE"),
    ("OVA", "OVA"),
    ("OAV", "OVA"),
    ("oav", "OVA"),
    ("ONA", "ONA"),
    ("special", "SPECIAL"),
    ("TV Special", "SPECIAL"),
    ("Web", "ONA"),
    (" TV ", "TV"),
    ("Music Video", "MUSIC_VIDEO"),
    ("Unknown", None),
    ("", None),
])
def test_ann_type_map(ann_type, expected):
    assert _normalize_format(ann_type) == expected


# ---------------------------------------------------------------------------
# _parse_vintage
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
# _parse_theme
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("text,title,artist", [
    ('"All My Soul" by Naw Naw', "All My Soul", "Naw Naw"),
    ('"Title"', "Title", ""),
    ("Title by Artist", "Title", "Artist"),
    ("Plain title", "Plain title", ""),
])
def test_parse_theme(text, title, artist):
    result = _parse_theme(text)
    assert result["title"] == title
    assert result["artist"] == artist


# ---------------------------------------------------------------------------
# parse_person_xml — broken ANN endpoint
# ---------------------------------------------------------------------------


def test_persons_xml_endpoint_returns_warning():
    root = ET.fromstring(_load("persons_batch.xml"))
    persons = parse_person_xml(root)
    assert persons == []


# ---------------------------------------------------------------------------
# Masterlist endpoint — HTML response
# ---------------------------------------------------------------------------


def test_masterlist_returns_html():
    text = _load("masterlist_html_response.html")
    stripped = text.lstrip()
    assert stripped.startswith("<!DOCTYPE") or stripped.startswith("<html")


# ---------------------------------------------------------------------------
# _parse_dob_html
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("text,expected", [
    ("1941-01-05", "1941-01-05"),
    ("Jan 5, 1941", "1941-01-05"),
    ("January 5, 1941", "1941-01-05"),
    ("1941", "1941"),
    ("born 1941 somewhere", "1941"),
    ("", None),
    ("unknown", None),
])
def test_parse_dob_html(text, expected):
    assert _parse_dob_html(text) == expected


# ---------------------------------------------------------------------------
# parse_person_html — real fixture: person_260.html
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


def test_person_html_family_name_ja(person_260):
    assert person_260.family_name_ja == "山口"


def test_person_html_given_name_ja(person_260):
    assert person_260.given_name_ja == "祐司"


def test_person_html_no_birth_date(person_260):
    assert person_260.date_of_birth is None


def test_person_html_alt_names(person_260):
    alt = json.loads(person_260.alt_names_json)
    assert isinstance(alt, list)
    assert any(entry["name"] == "Isao Torada" for entry in alt)


def test_person_html_credits_list(person_260):
    credits = json.loads(person_260.credits_json)
    assert isinstance(credits, list)
    assert len(credits) > 10
    ann1_credit = next((c for c in credits if c["ann_anime_id"] == 1), None)
    assert ann1_credit is not None
    assert "Director" in ann1_credit["task"]


def test_person_html_cloudflare_block():
    cf_html = (
        "<html><head><title>Just a moment...</title></head>"
        "<body>Please wait while we verify your browser.</body></html>"
    )
    assert parse_person_html(cf_html, ann_id=999) is None


def test_person_html_minimal():
    html = """<html><head><title>Test Person - Anime News Network</title></head>
    <body><div id="page-title"><h1 id="page_header">Test Person</h1></div></body></html>"""
    result = parse_person_html(html, ann_id=1)
    assert result is not None
    assert result.name_en == "Test Person"
    assert result.date_of_birth is None
    assert json.loads(result.credits_json) == []
    assert json.loads(result.alt_names_json) == []


def test_person_html_with_birth_date():
    html = """<html><head><title>Born Person - Anime News Network</title></head>
    <body>
      <div id="page-title"><h1 id="page_header">Born Person</h1>宮崎 駿</div>
      <div id="infotype-5"><strong>Birthdate:</strong> <span>1941-01-05</span></div>
      <div id="infotype-7"><strong>Hometown:</strong> <span>Tokyo</span></div>
      <div id="infotype-8"><strong>Blood type:</strong> <span>A</span></div>
    </body></html>"""
    result = parse_person_html(html, ann_id=2)
    assert result is not None
    assert result.date_of_birth == "1941-01-05"
    assert result.hometown == "Tokyo"
    assert result.blood_type == "A"


def test_person_html_description_no_truncation():
    long_desc = "A" * 3000
    html = f"""<html><head><title>X - Anime News Network</title></head>
    <body>
      <div id="page-title"><h1 id="page_header">X</h1></div>
      <div id="infotype-9"><strong>Biography:</strong> <span>{long_desc}</span></div>
    </body></html>"""
    result = parse_person_html(html, ann_id=3)
    assert result is not None
    assert result.description_raw == long_desc
    assert result.description == long_desc[:2000]
