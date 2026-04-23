"""allcinema parser unit tests using captured HTML fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.scrapers.allcinema_scraper import _parse_cinema_html, _parse_person_html

FIXTURES = Path(__file__).parent / "fixtures" / "scrapers" / "allcinema"


def _load(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def cinema_360181():
    """妖怪アパートの幽雅な日常 (2017) — TVアニメ"""
    return _parse_cinema_html(_load("cinema_360181.html"), 360181)


def test_cinema_parse_returns_anime_record(cinema_360181):
    assert cinema_360181 is not None
    assert cinema_360181.cinema_id == 360181


def test_cinema_title_extracted(cinema_360181):
    assert "妖怪アパート" in cinema_360181.title_ja


def test_cinema_year_extracted(cinema_360181):
    assert cinema_360181.year == 2017


def test_cinema_credits_extracted(cinema_360181):
    """CreditJson should yield staff entries."""
    assert len(cinema_360181.staff) > 0


def test_cinema_credits_have_required_fields(cinema_360181):
    for c in cinema_360181.staff[:5]:
        assert c.allcinema_person_id > 0
        assert c.job_name  # non-empty
        # name_ja or name_en should be present
        assert c.name_ja or c.name_en


def test_cinema_returns_none_for_non_anime():
    """Cinema pages where animeFlag != 'アニメ' return None."""
    fake_html = """
    <html><head></head><body>
    <script>
    var PageSetting = function(){this.animeFlag = "実写"};
    </script>
    </body></html>
    """
    assert _parse_cinema_html(fake_html, 999) is None


def test_cinema_returns_none_when_no_pagesetting():
    assert _parse_cinema_html("<html></html>", 1) is None


# ---------------------------------------------------------------------------
# Person page
# ---------------------------------------------------------------------------


def test_person_parse_extracts_name():
    rec = _parse_person_html(_load("person_300001.html"), 300001)
    assert rec.allcinema_id == 300001
    assert rec.name_ja  # has Japanese name
    assert "赤坂陽一" in rec.name_ja or "赤坂" in rec.name_ja
