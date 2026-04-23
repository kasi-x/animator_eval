"""Jikan (MAL) parser unit tests using captured fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.scrapers.mal_scraper import parse_anime_data, parse_staff_data

FIXTURES = Path(__file__).parent / "fixtures" / "scrapers" / "mal"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def anime_1_raw():
    return _load("anime_1.json")["data"]


@pytest.fixture
def anime_1_staff_raw():
    return _load("anime_1_staff.json")["data"]


def test_parse_anime_basic_fields(anime_1_raw):
    bronze = parse_anime_data(anime_1_raw)
    assert bronze.id == "mal:1"
    assert bronze.mal_id == 1
    assert bronze.title_en == "Cowboy Bebop"
    assert bronze.title_ja  # has Japanese title
    assert bronze.year == 1998
    assert bronze.episodes == 26


def test_parse_anime_score_present(anime_1_raw):
    """anime.score is in the bronze layer for display only.
    It must NOT be used in scoring (CLAUDE.md constraint), but parser should
    still capture it from the source for audit trail."""
    bronze = parse_anime_data(anime_1_raw)
    assert bronze.score is not None
    assert bronze.score > 0


def test_parse_staff_creates_persons_and_credits(anime_1_staff_raw):
    persons, credits = parse_staff_data(anime_1_staff_raw, anime_id="mal:1")
    assert len(persons) > 0
    assert len(credits) > 0


def test_parse_staff_person_id_format(anime_1_staff_raw):
    persons, _ = parse_staff_data(anime_1_staff_raw, anime_id="mal:1")
    assert all(p.id.startswith("mal:p") for p in persons)


def test_parse_staff_credit_anime_id(anime_1_staff_raw):
    _, credits = parse_staff_data(anime_1_staff_raw, anime_id="mal:1")
    assert all(c.anime_id == "mal:1" for c in credits)
    assert all(c.source == "mal" for c in credits)


def test_parse_staff_handles_lastname_firstname(anime_1_staff_raw):
    """Jikan returns names as 'Last, First'. Parser must reverse to 'First Last'."""
    persons, _ = parse_staff_data(anime_1_staff_raw, anime_id="mal:1")
    # No comma should appear in name_en after parse
    assert all("," not in p.name_en for p in persons), (
        f"comma found in: {[p.name_en for p in persons if ',' in p.name_en][:3]}"
    )


def test_parse_staff_skips_missing_person_id():
    """Entries without person.mal_id must be silently skipped."""
    fake = [
        {"person": {"mal_id": None, "name": "Anon"}, "positions": ["Director"]},
        {"person": {"mal_id": 99, "name": "Real Name"}, "positions": ["Director"]},
    ]
    persons, credits = parse_staff_data(fake, anime_id="mal:1")
    assert len(persons) == 1
    assert persons[0].id == "mal:p99"


def test_parse_anime_handles_missing_titles():
    """No 'titles' array → fall back to 'title' string."""
    raw = {"mal_id": 42, "title": "Fallback Title"}
    bronze = parse_anime_data(raw)
    assert bronze.title_en == "Fallback Title"
    assert bronze.title_ja == ""
