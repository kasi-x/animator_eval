"""AniList parser unit tests using captured GraphQL JSON fixtures."""

from __future__ import annotations

import json
from pathlib import Path

from src.scrapers.anilist_scraper import parse_anilist_anime

FIXTURES = Path(__file__).parent / "fixtures" / "scrapers" / "anilist"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_parse_anime_basic():
    """Cowboy Bebop fixture (Media id=1)."""
    raw = _load("media_1.json")["data"]["Media"]
    bronze = parse_anilist_anime(raw)
    assert bronze.id == "anilist:1"
    assert bronze.anilist_id == 1
    assert bronze.title_en == "Cowboy Bebop"
    assert "カウボーイビバップ" in bronze.title_ja
    assert bronze.start_date == "1998-04-03"
    assert bronze.episodes == 26
    assert bronze.format == "TV"


def test_parse_anime_handles_missing_optional_fields():
    """Minimal fixture: only id + title; no startDate, episodes, etc."""
    raw = {"id": 999, "title": {"romaji": "X", "english": None, "native": None}}
    bronze = parse_anilist_anime(raw)
    assert bronze.id == "anilist:999"
    assert bronze.start_date is None
    assert bronze.episodes is None


def test_parse_anime_id_format():
    raw = _load("media_1.json")["data"]["Media"]
    bronze = parse_anilist_anime(raw)
    # Bronze IDs follow {source}:{native_id} convention
    assert bronze.id == f"anilist:{bronze.anilist_id}"


def test_parse_anime_score_in_bronze_only():
    """anime.score must end up in BRONZE for display, never used in scoring.
    The parser captures it from API. Downstream policy enforces non-use."""
    raw = _load("media_1.json")["data"]["Media"]
    bronze = parse_anilist_anime(raw)
    # averageScore comes from API
    assert bronze.score is not None
