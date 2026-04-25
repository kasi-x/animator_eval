"""AniList parser unit tests using captured GraphQL JSON fixtures."""

from __future__ import annotations

import json
from pathlib import Path

from src.scrapers.anilist_scraper import parse_anilist_anime
from src.scrapers.parsers.anilist import parse_anilist_characters, parse_anilist_person, parse_anilist_staff

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


def test_parse_anime_airing_schedule():
    raw = _load("media_1.json")["data"]["Media"]
    bronze = parse_anilist_anime(raw)
    assert bronze.airing_schedule_json is not None
    schedule = json.loads(bronze.airing_schedule_json)
    assert len(schedule) == 2
    assert schedule[0]["episode"] == 1
    assert schedule[0]["airingAt"] == 891648000


def test_parse_anime_airing_schedule_absent():
    raw = {"id": 999, "title": {"romaji": "X", "english": None, "native": None}}
    bronze = parse_anilist_anime(raw)
    assert bronze.airing_schedule_json is None


def test_parse_anilist_person_primary_occupations():
    staff = {
        "id": 42,
        "name": {"full": "Test Person", "native": "テスト", "alternative": []},
        "primaryOccupations": ["Animator", "Character Designer"],
        "yearsActive": [2010, 2024],
        "homeTown": None,
        "dateOfBirth": {"year": 1985, "month": 3, "day": 10},
        "age": 39,
        "gender": "Male",
        "bloodType": "A",
        "description": None,
        "favourites": 100,
        "siteUrl": None,
        "image": {"large": None, "medium": None},
    }
    person = parse_anilist_person(staff)
    assert person.primary_occupations == ["Animator", "Character Designer"]


def test_parse_anilist_person_primary_occupations_absent():
    staff = {
        "id": 43,
        "name": {"full": "No Occupations", "native": None, "alternative": []},
        "image": {},
    }
    person = parse_anilist_person(staff)
    assert person.primary_occupations == []


def test_parse_anilist_staff_primary_occupations():
    edges = [
        {
            "role": "Animation Director",
            "node": {
                "id": 99,
                "name": {"full": "Staff San", "native": "スタッフさん", "alternative": []},
                "primaryOccupations": ["Key Animator"],
                "yearsActive": [2015],
                "homeTown": None,
                "dateOfBirth": {},
                "age": None,
                "gender": None,
                "bloodType": None,
                "description": None,
                "favourites": 50,
                "siteUrl": None,
                "image": {"large": None, "medium": None},
            },
        }
    ]
    persons, credits = parse_anilist_staff(edges, "anilist:1")
    assert len(persons) == 1
    assert persons[0].primary_occupations == ["Key Animator"]
    assert len(credits) >= 1


_CHAR_EDGES = [
    {
        "role": "MAIN",
        "node": {
            "id": 101,
            "name": {"full": "Spike Spiegel", "native": "スパイク・スピーゲル", "alternative": []},
            "image": {"large": "https://example.com/spike.jpg", "medium": None},
            "description": "A bounty hunter.",
            "gender": "Male",
            "dateOfBirth": {"year": None, "month": None, "day": None},
            "age": "27",
            "bloodType": "O",
            "favourites": 5000,
            "siteUrl": "https://anilist.co/character/101",
        },
        "voiceActors": [{"id": 201}],
    },
    {
        "role": "SUPPORTING",
        "node": {
            "id": 102,
            "name": {"full": "Jet Black", "native": "ジェット・ブラック", "alternative": []},
            "image": {"large": None, "medium": None},
            "description": None,
            "gender": "Male",
            "dateOfBirth": {},
            "age": None,
            "bloodType": None,
            "favourites": 0,
            "siteUrl": None,
        },
        "voiceActors": [],
    },
]


def test_parse_anilist_characters_basic():
    characters, cva_list = parse_anilist_characters(_CHAR_EDGES, "anilist:1")
    assert len(characters) == 2
    assert characters[0].id == "anilist:c101"
    assert characters[0].name_ja == "スパイク・スピーゲル"
    assert characters[0].name_en == "Spike Spiegel"
    assert characters[0].gender == "Male"
    assert characters[0].blood_type == "O"
    assert characters[0].favourites == 5000
    assert characters[1].id == "anilist:c102"


def test_parse_anilist_characters_cva_mapping():
    _, cva_list = parse_anilist_characters(_CHAR_EDGES, "anilist:1")
    assert len(cva_list) == 1
    assert cva_list[0].character_id == "anilist:c101"
    assert cva_list[0].person_id == "anilist:p201"
    assert cva_list[0].anime_id == "anilist:1"
    assert cva_list[0].character_role == "MAIN"


def test_parse_anilist_characters_dedup():
    duplicate_edges = _CHAR_EDGES + [_CHAR_EDGES[0]]
    characters, _ = parse_anilist_characters(duplicate_edges, "anilist:1")
    assert len(characters) == 2  # same id deduplicated


def test_parse_anilist_characters_empty():
    characters, cva_list = parse_anilist_characters([], "anilist:1")
    assert characters == []
    assert cva_list == []
