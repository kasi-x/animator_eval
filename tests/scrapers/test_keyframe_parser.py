"""Tests for src/scrapers/parsers/keyframe.py — HTML preloadData parsers."""
from __future__ import annotations

from pathlib import Path

from src.scrapers.parsers.keyframe import (
    _extract_episode_num,
    _extract_episode_title,
    collect_studio_master,
    extract_preload_data,
    parse_anime_meta,
    parse_anime_studios,
    parse_credits_from_data,
    parse_settings_categories,
)

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "scrapers" / "keyframe"


# ---------------------------------------------------------------------------
# _extract_episode_num
# ---------------------------------------------------------------------------


class TestExtractEpisodeNum:
    def test_hash_format(self):
        assert _extract_episode_num("#01") == 1

    def test_hash_format_large(self):
        assert _extract_episode_num("#1234") == 1234

    def test_episode_word(self):
        assert _extract_episode_num("Episode 5") == 5

    def test_ep_abbreviation(self):
        assert _extract_episode_num("Ep. 3") == 3

    def test_overview_returns_minus_one(self):
        assert _extract_episode_num("Overview") == -1

    def test_op_returns_minus_one(self):
        assert _extract_episode_num("OP") == -1

    def test_empty_string_returns_minus_one(self):
        assert _extract_episode_num("") == -1


# ---------------------------------------------------------------------------
# _extract_episode_title
# ---------------------------------------------------------------------------


class TestExtractEpisodeTitle:
    def test_kagi_bracket(self):
        title = _extract_episode_title("#01「夜明けの冒険！」")
        assert title == "夜明けの冒険！"

    def test_nijukagi_bracket(self):
        title = _extract_episode_title("#02『特別な回』")
        assert title == "特別な回"

    def test_returns_none_when_absent(self):
        assert _extract_episode_title("#03") is None

    def test_returns_none_for_overview(self):
        assert _extract_episode_title("Overview") is None


# ---------------------------------------------------------------------------
# extract_preload_data
# ---------------------------------------------------------------------------


_MINIMAL_PRELOAD_HTML = """\
<html><body>
<script>
preloadData = {"title": "Test Anime", "menus": []};
</script>
</body></html>
"""

_NO_PRELOAD_HTML = "<html><body><p>No data here.</p></body></html>"

_TRAILING_COMMA_HTML = """\
<html><body>
<script>
preloadData = {"title": "Trailing", "menus": [], "extra": "ok",};
</script>
</body></html>
"""


class TestExtractPreloadData:
    def test_basic_extraction(self):
        data = extract_preload_data(_MINIMAL_PRELOAD_HTML)
        assert data is not None
        assert data["title"] == "Test Anime"

    def test_returns_none_when_absent(self):
        assert extract_preload_data(_NO_PRELOAD_HTML) is None

    def test_trailing_comma_recovery(self):
        data = extract_preload_data(_TRAILING_COMMA_HTML)
        assert data is not None
        assert data["title"] == "Trailing"

    def test_fixture_html(self):
        html = (FIXTURE_DIR / "sample_one-piece.html").read_text()
        data = extract_preload_data(html)
        assert data is not None
        assert data.get("title") == "ONE PIECE"


# ---------------------------------------------------------------------------
# parse_anime_meta
# ---------------------------------------------------------------------------


class TestParseAnimeMeta:
    def _make_data(self):
        return {
            "uuid": "u-123",
            "savingId": 42,
            "author": "author1",
            "status": "public",
            "comment": None,
            "anilistId": 21,
            "anilist": {
                "id": 21,
                "title": {"native": "ワンピース", "english": "ONE PIECE", "romaji": "One Piece"},
                "synonyms": ["OP"],
                "format": "TV",
                "episodes": None,
                "season": "FALL",
                "seasonYear": 1999,
                "startDate": {"year": 1999, "month": 10, "day": 20},
                "endDate": {},
                "coverImage": {"extraLarge": "https://img.ex/op.jpg"},
                "isAdult": False,
                "status": "RELEASING",
                "studios": {"edges": [{"node": {"name": "Toei"}, "isMain": True}]},
            },
            "settings": {
                "categories": [{"name": "Main Staff"}],
                "delimiters": None,
            },
            "menus": [],
        }

    def test_basic_fields(self):
        data = self._make_data()
        meta = parse_anime_meta(data, "one-piece")
        assert meta["kf_uuid"] == "u-123"
        assert meta["title_ja"] == "ワンピース"
        assert meta["title_en"] == "ONE PIECE"
        assert meta["anilist_id"] == 21
        assert meta["season"] == "FALL"
        assert meta["slug"] == "one-piece"

    def test_from_fixture(self):
        html = (FIXTURE_DIR / "sample_one-piece.html").read_text()
        data = extract_preload_data(html)
        assert data is not None
        meta = parse_anime_meta(data, "one-piece")
        assert meta["slug"] == "one-piece"
        assert meta["anilist_id"] is not None


# ---------------------------------------------------------------------------
# parse_anime_studios
# ---------------------------------------------------------------------------


class TestParseAnimeStudios:
    def test_extracts_studios_from_edges(self):
        data = {
            "anilist": {
                "studios": {
                    "edges": [
                        {"node": {"name": "Toei Animation"}, "isMain": True},
                        {"node": {"name": "Sub Studio"}, "isMain": False},
                    ]
                }
            }
        }
        studios = parse_anime_studios(data)
        assert len(studios) == 2
        assert studios[0]["studio_name"] == "Toei Animation"
        assert studios[0]["is_main"] is True
        assert studios[1]["is_main"] is False

    def test_returns_empty_when_no_anilist(self):
        assert parse_anime_studios({}) == []

    def test_from_fixture(self):
        html = (FIXTURE_DIR / "sample_one-piece.html").read_text()
        data = extract_preload_data(html)
        assert data is not None
        studios = parse_anime_studios(data)
        assert len(studios) >= 1


# ---------------------------------------------------------------------------
# parse_settings_categories
# ---------------------------------------------------------------------------


class TestParseSettingsCategories:
    def test_order_preserved(self):
        data = {
            "settings": {
                "categories": [
                    {"name": "Main Staff"},
                    {"name": "Animation"},
                    {"name": "Music"},
                ]
            }
        }
        cats = parse_settings_categories(data)
        assert len(cats) == 3
        assert cats[0]["category_order"] == 0
        assert cats[1]["category_name"] == "Animation"
        assert cats[2]["category_order"] == 2

    def test_skips_entries_without_name(self):
        data = {"settings": {"categories": [{"name": "A"}, {}, {"name": "C"}]}}
        cats = parse_settings_categories(data)
        assert len(cats) == 2

    def test_from_fixture(self):
        html = (FIXTURE_DIR / "sample_one-piece.html").read_text()
        data = extract_preload_data(html)
        assert data is not None
        cats = parse_settings_categories(data)
        assert len(cats) >= 1


# ---------------------------------------------------------------------------
# parse_credits_from_data
# ---------------------------------------------------------------------------


_SIMPLE_PRELOAD = {
    "menus": [
        {
            "name": "#01「始まり」",
            "note": None,
            "credits": [
                {
                    "name": "Core",
                    "roles": [
                        {
                            "name": "Director",
                            "original": "監督",
                            "staff": [
                                {"id": 100, "ja": "田中", "en": "Tanaka", "isStudio": False},
                                {"id": None, "ja": "テスト", "en": "", "isStudio": False},
                            ],
                        },
                        {
                            "name": "Studio",
                            "original": "制作会社",
                            "staff": [
                                {
                                    "id": 500,
                                    "ja": "スタジオA",
                                    "en": "Studio A",
                                    "isStudio": True,
                                }
                            ],
                        },
                    ],
                }
            ],
        }
    ]
}


class TestParseCreditsFromData:
    def test_basic_credit_extracted(self):
        credits = parse_credits_from_data(_SIMPLE_PRELOAD, "test-slug")
        person_ids = [c["person_id"] for c in credits]
        assert 100 in person_ids

    def test_episode_num_extracted(self):
        credits = parse_credits_from_data(_SIMPLE_PRELOAD, "test-slug")
        ep_credits = [c for c in credits if c["person_id"] == 100]
        assert ep_credits[0]["episode"] == 1

    def test_episode_title_extracted(self):
        credits = parse_credits_from_data(_SIMPLE_PRELOAD, "test-slug")
        ep_credits = [c for c in credits if c["person_id"] == 100]
        assert ep_credits[0]["episode_title"] == "始まり"

    def test_studio_role_kept(self):
        """isStudio=True entries should now be retained."""
        credits = parse_credits_from_data(_SIMPLE_PRELOAD, "test-slug")
        studio_credits = [c for c in credits if c.get("is_studio_role")]
        assert len(studio_credits) >= 1

    def test_empty_person_skipped(self):
        """Staff with id=None AND no name should be skipped."""
        data = {"menus": [{"name": "Overview", "credits": [{"roles": [
            {"name": "Misc", "original": "", "staff": [
                {"id": None, "ja": "", "en": "", "isStudio": False}
            ]}
        ]}]}]}
        credits = parse_credits_from_data(data, "slug")
        assert len(credits) == 0

    def test_returns_list_from_fixture(self):
        html = (FIXTURE_DIR / "sample_one-piece.html").read_text()
        data = extract_preload_data(html)
        assert data is not None
        credits = parse_credits_from_data(data, "one-piece")
        assert isinstance(credits, list)
        assert len(credits) > 0


# ---------------------------------------------------------------------------
# collect_studio_master
# ---------------------------------------------------------------------------


class TestCollectStudioMaster:
    def test_deduplicates_studio_ids(self):
        credits = [
            {"is_studio_role": True, "person_id": 10, "name_ja": "スタジオ", "name_en": "Studio"},
            {"is_studio_role": True, "person_id": 10, "name_ja": "スタジオ", "name_en": "Studio"},
            {"is_studio_role": False, "person_id": 20, "name_ja": "Person", "name_en": "Person"},
        ]
        masters = collect_studio_master(credits)
        assert len(masters) == 1
        assert masters[0]["studio_id"] == 10

    def test_returns_empty_when_no_studio_roles(self):
        credits = [{"is_studio_role": False, "person_id": 1}]
        assert collect_studio_master(credits) == []

    def test_sorted_by_studio_id(self):
        credits = [
            {"is_studio_role": True, "person_id": 30, "name_ja": "C", "name_en": "C"},
            {"is_studio_role": True, "person_id": 10, "name_ja": "A", "name_en": "A"},
            {"is_studio_role": True, "person_id": 20, "name_ja": "B", "name_en": "B"},
        ]
        masters = collect_studio_master(credits)
        ids = [m["studio_id"] for m in masters]
        assert ids == sorted(ids)
