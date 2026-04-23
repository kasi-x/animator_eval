"""Tests for keyframe_scraper parse functions (no network calls)."""
from __future__ import annotations

from src.scrapers.keyframe_scraper import (
    _extract_episode_num,
    extract_preload_data,
    parse_credits_from_data,
)


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
    def test_extracts_valid_json(self):
        result = extract_preload_data(_MINIMAL_PRELOAD_HTML)
        assert result is not None
        assert result["title"] == "Test Anime"
        assert result["menus"] == []

    def test_returns_none_when_no_preload(self):
        result = extract_preload_data(_NO_PRELOAD_HTML)
        assert result is None

    def test_fixes_trailing_comma(self):
        result = extract_preload_data(_TRAILING_COMMA_HTML)
        assert result is not None
        assert result["title"] == "Trailing"

    def test_returns_none_for_completely_broken_json(self):
        broken = "<script>preloadData = {unclosed brace;</script>"
        result = extract_preload_data(broken)
        assert result is None


# ---------------------------------------------------------------------------
# parse_credits_from_data
# ---------------------------------------------------------------------------

def _make_preload(menus: list[dict]) -> dict:
    return {"title": "Anime X", "menus": menus}


def _make_menu(name: str, staff_list: list[dict], role_ja="原画", role_en="Key Animation") -> dict:
    return {
        "name": name,
        "credits": [
            {
                "roles": [
                    {
                        "original": role_ja,
                        "name": role_en,
                        "staff": staff_list,
                    }
                ]
            }
        ],
    }


def _make_staff(pid: int, name_ja: str = "", name_en: str = "", is_studio: bool = False) -> dict:
    return {"id": pid, "ja": name_ja, "en": name_en, "isStudio": is_studio}


class TestParseCreditsFromData:
    def test_empty_menus_returns_empty(self):
        result = parse_credits_from_data(_make_preload([]), "anime-x")
        assert result == []

    def test_single_credit_parsed(self):
        staff = [_make_staff(101, name_ja="田中太郎", name_en="Taro Tanaka")]
        menu = _make_menu("#01", staff)
        result = parse_credits_from_data(_make_preload([menu]), "anime-x")
        assert len(result) == 1
        credit = result[0]
        assert credit["person_id"] == 101
        assert credit["name_ja"] == "田中太郎"
        assert credit["name_en"] == "Taro Tanaka"
        assert credit["episode"] == 1

    def test_episode_number_extracted(self):
        staff = [_make_staff(102, name_en="Jane Doe")]
        menu = _make_menu("Episode 3", staff)
        result = parse_credits_from_data(_make_preload([menu]), "anime-x")
        assert result[0]["episode"] == 3

    def test_overview_menu_episode_minus_one(self):
        staff = [_make_staff(103, name_en="John Smith")]
        menu = _make_menu("Overview", staff)
        result = parse_credits_from_data(_make_preload([menu]), "anime-x")
        assert result[0]["episode"] == -1

    def test_studio_entry_skipped(self):
        studio = _make_staff(999, name_en="Cool Studio", is_studio=True)
        person = _make_staff(200, name_en="Real Person")
        menu = _make_menu("#01", [studio, person])
        result = parse_credits_from_data(_make_preload([menu]), "anime-x")
        assert all(c["person_id"] != 999 for c in result)
        assert any(c["person_id"] == 200 for c in result)

    def test_no_id_skipped(self):
        bad_staff = {"ja": "誰か", "en": "Someone"}  # no 'id' key
        good_staff = _make_staff(201, name_en="Good Person")
        menu = _make_menu("#01", [bad_staff, good_staff])
        result = parse_credits_from_data(_make_preload([menu]), "anime-x")
        assert len(result) == 1
        assert result[0]["person_id"] == 201

    def test_no_name_skipped(self):
        nameless = _make_staff(300, name_ja="", name_en="")
        named = _make_staff(301, name_en="Named Person")
        menu = _make_menu("#01", [nameless, named])
        result = parse_credits_from_data(_make_preload([menu]), "anime-x")
        assert all(c["person_id"] != 300 for c in result)

    def test_role_fields_captured(self):
        staff = [_make_staff(400, name_en="Director Person")]
        menu = _make_menu("#01", staff, role_ja="監督", role_en="Director")
        result = parse_credits_from_data(_make_preload([menu]), "anime-x")
        assert result[0]["role_ja"] == "監督"
        assert result[0]["role_en"] == "Director"

    def test_multiple_menus_aggregated(self):
        staff1 = [_make_staff(501, name_en="Person A")]
        staff2 = [_make_staff(502, name_en="Person B")]
        menus = [_make_menu("#01", staff1), _make_menu("#02", staff2)]
        result = parse_credits_from_data(_make_preload(menus), "anime-x")
        assert len(result) == 2
        ids = {c["person_id"] for c in result}
        assert ids == {501, 502}
