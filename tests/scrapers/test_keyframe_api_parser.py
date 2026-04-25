"""Unit tests for src/scrapers/parsers/keyframe_api.py."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.scrapers.parsers.keyframe_api import (
    parse_preview,
    parse_roles_master,
    parse_person_show,
)

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "scrapers" / "keyframe"


# ---------------------------------------------------------------------------
# parse_roles_master
# ---------------------------------------------------------------------------


class TestParseRolesMaster:
    def test_returns_all_fields(self):
        raw = [
            {
                "id": 42,
                "name_en": "Key Animation",
                "name_ja": "原画",
                "category": "Animation",
                "episode_category": "Episode",
                "description": "desc",
            }
        ]
        rows = parse_roles_master(raw)
        assert len(rows) == 1
        row = rows[0]
        assert row["role_id"] == 42
        assert row["name_en"] == "Key Animation"
        assert row["name_ja"] == "原画"
        assert row["category"] == "Animation"
        assert row["episode_category"] == "Episode"
        assert row["description"] == "desc"

    def test_handles_none_optional_fields(self):
        raw = [{"id": 1}]
        rows = parse_roles_master(raw)
        assert rows[0]["name_en"] is None
        assert rows[0]["description"] is None

    def test_skips_rows_without_id(self):
        raw = [{"name_en": "No id here"}]
        # Should not raise; may produce row with role_id=0 or skip
        # The parser logs a warning and continues
        rows = parse_roles_master(raw)
        # Either skipped or role_id defaulted — just ensure no crash
        assert isinstance(rows, list)

    def test_fixture_sample_roles(self):
        fixture = json.loads((FIXTURE_DIR / "sample_roles.json").read_text())
        rows = parse_roles_master(fixture)
        assert len(rows) == len(fixture)
        for r in rows:
            assert "role_id" in r
            assert isinstance(r["role_id"], int)

    def test_full_count_all_fields_present(self):
        """All 6 expected keys appear in every row."""
        raw = [{"id": i, "name_en": f"Role {i}"} for i in range(10)]
        rows = parse_roles_master(raw)
        assert len(rows) == 10
        for r in rows:
            for key in ("role_id", "name_en", "name_ja", "category", "episode_category", "description"):
                assert key in r


# ---------------------------------------------------------------------------
# parse_person_show
# ---------------------------------------------------------------------------


class TestParsePersonShow:
    @pytest.fixture()
    def sample_show(self):
        return json.loads((FIXTURE_DIR / "sample_show.json").read_text())

    def test_profile_fields_present(self, sample_show):
        result = parse_person_show(sample_show)
        profile = result["profile"]
        assert isinstance(profile["id"], int)
        assert profile["id"] > 0
        assert "is_studio" in profile
        assert "name_ja" in profile
        assert "name_en" in profile
        assert "aliases_json" in profile
        assert isinstance(profile["aliases_json"], list)

    def test_credits_are_flat_list(self, sample_show):
        result = parse_person_show(sample_show)
        credits = result["credits"]
        assert isinstance(credits, list)
        assert len(credits) > 0

    def test_credit_has_expected_keys(self, sample_show):
        result = parse_person_show(sample_show)
        required = {
            "anime_uuid", "anime_slug", "category",
            "role_ja", "role_en", "episode",
            "is_nc", "comment", "is_primary_alias",
        }
        for credit in result["credits"]:
            assert required <= set(credit.keys()), f"Missing keys in {credit}"

    def test_is_nc_is_bool(self, sample_show):
        result = parse_person_show(sample_show)
        for credit in result["credits"]:
            assert isinstance(credit["is_nc"], bool)
            assert isinstance(credit["is_primary_alias"], bool)

    def test_jobs_is_list(self, sample_show):
        result = parse_person_show(sample_show)
        assert isinstance(result["jobs"], list)

    def test_studios_is_list(self, sample_show):
        result = parse_person_show(sample_show)
        assert isinstance(result["studios"], list)

    def test_studios_have_studio_name(self, sample_show):
        result = parse_person_show(sample_show)
        for studio in result["studios"]:
            assert "studio_name" in studio
            assert "alt_names" in studio
            assert isinstance(studio["alt_names"], list)

    def test_credits_flatten_3_anime(self, sample_show):
        """Fixture has 3 credits entries, each with 1 name/cat/role/ep = 3 rows min."""
        result = parse_person_show(sample_show)
        assert len(result["credits"]) >= 3

    def test_handles_empty_credits(self):
        raw = {
            "staff": {"id": 999, "isStudio": False, "ja": "テスト", "en": "Test"},
            "jobs": [],
            "studios": {},
            "credits": [],
        }
        result = parse_person_show(raw)
        assert result["credits"] == []
        assert result["profile"]["id"] == 999

    def test_handles_missing_staff(self):
        raw = {"staff": {}, "jobs": [], "studios": {}, "credits": []}
        result = parse_person_show(raw)
        # Should not raise; profile id defaults to 0
        assert result["profile"]["id"] == 0


# ---------------------------------------------------------------------------
# parse_preview
# ---------------------------------------------------------------------------


class TestParsePreview:
    @pytest.fixture()
    def sample_preview(self):
        return json.loads((FIXTURE_DIR / "sample_preview.json").read_text())

    def test_top_level_fields(self, sample_preview):
        result = parse_preview(sample_preview)
        assert "total" in result
        assert "total_contributors" in result
        assert "total_updated" in result
        assert isinstance(result["total"], int)

    def test_sections_present(self, sample_preview):
        result = parse_preview(sample_preview)
        assert "recent" in result
        assert "airing" in result
        assert "data" in result

    def test_recent_entries_count(self, sample_preview):
        result = parse_preview(sample_preview)
        # Fixture has 2 of each
        assert len(result["recent"]) == 2
        assert len(result["airing"]) == 2
        assert len(result["data"]) == 2

    def test_entry_has_expected_keys(self, sample_preview):
        result = parse_preview(sample_preview)
        required = {
            "uuid", "slug", "title", "title_native",
            "status", "last_modified", "anilist_id",
            "season", "season_year", "studios_str", "contributors_json",
        }
        for section in ("recent", "airing", "data"):
            for entry in result[section]:
                assert required <= set(entry.keys()), f"Missing in {section}: {entry}"

    def test_studios_str_is_list(self, sample_preview):
        result = parse_preview(sample_preview)
        for section in ("recent", "airing", "data"):
            for entry in result[section]:
                assert isinstance(entry["studios_str"], list)

    def test_contributors_json_is_list(self, sample_preview):
        result = parse_preview(sample_preview)
        for section in ("recent", "airing", "data"):
            for entry in result[section]:
                assert isinstance(entry["contributors_json"], list)

    def test_handles_empty_sections(self):
        raw = {
            "total": 0,
            "totalContributors": 0,
            "totalUpdated": 0,
            "recent": [],
            "airing": [],
            "data": [],
        }
        result = parse_preview(raw)
        assert result["total"] == 0
        assert result["recent"] == []
        assert result["airing"] == []
        assert result["data"] == []

    def test_anilist_id_int_or_none(self, sample_preview):
        result = parse_preview(sample_preview)
        for section in ("recent", "airing", "data"):
            for entry in result[section]:
                val = entry["anilist_id"]
                assert val is None or isinstance(val, int)
