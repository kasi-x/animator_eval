"""Tests for parent_madb_id / record_type extraction from MADB JSON-LD.

Verifies that parse_jsonld_dump correctly extracts:
  - parent_madb_id: C-series ID from schema:isPartOf (M-rows)
  - record_type:    @type suffix (all rows)
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.scrapers.parsers.mediaarts import (
    _extract_parent_madb_id,
    _extract_record_type,
    parse_jsonld_dump,
)


# ---------------------------------------------------------------------------
# Unit tests: extraction helpers
# ---------------------------------------------------------------------------


class TestExtractParentMadbId:
    def test_m_row_with_c_parent(self):
        """M-row with schema:isPartOf containing a C-series URI."""
        item = {
            "schema:isPartOf": {
                "@id": "https://mediaarts-db.artmuseums.go.jp/id/C14880"
            }
        }
        assert _extract_parent_madb_id(item) == "C14880"

    def test_c_row_no_parent(self):
        """C-row has no isPartOf → empty string."""
        item = {"schema:identifier": "C10001"}
        assert _extract_parent_madb_id(item) == ""

    def test_none_parent(self):
        """isPartOf is None → empty string."""
        item = {"schema:isPartOf": None}
        assert _extract_parent_madb_id(item) == ""

    def test_missing_id_in_partof(self):
        """isPartOf dict without @id key → empty string."""
        item = {"schema:isPartOf": {"other": "value"}}
        assert _extract_parent_madb_id(item) == ""

    def test_non_dict_partof(self):
        """isPartOf as non-dict → empty string."""
        item = {"schema:isPartOf": "C10001"}
        assert _extract_parent_madb_id(item) == ""

    def test_complex_uri_extraction(self):
        """Correct C-id is extracted from a full URI."""
        item = {
            "schema:isPartOf": {
                "@id": "https://mediaarts-db.artmuseums.go.jp/id/C454415"
            }
        }
        assert _extract_parent_madb_id(item) == "C454415"


class TestExtractRecordType:
    def test_tv_regular_series(self):
        item = {"@type": "class:AnimationTVRegularSeries"}
        assert _extract_record_type(item) == "AnimationTVRegularSeries"

    def test_tv_program(self):
        item = {"@type": "class:AnimationTVProgram"}
        assert _extract_record_type(item) == "AnimationTVProgram"

    def test_no_type(self):
        item = {}
        assert _extract_record_type(item) == ""

    def test_plain_type_no_colon(self):
        """Types without colon prefix are returned as-is."""
        item = {"@type": "AnimationMovieSeries"}
        assert _extract_record_type(item) == "AnimationMovieSeries"


# ---------------------------------------------------------------------------
# Integration: parse_jsonld_dump fixture test
# ---------------------------------------------------------------------------


def _make_sazae_fixture() -> dict:
    """Minimal JSON-LD graph with a C-series and M-episode rows (サザエさん style)."""
    return {
        "@graph": [
            # C-row: series
            {
                "schema:identifier": "C7207",
                "@type": "class:AnimationTVRegularSeries",
                "schema:name": "サザエさん",
                "schema:startDate": "1969-10-05",
                "schema:contributor": "[演出]原征太郎",
            },
            # M-row 1: episode with parent link to C7207
            {
                "schema:identifier": "M20205",
                "@type": "class:AnimationTVProgram",
                "schema:name": "#1　サザエさん",
                "schema:datePublished": "1969-10-05",
                "schema:isPartOf": {
                    "@id": "https://mediaarts-db.artmuseums.go.jp/id/C7207"
                },
                "schema:contributor": "[演出]原征太郎",
            },
            # M-row 2: episode with parent link to C7207
            {
                "schema:identifier": "M20206",
                "@type": "class:AnimationTVProgram",
                "schema:name": "#2　磯野家の秘密",
                "schema:datePublished": "1969-10-12",
                "schema:isPartOf": {
                    "@id": "https://mediaarts-db.artmuseums.go.jp/id/C7207"
                },
            },
            # M-row 3: orphan M (no isPartOf)
            {
                "schema:identifier": "M1067329",
                "@type": "class:AnimationTVProgram",
                "schema:name": "サザエさん",
                "schema:datePublished": "2020-01-01",
            },
        ]
    }


@pytest.fixture()
def sazae_jsonld_path(tmp_path: Path) -> Path:
    fixture = _make_sazae_fixture()
    path = tmp_path / "sazae_test.json"
    path.write_text(json.dumps(fixture), encoding="utf-8")
    return path


class TestParseJsonldDumpParentLink:
    def test_c_row_has_no_parent(self, sazae_jsonld_path: Path):
        """C-prefix rows should have empty parent_madb_id."""
        records = parse_jsonld_dump(sazae_jsonld_path, format_code="TV")
        c_rows = [r for r in records if r["id"] == "C7207"]
        assert len(c_rows) == 1
        assert c_rows[0]["parent_madb_id"] == ""
        assert c_rows[0]["record_type"] == "AnimationTVRegularSeries"

    def test_m_row_with_parent_link(self, sazae_jsonld_path: Path):
        """M-rows with isPartOf should have parent_madb_id set to the C-series ID."""
        records = parse_jsonld_dump(sazae_jsonld_path, format_code="TV")
        m_rows = {r["id"]: r for r in records if r["id"].startswith("M2020")}
        assert "M20205" in m_rows
        assert m_rows["M20205"]["parent_madb_id"] == "C7207"
        assert m_rows["M20205"]["record_type"] == "AnimationTVProgram"
        assert "M20206" in m_rows
        assert m_rows["M20206"]["parent_madb_id"] == "C7207"

    def test_orphan_m_row_empty_parent(self, sazae_jsonld_path: Path):
        """M-rows without isPartOf should have empty parent_madb_id."""
        records = parse_jsonld_dump(sazae_jsonld_path, format_code="TV")
        orphan = [r for r in records if r["id"] == "M1067329"]
        assert len(orphan) == 1
        assert orphan[0]["parent_madb_id"] == ""

    def test_all_records_have_parent_madb_id_key(self, sazae_jsonld_path: Path):
        """Every record dict must contain the parent_madb_id key."""
        records = parse_jsonld_dump(sazae_jsonld_path, format_code="TV")
        assert len(records) > 0
        for r in records:
            assert "parent_madb_id" in r, f"Missing parent_madb_id in record {r['id']}"
            assert "record_type" in r, f"Missing record_type in record {r['id']}"

    def test_m_rows_share_same_parent(self, sazae_jsonld_path: Path):
        """All M-rows for the same series should share the same parent_madb_id."""
        records = parse_jsonld_dump(sazae_jsonld_path, format_code="TV")
        m_parents = {
            r["parent_madb_id"]
            for r in records
            if r["id"].startswith("M") and r["parent_madb_id"]
        }
        # Both M20205 and M20206 point to C7207
        assert m_parents == {"C7207"}
