"""Unit tests for migrate_bangumi_subjects_to_parquet.py.

Tests the row-level helpers (_serialise_row, _stream_anime_rows, _release_date_from_tag)
and the end-to-end _write_parquet_streaming function using in-memory fixture data.
No CLI invocation — keeps tests fast and focused.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

# Import the helpers directly from the script
from scripts.migrate_bangumi_subjects_to_parquet import (
    _SCHEMA,
    _release_date_from_tag,
    _serialise_row,
    _stream_anime_rows,
    _write_parquet_streaming,
)

# ---------------------------------------------------------------------------
# 5-line fixture: types 1, 2, 3, 4, 2
# ---------------------------------------------------------------------------

_FIXTURE_LINES = [
    # type=1 (manga) — should be skipped
    json.dumps({
        "id": 1, "type": 1, "name": "Manga Title", "name_cn": "",
        "infobox": "{{Infobox animanga/Manga|中文名=Manga}}",
        "platform": 0, "summary": "", "nsfw": False, "tags": [], "meta_tags": [],
        "score": 0.0, "score_details": {}, "rank": 0, "date": "2020-01-01",
        "favorite": {}, "series": False,
    }),
    # type=2 (anime) — should be included
    json.dumps({
        "id": 2, "type": 2, "name": "Anime Title One", "name_cn": "动漫一",
        "infobox": "{{Infobox animanga/TVAnime|话数=12}}",
        "platform": 1, "summary": "An anime.", "nsfw": False,
        "tags": [{"name": "action", "count": 100}],
        "meta_tags": ["shounen"],
        "score": 7.5, "score_details": {"1": 10, "2": 20},
        "rank": 500, "date": "2021-04-01",
        "favorite": {"wish": 1, "collect": 200, "doing": 30, "on_hold": 5, "dropped": 2},
        "series": False,
    }),
    # type=3 (music) — should be skipped
    json.dumps({
        "id": 3, "type": 3, "name": "Music Album", "name_cn": "",
        "infobox": "",
        "platform": 0, "summary": "", "nsfw": False, "tags": [], "meta_tags": [],
        "score": 0.0, "score_details": {}, "rank": 0, "date": "",
        "favorite": {}, "series": False,
    }),
    # type=4 (game) — should be skipped
    json.dumps({
        "id": 4, "type": 4, "name": "Game Title", "name_cn": "",
        "infobox": "",
        "platform": 0, "summary": "", "nsfw": False, "tags": [], "meta_tags": [],
        "score": 0.0, "score_details": {}, "rank": 0, "date": "",
        "favorite": {}, "series": False,
    }),
    # type=2 (anime) — should be included
    json.dumps({
        "id": 5, "type": 2, "name": "Anime Title Two", "name_cn": "",
        "infobox": "",
        "platform": 2, "summary": "", "nsfw": True,
        "tags": [],
        "meta_tags": [],
        "score": 6.0, "score_details": {},
        "rank": 1200, "date": "2022-10-07",
        "favorite": {},
        "series": True,
    }),
]


def _write_fixture(tmp_path: Path) -> Path:
    p = tmp_path / "subject.jsonlines"
    p.write_text("\n".join(_FIXTURE_LINES) + "\n", encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# _release_date_from_tag
# ---------------------------------------------------------------------------


def test_release_date_from_tag_standard():
    assert _release_date_from_tag("dump-2025-10-07.142137Z") == "20251007"


def test_release_date_from_tag_no_date_raises():
    with pytest.raises(ValueError, match="Cannot extract date"):
        _release_date_from_tag("nodateatall")


# ---------------------------------------------------------------------------
# _serialise_row
# ---------------------------------------------------------------------------


def test_serialise_row_skips_non_anime():
    for t in (1, 3, 4):
        assert _serialise_row({"type": t, "name": "X"}) is None


def test_serialise_row_keeps_type2():
    raw = json.loads(_FIXTURE_LINES[1])
    row = _serialise_row(raw)
    assert row is not None
    assert row["type"] == 2
    assert row["name"] == "Anime Title One"
    assert row["nsfw"] is False


def test_serialise_row_renames_date_to_release_date():
    raw = json.loads(_FIXTURE_LINES[1])
    row = _serialise_row(raw)
    assert "release_date" in row
    assert "date" not in row
    assert row["release_date"] == "2021-04-01"


def test_serialise_row_json_fields_are_strings():
    """tags / score_details / favorite / meta_tags stored as JSON strings."""
    raw = json.loads(_FIXTURE_LINES[1])
    row = _serialise_row(raw)
    assert isinstance(row["tags"], str)
    assert isinstance(row["score_details"], str)
    assert isinstance(row["favorite"], str)
    # Round-trip verifiable
    assert isinstance(json.loads(row["tags"]), list)
    assert isinstance(json.loads(row["favorite"]), dict)


def test_serialise_row_infobox_stored_as_raw_string_not_deserialized():
    """infobox is stored as-is — in real dumps it's a raw template string."""
    raw = json.loads(_FIXTURE_LINES[1])
    row = _serialise_row(raw)
    # infobox is not in _JSON_FIELDS, so _serialise_row passes it through unchanged.
    # In real bangumi dumps infobox is already a str (MediaWiki template text).
    assert row["infobox"] is not None
    assert isinstance(row["infobox"], str)
    assert "Infobox" in row["infobox"]


# ---------------------------------------------------------------------------
# _stream_anime_rows (integration: only type=2 yielded)
# ---------------------------------------------------------------------------


def test_stream_anime_rows_filters_to_type2_only(tmp_path: Path):
    jl = _write_fixture(tmp_path)
    rows = list(_stream_anime_rows(jl))
    assert len(rows) == 2
    assert all(r["type"] == 2 for r in rows)


def test_stream_anime_rows_ids_correct(tmp_path: Path):
    jl = _write_fixture(tmp_path)
    rows = list(_stream_anime_rows(jl))
    ids = {r["id"] for r in rows}
    assert ids == {2, 5}


# ---------------------------------------------------------------------------
# _write_parquet_streaming — schema and rows
# ---------------------------------------------------------------------------


def test_write_parquet_streaming_writes_2_rows(tmp_path: Path):
    """Only 2 type=2 rows should be written from the 5-line fixture."""
    jl = _write_fixture(tmp_path)
    out = tmp_path / "out.parquet"
    written = _write_parquet_streaming(jl, out, dry_run=False, estimated_rows=2)
    assert written == 2
    assert out.exists()


def test_write_parquet_schema_has_16_columns(tmp_path: Path):
    """Output schema must have exactly 16 columns matching _SCHEMA."""
    jl = _write_fixture(tmp_path)
    out = tmp_path / "out.parquet"
    _write_parquet_streaming(jl, out, dry_run=False, estimated_rows=2)
    schema = pq.read_schema(out)
    assert len(schema) == 16
    assert set(schema.names) == set(f.name for f in _SCHEMA)


def test_write_parquet_key_column_types(tmp_path: Path):
    """id is int64, type is int32, name is string, nsfw is bool."""
    import pyarrow as pa
    jl = _write_fixture(tmp_path)
    out = tmp_path / "out.parquet"
    _write_parquet_streaming(jl, out, dry_run=False, estimated_rows=2)
    schema = pq.read_schema(out)
    col = {f.name: f.type for f in schema}
    assert col["id"] == pa.int64()
    assert col["type"] == pa.int32()
    assert col["name"] == pa.string()
    assert col["nsfw"] == pa.bool_()


def test_write_parquet_tags_and_score_details_are_strings(tmp_path: Path):
    """Columns tags, score_details, favorite must be string (JSON) in parquet."""
    import pyarrow as pa
    jl = _write_fixture(tmp_path)
    out = tmp_path / "out.parquet"
    _write_parquet_streaming(jl, out, dry_run=False, estimated_rows=2)
    tbl = pq.read_table(out)
    for col_name in ("tags", "score_details", "favorite"):
        col = tbl.column(col_name)
        assert col.type == pa.string(), f"{col_name} should be string, got {col.type}"
        # Each non-null value should be valid JSON
        for val in col.to_pylist():
            if val is not None:
                json.loads(val)  # should not raise


def test_write_parquet_dry_run_does_not_write(tmp_path: Path):
    """Dry run: returns row count but writes no file."""
    jl = _write_fixture(tmp_path)
    out = tmp_path / "dryout.parquet"
    written = _write_parquet_streaming(jl, out, dry_run=True, estimated_rows=2)
    assert written == 2
    assert not out.exists()
