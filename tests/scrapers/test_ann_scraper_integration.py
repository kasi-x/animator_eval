"""ANN scraper integration tests — verifies 8-table BRONZE output.

Tests use pre-fetched XML fixtures and a tmp_path BRONZE root.
No network required.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import pyarrow.parquet as pq

from src.scrapers.ann_scraper import (
    _AnimeBronzeWriters,
    save_anime_parse_result,
    save_person_detail,
)
from src.scrapers.parsers.ann import parse_anime_xml, parse_person_html

FIXTURES = Path(__file__).parent.parent / "fixtures" / "scrapers" / "ann"
TABLES_8 = ("anime", "credits", "cast", "company", "episodes", "releases", "news", "related")


def _make_writers(tmp_path: Path) -> _AnimeBronzeWriters:
    return _AnimeBronzeWriters(root=tmp_path)


def test_save_anime_parse_result_produces_8_tables(tmp_path):
    root = ET.fromstring((FIXTURES / "anime_batch.xml").read_text())
    result = parse_anime_xml(root)

    writers = _make_writers(tmp_path)
    n_anime, n_credits = save_anime_parse_result(writers, result)
    writers.flush_all()

    assert n_anime == 4
    assert n_credits > 0

    for tbl in TABLES_8:
        files = list((tmp_path / "source=ann" / f"table={tbl}").rglob("*.parquet"))
        assert files, f"Missing parquet for table={tbl}"


def test_anime_table_has_expected_columns(tmp_path):
    root = ET.fromstring((FIXTURES / "anime_batch.xml").read_text())
    writers = _make_writers(tmp_path)
    save_anime_parse_result(writers, parse_anime_xml(root))
    writers.flush_all()

    files = list((tmp_path / "source=ann" / "table=anime").rglob("*.parquet"))
    schema = pq.read_schema(files[0])
    col_names = set(schema.names)
    for col in ("ann_id", "title_en", "display_rating_votes", "opening_themes_json", "image_url"):
        assert col in col_names, f"Missing column {col!r} in anime table"


def test_credits_table_has_role_column(tmp_path):
    root = ET.fromstring((FIXTURES / "anime_batch.xml").read_text())
    writers = _make_writers(tmp_path)
    save_anime_parse_result(writers, parse_anime_xml(root))
    writers.flush_all()

    files = list((tmp_path / "source=ann" / "table=credits").rglob("*.parquet"))
    schema = pq.read_schema(files[0])
    assert "role" in schema.names
    assert "task_raw" in schema.names
    assert "gid" in schema.names


def test_cast_table_rows(tmp_path):
    root = ET.fromstring((FIXTURES / "anime_batch.xml").read_text())
    writers = _make_writers(tmp_path)
    save_anime_parse_result(writers, parse_anime_xml(root))
    writers.flush_all()

    files = list((tmp_path / "source=ann" / "table=cast").rglob("*.parquet"))
    tbl = pq.read_table(files[0])
    assert tbl.num_rows > 0
    assert "voice_actor_name" in tbl.schema.names
    assert "character_name" in tbl.schema.names


def test_person_detail_new_columns(tmp_path):
    from src.scrapers.bronze_writer import BronzeWriter

    html = (FIXTURES / "person_260.html").read_text()
    detail = parse_person_html(html, ann_id=260)
    assert detail is not None

    persons_bw = BronzeWriter("ann", table="persons", root=tmp_path)
    save_person_detail(persons_bw, detail)
    persons_bw.flush()

    files = list((tmp_path / "source=ann" / "table=persons").rglob("*.parquet"))
    assert files
    schema = pq.read_schema(files[0])
    col_names = set(schema.names)
    for col in ("credits_json", "alt_names_json", "family_name_ja", "given_name_ja", "description_raw"):
        assert col in col_names, f"Missing column {col!r} in persons table"
