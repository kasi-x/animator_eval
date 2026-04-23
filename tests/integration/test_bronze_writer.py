"""Tests for BronzeWriter (parquet append-only writer)."""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.scrapers.bronze_writer import ALLOWED_SOURCES, BronzeWriter


def test_unknown_source_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unknown source"):
        BronzeWriter("nonexistent", table="anime", root=tmp_path)


def test_partition_layout(tmp_path: Path) -> None:
    bw = BronzeWriter(
        "anilist", table="anime", root=tmp_path, date=_dt.date(2026, 4, 23)
    )
    bw.append({"id": "ani:1", "title": "X", "score": 8.5})
    out = bw.flush()
    assert out is not None
    assert out.parent == (
        tmp_path / "source=anilist" / "table=anime" / "date=2026-04-23"
    )
    assert out.suffix == ".parquet"


def test_roundtrip_single_file(tmp_path: Path) -> None:
    rows = [{"id": f"ani:{i}", "title": f"T{i}", "score": float(i)} for i in range(5)]
    with BronzeWriter("anilist", table="anime", root=tmp_path) as bw:
        bw.extend(rows)
    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) == 1
    table = pq.read_table(files[0])
    assert table.num_rows == 5
    # pyarrow 23+ may add hive partition columns; user columns must be present
    assert {"id", "title", "score"}.issubset(set(table.column_names))


def test_empty_buffer_flush_is_noop(tmp_path: Path) -> None:
    bw = BronzeWriter("ann", table="credits", root=tmp_path)
    assert bw.flush() is None
    assert list(tmp_path.rglob("*.parquet")) == []


def test_multiple_flushes_produce_multiple_files(tmp_path: Path) -> None:
    bw = BronzeWriter("mal", table="anime", root=tmp_path)
    bw.append({"id": "mal:1"})
    bw.flush()
    bw.append({"id": "mal:2"})
    bw.flush()
    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) == 2


def test_exception_drops_buffer(tmp_path: Path) -> None:
    """Exception inside context must NOT flush partial buffer."""
    with pytest.raises(RuntimeError):
        with BronzeWriter("anilist", table="anime", root=tmp_path) as bw:
            bw.append({"id": "ani:1"})
            raise RuntimeError("scraper crashed")
    assert list(tmp_path.rglob("*.parquet")) == []


def test_all_allowed_sources(tmp_path: Path) -> None:
    for src in ALLOWED_SOURCES:
        bw = BronzeWriter(src, table="anime", root=tmp_path)
        bw.append({"id": f"{src}:1"})
        bw.flush()
    assert len(list(tmp_path.rglob("*.parquet"))) == len(ALLOWED_SOURCES)
