"""Tests for src.scrapers.sinks.BronzeSink."""
from __future__ import annotations

import dataclasses


from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.sinks import BronzeSink


@dataclasses.dataclass
class _Anime:
    anime_id: int
    title: str
    credits: list[dict] = dataclasses.field(default_factory=list)


def _mapper(rec: _Anime) -> dict[str, list[dict]]:
    row = dataclasses.asdict(rec)
    credits = row.pop("credits")
    return {
        "anime": [row],
        "credits": [{**c, "anime_id": rec.anime_id} for c in credits],
    }


def test_sink_writes_primary_and_secondary_tables(tmp_path):
    rec = _Anime(anime_id=1, title="A", credits=[{"name": "X"}, {"name": "Y"}])
    with BronzeWriterGroup("allcinema", tables=["anime", "credits"], root=tmp_path) as g:
        sink = BronzeSink(g, _mapper, add_hash=True)
        n = sink(rec)
    assert n == 3  # 1 anime + 2 credits


def test_sink_injects_hash_and_fetched_at(tmp_path):
    """Sink stamps fetched_at + content_hash onto the first table's first row."""
    import pyarrow.dataset as ds

    rec = _Anime(anime_id=2, title="B")
    with BronzeWriterGroup("allcinema", tables=["anime", "credits"], root=tmp_path) as g:
        sink = BronzeSink(g, _mapper, add_hash=True)
        sink(rec)

    anime_path = tmp_path / "source=allcinema" / "table=anime"
    tbl = ds.dataset(anime_path, format="parquet").to_table()
    cols = tbl.column_names
    assert "fetched_at" in cols
    assert "content_hash" in cols
    assert len(tbl.column("content_hash")[0].as_py()) == 64  # SHA-256 hex


def test_sink_no_hash_when_disabled(tmp_path):
    captured: list[dict] = []

    def _mapper_no_hash(rec: _Anime) -> dict[str, list[dict]]:
        row = dataclasses.asdict(rec)
        row.pop("credits")
        captured.append(row)
        return {"anime": [row]}

    rec = _Anime(anime_id=3, title="C")
    with BronzeWriterGroup("allcinema", tables=["anime"], root=tmp_path) as g:
        sink = BronzeSink(g, _mapper_no_hash, add_hash=False)
        sink(rec)

    assert "fetched_at" not in captured[0]
    assert "content_hash" not in captured[0]


def test_sink_returns_total_row_count(tmp_path):
    rec = _Anime(anime_id=4, title="D", credits=[{"name": "Z"}])
    with BronzeWriterGroup("allcinema", tables=["anime", "credits"], root=tmp_path) as g:
        sink = BronzeSink(g, _mapper, add_hash=False)
        assert sink(rec) == 2  # 1 anime + 1 credit
