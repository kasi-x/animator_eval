"""Append-only Parquet writer for BRONZE layer (scraper output).

Each scraper instantiates BronzeWriter(source) and calls .append(row)
during scraping. .flush() (or context manager exit) writes a single
parquet file under bronze/source={src}/table={tbl}/date={YYYYMMDD}/{uuid}.parquet.

Files are immutable once written. Re-running a scraper produces a new
file under the same partition. integrate ETL (04_duckdb/03) reads the
glob and dedups in SILVER.
"""
from __future__ import annotations

import datetime as _dt
import os
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

logger = structlog.get_logger()

DEFAULT_BRONZE_ROOT: Path = Path(
    os.environ.get(
        "ANIMETOR_BRONZE_ROOT",
        str(Path(__file__).resolve().parent.parent.parent / "result" / "bronze"),
    )
)

ALLOWED_SOURCES = {
    "anilist", "ann", "allcinema", "seesaawiki", "keyframe", "mal", "mediaarts", "jvmg",
}


class BronzeWriter:
    """Append-only parquet writer scoped to one (source, table, date) partition.

    Usage:
        with BronzeWriter("anilist", table="anime") as bw:
            for row in scraped_rows:
                bw.append(row)
        # parquet file flushed on exit

    Multiple scrapers can run in parallel — each writes its own file
    under its own partition, no contention.
    """

    def __init__(
        self,
        source: str,
        *,
        table: str,
        root: Path | str | None = None,
        date: _dt.date | None = None,
    ) -> None:
        if source not in ALLOWED_SOURCES:
            raise ValueError(f"Unknown source: {source!r} (allowed: {ALLOWED_SOURCES})")
        self.source = source
        self.table = table
        self._root = Path(root or DEFAULT_BRONZE_ROOT)
        self._date = date or _dt.date.today()
        self._buffer: list[dict[str, Any]] = []
        self._partition: Path = (
            self._root
            / f"source={source}"
            / f"table={table}"
            / f"date={self._date.isoformat()}"
        )

    def __enter__(self) -> "BronzeWriter":
        return self

    def __exit__(self, exc_type, *_: object) -> None:
        if exc_type is None:
            self.flush()
        # On exception, drop buffer — caller will retry the whole scrape

    def append(self, row: dict[str, Any]) -> None:
        self._buffer.append(row)

    def extend(self, rows: list[dict[str, Any]]) -> None:
        self._buffer.extend(rows)

    def flush(self) -> Path | None:
        """Write buffered rows to a new parquet file. No-op if buffer empty."""
        if not self._buffer:
            return None
        self._partition.mkdir(parents=True, exist_ok=True)
        path = self._partition / f"{uuid.uuid4().hex}.parquet"
        table = pa.Table.from_pylist(self._buffer)
        pq.write_table(table, path, compression="zstd")
        logger.info(
            "bronze_parquet_written",
            source=self.source,
            table=self.table,
            rows=len(self._buffer),
            path=str(path),
        )
        self._buffer.clear()
        return path
