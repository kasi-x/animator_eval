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
    "sakuga_atwiki", "bangumi",
}


class BronzeWriter:
    """Append-only parquet writer scoped to one (source, table, date) partition.

    Usage:
        with BronzeWriter("anilist", table="anime") as bw:
            for row in scraped_rows:
                bw.append(row)
        # parquet flushed on exit, then small files in this partition are
        # auto-compacted into one file (compact_on_exit=True default).

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
        compact_on_exit: bool = True,
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
        self._compact_on_exit = compact_on_exit

    @property
    def date(self) -> _dt.date:
        return self._date

    @property
    def root(self) -> Path:
        return self._root

    def __enter__(self) -> "BronzeWriter":
        return self

    def __exit__(self, exc_type, *_: object) -> None:
        if exc_type is None:
            self.flush()
            if self._compact_on_exit:
                self.compact()
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

    def compact(self) -> Path | None:
        """Merge small parquet files in this writer's partition into one.

        Best-effort: compaction failure is logged but not raised.
        """
        # Local import to avoid circular import (bronze_compaction → bronze_writer).
        from src.scrapers.bronze_compaction import compact_partition
        try:
            return compact_partition(
                self.source, self.table, self._date.isoformat(), root=self._root
            )
        except Exception as exc:  # noqa: BLE001 — compaction must not crash scrapes
            logger.warning(
                "bronze_compaction_failed",
                source=self.source,
                table=self.table,
                date=self._date.isoformat(),
                error=str(exc),
            )
            return None


class BronzeWriterGroup:
    """Manage multiple BronzeWriter instances as one lifecycle unit.

    Usage:
        with BronzeWriterGroup("ann", tables=["anime", "credits", "cast"]) as g:
            g["anime"].append(...)
            g["credits"].append(...)
        # All writers flushed and their partitions compacted on exit.

    Sub-writers have compact_on_exit=False — the group performs one
    compaction pass at the end so that mid-scrape sub-writer .flush()
    calls don't trigger redundant compactions.
    """

    def __init__(
        self,
        source: str,
        *,
        tables: list[str],
        root: Path | str | None = None,
        date: _dt.date | None = None,
    ) -> None:
        self.source = source
        self._writers: dict[str, BronzeWriter] = {
            t: BronzeWriter(
                source, table=t, root=root, date=date, compact_on_exit=False
            )
            for t in tables
        }

    def __enter__(self) -> "BronzeWriterGroup":
        return self

    def __exit__(self, exc_type, *_: object) -> None:
        if exc_type is None:
            self.flush_all()
            self.compact_all()

    def __getitem__(self, table: str) -> BronzeWriter:
        return self._writers[table]

    def __iter__(self):
        return iter(self._writers.values())

    def writers(self) -> dict[str, BronzeWriter]:
        return self._writers

    def flush_all(self) -> None:
        for bw in self._writers.values():
            bw.flush()

    def compact_all(self) -> None:
        for bw in self._writers.values():
            bw.compact()


# ---------------------------------------------------------------------------
# 作画@wiki — 3-table BRONZE export
# ---------------------------------------------------------------------------

def write_sakuga_atwiki_bronze(
    persons: list,                        # list[ParsedSakugaPerson]
    pages_metadata: list[dict],
    output_dir: "Path | str",
    date_partition: str,
    raw_texts: "dict[int, str] | None" = None,
    works: "list | None" = None,          # list[ParsedSakugaWork]
) -> dict[str, Path]:
    """Write src_sakuga_atwiki_{pages,persons,credits,work_staff} parquet.

    pages_metadata: list of dicts from discovered_pages.json.
    persons:        ParsedSakugaPerson instances parsed from HTML cache.
    raw_texts:      {page_id: wikibody_plaintext} — stored in persons table so
                    rows where parse failed can be re-parsed without the gz cache.
                    Pass None to omit (raw HTML remains only in the gz cache).
    works:          ParsedSakugaWork instances parsed from work pages.
    """
    import json as _json

    output_dir = Path(output_dir)
    raw_texts = raw_texts or {}
    works = works or []

    page_rows: list[dict] = []
    person_rows: list[dict] = []
    credit_rows: list[dict] = []
    work_staff_rows: list[dict] = []

    person_map: dict[int, object] = {p.page_id: p for p in persons}  # type: ignore[union-attr]
    work_map: dict[int, object] = {w.page_id: w for w in works}  # type: ignore[union-attr]

    for meta in pages_metadata:
        pid = meta["id"]
        kind = meta.get("page_kind", "unknown")
        parsed_person = person_map.get(pid)
        parsed_work = work_map.get(pid)
        parse_ok = (
            (parsed_person is not None and len(parsed_person.credits) > 0)  # type: ignore[union-attr]
            or (parsed_work is not None and len(parsed_work.staff) > 0)  # type: ignore[union-attr]
        )

        page_rows.append({
            "page_id": pid,
            "url": meta.get("url", ""),
            "title": meta.get("title", ""),
            "page_kind": kind,
            "last_fetched_at": meta.get("discovered_at", ""),
            "html_sha256": meta.get("last_hash", ""),
            "parse_ok": parse_ok,
            "date_partition": date_partition,
        })

        if parsed_person is not None:
            person_rows.append({
                "page_id": pid,
                "name": parsed_person.name,  # type: ignore[union-attr]
                "aliases_json": _json.dumps(parsed_person.aliases, ensure_ascii=False),  # type: ignore[union-attr]
                "active_since_year": parsed_person.active_since_year,  # type: ignore[union-attr]
                "html_sha256": parsed_person.source_html_sha256,  # type: ignore[union-attr]
                "raw_wikibody_text": raw_texts.get(pid, ""),
                "parse_ok": len(parsed_person.credits) > 0,  # type: ignore[union-attr]
                "date_partition": date_partition,
            })
            for credit in parsed_person.credits:  # type: ignore[union-attr]
                credit_rows.append({
                    "person_page_id": pid,
                    "work_title": credit.work_title,
                    "work_year": credit.work_year,
                    "work_format": credit.work_format,
                    "role_raw": credit.role_raw,
                    "episode_raw": credit.episode_raw,
                    "episode_num": credit.episode_num,
                    "evidence_source": "sakuga_atwiki",
                    "date_partition": date_partition,
                })

        if parsed_work is not None:
            for s in parsed_work.staff:  # type: ignore[union-attr]
                work_staff_rows.append({
                    "work_page_id": pid,
                    "work_title": parsed_work.title,  # type: ignore[union-attr]
                    "work_year": parsed_work.year,  # type: ignore[union-attr]
                    "work_format": parsed_work.work_format,  # type: ignore[union-attr]
                    "person_name": s.person_name,
                    "role_raw": s.role_raw,
                    "episode_num": s.episode_num,
                    "episode_raw": s.episode_raw,
                    "is_main_staff": s.is_main_staff,
                    "evidence_source": "sakuga_atwiki",
                    "date_partition": date_partition,
                })

    written: dict[str, Path] = {}
    tables = [
        ("pages", page_rows),
        ("persons", person_rows),
        ("credits", credit_rows),
        ("work_staff", work_staff_rows),
    ]
    for table, rows in tables:
        with BronzeWriter("sakuga_atwiki", table=table, root=output_dir, date=None) as bw:
            bw._partition = (
                output_dir
                / "source=sakuga_atwiki"
                / f"table={table}"
                / f"date={date_partition}"
            )
            bw.extend(rows)
            p = bw.flush()
            if p:
                written[table] = p
                logger.info("sakuga_bronze_written", table=table, rows=len(rows), path=str(p))
            else:
                logger.warning("sakuga_bronze_empty", table=table)

    return written
