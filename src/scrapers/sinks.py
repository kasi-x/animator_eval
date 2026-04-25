"""Bronze sink helpers for ScrapeRunner.

A BronzeSink is a callable: Rec → int (rows written).
It maps a parsed Record into one or more BRONZE table rows
via a user-supplied mapper, then appends each row to the
corresponding BronzeWriter in a BronzeWriterGroup.

Usage::

    from src.scrapers.sinks import BronzeSink
    from src.scrapers.bronze_writer import BronzeWriterGroup
    import dataclasses

    with BronzeWriterGroup("allcinema", tables=["anime", "credits"]) as g:
        sink = BronzeSink(
            group=g,
            mapper=lambda rec: {
                "anime": [dataclasses.asdict(rec)],
                "credits": [dataclasses.asdict(c) for c in rec.credits],
            },
        )
        # pass sink to ScrapeRunner
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Callable
from typing import Generic, TypeVar

from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.hash_utils import hash_anime_data

Rec = TypeVar("Rec")


class BronzeSink(Generic[Rec]):
    """Callable sink: Record → BRONZE rows.

    Args:
        group:      BronzeWriterGroup already opened by the caller.
                    The sink does NOT manage the group lifecycle.
        mapper:     Pure function Rec → dict[table_name, list[row_dict]].
                    Keys must match the tables in ``group``.
        add_hash:   When True, add ``fetched_at`` and ``content_hash``
                    to the first table's rows (typically the anime table).
                    Uses hash_anime_data() which excludes fetched_at/content_hash.

    Returns (when called):
        Total rows written across all tables.
    """

    def __init__(
        self,
        group: BronzeWriterGroup,
        mapper: Callable[[Rec], dict[str, list[dict]]],
        *,
        add_hash: bool = True,
    ) -> None:
        self._group = group
        self._mapper = mapper
        self._add_hash = add_hash

    def __call__(self, rec: Rec) -> int:
        """Map record to rows, optionally stamp hash, append to writers."""
        table_rows = self._mapper(rec)
        total = 0
        first = True
        for table_name, rows in table_rows.items():
            writer = self._group[table_name]
            for row in rows:
                if first and self._add_hash:
                    row = self._stamp_hash(row)
                    first = False
                writer.append(row)
                total += 1
            if rows:
                first = False  # only stamp the very first row
        return total

    @staticmethod
    def _stamp_hash(row: dict) -> dict:
        """Return copy of row with fetched_at and content_hash added."""
        row = dict(row)
        row["fetched_at"] = _dt.datetime.now(_dt.timezone.utc).isoformat()
        row["content_hash"] = hash_anime_data(row)
        return row
