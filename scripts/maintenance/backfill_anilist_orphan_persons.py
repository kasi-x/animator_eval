"""Backfill AniList orphan persons (id-only rows from credits).

Card 14/13 follow-up: 481K orphan credits were resolved by inserting id-only
person rows (`name_en = ''`). This script re-fetches each orphan via the
AniList Staff GraphQL endpoint and appends a fully-populated row to the
BRONZE persons parquet. Re-running `pixi run python -m src.etl.integrate_duckdb`
afterwards propagates the new fields (gender / hometown / birthday / image /
description) into the Conformed `persons` table.

Target population (default): persons where the row was inserted as id-only by
the credits fallback in `src/etl/conformed_loaders/anilist.py`.

    SELECT COUNT(*) FROM conformed.persons
    WHERE id LIKE 'anilist:p%'
      AND (name_en IS NULL OR TRIM(name_en) = '')
      AND (name_ja IS NULL OR TRIM(name_ja) = '')

Currently ~90,068 candidates (97,596 total anilist:p rows minus 7,528 with names).

Usage:
    pixi run python scripts/maintenance/backfill_anilist_orphan_persons.py --dry-run
    pixi run python scripts/maintenance/backfill_anilist_orphan_persons.py --limit 100
    pixi run python scripts/maintenance/backfill_anilist_orphan_persons.py
    pixi run python scripts/maintenance/backfill_anilist_orphan_persons.py --batch-size 25 --resume

Throttling: AniList unauthenticated rate limit is 30 req/min (~2 s/req). At
that rate, 90K orphans take ~50 hours. The shared `AniListClient` already
honours the X-RateLimit headers and back-off; no additional sleep is added.

Resume: writes a checkpoint to `data/anilist/orphan_backfill_checkpoint.json`
after every successful batch flush. Re-running with --resume picks up where
it left off (safe to interrupt at any time).
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb
import structlog

from src.scrapers.anilist_scraper import (
    AniListClient,
    save_persons_batch_to_bronze,
)
from src.scrapers.bronze_writer import BronzeWriter
from src.scrapers.parsers.anilist import parse_anilist_person

log = structlog.get_logger()

DEFAULT_DB_PATH = Path("result/animetor.duckdb")
CHECKPOINT_PATH = Path("data/anilist/orphan_backfill_checkpoint.json")
DEFAULT_BATCH_SIZE = 25  # persons per BRONZE flush


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Conformed DB path (default: {DEFAULT_DB_PATH})",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max persons to fetch (0 = all candidates)",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Persons per BRONZE flush + checkpoint (default: {DEFAULT_BATCH_SIZE})",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Identify candidates and print sample, but make no API calls",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Skip staff IDs already recorded in the checkpoint",
    )
    p.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Delete the checkpoint file before running (forces full restart)",
    )
    return p.parse_args()


def _load_checkpoint() -> set[int]:
    if not CHECKPOINT_PATH.exists():
        return set()
    try:
        data = json.loads(CHECKPOINT_PATH.read_text())
        return set(int(x) for x in data.get("processed_ids", []))
    except Exception:
        log.warning("checkpoint_load_failed", path=str(CHECKPOINT_PATH))
        return set()


def _save_checkpoint(processed: set[int]) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps({"processed_ids": sorted(processed)}))
    tmp.replace(CHECKPOINT_PATH)


def _query_orphan_staff_ids(db_path: Path, limit: int) -> list[int]:
    """Return AniList staff IDs (int) for orphan persons.

    Orphan = anilist:p<id> row with empty name_en AND empty name_ja
    (= id-only insert from `_PERSONS_FROM_CREDITS_SQL`).
    """
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        sql = """
            SELECT CAST(SUBSTRING(id, LENGTH('anilist:p') + 1) AS INTEGER) AS staff_id
            FROM conformed.persons
            WHERE id LIKE 'anilist:p%'
              AND (name_en IS NULL OR TRIM(name_en) = '')
              AND (name_ja IS NULL OR TRIM(name_ja) = '')
            ORDER BY staff_id
        """
        if limit > 0:
            sql += f" LIMIT {limit}"
        rows = conn.execute(sql).fetchall()
        return [int(r[0]) for r in rows if r[0] is not None]
    finally:
        conn.close()


async def _run(args: argparse.Namespace) -> None:
    if args.reset_checkpoint and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()
        log.info("checkpoint_reset")

    candidates = _query_orphan_staff_ids(args.db_path, args.limit)
    print(f"Orphan AniList persons (no name): {len(candidates):,}")
    if not candidates:
        print("Nothing to backfill.")
        return

    processed: set[int] = _load_checkpoint() if args.resume else set()
    if args.resume and processed:
        before = len(candidates)
        candidates = [sid for sid in candidates if sid not in processed]
        print(
            f"Resume: skipping {before - len(candidates):,} already-processed; "
            f"remaining {len(candidates):,}"
        )

    if args.dry_run:
        sample = candidates[:5]
        print(f"DRY RUN. First 5 staff_ids to fetch: {sample}")
        return

    today = _dt.date.today()
    fetched = 0
    skipped = 0
    errors = 0
    person_batch = []

    client = AniListClient()
    try:
        with BronzeWriter("anilist", table="persons", date=today) as persons_bw:
            for i, staff_id in enumerate(candidates, 1):
                try:
                    resp = await client.get_person_details(staff_id)
                    staff = (resp or {}).get("Staff")
                    if staff is None:
                        skipped += 1
                        log.info("orphan_skipped_not_found", staff_id=staff_id)
                    else:
                        person = parse_anilist_person(staff)
                        person_batch.append(person)
                        fetched += 1
                except Exception as exc:
                    errors += 1
                    log.warning(
                        "orphan_fetch_failed",
                        staff_id=staff_id,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )

                processed.add(staff_id)

                # Flush BRONZE every batch_size successful fetches.
                if len(person_batch) >= args.batch_size:
                    save_persons_batch_to_bronze(persons_bw, person_batch)
                    persons_bw.flush()
                    person_batch.clear()

                # Persist checkpoint every batch_size *attempts* (incl. 404 skip)
                # so deleted-ID skips do not get re-tried on resume.
                if i % args.batch_size == 0:
                    _save_checkpoint(processed)
                    print(
                        f"[{i}/{len(candidates)}] checkpoint — "
                        f"fetched={fetched:,} skipped={skipped:,} errors={errors:,}"
                    )

            if person_batch:
                save_persons_batch_to_bronze(persons_bw, person_batch)
                persons_bw.flush()
                _save_checkpoint(processed)
    finally:
        await client.close()

    print(
        f"\nDone. fetched={fetched:,} skipped={skipped:,} errors={errors:,}\n"
        f"Next: pixi run python -m src.etl.integrate_duckdb"
    )


def main() -> None:
    args = _parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
