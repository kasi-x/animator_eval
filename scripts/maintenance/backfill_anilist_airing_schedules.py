"""Fetch AniList global airing schedules (Page.airingSchedules, chronological).

Complements per-anime airingSchedule (already captured by ANIME_STAFF_QUERY) by
providing a cross-anime chronological view: "what aired on date X" rather than
"when does anime Y air."

Useful for:
- Cohort construction by broadcast date
- Cross-anime concurrency analysis (overlapping seasons)
- Filling per-episode dates for anime where the per-anime query returned only
  partial schedules

Pagination: Page.airingSchedules returns 50 per page max. Optional time-window
filters (`airingAt_greater` / `airingAt_lesser`) reduce volume; default fetches
the full timeline (~100K+ entries since 2014).

Usage:
    pixi run python scripts/maintenance/backfill_anilist_airing_schedules.py --dry-run
    pixi run python scripts/maintenance/backfill_anilist_airing_schedules.py --since 2024-01-01
    pixi run python scripts/maintenance/backfill_anilist_airing_schedules.py --resume
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import structlog

from src.scrapers.anilist_scraper import AniListClient
from src.scrapers.bronze_writer import BronzeWriter
from src.scrapers.queries.anilist import GLOBAL_AIRING_SCHEDULE_QUERY

log = structlog.get_logger()

CHECKPOINT_PATH = Path("data/anilist/airing_schedules_checkpoint.json")
DEFAULT_PER_PAGE = 50
DEFAULT_BATCH_SIZE = 200


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--since", type=str, default=None,
                   help="Start date YYYY-MM-DD (filter airingAt_greater)")
    p.add_argument("--until", type=str, default=None,
                   help="End date YYYY-MM-DD (filter airingAt_lesser)")
    p.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument("--max-pages", type=int, default=0, help="Stop after N pages (0=unbounded)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--reset-checkpoint", action="store_true")
    return p.parse_args()


def _load_checkpoint() -> int:
    if not CHECKPOINT_PATH.exists():
        return 1
    try:
        return int(json.loads(CHECKPOINT_PATH.read_text()).get("next_page", 1))
    except Exception:
        return 1


def _save_checkpoint(next_page: int) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps({"next_page": next_page}))
    tmp.replace(CHECKPOINT_PATH)


def _date_to_unix(s: str | None) -> int | None:
    if not s:
        return None
    return int(_dt.datetime.fromisoformat(s).replace(tzinfo=_dt.timezone.utc).timestamp())


async def _run(args: argparse.Namespace) -> None:
    if args.reset_checkpoint and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()

    start_page = _load_checkpoint() if args.resume else 1
    print(f"Starting at page {start_page}")

    if args.dry_run:
        print(f"DRY RUN. since={args.since!r} until={args.until!r} per_page={args.per_page}")
        return

    today = _dt.date.today()
    rows_total = 0
    page = start_page
    buffer: list[dict] = []
    variables: dict = {"perPage": args.per_page}
    if args.since:
        variables["airingAt_greater"] = _date_to_unix(args.since)
    if args.until:
        variables["airingAt_lesser"] = _date_to_unix(args.until)

    client = AniListClient()
    try:
        with BronzeWriter("anilist", table="airing_schedules", date=today) as bw:
            while True:
                if args.max_pages and (page - start_page) >= args.max_pages:
                    break
                try:
                    resp = await client.query(
                        GLOBAL_AIRING_SCHEDULE_QUERY,
                        {**variables, "page": page},
                    )
                except Exception as exc:
                    log.warning("airing_page_failed", page=page, error=str(exc))
                    break

                page_data = (resp or {}).get("Page") or {}
                schedules = page_data.get("airingSchedules") or []
                for s in schedules:
                    buffer.append({
                        "id": s.get("id"),
                        "media_id": s.get("mediaId"),
                        "airing_at": s.get("airingAt"),
                        "time_until_airing": s.get("timeUntilAiring"),
                        "episode": s.get("episode"),
                    })
                rows_total += len(schedules)

                page_info = page_data.get("pageInfo") or {}
                total = page_info.get("total")

                if len(buffer) >= args.batch_size:
                    for r in buffer:
                        bw.append(r)
                    bw.flush()
                    buffer.clear()
                    _save_checkpoint(page + 1)
                    print(f"page={page:>5} rows={rows_total:>7,} total≈{total or '?'}")

                if not page_info.get("hasNextPage"):
                    break
                page += 1

            if buffer:
                for r in buffer:
                    bw.append(r)
                bw.flush()
            _save_checkpoint(page + 1)
    finally:
        await client.close()

    print(f"\nDone. last_page={page} rows={rows_total:,}\n"
          f"Next: pixi run python -m src.etl.integrate_duckdb")


def main() -> None:
    asyncio.run(_run(_parse_args()))


if __name__ == "__main__":
    main()
