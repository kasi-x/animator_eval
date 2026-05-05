"""Fetch AniList MediaTrend (popularity / score history) for each anime.

AniList returns per-day datapoints of: trending score, popularity rank,
averageScore, in-progress count, releasing flag, episode. Display-only data
per H1 (no scoring use), but valuable for temporal popularity / soft-power
analysis.

Target: every anime in `conformed.anime` with `id LIKE 'anilist:%'`. The
anilist_id integer is extracted from the suffix.

Cost: each anime requires `ceil(n_datapoints / 50)` paginated requests.
Long-running anime (Naruto, One Piece) may have hundreds of pages. Median
anime ~1-3 pages. Estimate ~3-5 requests per anime average → 60-100K
requests total at 30 req/min (~33-55 hours).

Resume: checkpoint records (mediaId, last_fetched_page) so reruns continue
from the next un-fetched page. Per-anime atomicity: a partially-fetched anime
is re-fetched from scratch on the next run.

Usage:
    pixi run python scripts/maintenance/backfill_anilist_media_trends.py --dry-run
    pixi run python scripts/maintenance/backfill_anilist_media_trends.py --limit 10
    pixi run python scripts/maintenance/backfill_anilist_media_trends.py --resume
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

from src.scrapers.anilist_scraper import AniListClient
from src.scrapers.bronze_writer import BronzeWriter
from src.scrapers.queries.anilist import MEDIA_TREND_QUERY

log = structlog.get_logger()

DEFAULT_DB_PATH = Path("result/animetor.duckdb")
CHECKPOINT_PATH = Path("data/anilist/media_trends_checkpoint.json")
DEFAULT_PER_PAGE = 50  # AniList max
DEFAULT_BATCH_SIZE = 100  # Trend rows per BRONZE flush


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    p.add_argument("--limit", type=int, default=0, help="Max anime to process (0=all)")
    p.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument("--top-popular", type=int, default=0,
                   help="Limit to top-N anime by display_popularity (recommended for "
                        "feasible scrape). 0 = all anime (~1000 hours).")
    p.add_argument("--since-year", type=int, default=0,
                   help="Limit to anime with year >= N. 0 = no filter.")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--reset-checkpoint", action="store_true")
    return p.parse_args()


def _load_checkpoint() -> set[int]:
    if not CHECKPOINT_PATH.exists():
        return set()
    try:
        return set(int(x) for x in json.loads(CHECKPOINT_PATH.read_text()).get("processed_media_ids", []))
    except Exception:
        return set()


def _save_checkpoint(processed: set[int]) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps({"processed_media_ids": sorted(processed)}))
    tmp.replace(CHECKPOINT_PATH)


def _query_anilist_media_ids(
    db_path: Path,
    limit: int,
    top_popular: int = 0,
    since_year: int = 0,
) -> list[int]:
    """Pick anime to fetch trends for.

    Order priority: --top-popular (high popularity first) > --since-year > all.
    The popularity column used is display_popularity_anilist (H1-prefixed,
    display only; not used for scoring).
    """
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        where = ["id LIKE 'anilist:%'"]
        if since_year > 0:
            where.append(f"year >= {since_year}")
        if top_popular > 0:
            sql = f"""
                SELECT CAST(SUBSTRING(id, LENGTH('anilist:') + 1) AS INTEGER) AS mid
                FROM conformed.anime
                WHERE {' AND '.join(where)}
                ORDER BY COALESCE(display_favourites, 0) DESC NULLS LAST
                LIMIT {top_popular}
            """
        else:
            sql = f"""
                SELECT CAST(SUBSTRING(id, LENGTH('anilist:') + 1) AS INTEGER) AS mid
                FROM conformed.anime
                WHERE {' AND '.join(where)}
                ORDER BY mid
            """
            if limit > 0:
                sql += f" LIMIT {limit}"
        rows = conn.execute(sql).fetchall()
        return [int(r[0]) for r in rows if r[0] is not None]
    finally:
        conn.close()


async def _fetch_trends_for_media(client, media_id: int, per_page: int) -> list[dict]:
    """Fetch all MediaTrend pages for a single media. Returns list of trend dicts."""
    out: list[dict] = []
    page = 1
    while True:
        try:
            resp = await client.query(
                MEDIA_TREND_QUERY,
                {"mediaId": media_id, "page": page, "perPage": per_page},
            )
        except Exception as exc:
            log.warning("trend_page_failed", media_id=media_id, page=page, error=str(exc))
            break
        page_data = (resp or {}).get("Page") or {}
        trends = page_data.get("mediaTrends") or []
        out.extend(trends)
        page_info = page_data.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        page += 1
    return out


async def _run(args: argparse.Namespace) -> None:
    if args.reset_checkpoint and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()

    media_ids = _query_anilist_media_ids(
        args.db_path,
        args.limit,
        top_popular=args.top_popular,
        since_year=args.since_year,
    )
    print(f"Target anime: {len(media_ids):,}")
    if args.top_popular:
        print(f"  filter: top {args.top_popular} by popularity")
    if args.since_year:
        print(f"  filter: year >= {args.since_year}")

    processed = _load_checkpoint() if args.resume else set()
    if args.resume and processed:
        before = len(media_ids)
        media_ids = [m for m in media_ids if m not in processed]
        print(f"Resume: skipping {before - len(media_ids):,}; remaining {len(media_ids):,}")

    if args.dry_run:
        print(f"DRY RUN. First 5 media_ids: {media_ids[:5]}")
        return

    today = _dt.date.today()
    rows_total = 0
    media_done = 0
    errors = 0
    buffer: list[dict] = []

    client = AniListClient()
    try:
        with BronzeWriter("anilist", table="media_trends", date=today) as bw:
            for i, mid in enumerate(media_ids, 1):
                try:
                    trends = await _fetch_trends_for_media(client, mid, args.per_page)
                    for t in trends:
                        buffer.append({
                            "media_id": t.get("mediaId"),
                            # Note: API field is `date` but renamed to avoid
                            # collision with hive partition `date=YYYY-MM-DD`.
                            "trend_date": t.get("date"),
                            "trending": t.get("trending"),
                            "average_score": t.get("averageScore"),
                            "popularity": t.get("popularity"),
                            "in_progress": t.get("inProgress"),
                            "releasing": t.get("releasing"),
                            "episode": t.get("episode"),
                        })
                    media_done += 1
                    rows_total += len(trends)
                except Exception as exc:
                    errors += 1
                    log.warning("trend_media_failed", media_id=mid, error=str(exc))

                processed.add(mid)

                if len(buffer) >= args.batch_size:
                    for r in buffer:
                        bw.append(r)
                    bw.flush()
                    buffer.clear()
                    _save_checkpoint(processed)
                    print(f"[{i}/{len(media_ids)}] flushed — media_done={media_done:,} rows={rows_total:,} errors={errors:,}")

            if buffer:
                for r in buffer:
                    bw.append(r)
                bw.flush()
            _save_checkpoint(processed)
    finally:
        await client.close()

    print(f"\nDone. media_done={media_done:,} rows={rows_total:,} errors={errors:,}\n"
          f"Next: pixi run python -m src.etl.integrate_duckdb")


def main() -> None:
    asyncio.run(_run(_parse_args()))


if __name__ == "__main__":
    main()
