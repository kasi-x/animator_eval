"""Repair anilist credits/characters lost by the rebuild's 2-pass page cap.

The rebuild covers staff p1-10 + char p1-7. Mega-shows (1000+ staff) lose
records past those pages. This script identifies those anime via v1 vs new
distinct (anime_id, person_id, role) diff, then re-fetches deep pages
(staff p11+ / char p8+) recursively until hasNextPage=False.

Run after the main rebuild has completed and v1 is still available.
"""

import asyncio
import sys
from pathlib import Path

import duckdb
import structlog

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.scrapers.anilist_scraper import (  # noqa: E402
    AniListClient,
    save_characters_to_bronze,
    save_credits_batch_to_bronze,
    save_cva_to_bronze,
)


def _collect_pages_from_range(
    media: dict, prefix: str, start: int, count: int
) -> tuple[list[dict], bool]:
    """Collect alias pages prefix_p{start..start+count-1} from a deep-pages
    response. Returns (all_edges, has_more) where has_more reflects the
    deepest page's hasNextPage.
    """
    edges: list[dict] = []
    has_more = False
    for page in range(start, start + count):
        block = media.get(f"{prefix}_p{page}")
        if not block:
            continue
        edges.extend(block.get("edges") or [])
        has_more = bool((block.get("pageInfo") or {}).get("hasNextPage"))
    return edges, has_more
from src.scrapers.bronze_writer import BronzeWriterGroup  # noqa: E402
from src.scrapers.parsers.anilist import (  # noqa: E402
    parse_anilist_characters,
    parse_anilist_staff,
    parse_anilist_voice_actors,
)

log = structlog.get_logger()

THRESHOLD = 100              # diff >= N credits per anime → repair
# AniList caps query complexity at 500 per request. Each alias page costs
# ~30-40 complexity points, so keep total alias count conservative.
STAFF_PAGES_PER_REQ = 8      # alias staff pages bundled per request
CHAR_PAGES_PER_REQ = 4       # alias char pages bundled per request
STAFF_RESUME_PAGE = 11       # rebuild covered staff p1-10
CHAR_RESUME_PAGE = 8         # rebuild covered char p1-7

V1 = "result/bronze/source=anilist_v1"
NEW = "result/bronze/source=anilist"


async def fetch_remaining_pages(client, anilist_id: int):
    """Pull staff p>=STAFF_RESUME_PAGE and char p>=CHAR_RESUME_PAGE
    until hasNextPage=False. Returns aggregated edges.
    """
    all_staff: list[dict] = []
    all_char: list[dict] = []
    staff_page = STAFF_RESUME_PAGE
    char_page = CHAR_RESUME_PAGE
    staff_more = True
    char_more = True

    while staff_more or char_more:
        sp_count = STAFF_PAGES_PER_REQ if staff_more else 0
        cp_count = CHAR_PAGES_PER_REQ if char_more else 0
        if sp_count + cp_count == 0:
            break
        try:
            resp = await client.get_anime_deep_pages(
                anilist_id,
                staff_start=staff_page,
                staff_count=sp_count,
                char_start=char_page,
                char_count=cp_count,
            )
        except Exception as e:
            log.warning(
                "repair_deep_fetch_error",
                anilist_id=anilist_id,
                staff_page=staff_page,
                char_page=char_page,
                error=str(e),
            )
            break
        media = (resp or {}).get("Media") or {}
        if not media:
            break
        if sp_count:
            edges, more = _collect_pages_from_range(
                media, "staff", staff_page, sp_count
            )
            all_staff.extend(edges)
            staff_more = more
            staff_page += sp_count
        if cp_count:
            edges, more = _collect_pages_from_range(
                media, "characters", char_page, cp_count
            )
            all_char.extend(edges)
            char_more = more
            char_page += cp_count
    return all_staff, all_char


def collect_targets() -> list[tuple[str, int]]:
    """Return (anime_id, anilist_id) pairs whose credit count diff >= threshold."""
    c = duckdb.connect()
    diff_rows = c.execute(
        f"""
        WITH v AS (
            SELECT anime_id, COUNT(DISTINCT (person_id, role)) AS cnt
            FROM read_parquet('{V1}/table=credits/**/*.parquet') GROUP BY anime_id
        ),
        n AS (
            SELECT anime_id, COUNT(DISTINCT (person_id, role)) AS cnt
            FROM read_parquet('{NEW}/table=credits/**/*.parquet') GROUP BY anime_id
        )
        SELECT v.anime_id, v.cnt - COALESCE(n.cnt, 0) AS diff
        FROM v LEFT JOIN n USING (anime_id)
        WHERE v.cnt - COALESCE(n.cnt, 0) >= {THRESHOLD}
        ORDER BY diff DESC
        """
    ).fetchall()
    if not diff_rows:
        return []
    placeholders = ",".join(repr(r[0]) for r in diff_rows)
    pairs = c.execute(
        f"""
        SELECT id, anilist_id FROM read_parquet('{NEW}/table=anime/**/*.parquet')
        WHERE id IN ({placeholders}) AND anilist_id IS NOT NULL
        """
    ).fetchall()
    diff_map = dict(diff_rows)
    return sorted(
        [(str(r[0]), int(r[1])) for r in pairs],
        key=lambda x: -diff_map.get(x[0], 0),
    )


async def main():
    targets = collect_targets()
    print(f"threshold:    diff >= {THRESHOLD}")
    print(f"target anime: {len(targets)}")
    if not targets:
        print("nothing to repair")
        return

    client = AniListClient()
    await client.verify_auth()

    seen_person_ids: set = set()
    repaired = 0
    failed = 0
    new_credits = 0
    new_chars = 0
    new_cvas = 0

    with BronzeWriterGroup(
        "anilist",
        tables=["credits", "characters", "character_voice_actors", "persons"],
    ) as g:
        for idx, (anime_id, anilist_id) in enumerate(targets, 1):
            try:
                staff_edges, char_edges = await fetch_remaining_pages(
                    client, anilist_id
                )
                persons_p, credits_p = parse_anilist_staff(staff_edges, anime_id)
                chars_p, cvas_p = parse_anilist_characters(char_edges, anime_id)
                va_persons_p, va_credits_p = parse_anilist_voice_actors(
                    char_edges, anime_id
                )

                save_credits_batch_to_bronze(g["credits"], credits_p)
                save_credits_batch_to_bronze(g["credits"], va_credits_p)
                save_characters_to_bronze(g["characters"], chars_p)
                save_cva_to_bronze(g["character_voice_actors"], cvas_p)
                for p in persons_p + va_persons_p:
                    if p.id in seen_person_ids:
                        continue
                    seen_person_ids.add(p.id)
                    g["persons"].append(p.model_dump(mode="json"))

                repaired += 1
                new_credits += len(credits_p) + len(va_credits_p)
                new_chars += len(chars_p)
                new_cvas += len(cvas_p)

                if idx % 25 == 0 or idx == len(targets):
                    print(
                        f"  [{idx}/{len(targets)}] repaired={repaired} "
                        f"failed={failed} +credits={new_credits} +chars={new_chars}"
                    )
                    g.flush_all()
            except Exception as e:
                failed += 1
                log.error(
                    "repair_failed",
                    anime_id=anime_id,
                    anilist_id=anilist_id,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )

    await client.close()
    print(
        f"done: repaired={repaired}, failed={failed}, "
        f"+credits={new_credits}, +characters={new_chars}, "
        f"+cvas={new_cvas}, +persons={len(seen_person_ids)}"
    )


if __name__ == "__main__":
    asyncio.run(main())
