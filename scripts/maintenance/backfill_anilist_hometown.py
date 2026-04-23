"""Re-fetch AniList person data for persons with NULL hometown.

Fixes Korean/Chinese names that were incorrectly routed to name_ja because
hometown was unavailable at the time of the original scrape.

Queries both src_anilist_persons and persons tables, re-fetches via AniList
PERSON_DETAILS_QUERY, updates hometown + re-routes name_ja/name_ko/name_zh.

Usage:
  pixi run python scripts/maintenance/backfill_anilist_hometown.py
  pixi run python scripts/maintenance/backfill_anilist_hometown.py --limit 100
  pixi run python scripts/maintenance/backfill_anilist_hometown.py --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

import httpx

from src.db import get_connection, init_db
from src.scrapers.queries.anilist import PERSON_DETAILS_QUERY
from src.utils.name_utils import parse_anilist_native_name

ANILIST_URL = "https://graphql.anilist.co"
_REQUEST_DELAY = 0.7  # seconds between requests (unauthenticated: 30 req/min)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--limit", type=int, default=0, help="Max persons to process (0=all)")
    p.add_argument("--dry-run", action="store_true", help="Fetch but do not write to DB")
    return p.parse_args()


def _query_candidates(conn, limit: int) -> list[tuple[int, str]]:
    """Return (anilist_id, person_id) pairs where hometown IS NULL."""
    sql = """
        SELECT p.anilist_id, p.id
        FROM persons p
        WHERE p.anilist_id IS NOT NULL
          AND p.hometown IS NULL
        ORDER BY p.anilist_id
    """
    if limit:
        sql += f" LIMIT {limit}"
    rows = conn.execute(sql).fetchall()
    return [(r[0], r[1]) for r in rows]


async def _fetch_person(client: httpx.AsyncClient, anilist_id: int) -> dict | None:
    try:
        resp = await client.post(
            ANILIST_URL,
            json={"query": PERSON_DETAILS_QUERY, "variables": {"id": anilist_id}},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        data = resp.json()
        return (data.get("data") or {}).get("Staff")
    except Exception as exc:
        print(f"  fetch error anilist_id={anilist_id}: {exc}", file=sys.stderr)
        return None


def _update_person(conn, person_id: str, anilist_id: int, staff: dict) -> dict:
    name = staff.get("name") or {}
    hometown_val = staff.get("homeTown")
    name_ja, name_ko, name_zh, names_alt_json, native, nationality = parse_anilist_native_name(
        name, hometown_val
    )
    conn.execute(
        """UPDATE persons SET
               hometown  = COALESCE(?, hometown),
               name_ja   = CASE WHEN ? != '' THEN ? ELSE name_ja END,
               name_ko   = COALESCE(NULLIF(?, ''), name_ko),
               name_zh   = COALESCE(NULLIF(?, ''), name_zh),
               names_alt = CASE WHEN ? != '{}' THEN ? ELSE names_alt END,
               nationality = CASE WHEN ? != '[]' THEN ? ELSE nationality END,
               updated_at = CURRENT_TIMESTAMP
           WHERE id = ?""",
        (
            hometown_val,
            name_ja, name_ja,
            name_ko,
            name_zh,
            names_alt_json, names_alt_json,
            json.dumps(nationality, ensure_ascii=False),
            json.dumps(nationality, ensure_ascii=False),
            person_id,
        ),
    )
    conn.execute(
        """UPDATE src_anilist_persons SET
               hometown  = COALESCE(?, hometown),
               name_ja   = CASE WHEN ? != '' THEN ? ELSE name_ja END,
               name_ko   = COALESCE(NULLIF(?, ''), name_ko),
               name_zh   = COALESCE(NULLIF(?, ''), name_zh),
               names_alt = CASE WHEN ? != '{}' THEN ? ELSE names_alt END,
               nationality = CASE WHEN ? != '[]' THEN ? ELSE nationality END
           WHERE anilist_id = ?""",
        (
            hometown_val,
            name_ja, name_ja,
            name_ko,
            name_zh,
            names_alt_json, names_alt_json,
            json.dumps(nationality, ensure_ascii=False),
            json.dumps(nationality, ensure_ascii=False),
            anilist_id,
        ),
    )
    return {"hometown": hometown_val, "name_ja": name_ja, "name_ko": name_ko, "name_zh": name_zh}


async def _run(args: argparse.Namespace) -> None:
    conn = get_connection()
    init_db(conn)
    candidates = _query_candidates(conn, args.limit)
    print(f"Candidates (hometown IS NULL, anilist_id set): {len(candidates)}")
    if not candidates:
        print("Nothing to backfill.")
        return

    updated = 0
    rerouted = 0
    errors = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i, (anilist_id, person_id) in enumerate(candidates, 1):
            print(f"[{i}/{len(candidates)}] anilist_id={anilist_id} person_id={person_id}", end=" ")
            staff = await _fetch_person(client, anilist_id)
            if staff is None:
                print("SKIP (fetch error)")
                errors += 1
                await asyncio.sleep(_REQUEST_DELAY)
                continue

            if not args.dry_run:
                result = _update_person(conn, person_id, anilist_id, staff)
                conn.commit()
                updated += 1
                rerouted_flag = bool(result["name_ko"] or result["name_zh"])
                if rerouted_flag:
                    rerouted += 1
                print(f"hometown={result['hometown']!r} ja={result['name_ja']!r} ko={result['name_ko']!r} zh={result['name_zh']!r}")
            else:
                name = staff.get("name") or {}
                hometown_val = staff.get("homeTown")
                name_ja, name_ko, name_zh, *_ = parse_anilist_native_name(name, hometown_val)
                print(f"[DRY] hometown={hometown_val!r} ja={name_ja!r} ko={name_ko!r} zh={name_zh!r}")

            await asyncio.sleep(_REQUEST_DELAY)

    print(f"\nDone. updated={updated} rerouted={rerouted} errors={errors}")


def main() -> None:
    args = _parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
