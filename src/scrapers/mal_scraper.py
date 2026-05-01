"""MyAnimeList credit data collection (via Jikan API v4).

3-Phase scraper:
  Phase A: anime + 13 sub-endpoints (~60h)
  Phase B: persons + characters (~33h)
  Phase C: producers + manga + masters (~90h)

Rate limit: 3 req/s + 60 req/min (DualWindowRateLimiter).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from datetime import date
from pathlib import Path

import structlog
import typer

from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.cache_store import load_cached_json, save_cached_json
from src.scrapers.checkpoint import Checkpoint
from src.scrapers.cli_common import (
    CheckpointIntervalOpt,
    DelayOpt,
    ForceOpt,
    LimitOpt,
    ProgressOpt,
    QuietOpt,
    ResumeOpt,
    make_scraper_app,
    resolve_progress_enabled,
)
from src.scrapers.http_client import DualWindowRateLimiter, RetryingHttpClient
from src.scrapers.parsers.mal import (
    parse_anime_characters_va,
    parse_anime_episodes,
    parse_anime_external,
    parse_anime_full,
    parse_anime_moreinfo,
    parse_anime_news,
    parse_anime_pictures,
    parse_anime_recommendations,
    parse_anime_staff_full,
    parse_anime_statistics,
    parse_anime_streaming,
    parse_anime_videos,
    parse_character_full,
    parse_character_pictures,
    parse_manga_full,
    parse_master_genres,
    parse_master_magazines,
    parse_person_full,
    parse_person_pictures,
    parse_producer_external,
    parse_producer_full,
    parse_schedules,
)
from src.scrapers.progress import scrape_progress

log = structlog.get_logger()

BASE_URL = "https://api.jikan.moe/v4"
CHECKPOINT_FILE = Path(__file__).parent.parent.parent / "data" / "mal" / "checkpoint.json"

DAYS_OF_WEEK = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

ALL_TABLES = [
    "anime", "anime_genres", "anime_relations", "anime_themes",
    "anime_external", "anime_streaming", "anime_studios",
    "anime_videos_promo", "anime_videos_ep", "anime_episodes",
    "anime_pictures", "anime_statistics", "anime_moreinfo",
    "anime_recommendations", "anime_characters", "va_credits",
    "staff_credits", "anime_news", "anime_schedule",
    "persons", "person_pictures",
    "characters", "character_pictures",
    "producers", "producer_external",
    "manga", "manga_authors", "manga_serializations",
    "master_genres", "master_magazines",
]

JIKAN_LIMITER = DualWindowRateLimiter(per_second=2, per_minute=45)

app = make_scraper_app("mal")


def _empty_checkpoint() -> dict:
    return {
        "phase": "A",
        "all_anime_ids": [],
        "completed_anime_ids": [],
        "discovered_person_ids": [],
        "discovered_character_ids": [],
        "discovered_producer_ids": [],
        "discovered_manga_ids": [],
        "completed_person_ids": [],
        "completed_character_ids": [],
        "completed_producer_ids": [],
        "completed_manga_ids": [],
        "completed_phase_c_masters": False,
        "last_page_anime_list": 0,
        "timestamp": None,
    }


class JikanClient:
    """Async Jikan v4 client — all endpoints cached + rate limited."""

    def __init__(self, transport=None) -> None:
        self._limiter = JIKAN_LIMITER
        self._http = RetryingHttpClient(
            source="mal",
            base_url=BASE_URL,
            delay=0.0,
            timeout=30.0,
            headers={"Accept": "application/json"},
            transport=transport,
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def get(self, endpoint: str, params: dict | None = None) -> dict:
        cache_key = {"endpoint": endpoint, "params": params or {}}
        cached = load_cached_json("mal/rest", cache_key)
        if cached is not None:
            return cached
        await self._limiter.acquire()
        resp = await self._http.get(endpoint, params=params)
        resp.raise_for_status()
        data = resp.json()
        save_cached_json("mal/rest", cache_key, data)
        return data

    # ── anime list ────────────────────────────────────────────────────────────

    async def get_all_anime(self, page: int = 1) -> dict:
        return await self.get("/anime", params={"page": page, "limit": 25,
                                                "order_by": "mal_id", "sort": "asc"})

    # ── anime sub-endpoints ───────────────────────────────────────────────────

    async def get_anime_full(self, mal_id: int) -> dict:
        return await self.get(f"/anime/{mal_id}/full")

    async def get_anime_staff(self, mal_id: int) -> dict:
        return await self.get(f"/anime/{mal_id}/staff")

    async def get_anime_characters(self, mal_id: int) -> dict:
        return await self.get(f"/anime/{mal_id}/characters")

    async def get_anime_episodes(self, mal_id: int, page: int = 1) -> dict:
        return await self.get(f"/anime/{mal_id}/episodes", params={"page": page})

    async def get_anime_external(self, mal_id: int) -> dict:
        return await self.get(f"/anime/{mal_id}/external")

    async def get_anime_streaming(self, mal_id: int) -> dict:
        return await self.get(f"/anime/{mal_id}/streaming")

    async def get_anime_videos(self, mal_id: int) -> dict:
        return await self.get(f"/anime/{mal_id}/videos")

    async def get_anime_pictures(self, mal_id: int) -> dict:
        return await self.get(f"/anime/{mal_id}/pictures")

    async def get_anime_statistics(self, mal_id: int) -> dict:
        return await self.get(f"/anime/{mal_id}/statistics")

    async def get_anime_moreinfo(self, mal_id: int) -> dict:
        return await self.get(f"/anime/{mal_id}/moreinfo")

    async def get_anime_recommendations(self, mal_id: int) -> dict:
        return await self.get(f"/anime/{mal_id}/recommendations")

    async def get_anime_news(self, mal_id: int, page: int = 1) -> dict:
        return await self.get(f"/anime/{mal_id}/news", params={"page": page})

    # ── persons / characters ──────────────────────────────────────────────────

    async def get_person_full(self, pid: int) -> dict:
        return await self.get(f"/people/{pid}/full")

    async def get_person_pictures(self, pid: int) -> dict:
        return await self.get(f"/people/{pid}/pictures")

    async def get_character_full(self, cid: int) -> dict:
        return await self.get(f"/characters/{cid}/full")

    async def get_character_pictures(self, cid: int) -> dict:
        return await self.get(f"/characters/{cid}/pictures")

    # ── producers / manga ────────────────────────────────────────────────────

    async def get_producer_full(self, pid: int) -> dict:
        return await self.get(f"/producers/{pid}/full")

    async def get_producer_external(self, pid: int) -> dict:
        return await self.get(f"/producers/{pid}/external")

    async def get_producers_list(self, page: int = 1) -> dict:
        return await self.get("/producers", params={"page": page, "order_by": "mal_id",
                                                    "sort": "asc", "limit": 25})

    async def get_manga_full(self, mid: int) -> dict:
        return await self.get(f"/manga/{mid}/full")

    # ── masters ───────────────────────────────────────────────────────────────

    async def get_genres_anime(self, filter_kind: str | None = None) -> dict:
        params = {"filter": filter_kind} if filter_kind else None
        return await self.get("/genres/anime", params=params)

    async def get_magazines(self, page: int = 1) -> dict:
        return await self.get("/magazines", params={"page": page, "limit": 25})

    async def get_schedules(self, day: str) -> dict:
        return await self.get("/schedules", params={"filter": day})


# ── Phase A ───────────────────────────────────────────────────────────────────

async def _phase_a_anime(
    client: JikanClient,
    group: BronzeWriterGroup,
    cp: Checkpoint,
    p,
    checkpoint_interval: int,
) -> None:
    """Enumerate all anime IDs, then fetch 13 sub-endpoints each."""
    ckpt = cp.data
    completed: set[int] = set(ckpt["completed_anime_ids"])
    discovered_persons: set[int] = set(ckpt["discovered_person_ids"])
    discovered_chars: set[int] = set(ckpt["discovered_character_ids"])
    discovered_producers: set[int] = set(ckpt["discovered_producer_ids"])
    discovered_manga: set[int] = set(ckpt["discovered_manga_ids"])

    # Step 1: enumerate all anime IDs
    if not ckpt["all_anime_ids"]:
        log.info("phase_a_enumerate_start")
        all_ids: list[int] = []
        page = ckpt.get("last_page_anime_list", 0) + 1
        while True:
            try:
                resp = await client.get_all_anime(page=page)
            except Exception as e:
                log.error("phase_a_enumerate_error", page=page, error=str(e))
                break
            items = resp.get("data") or []
            if not items:
                break
            for item in items:
                if item.get("mal_id"):
                    all_ids.append(item["mal_id"])
            pagination = resp.get("pagination") or {}
            ckpt["last_page_anime_list"] = page
            if not pagination.get("has_next_page"):
                break
            page += 1
        ckpt["all_anime_ids"] = all_ids
        cp.save(stamp_time=False)
        log.info("phase_a_enumerate_done", total=len(all_ids))
    else:
        all_ids = ckpt["all_anime_ids"]

    # Step 2: fetch each anime's 13 endpoints
    pending = [mid for mid in all_ids if mid not in completed]
    log.info("phase_a_fetch_start", pending=len(pending), total=len(all_ids))

    for i, mal_id in enumerate(pending):
        try:
            await _fetch_one_anime(
                client, group, mal_id,
                discovered_persons, discovered_chars,
                discovered_producers, discovered_manga,
            )
        except Exception as e:
            log.error("phase_a_anime_failed", mal_id=mal_id, error=str(e))

        completed.add(mal_id)
        p.advance()

        if (i + 1) % checkpoint_interval == 0:
            ckpt["completed_anime_ids"] = list(completed)
            ckpt["discovered_person_ids"] = list(discovered_persons)
            ckpt["discovered_character_ids"] = list(discovered_chars)
            ckpt["discovered_producer_ids"] = list(discovered_producers)
            ckpt["discovered_manga_ids"] = list(discovered_manga)
            group.flush_all()
            cp.save(stamp_time=False)
            p.log("phase_a_checkpoint", done=len(completed), total=len(all_ids))

    ckpt["completed_anime_ids"] = list(completed)
    ckpt["discovered_person_ids"] = list(discovered_persons)
    ckpt["discovered_character_ids"] = list(discovered_chars)
    ckpt["discovered_producer_ids"] = list(discovered_producers)
    ckpt["discovered_manga_ids"] = list(discovered_manga)


async def _safe_call(endpoint: str, mal_id: int, coro):
    """Run a Jikan call, log + swallow exceptions, return result or None."""
    try:
        return await coro
    except Exception as e:
        log.error(f"fetch_{endpoint}_failed", mal_id=mal_id, error=str(e))
        return None


def _emit_list(group: BronzeWriterGroup, table: str, records) -> None:
    for r in records:
        group[table].append(asdict(r))


# Simple endpoints: result_or_None → list-of-records → single bronze table.
# (Complex endpoints with tuple unpacking / side effects stay inline below.)
_SIMPLE_LIST_ENDPOINTS = (
    ("anime_external", "anime_external", "get_anime_external", parse_anime_external),
    ("anime_streaming", "anime_streaming", "get_anime_streaming", parse_anime_streaming),
    ("anime_pictures", "anime_pictures", "get_anime_pictures", parse_anime_pictures),
    ("anime_recommendations", "anime_recommendations", "get_anime_recommendations", parse_anime_recommendations),
    ("anime_news", "anime_news", "get_anime_news", parse_anime_news),
)

# Single-record endpoints: result → one record → append to one table.
_SINGLE_RECORD_ENDPOINTS = (
    ("anime_statistics", "anime_statistics", "get_anime_statistics", parse_anime_statistics),
    ("anime_moreinfo", "anime_moreinfo", "get_anime_moreinfo", parse_anime_moreinfo),
)


async def _fetch_one_anime(
    client: JikanClient,
    group: BronzeWriterGroup,
    mal_id: int,
    discovered_persons: set[int],
    discovered_chars: set[int],
    discovered_producers: set[int],
    discovered_manga: set[int],
) -> None:
    """Fetch 13 sub-endpoints for one anime and write to BronzeWriters."""
    # /anime/{id}/full → 7 dataclass types, with manga & producer discovery
    raw = await _safe_call("anime_full", mal_id, client.get_anime_full(mal_id))
    if raw is not None:
        record, genres, relations, themes, externals, streamings, studios = parse_anime_full(raw)
        group["anime"].append(asdict(record))
        _emit_list(group, "anime_genres", genres)
        for relation in relations:
            group["anime_relations"].append(asdict(relation))
            if relation.target_type == "manga":
                discovered_manga.add(relation.target_mal_id)
        _emit_list(group, "anime_themes", themes)
        _emit_list(group, "anime_external", externals)
        _emit_list(group, "anime_streaming", streamings)
        for studio in studios:
            group["anime_studios"].append(asdict(studio))
            discovered_producers.add(studio.mal_producer_id)

    # /anime/{id}/staff → person discovery
    raw = await _safe_call("anime_staff", mal_id, client.get_anime_staff(mal_id))
    if raw is not None:
        for credit in parse_anime_staff_full(mal_id, raw):
            group["staff_credits"].append(asdict(credit))
            discovered_persons.add(credit.mal_person_id)

    # /anime/{id}/characters → character + VA-person discovery
    raw = await _safe_call("anime_characters", mal_id, client.get_anime_characters(mal_id))
    if raw is not None:
        chars, vas = parse_anime_characters_va(mal_id, raw)
        for char in chars:
            group["anime_characters"].append(asdict(char))
            discovered_chars.add(char.mal_character_id)
        for va in vas:
            group["va_credits"].append(asdict(va))
            discovered_persons.add(va.mal_person_id)

    # /anime/{id}/episodes (paginated, no side effects)
    page = 1
    while True:
        raw = await _safe_call(
            "anime_episodes", mal_id, client.get_anime_episodes(mal_id, page=page)
        )
        if raw is None:
            break
        _emit_list(group, "anime_episodes", parse_anime_episodes(mal_id, raw))
        if not (raw.get("pagination") or {}).get("has_next_page"):
            break
        page += 1

    # /anime/{id}/videos → 2 lists, no side effects
    raw = await _safe_call("anime_videos", mal_id, client.get_anime_videos(mal_id))
    if raw is not None:
        promos, ep_vids = parse_anime_videos(mal_id, raw)
        _emit_list(group, "anime_videos_promo", promos)
        _emit_list(group, "anime_videos_ep", ep_vids)

    for endpoint_name, table, getter_name, parser in _SIMPLE_LIST_ENDPOINTS:
        raw = await _safe_call(endpoint_name, mal_id, getattr(client, getter_name)(mal_id))
        if raw is not None:
            _emit_list(group, table, parser(mal_id, raw))

    for endpoint_name, table, getter_name, parser in _SINGLE_RECORD_ENDPOINTS:
        raw = await _safe_call(endpoint_name, mal_id, getattr(client, getter_name)(mal_id))
        if raw is not None:
            group[table].append(asdict(parser(mal_id, raw)))


# ── shared loop helper ────────────────────────────────────────────────────────

async def _iterate_with_checkpoints(
    items: list[int],
    completed: set[int],
    processor: Callable[[int], Awaitable[None]],
    *,
    cp: Checkpoint,
    ckpt_key: str,
    group: BronzeWriterGroup,
    progress,
    checkpoint_interval: int,
    error_event: str,
    done_start: int = 0,
) -> int:
    """Iterate ``items``, calling ``processor(id)`` for each, with periodic checkpoints.

    Wraps each ``processor`` call in try/except so a failure on one ID does not
    abort the whole loop; the failed ID is still added to ``completed`` to avoid
    re-fetching on resume (matches existing per-phase behaviour).

    Args:
        items:               IDs to process (pre-filtered, completed already excluded).
        completed:           Set tracking completed IDs; mutated in place.
        processor:           Async callback that fetches/parses/appends rows for one ID.
        cp:                  Checkpoint to flush mid-loop.
        ckpt_key:            Key under ``cp.data`` to write ``list(completed)`` to.
        group:               BronzeWriterGroup whose ``flush_all`` is called every checkpoint.
        progress:            Object exposing ``.advance()`` per processed item.
        checkpoint_interval: Number of items between flush+checkpoint saves.
        error_event:         structlog event name used when ``processor`` raises.
        done_start:          Initial value of the running counter (lets phases share
                             a single counter across two sub-loops).

    Returns:
        Total ``done`` count after the loop (callers can pass this as ``done_start``
        of a subsequent call to keep checkpoint cadence consistent).
    """
    ckpt = cp.data
    done = done_start
    for item_id in items:
        try:
            await processor(item_id)
        except Exception as e:
            log.error(error_event, id=item_id, error=str(e))
        completed.add(item_id)
        done += 1
        progress.advance()
        if done % checkpoint_interval == 0:
            ckpt[ckpt_key] = list(completed)
            group.flush_all()
            cp.save(stamp_time=False)
    return done


# ── Phase B ───────────────────────────────────────────────────────────────────

async def _phase_b_persons_characters(
    client: JikanClient,
    group: BronzeWriterGroup,
    cp: Checkpoint,
    p,
    checkpoint_interval: int,
) -> None:
    """Fetch person and character detail pages."""
    ckpt = cp.data
    completed_persons: set[int] = set(ckpt["completed_person_ids"])
    completed_chars: set[int] = set(ckpt["completed_character_ids"])
    discovered_persons: list[int] = ckpt["discovered_person_ids"]
    discovered_chars: list[int] = ckpt["discovered_character_ids"]

    pending_persons = [pid for pid in discovered_persons if pid not in completed_persons]
    pending_chars = [cid for cid in discovered_chars if cid not in completed_chars]

    log.info("phase_b_start", pending_persons=len(pending_persons),
             pending_chars=len(pending_chars))

    async def _process_person(pid: int) -> None:
        raw = await client.get_person_full(pid)
        group["persons"].append(asdict(parse_person_full(raw)))
        for pic in parse_person_pictures(pid, await client.get_person_pictures(pid)):
            group["person_pictures"].append(asdict(pic))

    async def _process_character(cid: int) -> None:
        raw = await client.get_character_full(cid)
        group["characters"].append(asdict(parse_character_full(raw)))
        for pic in parse_character_pictures(cid, await client.get_character_pictures(cid)):
            group["character_pictures"].append(asdict(pic))

    done = await _iterate_with_checkpoints(
        pending_persons, completed_persons, _process_person,
        cp=cp, ckpt_key="completed_person_ids", group=group, progress=p,
        checkpoint_interval=checkpoint_interval, error_event="phase_b_person_failed",
    )
    await _iterate_with_checkpoints(
        pending_chars, completed_chars, _process_character,
        cp=cp, ckpt_key="completed_character_ids", group=group, progress=p,
        checkpoint_interval=checkpoint_interval, error_event="phase_b_character_failed",
        done_start=done,
    )

    ckpt["completed_person_ids"] = list(completed_persons)
    ckpt["completed_character_ids"] = list(completed_chars)


# ── Phase C ───────────────────────────────────────────────────────────────────

async def _phase_c_producers_manga_masters(
    client: JikanClient,
    group: BronzeWriterGroup,
    cp: Checkpoint,
    p,
    checkpoint_interval: int,
) -> None:
    """Fetch producers, manga, and master tables."""
    ckpt = cp.data
    completed_producers: set[int] = set(ckpt["completed_producer_ids"])
    completed_manga: set[int] = set(ckpt["completed_manga_ids"])
    discovered_producers: set[int] = set(ckpt["discovered_producer_ids"])
    discovered_manga: set[int] = set(ckpt["discovered_manga_ids"])

    # Enumerate all producers from API + merge discovered
    if not completed_producers:
        page = 1
        while True:
            try:
                resp = await client.get_producers_list(page=page)
            except Exception as e:
                log.error("phase_c_producers_list_error", page=page, error=str(e))
                break
            items = resp.get("data") or []
            if not items:
                break
            for item in items:
                if item.get("mal_id"):
                    discovered_producers.add(item["mal_id"])
            pagination = resp.get("pagination") or {}
            if not pagination.get("has_next_page"):
                break
            page += 1
        ckpt["discovered_producer_ids"] = list(discovered_producers)

    pending_producers = [pid for pid in discovered_producers if pid not in completed_producers]
    pending_manga = [mid for mid in discovered_manga if mid not in completed_manga]

    log.info("phase_c_start", pending_producers=len(pending_producers),
             pending_manga=len(pending_manga))

    async def _process_producer(pid: int) -> None:
        raw = await client.get_producer_full(pid)
        prod, externals = parse_producer_full(raw)
        group["producers"].append(asdict(prod))
        # Completeness: also hit /external endpoint
        ext_raw = await client.get_producer_external(pid)
        for e in parse_producer_external(pid, ext_raw):
            externals.append(e)
        seen_urls: set[str] = set()
        for e in externals:
            if e.url not in seen_urls:
                group["producer_external"].append(asdict(e))
                seen_urls.add(e.url)

    async def _process_manga(mid: int) -> None:
        raw = await client.get_manga_full(mid)
        manga, authors, serials, rels = parse_manga_full(raw)
        group["manga"].append(asdict(manga))
        for a in authors:
            group["manga_authors"].append(asdict(a))
        for s in serials:
            group["manga_serializations"].append(asdict(s))

    done = await _iterate_with_checkpoints(
        pending_producers, completed_producers, _process_producer,
        cp=cp, ckpt_key="completed_producer_ids", group=group, progress=p,
        checkpoint_interval=checkpoint_interval, error_event="phase_c_producer_failed",
    )
    await _iterate_with_checkpoints(
        pending_manga, completed_manga, _process_manga,
        cp=cp, ckpt_key="completed_manga_ids", group=group, progress=p,
        checkpoint_interval=checkpoint_interval, error_event="phase_c_manga_failed",
        done_start=done,
    )

    # Masters (one-shot)
    if not ckpt.get("completed_phase_c_masters"):
        today = date.today().isoformat()
        for kind in ("genres", "explicit_genres", "themes", "demographics"):
            try:
                raw = await client.get_genres_anime(filter_kind=kind)
                for g in parse_master_genres(raw, kind):
                    group["master_genres"].append(asdict(g))
            except Exception as e:
                log.error("phase_c_genres_failed", kind=kind, error=str(e))

        mag_page = 1
        while True:
            try:
                raw = await client.get_magazines(page=mag_page)
                mags = parse_master_magazines(raw)
                for m in mags:
                    group["master_magazines"].append(asdict(m))
                if not (raw.get("pagination") or {}).get("has_next_page"):
                    break
                mag_page += 1
            except Exception as e:
                log.error("phase_c_magazines_failed", page=mag_page, error=str(e))
                break

        for day in DAYS_OF_WEEK:
            try:
                raw = await client.get_schedules(day)
                for s in parse_schedules(raw, day, today):
                    group["anime_schedule"].append(asdict(s))
            except Exception as e:
                log.error("phase_c_schedules_failed", day=day, error=str(e))

        ckpt["completed_phase_c_masters"] = True

    ckpt["completed_producer_ids"] = list(completed_producers)
    ckpt["completed_manga_ids"] = list(completed_manga)


# ── CLI entry point ───────────────────────────────────────────────────────────

@app.command()
def run(
    phase: str = typer.Option("all", "--phase", help="A / B / C / all"),
    force: ForceOpt = False,
    resume: ResumeOpt = True,
    limit: LimitOpt = 0,
    delay: DelayOpt = 0.0,
    checkpoint_interval: CheckpointIntervalOpt = 10,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Jikan v4 全 endpoint scrape (3 Phase: A=anime, B=persons+chars, C=producers+manga+masters)."""
    log.info("mal_scrape_start", phase=phase)
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)

    cp_data = _empty_checkpoint()
    if resume and not force:
        loaded = Checkpoint.load(CHECKPOINT_FILE)
        cp_data.update(loaded.data)
    cp = Checkpoint(CHECKPOINT_FILE, cp_data)
    ckpt = cp.data

    progress_enabled = resolve_progress_enabled(quiet, progress)

    async def _run() -> None:
        client = JikanClient()
        try:
            with BronzeWriterGroup("mal", tables=ALL_TABLES) as group:
                with scrape_progress(
                    total=None,
                    description=f"MAL phase={phase}",
                    enabled=progress_enabled,
                ) as p:
                    if phase in ("A", "all"):
                        await _phase_a_anime(client, group, cp, p, checkpoint_interval)
                        ckpt["phase"] = "B"
                        cp.save(stamp_time=False)

                    if phase in ("B", "all"):
                        await _phase_b_persons_characters(client, group, cp, p, checkpoint_interval)
                        ckpt["phase"] = "C"
                        cp.save(stamp_time=False)

                    if phase in ("C", "all"):
                        await _phase_c_producers_manga_masters(
                            client, group, cp, p, checkpoint_interval
                        )
                        ckpt["phase"] = "DONE"
                        cp.save(stamp_time=False)
        finally:
            await client.close()

        log.info(
            "mal_scrape_complete",
            anime=len(ckpt["completed_anime_ids"]),
            persons=len(ckpt["completed_person_ids"]),
            characters=len(ckpt["completed_character_ids"]),
            producers=len(ckpt["completed_producer_ids"]),
            manga=len(ckpt["completed_manga_ids"]),
        )

    asyncio.run(_run())


if __name__ == "__main__":
    app()
