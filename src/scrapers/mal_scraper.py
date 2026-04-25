"""MyAnimeList credit data collection (via Jikan API v4).

3-Phase scraper:
  Phase A: anime + 13 sub-endpoints (~60h)
  Phase B: persons + characters (~33h)
  Phase C: producers + manga + masters (~90h)

Rate limit: 3 req/s + 60 req/min (DualWindowRateLimiter).
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

import structlog
import typer

from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.cache_store import load_cached_json, save_cached_json
from src.scrapers.checkpoint import atomic_write_json, load_json_or
from src.scrapers.cli_common import (
    CheckpointIntervalOpt,
    ProgressOpt,
    QuietOpt,
    ResumeOpt,
    resolve_progress_enabled,
)
from src.scrapers.http_client import DualWindowRateLimiter, RetryingHttpClient
from src.scrapers.logging_utils import configure_file_logging
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

JIKAN_LIMITER = DualWindowRateLimiter(per_second=3, per_minute=60)

app = typer.Typer()


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


def _save_ckpt(ckpt: dict) -> None:
    ckpt["timestamp"] = datetime.now(tz=timezone.utc).isoformat()
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(CHECKPOINT_FILE, ckpt, indent=2)


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
    ckpt: dict,
    p,
    checkpoint_interval: int,
) -> None:
    """Enumerate all anime IDs, then fetch 13 sub-endpoints each."""
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
        _save_ckpt(ckpt)
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
            _save_ckpt(ckpt)
            p.log("phase_a_checkpoint", done=len(completed), total=len(all_ids))

    ckpt["completed_anime_ids"] = list(completed)
    ckpt["discovered_person_ids"] = list(discovered_persons)
    ckpt["discovered_character_ids"] = list(discovered_chars)
    ckpt["discovered_producer_ids"] = list(discovered_producers)
    ckpt["discovered_manga_ids"] = list(discovered_manga)


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
    # /anime/{id}/full → 7 dataclass types
    try:
        raw = await client.get_anime_full(mal_id)
        record, genres, relations, themes, externals, streamings, studios = parse_anime_full(raw)
        group["anime"].append(asdict(record))
        for g in genres:
            group["anime_genres"].append(asdict(g))
        for r in relations:
            group["anime_relations"].append(asdict(r))
            if r.target_type == "manga":
                discovered_manga.add(r.target_mal_id)
        for t in themes:
            group["anime_themes"].append(asdict(t))
        for e in externals:
            group["anime_external"].append(asdict(e))
        for s in streamings:
            group["anime_streaming"].append(asdict(s))
        for s in studios:
            group["anime_studios"].append(asdict(s))
            discovered_producers.add(s.mal_producer_id)
    except Exception as e:
        log.error("fetch_anime_full_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/staff
    try:
        raw = await client.get_anime_staff(mal_id)
        for c in parse_anime_staff_full(mal_id, raw):
            group["staff_credits"].append(asdict(c))
            discovered_persons.add(c.mal_person_id)
    except Exception as e:
        log.error("fetch_anime_staff_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/characters
    try:
        raw = await client.get_anime_characters(mal_id)
        chars, vas = parse_anime_characters_va(mal_id, raw)
        for c in chars:
            group["anime_characters"].append(asdict(c))
            discovered_chars.add(c.mal_character_id)
        for v in vas:
            group["va_credits"].append(asdict(v))
            discovered_persons.add(v.mal_person_id)
    except Exception as e:
        log.error("fetch_anime_characters_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/episodes (paginated)
    try:
        page = 1
        while True:
            raw = await client.get_anime_episodes(mal_id, page=page)
            for ep in parse_anime_episodes(mal_id, raw):
                group["anime_episodes"].append(asdict(ep))
            pagination = raw.get("pagination") or {}
            if not pagination.get("has_next_page"):
                break
            page += 1
    except Exception as e:
        log.error("fetch_anime_episodes_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/external (completeness supplement)
    try:
        raw = await client.get_anime_external(mal_id)
        for e in parse_anime_external(mal_id, raw):
            group["anime_external"].append(asdict(e))
    except Exception as e:
        log.error("fetch_anime_external_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/streaming
    try:
        raw = await client.get_anime_streaming(mal_id)
        for s in parse_anime_streaming(mal_id, raw):
            group["anime_streaming"].append(asdict(s))
    except Exception as e:
        log.error("fetch_anime_streaming_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/videos
    try:
        raw = await client.get_anime_videos(mal_id)
        promos, ep_vids = parse_anime_videos(mal_id, raw)
        for p in promos:
            group["anime_videos_promo"].append(asdict(p))
        for v in ep_vids:
            group["anime_videos_ep"].append(asdict(v))
    except Exception as e:
        log.error("fetch_anime_videos_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/pictures
    try:
        raw = await client.get_anime_pictures(mal_id)
        for pic in parse_anime_pictures(mal_id, raw):
            group["anime_pictures"].append(asdict(pic))
    except Exception as e:
        log.error("fetch_anime_pictures_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/statistics
    try:
        raw = await client.get_anime_statistics(mal_id)
        group["anime_statistics"].append(asdict(parse_anime_statistics(mal_id, raw)))
    except Exception as e:
        log.error("fetch_anime_statistics_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/moreinfo
    try:
        raw = await client.get_anime_moreinfo(mal_id)
        group["anime_moreinfo"].append(asdict(parse_anime_moreinfo(mal_id, raw)))
    except Exception as e:
        log.error("fetch_anime_moreinfo_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/recommendations
    try:
        raw = await client.get_anime_recommendations(mal_id)
        for r in parse_anime_recommendations(mal_id, raw):
            group["anime_recommendations"].append(asdict(r))
    except Exception as e:
        log.error("fetch_anime_recommendations_failed", mal_id=mal_id, error=str(e))

    # /anime/{id}/news
    try:
        raw = await client.get_anime_news(mal_id)
        for n in parse_anime_news(mal_id, raw):
            group["anime_news"].append(asdict(n))
    except Exception as e:
        log.error("fetch_anime_news_failed", mal_id=mal_id, error=str(e))


# ── Phase B ───────────────────────────────────────────────────────────────────

async def _phase_b_persons_characters(
    client: JikanClient,
    group: BronzeWriterGroup,
    ckpt: dict,
    p,
    checkpoint_interval: int,
) -> None:
    """Fetch person and character detail pages."""
    completed_persons: set[int] = set(ckpt["completed_person_ids"])
    completed_chars: set[int] = set(ckpt["completed_character_ids"])
    discovered_persons: list[int] = ckpt["discovered_person_ids"]
    discovered_chars: list[int] = ckpt["discovered_character_ids"]

    pending_persons = [pid for pid in discovered_persons if pid not in completed_persons]
    pending_chars = [cid for cid in discovered_chars if cid not in completed_chars]

    log.info("phase_b_start", pending_persons=len(pending_persons),
             pending_chars=len(pending_chars))

    done = 0
    for pid in pending_persons:
        try:
            raw = await client.get_person_full(pid)
            group["persons"].append(asdict(parse_person_full(raw)))
            for pic in parse_person_pictures(pid, await client.get_person_pictures(pid)):
                group["person_pictures"].append(asdict(pic))
        except Exception as e:
            log.error("phase_b_person_failed", pid=pid, error=str(e))
        completed_persons.add(pid)
        done += 1
        p.advance()
        if done % checkpoint_interval == 0:
            ckpt["completed_person_ids"] = list(completed_persons)
            group.flush_all()
            _save_ckpt(ckpt)

    for cid in pending_chars:
        try:
            raw = await client.get_character_full(cid)
            group["characters"].append(asdict(parse_character_full(raw)))
            for pic in parse_character_pictures(cid, await client.get_character_pictures(cid)):
                group["character_pictures"].append(asdict(pic))
        except Exception as e:
            log.error("phase_b_character_failed", cid=cid, error=str(e))
        completed_chars.add(cid)
        done += 1
        p.advance()
        if done % checkpoint_interval == 0:
            ckpt["completed_character_ids"] = list(completed_chars)
            group.flush_all()
            _save_ckpt(ckpt)

    ckpt["completed_person_ids"] = list(completed_persons)
    ckpt["completed_character_ids"] = list(completed_chars)


# ── Phase C ───────────────────────────────────────────────────────────────────

async def _phase_c_producers_manga_masters(
    client: JikanClient,
    group: BronzeWriterGroup,
    ckpt: dict,
    p,
    checkpoint_interval: int,
) -> None:
    """Fetch producers, manga, and master tables."""
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

    done = 0
    for pid in pending_producers:
        try:
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
        except Exception as e:
            log.error("phase_c_producer_failed", pid=pid, error=str(e))
        completed_producers.add(pid)
        done += 1
        p.advance()
        if done % checkpoint_interval == 0:
            ckpt["completed_producer_ids"] = list(completed_producers)
            group.flush_all()
            _save_ckpt(ckpt)

    for mid in pending_manga:
        try:
            raw = await client.get_manga_full(mid)
            manga, authors, serials, rels = parse_manga_full(raw)
            group["manga"].append(asdict(manga))
            for a in authors:
                group["manga_authors"].append(asdict(a))
            for s in serials:
                group["manga_serializations"].append(asdict(s))
        except Exception as e:
            log.error("phase_c_manga_failed", mid=mid, error=str(e))
        completed_manga.add(mid)
        done += 1
        p.advance()
        if done % checkpoint_interval == 0:
            ckpt["completed_manga_ids"] = list(completed_manga)
            group.flush_all()
            _save_ckpt(ckpt)

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
def main(
    phase: str = typer.Option("all", "--phase", help="A / B / C / all"),
    resume: ResumeOpt = True,
    checkpoint_interval: CheckpointIntervalOpt = 10,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Jikan v4 全 endpoint scrape (3 Phase: A=anime, B=persons+chars, C=producers+manga+masters)."""
    from src.infra.logging import setup_logging

    setup_logging()
    log_path = configure_file_logging("mal")
    log.info("mal_scrape_start", phase=phase, log_file=str(log_path))
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)

    ckpt = (
        load_json_or(CHECKPOINT_FILE, _empty_checkpoint())
        if resume
        else _empty_checkpoint()
    )

    progress_enabled = resolve_progress_enabled(quiet, progress)

    async def _run() -> None:
        client = JikanClient()
        group = BronzeWriterGroup("mal", tables=ALL_TABLES)
        try:
            with scrape_progress(
                total=None,
                description=f"MAL phase={phase}",
                enabled=progress_enabled,
            ) as p:
                if phase in ("A", "all"):
                    await _phase_a_anime(client, group, ckpt, p, checkpoint_interval)
                    ckpt["phase"] = "B"
                    _save_ckpt(ckpt)

                if phase in ("B", "all"):
                    await _phase_b_persons_characters(client, group, ckpt, p, checkpoint_interval)
                    ckpt["phase"] = "C"
                    _save_ckpt(ckpt)

                if phase in ("C", "all"):
                    await _phase_c_producers_manga_masters(
                        client, group, ckpt, p, checkpoint_interval
                    )
                    ckpt["phase"] = "DONE"
                    _save_ckpt(ckpt)
        finally:
            await client.close()
            group.flush_all()
            group.compact_all()

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
