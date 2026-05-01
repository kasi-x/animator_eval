"""TMDB scraper — Animation genre (id=16) TV + Movie BRONZE collection.

Hard Rule: vote_average / popularity flow into ``display_*`` only. They
MUST NOT enter scoring, edge weights, or optimization targets.

Auth:
  TMDB_BEARER  — v4 read access token (preferred). Sent as ``Authorization: Bearer …``.
  TMDB_API_KEY — v3 query-param fallback. Used only if TMDB_BEARER is absent.

Rate limit: 50 req/sec → REQUEST_INTERVAL = 0.025 s.

Pipeline (3 phases):
  1. discover  — /discover/{tv,movie}?with_genres=16, paginated. Auto year-split when
                 a single combo would exceed the 10 000-item TMDB hard cap.
  2. detail    — /{tv,movie}/{id}?append_to_response=external_ids,(aggregate_)credits.
                 One request per anime; emits anime row + credit rows.
  3. person    — /person/{id}?append_to_response=external_ids. Fanout over the
                 unique person_ids that appeared in phase-2 credits.

Each phase has its own checkpoint and BRONZE writer group, so any phase can
resume independently.
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as _dt
import os

import httpx
import structlog
import typer
from dotenv import dotenv_values, find_dotenv

from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.checkpoint import Checkpoint
from src.scrapers.cli_common import (
    DelayOpt,
    ForceOpt,
    LimitOpt,
    ProgressOpt,
    QuietOpt,
    ResumeOpt,
    make_scraper_app,
    resolve_progress_enabled,
)
from src.scrapers.http_client import RetryingHttpClient
from src.scrapers.parsers.tmdb import (
    TmdbAnimeRecord,
    TmdbCreditEntry,
    TmdbPersonRecord,
    discover_results,
    parse_tmdb_anime,
    parse_tmdb_person,
)
from src.scrapers.queries.tmdb import (
    DISCOVER_PAGE_LIMIT,
    detail_url,
    discover_url,
    person_url,
)
from src.scrapers.runner import ScrapeRunner
from src.scrapers.sinks import BronzeSink

log = structlog.get_logger()

REQUEST_INTERVAL = 0.025  # 50 req/sec (TMDB official ceiling)
CHECKPOINT_DIR = "result/checkpoints"
PHASE1_CHECKPOINT = f"{CHECKPOINT_DIR}/tmdb_discover.json"
PHASE2_CHECKPOINT = f"{CHECKPOINT_DIR}/tmdb_detail.json"
PHASE3_CHECKPOINT = f"{CHECKPOINT_DIR}/tmdb_person.json"

_env = dotenv_values(find_dotenv())


def _auth_headers() -> dict[str, str]:
    """Return Authorization header (Bearer) if TMDB_BEARER set; else empty."""
    token = os.environ.get("TMDB_BEARER") or _env.get("TMDB_BEARER")
    if token:
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    return {"Accept": "application/json"}


def _api_key_param() -> dict[str, str]:
    """Return ``api_key`` query param dict if v4 Bearer absent."""
    if os.environ.get("TMDB_BEARER") or _env.get("TMDB_BEARER"):
        return {}
    key = os.environ.get("TMDB_API_KEY") or _env.get("TMDB_API_KEY")
    return {"api_key": key} if key else {}


class TmdbClient:
    """Thin wrapper around RetryingHttpClient with TMDB auth + JSON helper."""

    def __init__(self, *, delay: float = REQUEST_INTERVAL) -> None:
        self._client = RetryingHttpClient(
            source="tmdb",
            delay=delay,
            timeout=30.0,
            headers=_auth_headers(),
        )
        self._api_key = _api_key_param()
        if not (_auth_headers().get("Authorization") or self._api_key):
            raise RuntimeError(
                "TMDB credentials missing: set TMDB_BEARER (preferred) or TMDB_API_KEY"
            )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get_json(self, url: str) -> dict | None:
        """GET url, return parsed JSON or None on 404."""
        params = self._api_key or None
        try:
            resp = await self._client.get(url, params=params)
        except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()


# ─── Phase 1: discover ────────────────────────────────────────────────────


async def _discover_one_year(
    client: TmdbClient, media: str, year: int | None
) -> list[int]:
    """Page through /discover for one (media, year) combo. Honors 500-page cap."""
    out: list[int] = []
    page = 1
    while True:
        data = await client.get_json(discover_url(media, page=page, year=year))
        if data is None:
            break
        ids, total_pages = discover_results(data)
        out.extend(ids)
        if not ids or page >= min(total_pages, DISCOVER_PAGE_LIMIT):
            break
        page += 1
    log.info(
        "tmdb_discover_year_done",
        media=media,
        year=year,
        page_max=page,
        ids=len(out),
    )
    return out


async def discover_all_ids(
    client: TmdbClient, media: str, *, year_from: int = 1900, year_to: int | None = None
) -> list[tuple[str, int]]:
    """Enumerate all TMDB ids for the given media with genre=Animation.

    Strategy:
      1. Try unfiltered combo first; if total_pages > 500 (= 10 000 items),
         fall back to year-by-year split.
      2. Year split spans year_from..year_to inclusive (year_to defaults to
         current year).

    Returns: list of (media_type, tmdb_id).
    """
    if year_to is None:
        year_to = _dt.date.today().year

    # Probe page 1 to see total_pages
    probe = await client.get_json(discover_url(media, page=1))
    if probe is None:
        return []
    _, total_pages = discover_results(probe)

    if total_pages <= DISCOVER_PAGE_LIMIT:
        ids = await _discover_one_year(client, media, year=None)
    else:
        log.info(
            "tmdb_discover_year_split_required",
            media=media,
            total_pages_unfiltered=total_pages,
        )
        seen: set[int] = set()
        ids = []
        for y in range(year_from, year_to + 1):
            year_ids = await _discover_one_year(client, media, year=y)
            for i in year_ids:
                if i not in seen:
                    seen.add(i)
                    ids.append(i)

    return [(media, i) for i in ids]


# ─── Phase 2: detail + credits ────────────────────────────────────────────


async def fetch_detail(
    client: TmdbClient, media_type: str, tmdb_id: int
) -> dict | None:
    return await client.get_json(detail_url(media_type, tmdb_id))


def _anime_row(rec: TmdbAnimeRecord) -> dict:
    """TmdbAnimeRecord → BRONZE row dict (drops the credits list)."""
    d = dataclasses.asdict(rec)
    d.pop("credits", None)
    return d


def _credit_row(c: TmdbCreditEntry) -> dict:
    d = dataclasses.asdict(c)
    role = d.get("role")
    if hasattr(role, "value"):
        d["role"] = role.value
    return d


# ─── Phase 3: person ──────────────────────────────────────────────────────


async def fetch_person(client: TmdbClient, tmdb_person_id: int) -> dict | None:
    return await client.get_json(person_url(tmdb_person_id))


def _person_row(rec: TmdbPersonRecord) -> dict:
    return dataclasses.asdict(rec)


# ─── CLI ──────────────────────────────────────────────────────────────────


app = make_scraper_app("tmdb")


@app.command()
def run(
    media: list[str] = typer.Option(
        ["tv", "movie"],
        "--media",
        help="Media types to scrape: tv, movie, or both.",
    ),
    year_from: int = typer.Option(
        1900, "--year-from", help="Earliest year used when year-split is required."
    ),
    year_to: int = typer.Option(
        0, "--year-to", help="Latest year (0 = current year)."
    ),
    skip_persons: bool = typer.Option(
        False, "--skip-persons", help="Skip phase 3 (person fanout)."
    ),
    limit: LimitOpt = 0,
    delay: DelayOpt = REQUEST_INTERVAL,
    resume: ResumeOpt = True,
    force: ForceOpt = False,
    progress: ProgressOpt = False,
    quiet: QuietOpt = False,
) -> None:
    """Run TMDB Animation BRONZE scrape (discover → detail → person)."""
    progress_override = resolve_progress_enabled(quiet, progress)

    for m in media:
        if m not in ("tv", "movie"):
            raise typer.BadParameter(f"--media must be tv or movie, got {m!r}")

    asyncio.run(
        _main(
            media_list=media,
            year_from=year_from,
            year_to=year_to or None,
            skip_persons=skip_persons,
            limit=limit,
            delay=delay,
            resume=resume,
            force=force,
            progress_override=progress_override,
        )
    )


async def _main(
    *,
    media_list: list[str],
    year_from: int,
    year_to: int | None,
    skip_persons: bool,
    limit: int,
    delay: float,
    resume: bool,
    force: bool,
    progress_override: bool | None,
) -> None:
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    client = TmdbClient(delay=delay)
    try:
        # Phase 1: discover
        all_anime_ids: list[tuple[str, int]] = []
        cp1 = Checkpoint.load(PHASE1_CHECKPOINT)
        if force:
            cp1.delete()
            cp1 = Checkpoint.load(PHASE1_CHECKPOINT)
        if resume and cp1.get("ids"):
            all_anime_ids = [tuple(x) for x in cp1["ids"]]
            log.info("tmdb_discover_resume", n=len(all_anime_ids))
        else:
            for m in media_list:
                ids = await discover_all_ids(
                    client, m, year_from=year_from, year_to=year_to
                )
                all_anime_ids.extend(ids)
            cp1["ids"] = [list(t) for t in all_anime_ids]
            cp1.save()
            log.info("tmdb_discover_done", total=len(all_anime_ids))

        if limit:
            all_anime_ids = all_anime_ids[:limit]

        # Phase 2: detail + credits
        person_ids_seen: set[int] = set()
        cp2 = Checkpoint.load(PHASE2_CHECKPOINT)
        if force:
            cp2.delete()
            cp2 = Checkpoint.load(PHASE2_CHECKPOINT)

        with BronzeWriterGroup("tmdb", tables=["anime", "credits"]) as g2:
            sink2 = BronzeSink(
                group=g2,
                mapper=lambda rec: {
                    "anime": [_anime_row(rec)],
                    "credits": [_credit_row(c) for c in rec.credits],
                },
            )

            async def fetcher2(item: tuple[str, int]) -> dict | None:
                m, tid = item
                raw = await fetch_detail(client, m, tid)
                if raw is None:
                    return None
                raw["__media_type__"] = m
                return raw

            def parser2(raw: dict, item: tuple[str, int]) -> TmdbAnimeRecord | None:
                m = raw.pop("__media_type__", item[0])
                rec = parse_tmdb_anime(raw, m)
                for c in rec.credits:
                    person_ids_seen.add(c.tmdb_person_id)
                return rec

            runner2 = ScrapeRunner(
                fetcher=fetcher2,
                parser=parser2,
                sink=sink2,
                checkpoint=cp2,
                label="tmdb_detail",
                flush=g2.flush_all,
                flush_every=200,
            )
            stats2 = await runner2.run(all_anime_ids, progress_override=progress_override)
            log.info("tmdb_detail_stats", **dataclasses.asdict(stats2))

        if skip_persons:
            log.info("tmdb_skip_persons")
            return

        # Phase 3: persons
        if not person_ids_seen:
            person_ids_seen = _load_person_ids_from_bronze()

        cp3 = Checkpoint.load(PHASE3_CHECKPOINT)
        if force:
            cp3.delete()
            cp3 = Checkpoint.load(PHASE3_CHECKPOINT)

        with BronzeWriterGroup("tmdb", tables=["persons"]) as g3:
            sink3 = BronzeSink(
                group=g3,
                mapper=lambda rec: {"persons": [_person_row(rec)]},
                add_hash=False,
            )

            async def fetcher3(pid: int) -> dict | None:
                return await fetch_person(client, pid)

            def parser3(raw: dict, _pid: int) -> TmdbPersonRecord | None:
                return parse_tmdb_person(raw)

            runner3 = ScrapeRunner(
                fetcher=fetcher3,
                parser=parser3,
                sink=sink3,
                checkpoint=cp3,
                label="tmdb_person",
                flush=g3.flush_all,
                flush_every=200,
            )
            stats3 = await runner3.run(
                sorted(person_ids_seen), progress_override=progress_override
            )
            log.info("tmdb_person_stats", **dataclasses.asdict(stats3))
    finally:
        await client.aclose()


def _load_person_ids_from_bronze() -> set[int]:
    """Read all tmdb_person_id values from the credits BRONZE partitions."""
    import duckdb

    from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT

    pattern = f"{DEFAULT_BRONZE_ROOT}/source=tmdb/table=credits/date=*/*.parquet"
    try:
        rows = duckdb.sql(
            f"SELECT DISTINCT tmdb_person_id FROM read_parquet('{pattern}')"
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.warning("tmdb_credits_bronze_missing", error=str(exc))
        return set()
    return {int(r[0]) for r in rows if r[0] is not None}


if __name__ == "__main__":
    app()
