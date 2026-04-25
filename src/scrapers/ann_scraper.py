"""Anime News Network Encyclopedia scraper.

3-phase pipeline:
  Phase 1 (masterlist): fetch all anime IDs from CDN
  Phase 2 (anime):      fetch staff credits via XML API per anime ID
  Phase 3 (persons):    fetch person metadata via HTML pages (people.php?id=N)

Data sources:
  Masterlist : https://cdn.animenewsnetwork.com/encyclopedia/reports.xml?tag=masterlist&nlist=all
  Anime XML  : https://www.animenewsnetwork.com/encyclopedia/api.xml?anime=<id>
  People XML : https://www.animenewsnetwork.com/encyclopedia/api.xml?people=<id>

Rate limit: ANN has no published limit. Default 1 req/sec with exponential backoff on 429/503.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import httpx
import structlog
import typer

from src.scrapers.hash_utils import hash_anime_data

from src.runtime.models import parse_role
from src.scrapers.cli_common import (
    CheckpointIntervalOpt,
    DataDirOpt,
    DelayOpt,
    LimitOpt,
    ProgressOpt,
    QuietOpt,
    ResumeOpt,
    resolve_progress_enabled,
)
from src.scrapers.http_base import RateLimitedHttpClient
from src.scrapers.logging_utils import configure_file_logging
from src.scrapers.progress import scrape_progress
from src.scrapers.parsers.ann import (  # noqa: F401
    _ANN_TYPE_MAP,
    _HTML_LABEL_MAP,
    _normalize_format,
    _parse_dob_html,
    _parse_vintage,
    AnnStaffEntry,
    AnnAnimeRecord,
    AnnPersonDetail,
    AnimeXmlParseResult,
    parse_anime_xml,
    parse_person_xml,
    parse_person_html,
)
from src.utils.config import SCRAPE_CHECKPOINT_INTERVAL, SCRAPE_DELAY_SECONDS

log = structlog.get_logger()

# ─── URLs ────────────────────────────────────────────────────────────────────
MASTERLIST_URL = (
    "https://cdn.animenewsnetwork.com/encyclopedia/reports.xml?tag=masterlist&nlist=all"
)
ANIME_API_URL = "https://www.animenewsnetwork.com/encyclopedia/api.xml"
PEOPLE_API_URL = ANIME_API_URL  # same XML endpoint as anime: ?people=ID (currently returns "ignored")
PEOPLE_HTML_BASE = "https://www.animenewsnetwork.com/encyclopedia/people.php"

# XML API supports up to 50 IDs per request, slash-delimited
BATCH_SIZE = 50

DEFAULT_DELAY = max(SCRAPE_DELAY_SECONDS, 1.5)  # ANN minimum 1.5s between requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AnimtorEval/1.0; "
        "+https://github.com/kashi-x/animetor_eval)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
}

DEFAULT_DATA_DIR = Path("data/ann")

app = typer.Typer()


# ─── HTTP client ────────────────────────────────────────────────────────────


class AnnClient(RateLimitedHttpClient):
    """Async HTTP client for ANN with exponential backoff retry."""

    def __init__(self, delay: float = DEFAULT_DELAY) -> None:
        super().__init__(delay=delay)
        self._last_request = 0.0
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request
        wait = self._delay - elapsed
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_request = time.monotonic()

    # HTTP status codes that trigger exponential backoff retry.
    # 429: rate limit / 500-504: transient server errors / 522,524: Cloudflare timeout
    _RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504, 522, 524})

    # httpx exceptions treated as transient network failures
    _RETRYABLE_EXC = (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.ReadError,
        httpx.RemoteProtocolError,
        httpx.PoolTimeout,
    )

    async def get(
        self,
        url: str,
        params: dict | None = None,
        *,
        max_attempts: int = 8,
    ) -> httpx.Response:
        """GET with exponential backoff retry on transient failures.

        Retries on:
          - HTTP 429, 500-504, 522, 524
          - httpx Timeout/Connect/Read/RemoteProtocol/PoolTimeout exceptions
        Propagates the last exception (or response) after max_attempts.
        """
        backoff = 4.0
        attempt = 0
        while True:
            attempt += 1
            await self._throttle()
            try:
                resp = await self._client.get(url, params=params)
            except self._RETRYABLE_EXC as exc:
                if attempt >= max_attempts:
                    log.error(
                        "ann_request_giveup",
                        url=url,
                        error_type=type(exc).__name__,
                        error=str(exc),
                        attempts=attempt,
                    )
                    raise
                wait = min(backoff, 120)
                log.warning(
                    "ann_request_error",
                    url=url,
                    error_type=type(exc).__name__,
                    error=str(exc),
                    attempt=attempt,
                    max_attempts=max_attempts,
                    wait_s=wait,
                )
                await asyncio.sleep(wait)
                backoff *= 2
                continue

            if resp.status_code in self._RETRYABLE_STATUS:
                raw_ra = resp.headers.get("Retry-After", "")
                try:
                    retry_after = max(int(raw_ra), 5)
                except (ValueError, TypeError):
                    retry_after = int(min(backoff * 2, 300))
                if attempt >= max_attempts:
                    log.error(
                        "ann_rate_giveup",
                        url=url,
                        status=resp.status_code,
                        attempts=attempt,
                    )
                    resp.raise_for_status()
                    return resp
                log.warning(
                    "ann_rate_limited",
                    url=url,
                    status=resp.status_code,
                    wait_s=retry_after,
                    attempt=attempt,
                    max_attempts=max_attempts,
                )
                await asyncio.sleep(retry_after)
                backoff = min(max(backoff * 2, retry_after), 300)
                continue

            resp.raise_for_status()
            return resp


# ─── Phase 1: masterlist fetch ───────────────────────────────────────────────


async def fetch_masterlist(client: AnnClient) -> list[int]:
    """Fetch all anime IDs from the CDN masterlist XML.

    Falls back to _probe_max_id sequential list if the CDN endpoint returns HTML
    (URL changed or blocked).
    """
    log.info("ann_masterlist_fetch_start", url=MASTERLIST_URL)
    try:
        resp = await client.get(MASTERLIST_URL)
        text = resp.text.lstrip()
        if not text.startswith("<") or text.lstrip("<").startswith("!DOCTYPE"):
            raise ValueError("HTML response — masterlist endpoint returned non-XML")
        root = ET.fromstring(text)
        ids: list[int] = []
        for item in root.findall(".//item"):
            if item.get("type", "") != "anime":
                continue
            try:
                ids.append(int(item.get("id", "")))
            except (ValueError, TypeError):
                continue
        if ids:
            log.info("ann_masterlist_fetched", total=len(ids))
            return ids
        raise ValueError("masterlist returned 0 anime items")
    except (ET.ParseError, ValueError) as exc:
        log.warning("ann_masterlist_fallback", reason=str(exc))

    return await _probe_max_id(client)


async def _probe_max_id(client: AnnClient) -> list[int]:
    """Probe the API to estimate the current maximum anime ID and return a sequential list.

    ANN anime IDs are roughly sequential with gaps; gaps are silently skipped during fetching.
    Upper bound as of 2026 is ~27000.
    """
    # probe near the known high watermark to find the current ceiling
    known_high = 27000
    probe_ids = list(range(known_high - 49, known_high + 1))
    try:
        resp = await client.get(
            f"{ANIME_API_URL}?anime={'/'.join(str(i) for i in probe_ids)}"
        )
        root = ET.fromstring(resp.text)
        found_ids = [int(el.get("id")) for el in root.findall("anime") if el.get("id")]
        if found_ids:
            max_found = max(found_ids)
            max_id = max_found + 500  # add buffer above highest found ID
        else:
            max_id = known_high
    except Exception:
        max_id = known_high

    ids = list(range(1, max_id + 1))
    log.info("ann_masterlist_sequential_fallback", max_id=max_id, total=len(ids))
    return ids


async def fetch_anime_batch(
    client: AnnClient,
    ann_ids: list[int],
) -> AnimeXmlParseResult:
    """Fetch up to BATCH_SIZE anime IDs from the XML API in one request."""
    ids_str = "/".join(str(i) for i in ann_ids)
    resp = await client.get(f"{ANIME_API_URL}?anime={ids_str}")
    text = resp.text.lstrip()
    if not text.startswith("<") or text.lstrip("<").startswith("!DOCTYPE"):
        log.warning("ann_anime_html_response", ids_sample=ann_ids[:3])
        return AnimeXmlParseResult()
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        log.error("ann_xml_parse_error", ids=ann_ids[:5], error=str(exc))
        return AnimeXmlParseResult()
    return parse_anime_xml(root)


async def fetch_person_batch(
    client: AnnClient,
    ann_ids: list[int],
) -> list[AnnPersonDetail]:
    """Fetch up to BATCH_SIZE person details from the ANN XML API.

    NOTE: As of 2026-04-23, ?people=ID returns <warning>ignored</warning>.
    Phase 3 uses fetch_person_html() instead.
    """
    ids_str = "/".join(str(i) for i in ann_ids)
    resp = await client.get(f"{PEOPLE_API_URL}?people={ids_str}")
    text = resp.text.strip()
    if text.lstrip().startswith("<!"):
        log.warning("ann_people_html_response", ids_sample=ann_ids[:3])
        return []
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        log.error("ann_people_xml_parse_error", ids=ann_ids[:5], error=str(exc))
        return []
    return parse_person_xml(root)


# ─── Phase 3 (HTML): person HTML scraping ────────────────────────────────────

async def fetch_person_html(
    client: AnnClient,
    ann_id: int,
) -> AnnPersonDetail | None:
    """Fetch one person's detail from the ANN HTML page (people.php?id=NUM).

    Replacement for the XML API (?people=ID) which returns <warning>ignored</warning>.
    One request per person — batch fetching is not available via HTML.
    """
    try:
        resp = await client.get(PEOPLE_HTML_BASE, params={"id": ann_id})
    except Exception as exc:
        log.warning("ann_person_html_fetch_error", ann_id=ann_id, error=str(exc))
        return None
    if resp.status_code in (403, 404):
        log.debug("ann_person_not_found", ann_id=ann_id, status=resp.status_code)
        return None
    result = parse_person_html(resp.text, ann_id)
    if result is None:
        log.debug("ann_person_html_parse_failed", ann_id=ann_id)
    return result


# ─── Bronze write helpers ────────────────────────────────────────────────────


_ANIME_BRONZE_TABLES = (
    "anime", "credits", "cast", "company",
    "episodes", "releases", "news", "related",
)


class _AnimeBronzeWriters:
    """Attribute-style facade over BronzeWriterGroup for the 8 anime tables.

    Preserves `writers.anime.append(...)` callsites while delegating
    flush/compact to the shared group lifecycle.
    """

    def __init__(self, root: "Path | str | None" = None) -> None:
        from pathlib import Path as _Path
        from src.scrapers.bronze_writer import BronzeWriterGroup
        self._group = BronzeWriterGroup("ann", tables=list(_ANIME_BRONZE_TABLES), root=root)
        for tbl in _ANIME_BRONZE_TABLES:
            setattr(self, tbl, self._group[tbl])

    def flush_all(self) -> None:
        self._group.flush_all()

    def compact_all(self) -> None:
        self._group.compact_all()


def save_anime_parse_result(
    writers: _AnimeBronzeWriters,
    result: AnimeXmlParseResult,
) -> tuple[int, int]:
    """Write all tables from one XML parse result to BRONZE parquet.

    Returns (n_anime, n_credits) for progress logging.
    """
    n_anime = 0
    n_credits = 0
    for rec in result.anime:
        anime_row = dataclasses.asdict(rec)
        anime_row.pop("staff", None)
        anime_row["fetched_at"] = datetime.now(timezone.utc).isoformat()
        anime_row["content_hash"] = hash_anime_data(anime_row)
        writers.anime.append(anime_row)
        n_anime += 1
        for entry in rec.staff:
            credit_row = dataclasses.asdict(entry)
            credit_row["ann_anime_id"] = rec.ann_id
            credit_row["role"] = parse_role(entry.task)
            writers.credits.append(credit_row)
            n_credits += 1
    for cast in result.cast:
        writers.cast.append(dataclasses.asdict(cast))
    for comp in result.company:
        writers.company.append(dataclasses.asdict(comp))
    for ep in result.episodes:
        writers.episodes.append(dataclasses.asdict(ep))
    for rel in result.releases:
        writers.releases.append(dataclasses.asdict(rel))
    for news in result.news:
        writers.news.append(dataclasses.asdict(news))
    for r in result.related:
        writers.related.append(dataclasses.asdict(r))
    return n_anime, n_credits


def save_person_detail(persons_bw, detail: AnnPersonDetail) -> None:
    """Write AnnPersonDetail to BRONZE parquet."""
    persons_bw.append(dataclasses.asdict(detail))


# ─── Checkpoint helpers ──────────────────────────────────────────────────────


def _load_checkpoint(path: Path) -> dict:
    from src.scrapers.checkpoint import load_json_or
    return load_json_or(path, {})


def _save_checkpoint(path: Path, data: dict) -> None:
    from src.scrapers.checkpoint import atomic_write_json
    atomic_write_json(path, data, indent=2)


# ─── typer commands ──────────────────────────────────────────────────────────


@app.command("scrape-anime")
def cmd_scrape_anime(
    limit: LimitOpt = 0,
    batch_size: int = typer.Option(BATCH_SIZE, "--batch-size", help="XML API batch size"),
    delay: DelayOpt = DEFAULT_DELAY,
    checkpoint_interval: CheckpointIntervalOpt = SCRAPE_CHECKPOINT_INTERVAL,
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    resume: ResumeOpt = True,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Phase 1+2: fetch masterlist → scrape anime XML."""
    log_path = configure_file_logging("ann")
    log.info("ann_scrape_anime_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_scrape_anime(
            limit=limit,
            batch_size=batch_size,
            delay=delay,
            checkpoint_interval=checkpoint_interval,
            data_dir=data_dir,
            resume=resume,
            progress_override=resolve_progress_enabled(quiet, progress),
        )
    )


async def _run_scrape_anime(
    limit: int,
    batch_size: int,
    delay: float,
    checkpoint_interval: int,
    data_dir: Path,
    resume: bool,
    progress_override: bool | None = None,
) -> None:
    cp_path = data_dir / "anime_checkpoint.json"
    cp = _load_checkpoint(cp_path) if resume else {}

    writers = _AnimeBronzeWriters()

    client = AnnClient(delay=delay)
    try:
        if "all_ids" in cp:
            all_ids: list[int] = cp["all_ids"]
            log.info("ann_masterlist_from_checkpoint", count=len(all_ids))
        else:
            all_ids = await fetch_masterlist(client)
            cp["all_ids"] = all_ids
            _save_checkpoint(cp_path, cp)

        completed: set[int] = set(cp.get("completed_ids", []))
        pending = [i for i in all_ids if i not in completed]
        if limit:
            pending = pending[:limit]

        log.info(
            "ann_anime_scrape_start",
            total=len(all_ids),
            completed=len(completed),
            pending=len(pending),
        )

        total_anime = 0
        total_credits = 0
        done_this_run = 0
        flush_every = max(1, checkpoint_interval // batch_size)
        batches = [pending[i : i + batch_size] for i in range(0, len(pending), batch_size)]

        with scrape_progress(
            total=len(pending),
            description="scraping ANN anime",
            enabled=progress_override,
        ) as p:
            for batch_idx, batch in enumerate(batches):
                result = await fetch_anime_batch(client, batch)
                n_a, n_c = save_anime_parse_result(writers, result)
                total_anime += n_a
                total_credits += n_c
                for ann_id in batch:
                    completed.add(ann_id)
                done_this_run += len(batch)
                p.advance(len(batch))

                if (batch_idx + 1) % flush_every == 0:
                    writers.flush_all()
                    cp["completed_ids"] = list(completed)
                    _save_checkpoint(cp_path, cp)
                    p.log(
                        "ann_anime_progress",
                        done=done_this_run,
                        remaining=len(pending) - done_this_run,
                        total_anime=total_anime,
                        total_credits=total_credits,
                    )

        writers.flush_all()
        cp["completed_ids"] = list(completed)
        _save_checkpoint(cp_path, cp)
        log.info(
            "ann_anime_scrape_done",
            total_anime=total_anime,
            total_credits=total_credits,
        )
        writers.compact_all()

    finally:
        await client.close()


@app.command("scrape-persons")
def cmd_scrape_persons(
    limit: LimitOpt = 0,
    batch_size: int = typer.Option(BATCH_SIZE, "--batch-size", help="(unused; HTML is per-ID)"),
    delay: DelayOpt = DEFAULT_DELAY,
    checkpoint_interval: CheckpointIntervalOpt = SCRAPE_CHECKPOINT_INTERVAL,
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    resume: ResumeOpt = True,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Phase 3: scrape HTML pages for all persons with ann_id in the DB.

    Uses people.php?id=NUM HTML scraping because the XML API (?people=ID)
    returns <warning>ignored</warning>. One request per person; throughput ~40/min at 1.5s intervals.
    """
    configure_file_logging("ann")
    asyncio.run(
        _run_scrape_persons(
            limit=limit,
            batch_size=batch_size,
            delay=delay,
            checkpoint_interval=checkpoint_interval,
            data_dir=data_dir,
            resume=resume,
            progress_override=resolve_progress_enabled(quiet, progress),
        )
    )


async def _run_scrape_persons(
    limit: int,
    batch_size: int,  # noqa: ARG001 — unused; HTML scraping is per-ID, no batching
    delay: float,
    checkpoint_interval: int,
    data_dir: Path,
    resume: bool,
    progress_override: bool | None = None,
) -> None:
    import pyarrow.dataset as ds

    from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT, BronzeWriter

    cp_path = data_dir / "persons_checkpoint.json"
    cp = _load_checkpoint(cp_path) if resume else {}
    completed: set[int] = set(cp.get("completed_ids", []))

    # Read ann_person_id list from bronze parquet (written by anime phase)
    credits_path = DEFAULT_BRONZE_ROOT / "source=ann" / "table=credits"
    if not credits_path.exists():
        log.warning("ann_persons_no_credits", msg="Run scrape-anime phase first")
        return

    credits_ds = ds.dataset(credits_path, format="parquet")
    tbl = credits_ds.to_table(columns=["ann_person_id"])
    all_ann_ids: list[int] = list(
        dict.fromkeys(pid for pid in tbl.column("ann_person_id").to_pylist() if pid is not None)
    )

    pending = [i for i in all_ann_ids if i not in completed]
    if limit:
        pending = pending[:limit]

    log.info(
        "ann_persons_scrape_start",
        total=len(all_ann_ids),
        completed=len(completed),
        pending=len(pending),
        method="html",
    )

    done_this_run = 0
    collected = 0
    persons_bw = BronzeWriter("ann", table="persons")
    client = AnnClient(delay=delay)
    try:
        with scrape_progress(
            total=len(pending),
            description="scraping ANN persons",
            enabled=progress_override,
        ) as p:
            for ann_id in pending:
                detail = await fetch_person_html(client, ann_id)
                if detail is not None:
                    save_person_detail(persons_bw, detail)
                    collected += 1
                completed.add(ann_id)
                done_this_run += 1
                p.advance()

                if done_this_run % checkpoint_interval == 0:
                    persons_bw.flush()
                    cp["completed_ids"] = list(completed)
                    _save_checkpoint(cp_path, cp)
                    p.log(
                        "ann_persons_progress",
                        done=done_this_run,
                        collected=collected,
                        remaining=len(pending) - done_this_run,
                    )

        persons_bw.flush()
        persons_bw.compact()
        cp["completed_ids"] = list(completed)
        _save_checkpoint(cp_path, cp)
        log.info("ann_persons_scrape_done", total_requested=done_this_run, collected=collected)

    finally:
        await client.close()


@app.command("scrape-all")
def cmd_scrape_all(
    limit: LimitOpt = 0,
    delay: DelayOpt = DEFAULT_DELAY,
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Phase 1-3 を順番に実行する."""
    log_path = configure_file_logging("ann")
    log.info("ann_scrape_all_command_start", log_file=str(log_path), limit=limit)
    progress_override = resolve_progress_enabled(quiet, progress)
    asyncio.run(
        _run_scrape_anime(
            limit=limit,
            batch_size=BATCH_SIZE,
            delay=delay,
            checkpoint_interval=SCRAPE_CHECKPOINT_INTERVAL,
            data_dir=data_dir,
            resume=True,
            progress_override=progress_override,
        )
    )
    asyncio.run(
        _run_scrape_persons(
            limit=0,
            batch_size=BATCH_SIZE,
            delay=delay,
            checkpoint_interval=SCRAPE_CHECKPOINT_INTERVAL,
            data_dir=data_dir,
            resume=True,
            progress_override=progress_override,
        )
    )


if __name__ == "__main__":
    app()
