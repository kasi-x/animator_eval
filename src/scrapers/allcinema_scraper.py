"""allcinema.net scraper.

3-phase structure:
  Phase 1 (sitemap):  retrieve all cinema/person IDs from sitemap XML.gz files
  Phase 2 (cinema):   extract anime staff info from CreditJson embedded in each cinema page
  Phase 3 (persons):  fetch name and yomigana for each person ID collected in Phase 2

Data sources:
  Sitemap (cinema) : https://www.allcinema.net/sitemap_c{N}.xml.gz  (3 files)
  Sitemap (person) : https://www.allcinema.net/sitemap_p{N}.xml.gz  (12 files)
  Cinema page      : https://www.allcinema.net/cinema/{id}   (HTML, CreditJson embedded)
  Person page      : https://www.allcinema.net/person/{id}   (HTML, name + yomigana)

Rate limit: no Disallow in robots.txt. Running at ≤2 req/sec.
Copyright: Stingray Co., Ltd. Credit data used as publicly disclosed facts.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gzip
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from src.scrapers.hash_utils import hash_anime_data
from xml.etree import ElementTree as ET

import httpx
import structlog
import typer

from src.scrapers.http_client import RetryingHttpClient
from src.scrapers.logging_utils import configure_file_logging
from src.scrapers.parsers.allcinema import (  # noqa: F401
    AllcinemaCredit,
    AllcinemaAnimeRecord,
    AllcinemaPersonRecord,
    _parse_cinema_html,
    _parse_person_html,
)
from src.utils.config import SCRAPE_CHECKPOINT_INTERVAL, SCRAPE_DELAY_SECONDS

log = structlog.get_logger()

app = typer.Typer()

# ─── URLs ────────────────────────────────────────────────────────────────────

SITE_BASE = "https://www.allcinema.net"
CINEMA_BASE = f"{SITE_BASE}/cinema/"
PERSON_BASE = f"{SITE_BASE}/person/"
AJAX_PERSON = f"{SITE_BASE}/ajax/person"

SITEMAP_CINEMA_PATTERN = f"{SITE_BASE}/sitemap_c{{n}}.xml.gz"
SITEMAP_PERSON_PATTERN = f"{SITE_BASE}/sitemap_p{{n}}.xml.gz"

# allcinema の cinema サイトマップは 3 ファイル、person は 12 ファイル (2026/04 時点)
SITEMAP_CINEMA_COUNT = 3
SITEMAP_PERSON_COUNT = 12

DEFAULT_DELAY = max(SCRAPE_DELAY_SECONDS, 2.0)
DEFAULT_DATA_DIR = Path("data/allcinema")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AnimtorEval/1.0; "
        "+https://github.com/kashi-x/animetor_eval)"
    ),
    "Accept-Language": "ja,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

# Mapping from allcinema jobname → internal Role string
# jobid is included for reference but the mapping is jobname-based
_JOB_ROLE_MAP: dict[str, str] = {
    "監督": "director",
    "総監督": "director",
    "副監督": "episode_director",
    "シリーズディレクター": "director",
    "演出": "episode_director",
    "絵コンテ": "episode_director",
    "シリーズ構成": "screenplay",
    "脚本": "screenplay",
    "キャラクターデザイン": "character_designer",
    "キャラクター原案": "original_creator",
    "総作画監督": "animation_director",
    "作画監督": "animation_director",
    "原画": "key_animator",
    "第二原画": "second_key_animator",
    "動画": "in_between",
    "動画チェック": "finishing",
    "仕上げ": "finishing",
    "色彩設計": "finishing",
    "撮影監督": "photography_director",
    "３ＤＣＧ監督": "cgi_director",
    "CGディレクター": "cgi_director",
    "美術監督": "background_art",
    "美術設定": "background_art",
    "背景": "background_art",
    "音楽": "music",
    "音響監督": "sound_director",
    "音響効果": "sound_director",
    "録音調整": "sound_director",
    "プロデューサー": "producer",
    "エグゼクティブプロデューサー": "producer",
    "制作プロデューサー": "producer",
    "アニメーションプロデューサー": "producer",
    "アニメーション制作": "production_manager",
    "制作": "production_manager",
    "制作進行": "production_manager",
    "製作総指揮": "producer",
    "原作": "original_creator",
    "出演": "voice_actor",
    "声の出演": "voice_actor",
}


# ─── HTTP client ─────────────────────────────────────────────────────────────


class AllcinemaClient:
    """Async HTTP client for allcinema.

    GET requests are delegated to the shared RetryingHttpClient (http_client.py).
    CSRF-bearing POST (/ajax/person) uses custom handling for 419 retries etc.
    """

    def __init__(self, delay: float = DEFAULT_DELAY, transport=None) -> None:
        self._csrf_token: str = ""
        self._http = RetryingHttpClient(
            source="allcinema",
            delay=delay,
            timeout=30.0,
            headers=HEADERS,
            transport=transport,
        )
        # Expose the underlying httpx Client so post_ajax can share it
        self._client = self._http._client

    async def close(self) -> None:
        await self._http.aclose()

    async def get(self, url: str) -> httpx.Response:
        """GET request. 5xx/429 are retried by the shared client; 404 is handled by the caller."""
        resp = await self._http.get(url)
        if resp.status_code == 404:
            return resp
        resp.raise_for_status()
        if resp.status_code == 200:
            m = re.search(r'name="csrf-token"\s+content="([^"]+)"', resp.text)
            if m:
                self._csrf_token = m.group(1)
        return resp

    async def post_ajax(self, ajax_data: str, key: int, page: int = 1) -> dict:
        """Hit the /ajax/person endpoint via a CSRF-authenticated POST."""
        if not self._csrf_token:
            # GET the person page first to obtain the CSRF token
            await self.get(f"{PERSON_BASE}{key}")

        backoff = 4.0
        attempt = 0
        while True:
            await self._throttle()
            try:
                resp = await self._client.post(
                    AJAX_PERSON,
                    data={"ajax_data": ajax_data, "key": str(key), "page": str(page)},
                    headers={
                        "X-CSRF-TOKEN": self._csrf_token,
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": f"{PERSON_BASE}{key}",
                    },
                )
            except (httpx.TimeoutException, httpx.ConnectError):
                if attempt >= 5:
                    raise
                await asyncio.sleep(min(backoff, 120))
                backoff *= 2
                attempt += 1
                continue

            if resp.status_code in (429, 503):
                wait = min(backoff * 2, 300)
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, 300)
                attempt += 1
                continue

            if resp.status_code == 419:  # CSRF mismatch → refresh token
                log.info("allcinema_csrf_refresh", key=key)
                await self.get(f"{PERSON_BASE}{key}")
                attempt += 1
                continue

            resp.raise_for_status()
            return resp.json()


# ─── Phase 1: Sitemap retrieval ──────────────────────────────────────────────


async def fetch_sitemap_ids(
    client: AllcinemaClient,
    pattern: str,
    count: int,
) -> list[int]:
    """Retrieve a list of IDs from a sitemap XML.gz file."""
    ids: list[int] = []
    for n in range(1, count + 1):
        url = pattern.format(n=n)
        log.info("allcinema_sitemap_fetch", url=url)
        try:
            resp = await client.get(url)
            if resp.status_code == 404:
                log.info("allcinema_sitemap_end", n=n)
                break
            gz_data = gzip.decompress(resp.content)
            root = ET.fromstring(gz_data)
            ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for loc in root.findall(".//s:loc", ns):
                if loc.text:
                    m = re.search(r"/(\d+)$", loc.text.strip())
                    if m:
                        ids.append(int(m.group(1)))
            log.info("allcinema_sitemap_done", n=n, found=len(ids))
        except Exception as exc:
            log.error("allcinema_sitemap_error", n=n, error=str(exc))
    return ids


async def scrape_cinema(
    client: AllcinemaClient,
    cinema_id: int,
) -> AllcinemaAnimeRecord | None:
    """Scrape a single cinema page and return the record, or None for 404 or non-anime."""
    url = f"{CINEMA_BASE}{cinema_id}"
    resp = await client.get(url)
    if resp.status_code == 404:
        return None
    return _parse_cinema_html(resp.text, cinema_id)


async def scrape_person(
    client: AllcinemaClient,
    person_id: int,
) -> AllcinemaPersonRecord | None:
    """Scrape a single person page and return the record, or None for 404."""
    url = f"{PERSON_BASE}{person_id}"
    resp = await client.get(url)
    if resp.status_code == 404:
        return None
    return _parse_person_html(resp.text, person_id)


# ─── Bronze writes ────────────────────────────────────────────────────────────


def save_anime_record(anime_bw, credits_bw, rec: AllcinemaAnimeRecord) -> int:
    """Write an AllcinemaAnimeRecord to BRONZE and return the credit count."""
    anime_row = dataclasses.asdict(rec)
    # credits are in staff/cast lists — write credits separately
    all_credits = rec.staff + rec.cast
    anime_row.pop("staff", None)
    anime_row.pop("cast", None)
    # Add hash tracking for diff detection
    anime_row["fetched_at"] = datetime.now(timezone.utc).isoformat()
    anime_row["content_hash"] = hash_anime_data(anime_row)
    anime_bw.append(anime_row)
    for credit_entry in all_credits:
        credit_row = dataclasses.asdict(credit_entry)
        credit_row["cinema_id"] = rec.cinema_id
        credits_bw.append(credit_row)
    return len(all_credits)


def save_person_record(persons_bw, rec: AllcinemaPersonRecord) -> None:
    """Write an AllcinemaPersonRecord to BRONZE."""
    persons_bw.append(dataclasses.asdict(rec))


# ─── Checkpoint ──────────────────────────────────────────────────────────────


def _load_checkpoint(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def _save_checkpoint(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


# ─── Phase 2 execution ───────────────────────────────────────────────────────


async def _run_scrape_cinema(
    limit: int,
    batch_save: int,
    delay: float,
    data_dir: Path,
) -> None:
    from src.scrapers.bronze_writer import BronzeWriter

    ckpt_path = data_dir / "checkpoint_cinema.json"
    ckpt = _load_checkpoint(ckpt_path)

    completed: set[int] = set(ckpt.get("completed", []))
    cinema_ids: list[int] = ckpt.get("cinema_ids", [])

    anime_bw = BronzeWriter("allcinema", table="anime")
    credits_bw = BronzeWriter("allcinema", table="credits")

    client = AllcinemaClient(delay=delay)
    done_this_run = 0
    anime_count = 0
    credit_count = 0
    try:
        if not cinema_ids:
            log.info("allcinema_cinema_sitemap_start")
            cinema_ids = await fetch_sitemap_ids(
                client, SITEMAP_CINEMA_PATTERN, SITEMAP_CINEMA_COUNT
            )
            ckpt["cinema_ids"] = cinema_ids
            _save_checkpoint(ckpt_path, ckpt)
            log.info("allcinema_cinema_ids_total", total=len(cinema_ids))

        pending = [cid for cid in cinema_ids if cid not in completed]
        if limit > 0:
            pending = pending[:limit]

        total = len(pending)

        log.info(
            "allcinema_cinema_scrape_start", pending=total, completed=len(completed)
        )

        for cinema_id in pending:
            rec = await scrape_cinema(client, cinema_id)
            completed.add(cinema_id)
            done_this_run += 1

            if rec is not None:
                n_credits = save_anime_record(anime_bw, credits_bw, rec)
                anime_count += 1
                credit_count += n_credits

            if done_this_run % batch_save == 0:
                anime_bw.flush()
                credits_bw.flush()
                ckpt["completed"] = list(completed)
                _save_checkpoint(ckpt_path, ckpt)
                remaining = total - done_this_run
                log.info(
                    "allcinema_cinema_progress",
                    done=done_this_run,
                    remaining=remaining,
                    anime=anime_count,
                    credits=credit_count,
                )

        anime_bw.flush()
        credits_bw.flush()
        ckpt["completed"] = list(completed)
        _save_checkpoint(ckpt_path, ckpt)

    finally:
        await client.close()

    log.info(
        "allcinema_cinema_done",
        total_scraped=done_this_run,
        anime_found=anime_count,
        credits=credit_count,
    )


# ─── Phase 3 execution ───────────────────────────────────────────────────────


async def _run_scrape_persons(
    limit: int,
    batch_save: int,
    delay: float,
    data_dir: Path,
) -> None:
    import pyarrow.dataset as ds

    from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT, BronzeWriter

    ckpt_person_path = data_dir / "checkpoint_persons.json"
    ckpt_person = _load_checkpoint(ckpt_person_path)

    completed_persons: set[int] = set(ckpt_person.get("completed", []))

    # Read person IDs from bronze parquet (written by cinema phase)
    credits_path = DEFAULT_BRONZE_ROOT / "source=allcinema" / "table=credits"
    if not credits_path.exists():
        log.warning(
            "allcinema_persons_no_credits",
            msg="Run cinema phase first to populate credits",
        )
        return

    credits_ds = ds.dataset(credits_path, format="parquet")
    tbl = credits_ds.to_table(columns=["allcinema_person_id"])
    all_person_ids: list[int] = tbl.column("allcinema_person_id").to_pylist()
    all_person_ids = list(dict.fromkeys(pid for pid in all_person_ids if pid is not None))

    if not all_person_ids:
        log.warning(
            "allcinema_persons_no_credits",
            msg="Run cinema phase first to populate credits",
        )
        return

    pending = [pid for pid in all_person_ids if pid not in completed_persons]
    if limit > 0:
        pending = pending[:limit]

    total = len(pending)
    log.info("allcinema_persons_start", pending=total, completed=len(completed_persons))

    persons_bw = BronzeWriter("allcinema", table="persons")
    client = AllcinemaClient(delay=delay)
    done_this_run = 0
    try:
        for allcinema_pid in pending:
            rec = await scrape_person(client, allcinema_pid)
            completed_persons.add(allcinema_pid)
            done_this_run += 1

            if rec is not None:
                save_person_record(persons_bw, rec)

            if done_this_run % batch_save == 0:
                persons_bw.flush()
                ckpt_person["completed"] = list(completed_persons)
                _save_checkpoint(ckpt_person_path, ckpt_person)
                log.info(
                    "allcinema_persons_progress",
                    done=done_this_run,
                    remaining=total - done_this_run,
                )

        persons_bw.flush()
        ckpt_person["completed"] = list(completed_persons)
        _save_checkpoint(ckpt_person_path, ckpt_person)

    finally:
        await client.close()

    log.info("allcinema_persons_done", total_scraped=done_this_run)


# ─── CLI ─────────────────────────────────────────────────────────────────────


@app.command("sitemap")
def cmd_sitemap(
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, help="checkpoint save directory"),
) -> None:
    """Fetch the cinema/person ID list from sitemaps and save to files."""
    log_path = configure_file_logging("allcinema")
    log.info("allcinema_sitemap_command_start", log_file=str(log_path))

    async def _run() -> None:
        client = AllcinemaClient()
        try:
            cinema_ids = await fetch_sitemap_ids(
                client, SITEMAP_CINEMA_PATTERN, SITEMAP_CINEMA_COUNT
            )
            person_ids = await fetch_sitemap_ids(
                client, SITEMAP_PERSON_PATTERN, SITEMAP_PERSON_COUNT
            )
        finally:
            await client.close()

        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "cinema_ids.json").write_text(json.dumps(cinema_ids))
        (data_dir / "person_ids.json").write_text(json.dumps(person_ids))
        typer.echo(f"cinema: {len(cinema_ids)}, persons: {len(person_ids)}")

    asyncio.run(_run())


@app.command("cinema")
def cmd_cinema(
    limit: int = typer.Option(0, help="0=all"),
    batch_save: int = typer.Option(SCRAPE_CHECKPOINT_INTERVAL, help="commit interval"),
    delay: float = typer.Option(DEFAULT_DELAY, help="request interval (seconds)"),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR),
) -> None:
    """Phase 2: scrape cinema pages and save anime and credits to BRONZE."""
    log_path = configure_file_logging("allcinema")
    log.info("allcinema_cinema_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_scrape_cinema(
            limit=limit,
            batch_save=batch_save,
            delay=delay,
            data_dir=data_dir,
        )
    )


@app.command("persons")
def cmd_persons(
    limit: int = typer.Option(0, help="0=all"),
    batch_save: int = typer.Option(SCRAPE_CHECKPOINT_INTERVAL, help="commit interval"),
    delay: float = typer.Option(DEFAULT_DELAY, help="request interval (seconds)"),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR),
) -> None:
    """Phase 3: scrape person pages and save names and yomigana to BRONZE."""
    log_path = configure_file_logging("allcinema")
    log.info("allcinema_persons_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_scrape_persons(
            limit=limit,
            batch_save=batch_save,
            delay=delay,
            data_dir=data_dir,
        )
    )


@app.command("run")
def cmd_run(
    limit: int = typer.Option(0, help="max items per phase (0=all)"),
    delay: float = typer.Option(DEFAULT_DELAY),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR),
    skip_persons: bool = typer.Option(False, help="skip Phase 3"),
) -> None:
    """Run Phase 2 then Phase 3 in sequence."""
    log_path = configure_file_logging("allcinema")
    log.info("allcinema_run_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_scrape_cinema(
            limit=limit,
            batch_save=SCRAPE_CHECKPOINT_INTERVAL,
            delay=delay,
            data_dir=data_dir,
        )
    )
    if not skip_persons:
        asyncio.run(
            _run_scrape_persons(
                limit=limit,
                batch_save=SCRAPE_CHECKPOINT_INTERVAL,
                delay=delay,
                data_dir=data_dir,
            )
        )


if __name__ == "__main__":
    app()
