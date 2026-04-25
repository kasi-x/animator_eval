"""allcinema.net scraper.

3-phase structure:
  Phase 1 (sitemap):  retrieve all cinema/person IDs from sitemap XML.gz files
  Phase 1-alt (sitemap-anime): retrieve anime-only cinema IDs via /ajax/search
  Phase 2 (cinema):   extract anime staff info from CreditJson embedded in each cinema page
  Phase 3 (persons):  fetch name and yomigana for each person ID collected in Phase 2

Data sources:
  Sitemap (cinema) : https://www.allcinema.net/sitemap_c{N}.xml.gz  (3 files)
  Sitemap (person) : https://www.allcinema.net/sitemap_p{N}.xml.gz  (12 files)
  Ajax search      : https://www.allcinema.net/ajax/search  (POST, search_cinema_media=anime)
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
from pathlib import Path

from src.scrapers.checkpoint import resolve_checkpoint
from src.scrapers.cli_common import (
    DataDirOpt,
    DelayOpt,
    ForceOpt,
    LimitOpt,
    ProgressOpt,
    QuietOpt,
    ResumeOpt,
    resolve_progress_enabled,
)
from src.scrapers.fetchers import HtmlFetcher
from src.scrapers.runner import ScrapeRunner
from src.scrapers.sinks import BronzeSink
from xml.etree import ElementTree as ET

import httpx
import structlog
import typer

from src.scrapers.http_base import RateLimitedHttpClient
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
AJAX_SEARCH = f"{SITE_BASE}/ajax/search"

SITEMAP_CINEMA_PATTERN = f"{SITE_BASE}/sitemap_c{{n}}.xml.gz"
SITEMAP_PERSON_PATTERN = f"{SITE_BASE}/sitemap_p{{n}}.xml.gz"

# allcinema の cinema サイトマップは 3 ファイル、person は 12 ファイル (2026/04 時点)
SITEMAP_CINEMA_COUNT = 3
SITEMAP_PERSON_COUNT = 12

# /ajax/search の search_cinema_media=anime に対して使うシード文字集合。
# 1 文字では >1000 件を超えるものがあるため、超えた場合は 2 文字目を付加して再帰探索する。
# ひらがな + カタカナ + 主要アルファベット (大/小) + 数字。
# 日本語アニメタイトルはほぼ必ずこれらのいずれかを含む。
_SEARCH_SEEDS: tuple[str, ...] = tuple(
    "あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん"
    "がぎぐげござじずぜぞだぢづでどばびぶべぼぱぴぷぺぽぁぃぅぇぉっゃゅょ"
    "アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン"
    "ガギグゲゴザジズゼゾダヂヅデドバビブベボパピプペポァィゥェォッャュョ"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    "0123456789"
)

# 再帰展開の際に付加する文字 (ひらがな + 数字のみで十分深い分割が得られる)
_EXPAND_CHARS: tuple[str, ...] = tuple(
    "あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん"
    "がぎぐげござじずぜぞだぢづでどばびぶべぼぱぴぷぺぽ"
    "0123456789"
)

# search 結果が >1000 件の場合の再帰展開の最大深度
_MAX_PREFIX_DEPTH = 4

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


class AllcinemaClient(RateLimitedHttpClient):
    """Async HTTP client for allcinema.

    GET requests are delegated to the shared RetryingHttpClient (http_client.py).
    CSRF-bearing POST (/ajax/person) uses custom handling for 419 retries etc.
    """

    def __init__(self, delay: float = DEFAULT_DELAY, transport=None) -> None:
        super().__init__(delay=delay)
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


async def _search_anime_one_page(
    client: AllcinemaClient,
    word: str,
    page: int,
    page_limit: int = 100,
) -> tuple[list[int], int]:
    """POST to /ajax/search for one page.

    Returns (cinema_id_list, max_page).
    Returns ([], 0) when results exceed the server limit (>1000) or on error.
    """
    headers = {
        "X-CSRF-TOKEN": client._csrf_token,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{SITE_BASE}/search/",
    }
    data = {
        "search_target": "cinema",
        "search_word": word,
        "search_cinema_media": "anime",
        "page": str(page),
        "page_limit": str(page_limit),
        "sort": "2",  # 製作年 昇順
    }
    try:
        resp = await client._client.post(AJAX_SEARCH, data=data, headers=headers)
        resp.raise_for_status()
        body = resp.json()
    except Exception as exc:
        log.warning("allcinema_search_error", word=word, page=page, error=str(exc))
        return [], 0

    sm = body.get("search", {}).get("searchmovies", {})
    movies = sm.get("movies", [])
    if not movies:
        # Over-limit case: server returns empty movies with a warning message
        return [], 0

    ids = [m["movie"]["cinemaid"] for m in movies if "movie" in m]
    max_page = sm.get("page", {}).get("maxpage", 1)
    return ids, max_page


async def _fetch_anime_ids_for_word(
    client: AllcinemaClient,
    word: str,
    collected: set[int],
    depth: int = 0,
) -> None:
    """Collect anime cinema IDs for all pages of the given search word.

    When the server returns no movies (results > 1000), recursively expands
    the prefix by appending each character in _EXPAND_CHARS — up to
    _MAX_PREFIX_DEPTH levels deep.

    Results are accumulated into *collected* (mutated in place).
    """
    if not client._csrf_token:
        # Ensure CSRF token is available before first POST
        await client.get(f"{SITE_BASE}/search/")

    ids_page1, max_page = await _search_anime_one_page(client, word, page=1)

    if not ids_page1:
        # Server returned nothing → results > 1000 OR genuinely empty.
        if depth >= _MAX_PREFIX_DEPTH:
            log.warning("allcinema_search_depth_limit", word=word, depth=depth)
            return
        # Recurse: extend prefix with each expansion character.
        for ch in _EXPAND_CHARS:
            extended = word + ch
            # Skip if the extension is redundant (same prefix already visited).
            await _fetch_anime_ids_for_word(client, extended, collected, depth + 1)
        return

    # Collect page 1 results
    collected.update(ids_page1)

    # Paginate through remaining pages
    for pg in range(2, max_page + 1):
        ids_pg, _ = await _search_anime_one_page(client, word, page=pg)
        if not ids_pg:
            break
        collected.update(ids_pg)

    log.debug(
        "allcinema_search_word_done",
        word=word,
        depth=depth,
        pages=max_page,
        new_ids=len(ids_page1),
    )


async def fetch_anime_ids_from_search(client: AllcinemaClient) -> list[int]:
    """Collect all anime cinema IDs via /ajax/search (Scenario β).

    Uses a systematic prefix search over _SEARCH_SEEDS.  For any seed that
    returns >1000 results (empty movies list), the prefix is recursively
    extended with _EXPAND_CHARS until results fit within the server limit.

    Returns a deduplicated, sorted list of cinema IDs.
    """
    collected: set[int] = set()
    total_seeds = len(_SEARCH_SEEDS)

    for i, seed in enumerate(_SEARCH_SEEDS):
        log.info(
            "allcinema_search_seed",
            seed=seed,
            progress=f"{i + 1}/{total_seeds}",
            collected=len(collected),
        )
        await _fetch_anime_ids_for_word(client, seed, collected, depth=0)

    ids = sorted(collected)
    log.info("allcinema_search_complete", total=len(ids))
    return ids


# ─── Bronze mappers ──────────────────────────────────────────────────────────


def _map_cinema_record(rec: AllcinemaAnimeRecord) -> dict[str, list[dict]]:
    anime_row = dataclasses.asdict(rec)
    credits = [
        {**dataclasses.asdict(c), "cinema_id": rec.cinema_id}
        for c in (rec.staff + rec.cast)
    ]
    anime_row.pop("staff", None)
    anime_row.pop("cast", None)
    return {"anime": [anime_row], "credits": credits}


# ─── Phase 2 execution ───────────────────────────────────────────────────────


async def _run_scrape_cinema(
    limit: int,
    batch_save: int,
    delay: float,
    data_dir: Path,
    resume: bool = True,
    force: bool = False,
    progress_override: bool | None = None,
) -> None:
    from src.scrapers.bronze_writer import BronzeWriterGroup

    cp = resolve_checkpoint(data_dir / "checkpoint_cinema.json", force=force, resume=resume)
    cinema_ids: list[int] = cp.get("cinema_ids") or []

    client = AllcinemaClient(delay=delay)
    try:
        if not cinema_ids:
            log.info("allcinema_cinema_sitemap_start")
            cinema_ids = await fetch_sitemap_ids(
                client, SITEMAP_CINEMA_PATTERN, SITEMAP_CINEMA_COUNT
            )
            cp["cinema_ids"] = cinema_ids
            cp.save()
            log.info("allcinema_cinema_ids_total", total=len(cinema_ids))

        with BronzeWriterGroup("allcinema", tables=["anime", "credits"]) as g:
            runner = ScrapeRunner(
                fetcher=HtmlFetcher(
                    client, f"{CINEMA_BASE}{{id}}",
                    namespace="allcinema/cinema", source="allcinema_cinema",
                ),
                parser=_parse_cinema_html,
                sink=BronzeSink(g, _map_cinema_record, add_hash=True),
                checkpoint=cp,
                label="allcinema_cinema",
                flush=g.flush_all,
                flush_every=batch_save,
            )
            stats = await runner.run(cinema_ids, limit=limit, progress_override=progress_override)
        log.info("allcinema_cinema_done", **dataclasses.asdict(stats))
    finally:
        await client.close()


# ─── Phase 3 execution ───────────────────────────────────────────────────────


async def _run_scrape_persons(
    limit: int,
    batch_save: int,
    delay: float,
    data_dir: Path,
    resume: bool = True,
    force: bool = False,
    progress_override: bool | None = None,
) -> None:
    import pyarrow.dataset as ds

    from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT, BronzeWriterGroup

    cp = resolve_checkpoint(data_dir / "checkpoint_persons.json", force=force, resume=resume)

    credits_path = DEFAULT_BRONZE_ROOT / "source=allcinema" / "table=credits"
    if not credits_path.exists():
        log.warning("allcinema_persons_no_credits", msg="Run cinema phase first")
        return

    credits_ds = ds.dataset(credits_path, format="parquet")
    tbl = credits_ds.to_table(columns=["allcinema_person_id"])
    all_person_ids: list[int] = list(
        dict.fromkeys(pid for pid in tbl.column("allcinema_person_id").to_pylist() if pid is not None)
    )
    if not all_person_ids:
        log.warning("allcinema_persons_no_credits", msg="Run cinema phase first")
        return

    client = AllcinemaClient(delay=delay)
    try:
        with BronzeWriterGroup("allcinema", tables=["persons"]) as g:
            runner = ScrapeRunner(
                fetcher=HtmlFetcher(
                    client, f"{PERSON_BASE}{{id}}",
                    namespace="allcinema/persons", source="allcinema_person",
                ),
                parser=_parse_person_html,
                sink=BronzeSink(g, lambda rec: {"persons": [dataclasses.asdict(rec)]}, add_hash=False),
                checkpoint=cp,
                label="allcinema_persons",
                flush=g.flush_all,
                flush_every=batch_save,
            )
            stats = await runner.run(all_person_ids, limit=limit, progress_override=progress_override)
        log.info("allcinema_persons_done", **dataclasses.asdict(stats))
    finally:
        await client.close()


# ─── Phase 1-alt: anime ID list via search ───────────────────────────────────


async def _run_sitemap_anime(
    delay: float,
    data_dir: Path,
) -> None:
    """Collect anime-only cinema IDs via /ajax/search and update checkpoint.

    Backs up existing checkpoint before overwriting cinema_ids.
    The `completed` set is preserved so already-scraped IDs are not re-fetched.
    """
    effective_delay = max(delay, 1.0)  # Hard lower bound per task card
    ckpt_path = data_dir / "checkpoint_cinema.json"
    bak_path = data_dir / "checkpoint_cinema.json.bak"

    ckpt = resolve_checkpoint(ckpt_path, resume=True)
    old_count = len(ckpt.get("cinema_ids", []))

    # Safety: ensure backup exists before touching cinema_ids
    if not bak_path.exists():
        bak_path.write_text(json.dumps(ckpt.data))
        log.info("allcinema_sitemap_anime_backup_created", path=str(bak_path))
    else:
        log.info("allcinema_sitemap_anime_backup_exists", path=str(bak_path))

    client = AllcinemaClient(delay=effective_delay)
    try:
        anime_ids = await fetch_anime_ids_from_search(client)
    finally:
        await client.close()

    new_count = len(anime_ids)
    if new_count < 100:
        log.error(
            "allcinema_sitemap_anime_too_few",
            count=new_count,
            msg="Fewer than 100 IDs collected — aborting checkpoint update",
        )
        raise RuntimeError(
            f"fetch_anime_ids_from_search returned only {new_count} IDs; "
            "expected at least 1000. Not updating checkpoint."
        )

    ckpt["cinema_ids"] = anime_ids
    # Preserve completed entries (do not reset already-scraped IDs)
    ckpt.save()

    log.info(
        "allcinema_sitemap_anime_done",
        before=old_count,
        after=new_count,
        completed=len(ckpt.get("completed", [])),
    )


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


@app.command("sitemap-anime")
def cmd_sitemap_anime(
    delay: float = typer.Option(DEFAULT_DELAY, help="request interval (seconds); minimum 1.0"),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, help="checkpoint save directory"),
) -> None:
    """Phase 1-alt: アニメ作品のみの cinema_ids を構築し checkpoint に上書き。

    /ajax/search に search_cinema_media=anime を POST し、体系的なプレフィックス探索で
    全アニメ ID を収集する (シナリオ β)。
    既存の completed リストは保持し、checkpoint_cinema.json.bak は touch しない。
    """
    log_path = configure_file_logging("allcinema")
    log.info("allcinema_sitemap_anime_command_start", log_file=str(log_path))
    asyncio.run(_run_sitemap_anime(delay=delay, data_dir=data_dir))


@app.command("cinema")
def cmd_cinema(
    limit: LimitOpt = 0,
    batch_save: int = typer.Option(SCRAPE_CHECKPOINT_INTERVAL, "--batch-save", help="commit interval"),
    delay: DelayOpt = DEFAULT_DELAY,
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    resume: ResumeOpt = True,
    force: ForceOpt = False,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
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
            resume=resume,
            force=force,
            progress_override=resolve_progress_enabled(quiet, progress),
        )
    )


@app.command("persons")
def cmd_persons(
    limit: LimitOpt = 0,
    batch_save: int = typer.Option(SCRAPE_CHECKPOINT_INTERVAL, "--batch-save", help="commit interval"),
    delay: DelayOpt = DEFAULT_DELAY,
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    resume: ResumeOpt = True,
    force: ForceOpt = False,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
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
            resume=resume,
            force=force,
            progress_override=resolve_progress_enabled(quiet, progress),
        )
    )


@app.command("run")
def cmd_run(
    limit: LimitOpt = 0,
    delay: DelayOpt = DEFAULT_DELAY,
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    resume: ResumeOpt = True,
    force: ForceOpt = False,
    skip_persons: bool = typer.Option(False, "--skip-persons", help="skip Phase 3"),
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Run Phase 2 then Phase 3 in sequence."""
    log_path = configure_file_logging("allcinema")
    log.info("allcinema_run_command_start", log_file=str(log_path), limit=limit)
    progress_override = resolve_progress_enabled(quiet, progress)
    asyncio.run(
        _run_scrape_cinema(
            limit=limit,
            batch_save=SCRAPE_CHECKPOINT_INTERVAL,
            delay=delay,
            data_dir=data_dir,
            resume=resume,
            force=force,
            progress_override=progress_override,
        )
    )
    if not skip_persons:
        asyncio.run(
            _run_scrape_persons(
                limit=limit,
                batch_save=SCRAPE_CHECKPOINT_INTERVAL,
                delay=delay,
                data_dir=data_dir,
                resume=resume,
                force=force,
                progress_override=progress_override,
            )
        )


if __name__ == "__main__":
    app()
