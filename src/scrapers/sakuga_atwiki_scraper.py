"""作画@wiki scraper.

Commands:
  discover      BFS crawl to enumerate all page IDs and classify them.
  export-bronze Parse cached HTML and write BRONZE parquet (3 tables).

Data layout:
  data/sakuga/discovered_pages.json  — [{id, url, title, page_kind, discovered_at, last_hash}]
  data/sakuga/cache/<id>.html.gz     — gzip-compressed raw HTML

robots.txt: /sakuga/pages/* is allow. /*/search, /*/backup, /*/edit* are disallow.
Rate limit: delay >= 3.0s per request (atwiki ToS + robots convention).
"""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict

import structlog
import typer
from bs4 import BeautifulSoup

from src.scrapers.bronze_writer import write_sakuga_atwiki_bronze
from src.scrapers.http_playwright import PlaywrightFetcher
from src.scrapers.parsers.sakuga_atwiki import classify_page_kind, extract_page_ids, parse_person_page
from src.scrapers.parsers.sakuga_atwiki_robots import fetch_disallow_patterns, is_allowed
from src.utils.config import SCRAPE_DELAY_SECONDS

log = structlog.get_logger()
app = typer.Typer()

_SITE = "https://www18.atwiki.jp/sakuga"
_SEED_IDS = [1, 2, 100]
_DEFAULT_DELAY = max(SCRAPE_DELAY_SECONDS, 3.0)
_DEFAULT_DATA_DIR = Path("data/sakuga")


class PageRecord(TypedDict):
    id: int
    url: str
    title: str
    page_kind: str
    discovered_at: str
    last_hash: str


def _page_url(page_id: int) -> str:
    return f"{_SITE}/pages/{page_id}.html"


def _cache_path(data_dir: Path, page_id: int) -> Path:
    return data_dir / "cache" / f"{page_id}.html.gz"


def _html_hash(html: str) -> str:
    return hashlib.sha256(html.encode()).hexdigest()


def _load_discovered(path: Path) -> dict[int, PageRecord]:
    if not path.exists():
        return {}
    records: list[PageRecord] = json.loads(path.read_text(encoding="utf-8"))
    return {r["id"]: r for r in records}


def _save_discovered(path: Path, records: dict[int, PageRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(list(records.values()), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _write_html_cache(data_dir: Path, page_id: int, html: str) -> None:
    p = _cache_path(data_dir, page_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(gzip.compress(html.encode()))


def _extract_title(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    tag = soup.find("title")
    return tag.get_text(strip=True) if tag else ""


@app.command()
def discover(
    max_pages: int = typer.Option(3000, "--max-pages", help="Safety page cap"),
    delay: float = typer.Option(_DEFAULT_DELAY, "--delay", help="Seconds between requests"),
    data_dir: Path = typer.Option(_DEFAULT_DATA_DIR, "--data-dir"),
    headless: bool = typer.Option(True, "--headless/--headful"),
) -> None:
    """BFS crawl 作画@wiki to discover and classify all pages."""
    asyncio.run(_discover(max_pages=max_pages, delay=delay, data_dir=data_dir, headless=headless))


async def _discover(
    *,
    max_pages: int,
    delay: float,
    data_dir: Path,
    headless: bool,
) -> None:
    discovered = _load_discovered(data_dir / "discovered_pages.json")
    log.info("discover_start", already_known=len(discovered))

    disallow = await fetch_disallow_patterns()

    async with PlaywrightFetcher(headless=headless) as fetcher:
        queue: list[int] = [i for i in _SEED_IDS if i not in discovered]
        visited: set[int] = set(discovered.keys())

        while queue and len(discovered) < max_pages:
            page_id = queue.pop(0)
            if page_id in visited:
                continue
            visited.add(page_id)

            url_path = f"/sakuga/pages/{page_id}.html"
            if not is_allowed(url_path, disallow):
                log.warning("robots_disallow", page_id=page_id)
                continue

            url = _page_url(page_id)
            try:
                html = await fetcher.fetch(url)
            except Exception as exc:
                log.warning("fetch_error", page_id=page_id, error=str(exc))
                await asyncio.sleep(delay)
                continue

            title = _extract_title(html)
            # pages/1 is always the top/meta page
            kind = "meta" if page_id == 1 else classify_page_kind(title, html)
            h = _html_hash(html)

            record: PageRecord = {
                "id": page_id,
                "url": url,
                "title": title,
                "page_kind": kind,
                "discovered_at": datetime.now(timezone.utc).isoformat(),
                "last_hash": h,
            }
            discovered[page_id] = record
            _write_html_cache(data_dir, page_id, html)

            new_ids = [i for i in extract_page_ids(html) if i not in visited]
            queue.extend(new_ids)
            queue.sort()

            log.info(
                "page_fetched",
                page_id=page_id,
                kind=kind,
                title=title[:50],
                queue_len=len(queue),
                total=len(discovered),
            )

            if len(discovered) % 50 == 0:
                _save_discovered(data_dir / "discovered_pages.json", discovered)

            await asyncio.sleep(delay)

    _save_discovered(data_dir / "discovered_pages.json", discovered)
    kinds = {}
    for r in discovered.values():
        kinds[r["page_kind"]] = kinds.get(r["page_kind"], 0) + 1
    log.info("discover_done", total=len(discovered), by_kind=kinds)


_DEFAULT_BRONZE_ROOT = Path("result/bronze")


@app.command("export-bronze")
def export_bronze(
    input_dir: Path = typer.Option(_DEFAULT_DATA_DIR, "--input"),
    output_dir: Path = typer.Option(_DEFAULT_BRONZE_ROOT, "--output"),
    date: str = typer.Option("", "--date", help="YYYYMMDD; defaults to today"),
) -> None:
    """Parse cached HTML and write BRONZE parquet (3 tables)."""
    from datetime import date as _date

    date_partition = date or _date.today().strftime("%Y%m%d")

    discovered_path = input_dir / "discovered_pages.json"
    if not discovered_path.exists():
        log.error("discovered_pages_missing", path=str(discovered_path))
        raise typer.Exit(1)

    all_pages: list[PageRecord] = json.loads(discovered_path.read_text(encoding="utf-8"))
    person_pages = [p for p in all_pages if p.get("page_kind") == "person"]
    log.info("export_bronze_start", person_pages=len(person_pages), total_pages=len(all_pages))

    persons = []
    raw_texts: dict[int, str] = {}
    parse_ok_count = 0
    for meta in person_pages:
        pid = meta["id"]
        cache = _cache_path(input_dir, pid)
        if not cache.exists():
            log.warning("cache_missing", page_id=pid)
            continue
        html = gzip.decompress(cache.read_bytes()).decode()
        # Extract wikibody plaintext for raw storage (re-parse safety net)
        try:
            from bs4 import BeautifulSoup as _BS
            _soup = _BS(html, "lxml")
            _wb = _soup.find("div", id="wikibody") or _soup.find("body") or _soup
            raw_texts[pid] = _wb.get_text(separator="\n")[:8000]
        except Exception:
            raw_texts[pid] = html[:2000]
        try:
            parsed = parse_person_page(html, page_id=pid)
            persons.append(parsed)
            if parsed.credits:
                parse_ok_count += 1
        except Exception as exc:
            log.warning("parse_error", page_id=pid, error=str(exc))

    log.info(
        "export_parse_summary",
        parsed=len(persons),
        regex_ok=parse_ok_count,
        regex_ok_rate=f"{parse_ok_count/max(len(persons),1):.1%}",
    )

    written = write_sakuga_atwiki_bronze(
        persons=persons,
        pages_metadata=list(all_pages),
        output_dir=output_dir,
        date_partition=date_partition,
        raw_texts=raw_texts,
    )
    log.info("export_bronze_done", written={k: str(v) for k, v in written.items()})


@app.command()
def incremental(
    cache_dir: Path = typer.Option(_DEFAULT_DATA_DIR, "--cache-dir"),
    output: Path = typer.Option(_DEFAULT_BRONZE_ROOT, "--output"),
    date: str = typer.Option("", "--date", help="YYYYMMDD; defaults to today"),
    delay: float = typer.Option(_DEFAULT_DELAY, "--delay"),
    max_pages: int = typer.Option(3000, "--max-pages", help="Safety cap for new-page BFS"),
    headless: bool = typer.Option(True, "--headless/--headful"),
) -> None:
    """Diff-update: re-fetch changed pages, append new date partition to BRONZE."""
    asyncio.run(
        _incremental(
            cache_dir=cache_dir,
            output=output,
            date=date,
            delay=delay,
            max_pages=max_pages,
            headless=headless,
        )
    )


async def _incremental(
    *,
    cache_dir: Path,
    output: Path,
    date: str,
    delay: float,
    max_pages: int,
    headless: bool,
) -> dict[str, int]:
    """Core incremental logic. Returns stats dict (also usable in tests)."""
    from datetime import date as _date

    date_partition = date or _date.today().strftime("%Y%m%d")

    discovered_path = cache_dir / "discovered_pages.json"
    if not discovered_path.exists():
        log.error("discovered_pages_missing", path=str(discovered_path))
        return {}

    discovered = _load_discovered(discovered_path)
    disallow = await fetch_disallow_patterns()

    stats = {"fetched": 0, "unchanged": 0, "changed": 0, "new_pages": 0, "errors": 0}

    changed_persons = []
    changed_raw_texts: dict[int, str] = {}
    changed_meta: list[PageRecord] = []

    person_ids = [pid for pid, r in discovered.items() if r.get("page_kind") == "person"]

    async with PlaywrightFetcher(headless=headless) as fetcher:
        # ── Phase 1: diff-check existing person pages ──────────────────────
        for pid in person_ids:
            url_path = f"/sakuga/pages/{pid}.html"
            if not is_allowed(url_path, disallow):
                log.warning("robots_disallow", page_id=pid)
                continue

            url = _page_url(pid)
            try:
                html = await fetcher.fetch(url)
            except Exception as exc:
                log.warning("fetch_error", page_id=pid, error=str(exc))
                stats["errors"] += 1
                await asyncio.sleep(delay)
                continue

            stats["fetched"] += 1
            new_hash = _html_hash(html)
            old_hash = discovered[pid].get("last_hash", "")

            if new_hash == old_hash:
                stats["unchanged"] += 1
                log.debug("page_unchanged", page_id=pid)
                await asyncio.sleep(delay)
                continue

            # Changed — re-parse
            stats["changed"] += 1
            title = _extract_title(html)
            kind = "meta" if pid == 1 else classify_page_kind(title, html)

            discovered[pid]["last_hash"] = new_hash
            discovered[pid]["title"] = title
            discovered[pid]["page_kind"] = kind
            discovered[pid]["discovered_at"] = datetime.now(timezone.utc).isoformat()

            _write_html_cache(cache_dir, pid, html)

            if kind == "person":
                try:
                    parsed = parse_person_page(html, page_id=pid)
                    changed_persons.append(parsed)
                    changed_meta.append(discovered[pid])

                    soup_wb = BeautifulSoup(html, "lxml")
                    wb = soup_wb.find("div", id="wikibody") or soup_wb.find("body") or soup_wb
                    changed_raw_texts[pid] = wb.get_text(separator="\n")[:8000]
                except Exception as exc:
                    log.warning("parse_error", page_id=pid, error=str(exc))
                    stats["errors"] += 1

            # BFS: discover new page IDs from changed pages
            new_ids = [i for i in extract_page_ids(html) if i not in discovered]
            for new_id in new_ids[:max_pages]:
                new_url = _page_url(new_id)
                new_url_path = f"/sakuga/pages/{new_id}.html"
                if not is_allowed(new_url_path, disallow):
                    continue
                try:
                    new_html = await fetcher.fetch(new_url)
                    await asyncio.sleep(delay)
                except Exception:
                    continue

                new_title = _extract_title(new_html)
                new_kind = "meta" if new_id == 1 else classify_page_kind(new_title, new_html)
                new_hash2 = _html_hash(new_html)
                rec: PageRecord = {
                    "id": new_id,
                    "url": new_url,
                    "title": new_title,
                    "page_kind": new_kind,
                    "discovered_at": datetime.now(timezone.utc).isoformat(),
                    "last_hash": new_hash2,
                }
                discovered[new_id] = rec
                _write_html_cache(cache_dir, new_id, new_html)
                stats["new_pages"] += 1

                if new_kind == "person":
                    try:
                        parsed = parse_person_page(new_html, page_id=new_id)
                        changed_persons.append(parsed)
                        changed_meta.append(rec)
                        soup_wb2 = BeautifulSoup(new_html, "lxml")
                        wb2 = soup_wb2.find("div", id="wikibody") or soup_wb2.find("body") or soup_wb2
                        changed_raw_texts[new_id] = wb2.get_text(separator="\n")[:8000]
                    except Exception as exc:
                        log.warning("new_page_parse_error", page_id=new_id, error=str(exc))

            await asyncio.sleep(delay)

    # ── Write changed pages to new date partition ───────────────────────────
    if changed_persons or stats["new_pages"]:
        write_sakuga_atwiki_bronze(
            persons=changed_persons,
            pages_metadata=changed_meta,
            output_dir=output,
            date_partition=date_partition,
            raw_texts=changed_raw_texts,
        )

    # Persist updated discovered state
    _save_discovered(discovered_path, discovered)

    log.info(
        "incremental_done",
        fetched=stats["fetched"],
        unchanged=stats["unchanged"],
        changed=stats["changed"],
        new_pages=stats["new_pages"],
        errors=stats["errors"],
        date_partition=date_partition,
    )
    return stats


if __name__ == "__main__":
    app()
