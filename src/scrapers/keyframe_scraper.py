"""KeyFrame Staff List scraper.

Scrapes structured anime staff credit data from keyframe-staff-list.com.
Data is embedded as JSON (preloadData) in each page's HTML — no LLM needed.

Features:
- Bilingual names (ja + en)
- Stable numeric person IDs
- Episode-level credits
- Sitemap-based page discovery (~800+ anime)

This scraper only fetches and stores data. Matching with AniList is handled
separately in the pipeline's Entity Resolution phase.
"""

from __future__ import annotations

import asyncio
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import structlog
import typer

from src.runtime.models import BronzeAnime, Credit, Person, parse_role
from src.scrapers.parsers.keyframe import (  # noqa: F401
    extract_preload_data,
    _extract_episode_num,
    parse_credits_from_data,
)

log = structlog.get_logger()

# =============================================================================
# Constants
# =============================================================================

BASE_URL = "https://keyframe-staff-list.com"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DEFAULT_DELAY = 3.0  # seconds between requests (conservative for rate limits)
DEFAULT_DATA_DIR = Path("data/keyframe")

app = typer.Typer()


# =============================================================================
# ID generation
# =============================================================================


def make_keyframe_person_id(person_id: int | str) -> str:
    """Generate person ID from KeyFrame's stable numeric ID."""
    return f"keyframe:p_{person_id}"


def make_keyframe_anime_id(slug: str) -> str:
    """Generate anime ID from KeyFrame's URL slug."""
    return f"keyframe:{slug}"


# =============================================================================
# Sitemap parsing — discover all anime page slugs
# =============================================================================


async def fetch_sitemap(client: httpx.AsyncClient) -> list[str]:
    """Fetch sitemap.xml and extract all /staff/ slugs.

    Returns: list of slug strings (e.g., ["one-piece", "hyouka", ...])
    """
    resp = await client.get(SITEMAP_URL, headers=HEADERS)
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    # Namespace: sitemaps use {http://www.sitemaps.org/schemas/sitemap/0.9}
    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    slugs: list[str] = []
    for url_elem in root.findall("s:url/s:loc", ns):
        url = url_elem.text or ""
        # Extract /staff/{slug} paths
        match = re.search(r"/staff/([^/]+)$", url)
        if match:
            slugs.append(match.group(1))

    log.info("keyframe_sitemap_parsed", total_slugs=len(slugs))
    return slugs


# =============================================================================
# Page fetching & JSON extraction
# =============================================================================


async def fetch_anime_page(
    client: httpx.AsyncClient, slug: str, max_retries: int = 3
) -> dict | None:
    """Fetch a single anime page and extract preloadData.

    Retries on 429 with exponential backoff.
    """
    url = f"{BASE_URL}/staff/{slug}"

    for attempt in range(1, max_retries + 1):
        resp = await client.get(url, headers=HEADERS)

        if resp.status_code == 404:
            log.debug("keyframe_page_not_found", slug=slug)
            return None

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 30))
            wait = max(retry_after, 10 * attempt)
            log.warning(
                "keyframe_rate_limited",
                slug=slug,
                attempt=attempt,
                wait_seconds=wait,
            )
            await asyncio.sleep(wait)
            continue

        resp.raise_for_status()
        return extract_preload_data(resp.text)

    log.warning("keyframe_max_retries", slug=slug)
    return None


# =============================================================================
# Checkpoint
# =============================================================================


def save_checkpoint(
    data_dir: Path,
    processed_slugs: set[str],
    stats: dict,
) -> None:
    """Save checkpoint to disk."""
    data_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = data_dir / "checkpoint.json"
    checkpoint_path.write_text(
        json.dumps(
            {"processed_slugs": sorted(processed_slugs), "stats": stats},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def load_checkpoint(data_dir: Path) -> tuple[set[str], dict]:
    """Load checkpoint from disk.

    Returns: (processed_slugs, stats)
    """
    checkpoint_path = data_dir / "checkpoint.json"
    if not checkpoint_path.exists():
        return set(), {}

    data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    return set(data.get("processed_slugs", [])), data.get("stats", {})


# =============================================================================
# Main orchestrator
# =============================================================================


async def scrape_keyframe(
    data_dir: Path | None = None,
    max_anime: int = 0,
    checkpoint_interval: int = 10,
    delay: float = DEFAULT_DELAY,
    fresh: bool = False,
) -> dict:
    """Fetch credit data from KeyFrame Staff List.

    1. Fetch sitemap.xml → discover all anime slugs
    2. Fetch each anime page → extract preloadData JSON
    3. Write to BRONZE parquet (ETL handles dedup and ID resolution)

    Returns: Statistics dict
    """
    from src.scrapers.bronze_writer import BronzeWriter

    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)

    stats = {
        "anime_fetched": 0,
        "anime_with_credits": 0,
        "credits_created": 0,
        "persons_created": 0,
        "pages_skipped": 0,
        "pages_failed": 0,
    }

    # Load checkpoint
    if fresh:
        processed_slugs: set[str] = set()
    else:
        processed_slugs, prev_stats = load_checkpoint(data_dir)
        if processed_slugs:
            log.info(
                "keyframe_checkpoint_loaded",
                processed=len(processed_slugs),
            )

    person_cache: set[str] = set()

    anime_bw = BronzeWriter("keyframe", table="anime")
    persons_bw = BronzeWriter("keyframe", table="persons")
    credits_bw = BronzeWriter("keyframe", table="credits")

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        # Phase 1: Discover anime slugs from sitemap
        slugs = await fetch_sitemap(client)
        if not slugs:
            log.warning("keyframe_no_slugs_found")
            return stats

        # Filter already-processed
        remaining = [s for s in slugs if s not in processed_slugs]
        if max_anime > 0:
            remaining = remaining[:max_anime]

        log.info(
            "keyframe_scrape_start",
            total_slugs=len(slugs),
            remaining=len(remaining),
            already_processed=len(processed_slugs),
        )

        # Phase 2-3: Fetch, parse, write to parquet
        for i, slug in enumerate(remaining):
            try:
                await asyncio.sleep(delay)
                data = await fetch_anime_page(client, slug)

                if data is None:
                    stats["pages_skipped"] += 1
                    processed_slugs.add(slug)
                    continue

                # Extract anime metadata
                title = data.get("title", slug)
                anilist_id_str = data.get("anilistId")
                season_year = data.get("seasonYear")

                # Parse anilist_id
                anilist_id: int | None = None
                if anilist_id_str:
                    try:
                        anilist_id = int(anilist_id_str)
                    except (ValueError, TypeError):
                        pass

                anime_id = make_keyframe_anime_id(slug)
                anime = BronzeAnime(
                    id=anime_id,
                    title_en=title,
                    year=season_year,
                    anilist_id=anilist_id,
                )
                anime_bw.append({**anime.model_dump(mode="json"), "slug": slug})
                stats["anime_fetched"] += 1

                # Parse credits
                page_credits = parse_credits_from_data(data, slug)

                if page_credits:
                    stats["anime_with_credits"] += 1

                for credit_data in page_credits:
                    pid = credit_data["person_id"]
                    person_id = make_keyframe_person_id(pid)

                    if person_id not in person_cache:
                        person = Person(
                            id=person_id,
                            name_ja=credit_data["name_ja"],
                            name_en=credit_data["name_en"],
                        )
                        persons_bw.append(person.model_dump(mode="json"))
                        person_cache.add(person_id)
                        stats["persons_created"] += 1

                    role_ja = credit_data["role_ja"]
                    role_en = credit_data["role_en"]
                    role = parse_role(role_ja) if role_ja else parse_role(role_en)
                    raw_role = role_ja or role_en

                    credit = Credit(
                        person_id=person_id,
                        anime_id=anime_id,
                        role=role,
                        raw_role=raw_role,
                        episode=credit_data["episode"],
                        source="keyframe",
                    )
                    credits_bw.append(credit.model_dump(mode="json"))
                    stats["credits_created"] += 1

                processed_slugs.add(slug)

                # Checkpoint flush
                if (i + 1) % checkpoint_interval == 0:
                    anime_bw.flush()
                    persons_bw.flush()
                    credits_bw.flush()
                    save_checkpoint(data_dir, processed_slugs, stats)
                    log.info(
                        "keyframe_checkpoint",
                        progress=f"{i + 1}/{len(remaining)}",
                        anime=stats["anime_fetched"],
                        credits=stats["credits_created"],
                        persons=stats["persons_created"],
                    )

            except httpx.HTTPStatusError as e:
                log.warning(
                    "keyframe_page_error",
                    slug=slug,
                    status=e.response.status_code,
                )
                stats["pages_failed"] += 1
            except Exception as e:
                log.warning(
                    "keyframe_page_error",
                    slug=slug,
                    error=str(e)[:200],
                )
                stats["pages_failed"] += 1

    # Final flush
    anime_bw.flush()
    persons_bw.flush()
    credits_bw.flush()
    save_checkpoint(data_dir, processed_slugs, stats)

    log.info("keyframe_scrape_complete", source="keyframe", **stats)
    return stats


# =============================================================================
# Typer CLI
# =============================================================================


@app.command()
def main(
    max_anime: int = typer.Option(
        0, "--max-anime", "-n", help="Maximum anime pages to scrape (0 = all)"
    ),
    checkpoint: int = typer.Option(
        10, "--checkpoint", "-c", help="Checkpoint interval (pages)"
    ),
    delay: float = typer.Option(
        DEFAULT_DELAY, "--delay", "-d", help="Delay between requests (seconds)"
    ),
    fresh: bool = typer.Option(False, "--fresh", help="Ignore existing checkpoint"),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", help="Data directory"
    ),
) -> None:
    """Fetch credit data from KeyFrame Staff List."""
    from src.infra.logging import setup_logging
    from src.scrapers.logging_utils import configure_file_logging

    setup_logging()
    log_path = configure_file_logging("keyframe")
    log.info("keyframe_command_start", log_file=str(log_path))

    stats = asyncio.run(
        scrape_keyframe(
            data_dir=data_dir,
            max_anime=max_anime,
            checkpoint_interval=checkpoint,
            delay=delay,
            fresh=fresh,
        )
    )

    log.info("keyframe_scrape_complete", **stats)


if __name__ == "__main__":
    app()
