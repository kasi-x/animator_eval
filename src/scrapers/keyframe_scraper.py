"""KeyFrame Staff List scraper — API + HTML hybrid 4-phase orchestrator.

Phase 0: roles master  — GET /api/data/roles.php (once)
Phase 1: sitemap       — GET /sitemap.xml (once)
Phase 2: anime HTML    — GET /staff/<slug> × 5512 slugs (raw HTML gzip)
Phase 3: person API    — GET /api/person/show.php × N persons (raw JSON gzip)
Phase 4: preview       — GET /api/stafflists/preview.php (cron snapshot)

Post-scrape enrichment lives in `src.scrapers.keyframe_enrich` (translate.v4.php)
and is invoked separately via `pixi run scrape-keyframe-enrich`.

BRONZE layout:
  result/bronze/source=keyframe/table=roles_master/
  result/bronze/source=keyframe/table=anime/
  result/bronze/source=keyframe/table=anime_studios/
  result/bronze/source=keyframe/table=settings_categories/
  result/bronze/source=keyframe/table=credits/
  result/bronze/source=keyframe/table=studios_master/
  result/bronze/source=keyframe/table=person_profile/
  result/bronze/source=keyframe/table=person_jobs/
  result/bronze/source=keyframe/table=person_studios/
  result/bronze/source=keyframe/table=person_credits/
  result/bronze/source=keyframe/table=preview/

Raw storage:
  data/keyframe/raw/<slug>.html.gz
  data/keyframe/api/roles.json
  data/keyframe/api/preview.json
  data/keyframe/checkpoint_anime.json
  data/keyframe/checkpoint_person.json
"""

from __future__ import annotations

import asyncio
import gzip
import json
import os
import time
from pathlib import Path

import structlog
import typer

from src.scrapers.bronze_writer import BronzeWriter, BronzeWriterGroup
from src.scrapers.cache_store import load_cached_json, save_cached_json
from src.scrapers.checkpoint import Checkpoint, resolve_checkpoint
from src.scrapers.cli_common import (
    DataDirOpt,
    DelayOpt,
    ForceOpt,
    LimitOpt,
    make_scraper_app,
)
from src.scrapers.keyframe_api import (
    DEFAULT_DELAY,
    KeyframeApiClient,
    KeyframeQuotaExceeded,
)
from src.scrapers.parsers import keyframe as html_parser
from src.scrapers.parsers import keyframe_api as api_parser
from src.scrapers.runner import ScrapeRunner
from src.scrapers.sinks import BronzeSink

log = structlog.get_logger()

app = make_scraper_app("keyframe")

DEFAULT_DATA_DIR = Path("data/keyframe")
CHECKPOINT_INTERVAL = 10  # flush checkpoint every N items

# 日次クォータ ~300 req、リセット時刻 不明 → 環境変数で全スキップ可能。
# `ANIMETOR_SKIP_KEYFRAME=1` で scrape-keyframe / scrape-keyframe-enrich 共に no-op。
SKIP_ENV_VAR = "ANIMETOR_SKIP_KEYFRAME"


def keyframe_skip_requested() -> bool:
    return os.environ.get(SKIP_ENV_VAR, "").strip().lower() in {"1", "true", "yes", "on"}

PHASE2_TABLES = (
    "anime",
    "anime_studios",
    "settings_categories",
    "credits",
    "studios_master",
)
PHASE3_TABLES = ("person_profile", "person_jobs", "person_studios", "person_credits")


# =============================================================================
# Main async runner
# =============================================================================


async def run_scraper(
    *,
    data_dir: Path,
    delay: float,
    skip_persons: bool,
    max_anime: int,
    fresh: bool,
) -> dict:
    """Execute Phase 0-4 sequentially. Returns final stats dict."""
    raw_html_dir = data_dir / "raw"
    api_dir = data_dir / "api"
    for d in (raw_html_dir, api_dir):
        d.mkdir(parents=True, exist_ok=True)

    cp_anime = resolve_checkpoint(data_dir / "checkpoint_anime.json", force=fresh)
    cp_person = resolve_checkpoint(data_dir / "checkpoint_person.json", force=fresh)

    stats: dict = cp_anime.get("stats") or {}
    for k in ("roles_fetched", "anime_fetched", "anime_skipped",
              "person_fetched", "person_skipped"):
        stats.setdefault(k, 0)

    client = KeyframeApiClient(delay=delay)
    try:
        await _phase0_roles(client, api_dir, cp_anime, stats)
        cp_anime["stats"] = stats
        cp_anime.save()

        slugs = await _phase1_sitemap(client, cp_anime, stats)
        cp_anime["stats"] = stats
        cp_anime.save()

        if not slugs:
            log.warning("keyframe_no_slugs_found")
            return stats

        remaining = _filter_remaining_slugs(slugs, cp_anime, max_anime)
        person_ids = await _phase2_anime_html(
            client, remaining, raw_html_dir, cp_anime, cp_person, stats
        )

        cp_person["all_ids"] = sorted(person_ids)
        cp_person.save()

        if not skip_persons:
            await _phase3_persons(client, cp_person, stats)

        await _phase4_preview(client, api_dir, cp_anime, stats)
        cp_anime["stats"] = stats
        cp_anime.save()

    except KeyframeQuotaExceeded:
        log.warning(
            "keyframe_quota_exhausted_stop",
            note="daily quota (~300 req) hit — resume tomorrow with `pixi run scrape-keyframe`",
            **stats,
        )
        return stats
    finally:
        cp_anime["stats"] = stats
        cp_anime.save()
        cp_person.save()
        await client.close()

    log.info("keyframe_scrape_complete", **stats)
    return stats


# =============================================================================
# Phase 0: roles master
# =============================================================================


async def _phase0_roles(
    client: KeyframeApiClient,
    api_dir: Path,
    cp_anime: Checkpoint,
    stats: dict,
) -> None:
    """Fetch /api/data/roles.php and write to BRONZE roles_master table."""
    if cp_anime.get("roles_master_fetched_at"):
        log.info("keyframe_phase0_skip", reason="already_fetched")
        return

    log.info("keyframe_phase0_start")
    roles = await client.get_roles_master()
    if not roles:
        log.warning("keyframe_phase0_failed")
        return

    (api_dir / "roles.json").write_text(
        json.dumps(roles, ensure_ascii=False), encoding="utf-8"
    )

    parsed = api_parser.parse_roles_master(roles)
    with BronzeWriter("keyframe", table="roles_master") as bw:
        bw.extend(parsed)

    cp_anime["roles_master_fetched_at"] = int(time.time())
    stats["roles_fetched"] = len(roles)
    log.info("keyframe_phase0_done", count=len(roles))


# =============================================================================
# Phase 1: sitemap
# =============================================================================


async def _phase1_sitemap(
    client: KeyframeApiClient,
    cp_anime: Checkpoint,
    stats: dict,
) -> list[str]:
    """Fetch sitemap.xml and return full slug list (cache in checkpoint)."""
    if cp_anime.get("all_slugs"):
        log.info("keyframe_phase1_skip", reason="already_in_checkpoint")
        return cp_anime["all_slugs"]

    log.info("keyframe_phase1_start")
    xml_text = await client.get_sitemap()
    if not xml_text:
        log.warning("keyframe_phase1_failed")
        return []

    slugs = api_parser.parse_sitemap_xml(xml_text)
    cp_anime["all_slugs"] = slugs
    stats["sitemap_slugs"] = len(slugs)
    log.info("keyframe_phase1_done", slugs=len(slugs))
    return slugs


def _filter_remaining_slugs(
    all_slugs: list[str], cp_anime: Checkpoint, max_anime: int
) -> list[str]:
    """Return slugs not yet completed, capped at max_anime (0 = uncapped)."""
    done = cp_anime.completed_set
    remaining = [s for s in all_slugs if s not in done]
    if max_anime > 0:
        remaining = remaining[:max_anime]
    log.info(
        "keyframe_phase2_plan",
        total=len(all_slugs),
        done=len(done),
        remaining=len(remaining),
        cap=max_anime,
    )
    return remaining


# =============================================================================
# Phase 2: anime HTML
# =============================================================================


def _save_html_gz(raw_html_dir: Path, slug: str, html_text: str) -> None:
    """Persist raw HTML for the slug to data/keyframe/raw/<slug>.html.gz."""
    with gzip.open(raw_html_dir / f"{slug}.html.gz", "wt", encoding="utf-8") as fh:
        fh.write(html_text)


def _emit_anime_rows(
    group: BronzeWriterGroup,
    data: dict,
    slug: str,
) -> None:
    """Parse one preloadData payload and append rows to the 5 Phase 2 tables."""
    anime_id = f"keyframe:{slug}"
    anime_meta = html_parser.parse_anime_meta(data, slug)
    group["anime"].append(html_parser.build_anime_row(anime_meta, anime_id))

    for studio_row in html_parser.parse_anime_studios(data):
        group["anime_studios"].append({"anime_id": anime_id, **studio_row})

    for cat_row in html_parser.parse_settings_categories(data):
        group["settings_categories"].append({"anime_id": anime_id, **cat_row})

    credit_rows = html_parser.parse_credits_from_data(data, slug)
    for c in credit_rows:
        group["credits"].append(html_parser.normalize_credit_row(c, anime_id))

    for studio_row in html_parser.collect_studio_master(credit_rows):
        group["studios_master"].append(studio_row)


async def _phase2_anime_html(
    client: KeyframeApiClient,
    remaining: list[str],
    raw_html_dir: Path,
    cp_anime: Checkpoint,
    cp_person: Checkpoint,
    stats: dict,
) -> set[int]:
    """Fetch and parse anime HTML pages. Returns accumulated person id set.

    Phase 2 stays as a hand-written loop instead of using ScrapeRunner because
    each page emits rows for 5 tables AND contributes person_ids to a shared
    set consumed by Phase 3 — the runner's single-record sink contract doesn't
    fit cleanly without leaking that side-effect through generic helpers.
    """
    log.info("keyframe_phase2_start", count=len(remaining))
    person_ids: set[int] = set(cp_person.get("all_ids") or [])

    def _persist_quota_state() -> None:
        cp_anime.save()
        cp_person["all_ids"] = sorted(person_ids)
        cp_person.save()

    with BronzeWriterGroup("keyframe", tables=list(PHASE2_TABLES)) as group:
        for i, slug in enumerate(remaining):
            try:
                html_text = await client.get_anime_page(slug)
            except KeyframeQuotaExceeded:
                group.flush_all()
                _persist_quota_state()
                log.warning(
                    "keyframe_phase2_quota_exhausted",
                    processed=i,
                    remaining=len(remaining) - i,
                    persons_seen=len(person_ids),
                )
                raise

            if html_text is None:
                log.debug("keyframe_phase2_skip", slug=slug)
                cp_anime.mark_completed(slug)
                stats["anime_skipped"] += 1
                continue

            _save_html_gz(raw_html_dir, slug, html_text)

            data = html_parser.extract_preload_data(html_text)
            if data is None:
                log.warning("keyframe_phase2_no_preload", slug=slug)
                cp_anime.mark_completed(slug)
                continue

            _emit_anime_rows(group, data, slug)
            person_ids.update(html_parser.collect_person_ids_from_preload(data))

            cp_anime.mark_completed(slug)
            stats["anime_fetched"] += 1

            if (i + 1) % CHECKPOINT_INTERVAL == 0:
                group.flush_all()
                cp_anime.save()
                log.info(
                    "keyframe_phase2_checkpoint",
                    progress=f"{i + 1}/{len(remaining)}",
                    anime=stats["anime_fetched"],
                    persons_seen=len(person_ids),
                )

    log.info(
        "keyframe_phase2_done",
        fetched=stats["anime_fetched"],
        person_ids_seen=len(person_ids),
    )
    return person_ids


# =============================================================================
# Phase 3: person API
# =============================================================================


def _make_person_fetcher(client: KeyframeApiClient):
    """Build a cache-aware fetcher for /api/person/show.php."""

    async def _fetch(pid: int) -> dict | None:
        cached = load_cached_json("keyframe/persons", {"person_id": pid})
        if cached is not None:
            return cached
        data = await client.get_person_show(pid)
        if data is not None:
            save_cached_json("keyframe/persons", {"person_id": pid}, data)
        return data

    return _fetch


async def _phase3_persons(
    client: KeyframeApiClient,
    cp_person: Checkpoint,
    stats: dict,
) -> None:
    """Fetch person show.php for each unique person id collected in Phase 2."""
    all_ids = cp_person.get("all_ids") or []

    with BronzeWriterGroup("keyframe", tables=list(PHASE3_TABLES)) as group:
        runner = ScrapeRunner(
            fetcher=_make_person_fetcher(client),
            parser=lambda raw, _pid: api_parser.parse_person_show(raw),
            sink=BronzeSink(group, api_parser.build_person_show_rows, add_hash=False),
            checkpoint=cp_person,
            label="keyframe_person",
            flush=group.flush_all,
            flush_every=CHECKPOINT_INTERVAL,
            fatal_exceptions=(KeyframeQuotaExceeded,),
        )
        result = await runner.run(all_ids)

    stats["person_fetched"] = result.processed
    stats["person_skipped"] = result.skipped
    log.info("keyframe_phase3_done", **stats)


# =============================================================================
# Phase 4: preview snapshot
# =============================================================================


def _build_preview_row(section: str, entry: dict, fetched_at: int) -> dict:
    """Return one BRONZE preview row with nested fields JSON-serialised."""
    return {
        "fetched_at": fetched_at,
        "section": section,
        **entry,
        "studios_str": json.dumps(entry.get("studios_str", []), ensure_ascii=False),
        "contributors_json": json.dumps(
            entry.get("contributors_json", []), ensure_ascii=False
        ),
    }


async def _phase4_preview(
    client: KeyframeApiClient,
    api_dir: Path,
    cp_anime: Checkpoint,
    stats: dict,
) -> None:
    """Fetch /api/stafflists/preview.php and write to BRONZE preview table."""
    log.info("keyframe_phase4_start")
    raw_data = await client.get_preview()
    if not raw_data:
        log.warning("keyframe_phase4_failed")
        return

    (api_dir / "preview.json").write_text(
        json.dumps(raw_data, ensure_ascii=False), encoding="utf-8"
    )

    normalized = api_parser.parse_preview(raw_data)
    fetched_at = int(time.time())

    with BronzeWriter("keyframe", table="preview") as bw:
        for section in ("recent", "airing", "data"):
            for entry in normalized[section]:
                bw.append(_build_preview_row(section, entry, fetched_at))

    cp_anime["preview_fetched_at"] = fetched_at
    stats["preview_total"] = normalized["total"]
    log.info("keyframe_phase4_done", total=normalized["total"])


# =============================================================================
# Typer CLI
# =============================================================================


@app.command()
def run(
    delay: DelayOpt = DEFAULT_DELAY,
    skip_persons: bool = typer.Option(False, help="Skip Phase 3 (person API)"),
    max_anime: LimitOpt = 0,
    fresh: ForceOpt = False,
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    skip: bool = typer.Option(
        False,
        "--skip",
        help=f"Skip entire scrape (also via env {SKIP_ENV_VAR}=1). 日次 ~300 req クォータのリセット時刻 不明回避用",
    ),
) -> None:
    """Phase 0-4: roles → sitemap → anime HTML → person API → preview."""
    if skip or keyframe_skip_requested():
        log.warning(
            "keyframe_scrape_skipped",
            reason=f"--skip or {SKIP_ENV_VAR} set",
            note="daily ~300 req quota — reset time unknown",
        )
        return
    stats = asyncio.run(
        run_scraper(
            data_dir=data_dir,
            delay=delay,
            skip_persons=skip_persons,
            max_anime=max_anime,
            fresh=fresh,
        )
    )
    log.info("keyframe_done", **stats)


if __name__ == "__main__":
    app()
