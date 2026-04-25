"""KeyFrame Staff List scraper — API + HTML hybrid 4-phase orchestrator.

Phase 0: roles master  — GET /api/data/roles.php (once)
Phase 1: sitemap       — GET /sitemap.xml (once)
Phase 2: anime HTML    — GET /staff/<slug> × 5512 slugs (raw HTML gzip)
Phase 3: person API    — GET /api/person/show.php × N persons (raw JSON gzip)
Phase 4: preview       — GET /api/stafflists/preview.php (cron snapshot)

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
  data/keyframe/person_raw/<id>.json.gz
  data/keyframe/api/roles.json
  data/keyframe/api/preview.json
  data/keyframe/checkpoint.json
"""

from __future__ import annotations

import asyncio
import gzip
import json
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import typer
import structlog

from src.scrapers.bronze_writer import BronzeWriter
from src.scrapers.checkpoint import atomic_write_json, load_json_or
from src.scrapers.keyframe_api import KeyframeApiClient
from src.scrapers.parsers import keyframe as html_parser
from src.scrapers.parsers import keyframe_api as api_parser

log = structlog.get_logger()

app = typer.Typer()

DEFAULT_DATA_DIR = Path("data/keyframe")
DEFAULT_DELAY = 5.0
CHECKPOINT_INTERVAL = 10  # flush checkpoint every N items


# =============================================================================
# Checkpoint helpers
# =============================================================================


def _blank_checkpoint() -> dict:
    return {
        "anime_phase": {"completed_slugs": [], "all_slugs": []},
        "person_phase": {"completed_ids": [], "all_ids": []},
        "roles_master_fetched_at": None,
        "preview_fetched_at": None,
        "stats": {},
    }


def _load_checkpoint(cp_path: Path, fresh: bool) -> dict:
    """Load checkpoint or return blank structure."""
    if fresh:
        return _blank_checkpoint()
    data = load_json_or(cp_path, None)
    if not isinstance(data, dict):
        return _blank_checkpoint()
    blank = _blank_checkpoint()
    for k, v in blank.items():
        data.setdefault(k, v)
    return data


def _save_checkpoint(cp_path: Path, cp: dict) -> None:
    """Atomically write checkpoint JSON."""
    atomic_write_json(cp_path, cp, ensure_ascii=False, indent=2)


# =============================================================================
# Phase 1 helper: sitemap
# =============================================================================


def _parse_sitemap_xml(xml_text: str) -> list[str]:
    """Parse sitemap XML and extract /staff/<slug> paths."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        log.warning("keyframe_sitemap_parse_error", err=str(exc)[:120])
        return []

    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    slugs: list[str] = []
    for url_elem in root.findall("s:url/s:loc", ns):
        url = url_elem.text or ""
        m = re.search(r"/staff/([^/]+)$", url)
        if m:
            slugs.append(m.group(1))
    log.info("keyframe_sitemap_parsed", total_slugs=len(slugs))
    return slugs


# =============================================================================
# Phase 2 helper: extract person ids from preloadData
# =============================================================================


def _collect_person_ids_from_preload(data: dict) -> set[int]:
    """Extract all numeric person IDs from menus credits (non-studio entries)."""
    ids: set[int] = set()
    for menu in data.get("menus") or []:
        for section in menu.get("credits") or []:
            for role_entry in section.get("roles") or []:
                for staff in role_entry.get("staff") or []:
                    pid = staff.get("id")
                    if pid is not None and not staff.get("isStudio"):
                        try:
                            ids.add(int(pid))
                        except (TypeError, ValueError):
                            pass
    return ids


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
    """Execute Phase 0-4 sequentially.

    Returns final stats dict.
    """
    raw_html_dir = data_dir / "raw"
    raw_json_dir = data_dir / "person_raw"
    api_dir = data_dir / "api"
    cp_path = data_dir / "checkpoint.json"

    for d in (raw_html_dir, raw_json_dir, api_dir):
        d.mkdir(parents=True, exist_ok=True)

    cp = _load_checkpoint(cp_path, fresh)
    stats: dict = cp.get("stats") or {}
    stats.setdefault("roles_fetched", 0)
    stats.setdefault("anime_fetched", 0)
    stats.setdefault("anime_skipped", 0)
    stats.setdefault("person_fetched", 0)
    stats.setdefault("person_skipped", 0)

    client = KeyframeApiClient(delay=delay)
    try:
        await _phase0_roles(client, api_dir, cp, stats)
        _save_checkpoint(cp_path, {**cp, "stats": stats})

        slugs = await _phase1_sitemap(client, cp, stats)
        _save_checkpoint(cp_path, {**cp, "stats": stats})

        if not slugs:
            log.warning("keyframe_no_slugs_found")
            return stats

        remaining = _filter_remaining_slugs(slugs, cp, max_anime)
        person_ids = await _phase2_anime_html(
            client, remaining, raw_html_dir, cp, cp_path, stats
        )

        cp["person_phase"]["all_ids"] = sorted(person_ids)
        _save_checkpoint(cp_path, {**cp, "stats": stats})

        if not skip_persons:
            await _phase3_persons(
                client, cp, cp_path, raw_json_dir, stats
            )

        await _phase4_preview(client, api_dir, cp, stats)
        _save_checkpoint(cp_path, {**cp, "stats": stats})

    finally:
        _save_checkpoint(cp_path, {**cp, "stats": stats})
        await client.close()

    log.info("keyframe_scrape_complete", **stats)
    return stats


# =============================================================================
# Phase 0: roles master
# =============================================================================


async def _phase0_roles(
    client: KeyframeApiClient,
    api_dir: Path,
    cp: dict,
    stats: dict,
) -> None:
    """Fetch /api/data/roles.php and write to BRONZE roles_master table."""
    if cp.get("roles_master_fetched_at"):
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

    cp["roles_master_fetched_at"] = int(time.time())
    stats["roles_fetched"] = len(roles)
    log.info("keyframe_phase0_done", count=len(roles))


# =============================================================================
# Phase 1: sitemap
# =============================================================================


async def _phase1_sitemap(
    client: KeyframeApiClient,
    cp: dict,
    stats: dict,
) -> list[str]:
    """Fetch sitemap.xml and return full slug list (cache in checkpoint)."""
    if cp["anime_phase"]["all_slugs"]:
        log.info("keyframe_phase1_skip", reason="already_in_checkpoint")
        return cp["anime_phase"]["all_slugs"]

    log.info("keyframe_phase1_start")
    xml_text = await client.get_sitemap()
    if not xml_text:
        log.warning("keyframe_phase1_failed")
        return []

    slugs = _parse_sitemap_xml(xml_text)
    cp["anime_phase"]["all_slugs"] = slugs
    stats["sitemap_slugs"] = len(slugs)
    log.info("keyframe_phase1_done", slugs=len(slugs))
    return slugs


def _filter_remaining_slugs(
    all_slugs: list[str], cp: dict, max_anime: int
) -> list[str]:
    """Return slugs not yet in completed_slugs, capped at max_anime."""
    done = set(cp["anime_phase"]["completed_slugs"])
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


async def _phase2_anime_html(
    client: KeyframeApiClient,
    remaining: list[str],
    raw_html_dir: Path,
    cp: dict,
    cp_path: Path,
    stats: dict,
) -> set[int]:
    """Fetch and parse anime HTML pages. Returns accumulated person id set."""
    log.info("keyframe_phase2_start", count=len(remaining))

    person_ids: set[int] = set(cp["person_phase"].get("all_ids") or [])

    anime_bw = BronzeWriter("keyframe", table="anime")
    studios_bw = BronzeWriter("keyframe", table="anime_studios")
    categories_bw = BronzeWriter("keyframe", table="settings_categories")
    credits_bw = BronzeWriter("keyframe", table="credits")
    studios_master_bw = BronzeWriter("keyframe", table="studios_master")

    for i, slug in enumerate(remaining):
        html_text = await client.get_anime_page(slug)

        if html_text is None:
            log.debug("keyframe_phase2_skip", slug=slug)
            cp["anime_phase"]["completed_slugs"].append(slug)
            stats["anime_skipped"] = stats.get("anime_skipped", 0) + 1
            continue

        # Save raw HTML (gzip)
        gz_path = raw_html_dir / f"{slug}.html.gz"
        with gzip.open(gz_path, "wt", encoding="utf-8") as fh:
            fh.write(html_text)

        data = html_parser.extract_preload_data(html_text)
        if data is None:
            log.warning("keyframe_phase2_no_preload", slug=slug)
            cp["anime_phase"]["completed_slugs"].append(slug)
            continue

        anime_id = f"keyframe:{slug}"

        # anime metadata
        anime_meta = html_parser.parse_anime_meta(data, slug)
        # Serialize nested dicts to JSON strings for Parquet compatibility
        anime_row = {
            "id": anime_id,
            **anime_meta,
            "start_date": json.dumps(anime_meta.get("start_date") or {}, ensure_ascii=False),
            "end_date": json.dumps(anime_meta.get("end_date") or {}, ensure_ascii=False),
            "synonyms": json.dumps(anime_meta.get("synonyms") or [], ensure_ascii=False),
            "delimiters": json.dumps(anime_meta.get("delimiters"), ensure_ascii=False)
            if anime_meta.get("delimiters") is not None
            else None,
            "episode_delimiters": json.dumps(anime_meta.get("episode_delimiters"), ensure_ascii=False)
            if anime_meta.get("episode_delimiters") is not None
            else None,
            "role_delimiters": json.dumps(anime_meta.get("role_delimiters"), ensure_ascii=False)
            if anime_meta.get("role_delimiters") is not None
            else None,
            "staff_delimiters": json.dumps(anime_meta.get("staff_delimiters"), ensure_ascii=False)
            if anime_meta.get("staff_delimiters") is not None
            else None,
        }
        anime_bw.append(anime_row)

        # studios
        for studio_row in html_parser.parse_anime_studios(data):
            studios_bw.append({"anime_id": anime_id, **studio_row})

        # settings categories
        for cat_row in html_parser.parse_settings_categories(data):
            categories_bw.append({"anime_id": anime_id, **cat_row})

        # credits
        credit_rows = html_parser.parse_credits_from_data(data, slug)
        for c in credit_rows:
            # Normalize person_id and studio_id to str for stable parquet schema
            row = {"anime_id": anime_id, **c}
            if row.get("person_id") is not None:
                row["person_id"] = str(row["person_id"])
            if row.get("studio_id") is not None:
                try:
                    row["studio_id"] = int(row["studio_id"])
                except (TypeError, ValueError):
                    row["studio_id"] = None
            credits_bw.append(row)

        # studios master (dedup in memory across pages)
        for studio_row in html_parser.collect_studio_master(credit_rows):
            studios_master_bw.append(studio_row)

        # collect person ids
        person_ids.update(_collect_person_ids_from_preload(data))

        cp["anime_phase"]["completed_slugs"].append(slug)
        stats["anime_fetched"] = stats.get("anime_fetched", 0) + 1

        # Periodic checkpoint flush
        if (i + 1) % CHECKPOINT_INTERVAL == 0:
            for bw in (anime_bw, studios_bw, categories_bw, credits_bw, studios_master_bw):
                bw.flush()
            _save_checkpoint(cp_path, cp)
            log.info(
                "keyframe_phase2_checkpoint",
                progress=f"{i + 1}/{len(remaining)}",
                anime=stats["anime_fetched"],
                persons_seen=len(person_ids),
            )

    # Final flush
    for bw in (anime_bw, studios_bw, categories_bw, credits_bw, studios_master_bw):
        bw.flush()

    log.info(
        "keyframe_phase2_done",
        fetched=stats["anime_fetched"],
        person_ids_seen=len(person_ids),
    )
    return person_ids


# =============================================================================
# Phase 3: person API
# =============================================================================


async def _phase3_persons(
    client: KeyframeApiClient,
    cp: dict,
    cp_path: Path,
    raw_json_dir: Path,
    stats: dict,
) -> None:
    """Fetch person show.php for each unique person id collected in Phase 2."""
    done = set(cp["person_phase"]["completed_ids"])
    all_ids = cp["person_phase"]["all_ids"]
    remaining_ids = [i for i in all_ids if i not in done]

    log.info(
        "keyframe_phase3_start",
        total=len(all_ids),
        done=len(done),
        remaining=len(remaining_ids),
    )

    profile_bw = BronzeWriter("keyframe", table="person_profile")
    jobs_bw = BronzeWriter("keyframe", table="person_jobs")
    studios_bw = BronzeWriter("keyframe", table="person_studios")
    credits_bw = BronzeWriter("keyframe", table="person_credits")

    for i, pid in enumerate(remaining_ids):
        raw_data = await client.get_person_show(pid)

        if raw_data is None:
            log.debug("keyframe_phase3_skip", person_id=pid)
            cp["person_phase"]["completed_ids"].append(pid)
            stats["person_skipped"] = stats.get("person_skipped", 0) + 1
            continue

        # Save raw JSON (gzip)
        gz_path = raw_json_dir / f"{pid}.json.gz"
        with gzip.open(gz_path, "wt", encoding="utf-8") as fh:
            json.dump(raw_data, fh, ensure_ascii=False)

        parsed = api_parser.parse_person_show(raw_data)
        profile = parsed["profile"]

        # person_profile
        profile_bw.append({
            "person_id": profile["id"],
            "is_studio": profile["is_studio"],
            "name_ja": profile["name_ja"],
            "name_en": profile["name_en"],
            "aliases_json": json.dumps(profile["aliases_json"], ensure_ascii=False),
            "avatar": profile["avatar"],
            "bio": profile["bio"],
        })

        # person_jobs
        for job in parsed["jobs"]:
            jobs_bw.append({"person_id": pid, "job": job})

        # person_studios
        for studio in parsed["studios"]:
            studios_bw.append({
                "person_id": pid,
                "studio_name": studio["studio_name"],
                "alt_names": json.dumps(studio["alt_names"], ensure_ascii=False),
            })

        # person_credits
        for cred in parsed["credits"]:
            credits_bw.append({"person_id": pid, **cred})

        cp["person_phase"]["completed_ids"].append(pid)
        stats["person_fetched"] = stats.get("person_fetched", 0) + 1

        # Periodic flush
        if (i + 1) % CHECKPOINT_INTERVAL == 0:
            for bw in (profile_bw, jobs_bw, studios_bw, credits_bw):
                bw.flush()
            _save_checkpoint(cp_path, cp)
            log.info(
                "keyframe_phase3_checkpoint",
                progress=f"{i + 1}/{len(remaining_ids)}",
                persons=stats["person_fetched"],
            )

    for bw in (profile_bw, jobs_bw, studios_bw, credits_bw):
        bw.flush()

    log.info("keyframe_phase3_done", fetched=stats["person_fetched"])


# =============================================================================
# Phase 4: preview snapshot
# =============================================================================


async def _phase4_preview(
    client: KeyframeApiClient,
    api_dir: Path,
    cp: dict,
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
                bw.append({
                    "fetched_at": fetched_at,
                    "section": section,
                    **entry,
                    "studios_str": json.dumps(
                        entry.get("studios_str", []), ensure_ascii=False
                    ),
                    "contributors_json": json.dumps(
                        entry.get("contributors_json", []), ensure_ascii=False
                    ),
                })

    cp["preview_fetched_at"] = fetched_at
    stats["preview_total"] = normalized["total"]
    log.info("keyframe_phase4_done", total=normalized["total"])


# =============================================================================
# Typer CLI
# =============================================================================


@app.command()
def cmd_scrape_all(
    delay: float = typer.Option(DEFAULT_DELAY, help="Minimum seconds between requests"),
    skip_persons: bool = typer.Option(False, help="Skip Phase 3 (person API)"),
    max_anime: int = typer.Option(0, help="Cap Phase 2 to N anime (0 = all)"),
    fresh: bool = typer.Option(False, help="Ignore existing checkpoint"),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, help="Data directory"),
) -> None:
    """Phase 0-4: roles → sitemap → anime HTML → person API → preview."""
    from src.infra.logging import setup_logging
    from src.scrapers.logging_utils import configure_file_logging

    setup_logging()
    log_path = configure_file_logging("keyframe")
    log.info("keyframe_command_start", log_file=str(log_path))

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
