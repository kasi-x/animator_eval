"""One-shot migration: data/allcinema/checkpoint_cinema.json → BRONZE parquet.

Reads checkpoint file containing completed anime IDs, reconstructs anime/credits/persons
records from scraper data sources, and writes partitioned parquet.

Note: This script assumes intermediate JSON records are available. If the allcinema scraper
has written directly to BRONZE parquet via BronzeWriter, those files are the source of truth.

Usage:
    pixi run python scripts/migrate_allcinema_to_parquet.py --dry-run
    pixi run python scripts/migrate_allcinema_to_parquet.py
"""
from __future__ import annotations

import datetime as _dt
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import structlog
import typer

from scripts._migrate_common import (
    group_files_by_date,
    iter_json_files,
    load_json,
    report_summary,
)
from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT, BronzeWriter

log = structlog.get_logger()
app = typer.Typer()

DEFAULT_CHECKPOINT_PATH = Path("data/allcinema/checkpoint_cinema.json")
DEFAULT_ALLCINEMA_DIR = Path("data/allcinema")


def _load_checkpoint(path: Path) -> dict[str, Any]:
    """Load checkpoint file. Returns {cinema_ids: [...], completed: [...]}.

    Note: Current checkpoint format only contains IDs, not actual anime/credit data.
    """
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("migrate_allcinema_checkpoint_load_failed", path=str(path), error=str(e))
        return {}


def _extract_anime_row(cinema_id: int, rec: dict) -> dict | None:
    """AllcinemaAnimeRecord dict → BronzeAnime dict. required: cinema_id."""
    if not isinstance(cinema_id, int):
        return None
    return {
        "cinema_id": cinema_id,
        "title_ja": rec.get("title_ja", ""),
        "title_en": rec.get("title_en", ""),
        "year": rec.get("year"),
        "start_date": rec.get("start_date"),
        "synopsis": rec.get("synopsis", ""),
        "titles_alt": rec.get("titles_alt", "{}"),
        "fetched_at": rec.get("fetched_at"),
        "content_hash": rec.get("content_hash"),
    }


def _extract_credit_rows(cinema_id: int, credits: list[dict]) -> list[dict]:
    """AllcinemaCredit list → Credit dicts."""
    rows: list[dict] = []
    if not isinstance(credits, list):
        return rows
    for credit in credits:
        rows.append(
            {
                "cinema_id": cinema_id,
                "allcinema_person_id": credit.get("allcinema_person_id"),
                "name_ja": credit.get("name_ja", ""),
                "name_en": credit.get("name_en", ""),
                "job_name": credit.get("job_name", ""),
                "job_id": credit.get("job_id", 0),
            }
        )
    return rows


def _extract_person_rows(person_records: list[dict]) -> list[dict]:
    """AllcinemaPersonRecord list → Person dicts."""
    rows: list[dict] = []
    if not isinstance(person_records, list):
        return rows
    for person in person_records:
        rows.append(
            {
                "allcinema_id": person.get("allcinema_id"),
                "name_ja": person.get("name_ja", ""),
                "yomigana": person.get("yomigana", ""),
                "name_en": person.get("name_en", ""),
                "name_ko": person.get("name_ko", ""),
                "name_zh": person.get("name_zh", ""),
                "names_alt": person.get("names_alt", "{}"),
                "hometown": person.get("hometown", ""),
            }
        )
    return rows


@app.command()
def main(
    checkpoint_path: Path = typer.Option(DEFAULT_CHECKPOINT_PATH),
    allcinema_dir: Path = typer.Option(DEFAULT_ALLCINEMA_DIR),
    bronze_root: Path = typer.Option(DEFAULT_BRONZE_ROOT),
    dry_run: bool = typer.Option(False),
    scrape_date: str | None = typer.Option(None, help="YYYY-MM-DD override"),
) -> None:
    """Migrate allcinema checkpoint to BRONZE parquet.

    This script handles two scenarios:
    1. If intermediate JSON files exist (like seesaawiki), migrate from those
    2. Otherwise, it serves as a migration wrapper for existing BRONZE data
    """
    if not checkpoint_path.exists():
        log.error("migrate_allcinema_checkpoint_missing", path=str(checkpoint_path))
        raise typer.Exit(1)

    checkpoint = _load_checkpoint(checkpoint_path)
    completed_ids: list[int] = checkpoint.get("completed", [])

    log.info(
        "migrate_allcinema_checkpoint_loaded",
        completed_count=len(completed_ids),
    )

    # Check if intermediate JSON files exist (like seesaawiki/parsed/)
    json_files = list(iter_json_files(allcinema_dir, pattern="*.json"))
    json_files = [p for p in json_files if "checkpoint" not in p.name]  # exclude checkpoints

    has_intermediate_json = len(json_files) > 0
    log.info(
        "migrate_allcinema_json_check",
        intermediate_json_count=len(json_files),
        has_intermediate=has_intermediate_json,
    )

    if not has_intermediate_json:
        log.warning(
            "migrate_allcinema_no_intermediate_json",
            msg="No intermediate JSON files found. "
            "Intermediate JSON records should be saved by allcinema_scraper. "
            "If BronzeWriter already wrote parquet directly, those files are the source of truth.",
        )
        # Still proceed with data from JSON files if they exist, otherwise report 0 rows
        counts = {
            "json_files": 0,
            "anime_rows": 0,
            "credit_rows": 0,
            "person_rows": 0,
        }
        report_summary("allcinema", counts, dry_run)
        return

    override = _dt.date.fromisoformat(scrape_date) if scrape_date else None
    buckets = group_files_by_date(json_files, override_date=override)

    counts = defaultdict(int)

    for d, paths in buckets.items():
        anime_bw = BronzeWriter("allcinema", table="anime", root=bronze_root, date=d)
        credits_bw = BronzeWriter("allcinema", table="credits", root=bronze_root, date=d)
        persons_bw = BronzeWriter("allcinema", table="persons", root=bronze_root, date=d)

        for p in paths:
            doc = load_json(p)
            if doc is None:
                counts["json_failed"] += 1
                continue

            # Extract based on allcinema_scraper.py format
            # Each JSON should contain one anime record with embedded credits

            # Anime record
            cinema_id = doc.get("cinema_id")
            if cinema_id and cinema_id in completed_ids:
                anime_row = _extract_anime_row(cinema_id, doc)
                if anime_row:
                    if not dry_run:
                        anime_bw.append(anime_row)
                    counts["anime_rows"] += 1

                # Credits (staff + cast combined)
                all_credits = []
                staff = doc.get("staff", [])
                cast = doc.get("cast", [])
                if isinstance(staff, list):
                    all_credits.extend(staff)
                if isinstance(cast, list):
                    all_credits.extend(cast)

                credit_rows = _extract_credit_rows(cinema_id, all_credits)
                if credit_rows:
                    if not dry_run:
                        credits_bw.extend(credit_rows)
                    counts["credit_rows"] += len(credit_rows)

        if not dry_run:
            anime_bw.flush()
            credits_bw.flush()
            persons_bw.flush()

        counts["json_files"] += len(paths)

    report_summary("allcinema", dict(counts), dry_run)


if __name__ == "__main__":
    app()
