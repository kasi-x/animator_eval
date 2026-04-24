"""One-shot migration: data/seesaawiki/parsed/*.json → BRONZE parquet.

Reads parsed JSON intermediates (saved by seesaawiki_scraper.save_parsed_intermediate),
converts to BronzeAnime / Credit rows, writes partitioned parquet.

Usage:
    pixi run python scripts/migrate_seesaawiki_to_parquet.py
    pixi run python scripts/migrate_seesaawiki_to_parquet.py --dry-run
"""
from __future__ import annotations

import datetime as _dt
from collections import defaultdict
from pathlib import Path

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

DEFAULT_PARSED_DIR = Path("data/seesaawiki/parsed")


def _extract_anime_row(doc: dict) -> dict | None:
    """parsed JSON → BronzeAnime dict. required: title + anime_id."""
    anime_id = doc.get("anime_id")
    title = doc.get("title") or ""
    if not anime_id:
        return None
    return {
        "id": anime_id,
        "title_ja": title,
        "title_en": "",
    }


def _extract_credit_rows(doc: dict) -> list[dict]:
    """parsed JSON → Credit dicts. episodes + series_staff を統合、dedup."""
    anime_id = doc.get("anime_id")
    if not anime_id:
        return []

    rows: list[dict] = []
    # 1. series_staff (全話共通)
    for credit in doc.get("series_staff", []) or []:
        rows.append(
            {
                "anime_id": anime_id,
                "role": credit.get("role", ""),
                "name": credit.get("name", ""),
                "position": credit.get("position", 0),
                "episode": None,
                "is_company": credit.get("is_company", False),
                "affiliation": credit.get("affiliation"),
            }
        )
    # 2. episodes[].credits (話別)
    for ep in doc.get("episodes", []) or []:
        ep_no = ep.get("episode")
        for credit in ep.get("credits", []) or []:
            rows.append(
                {
                    "anime_id": anime_id,
                    "role": credit.get("role", ""),
                    "name": credit.get("name", ""),
                    "position": credit.get("position", 0),
                    "episode": ep_no,
                    "is_company": credit.get("is_company", False),
                    "affiliation": credit.get("affiliation"),
                }
            )
    return rows


@app.command()
def main(
    parsed_dir: Path = typer.Option(DEFAULT_PARSED_DIR),
    bronze_root: Path = typer.Option(DEFAULT_BRONZE_ROOT),
    dry_run: bool = typer.Option(False),
    scrape_date: str | None = typer.Option(None, help="YYYY-MM-DD override"),
) -> None:
    if not parsed_dir.exists():
        log.error("migrate_seesaa_parsed_dir_missing", path=str(parsed_dir))
        raise typer.Exit(1)

    files = list(iter_json_files(parsed_dir, pattern="*.json"))
    log.info("migrate_seesaa_scanning", count=len(files))

    override = _dt.date.fromisoformat(scrape_date) if scrape_date else None
    buckets = group_files_by_date(files, override_date=override)

    counts = defaultdict(int)

    for d, paths in buckets.items():
        anime_bw = BronzeWriter("seesaawiki", table="anime", root=bronze_root, date=d)
        credits_bw = BronzeWriter("seesaawiki", table="credits", root=bronze_root, date=d)

        for p in paths:
            doc = load_json(p)
            if doc is None:
                counts["json_failed"] += 1
                continue
            anime_row = _extract_anime_row(doc)
            if anime_row is None:
                counts["anime_skipped"] += 1
                continue
            if not dry_run:
                anime_bw.append(anime_row)
            counts["anime_rows"] += 1

            credit_rows = _extract_credit_rows(doc)
            if not dry_run:
                credits_bw.extend(credit_rows)
            counts["credit_rows"] += len(credit_rows)

        if not dry_run:
            anime_bw.flush()
            credits_bw.flush()

    report_summary("seesaawiki", dict(counts), dry_run)


if __name__ == "__main__":
    app()
