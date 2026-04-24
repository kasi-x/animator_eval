"""One-shot migration: data/madb/metadata*.json → BRONZE parquet.

Reads MADB JSON-LD dumps (JSON-LD format with @graph entries),
converts to BronzeAnime / Person / Credit rows, writes partitioned parquet.

Usage:
    pixi run python scripts/migrate_madb_to_parquet.py
    pixi run python scripts/migrate_madb_to_parquet.py --dry-run
"""
from __future__ import annotations

import datetime as _dt
from collections import defaultdict
from pathlib import Path

import structlog
import typer

import sys
from pathlib import Path as PathlibPath

# Add parent to path for relative imports
sys.path.insert(0, str(PathlibPath(__file__).parent))

from _migrate_common import (
    group_files_by_date,
    load_json,
    report_summary,
)
from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT, BronzeWriter
from src.scrapers.parsers.mediaarts import (
    parse_jsonld_dump,
    make_madb_person_id,
)

log = structlog.get_logger()
app = typer.Typer()

DEFAULT_MADB_DIR = Path("data/madb")


def _iter_madb_files(madb_dir: Path) -> list[Path]:
    """metadata*.json ファイルを昇順で列挙."""
    return sorted(madb_dir.glob("metadata*.json"))


def _extract_anime_row(record: dict) -> dict:
    """parse_jsonld_dump output → BronzeAnime dict."""
    return {
        "id": f"madb:{record['id']}",
        "title_ja": record["title"],
        "year": record["year"],
        "format": record["format"] or None,
        "madb_id": record["id"],
        "studios": record["studios"],
    }


def _extract_person_rows(record: dict) -> list[dict]:
    """parse_jsonld_dump output → Person dicts (dedup by name_ja)."""
    contributors = record["contributors"]
    if not contributors:
        return []

    seen = set()
    rows: list[dict] = []
    for _role_ja, name_ja in contributors:
        if len(name_ja) < 2:
            continue
        if name_ja not in seen:
            person_id = make_madb_person_id(name_ja)
            rows.append(
                {
                    "id": person_id,
                    "name_ja": name_ja,
                    "madb_id": person_id,
                }
            )
            seen.add(name_ja)
    return rows


def _extract_credit_rows(record: dict) -> list[dict]:
    """parse_jsonld_dump output → Credit dicts."""
    anime_id = f"madb:{record['id']}"
    contributors = record["contributors"]
    if not contributors:
        return []

    rows: list[dict] = []
    for role_ja, name_ja in contributors:
        if len(name_ja) < 2:
            continue
        person_id = make_madb_person_id(name_ja)
        rows.append(
            {
                "person_id": person_id,
                "anime_id": anime_id,
                "role": "",  # role parsing は pipeline 側で実施
                "raw_role": role_ja,
                "source": "mediaarts",
            }
        )
    return rows


@app.command()
def main(
    madb_dir: Path = typer.Option(DEFAULT_MADB_DIR),
    bronze_root: Path = typer.Option(DEFAULT_BRONZE_ROOT),
    dry_run: bool = typer.Option(False),
    scrape_date: str | None = typer.Option(None, help="YYYY-MM-DD override"),
) -> None:
    if not madb_dir.exists():
        log.error("migrate_madb_dir_missing", path=str(madb_dir))
        raise typer.Exit(1)

    files = _iter_madb_files(madb_dir)
    if not files:
        log.error("migrate_madb_no_files_found", path=str(madb_dir))
        raise typer.Exit(1)

    log.info("migrate_madb_scanning", count=len(files))

    override = _dt.date.fromisoformat(scrape_date) if scrape_date else None
    buckets = group_files_by_date(files, override_date=override)

    counts = defaultdict(int)
    person_cache: dict[str, None] = {}  # name_ja dedup tracker

    for d, paths in buckets.items():
        anime_bw = BronzeWriter("mediaarts", table="anime", root=bronze_root, date=d)
        persons_bw = BronzeWriter(
            "mediaarts", table="persons", root=bronze_root, date=d
        )
        credits_bw = BronzeWriter(
            "mediaarts", table="credits", root=bronze_root, date=d
        )

        for p in paths:
            log.info("migrate_madb_parsing_file", file=p.name)
            doc = load_json(p)
            if doc is None:
                counts["json_failed"] += 1
                continue

            # parse_jsonld_dump はすべてのメタデータ*.json をサポート
            # format_code は dry-run では不要だが、推測可能な場合は使用
            records = parse_jsonld_dump(p, format_code="")
            counts["graph_entries_parsed"] += len(records)

            for record in records:
                # Extract anime row
                anime_row = _extract_anime_row(record)
                if not dry_run:
                    anime_bw.append(anime_row)
                counts["anime_rows"] += 1

                # Extract person rows (dedup within this batch)
                person_rows = _extract_person_rows(record)
                for person_row in person_rows:
                    name_ja = person_row["name_ja"]
                    if name_ja not in person_cache:
                        if not dry_run:
                            persons_bw.append(person_row)
                        person_cache[name_ja] = None
                        counts["person_rows"] += 1

                # Extract credit rows
                credit_rows = _extract_credit_rows(record)
                if not dry_run:
                    credits_bw.extend(credit_rows)
                counts["credit_rows"] += len(credit_rows)

        if not dry_run:
            anime_bw.flush()
            persons_bw.flush()
            credits_bw.flush()

    report_summary("mediaarts", dict(counts), dry_run)


if __name__ == "__main__":
    app()
