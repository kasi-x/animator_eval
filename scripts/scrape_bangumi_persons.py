"""Scrape bangumi person details via /v0/persons/{id} API → BRONZE parquet.

Reads the referenced person_id set from Card 03 parquet outputs
(subject_persons ∪ person_characters), then fetches each person via
BangumiClient.fetch_person() (already implemented in Card 03).

Output (UUID filenames, append-safe on resume):
    result/bronze/source=bangumi/table=persons/date=YYYYMMDD/{uuid}.parquet

Each run (including resumed runs) produces new UUID files; old files are preserved.
DuckDB reads all files via glob: read_parquet('.../**/*.parquet').

Checkpoint: data/bangumi/checkpoint_persons.json
    {"completed_ids": [...], "failed_ids": [{"id": 42, "status": 404}], "last_run_at": "..."}

Usage:
    pixi run python scripts/scrape_bangumi_persons.py --dry-run --limit 10
    pixi run python scripts/scrape_bangumi_persons.py --limit 10
    pixi run python scripts/scrape_bangumi_persons.py --resume
    pixi run python scripts/scrape_bangumi_persons.py --force
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

# Project root on sys.path when executed directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn

from src.scrapers.bangumi_scraper import BangumiClient
from src.scrapers.bronze_writer import BronzeWriter
from src.scrapers.exceptions import ScraperError

log = structlog.get_logger()
console = Console()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_CHECKPOINT_PATH = Path("data/bangumi/checkpoint_persons.json")
_SUBJECT_PERSONS_GLOB = "result/bronze/source=bangumi/table=subject_persons/**/*.parquet"
_PERSON_CHARACTERS_GLOB = "result/bronze/source=bangumi/table=person_characters/**/*.parquet"

_CHECKPOINT_FLUSH_EVERY = 100  # completed persons per flush

# ---------------------------------------------------------------------------
# Expected row keys (reference — BronzeWriter infers schema from first flush)
#
# Observed response keys from /v0/persons/{id}:
#   id, name, type, career, summary, infobox, gender, blood_type,
#   birth_year, birth_mon, birth_day, images, stat_comments,
#   stat_collects, last_modified, fetched_at
#
# infobox serialised as json.dumps(list) — raw, unexpanded.
# images serialised as json.dumps(dict) — all 4 keys preserved.
# Optional numeric fields (blood_type, birth_*) default to None.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------


def _load_checkpoint() -> dict[str, Any]:
    """Load persons checkpoint from disk; return empty structure if missing."""
    if _CHECKPOINT_PATH.exists():
        try:
            return json.loads(_CHECKPOINT_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("checkpoint_persons_load_error", error=str(exc))
    return {"completed_ids": [], "failed_ids": [], "last_run_at": None}


def _save_checkpoint(checkpoint: dict[str, Any]) -> None:
    """Atomically write checkpoint (tmp → rename)."""
    checkpoint["last_run_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    _CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(
        dir=_CHECKPOINT_PATH.parent, prefix=".checkpoint_persons_tmp_"
    )
    try:
        os.write(fd, json.dumps(checkpoint, ensure_ascii=False).encode("utf-8"))
        os.close(fd)
        Path(tmp_str).rename(_CHECKPOINT_PATH)
    except Exception:
        os.close(fd)
        Path(tmp_str).unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# Person ID source
# ---------------------------------------------------------------------------


def _load_person_ids() -> list[int]:
    """Return sorted list of referenced person_ids (subject_persons ∪ person_characters)."""
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT DISTINCT person_id
        FROM read_parquet('{_SUBJECT_PERSONS_GLOB}')
        UNION
        SELECT DISTINCT person_id
        FROM read_parquet('{_PERSON_CHARACTERS_GLOB}')
        ORDER BY 1
    """).fetchall()
    con.close()
    return [int(r[0]) for r in rows]


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------


def _build_person_row(person: dict[str, Any], fetched_at: dt.datetime) -> dict[str, Any]:
    """Convert /v0/persons/{id} response dict to a parquet row dict."""
    stat = person.get("stat") or {}
    images = person.get("images") or {}
    return {
        "id": int(person.get("id") or 0),
        "name": str(person.get("name") or ""),
        "type": _to_int32(person.get("type")),
        "career": json.dumps(person.get("career") or [], ensure_ascii=False),
        "summary": str(person.get("summary") or ""),
        "infobox": json.dumps(person.get("infobox") or [], ensure_ascii=False),
        "gender": str(person.get("gender") or "") or None,
        "blood_type": _to_int32(person.get("blood_type")),
        "birth_year": _to_int32(person.get("birth_year")),
        "birth_mon": _to_int32(person.get("birth_mon")),
        "birth_day": _to_int32(person.get("birth_day")),
        "images": json.dumps(images, ensure_ascii=False),
        "locked": bool(person.get("locked", False)),
        "stat_comments": _to_int32(stat.get("comments")),
        "stat_collects": _to_int32(stat.get("collects")),
        "last_modified": str(person.get("last_modified") or ""),
        "fetched_at": fetched_at,
    }


def _to_int32(value: Any) -> int | None:
    """Convert value to int, returning None for None/falsy zero-like misses."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Core async scrape loop
# ---------------------------------------------------------------------------


async def _scrape(
    person_ids: list[int],
    checkpoint: dict[str, Any],
    date_str: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Fetch person detail for each person_id; write parquet + checkpoint.

    Each flush writes a new UUID parquet file so resumed runs never overwrite
    previously written data — old files are preserved and DuckDB reads all via glob.
    """
    completed_set: set[int] = set(checkpoint.get("completed_ids") or [])
    failed_ids: list[dict[str, Any]] = list(checkpoint.get("failed_ids") or [])
    failed_set: set[int] = {f["id"] for f in failed_ids}

    pending = [pid for pid in person_ids if pid not in completed_set and pid not in failed_set]

    if dry_run:
        eta_secs = len(pending)  # 1 req/sec
        console.print(
            f"[bold cyan]dry-run[/bold cyan]  "
            f"total={len(person_ids):,}  completed={len(completed_set):,}  "
            f"failed={len(failed_set):,}  pending={len(pending):,}  "
            f"ETA≈{eta_secs // 60}m {eta_secs % 60}s"
        )
        return checkpoint

    if not pending:
        console.print("[green]All persons already completed — nothing to do.[/green]")
        return checkpoint

    # BronzeWriter: each flush writes a new UUID file; old files are preserved on resume.
    # with block auto-flushes + compacts the partition on exit.
    date = dt.date.fromisoformat(date_str)
    with BronzeWriter("bangumi", table="persons", date=date) as bw:
        try:
            async with BangumiClient() as client:
                with Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeRemainingColumn(),
                    console=console,
                    transient=False,
                ) as progress:
                    task = progress.add_task("scraping persons", total=len(pending))

                    for i, person_id in enumerate(pending):
                        fetched_at = dt.datetime.now(dt.timezone.utc)

                        try:
                            person = await client.fetch_person(person_id)
                        except ScraperError as exc:
                            log.error(
                                "bangumi_person_fetch_failed",
                                person_id=person_id,
                                error=str(exc),
                            )
                            failed_ids.append(
                                {"id": person_id, "status": "error", "detail": str(exc)}
                            )
                            failed_set.add(person_id)
                            progress.advance(task)
                            continue

                        if person is None:
                            # 404 — skip + record
                            failed_ids.append({"id": person_id, "status": 404})
                            failed_set.add(person_id)
                            log.info("bangumi_person_not_found", person_id=person_id)
                            progress.advance(task)
                            continue

                        bw.append(_build_person_row(person, fetched_at))
                        completed_set.add(person_id)
                        checkpoint["completed_ids"] = sorted(completed_set)
                        checkpoint["failed_ids"] = failed_ids

                        if (i + 1) % _CHECKPOINT_FLUSH_EVERY == 0:
                            bw.flush()
                            _save_checkpoint(checkpoint)
                            log.info(
                                "bangumi_persons_checkpoint_flushed",
                                completed=len(completed_set),
                                pending_remaining=len(pending) - (i + 1),
                            )

                        progress.advance(task)

        finally:
            # Always flush remaining rows so partial progress is persisted on Ctrl+C
            bw.flush()

    checkpoint["completed_ids"] = sorted(completed_set)
    checkpoint["failed_ids"] = failed_ids
    _save_checkpoint(checkpoint)
    log.info(
        "bangumi_persons_scrape_done",
        completed=len(completed_set),
        failed=len(failed_ids),
    )
    return checkpoint


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="scrape-bangumi-persons",
    help="Fetch bangumi person details via /v0/persons/{id} API → BRONZE parquet.",
    add_completion=False,
)


@app.command()
def main(
    limit: int = typer.Option(
        0,
        "--limit",
        "-n",
        help="Process at most N not-yet-completed persons (0 = all).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show pending count and ETA; do not write anything.",
    ),
    resume: bool = typer.Option(
        True,
        "--resume/--no-resume",
        help="Honor existing checkpoint (default: yes).",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Ignore checkpoint; reprocess all persons.",
    ),
) -> None:
    """Scrape bangumi person details (Card 04) via /v0/persons/{id} API."""
    structlog.configure(processors=[structlog.dev.ConsoleRenderer()])
    asyncio.run(
        _main(
            limit=limit,
            dry_run=dry_run,
            resume=resume,
            force=force,
        )
    )


async def _main(
    *,
    limit: int,
    dry_run: bool,
    resume: bool,
    force: bool,
) -> None:
    date_str = dt.date.today().strftime("%Y%m%d")

    # 1. Load referenced person_ids from Card 03 parquet outputs
    try:
        all_ids = _load_person_ids()
    except Exception as exc:
        console.print(f"[red]Failed to load person IDs:[/red] {exc}")
        raise typer.Exit(1) from exc

    console.print(f"[cyan]Referenced person IDs:[/cyan] {len(all_ids):,}")

    # 2. Load checkpoint
    if force:
        checkpoint: dict[str, Any] = {"completed_ids": [], "failed_ids": [], "last_run_at": None}
        log.info("bangumi_persons_checkpoint_cleared_force")
    elif resume:
        checkpoint = _load_checkpoint()
        completed_count = len(checkpoint.get("completed_ids") or [])
        failed_count = len(checkpoint.get("failed_ids") or [])
        console.print(
            f"[cyan]Checkpoint:[/cyan] completed={completed_count:,}  failed={failed_count:,}"
        )
    else:
        checkpoint = {"completed_ids": [], "failed_ids": [], "last_run_at": None}

    # 3. Apply --limit: slice the pending list only
    completed_set = set(checkpoint.get("completed_ids") or [])
    failed_set = {f["id"] for f in (checkpoint.get("failed_ids") or [])}
    pending = [pid for pid in all_ids if pid not in completed_set and pid not in failed_set]

    if limit > 0:
        pending = pending[:limit]

    console.print(
        f"[cyan]Pending this run:[/cyan] {len(pending):,}  "
        f"(limit={'all' if limit == 0 else limit})"
    )

    # Pass only the pending slice (not all_ids) so --limit is honoured exactly
    await _scrape(
        person_ids=pending if limit > 0 else all_ids,
        checkpoint=checkpoint,
        date_str=date_str,
        dry_run=dry_run,
    )

    if not dry_run:
        console.print(
            f"[green]Done.[/green] "
            f"Output: result/bronze/source=bangumi/table=persons/date={date_str}/ "
            f"(UUID parquet files; read via glob)"
        )


if __name__ == "__main__":
    app()
