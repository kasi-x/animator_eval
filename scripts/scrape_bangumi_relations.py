"""Scrape bangumi subject × persons/characters relations via /v0 API → BRONZE parquet.

Reads anime subject IDs from Card 02 parquet, then for each subject fetches:
    GET /v0/subjects/{id}/persons    → src_bangumi_subject_persons
    GET /v0/subjects/{id}/characters → src_bangumi_subject_characters
                                       + src_bangumi_person_characters (actors nest)

Outputs three partitioned parquet tables (UUID filenames, append-safe on resume):
    result/bronze/source=bangumi/table=subject_persons/date=YYYYMMDD/{uuid}.parquet
    result/bronze/source=bangumi/table=subject_characters/date=YYYYMMDD/{uuid}.parquet
    result/bronze/source=bangumi/table=person_characters/date=YYYYMMDD/{uuid}.parquet

Each run (including resumed runs) produces new UUID files; old files are preserved.
DuckDB reads all files via glob: read_parquet('.../**/*.parquet').

Checkpoint: data/bangumi/checkpoint_relations.json
    {"completed_ids": [...], "failed_ids": [{"id": 42, "status": 404}], "last_run_at": "..."}

Usage:
    pixi run python scripts/scrape_bangumi_relations.py --dry-run --limit 10
    pixi run python scripts/scrape_bangumi_relations.py --limit 10
    pixi run python scripts/scrape_bangumi_relations.py --resume
    pixi run python scripts/scrape_bangumi_relations.py --force
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

_BRONZE_ROOT = Path("result/bronze/source=bangumi")
_CHECKPOINT_PATH = Path("data/bangumi/checkpoint_relations.json")
_SUBJECTS_PARQUET_GLOB = "result/bronze/source=bangumi/table=subjects/**/*.parquet"

_CHECKPOINT_FLUSH_EVERY = 100  # completed subjects per flush


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------


def _load_checkpoint() -> dict[str, Any]:
    """Load checkpoint from disk; return empty structure if missing."""
    if _CHECKPOINT_PATH.exists():
        try:
            return json.loads(_CHECKPOINT_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("checkpoint_load_error", error=str(exc))
    return {"completed_ids": [], "failed_ids": [], "last_run_at": None}


def _save_checkpoint(checkpoint: dict[str, Any]) -> None:
    """Atomically write checkpoint (tmp → rename)."""
    checkpoint["last_run_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    _CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(
        dir=_CHECKPOINT_PATH.parent, prefix=".checkpoint_relations_tmp_"
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
# Subject ID source
# ---------------------------------------------------------------------------


def _load_subject_ids(subject_ids_file: Path | None = None) -> list[int]:
    """Return sorted list of anime subject_ids from Bronze parquet (or override file)."""
    con = duckdb.connect()
    if subject_ids_file is not None:
        glob = str(subject_ids_file)
    else:
        glob = _SUBJECTS_PARQUET_GLOB
    rows = con.execute(
        f"SELECT id FROM read_parquet('{glob}') ORDER BY id"
    ).fetchall()
    con.close()
    return [int(r[0]) for r in rows]


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------


def _build_person_rows(
    subject_id: int,
    persons: list[dict[str, Any]],
    fetched_at: dt.datetime,
) -> list[dict[str, Any]]:
    """Convert /persons API list to subject_persons rows."""
    rows = []
    for p in persons:
        rows.append(
            {
                "subject_id": subject_id,
                "person_id": int(p.get("id", 0)),
                "position": str(p.get("relation") or ""),
                "person_type": int(p.get("type") or 0),
                "career": json.dumps(p.get("career") or [], ensure_ascii=False),
                "eps": str(p.get("eps") or ""),
                "name_raw": str(p.get("name") or ""),
                "fetched_at": fetched_at,
            }
        )
    return rows


def _build_character_and_actor_rows(
    subject_id: int,
    characters: list[dict[str, Any]],
    fetched_at: dt.datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Convert /characters API list to (subject_characters rows, person_characters rows)."""
    char_rows: list[dict[str, Any]] = []
    actor_rows: list[dict[str, Any]] = []

    for c in characters:
        char_id = int(c.get("id", 0))
        char_rows.append(
            {
                "subject_id": subject_id,
                "character_id": char_id,
                "relation": str(c.get("relation") or ""),
                "type": int(c.get("type") or 0),
                "name_raw": str(c.get("name") or ""),
                "fetched_at": fetched_at,
            }
        )
        # Explode actors nest → person_characters
        for actor in c.get("actors") or []:
            actor_rows.append(
                {
                    "subject_id": subject_id,
                    "character_id": char_id,
                    "person_id": int(actor.get("id", 0)),
                    "actor_type": int(actor.get("type") or 0),
                    "fetched_at": fetched_at,
                }
            )

    return char_rows, actor_rows


# ---------------------------------------------------------------------------
# Core async scrape loop
# ---------------------------------------------------------------------------


async def _scrape(
    subject_ids: list[int],
    checkpoint: dict[str, Any],
    date_str: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Scrape persons + characters for each subject_id; write parquet + checkpoint.

    Each flush writes a new UUID parquet file so resumed runs never overwrite
    previously written data — old files are preserved and DuckDB reads all via glob.
    """
    completed_set: set[int] = set(checkpoint.get("completed_ids") or [])
    failed_ids: list[dict[str, Any]] = list(checkpoint.get("failed_ids") or [])
    failed_set: set[int] = {f["id"] for f in failed_ids}

    pending = [sid for sid in subject_ids if sid not in completed_set and sid not in failed_set]

    if dry_run:
        total_req = len(pending) * 2
        eta_secs = total_req  # 1 req/sec + overhead
        console.print(
            f"[bold cyan]dry-run[/bold cyan]  "
            f"total={len(subject_ids):,}  completed={len(completed_set):,}  "
            f"pending={len(pending):,}  "
            f"estimated_requests={total_req:,}  "
            f"ETA≈{eta_secs // 60}m {eta_secs % 60}s"
        )
        return checkpoint

    if not pending:
        console.print("[green]All subjects already completed — nothing to do.[/green]")
        return checkpoint

    # Use BronzeWriter for each table: each flush writes a new UUID file, so
    # resuming a scrape appends new files without overwriting previous data.
    date = dt.date.fromisoformat(date_str)
    bw_persons = BronzeWriter("bangumi", table="subject_persons", date=date)
    bw_chars = BronzeWriter("bangumi", table="subject_characters", date=date)
    bw_actors = BronzeWriter("bangumi", table="person_characters", date=date)

    def _flush_writers() -> None:
        bw_persons.flush()
        bw_chars.flush()
        bw_actors.flush()

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
                task = progress.add_task("scraping", total=len(pending))

                for i, subject_id in enumerate(pending):
                    fetched_at = dt.datetime.now(dt.timezone.utc)

                    # Fetch persons
                    try:
                        persons = await client.fetch_subject_persons(subject_id)
                    except ScraperError as exc:
                        log.error(
                            "bangumi_persons_fetch_failed",
                            subject_id=subject_id,
                            error=str(exc),
                        )
                        failed_ids.append({"id": subject_id, "status": "error", "detail": str(exc)})
                        failed_set.add(subject_id)
                        progress.advance(task)
                        continue

                    if persons is None:
                        # 404 — record in failed_ids with status 404
                        failed_ids.append({"id": subject_id, "status": 404})
                        failed_set.add(subject_id)
                        persons = []

                    for row in _build_person_rows(subject_id, persons, fetched_at):
                        bw_persons.append(row)

                    # Fetch characters
                    try:
                        characters = await client.fetch_subject_characters(subject_id)
                    except ScraperError as exc:
                        log.error(
                            "bangumi_characters_fetch_failed",
                            subject_id=subject_id,
                            error=str(exc),
                        )
                        characters = []

                    if characters is None:
                        characters = []

                    c_rows, a_rows = _build_character_and_actor_rows(
                        subject_id, characters, fetched_at
                    )
                    for row in c_rows:
                        bw_chars.append(row)
                    for row in a_rows:
                        bw_actors.append(row)

                    completed_set.add(subject_id)
                    checkpoint["completed_ids"] = sorted(completed_set)
                    checkpoint["failed_ids"] = failed_ids

                    # Flush every CHECKPOINT_FLUSH_EVERY subjects — each flush
                    # writes a new UUID file; old files are preserved on resume.
                    if (i + 1) % _CHECKPOINT_FLUSH_EVERY == 0:
                        _flush_writers()
                        _save_checkpoint(checkpoint)
                        log.info(
                            "bangumi_checkpoint_flushed",
                            completed=len(completed_set),
                            pending_remaining=len(pending) - (i + 1),
                        )

                    progress.advance(task)

    finally:
        # Always flush remaining rows so partial progress is persisted on Ctrl+C
        _flush_writers()

    # Final checkpoint save
    checkpoint["completed_ids"] = sorted(completed_set)
    checkpoint["failed_ids"] = failed_ids
    _save_checkpoint(checkpoint)
    log.info(
        "bangumi_relations_scrape_done",
        completed=len(completed_set),
        failed=len(failed_ids),
    )
    return checkpoint


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="scrape-bangumi-relations",
    help="Fetch bangumi subject×persons/characters via /v0 API → BRONZE parquet.",
    add_completion=False,
)


@app.command()
def main(
    limit: int = typer.Option(
        0,
        "--limit",
        "-n",
        help="Process at most N not-yet-completed subjects (0 = all).",
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
        help="Ignore checkpoint; reprocess all subjects.",
    ),
    subject_ids_file: Path = typer.Option(
        None,
        "--subject-ids-file",
        help="Override subject ID source (parquet glob or path). Used by Card 06.",
        exists=False,
    ),
) -> None:
    """Scrape bangumi subject relations (persons + characters) via /v0 API."""
    structlog.configure(processors=[structlog.dev.ConsoleRenderer()])
    asyncio.run(
        _main(
            limit=limit,
            dry_run=dry_run,
            resume=resume,
            force=force,
            subject_ids_file=subject_ids_file,
        )
    )


async def _main(
    *,
    limit: int,
    dry_run: bool,
    resume: bool,
    force: bool,
    subject_ids_file: Path | None,
) -> None:
    date_str = dt.date.today().strftime("%Y%m%d")

    # 1. Load subject IDs
    try:
        all_ids = _load_subject_ids(subject_ids_file)
    except Exception as exc:
        console.print(f"[red]Failed to load subject IDs:[/red] {exc}")
        raise typer.Exit(1) from exc

    console.print(f"[cyan]Subject IDs loaded:[/cyan] {len(all_ids):,}")

    # 2. Idempotency check: if parquet already exists for today and all done, skip
    if not force and not dry_run:
        _check_idempotent_exit(all_ids, date_str)

    # 3. Load checkpoint
    if force:
        checkpoint: dict[str, Any] = {"completed_ids": [], "failed_ids": [], "last_run_at": None}
        log.info("bangumi_checkpoint_cleared_force")
    elif resume:
        checkpoint = _load_checkpoint()
        completed_count = len(checkpoint.get("completed_ids") or [])
        failed_count = len(checkpoint.get("failed_ids") or [])
        console.print(
            f"[cyan]Checkpoint:[/cyan] completed={completed_count:,}  failed={failed_count:,}"
        )
    else:
        checkpoint = {"completed_ids": [], "failed_ids": [], "last_run_at": None}

    # 4. Apply --limit
    completed_set = set(checkpoint.get("completed_ids") or [])
    failed_set = {f["id"] for f in (checkpoint.get("failed_ids") or [])}
    pending = [sid for sid in all_ids if sid not in completed_set and sid not in failed_set]

    if limit > 0:
        pending = pending[:limit]
        # Only scrape these ids — rebuild the "all_ids" list for the run
        target_ids = sorted(completed_set | set(pending))
    else:
        target_ids = all_ids

    console.print(
        f"[cyan]Pending:[/cyan] {len(pending):,}  "
        f"(limit={'all' if limit == 0 else limit})"
    )

    await _scrape(
        subject_ids=target_ids if limit == 0 else pending,
        checkpoint=checkpoint,
        date_str=date_str,
        dry_run=dry_run,
    )

    if not dry_run:
        console.print(
            f"[green]Done.[/green] "
            f"Output: result/bronze/source=bangumi/table={{subject_persons,subject_characters,person_characters}}/date={date_str}/ "
            f"(UUID parquet files; read via glob)"
        )


def _check_idempotent_exit(all_ids: list[int], date_str: str) -> None:
    """If parquet files exist for today AND checkpoint is complete, log + exit."""
    tables = ["subject_persons", "subject_characters", "person_characters"]
    all_have_data = all(
        any((_BRONZE_ROOT / f"table={t}" / f"date={date_str}").glob("*.parquet"))
        for t in tables
    )
    if not all_have_data:
        return

    checkpoint = _load_checkpoint()
    completed_set = set(checkpoint.get("completed_ids") or [])
    if set(all_ids).issubset(completed_set):
        console.print(
            "[green]All subjects already completed and parquet exists for today. "
            "Use --force to re-run.[/green]"
        )
        raise typer.Exit(0)


if __name__ == "__main__":
    app()
