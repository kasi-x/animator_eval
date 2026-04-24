"""Scrape bangumi character details via /v0/characters/{id} API → BRONZE parquet.

Reads the referenced character_id set from Card 03 parquet outputs
(subject_characters ∪ person_characters), then fetches each character via
BangumiClient.fetch_character() (already implemented in Card 03).

Output (UUID filenames, append-safe on resume):
    result/bronze/source=bangumi/table=characters/date=YYYYMMDD/{uuid}.parquet

Each run (including resumed runs) produces new UUID files; old files are preserved.
DuckDB reads all files via glob: read_parquet('.../**/*.parquet').

Checkpoint: data/bangumi/checkpoint_characters.json
    {"completed_ids": [...], "failed_ids": [{"id": 42, "status": 404}], "last_run_at": "..."}

API response shape (/v0/characters/{id}) confirmed 2026-04-24:
    id, name, type (int: 1=角色/2=機体/3=組織), gender, birth_day, birth_mon,
    birth_year, blood_type, images (dict: small/grid/large/medium),
    summary, infobox (list[dict], same nested structure as /v0/persons),
    stat (dict: comments/collects), locked (bool)
    NOTE: last_modified is NOT present in character responses (unlike persons).

Usage:
    pixi run python scripts/scrape_bangumi_characters.py --dry-run --limit 10
    pixi run python scripts/scrape_bangumi_characters.py --limit 10
    pixi run python scripts/scrape_bangumi_characters.py --resume
    pixi run python scripts/scrape_bangumi_characters.py --force
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

_CHECKPOINT_PATH = Path("data/bangumi/checkpoint_characters.json")
_SUBJECT_CHARACTERS_GLOB = "result/bronze/source=bangumi/table=subject_characters/**/*.parquet"
_PERSON_CHARACTERS_GLOB = "result/bronze/source=bangumi/table=person_characters/**/*.parquet"

_CHECKPOINT_FLUSH_EVERY = 100  # completed characters per flush

# ---------------------------------------------------------------------------
# Observed API schema note (Step 0 findings, 2026-04-24):
#
# /v0/characters/{id} keys:
#   id, name, type, gender, birth_day, birth_mon, birth_year, blood_type,
#   images (dict: small/grid/large/medium), summary,
#   infobox (list[dict] with nested value lists — same surprise as /v0/persons),
#   stat (dict: {comments, collects}), locked (bool)
#
# ABSENT (unlike /v0/persons): last_modified, career
# type (not role) is the character category integer (1=角色, 2=機体, 3=組織)
#
# Row schema (BronzeWriter infers from first flush):
#   id: int64 | name: string | type: int32 | summary: string | locked: bool
#   infobox: string (json.dumps of list)
#   gender: string | null | blood_type: int32 | null
#   birth_year: int32 | null | birth_mon: int32 | null | birth_day: int32 | null
#   images: string (json.dumps of dict)
#   stat_comments: int32 | null | stat_collects: int32 | null
#   fetched_at: timestamp (UTC)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------


def _load_checkpoint() -> dict[str, Any]:
    """Load characters checkpoint from disk; return empty structure if missing."""
    if _CHECKPOINT_PATH.exists():
        try:
            return json.loads(_CHECKPOINT_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("checkpoint_characters_load_error", error=str(exc))
    return {"completed_ids": [], "failed_ids": [], "last_run_at": None}


def _save_checkpoint(checkpoint: dict[str, Any]) -> None:
    """Atomically write checkpoint (tmp → rename)."""
    checkpoint["last_run_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    _CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(
        dir=_CHECKPOINT_PATH.parent, prefix=".checkpoint_characters_tmp_"
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
# Character ID source
# ---------------------------------------------------------------------------


def _load_character_ids() -> list[int]:
    """Return sorted list of referenced character_ids (subject_characters ∪ person_characters)."""
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT DISTINCT character_id
        FROM read_parquet('{_SUBJECT_CHARACTERS_GLOB}')
        UNION
        SELECT DISTINCT character_id
        FROM read_parquet('{_PERSON_CHARACTERS_GLOB}')
        ORDER BY 1
    """).fetchall()
    con.close()
    return [int(r[0]) for r in rows]


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------


def _build_character_row(character: dict[str, Any], fetched_at: dt.datetime) -> dict[str, Any]:
    """Convert /v0/characters/{id} response dict to a parquet row dict.

    Schema deviations from card (confirmed from live API 2026-04-24):
    - ``type`` stored instead of ``role`` — API returns ``type``, not ``role``
    - ``locked`` included — present in all observed responses
    - ``last_modified`` omitted — absent from character responses (unlike persons)
    """
    stat = character.get("stat") or {}
    images = character.get("images") or {}
    return {
        "id": int(character.get("id") or 0),
        "name": str(character.get("name") or ""),
        # type: character category (1=角色, 2=機体, 3=組織) — API field name is "type" not "role"
        "type": _to_int32(character.get("type")),
        "locked": bool(character.get("locked") or False),
        "summary": str(character.get("summary") or ""),
        # infobox is list[dict] with nested values — raw serialised as JSON string
        "infobox": json.dumps(character.get("infobox") or [], ensure_ascii=False),
        "gender": str(character.get("gender") or "") or None,
        "blood_type": _to_int32(character.get("blood_type")),
        "birth_year": _to_int32(character.get("birth_year")),
        "birth_mon": _to_int32(character.get("birth_mon")),
        "birth_day": _to_int32(character.get("birth_day")),
        "images": json.dumps(images, ensure_ascii=False),
        "stat_comments": _to_int32(stat.get("comments")),
        "stat_collects": _to_int32(stat.get("collects")),
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
    character_ids: list[int],
    checkpoint: dict[str, Any],
    date_str: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Fetch character detail for each character_id; write parquet + checkpoint.

    Each flush writes a new UUID parquet file so resumed runs never overwrite
    previously written data — old files are preserved and DuckDB reads all via glob.
    """
    completed_set: set[int] = set(checkpoint.get("completed_ids") or [])
    failed_ids: list[dict[str, Any]] = list(checkpoint.get("failed_ids") or [])
    failed_set: set[int] = {f["id"] for f in failed_ids}

    pending = [cid for cid in character_ids if cid not in completed_set and cid not in failed_set]

    if dry_run:
        eta_secs = len(pending)  # 1 req/sec
        console.print(
            f"[bold cyan]dry-run[/bold cyan]  "
            f"total={len(character_ids):,}  completed={len(completed_set):,}  "
            f"failed={len(failed_set):,}  pending={len(pending):,}  "
            f"ETA≈{eta_secs // 60}m {eta_secs % 60}s"
        )
        return checkpoint

    if not pending:
        console.print("[green]All characters already completed — nothing to do.[/green]")
        return checkpoint

    # BronzeWriter: each flush writes a new UUID file; old files are preserved on resume.
    date = dt.date.fromisoformat(date_str)
    bw = BronzeWriter("bangumi", table="characters", date=date)

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
                task = progress.add_task("scraping characters", total=len(pending))

                for i, character_id in enumerate(pending):
                    fetched_at = dt.datetime.now(dt.timezone.utc)

                    try:
                        character = await client.fetch_character(character_id)
                    except ScraperError as exc:
                        log.error(
                            "bangumi_character_fetch_failed",
                            character_id=character_id,
                            error=str(exc),
                        )
                        failed_ids.append(
                            {"id": character_id, "status": "error", "detail": str(exc)}
                        )
                        failed_set.add(character_id)
                        progress.advance(task)
                        continue

                    if character is None:
                        # 404 — skip + record
                        failed_ids.append({"id": character_id, "status": 404})
                        failed_set.add(character_id)
                        log.info("bangumi_character_not_found", character_id=character_id)
                        progress.advance(task)
                        continue

                    bw.append(_build_character_row(character, fetched_at))
                    completed_set.add(character_id)
                    checkpoint["completed_ids"] = sorted(completed_set)
                    checkpoint["failed_ids"] = failed_ids

                    if (i + 1) % _CHECKPOINT_FLUSH_EVERY == 0:
                        bw.flush()
                        _save_checkpoint(checkpoint)
                        log.info(
                            "bangumi_characters_checkpoint_flushed",
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
        "bangumi_characters_scrape_done",
        completed=len(completed_set),
        failed=len(failed_ids),
    )
    return checkpoint


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="scrape-bangumi-characters",
    help="Fetch bangumi character details via /v0/characters/{id} API → BRONZE parquet.",
    add_completion=False,
)


@app.command()
def main(
    limit: int = typer.Option(
        0,
        "--limit",
        "-n",
        help="Process at most N not-yet-completed characters (0 = all).",
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
        help="Ignore checkpoint; reprocess all characters.",
    ),
) -> None:
    """Scrape bangumi character details (Card 05) via /v0/characters/{id} API."""
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

    # 1. Load referenced character_ids from Card 03 parquet outputs
    try:
        all_ids = _load_character_ids()
    except Exception as exc:
        console.print(f"[red]Failed to load character IDs:[/red] {exc}")
        raise typer.Exit(1) from exc

    console.print(f"[cyan]Referenced character IDs:[/cyan] {len(all_ids):,}")

    # 2. Load checkpoint
    if force:
        checkpoint: dict[str, Any] = {"completed_ids": [], "failed_ids": [], "last_run_at": None}
        log.info("bangumi_characters_checkpoint_cleared_force")
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
    pending = [cid for cid in all_ids if cid not in completed_set and cid not in failed_set]

    if limit > 0:
        pending = pending[:limit]

    console.print(
        f"[cyan]Pending this run:[/cyan] {len(pending):,}  "
        f"(limit={'all' if limit == 0 else limit})"
    )

    # Pass only the pending slice so --limit is honoured exactly
    await _scrape(
        character_ids=pending,
        checkpoint=checkpoint,
        date_str=date_str,
        dry_run=dry_run,
    )

    if not dry_run:
        console.print(
            f"[green]Done.[/green] "
            f"Output: result/bronze/source=bangumi/table=characters/date={date_str}/ "
            f"(UUID parquet files; read via glob)"
        )


if __name__ == "__main__":
    app()
