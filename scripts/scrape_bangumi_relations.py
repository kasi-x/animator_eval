"""Scrape bangumi subject × persons/characters relations via /v0 API → BRONZE parquet.

Reads anime subject IDs from Card 02 parquet, then for each subject fetches:
    GET /v0/subjects/{id}/persons    → src_bangumi_subject_persons
    GET /v0/subjects/{id}/characters → src_bangumi_subject_characters
                                       + src_bangumi_person_characters (actors nest)

Outputs three partitioned parquet tables:
    result/bronze/source=bangumi/table=subject_persons/date=YYYYMMDD/part-0.parquet
    result/bronze/source=bangumi/table=subject_characters/date=YYYYMMDD/part-0.parquet
    result/bronze/source=bangumi/table=person_characters/date=YYYYMMDD/part-0.parquet

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
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn

from src.scrapers.bangumi_scraper import BangumiClient
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
_ROW_GROUP_SIZE = 10_000

# ---------------------------------------------------------------------------
# PyArrow schemas (verified against live API responses 2026-04-25)
# ---------------------------------------------------------------------------

_SCHEMA_SUBJECT_PERSONS = pa.schema(
    [
        pa.field("subject_id", pa.int64()),
        pa.field("person_id", pa.int64()),
        # raw Chinese relation string e.g. "导演", "原画", "出版社"
        pa.field("position", pa.string()),
        # person type from API (1=individual, 2=corporation)
        pa.field("person_type", pa.int32()),
        # career list serialised as JSON string e.g. '["producer","director"]'
        pa.field("career", pa.string()),
        # episode range string, empty string if not present
        pa.field("eps", pa.string()),
        pa.field("name_raw", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    ]
)

_SCHEMA_SUBJECT_CHARACTERS = pa.schema(
    [
        pa.field("subject_id", pa.int64()),
        pa.field("character_id", pa.int64()),
        # raw relation string e.g. "主角", "配角", "客串"
        pa.field("relation", pa.string()),
        # character type from API (1=个人, 2=机体/舰艇/组织, 3=音乐作品, 4=其他)
        pa.field("type", pa.int32()),
        pa.field("name_raw", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    ]
)

_SCHEMA_PERSON_CHARACTERS = pa.schema(
    [
        pa.field("subject_id", pa.int64()),
        pa.field("character_id", pa.int64()),
        # voice actor / seiyuu person_id
        pa.field("person_id", pa.int64()),
        # actor type (1=individual, 2=corporation)
        pa.field("actor_type", pa.int32()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    ]
)

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
# Parquet writer helpers
# ---------------------------------------------------------------------------


def _rows_to_table(
    rows: list[dict[str, Any]], schema: pa.Schema
) -> pa.Table:
    """Convert a list of dicts to a pyarrow Table with the given schema."""
    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)
    columns: dict[str, list[Any]] = {f.name: [] for f in schema}
    for row in rows:
        for f in schema:
            columns[f.name].append(row.get(f.name))
    arrays = {
        f.name: pa.array(columns[f.name], type=f.type) for f in schema
    }
    return pa.table(arrays, schema=schema)


def _open_writer(
    table_name: str, date_str: str, schema: pa.Schema
) -> pq.ParquetWriter:
    """Create parquet output directory and open ParquetWriter."""
    out_dir = _BRONZE_ROOT / f"table={table_name}" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part-0.parquet"
    return pq.ParquetWriter(
        str(out_path),
        schema=schema,
        compression="zstd",
        write_batch_size=_ROW_GROUP_SIZE,
    )


# ---------------------------------------------------------------------------
# Core async scrape loop
# ---------------------------------------------------------------------------


async def _scrape(
    subject_ids: list[int],
    checkpoint: dict[str, Any],
    date_str: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Scrape persons + characters for each subject_id; write parquet + checkpoint."""
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

    writers = {
        "subject_persons": _open_writer("subject_persons", date_str, _SCHEMA_SUBJECT_PERSONS),
        "subject_characters": _open_writer("subject_characters", date_str, _SCHEMA_SUBJECT_CHARACTERS),
        "person_characters": _open_writer("person_characters", date_str, _SCHEMA_PERSON_CHARACTERS),
    }

    person_buf: list[dict[str, Any]] = []
    char_buf: list[dict[str, Any]] = []
    actor_buf: list[dict[str, Any]] = []

    def _flush_buffers() -> None:
        if person_buf:
            writers["subject_persons"].write_table(
                _rows_to_table(person_buf, _SCHEMA_SUBJECT_PERSONS),
                row_group_size=_ROW_GROUP_SIZE,
            )
            person_buf.clear()
        if char_buf:
            writers["subject_characters"].write_table(
                _rows_to_table(char_buf, _SCHEMA_SUBJECT_CHARACTERS),
                row_group_size=_ROW_GROUP_SIZE,
            )
            char_buf.clear()
        if actor_buf:
            writers["person_characters"].write_table(
                _rows_to_table(actor_buf, _SCHEMA_PERSON_CHARACTERS),
                row_group_size=_ROW_GROUP_SIZE,
            )
            actor_buf.clear()

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

                    person_buf.extend(
                        _build_person_rows(subject_id, persons, fetched_at)
                    )

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
                    char_buf.extend(c_rows)
                    actor_buf.extend(a_rows)

                    completed_set.add(subject_id)
                    checkpoint["completed_ids"] = sorted(completed_set)
                    checkpoint["failed_ids"] = failed_ids

                    # Flush every CHECKPOINT_FLUSH_EVERY subjects
                    if (i + 1) % _CHECKPOINT_FLUSH_EVERY == 0:
                        _flush_buffers()
                        _save_checkpoint(checkpoint)
                        log.info(
                            "bangumi_checkpoint_flushed",
                            completed=len(completed_set),
                            pending_remaining=len(pending) - (i + 1),
                        )

                    progress.advance(task)

    finally:
        # Flush any remaining buffered rows
        _flush_buffers()
        for w in writers.values():
            w.close()

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
            f"Output: result/bronze/source=bangumi/table={{subject_persons,subject_characters,person_characters}}/date={date_str}/"
        )


def _check_idempotent_exit(all_ids: list[int], date_str: str) -> None:
    """If all parquet files exist for today AND checkpoint is complete, log + exit."""
    tables = ["subject_persons", "subject_characters", "person_characters"]
    all_exist = all(
        (_BRONZE_ROOT / f"table={t}" / f"date={date_str}" / "part-0.parquet").exists()
        for t in tables
    )
    if not all_exist:
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
