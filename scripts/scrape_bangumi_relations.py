"""Scrape bangumi subject × persons/characters relations → BRONZE parquet.

Reads anime subject IDs from Card 02 parquet, then for each subject fetches:
    persons    → src_bangumi_subject_persons
    characters → src_bangumi_subject_characters + src_bangumi_person_characters (actors)

Client selection (--client flag, default: graphql):
    graphql  Uses BangumiGraphQLClient.fetch_subjects_batched() (batch_size=25).
             One POST per 25 subjects → ~100x throughput vs v0 sequential.
    v0       Uses BangumiClient (REST) — legacy, --client v0 flag required.

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
    pixi run python scripts/scrape_bangumi_relations.py --client v0   # legacy fallback
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any
from typing_extensions import Annotated

# Project root on sys.path when executed directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import structlog
import typer
from rich.console import Console

from src.scrapers.bangumi_graphql_scraper import (
    BangumiGraphQLClient,
    adapt_subject_characters_gql,
    adapt_subject_persons_gql,
)
from src.scrapers.bangumi_scraper import BangumiClient
from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.checkpoint import Checkpoint
from src.scrapers.cli_common import (
    DryRunOpt,
    ForceOpt,
    LimitOpt,
    ProgressOpt,
    QuietOpt,
    ResumeOpt,
    resolve_progress_enabled,
)
from src.scrapers.exceptions import ScraperError
from src.scrapers.progress import scrape_progress

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
                "images": json.dumps(p.get("images") or {}, ensure_ascii=False),
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
                "images": json.dumps(c.get("images") or {}, ensure_ascii=False),
                "summary": c.get("summary"),
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
                    "actor_career": json.dumps(actor.get("career") or [], ensure_ascii=False),
                    "fetched_at": fetched_at,
                }
            )

    return char_rows, actor_rows


# ---------------------------------------------------------------------------
# Core async scrape loop — GraphQL path (default)
# ---------------------------------------------------------------------------

_GRAPHQL_BATCH_SIZE = 25  # subjects per POST; keeps response < ~400 KB


async def _scrape_graphql(
    subject_ids: list[int],
    cp: Checkpoint,
    date_str: str,
    dry_run: bool,
    progress_override: bool | None = None,
) -> Checkpoint:
    """Scrape via GraphQL batched queries (25 subjects per POST).

    This is the default path.  Each POST covers 25 subjects; the 1-req/sec
    floor still applies but is amortised over the batch, giving ~25x
    throughput vs the v0 sequential path.
    """
    completed_set: set[int] = set(cp.completed_set)  # type: ignore[arg-type]
    pending = cp.pending(subject_ids)

    if dry_run:
        batches = (len(pending) + _GRAPHQL_BATCH_SIZE - 1) // _GRAPHQL_BATCH_SIZE
        eta_secs = batches  # 1 req/sec; each req covers 25 subjects
        console.print(
            f"[bold cyan]dry-run[/bold cyan] [client=graphql]  "
            f"total={len(subject_ids):,}  completed={len(completed_set):,}  "
            f"pending={len(pending):,}  "
            f"batches={batches:,} (size={_GRAPHQL_BATCH_SIZE})  "
            f"ETA≈{eta_secs // 60}m {eta_secs % 60}s"
        )
        return cp

    if not pending:
        console.print("[green]All subjects already completed — nothing to do.[/green]")
        return cp

    # Chunk the pending list into batches of _GRAPHQL_BATCH_SIZE.
    chunks: list[list[int]] = [
        pending[i : i + _GRAPHQL_BATCH_SIZE]
        for i in range(0, len(pending), _GRAPHQL_BATCH_SIZE)
    ]

    date = dt.date.fromisoformat(date_str)
    with BronzeWriterGroup(
        "bangumi",
        tables=["subject_persons", "subject_characters", "person_characters"],
        date=date,
    ) as group:
        bw_persons = group["subject_persons"]
        bw_chars = group["subject_characters"]
        bw_actors = group["person_characters"]

        chunk_index = 0
        try:
            async with BangumiGraphQLClient() as client, scrape_progress(
                total=len(chunks),
                description="scraping subjects (graphql)",
                enabled=progress_override,
            ) as p:
                for chunk in chunks:
                    fetched_at = dt.datetime.now(dt.timezone.utc)

                    try:
                        batch_result = await client.fetch_subjects_batched(chunk)
                    except ScraperError as exc:
                        log.error(
                            "bangumi_graphql_batch_failed",
                            chunk=chunk,
                            error=str(exc),
                        )
                        for sid in chunk:
                            cp.mark_failed(sid, status="error", detail=str(exc))
                        p.advance()
                        chunk_index += 1
                        continue

                    for subject_id in chunk:
                        subject_data = batch_result.get(subject_id)
                        if subject_data is None:
                            # Server returned null → treat as 404
                            cp.mark_failed(subject_id, status=404)
                            continue

                        persons = adapt_subject_persons_gql(subject_id, subject_data)
                        for row in _build_person_rows(subject_id, persons, fetched_at):
                            bw_persons.append(row)

                        characters = adapt_subject_characters_gql(subject_id, subject_data)
                        c_rows, a_rows = _build_character_and_actor_rows(
                            subject_id, characters, fetched_at
                        )
                        for row in c_rows:
                            bw_chars.append(row)
                        for row in a_rows:
                            bw_actors.append(row)

                        completed_set.add(subject_id)

                    cp.sync_completed(completed_set)
                    chunk_index += 1

                    # Flush every _CHECKPOINT_FLUSH_EVERY subjects
                    subjects_done = chunk_index * _GRAPHQL_BATCH_SIZE
                    if subjects_done % _CHECKPOINT_FLUSH_EVERY < _GRAPHQL_BATCH_SIZE:
                        group.flush_all()
                        cp.save()
                        p.log(
                            "bangumi_graphql_checkpoint_flushed",
                            completed=len(completed_set),
                            chunks_remaining=len(chunks) - chunk_index,
                        )

                    p.advance()

        finally:
            group.flush_all()

    cp.sync_completed(completed_set)
    cp.save()
    log.info(
        "bangumi_relations_graphql_done",
        completed=len(completed_set),
        failed=len(cp.failed_ids),
    )
    return cp


# ---------------------------------------------------------------------------
# Core async scrape loop — v0 REST path (legacy fallback)
# ---------------------------------------------------------------------------


async def _scrape(
    subject_ids: list[int],
    cp: Checkpoint,
    date_str: str,
    dry_run: bool,
    progress_override: bool | None = None,
) -> Checkpoint:
    """Scrape persons + characters for each subject_id; write parquet + checkpoint.

    Each flush writes a new UUID parquet file so resumed runs never overwrite
    previously written data — old files are preserved and DuckDB reads all via glob.
    """
    completed_set: set[int] = set(cp.completed_set)  # type: ignore[arg-type]
    pending = cp.pending(subject_ids)

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
        return cp

    if not pending:
        console.print("[green]All subjects already completed — nothing to do.[/green]")
        return cp

    # BronzeWriterGroup auto-flushes all writers + compacts partitions on exit.
    date = dt.date.fromisoformat(date_str)
    with BronzeWriterGroup(
        "bangumi",
        tables=["subject_persons", "subject_characters", "person_characters"],
        date=date,
    ) as group:
        bw_persons = group["subject_persons"]
        bw_chars = group["subject_characters"]
        bw_actors = group["person_characters"]

        try:
            async with BangumiClient() as client, scrape_progress(
                total=len(pending),
                description="scraping subjects",
                enabled=progress_override,
            ) as p:
                for i, subject_id in enumerate(pending):
                    fetched_at = dt.datetime.now(dt.timezone.utc)

                    # NOTE: persons and characters are fetched serially (not via
                    # asyncio.gather) intentionally.  BangumiClient enforces a
                    # >= 1 req/sec floor through a single asyncio.Lock + sleep in
                    # _throttle().  With gather, both coroutines would race to
                    # acquire the lock; the second one would sleep ~1 s anyway
                    # before its HTTP call fires.  Total wall-time per subject is
                    # identical to the serial case (≈ 2 s minimum), so gather
                    # buys no throughput improvement and adds task-management
                    # complexity with no compensating benefit.

                    # Fetch persons
                    try:
                        persons = await client.fetch_subject_persons(subject_id)
                    except ScraperError as exc:
                        log.error(
                            "bangumi_persons_fetch_failed",
                            subject_id=subject_id,
                            error=str(exc),
                        )
                        cp.mark_failed(subject_id, status="error", detail=str(exc))
                        p.advance()
                        continue

                    if persons is None:
                        # 404 — record in failed_ids with status 404
                        cp.mark_failed(subject_id, status=404)
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
                    cp.sync_completed(completed_set)

                    # Flush every CHECKPOINT_FLUSH_EVERY subjects — each flush
                    # writes a new UUID file; old files are preserved on resume.
                    if (i + 1) % _CHECKPOINT_FLUSH_EVERY == 0:
                        group.flush_all()
                        cp.save()
                        p.log(
                            "bangumi_checkpoint_flushed",
                            completed=len(completed_set),
                            pending_remaining=len(pending) - (i + 1),
                        )

                    p.advance()

        finally:
            # Always flush remaining rows so partial progress is persisted on Ctrl+C
            group.flush_all()

    # Final checkpoint save
    cp.sync_completed(completed_set)
    cp.save()
    log.info(
        "bangumi_relations_scrape_done",
        completed=len(completed_set),
        failed=len(cp.failed_ids),
    )
    return cp


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="scrape-bangumi-relations",
    help="Fetch bangumi subject×persons/characters → BRONZE parquet.",
    add_completion=False,
)

# Literal type alias for the --client flag
_ClientOpt = Annotated[
    str,
    typer.Option(
        "--client",
        help="API client to use: 'graphql' (default, batched) or 'v0' (legacy REST, one-by-one).",
    ),
]


@app.command()
def main(
    limit: LimitOpt = 0,
    dry_run: DryRunOpt = False,
    resume: ResumeOpt = True,
    force: ForceOpt = False,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
    client: _ClientOpt = "graphql",
    subject_ids_file: Path = typer.Option(
        None,
        "--subject-ids-file",
        help="Override subject ID source (parquet glob or path). Used by Card 06.",
        exists=False,
    ),
) -> None:
    """Scrape bangumi subject relations (persons + characters) via GraphQL (default) or v0 API."""
    if client not in ("graphql", "v0"):
        raise typer.BadParameter(f"--client must be 'graphql' or 'v0', got: {client!r}")
    structlog.configure(processors=[structlog.dev.ConsoleRenderer()])
    asyncio.run(
        _main(
            limit=limit,
            dry_run=dry_run,
            resume=resume,
            force=force,
            client=client,
            subject_ids_file=subject_ids_file,
            progress_override=resolve_progress_enabled(quiet, progress),
        )
    )


async def _main(
    *,
    limit: int,
    dry_run: bool,
    resume: bool,
    force: bool,
    client: str = "graphql",
    subject_ids_file: Path | None,
    progress_override: bool | None = None,
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
        cp = Checkpoint(_CHECKPOINT_PATH)
        log.info("bangumi_checkpoint_cleared_force")
    elif resume:
        cp = Checkpoint.load(_CHECKPOINT_PATH)
        console.print(
            f"[cyan]Checkpoint:[/cyan] "
            f"completed={len(cp.completed_set):,}  failed={len(cp.failed_set):,}"
        )
    else:
        cp = Checkpoint(_CHECKPOINT_PATH)

    # 4. Apply --limit
    pending = cp.pending(all_ids)

    if limit > 0:
        pending = pending[:limit]
        # Only scrape these ids — rebuild the "all_ids" list for the run
        target_ids = sorted(cp.completed_set | set(pending))  # type: ignore[type-var]
    else:
        target_ids = all_ids

    console.print(
        f"[cyan]Pending:[/cyan] {len(pending):,}  "
        f"(limit={'all' if limit == 0 else limit})  "
        f"[client={client}]"
    )

    run_ids = target_ids if limit == 0 else pending
    if client == "graphql":
        await _scrape_graphql(
            subject_ids=run_ids,
            cp=cp,
            date_str=date_str,
            dry_run=dry_run,
            progress_override=progress_override,
        )
    else:
        await _scrape(
            subject_ids=run_ids,
            cp=cp,
            date_str=date_str,
            dry_run=dry_run,
            progress_override=progress_override,
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
