"""Scrape bangumi person details → BRONZE parquet.

Reads the referenced person_id set from Card 03 parquet outputs
(subject_persons ∪ person_characters), then fetches each person via
BangumiClient.fetch_person() (already implemented in Card 03).

Output (UUID filenames, append-safe on resume):
    result/bronze/source=bangumi/table=persons/date=YYYYMMDD/{uuid}.parquet

Each run (including resumed runs) produces new UUID files; old files are preserved.
DuckDB reads all files via glob: read_parquet('.../**/*.parquet').

Checkpoint: data/bangumi/checkpoint_persons.json
    {"completed_ids": [...], "failed_ids": [{"id": 42, "status": 404}], "last_run_at": "..."}

Client selection (--client flag, default: graphql):
    graphql  Uses BangumiGraphQLClient.fetch_person() (single-subject GraphQL POST).
    v0       Uses BangumiClient.fetch_person() (legacy REST).

Usage:
    pixi run python scripts/scrape_bangumi_persons.py --dry-run --limit 10
    pixi run python scripts/scrape_bangumi_persons.py --limit 10
    pixi run python scripts/scrape_bangumi_persons.py --resume
    pixi run python scripts/scrape_bangumi_persons.py --force
    pixi run python scripts/scrape_bangumi_persons.py --client v0   # legacy fallback
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

from src.scrapers.bangumi_graphql_scraper import BangumiGraphQLClient, adapt_person_gql_to_v0
from src.scrapers.bangumi_scraper import BangumiClient
from src.scrapers.bronze_writer import BronzeWriter
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
    cp: Checkpoint,
    date_str: str,
    dry_run: bool,
    use_graphql: bool = True,
    progress_override: bool | None = None,
) -> Checkpoint:
    """Fetch person detail for each person_id; write parquet + checkpoint.

    Each flush writes a new UUID parquet file so resumed runs never overwrite
    previously written data — old files are preserved and DuckDB reads all via glob.

    Args:
        person_ids: list of person IDs to process.
        cp: Checkpoint instance.
        date_str: YYYYMMDD string for output partitioning.
        dry_run: if True, only print stats and return.
        use_graphql: if True, use BangumiGraphQLClient; if False, use BangumiClient (v0).
        progress_override: rich progress bar override (None = auto).
    """
    completed_set: set[int] = set(cp.completed_set)  # type: ignore[arg-type]
    pending = cp.pending(person_ids)

    client_label = "graphql" if use_graphql else "v0"

    if dry_run:
        eta_secs = len(pending)  # 1 req/sec
        console.print(
            f"[bold cyan]dry-run[/bold cyan] [client={client_label}]  "
            f"total={len(person_ids):,}  completed={len(completed_set):,}  "
            f"failed={len(cp.failed_set):,}  pending={len(pending):,}  "
            f"ETA≈{eta_secs // 60}m {eta_secs % 60}s"
        )
        return cp

    if not pending:
        console.print("[green]All persons already completed — nothing to do.[/green]")
        return cp

    # BronzeWriter: each flush writes a new UUID file; old files are preserved on resume.
    # with block auto-flushes + compacts the partition on exit.
    date = dt.date.fromisoformat(date_str)
    with BronzeWriter("bangumi", table="persons", date=date) as bw:
        try:
            # Open either the GraphQL client or the legacy v0 client.
            if use_graphql:
                active_client = BangumiGraphQLClient()
            else:
                active_client = BangumiClient()  # type: ignore[assignment]

            async with active_client, scrape_progress(
                total=len(pending),
                description=f"scraping persons [{client_label}]",
                enabled=progress_override,
            ) as p:
                for i, person_id in enumerate(pending):
                    fetched_at = dt.datetime.now(dt.timezone.utc)

                    try:
                        person_raw = await active_client.fetch_person(person_id)
                    except ScraperError as exc:
                        log.error(
                            "bangumi_person_fetch_failed",
                            person_id=person_id,
                            client=client_label,
                            error=str(exc),
                        )
                        cp.mark_failed(person_id, status="error", detail=str(exc))
                        p.advance()
                        continue

                    if person_raw is None:
                        # 404 / NOT_FOUND — skip + record
                        cp.mark_failed(person_id, status=404)
                        log.info("bangumi_person_not_found", person_id=person_id)
                        p.advance()
                        continue

                    # Normalise GraphQL camelCase response to v0 snake_case shape.
                    if use_graphql:
                        person = adapt_person_gql_to_v0(person_raw)
                    else:
                        person = person_raw

                    bw.append(_build_person_row(person, fetched_at))
                    completed_set.add(person_id)
                    cp.sync_completed(completed_set)

                    if (i + 1) % _CHECKPOINT_FLUSH_EVERY == 0:
                        bw.flush()
                        cp.save()
                        p.log(
                            "bangumi_persons_checkpoint_flushed",
                            completed=len(completed_set),
                            pending_remaining=len(pending) - (i + 1),
                        )

                    p.advance()

        finally:
            # Always flush remaining rows so partial progress is persisted on Ctrl+C
            bw.flush()

    cp.sync_completed(completed_set)
    cp.save()
    log.info(
        "bangumi_persons_scrape_done",
        completed=len(completed_set),
        failed=len(cp.failed_ids),
    )
    return cp


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="scrape-bangumi-persons",
    help="Fetch bangumi person details → BRONZE parquet.",
    add_completion=False,
)

_ClientOpt = Annotated[
    str,
    typer.Option(
        "--client",
        help="API client to use: 'graphql' (default) or 'v0' (legacy REST).",
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
) -> None:
    """Scrape bangumi person details (Card 04) via GraphQL (default) or v0 API."""
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
    progress_override: bool | None = None,
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
        cp = Checkpoint(_CHECKPOINT_PATH)
        log.info("bangumi_persons_checkpoint_cleared_force")
    elif resume:
        cp = Checkpoint.load(_CHECKPOINT_PATH)
        console.print(
            f"[cyan]Checkpoint:[/cyan] "
            f"completed={len(cp.completed_set):,}  failed={len(cp.failed_set):,}"
        )
    else:
        cp = Checkpoint(_CHECKPOINT_PATH)

    # 3. Apply --limit: slice the pending list only
    pending = cp.pending(all_ids)

    if limit > 0:
        pending = pending[:limit]

    console.print(
        f"[cyan]Pending this run:[/cyan] {len(pending):,}  "
        f"(limit={'all' if limit == 0 else limit})"
    )

    # Pass only the pending slice (not all_ids) so --limit is honoured exactly
    await _scrape(
        person_ids=pending if limit > 0 else all_ids,
        cp=cp,
        date_str=date_str,
        dry_run=dry_run,
        use_graphql=(client == "graphql"),
        progress_override=progress_override,
    )

    if not dry_run:
        console.print(
            f"[green]Done.[/green] "
            f"Output: result/bronze/source=bangumi/table=persons/date={date_str}/ "
            f"(UUID parquet files; read via glob)"
        )


if __name__ == "__main__":
    app()
