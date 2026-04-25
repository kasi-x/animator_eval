"""Scrape bangumi character details → BRONZE parquet.

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

Client selection (--client flag, default: graphql):
    graphql  Uses BangumiGraphQLClient.fetch_character() (single-subject GraphQL POST).
    v0       Uses BangumiClient.fetch_character() (legacy REST).

Usage:
    pixi run python scripts/scrape_bangumi_characters.py --dry-run --limit 10
    pixi run python scripts/scrape_bangumi_characters.py --limit 10
    pixi run python scripts/scrape_bangumi_characters.py --resume
    pixi run python scripts/scrape_bangumi_characters.py --force
    pixi run python scripts/scrape_bangumi_characters.py --client v0   # legacy fallback
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

from src.scrapers.bangumi_graphql_scraper import BangumiGraphQLClient, adapt_character_gql_to_v0
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
#   stat (dict: {comments, collects}), locked (bool), nsfw (bool)
#
# ABSENT (unlike /v0/persons): last_modified, career
# type (not role) is the character category integer (1=角色, 2=機体, 3=組織)
#
# Row schema (BronzeWriter infers from first flush):
#   id: int64 | name: string | type: int32 | summary: string
#   locked: bool | nsfw: bool
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
        "nsfw": bool(character.get("nsfw", False)),
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
    cp: Checkpoint,
    date_str: str,
    dry_run: bool,
    use_graphql: bool = True,
    progress_override: bool | None = None,
) -> Checkpoint:
    """Fetch character detail for each character_id; write parquet + checkpoint.

    Each flush writes a new UUID parquet file so resumed runs never overwrite
    previously written data — old files are preserved and DuckDB reads all via glob.

    Args:
        character_ids: list of character IDs to process.
        cp: Checkpoint instance.
        date_str: YYYYMMDD string for output partitioning.
        dry_run: if True, only print stats and return.
        use_graphql: if True, use BangumiGraphQLClient; if False, use BangumiClient (v0).
        progress_override: rich progress bar override (None = auto).
    """
    completed_set: set[int] = set(cp.completed_set)  # type: ignore[arg-type]
    pending = cp.pending(character_ids)

    client_label = "graphql" if use_graphql else "v0"

    if dry_run:
        eta_secs = len(pending)  # 1 req/sec
        console.print(
            f"[bold cyan]dry-run[/bold cyan] [client={client_label}]  "
            f"total={len(character_ids):,}  completed={len(completed_set):,}  "
            f"failed={len(cp.failed_set):,}  pending={len(pending):,}  "
            f"ETA≈{eta_secs // 60}m {eta_secs % 60}s"
        )
        return cp

    if not pending:
        console.print("[green]All characters already completed — nothing to do.[/green]")
        return cp

    # BronzeWriter: each flush writes a new UUID file; old files are preserved on resume.
    # with block auto-flushes + compacts the partition on exit.
    date = dt.date.fromisoformat(date_str)
    with BronzeWriter("bangumi", table="characters", date=date) as bw:
        try:
            if use_graphql:
                active_client = BangumiGraphQLClient()
            else:
                active_client = BangumiClient()  # type: ignore[assignment]

            async with active_client, scrape_progress(
                total=len(pending),
                description=f"scraping characters [{client_label}]",
                enabled=progress_override,
            ) as p:
                for i, character_id in enumerate(pending):
                    fetched_at = dt.datetime.now(dt.timezone.utc)

                    try:
                        character_raw = await active_client.fetch_character(character_id)
                    except ScraperError as exc:
                        log.error(
                            "bangumi_character_fetch_failed",
                            character_id=character_id,
                            client=client_label,
                            error=str(exc),
                        )
                        cp.mark_failed(character_id, status="error", detail=str(exc))
                        p.advance()
                        continue

                    if character_raw is None:
                        # 404 / NOT_FOUND — skip + record
                        cp.mark_failed(character_id, status=404)
                        log.info("bangumi_character_not_found", character_id=character_id)
                        p.advance()
                        continue

                    # Normalise GraphQL camelCase response to v0 snake_case shape.
                    if use_graphql:
                        character = adapt_character_gql_to_v0(character_raw)
                    else:
                        character = character_raw

                    bw.append(_build_character_row(character, fetched_at))
                    completed_set.add(character_id)
                    cp.sync_completed(completed_set)

                    if (i + 1) % _CHECKPOINT_FLUSH_EVERY == 0:
                        bw.flush()
                        cp.save()
                        p.log(
                            "bangumi_characters_checkpoint_flushed",
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
        "bangumi_characters_scrape_done",
        completed=len(completed_set),
        failed=len(cp.failed_ids),
    )
    return cp


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="scrape-bangumi-characters",
    help="Fetch bangumi character details → BRONZE parquet.",
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
    """Scrape bangumi character details (Card 05) via GraphQL (default) or v0 API."""
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

    # 1. Load referenced character_ids from Card 03 parquet outputs
    try:
        all_ids = _load_character_ids()
    except Exception as exc:
        console.print(f"[red]Failed to load character IDs:[/red] {exc}")
        raise typer.Exit(1) from exc

    console.print(f"[cyan]Referenced character IDs:[/cyan] {len(all_ids):,}")

    # 2. Load checkpoint
    if force:
        cp = Checkpoint(_CHECKPOINT_PATH)
        log.info("bangumi_characters_checkpoint_cleared_force")
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

    # Pass only the pending slice so --limit is honoured exactly
    await _scrape(
        character_ids=pending,
        cp=cp,
        date_str=date_str,
        dry_run=dry_run,
        use_graphql=(client == "graphql"),
        progress_override=progress_override,
    )

    if not dry_run:
        console.print(
            f"[green]Done.[/green] "
            f"Output: result/bronze/source=bangumi/table=characters/date={date_str}/ "
            f"(UUID parquet files; read via glob)"
        )


if __name__ == "__main__":
    app()
