"""bangumi scraper — unified CLI entry point.

Follows the same structure as ann_scraper.py / allcinema_scraper.py.

Commands:
  fetch-dump  : download bangumi/Archive dump from GitHub
  subjects    : migrate dump jsonlines → BRONZE parquet (type=2 anime)
  relations   : scrape subject×persons/characters via GraphQL
  persons     : scrape person details
  characters  : scrape character details
  run         : relations → persons → characters in sequence
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from src.scrapers.bangumi_dump import (
    build_asset_url,
    build_manifest,
    download_zip,
    extract_zip,
    fetch_latest_release_meta,
    fmt_bytes,
    update_latest_symlink,
)
from src.scrapers.bangumi_graphql_scraper import (
    BangumiGraphQLClient,
    adapt_character_gql_to_v0,
    adapt_person_gql_to_v0,
    adapt_subject_characters_gql,
    adapt_subject_persons_gql,
)
from src.scrapers.bronze_writer import BronzeWriter, BronzeWriterGroup
from src.scrapers.checkpoint import Checkpoint, prepare_checkpoint_run
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
from src.scrapers.logging_utils import configure_file_logging
from src.scrapers.progress import scrape_progress

log = structlog.get_logger()
console = Console()

app = typer.Typer(
    name="bangumi",
    help="bangumi scraper: fetch-dump → subjects → relations → persons → characters",
    add_completion=False,
)

# ─── Paths / constants ────────────────────────────────────────────────────────

_BRONZE_ROOT = Path("result/bronze/source=bangumi")
_DUMP_ROOT = Path("data/bangumi/dump")
_DUMP_LATEST = _DUMP_ROOT / "latest"
_DATA_DIR = Path("data/bangumi")

_CP_RELATIONS = _DATA_DIR / "checkpoint_relations.json"
_CP_PERSONS = _DATA_DIR / "checkpoint_persons.json"
_CP_CHARACTERS = _DATA_DIR / "checkpoint_characters.json"

_SUBJECTS_GLOB = "result/bronze/source=bangumi/table=subjects/**/*.parquet"
_SUBJECT_PERSONS_GLOB = "result/bronze/source=bangumi/table=subject_persons/**/*.parquet"
_SUBJECT_CHARS_GLOB = "result/bronze/source=bangumi/table=subject_characters/**/*.parquet"
_PERSON_CHARS_GLOB = "result/bronze/source=bangumi/table=person_characters/**/*.parquet"

_GRAPHQL_BATCH = 25   # subjects per GraphQL POST
_CHECKPOINT_FLUSH = 100  # items between parquet flush + checkpoint save

# ─── subjects schema ──────────────────────────────────────────────────────────

_SUBJECTS_JSON_FIELDS = frozenset({"tags", "meta_tags", "score_details", "favorite"})
_SUBJECTS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("type", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("name_cn", pa.string()),
        pa.field("infobox", pa.string()),
        pa.field("platform", pa.int32()),
        pa.field("summary", pa.string()),
        pa.field("nsfw", pa.bool_()),
        pa.field("tags", pa.string()),
        pa.field("meta_tags", pa.string()),
        pa.field("score", pa.float64()),
        pa.field("score_details", pa.string()),
        pa.field("rank", pa.int32()),
        pa.field("release_date", pa.string()),
        pa.field("favorite", pa.string()),
        pa.field("series", pa.bool_()),
    ]
)

# ─── shared helpers ───────────────────────────────────────────────────────────


def _to_int32(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _today() -> str:
    return dt.date.today().strftime("%Y%m%d")


# ─── subjects helpers ─────────────────────────────────────────────────────────


def _release_date_from_tag(release_tag: str) -> str:
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", release_tag)
    if not m:
        raise ValueError(f"Cannot extract date from release_tag {release_tag!r}")
    return m.group(1) + m.group(2) + m.group(3)


def _serialise_subject_row(raw: dict[str, Any]) -> dict[str, Any] | None:
    if raw.get("type") != 2:
        return None
    row: dict[str, Any] = {}
    for field in _SUBJECTS_SCHEMA:
        name = field.name
        src_name = "date" if name == "release_date" else name
        if src_name in _SUBJECTS_JSON_FIELDS:
            value = raw.get(src_name)
            row[name] = json.dumps(value, ensure_ascii=False) if value is not None else None
        else:
            row[name] = raw.get(src_name)
    return row


def _stream_anime_rows(jsonlines_path: Path):
    with jsonlines_path.open(encoding="utf-8") as fh:
        for line_num, raw_line in enumerate(fh, start=1):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                doc = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                log.warning("bangumi_subjects_json_parse_error", line=line_num, error=str(exc))
                continue
            row = _serialise_subject_row(doc)
            if row is not None:
                yield row


def _count_type2_lines(jsonlines_path: Path) -> int:
    count = 0
    with jsonlines_path.open(encoding="utf-8") as fh:
        for line in fh:
            if '"type": 2' in line or '"type":2' in line:
                count += 1
    return count


def _write_subjects_parquet(
    jsonlines_path: Path,
    out_path: Path,
    *,
    dry_run: bool,
    estimated_rows: int,
) -> int:
    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
    total_written = 0
    writer: pq.ParquetWriter | None = None
    buffer: list[dict[str, Any]] = []
    chunk_size = 10_000

    def _flush() -> None:
        nonlocal total_written, writer
        if not buffer:
            return
        table = pa.Table.from_pylist(buffer, schema=_SUBJECTS_SCHEMA)
        if not dry_run:
            if writer is None:
                writer = pq.ParquetWriter(out_path, schema=_SUBJECTS_SCHEMA, compression="zstd")
            writer.write_table(table, row_group_size=chunk_size)
        total_written += len(buffer)
        buffer.clear()

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("converting", total=estimated_rows)
        for row in _stream_anime_rows(jsonlines_path):
            buffer.append(row)
            if len(buffer) >= chunk_size:
                _flush()
                progress.advance(task, chunk_size)
        _flush()
        progress.advance(task, len(buffer))

    if writer is not None:
        writer.close()
    return total_written


# ─── relations helpers ────────────────────────────────────────────────────────


def _load_subject_ids() -> list[int]:
    con = duckdb.connect()
    rows = con.execute(f"SELECT id FROM read_parquet('{_SUBJECTS_GLOB}') ORDER BY id").fetchall()
    con.close()
    return [int(r[0]) for r in rows]


def _build_person_rows(
    subject_id: int,
    persons: list[dict[str, Any]],
    fetched_at: dt.datetime,
) -> list[dict[str, Any]]:
    return [
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
        for p in persons
    ]


def _build_character_and_actor_rows(
    subject_id: int,
    characters: list[dict[str, Any]],
    fetched_at: dt.datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
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


def _check_relations_idempotent(all_ids: list[int], date_str: str) -> None:
    tables = ["subject_persons", "subject_characters", "person_characters"]
    all_have_data = all(
        any((_BRONZE_ROOT / f"table={t}" / f"date={date_str}").glob("*.parquet"))
        for t in tables
    )
    if not all_have_data:
        return
    cp = Checkpoint.load(_CP_RELATIONS)
    if set(all_ids).issubset(cp.completed_set):
        console.print("[green]All subjects completed + parquet exists. Use --force to re-run.[/green]")
        raise typer.Exit(0)


async def _run_relations(
    *,
    limit: int,
    dry_run: bool,
    resume: bool,
    force: bool,
    progress_override: bool | None = None,
) -> None:
    date_str = _today()
    try:
        all_ids = _load_subject_ids()
    except Exception as exc:
        console.print(f"[red]Failed to load subject IDs:[/red] {exc}")
        raise typer.Exit(1) from exc

    console.print(f"[cyan]Subject IDs:[/cyan] {len(all_ids):,}")

    if not force and not dry_run:
        _check_relations_idempotent(all_ids, date_str)

    cp, pending, completed_set = prepare_checkpoint_run(  # type: ignore[assignment]
        all_ids, _CP_RELATIONS, limit=limit, force=force, resume=resume
    )
    if resume and not force:
        console.print(f"[cyan]Checkpoint:[/cyan] completed={len(completed_set):,}  failed={len(cp.failed_set):,}")
    console.print(f"[cyan]Pending:[/cyan] {len(pending):,}")

    if dry_run:
        batches = (len(pending) + _GRAPHQL_BATCH - 1) // _GRAPHQL_BATCH
        console.print(
            f"[bold cyan]dry-run[/bold cyan]  "
            f"total={len(all_ids):,}  completed={len(completed_set):,}  "
            f"pending={len(pending):,}  batches={batches:,}  "
            f"ETA≈{batches // 60}m {batches % 60}s"
        )
        return

    if not pending:
        console.print("[green]relations: all subjects completed.[/green]")
        return

    chunks = [pending[i : i + _GRAPHQL_BATCH] for i in range(0, len(pending), _GRAPHQL_BATCH)]
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
            with scrape_progress(
                total=len(chunks),
                description="relations",
                enabled=progress_override,
            ) as p:
                async with BangumiGraphQLClient() as client:
                    for idx, chunk in enumerate(chunks):
                        fetched_at = dt.datetime.now(dt.timezone.utc)
                        try:
                            batch_result = await client.fetch_subjects_batched(chunk)
                        except ScraperError as exc:
                            log.error("bangumi_graphql_batch_failed", chunk=chunk, error=str(exc))
                            for sid in chunk:
                                cp.mark_failed(sid, status="error", detail=str(exc))
                            p.advance()
                            continue

                        for subject_id in chunk:
                            subject_data = batch_result.get(subject_id)
                            if subject_data is None:
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
                        if (idx + 1) * _GRAPHQL_BATCH % _CHECKPOINT_FLUSH < _GRAPHQL_BATCH:
                            group.flush_all()
                            cp.save()
                            p.log(
                                "bangumi_relations_checkpoint",
                                completed=len(completed_set),
                                remaining=len(chunks) - idx - 1,
                            )
                        p.advance()
        finally:
            group.flush_all()

    cp.sync_completed(completed_set)
    cp.save()
    log.info("bangumi_relations_done", completed=len(completed_set), failed=len(cp.failed_ids))


# ─── persons helpers ──────────────────────────────────────────────────────────


def _load_person_ids() -> list[int]:
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT DISTINCT person_id FROM read_parquet('{_SUBJECT_PERSONS_GLOB}')
        UNION
        SELECT DISTINCT person_id FROM read_parquet('{_PERSON_CHARS_GLOB}')
        ORDER BY 1
    """).fetchall()
    con.close()
    return [int(r[0]) for r in rows]


def _build_person_row(person: dict[str, Any], fetched_at: dt.datetime) -> dict[str, Any]:
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


async def _run_persons(
    *,
    limit: int,
    dry_run: bool,
    resume: bool,
    force: bool,
    progress_override: bool | None = None,
) -> None:
    date_str = _today()
    try:
        all_ids = _load_person_ids()
    except Exception as exc:
        console.print(f"[red]Failed to load person IDs:[/red] {exc}")
        raise typer.Exit(1) from exc

    console.print(f"[cyan]Person IDs:[/cyan] {len(all_ids):,}")

    cp, pending, completed_set = prepare_checkpoint_run(  # type: ignore[assignment]
        all_ids, _CP_PERSONS, limit=limit, force=force, resume=resume
    )
    if resume and not force:
        console.print(f"[cyan]Checkpoint:[/cyan] completed={len(completed_set):,}  failed={len(cp.failed_set):,}")
    console.print(f"[cyan]Pending:[/cyan] {len(pending):,}")

    if dry_run:
        console.print(
            f"[bold cyan]dry-run[/bold cyan]  "
            f"total={len(all_ids):,}  completed={len(completed_set):,}  "
            f"pending={len(pending):,}  ETA≈{len(pending) // 60}m"
        )
        return

    if not pending:
        console.print("[green]persons: all completed.[/green]")
        return

    date = dt.date.fromisoformat(date_str)
    with BronzeWriter("bangumi", table="persons", date=date) as bw:
        try:
            with scrape_progress(
                total=len(pending),
                description="persons",
                enabled=progress_override,
            ) as p:
                async with BangumiGraphQLClient() as client:
                    for i, person_id in enumerate(pending):
                        fetched_at = dt.datetime.now(dt.timezone.utc)
                        try:
                            person_raw = await client.fetch_person(person_id)
                        except ScraperError as exc:
                            log.error("bangumi_person_fetch_failed", person_id=person_id, error=str(exc))
                            cp.mark_failed(person_id, status="error", detail=str(exc))
                            p.advance()
                            continue
                        if person_raw is None:
                            cp.mark_failed(person_id, status=404)
                            p.advance()
                            continue
                        bw.append(_build_person_row(adapt_person_gql_to_v0(person_raw), fetched_at))
                        completed_set.add(person_id)
                        cp.sync_completed(completed_set)
                        if (i + 1) % _CHECKPOINT_FLUSH == 0:
                            bw.flush()
                            cp.save()
                            p.log("bangumi_persons_checkpoint", completed=len(completed_set))
                        p.advance()
        finally:
            bw.flush()

    cp.sync_completed(completed_set)
    cp.save()
    log.info("bangumi_persons_done", completed=len(completed_set), failed=len(cp.failed_ids))


# ─── characters helpers ───────────────────────────────────────────────────────


def _load_character_ids() -> list[int]:
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT DISTINCT character_id FROM read_parquet('{_SUBJECT_CHARS_GLOB}')
        UNION
        SELECT DISTINCT character_id FROM read_parquet('{_PERSON_CHARS_GLOB}')
        ORDER BY 1
    """).fetchall()
    con.close()
    return [int(r[0]) for r in rows]


def _build_character_row(character: dict[str, Any], fetched_at: dt.datetime) -> dict[str, Any]:
    stat = character.get("stat") or {}
    images = character.get("images") or {}
    return {
        "id": int(character.get("id") or 0),
        "name": str(character.get("name") or ""),
        "type": _to_int32(character.get("type")),
        "locked": bool(character.get("locked") or False),
        "nsfw": bool(character.get("nsfw", False)),
        "summary": str(character.get("summary") or ""),
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


async def _run_characters(
    *,
    limit: int,
    dry_run: bool,
    resume: bool,
    force: bool,
    progress_override: bool | None = None,
) -> None:
    date_str = _today()
    try:
        all_ids = _load_character_ids()
    except Exception as exc:
        console.print(f"[red]Failed to load character IDs:[/red] {exc}")
        raise typer.Exit(1) from exc

    console.print(f"[cyan]Character IDs:[/cyan] {len(all_ids):,}")

    cp, pending, completed_set = prepare_checkpoint_run(  # type: ignore[assignment]
        all_ids, _CP_CHARACTERS, limit=limit, force=force, resume=resume
    )
    if resume and not force:
        console.print(f"[cyan]Checkpoint:[/cyan] completed={len(completed_set):,}  failed={len(cp.failed_set):,}")
    console.print(f"[cyan]Pending:[/cyan] {len(pending):,}")

    if dry_run:
        console.print(
            f"[bold cyan]dry-run[/bold cyan]  "
            f"total={len(all_ids):,}  completed={len(completed_set):,}  "
            f"pending={len(pending):,}  ETA≈{len(pending) // 60}m"
        )
        return

    if not pending:
        console.print("[green]characters: all completed.[/green]")
        return

    date = dt.date.fromisoformat(date_str)
    with BronzeWriter("bangumi", table="characters", date=date) as bw:
        try:
            with scrape_progress(
                total=len(pending),
                description="characters",
                enabled=progress_override,
            ) as p:
                async with BangumiGraphQLClient() as client:
                    for i, character_id in enumerate(pending):
                        fetched_at = dt.datetime.now(dt.timezone.utc)
                        try:
                            char_raw = await client.fetch_character(character_id)
                        except ScraperError as exc:
                            log.error("bangumi_character_fetch_failed", character_id=character_id, error=str(exc))
                            cp.mark_failed(character_id, status="error", detail=str(exc))
                            p.advance()
                            continue
                        if char_raw is None:
                            cp.mark_failed(character_id, status=404)
                            p.advance()
                            continue
                        bw.append(_build_character_row(adapt_character_gql_to_v0(char_raw), fetched_at))
                        completed_set.add(character_id)
                        cp.sync_completed(completed_set)
                        if (i + 1) % _CHECKPOINT_FLUSH == 0:
                            bw.flush()
                            cp.save()
                            p.log("bangumi_characters_checkpoint", completed=len(completed_set))
                        p.advance()
        finally:
            bw.flush()

    cp.sync_completed(completed_set)
    cp.save()
    log.info("bangumi_characters_done", completed=len(completed_set), failed=len(cp.failed_ids))


# ─── CLI commands ─────────────────────────────────────────────────────────────


@app.command("fetch-dump")
def cmd_fetch_dump(
    tag: str = typer.Option("", "--tag", help="Dump tag (omit for latest)"),
    force: ForceOpt = False,
) -> None:
    """Download and extract a bangumi/Archive dump from GitHub."""
    log_path = configure_file_logging("bangumi")
    log.info("bangumi_fetch_dump_start", log_file=str(log_path), tag=tag or "latest")
    asyncio.run(_run_fetch_dump(tag=tag, force=force))


async def _run_fetch_dump(*, tag: str, force: bool) -> None:
    meta = await fetch_latest_release_meta()
    if tag and meta["tag"] != tag:
        meta = {"tag": tag, "url": build_asset_url(tag), "size": None, "sha256": None}
    elif not tag:
        tag = meta["tag"]

    tag_dir = _DUMP_ROOT / tag
    manifest_path = tag_dir / "manifest.json"
    if manifest_path.exists() and not force:
        log.info("bangumi_dump_skip_already_extracted", tag=tag)
        console.print(f"[green]Already extracted:[/green] {tag} — use --force to re-download.")
        return

    _DUMP_ROOT.mkdir(parents=True, exist_ok=True)
    zip_path = _DUMP_ROOT / f"{tag}.zip"
    console.print(f"[cyan]↓[/cyan] Downloading {tag}  (~{fmt_bytes(meta['size'])})")

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=True,
    ) as prog:
        task = prog.add_task("download", total=meta["size"])

        def on_progress(downloaded: int, total: int | None) -> None:
            prog.update(task, completed=downloaded, total=total or meta["size"])

        await download_zip(meta["url"], zip_path, on_progress=on_progress)

    console.print(f"[cyan]→[/cyan] Extracting to {tag_dir} …")
    extracted_paths = extract_zip(zip_path, tag_dir)
    console.print(f"[cyan]✓[/cyan] Extracted {len(extracted_paths)} file(s)")

    manifest = build_manifest(tag_dir, tag)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    zip_path.unlink(missing_ok=True)
    update_latest_symlink(_DUMP_ROOT, tag)
    console.print(f"[green]✓[/green] Done: {tag}")
    for f in manifest["files"]:
        console.print(f"  {f['name']:40s}  {fmt_bytes(f['size']):>10s}  {f['line_count']:>10,} lines")


@app.command("subjects")
def cmd_subjects(
    dump_dir: Path = typer.Option(_DUMP_LATEST, help="Dump directory (default: latest)"),
    bronze_root: Path = typer.Option(Path("result/bronze"), help="BRONZE root"),
    dry_run: DryRunOpt = False,
    force: ForceOpt = False,
) -> None:
    """Migrate dump jsonlines → BRONZE parquet (type=2 anime only)."""
    log_path = configure_file_logging("bangumi")
    log.info("bangumi_subjects_command_start", log_file=str(log_path))

    resolved_dump = dump_dir.resolve() if not dump_dir.is_symlink() else dump_dir
    jsonlines_path = resolved_dump / "subject.jsonlines"
    if not jsonlines_path.exists():
        log.error("bangumi_subjects_input_missing", path=str(jsonlines_path))
        raise typer.Exit(1)

    manifest_path = resolved_dump / "manifest.json"
    if not manifest_path.exists():
        log.error("bangumi_subjects_manifest_missing", path=str(manifest_path))
        raise typer.Exit(1)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    date_str = _release_date_from_tag(manifest["release_tag"])

    out_path = (
        bronze_root / "source=bangumi" / "table=subjects" / f"date={date_str}" / "part-0.parquet"
    )
    if out_path.exists() and not force and not dry_run:
        console.print(f"[yellow]⚠[/yellow] Already exists: {out_path} — use --force to overwrite.")
        raise typer.Exit(0)

    console.print(f"[cyan]→[/cyan] Scanning {jsonlines_path} …")
    estimated = _count_type2_lines(jsonlines_path)
    console.print(f"   ~{estimated:,} anime records")

    written = _write_subjects_parquet(jsonlines_path, out_path, dry_run=dry_run, estimated_rows=estimated)
    if dry_run:
        console.print(f"[green]dry-run:[/green] would write {written:,} rows")
    else:
        console.print(f"[green]✓[/green] {written:,} rows → {out_path}")
    log.info("bangumi_subjects_done", rows_written=written, dry_run=dry_run)


@app.command("relations")
def cmd_relations(
    limit: LimitOpt = 0,
    dry_run: DryRunOpt = False,
    resume: ResumeOpt = True,
    force: ForceOpt = False,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Scrape subject×persons/characters → BRONZE parquet."""
    log_path = configure_file_logging("bangumi")
    log.info("bangumi_relations_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_relations(
            limit=limit,
            dry_run=dry_run,
            resume=resume,
            force=force,
            progress_override=resolve_progress_enabled(quiet, progress),
        )
    )


@app.command("persons")
def cmd_persons(
    limit: LimitOpt = 0,
    dry_run: DryRunOpt = False,
    resume: ResumeOpt = True,
    force: ForceOpt = False,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Scrape person details → BRONZE parquet."""
    log_path = configure_file_logging("bangumi")
    log.info("bangumi_persons_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_persons(
            limit=limit,
            dry_run=dry_run,
            resume=resume,
            force=force,
            progress_override=resolve_progress_enabled(quiet, progress),
        )
    )


@app.command("characters")
def cmd_characters(
    limit: LimitOpt = 0,
    dry_run: DryRunOpt = False,
    resume: ResumeOpt = True,
    force: ForceOpt = False,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Scrape character details → BRONZE parquet."""
    log_path = configure_file_logging("bangumi")
    log.info("bangumi_characters_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_characters(
            limit=limit,
            dry_run=dry_run,
            resume=resume,
            force=force,
            progress_override=resolve_progress_enabled(quiet, progress),
        )
    )


@app.command("run")
def cmd_run(
    limit: LimitOpt = 0,
    dry_run: DryRunOpt = False,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
    skip_persons: bool = typer.Option(False, "--skip-persons", help="Skip persons phase"),
    skip_characters: bool = typer.Option(False, "--skip-characters", help="Skip characters phase"),
) -> None:
    """Run relations → persons → characters in sequence."""
    log_path = configure_file_logging("bangumi")
    log.info("bangumi_run_start", log_file=str(log_path))
    progress_override = resolve_progress_enabled(quiet, progress)

    asyncio.run(
        _run_relations(
            limit=limit,
            dry_run=dry_run,
            resume=True,
            force=False,
            progress_override=progress_override,
        )
    )
    if not skip_persons:
        asyncio.run(
            _run_persons(
                limit=0,
                dry_run=dry_run,
                resume=True,
                force=False,
                progress_override=progress_override,
            )
        )
    if not skip_characters:
        asyncio.run(
            _run_characters(
                limit=0,
                dry_run=dry_run,
                resume=True,
                force=False,
                progress_override=progress_override,
            )
        )


if __name__ == "__main__":
    app()
