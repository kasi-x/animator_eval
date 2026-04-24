"""One-shot migration: data/bangumi/dump/latest/subject.jsonlines → BRONZE parquet.

Reads subject.jsonlines from the latest bangumi/Archive dump, filters type=2
(anime only), and writes partitioned parquet under:

    result/bronze/source=bangumi/table=subjects/date=YYYYMMDD/part-0.parquet

The ``date=`` partition is derived from the dump's ``release_tag`` in
manifest.json (e.g. ``dump-2025-10-07.142137Z`` → ``20251007``).

Streams in chunks — does NOT load the full file into memory.

Usage:
    pixi run python scripts/migrate_bangumi_subjects_to_parquet.py
    pixi run python scripts/migrate_bangumi_subjects_to_parquet.py --force
    pixi run python scripts/migrate_bangumi_subjects_to_parquet.py --dry-run
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

# Ensure project root is on sys.path when invoked as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn

log = structlog.get_logger()
console = Console()

app = typer.Typer(
    name="migrate-bangumi-subjects",
    help="Migrate bangumi subject.jsonlines to BRONZE parquet (type=2 anime only).",
    add_completion=False,
)

_DUMP_LATEST = Path("data/bangumi/dump/latest")
_BRONZE_ROOT = Path("result/bronze")
_CHUNK_SIZE = 10_000  # rows per parquet row group

# Fields whose values are complex types and must be serialised to JSON strings.
_JSON_FIELDS = frozenset({"tags", "meta_tags", "score_details", "favorite"})

# Explicit schema — keeps column order stable across re-runs.
# All complex fields become utf8 (JSON string). ``date`` is renamed
# ``release_date`` to avoid the SQL reserved word.
_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("type", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("name_cn", pa.string()),
        pa.field("infobox", pa.string()),
        pa.field("platform", pa.int32()),
        pa.field("summary", pa.string()),
        pa.field("nsfw", pa.bool_()),
        pa.field("tags", pa.string()),           # JSON string
        pa.field("meta_tags", pa.string()),      # JSON string
        pa.field("score", pa.float64()),
        pa.field("score_details", pa.string()),  # JSON string
        pa.field("rank", pa.int32()),
        pa.field("release_date", pa.string()),   # renamed from ``date``
        pa.field("favorite", pa.string()),       # JSON string
        pa.field("series", pa.bool_()),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_manifest(dump_dir: Path) -> dict[str, Any]:
    """Load manifest.json from the dump directory."""
    manifest_path = dump_dir / "manifest.json"
    if not manifest_path.exists():
        log.error("bangumi_subjects_manifest_missing", path=str(manifest_path))
        raise FileNotFoundError(f"manifest.json not found: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _release_date_from_tag(release_tag: str) -> str:
    """Extract YYYYMMDD from a release tag like ``dump-2025-10-07.142137Z``.

    Returns:
        ``"20251007"`` for the example above.

    Raises:
        ValueError: if the tag does not contain a parseable date.
    """
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", release_tag)
    if not m:
        raise ValueError(
            f"Cannot extract date from release_tag {release_tag!r}"
        )
    return m.group(1) + m.group(2) + m.group(3)


def _serialise_row(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Convert a raw subject JSON record to a schema-conformant row dict.

    Returns None if the record should be skipped (wrong type).
    """
    if raw.get("type") != 2:
        return None

    row: dict[str, Any] = {}
    for field in _SCHEMA:
        name = field.name
        # ``release_date`` comes from the ``date`` key in the source
        src_name = "date" if name == "release_date" else name

        if src_name in _JSON_FIELDS:
            value = raw.get(src_name)
            row[name] = json.dumps(value, ensure_ascii=False) if value is not None else None
        else:
            row[name] = raw.get(src_name)

    return row


def _output_path(bronze_root: Path, date_str: str) -> Path:
    """Return the output parquet path for the given date partition."""
    return (
        bronze_root
        / "source=bangumi"
        / "table=subjects"
        / f"date={date_str}"
        / "part-0.parquet"
    )


def _count_type2_lines(jsonlines_path: Path) -> int:
    """Count type=2 lines for progress reporting (fast pre-scan)."""
    count = 0
    with jsonlines_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            if '"type": 2' in line or '"type":2' in line:
                count += 1
    return count


def _stream_anime_rows(jsonlines_path: Path):
    """Yield parsed and serialised rows for type=2 subjects."""
    with jsonlines_path.open(encoding="utf-8") as fh:
        for line_num, raw_line in enumerate(fh, start=1):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                doc = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                log.warning(
                    "bangumi_subjects_json_parse_error",
                    line=line_num,
                    error=str(exc),
                )
                continue

            row = _serialise_row(doc)
            if row is not None:
                yield row


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


def _write_parquet_streaming(
    jsonlines_path: Path,
    out_path: Path,
    *,
    dry_run: bool,
    estimated_rows: int,
) -> int:
    """Stream jsonlines → parquet, returns total rows written."""
    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)

    total_written = 0
    writer: pq.ParquetWriter | None = None
    buffer: list[dict[str, Any]] = []

    def _flush_buffer() -> None:
        nonlocal total_written, writer
        if not buffer:
            return
        table = pa.Table.from_pylist(buffer, schema=_SCHEMA)
        if not dry_run:
            if writer is None:
                writer = pq.ParquetWriter(
                    out_path,
                    schema=_SCHEMA,
                    compression="zstd",
                )
            writer.write_table(table, row_group_size=_CHUNK_SIZE)
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
            if len(buffer) >= _CHUNK_SIZE:
                _flush_buffer()
                progress.advance(task, _CHUNK_SIZE)

        # Flush remainder
        _flush_buffer()
        progress.advance(task, len(buffer))

    if writer is not None:
        writer.close()

    return total_written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@app.command()
def main(
    dump_dir: Path = typer.Option(
        _DUMP_LATEST,
        help="Path to the dump directory (default: data/bangumi/dump/latest).",
    ),
    bronze_root: Path = typer.Option(
        _BRONZE_ROOT,
        help="Root of the BRONZE layer (default: result/bronze).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Parse and count rows without writing any files.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite existing parquet partition if it already exists.",
    ),
) -> None:
    """Migrate bangumi subject.jsonlines → BRONZE parquet (type=2 anime only).

    Reads the jsonlines dump at dump_dir/subject.jsonlines, filters to
    type=2 records, and writes a single parquet file under:

        bronze_root/source=bangumi/table=subjects/date=YYYYMMDD/part-0.parquet

    The date partition is derived from manifest.json's ``release_tag``.
    Idempotent: skips if the target parquet already exists unless --force.
    """
    structlog.configure(
        processors=[structlog.dev.ConsoleRenderer()],
    )

    # 1. Resolve dump location
    resolved_dump = dump_dir.resolve() if not dump_dir.is_symlink() else dump_dir
    jsonlines_path = resolved_dump / "subject.jsonlines"

    if not jsonlines_path.exists():
        log.error(
            "bangumi_subjects_input_missing",
            path=str(jsonlines_path),
        )
        raise typer.Exit(1)

    # 2. Read manifest → derive release date partition
    manifest = _read_manifest(resolved_dump)
    release_tag = manifest["release_tag"]
    date_str = _release_date_from_tag(release_tag)

    log.info(
        "bangumi_subjects_start",
        release_tag=release_tag,
        date_partition=date_str,
        jsonlines=str(jsonlines_path),
        dry_run=dry_run,
    )

    # 3. Idempotency check
    out_path = _output_path(bronze_root, date_str)
    if out_path.exists() and not force and not dry_run:
        console.print(
            f"[yellow]⚠[/yellow]  Output already exists: [bold]{out_path}[/bold]\n"
            "   Use [italic]--force[/italic] to overwrite."
        )
        log.info(
            "bangumi_subjects_skip_exists",
            path=str(out_path),
        )
        raise typer.Exit(0)

    # 4. Count type=2 lines for progress bar (fast string scan)
    console.print("[cyan]→[/cyan] Scanning for type=2 (anime) records …")
    estimated = _count_type2_lines(jsonlines_path)
    console.print(f"   Found ~[bold]{estimated:,}[/bold] anime records")

    # 5. Stream → parquet
    console.print(f"[cyan]→[/cyan] Writing to [bold]{out_path}[/bold] …")
    written = _write_parquet_streaming(
        jsonlines_path,
        out_path,
        dry_run=dry_run,
        estimated_rows=estimated,
    )

    # 6. Report
    if dry_run:
        console.print(
            f"[green]✓[/green] Dry run complete — would write [bold]{written:,}[/bold] rows"
        )
    else:
        console.print(
            f"[green]✓[/green] Written [bold]{written:,}[/bold] rows → {out_path}"
        )

    log.info(
        "bangumi_subjects_done",
        rows_written=written,
        output=str(out_path) if not dry_run else "(dry-run)",
        dry_run=dry_run,
    )


if __name__ == "__main__":
    app()
