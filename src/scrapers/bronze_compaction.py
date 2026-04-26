"""Compact small parquet files within a BRONZE partition into one file.

Scrapers flush frequently (every checkpoint) → many small parquet files
under one `source=X/table=Y/date=Z/` partition. Small parquet has high
metadata overhead (footer/schema per file) and slows DuckDB query planning.

This module merges all parquet files in a partition into a single file.

Usage:
    from src.scrapers.bronze_compaction import compact_source, compact_all_sources
    compact_source("ann", date="2026-04-25")  # one date
    compact_source("ann")                      # all dates for source
    compact_all_sources()                      # everything

CLI:
    python -m src.scrapers.bronze_compaction --source ann --date 2026-04-25
    python -m src.scrapers.bronze_compaction --source ann            # all dates
    python -m src.scrapers.bronze_compaction --all                   # all sources
    python -m src.scrapers.bronze_compaction --all --dry-run         # plan only
"""
from __future__ import annotations

import uuid
from pathlib import Path

import duckdb
import structlog
import typer

from src.scrapers.bronze_writer import ALLOWED_SOURCES, DEFAULT_BRONZE_ROOT

logger = structlog.get_logger()

# Skip compaction if partition already optimal (≤ this many files).
SKIP_THRESHOLD = 1


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------


def _list_dates(source: str, table: str, root: Path) -> list[str]:
    """Return sorted list of date partition values under source/table."""
    table_dir = root / f"source={source}" / f"table={table}"
    if not table_dir.exists():
        return []
    return sorted(d.name.removeprefix("date=") for d in table_dir.glob("date=*"))


def _list_tables(source: str, root: Path) -> list[str]:
    """Return sorted list of tables under source."""
    src_dir = root / f"source={source}"
    if not src_dir.exists():
        return []
    return sorted(t.name.removeprefix("table=") for t in src_dir.glob("table=*"))


def _list_sources(root: Path) -> list[str]:
    """Return sorted list of sources under root."""
    if not root.exists():
        return []
    return sorted(s.name.removeprefix("source=") for s in root.glob("source=*"))


def _count_files(source: str, table: str, date: str, root: Path) -> int:
    partition = root / f"source={source}" / f"table={table}" / f"date={date}"
    return len(list(partition.glob("*.parquet"))) if partition.exists() else 0


# ---------------------------------------------------------------------------
# Compaction core
# ---------------------------------------------------------------------------


def compact_partition(
    source: str,
    table: str,
    date: str,
    *,
    root: Path | None = None,
    compression: str = "zstd",
) -> Path | None:
    """Merge all parquet files in one partition into one file.

    Returns the merged file path, or None if partition empty / already compact.
    Atomic: writes tmp file → verifies row count → deletes originals → renames.
    """
    root = Path(root or DEFAULT_BRONZE_ROOT)
    partition = root / f"source={source}" / f"table={table}" / f"date={date}"

    if not partition.exists():
        return None

    files = sorted(p for p in partition.glob("*.parquet") if not p.name.startswith("."))
    if len(files) <= SKIP_THRESHOLD:
        return files[0] if files else None

    glob = str(partition / "*.parquet")
    tmp_path = partition / f".compact-{uuid.uuid4().hex}.parquet.tmp"

    con = duckdb.connect(":memory:")
    try:
        src_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{glob}', union_by_name=true)"
        ).fetchone()[0]

        con.execute(
            f"""
            COPY (SELECT * FROM read_parquet('{glob}', union_by_name=true))
            TO '{tmp_path}'
            (FORMAT PARQUET, COMPRESSION {compression})
            """
        )

        merged_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{tmp_path}')"
        ).fetchone()[0]

        if merged_count != src_count:
            tmp_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"compaction row count mismatch: src={src_count} merged={merged_count}"
            )
    finally:
        con.close()

    for f in files:
        f.unlink()
    final_path = partition / f"{uuid.uuid4().hex}.parquet"
    tmp_path.rename(final_path)

    logger.info(
        "bronze_compacted",
        source=source,
        table=table,
        date=date,
        files_merged=len(files),
        rows=src_count,
        path=str(final_path),
    )
    return final_path


def dedup_partition(
    source: str,
    table: str,
    date: str,
    *,
    root: Path | None = None,
    compression: str = "zstd",
) -> tuple[Path | None, int, int]:
    """Overwrite partition with DISTINCT rows, eliminating exact duplicates.

    Returns (path, before_count, after_count).
    """
    root = Path(root or DEFAULT_BRONZE_ROOT)
    partition = root / f"source={source}" / f"table={table}" / f"date={date}"

    if not partition.exists():
        return None, 0, 0

    files = sorted(p for p in partition.glob("*.parquet") if not p.name.startswith("."))
    if not files:
        return None, 0, 0

    glob = str(partition / "*.parquet")
    tmp_path = partition / f".dedup-{uuid.uuid4().hex}.parquet.tmp"

    con = duckdb.connect(":memory:")
    try:
        src_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{glob}', union_by_name=true)"
        ).fetchone()[0]

        con.execute(
            f"""
            COPY (SELECT DISTINCT * FROM read_parquet('{glob}', union_by_name=true))
            TO '{tmp_path}'
            (FORMAT PARQUET, COMPRESSION {compression})
            """
        )

        dedup_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{tmp_path}')"
        ).fetchone()[0]
    finally:
        con.close()

    for f in files:
        f.unlink()
    final_path = partition / f"{uuid.uuid4().hex}.parquet"
    tmp_path.rename(final_path)

    logger.info(
        "bronze_deduped",
        source=source,
        table=table,
        date=date,
        before=src_count,
        after=dedup_count,
        removed=src_count - dedup_count,
    )
    return final_path, src_count, dedup_count


def dedup_source(
    source: str,
    *,
    date: str | None = None,
    table: str | None = None,
    root: Path | None = None,
) -> dict[tuple[str, str], tuple[int, int]]:
    """Dedup all partitions for one source. Returns {(table, date): (before, after)}."""
    if source not in ALLOWED_SOURCES:
        raise ValueError(f"Unknown source: {source!r}")

    root = Path(root or DEFAULT_BRONZE_ROOT)
    tables = [table] if table else _list_tables(source, root)

    results: dict[tuple[str, str], tuple[int, int]] = {}
    for tbl in tables:
        dates = [date] if date else _list_dates(source, tbl, root)
        for d in dates:
            _, before, after = dedup_partition(source, tbl, d, root=root)
            if before > 0:
                results[(tbl, d)] = (before, after)
    return results


def compact_source(
    source: str,
    *,
    date: str | None = None,
    table: str | None = None,
    root: Path | None = None,
) -> dict[tuple[str, str], Path | None]:
    """Compact partitions for one source.

    - date=None  → every date partition under each table
    - table=None → every table; otherwise restricted to one table
    Returns mapping (table, date) → merged path (or None).
    """
    if source not in ALLOWED_SOURCES:
        raise ValueError(f"Unknown source: {source!r}")

    root = Path(root or DEFAULT_BRONZE_ROOT)
    tables = [table] if table else _list_tables(source, root)

    results: dict[tuple[str, str], Path | None] = {}
    for tbl in tables:
        dates = [date] if date else _list_dates(source, tbl, root)
        for d in dates:
            results[(tbl, d)] = compact_partition(source, tbl, d, root=root)
    return results


def compact_all_sources(
    *,
    root: Path | None = None,
) -> dict[tuple[str, str, str], Path | None]:
    """Compact every (source, table, date) partition under root.

    Returns mapping (source, table, date) → merged path.
    """
    root = Path(root or DEFAULT_BRONZE_ROOT)
    results: dict[tuple[str, str, str], Path | None] = {}
    for src in _list_sources(root):
        if src not in ALLOWED_SOURCES:
            logger.warning("bronze_compaction_skip_unknown_source", source=src)
            continue
        for tbl in _list_tables(src, root):
            for d in _list_dates(src, tbl, root):
                results[(src, tbl, d)] = compact_partition(src, tbl, d, root=root)
    return results


def plan_compaction(
    *,
    source: str | None = None,
    table: str | None = None,
    date: str | None = None,
    root: Path | None = None,
) -> list[tuple[str, str, str, int]]:
    """Return list of (source, table, date, file_count) candidates to compact.

    Includes only partitions with > SKIP_THRESHOLD files.
    """
    root = Path(root or DEFAULT_BRONZE_ROOT)
    sources = [source] if source else _list_sources(root)

    plan: list[tuple[str, str, str, int]] = []
    for src in sources:
        if src not in ALLOWED_SOURCES:
            continue
        tables = [table] if table else _list_tables(src, root)
        for tbl in tables:
            dates = [date] if date else _list_dates(src, tbl, root)
            for d in dates:
                n = _count_files(src, tbl, d, root)
                if n > SKIP_THRESHOLD:
                    plan.append((src, tbl, d, n))
    return plan


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli(
    source: str | None = typer.Option(
        None, help=f"Source name ({sorted(ALLOWED_SOURCES)}). Omit with --all."
    ),
    table: str | None = typer.Option(None, help="Single table only (default: all tables)"),
    date: str | None = typer.Option(
        None, help="Partition date YYYY-MM-DD (default: all dates)"
    ),
    all_sources: bool = typer.Option(
        False, "--all", help="Compact every source under bronze root"
    ),
    dedup: bool = typer.Option(
        False, "--dedup", help="Dedup (DISTINCT) instead of plain merge"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="List candidates without merging"),
    root: Path | None = typer.Option(None, help="Bronze root override"),
) -> None:
    """Merge small parquet files in BRONZE partitions into one file each."""
    if not all_sources and not source:
        raise typer.BadParameter("Specify --source <name> or --all.")
    if all_sources and (source or table or date):
        raise typer.BadParameter("--all is exclusive with --source/--table/--date.")

    if dry_run:
        plan = plan_compaction(source=source, table=table, date=date, root=root)
        if not plan:
            typer.echo("(nothing to compact)")
            return
        for src, tbl, d, n in plan:
            typer.echo(f"{src}/{tbl}/{d}: {n} files")
        typer.echo(f"--- total partitions: {len(plan)}")
        return

    if dedup:
        if all_sources:
            raise typer.BadParameter("--dedup requires --source (not --all)")
        assert source is not None
        results3 = dedup_source(source, date=date, table=table, root=root)
        for (tbl, d), (before, after) in results3.items():
            typer.echo(f"{tbl}/{d}: {before} → {after} rows (removed {before - after})")
        return

    if all_sources:
        results = compact_all_sources(root=root)
        for (src, tbl, d), path in results.items():
            if path is not None:
                typer.echo(f"{src}/{tbl}/{d}: {path.name}")
    else:
        assert source is not None  # narrowed by check above
        results2 = compact_source(source, date=date, table=table, root=root)
        for (tbl, d), path in results2.items():
            if path is not None:
                typer.echo(f"{tbl}/{d}: {path.name}")


if __name__ == "__main__":
    typer.run(_cli)
