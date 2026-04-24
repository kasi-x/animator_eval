"""CLI: download and extract a bangumi/Archive dump.

Usage:
    pixi run python scripts/fetch_bangumi_dump.py
    pixi run python scripts/fetch_bangumi_dump.py --tag dump-2026-04-21.210419Z
    pixi run python scripts/fetch_bangumi_dump.py --force

Output layout:
    data/bangumi/dump/<tag>/*.jsonlines
    data/bangumi/dump/<tag>/manifest.json
    data/bangumi/dump/latest  →  <tag>  (symlink, updated atomically)

Idempotent: if ``manifest.json`` already exists for the requested tag and
``--force`` is not set, the script exits immediately with a skip log.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

# Ensure project root is on sys.path when invoked as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import structlog
import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from src.scrapers.bangumi_dump import (
    build_manifest,
    download_zip,
    extract_zip,
    fetch_latest_release_meta,
)

log = structlog.get_logger()
console = Console()

_DUMP_ROOT = Path("data/bangumi/dump")
_LATEST_LINK = _DUMP_ROOT / "latest"

app = typer.Typer(
    name="fetch-bangumi-dump",
    help="Download and extract a bangumi/Archive dump.",
    add_completion=False,
)


@app.command()
def main(
    tag: str = typer.Option(
        "",
        "--tag",
        help=(
            "Dump tag to fetch, e.g. dump-2026-04-21.210419Z.  "
            "Omit to fetch the latest."
        ),
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-download even if the tag is already extracted.",
    ),
) -> None:
    """Download and extract a bangumi/Archive dump zip."""
    asyncio.run(_run(tag=tag, force=force))


async def _run(*, tag: str, force: bool) -> None:
    structlog.configure(
        processors=[
            structlog.dev.ConsoleRenderer(),
        ],
    )

    # 1. Resolve metadata (latest or requested tag)
    meta = await fetch_latest_release_meta()

    if tag:
        # User requested a specific tag — override url only if the resolved
        # tag differs; otherwise the resolved meta (with sha256) applies.
        if meta["tag"] != tag:
            log.info(
                "bangumi_dump_tag_override",
                requested=tag,
                latest=meta["tag"],
            )
            # Build a minimal meta for the requested tag without sha256
            meta = {
                "tag": tag,
                "url": _build_asset_url(tag),
                "size": None,
                "sha256": None,
            }
    else:
        tag = meta["tag"]

    # 2. Idempotency check
    tag_dir = _DUMP_ROOT / tag
    manifest_path = tag_dir / "manifest.json"

    if manifest_path.exists() and not force:
        log.info(
            "bangumi_dump_skip_already_extracted",
            tag=tag,
            manifest=str(manifest_path),
        )
        console.print(
            f"[green]✓[/green] Already extracted: [bold]{tag}[/bold] "
            f"— use [italic]--force[/italic] to re-download."
        )
        return

    # 3. Download
    _DUMP_ROOT.mkdir(parents=True, exist_ok=True)
    zip_path = _DUMP_ROOT / f"{tag}.zip"

    console.print(
        f"[cyan]↓[/cyan] Downloading [bold]{tag}[/bold]  "
        f"(~{_fmt_bytes(meta['size'])})"
    )

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("download", total=meta["size"])

        def on_progress(downloaded: int, total: int | None) -> None:
            progress.update(task, completed=downloaded, total=total or meta["size"])

        await download_zip(meta["url"], zip_path, on_progress=on_progress)

    console.print(f"[cyan]✓[/cyan] Download complete: {zip_path}")

    # 4. Extract
    console.print(f"[cyan]⟗[/cyan] Extracting to [bold]{tag_dir}[/bold] …")
    extracted_paths = extract_zip(zip_path, tag_dir)
    console.print(f"[cyan]✓[/cyan] Extracted {len(extracted_paths)} file(s)")

    # 5. Build and write manifest
    manifest = build_manifest(tag_dir, tag)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    console.print(f"[green]✓[/green] Manifest written: {manifest_path}")

    # 6. Remove the zip (large; manifest + jsonlines are the authoritative output)
    zip_path.unlink(missing_ok=True)
    log.info("bangumi_dump_zip_removed", path=str(zip_path))

    # 7. Update latest symlink atomically (tmp → rename)
    _update_latest_symlink(tag)
    console.print(f"[green]✓[/green] Symlink updated: {_LATEST_LINK} → {tag}")

    # 8. Summary
    console.print()
    console.rule("[bold green]Done[/bold green]")
    for f in manifest["files"]:
        console.print(
            f"  {f['name']:40s}  {_fmt_bytes(f['size']):>10s}  "
            f"{f['line_count']:>10,} lines"
        )


def _build_asset_url(tag: str) -> str:
    """Construct a GitHub release download URL from a dump tag."""
    return (
        "https://github.com/bangumi/Archive/releases/download/archive/"
        f"{tag}.zip"
    )


def _update_latest_symlink(tag: str) -> None:
    """Atomically update ``data/bangumi/dump/latest`` → ``<tag>``."""
    link_parent = _LATEST_LINK.parent
    # Create a temporary symlink then rename it (atomic on POSIX)
    fd, tmp_path_str = tempfile.mkstemp(
        dir=link_parent, prefix=".latest_tmp_"
    )
    os.close(fd)
    tmp_path = Path(tmp_path_str)
    tmp_path.unlink()  # mkstemp creates the file; remove before symlinking
    tmp_path.symlink_to(tag)
    tmp_path.rename(_LATEST_LINK)


def _fmt_bytes(n: int | None) -> str:
    """Human-readable byte count."""
    if n is None:
        return "unknown"
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


if __name__ == "__main__":
    app()
