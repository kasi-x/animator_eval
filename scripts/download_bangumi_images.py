"""Download bangumi images referenced by URL in BRONZE parquets → local files + manifest.

Reads `large` (default) image URLs from:
    result/bronze/source=bangumi/table=persons/**/*.parquet
    result/bronze/source=bangumi/table=characters/**/*.parquet

Dedupes by (kind, id, variant), downloads at <= 1 req/sec, writes files under:
    data/bangumi/images/persons/{id}.{ext}
    data/bangumi/images/characters/{id}.{ext}

Manifest BRONZE parquet (UUID filenames, DuckDB-readable via glob):
    result/bronze/source=bangumi/table=image_manifest/date=YYYYMMDD/{uuid}.parquet
Columns: kind, id, variant, url, local_path, bytes, sha256, content_type, fetched_at

Checkpoint: data/bangumi/checkpoint_images.json
    {"completed": [["person", 953, "large"], ...], "failed": [...], "last_run_at": "..."}

Usage:
    pixi run python scripts/download_bangumi_images.py --dry-run --limit 5
    pixi run python scripts/download_bangumi_images.py --limit 5 --variant large
    pixi run python scripts/download_bangumi_images.py --resume
    pixi run python scripts/download_bangumi_images.py --force

NOTE: BronzeWriter("bangumi", table="image_manifest") requires no bronze_writer.py changes —
"bangumi" is already in ALLOWED_SOURCES (added in Card 01).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import mimetypes
import os
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb
import httpx
import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn

from src.scrapers.bronze_writer import BronzeWriter

log = structlog.get_logger()
console = Console()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PERSONS_GLOB = "result/bronze/source=bangumi/table=persons/**/*.parquet"
_CHARACTERS_GLOB = "result/bronze/source=bangumi/table=characters/**/*.parquet"
_SUBJECT_PERSONS_GLOB = "result/bronze/source=bangumi/table=subject_persons/**/*.parquet"
_SUBJECT_CHARACTERS_GLOB = "result/bronze/source=bangumi/table=subject_characters/**/*.parquet"

_IMAGES_ROOT = Path("data/bangumi/images")
_CHECKPOINT_PATH = Path("data/bangumi/checkpoint_images.json")

# 100 KB heuristic for disk estimation when no prior data is available.
_AVG_IMAGE_BYTES_HEURISTIC = 100 * 1024

# CDN rate limit independent from BangumiClient — lain.bgm.tv vs api.bgm.tv are separate hosts.
_CDN_MIN_INTERVAL_SECS = 1.0

_CHECKPOINT_FLUSH_EVERY = 100

_USER_AGENT = "animetor_eval/0.1 (https://github.com/kashi-x)"

# Stop-if guard: abort if images dir exceeds 50 GB.
_DISK_LIMIT_BYTES = 50 * 1024 ** 3


# ---------------------------------------------------------------------------
# URL collection
# ---------------------------------------------------------------------------


def _extract_urls_from_table(
    glob: str, id_col: str, variant: str, kind: str
) -> list[tuple[str, int, str, str]]:
    """Return [(kind, id, variant, url)] from a parquet table's `images` JSON column."""
    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT {id_col} AS id, json_extract_string(images, '$.{variant}') AS url
            FROM read_parquet('{glob}')
            WHERE images IS NOT NULL
              AND json_extract_string(images, '$.{variant}') IS NOT NULL
              AND json_extract_string(images, '$.{variant}') != ''
            GROUP BY {id_col}, url
            ORDER BY {id_col}
        """).fetchall()
    except Exception as exc:
        log.warning("image_url_load_error", glob=glob, error=str(exc))
        rows = []
    finally:
        con.close()
    return [(kind, int(r[0]), variant, r[1]) for r in rows if r[1]]


def _collect_target_urls(kind: str, variant: str) -> list[tuple[str, int, str, str]]:
    """Collect (kind, id, variant, url) tuples; primary tables take dedup precedence."""
    results: dict[tuple[str, int, str], str] = {}

    def _merge(entries: list[tuple[str, int, str, str]]) -> None:
        for k, eid, var, url in entries:
            key = (k, eid, var)
            if key not in results:
                results[key] = url

    if kind in ("person", "both"):
        _merge(_extract_urls_from_table(_PERSONS_GLOB, "id", variant, "person"))
        _merge(_extract_urls_from_table(_SUBJECT_PERSONS_GLOB, "person_id", variant, "person"))

    if kind in ("character", "both"):
        _merge(_extract_urls_from_table(_CHARACTERS_GLOB, "id", variant, "character"))
        _merge(
            _extract_urls_from_table(_SUBJECT_CHARACTERS_GLOB, "character_id", variant, "character")
        )

    return [(k, eid, var, url) for (k, eid, var), url in sorted(results.items())]


# ---------------------------------------------------------------------------
# File / hash helpers
# ---------------------------------------------------------------------------


def _ext_from_url(url: str) -> str:
    """Infer extension from URL path; default '.jpg'."""
    ext = Path(urlparse(url).path).suffix.lower()
    return ext if ext in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"} else ".jpg"


def _local_path(kind: str, entity_id: int, url: str) -> Path:
    subdir = "persons" if kind == "person" else "characters"
    ext = _ext_from_url(url)
    return _IMAGES_ROOT / subdir / f"{entity_id}{ext}"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(524288), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _images_dir_bytes() -> int:
    total = 0
    if _IMAGES_ROOT.exists():
        for p in _IMAGES_ROOT.rglob("*"):
            if p.is_file():
                try:
                    total += p.stat().st_size
                except OSError:
                    pass
    return total


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


def _load_checkpoint() -> dict[str, Any]:
    if _CHECKPOINT_PATH.exists():
        try:
            return json.loads(_CHECKPOINT_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("checkpoint_images_load_error", error=str(exc))
    return {"completed": [], "failed": [], "last_run_at": None}


def _save_checkpoint(checkpoint: dict[str, Any]) -> None:
    checkpoint["last_run_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    _CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(
        dir=_CHECKPOINT_PATH.parent, prefix=".checkpoint_images_tmp_"
    )
    try:
        os.write(fd, json.dumps(checkpoint, ensure_ascii=False, indent=2).encode("utf-8"))
        os.close(fd)
        Path(tmp_str).rename(_CHECKPOINT_PATH)
    except Exception:
        os.close(fd)
        Path(tmp_str).unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# CDN rate limiter (separate from BangumiClient — different host)
# ---------------------------------------------------------------------------


class _CdnRateLimiter:
    def __init__(self, min_interval: float = _CDN_MIN_INTERVAL_SECS) -> None:
        self._lock = asyncio.Lock()
        self._last: float = 0.0
        self.min_interval = min_interval

    async def acquire(self) -> None:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            sleep_for = self.min_interval - (now - self._last)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            self._last = asyncio.get_event_loop().time()


class _RateLimitedError(Exception):
    """CDN returned 429 — triggers cooldown + stop-if logic in the download loop."""


# ---------------------------------------------------------------------------
# Single-image download
# ---------------------------------------------------------------------------


async def _download_one(
    client: httpx.AsyncClient,
    limiter: _CdnRateLimiter,
    kind: str,
    entity_id: int,
    variant: str,
    url: str,
    skip_existing: bool,
) -> dict[str, Any] | None:
    """Download one image. Returns a manifest row dict, or None on skip/failure."""
    local = _local_path(kind, entity_id, url)

    # Idempotency: local file present with non-zero size → skip download
    if skip_existing and local.exists() and local.stat().st_size > 0:
        return {
            "kind": kind, "id": entity_id, "variant": variant, "url": url,
            "local_path": str(local), "bytes": local.stat().st_size,
            "sha256": _sha256_file(local),
            "content_type": mimetypes.guess_type(str(local))[0] or "image/jpeg",
            "fetched_at": dt.datetime.now(dt.timezone.utc),
        }

    await limiter.acquire()

    try:
        response = await client.get(url, follow_redirects=True)
    except httpx.HTTPError as exc:
        log.error("image_download_http_error", kind=kind, id=entity_id, url=url, error=str(exc))
        return None

    if response.status_code == 429:
        raise _RateLimitedError(f"429 from CDN for {url}")

    if not response.is_success:
        log.warning("image_download_non_success", kind=kind, id=entity_id,
                    url=url, status=response.status_code)
        return None

    data = response.content
    content_type = response.headers.get("content-type", "image/jpeg").split(";")[0].strip()
    checksum = _sha256_bytes(data)
    fetched_at = dt.datetime.now(dt.timezone.utc)

    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(data)

    # Verify written file to detect partial write
    if _sha256_file(local) != checksum:
        log.error("image_sha256_mismatch", kind=kind, id=entity_id)
        local.unlink(missing_ok=True)
        return None

    return {
        "kind": kind, "id": entity_id, "variant": variant, "url": url,
        "local_path": str(local), "bytes": len(data),
        "sha256": checksum, "content_type": content_type, "fetched_at": fetched_at,
    }


# ---------------------------------------------------------------------------
# Download loop
# ---------------------------------------------------------------------------


async def _download_all(
    targets: list[tuple[str, int, str, str]],
    checkpoint: dict[str, Any],
    date_str: str,
    dry_run: bool,
    skip_existing: bool,
) -> None:
    """Download all target images; write manifest BRONZE parquet + checkpoint."""
    completed_set: set[tuple[Any, ...]] = {
        tuple(e) for e in (checkpoint.get("completed") or [])
    }
    failed_list: list[dict[str, Any]] = list(checkpoint.get("failed") or [])
    failed_set: set[tuple[Any, ...]] = {
        (f["kind"], f["id"], f["variant"]) for f in failed_list
    }

    pending = [
        t for t in targets
        if (t[0], t[1], t[2]) not in completed_set and (t[0], t[1], t[2]) not in failed_set
    ]

    if dry_run:
        persons_n = sum(1 for k, *_ in pending if k == "person")
        chars_n = len(pending) - persons_n
        eta_secs = len(pending)
        eta_h, rem = divmod(eta_secs, 3600)
        eta_m, eta_s = divmod(rem, 60)
        disk_gb = len(pending) * _AVG_IMAGE_BYTES_HEURISTIC / 1024 ** 3
        console.print(
            f"\n[bold cyan]dry-run[/bold cyan]\n"
            f"  Unique URLs  persons={persons_n:,}  characters={chars_n:,}  "
            f"total={len(pending):,}\n"
            f"  Already completed: {len(completed_set):,}  failed: {len(failed_set):,}"
        )
        console.print("\n  Sample URLs:")
        for k, eid, _var, url in pending[:5]:
            console.print(f"    [{k:10s} {eid:6d}] {url}")
        console.print(
            f"\n  Estimated wall time @ 1 req/s : {eta_h}h {eta_m}m {eta_s}s\n"
            f"  Estimated disk (100 KB/img)   : {disk_gb:.1f} GB\n"
        )
        return

    if not pending:
        console.print("[green]All images already downloaded — nothing to do.[/green]")
        return

    date = dt.date.fromisoformat(date_str)
    limiter = _CdnRateLimiter()
    consecutive_429 = 0
    cooldown_secs = 300  # 5 min on first 429

    with BronzeWriter("bangumi", table="image_manifest", date=date) as bw:
        async with httpx.AsyncClient(
            headers={"User-Agent": _USER_AGENT}, timeout=30.0
        ) as client:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(), MofNCompleteColumn(), TimeRemainingColumn(),
                console=console, transient=False,
            ) as progress:
                task = progress.add_task("downloading images", total=len(pending))

                for i, (kind, entity_id, variant, url) in enumerate(pending):
                    # Disk guard (check every 500 images)
                    if i % 500 == 0 and _images_dir_bytes() > _DISK_LIMIT_BYTES:
                        console.print("[red]STOP: disk limit exceeded (50 GB). Aborting.[/red]")
                        break

                    try:
                        row = await _download_one(
                            client, limiter, kind, entity_id, variant, url, skip_existing
                        )
                    except _RateLimitedError:
                        consecutive_429 += 1
                        if consecutive_429 >= 2:
                            console.print(
                                "[red]STOP: 2 consecutive 429s. "
                                "Wait 5min then restart with --resume.[/red]"
                            )
                            break
                        console.print(f"[yellow]429 — sleeping {cooldown_secs}s...[/yellow]")
                        await asyncio.sleep(cooldown_secs)
                        limiter.min_interval = 2.0  # halve throughput after first 429
                        progress.advance(task)
                        continue
                    else:
                        consecutive_429 = 0

                    key: tuple[Any, ...] = (kind, entity_id, variant)
                    if row is not None:
                        bw.append(row)
                        completed_set.add(key)
                        checkpoint["completed"] = [list(c) for c in sorted(completed_set)]
                    else:
                        failed_list.append(
                            {"kind": kind, "id": entity_id, "variant": variant, "url": url}
                        )
                        failed_set.add(key)
                        checkpoint["failed"] = failed_list

                    if (i + 1) % _CHECKPOINT_FLUSH_EVERY == 0:
                        bw.flush()
                        _save_checkpoint(checkpoint)
                        log.info("images_checkpoint_flushed", completed=len(completed_set),
                                 remaining=len(pending) - (i + 1))

                    progress.advance(task)

    checkpoint["completed"] = [list(c) for c in sorted(completed_set)]
    checkpoint["failed"] = failed_list
    _save_checkpoint(checkpoint)
    log.info("bangumi_images_done", completed=len(completed_set), failed=len(failed_list))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="download-bangumi-images",
    help="Download bangumi images from BRONZE parquet URLs → local files + manifest.",
    add_completion=False,
)


@app.command()
def main(
    variant: str = typer.Option("large", "--variant",
        help="Image variant: large | medium | small | grid."),
    kind: str = typer.Option("both", "--kind",
        help="Entity type: person | character | both."),
    limit: int = typer.Option(0, "--limit", "-n",
        help="Max images to process (0 = all). Useful for testing."),
    dry_run: bool = typer.Option(False, "--dry-run",
        help="Show URL counts, sample URLs, ETA, disk estimate. No downloads."),
    resume: bool = typer.Option(True, "--resume/--no-resume",
        help="Honor existing checkpoint (default: yes)."),
    force: bool = typer.Option(False, "--force",
        help="Ignore checkpoint; reprocess all images."),
    skip_existing: bool = typer.Option(True, "--skip-existing/--no-skip-existing",
        help="Skip if local file already exists with non-zero size (default: yes)."),
) -> None:
    """Download bangumi CDN images (Card 08) from BRONZE parquets → local + manifest."""
    _validate_options(variant=variant, kind=kind)
    structlog.configure(processors=[structlog.dev.ConsoleRenderer()])
    asyncio.run(_main(
        variant=variant, kind=kind, limit=limit,
        dry_run=dry_run, resume=resume, force=force, skip_existing=skip_existing,
    ))


def _validate_options(*, variant: str, kind: str) -> None:
    if variant not in {"large", "medium", "small", "grid"}:
        console.print("[red]--variant must be one of: large medium small grid[/red]")
        raise typer.Exit(1)
    if kind not in {"person", "character", "both"}:
        console.print("[red]--kind must be one of: person character both[/red]")
        raise typer.Exit(1)


async def _main(
    *, variant: str, kind: str, limit: int,
    dry_run: bool, resume: bool, force: bool, skip_existing: bool,
) -> None:
    date_str = dt.date.today().strftime("%Y%m%d")

    console.print(f"[cyan]Collecting URLs  variant={variant!r}  kind={kind!r} ...[/cyan]")
    try:
        all_targets = _collect_target_urls(kind=kind, variant=variant)
    except Exception as exc:
        console.print(f"[red]Failed to collect URLs:[/red] {exc}")
        raise typer.Exit(1) from exc

    console.print(f"[cyan]Unique (kind, id, variant) tuples:[/cyan] {len(all_targets):,}")

    if force:
        checkpoint: dict[str, Any] = {"completed": [], "failed": [], "last_run_at": None}
        log.info("images_checkpoint_cleared_force")
    elif resume:
        checkpoint = _load_checkpoint()
        console.print(
            f"[cyan]Checkpoint:[/cyan] "
            f"completed={len(checkpoint.get('completed') or []):,}  "
            f"failed={len(checkpoint.get('failed') or []):,}"
        )
    else:
        checkpoint = {"completed": [], "failed": [], "last_run_at": None}

    # Apply --limit by slicing only the pending subset
    if limit > 0:
        completed_set: set[tuple[Any, ...]] = {
            tuple(e) for e in (checkpoint.get("completed") or [])
        }
        failed_set: set[tuple[Any, ...]] = {
            (f["kind"], f["id"], f["variant"]) for f in (checkpoint.get("failed") or [])
        }
        pending = [
            t for t in all_targets
            if (t[0], t[1], t[2]) not in completed_set and (t[0], t[1], t[2]) not in failed_set
        ]
        all_targets = pending[:limit]
        console.print(f"[cyan]Limiting to:[/cyan] {len(all_targets):,} images")

    await _download_all(
        targets=all_targets, checkpoint=checkpoint,
        date_str=date_str, dry_run=dry_run, skip_existing=skip_existing,
    )

    if not dry_run:
        console.print(
            f"[green]Done.[/green] "
            f"data/bangumi/images/{{persons,characters}}/  "
            f"result/bronze/source=bangumi/table=image_manifest/date={date_str}/"
        )


if __name__ == "__main__":
    app()
