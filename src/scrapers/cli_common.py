"""Reusable Typer CLI flags shared across scrapers.

Eliminates duplicate `typer.Option(...)` declarations for the 4 standard flags
(`--limit`, `--resume`, `--force`, `--dry-run`) that almost every scraper main
exposes.

Usage::

    from src.scrapers.cli_common import LimitOpt, ResumeOpt, ForceOpt, DryRunOpt

    @app.command()
    def main(
        limit: LimitOpt = 0,
        dry_run: DryRunOpt = False,
        resume: ResumeOpt = True,
        force: ForceOpt = False,
    ) -> None:
        ...

The typer.Option metadata (flag names, help text) is attached to the type via
`Annotated`, so each call site only specifies the default value.
"""
from __future__ import annotations

from pathlib import Path
from typing_extensions import Annotated

import typer

# ---------------------------------------------------------------------------
# Standard scrape-loop flags
# ---------------------------------------------------------------------------

LimitOpt = Annotated[
    int,
    typer.Option(
        "--limit",
        "-n",
        help="Process at most N not-yet-completed items (0 = all).",
    ),
]

DryRunOpt = Annotated[
    bool,
    typer.Option(
        "--dry-run",
        help="Show pending count and ETA; do not write anything.",
    ),
]

ResumeOpt = Annotated[
    bool,
    typer.Option(
        "--resume/--no-resume",
        help="Honor existing checkpoint (default: yes).",
    ),
]

ForceOpt = Annotated[
    bool,
    typer.Option(
        "--force",
        help="Ignore checkpoint; reprocess all items.",
    ),
]


# ---------------------------------------------------------------------------
# Common rate / IO flags
# ---------------------------------------------------------------------------

DelayOpt = Annotated[
    float,
    typer.Option(
        "--delay",
        "-d",
        help="Delay between requests in seconds.",
    ),
]

DataDirOpt = Annotated[
    Path,
    typer.Option(
        "--data-dir",
        help="Checkpoint and intermediate-data directory.",
    ),
]

CheckpointIntervalOpt = Annotated[
    int,
    typer.Option(
        "--checkpoint-interval",
        "--checkpoint",
        "-c",
        help="Checkpoint save / parquet flush interval (items between saves).",
    ),
]


# ---------------------------------------------------------------------------
# Progress-bar override (mutex pair: --quiet vs --progress, default = auto)
# ---------------------------------------------------------------------------

QuietOpt = Annotated[
    bool,
    typer.Option(
        "--quiet",
        help=(
            "Force structured-log only (no rich progress bar). "
            "Default: auto-detect (bar when stdout is a TTY)."
        ),
    ),
]

ProgressOpt = Annotated[
    bool,
    typer.Option(
        "--progress",
        help="Force rich progress bar even on non-TTY (e.g. piped output).",
    ),
]


def resolve_progress_enabled(quiet: bool, progress: bool) -> bool | None:
    """Combine `--quiet` and `--progress` flags into a `progress_enabled` override.

    Returns:
        - False if `--quiet`
        - True  if `--progress`
        - None  (auto-detect) if neither

    Raises:
        typer.BadParameter: if both flags are set.
    """
    if quiet and progress:
        raise typer.BadParameter("--quiet and --progress are mutually exclusive.")
    if quiet:
        return False
    if progress:
        return True
    return None
