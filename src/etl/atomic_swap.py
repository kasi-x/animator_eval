"""Atomic file swap for DuckDB output (no writer block on readers)."""
from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import Iterator

import structlog

logger = structlog.get_logger()


@contextlib.contextmanager
def atomic_duckdb_swap(target: Path | str) -> Iterator[Path]:
    """Yield a temporary path for building a new DuckDB file.

    On successful exit, atomically replaces `target` with the new file.
    On exception, deletes the temporary file and re-raises (target unchanged).

    Readers holding `target` open keep the old inode (POSIX); they continue
    to see the pre-swap data until they close the connection.

    Usage:
        with atomic_duckdb_swap("silver.duckdb") as new_path:
            conn = duckdb.connect(str(new_path))
            conn.execute("PRAGMA memory_limit='2GB'")
            ...
            conn.close()
        # silver.duckdb is now the new file
    """
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".new")
    if tmp.exists():
        tmp.unlink()  # leftover from prior crashed run
    try:
        yield tmp
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise
    if not tmp.exists():
        raise RuntimeError(f"atomic_duckdb_swap: {tmp} was not created")
    os.replace(tmp, target)  # POSIX atomic rename
    logger.info("duckdb_atomic_swap", target=str(target))
