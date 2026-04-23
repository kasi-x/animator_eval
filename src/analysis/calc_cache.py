"""Incremental calculation cache (DuckDB, separate file from gold).

Tracks (scope, calc_name) -> input_hash for Phase 9 analysis modules so
tasks with unchanged inputs can be skipped on re-runs.

Stored in a dedicated cache.duckdb file so the pipeline's atomic swap of
gold.duckdb (see src/analysis/gold_writer.py) does not wipe the cache.

Per-call open/close lifecycle: cheap (DuckDB opens in <5ms on a small
file) and keeps us off any long-lived inode in case an admin rotates
cache.duckdb out of band.
"""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb
import structlog

logger = structlog.get_logger()

DEFAULT_CACHE_PATH: Path = Path(
    os.environ.get("ANIMETOR_CACHE_PATH", "result/cache.duckdb")
)

_DDL = """
CREATE TABLE IF NOT EXISTS calc_execution_records (
    scope        VARCHAR NOT NULL,
    calc_name    VARCHAR NOT NULL,
    input_hash   VARCHAR NOT NULL,
    status       VARCHAR NOT NULL DEFAULT 'success',
    output_path  VARCHAR NOT NULL DEFAULT '',
    computed_at  TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (scope, calc_name)
);
CREATE INDEX IF NOT EXISTS idx_calc_exec_scope_hash
    ON calc_execution_records(scope, input_hash);
"""

_UPSERT_SQL = """
INSERT INTO calc_execution_records
    (scope, calc_name, input_hash, status, output_path, computed_at)
VALUES (?, ?, ?, ?, ?, current_timestamp)
ON CONFLICT (scope, calc_name) DO UPDATE SET
    input_hash  = excluded.input_hash,
    status      = excluded.status,
    output_path = excluded.output_path,
    computed_at = excluded.computed_at
"""


@contextmanager
def _connect(path: Path | str | None = None) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Open a DuckDB connection, ensure DDL, yield, close."""
    resolved = str(path or DEFAULT_CACHE_PATH)
    Path(resolved).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(resolved)
    try:
        conn.execute("SET memory_limit='512MB'")
        conn.execute(_DDL)
        yield conn
    finally:
        conn.close()


def get_calc_execution_hashes(
    scope: str,
    *,
    path: Path | str | None = None,
) -> dict[str, str]:
    """Return {calc_name: input_hash} for all recorded entries in scope."""
    with _connect(path) as conn:
        rows = conn.execute(
            "SELECT calc_name, input_hash FROM calc_execution_records WHERE scope = ?",
            [scope],
        ).fetchall()
    return {row[0]: row[1] for row in rows}


def record_calc_execution(
    scope: str,
    calc_name: str,
    input_hash: str,
    *,
    status: str = "success",
    output_path: str = "",
    path: Path | str | None = None,
) -> None:
    """Upsert a single calc execution record."""
    with _connect(path) as conn:
        conn.execute(_UPSERT_SQL, [scope, calc_name, input_hash, status, output_path])


def record_calc_executions_batch(
    scope: str,
    items: list[tuple[str, str, str]],  # (calc_name, input_hash, output_path)
    *,
    status: str = "success",
    path: Path | str | None = None,
) -> None:
    """Upsert multiple calc execution records in a single connection.

    Args:
        scope: Pipeline scope identifier (e.g. 'phase9_analysis_modules').
        items: List of (calc_name, input_hash, output_path) tuples.
        status: Status string applied to all records in this batch.
        path: Override cache file path; defaults to DEFAULT_CACHE_PATH.
    """
    if not items:
        return
    params = [(scope, name, h, status, p) for (name, h, p) in items]
    with _connect(path) as conn:
        conn.executemany(_UPSERT_SQL, params)
    logger.debug("calc_cache_recorded", scope=scope, n_items=len(items))
