"""Incremental calculation cache (DuckDB, separate file from gold).

Tracks (scope, calc_name) -> input_hash for Phase 9 analysis modules so
tasks with unchanged inputs can be skipped on re-runs.

Also caches LLM decisions (org_classification, name_normalization) to avoid
redundant LLM calls.

Stored in a dedicated cache.duckdb file so the pipeline's atomic swap of
gold.duckdb (see src/analysis/mart_writer.py) does not wipe the cache.

Per-call open/close lifecycle: cheap (DuckDB opens in <5ms on a small
file) and keeps us off any long-lived inode in case an admin rotates
cache.duckdb out of band.
"""

import json
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

_LLM_DDL = """
CREATE TABLE IF NOT EXISTS llm_decisions (
    name        VARCHAR NOT NULL,
    task        VARCHAR NOT NULL,
    result_json VARCHAR NOT NULL,
    model       VARCHAR NOT NULL DEFAULT '',
    updated_at  TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (name, task)
);
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
        conn.execute(_LLM_DDL)
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


# ---------------------------------------------------------------------------
# LLM decision cache
# ---------------------------------------------------------------------------


def get_all_llm_decisions_bulk(
    task: str, *, path: Path | str | None = None
) -> dict[str, dict]:
    """Fetch all cached LLM decisions for a given task.

    Args:
        task: Task identifier (e.g. 'org_classification', 'name_normalization').
        path: Override cache file path; defaults to DEFAULT_CACHE_PATH.

    Returns:
        Dictionary {name: result_dict} for all entries in the given task.
        Silently skips rows where result_json cannot be parsed.
    """
    with _connect(path) as conn:
        rows = conn.execute(
            "SELECT name, result_json FROM llm_decisions WHERE task = ?",
            [task],
        ).fetchall()
    result = {}
    for name, result_json in rows:
        try:
            result[name] = json.loads(result_json)
        except json.JSONDecodeError:
            logger.warning("llm_cache_malformed_json", name=name, task=task)
    return result


def upsert_llm_decision(
    name: str,
    task: str,
    result: dict,
    model: str = "",
    *,
    path: Path | str | None = None,
) -> None:
    """Save or update an LLM decision.

    Upserts on (name, task) primary key — overwrites if already present.

    Args:
        name: Entity name (e.g. person name, studio name).
        task: Task identifier (e.g. 'org_classification', 'name_normalization').
        result: Decision result as a dict, serialized to JSON.
        model: Model name (optional, e.g. 'qwen-3').
        path: Override cache file path; defaults to DEFAULT_CACHE_PATH.
    """
    result_json = json.dumps(result, ensure_ascii=False)
    with _connect(path) as conn:
        conn.execute(
            """
            INSERT INTO llm_decisions (name, task, result_json, model, updated_at)
            VALUES (?, ?, ?, ?, current_timestamp)
            ON CONFLICT (name, task) DO UPDATE SET
                result_json = excluded.result_json,
                model = excluded.model,
                updated_at = excluded.updated_at
            """,
            [name, task, result_json, model],
        )


def get_llm_decision(
    name: str,
    task: str,
    *,
    path: Path | str | None = None,
) -> dict | None:
    """Fetch a single cached LLM decision.

    Args:
        name: Entity name (e.g. person name, studio name).
        task: Task identifier (e.g. 'org_classification', 'name_normalization').
        path: Override cache file path; defaults to DEFAULT_CACHE_PATH.

    Returns:
        Decision result dict, or None if not found or unparseable.
    """
    with _connect(path) as conn:
        rows = conn.execute(
            "SELECT result_json FROM llm_decisions WHERE name = ? AND task = ?",
            [name, task],
        ).fetchall()
    if not rows:
        return None
    try:
        return json.loads(rows[0][0])
    except (json.JSONDecodeError, IndexError):
        return None
