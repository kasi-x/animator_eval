"""credits within-source duplicate deletion.

Removes rows from ``credits`` where ``(person_id, anime_id, role,
evidence_source, episode)`` is identical within the same ``evidence_source``,
keeping one row per unique key combination (lowest rowid wins).

Design decisions
----------------
- Method A (task-card §重複削除戦略): partition on the five-key tuple;
  ``raw_role`` / ``affiliation`` / ``position`` differences within the same
  logical key are treated as ETL artefacts.
- H1: ``anime.score`` / ``display_*`` columns are never read here.
- H4: ``evidence_source`` column is preserved on every surviving row.
- Idempotent: running twice leaves row count unchanged on the second call.

Usage::

    import duckdb
    from src.etl.dedup.credits_within_source import dedup

    conn = duckdb.connect("result/silver.duckdb")
    stats = dedup(conn, dry_run=True)   # preview
    stats = dedup(conn, dry_run=False)  # execute
    print(stats)
"""

from __future__ import annotations

import structlog

try:
    import duckdb
except ImportError as exc:  # pragma: no cover
    raise ImportError("duckdb is required: pixi install") from exc

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# SQL templates
# ---------------------------------------------------------------------------

# Count rows that would be deleted (dry-run check).
_COUNT_SQL = """
WITH ranked AS (
    SELECT rowid,
           ROW_NUMBER() OVER (
               PARTITION BY person_id, anime_id, role, evidence_source, episode
               ORDER BY rowid
           ) AS rn
    FROM credits
)
SELECT COUNT(*) FROM ranked WHERE rn > 1
"""

# Breakdown by evidence_source of rows that would be deleted.
_BREAKDOWN_SQL = """
WITH ranked AS (
    SELECT rowid, evidence_source,
           ROW_NUMBER() OVER (
               PARTITION BY person_id, anime_id, role, evidence_source, episode
               ORDER BY rowid
           ) AS rn
    FROM credits
)
SELECT evidence_source, COUNT(*) AS rows_to_delete
FROM ranked
WHERE rn > 1
GROUP BY evidence_source
ORDER BY rows_to_delete DESC
"""

# Sample 10 rows that would be deleted (for pre-flight logging).
_SAMPLE_SQL = """
WITH ranked AS (
    SELECT rowid, person_id, anime_id, role, evidence_source, episode,
           ROW_NUMBER() OVER (
               PARTITION BY person_id, anime_id, role, evidence_source, episode
               ORDER BY rowid
           ) AS rn
    FROM credits
)
SELECT person_id, anime_id, role, evidence_source, episode
FROM ranked
WHERE rn > 1
LIMIT 10
"""

# Delete all rows with rn > 1 (keep lowest rowid per partition key).
_DELETE_SQL = """
DELETE FROM credits
WHERE rowid IN (
    SELECT rowid FROM (
        SELECT rowid,
               ROW_NUMBER() OVER (
                   PARTITION BY person_id, anime_id, role, evidence_source, episode
                   ORDER BY rowid
               ) AS rn
        FROM credits
    ) ranked
    WHERE rn > 1
)
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def dedup(
    conn: "duckdb.DuckDBPyConnection",
    dry_run: bool = False,
    force: bool = False,
) -> dict[str, object]:
    """Remove within-source duplicate credits rows.

    Keeps the row with the lowest ``rowid`` for each
    ``(person_id, anime_id, role, evidence_source, episode)`` combination.
    Cross-source duplicates (same key, different ``evidence_source``) are
    intentionally preserved.

    Args:
        conn: Open read-write DuckDB connection to the SILVER database.
        dry_run: When ``True``, compute statistics without deleting any rows.
        force: When ``True``, skip the stop-if guard that blocks deletion of
            more than 1,000,000 rows.  Use only after verifying ``dry_run``
            output confirms the deletion is expected.

    Returns:
        Dict with keys:

        - ``"before"`` (int): total ``credits`` rows before deletion.
        - ``"after"`` (int): total rows after deletion (equals ``before``
          when ``dry_run=True``).
        - ``"deleted"`` (int): rows removed (0 when ``dry_run=True``).
        - ``"deleted_per_source"`` (dict[str, int]): per-source breakdown.
        - ``"dry_run"`` (bool): echoes the input flag.

    Raises:
        RuntimeError: if ``to_delete > 1_000_000`` and ``dry_run=False``
            and ``force=False`` (stop-if guard — inspect with ``dry_run=True``
            first, then re-run with ``force=True`` to proceed).
    """
    before: int = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]  # type: ignore[index]
    log.info("credits_dedup.start", before=before, dry_run=dry_run)

    to_delete: int = conn.execute(_COUNT_SQL).fetchone()[0]  # type: ignore[index]
    breakdown_rows = conn.execute(_BREAKDOWN_SQL).fetchall()
    deleted_per_source: dict[str, int] = {r[0]: r[1] for r in breakdown_rows}

    _log_sample(conn, to_delete)

    log.info(
        "credits_dedup.planned",
        to_delete=to_delete,
        per_source=deleted_per_source,
        dry_run=dry_run,
        force=force,
    )

    if to_delete > 1_000_000 and not dry_run and not force:
        raise RuntimeError(
            f"Stop-if guard: {to_delete:,} rows would be deleted "
            f"(exceeds 1,000,000 limit). "
            f"Verify with dry_run=True, then re-run with force=True to proceed."
        )

    deleted = 0
    if not dry_run:
        conn.execute(_DELETE_SQL)
        deleted = to_delete

    after: int = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]  # type: ignore[index]
    log.info("credits_dedup.done", before=before, after=after, deleted=deleted, dry_run=dry_run)

    return {
        "before": before,
        "after": after,
        "deleted": deleted,
        "deleted_per_source": deleted_per_source,
        "dry_run": dry_run,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _log_sample(conn: "duckdb.DuckDBPyConnection", to_delete: int) -> None:
    """Log a sample of rows that would be deleted for human inspection."""
    if to_delete == 0:
        log.info("credits_dedup.sample: nothing to delete")
        return
    sample_rows = conn.execute(_SAMPLE_SQL).fetchall()
    log.info(
        "credits_dedup.sample (first 10 rows that would be deleted)",
        sample=[
            {
                "person_id": r[0],
                "anime_id": r[1],
                "role": r[2],
                "evidence_source": r[3],
                "episode": r[4],
            }
            for r in sample_rows
        ],
    )
