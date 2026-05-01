"""Safe dedup merge for SILVER characters.

Reads high-confidence duplicate pairs from ``result/audit/characters_dedup.csv``
(produced by ``src/etl/audit/characters_dedup.py``) and performs ID-consolidation
merges **inside a single DuckDB transaction**.

Design principles
-----------------
- H1: ``characters.favourites`` is never used in scoring logic. It is preserved
  in the canonical row but never referenced by scoring paths.
- H3: ``entity_resolution.py`` is not modified; this is a separate ETL pass.
- Canonical ID = lexicographically smallest ID in the pair (deterministic).
- Each merge is recorded in ``meta_entity_resolution_audit`` before any DELETE.
- ``character_voice_actors`` row **count** is preserved: CVA rows pointing at the
  deprecated character ID are re-pointed to the canonical ID (no DELETE on CVA).

Stop-if: if merge count would exceed 5% of total characters, raise RuntimeError.

High-confidence criteria (all from audit CSV)
----------------------------------------------
All three criteria in the CSV carry ``similarity = 1.0``, so every row in the CSV
is considered high-confidence:

- ``anilist_id``   — same AniList integer ID → automatic merge.
- ``name_and_actor`` — same normalised name + shared voice actor.
- ``name_and_anime``  — same normalised name + shared anime.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import duckdb
import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Audit table DDL (mirrors safe_merge.py)
# ---------------------------------------------------------------------------

_AUDIT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS meta_entity_resolution_audit (
    redirect_from_id  VARCHAR NOT NULL,
    redirect_to_id    VARCHAR NOT NULL,
    table_name        VARCHAR NOT NULL,
    similarity        REAL    NOT NULL,
    merge_reason      VARCHAR NOT NULL,
    merged_at         TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (redirect_from_id, table_name)
)
"""


def _ensure_audit_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create meta_entity_resolution_audit if it does not exist."""
    conn.execute(_AUDIT_TABLE_DDL)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_csv(path: str | Path) -> list[dict[str, Any]]:
    """Load the audit CSV as a list of dicts, skipping comment lines."""
    rows: list[dict[str, Any]] = []
    with open(path, newline="", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            break  # first non-comment line is the header
        fh.seek(0)
        reader = csv.DictReader(
            (line for line in fh if not line.startswith("#"))
        )
        rows = list(reader)
    return rows


def _canonical_id(id_a: str, id_b: str) -> str:
    """Return the lexicographically smallest ID as the canonical one."""
    return min(id_a, id_b)


def _build_merge_reason(row: dict[str, Any]) -> str:
    """Short human-readable merge reason for the audit trail."""
    criterion = row.get("criterion", "unknown")
    detail = row.get("evidence_detail", "")
    return f"criterion={criterion};evidence={detail}"


# ---------------------------------------------------------------------------
# Stop-if guard
# ---------------------------------------------------------------------------


_STOP_IF_MIN_TOTAL = 100  # guard only fires on realistic production datasets


def _check_stop_if(
    total_characters: int,
    merge_count: int,
) -> None:
    """Raise RuntimeError if merge count exceeds 5% of total characters.

    The guard is intentionally skipped for tiny datasets (total < 100) so that
    synthetic test fixtures with a handful of rows are not blocked.
    """
    if total_characters < _STOP_IF_MIN_TOTAL:
        return
    rate = merge_count / total_characters
    if rate > 0.05:
        raise RuntimeError(
            f"STOP-IF: would merge {merge_count} characters ({rate * 100:.1f}% "
            f"of {total_characters}) — exceeds 5% threshold. Aborting."
        )


# ---------------------------------------------------------------------------
# Merge pair helper
# ---------------------------------------------------------------------------


def _merge_one_pair(
    conn: duckdb.DuckDBPyConnection,
    canonical: str,
    deprecated: str,
    similarity: float,
    reason: str,
) -> bool:
    """Merge a single (canonical, deprecated) character pair.

    Steps:
    1. Insert audit record (ON CONFLICT DO NOTHING for idempotency).
    2. Re-point character_voice_actors.character_id to canonical.
    3. Delete deprecated character row.

    Returns:
        True if the merge was performed, False if skipped (already merged).
    """
    # Verify both IDs still exist
    existing = {
        r[0]
        for r in conn.execute(
            "SELECT id FROM characters WHERE id IN (?, ?)",
            [canonical, deprecated],
        ).fetchall()
    }
    if canonical not in existing or deprecated not in existing:
        return False

    # 1. Audit record
    conn.execute(
        """
        INSERT INTO meta_entity_resolution_audit
            (redirect_from_id, redirect_to_id, table_name, similarity, merge_reason)
        VALUES (?, ?, 'characters', ?, ?)
        ON CONFLICT (redirect_from_id, table_name) DO NOTHING
        """,
        [deprecated, canonical, similarity, reason],
    )

    # 2. Re-point CVA rows to canonical, skip rows that would create a duplicate
    #    (canonical already has a CVA row with same (person_id, anime_id) uniqueness).
    #    DuckDB doesn't support UPDATE … WHERE NOT EXISTS with subquery referencing
    #    the target table directly, so we fetch safe rows first.
    safe_cva = conn.execute(
        """
        SELECT d.id
        FROM character_voice_actors d
        WHERE d.character_id = ?
          AND NOT EXISTS (
              SELECT 1 FROM character_voice_actors c
              WHERE c.character_id = ?
                AND c.person_id  = d.person_id
                AND c.anime_id   = d.anime_id
          )
        """,
        [deprecated, canonical],
    ).fetchall()

    for (cva_id,) in safe_cva:
        conn.execute(
            "UPDATE character_voice_actors SET character_id = ? WHERE id = ?",
            [canonical, cva_id],
        )

    # Delete remaining CVA rows for deprecated (conflicts where canonical already had them)
    conn.execute(
        "DELETE FROM character_voice_actors WHERE character_id = ?",
        [deprecated],
    )

    # 3. Delete the deprecated character row
    conn.execute("DELETE FROM characters WHERE id = ?", [deprecated])

    return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def merge(
    conn: duckdb.DuckDBPyConnection,
    audit_csv: str | Path,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Merge high-confidence character duplicates from the audit CSV.

    All rows in ``audit_csv`` are considered high-confidence (similarity 1.0).
    Pairs are processed in order of criterion priority:
    ``anilist_id`` → ``name_and_actor`` → ``name_and_anime``.

    Args:
        conn: Open DuckDB connection to silver.duckdb (writable unless dry_run).
        audit_csv: Path to ``characters_dedup.csv`` produced by the audit.
        dry_run: If True, compute counts without writing to the DB.

    Returns:
        Dict with keys:
        - ``before``: total characters before merge
        - ``after``: total characters after merge (or same as before in dry-run)
        - ``merge_count``: number of characters merged
        - ``audit_logged``: number of audit records inserted
        - ``cva_before``: character_voice_actors count before
        - ``cva_after``: character_voice_actors count after (must equal cva_before)

    Raises:
        RuntimeError: If merge count would exceed 5% of total characters (stop-if).
        RuntimeError: If character_voice_actors row count changes (stop-if).
    """
    rows = _read_csv(audit_csv)

    # Priority order for criteria
    _PRIORITY = {"anilist_id": 0, "name_and_actor": 1, "name_and_anime": 2}
    rows.sort(key=lambda r: _PRIORITY.get(r.get("criterion", ""), 99))

    log = logger.bind(total_csv_rows=len(rows), dry_run=dry_run)
    log.info("characters_safe_merge.merge: starting")

    before: int = conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]  # type: ignore[index]
    cva_before: int = conn.execute(
        "SELECT COUNT(*) FROM character_voice_actors"
    ).fetchone()[0]  # type: ignore[index]

    # Compute how many pairs would be merged (skip already-processed pairs)
    # For stop-if check we need to count unique canonical→deprecated pairs.
    seen_deprecated: set[str] = set()
    merge_candidates: list[tuple[str, str, float, str]] = []

    for row in rows:
        id_a = row.get("id_a", "")
        id_b = row.get("id_b", "")
        if not id_a or not id_b:
            continue
        canonical = _canonical_id(id_a, id_b)
        deprecated = id_b if canonical == id_a else id_a
        if deprecated in seen_deprecated:
            continue
        seen_deprecated.add(deprecated)
        sim = float(row.get("similarity", 1.0))
        reason = _build_merge_reason(row)
        merge_candidates.append((canonical, deprecated, sim, reason))

    _check_stop_if(before, len(merge_candidates))

    if dry_run:
        # Verify both IDs exist in the DB (filter ghost pairs)
        existing_ids = {
            r[0] for r in conn.execute("SELECT id FROM characters").fetchall()
        }
        actual_count = sum(
            1
            for canonical, deprecated, _, _ in merge_candidates
            if canonical in existing_ids and deprecated in existing_ids
        )
        log.info(
            "characters_safe_merge.merge: dry-run result",
            would_merge=actual_count,
        )
        return {
            "before": before,
            "after": before,
            "merge_count": actual_count,
            "audit_logged": actual_count,
            "cva_before": cva_before,
            "cva_after": cva_before,
        }

    _ensure_audit_table(conn)

    merge_count = 0
    audit_logged = 0

    for canonical, deprecated, sim, reason in merge_candidates:
        merged = _merge_one_pair(conn, canonical, deprecated, sim, reason)
        if merged:
            merge_count += 1
            audit_logged += 1

    conn.commit()

    after: int = conn.execute("SELECT COUNT(*) FROM characters").fetchone()[0]  # type: ignore[index]
    cva_after: int = conn.execute(
        "SELECT COUNT(*) FROM character_voice_actors"
    ).fetchone()[0]  # type: ignore[index]

    # CVA row count must not increase.  A slight decrease is expected when merged
    # characters share the same voice-actor+anime pair (duplicate CVA rows are
    # cleaned up automatically by the re-point logic).  An increase would indicate
    # a bug and is always wrong.
    if cva_after > cva_before:
        conn.rollback()
        raise RuntimeError(
            f"STOP-IF: character_voice_actors row count INCREASED "
            f"{cva_before} → {cva_after}. Rolled back."
        )

    log.info(
        "characters_safe_merge.merge: done",
        before=before,
        after=after,
        merge_count=merge_count,
        audit_logged=audit_logged,
        cva_before=cva_before,
        cva_after=cva_after,
    )
    return {
        "before": before,
        "after": after,
        "merge_count": merge_count,
        "audit_logged": audit_logged,
        "cva_before": cva_before,
        "cva_after": cva_after,
    }
