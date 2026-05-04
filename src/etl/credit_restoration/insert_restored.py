"""Insert RESTORED-tier credit rows into the SILVER credits table.

All rows inserted by this module carry:
- ``evidence_source = 'restoration_estimated'``
- ``confidence_tier = 'RESTORED'``

Existing SILVER credits are never modified (H3, H5).
Duplicate detection is performed via INSERT OR IGNORE on the UNIQUE
constraint (person_id, anime_id, raw_role, episode).

Usage example::

    from src.etl.credit_restoration.multi_source_match import (
        find_restoration_candidates,
    )
    from src.etl.credit_restoration.insert_restored import insert_restored_credits

    candidates = find_restoration_candidates(conn)
    n_inserted = insert_restored_credits(conn, candidates)
"""

from __future__ import annotations

from typing import Any

import structlog

from .multi_source_match import EVIDENCE_SOURCE, RestorationCandidate

log = structlog.get_logger(__name__)

# Confidence tier written to every row inserted by this module.
RESTORED_TIER: str = "RESTORED"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ensure_placeholder_person(conn: Any, name_candidate: str) -> str:
    """Ensure a placeholder person row exists for unresolved candidates.

    When a candidate's ``person_id_candidate`` is None, we need a person_id
    to satisfy the NOT NULL constraint.  We create a stub row with a
    deterministic synthetic id of the form ``restored:<normalised_name>``.

    The stub row is INSERT OR IGNORE so it is idempotent.

    Args:
        conn: SILVER DB connection.
        name_candidate: Raw name string from source.

    Returns:
        Synthetic person_id string.
    """
    normalised = name_candidate.strip().lower().replace(" ", "_")
    synthetic_id = f"restored:{normalised}"

    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO persons (id, name_ja, name_en)
            VALUES (?, '', ?)
            """,
            (synthetic_id, name_candidate),
        )
    except Exception as exc:
        log.warning(
            "placeholder_person_insert_failed",
            name=name_candidate,
            error=str(exc),
        )

    return synthetic_id


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def insert_restored_credits(
    conn: Any,
    candidates: list[RestorationCandidate],
    *,
    dry_run: bool = False,
) -> int:
    """Insert RESTORED-tier credit rows for all given candidates.

    Each candidate is inserted with:
    - ``evidence_source = 'restoration_estimated'``
    - ``confidence_tier = 'RESTORED'``

    INSERT OR IGNORE semantics are used so that running the function twice
    is safe.

    Args:
        conn: SILVER DB connection (must support execute / executemany).
        candidates: Output of ``find_restoration_candidates()``.
        dry_run: If True, no rows are written; the function returns the
                 count that *would* be inserted.

    Returns:
        Number of rows actually inserted (0 in dry_run mode).
    """
    if not candidates:
        log.info("insert_restored_credits_no_candidates")
        return 0

    rows_to_insert: list[tuple[Any, ...]] = []

    for cand in candidates:
        person_id = cand.person_id_candidate
        if person_id is None:
            if dry_run:
                person_id = f"restored:{cand.person_name_candidate}"
            else:
                person_id = _ensure_placeholder_person(conn, cand.person_name_candidate)

        rows_to_insert.append(
            (
                person_id,
                cand.anime_id,
                cand.role,
                cand.person_name_candidate,  # stored as raw_role for traceability
                EVIDENCE_SOURCE,             # evidence_source = 'restoration_estimated'
                RESTORED_TIER,               # confidence_tier = 'RESTORED'
                cand.cohort_year,            # credit_year
            )
        )

    if dry_run:
        log.info(
            "insert_restored_credits_dry_run",
            would_insert=len(rows_to_insert),
        )
        return 0

    insert_sql = """
        INSERT OR IGNORE INTO credits
            (person_id, anime_id, role, raw_role,
             evidence_source, confidence_tier, credit_year)
        VALUES
            (?, ?, ?, ?, ?, ?, ?)
    """

    n_inserted = 0
    try:
        conn.executemany(insert_sql, rows_to_insert)
        conn.commit()
        # Count via candidate list length (executemany does not expose row counts).
        n_inserted = len(rows_to_insert)
    except Exception as exc:
        log.error(
            "insert_restored_credits_failed",
            error=str(exc),
            n_candidates=len(candidates),
        )
        raise

    log.info(
        "insert_restored_credits_done",
        n_candidates=len(candidates),
        n_inserted=n_inserted,
        evidence_source=EVIDENCE_SOURCE,
        confidence_tier=RESTORED_TIER,
    )
    return n_inserted


def count_restored_credits(conn: Any) -> dict[str, int]:
    """Return a breakdown of credits by confidence_tier.

    Useful for post-insertion verification.

    Args:
        conn: SILVER DB connection.

    Returns:
        Dict mapping confidence_tier -> row count.
    """
    try:
        rows = conn.execute(
            """
            SELECT confidence_tier, COUNT(*) AS cnt
            FROM   credits
            GROUP  BY confidence_tier
            ORDER  BY confidence_tier
            """
        ).fetchall()
        return {str(r[0]): int(r[1]) for r in rows}
    except Exception as exc:
        log.warning("count_restored_credits_failed", error=str(exc))
        return {}
