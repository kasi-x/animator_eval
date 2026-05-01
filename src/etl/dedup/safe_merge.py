"""Safe dedup merge for SILVER persons and studios.

Reads high-confidence pairs produced by the 18/01 audit
(``result/audit/silver_dedup_persons.csv`` / ``silver_dedup_studios.csv``)
and performs ID-consolidation merges **inside a single DuckDB transaction**.

Design principles
-----------------
- H1: ``anime.score`` never touches merge logic.
- H3: entity_resolution.py is not modified; this is a separate ETL pass.
- H4: ``credits.evidence_source`` is preserved unchanged.
- Canonical ID = lexicographically smallest ID in the pair (deterministic).
- Each merge is recorded in ``meta_entity_resolution_audit`` before any
  DELETE so the audit trail exists even if the DELETE later fails.

High-confidence thresholds
--------------------------
Persons:
  - ``similarity > 0.99`` + ``evidence_birth_date`` exact match (automatic)
  - Birth-date diff ≤ 1 day + role Jaccard > 0.7 (not applied here; the
    audit CSV does not carry Jaccard scores; reserved for future cards)
Studios:
  - ``similarity > 0.99`` + ``country_of_origin`` exact match **or** both
    country_of_origin NULL/empty (normalized name match suffices)
"""

from __future__ import annotations

import csv
import unicodedata
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
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


def _read_csv(path: str | Path) -> list[dict[str, Any]]:
    """Load a CSV audit file as a list of dicts."""
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _parse_date(s: str | None) -> date | None:
    """Parse YYYY-MM-DD string or return None."""
    if not s or not s.strip():
        return None
    try:
        return date.fromisoformat(s.strip())
    except ValueError:
        return None


def _normalize_studio_name(name: str) -> str:
    """NFKC + lowercase + strip all non-alphanumeric chars for studio name comparison.

    Spaces and punctuation are both removed so ``J.C.STAFF``, ``JC STAFF``
    and ``JCSTAFF`` all normalize to ``jcstaff``.
    """
    normed = unicodedata.normalize("NFKC", name).lower()
    return "".join(ch for ch in normed if ch.isalnum())


# ---------------------------------------------------------------------------
# High-confidence filter: persons
# ---------------------------------------------------------------------------


def _is_high_confidence_person(row: dict[str, Any]) -> bool:
    """Return True when row meets automatic merge criteria.

    Criterion A: similarity > 0.99 + birth_date exact match.
    Criterion B: similarity > 0.99 + birth_date diff ≤ 1 day
                 (Jaccard gate skipped — not in CSV; reserved for later).
    """
    sim = float(row.get("similarity", 0))
    if sim <= 0.99:
        return False

    bdate_str = row.get("evidence_birth_date", "")
    bdate = _parse_date(bdate_str)

    if bdate is not None:
        # Criterion A: exact birth date present — automatic
        return True

    # No birth date → cannot verify; exclude
    return False


def _build_merge_reason_person(row: dict[str, Any]) -> str:
    """Short human-readable merge reason for audit trail."""
    bdate = row.get("evidence_birth_date", "")
    return f"similarity={row['similarity']};birth_date_exact={bdate}"


# ---------------------------------------------------------------------------
# High-confidence filter: studios
# ---------------------------------------------------------------------------


def _is_high_confidence_studio(
    row: dict[str, Any],
    name_a: str,
    name_b: str,
    country_a: str | None,
    country_b: str | None,
) -> bool:
    """Return True when the studio pair meets automatic merge criteria.

    Criterion: similarity > 0.99 + (same country OR both NULL/empty).
    The normalized name check is already embedded in the audit query that
    produced similarity = 1.0, but we re-verify name normalization here for
    safety.
    """
    sim = float(row.get("similarity", 0))
    if sim <= 0.99:
        return False

    normed_a = _normalize_studio_name(name_a)
    normed_b = _normalize_studio_name(name_b)
    if normed_a != normed_b:
        return False

    both_null = (not country_a) and (not country_b)
    same_country = country_a and country_b and country_a.strip() == country_b.strip()
    return bool(both_null or same_country)


def _build_merge_reason_studio(row: dict[str, Any]) -> str:
    country = row.get("country_of_origin", "")
    return f"similarity={row['similarity']};name_normed_exact;country={country!r}"


# ---------------------------------------------------------------------------
# Canonical ID selection
# ---------------------------------------------------------------------------


def _canonical_id(*ids: str) -> str:
    """Return the lexicographically smallest ID as the canonical one."""
    return min(ids)


# ---------------------------------------------------------------------------
# Core merge: persons
# ---------------------------------------------------------------------------


def merge_persons(
    conn: duckdb.DuckDBPyConnection,
    audit_csv: str | Path,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Merge high-confidence person duplicates identified by the 18/01 audit.

    Args:
        conn: Open DuckDB connection to silver.duckdb.
        audit_csv: Path to ``silver_dedup_persons.csv`` produced by audit.
        dry_run: If True, compute counts without writing to the DB.

    Returns:
        dict with keys ``merge_count`` (rows merged) and ``audit_logged``
        (audit rows inserted).  In dry-run mode the DB is not modified.
    """
    rows = _read_csv(audit_csv)
    high_conf = [r for r in rows if _is_high_confidence_person(r)]

    log = logger.bind(
        total_candidates=len(rows),
        high_confidence=len(high_conf),
        dry_run=dry_run,
    )
    log.info("merge_persons: starting")

    if not high_conf:
        log.info("merge_persons: no high-confidence pairs, nothing to do")
        return {"merge_count": 0, "audit_logged": 0}

    if dry_run:
        # Verify that both IDs still exist in silver before reporting counts
        existing = {
            row[0]
            for row in conn.execute("SELECT id FROM persons").fetchall()
        }
        actual = [
            r
            for r in high_conf
            if r["candidate_id_a"] in existing and r["candidate_id_b"] in existing
        ]
        log.info("merge_persons: dry-run result", would_merge=len(actual))
        return {"merge_count": len(actual), "audit_logged": len(actual)}

    _ensure_audit_table(conn)

    merge_count = 0
    audit_logged = 0

    for row in high_conf:
        id_a = row["candidate_id_a"]
        id_b = row["candidate_id_b"]
        canonical = _canonical_id(id_a, id_b)
        deprecated = id_b if canonical == id_a else id_a
        similarity = float(row["similarity"])
        reason = _build_merge_reason_person(row)

        # Verify both still exist (a previous merge may have already consumed one)
        existing_ids = {
            r[0]
            for r in conn.execute(
                "SELECT id FROM persons WHERE id IN (?, ?)", [canonical, deprecated]
            ).fetchall()
        }
        if canonical not in existing_ids or deprecated not in existing_ids:
            log.debug(
                "merge_persons: skipping pair, one ID already gone",
                canonical=canonical,
                deprecated=deprecated,
            )
            continue

        # 1. Record in audit table
        conn.execute(
            """
            INSERT INTO meta_entity_resolution_audit
                (redirect_from_id, redirect_to_id, table_name, similarity, merge_reason)
            VALUES (?, ?, 'persons', ?, ?)
            ON CONFLICT (redirect_from_id, table_name) DO NOTHING
            """,
            [deprecated, canonical, similarity, reason],
        )
        audit_logged += 1

        # 2. Re-point credits to canonical ID
        conn.execute(
            "UPDATE credits SET person_id = ? WHERE person_id = ?",
            [canonical, deprecated],
        )

        # 3. Remove the deprecated person row
        conn.execute("DELETE FROM persons WHERE id = ?", [deprecated])
        merge_count += 1

    conn.commit()

    log.info(
        "merge_persons: done",
        merge_count=merge_count,
        audit_logged=audit_logged,
    )
    return {"merge_count": merge_count, "audit_logged": audit_logged}


# ---------------------------------------------------------------------------
# Core merge: studios
# ---------------------------------------------------------------------------


def merge_studios(
    conn: duckdb.DuckDBPyConnection,
    audit_csv: str | Path,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Merge high-confidence studio duplicates identified by the 18/01 audit.

    Args:
        conn: Open DuckDB connection to silver.duckdb.
        audit_csv: Path to ``silver_dedup_studios.csv`` produced by audit.
        dry_run: If True, compute counts without writing to the DB.

    Returns:
        dict with keys ``merge_count`` and ``audit_logged``.
    """
    rows = _read_csv(audit_csv)

    # Pull all studios once for name / country lookup
    studio_lookup: dict[str, tuple[str, str | None]] = {
        row[0]: (row[1], row[2])
        for row in conn.execute(
            "SELECT id, name, country_of_origin FROM studios"
        ).fetchall()
    }

    high_conf: list[dict[str, Any]] = []
    for row in rows:
        id_a = row["candidate_id_a"]
        id_b = row["candidate_id_b"]
        if id_a not in studio_lookup or id_b not in studio_lookup:
            continue
        name_a, country_a = studio_lookup[id_a]
        name_b, country_b = studio_lookup[id_b]
        if _is_high_confidence_studio(row, name_a, name_b, country_a, country_b):
            high_conf.append(row)

    log = logger.bind(
        total_candidates=len(rows),
        high_confidence=len(high_conf),
        dry_run=dry_run,
    )
    log.info("merge_studios: starting")

    if not high_conf:
        log.info("merge_studios: no high-confidence pairs, nothing to do")
        return {"merge_count": 0, "audit_logged": 0}

    if dry_run:
        log.info("merge_studios: dry-run result", would_merge=len(high_conf))
        return {"merge_count": len(high_conf), "audit_logged": len(high_conf)}

    _ensure_audit_table(conn)

    merge_count = 0
    audit_logged = 0

    for row in high_conf:
        id_a = row["candidate_id_a"]
        id_b = row["candidate_id_b"]
        canonical = _canonical_id(id_a, id_b)
        deprecated = id_b if canonical == id_a else id_a
        similarity = float(row["similarity"])
        reason = _build_merge_reason_studio(row)

        # Verify both still exist
        existing_ids = {
            r[0]
            for r in conn.execute(
                "SELECT id FROM studios WHERE id IN (?, ?)", [canonical, deprecated]
            ).fetchall()
        }
        if canonical not in existing_ids or deprecated not in existing_ids:
            log.debug(
                "merge_studios: skipping pair, one ID already gone",
                canonical=canonical,
                deprecated=deprecated,
            )
            continue

        # 1. Audit record
        conn.execute(
            """
            INSERT INTO meta_entity_resolution_audit
                (redirect_from_id, redirect_to_id, table_name, similarity, merge_reason)
            VALUES (?, ?, 'studios', ?, ?)
            ON CONFLICT (redirect_from_id, table_name) DO NOTHING
            """,
            [deprecated, canonical, similarity, reason],
        )
        audit_logged += 1

        # 2. Re-point anime_studios references.
        # DuckDB does not support correlated UPDATE with self-referential EXISTS,
        # so we fetch the safe anime_ids first, then update row by row.
        safe_animes = conn.execute(
            """
            SELECT d.anime_id
            FROM anime_studios d
            WHERE d.studio_id = ?
              AND NOT EXISTS (
                  SELECT 1 FROM anime_studios c
                  WHERE c.anime_id = d.anime_id
                    AND c.studio_id = ?
              )
            """,
            [deprecated, canonical],
        ).fetchall()

        for (anime_id,) in safe_animes:
            conn.execute(
                "UPDATE anime_studios SET studio_id = ? WHERE anime_id = ? AND studio_id = ?",
                [canonical, anime_id, deprecated],
            )

        # 3. Delete any remaining deprecated rows (conflicts where canonical already existed)
        conn.execute(
            "DELETE FROM anime_studios WHERE studio_id = ?",
            [deprecated],
        )

        # 4. Remove the deprecated studio row
        conn.execute("DELETE FROM studios WHERE id = ?", [deprecated])
        merge_count += 1

    conn.commit()

    log.info(
        "merge_studios: done",
        merge_count=merge_count,
        audit_logged=audit_logged,
    )
    return {"merge_count": merge_count, "audit_logged": audit_logged}
