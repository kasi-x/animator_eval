"""Studio canonical row builder for the Resolved layer (Phase 2a).

Reads conformed.studios rows and builds one canonical row per unique studio.

Entity clustering strategy (Phase 2a):
- Each unique conformed id gets its own canonical row (conservative).
- Future work: cross-source studio name matching (Phase 2b+).

canonical_id: use the conformed id directly for Phase 2a.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import structlog

from src.etl.resolved._ddl import ALL_DDL
from src.etl.resolved._select import build_audit_entries, select_representative_value
from src.etl.resolved.source_ranking import STUDIOS_RANKING

logger = structlog.get_logger()

STUDIOS_FIELDS: list[str] = list(STUDIOS_RANKING.keys())

# NOT NULL DEFAULT '' fields — coerce None → '' on insert
_STUDIOS_NOT_NULL_STR = frozenset({"name", "source_ids_json"})


def _load_conformed_studios(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Load conformed studios rows as dicts."""
    rel = conn.execute(
        "SELECT id, name, is_animation_studio, country_of_origin "
        "FROM conformed.studios"
    )
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def _resolve_studio_row(row: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Resolve a single conformed studio into a canonical row.

    Phase 2a: single-source, so no multi-row selection needed.
    """
    canonical_id = row["id"]  # conservative: conformed id IS canonical id
    source_ids = [row["id"]]

    resolved: dict[str, Any] = {
        "canonical_id": canonical_id,
        "source_ids_json": json.dumps(source_ids),
    }
    audit_entries: list[dict[str, Any]] = []

    for field in STUDIOS_FIELDS:
        priority = STUDIOS_RANKING[field]
        value, winning_source, reason = select_representative_value(
            field=field,
            candidates=[row],
            priority=priority,
            id_key="id",
        )
        resolved[field] = value
        resolved[f"{field}_source"] = winning_source if winning_source else None
        if winning_source:
            audit_entries.append(
                build_audit_entries(
                    canonical_id=canonical_id,
                    entity_type="studio",
                    field=field,
                    selected_value=value,
                    winning_source=winning_source,
                    reason=reason,
                )
            )

    return resolved, audit_entries


def build_resolved_studios(
    conformed_path: Path | str,
    resolved_path: Path | str,
    *,
    batch_size: int = 10_000,
) -> int:
    """Full-rebuild of resolved studios from conformed.studios.

    Returns number of canonical studio rows written.
    """
    logger.info(
        "resolve_studios_start",
        conformed=str(conformed_path),
        resolved=str(resolved_path),
    )

    resolved_conn = duckdb.connect(str(resolved_path))
    try:
        for ddl in ALL_DDL:
            resolved_conn.execute(ddl)
        resolved_conn.execute("DELETE FROM studios")
        resolved_conn.execute(
            "DELETE FROM meta_resolution_audit WHERE entity_type='studio'"
        )
    finally:
        resolved_conn.close()

    with duckdb.connect(str(conformed_path), read_only=True) as conf_conn:
        conf_conn.execute("SET memory_limit='4GB'")
        rows = _load_conformed_studios(conf_conn)

    logger.info("resolve_studios_conformed_loaded", count=len(rows))

    resolved_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    for row in rows:
        resolved_row, row_audit = _resolve_studio_row(row)
        resolved_rows.append(resolved_row)
        audit_rows.extend(row_audit)

    _insert_resolved_studios(resolved_path, resolved_rows, batch_size)
    _insert_audit_studios(resolved_path, audit_rows, batch_size)

    count = len(resolved_rows)
    logger.info(
        "resolve_studios_done",
        canonical_count=count,
        audit_count=len(audit_rows),
    )
    return count


def _insert_resolved_studios(
    resolved_path: Path | str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Insert resolved studios rows in batches."""
    if not rows:
        return

    insert_fields = [
        "canonical_id", "name", "is_animation_studio", "country_of_origin",
        "source_ids_json",
        "name_source", "country_source",
    ]
    placeholders = ", ".join("?" * len(insert_fields))
    sql = (
        f"INSERT OR REPLACE INTO studios "
        f"({', '.join(insert_fields)}) VALUES ({placeholders})"
    )

    def _row_values(r: dict[str, Any]) -> tuple:
        return tuple(
            (r.get(f) or "") if f in _STUDIOS_NOT_NULL_STR else r.get(f)
            for f in insert_fields
        )

    conn = duckdb.connect(str(resolved_path))
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            values = [_row_values(r) for r in batch]
            conn.executemany(sql, values)
        conn.commit()
    finally:
        conn.close()


def _insert_audit_studios(
    resolved_path: Path | str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Insert audit entries for studios in batches."""
    if not rows:
        return

    sql = (
        "INSERT INTO meta_resolution_audit "
        "(canonical_id, entity_type, field_name, field_value, source_name, selection_reason) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    conn = duckdb.connect(str(resolved_path))
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            values = [
                (
                    r["canonical_id"],
                    r["entity_type"],
                    r["field_name"],
                    r.get("field_value"),
                    r["source_name"],
                    r["selection_reason"],
                )
                for r in batch
            ]
            conn.executemany(sql, values)
        conn.commit()
    finally:
        conn.close()
