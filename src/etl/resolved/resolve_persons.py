"""Person canonical row builder for the Resolved layer (Phase 2a).

Reads conformed.persons rows, builds one canonical row per unique identity.

Entity clustering strategy:
- Each conformed persons row already carries a unique `id` (source-prefixed).
- Cross-source entity resolution for persons is handled by the existing
  src/analysis/entity/entity_resolution.py pipeline (H3: do not modify).
- Phase 2a: conservative approach — each unique conformed id becomes its own
  canonical person unless a future entity_resolution pass has populated a
  `canonical_id` field in conformed.persons.
- When conformed.persons has a populated `canonical_id`, rows sharing the same
  canonical_id are grouped and representative values are selected.

canonical_id format:
- If conformed row has canonical_id: use that directly.
- Otherwise: use the conformed id itself (single-source canonical).
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb
import structlog

from src.etl.resolved._ddl import ALL_DDL
from src.etl.resolved._select import build_audit_entries, select_representative_value
from src.etl.resolved.source_ranking import PERSONS_RANKING

logger = structlog.get_logger()

PERSONS_FIELDS: list[str] = list(PERSONS_RANKING.keys())

# NOT NULL DEFAULT '' fields — coerce None → '' on insert
_PERSONS_NOT_NULL_STR = frozenset(
    {"name_ja", "name_en", "name_ko", "name_zh", "source_ids_json"}
)


def _load_conformed_persons(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Load conformed persons rows. Include canonical_id if present."""
    # Select only known fields to avoid surprises from extra columns
    rel = conn.execute(
        "SELECT id, name_ja, name_en, name_ko, name_zh, "
        "birth_date, death_date, gender, nationality "
        "FROM conformed.persons"
    )
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def _group_persons_by_canonical(
    rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Group conformed persons by canonical_id (or their own id if none)."""
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        cid = row.get("canonical_id") or row["id"]
        groups[cid].append(row)
    return dict(groups)


def _resolve_persons_cluster(
    canonical_id: str,
    cluster_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Resolve a cluster of conformed persons into one canonical row."""
    source_ids = [r["id"] for r in cluster_rows]

    resolved: dict[str, Any] = {
        "canonical_id": canonical_id,
        "source_ids_json": json.dumps(source_ids),
    }
    audit_entries: list[dict[str, Any]] = []

    for field in PERSONS_FIELDS:
        priority = PERSONS_RANKING[field]
        value, winning_source, reason = select_representative_value(
            field=field,
            candidates=cluster_rows,
            priority=priority,
            id_key="id",
        )
        resolved[field] = value
        resolved[f"{field}_source"] = winning_source if winning_source else None
        if winning_source:
            audit_entries.append(
                build_audit_entries(
                    canonical_id=canonical_id,
                    entity_type="person",
                    field=field,
                    selected_value=value,
                    winning_source=winning_source,
                    reason=reason,
                )
            )

    return resolved, audit_entries


def build_resolved_persons(
    conformed_path: Path | str,
    resolved_path: Path | str,
    *,
    batch_size: int = 10_000,
) -> int:
    """Full-rebuild of resolved persons from conformed.persons.

    Returns number of canonical person rows written.
    """
    logger.info(
        "resolve_persons_start",
        conformed=str(conformed_path),
        resolved=str(resolved_path),
    )

    resolved_conn = duckdb.connect(str(resolved_path))
    try:
        for ddl in ALL_DDL:
            resolved_conn.execute(ddl)
        resolved_conn.execute("DELETE FROM persons")
        resolved_conn.execute(
            "DELETE FROM meta_resolution_audit WHERE entity_type='person'"
        )
    finally:
        resolved_conn.close()

    with duckdb.connect(str(conformed_path), read_only=True) as conf_conn:
        conf_conn.execute("SET memory_limit='8GB'")
        rows = _load_conformed_persons(conf_conn)

    logger.info("resolve_persons_conformed_loaded", count=len(rows))

    groups = _group_persons_by_canonical(rows)
    logger.info("resolve_persons_groups", count=len(groups))

    resolved_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    for canonical_id, cluster_rows in groups.items():
        resolved_row, cluster_audit = _resolve_persons_cluster(canonical_id, cluster_rows)
        resolved_rows.append(resolved_row)
        audit_rows.extend(cluster_audit)

    _insert_resolved_persons(resolved_path, resolved_rows, batch_size)
    _insert_audit_persons(resolved_path, audit_rows, batch_size)

    count = len(resolved_rows)
    logger.info(
        "resolve_persons_done",
        canonical_count=count,
        audit_count=len(audit_rows),
    )
    return count


def _insert_resolved_persons(
    resolved_path: Path | str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Insert resolved persons rows in batches."""
    if not rows:
        return

    insert_fields = [
        "canonical_id", "name_ja", "name_en", "name_ko", "name_zh",
        "birth_date", "death_date", "gender", "nationality",
        "source_ids_json",
        "name_ja_source", "name_en_source", "gender_source", "birth_date_source",
    ]
    placeholders = ", ".join("?" * len(insert_fields))
    sql = (
        f"INSERT OR REPLACE INTO persons "
        f"({', '.join(insert_fields)}) VALUES ({placeholders})"
    )

    def _row_values(r: dict[str, Any]) -> tuple:
        return tuple(
            (r.get(f) or "") if f in _PERSONS_NOT_NULL_STR else r.get(f)
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


def _insert_audit_persons(
    resolved_path: Path | str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Insert audit entries for persons in batches."""
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
