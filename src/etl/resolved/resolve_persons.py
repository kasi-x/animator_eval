"""Person canonical row builder for the Resolved layer (Phase 2b).

Reads conformed.persons rows, applies entity_resolution (H3-compliant),
and writes one canonical row per resolved identity.

Entity clustering strategy (Phase 2b):
- Calls entity_resolution.exact_match_cluster + cross_source_match on
  all conformed.persons rows to produce {person_id → canonical_id} mapping.
- Rows not merged by entity_resolution become singleton canonical rows
  (their conformed id IS their canonical_id).
- fast_only=True by default: only exact + cross_source steps run during ETL
  to keep runtime feasible for 270K+ persons.

H3 compliance:
  - entity_resolution logic is invoked unmodified.
  - No algorithm changes, threshold changes, or new merge conditions.
  - This module only reads the output and builds canonical rows.

canonical_id format:
  - Merged rows: canonical_id = the "representative" person_id chosen by
    entity_resolution (the first alphabetically in the cluster, per the
    resolve_all logic).
  - Singletons: canonical_id = conformed person id.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import structlog

from src.etl.resolved._ddl import ALL_DDL
from src.etl.resolved._decisions_log import DecisionsLogger, build_decision_record
from src.etl.resolved._persons_cluster import (
    build_persons_canonical_map,
    group_persons_by_canonical,
)
from src.etl.resolved._select import build_audit_entries, select_representative_value
from src.etl.resolved.source_ranking import PERSONS_RANKING

logger = structlog.get_logger()

PERSONS_FIELDS: list[str] = list(PERSONS_RANKING.keys())

# NOT NULL DEFAULT '' fields — coerce None → '' on insert
_PERSONS_NOT_NULL_STR = frozenset(
    {"name_ja", "name_en", "name_ko", "name_zh", "source_ids_json"}
)


def _load_conformed_persons(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Load conformed persons rows as dicts.

    bgm_id is selected gracefully: falls back to NULL when the column
    does not yet exist (e.g. in test fixtures).
    """
    table_cols = {
        c[0]
        for c in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='conformed' AND table_name='persons'"
        ).fetchall()
    }
    bgm_id_expr = "bgm_id" if "bgm_id" in table_cols else "NULL AS bgm_id"

    rel = conn.execute(
        f"SELECT id, name_ja, name_en, name_ko, name_zh, "
        f"birth_date, death_date, gender, nationality, {bgm_id_expr} "
        "FROM conformed.persons"
    )
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def _resolve_persons_cluster(
    canonical_id: str,
    cluster_rows: list[dict[str, Any]],
    *,
    decisions_logger: DecisionsLogger | None = None,
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
        if decisions_logger is not None:
            decisions_logger.write(
                build_decision_record(
                    canonical_id=canonical_id,
                    entity_type="person",
                    field=field,
                    candidates=cluster_rows,
                    selected_value=value,
                    winning_source=winning_source,
                    selection_rule=reason,
                    priority=priority,
                )
            )

    return resolved, audit_entries


def build_resolved_persons(
    conformed_path: Path | str,
    resolved_path: Path | str,
    *,
    fast_only: bool = True,
    batch_size: int = 10_000,
    decisions_path: Path | str | None = None,
    decisions_sample_rate: float = 0.0,
) -> int:
    """Full-rebuild of resolved persons from conformed.persons.

    Applies entity_resolution (exact + cross_source by default) to cluster
    conformed persons, then writes one canonical row per resolved identity.

    Args:
        conformed_path: Path to animetor.duckdb (Conformed layer).
        resolved_path: Path to resolved.duckdb (Resolved layer).
        fast_only: When True (default), only run exact_match_cluster +
                   cross_source_match (fast). When False, also run romaji
                   and similarity steps (slow, for overnight runs).
        batch_size: INSERT batch size.

    Returns:
        Number of canonical person rows written.
    """
    logger.info(
        "resolve_persons_start",
        conformed=str(conformed_path),
        resolved=str(resolved_path),
        fast_only=fast_only,
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

    # Build cross-source canonical map via entity_resolution
    canonical_map = build_persons_canonical_map(rows, fast_only=fast_only)
    logger.info("resolve_persons_canonical_map", merges=len(canonical_map))

    # Group rows by canonical_id
    groups = group_persons_by_canonical(rows, canonical_map)
    logger.info("resolve_persons_groups", count=len(groups))

    resolved_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    use_log = decisions_path is not None and decisions_sample_rate > 0.0
    if use_log:
        log_ctx = DecisionsLogger(decisions_path, sample_rate=decisions_sample_rate)
        log_ctx.truncate()
        log_ctx.__enter__()
    else:
        log_ctx = None

    try:
        for canonical_id, cluster_rows in groups.items():
            resolved_row, cluster_audit = _resolve_persons_cluster(
                canonical_id, cluster_rows, decisions_logger=log_ctx
            )
            resolved_rows.append(resolved_row)
            audit_rows.extend(cluster_audit)
    finally:
        if log_ctx is not None:
            log_ctx.__exit__(None, None, None)
            logger.info(
                "resolve_persons_decisions_logged",
                written=log_ctx.written,
                skipped=log_ctx.skipped,
                path=str(decisions_path),
            )

    _insert_resolved_persons(resolved_path, resolved_rows, batch_size)
    _insert_audit_persons(resolved_path, audit_rows, batch_size)

    count = len(resolved_rows)
    logger.info(
        "resolve_persons_done",
        canonical_count=count,
        audit_count=len(audit_rows),
    )
    return count


_PERSONS_INSERT_FIELDS: list[str] = [
    "canonical_id", "name_ja", "name_en", "name_ko", "name_zh",
    "birth_date", "death_date", "gender", "nationality",
    "source_ids_json",
    "name_ja_source", "name_en_source", "gender_source", "birth_date_source",
]


def _rows_to_arrow_persons(
    rows: list[dict[str, Any]],
    fields: list[str],
    not_null_str: frozenset[str],
) -> "Any":
    """Convert persons row dicts to a PyArrow table for bulk insert."""
    import pyarrow as pa

    columns: dict[str, list] = {f: [] for f in fields}
    for r in rows:
        for f in fields:
            v = r.get(f)
            if f in not_null_str and v is None:
                v = ""
            columns[f].append(v)

    return pa.table(
        [pa.array(columns[f], type=pa.string()) for f in fields],
        names=fields,
    )


def _insert_resolved_persons(
    resolved_path: Path | str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Insert resolved persons rows using PyArrow bulk insert for performance."""
    if not rows:
        return

    fields = _PERSONS_INSERT_FIELDS

    conn = duckdb.connect(str(resolved_path))
    try:
        try:
            tbl = _rows_to_arrow_persons(rows, fields, _PERSONS_NOT_NULL_STR)
            conn.register("_persons_staging", tbl)
            conn.execute(
                f"INSERT OR REPLACE INTO persons ({', '.join(fields)}) "
                f"SELECT {', '.join(fields)} FROM _persons_staging"
            )
            conn.unregister("_persons_staging")
        except ImportError:
            placeholders = ", ".join("?" * len(fields))
            sql = (
                f"INSERT OR REPLACE INTO persons ({', '.join(fields)}) "
                f"VALUES ({placeholders})"
            )
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                values = [
                    tuple(
                        (r.get(f) or "") if f in _PERSONS_NOT_NULL_STR else r.get(f)
                        for f in fields
                    )
                    for r in batch
                ]
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

    _AUDIT_FIELDS_P = [
        "canonical_id", "entity_type", "field_name",
        "field_value", "source_name", "selection_reason",
    ]

    conn = duckdb.connect(str(resolved_path))
    try:
        try:
            tbl = _rows_to_arrow_persons(rows, _AUDIT_FIELDS_P, frozenset())
            conn.register("_persons_audit_staging", tbl)
            conn.execute(
                "INSERT INTO meta_resolution_audit "
                "(canonical_id, entity_type, field_name, field_value, source_name, selection_reason) "
                "SELECT canonical_id, entity_type, field_name, field_value, source_name, selection_reason "
                "FROM _persons_audit_staging"
            )
            conn.unregister("_persons_audit_staging")
        except ImportError:
            sql = (
                "INSERT INTO meta_resolution_audit "
                "(canonical_id, entity_type, field_name, field_value, source_name, selection_reason) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
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
