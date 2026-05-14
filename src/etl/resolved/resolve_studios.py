"""Studio canonical row builder for the Resolved layer (Phase 2b).

Reads conformed.studios rows and builds one canonical row per unique studio.

Entity clustering strategy:
- Phase 2a (legacy): 1 conformed = 1 canonical (conservative).
- Phase 2b (current): cluster rows by normalized name. Rows whose normalized
  name match are merged into a single canonical row. canonical_id = the id
  of the highest-priority-source member (per `name` field priority).
- Rows with NULL/empty name are kept as singletons (no normalization key).

canonical_id format:
- single-member cluster: conformed id (back-compat with Phase 2a).
- multi-member cluster: id of the cluster's highest-priority source.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import duckdb
import structlog

from src.etl.resolved._ddl import ALL_DDL
from src.etl.resolved._decisions_log import DecisionsLogger, build_decision_record
from src.etl.resolved._select import build_audit_entries, select_representative_value
from src.etl.resolved.source_ranking import STUDIOS_RANKING, source_prefix

logger = structlog.get_logger()

STUDIOS_FIELDS: list[str] = list(STUDIOS_RANKING.keys())

# NOT NULL DEFAULT '' fields — coerce None → '' on insert
_STUDIOS_NOT_NULL_STR = frozenset({"name", "source_ids_json"})

# Cluster key normalization: legal/corporate suffix stripping + lowercase trim.
_STUDIO_NAME_SUFFIX_RE = re.compile(
    r"株式会社|㈱|有限会社|\(株\)|\(有\)|Inc\.?|Ltd\.?|Co\.?,? ?Ltd\.?",
    flags=re.IGNORECASE,
)


def _normalize_studio_name(name: str | None) -> str:
    """Return cluster key for a studio name. Empty string means no clustering."""
    if not name:
        return ""
    stripped = _STUDIO_NAME_SUFFIX_RE.sub("", name).strip().lower()
    return stripped


def _cluster_canonical_id(rows: list[dict[str, Any]]) -> str:
    """Pick canonical_id from cluster rows: highest-priority source for `name` field."""
    priority = STUDIOS_RANKING["name"]
    rank: dict[str, int] = {src: i for i, src in enumerate(priority)}
    sentinel = len(priority)

    def _row_rank(row: dict[str, Any]) -> tuple[int, str]:
        return (rank.get(source_prefix(row["id"]), sentinel), row["id"])

    return min(rows, key=_row_rank)["id"]


def _load_conformed_studios(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Load conformed studios rows as dicts."""
    rel = conn.execute(
        "SELECT id, name, is_animation_studio, country_of_origin FROM conformed.studios"
    )
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def _resolve_studio_cluster(
    rows: list[dict[str, Any]],
    *,
    decisions_logger: DecisionsLogger | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Resolve one studio cluster (1+ conformed rows) into a canonical row.

    For single-row clusters, canonical_id = conformed id (back-compat).
    For multi-row clusters, canonical_id = id of the highest-priority source
    member, and per-field values are picked via `select_representative_value`.
    """
    canonical_id = rows[0]["id"] if len(rows) == 1 else _cluster_canonical_id(rows)
    source_ids = [r["id"] for r in rows]

    resolved: dict[str, Any] = {
        "canonical_id": canonical_id,
        "source_ids_json": json.dumps(source_ids),
    }
    audit_entries: list[dict[str, Any]] = []

    for field in STUDIOS_FIELDS:
        priority = STUDIOS_RANKING[field]
        value, winning_source, reason = select_representative_value(
            field=field,
            candidates=rows,
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
        if decisions_logger is not None:
            decisions_logger.write(
                build_decision_record(
                    canonical_id=canonical_id,
                    entity_type="studio",
                    field=field,
                    candidates=rows,
                    selected_value=value,
                    winning_source=winning_source,
                    selection_rule=reason,
                    priority=priority,
                )
            )

    return resolved, audit_entries


def _cluster_rows_by_name(
    rows: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    """Group conformed studio rows by normalized name. Empty key → singleton."""
    by_key: dict[str, list[dict[str, Any]]] = {}
    singletons: list[list[dict[str, Any]]] = []
    for r in rows:
        key = _normalize_studio_name(r.get("name"))
        if not key:
            singletons.append([r])
            continue
        by_key.setdefault(key, []).append(r)
    return list(by_key.values()) + singletons


def build_resolved_studios(
    conformed_path: Path | str,
    resolved_path: Path | str,
    *,
    batch_size: int = 10_000,
    decisions_path: Path | str | None = None,
    decisions_sample_rate: float = 0.0,
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

    use_log = decisions_path is not None and decisions_sample_rate > 0.0
    if use_log:
        log_ctx = DecisionsLogger(decisions_path, sample_rate=decisions_sample_rate)
        log_ctx.__enter__()
    else:
        log_ctx = None

    try:
        clusters = _cluster_rows_by_name(rows)
        multi_source_clusters = sum(1 for c in clusters if len(c) > 1)
        logger.info(
            "resolve_studios_clusters",
            total=len(clusters),
            multi_source=multi_source_clusters,
        )
        for cluster in clusters:
            resolved_row, row_audit = _resolve_studio_cluster(
                cluster, decisions_logger=log_ctx
            )
            resolved_rows.append(resolved_row)
            audit_rows.extend(row_audit)
    finally:
        if log_ctx is not None:
            log_ctx.__exit__(None, None, None)
            logger.info(
                "resolve_studios_decisions_logged",
                written=log_ctx.written,
                skipped=log_ctx.skipped,
                path=str(decisions_path),
            )

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
        "canonical_id",
        "name",
        "is_animation_studio",
        "country_of_origin",
        "source_ids_json",
        "name_source",
        "country_source",
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
