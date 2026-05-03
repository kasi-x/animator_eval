"""Anime canonical row builder for the Resolved layer (Phase 2a).

Reads conformed.anime rows (all sources, per-source ID prefix), groups them
into entity clusters, selects representative values per field, and writes
resolved anime canonical rows into result/resolved.duckdb.

Entity clustering strategy (Phase 2a conservative):
- Primary cluster key = normalized title_ja + year.
- Each conformed row that cannot be matched to an existing cluster gets its
  own canonical_id (single-source canonical).
- This is intentionally conservative: false merges are worse than missed merges
  (H3 spirit for anime analogous to person entity resolution).

canonical_id format: `resolved:anime:<hash12>` where hash12 is a deterministic
12-char prefix of SHA256(normalized_title_ja + str(year)).
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
from pathlib import Path
from typing import Any

import duckdb
import structlog

from src.etl.resolved._ddl import ALL_DDL
from src.etl.resolved._select import build_audit_entries, select_representative_value
from src.etl.resolved.source_ranking import ANIME_RANKING

logger = structlog.get_logger()

# Fields to resolve per canonical anime
ANIME_FIELDS: list[str] = list(ANIME_RANKING.keys())

# NOT NULL DEFAULT '' fields — coerce None → '' on insert
_ANIME_NOT_NULL_STR = frozenset({"title_ja", "title_en", "source_ids_json"})


def _normalize_title(title: str) -> str:
    """Normalize a title for clustering: NFKC + strip whitespace + lowercase."""
    if not title:
        return ""
    t = unicodedata.normalize("NFKC", title)
    return t.strip().lower()


def _cluster_key(row: dict[str, Any]) -> str:
    """Compute a stable cluster key from title_ja + year."""
    title = _normalize_title(row.get("title_ja") or "")
    year = str(row.get("year") or "")
    return f"{title}|{year}"


def _canonical_id_from_key(cluster_key: str) -> str:
    """Derive a stable canonical_id from a cluster key."""
    digest = hashlib.sha256(cluster_key.encode()).hexdigest()[:12]
    return f"resolved:anime:{digest}"


def _group_conformed_rows(
    rows: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Group conformed anime rows into clusters by (normalized title_ja, year).

    Each cluster will produce exactly one resolved row.
    Rows with empty title_ja get a solo cluster keyed on their conformed id.
    """
    clusters: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        title_ja = (row.get("title_ja") or "").strip()
        if not title_ja:
            # Untitled entries get their own cluster
            key = f"__nontitle__|{row['id']}"
        else:
            key = _cluster_key(row)
        clusters.setdefault(key, []).append(row)
    return clusters


def _resolve_cluster(
    cluster_key: str,
    cluster_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Resolve a cluster of conformed rows into one canonical anime row.

    Returns:
        Tuple of (resolved_row_dict, audit_entries_list).
    """
    canonical_id = _canonical_id_from_key(cluster_key)
    source_ids = [r["id"] for r in cluster_rows]

    resolved: dict[str, Any] = {
        "canonical_id": canonical_id,
        "source_ids_json": json.dumps(source_ids),
    }
    audit_entries: list[dict[str, Any]] = []

    for field in ANIME_FIELDS:
        priority = ANIME_RANKING[field]
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
                    entity_type="anime",
                    field=field,
                    selected_value=value,
                    winning_source=winning_source,
                    reason=reason,
                )
            )

    return resolved, audit_entries


def _load_conformed_anime(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Load all conformed anime rows as dicts."""
    rel = conn.execute(
        "SELECT id, title_ja, title_en, year, season, quarter, episodes, format, "
        "duration, start_date, end_date, status, source_mat, work_type, scale_class, "
        "country_of_origin FROM conformed.anime"
    )
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def build_resolved_anime(
    conformed_path: Path | str,
    resolved_path: Path | str,
    *,
    batch_size: int = 10_000,
) -> int:
    """Full-rebuild of resolved anime from conformed.anime.

    Wipes the anime table, re-clusters all conformed rows, inserts canonical rows.

    Args:
        conformed_path: Path to animetor.duckdb (Conformed layer).
        resolved_path: Path to resolved.duckdb (Resolved layer, created if absent).
        batch_size: INSERT batch size.

    Returns:
        Number of canonical anime rows written.
    """
    logger.info(
        "resolve_anime_start",
        conformed=str(conformed_path),
        resolved=str(resolved_path),
    )

    # Ensure resolved.duckdb exists and schema is initialized
    resolved_conn = duckdb.connect(str(resolved_path))
    try:
        for ddl in ALL_DDL:
            resolved_conn.execute(ddl)
        resolved_conn.execute("DELETE FROM anime")
        resolved_conn.execute(
            "DELETE FROM meta_resolution_audit WHERE entity_type='anime'"
        )
    finally:
        resolved_conn.close()

    # Load conformed rows (read-only)
    with duckdb.connect(str(conformed_path), read_only=True) as conf_conn:
        conf_conn.execute("SET memory_limit='8GB'")
        rows = _load_conformed_anime(conf_conn)

    logger.info("resolve_anime_conformed_loaded", count=len(rows))

    # Cluster
    clusters = _group_conformed_rows(rows)
    logger.info("resolve_anime_clusters", count=len(clusters))

    # Resolve and insert
    resolved_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    for cluster_key, cluster_rows in clusters.items():
        resolved_row, cluster_audit = _resolve_cluster(cluster_key, cluster_rows)
        resolved_rows.append(resolved_row)
        audit_rows.extend(cluster_audit)

    _insert_resolved_anime(resolved_path, resolved_rows, batch_size)
    _insert_audit(resolved_path, audit_rows, batch_size)

    count = len(resolved_rows)
    logger.info(
        "resolve_anime_done",
        canonical_count=count,
        audit_count=len(audit_rows),
    )
    return count


def _insert_resolved_anime(
    resolved_path: Path | str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Insert resolved anime rows in batches."""
    if not rows:
        return

    insert_fields = [
        "canonical_id", "title_ja", "title_en", "year", "season", "quarter",
        "episodes", "format", "duration", "start_date", "end_date", "status",
        "source_mat", "work_type", "scale_class", "country_of_origin",
        "source_ids_json",
        "title_ja_source", "title_en_source", "year_source", "episodes_source",
        "format_source", "duration_source", "source_mat_source",
    ]
    placeholders = ", ".join("?" * len(insert_fields))
    sql = (
        f"INSERT OR REPLACE INTO anime "
        f"({', '.join(insert_fields)}) VALUES ({placeholders})"
    )

    def _row_values(r: dict[str, Any]) -> tuple:
        return tuple(
            (r.get(f) or "") if f in _ANIME_NOT_NULL_STR else r.get(f)
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


def _insert_audit(
    resolved_path: Path | str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Insert audit entries in batches."""
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
