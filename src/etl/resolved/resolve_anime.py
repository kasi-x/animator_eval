"""Anime canonical row builder for the Resolved layer (Phase 2b).

Reads conformed.anime rows (all sources, per-source ID prefix), groups them
into entity clusters using cross-source ID linking + title/year fallback,
selects representative values per field, and writes resolved anime canonical
rows into result/resolved.duckdb.

Entity clustering strategy (Phase 2b):
1. Cross-source ID links (when bronze_root provided):
   - anilist:X ↔ mal:Y via BRONZE anilist.mal_id (18K+ links)
   - keyframe:X ↔ anilist:Y via BRONZE keyframe.anilist_id (1.6K links)
2. Title+year fallback for rows not linked by numeric ID.
Both strategies use Union-Find for transitive cluster closure.

canonical_id format: `resolved:anime:<hash12>` where hash12 is a deterministic
12-char prefix of SHA256(normalized_title_ja + str(year)) of a representative row.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import structlog

from src.etl.resolved._cross_source_ids import build_cross_source_anime_clusters
from src.etl.resolved._ddl import ALL_DDL
from src.etl.resolved._select import build_audit_entries, select_representative_value
from src.etl.resolved.source_ranking import ANIME_RANKING

logger = structlog.get_logger()

# Fields to resolve per canonical anime
ANIME_FIELDS: list[str] = list(ANIME_RANKING.keys())

# NOT NULL DEFAULT '' fields — coerce None → '' on insert
_ANIME_NOT_NULL_STR = frozenset({"title_ja", "title_en", "source_ids_json"})


def _resolve_cluster(
    canonical_id: str,
    cluster_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Resolve a cluster of conformed rows into one canonical anime row.

    Args:
        canonical_id: Pre-computed canonical_id for this cluster
                      (from build_cross_source_anime_clusters).
        cluster_rows: All conformed rows belonging to this cluster.

    Returns:
        Tuple of (resolved_row_dict, audit_entries_list).
    """
    source_ids = [r["id"] for r in cluster_rows]

    resolved: dict[str, Any] = {
        "canonical_id": canonical_id,
        "source_ids_json": json.dumps(source_ids),
        "source_count": len(cluster_rows),
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
    """Load all conformed anime rows as dicts, including cross-source link columns.

    mal_id_int is selected via a CASE expression to gracefully handle databases
    that do not yet have this column (e.g. test fixtures).
    """
    # Check if mal_id_int column exists on the table
    table_cols = {
        c[0]
        for c in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='conformed' AND table_name='anime'"
        ).fetchall()
    }
    mal_id_expr = "mal_id_int" if "mal_id_int" in table_cols else "NULL AS mal_id_int"

    rel = conn.execute(
        f"SELECT id, title_ja, title_en, year, season, quarter, episodes, format, "
        f"duration, start_date, end_date, status, source_mat, work_type, scale_class, "
        f"country_of_origin, {mal_id_expr} FROM conformed.anime"
    )
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def build_resolved_anime(
    conformed_path: Path | str,
    resolved_path: Path | str,
    *,
    bronze_root: Path | str | None = None,
    batch_size: int = 10_000,
) -> int:
    """Full-rebuild of resolved anime from conformed.anime.

    Wipes the anime table, re-clusters all conformed rows using cross-source ID
    linking + title/year fallback, inserts canonical rows.

    Args:
        conformed_path: Path to animetor.duckdb (Conformed layer).
        resolved_path: Path to resolved.duckdb (Resolved layer, created if absent).
        bronze_root: Path to BRONZE parquet root (enables ID-based cross-source
                     clustering; when None, falls back to title+year only).
        batch_size: INSERT batch size.

    Returns:
        Number of canonical anime rows written.
    """
    logger.info(
        "resolve_anime_start",
        conformed=str(conformed_path),
        resolved=str(resolved_path),
        bronze_root=str(bronze_root) if bronze_root else None,
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

    # Cluster using cross-source IDs + title/year fallback
    clusters = build_cross_source_anime_clusters(
        rows,
        bronze_root=bronze_root,
    )
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


_ANIME_INSERT_FIELDS: list[str] = [
    "canonical_id", "title_ja", "title_en", "year", "season", "quarter",
    "episodes", "format", "duration", "start_date", "end_date", "status",
    "source_mat", "work_type", "scale_class", "country_of_origin",
    "source_ids_json", "source_count",
    "title_ja_source", "title_en_source", "year_source", "episodes_source",
    "format_source", "duration_source", "source_mat_source",
]


def _rows_to_arrow(
    rows: list[dict[str, Any]],
    fields: list[str],
    not_null_str: frozenset[str],
) -> "Any":
    """Convert row dicts to a PyArrow table for bulk DuckDB insert."""
    import pyarrow as pa

    # Build column arrays — all as string (VARCHAR) unless known integer
    _INT_FIELDS = frozenset({
        "year", "quarter", "episodes", "duration", "source_count",
    })

    columns: dict[str, list] = {f: [] for f in fields}
    for r in rows:
        for f in fields:
            v = r.get(f)
            if f in not_null_str and v is None:
                v = ""
            columns[f].append(v)

    arrays = []
    for f in fields:
        if f in _INT_FIELDS:
            arrays.append(pa.array(columns[f], type=pa.int32()))
        else:
            arrays.append(pa.array(columns[f], type=pa.string()))

    return pa.table(arrays, names=fields)


def _insert_resolved_anime(
    resolved_path: Path | str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Insert resolved anime rows using PyArrow for bulk performance.

    Falls back to batched executemany if pyarrow unavailable.
    """
    if not rows:
        return

    fields = _ANIME_INSERT_FIELDS

    conn = duckdb.connect(str(resolved_path))
    try:
        try:
            tbl = _rows_to_arrow(rows, fields, _ANIME_NOT_NULL_STR)
            conn.register("_anime_staging", tbl)
            conn.execute(
                f"INSERT OR REPLACE INTO anime ({', '.join(fields)}) "
                f"SELECT {', '.join(fields)} FROM _anime_staging"
            )
            conn.unregister("_anime_staging")
        except ImportError:
            # pyarrow not available — fall back to batched executemany
            placeholders = ", ".join("?" * len(fields))
            sql = (
                f"INSERT OR REPLACE INTO anime ({', '.join(fields)}) "
                f"VALUES ({placeholders})"
            )
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                values = [
                    tuple(
                        (r.get(f) or "") if f in _ANIME_NOT_NULL_STR else r.get(f)
                        for f in fields
                    )
                    for r in batch
                ]
                conn.executemany(sql, values)
        conn.commit()
    finally:
        conn.close()


_AUDIT_FIELDS: list[str] = [
    "canonical_id", "entity_type", "field_name",
    "field_value", "source_name", "selection_reason",
]


def _insert_audit(
    resolved_path: Path | str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Insert audit entries using PyArrow bulk insert for performance."""
    if not rows:
        return

    conn = duckdb.connect(str(resolved_path))
    try:
        try:
            tbl = _rows_to_arrow(rows, _AUDIT_FIELDS, frozenset())
            conn.register("_audit_staging", tbl)
            conn.execute(
                "INSERT INTO meta_resolution_audit "
                "(canonical_id, entity_type, field_name, field_value, source_name, selection_reason) "
                "SELECT canonical_id, entity_type, field_name, field_value, source_name, selection_reason "
                "FROM _audit_staging"
            )
            conn.unregister("_audit_staging")
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
