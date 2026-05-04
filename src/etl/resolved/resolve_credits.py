"""Credits canonical ID substitution for the Resolved layer (Phase 2c).

Reads conformed.credits rows and replaces per-source person_id / anime_id values
with their resolved canonical_id equivalents (e.g. `anilist:p123` →
`resolved:person:abc123`, `mal:a456` → `resolved:anime:def456`).

Design rules:
- ID substitution only — no deduplication, no schema changes to the row.
  Each conformed credit row becomes exactly one resolved credit row.
- evidence_source is preserved as-is (H4).
- Credits whose person_id or anime_id are not found in the canonical maps
  are passed through unchanged (their IDs are already canonical, or they
  originate from a source not yet in the resolved layer).
- Full-rebuild semantics: `DELETE FROM credits` before insert, making the
  operation idempotent.

canonical_id maps come from resolved.duckdb (built by Phase 2a/2b):
  - anime:   source_ids_json → canonical_id  (load via resolved.anime)
  - persons: source_ids_json → canonical_id  (load via resolved.persons)

Result is written to the `credits` table in resolved.duckdb.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import structlog

from src.etl.resolved._ddl import ALL_DDL

logger = structlog.get_logger()

# Fields written per resolved credit row.
_CREDITS_FIELDS: list[str] = [
    "person_id",
    "anime_id",
    "role",
    "raw_role",
    "episode",
    "evidence_source",
    "credit_year",
    "credit_quarter",
    "affiliation",
    "position",
]


def _build_canonical_map_from_resolved(
    resolved_conn: duckdb.DuckDBPyConnection,
    entity_table: str,
) -> dict[str, str]:
    """Build conformed_id → canonical_id mapping from a resolved entity table.

    Reads source_ids_json from every row and inverts the mapping.

    Args:
        resolved_conn: Open connection to resolved.duckdb.
        entity_table: Table name in resolved.duckdb ('anime' or 'persons').

    Returns:
        Dict of {conformed_id: canonical_id}.
    """
    import json as _json

    try:
        rows = resolved_conn.execute(
            f"SELECT canonical_id, source_ids_json FROM {entity_table}"
        ).fetchall()
    except Exception as exc:
        logger.warning(
            "resolve_credits_map_failed",
            entity_table=entity_table,
            error=str(exc),
        )
        return {}

    result: dict[str, str] = {}
    for canonical_id, source_ids_json in rows:
        try:
            source_ids = _json.loads(source_ids_json or "[]")
        except Exception:
            source_ids = []
        for sid in source_ids:
            result[sid] = canonical_id

    return result


def _load_conformed_credits(
    conf_conn: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    """Load all credits from conformed.credits as row dicts."""
    table_cols = {
        c[0]
        for c in conf_conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='conformed' AND table_name='credits'"
        ).fetchall()
    }

    # Select all expected fields; use NULL fallback for optional columns.
    optional_cols = {
        "credit_year": "NULL AS credit_year",
        "credit_quarter": "NULL AS credit_quarter",
        "affiliation": "NULL AS affiliation",
        "position": "NULL AS position",
        "raw_role": "'' AS raw_role",
    }
    select_parts: list[str] = []
    for col in _CREDITS_FIELDS:
        if col in ("person_id", "anime_id", "role", "evidence_source"):
            select_parts.append(col)
        elif col in table_cols:
            select_parts.append(col)
        else:
            select_parts.append(optional_cols.get(col, f"NULL AS {col}"))

    sql = f"SELECT {', '.join(select_parts)} FROM conformed.credits"
    rel = conf_conn.execute(sql)
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


def _apply_canonical_ids(
    rows: list[dict[str, Any]],
    person_map: dict[str, str],
    anime_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Replace per-source person_id / anime_id with canonical IDs.

    Credits whose IDs are absent from the maps are passed through unchanged
    (they may already carry canonical IDs or come from sources not yet resolved).

    Args:
        rows: Conformed credit rows.
        person_map: conformed person_id → canonical_id.
        anime_map: conformed anime_id → canonical_id.

    Returns:
        New list of rows with substituted IDs.
    """
    unmapped_persons: set[str] = set()
    unmapped_anime: set[str] = set()
    result: list[dict[str, Any]] = []

    skipped_null = 0
    for r in rows:
        pid = r["person_id"]
        aid = r["anime_id"]
        # NOT NULL 制約のある列が None の行は skip (resolved.credits は person_id/anime_id NOT NULL)
        if pid is None or aid is None:
            skipped_null += 1
            continue

        new_row = dict(r)
        new_row["person_id"] = person_map.get(pid, pid)
        new_row["anime_id"] = anime_map.get(aid, aid)

        if pid not in person_map:
            unmapped_persons.add(pid)
        if aid not in anime_map:
            unmapped_anime.add(aid)

        result.append(new_row)

    if skipped_null:
        logger.info("resolve_credits_skipped_null_ids", count=skipped_null)

    if unmapped_persons:
        logger.debug(
            "resolve_credits_unmapped_persons",
            sample=sorted(s for s in unmapped_persons if s is not None)[:5],
            total=len(unmapped_persons),
        )
    if unmapped_anime:
        logger.debug(
            "resolve_credits_unmapped_anime",
            sample=sorted(s for s in unmapped_anime if s is not None)[:5],
            total=len(unmapped_anime),
        )

    return result


def _insert_resolved_credits(
    resolved_path: Path | str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> None:
    """Insert resolved credit rows into resolved.duckdb using bulk insert.

    Uses PyArrow for performance; falls back to batched executemany if
    pyarrow is unavailable.

    Args:
        resolved_path: Path to resolved.duckdb.
        rows: Resolved credit rows with canonical IDs.
        batch_size: INSERT batch size for the executemany fallback path.
    """
    if not rows:
        return

    fields = _CREDITS_FIELDS

    conn = duckdb.connect(str(resolved_path))
    try:
        try:
            import pyarrow as pa

            _INT_FIELDS = frozenset(
                {"episode", "credit_year", "credit_quarter", "position"}
            )
            columns: dict[str, list] = {f: [] for f in fields}
            for r in rows:
                for f in fields:
                    v = r.get(f)
                    if f in ("raw_role", "evidence_source") and v is None:
                        v = ""
                    columns[f].append(v)

            arrays = []
            for f in fields:
                if f in _INT_FIELDS:
                    arrays.append(pa.array(columns[f], type=pa.int32()))
                else:
                    arrays.append(pa.array(columns[f], type=pa.string()))

            tbl = pa.table(arrays, names=fields)
            conn.register("_credits_staging", tbl)
            conn.execute(
                f"INSERT INTO credits ({', '.join(fields)}) "
                f"SELECT {', '.join(fields)} FROM _credits_staging"
            )
            conn.unregister("_credits_staging")
        except ImportError:
            placeholders = ", ".join("?" * len(fields))
            sql = (
                f"INSERT INTO credits ({', '.join(fields)}) "
                f"VALUES ({placeholders})"
            )
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                values = [
                    tuple(
                        (r.get(f) or "") if f in ("raw_role", "evidence_source") else r.get(f)
                        for f in fields
                    )
                    for r in batch
                ]
                conn.executemany(sql, values)
        conn.commit()
    finally:
        conn.close()


def build_resolved_credits(
    conformed_path: Path | str,
    resolved_path: Path | str,
    *,
    batch_size: int = 50_000,
) -> int:
    """Full-rebuild of resolved credits from conformed.credits.

    Wipes the credits table in resolved.duckdb, loads all conformed credits,
    replaces person_id and anime_id with canonical IDs from the resolved anime
    and persons tables, and inserts the result.

    Prerequisite: resolved.duckdb must already have anime and persons tables
    populated (Phase 2a/2b).  When resolved.duckdb is absent, DDL is applied
    first (schema initialised to empty tables).

    Args:
        conformed_path: Path to animetor.duckdb (Conformed layer).
        resolved_path: Path to resolved.duckdb (Resolved layer).
        batch_size: INSERT batch size (for executemany fallback path).

    Returns:
        Number of resolved credit rows written.
    """
    logger.info(
        "resolve_credits_start",
        conformed=str(conformed_path),
        resolved=str(resolved_path),
    )

    # Ensure resolved.duckdb schema exists
    resolved_conn = duckdb.connect(str(resolved_path))
    try:
        for ddl in ALL_DDL:
            resolved_conn.execute(ddl)
        resolved_conn.execute("DELETE FROM credits")

        # Build canonical ID maps from already-resolved entities
        person_map = _build_canonical_map_from_resolved(resolved_conn, "persons")
        anime_map = _build_canonical_map_from_resolved(resolved_conn, "anime")
    finally:
        resolved_conn.close()

    logger.info(
        "resolve_credits_maps_built",
        person_canonical_count=len(person_map),
        anime_canonical_count=len(anime_map),
    )

    # Load conformed credits (read-only)
    with duckdb.connect(str(conformed_path), read_only=True) as conf_conn:
        conf_conn.execute("SET memory_limit='8GB'")
        rows = _load_conformed_credits(conf_conn)

    logger.info("resolve_credits_conformed_loaded", count=len(rows))

    # Substitute canonical IDs
    resolved_rows = _apply_canonical_ids(rows, person_map, anime_map)

    # Insert into resolved.duckdb
    _insert_resolved_credits(resolved_path, resolved_rows, batch_size)

    count = len(resolved_rows)
    logger.info("resolve_credits_done", count=count)
    return count
