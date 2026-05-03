"""DuckDB reader for the Resolved layer (Phase 2a).

Provides resolved_connect() context manager and typed loaders for
result/resolved.duckdb — the entity-resolved canonical 1-row-per-entity layer.

Design rules (mirrors conformed_reader.py conventions):
- Per-query open/close — never hold a long-lived connection.
- `memory_limit` always set explicitly.
- No display_* columns — H1: this layer is score-free.
- Graceful empty-list fallback when resolved.duckdb does not yet exist.

Phase 3 migration target: scoring modules will import from here instead of
conformed_reader.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import duckdb
import structlog

logger = structlog.get_logger()

# Phase 2a: resolved.duckdb lives alongside animetor.duckdb in result/
DEFAULT_RESOLVED_PATH: Path = Path(
    os.environ.get("ANIMETOR_RESOLVED_PATH", "result/resolved.duckdb")
)


@contextmanager
def resolved_connect(
    path: Path | str | None = None,
    *,
    memory_limit: str = "8GB",
    read_only: bool = True,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open resolved.duckdb for the duration of one query block.

    Tables live in the `main` schema (unqualified names work directly).

    Raises:
        duckdb.IOException: if the file does not exist (caller should check
            resolved_available() first if graceful degradation is needed).
    """
    p = str(path or DEFAULT_RESOLVED_PATH)
    conn = duckdb.connect(p, read_only=read_only)
    try:
        conn.execute(f"SET memory_limit='{memory_limit}'")
        conn.execute("SET temp_directory='/tmp/duckdb_spill'")
        yield conn
    finally:
        conn.close()


def resolved_available(path: Path | str | None = None) -> bool:
    """Return True if resolved.duckdb exists and is readable."""
    return Path(str(path or DEFAULT_RESOLVED_PATH)).exists()


def _rows_as_dicts(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | None = None,
) -> list[dict[str, Any]]:
    rel = conn.execute(sql, params or [])
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


# ---------------------------------------------------------------------------
# Typed loaders — return AnimeAnalysis / Person model instances
# ---------------------------------------------------------------------------


def _build_studios_map_from_conformed(
    source_ids_by_canonical: dict[str, list[str]],
    conformed_path: Path | str,
) -> dict[str, list[str]]:
    """Build canonical_id → studio_names list by cross-referencing conformed anime_studios.

    For each canonical anime, unions studio names from all contributing conformed IDs.

    Args:
        source_ids_by_canonical: canonical_id → list of conformed IDs (from source_ids_json)
        conformed_path: path to animetor.duckdb (Conformed layer)

    Returns:
        canonical_id → sorted list of studio names (deduplicated)
    """
    # Flatten conformed ID → canonical_id lookup
    conformed_to_canonical: dict[str, str] = {}
    for cid, source_ids in source_ids_by_canonical.items():
        for sid in source_ids:
            conformed_to_canonical[sid] = cid

    if not conformed_to_canonical:
        return {}

    try:
        from src.analysis.io.conformed_reader import conformed_connect

        with conformed_connect(conformed_path) as conn:
            rows = conn.execute(
                """
                SELECT a.anime_id, s.name
                FROM anime_studios a
                JOIN studios s ON s.id = a.studio_id
                WHERE s.name IS NOT NULL AND s.name != ''
                """
            ).fetchall()
    except Exception as exc:
        logger.warning("resolved_anime_studios_conformed_failed", error=str(exc))
        return {}

    result: dict[str, set[str]] = {}
    for conformed_anime_id, studio_name in rows:
        canonical_id = conformed_to_canonical.get(conformed_anime_id)
        if canonical_id:
            result.setdefault(canonical_id, set()).add(studio_name)

    return {cid: sorted(names) for cid, names in result.items()}


def load_anime_resolved(
    path: Path | str | None = None,
    *,
    conformed_path: Path | str | None = None,
) -> list:
    """Load canonical anime rows from resolved.duckdb as AnimeAnalysis instances.

    Populates anime.studios by cross-referencing conformed anime_studios when
    conformed_path is provided. Without conformed_path, studios defaults to [].

    Returns [] if resolved.duckdb does not yet exist (graceful degradation).

    Args:
        path: Path to resolved.duckdb (defaults to DEFAULT_RESOLVED_PATH).
        conformed_path: Optional path to animetor.duckdb for studio lookup.
            When provided, unions studio names across all source IDs per canonical anime.
            Required for AKM / scoring which rely on anime.studios.
    """
    from src.runtime.models import AnimeAnalysis

    if not resolved_available(path):
        logger.warning("resolved_db_absent", path=str(path or DEFAULT_RESOLVED_PATH))
        return []

    with resolved_connect(path) as conn:
        try:
            rows = _rows_as_dicts(conn, "SELECT * FROM anime")
        except Exception as exc:
            logger.warning("resolved_anime_load_failed", error=str(exc))
            return []

    # Build studios map if conformed_path given (for AKM / scoring)
    studios_map: dict[str, list[str]] = {}
    if conformed_path is not None:
        import json as _json

        source_ids_by_canonical: dict[str, list[str]] = {}
        for r in rows:
            try:
                source_ids = _json.loads(r.get("source_ids_json") or "[]")
            except Exception:
                source_ids = []
            source_ids_by_canonical[r["canonical_id"]] = source_ids

        studios_map = _build_studios_map_from_conformed(
            source_ids_by_canonical, conformed_path
        )
        no_studios = sum(1 for cid in source_ids_by_canonical if cid not in studios_map)
        logger.info(
            "resolved_anime_studios_loaded",
            canonical_with_studios=len(studios_map),
            canonical_without_studios=no_studios,
        )

    result: list = []
    skipped = 0
    for r in rows:
        try:
            canonical_id = r["canonical_id"]
            result.append(
                AnimeAnalysis(
                    id=canonical_id,
                    title_ja=r.get("title_ja") or "",
                    title_en=r.get("title_en") or "",
                    year=r.get("year"),
                    season=r.get("season"),
                    quarter=r.get("quarter"),
                    episodes=r.get("episodes"),
                    format=r.get("format"),
                    status=r.get("status"),
                    start_date=r.get("start_date"),
                    end_date=r.get("end_date"),
                    duration=r.get("duration"),
                    original_work_type=r.get("source_mat"),
                    source=r.get("source_mat"),
                    work_type=r.get("work_type"),
                    scale_class=r.get("scale_class"),
                    studios=studios_map.get(canonical_id, []),
                )
            )
        except Exception:
            skipped += 1
            logger.debug("resolved_anime_row_skip", id=r.get("canonical_id"))

    if skipped:
        logger.warning("resolved_anime_skipped", count=skipped)
    logger.info("resolved_anime_loaded", total=len(result))
    return result


def load_persons_resolved(path: Path | str | None = None) -> list:
    """Load canonical persons rows from resolved.duckdb as Person model instances.

    Returns [] if resolved.duckdb does not yet exist.
    """
    from src.runtime.models import Person

    if not resolved_available(path):
        logger.warning("resolved_db_absent", path=str(path or DEFAULT_RESOLVED_PATH))
        return []

    with resolved_connect(path) as conn:
        try:
            rows = _rows_as_dicts(conn, "SELECT * FROM persons")
        except Exception as exc:
            logger.warning("resolved_persons_load_failed", error=str(exc))
            return []

    result: list = []
    skipped = 0
    for r in rows:
        try:
            result.append(
                Person(
                    id=r["canonical_id"],
                    name_ja=r.get("name_ja") or "",
                    name_en=r.get("name_en") or "",
                    date_of_birth=r.get("birth_date"),
                )
            )
        except Exception:
            skipped += 1
            logger.debug("resolved_person_row_skip", id=r.get("canonical_id"))

    if skipped:
        logger.warning("resolved_persons_skipped", count=skipped)
    logger.info("resolved_persons_loaded", total=len(result))
    return result


def load_studios_resolved(path: Path | str | None = None) -> list[dict[str, Any]]:
    """Load canonical studio rows from resolved.duckdb as dicts.

    Returns [] if resolved.duckdb does not yet exist.
    """
    if not resolved_available(path):
        logger.warning("resolved_db_absent", path=str(path or DEFAULT_RESOLVED_PATH))
        return []

    with resolved_connect(path) as conn:
        try:
            return _rows_as_dicts(conn, "SELECT * FROM studios")
        except Exception as exc:
            logger.warning("resolved_studios_load_failed", error=str(exc))
            return []


def query_resolved(
    sql: str,
    params: list[Any] | None = None,
    path: Path | str | None = None,
) -> list[dict[str, Any]]:
    """Run an arbitrary read-only query against resolved.duckdb.

    Returns [] if the database does not exist.
    """
    if not resolved_available(path):
        return []
    with resolved_connect(path) as conn:
        return _rows_as_dicts(conn, sql, params)
