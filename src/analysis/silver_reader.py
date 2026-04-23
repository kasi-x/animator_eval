"""DuckDB-native readers for the SILVER layer.

Replaces SQLite `get_connection()` + `load_all_*()` calls in analysis modules
with direct DuckDB access to silver.duckdb.

Design rules:
- Per-query open/close — never hold a long-lived connection (atomic swap safety).
- `memory_limit` is always set explicitly to avoid unbounded allocations.
- Silver columns differ from SQLite: see column-mapping notes in each loader.
- Graceful empty-list fallback for tables not yet in silver
  (anime_genres, anime_tags, anime_studios — Card 06 scope).
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import duckdb
import structlog

logger = structlog.get_logger()

DEFAULT_SILVER_PATH: Path = Path(
    os.environ.get("ANIMETOR_SILVER_PATH", "result/silver.duckdb")
)


@contextmanager
def silver_connect(
    path: Path | str | None = None,
    *,
    memory_limit: str = "2GB",
    read_only: bool = True,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open silver.duckdb for the duration of one query block, then close.

    Always opens per-query so analysis modules never pin to a stale inode
    after the pipeline atomically swaps silver.duckdb.
    """
    p = str(path or DEFAULT_SILVER_PATH)
    conn = duckdb.connect(p, read_only=read_only)
    try:
        conn.execute(f"SET memory_limit='{memory_limit}'")
        conn.execute("SET temp_directory='/tmp/duckdb_spill'")
        yield conn
    finally:
        conn.close()


def _rows_as_dicts(
    conn: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None
) -> list[dict[str, Any]]:
    rel = conn.execute(sql, params or [])
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, row)) for row in rel.fetchall()]


# ---------------------------------------------------------------------------
# Typed loaders — return the same Pydantic model types as database.py
# ---------------------------------------------------------------------------

def load_persons_silver(path: Path | str | None = None) -> list:
    """Load all persons from silver.duckdb as Person model instances.

    Silver column mapping:
      birth_date  → date_of_birth
      website_url → site_url
    Missing silver columns (aliases, mal_id, anilist_id, …) get safe defaults.
    """
    from src.runtime.models import Person

    with silver_connect(path) as conn:
        rows = _rows_as_dicts(conn, "SELECT * FROM persons")

    persons = []
    for r in rows:
        try:
            persons.append(Person(
                id=r["id"],
                name_ja=r.get("name_ja") or "",
                name_en=r.get("name_en") or "",
                date_of_birth=r.get("birth_date"),
                site_url=r.get("website_url"),
            ))
        except Exception:
            logger.debug("silver_person_skip", id=r.get("id"))
    return persons


def load_anime_silver(path: Path | str | None = None) -> list:
    """Load all anime from silver.duckdb as AnimeAnalysis model instances.

    Silver column mapping:
      source_mat → original_work_type / source
    Related tables (anime_genres, anime_tags, anime_studios, anime_external_ids)
    are not yet in silver; genres/tags/studios default to empty lists.
    """
    from src.runtime.models import AnimeAnalysis

    with silver_connect(path) as conn:
        rows = _rows_as_dicts(conn, "SELECT * FROM anime")

    anime_list = []
    for r in rows:
        try:
            studios_raw = r.get("studios")
            if isinstance(studios_raw, (list, tuple)):
                studios = list(studios_raw)
            else:
                studios = []
            anime_list.append(AnimeAnalysis(
                id=r["id"],
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
                studios=studios,
            ))
        except Exception:
            logger.debug("silver_anime_skip", id=r.get("id"))
    return anime_list


def load_credits_silver(path: Path | str | None = None) -> list:
    """Load all credits from silver.duckdb as Credit model instances.

    Silver has `evidence_source` (no legacy `source` column). `credit_year`
    and `credit_quarter` are not yet in silver (Card 06 scope); they default
    to None which is safe — callers that need them fall back to SQLite.
    """
    from src.runtime.models import Credit, Role

    with silver_connect(path) as conn:
        rows = _rows_as_dicts(conn, "SELECT * FROM credits")

    credits: list[Credit] = []
    skipped = 0
    for r in rows:
        try:
            credits.append(Credit(
                person_id=r["person_id"],
                anime_id=r["anime_id"],
                role=Role(r["role"]),
                raw_role=r.get("raw_role") or None,
                episode=r.get("episode"),
                source=r.get("evidence_source") or "",
                evidence_source=r.get("evidence_source"),
            ))
        except (ValueError, KeyError):
            skipped += 1
    if skipped:
        logger.warning("silver_credits_skipped_unknown_role", count=skipped)
    return credits


# ---------------------------------------------------------------------------
# Raw dict loaders — for analytics that don't need Pydantic model conversion
# ---------------------------------------------------------------------------

def query_silver(
    sql: str,
    params: list[Any] | None = None,
    path: Path | str | None = None,
) -> list[dict[str, Any]]:
    """Run an arbitrary read-only query against silver.duckdb."""
    with silver_connect(path) as conn:
        return _rows_as_dicts(conn, sql, params)


# ---------------------------------------------------------------------------
# Convenience helpers used by api.py (Step C migration)
# ---------------------------------------------------------------------------

def silver_available(path: Path | str | None = None) -> bool:
    """Return True if silver.duckdb exists and is readable."""
    return Path(str(path or DEFAULT_SILVER_PATH)).exists()


def load_all_credits(path: Path | str | None = None) -> list:
    """Load credits from silver.duckdb as Credit objects. Returns [] if unavailable."""
    from src.runtime.models import Credit, Role

    if not silver_available(path):
        return []
    result: list = []
    skipped = 0
    with silver_connect(path) as conn:
        rel = conn.execute("SELECT * FROM credits")
        cols = [d[0] for d in rel.description]
        for row in rel.fetchall():
            r = dict(zip(cols, row))
            try:
                result.append(
                    Credit(
                        person_id=r["person_id"],
                        anime_id=r["anime_id"],
                        role=Role(r["role"]),
                        raw_role=r.get("raw_role") or None,
                        episode=r.get("episode"),
                        source=r.get("evidence_source") or "",
                        evidence_source=r.get("evidence_source"),
                    )
                )
            except Exception:
                skipped += 1
    if skipped:
        logger.warning("silver_credits_skipped", count=skipped)
    return result


def load_all_anime(path: Path | str | None = None) -> list:
    """Load anime from silver.duckdb (base fields only). Returns [] if unavailable."""
    from src.runtime.models import AnimeAnalysis

    if not silver_available(path):
        return []
    result: list = []
    with silver_connect(path) as conn:
        rel = conn.execute("SELECT * FROM anime")
        cols = [d[0] for d in rel.description]
        for row in rel.fetchall():
            r = dict(zip(cols, row))
            try:
                studios_raw = r.get("studios")
                studios = list(studios_raw) if isinstance(studios_raw, (list, tuple)) else []
                result.append(
                    AnimeAnalysis(
                        id=r["id"],
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
                        studios=studios,
                    )
                )
            except Exception:
                pass
    return result


def silver_db_stats(
    silver_path: Path | str | None = None,
    gold_path: Path | str | None = None,
) -> dict:
    """Return DB stats for the /api/stats endpoint. Returns {} if gold unavailable."""
    from src.analysis.gold_writer import gold_connect_with_silver, DEFAULT_GOLD_DB_PATH

    g_path = Path(str(gold_path or DEFAULT_GOLD_DB_PATH))
    if not g_path.exists():
        return {}
    stats: dict = {}
    try:
        with gold_connect_with_silver(g_path, silver_path) as conn:
            for table in ("persons", "anime", "credits", "person_scores"):
                try:
                    stats[f"{table}_count"] = conn.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0]
                except Exception:
                    stats[f"{table}_count"] = 0
            try:
                role_counts = conn.execute(
                    "SELECT role, COUNT(*) AS cnt FROM credits GROUP BY role"
                ).fetchall()
                stats["distinct_roles"] = len(role_counts)
            except Exception:
                pass
            try:
                yr = conn.execute(
                    "SELECT MIN(start_year), MAX(start_year) FROM anime"
                    " WHERE start_year IS NOT NULL"
                ).fetchone()
                if yr and yr[0]:
                    stats["year_min"], stats["year_max"] = yr[0], yr[1]
            except Exception:
                pass
            try:
                for source, cnt in conn.execute(
                    "SELECT evidence_source, COUNT(*) AS cnt FROM credits"
                    " WHERE evidence_source != '' GROUP BY evidence_source ORDER BY cnt DESC"
                ).fetchall():
                    stats[f"credits_source_{source or 'unknown'}"] = cnt
            except Exception:
                pass
    except Exception as exc:
        logger.warning("silver_db_stats_failed", error=str(exc))
    return stats
