"""Bronze-layer display helper — single entrance for viewer-facing fields.

Only called from scripts/report_generators/ — never from src/analysis/ or
src/pipeline_phases/ (enforced in Phase 1-7).

Rationale
---------
Silver (`anime`, `persons`, `credits`, ...) deliberately does not carry
`score`, `popularity`, `favourites`, `description`, `cover_*`, `banner`,
`site_url`, or viewer-sourced JSON blobs (genres/tags/synonyms). Those fields
live only in the bronze source tables (`src_anilist_anime`, `src_ann_anime`,
`src_allcinema_anime`, `src_seesaawiki_anime`, `src_keyframe_anime`).

Reports sometimes need to surface such values as informational metadata
("参考値 — 視聴者評価"). This module is the *only* approved path from a
downstream consumer to those bronze fields.

Hard rules
----------
- **Do not import from src/analysis/** or **src/pipeline_phases/**. An
  import-guard lint (Phase 1-7) will fail CI if these helpers appear in
  analysis or pipeline code.
- The values returned here **must never** enter a scoring formula, edge
  weight, optimization target, classification threshold, or any input to
  any metric published in the two-layer evaluation model.
- Always present the value as a reference figure, never as a score input.
- To make grep-audits trivial, every public function starts with
  ``get_display_``.

anime_id convention
-------------------
Silver uses composite IDs of the form ``{source}:{external_id}``:

- ``anilist:123``       → ``src_anilist_anime.anilist_id = 123``
- ``ann:456``           → ``src_ann_anime.ann_id = 456``
- ``allcinema:789``     → ``src_allcinema_anime.allcinema_id = 789``
- ``seesaawiki:some-slug`` → ``src_seesaawiki_anime.id = 'some-slug'``
- ``keyframe:some-slug`` → ``src_keyframe_anime.slug = 'some-slug'``

The functions below parse the prefix directly from ``anime_id`` and dispatch
to the matching bronze table. If the primary source cannot answer the field
(either the row is missing, or that source does not store the field at all),
the helper falls back through a documented precedence list — see each
function's docstring for the specific ordering.

Caching
-------
Results are memoized in a process-local dict keyed by
``(id(conn), anime_id, field)``. Wire-conn identity is folded into the key
so tests with ephemeral in-memory databases do not collide. TTL is
``_TTL_SECONDS`` (default 300 s), matching ``src/utils/json_io.py`` patterns.
``clear_cache()`` is exposed for tests.
"""

from __future__ import annotations

import json
import sqlite3
import time
from threading import Lock
from typing import Any

import structlog

logger = structlog.get_logger()

__all__ = [
    "get_display_score",
    "get_display_popularity",
    "get_display_favourites",
    "get_display_description",
    "get_display_cover_url",
    "get_display_banner_url",
    "get_display_site_url",
    "get_display_genres",
    "get_display_tags",
    "get_display_synonyms",
    "clear_cache",
]

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

_TTL_SECONDS: int = 300
_cache: dict[tuple, tuple[float, Any]] = {}
_cache_lock = Lock()


def clear_cache() -> None:
    """Drop all cached display-lookup results. Intended for tests."""
    with _cache_lock:
        _cache.clear()


def _cache_get(key: tuple) -> tuple[bool, Any]:
    """Return (hit, value). hit=False means not cached or expired."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return False, None
        ts, value = entry
        if time.time() - ts > _TTL_SECONDS:
            _cache.pop(key, None)
            return False, None
        return True, value


def _cache_put(key: tuple, value: Any) -> None:
    with _cache_lock:
        _cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Prefix routing
# ---------------------------------------------------------------------------

_VALID_PREFIXES = ("anilist", "ann", "allcinema", "seesaawiki", "keyframe")


def _split_anime_id(anime_id: str) -> tuple[str | None, str | None]:
    """Parse ``'{source}:{external}'`` into (source, external_id_str).

    Returns ``(None, None)`` if the format is unrecognized.
    """
    if not isinstance(anime_id, str) or ":" not in anime_id:
        return None, None
    source, _, external = anime_id.partition(":")
    if source not in _VALID_PREFIXES or not external:
        return None, None
    return source, external


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _fetch_from_anilist(
    conn: sqlite3.Connection, external_id: str, column: str
) -> Any:
    """Look up a column in ``src_anilist_anime`` keyed by ``anilist_id``."""
    if not _table_exists(conn, "src_anilist_anime"):
        return None
    try:
        aid = int(external_id)
    except (TypeError, ValueError):
        return None
    row = conn.execute(
        f"SELECT {column} FROM src_anilist_anime WHERE anilist_id = ?",
        (aid,),
    ).fetchone()
    if row is None:
        return None
    return row[0]


def _fetch_from_ann(
    conn: sqlite3.Connection, external_id: str, column: str
) -> Any:
    if not _table_exists(conn, "src_ann_anime"):
        return None
    try:
        aid = int(external_id)
    except (TypeError, ValueError):
        return None
    row = conn.execute(
        f"SELECT {column} FROM src_ann_anime WHERE ann_id = ?",
        (aid,),
    ).fetchone()
    if row is None:
        return None
    return row[0]


def _fetch_from_allcinema(
    conn: sqlite3.Connection, external_id: str, column: str
) -> Any:
    if not _table_exists(conn, "src_allcinema_anime"):
        return None
    try:
        aid = int(external_id)
    except (TypeError, ValueError):
        return None
    row = conn.execute(
        f"SELECT {column} FROM src_allcinema_anime WHERE allcinema_id = ?",
        (aid,),
    ).fetchone()
    if row is None:
        return None
    return row[0]


def _fetch_from_seesaawiki(
    conn: sqlite3.Connection, external_id: str, column: str
) -> Any:
    if not _table_exists(conn, "src_seesaawiki_anime"):
        return None
    row = conn.execute(
        f"SELECT {column} FROM src_seesaawiki_anime WHERE id = ?",
        (external_id,),
    ).fetchone()
    if row is None:
        return None
    return row[0]


def _fetch_from_keyframe(
    conn: sqlite3.Connection, external_id: str, column: str
) -> Any:
    if not _table_exists(conn, "src_keyframe_anime"):
        return None
    row = conn.execute(
        f"SELECT {column} FROM src_keyframe_anime WHERE slug = ?",
        (external_id,),
    ).fetchone()
    if row is None:
        return None
    return row[0]


# Primary fetch by silver-id prefix.
_ROUTERS = {
    "anilist": _fetch_from_anilist,
    "ann": _fetch_from_ann,
    "allcinema": _fetch_from_allcinema,
    "seesaawiki": _fetch_from_seesaawiki,
    "keyframe": _fetch_from_keyframe,
}


def _route_primary(
    conn: sqlite3.Connection, anime_id: str, column_by_source: dict[str, str]
) -> Any:
    """Dispatch to the bronze table matching the anime_id prefix.

    ``column_by_source`` maps source_code → column_name for that source's
    bronze table. A source absent from the dict means that source cannot
    answer this field (e.g. ann has no ``score``).
    """
    source, external = _split_anime_id(anime_id)
    if source is None:
        logger.debug("display_lookup.unrecognized_anime_id", anime_id=anime_id)
        return None
    column = column_by_source.get(source)
    if column is None:
        return None
    return _ROUTERS[source](conn, external, column)


def _route_with_fallback(
    conn: sqlite3.Connection,
    anime_id: str,
    column_by_source: dict[str, str],
    fallback_order: tuple[str, ...],
) -> Any:
    """Try primary source first; if no value, walk ``fallback_order``.

    Only bronze tables that actually store the requested field (i.e., present
    as keys in ``column_by_source``) are consulted during fallback.

    For cross-source mappings (e.g. ``anime_id`` is ``ann:456`` but we want
    AniList's description), we attempt to resolve the other-source external
    id via ``anime_external_ids`` if that table exists. Otherwise fallback
    is skipped — there is no silver-era correspondence table yet.
    """
    source, external = _split_anime_id(anime_id)
    if source is None:
        return None

    # 1. Primary: the anime_id's own source.
    primary_col = column_by_source.get(source)
    if primary_col is not None:
        value = _ROUTERS[source](conn, external, primary_col)
        if value is not None and value != "":
            return value

    # 2. Fallbacks: try the rest, resolving cross-source external IDs if we
    #    have the normalized table available.
    has_ext_ids = _table_exists(conn, "anime_external_ids")
    for alt_source in fallback_order:
        if alt_source == source:
            continue
        alt_col = column_by_source.get(alt_source)
        if alt_col is None:
            continue
        alt_external = external if alt_source == source else None
        if has_ext_ids:
            row = conn.execute(
                "SELECT external_id FROM anime_external_ids "
                "WHERE anime_id = ? AND source = ?",
                (anime_id, alt_source),
            ).fetchone()
            if row is not None:
                alt_external = row[0]
        if alt_external is None:
            continue
        value = _ROUTERS[alt_source](conn, alt_external, alt_col)
        if value is not None and value != "":
            return value

    return None


def _cached(
    conn: sqlite3.Connection, anime_id: str, field: str, compute
) -> Any:
    key = (id(conn), anime_id, field)
    hit, cached_value = _cache_get(key)
    if hit:
        return cached_value
    value = compute()
    _cache_put(key, value)
    return value


# ---------------------------------------------------------------------------
# Public display helpers
# ---------------------------------------------------------------------------


def get_display_score(conn: sqlite3.Connection, anime_id: str) -> float | None:
    """AniList viewer rating (0-100). Display only — never use in analysis.

    Source: AniList only. Scores are not cross-comparable across platforms,
    so no fallback is performed.
    """

    def _compute() -> float | None:
        val = _route_primary(conn, anime_id, {"anilist": "score"})
        return float(val) if val is not None else None

    return _cached(conn, anime_id, "score", _compute)


def get_display_popularity(
    conn: sqlite3.Connection, anime_id: str
) -> int | None:
    """AniList popularity (user-list count). Display only — audience metric.

    Source: AniList only.
    """

    def _compute() -> int | None:
        val = _route_primary(conn, anime_id, {"anilist": "popularity"})
        return int(val) if val is not None else None

    return _cached(conn, anime_id, "popularity", _compute)


def get_display_favourites(
    conn: sqlite3.Connection, anime_id: str
) -> int | None:
    """AniList favourites count. Display only.

    Source: AniList only.
    """

    def _compute() -> int | None:
        val = _route_primary(conn, anime_id, {"anilist": "favourites"})
        return int(val) if val is not None else None

    return _cached(conn, anime_id, "favourites", _compute)


def get_display_description(
    conn: sqlite3.Connection, anime_id: str
) -> str | None:
    """Work synopsis for display. Never enter this into scoring.

    Fallback precedence: anilist > allcinema (``synopsis``) > ann > seesaawiki
    > keyframe. AniList descriptions are richer; allcinema provides JP
    synopses when AniList is absent. ann has no description column in
    bronze; seesaawiki / keyframe likewise — they are skipped automatically
    because they are absent from ``column_by_source``.
    """

    def _compute() -> str | None:
        val = _route_with_fallback(
            conn,
            anime_id,
            column_by_source={
                "anilist": "description",
                "allcinema": "synopsis",
            },
            fallback_order=(
                "anilist",
                "allcinema",
                "ann",
                "seesaawiki",
                "keyframe",
            ),
        )
        return str(val) if val is not None else None

    return _cached(conn, anime_id, "description", _compute)


def get_display_cover_url(
    conn: sqlite3.Connection, anime_id: str
) -> str | None:
    """Cover image URL (large → medium). Display only.

    Source: AniList only — it is the only bronze table that stores image
    URLs.
    """

    def _compute() -> str | None:
        source, external = _split_anime_id(anime_id)
        if source is None:
            return None
        # Try primary if it is anilist; otherwise try resolving via
        # anime_external_ids to the AniList external_id.
        anilist_external: str | None = None
        if source == "anilist":
            anilist_external = external
        elif _table_exists(conn, "anime_external_ids"):
            row = conn.execute(
                "SELECT external_id FROM anime_external_ids "
                "WHERE anime_id = ? AND source = 'anilist'",
                (anime_id,),
            ).fetchone()
            if row is not None:
                anilist_external = row[0]
        if anilist_external is None:
            return None
        if not _table_exists(conn, "src_anilist_anime"):
            return None
        try:
            aid = int(anilist_external)
        except (TypeError, ValueError):
            return None
        row = conn.execute(
            "SELECT cover_large, cover_medium "
            "FROM src_anilist_anime WHERE anilist_id = ?",
            (aid,),
        ).fetchone()
        if row is None:
            return None
        return row[0] or row[1] or None

    return _cached(conn, anime_id, "cover_url", _compute)


def get_display_banner_url(
    conn: sqlite3.Connection, anime_id: str
) -> str | None:
    """Banner image URL. Display only. AniList source only."""

    def _compute() -> str | None:
        val = _route_primary(conn, anime_id, {"anilist": "banner"})
        return str(val) if val else None

    return _cached(conn, anime_id, "banner_url", _compute)


def get_display_site_url(
    conn: sqlite3.Connection, anime_id: str
) -> str | None:
    """Canonical site URL for the work (AniList page). Display only."""

    def _compute() -> str | None:
        val = _route_primary(conn, anime_id, {"anilist": "site_url"})
        return str(val) if val else None

    return _cached(conn, anime_id, "site_url", _compute)


def _parse_json_list(raw: Any) -> list:
    if raw in (None, ""):
        return []
    if not isinstance(raw, str):
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.debug("display_lookup.bad_json", head=raw[:64])
        return []
    return parsed if isinstance(parsed, list) else []


def get_display_genres(
    conn: sqlite3.Connection, anime_id: str
) -> list[str]:
    """Display-oriented genre list (raw AniList JSON).

    Analysis code should read the normalized ``anime_genres`` junction
    table instead — this helper is for report-page chips only.
    """

    def _compute() -> list[str]:
        val = _route_primary(conn, anime_id, {"anilist": "genres"})
        return [g for g in _parse_json_list(val) if isinstance(g, str)]

    return _cached(conn, anime_id, "genres", _compute)


def get_display_tags(
    conn: sqlite3.Connection, anime_id: str
) -> list[dict]:
    """Display-oriented tag list (raw AniList JSON with name/rank)."""

    def _compute() -> list[dict]:
        val = _route_primary(conn, anime_id, {"anilist": "tags"})
        return [t for t in _parse_json_list(val) if isinstance(t, dict)]

    return _cached(conn, anime_id, "tags", _compute)


def get_display_synonyms(
    conn: sqlite3.Connection, anime_id: str
) -> list[str]:
    """Alternate titles for display. AniList source only."""

    def _compute() -> list[str]:
        val = _route_primary(conn, anime_id, {"anilist": "synonyms"})
        return [s for s in _parse_json_list(val) if isinstance(s, str)]

    return _cached(conn, anime_id, "synonyms", _compute)
