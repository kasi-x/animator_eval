"""Bronze アクセスの唯一の正規ルート (表示専用).

このモジュールは **レポート生成層からのみ呼び出す**。
`src/analysis/**` / `src/pipeline_phases/**` から import することは禁止
(Phase 1-7 の import guard で検知)。

目的:
- silver.anime から score / popularity / description などを物理除去した代わりに、
  レポートが "視聴者評価 X.X" などを表示したい場合のアクセスを単一経路に集約する。
- `get_display_*` 命名で統一し、lint / grep で呼び出し箇所を全列挙可能にする。

注意:
- **分析 (scoring, graph weight, optimization) に使うな**。
- ここで取得した値は UI 表示・メタ情報目的にのみ使う。
"""

from __future__ import annotations

import json
import sqlite3

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
]


def _anilist_id(conn: sqlite3.Connection, anime_id: str) -> int | None:
    row = conn.execute(
        "SELECT anilist_id FROM anime WHERE id = ?", (anime_id,)
    ).fetchone()
    if row is None:
        return None
    return row[0]


def get_display_score(conn: sqlite3.Connection, anime_id: str) -> float | None:
    """AniList 視聴者評価を返す (0-100)。表示専用 — 分析に使うな."""
    aid = _anilist_id(conn, anime_id)
    if aid is None:
        return None
    row = conn.execute(
        "SELECT score FROM src_anilist_anime WHERE anilist_id = ?", (aid,)
    ).fetchone()
    return row[0] if row else None


def get_display_popularity(conn: sqlite3.Connection, anime_id: str) -> int | None:
    """AniList popularity (視聴者母数の proxy)。表示専用 — 分析に使うな."""
    aid = _anilist_id(conn, anime_id)
    if aid is None:
        return None
    row = conn.execute(
        "SELECT popularity FROM src_anilist_anime WHERE anilist_id = ?", (aid,)
    ).fetchone()
    return row[0] if row else None


def get_display_favourites(conn: sqlite3.Connection, anime_id: str) -> int | None:
    """AniList favourites (お気に入り数)。表示専用 — 分析に使うな."""
    aid = _anilist_id(conn, anime_id)
    if aid is None:
        return None
    row = conn.execute(
        "SELECT favourites FROM src_anilist_anime WHERE anilist_id = ?", (aid,)
    ).fetchone()
    return row[0] if row else None


def get_display_description(conn: sqlite3.Connection, anime_id: str) -> str | None:
    """あらすじ (AniList description)。表示専用."""
    aid = _anilist_id(conn, anime_id)
    if aid is None:
        return None
    row = conn.execute(
        "SELECT description FROM src_anilist_anime WHERE anilist_id = ?", (aid,)
    ).fetchone()
    return row[0] if row else None


def get_display_cover_url(conn: sqlite3.Connection, anime_id: str) -> str | None:
    """カバー画像 URL (large 優先 → medium)。表示専用."""
    aid = _anilist_id(conn, anime_id)
    if aid is None:
        return None
    row = conn.execute(
        "SELECT cover_large, cover_medium FROM src_anilist_anime WHERE anilist_id = ?",
        (aid,),
    ).fetchone()
    if row is None:
        return None
    return row[0] or row[1]


def get_display_banner_url(conn: sqlite3.Connection, anime_id: str) -> str | None:
    """バナー画像 URL。表示専用."""
    aid = _anilist_id(conn, anime_id)
    if aid is None:
        return None
    row = conn.execute(
        "SELECT banner FROM src_anilist_anime WHERE anilist_id = ?", (aid,)
    ).fetchone()
    return row[0] if row else None


def get_display_site_url(conn: sqlite3.Connection, anime_id: str) -> str | None:
    """AniList 作品ページ URL。表示専用."""
    aid = _anilist_id(conn, anime_id)
    if aid is None:
        return None
    row = conn.execute(
        "SELECT site_url FROM src_anilist_anime WHERE anilist_id = ?", (aid,)
    ).fetchone()
    return row[0] if row else None


def get_display_genres(conn: sqlite3.Connection, anime_id: str) -> list[str]:
    """表示用ジャンル list (AniList 由来)。

    分析で使うなら正規化テーブル `anime_genres` を SELECT すること。
    """
    aid = _anilist_id(conn, anime_id)
    if aid is None:
        return []
    row = conn.execute(
        "SELECT genres FROM src_anilist_anime WHERE anilist_id = ?", (aid,)
    ).fetchone()
    if not row or not row[0]:
        return []
    try:
        parsed = json.loads(row[0])
    except json.JSONDecodeError:
        return []
    return [g for g in parsed if isinstance(g, str)]


def get_display_tags(conn: sqlite3.Connection, anime_id: str) -> list[dict]:
    """表示用タグ list (name/rank 構造)。

    分析で使うなら正規化テーブル `anime_tags` を SELECT すること。
    """
    aid = _anilist_id(conn, anime_id)
    if aid is None:
        return []
    row = conn.execute(
        "SELECT tags FROM src_anilist_anime WHERE anilist_id = ?", (aid,)
    ).fetchone()
    if not row or not row[0]:
        return []
    try:
        parsed = json.loads(row[0])
    except json.JSONDecodeError:
        return []
    return [t for t in parsed if isinstance(t, dict)]


def get_display_synonyms(conn: sqlite3.Connection, anime_id: str) -> list[str]:
    """表示用タイトル別名 list。表示専用."""
    aid = _anilist_id(conn, anime_id)
    if aid is None:
        return []
    row = conn.execute(
        "SELECT synonyms FROM src_anilist_anime WHERE anilist_id = ?", (aid,)
    ).fetchone()
    if not row or not row[0]:
        return []
    try:
        parsed = json.loads(row[0])
    except json.JSONDecodeError:
        return []
    return [s for s in parsed if isinstance(s, str)]
