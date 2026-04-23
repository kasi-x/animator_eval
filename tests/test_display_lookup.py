"""Smoke tests for src/utils/display_lookup.py.

Covers:
- Prefix routing: ``anilist:N`` hits src_anilist_anime, ``ann:N`` hits src_ann_anime, etc.
- Missing rows return ``None`` (not KeyError or row-tuple access bugs).
- Unknown / malformed anime_id returns ``None``.
- Sources that cannot answer a field (e.g. ann has no score) return ``None``
  and the fallback only runs when sensible.
- Description fallback walks anilist → allcinema.
- JSON fields (genres/tags/synonyms) parse properly from bronze blobs.
- Cache memoizes results; clear_cache() resets it.
- File-not-found (no bronze path) returns None gracefully.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from src.utils.display_lookup import (
    clear_cache,
    get_display_cover_url,
    get_display_description,
    get_display_favourites,
    get_display_genres,
    get_display_popularity,
    get_display_score,
    get_display_synonyms,
    get_display_tags,
)


# ---------------------------------------------------------------------------
# Helpers: write minimal bronze schema to a real SQLite file on disk.
# DuckDB's SQLite scanner requires a real file (no :memory: attach).
# ---------------------------------------------------------------------------


_BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS src_anilist_anime (
    anilist_id INTEGER PRIMARY KEY,
    title_ja TEXT, title_en TEXT,
    description TEXT,
    score REAL, popularity INTEGER, favourites INTEGER,
    cover_large TEXT, cover_medium TEXT, banner TEXT, site_url TEXT,
    genres TEXT DEFAULT '[]',
    tags TEXT DEFAULT '[]',
    synonyms TEXT DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS src_ann_anime (
    ann_id INTEGER PRIMARY KEY,
    title_en TEXT, title_ja TEXT,
    year INTEGER
);
CREATE TABLE IF NOT EXISTS src_allcinema_anime (
    allcinema_id INTEGER PRIMARY KEY,
    title_ja TEXT,
    synopsis TEXT
);
CREATE TABLE IF NOT EXISTS src_seesaawiki_anime (
    id TEXT PRIMARY KEY,
    title_ja TEXT,
    year INTEGER
);
CREATE TABLE IF NOT EXISTS src_keyframe_anime (
    slug TEXT PRIMARY KEY,
    title_ja TEXT,
    title_en TEXT,
    anilist_id INTEGER
);
"""


def _make_db(tmp_path: Path, extra_ddl: str = "", rows: list[tuple] = ()) -> Path:
    """Create a fresh SQLite file with bronze schema and optional seed rows.

    ``rows`` is a list of ``(sql, params)`` tuples executed after DDL.
    """
    db_path = tmp_path / "bronze.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_BRONZE_DDL)
    if extra_ddl:
        conn.executescript(extra_ddl)
    for sql, params in rows:
        conn.execute(sql, params)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def db(tmp_path) -> Path:
    """Empty bronze DB with the standard schema."""
    clear_cache()
    path = _make_db(tmp_path)
    yield path
    clear_cache()


# ---------------------------------------------------------------------------
# Primary routing by prefix
# ---------------------------------------------------------------------------


def test_anilist_prefix_routes_to_anilist_bronze(tmp_path):
    clear_cache()
    db_path = _make_db(
        tmp_path,
        rows=[
            (
                "INSERT INTO src_anilist_anime "
                "(anilist_id, score, popularity, favourites, description, "
                " cover_large, cover_medium, banner, site_url, genres, tags, synonyms) "
                "VALUES (123, 82.5, 1000, 42, 'desc-A', "
                "'https://img/large.png', 'https://img/medium.png', "
                "'https://img/banner.png', 'https://anilist.co/anime/123', "
                "?, ?, ?)",
                (
                    json.dumps(["Action", "Drama"]),
                    json.dumps([{"name": "Mecha", "rank": 90}]),
                    json.dumps(["SynA"]),
                ),
            )
        ],
    )
    try:
        assert get_display_score("anilist:123", db_path) == 82.5
        assert get_display_popularity("anilist:123", db_path) == 1000
        assert get_display_favourites("anilist:123", db_path) == 42
        assert get_display_description("anilist:123", db_path) == "desc-A"
        assert get_display_cover_url("anilist:123", db_path) == "https://img/large.png"
        assert get_display_genres("anilist:123", db_path) == ["Action", "Drama"]
        tags = get_display_tags("anilist:123", db_path)
        assert tags == [{"name": "Mecha", "rank": 90}]
        assert get_display_synonyms("anilist:123", db_path) == ["SynA"]
    finally:
        clear_cache()


def test_ann_prefix_has_no_score(tmp_path):
    """ann bronze does not store score → get_display_score returns None
    (fallback is not performed for score; cross-platform score is meaningless)."""
    clear_cache()
    db_path = _make_db(
        tmp_path,
        rows=[
            ("INSERT INTO src_ann_anime (ann_id, title_ja, year) VALUES (1, 'x', 2020)", ()),
        ],
    )
    try:
        assert get_display_score("ann:1", db_path) is None
        assert get_display_popularity("ann:1", db_path) is None
    finally:
        clear_cache()


def test_allcinema_description_primary_routing(tmp_path):
    """For an allcinema:NNN id, description primary source is its synopsis."""
    clear_cache()
    db_path = _make_db(
        tmp_path,
        rows=[
            (
                "INSERT INTO src_allcinema_anime (allcinema_id, title_ja, synopsis) "
                "VALUES (77, 'タイトル', '邦画向けあらすじ')",
                (),
            )
        ],
    )
    try:
        assert get_display_description("allcinema:77", db_path) == "邦画向けあらすじ"
    finally:
        clear_cache()


def test_seesaawiki_prefix_uses_text_id(tmp_path):
    """seesaawiki id is TEXT (slug). Score field is not present, returns None."""
    clear_cache()
    db_path = _make_db(
        tmp_path,
        rows=[
            (
                "INSERT INTO src_seesaawiki_anime (id, title_ja, year) "
                "VALUES ('wiki-slug', 'foo', 2021)",
                (),
            )
        ],
    )
    try:
        # score is not on seesaawiki bronze → None
        assert get_display_score("seesaawiki:wiki-slug", db_path) is None
        # description is also absent here → None (no fallback target in this fixture)
        assert get_display_description("seesaawiki:wiki-slug", db_path) is None
    finally:
        clear_cache()


def test_keyframe_prefix_uses_text_slug(tmp_path):
    clear_cache()
    db_path = _make_db(
        tmp_path,
        rows=[
            (
                "INSERT INTO src_keyframe_anime (slug, title_ja, title_en) "
                "VALUES ('ep-1', 'KF', 'KF')",
                (),
            )
        ],
    )
    try:
        assert get_display_score("keyframe:ep-1", db_path) is None
    finally:
        clear_cache()


# ---------------------------------------------------------------------------
# Missing rows and malformed IDs
# ---------------------------------------------------------------------------


def test_missing_row_returns_none(db):
    # Anilist table present but no matching id.
    assert get_display_score("anilist:999999", db) is None
    assert get_display_description("anilist:999999", db) is None
    assert get_display_cover_url("anilist:999999", db) is None
    assert get_display_genres("anilist:999999", db) == []


def test_unknown_prefix_returns_none(db):
    # Unknown prefix is not routed; returns None for scalar, [] for list.
    assert get_display_score("bogus:123", db) is None
    assert get_display_description("bogus:123", db) is None
    assert get_display_genres("bogus:123", db) == []


def test_malformed_anime_id_returns_none(db):
    for bad in ["", "no-colon", "anilist:", ":123", "anilist:not-an-int"]:
        assert get_display_score(bad, db) is None
        assert get_display_description(bad, db) is None


# ---------------------------------------------------------------------------
# Fallback precedence
# ---------------------------------------------------------------------------


def test_description_falls_back_to_allcinema_when_anilist_row_missing(tmp_path):
    """For allcinema:77 id, primary is allcinema.synopsis. Verified in earlier
    test. Here we confirm the reverse is skipped — an anilist:NNN with no
    anilist row and no anime_external_ids table should return None, not
    pick up an unrelated allcinema row."""
    clear_cache()
    db_path = _make_db(
        tmp_path,
        rows=[
            (
                "INSERT INTO src_allcinema_anime (allcinema_id, title_ja, synopsis) "
                "VALUES (77, 'x', 'fallback-text')",
                (),
            )
        ],
    )
    try:
        # No anilist row, no anime_external_ids mapping → None.
        assert get_display_description("anilist:42", db_path) is None
    finally:
        clear_cache()


def test_description_cross_source_fallback_via_external_ids(tmp_path):
    """When anime_external_ids is present and maps anilist:42 → allcinema:77,
    a missing AniList description should fall back to the allcinema synopsis."""
    clear_cache()
    extra_ddl = """
        CREATE TABLE anime_external_ids (
            anime_id TEXT NOT NULL,
            source   TEXT NOT NULL,
            external_id TEXT NOT NULL,
            PRIMARY KEY (anime_id, source)
        );
        INSERT INTO anime_external_ids VALUES ('anilist:42','allcinema','77');
    """
    db_path = _make_db(
        tmp_path,
        extra_ddl=extra_ddl,
        rows=[
            (
                "INSERT INTO src_allcinema_anime (allcinema_id, title_ja, synopsis) "
                "VALUES (77, 'x', 'fallback-text')",
                (),
            ),
            # Anilist has the row but no description (NULL) — fallback should trigger.
            ("INSERT INTO src_anilist_anime (anilist_id, description) VALUES (42, NULL)", ()),
        ],
    )
    try:
        assert get_display_description("anilist:42", db_path) == "fallback-text"
    finally:
        clear_cache()


# ---------------------------------------------------------------------------
# Absent bronze file is handled gracefully
# ---------------------------------------------------------------------------


def test_missing_bronze_file_returns_none(tmp_path):
    """If the bronze file does not exist, helpers return None/[] gracefully."""
    clear_cache()
    nonexistent = tmp_path / "does_not_exist.db"
    try:
        assert get_display_score("anilist:1", nonexistent) is None
        assert get_display_description("anilist:1", nonexistent) is None
        assert get_display_genres("anilist:1", nonexistent) == []
    finally:
        clear_cache()


# ---------------------------------------------------------------------------
# JSON parsing robustness
# ---------------------------------------------------------------------------


def test_bad_json_in_bronze_returns_empty(tmp_path):
    clear_cache()
    db_path = _make_db(
        tmp_path,
        rows=[
            (
                "INSERT INTO src_anilist_anime (anilist_id, genres, tags, synonyms) "
                "VALUES (5, 'not-json', '[{broken', NULL)",
                (),
            )
        ],
    )
    try:
        assert get_display_genres("anilist:5", db_path) == []
        assert get_display_tags("anilist:5", db_path) == []
        assert get_display_synonyms("anilist:5", db_path) == []
    finally:
        clear_cache()


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


def test_cache_returns_memoized_value(tmp_path):
    clear_cache()
    db_path = _make_db(
        tmp_path,
        rows=[
            ("INSERT INTO src_anilist_anime (anilist_id, score) VALUES (7, 70.0)", ()),
        ],
    )
    try:
        assert get_display_score("anilist:7", db_path) == 70.0

        # Mutate the underlying row; a fresh call should still see 70.0 because
        # the result is cached.
        conn = sqlite3.connect(str(db_path))
        conn.execute("UPDATE src_anilist_anime SET score = 10.0 WHERE anilist_id = 7")
        conn.commit()
        conn.close()

        assert get_display_score("anilist:7", db_path) == 70.0

        # After clear_cache(), the new value becomes visible.
        clear_cache()
        assert get_display_score("anilist:7", db_path) == 10.0
    finally:
        clear_cache()
