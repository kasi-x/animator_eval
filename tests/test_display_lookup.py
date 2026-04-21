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
"""

from __future__ import annotations

import json
import sqlite3

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
# Fixtures: minimal bronze schema identical in shape to v49 `src_*_anime`.
# ---------------------------------------------------------------------------


def _mk_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE src_anilist_anime (
            anilist_id INTEGER PRIMARY KEY,
            title_ja TEXT, title_en TEXT,
            description TEXT,
            score REAL, popularity INTEGER, favourites INTEGER,
            cover_large TEXT, cover_medium TEXT, banner TEXT, site_url TEXT,
            genres TEXT DEFAULT '[]',
            tags TEXT DEFAULT '[]',
            synonyms TEXT DEFAULT '[]'
        );
        CREATE TABLE src_ann_anime (
            ann_id INTEGER PRIMARY KEY,
            title_en TEXT, title_ja TEXT,
            year INTEGER
        );
        CREATE TABLE src_allcinema_anime (
            allcinema_id INTEGER PRIMARY KEY,
            title_ja TEXT,
            synopsis TEXT
        );
        CREATE TABLE src_seesaawiki_anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT,
            year INTEGER
        );
        CREATE TABLE src_keyframe_anime (
            slug TEXT PRIMARY KEY,
            title_ja TEXT,
            title_en TEXT,
            anilist_id INTEGER
        );
        """
    )
    return conn


@pytest.fixture
def conn() -> sqlite3.Connection:
    clear_cache()
    c = _mk_conn()
    yield c
    c.close()
    clear_cache()


# ---------------------------------------------------------------------------
# Primary routing by prefix
# ---------------------------------------------------------------------------


def test_anilist_prefix_routes_to_anilist_bronze(conn):
    conn.execute(
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
    conn.commit()

    assert get_display_score(conn, "anilist:123") == 82.5
    assert get_display_popularity(conn, "anilist:123") == 1000
    assert get_display_favourites(conn, "anilist:123") == 42
    assert get_display_description(conn, "anilist:123") == "desc-A"
    assert get_display_cover_url(conn, "anilist:123") == "https://img/large.png"
    assert get_display_genres(conn, "anilist:123") == ["Action", "Drama"]
    tags = get_display_tags(conn, "anilist:123")
    assert tags == [{"name": "Mecha", "rank": 90}]
    assert get_display_synonyms(conn, "anilist:123") == ["SynA"]


def test_ann_prefix_has_no_score(conn):
    """ann bronze does not store score → get_display_score returns None
    (fallback is not performed for score; cross-platform score is meaningless)."""
    conn.execute(
        "INSERT INTO src_ann_anime (ann_id, title_ja, year) VALUES (1, 'x', 2020)"
    )
    conn.commit()
    assert get_display_score(conn, "ann:1") is None
    assert get_display_popularity(conn, "ann:1") is None


def test_allcinema_description_primary_routing(conn):
    """For an allcinema:NNN id, description primary source is its synopsis."""
    conn.execute(
        "INSERT INTO src_allcinema_anime (allcinema_id, title_ja, synopsis) "
        "VALUES (77, 'タイトル', '邦画向けあらすじ')"
    )
    conn.commit()
    assert get_display_description(conn, "allcinema:77") == "邦画向けあらすじ"


def test_seesaawiki_prefix_uses_text_id(conn):
    """seesaawiki id is TEXT (slug). Score field is not present, returns None."""
    conn.execute(
        "INSERT INTO src_seesaawiki_anime (id, title_ja, year) "
        "VALUES ('wiki-slug', 'foo', 2021)"
    )
    conn.commit()
    # score is not on seesaawiki bronze → None
    assert get_display_score(conn, "seesaawiki:wiki-slug") is None
    # description is also absent here → None (no fallback target in this fixture)
    assert get_display_description(conn, "seesaawiki:wiki-slug") is None


def test_keyframe_prefix_uses_text_slug(conn):
    conn.execute(
        "INSERT INTO src_keyframe_anime (slug, title_ja, title_en) "
        "VALUES ('ep-1', 'KF', 'KF')"
    )
    conn.commit()
    assert get_display_score(conn, "keyframe:ep-1") is None


# ---------------------------------------------------------------------------
# Missing rows and malformed IDs
# ---------------------------------------------------------------------------


def test_missing_row_returns_none(conn):
    # Anilist table present but no matching id.
    assert get_display_score(conn, "anilist:999999") is None
    assert get_display_description(conn, "anilist:999999") is None
    assert get_display_cover_url(conn, "anilist:999999") is None
    assert get_display_genres(conn, "anilist:999999") == []


def test_unknown_prefix_returns_none(conn):
    # Unknown prefix is not routed; returns None for scalar, [] for list.
    assert get_display_score(conn, "bogus:123") is None
    assert get_display_description(conn, "bogus:123") is None
    assert get_display_genres(conn, "bogus:123") == []


def test_malformed_anime_id_returns_none(conn):
    for bad in ["", "no-colon", "anilist:", ":123", "anilist:not-an-int"]:
        assert get_display_score(conn, bad) is None
        assert get_display_description(conn, bad) is None


# ---------------------------------------------------------------------------
# Fallback precedence
# ---------------------------------------------------------------------------


def test_description_falls_back_to_allcinema_when_anilist_row_missing(conn):
    """For allcinema:77 id, primary is allcinema.synopsis. Verified in earlier
    test. Here we confirm the reverse is skipped — an anilist:NNN with no
    anilist row and no anime_external_ids table should return None, not
    pick up an unrelated allcinema row."""
    conn.execute(
        "INSERT INTO src_allcinema_anime (allcinema_id, title_ja, synopsis) "
        "VALUES (77, 'x', 'fallback-text')"
    )
    conn.commit()
    # No anilist row, no anime_external_ids mapping → None.
    assert get_display_description(conn, "anilist:42") is None


def test_description_cross_source_fallback_via_external_ids(conn):
    """When anime_external_ids is present and maps anilist:42 → allcinema:77,
    a missing AniList description should fall back to the allcinema synopsis."""
    conn.executescript(
        """
        CREATE TABLE anime_external_ids (
            anime_id TEXT NOT NULL,
            source   TEXT NOT NULL,
            external_id TEXT NOT NULL,
            PRIMARY KEY (anime_id, source)
        );
        INSERT INTO anime_external_ids VALUES ('anilist:42','allcinema','77');
        """
    )
    conn.execute(
        "INSERT INTO src_allcinema_anime (allcinema_id, title_ja, synopsis) "
        "VALUES (77, 'x', 'fallback-text')"
    )
    # Anilist has the row but no description (NULL) — fallback should trigger.
    conn.execute(
        "INSERT INTO src_anilist_anime (anilist_id, description) VALUES (42, NULL)"
    )
    conn.commit()
    assert get_display_description(conn, "anilist:42") == "fallback-text"


# ---------------------------------------------------------------------------
# Absent bronze tables are handled gracefully
# ---------------------------------------------------------------------------


def test_missing_bronze_table_returns_none():
    """If a bronze table is entirely absent, helpers return None, not crash."""
    clear_cache()
    c = sqlite3.connect(":memory:")
    try:
        # No src_anilist_anime at all.
        assert get_display_score(c, "anilist:1") is None
        assert get_display_description(c, "anilist:1") is None
        assert get_display_genres(c, "anilist:1") == []
    finally:
        c.close()
        clear_cache()


# ---------------------------------------------------------------------------
# JSON parsing robustness
# ---------------------------------------------------------------------------


def test_bad_json_in_bronze_returns_empty(conn):
    conn.execute(
        "INSERT INTO src_anilist_anime (anilist_id, genres, tags, synonyms) "
        "VALUES (5, 'not-json', '[{broken', NULL)"
    )
    conn.commit()
    assert get_display_genres(conn, "anilist:5") == []
    assert get_display_tags(conn, "anilist:5") == []
    assert get_display_synonyms(conn, "anilist:5") == []


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


def test_cache_returns_memoized_value(conn):
    conn.execute("INSERT INTO src_anilist_anime (anilist_id, score) VALUES (7, 70.0)")
    conn.commit()
    assert get_display_score(conn, "anilist:7") == 70.0

    # Mutate the underlying row; a fresh call should still see 70.0 because
    # the result is cached.
    conn.execute("UPDATE src_anilist_anime SET score = 10.0 WHERE anilist_id = 7")
    conn.commit()
    assert get_display_score(conn, "anilist:7") == 70.0

    # After clear_cache(), the new value becomes visible.
    clear_cache()
    assert get_display_score(conn, "anilist:7") == 10.0
