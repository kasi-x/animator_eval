"""Runtime import guard tests for analysis/pipeline layer boundaries."""

from __future__ import annotations

import sqlite3

import pytest

import src.analysis  # noqa: F401  # installs guard at import time
from src.etl.integrate import _upsert_anime_genres_tags


def test_analysis_layer_cannot_import_display_lookup():
    with pytest.raises(ImportError, match="must not import"):
        exec(
            "from src.utils.display_lookup import get_display_score",
            {"__name__": "src.analysis._guard_probe"},
        )


def test_pipeline_layer_cannot_import_display_lookup():
    with pytest.raises(ImportError, match="must not import"):
        exec(
            "from src.utils.display_lookup import get_display_score",
            {"__name__": "src.pipeline_phases._guard_probe"},
        )


def test_non_analysis_import_of_display_lookup_is_allowed():
    exec(
        "from src.utils.display_lookup import get_display_score",
        {"__name__": "scripts._guard_probe"},
    )


def test_upsert_anime_genres_tags_normalizes_and_replaces_rows():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE anime_genres (anime_id TEXT NOT NULL, genre_name TEXT NOT NULL, "
        "PRIMARY KEY (anime_id, genre_name))"
    )
    conn.execute(
        "CREATE TABLE anime_tags (anime_id TEXT NOT NULL, tag_name TEXT NOT NULL, rank INTEGER, "
        "PRIMARY KEY (anime_id, tag_name))"
    )

    _upsert_anime_genres_tags(
        conn,
        anime_id="anilist:1",
        genres=["Action", {"name": "Drama"}, "", 123],
        tags=[
            "Mecha",
            {"name": "Sci-Fi", "rank": 90},
            {"name": "BadRank", "rank": 999},
        ],
    )
    conn.commit()

    genres = conn.execute(
        "SELECT genre_name FROM anime_genres WHERE anime_id = ? ORDER BY genre_name",
        ("anilist:1",),
    ).fetchall()
    tags = conn.execute(
        "SELECT tag_name, rank FROM anime_tags WHERE anime_id = ? ORDER BY tag_name",
        ("anilist:1",),
    ).fetchall()

    assert [r[0] for r in genres] == ["Action", "Drama"]
    assert [(r[0], r[1]) for r in tags] == [
        ("BadRank", None),
        ("Mecha", None),
        ("Sci-Fi", 90),
    ]

    _upsert_anime_genres_tags(
        conn,
        anime_id="anilist:1",
        genres=["Comedy"],
        tags=[{"name": "Slice of Life", "rank": 77}],
    )
    conn.commit()

    genres2 = conn.execute(
        "SELECT genre_name FROM anime_genres WHERE anime_id = ?",
        ("anilist:1",),
    ).fetchall()
    tags2 = conn.execute(
        "SELECT tag_name, rank FROM anime_tags WHERE anime_id = ?",
        ("anilist:1",),
    ).fetchall()

    assert genres2 == [("Comedy",)]
    assert tags2 == [("Slice of Life", 77)]
