"""Tests for src/etl/cross_source_copy/anime_extras.py.

Verifies that copy_from_anilist():
  - adds anilist_id_int column (DDL)
  - populates anilist_id_int for anilist / keyframe rows
  - copies extra columns (country_of_origin, synonyms, description,
    external_links_json) from anilist rows to non-anilist rows
  - respects COALESCE semantics (existing dst values are not overwritten)
  - is idempotent
  - honours H1 (no score columns added)
  - works when no linkage is possible (no-op, no error)
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from src.scrapers.bronze_writer import BronzeWriter
from src.etl.cross_source_copy.anime_extras import copy_from_anilist

# ─── Minimal SILVER DDL ──────────────────────────────────────────────────────
# Must include the extension columns that anilist_loader normally adds via ALTER.

_SILVER_DDL = """
CREATE TABLE IF NOT EXISTS anime (
    id                   VARCHAR PRIMARY KEY,
    title_ja             VARCHAR NOT NULL DEFAULT '',
    title_en             VARCHAR NOT NULL DEFAULT '',
    year                 INTEGER,
    season               VARCHAR,
    quarter              INTEGER,
    episodes             INTEGER,
    format               VARCHAR,
    duration             INTEGER,
    start_date           VARCHAR,
    end_date             VARCHAR,
    status               VARCHAR,
    source_mat           VARCHAR,
    work_type            VARCHAR,
    scale_class          VARCHAR,
    fetched_at           TIMESTAMP,
    content_hash         VARCHAR,
    updated_at           TIMESTAMP DEFAULT now(),
    -- anilist extension columns (added by silver_loaders.anilist)
    synonyms             TEXT,
    country_of_origin    TEXT,
    is_licensed          INTEGER,
    is_adult             INTEGER,
    hashtag              TEXT,
    site_url             TEXT,
    trailer_url          TEXT,
    trailer_site         TEXT,
    description          TEXT,
    cover_large          TEXT,
    cover_extra_large    TEXT,
    cover_medium         TEXT,
    banner               TEXT,
    external_links_json  TEXT,
    airing_schedule_json TEXT,
    relations_json       TEXT,
    display_score        REAL,
    display_mean_score   REAL,
    display_favourites   INTEGER,
    display_popularity_rank INTEGER,
    display_rankings_json TEXT,
    -- MAL extension
    mal_id_int           INTEGER
);
"""


def _make_silver_conn() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB with the minimal SILVER anime table."""
    conn = duckdb.connect(":memory:")
    conn.execute(_SILVER_DDL)
    return conn


def _insert_anime(
    conn: duckdb.DuckDBPyConnection,
    anime_id: str,
    *,
    country_of_origin: str | None = None,
    synonyms: str | None = None,
    description: str | None = None,
    external_links_json: str | None = None,
    mal_id_int: int | None = None,
) -> None:
    conn.execute(
        """INSERT INTO anime
               (id, title_ja, title_en,
                country_of_origin, synonyms, description, external_links_json,
                mal_id_int)
           VALUES (?, 'テスト', 'Test', ?, ?, ?, ?, ?)""",
        [
            anime_id,
            country_of_origin,
            synonyms,
            description,
            external_links_json,
            mal_id_int,
        ],
    )


# ─── BRONZE helpers ──────────────────────────────────────────────────────────


def _write_anilist_anime_bronze(
    root: Path,
    *,
    anilist_id: int,
    mal_id: int | None = None,
    country_of_origin: str = "JP",
    synonyms: str = '["AltTitle"]',
    description: str = "A test anime description.",
    external_links_json: str = '[{"url":"https://example.com","site":"Official"}]',
) -> None:
    """Write a minimal AniList BRONZE anime parquet."""
    with BronzeWriter("anilist", table="anime", root=root) as bw:
        bw.append(
            {
                "id": f"anilist:{anilist_id}",
                "title_ja": "テストアニメ",
                "title_en": "Test Anime",
                "year": 2024,
                "season": "WINTER",
                "quarter": 1,
                "episodes": 12,
                "format": "TV",
                "duration": 24,
                "start_date": "2024-01-01",
                "end_date": "2024-03-25",
                "status": "FINISHED",
                "original_work_type": "MANGA",
                "source": "MANGA",
                "work_type": None,
                "scale_class": None,
                "score": 7.5,
                "mean_score": 74.0,
                "favourites": 1234,
                "popularity_rank": 500,
                "rankings_json": "[]",
                "synonyms": synonyms,
                "country_of_origin": country_of_origin,
                "is_licensed": 1,
                "is_adult": 0,
                "hashtag": "#TestAnime",
                "site_url": f"https://anilist.co/anime/{anilist_id}",
                "trailer_url": None,
                "trailer_site": None,
                "description": description,
                "cover_large": None,
                "cover_extra_large": None,
                "cover_medium": None,
                "banner": None,
                "external_links_json": external_links_json,
                "airing_schedule_json": "[]",
                "relations_json": "[]",
                "fetched_at": "2024-04-24T12:00:00",
                "content_hash": f"hash_{anilist_id}",
                "anilist_id": anilist_id,
                "mal_id": mal_id,
            }
        )


def _write_keyframe_anime_bronze(
    root: Path,
    *,
    kf_id: str,
    anilist_id: int,
) -> None:
    """Write a minimal Keyframe BRONZE anime parquet."""
    with BronzeWriter("keyframe", table="anime", root=root) as bw:
        bw.append(
            {
                "id": kf_id,
                "title": "キーフレームテスト",
                "anilist_id": anilist_id,
                "kf_uuid": "test-uuid",
                "status": "finished",
                "slug": "test-slug",
                "delimiters": None,
                "episode_delimiters": None,
                "role_delimiters": None,
                "staff_delimiters": None,
            }
        )


# ─── Tests: DDL ──────────────────────────────────────────────────────────────


def test_ddl_adds_anilist_id_int_column() -> None:
    """copy_from_anilist adds the anilist_id_int column."""
    conn = _make_silver_conn()
    copy_from_anilist(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info('anime')").fetchall()}
    conn.close()
    assert "anilist_id_int" in cols


def test_ddl_idempotent() -> None:
    """Calling copy_from_anilist twice does not raise on DDL re-run."""
    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:100", country_of_origin="JP", synonyms='["Alt"]')
    copy_from_anilist(conn)
    copy_from_anilist(conn)
    conn.close()


# ─── Tests: anilist_id_int population ────────────────────────────────────────


def test_anilist_rows_get_anilist_id_int() -> None:
    """anilist:N rows get anilist_id_int = N."""
    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:12345", country_of_origin="JP")
    copy_from_anilist(conn)
    row = conn.execute(
        "SELECT anilist_id_int FROM anime WHERE id = 'anilist:12345'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == 12345


def test_keyframe_rows_get_anilist_id_int_via_bronze(tmp_path: Path) -> None:
    """keyframe rows get anilist_id_int from BRONZE keyframe anime anilist_id."""
    root = tmp_path / "bronze"
    _write_keyframe_anime_bronze(root, kf_id="keyframe:abc123", anilist_id=42)
    # Also write anilist anime bronze so it doesn't fail on missing parquet
    _write_anilist_anime_bronze(root, anilist_id=42)

    conn = _make_silver_conn()
    _insert_anime(conn, "keyframe:abc123")
    copy_from_anilist(conn, bronze_root=root)
    row = conn.execute(
        "SELECT anilist_id_int FROM anime WHERE id = 'keyframe:abc123'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == 42


def test_mal_rows_get_anilist_id_int_via_bronze(tmp_path: Path) -> None:
    """mal rows get anilist_id_int via anilist BRONZE mal_id cross-reference."""
    root = tmp_path / "bronze"
    # anilist:99 was scraped with mal_id = 7
    _write_anilist_anime_bronze(root, anilist_id=99, mal_id=7)

    conn = _make_silver_conn()
    # Anilist row (source, already has country etc.)
    _insert_anime(
        conn,
        "anilist:99",
        country_of_origin="JP",
        synonyms='["JPSyn"]',
        description="Anilist desc.",
        external_links_json='[{"url":"http://x.com"}]',
    )
    # MAL row with mal_id_int = 7, no extras yet
    _insert_anime(conn, "mal:a7", mal_id_int=7)

    copy_from_anilist(conn, bronze_root=root)

    row = conn.execute(
        "SELECT anilist_id_int FROM anime WHERE id = 'mal:a7'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == 99


# ─── Tests: copy behavior ────────────────────────────────────────────────────


def test_copy_country_to_non_anilist(tmp_path: Path) -> None:
    """country_of_origin is copied from anilist row to linked non-anilist row."""
    root = tmp_path / "bronze"
    _write_keyframe_anime_bronze(root, kf_id="keyframe:kf1", anilist_id=10)
    _write_anilist_anime_bronze(root, anilist_id=10, country_of_origin="KR")

    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:10", country_of_origin="KR", synonyms='["K"]')
    _insert_anime(conn, "keyframe:kf1")  # no extras

    copy_from_anilist(conn, bronze_root=root)

    row = conn.execute(
        "SELECT country_of_origin FROM anime WHERE id = 'keyframe:kf1'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "KR"


def test_copy_synonyms_to_non_anilist(tmp_path: Path) -> None:
    """synonyms is copied from anilist row to linked non-anilist row."""
    root = tmp_path / "bronze"
    _write_keyframe_anime_bronze(root, kf_id="keyframe:kf2", anilist_id=11)
    _write_anilist_anime_bronze(root, anilist_id=11, synonyms='["SynA","SynB"]')

    conn = _make_silver_conn()
    _insert_anime(
        conn, "anilist:11", country_of_origin="JP", synonyms='["SynA","SynB"]'
    )
    _insert_anime(conn, "keyframe:kf2")

    copy_from_anilist(conn, bronze_root=root)

    row = conn.execute(
        "SELECT synonyms FROM anime WHERE id = 'keyframe:kf2'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == '["SynA","SynB"]'


def test_copy_description_coalesce_keeps_existing(tmp_path: Path) -> None:
    """description copy uses COALESCE(dst, src) — existing dst value is preserved."""
    root = tmp_path / "bronze"
    _write_keyframe_anime_bronze(root, kf_id="keyframe:kf3", anilist_id=12)
    _write_anilist_anime_bronze(root, anilist_id=12, description="Anilist description.")

    conn = _make_silver_conn()
    _insert_anime(
        conn, "anilist:12", country_of_origin="JP", description="Anilist description."
    )
    _insert_anime(conn, "keyframe:kf3", description="Keyframe own description.")

    copy_from_anilist(conn, bronze_root=root)

    row = conn.execute(
        "SELECT description FROM anime WHERE id = 'keyframe:kf3'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "Keyframe own description."


def test_copy_description_set_when_null(tmp_path: Path) -> None:
    """description is copied from anilist when dst description IS NULL."""
    root = tmp_path / "bronze"
    _write_keyframe_anime_bronze(root, kf_id="keyframe:kf4", anilist_id=13)
    _write_anilist_anime_bronze(root, anilist_id=13, description="Anilist desc.")

    conn = _make_silver_conn()
    _insert_anime(
        conn, "anilist:13", country_of_origin="JP", description="Anilist desc."
    )
    _insert_anime(conn, "keyframe:kf4")  # description IS NULL

    copy_from_anilist(conn, bronze_root=root)

    row = conn.execute(
        "SELECT description FROM anime WHERE id = 'keyframe:kf4'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "Anilist desc."


def test_copy_external_links_to_non_anilist(tmp_path: Path) -> None:
    """external_links_json is copied from anilist row to linked non-anilist row."""
    root = tmp_path / "bronze"
    _write_keyframe_anime_bronze(root, kf_id="keyframe:kf5", anilist_id=14)
    links = '[{"url":"https://site.com","site":"Official"}]'
    _write_anilist_anime_bronze(root, anilist_id=14, external_links_json=links)

    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:14", country_of_origin="JP", external_links_json=links)
    _insert_anime(conn, "keyframe:kf5")

    copy_from_anilist(conn, bronze_root=root)

    row = conn.execute(
        "SELECT external_links_json FROM anime WHERE id = 'keyframe:kf5'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == links


def test_copy_returns_counts(tmp_path: Path) -> None:
    """copy_from_anilist returns non-zero copy counts when rows are updated."""
    root = tmp_path / "bronze"
    _write_keyframe_anime_bronze(root, kf_id="keyframe:kf6", anilist_id=15)
    _write_anilist_anime_bronze(
        root,
        anilist_id=15,
        country_of_origin="JP",
        synonyms='["Syn"]',
        description="Desc.",
        external_links_json='[{"url":"http://x.com"}]',
    )

    conn = _make_silver_conn()
    _insert_anime(
        conn,
        "anilist:15",
        country_of_origin="JP",
        synonyms='["Syn"]',
        description="Desc.",
        external_links_json='[{"url":"http://x.com"}]',
    )
    _insert_anime(conn, "keyframe:kf6")

    result = copy_from_anilist(conn, bronze_root=root)
    conn.close()

    assert result["country_copied"] >= 1
    assert result["synonyms_copied"] >= 1
    assert result["description_copied"] >= 1
    assert result["external_links_copied"] >= 1


# ─── Tests: idempotency ──────────────────────────────────────────────────────


def test_copy_is_idempotent(tmp_path: Path) -> None:
    """Calling copy_from_anilist twice does not change values on second call."""
    root = tmp_path / "bronze"
    _write_keyframe_anime_bronze(root, kf_id="keyframe:kf7", anilist_id=16)
    _write_anilist_anime_bronze(root, anilist_id=16, country_of_origin="JP")

    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:16", country_of_origin="JP", synonyms='["S"]')
    _insert_anime(conn, "keyframe:kf7")

    copy_from_anilist(conn, bronze_root=root)
    result2 = copy_from_anilist(conn, bronze_root=root)
    conn.close()

    # Second call should copy 0 rows (all already filled)
    assert result2["country_copied"] == 0


# ─── Tests: no-match cases ───────────────────────────────────────────────────


def test_no_copy_without_anilist_id_int(tmp_path: Path) -> None:
    """Rows without anilist_id_int are not updated."""
    root = tmp_path / "bronze"
    _write_anilist_anime_bronze(root, anilist_id=20, country_of_origin="JP")

    conn = _make_silver_conn()
    # bgm row has no linkage to anilist
    _insert_anime(conn, "bgm:s9999")
    _insert_anime(conn, "anilist:20", country_of_origin="JP", synonyms='["X"]')

    copy_from_anilist(conn, bronze_root=root)

    row = conn.execute(
        "SELECT country_of_origin, anilist_id_int FROM anime WHERE id = 'bgm:s9999'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] is None     # country not copied
    assert row[1] is None     # anilist_id_int not set


def test_no_op_when_no_bronze(tmp_path: Path) -> None:
    """copy_from_anilist works without errors when bronze_root has no parquet."""
    empty_root = tmp_path / "empty_bronze"
    empty_root.mkdir()

    conn = _make_silver_conn()
    _insert_anime(conn, "anilist:30", country_of_origin="JP")
    _insert_anime(conn, "keyframe:kf_orphan")

    result = copy_from_anilist(conn, bronze_root=empty_root)
    conn.close()

    assert "country_copied" in result
    assert result["country_copied"] == 0


# ─── Tests: H1 compliance ────────────────────────────────────────────────────


def test_h1_no_score_columns_added() -> None:
    """H1: copy_from_anilist must not add score/popularity/favourites columns."""
    conn = _make_silver_conn()
    copy_from_anilist(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info('anime')").fetchall()}
    conn.close()

    forbidden = {"score", "popularity", "favourites", "mean_score", "popularity_rank"}
    assert not cols & forbidden, f"Forbidden columns found: {cols & forbidden}"


# ─── Tests: MAL cross-reference ──────────────────────────────────────────────


def test_mal_cross_reference_copies_extras(tmp_path: Path) -> None:
    """MAL rows get country_of_origin via anilist BRONZE mal_id cross-reference."""
    root = tmp_path / "bronze"
    _write_anilist_anime_bronze(
        root,
        anilist_id=50,
        mal_id=777,
        country_of_origin="CN",
        synonyms='["MalLinked"]',
    )

    conn = _make_silver_conn()
    _insert_anime(
        conn,
        "anilist:50",
        country_of_origin="CN",
        synonyms='["MalLinked"]',
    )
    _insert_anime(conn, "mal:a777", mal_id_int=777)

    copy_from_anilist(conn, bronze_root=root)

    row = conn.execute(
        "SELECT country_of_origin, synonyms FROM anime WHERE id = 'mal:a777'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "CN"
    assert row[1] == '["MalLinked"]'
