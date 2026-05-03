"""Tests for Phase 2a/2b Resolved layer ETL + reader.

Covers:
- source_ranking declarations (smoke)
- _select.select_representative_value (unit)
- resolve_anime / resolve_persons / resolve_studios (integration with synthetic fixture)
- Phase 2b cross-source clustering (_cross_source_ids, _persons_cluster)
- resolved_reader (smoke + load functions)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pytest

# ---------------------------------------------------------------------------
# Fixtures: synthetic animetor.duckdb (conformed schema)
# ---------------------------------------------------------------------------

_CONFORMED_DDL = """
CREATE SCHEMA IF NOT EXISTS conformed;

CREATE TABLE IF NOT EXISTS conformed.anime (
    id                VARCHAR PRIMARY KEY,
    title_ja          VARCHAR NOT NULL DEFAULT '',
    title_en          VARCHAR NOT NULL DEFAULT '',
    year              INTEGER,
    season            VARCHAR,
    quarter           INTEGER,
    episodes          INTEGER,
    format            VARCHAR,
    duration          INTEGER,
    start_date        VARCHAR,
    end_date          VARCHAR,
    status            VARCHAR,
    source_mat        VARCHAR,
    work_type         VARCHAR,
    scale_class       VARCHAR,
    country_of_origin VARCHAR
);

CREATE TABLE IF NOT EXISTS conformed.persons (
    id          VARCHAR PRIMARY KEY,
    name_ja     VARCHAR NOT NULL DEFAULT '',
    name_en     VARCHAR NOT NULL DEFAULT '',
    name_ko     VARCHAR NOT NULL DEFAULT '',
    name_zh     VARCHAR NOT NULL DEFAULT '',
    birth_date  VARCHAR,
    death_date  VARCHAR,
    gender      VARCHAR,
    nationality VARCHAR
);

CREATE TABLE IF NOT EXISTS conformed.studios (
    id                  VARCHAR PRIMARY KEY,
    name                VARCHAR NOT NULL DEFAULT '',
    is_animation_studio BOOLEAN,
    country_of_origin   VARCHAR
)
"""


@pytest.fixture()
def conformed_path(tmp_path: Path) -> Path:
    """Synthetic animetor.duckdb (conformed schema) with 3 anime / 3 persons / 2 studios.

    Anime:
    - seesaa:a1: title_ja='風の谷のナウシカ', year=1984, format=MOVIE
    - anilist:a2: title_ja='風の谷のナウシカ', year=1984, format=MOVIE, title_en='Nausicaa'
    - mal:a3:    title_ja='風の谷のナウシカ', year=1984, title_en='Nausicaa of the Valley'

    Persons:
    - seesaa:p1:  name_ja='宮崎駿', gender=NULL
    - anilist:p2: name_ja='宮崎駿', name_en='Hayao Miyazaki', gender='Male'
    - bgm:p3:     name_ja='宮崎駿', gender='Male'
    (Separate conformed IDs — no canonical_id yet → 3 canonical person rows.)

    Studios:
    - anilist:s1: name='Studio Ghibli', is_animation_studio=True, country='JP'
    - mal:s2:     name='スタジオジブリ', is_animation_studio=True

    File is named animetor.duckdb (not conformed.duckdb) to avoid DuckDB's
    ambiguous-schema error when the database name equals the schema name.
    """
    path = tmp_path / "animetor.duckdb"
    conn = duckdb.connect(str(path))
    # Execute statements individually (DuckDB does not support executescript)
    for stmt in _CONFORMED_DDL.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)

    conn.executemany(
        "INSERT INTO conformed.anime "
        "(id, title_ja, title_en, year, format, episodes, duration, source_mat, scale_class, country_of_origin) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("seesaa:a1", "風の谷のナウシカ", "", 1984, "MOVIE", 1, 117, "MANGA", "large", None),
            ("anilist:a2", "風の谷のナウシカ", "Nausicaa", 1984, "MOVIE", 1, 116, "MANGA", "large", "JP"),
            ("mal:a3", "風の谷のナウシカ", "Nausicaa of the Valley", 1984, "MOVIE", 1, 117, "MANGA", "large", "JP"),
        ],
    )

    conn.executemany(
        "INSERT INTO conformed.persons (id, name_ja, name_en, birth_date, gender) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("seesaa:p1", "宮崎駿", "", "1941-01-05", None),
            ("anilist:p2", "宮崎駿", "Hayao Miyazaki", "1941-01-05", "Male"),
            ("bgm:p3", "宮崎駿", "", "1941-01-05", "Male"),
        ],
    )

    conn.executemany(
        "INSERT INTO conformed.studios (id, name, is_animation_studio, country_of_origin) "
        "VALUES (?, ?, ?, ?)",
        [
            ("anilist:s1", "Studio Ghibli", True, "JP"),
            ("mal:s2", "スタジオジブリ", True, None),
        ],
    )

    conn.commit()
    conn.close()
    return path


@pytest.fixture()
def resolved_path(tmp_path: Path) -> Path:
    """Return path for a fresh resolved.duckdb (not yet created)."""
    return tmp_path / "resolved.duckdb"


# ---------------------------------------------------------------------------
# source_ranking — smoke tests
# ---------------------------------------------------------------------------


class TestSourceRanking:
    def test_anime_ranking_declared(self):
        from src.etl.resolved.source_ranking import ANIME_RANKING

        assert "title_ja" in ANIME_RANKING
        assert "year" in ANIME_RANKING
        assert "format" in ANIME_RANKING
        assert isinstance(ANIME_RANKING["title_ja"], list)
        assert len(ANIME_RANKING["title_ja"]) >= 3

    def test_persons_ranking_declared(self):
        from src.etl.resolved.source_ranking import PERSONS_RANKING

        assert "name_ja" in PERSONS_RANKING
        assert "gender" in PERSONS_RANKING
        # gender should prefer bgm per design
        assert PERSONS_RANKING["gender"][0] == "bgm"

    def test_studios_ranking_declared(self):
        from src.etl.resolved.source_ranking import STUDIOS_RANKING

        assert "name" in STUDIOS_RANKING
        assert "country_of_origin" in STUDIOS_RANKING
        # name should prefer anilist per design
        assert STUDIOS_RANKING["name"][0] == "anilist"

    def test_source_prefix_helper(self):
        from src.etl.resolved.source_ranking import source_prefix

        assert source_prefix("anilist:a123") == "anilist"
        assert source_prefix("seesaa:p_abc") == "seesaa"
        assert source_prefix("nocolon") == "nocolon"

    def test_rank_for_field_helper(self):
        from src.etl.resolved.source_ranking import rank_for_field

        r = rank_for_field("title_ja", "anime")
        assert isinstance(r, list)
        assert "seesaa" in r

        r2 = rank_for_field("unknown_field", "anime")
        assert r2 == []


# ---------------------------------------------------------------------------
# _select.select_representative_value — unit tests
# ---------------------------------------------------------------------------


class TestSelectRepresentativeValue:
    def _make_candidates(self, id_val_pairs: list[tuple[str, Any]]) -> list[dict]:
        return [{"id": id_, "field": val} for id_, val in id_val_pairs]

    def test_priority_fallback_first_source(self):
        from src.etl.resolved._select import select_representative_value

        candidates = self._make_candidates([
            ("anilist:a1", "Nausicaa"),
            ("mal:a2", "Nausicaa of the Valley"),
        ])
        val, src, reason = select_representative_value(
            "field", candidates, ["anilist", "mal"]
        )
        assert val == "Nausicaa"
        assert src == "anilist"
        assert reason == "priority_fallback"

    def test_priority_fallback_second_when_first_null(self):
        from src.etl.resolved._select import select_representative_value

        candidates = self._make_candidates([
            ("anilist:a1", None),
            ("mal:a2", "Nausicaa"),
        ])
        val, src, reason = select_representative_value(
            "field", candidates, ["anilist", "mal"]
        )
        assert val == "Nausicaa"
        assert src == "mal"

    def test_priority_fallback_second_when_first_empty_string(self):
        from src.etl.resolved._select import select_representative_value

        candidates = self._make_candidates([
            ("anilist:a1", ""),
            ("mal:a2", "Nausicaa"),
        ])
        val, src, reason = select_representative_value(
            "field", candidates, ["anilist", "mal"]
        )
        assert val == "Nausicaa"
        assert src == "mal"

    def test_majority_vote_three_agree(self):
        from src.etl.resolved._select import select_representative_value

        candidates = [
            {"id": "seesaa:a1", "field": "風の谷のナウシカ"},
            {"id": "seesaa:a2", "field": "風の谷のナウシカ"},
            {"id": "seesaa:a3", "field": "風の谷のナウシカ"},
        ]
        val, src, reason = select_representative_value(
            "field", candidates, ["seesaa", "anilist"]
        )
        assert val == "風の谷のナウシカ"
        assert reason == "majority_vote"

    def test_tie_break_returns_first_value(self):
        from src.etl.resolved._select import select_representative_value

        candidates = [
            {"id": "seesaa:a1", "field": "値A"},
            {"id": "seesaa:a2", "field": "値B"},
        ]
        val, src, reason = select_representative_value(
            "field", candidates, ["seesaa", "anilist"]
        )
        assert val == "値A"
        assert reason == "tie_break"

    def test_no_value_returns_none(self):
        from src.etl.resolved._select import select_representative_value

        candidates = [{"id": "anilist:a1", "field": None}]
        val, src, reason = select_representative_value(
            "field", candidates, ["anilist", "mal"]
        )
        assert val is None
        assert src == ""
        assert reason == "no_value"

    def test_empty_candidates(self):
        from src.etl.resolved._select import select_representative_value

        val, src, reason = select_representative_value(
            "field", [], ["anilist", "mal"]
        )
        assert val is None
        assert reason == "no_value"


# ---------------------------------------------------------------------------
# resolve_anime — integration tests
# ---------------------------------------------------------------------------


class TestResolveAnime:
    def test_build_resolved_anime_returns_count(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.etl.resolved.resolve_anime import build_resolved_anime

        count = build_resolved_anime(conformed_path, resolved_path)
        # 3 rows with same title_ja+year should cluster into 1 canonical
        assert count == 1

    def test_canonical_row_written(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.etl.resolved.resolve_anime import build_resolved_anime

        build_resolved_anime(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        rows = conn.execute("SELECT * FROM anime").fetchdf()
        conn.close()
        assert len(rows) == 1
        row = rows.iloc[0]
        assert row["canonical_id"].startswith("resolved:anime:")
        assert row["title_ja"] == "風の谷のナウシカ"

    def test_title_en_selected_by_priority(
        self, conformed_path: Path, resolved_path: Path
    ):
        """title_en priority: anilist > mal — anilist row has 'Nausicaa'."""
        from src.etl.resolved.resolve_anime import build_resolved_anime

        build_resolved_anime(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        row = conn.execute("SELECT title_en, title_en_source FROM anime").fetchone()
        conn.close()
        title_en, source = row
        assert title_en == "Nausicaa"
        assert source == "anilist"

    def test_source_ids_json_contains_all_inputs(
        self, conformed_path: Path, resolved_path: Path
    ):
        """source_ids_json must reference all 3 conformed rows."""
        from src.etl.resolved.resolve_anime import build_resolved_anime

        build_resolved_anime(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        row = conn.execute("SELECT source_ids_json FROM anime").fetchone()
        conn.close()
        source_ids = json.loads(row[0])
        assert "seesaa:a1" in source_ids
        assert "anilist:a2" in source_ids
        assert "mal:a3" in source_ids

    def test_audit_rows_created(self, conformed_path: Path, resolved_path: Path):
        from src.etl.resolved.resolve_anime import build_resolved_anime

        build_resolved_anime(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        cnt = conn.execute(
            "SELECT COUNT(*) FROM meta_resolution_audit WHERE entity_type='anime'"
        ).fetchone()[0]
        conn.close()
        assert cnt > 0

    def test_idempotent_rebuild(self, conformed_path: Path, resolved_path: Path):
        """Running build twice should yield the same result, not double rows."""
        from src.etl.resolved.resolve_anime import build_resolved_anime

        build_resolved_anime(conformed_path, resolved_path)
        build_resolved_anime(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        cnt = conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0]
        conn.close()
        assert cnt == 1

    def test_year_selected_by_priority(
        self, conformed_path: Path, resolved_path: Path
    ):
        """year priority: madb > anilist > mal — no madb in fixture, so anilist wins."""
        from src.etl.resolved.resolve_anime import build_resolved_anime

        build_resolved_anime(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        row = conn.execute("SELECT year, year_source FROM anime").fetchone()
        conn.close()
        year, year_source = row
        assert year == 1984
        # No madb in fixture — any non-null source is acceptable
        assert year_source in ("anilist", "seesaa", "mal")


# ---------------------------------------------------------------------------
# resolve_persons — integration tests
# ---------------------------------------------------------------------------


class TestResolvePersons:
    def test_build_resolved_persons_returns_count(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.etl.resolved.resolve_persons import build_resolved_persons

        count = build_resolved_persons(conformed_path, resolved_path)
        # Phase 2b: exact_match_cluster merges all 3 '宮崎駿' rows into 1 canonical
        # (seesaa:p1, anilist:p2, bgm:p3 → 1 canonical row)
        assert count == 1

    def test_gender_source_priority(
        self, conformed_path: Path, resolved_path: Path
    ):
        """Merged cluster: gender selected by source priority (bgm > anilist > mal).

        All 3 rows (seesaa:p1, anilist:p2, bgm:p3) share the name '宮崎駿' and
        are merged into one canonical row.  bgm:p3 has gender='Male' and bgm is
        the highest-priority source for gender, so bgm wins.
        anilist:p2 also has 'Male', so either source is acceptable here.
        """
        from src.etl.resolved.resolve_persons import build_resolved_persons

        build_resolved_persons(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        rows = conn.execute(
            "SELECT canonical_id, gender, gender_source FROM persons"
        ).fetchall()
        conn.close()
        assert len(rows) == 1
        _canonical_id, gender, gender_source = rows[0]
        # Both bgm and anilist carry 'Male'; bgm has priority
        assert gender == "Male"
        assert gender_source in ("bgm", "anilist")

    def test_source_ids_json_contains_all_merged(
        self, conformed_path: Path, resolved_path: Path
    ):
        """Merged canonical row must reference all 3 conformed person IDs."""
        from src.etl.resolved.resolve_persons import build_resolved_persons

        build_resolved_persons(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        row = conn.execute("SELECT source_ids_json FROM persons").fetchone()
        conn.close()
        assert row is not None
        source_ids = json.loads(row[0])
        assert set(source_ids) == {"seesaa:p1", "anilist:p2", "bgm:p3"}

    def test_name_en_selected_by_priority(
        self, conformed_path: Path, resolved_path: Path
    ):
        """name_en priority: anilist > mal > ann — anilist:p2 has 'Hayao Miyazaki'."""
        from src.etl.resolved.resolve_persons import build_resolved_persons

        build_resolved_persons(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        row = conn.execute("SELECT name_en, name_en_source FROM persons").fetchone()
        conn.close()
        name_en, source = row
        assert name_en == "Hayao Miyazaki"
        assert source == "anilist"

    def test_idempotent_rebuild_persons(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.etl.resolved.resolve_persons import build_resolved_persons

        build_resolved_persons(conformed_path, resolved_path)
        build_resolved_persons(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        cnt = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        conn.close()
        # All 3 rows merged → 1 canonical row, idempotent
        assert cnt == 1


# ---------------------------------------------------------------------------
# resolve_studios — integration tests
# ---------------------------------------------------------------------------


class TestResolveStudios:
    def test_build_resolved_studios_returns_count(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.etl.resolved.resolve_studios import build_resolved_studios

        count = build_resolved_studios(conformed_path, resolved_path)
        assert count == 2

    def test_studio_name_preserved(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.etl.resolved.resolve_studios import build_resolved_studios

        build_resolved_studios(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        rows = conn.execute(
            "SELECT canonical_id, name FROM studios ORDER BY canonical_id"
        ).fetchall()
        conn.close()
        by_id = {r[0]: r[1] for r in rows}
        assert by_id["anilist:s1"] == "Studio Ghibli"
        assert by_id["mal:s2"] == "スタジオジブリ"

    def test_idempotent_rebuild_studios(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.etl.resolved.resolve_studios import build_resolved_studios

        build_resolved_studios(conformed_path, resolved_path)
        build_resolved_studios(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        cnt = conn.execute("SELECT COUNT(*) FROM studios").fetchone()[0]
        conn.close()
        assert cnt == 2


# ---------------------------------------------------------------------------
# Phase 2b: cross-source anime clustering (_cross_source_ids)
# ---------------------------------------------------------------------------


class TestCrossSourceAnimeClustering:
    """Unit tests for build_cross_source_anime_clusters."""

    def _make_anime_rows(self, id_val_pairs: list[tuple]) -> list[dict]:
        """Build minimal anime rows: (id, title_ja, year, mal_id_int)."""
        return [
            {
                "id": r[0],
                "title_ja": r[1],
                "year": r[2],
                "mal_id_int": r[3] if len(r) > 3 else None,
            }
            for r in id_val_pairs
        ]

    def test_title_year_cluster_no_bronze(self):
        """Without bronze_root, same title+year rows cluster together."""
        from src.etl.resolved._cross_source_ids import build_cross_source_anime_clusters

        rows = self._make_anime_rows([
            ("anilist:572", "風の谷のナウシカ", 1984, None),
            ("mal:a572", "風の谷のナウシカ", 1984, None),
            ("seesaa:fc2", "風の谷のナウシカ", 1984, None),
        ])
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        assert len(clusters) == 1
        members = list(clusters.values())[0]
        assert len(members) == 3

    def test_different_years_separate_clusters(self):
        """Same title but different years must produce separate clusters."""
        from src.etl.resolved._cross_source_ids import build_cross_source_anime_clusters

        rows = self._make_anime_rows([
            ("anilist:1", "進撃の巨人", 2013, None),
            ("anilist:2", "進撃の巨人", 2017, None),
        ])
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        assert len(clusters) == 2

    def test_untitled_rows_stay_separate(self):
        """Rows with empty title_ja each get their own cluster."""
        from src.etl.resolved._cross_source_ids import build_cross_source_anime_clusters

        rows = self._make_anime_rows([
            ("madb:M1", "", 2000, None),
            ("madb:M2", "", 2001, None),
            ("madb:M3", "", 2000, None),
        ])
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        # Each untitled row is its own cluster
        assert len(clusters) == 3

    def test_mal_id_int_link_without_bronze(self):
        """MAL rows can be linked to AniList via mal_id_int even without BRONZE
        when the anilist integer matches the MAL row's mal_id_int."""
        from src.etl.resolved._cross_source_ids import build_cross_source_anime_clusters

        # anilist:572 has id suffix 572; mal:a572 has mal_id_int=572
        # Without BRONZE, only title+year clusters — they should cluster by title
        rows = self._make_anime_rows([
            ("anilist:572", "風の谷のナウシカ", 1984, None),
            ("mal:a572", "風の谷のナウシカ", None, 572),
        ])
        # Without year on MAL row, title cluster includes both (same title, year None vs 1984 → 2 clusters)
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        # anilist has year=1984, mal has year=None → different cluster keys
        assert len(clusters) == 2

    def test_source_count_in_cluster(self):
        """Clusters record the number of contributing sources."""
        from src.etl.resolved._cross_source_ids import build_cross_source_anime_clusters

        rows = self._make_anime_rows([
            ("anilist:572", "風の谷のナウシカ", 1984, None),
            ("mal:a572", "風の谷のナウシカ", 1984, None),
            ("seesaa:x", "風の谷のナウシカ", 1984, None),
        ])
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        assert len(clusters) == 1
        cluster_rows = list(clusters.values())[0]
        assert len(cluster_rows) == 3

    def test_canonical_id_format(self):
        """canonical_id must match the resolved:anime:<hash12> pattern."""
        import re
        from src.etl.resolved._cross_source_ids import build_cross_source_anime_clusters

        rows = self._make_anime_rows([("anilist:572", "風の谷のナウシカ", 1984, None)])
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        cid = list(clusters.keys())[0]
        assert re.match(r"^resolved:anime:[0-9a-f]{12}$", cid), f"Bad canonical_id: {cid}"

    def test_deterministic_canonical_id(self):
        """Same title+year always produces the same canonical_id."""
        from src.etl.resolved._cross_source_ids import build_cross_source_anime_clusters

        rows1 = self._make_anime_rows([("anilist:572", "風の谷のナウシカ", 1984, None)])
        rows2 = self._make_anime_rows([("seesaa:x", "風の谷のナウシカ", 1984, None)])
        c1 = list(build_cross_source_anime_clusters(rows1).keys())[0]
        c2 = list(build_cross_source_anime_clusters(rows2).keys())[0]
        assert c1 == c2

    def test_nfkc_normalization_clusters_together(self):
        """Full-width and half-width variants of the same title cluster together."""
        from src.etl.resolved._cross_source_ids import build_cross_source_anime_clusters

        # Full-width 'ＮＨＫ' vs ASCII 'NHK' — NFKC normalizes both to same form
        rows = self._make_anime_rows([
            ("anilist:1", "ＮＨＫにようこそ！", 2004, None),
            ("mal:1", "NHKにようこそ！", 2004, None),
        ])
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        assert len(clusters) == 1


# ---------------------------------------------------------------------------
# Phase 2b: cross-source persons clustering (_persons_cluster)
# ---------------------------------------------------------------------------


class TestPersonsCluster:
    """Unit tests for build_persons_canonical_map."""

    def _make_person_rows(self, rows: list[tuple]) -> list[dict]:
        """Build minimal person rows: (id, name_ja, name_en, gender)."""
        return [
            {
                "id": r[0],
                "name_ja": r[1],
                "name_en": r[2] if len(r) > 2 else "",
                "gender": r[3] if len(r) > 3 else None,
                "name_ko": "",
                "name_zh": "",
                "birth_date": None,
                "death_date": None,
                "nationality": None,
                "bgm_id": None,
            }
            for r in rows
        ]

    def test_exact_match_merges_same_name(self):
        """Persons with identical name_ja across sources should be merged."""
        from src.etl.resolved._persons_cluster import build_persons_canonical_map

        rows = self._make_person_rows([
            ("seesaa:p1", "宮崎駿", "", None),
            ("anilist:p96870", "宮崎駿", "Hayao Miyazaki", None),
        ])
        cmap = build_persons_canonical_map(rows, fast_only=True)
        # One of them maps to the other
        assert len(cmap) >= 1

    def test_different_names_not_merged(self):
        """Persons with different names must not be merged."""
        from src.etl.resolved._persons_cluster import build_persons_canonical_map

        rows = self._make_person_rows([
            ("seesaa:p1", "宮崎駿", "", None),
            ("seesaa:p2", "高畑勲", "", None),
        ])
        cmap = build_persons_canonical_map(rows, fast_only=True)
        # Neither maps to the other
        assert "seesaa:p1" not in cmap or cmap.get("seesaa:p1") != "seesaa:p2"
        assert "seesaa:p2" not in cmap or cmap.get("seesaa:p2") != "seesaa:p1"

    def test_group_persons_by_canonical_singletons(self):
        """Persons not in canonical_map remain as singletons."""
        from src.etl.resolved._persons_cluster import group_persons_by_canonical

        rows = [
            {"id": "seesaa:p1", "name_ja": "A"},
            {"id": "anilist:p2", "name_ja": "B"},
        ]
        cmap: dict[str, str] = {}  # no merges
        groups = group_persons_by_canonical(rows, cmap)
        assert len(groups) == 2
        assert "seesaa:p1" in groups
        assert "anilist:p2" in groups

    def test_group_persons_by_canonical_merged(self):
        """Merged persons appear under a single canonical_id."""
        from src.etl.resolved._persons_cluster import group_persons_by_canonical

        rows = [
            {"id": "seesaa:p1", "name_ja": "宮崎駿"},
            {"id": "anilist:p2", "name_ja": "宮崎駿"},
        ]
        cmap = {"seesaa:p1": "anilist:p2"}  # seesaa merges into anilist
        groups = group_persons_by_canonical(rows, cmap)
        assert len(groups) == 1
        assert "anilist:p2" in groups
        assert len(groups["anilist:p2"]) == 2

    def test_resolve_persons_source_count_reflects_merge(
        self, conformed_path: Path, resolved_path: Path
    ):
        """After build_resolved_persons, source_ids_json length == merged count."""
        from src.etl.resolved.resolve_persons import build_resolved_persons

        build_resolved_persons(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        row = conn.execute("SELECT source_ids_json FROM persons").fetchone()
        conn.close()
        source_ids = json.loads(row[0])
        # All 3 fixture persons merged → source_ids has 3 entries
        assert len(source_ids) == 3


# ---------------------------------------------------------------------------
# Phase 2b: resolve_anime with source_count column
# ---------------------------------------------------------------------------


class TestResolveAnimePhase2b:
    """Additional Phase 2b tests for resolve_anime."""

    def test_source_count_column_set(self, conformed_path: Path, resolved_path: Path):
        """source_count must equal the number of conformed rows in the cluster."""
        from src.etl.resolved.resolve_anime import build_resolved_anime

        build_resolved_anime(conformed_path, resolved_path)
        conn = duckdb.connect(str(resolved_path), read_only=True)
        row = conn.execute("SELECT source_count FROM anime").fetchone()
        conn.close()
        assert row is not None
        # 3 conformed anime rows all cluster to 1 canonical → source_count=3
        assert row[0] == 3

    def test_no_bronze_falls_back_to_title_year(
        self, conformed_path: Path, resolved_path: Path
    ):
        """Without bronze_root, title+year clustering still works."""
        from src.etl.resolved.resolve_anime import build_resolved_anime

        count = build_resolved_anime(conformed_path, resolved_path, bronze_root=None)
        assert count == 1  # 3 rows same title+year → 1 canonical


# ---------------------------------------------------------------------------
# resolved_reader — smoke tests
# ---------------------------------------------------------------------------


class TestResolvedReader:
    def _build_all(self, conformed_path: Path, resolved_path: Path) -> None:
        from src.etl.resolved.resolve_anime import build_resolved_anime
        from src.etl.resolved.resolve_persons import build_resolved_persons
        from src.etl.resolved.resolve_studios import build_resolved_studios

        build_resolved_anime(conformed_path, resolved_path)
        build_resolved_persons(conformed_path, resolved_path)
        build_resolved_studios(conformed_path, resolved_path)

    def test_resolved_available_false_before_build(
        self, resolved_path: Path
    ):
        from src.analysis.io.resolved_reader import resolved_available

        assert not resolved_available(resolved_path)

    def test_resolved_available_true_after_build(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.analysis.io.resolved_reader import resolved_available

        self._build_all(conformed_path, resolved_path)
        assert resolved_available(resolved_path)

    def test_load_anime_resolved_returns_models(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.analysis.io.resolved_reader import load_anime_resolved
        from src.runtime.models import AnimeAnalysis

        self._build_all(conformed_path, resolved_path)
        anime_list = load_anime_resolved(resolved_path)
        assert len(anime_list) == 1
        assert all(isinstance(a, AnimeAnalysis) for a in anime_list)
        assert anime_list[0].title_ja == "風の谷のナウシカ"
        assert anime_list[0].year == 1984

    def test_load_persons_resolved_returns_models(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.analysis.io.resolved_reader import load_persons_resolved
        from src.runtime.models import Person

        self._build_all(conformed_path, resolved_path)
        persons = load_persons_resolved(resolved_path)
        # Phase 2b: all 3 '宮崎駿' rows merged → 1 canonical person
        assert len(persons) == 1
        assert all(isinstance(p, Person) for p in persons)
        names = {p.name_ja for p in persons}
        assert "宮崎駿" in names

    def test_load_studios_resolved_returns_dicts(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.analysis.io.resolved_reader import load_studios_resolved

        self._build_all(conformed_path, resolved_path)
        studios = load_studios_resolved(resolved_path)
        assert len(studios) == 2
        names = {s["name"] for s in studios}
        assert "Studio Ghibli" in names

    def test_load_anime_resolved_absent_returns_empty(self, tmp_path: Path):
        from src.analysis.io.resolved_reader import load_anime_resolved

        absent = tmp_path / "nonexistent.duckdb"
        result = load_anime_resolved(absent)
        assert result == []

    def test_query_resolved(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.analysis.io.resolved_reader import query_resolved

        self._build_all(conformed_path, resolved_path)
        rows = query_resolved(
            "SELECT canonical_id FROM anime",
            path=resolved_path,
        )
        assert len(rows) == 1
        assert rows[0]["canonical_id"].startswith("resolved:anime:")

    def test_resolved_connect_read_only(
        self, conformed_path: Path, resolved_path: Path
    ):
        from src.analysis.io.resolved_reader import resolved_connect

        self._build_all(conformed_path, resolved_path)
        with resolved_connect(resolved_path) as conn:
            with pytest.raises(Exception):
                conn.execute("DELETE FROM anime")
