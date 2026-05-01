"""Tests for src/etl/cluster/series_cluster.py.

Uses in-memory DuckDB fixtures; no real silver.duckdb required.

Coverage
--------
- _is_chain_relation: keyword matching across mixed-case variants
- UnionFind: find / union / path compression
- compute_clusters: isolated nodes, chain A-B-C, independent pair D-E,
  dangling related_id (foreign anime not in anime table)
- backfill: idempotency, column creation, row count
- H1: no anime.score / display_* referenced
"""

from __future__ import annotations

import duckdb

from src.etl.cluster.series_cluster import (
    _UnionFind,
    _is_chain_relation,
    backfill,
    compute_clusters,
)

# ---------------------------------------------------------------------------
# Minimal DDL mirroring production SILVER columns used by the ETL
# ---------------------------------------------------------------------------

_DDL_ANIME = """
CREATE TABLE IF NOT EXISTS anime (
    id                VARCHAR PRIMARY KEY,
    title_ja          VARCHAR NOT NULL DEFAULT '',
    series_cluster_id VARCHAR
);
"""

_DDL_ANIME_RELATIONS = """
CREATE TABLE IF NOT EXISTS anime_relations (
    anime_id         VARCHAR NOT NULL,
    related_anime_id VARCHAR NOT NULL,
    relation_type    VARCHAR NOT NULL DEFAULT ''
);
"""


def _make_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL_ANIME)
    conn.execute(_DDL_ANIME_RELATIONS)
    return conn


# ---------------------------------------------------------------------------
# _is_chain_relation
# ---------------------------------------------------------------------------


class TestIsChainRelation:
    def test_sequel_exact(self):
        assert _is_chain_relation("sequel") is True

    def test_sequel_capitalised(self):
        assert _is_chain_relation("Sequel") is True

    def test_sequel_of(self):
        assert _is_chain_relation("sequel of") is True

    def test_prequel_exact(self):
        assert _is_chain_relation("prequel") is True

    def test_prequel_of(self):
        assert _is_chain_relation("prequel of") is True

    def test_parent_story(self):
        assert _is_chain_relation("Parent Story") is True

    def test_side_story(self):
        assert _is_chain_relation("Side Story") is True

    def test_side_story_of(self):
        assert _is_chain_relation("side story of") is True

    def test_summary(self):
        assert _is_chain_relation("Summary") is True

    def test_alternative_version(self):
        assert _is_chain_relation("Alternative Version") is True

    def test_alternative_setting(self):
        assert _is_chain_relation("Alternative Setting") is True

    def test_full_story(self):
        assert _is_chain_relation("Full Story") is True

    def test_spinoff_excluded(self):
        assert _is_chain_relation("Spin-Off") is False

    def test_spinoff_lower_excluded(self):
        assert _is_chain_relation("spinoff") is False

    def test_character_excluded(self):
        assert _is_chain_relation("Character") is False

    def test_other_excluded(self):
        assert _is_chain_relation("Other") is False

    def test_related_excluded(self):
        assert _is_chain_relation("related") is False

    def test_adaptation_excluded(self):
        assert _is_chain_relation("adaptation") is False

    def test_empty_excluded(self):
        assert _is_chain_relation("") is False


# ---------------------------------------------------------------------------
# _UnionFind
# ---------------------------------------------------------------------------


class TestUnionFind:
    def test_single_node_is_own_root(self):
        uf = _UnionFind()
        uf.ensure("a1")
        assert uf.find("a1") == "a1"

    def test_union_merges_two_nodes(self):
        uf = _UnionFind()
        uf.ensure("a1")
        uf.ensure("a2")
        uf.union("a1", "a2")
        assert uf.find("a1") == uf.find("a2")

    def test_union_idempotent(self):
        uf = _UnionFind()
        uf.ensure("a1")
        uf.ensure("a2")
        uf.union("a1", "a2")
        uf.union("a1", "a2")
        assert uf.find("a1") == uf.find("a2")

    def test_union_transitive(self):
        uf = _UnionFind()
        for x in ("a1", "a2", "a3"):
            uf.ensure(x)
        uf.union("a1", "a2")
        uf.union("a2", "a3")
        assert uf.find("a1") == uf.find("a2") == uf.find("a3")

    def test_union_lex_min_root(self):
        """Smaller id should become the root."""
        uf = _UnionFind()
        uf.ensure("b")
        uf.ensure("a")
        uf.union("b", "a")
        assert uf.find("b") == "a"
        assert uf.find("a") == "a"

    def test_independent_components_stay_separate(self):
        uf = _UnionFind()
        for x in ("a1", "a2", "b1", "b2"):
            uf.ensure(x)
        uf.union("a1", "a2")
        uf.union("b1", "b2")
        # a and b components must not merge
        assert uf.find("a1") != uf.find("b1")


# ---------------------------------------------------------------------------
# compute_clusters
# ---------------------------------------------------------------------------


class TestComputeClusters:
    def _setup(self, conn, anime_ids, relations):
        """Insert anime rows and relation rows into in-memory DB."""
        for aid in anime_ids:
            conn.execute(
                "INSERT INTO anime(id, title_ja) VALUES (?, '')", [aid]
            )
        for anime_id, related_id, rel_type in relations:
            conn.execute(
                "INSERT INTO anime_relations VALUES (?, ?, ?)",
                [anime_id, related_id, rel_type],
            )

    def test_isolated_anime_self_cluster(self):
        conn = _make_conn()
        self._setup(conn, ["a1", "a2", "a3"], [])
        clusters = compute_clusters(conn)
        assert clusters == {"a1": "a1", "a2": "a2", "a3": "a3"}

    def test_chain_a_b_c(self):
        """A-B SEQUEL, B-C SEQUEL → all in one cluster with lex-min id."""
        conn = _make_conn()
        self._setup(
            conn,
            ["a1", "a2", "a3"],
            [
                ("a1", "a2", "Sequel"),
                ("a2", "a3", "Sequel"),
            ],
        )
        clusters = compute_clusters(conn)
        assert clusters["a1"] == clusters["a2"] == clusters["a3"]
        # lex-min is "a1"
        assert clusters["a1"] == "a1"

    def test_independent_pairs(self):
        """a1-a2 and a3-a4 are independent; a5 isolated."""
        conn = _make_conn()
        self._setup(
            conn,
            ["a1", "a2", "a3", "a4", "a5"],
            [
                ("a1", "a2", "Sequel"),
                ("a3", "a4", "Prequel"),
            ],
        )
        clusters = compute_clusters(conn)
        assert clusters["a1"] == clusters["a2"]
        assert clusters["a3"] == clusters["a4"]
        assert clusters["a5"] == "a5"
        # The two pairs must be in different clusters
        assert clusters["a1"] != clusters["a3"]

    def test_spinoff_does_not_merge(self):
        conn = _make_conn()
        self._setup(
            conn,
            ["a1", "a2"],
            [("a1", "a2", "Spin-Off")],
        )
        clusters = compute_clusters(conn)
        assert clusters["a1"] == "a1"
        assert clusters["a2"] == "a2"

    def test_dangling_related_id_ignored(self):
        """related_anime_id not in anime table must not cause errors."""
        conn = _make_conn()
        self._setup(
            conn,
            ["a1"],
            [("a1", "NONEXISTENT_99999", "Sequel")],
        )
        clusters = compute_clusters(conn)
        # a1 is isolated — dangling ref should not merge
        assert clusters["a1"] == "a1"

    def test_empty_anime_table(self):
        conn = _make_conn()
        clusters = compute_clusters(conn)
        assert clusters == {}

    def test_alternative_version_merges(self):
        conn = _make_conn()
        self._setup(
            conn,
            ["a1", "a2"],
            [("a1", "a2", "Alternative Version")],
        )
        clusters = compute_clusters(conn)
        assert clusters["a1"] == clusters["a2"] == "a1"

    def test_full_story_merges(self):
        conn = _make_conn()
        self._setup(
            conn,
            ["a1", "a2"],
            [("a1", "a2", "Full Story")],
        )
        clusters = compute_clusters(conn)
        assert clusters["a1"] == clusters["a2"] == "a1"

    def test_summary_merges(self):
        conn = _make_conn()
        self._setup(
            conn,
            ["a1", "a2"],
            [("a1", "a2", "Summary")],
        )
        clusters = compute_clusters(conn)
        assert clusters["a1"] == clusters["a2"] == "a1"

    def test_parent_story_merges(self):
        conn = _make_conn()
        self._setup(
            conn,
            ["a1", "a2"],
            [("a1", "a2", "Parent Story")],
        )
        clusters = compute_clusters(conn)
        assert clusters["a1"] == clusters["a2"] == "a1"

    def test_cluster_id_is_lex_min(self):
        """Cluster id must always be the lexicographically smallest member."""
        conn = _make_conn()
        # Insert in reverse lex order to stress the lex-min selection
        self._setup(
            conn,
            ["c3", "b2", "a1"],
            [("c3", "b2", "Sequel"), ("b2", "a1", "Sequel")],
        )
        clusters = compute_clusters(conn)
        assert clusters["c3"] == clusters["b2"] == clusters["a1"] == "a1"


# ---------------------------------------------------------------------------
# backfill
# ---------------------------------------------------------------------------


class TestBackfill:
    def _make_conn_with_data(self, anime_ids, relations):
        conn = _make_conn()
        for aid in anime_ids:
            conn.execute(
                "INSERT INTO anime(id, title_ja) VALUES (?, '')", [aid]
            )
        for anime_id, related_id, rel_type in relations:
            conn.execute(
                "INSERT INTO anime_relations VALUES (?, ?, ?)",
                [anime_id, related_id, rel_type],
            )
        return conn

    def test_backfill_returns_row_count(self):
        conn = self._make_conn_with_data(
            ["a1", "a2", "a3"],
            [("a1", "a2", "Sequel")],
        )
        updated = backfill(conn)
        assert updated == 3

    def test_backfill_sets_cluster_id(self):
        conn = self._make_conn_with_data(
            ["a1", "a2", "a3"],
            [("a1", "a2", "Sequel")],
        )
        backfill(conn)
        rows = conn.execute(
            "SELECT id, series_cluster_id FROM anime ORDER BY id"
        ).fetchall()
        cluster_map = {r[0]: r[1] for r in rows}
        assert cluster_map["a1"] == cluster_map["a2"] == "a1"
        assert cluster_map["a3"] == "a3"

    def test_backfill_idempotent(self):
        conn = self._make_conn_with_data(
            ["a1", "a2"],
            [("a1", "a2", "Sequel")],
        )
        r1 = backfill(conn)
        r2 = backfill(conn)
        assert r1 == r2 == 2
        rows = conn.execute(
            "SELECT id, series_cluster_id FROM anime ORDER BY id"
        ).fetchall()
        cluster_map = {r[0]: r[1] for r in rows}
        assert cluster_map["a1"] == cluster_map["a2"] == "a1"

    def test_backfill_all_rows_non_null(self):
        """Every anime row must have a non-NULL series_cluster_id after backfill."""
        conn = self._make_conn_with_data(
            ["a1", "a2", "a3", "a4"],
            [("a1", "a2", "Sequel")],
        )
        backfill(conn)
        null_count = conn.execute(
            "SELECT COUNT(*) FROM anime WHERE series_cluster_id IS NULL"
        ).fetchone()[0]
        assert null_count == 0

    def test_backfill_creates_index(self):
        """The idx_anime_series_cluster index must exist after backfill."""
        conn = self._make_conn_with_data(["a1"], [])
        backfill(conn)
        # DuckDB: use duckdb_indexes() system function
        idx_rows = conn.execute(
            "SELECT index_name FROM duckdb_indexes()"
            " WHERE table_name = 'anime' AND index_name = 'idx_anime_series_cluster'"
        ).fetchall()
        assert len(idx_rows) == 1

    def test_no_display_columns_referenced(self):
        """Guard: SQL constants must not reference display_* columns (H1)."""
        from src.etl.cluster import series_cluster as mod

        # Check SQL string constants only (docstring may reference column names
        # in constraint descriptions — that is fine)
        sql_constants = [
            mod._ALTER_SQL,
            mod._INDEX_SQL,
        ]
        for sql in sql_constants:
            assert "display_score" not in sql
            assert "display_mean_score" not in sql
            assert "display_popularity" not in sql
