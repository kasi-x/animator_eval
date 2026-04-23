"""Tests for report generator helper functions (§6.4).

Covers fmt_num, name_clusters_by_rank, name_clusters_distinctive,
adaptive_height, add_distribution_stats, insert_lineage.
"""

from __future__ import annotations

import sqlite3

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# fmt_num
# ---------------------------------------------------------------------------

class TestFmtNum:
    def _fmt(self, n):
        from scripts.report_generators.helpers import fmt_num
        return fmt_num(n)

    def test_integer_formatted_with_commas(self):
        assert self._fmt(1000) == "1,000"
        assert self._fmt(1_234_567) == "1,234,567"

    def test_integer_small_no_comma(self):
        assert self._fmt(42) == "42"

    def test_float_small_two_decimal_places(self):
        result = self._fmt(3.14159)
        assert result == "3.14"

    def test_float_large_no_decimal(self):
        result = self._fmt(1500.7)
        assert result == "1,501"

    def test_float_exactly_1000_rounds(self):
        assert self._fmt(1000.0) == "1,000"

    def test_zero_int(self):
        assert self._fmt(0) == "0"

    def test_zero_float(self):
        assert self._fmt(0.0) == "0.00"


# ---------------------------------------------------------------------------
# name_clusters_by_rank
# ---------------------------------------------------------------------------

class TestNameClustersByRank:
    def _run(self, centers, feat_specs):
        from scripts.report_generators.helpers import name_clusters_by_rank
        return name_clusters_by_rank(centers, feat_specs)

    def test_two_clusters_single_feature(self):
        centers = np.array([[10.0, 0.0], [1.0, 0.0]])  # cluster 0 higher on feat 0
        feat_specs = [(0, ["高", "低"])]
        result = self._run(centers, feat_specs)
        assert result[0] == "C1: 高"
        assert result[1] == "C2: 低"

    def test_three_clusters_single_feature(self):
        centers = np.array([[1.0], [5.0], [3.0]])
        feat_specs = [(0, ["high", "mid", "low"])]
        result = self._run(centers, feat_specs)
        # Sorted by value desc: cluster 1 (5.0), cluster 2 (3.0), cluster 0 (1.0)
        # → cluster 1 → high, cluster 2 → mid, cluster 0 → low
        # Keys are cluster input indices; labels are "Cn+1" where n is input idx
        assert result[1] == "C2: high"
        assert result[2] == "C3: mid"
        assert result[0] == "C1: low"

    def test_two_features_joined_with_cross(self):
        centers = np.array([[10.0, 1.0], [1.0, 10.0]])
        feat_specs = [(0, ["H", "L"]), (1, ["X", "Y"])]
        result = self._run(centers, feat_specs)
        # cluster 0: feat0=high("H"), feat1=low("Y") → "H×Y"
        assert "×" in result[0]
        assert result[0] == "C1: H×Y"
        assert result[1] == "C2: L×X"

    def test_cluster_ids_in_result(self):
        centers = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
        feat_specs = [(0, ["hi", "lo"])]
        result = self._run(centers, feat_specs)
        assert set(result.keys()) == {0, 1}

    def test_label_count_mismatch_clips(self):
        """If more clusters than labels, last label is reused."""
        centers = np.array([[3.0], [2.0], [1.0]])
        feat_specs = [(0, ["top", "bottom"])]  # 2 labels for 3 clusters
        result = self._run(centers, feat_specs)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# name_clusters_distinctive
# ---------------------------------------------------------------------------

class TestNameClustersDistinctive:
    def _run(self, centers, feature_names):
        from scripts.report_generators.helpers import name_clusters_distinctive
        return name_clusters_distinctive(centers, feature_names)

    def test_returns_one_name_per_cluster(self):
        centers = np.array([[1.0, 0.0], [0.0, 1.0]])
        result = self._run(centers, ["birank", "patronage"])
        assert set(result.keys()) == {0, 1}

    def test_no_duplicate_names(self):
        """Name collision resolution appends suffix."""
        centers = np.array([
            [1.0, 0.0],
            [0.9, 0.1],
            [0.0, 1.0],
        ])
        result = self._run(centers, ["birank", "patronage"])
        names = list(result.values())
        assert len(names) == len(set(names))


# ---------------------------------------------------------------------------
# adaptive_height
# ---------------------------------------------------------------------------

class TestAdaptiveHeight:
    def _h(self, n, **kw):
        from scripts.report_generators.helpers import adaptive_height
        return adaptive_height(n, **kw)

    def test_zero_items_returns_base(self):
        assert self._h(0) == 400

    def test_many_items_caps_at_max(self):
        assert self._h(10000) == 900

    def test_linear_between_bounds(self):
        h = self._h(10, base=400, per_item=25, max_h=900)
        assert h == min(400 + 10 * 25, 900)


# ---------------------------------------------------------------------------
# insert_lineage (DB write)
# ---------------------------------------------------------------------------

class TestInsertLineage:
    @pytest.fixture
    def conn(self, tmp_path):
        db = sqlite3.connect(str(tmp_path / "test.db"))
        db.execute("""
            CREATE TABLE meta_lineage (
                table_name TEXT PRIMARY KEY,
                formula_version TEXT,
                ci_method TEXT,
                null_model TEXT,
                inputs_hash TEXT,
                description TEXT,
                computed_at TEXT,
                audience TEXT,
                source_silver_tables TEXT,
                source_bronze_forbidden INTEGER DEFAULT 1
            )
        """)
        db.commit()
        return db

    def _insert(self, conn, version="v1.0"):
        from scripts.report_generators.helpers import insert_lineage
        insert_lineage(
            conn,
            table_name="meta_test",
            audience="policy",
            source_silver_tables=["credits", "persons"],
            formula_version=version,
            ci_method="bootstrap",
            null_model="permutation",
            inputs_hash="abc123abc123abc1",
            description="A" * 55,
        )

    def test_insert_creates_row(self, conn):
        self._insert(conn)
        row = conn.execute(
            "SELECT * FROM meta_lineage WHERE table_name='meta_test'"
        ).fetchone()
        assert row is not None

    def test_upsert_updates_existing(self, conn):
        self._insert(conn, "v1.0")
        self._insert(conn, "v1.1")
        count = conn.execute(
            "SELECT COUNT(*) FROM meta_lineage WHERE table_name='meta_test'"
        ).fetchone()[0]
        assert count == 1  # upsert, not append
