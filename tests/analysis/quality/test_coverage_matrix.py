"""Tests for src/analysis/quality/coverage_matrix.py.

Uses toy data (in-memory DuckDB) — no real resolved.duckdb required.
Follows the pattern in tests/conftest.py: build a minimal DuckDB, then
monkeypatch resolved_available / resolved_connect so the module under test
uses the toy DB instead of the real file.

Test groups:
  1. Unit tests for internal helpers (pure functions, no DB).
  2. Integration tests via compute_coverage_matrix() with toy data.
  3. Edge-case tests (empty DB, missing reference source, single cell).
  4. CoverageMatrix model behaviour (lookup, mean_coverage, is_empty).
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from collections.abc import Iterator
from unittest.mock import patch

import duckdb
import pytest

from src.analysis.quality.coverage_matrix import (
    CoverageCell,
    CoverageMatrix,
    _aggregate_to_role_groups,
    _build_reference_counts,
    _build_snapshot_note,
    _compute_coverage_ratio,
    _identify_under_credited_roles,
    _map_role_to_group,
    compute_coverage_matrix,
    coverage_matrix_to_records,
)


# ---------------------------------------------------------------------------
# Fixtures: toy in-memory DuckDB
# ---------------------------------------------------------------------------


def _create_toy_resolved_db(conn: duckdb.DuckDBPyConnection) -> None:
    """Populate an in-memory DuckDB with minimal credits + anime tables."""
    conn.execute("""
        CREATE TABLE anime (
            canonical_id VARCHAR PRIMARY KEY,
            title_ja     VARCHAR,
            year         INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE credits (
            person_id       VARCHAR,
            anime_id        VARCHAR,
            role            VARCHAR,
            evidence_source VARCHAR
        )
    """)
    # Anime: two years
    conn.execute("""
        INSERT INTO anime VALUES
          ('anime:1', 'テスト作品A', 2020),
          ('anime:2', 'テスト作品B', 2020),
          ('anime:3', 'テスト作品C', 2021)
    """)
    # Credits: two sources (ann = reference, anilist)
    # ann has all role groups; anilist is missing animation (under-credited scenario)
    conn.execute("""
        INSERT INTO credits VALUES
          -- ann 2020: key_animator (animation), director (direction)
          ('p1', 'anime:1', 'key_animator',  'ann'),
          ('p2', 'anime:1', 'key_animator',  'ann'),
          ('p3', 'anime:2', 'key_animator',  'ann'),
          ('p4', 'anime:1', 'director',      'ann'),
          -- anilist 2020: only director, no animation
          ('p4', 'anime:1', 'director',      'anilist'),
          -- ann 2021: key_animator, in_between
          ('p5', 'anime:3', 'key_animator',  'ann'),
          ('p6', 'anime:3', 'in_between',    'ann'),
          -- anilist 2021: key_animator only (1 vs 1 for ann → full coverage)
          ('p5', 'anime:3', 'key_animator',  'anilist')
    """)


@pytest.fixture
def toy_conn() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection pre-populated with toy data."""
    conn = duckdb.connect(":memory:")
    _create_toy_resolved_db(conn)
    return conn


@contextmanager
def _resolved_connect_from_conn(
    conn: duckdb.DuckDBPyConnection,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Context manager yielding an existing connection (no file open/close)."""
    yield conn


@pytest.fixture
def patched_matrix(toy_conn: duckdb.DuckDBPyConnection) -> CoverageMatrix:
    """Compute CoverageMatrix using toy DB via monkeypatching."""
    with (
        patch(
            "src.analysis.quality.coverage_matrix.resolved_available",
            return_value=True,
        ) as _mock_avail,
        patch(
            "src.analysis.quality.coverage_matrix.resolved_connect",
            side_effect=lambda p: _resolved_connect_from_conn(toy_conn),
        ) as _mock_conn,
    ):
        return compute_coverage_matrix(resolved_path=Path(":memory:"))


# ---------------------------------------------------------------------------
# 1. Unit tests — internal helpers (pure functions)
# ---------------------------------------------------------------------------


class TestMapRoleToGroup:
    def test_known_role_returns_category(self):
        assert _map_role_to_group("key_animator") == "animation"

    def test_director_returns_direction(self):
        assert _map_role_to_group("director") == "direction"

    def test_unknown_role_returns_non_production(self):
        assert _map_role_to_group("completely_unknown_xyz") == "non_production"

    def test_in_between_is_animation(self):
        assert _map_role_to_group("in_between") == "animation"

    def test_finishing_is_finishing(self):
        assert _map_role_to_group("finishing") == "finishing"


class TestAggregateToRoleGroups:
    def test_groups_by_role_category(self):
        raw = [
            {"source": "ann", "role": "key_animator", "year": 2020, "n_credits": 5},
            {"source": "ann", "role": "in_between", "year": 2020, "n_credits": 3},
        ]
        result = _aggregate_to_role_groups(raw)
        # Both collapse to "animation"
        assert result[("ann", "animation", 2020)] == 8

    def test_separate_source_separate_entry(self):
        raw = [
            {"source": "ann", "role": "key_animator", "year": 2020, "n_credits": 4},
            {"source": "anilist", "role": "key_animator", "year": 2020, "n_credits": 2},
        ]
        result = _aggregate_to_role_groups(raw)
        assert result[("ann", "animation", 2020)] == 4
        assert result[("anilist", "animation", 2020)] == 2

    def test_separate_year_separate_entry(self):
        raw = [
            {"source": "ann", "role": "director", "year": 2019, "n_credits": 1},
            {"source": "ann", "role": "director", "year": 2020, "n_credits": 2},
        ]
        result = _aggregate_to_role_groups(raw)
        assert result[("ann", "direction", 2019)] == 1
        assert result[("ann", "direction", 2020)] == 2

    def test_empty_input_returns_empty(self):
        assert _aggregate_to_role_groups([]) == {}


class TestBuildReferenceCounts:
    def test_reference_source_used_when_present(self):
        agg = {
            ("ann", "animation", 2020): 10,
            ("anilist", "animation", 2020): 6,
        }
        ref = _build_reference_counts(agg, "ann")
        assert ref[("animation", 2020)] == 10

    def test_fallback_max_when_reference_absent(self):
        agg = {
            ("anilist", "animation", 2020): 6,
            ("mal", "animation", 2020): 4,
        }
        ref = _build_reference_counts(agg, "ann")
        # ann absent → fallback = max(6, 4)
        assert ref[("animation", 2020)] == 6

    def test_reference_source_case_insensitive(self):
        agg = {("ANN", "animation", 2020): 8, ("anilist", "animation", 2020): 3}
        ref = _build_reference_counts(agg, "ann")
        assert ref[("animation", 2020)] == 8


class TestComputeCoverageRatio:
    def test_full_coverage(self):
        assert _compute_coverage_ratio(10, 10) == pytest.approx(1.0)

    def test_partial_coverage(self):
        assert _compute_coverage_ratio(5, 10) == pytest.approx(0.5)

    def test_zero_reference_returns_zero(self):
        assert _compute_coverage_ratio(5, 0) == pytest.approx(0.0)

    def test_exceeding_reference_capped_at_one(self):
        # Can happen in fallback scenarios
        assert _compute_coverage_ratio(12, 10) == pytest.approx(1.0)

    def test_zero_credits_returns_zero(self):
        assert _compute_coverage_ratio(0, 10) == pytest.approx(0.0)


class TestIdentifyUnderCreditedRoles:
    def test_flags_below_threshold(self):
        cells = [
            CoverageCell("ann", "animation", 2020, 10, 10, 1.0),
            CoverageCell("anilist", "animation", 2020, 3, 10, 0.3),
            CoverageCell("ann", "direction", 2020, 2, 2, 1.0),
            CoverageCell("anilist", "direction", 2020, 2, 2, 1.0),
        ]
        under = _identify_under_credited_roles(cells, threshold=0.5)
        # animation mean = (1.0 + 0.3) / 2 = 0.65 → NOT under 0.5
        assert "animation" not in under
        assert "direction" not in under

    def test_flags_when_mean_below_threshold(self):
        cells = [
            CoverageCell("anilist", "finishing", 2020, 1, 10, 0.1),
            CoverageCell("anilist", "finishing", 2021, 2, 10, 0.2),
        ]
        under = _identify_under_credited_roles(cells, threshold=0.5)
        assert "finishing" in under

    def test_empty_cells_returns_empty(self):
        assert _identify_under_credited_roles([], 0.5) == []


class TestBuildSnapshotNote:
    def test_empty_matrix_note_mentions_absent(self):
        m = CoverageMatrix()
        note = _build_snapshot_note(m, "ann")
        assert "resolved.duckdb" in note or "coverage 行列" in note

    def test_non_empty_matrix_note_contains_counts(self):
        cells = [CoverageCell("ann", "animation", 2020, 5, 5, 1.0)]
        m = CoverageMatrix(
            cells=cells,
            sources=["ann"],
            role_groups=["animation"],
            years=[2020],
            under_credited_roles=[],
        )
        note = _build_snapshot_note(m, "ann")
        assert "1 source" in note
        assert "1 role_group" in note
        assert "1 年" in note


# ---------------------------------------------------------------------------
# 2. Integration tests — compute_coverage_matrix() with toy DB
# ---------------------------------------------------------------------------


class TestComputeCoverageMatrixIntegration:
    def test_returns_coverage_matrix_type(self, patched_matrix):
        assert isinstance(patched_matrix, CoverageMatrix)

    def test_not_empty(self, patched_matrix):
        assert not patched_matrix.is_empty()

    def test_sources_include_ann_and_anilist(self, patched_matrix):
        assert "ann" in patched_matrix.sources
        assert "anilist" in patched_matrix.sources

    def test_role_groups_include_animation_and_direction(self, patched_matrix):
        assert "animation" in patched_matrix.role_groups
        assert "direction" in patched_matrix.role_groups

    def test_years_include_2020_and_2021(self, patched_matrix):
        assert 2020 in patched_matrix.years
        assert 2021 in patched_matrix.years

    def test_ann_animation_2020_is_full_coverage(self, patched_matrix):
        """ann is the reference source: ratio must be 1.0 for its own cells."""
        cell = patched_matrix.lookup("ann", "animation", 2020)
        assert cell is not None
        assert cell.coverage_ratio == pytest.approx(1.0)

    def test_anilist_direction_2020_has_full_coverage(self, patched_matrix):
        """anilist has same director count as ann for direction in 2020."""
        cell = patched_matrix.lookup("anilist", "direction", 2020)
        assert cell is not None
        assert cell.coverage_ratio == pytest.approx(1.0)

    def test_anilist_missing_animation_2020_is_under_covered(self, patched_matrix):
        """anilist has 0 animation credits in 2020 — should be absent or ratio=0."""
        cell = patched_matrix.lookup("anilist", "animation", 2020)
        # Either absent (no row) or coverage_ratio == 0
        if cell is not None:
            assert cell.coverage_ratio == pytest.approx(0.0)

    def test_cells_coverage_ratio_bounded(self, patched_matrix):
        for cell in patched_matrix.cells:
            assert 0.0 <= cell.coverage_ratio <= 1.0, (
                f"coverage_ratio {cell.coverage_ratio} out of [0,1] for {cell}"
            )

    def test_snapshot_note_is_non_empty_string(self, patched_matrix):
        assert isinstance(patched_matrix.snapshot_note, str)
        assert len(patched_matrix.snapshot_note) > 0


# ---------------------------------------------------------------------------
# 3. Edge cases
# ---------------------------------------------------------------------------


class TestComputeCoverageMatrixEdgeCases:
    def test_absent_resolved_db_returns_empty(self):
        with patch(
            "src.analysis.quality.coverage_matrix.resolved_available",
            return_value=False,
        ):
            matrix = compute_coverage_matrix(
                resolved_path=Path("/nonexistent/path.duckdb")
            )
        assert matrix.is_empty()

    def test_absent_matrix_snapshot_note_is_set(self):
        with patch(
            "src.analysis.quality.coverage_matrix.resolved_available",
            return_value=False,
        ):
            matrix = compute_coverage_matrix()
        assert len(matrix.snapshot_note) > 0

    def test_empty_credits_table_returns_empty(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE anime (canonical_id VARCHAR, year INTEGER)")
        conn.execute(
            "CREATE TABLE credits (person_id VARCHAR, anime_id VARCHAR, role VARCHAR, evidence_source VARCHAR)"
        )
        with (
            patch(
                "src.analysis.quality.coverage_matrix.resolved_available",
                return_value=True,
            ),
            patch(
                "src.analysis.quality.coverage_matrix.resolved_connect",
                side_effect=lambda p: _resolved_connect_from_conn(conn),
            ),
        ):
            matrix = compute_coverage_matrix(resolved_path=Path(":memory:"))
        assert matrix.is_empty()

    def test_single_source_single_cell(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE anime (canonical_id VARCHAR, year INTEGER)")
        conn.execute(
            "CREATE TABLE credits (person_id VARCHAR, anime_id VARCHAR, role VARCHAR, evidence_source VARCHAR)"
        )
        conn.execute("INSERT INTO anime VALUES ('a1', 2022)")
        conn.execute("INSERT INTO credits VALUES ('p1', 'a1', 'director', 'ann')")
        with (
            patch(
                "src.analysis.quality.coverage_matrix.resolved_available",
                return_value=True,
            ),
            patch(
                "src.analysis.quality.coverage_matrix.resolved_connect",
                side_effect=lambda p: _resolved_connect_from_conn(conn),
            ),
        ):
            matrix = compute_coverage_matrix(resolved_path=Path(":memory:"))
        assert not matrix.is_empty()
        assert matrix.sources == ["ann"]
        assert matrix.role_groups == ["direction"]
        assert matrix.years == [2022]
        cell = matrix.lookup("ann", "direction", 2022)
        assert cell is not None
        assert cell.coverage_ratio == pytest.approx(1.0)

    def test_custom_reference_source(self):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE anime (canonical_id VARCHAR, year INTEGER)")
        conn.execute(
            "CREATE TABLE credits (person_id VARCHAR, anime_id VARCHAR, role VARCHAR, evidence_source VARCHAR)"
        )
        conn.execute("INSERT INTO anime VALUES ('a1', 2020)")
        conn.execute("""
            INSERT INTO credits VALUES
              ('p1', 'a1', 'key_animator', 'mal'),
              ('p2', 'a1', 'key_animator', 'anilist'),
              ('p3', 'a1', 'key_animator', 'anilist')
        """)
        with (
            patch(
                "src.analysis.quality.coverage_matrix.resolved_available",
                return_value=True,
            ),
            patch(
                "src.analysis.quality.coverage_matrix.resolved_connect",
                side_effect=lambda p: _resolved_connect_from_conn(conn),
            ),
        ):
            matrix = compute_coverage_matrix(
                resolved_path=Path(":memory:"),
                reference_source="anilist",
            )
        # anilist has 2 credits; mal has 1 → mal coverage = 0.5
        mal_cell = matrix.lookup("mal", "animation", 2020)
        assert mal_cell is not None
        assert mal_cell.coverage_ratio == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# 4. CoverageMatrix model behaviour
# ---------------------------------------------------------------------------


class TestCoverageMatrixModel:
    def test_is_empty_true_when_no_cells(self):
        m = CoverageMatrix()
        assert m.is_empty()

    def test_is_empty_false_with_cells(self):
        m = CoverageMatrix(cells=[CoverageCell("ann", "animation", 2020, 5, 5, 1.0)])
        assert not m.is_empty()

    def test_lookup_returns_correct_cell(self):
        cell = CoverageCell("ann", "animation", 2020, 5, 5, 1.0)
        m = CoverageMatrix(cells=[cell])
        assert m.lookup("ann", "animation", 2020) is cell

    def test_lookup_returns_none_for_missing(self):
        m = CoverageMatrix(cells=[CoverageCell("ann", "animation", 2020, 5, 5, 1.0)])
        assert m.lookup("anilist", "animation", 2020) is None

    def test_mean_coverage_for_role_group(self):
        cells = [
            CoverageCell("ann", "animation", 2020, 10, 10, 1.0),
            CoverageCell("anilist", "animation", 2020, 5, 10, 0.5),
        ]
        m = CoverageMatrix(cells=cells)
        mean = m.mean_coverage_for_role_group("animation")
        assert mean == pytest.approx(0.75)

    def test_mean_coverage_returns_zero_for_absent_role(self):
        m = CoverageMatrix()
        assert m.mean_coverage_for_role_group("animation") == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# 5. coverage_matrix_to_records
# ---------------------------------------------------------------------------


class TestCoverageMatrixToRecords:
    def test_round_trips_all_fields(self):
        cell = CoverageCell("ann", "animation", 2020, 10, 10, 1.0, "test note")
        m = CoverageMatrix(cells=[cell])
        records = coverage_matrix_to_records(m)
        assert len(records) == 1
        r = records[0]
        assert r["source"] == "ann"
        assert r["role_group"] == "animation"
        assert r["year"] == 2020
        assert r["n_credits"] == 10
        assert r["reference_n"] == 10
        assert r["coverage_ratio"] == pytest.approx(1.0)
        assert r["note"] == "test note"

    def test_empty_matrix_returns_empty_list(self):
        assert coverage_matrix_to_records(CoverageMatrix()) == []

    def test_multiple_cells_preserve_order(self, patched_matrix):
        records = coverage_matrix_to_records(patched_matrix)
        assert len(records) == len(patched_matrix.cells)
