"""Tests for O3 IP 人的依存リスク分析 report.

Covers:
- smoke test: generate() returns a Path with valid HTML
- contribution_share: mathematical correctness on synthetic data
- counterfactual_drop: additive decomposition invariant
- null_percentile: in-range [0, 100]
- lint_vocab: no forbidden terms in report source
- method gate: CI + null model annotations present in output
- anime.score: not referenced in report SQL
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

from scripts.report_generators.reports.o3_ip_dependency import (
    ContributionRow,
    SeriesCluster,
    _build_series_clusters,
    _fetch_anime_scales,
    _null_percentile,
    compute_counterfactual_drop,
    compute_null_distribution,
    compute_series_contribution_shares,
    O3IpDependencyReport,
)
import random


# ---------------------------------------------------------------------------
# Synthetic in-memory SQLite DB
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS anime (
    id TEXT PRIMARY KEY,
    title_romaji TEXT,
    episodes INTEGER,
    duration INTEGER,
    relations_json TEXT
);

CREATE TABLE IF NOT EXISTS persons (
    id TEXT PRIMARY KEY,
    name_romaji TEXT
);

CREATE TABLE IF NOT EXISTS credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id TEXT NOT NULL,
    anime_id TEXT NOT NULL,
    role TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS roles (
    name TEXT PRIMARY KEY,
    weight REAL
);
"""


def _build_test_db() -> sqlite3.Connection:
    """Create a minimal in-memory DB with 5 series × ~6 persons."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA_SQL)

    # 5 series: s0 (3 anime), s1 (2 anime), s2–s4 (1 anime each)
    # Series s0: a0 → a1 (SEQUEL) → a2 (SEQUEL)
    anime_rows = [
        # id, title_romaji, episodes, duration, relations_json
        ("a0", "Series Alpha S1", 12, 24,
         '[{"relation_type":"SEQUEL","related_anime_id":"a1"}]'),
        ("a1", "Series Alpha S2", 12, 24,
         '[{"relation_type":"PREQUEL","related_anime_id":"a0"},'
         '{"relation_type":"SEQUEL","related_anime_id":"a2"}]'),
        ("a2", "Series Alpha S3", 12, 24,
         '[{"relation_type":"PREQUEL","related_anime_id":"a1"}]'),
        # Series s1: b0 → b1
        ("b0", "Series Beta S1", 24, 30,
         '[{"relation_type":"SEQUEL","related_anime_id":"b1"}]'),
        ("b1", "Series Beta S2", 24, 30,
         '[{"relation_type":"PREQUEL","related_anime_id":"b0"}]'),
        # Single-anime series
        ("c0", "Standalone Gamma", 1, 90, None),
        ("d0", "Standalone Delta", 6, 30, None),
        ("e0", "Standalone Epsilon", 13, 24, None),
    ]
    conn.executemany(
        "INSERT INTO anime VALUES (?, ?, ?, ?, ?)", anime_rows
    )

    # 6 persons
    persons = [
        ("p1", "Director One"),
        ("p2", "Animator Two"),
        ("p3", "Animator Three"),
        ("p4", "Designer Four"),
        ("p5", "Director Five"),
        ("p6", "Animator Six"),
    ]
    conn.executemany("INSERT INTO persons VALUES (?, ?)", persons)

    # Credits for series s0 (a0, a1, a2)
    # p1 = director on all 3 (high concentration)
    # p2, p3 spread across
    credits_s0 = [
        ("p1", "a0", "director"),
        ("p1", "a1", "director"),
        ("p1", "a2", "director"),
        ("p2", "a0", "key_animator"),
        ("p2", "a1", "key_animator"),
        ("p3", "a0", "animation_director"),
        ("p3", "a2", "animation_director"),
        ("p4", "a0", "character_designer"),
    ]
    # Credits for series s1 (b0, b1)
    # p5 on both
    credits_s1 = [
        ("p5", "b0", "director"),
        ("p5", "b1", "director"),
        ("p6", "b0", "key_animator"),
        ("p6", "b1", "key_animator"),
        ("p2", "b0", "animation_director"),
    ]
    # Credits for standalone series
    credits_single = [
        ("p1", "c0", "director"),
        ("p3", "c0", "animation_director"),
        ("p4", "d0", "character_designer"),
        ("p6", "e0", "key_animator"),
        ("p2", "e0", "animation_director"),
    ]

    all_credits = credits_s0 + credits_s1 + credits_single
    conn.executemany(
        "INSERT INTO credits (person_id, anime_id, role) VALUES (?, ?, ?)",
        all_credits,
    )

    # Role weights
    role_weights = [
        ("director", 3.0),
        ("animation_director", 2.8),
        ("key_animator", 2.0),
        ("character_designer", 2.3),
        ("episode_director", 2.49),
    ]
    conn.executemany("INSERT INTO roles VALUES (?, ?)", role_weights)

    conn.commit()
    return conn


@pytest.fixture
def test_conn() -> sqlite3.Connection:
    return _build_test_db()


@pytest.fixture
def rng() -> random.Random:
    return random.Random(0)


# ---------------------------------------------------------------------------
# Step 1: Series clustering tests
# ---------------------------------------------------------------------------


class TestBuildSeriesClusters:
    def test_returns_clusters(self, test_conn):
        clusters = _build_series_clusters(test_conn)
        assert isinstance(clusters, list)
        assert len(clusters) >= 1

    def test_multi_anime_series_grouped(self, test_conn):
        clusters = _build_series_clusters(test_conn)
        cluster_sizes = [len(c.anime_ids) for c in clusters]
        # Should have at least one cluster with 3 members (s0) and one with 2 (s1)
        assert 3 in cluster_sizes
        assert 2 in cluster_sizes

    def test_standalone_anime_is_own_series(self, test_conn):
        clusters = _build_series_clusters(test_conn)
        single_clusters = [c for c in clusters if len(c.anime_ids) == 1]
        # c0, d0, e0 should be their own clusters
        single_ids = {c.anime_ids[0] for c in single_clusters}
        assert {"c0", "d0", "e0"}.issubset(single_ids)

    def test_empty_db_returns_empty(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE anime (id TEXT, title_romaji TEXT, relations_json TEXT)")
        result = _build_series_clusters(conn)
        assert result == []


# ---------------------------------------------------------------------------
# Step 2: production_scale tests
# ---------------------------------------------------------------------------


class TestFetchAnimeScales:
    def test_returns_scales(self, test_conn):
        scales = _fetch_anime_scales(test_conn, ["a0", "a1"])
        assert "a0" in scales
        assert "a1" in scales

    def test_scale_is_positive(self, test_conn):
        scales = _fetch_anime_scales(test_conn, ["a0"])
        assert scales["a0"] > 0

    def test_scale_formula(self, test_conn):
        # a0: 12 episodes, 24 min duration, staff = count distinct credits
        # duration_mult = 24/30 = 0.8, staff_count = 4 (p1, p2, p3, p4)
        # scale = 4 × 12 × 0.8 = 38.4
        scales = _fetch_anime_scales(test_conn, ["a0"])
        expected = 4 * 12 * (24 / 30.0)
        assert abs(scales["a0"] - expected) < 0.1

    def test_empty_ids_returns_empty(self, test_conn):
        assert _fetch_anime_scales(test_conn, []) == {}


# ---------------------------------------------------------------------------
# Step 3: contribution_share tests
# ---------------------------------------------------------------------------


class TestComputeSeriesContributionShares:
    def _get_cluster_s0(self, test_conn) -> SeriesCluster:
        clusters = _build_series_clusters(test_conn)
        s0 = next(c for c in clusters if len(c.anime_ids) == 3)
        return s0

    def test_returns_rows(self, test_conn):
        cluster = self._get_cluster_s0(test_conn)
        scales = _fetch_anime_scales(test_conn, cluster.anime_ids)
        rows = compute_series_contribution_shares(
            test_conn, cluster,
            {"director": 3.0, "key_animator": 2.0, "animation_director": 2.8,
             "character_designer": 2.3},
            scales,
        )
        assert isinstance(rows, list)
        assert len(rows) > 0

    def test_shares_sum_to_one(self, test_conn):
        cluster = self._get_cluster_s0(test_conn)
        scales = _fetch_anime_scales(test_conn, cluster.anime_ids)
        rows = compute_series_contribution_shares(
            test_conn, cluster,
            {"director": 3.0, "key_animator": 2.0, "animation_director": 2.8,
             "character_designer": 2.3},
            scales,
        )
        total_share = sum(r.contribution_share for r in rows)
        assert abs(total_share - 1.0) < 1e-9, f"Sum={total_share}"

    def test_director_has_highest_share(self, test_conn):
        cluster = self._get_cluster_s0(test_conn)
        scales = _fetch_anime_scales(test_conn, cluster.anime_ids)
        rows = compute_series_contribution_shares(
            test_conn, cluster,
            {"director": 3.0, "key_animator": 2.0, "animation_director": 2.8,
             "character_designer": 2.3},
            scales,
        )
        # p1 (director on all 3 anime) should have the highest share
        assert rows[0].person_id == "p1"

    def test_share_in_range_01(self, test_conn):
        cluster = self._get_cluster_s0(test_conn)
        scales = _fetch_anime_scales(test_conn, cluster.anime_ids)
        rows = compute_series_contribution_shares(
            test_conn, cluster,
            {"director": 3.0, "key_animator": 2.0},
            scales,
        )
        for r in rows:
            assert 0.0 <= r.contribution_share <= 1.0

    def test_empty_cluster_returns_empty(self, test_conn):
        cluster = SeriesCluster(cluster_id="empty", anime_ids=[])
        rows = compute_series_contribution_shares(
            test_conn, cluster, {"director": 3.0}, {}
        )
        assert rows == []


# ---------------------------------------------------------------------------
# Step 4: counterfactual tests
# ---------------------------------------------------------------------------


class TestComputeCounterfactualDrop:
    def _make_contrib_rows(self) -> tuple[list[ContributionRow], float]:
        rows = [
            ContributionRow("p1", "Dir A", "s0", "Series A", 60.0, 0.6, 3),
            ContributionRow("p2", "Anim B", "s0", "Series A", 30.0, 0.3, 2),
            ContributionRow("p3", "Des C", "s0", "Series A", 10.0, 0.1, 1),
        ]
        total = sum(r.weighted_contribution for r in rows)
        return rows, total

    def test_drop_equals_weighted_contribution(self, rng):
        rows, total = self._make_contrib_rows()
        result = compute_counterfactual_drop(rows, total, "p1", rng, n_bootstrap=200)
        assert result is not None
        assert abs(result.counterfactual_drop - 60.0) < 1e-9

    def test_drop_pct_equals_share(self, rng):
        rows, total = self._make_contrib_rows()
        result = compute_counterfactual_drop(rows, total, "p1", rng, n_bootstrap=200)
        assert result is not None
        assert abs(result.counterfactual_drop_pct - 0.6) < 1e-9

    def test_ci_lower_lte_upper(self, rng):
        rows, total = self._make_contrib_rows()
        result = compute_counterfactual_drop(rows, total, "p1", rng, n_bootstrap=200)
        assert result is not None
        assert result.ci_lower <= result.ci_upper

    def test_missing_person_returns_none(self, rng):
        rows, total = self._make_contrib_rows()
        result = compute_counterfactual_drop(rows, total, "px_missing", rng)
        assert result is None

    def test_zero_total_returns_none(self, rng):
        rows, _ = self._make_contrib_rows()
        result = compute_counterfactual_drop(rows, 0.0, "p1", rng)
        assert result is None


# ---------------------------------------------------------------------------
# Step 5: null model tests
# ---------------------------------------------------------------------------


class TestComputeNullDistribution:
    def _make_contrib_rows(self) -> tuple[list[ContributionRow], float]:
        rows = [
            ContributionRow("p1", "Dir A", "s0", "S", 60.0, 0.6, 3),
            ContributionRow("p2", "Anim B", "s0", "S", 30.0, 0.3, 2),
            ContributionRow("p3", "Des C", "s0", "S", 10.0, 0.1, 1),
        ]
        total = 100.0
        return rows, total

    def test_returns_correct_length(self, rng):
        rows, total = self._make_contrib_rows()
        null = compute_null_distribution(rows, total, {}, rng, n_iter=500)
        assert len(null) == 500

    def test_all_values_in_range(self, rng):
        rows, total = self._make_contrib_rows()
        null = compute_null_distribution(rows, total, {}, rng, n_iter=500)
        for v in null:
            assert 0.0 <= v <= 1.0

    def test_empty_rows_returns_empty(self, rng):
        result = compute_null_distribution([], 100.0, {}, rng)
        assert result == []

    def test_zero_total_returns_empty(self, rng):
        rows, _ = self._make_contrib_rows()
        result = compute_null_distribution(rows, 0.0, {}, rng)
        assert result == []


class TestNullPercentile:
    def test_100_when_all_below(self):
        null = [0.1, 0.2, 0.3]
        assert _null_percentile(1.0, null) == 100.0

    def test_0_when_all_above(self):
        null = [0.5, 0.6, 0.7]
        assert _null_percentile(0.0, null) == 0.0

    def test_50_at_median(self):
        null = sorted([0.1 * i for i in range(1, 11)])  # 0.1..1.0
        pct = _null_percentile(0.5, null)
        assert 40.0 <= pct <= 60.0

    def test_empty_null_returns_50(self):
        assert _null_percentile(0.5, []) == 50.0

    def test_in_range_0_100(self, rng):
        null = [rng.random() for _ in range(100)]
        for obs in [0.0, 0.1, 0.5, 0.9, 1.0]:
            p = _null_percentile(obs, null)
            assert 0.0 <= p <= 100.0


# ---------------------------------------------------------------------------
# Smoke: full report generation
# ---------------------------------------------------------------------------


class TestO3IpDependencyReportSmoke:
    def test_generate_returns_path(self, test_conn, tmp_path):
        report = O3IpDependencyReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        assert isinstance(out, Path)
        assert out.exists()

    def test_output_is_html(self, test_conn, tmp_path):
        report = O3IpDependencyReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in text or "<html" in text.lower()

    def test_disclaimer_present(self, test_conn, tmp_path):
        report = O3IpDependencyReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        # REPORT_PHILOSOPHY §9: both JA + EN disclaimer required
        assert "ネットワーク" in text
        assert "network structure" in text.lower()

    def test_method_note_present(self, test_conn, tmp_path):
        report = O3IpDependencyReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        # method note must mention production_scale or CI
        assert "production_scale" in text or "bootstrap" in text.lower()

    def test_no_anime_score_in_output(self, test_conn, tmp_path):
        report = O3IpDependencyReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        # anime.score must not appear in output HTML
        assert "anime.score" not in text

    def test_generate_with_empty_db(self, tmp_path):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(_SCHEMA_SQL)
        report = O3IpDependencyReport(conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        assert out.exists()


# ---------------------------------------------------------------------------
# Lint vocab: forbidden terms must not appear in report source
# ---------------------------------------------------------------------------


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


class TestLintVocabCompliance:
    """Verify that o3_ip_dependency.py contains no forbidden vocabulary."""

    REPORT_FILE = _PROJECT_ROOT / "scripts/report_generators/reports/o3_ip_dependency.py"
    FORBIDDEN_PATTERN = re.compile(
        r"\b(ability|skill|talent|competence|capability)\b", re.IGNORECASE
    )

    def test_no_forbidden_vocab_in_source(self):
        source = self.REPORT_FILE.read_text(encoding="utf-8")
        matches = self.FORBIDDEN_PATTERN.findall(source)
        assert matches == [], (
            f"Forbidden vocabulary found in o3_ip_dependency.py: {matches}"
        )

    def test_no_anime_score_in_source(self):
        source = self.REPORT_FILE.read_text(encoding="utf-8")
        # anime.score must not appear in any SQL or formula
        assert "anime.score" not in source, (
            "anime.score found in o3_ip_dependency.py — violates H1"
        )


# ---------------------------------------------------------------------------
# Method gate: CI + null model both present
# ---------------------------------------------------------------------------


class TestMethodGate:
    """REPORT_PHILOSOPHY §3 — CI and null model must be present."""

    def test_bootstrap_ci_computed(self, rng):
        rows = [
            ContributionRow("p1", "Dir", "s0", "S", 60.0, 0.6, 3),
            ContributionRow("p2", "Anim", "s0", "S", 40.0, 0.4, 2),
        ]
        total = 100.0
        result = compute_counterfactual_drop(rows, total, "p1", rng, n_bootstrap=100)
        assert result is not None
        # CI must not be degenerate (lower == upper == 0) unless 0 credits
        assert not (result.ci_lower == 0.0 and result.ci_upper == 0.0
                    and result.counterfactual_drop > 0)

    def test_null_model_returns_distribution(self, rng):
        rows = [
            ContributionRow("p1", "Dir", "s0", "S", 60.0, 0.6, 3),
            ContributionRow("p2", "Anim", "s0", "S", 40.0, 0.4, 2),
        ]
        null = compute_null_distribution(rows, 100.0, {}, rng, n_iter=200)
        assert len(null) == 200
        # Distribution must have variance (not all identical)
        assert max(null) > min(null)

    def test_null_percentile_used_in_result(self, rng):
        rows = [
            ContributionRow("p1", "Dir", "s0", "S", 60.0, 0.6, 3),
            ContributionRow("p2", "Anim", "s0", "S", 40.0, 0.4, 2),
        ]
        total = 100.0
        cf = compute_counterfactual_drop(rows, total, "p1", rng, n_bootstrap=100)
        assert cf is not None
        null = compute_null_distribution(rows, total, {}, rng, n_iter=200)
        from scripts.report_generators.reports.o3_ip_dependency import _null_percentile
        cf.null_percentile = _null_percentile(cf.counterfactual_drop_pct, null)
        assert 0.0 <= cf.null_percentile <= 100.0
