"""Unit tests for Phase 5 core_scoring — 8-component structural scoring.

Tests the compute_core_scores_phase() orchestrator, which:
  1. AKM fixed effects (person_fe, studio_fe)
  2. BiRank (bipartite PageRank, then log-rescale)
  3. Knowledge Spanners (AWCC / NDI)
  4. Patronage Premium
  5. Dormancy Penalty
  6. Integrated Value (historical + current with dormancy)
"""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from src.pipeline_phases.context import PipelineContext
from src.testing.fixtures import generate_synthetic_data


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_context(monkeypatch, tmp_path: Path) -> PipelineContext:
    """Return a PipelineContext populated through Phases 1-4."""
    import src.db.init
    import src.runtime.pipeline
    import src.utils.config

    db_path = tmp_path / "core_scoring.db"
    json_dir = tmp_path / "json"
    json_dir.mkdir()

    monkeypatch.setattr(src.db.init, "DEFAULT_DB_PATH", db_path)
    monkeypatch.setattr(src.runtime.pipeline, "JSON_DIR", json_dir)
    monkeypatch.setattr(src.utils.config, "JSON_DIR", json_dir)

    from src.pipeline_phases import (
        PipelineContext,
        build_graphs_phase,
        load_pipeline_data,
        run_entity_resolution,
        run_validation_phase,
    )

    persons, anime_list, credits = generate_synthetic_data(
        n_directors=5, n_animators=30, n_anime=15, seed=42
    )

    from tests.conftest import build_silver_duckdb
    import src.analysis.io.silver_reader
    import src.analysis.io.gold_writer

    silver_path = tmp_path / "silver.duckdb"
    gold_path = tmp_path / "gold.duckdb"
    build_silver_duckdb(silver_path, persons, anime_list, credits)
    monkeypatch.setattr(src.analysis.io.silver_reader, "DEFAULT_SILVER_PATH", silver_path)
    monkeypatch.setattr(src.analysis.io.gold_writer, "DEFAULT_GOLD_DB_PATH", gold_path)

    ctx = PipelineContext(visualize=False, dry_run=False)
    load_pipeline_data(ctx)
    run_validation_phase(ctx)
    run_entity_resolution(ctx)
    build_graphs_phase(ctx)
    return ctx


@pytest.fixture
def ctx(monkeypatch, tmp_path):
    c = _make_context(monkeypatch, tmp_path)
    from src.pipeline_phases.core_scoring import compute_core_scores_phase
    compute_core_scores_phase(c)
    return c


# ---------------------------------------------------------------------------
# AKM (step 1)
# ---------------------------------------------------------------------------


class TestAKMOutputs:
    def test_person_fe_populated(self, ctx):
        assert len(ctx.person_fe) > 0

    def test_studio_fe_populated(self, ctx):
        assert len(ctx.studio_fe) > 0

    def test_studio_assignments_populated(self, ctx):
        assert len(ctx.studio_assignments) > 0

    def test_person_fe_values_finite(self, ctx):
        assert all(math.isfinite(v) for v in ctx.person_fe.values())

    def test_person_fe_covers_majority_of_credits(self, ctx):
        credit_persons = {c.person_id for c in ctx.credits}
        fe_persons = set(ctx.person_fe.keys())
        coverage = len(fe_persons & credit_persons) / len(credit_persons)
        assert coverage > 0.5, f"person_fe covers only {coverage:.0%} of credit persons"


# ---------------------------------------------------------------------------
# BiRank (step 2, after log rescale)
# ---------------------------------------------------------------------------


class TestBiRankOutputs:
    def test_birank_person_scores_populated(self, ctx):
        assert len(ctx.birank_person_scores) > 0

    def test_birank_anime_scores_populated(self, ctx):
        assert len(ctx.birank_anime_scores) > 0

    def test_birank_scores_are_positive(self, ctx):
        assert all(v >= 0 for v in ctx.birank_person_scores.values())
        assert all(v >= 0 for v in ctx.birank_anime_scores.values())

    def test_birank_log_rescale_applied(self, ctx):
        # After log1p(score * 10000), scores must be > 0 and in a narrow range.
        vals = list(ctx.birank_person_scores.values())
        assert max(vals) < 50, "birank not log-rescaled — raw scores would be ~0.001"


# ---------------------------------------------------------------------------
# IV Scores (step 6)
# ---------------------------------------------------------------------------


class TestIntegratedValueOutputs:
    def test_iv_scores_populated(self, ctx):
        assert len(ctx.iv_scores) > 0

    def test_iv_scores_historical_populated(self, ctx):
        assert len(ctx.iv_scores_historical) > 0

    def test_iv_lambda_weights_sum_to_one(self, ctx):
        total = sum(ctx.iv_lambda_weights.values())
        assert abs(total - 1.0) < 0.01, f"lambda weights sum = {total:.3f}"

    def test_iv_scores_finite(self, ctx):
        assert all(math.isfinite(v) for v in ctx.iv_scores.values())

    def test_iv_scores_non_negative(self, ctx):
        assert all(v >= 0 for v in ctx.iv_scores.values())

    def test_historical_and_current_iv_same_persons(self, ctx):
        # Both passes use the same person set; independent renormalization means
        # the raw inequality historical≥current doesn't hold, but coverage must match.
        assert set(ctx.iv_scores_historical.keys()) == set(ctx.iv_scores.keys())


# ---------------------------------------------------------------------------
# Patronage / Dormancy
# ---------------------------------------------------------------------------


class TestPatronageAndDormancy:
    def test_dormancy_scores_populated(self, ctx):
        assert len(ctx.dormancy_scores) > 0

    def test_dormancy_scores_in_0_1(self, ctx):
        for pid, score in ctx.dormancy_scores.items():
            assert 0.0 <= score <= 1.0 + 1e-9, (
                f"Person {pid}: dormancy={score} out of [0,1]"
            )

    def test_patronage_scores_non_negative(self, ctx):
        assert all(v >= 0 for v in ctx.patronage_scores.values())


# ---------------------------------------------------------------------------
# Component coverage
# ---------------------------------------------------------------------------


class TestComponentCoverage:
    def test_all_persons_have_iv_score(self, ctx):
        credit_persons = {c.person_id for c in ctx.credits}
        iv_persons = set(ctx.iv_scores.keys())
        # Not all credit persons may have scores (e.g. missing from AKM),
        # but at least 80% coverage is expected.
        coverage = len(iv_persons & credit_persons) / len(credit_persons)
        assert coverage > 0.8, f"IV score coverage: {coverage:.0%}"

    def test_knowledge_spanner_scores_populated(self, ctx):
        # May be empty if collaboration graph is too small — just check no crash.
        assert isinstance(ctx.knowledge_spanner_scores, dict)
