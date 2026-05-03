"""Tests for Card 05: era_fe / era_deflated_iv / opportunity_residual GOLD write.

Covers:
- _build_debut_year_map: debut year extraction from Credit objects
- _lookup_era_fe: era fixed effect lookup with nearest-year fallback
- _persist_causal_estimates_duckdb: end-to-end write to feat_causal_estimates
"""

from __future__ import annotations

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_credit(person_id: str, credit_year: int | None):
    """Return a minimal Credit-like object."""
    return SimpleNamespace(person_id=person_id, credit_year=credit_year)


def _make_era_effects(era_fe: dict[int, float]):
    """Return a minimal EraEffects-like object."""
    return SimpleNamespace(era_fe=era_fe)


def _make_context(
    iv_scores: dict[str, float],
    credits: list,
    era_effects=None,
    career_friction: dict | None = None,
    peer_effect_result=None,
    individual_profiles: dict | None = None,
) -> MagicMock:
    ctx = MagicMock()
    ctx.iv_scores = iv_scores
    ctx.credits = credits
    ctx.era_effects = era_effects
    ctx.career_friction = career_friction or {}
    ctx.peer_effect_result = peer_effect_result
    ctx.analysis_results = {"individual_profiles": individual_profiles or {}}
    return ctx


# ---------------------------------------------------------------------------
# Unit: _build_debut_year_map
# ---------------------------------------------------------------------------

class TestBuildDebutYearMap:
    def test_single_credit(self):
        from src.pipeline_phases.export_and_viz import _build_debut_year_map

        credits = [_make_credit("p1", 2015)]
        result = _build_debut_year_map(credits)
        assert result == {"p1": 2015}

    def test_multiple_credits_takes_minimum(self):
        from src.pipeline_phases.export_and_viz import _build_debut_year_map

        credits = [
            _make_credit("p1", 2018),
            _make_credit("p1", 2015),
            _make_credit("p1", 2020),
        ]
        result = _build_debut_year_map(credits)
        assert result["p1"] == 2015

    def test_none_credit_year_skipped(self):
        from src.pipeline_phases.export_and_viz import _build_debut_year_map

        credits = [_make_credit("p1", None), _make_credit("p1", 2017)]
        result = _build_debut_year_map(credits)
        assert result["p1"] == 2017

    def test_all_none_credit_year(self):
        from src.pipeline_phases.export_and_viz import _build_debut_year_map

        credits = [_make_credit("p1", None)]
        result = _build_debut_year_map(credits)
        assert "p1" not in result

    def test_multiple_persons(self):
        from src.pipeline_phases.export_and_viz import _build_debut_year_map

        credits = [
            _make_credit("p1", 2010),
            _make_credit("p2", 2018),
            _make_credit("p1", 2008),
        ]
        result = _build_debut_year_map(credits)
        assert result == {"p1": 2008, "p2": 2018}

    def test_empty_credits(self):
        from src.pipeline_phases.export_and_viz import _build_debut_year_map

        assert _build_debut_year_map([]) == {}


# ---------------------------------------------------------------------------
# Unit: _lookup_era_fe
# ---------------------------------------------------------------------------

class TestLookupEraFe:
    def test_exact_year_match(self):
        from src.pipeline_phases.export_and_viz import _lookup_era_fe

        era_fe = {2010: 0.3, 2015: 0.5, 2020: 0.7}
        assert _lookup_era_fe(2015, era_fe) == pytest.approx(0.5)

    def test_nearest_year_fallback(self):
        from src.pipeline_phases.export_and_viz import _lookup_era_fe

        era_fe = {2010: 0.3, 2020: 0.7}
        # 2014 is closer to 2010 (diff=4) than to 2020 (diff=6)
        assert _lookup_era_fe(2014, era_fe) == pytest.approx(0.3)
        # 2017 is closer to 2020 (diff=3) than to 2010 (diff=7)
        assert _lookup_era_fe(2017, era_fe) == pytest.approx(0.7)

    def test_empty_map_returns_none(self):
        from src.pipeline_phases.export_and_viz import _lookup_era_fe

        assert _lookup_era_fe(2015, {}) is None

    def test_none_debut_year_returns_none(self):
        from src.pipeline_phases.export_and_viz import _lookup_era_fe

        assert _lookup_era_fe(None, {2015: 0.5}) is None

    def test_single_entry_map(self):
        from src.pipeline_phases.export_and_viz import _lookup_era_fe

        assert _lookup_era_fe(1999, {2015: 0.5}) == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Unit: era_deflated_iv arithmetic
# ---------------------------------------------------------------------------

class TestEraDelfatedIvArithmetic:
    """Verify the central requirement: iv_score - era_fe = era_deflated_iv."""

    def test_era_deflated_iv_value(self):
        """debut_year=2015, era_fe[2015]=0.5, iv_score=2.0 → era_deflated_iv=1.5."""
        from src.pipeline_phases.export_and_viz import _lookup_era_fe

        era_fe_map = {2015: 0.5}
        iv_score = 2.0
        era_fe_val = _lookup_era_fe(2015, era_fe_map)
        era_deflated = round(iv_score - era_fe_val, 6)
        assert era_deflated == pytest.approx(1.5)


# ---------------------------------------------------------------------------
# Integration: _persist_causal_estimates_duckdb writes era columns
# ---------------------------------------------------------------------------

@pytest.fixture
def gold_with_ddl(tmp_path):
    """gold.duckdb with DDL applied (no silver needed for this test)."""
    gold_path = tmp_path / "gold.duckdb"
    with duckdb.connect(str(gold_path)) as conn:
        from src.analysis.io.mart_writer import _DDL
        conn.execute(_DDL)
    return gold_path


class TestPersistCausalEstimatesDuckdb:
    def test_era_fe_written_not_null(self, gold_with_ddl):
        from src.pipeline_phases.export_and_viz import _persist_causal_estimates_duckdb

        ctx = _make_context(
            iv_scores={"p1": 2.0},
            credits=[_make_credit("p1", 2015)],
            era_effects=_make_era_effects({2015: 0.5}),
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        with duckdb.connect(str(gold_with_ddl)) as conn:
            _persist_causal_estimates_duckdb(conn, ctx, now)
            row = conn.execute(
                "SELECT era_fe, era_deflated_iv FROM feat_causal_estimates WHERE person_id='p1'"
            ).fetchone()

        assert row is not None
        assert row[0] == pytest.approx(0.5)
        assert row[1] == pytest.approx(1.5)

    def test_era_deflated_iv_is_iv_minus_era_fe(self, gold_with_ddl):
        """era_deflated_iv = iv_score - era_fe regardless of career_friction."""
        from src.pipeline_phases.export_and_viz import _persist_causal_estimates_duckdb

        ctx = _make_context(
            iv_scores={"p1": 3.0},
            credits=[_make_credit("p1", 2010)],
            era_effects=_make_era_effects({2010: 1.0}),
            career_friction={"p1": 0.2},
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        with duckdb.connect(str(gold_with_ddl)) as conn:
            _persist_causal_estimates_duckdb(conn, ctx, now)
            row = conn.execute(
                "SELECT era_fe, era_deflated_iv FROM feat_causal_estimates WHERE person_id='p1'"
            ).fetchone()

        assert row[0] == pytest.approx(1.0)
        assert row[1] == pytest.approx(2.0)

    def test_nearest_year_fallback_used(self, gold_with_ddl):
        """Person debuts 2013 but era_fe only has 2010 and 2020 → nearest=2010."""
        from src.pipeline_phases.export_and_viz import _persist_causal_estimates_duckdb

        ctx = _make_context(
            iv_scores={"p1": 2.0},
            credits=[_make_credit("p1", 2013)],
            era_effects=_make_era_effects({2010: 0.3, 2020: 0.7}),
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        with duckdb.connect(str(gold_with_ddl)) as conn:
            _persist_causal_estimates_duckdb(conn, ctx, now)
            row = conn.execute(
                "SELECT era_fe FROM feat_causal_estimates WHERE person_id='p1'"
            ).fetchone()

        assert row[0] == pytest.approx(0.3)

    def test_era_fe_none_when_no_era_effects(self, gold_with_ddl):
        """When era_effects is None, era_fe and era_deflated_iv stay NULL."""
        from src.pipeline_phases.export_and_viz import _persist_causal_estimates_duckdb

        ctx = _make_context(
            iv_scores={"p1": 1.5},
            credits=[_make_credit("p1", 2015)],
            era_effects=None,
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        with duckdb.connect(str(gold_with_ddl)) as conn:
            _persist_causal_estimates_duckdb(conn, ctx, now)
            row = conn.execute(
                "SELECT era_fe, era_deflated_iv FROM feat_causal_estimates WHERE person_id='p1'"
            ).fetchone()

        assert row[0] is None
        assert row[1] is None

    def test_opportunity_residual_written_from_individual_profiles(self, gold_with_ddl):
        """opportunity_residual is taken from context.analysis_results individual_profiles."""
        from src.pipeline_phases.export_and_viz import _persist_causal_estimates_duckdb

        ctx = _make_context(
            iv_scores={"p1": 2.0},
            credits=[_make_credit("p1", 2015)],
            era_effects=_make_era_effects({2015: 0.5}),
            individual_profiles={"p1": {"opportunity_residual": 0.123}},
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        with duckdb.connect(str(gold_with_ddl)) as conn:
            _persist_causal_estimates_duckdb(conn, ctx, now)
            row = conn.execute(
                "SELECT opportunity_residual FROM feat_causal_estimates WHERE person_id='p1'"
            ).fetchone()

        assert row[0] == pytest.approx(0.123)

    def test_opportunity_residual_null_when_absent(self, gold_with_ddl):
        """When individual_profiles has no opportunity_residual, column stays NULL."""
        from src.pipeline_phases.export_and_viz import _persist_causal_estimates_duckdb

        ctx = _make_context(
            iv_scores={"p1": 1.0},
            credits=[_make_credit("p1", 2015)],
            era_effects=_make_era_effects({2015: 0.5}),
            individual_profiles={},
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        with duckdb.connect(str(gold_with_ddl)) as conn:
            _persist_causal_estimates_duckdb(conn, ctx, now)
            row = conn.execute(
                "SELECT opportunity_residual FROM feat_causal_estimates WHERE person_id='p1'"
            ).fetchone()

        assert row[0] is None

    def test_empty_iv_scores_writes_nothing(self, gold_with_ddl):
        from src.pipeline_phases.export_and_viz import _persist_causal_estimates_duckdb

        ctx = _make_context(
            iv_scores={},
            credits=[],
            era_effects=_make_era_effects({2015: 0.5}),
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        with duckdb.connect(str(gold_with_ddl)) as conn:
            _persist_causal_estimates_duckdb(conn, ctx, now)
            count = conn.execute("SELECT COUNT(*) FROM feat_causal_estimates").fetchone()[0]

        assert count == 0

    def test_multiple_persons(self, gold_with_ddl):
        """Multiple persons each get correct era_fe from their own debut year."""
        from src.pipeline_phases.export_and_viz import _persist_causal_estimates_duckdb

        ctx = _make_context(
            iv_scores={"p1": 2.0, "p2": 3.0},
            credits=[_make_credit("p1", 2015), _make_credit("p2", 2020)],
            era_effects=_make_era_effects({2015: 0.5, 2020: 0.8}),
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        with duckdb.connect(str(gold_with_ddl)) as conn:
            _persist_causal_estimates_duckdb(conn, ctx, now)
            rows = conn.execute(
                "SELECT person_id, era_fe, era_deflated_iv FROM feat_causal_estimates ORDER BY person_id"
            ).fetchall()

        rows_dict = {r[0]: r for r in rows}
        assert rows_dict["p1"][1] == pytest.approx(0.5)
        assert rows_dict["p1"][2] == pytest.approx(1.5)
        assert rows_dict["p2"][1] == pytest.approx(0.8)
        assert rows_dict["p2"][2] == pytest.approx(2.2)
