"""Tests for IV transparent decomposition.

Covers:
- IV reconstruction: Σ contrib_pct == 100 % (up to floating point)
- Component contribution non-negativity when all components positive
- Cohort label generation (decade × role group)
- Correlation check: high-corr triggers Shapley fallback
- Decompose result matches stored iv_scores within 1e-6
- rebuild_iv_from_components internal consistency
- Percentile within cohort bounded in [0, 100]
"""

from __future__ import annotations

import pytest

from src.analysis.scoring.iv_decomposition import (
    HIGH_CORR_THRESHOLD,
    CorrelationReport,
    _decade_label,
    _percentile_within,
    _primary_role_group,
    build_cohort_labels,
    build_person_cohort_data_from_scores,
    compute_component_correlations,
    decompose_iv_for_person,
    rebuild_iv_from_components,
    verify_iv_reconstruction,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def five_person_setup():
    """Minimal 5-person dataset with all components and cohort data."""
    pids = ["p1", "p2", "p3", "p4", "p5"]

    raw_components = {
        "person_fe":      {p: 0.1 * (i + 1) for i, p in enumerate(pids)},
        "birank":         {p: 0.08 * (i + 1) for i, p in enumerate(pids)},
        "studio_exposure":{p: 0.05 * (i + 1) for i, p in enumerate(pids)},
        "awcc":           {p: 0.06 * (i + 1) for i, p in enumerate(pids)},
        "patronage":      {p: 0.03 * (i + 1) for i, p in enumerate(pids)},
    }
    dormancy = {p: 1.0 for p in pids}
    dormancy["p1"] = 0.7  # p1 has partial dormancy

    # Simple iv_scores (mock, not recomputed from PCA)
    iv_scores = {p: 0.1 * (i + 1) for i, p in enumerate(pids)}

    # Build component breakdown (λ = 0.2 equal weights)
    lambdas = {name: 0.2 for name in raw_components}
    component_breakdown = {}
    for p in pids:
        bd = {name: lambdas[name] * raw_components[name][p] for name in raw_components}
        bd["dormancy"] = dormancy[p]
        component_breakdown[p] = bd

    last_credit_years = {"p1": 2018, "p2": 2020, "p3": 2022, "p4": 2023, "p5": 2024}

    cohort_labels = {
        "p1": "2000s_animation",
        "p2": "2000s_animation",
        "p3": "2010s_animation",
        "p4": "2010s_animation",
        "p5": "2010s_animation",
    }

    return {
        "pids": pids,
        "raw_components": raw_components,
        "dormancy": dormancy,
        "iv_scores": iv_scores,
        "lambdas": lambdas,
        "component_breakdown": component_breakdown,
        "last_credit_years": last_credit_years,
        "cohort_labels": cohort_labels,
    }


@pytest.fixture
def no_corr_report():
    """CorrelationReport with no high-correlation pairs."""
    return CorrelationReport(
        matrix=[],
        component_names=["person_fe", "birank", "studio_exposure", "awcc", "patronage"],
        max_abs_r=0.3,
        high_corr_pairs=[],
        shapley_fallback_triggered=False,
    )


@pytest.fixture
def high_corr_report():
    """CorrelationReport with a high-correlation pair triggering Shapley fallback."""
    return CorrelationReport(
        matrix=[],
        component_names=["person_fe", "birank"],
        max_abs_r=0.95,
        high_corr_pairs=[("person_fe", "birank", 0.95)],
        shapley_fallback_triggered=True,
    )


# ---------------------------------------------------------------------------
# Unit tests: cohort utilities
# ---------------------------------------------------------------------------


class TestDecadeLabel:
    def test_2015_is_2010s(self):
        assert _decade_label(2015) == "2010s"

    def test_2000_is_2000s(self):
        assert _decade_label(2000) == "2000s"

    def test_1999_is_1990s(self):
        assert _decade_label(1999) == "1990s"

    def test_none_returns_unknown(self):
        assert _decade_label(None) == "unknown"

    def test_very_old_year(self):
        assert _decade_label(1955) == "pre1960s"

    def test_2020s_boundary(self):
        assert _decade_label(2020) == "2020s"
        assert _decade_label(2025) == "2020s"


class TestPrimaryRoleGroup:
    def test_key_animator_maps_to_animation(self):
        # key_animator → animation
        result = _primary_role_group({"key_animator": 5, "in_between": 2})
        assert result == "animation"

    def test_director_maps_to_direction(self):
        result = _primary_role_group({"director": 10})
        assert result == "direction"

    def test_empty_returns_other(self):
        assert _primary_role_group({}) == "other"


class TestBuildCohortLabels:
    def test_basic_label_format(self):
        debut = {"p1": 2012, "p2": 2003}
        roles = {"p1": "animation", "p2": "direction"}
        labels = build_cohort_labels(debut, roles)
        assert labels["p1"] == "2010s_animation"
        assert labels["p2"] == "2000s_direction"

    def test_missing_debut_year(self):
        debut = {"p1": None}
        roles = {"p1": "animation"}
        labels = build_cohort_labels(debut, roles)
        assert labels["p1"] == "unknown_animation"

    def test_missing_role_defaults_to_other(self):
        debut = {"p1": 2015}
        roles = {}
        labels = build_cohort_labels(debut, roles)
        assert labels["p1"] == "2010s_other"


# ---------------------------------------------------------------------------
# Unit tests: percentile
# ---------------------------------------------------------------------------


class TestPercentileWithin:
    def test_lowest_value_is_zero(self):
        vals = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert _percentile_within(1.0, vals) == 0

    def test_highest_value_is_80(self):
        # 4 values strictly below 5.0 → 4/5*100 = 80
        vals = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert _percentile_within(5.0, vals) == 80

    def test_empty_returns_50(self):
        assert _percentile_within(1.0, []) == 50

    def test_single_value(self):
        assert _percentile_within(3.0, [3.0]) == 0


# ---------------------------------------------------------------------------
# Unit tests: correlation check
# ---------------------------------------------------------------------------


class TestComputeComponentCorrelations:
    def test_highly_correlated_triggers_warning(self):
        """Perfectly correlated components should trigger Shapley fallback."""
        pids = [f"p{i}" for i in range(20)]
        base = {p: float(i) for i, p in enumerate(pids)}
        components = {
            "person_fe": dict(base),
            "birank": dict(base),  # r = 1.0 with person_fe
            "studio_exposure": {p: 0.1 * i for i, p in enumerate(pids)},
            "awcc": {p: float(i) * 0.5 for i, p in enumerate(pids)},
            "patronage": {p: float(i) * 0.3 for i, p in enumerate(pids)},
        }
        report = compute_component_correlations(components)
        assert report.shapley_fallback_triggered is True
        assert report.max_abs_r > HIGH_CORR_THRESHOLD
        assert len(report.high_corr_pairs) > 0

    def test_orthogonal_components_no_fallback(self):
        """Independent components should NOT trigger fallback."""
        import numpy as np

        rng = np.random.default_rng(42)
        n = 50
        pids = [f"p{i}" for i in range(n)]
        components = {
            "person_fe": {p: float(rng.normal()) for p in pids},
            "birank": {p: float(rng.normal()) for p in pids},
            "studio_exposure": {p: float(rng.normal()) for p in pids},
            "awcc": {p: float(rng.normal()) for p in pids},
            "patronage": {p: float(rng.normal()) for p in pids},
        }
        report = compute_component_correlations(components)
        # With pure noise, max_abs_r should be well below 0.9
        assert report.max_abs_r < HIGH_CORR_THRESHOLD

    def test_too_few_persons_no_crash(self):
        """Less than 3 common persons returns empty report without error."""
        components = {
            "person_fe": {"p1": 1.0},
            "birank": {"p1": 2.0},
        }
        report = compute_component_correlations(components)
        assert report.matrix == []
        assert report.max_abs_r == 0.0

    def test_single_component_returns_empty(self):
        """Only one component — cannot compute a matrix."""
        components = {"person_fe": {"p1": 1.0, "p2": 2.0, "p3": 3.0}}
        report = compute_component_correlations(components)
        assert report.matrix == []


# ---------------------------------------------------------------------------
# Unit tests: decompose_iv_for_person
# ---------------------------------------------------------------------------


class TestDecomposeIvForPerson:
    def test_contrib_pct_sums_to_100(self, five_person_setup, no_corr_report):
        """Contribution percentages must sum to 100 for each person."""
        s = five_person_setup
        for pid in s["pids"]:
            result = decompose_iv_for_person(
                person_id=pid,
                iv_scores=s["iv_scores"],
                component_breakdown=s["component_breakdown"],
                lambda_weights=s["lambdas"],
                dormancy=s["dormancy"],
                last_credit_years=s["last_credit_years"],
                cohort_labels=s["cohort_labels"],
                raw_components=s["raw_components"],
                correlation_report=no_corr_report,
            )
            assert result is not None
            total_pct = sum(cd.contrib_pct for cd in result.components.values())
            assert abs(total_pct - 100.0) < 1e-3, (
                f"{pid}: contrib_pct sum = {total_pct:.6f}, expected 100.0"
            )

    def test_stored_iv_matches_result_iv(self, five_person_setup, no_corr_report):
        """result.iv must match iv_scores[person_id] within 1e-6."""
        s = five_person_setup
        for pid in s["pids"]:
            result = decompose_iv_for_person(
                person_id=pid,
                iv_scores=s["iv_scores"],
                component_breakdown=s["component_breakdown"],
                lambda_weights=s["lambdas"],
                dormancy=s["dormancy"],
                last_credit_years=s["last_credit_years"],
                cohort_labels=s["cohort_labels"],
                raw_components=s["raw_components"],
                correlation_report=no_corr_report,
            )
            assert result is not None
            assert abs(result.iv - s["iv_scores"][pid]) < 1e-6

    def test_cohort_percentile_in_valid_range(self, five_person_setup, no_corr_report):
        """Cohort percentile must be in [0, 100]."""
        s = five_person_setup
        for pid in s["pids"]:
            result = decompose_iv_for_person(
                person_id=pid,
                iv_scores=s["iv_scores"],
                component_breakdown=s["component_breakdown"],
                lambda_weights=s["lambdas"],
                dormancy=s["dormancy"],
                last_credit_years=s["last_credit_years"],
                cohort_labels=s["cohort_labels"],
                raw_components=s["raw_components"],
                correlation_report=no_corr_report,
            )
            assert result is not None
            assert 0 <= result.percentile_in_cohort <= 100
            for cd in result.components.values():
                assert 0 <= cd.cohort_pctl <= 100

    def test_dormancy_stored_correctly(self, five_person_setup, no_corr_report):
        """Dormancy multiplier in result matches input dormancy."""
        s = five_person_setup
        result = decompose_iv_for_person(
            person_id="p1",
            iv_scores=s["iv_scores"],
            component_breakdown=s["component_breakdown"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
            raw_components=s["raw_components"],
            correlation_report=no_corr_report,
        )
        assert result is not None
        assert abs(result.dormancy.D - 0.7) < 1e-9

    def test_last_credit_year_stored(self, five_person_setup, no_corr_report):
        """last_credit_year is passed through correctly."""
        s = five_person_setup
        result = decompose_iv_for_person(
            person_id="p2",
            iv_scores=s["iv_scores"],
            component_breakdown=s["component_breakdown"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
            raw_components=s["raw_components"],
            correlation_report=no_corr_report,
        )
        assert result is not None
        assert result.dormancy.last_credit_year == 2020

    def test_unknown_person_returns_none(self, five_person_setup, no_corr_report):
        """Missing person_id returns None."""
        s = five_person_setup
        result = decompose_iv_for_person(
            person_id="ghost_99",
            iv_scores=s["iv_scores"],
            component_breakdown=s["component_breakdown"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
            raw_components=s["raw_components"],
            correlation_report=no_corr_report,
        )
        assert result is None

    def test_shapley_fallback_flag_set(self, five_person_setup, high_corr_report):
        """When Shapley fallback triggered, result.shapley_fallback is True."""
        s = five_person_setup
        result = decompose_iv_for_person(
            person_id="p1",
            iv_scores=s["iv_scores"],
            component_breakdown=s["component_breakdown"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
            raw_components=s["raw_components"],
            correlation_report=high_corr_report,
        )
        assert result is not None
        assert result.shapley_fallback is True
        assert "Shapley" in result.method_note

    def test_no_fallback_flag_when_low_corr(self, five_person_setup, no_corr_report):
        """No Shapley fallback when correlation is low."""
        s = five_person_setup
        result = decompose_iv_for_person(
            person_id="p3",
            iv_scores=s["iv_scores"],
            component_breakdown=s["component_breakdown"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
            raw_components=s["raw_components"],
            correlation_report=no_corr_report,
        )
        assert result is not None
        assert result.shapley_fallback is False

    def test_cohort_size_correct(self, five_person_setup, no_corr_report):
        """Cohort size reflects number of persons in same cohort group."""
        s = five_person_setup
        # p3, p4, p5 are in 2010s_animation → cohort_size = 3
        result = decompose_iv_for_person(
            person_id="p3",
            iv_scores=s["iv_scores"],
            component_breakdown=s["component_breakdown"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
            raw_components=s["raw_components"],
            correlation_report=no_corr_report,
        )
        assert result is not None
        assert result.cohort == "2010s_animation"
        assert result.cohort_size == 3


# ---------------------------------------------------------------------------
# Unit tests: reconstruct / verify
# ---------------------------------------------------------------------------


class TestIVReconstruction:
    def test_verify_iv_reconstruction_passes_for_matching_iv(self, five_person_setup):
        """verify_iv_reconstruction passes when result.iv equals iv_scores[pid]."""
        s = five_person_setup
        from src.analysis.scoring.iv_decomposition import (
            CorrelationReport,
            decompose_iv_for_person,
        )
        no_corr = CorrelationReport(
            matrix=[],
            component_names=list(s["raw_components"].keys()),
            max_abs_r=0.0,
            high_corr_pairs=[],
            shapley_fallback_triggered=False,
        )
        for pid in s["pids"]:
            result = decompose_iv_for_person(
                person_id=pid,
                iv_scores=s["iv_scores"],
                component_breakdown=s["component_breakdown"],
                lambda_weights=s["lambdas"],
                dormancy=s["dormancy"],
                last_credit_years=s["last_credit_years"],
                cohort_labels=s["cohort_labels"],
                raw_components=s["raw_components"],
                correlation_report=no_corr,
            )
            assert result is not None
            ok = verify_iv_reconstruction(
                person_id=pid,
                iv_result=result,
                iv_scores=s["iv_scores"],
                component_breakdown=s["component_breakdown"],
                dormancy=s["dormancy"],
                tol=1e-6,
            )
            assert ok, f"{pid}: IV reconstruction failed"

    def test_rebuild_iv_internal_consistency(self, five_person_setup):
        """rebuild_iv_from_components: component sum is internally consistent."""
        s = five_person_setup
        for pid in s["pids"]:
            ok, err = rebuild_iv_from_components(
                person_id=pid,
                component_breakdown=s["component_breakdown"],
                dormancy=s["dormancy"],
                iv_scores_renorm=s["iv_scores"],
                tol=1e-6,
            )
            assert ok, f"{pid}: internal consistency error = {err:.2e}"
            assert err < 1e-6


# ---------------------------------------------------------------------------
# Unit tests: build_person_cohort_data_from_scores
# ---------------------------------------------------------------------------


class TestBuildPersonCohortDataFromScores:
    def test_extracts_debut_years_and_roles(self):
        rows = [
            {"person_id": "p1", "first_year": 2012, "primary_role": "key_animator"},
            {"person_id": "p2", "first_year": 2001, "primary_role": "director"},
        ]
        debut, roles = build_person_cohort_data_from_scores(rows)
        assert debut["p1"] == 2012
        assert debut["p2"] == 2001
        assert roles["p1"] == "animation"
        assert roles["p2"] == "direction"

    def test_missing_first_year_is_none(self):
        rows = [{"person_id": "p1", "first_year": None, "primary_role": "key_animator"}]
        debut, _ = build_person_cohort_data_from_scores(rows)
        assert debut["p1"] is None

    def test_empty_rows_returns_empty_dicts(self):
        debut, roles = build_person_cohort_data_from_scores([])
        assert debut == {}
        assert roles == {}

    def test_missing_person_id_skipped(self):
        rows = [{"person_id": None, "first_year": 2010, "primary_role": "director"}]
        debut, roles = build_person_cohort_data_from_scores(rows)
        assert len(debut) == 0
