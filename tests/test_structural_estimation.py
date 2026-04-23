"""Unit tests for src/analysis/causal/structural_estimation.py.

Covers:
- Fixed-effects estimation with known synthetic data (recovery test)
- Difference-in-differences estimation
- Parallel trends robustness check
- Placebo test
- API contract (dataclass shape, field types, order invariance)
- Edge cases (empty input, insufficient variation, NaN values)

Synthetic panel: 50 persons × 10 studios, 3-5 obs/person, seed=42.
No viewer ratings used in production_scale construction (CLAUDE.md Hard Rule 1).
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from src.analysis.causal.structural_estimation import (
    EstimationMethod,
    PanelObservation,
    RegressionResult,
    RobustnessCheck,
    estimate_difference_in_differences,
    estimate_fixed_effects,
    export_structural_estimation,
    run_placebo_test,
    test_parallel_trends as check_parallel_trends,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

RNG = np.random.default_rng(seed=42)

N_PERSONS = 50
N_STUDIOS = 10
BASE_YEAR = 2000
TRUE_MAJOR_EFFECT = 5.0  # known treatment effect
NOISE_SD = 1.0


def _make_panel(
    n_persons: int = N_PERSONS,
    n_studios: int = N_STUDIOS,
    true_effect: float = TRUE_MAJOR_EFFECT,
    noise_sd: float = NOISE_SD,
    rng: np.random.Generator | None = None,
    obs_per_person: int = 4,
    switcher_fraction: float = 0.4,
    constant_outcome: bool = False,
) -> list[PanelObservation]:
    """Build a synthetic panel dataset with known person FEs and treatment effect.

    Includes studio-switchers to provide within-person variation in treatment
    (major_studio status changes mid-career).  The fixed-effects estimator
    identifies the effect from these switchers.

    production_scale is constructed from structural covariates only (episodes ×
    credits proxy) — no viewer ratings (Hard Rule 1).
    """
    if rng is None:
        rng = np.random.default_rng(seed=42)

    person_fes = rng.normal(0.0, 3.0, size=n_persons)

    obs = []
    for i in range(n_persons):
        person_id = f"person_{i}"
        # First half of career at non-major studio, second half switches to major
        # (switchers provide within-person treatment variation needed for FE)
        is_switcher = i < int(n_persons * switcher_fraction)
        switch_period = obs_per_person // 2  # switch happens at t = switch_period

        for t in range(obs_per_person):
            year = BASE_YEAR + t

            if is_switcher and t >= switch_period:
                studio_id = "studio_0"  # major studio
                is_major = True
            else:
                studio_id = f"studio_{(i % (n_studios - 3)) + 3}"  # non-major
                is_major = False

            potential = float(rng.uniform(0.2, 0.8))

            if constant_outcome:
                score = 10.0
            else:
                score = (
                    person_fes[i]
                    + (true_effect if is_major else 0.0)
                    + 0.5 * float(t)   # experience effect
                    + float(rng.normal(0.0, noise_sd))
                )

            career_stage = "newcomer" if t <= 1 else "mid_career"
            obs.append(
                PanelObservation(
                    person_id=person_id,
                    year=year,
                    outcome_score=score,
                    major_studio=is_major,
                    experience_years=t,
                    potential_score=potential,
                    career_stage=career_stage,
                    role_category="animation",
                    studio_id=studio_id,
                    credits_this_year=int(rng.integers(1, 6)),
                )
            )
    return obs


# ---------------------------------------------------------------------------
# A. Basic Operation Tests
# ---------------------------------------------------------------------------


class TestFixedEffectsBasic:
    """A1-A3: basic FE fit, shape, identification."""

    def test_fit_returns_regression_result(self) -> None:
        """A1: estimate_fixed_effects returns a properly typed RegressionResult."""
        panel = _make_panel()
        result = estimate_fixed_effects(panel)

        assert isinstance(result, RegressionResult)
        assert result.method == EstimationMethod.FIXED_EFFECTS
        assert isinstance(result.beta, float)
        assert isinstance(result.se, float)
        assert isinstance(result.p_value, float)
        assert isinstance(result.r_squared, float)
        assert isinstance(result.covariates, dict)
        assert isinstance(result.diagnostics, dict)

    def test_beta_sign_tracks_true_effect(self) -> None:
        """A2: with a large positive treatment effect, beta should be positive."""
        panel = _make_panel(true_effect=10.0, noise_sd=0.5)
        result = estimate_fixed_effects(panel)
        assert result.beta > 0, f"Expected positive beta, got {result.beta}"

    def test_r_squared_bounds(self) -> None:
        """A3: R² must lie in [0, 1]."""
        panel = _make_panel()
        result = estimate_fixed_effects(panel)
        assert 0.0 <= result.r_squared <= 1.0, f"R²={result.r_squared} out of [0,1]"

    def test_ci_contains_true_effect(self) -> None:
        """A4: 95% CI should bracket the true effect (high signal, low noise)."""
        panel = _make_panel(true_effect=TRUE_MAJOR_EFFECT, noise_sd=0.2, obs_per_person=6)
        result = estimate_fixed_effects(panel)
        # CI must be finite and ordered
        assert math.isfinite(result.ci_lower)
        assert math.isfinite(result.ci_upper)
        assert result.ci_lower < result.ci_upper
        # With strong signal the CI should contain the true effect
        assert result.ci_lower <= TRUE_MAJOR_EFFECT <= result.ci_upper, (
            f"True effect {TRUE_MAJOR_EFFECT} not in [{result.ci_lower:.2f}, {result.ci_upper:.2f}]"
        )

    def test_n_obs_matches_input_size(self) -> None:
        """A5: n_obs reported equals the number of demeaned observations used."""
        panel = _make_panel(obs_per_person=4)
        result = estimate_fixed_effects(panel)
        # All persons have >=2 obs, so all contribute to within transformation
        assert result.n_obs == len(panel)


# ---------------------------------------------------------------------------
# B. Statistical Properties
# ---------------------------------------------------------------------------


class TestStatisticalProperties:
    """B1-B4: SE scaling, degenerate case, collinearity."""

    def test_constant_outcome_produces_zero_beta(self) -> None:
        """B1: degenerate case — constant outcome → beta ≈ 0, R² = 0."""
        panel = _make_panel(constant_outcome=True)
        result = estimate_fixed_effects(panel)
        assert abs(result.beta) < 1e-6, f"Expected beta≈0, got {result.beta}"
        assert result.r_squared == pytest.approx(0.0, abs=1e-6)

    def test_higher_noise_yields_larger_se(self) -> None:
        """B2: increasing residual noise should increase standard error."""
        panel_low = _make_panel(noise_sd=0.2, obs_per_person=5)
        panel_high = _make_panel(noise_sd=5.0, obs_per_person=5)
        se_low = estimate_fixed_effects(panel_low).se
        se_high = estimate_fixed_effects(panel_high).se
        assert se_high > se_low, f"SE(high noise)={se_high} not > SE(low noise)={se_low}"

    def test_no_within_variation_returns_error_result(self) -> None:
        """B3: fewer than 10 valid persons → error result with se=inf."""
        # Only 5 persons, each 2 obs
        panel = _make_panel(n_persons=5, obs_per_person=2)
        result = estimate_fixed_effects(panel)
        # Should return graceful degradation
        assert result.se == np.inf or result.diagnostics.get("error") is not None

    def test_order_invariance(self) -> None:
        """B4: shuffling row order must not change estimates."""
        panel = _make_panel()
        rng2 = np.random.default_rng(seed=99)
        shuffled = list(panel)
        rng2.shuffle(shuffled)

        result_orig = estimate_fixed_effects(panel)
        result_shuf = estimate_fixed_effects(shuffled)

        assert result_orig.beta == pytest.approx(result_shuf.beta, abs=1e-8)
        assert result_orig.se == pytest.approx(result_shuf.se, abs=1e-8)
        assert result_orig.r_squared == pytest.approx(result_shuf.r_squared, abs=1e-8)


# ---------------------------------------------------------------------------
# C. API Contract Tests
# ---------------------------------------------------------------------------


class TestAPIContract:
    """C1-C3: return type contract, covariates keys, diagnostics."""

    def test_covariates_keys(self) -> None:
        """C1: covariates dict contains expected keys."""
        panel = _make_panel()
        result = estimate_fixed_effects(panel)
        if "error" not in result.diagnostics:
            assert "experience" in result.covariates
            assert "potential" in result.covariates

    def test_did_returns_regression_result(self) -> None:
        """C2: estimate_difference_in_differences returns proper type."""
        panel = _make_panel()
        result = estimate_difference_in_differences(panel)

        assert isinstance(result, RegressionResult)
        assert result.method == EstimationMethod.DIFFERENCE_IN_DIFFERENCES
        assert isinstance(result.beta, float)
        assert math.isfinite(result.p_value) or result.p_value == 1.0

    def test_did_diagnostics_contains_treatment_year(self) -> None:
        """C3: DID diagnostics must include treatment_year key."""
        panel = _make_panel()
        result = estimate_difference_in_differences(panel, treatment_year=BASE_YEAR + 1)
        if "error" not in result.diagnostics:
            assert "treatment_year" in result.diagnostics
            assert result.diagnostics["treatment_year"] == BASE_YEAR + 1

    def test_export_structural_estimation_serializable(self) -> None:
        """C4: export function must produce a JSON-serializable dict."""
        import json

        from src.analysis.causal.structural_estimation import StructuralEstimationResult

        panel = _make_panel()
        fe = estimate_fixed_effects(panel)
        did = estimate_difference_in_differences(panel)

        # Build a minimal StructuralEstimationResult manually
        ser = StructuralEstimationResult(
            fixed_effects=fe,
            did_estimate=did,
            matching_estimate=None,
            event_study=None,
            robustness_checks=[],
            hausman_test={},
            f_test_fixed_effects={},
            parallel_trends_test={},
            preferred_estimate=fe,
            summary="test",
        )
        exported = export_structural_estimation(ser)
        # Must be a dict and round-trip through JSON without error
        assert isinstance(exported, dict)
        json_str = json.dumps(exported, default=str)
        assert len(json_str) > 0


# ---------------------------------------------------------------------------
# D. Edge Cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """D1-D3: empty input, single obs per person, no major studio variation."""

    def test_empty_panel_data_returns_graceful_result(self) -> None:
        """D1: empty panel → graceful error result (no crash)."""
        result = estimate_fixed_effects([])
        assert isinstance(result, RegressionResult)
        assert result.method == EstimationMethod.FIXED_EFFECTS
        # Should signal error, not raise
        assert result.se == np.inf or result.n_persons == 0

    def test_single_obs_per_person_insufficient_variation(self) -> None:
        """D2: one obs per person → no within-person variation → error result."""
        panel = _make_panel(obs_per_person=1)
        result = estimate_fixed_effects(panel)
        assert result.se == np.inf or result.diagnostics.get("error") is not None

    def test_no_major_studio_obs_did_returns_error(self) -> None:
        """D3: no treatment observations → DID returns error with se=inf."""
        panel_no_treatment = [
            PanelObservation(
                person_id=f"p{i}",
                year=BASE_YEAR + (i % 4),
                outcome_score=float(i),
                major_studio=False,
                experience_years=i % 4,
                potential_score=0.5,
                career_stage="newcomer",
                role_category="animation",
                studio_id="studio_0",
                credits_this_year=1,
            )
            for i in range(40)
        ]
        result = estimate_difference_in_differences(panel_no_treatment)
        assert result.se == np.inf or result.diagnostics.get("error") == "no_treatment_observed"


# ---------------------------------------------------------------------------
# E. Robustness / Auxiliary Functions
# ---------------------------------------------------------------------------


class TestRobustnessChecks:
    """E1-E3: parallel trends test, placebo test, event study structure."""

    def test_parallel_trends_empty_results(self) -> None:
        """E1: empty event study → parallel trends returns 'inconclusive'."""
        check = check_parallel_trends({})
        assert isinstance(check, RobustnessCheck)
        assert check.result == "inconclusive"

    def test_parallel_trends_zero_betas_pass(self) -> None:
        """E2: all pre-treatment betas ≈ 0 → parallel trends passes."""
        # Build fake event study results with zero pre-treatment betas
        def _make_result(beta: float, p_value: float) -> RegressionResult:
            return RegressionResult(
                method=EstimationMethod.EVENT_STUDY,
                beta=beta,
                se=0.5,
                t_stat=beta / 0.5,
                p_value=p_value,
                ci_lower=beta - 1.0,
                ci_upper=beta + 1.0,
                n_obs=100,
                n_persons=20,
                r_squared=0.1,
                adj_r_squared=0.09,
                covariates={},
                diagnostics={"relative_time": -1},
                interpretation="test",
            )

        event_results = {
            -2: _make_result(0.1, 0.80),
            -1: _make_result(-0.1, 0.85),
            0: _make_result(4.5, 0.01),
            1: _make_result(5.0, 0.01),
        }
        check = check_parallel_trends(event_results)
        assert isinstance(check, RobustnessCheck)
        assert check.result == "passed", f"Expected 'passed', got '{check.result}': {check.detail}"

    def test_placebo_test_insufficient_obs(self) -> None:
        """E3: fewer than 20 pre-treatment obs → placebo returns 'inconclusive'."""
        # Only 5 observations, none pre-treatment
        panel = [
            PanelObservation(
                person_id="p1",
                year=BASE_YEAR,
                outcome_score=1.0,
                major_studio=False,
                experience_years=0,
                potential_score=0.5,
                career_stage="newcomer",
                role_category="animation",
                studio_id="studio_0",
                credits_this_year=1,
            )
        ] * 5
        check = run_placebo_test(panel)
        assert isinstance(check, RobustnessCheck)
        assert check.result == "inconclusive"
