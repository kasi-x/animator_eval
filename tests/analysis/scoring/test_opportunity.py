"""Tests for opportunity residual panel OLS + analytical CI + permutation null.

Covers:
- OLS residual computation
- Analytical SE = σ/√n (H4 compliance)
- CI95 bounds and coverage calibration (95% ±2pp)
- Permutation null model and empirical p-value
- Insufficient data guard
- Integration with individual_contribution pipeline
"""

from __future__ import annotations

import numpy as np
import pytest

from src.analysis.scoring.opportunity import (
    MIN_PANEL_OBS,
    OpportunityResidualResult,
    compute_opportunity_residual_panel,
)
from src.analysis.scoring.individual_contribution import (
    IndividualProfile,
    IndividualContributionResult,
    compute_opportunity_residual,
    compute_opportunity_residual_full,
    compute_individual_profiles,
)
from src.runtime.models import BronzeAnime as Anime, Credit, Role


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_anime(aid: str, year: int = 2020, studios: list[str] | None = None) -> Anime:
    return Anime(
        id=aid,
        title_ja=f"Anime {aid}",
        title_en=f"Anime {aid}",
        year=year,
        studios=studios or ["StudioA"],
        source="test",
    )


def _make_credit(
    person_id: str, anime_id: str, role: Role = Role.KEY_ANIMATOR
) -> Credit:
    return Credit(person_id=person_id, anime_id=anime_id, role=role, source="test")


def _make_features(n: int = 20, seed: int = 0) -> dict[str, dict]:
    """Generate synthetic feature dicts for n persons."""
    rng = np.random.default_rng(seed)
    return {
        f"p{i}": {
            "credit_count": int(rng.integers(2, 30)),
            "career_years": int(rng.integers(1, 15)),
            "unique_studios": int(rng.integers(1, 5)),
            "primary_role": rng.choice(["director", "key_animator", "animator"]),
            "iv_score": float(rng.normal(50, 10)),
            "avg_staff_count": float(rng.integers(5, 40)),
            "career_band": "5-9y",
            "work_count": int(rng.integers(1, 15)),
        }
        for i in range(n)
    }


# ---------------------------------------------------------------------------
# OpportunityResidualResult dataclass
# ---------------------------------------------------------------------------


class TestOpportunityResidualResult:
    def test_fields_present(self):
        r = OpportunityResidualResult(
            residual=0.1, se=0.05, ci_lower=-0.0, ci_upper=0.2, n_years=4
        )
        assert r.residual == pytest.approx(0.1)
        assert r.se == pytest.approx(0.05)
        assert r.ci_lower == pytest.approx(-0.0)
        assert r.ci_upper == pytest.approx(0.2)
        assert r.n_years == 4
        assert r.p_value_permutation is None

    def test_p_value_optional(self):
        r = OpportunityResidualResult(
            residual=0.5, se=0.1, ci_lower=0.3, ci_upper=0.7,
            n_years=5, p_value_permutation=0.04
        )
        assert r.p_value_permutation == pytest.approx(0.04)


# ---------------------------------------------------------------------------
# compute_opportunity_residual_panel — core OLS
# ---------------------------------------------------------------------------


class TestComputeOpportunityResidualPanel:
    def test_returns_result_for_all_persons(self):
        features = _make_features(n=20)
        results, summary = compute_opportunity_residual_panel(
            features, n_permutations=0
        )
        assert len(results) == 20
        for pid, r in results.items():
            assert isinstance(r, OpportunityResidualResult)

    def test_residuals_are_not_all_none(self):
        features = _make_features(n=20)
        results, _ = compute_opportunity_residual_panel(features, n_permutations=0)
        non_null = [r for r in results.values() if r.residual is not None]
        assert len(non_null) >= MIN_PANEL_OBS

    def test_residuals_mean_near_zero(self):
        """OLS residuals must have mean ≈ 0 (arithmetic property)."""
        features = _make_features(n=30)
        results, _ = compute_opportunity_residual_panel(features, n_permutations=0)
        vals = [r.residual for r in results.values() if r.residual is not None]
        assert abs(np.mean(vals)) < 0.1

    def test_r_squared_in_unit_interval(self):
        features = _make_features(n=25)
        _, summary = compute_opportunity_residual_panel(features, n_permutations=0)
        assert summary.r_squared is not None
        assert 0.0 <= summary.r_squared <= 1.0

    def test_insufficient_data_returns_none_results(self):
        """Fewer than MIN_PANEL_OBS persons → all results are None."""
        features = _make_features(n=MIN_PANEL_OBS - 1)
        results, summary = compute_opportunity_residual_panel(
            features, n_permutations=0
        )
        assert summary.r_squared is None
        assert all(r.residual is None for r in results.values())

    def test_theta_map_accepted(self):
        """Passing theta_map does not raise; results still populated."""
        features = _make_features(n=20)
        theta = {pid: float(np.random.default_rng(1).normal()) for pid in features}
        results, summary = compute_opportunity_residual_panel(
            features, theta_map=theta, n_permutations=0
        )
        assert len(results) == 20
        non_null = [r for r in results.values() if r.residual is not None]
        assert len(non_null) >= MIN_PANEL_OBS

    def test_summary_n_persons_matches(self):
        features = _make_features(n=25)
        _, summary = compute_opportunity_residual_panel(features, n_permutations=0)
        assert summary.n_persons == 25

    def test_single_role_no_crash(self):
        """All persons with same role — dummy encoding returns empty; OLS still works."""
        n = 20
        features = {
            f"p{i}": {
                "credit_count": i + 2,
                "career_years": i,
                "unique_studios": 1,
                "primary_role": "key_animator",
                "iv_score": 50.0,
            }
            for i in range(n)
        }
        results, summary = compute_opportunity_residual_panel(
            features, n_permutations=0
        )
        assert len(results) == n

    def test_empty_features_returns_empty(self):
        results, summary = compute_opportunity_residual_panel({}, n_permutations=0)
        assert len(results) == 0
        assert summary.r_squared is None


# ---------------------------------------------------------------------------
# Analytical CI (H4 compliance: SE = σ/√n, not heuristic)
# ---------------------------------------------------------------------------


class TestAnalyticalCI:
    def test_ci_present_when_data_sufficient(self):
        """Every person with ≥ MIN_YEARS_FOR_CI rows should have a CI."""
        # In the cross-section fallback each person has exactly 1 row,
        # so CI will be None for everyone (n_years=1 < 2).
        # We test the CI formula directly via the panel module's internals.
        from src.analysis.scoring.opportunity import _compute_analytical_ci

        # Simulate 4 residual draws per person
        residuals_by_person = {
            "p1": [0.1, -0.2, 0.3, 0.0],
            "p2": [1.0, 1.1, 0.9, 1.2],
        }
        results = _compute_analytical_ci(residuals_by_person)

        # p1 CI should contain 0 (residuals near 0)
        r1 = results["p1"]
        assert r1.ci_lower is not None
        assert r1.ci_lower <= r1.residual <= r1.ci_upper

        # p2 CI should be positive (all residuals > 0)
        r2 = results["p2"]
        assert r2.ci_lower > 0

    def test_se_equals_sigma_over_sqrt_n(self):
        """Verify SE formula exactly: SE = std(residuals) / sqrt(n)."""
        from src.analysis.scoring.opportunity import _compute_analytical_ci

        vals = [0.5, 1.5, -0.5, 0.0, 1.0]
        results = _compute_analytical_ci({"p": vals})
        r = results["p"]
        expected_se = float(np.std(vals, ddof=1) / np.sqrt(len(vals)))
        assert r.se == pytest.approx(expected_se, rel=1e-6)

    def test_ci95_width_equals_two_t_se(self):
        """CI width = 2 × t_{n-1, 0.975} × SE (t-distribution, not z=1.96)."""
        from src.analysis.scoring.opportunity import _compute_analytical_ci, _t_critical

        vals = [1.0, 2.0, 1.5, 1.8, 1.2]
        n = len(vals)
        results = _compute_analytical_ci({"p": vals})
        r = results["p"]
        width = r.ci_upper - r.ci_lower
        t_crit = _t_critical(n)
        expected = 2 * t_crit * r.se
        assert width == pytest.approx(expected, rel=1e-6)

    def test_ci_none_when_single_observation(self):
        """n_years=1 → SE undefined → CI must be None."""
        from src.analysis.scoring.opportunity import _compute_analytical_ci

        results = _compute_analytical_ci({"p": [0.5]})
        r = results["p"]
        assert r.se is None
        assert r.ci_lower is None
        assert r.ci_upper is None

    def test_ci_coverage_calibration(self):
        """Empirical 95% CI coverage must be 95% ±3pp under the null.

        We inject per-person multiple residual observations (simulating multi-year
        panel) to trigger CI computation.
        """
        from src.analysis.scoring.opportunity import _compute_analytical_ci

        rng = np.random.default_rng(42)
        n_sim = 1_000
        n_per_person = 8
        coverage_hits = 0

        for _ in range(n_sim):
            # True mean is 0 for each person
            obs = rng.normal(loc=0.0, scale=1.0, size=n_per_person).tolist()
            results = _compute_analytical_ci({"p": obs})
            r = results["p"]
            if r.ci_lower is not None and r.ci_lower <= 0.0 <= r.ci_upper:
                coverage_hits += 1

        empirical_coverage = coverage_hits / n_sim
        # 95% CI should capture true mean in ≈95% of replications (±3pp tolerance)
        assert abs(empirical_coverage - 0.95) <= 0.03, (
            f"CI coverage {empirical_coverage:.3f} deviates from 95% by more than 3pp"
        )


# ---------------------------------------------------------------------------
# Permutation null model
# ---------------------------------------------------------------------------


class TestPermutationNull:
    def test_p_values_in_unit_interval(self):
        features = _make_features(n=20, seed=7)
        results, _ = compute_opportunity_residual_panel(
            features, n_permutations=100, rng=np.random.default_rng(0)
        )
        for r in results.values():
            if r.p_value_permutation is not None:
                assert 0.0 <= r.p_value_permutation <= 1.0

    def test_p_values_present_when_n_permutations_positive(self):
        features = _make_features(n=20)
        results, _ = compute_opportunity_residual_panel(
            features, n_permutations=50, rng=np.random.default_rng(1)
        )
        # All persons in panel should have p-values
        pids_in_panel = [
            pid for pid, r in results.items() if r.residual is not None
        ]
        for pid in pids_in_panel:
            assert results[pid].p_value_permutation is not None

    def test_p_values_absent_when_n_permutations_zero(self):
        features = _make_features(n=20)
        results, _ = compute_opportunity_residual_panel(
            features, n_permutations=0
        )
        for r in results.values():
            assert r.p_value_permutation is None

    def test_null_distribution_is_uniform_under_null(self):
        """Under the null (y ~ N(0,1) independent of X), p-values should be
        uniform (not consistently near 0 or 1).
        """
        rng = np.random.default_rng(99)
        n = 30
        # All features are identical (no systematic pattern) — near-null scenario
        features = {
            f"p{i}": {
                "credit_count": 10,
                "career_years": 5,
                "unique_studios": 2,
                "primary_role": "key_animator",
                "iv_score": 50.0,
            }
            for i in range(n)
        }
        results, _ = compute_opportunity_residual_panel(
            features, n_permutations=200, rng=rng
        )
        p_vals = [
            r.p_value_permutation
            for r in results.values()
            if r.p_value_permutation is not None
        ]
        if p_vals:
            # Under null, mean p should be near 0.5 (uniform distribution)
            # We use a loose bound to account for variance with 200 permutations
            assert np.mean(p_vals) > 0.1, (
                f"Mean p-value {np.mean(p_vals):.3f} is suspiciously low under the null"
            )

    def test_permutation_reproducible_with_fixed_seed(self):
        features = _make_features(n=20, seed=42)
        rng1 = np.random.default_rng(0)
        rng2 = np.random.default_rng(0)
        r1, _ = compute_opportunity_residual_panel(features, n_permutations=50, rng=rng1)
        r2, _ = compute_opportunity_residual_panel(features, n_permutations=50, rng=rng2)
        for pid in r1:
            if r1[pid].p_value_permutation is not None:
                assert r1[pid].p_value_permutation == r2[pid].p_value_permutation


# ---------------------------------------------------------------------------
# Individual contribution module integration
# ---------------------------------------------------------------------------


class TestComputeOpportunityResidualWrapper:
    """Tests for the backward-compatible wrapper in individual_contribution."""

    def test_returns_float_dict_and_r_squared(self):
        features = _make_features(n=20)
        residuals, r_sq = compute_opportunity_residual(features, n_permutations=0)
        assert isinstance(residuals, dict)
        assert len(residuals) == 20
        # r_squared may be None if panel is too small, but should be float otherwise
        if r_sq is not None:
            assert 0.0 <= r_sq <= 1.0

    def test_insufficient_data_returns_none_values(self):
        features = _make_features(n=MIN_PANEL_OBS - 1)
        residuals, r_sq = compute_opportunity_residual(features, n_permutations=0)
        assert r_sq is None
        assert all(v is None for v in residuals.values())


class TestComputeOpportunityResidualFull:
    """Tests for the full-result API returning OpportunityResidualResult."""

    def test_returns_opportunity_result_objects(self):
        features = _make_features(n=20)
        results, r_sq = compute_opportunity_residual_full(
            features, n_permutations=0
        )
        for r in results.values():
            assert isinstance(r, OpportunityResidualResult)

    def test_ci_fields_present_on_result(self):
        """Full result should expose CI fields (even if None for n_years<2)."""
        features = _make_features(n=20)
        results, _ = compute_opportunity_residual_full(features, n_permutations=0)
        for r in results.values():
            # Check attributes exist (values may be None)
            assert hasattr(r, "se")
            assert hasattr(r, "ci_lower")
            assert hasattr(r, "ci_upper")
            assert hasattr(r, "p_value_permutation")


class TestIndividualProfileCIFields:
    """IndividualProfile dataclass must carry CI + p-value fields."""

    def test_profile_has_ci_fields(self):
        profile = IndividualProfile(person_id="p1")
        assert hasattr(profile, "opportunity_residual_se")
        assert hasattr(profile, "opportunity_residual_ci_lower")
        assert hasattr(profile, "opportunity_residual_ci_upper")
        assert hasattr(profile, "opportunity_residual_p_value")

    def test_profile_ci_defaults_to_none(self):
        profile = IndividualProfile(person_id="p1")
        assert profile.opportunity_residual_se is None
        assert profile.opportunity_residual_ci_lower is None
        assert profile.opportunity_residual_ci_upper is None
        assert profile.opportunity_residual_p_value is None


class TestComputeIndividualProfilesWithCI:
    """Integration test: CI fields propagate through compute_individual_profiles."""

    @pytest.fixture
    def setup_data(self):
        anime_map = {
            f"a{i}": _make_anime(f"a{i}", year=2015 + i, studios=["StudioA"])
            for i in range(10)
        }
        credits = []
        for d in range(5):
            credits.append(_make_credit(f"dir{d}", f"a{d * 2}", Role.DIRECTOR))
            credits.append(_make_credit(f"dir{d}", f"a{d * 2 + 1}", Role.DIRECTOR))
        for ka in range(10):
            for a in range(min(ka + 1, 10)):
                credits.append(_make_credit(f"ka{ka}", f"a{a}", Role.KEY_ANIMATOR))
        results_list = [
            {"person_id": f"dir{d}", "iv_score": 65 + d * 4} for d in range(5)
        ] + [
            {"person_id": f"ka{ka}", "iv_score": 42 + ka * 3} for ka in range(10)
        ]
        role_profiles = {
            **{f"dir{d}": {"primary_role": "director"} for d in range(5)},
            **{f"ka{ka}": {"primary_role": "key_animator"} for ka in range(10)},
        }
        career_data = {
            **{f"dir{d}": {"active_years": 10 + d * 2} for d in range(5)},
            **{f"ka{ka}": {"active_years": 3 + ka} for ka in range(10)},
        }
        return dict(
            results=results_list,
            credits=credits,
            anime_map=anime_map,
            role_profiles=role_profiles,
            career_data=career_data,
        )

    def test_profiles_have_ci_fields(self, setup_data):
        result = compute_individual_profiles(
            **setup_data,
            opportunity_n_permutations=0,
        )
        assert isinstance(result, IndividualContributionResult)
        for pid, profile in result.profiles.items():
            assert "opportunity_residual_se" in profile
            assert "opportunity_residual_ci_lower" in profile
            assert "opportunity_residual_ci_upper" in profile
            assert "opportunity_residual_p_value" in profile

    def test_existing_fields_still_present(self, setup_data):
        result = compute_individual_profiles(
            **setup_data,
            opportunity_n_permutations=0,
        )
        for pid, profile in result.profiles.items():
            assert "peer_percentile" in profile
            assert "opportunity_residual" in profile
            assert "consistency" in profile
            assert "independent_value" in profile

    def test_r_squared_in_summary(self, setup_data):
        result = compute_individual_profiles(
            **setup_data,
            opportunity_n_permutations=0,
        )
        if result.model_r_squared is not None:
            assert 0.0 <= result.model_r_squared <= 1.0
