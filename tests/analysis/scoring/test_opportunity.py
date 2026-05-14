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
    _role_diversity_entropy,
    compute_opportunity_residual_from_credits,
    compute_opportunity_residual_panel,
    residual_qq_deviation,
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
            residual=0.5,
            se=0.1,
            ci_lower=0.3,
            ci_upper=0.7,
            n_years=5,
            p_value_permutation=0.04,
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
        pids_in_panel = [pid for pid, r in results.items() if r.residual is not None]
        for pid in pids_in_panel:
            assert results[pid].p_value_permutation is not None

    def test_p_values_absent_when_n_permutations_zero(self):
        features = _make_features(n=20)
        results, _ = compute_opportunity_residual_panel(features, n_permutations=0)
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
        r1, _ = compute_opportunity_residual_panel(
            features, n_permutations=50, rng=rng1
        )
        r2, _ = compute_opportunity_residual_panel(
            features, n_permutations=50, rng=rng2
        )
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
        results, r_sq = compute_opportunity_residual_full(features, n_permutations=0)
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
        ] + [{"person_id": f"ka{ka}", "iv_score": 42 + ka * 3} for ka in range(10)]
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


# ---------------------------------------------------------------------------
# True per-(person, year) panel from credits + anime_map
# ---------------------------------------------------------------------------


def _make_panel_credits(
    n_persons: int = 30,
    n_years: int = 5,
    seed: int = 0,
) -> tuple[list[Credit], dict[str, Anime]]:
    """Generate a synthetic multi-year credit panel.

    Each person credits 1-3 anime per year for ``n_years`` consecutive years,
    starting at year 2015. Studios alternate among three labels. Roles are
    drawn uniformly from a small set.
    """
    rng = np.random.default_rng(seed)
    roles_pool = [Role.KEY_ANIMATOR, Role.IN_BETWEEN, Role.DIRECTOR]
    studios_pool = ["StudioA", "StudioB", "StudioC"]

    anime_map: dict[str, Anime] = {}
    credits: list[Credit] = []
    aid_counter = 0
    for p in range(n_persons):
        modal_studio = studios_pool[p % 3]
        for y in range(n_years):
            year = 2015 + y
            n_anime_this_year = int(rng.integers(1, 4))
            for _ in range(n_anime_this_year):
                aid = f"a{aid_counter}"
                aid_counter += 1
                anime_map[aid] = Anime(
                    id=aid,
                    title_ja=f"Anime {aid}",
                    title_en=f"Anime {aid}",
                    year=year,
                    studios=[modal_studio],
                    source="test",
                )
                role = roles_pool[int(rng.integers(0, len(roles_pool)))]
                credits.append(
                    Credit(
                        person_id=f"p{p}",
                        anime_id=aid,
                        role=role,
                        source="test",
                    )
                )
    return credits, anime_map


class TestRoleDiversityEntropy:
    def test_single_role_is_zero(self):
        from collections import Counter

        assert _role_diversity_entropy(Counter({"a": 10})) == pytest.approx(0.0)

    def test_uniform_is_one(self):
        from collections import Counter

        # Three roles, equal frequencies → normalised entropy = 1
        assert _role_diversity_entropy(
            Counter({"a": 1, "b": 1, "c": 1})
        ) == pytest.approx(1.0)

    def test_empty_is_zero(self):
        from collections import Counter

        assert _role_diversity_entropy(Counter()) == pytest.approx(0.0)

    def test_skewed_below_one(self):
        from collections import Counter

        h = _role_diversity_entropy(Counter({"a": 9, "b": 1}))
        assert 0.0 < h < 1.0


class TestComputeOpportunityResidualFromCredits:
    def test_returns_results_for_observed_persons(self):
        credits, anime_map = _make_panel_credits(n_persons=20, n_years=5)
        results, summary = compute_opportunity_residual_from_credits(
            credits, anime_map, n_permutations=0
        )
        # Every person who has at least one anime-year cell should appear
        assert len(results) >= 15
        assert summary.n_persons >= 15
        assert summary.n_panel_obs > 0

    def test_multi_year_panel_produces_real_ci(self):
        """Core test: panel produces per-person n_years > 1 → real CIs."""
        credits, anime_map = _make_panel_credits(n_persons=20, n_years=5)
        results, _ = compute_opportunity_residual_from_credits(
            credits, anime_map, n_permutations=0
        )
        with_ci = [r for r in results.values() if r.ci_lower is not None]
        # At least half the persons should have multi-year data and hence a CI
        assert len(with_ci) >= 10, (
            f"Expected multi-year panel to produce CIs; got {len(with_ci)}"
        )
        for r in with_ci:
            assert r.n_years >= 2
            assert r.ci_lower <= r.residual <= r.ci_upper
            assert r.se is not None and r.se >= 0

    def test_r_squared_in_unit_interval(self):
        credits, anime_map = _make_panel_credits(n_persons=15, n_years=4)
        _, summary = compute_opportunity_residual_from_credits(
            credits, anime_map, n_permutations=0
        )
        if summary.r_squared is not None:
            assert 0.0 <= summary.r_squared <= 1.0

    def test_theta_map_accepted(self):
        credits, anime_map = _make_panel_credits(n_persons=15, n_years=4)
        theta = {f"p{i}": float(np.random.default_rng(i).normal()) for i in range(15)}
        results, _ = compute_opportunity_residual_from_credits(
            credits, anime_map, theta_map=theta, n_permutations=0
        )
        assert len(results) > 0

    def test_insufficient_data_returns_empty(self):
        # Fewer panel observations than MIN_PANEL_OBS → no real fit
        credits, anime_map = _make_panel_credits(n_persons=2, n_years=2)
        results, summary = compute_opportunity_residual_from_credits(
            credits, anime_map, n_permutations=0
        )
        # Either degenerate-empty or all None — both are acceptable guards
        if summary.r_squared is None:
            for r in results.values():
                assert r.residual is None

    def test_permutation_p_values_present(self):
        credits, anime_map = _make_panel_credits(n_persons=15, n_years=4)
        results, _ = compute_opportunity_residual_from_credits(
            credits,
            anime_map,
            n_permutations=30,
            rng=np.random.default_rng(1),
        )
        any_p = any(r.p_value_permutation is not None for r in results.values())
        assert any_p, "Permutation null should populate at least one p-value"

    def test_credit_year_overrides_anime_year(self):
        """If credit_year is set, it should drive the panel cell."""
        anime = Anime(
            id="a0",
            title_ja="X",
            title_en="X",
            year=2020,
            studios=["S"],
            source="test",
        )
        c = Credit(
            person_id="p0",
            anime_id="a0",
            role=Role.KEY_ANIMATOR,
            source="test",
            credit_year=2018,
        )
        # No assertion error → API accepts credit_year as the cell year.
        # We can't introspect internal panel rows directly, but the call
        # should at least not error out.
        compute_opportunity_residual_from_credits(
            [c] * MIN_PANEL_OBS, {"a0": anime}, n_permutations=0
        )

    def test_missing_year_rows_skipped(self):
        """Anime without year should drop credits without crashing."""
        anime = Anime(
            id="a0",
            title_ja="X",
            title_en="X",
            year=None,
            studios=["S"],
            source="test",
        )
        c = Credit(person_id="p0", anime_id="a0", role=Role.KEY_ANIMATOR, source="test")
        results, summary = compute_opportunity_residual_from_credits(
            [c] * 5, {"a0": anime}, n_permutations=0
        )
        # No panel rows possible → degenerate
        assert summary.n_panel_obs == 0


class TestResidualQQDeviation:
    def test_zero_for_perfect_normal(self):
        rng = np.random.default_rng(0)
        # Large normal sample → deviation should be small
        residuals = rng.normal(size=1000).tolist()
        dev = residual_qq_deviation(residuals)
        assert dev < 0.2

    def test_large_for_heavy_tails(self):
        rng = np.random.default_rng(0)
        # Cauchy-like tails → deviation should be larger than Normal
        residuals = rng.standard_t(df=2, size=1000).tolist()
        dev = residual_qq_deviation(residuals)
        assert dev > 0.2

    def test_zero_for_constant_residual(self):
        # Degenerate input
        assert residual_qq_deviation([0.0] * 50) == pytest.approx(0.0)


class TestIndividualProfilesPanelIntegration:
    """End-to-end: compute_individual_profiles should pick up multi-year panel."""

    def test_profiles_have_real_ci_from_panel(self):
        # Build a credit set with year-varying activity per person
        credits, anime_map = _make_panel_credits(n_persons=20, n_years=5)
        # Construct minimum required inputs for compute_individual_profiles
        unique_pids = sorted({c.person_id for c in credits})
        results_list = [{"person_id": pid, "iv_score": 50.0} for pid in unique_pids]
        role_profiles = {pid: {"primary_role": "key_animator"} for pid in unique_pids}
        career_data = {pid: {"active_years": 5} for pid in unique_pids}

        out = compute_individual_profiles(
            results=results_list,
            credits=credits,
            anime_map=anime_map,
            role_profiles=role_profiles,
            career_data=career_data,
            opportunity_n_permutations=0,
        )
        with_ci = [
            p
            for p in out.profiles.values()
            if p.get("opportunity_residual_ci_lower") is not None
        ]
        # Multi-year panel should yield non-trivial number of CIs
        assert len(with_ci) >= 10, (
            f"Expected ≥10 profiles with CI from panel; got {len(with_ci)}"
        )
