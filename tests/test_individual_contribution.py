"""Tests for individual contribution profile metrics (Layer 2)."""

import pytest

from src.analysis.scoring.individual_contribution import (
    IndividualContributionResult,
    _build_person_features,
    _get_career_band,
    compute_consistency,
    compute_independent_value,
    compute_individual_profiles,
    compute_opportunity_residual,
    compute_peer_percentile,
)
from src.models import BronzeAnime as Anime, Credit, Role


# --- Fixtures ---


def _make_anime(
    aid: str, score: float, year: int = 2020, studios: list[str] | None = None
) -> Anime:
    return Anime(
        id=aid,
        title_ja=f"Anime {aid}",
        title_en=f"Anime {aid}",
        score=score,
        year=year,
        studios=studios or ["StudioA"],
        source="test",
    )


def _make_credit(
    person_id: str, anime_id: str, role: Role = Role.KEY_ANIMATOR
) -> Credit:
    return Credit(person_id=person_id, anime_id=anime_id, role=role, source="test")


@pytest.fixture
def anime_map():
    """10 anime with varied scores."""
    return {
        f"a{i}": _make_anime(
            f"a{i}",
            score=60 + i * 3,
            year=2015 + i,
            studios=["StudioA"] if i < 5 else ["StudioB"],
        )
        for i in range(10)
    }


@pytest.fixture
def credits_list():
    """Credits for 15 persons across 10 anime."""
    credits = []
    # 5 directors, each directing 2 anime
    for d in range(5):
        credits.append(_make_credit(f"dir{d}", f"a{d * 2}", Role.DIRECTOR))
        credits.append(_make_credit(f"dir{d}", f"a{d * 2 + 1}", Role.DIRECTOR))

    # 10 key animators with varying coverage
    for ka in range(10):
        # Each animator works on ka+1 anime (1..10)
        for a in range(min(ka + 1, 10)):
            credits.append(_make_credit(f"ka{ka}", f"a{a}", Role.KEY_ANIMATOR))

    return credits


@pytest.fixture
def results_list():
    """Simulated pipeline results for all 15 persons."""
    results = []
    for d in range(5):
        results.append(
            {
                "person_id": f"dir{d}",
                "name": f"Director {d}",
                "birank": 70 + d * 5,
                "patronage": 60 + d * 3,
                "person_fe": 65 + d * 4,
                "iv_score": 65 + d * 4,
            }
        )
    for ka in range(10):
        results.append(
            {
                "person_id": f"ka{ka}",
                "name": f"Animator {ka}",
                "birank": 40 + ka * 3,
                "patronage": 35 + ka * 2,
                "person_fe": 50 + ka * 3,
                "iv_score": 42 + ka * 3,
            }
        )
    return results


@pytest.fixture
def role_profiles():
    profiles = {}
    for d in range(5):
        profiles[f"dir{d}"] = {"primary_role": "director"}
    for ka in range(10):
        profiles[f"ka{ka}"] = {"primary_role": "key_animator"}
    return profiles


@pytest.fixture
def career_data():
    data = {}
    for d in range(5):
        data[f"dir{d}"] = {"active_years": 10 + d * 2}
    for ka in range(10):
        data[f"ka{ka}"] = {"active_years": 3 + ka}
    return data


# --- Unit tests ---


class TestGetCareerBand:
    def test_zero_years(self):
        assert _get_career_band(0) == "0-4y"

    def test_mid_band(self):
        assert _get_career_band(3) == "0-4y"

    def test_boundary(self):
        assert _get_career_band(5) == "5-9y"

    def test_large_value(self):
        assert _get_career_band(23) == "20-24y"

    def test_band_width_respected(self):
        """All years in same band should produce same result."""
        for y in range(5, 10):
            assert _get_career_band(y) == "5-9y"


class TestBuildPersonFeatures:
    def test_basic_features(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        assert len(features) == 15
        assert "iv_score" in features["ka0"]
        assert "primary_role" in features["ka0"]
        assert "career_band" in features["ka0"]
        assert features["ka0"]["primary_role"] == "key_animator"

    def test_career_years_from_career_data(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        # dir0 has active_years=10
        assert features["dir0"]["career_years"] == 10

    def test_career_years_fallback_to_credits(
        self, results_list, credits_list, anime_map, role_profiles
    ):
        """When career_data has 0 active_years, fall back to credit date range."""
        empty_career = {pid: {"active_years": 0} for pid in role_profiles}
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, empty_career
        )
        # ka9 works on all 10 anime (years 2015-2024), so span = 10
        assert features["ka9"]["career_years"] == 10

    def test_avg_staff_count(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        # ka0 only works on a0; a0 has dir0 + ka0..ka9 = 11 unique persons
        assert features["ka0"]["avg_staff_count"] == 11


class TestPeerPercentile:
    def test_basic_percentile(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        result = compute_peer_percentile(features)
        assert len(result) == 15
        # Everyone should have a result
        for pid in features:
            assert pid in result

    def test_highest_in_cohort_gets_high_percentile(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        result = compute_peer_percentile(features)
        # ka9 has highest composite among key_animators
        ka9_pct = result["ka9"]["peer_percentile"]
        ka0_pct = result["ka0"]["peer_percentile"]
        assert ka9_pct is not None
        assert ka0_pct is not None
        assert ka9_pct > ka0_pct

    def test_small_cohort_merges(self):
        """Cohorts smaller than MIN_COHORT_SIZE should merge across bands."""
        features = {
            f"p{i}": {
                "iv_score": 50 + i,
                "primary_role": "key_animator",
                "career_band": "0-4y" if i < 3 else "5-9y",
            }
            for i in range(6)
        }
        result = compute_peer_percentile(features)
        # Both groups (3 each) are below MIN_COHORT_SIZE=5, so they merge
        # All 6 should get percentiles
        assert all(r["peer_percentile"] is not None for r in result.values())

    def test_none_for_tiny_role(self):
        """If role has too few persons even after merge, return None."""
        features = {
            f"p{i}": {
                "iv_score": 50 + i,
                "primary_role": f"unique_role_{i}",
                "career_band": "0-4y",
            }
            for i in range(3)
        }
        result = compute_peer_percentile(features)
        # Each "unique_role" has only 1 person, can't merge
        assert all(r["peer_percentile"] is None for r in result.values())


class TestOpportunityResidual:
    def test_basic_regression(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        residuals, r_squared = compute_opportunity_residual(features)
        assert len(residuals) == 15
        assert r_squared is not None
        assert 0 <= r_squared <= 1.0

    def test_residuals_are_z_scores(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        """Standardized residuals should have mean ~0 and std ~1."""
        import numpy as np

        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        residuals, _ = compute_opportunity_residual(features)
        vals = [v for v in residuals.values() if v is not None]
        assert abs(np.mean(vals)) < 0.1
        assert (
            abs(np.std(vals) - 1.0) < 0.3
        )  # studentized residuals have wider distribution

    def test_insufficient_data(self):
        """With <10 persons, should return None values."""
        features = {
            f"p{i}": {
                "iv_score": 50,
                "primary_role": "ka",
                "career_years": 5,
                "avg_staff_count": 10,
                "unique_studios": 2,
            }
            for i in range(5)
        }
        residuals, r_squared = compute_opportunity_residual(features)
        assert r_squared is None
        assert all(v is None for v in residuals.values())


class TestConsistency:
    def test_consistent_person(self):
        """Person with stable AKM residuals should be consistent."""
        features = {"p1": {"iv_score": 60}}
        anime_map = {f"a{i}": _make_anime(f"a{i}", score=75) for i in range(6)}
        credits = [_make_credit("p1", f"a{i}") for i in range(6)]
        # All residuals identical → std=0 → consistency=1.0
        akm_residuals = {("p1", f"a{i}"): 0.5 for i in range(6)}

        result = compute_consistency(
            features, credits, anime_map, akm_residuals=akm_residuals
        )
        assert result["p1"] is not None
        assert result["p1"] > 0.9  # Very consistent

    def test_inconsistent_person(self):
        """Person with widely varying AKM residuals should have lower consistency."""
        features = {"p1": {"iv_score": 60}}
        anime_map = {f"a{i}": _make_anime(f"a{i}", score=70) for i in range(6)}
        credits = [_make_credit("p1", f"a{i}") for i in range(6)]
        # Widely varying residuals → high std → low consistency
        akm_residuals = {("p1", f"a{i}"): -15 + i * 6 for i in range(6)}
        # Values: -15, -9, -3, 3, 9, 15 → std ≈ 10.0

        result = compute_consistency(
            features, credits, anime_map, akm_residuals=akm_residuals
        )
        assert result["p1"] is not None
        assert result["p1"] < 0.8

    def test_insufficient_works(self):
        """Person with fewer than MIN_WORKS_FOR_CONSISTENCY anime should get None."""
        features = {"p1": {"iv_score": 60}}
        anime_map = {f"a{i}": _make_anime(f"a{i}", score=75) for i in range(3)}
        credits = [_make_credit("p1", f"a{i}") for i in range(3)]

        result = compute_consistency(features, credits, anime_map)
        assert result["p1"] is None

    def test_consistency_with_akm_residuals(self):
        """AKM residuals should improve consistency measurement."""
        features = {"p1": {"iv_score": 60}, "p2": {"iv_score": 55}}
        anime_map = {
            f"a{i}": _make_anime(f"a{i}", score=60 + i * 5, year=2015 + i)
            for i in range(12)
        }
        # p1: small residuals (close to 0 → consistent)
        credits = [_make_credit("p1", f"a{i}") for i in range(6)]
        akm_residuals = {("p1", f"a{i}"): 0.1 * (-1) ** i for i in range(6)}
        # p2: larger residuals (wider variance → raises ref_scale)
        credits += [_make_credit("p2", f"a{i}") for i in range(6, 12)]
        akm_residuals.update({("p2", f"a{i}"): -10 + (i - 6) * 4 for i in range(6, 12)})

        result = compute_consistency(
            features, credits, anime_map, akm_residuals=akm_residuals
        )
        assert result["p1"] is not None
        assert result["p1"] > 0.8  # Small residuals → high consistency


class TestIndependentValue:
    def test_basic_computation(self):
        """Person X's spillover via IV-based residuals.

        independent_value for X = mean over collaborators of:
          (collab_iv - proj_quality_with_x) - (collab_iv - proj_quality_without_x)
        = mean of (proj_quality_without_x - proj_quality_with_x)

        For positive spillover: projects WITH X need lower remaining-peer IV
        (after excluding X and collab), projects WITHOUT X need higher peer IV.
        This means X is "lifting" projects — without X the other people are
        strong, but with X the other people are weaker (X substitutes for them).
        """
        # p0 = target; p1-p3 = collaborators (mid IV ~50)
        # low1-low3 = low IV on "withx" projects (keeps proj_quality low with X)
        # high1-high3 = high IV on "nox" projects (keeps proj_quality high without X)
        features = {
            "p0": {"iv_score": 80},
            "p1": {"iv_score": 50},
            "p2": {"iv_score": 52},
            "p3": {"iv_score": 48},
            "low1": {"iv_score": 10},
            "low2": {"iv_score": 12},
            "low3": {"iv_score": 8},
            "high1": {"iv_score": 90},
            "high2": {"iv_score": 88},
            "high3": {"iv_score": 92},
        }
        anime_map = {
            "withx": _make_anime("withx", score=80),
            "nox1": _make_anime("nox1", score=70),
            "nox2": _make_anime("nox2", score=70),
            "nox3": _make_anime("nox3", score=70),
        }
        # "withx": p0 + p1,p2,p3 + low1,low2,low3 (low IV peers besides p0)
        credits = [
            _make_credit("p0", "withx"),
            _make_credit("low1", "withx"),
            _make_credit("low2", "withx"),
            _make_credit("low3", "withx"),
        ]
        for i in range(1, 4):
            credits.append(_make_credit(f"p{i}", "withx"))  # With p0
            # "nox{i}": collab + high IV filler (no p0)
            credits.append(_make_credit(f"p{i}", f"nox{i}"))  # Without p0
            credits.append(_make_credit(f"high{i}", f"nox{i}"))  # High IV peer

        result = compute_independent_value(features, credits, anime_map)
        # For each collab (e.g. p1, IV=50):
        #   "withx": exclude p1 and p0 → remaining = {p2,p3,low1,low2,low3} → low avg
        #     resid = 50 - low_avg → positive (high)
        #   "nox1": exclude p1 → remaining = {high1} → high avg (~90)
        #     resid = 50 - 90 → negative (low)
        #   diff = high_resid - low_resid > 0
        assert result["p0"] is not None
        assert result["p0"] > 0

    def test_too_few_collaborators(self):
        """Should return None if < MIN_COLLABORATORS."""
        features = {"p0": {"iv_score": 50}, "p1": {"iv_score": 40}}
        anime_map = {"a1": _make_anime("a1", score=70)}
        credits = [_make_credit("p0", "a1"), _make_credit("p1", "a1")]

        result = compute_independent_value(features, credits, anime_map)
        assert result["p0"] is None


class TestClusterPercentile:
    def test_cluster_percentile_added(self):
        """Community map adds cluster percentile to results."""
        features = {
            f"p{i}": {
                "iv_score": 50 + i * 5,
                "primary_role": "key_animator",
                "career_band": "5-9y",
            }
            for i in range(10)
        }
        # All in same community
        community_map = {f"p{i}": 0 for i in range(10)}
        result = compute_peer_percentile(features, community_map=community_map)
        # Should have cluster_percentile for everyone
        for pid, data in result.items():
            if data.get("peer_percentile") is not None:
                assert "cluster_percentile" in data
                assert "cluster_id" in data
                assert data["cluster_id"] == 0
                assert "cluster_size" in data
                assert data["cluster_size"] == 10

    def test_no_cluster_without_community_map(self):
        """Without community_map, no cluster_percentile is added."""
        features = {
            f"p{i}": {
                "iv_score": 50 + i * 5,
                "primary_role": "key_animator",
                "career_band": "5-9y",
            }
            for i in range(10)
        }
        result = compute_peer_percentile(features, community_map=None)
        for pid, data in result.items():
            assert "cluster_percentile" not in data


class TestLeverageCorrection:
    def test_leverage_corrected_residuals(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        """Studentized residuals should handle high-leverage points."""
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        residuals, r_squared = compute_opportunity_residual(features)
        # Should still produce valid z-scores
        vals = [v for v in residuals.values() if v is not None]
        assert len(vals) == 15
        # z-scores should generally be bounded
        for v in vals:
            assert -10 < v < 10


class TestComputeIndividualProfiles:
    def test_full_integration(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        result = compute_individual_profiles(
            results=results_list,
            credits=credits_list,
            anime_map=anime_map,
            role_profiles=role_profiles,
            career_data=career_data,
        )
        assert isinstance(result, IndividualContributionResult)
        assert result.total_persons == 15
        assert result.cohort_count > 0
        assert len(result.profiles) == 15

    def test_profiles_have_all_fields(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        result = compute_individual_profiles(
            results=results_list,
            credits=credits_list,
            anime_map=anime_map,
            role_profiles=role_profiles,
            career_data=career_data,
        )
        for pid, profile in result.profiles.items():
            assert "person_id" in profile
            assert "peer_percentile" in profile
            assert "opportunity_residual" in profile
            assert "consistency" in profile
            assert "independent_value" in profile

    def test_r_squared_reasonable(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        result = compute_individual_profiles(
            results=results_list,
            credits=credits_list,
            anime_map=anime_map,
            role_profiles=role_profiles,
            career_data=career_data,
        )
        if result.model_r_squared is not None:
            assert 0 <= result.model_r_squared <= 1.0

    def test_serializable(
        self, results_list, credits_list, anime_map, role_profiles, career_data
    ):
        """Result should be JSON-serializable via asdict."""
        from dataclasses import asdict
        import json

        result = compute_individual_profiles(
            results=results_list,
            credits=credits_list,
            anime_map=anime_map,
            role_profiles=role_profiles,
            career_data=career_data,
        )
        d = asdict(result)
        # Should not raise
        json.dumps(d, default=str)
