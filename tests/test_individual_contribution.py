"""Tests for individual contribution profile metrics (Layer 2)."""

import pytest

from src.analysis.individual_contribution import (
    IndividualContributionResult,
    _build_person_features,
    _get_career_band,
    compute_consistency,
    compute_independent_value,
    compute_individual_profiles,
    compute_opportunity_residual,
    compute_peer_percentile,
)
from src.models import Anime, Credit, Role


# --- Fixtures ---


def _make_anime(aid: str, score: float, year: int = 2020, studios: list[str] | None = None) -> Anime:
    return Anime(
        id=aid,
        title_ja=f"Anime {aid}",
        title_en=f"Anime {aid}",
        score=score,
        year=year,
        studios=studios or ["StudioA"],
        source="test",
    )


def _make_credit(person_id: str, anime_id: str, role: Role = Role.KEY_ANIMATOR) -> Credit:
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
        results.append({
            "person_id": f"dir{d}",
            "name": f"Director {d}",
            "authority": 70 + d * 5,
            "trust": 60 + d * 3,
            "skill": 65 + d * 4,
            "composite": 65 + d * 4,
        })
    for ka in range(10):
        results.append({
            "person_id": f"ka{ka}",
            "name": f"Animator {ka}",
            "authority": 40 + ka * 3,
            "trust": 35 + ka * 2,
            "skill": 50 + ka * 3,
            "composite": 42 + ka * 3,
        })
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
    def test_basic_features(self, results_list, credits_list, anime_map, role_profiles, career_data):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        assert len(features) == 15
        assert "composite" in features["ka0"]
        assert "primary_role" in features["ka0"]
        assert "career_band" in features["ka0"]
        assert features["ka0"]["primary_role"] == "key_animator"

    def test_career_years_from_career_data(self, results_list, credits_list, anime_map, role_profiles, career_data):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        # dir0 has active_years=10
        assert features["dir0"]["career_years"] == 10

    def test_career_years_fallback_to_credits(self, results_list, credits_list, anime_map, role_profiles):
        """When career_data has 0 active_years, fall back to credit date range."""
        empty_career = {pid: {"active_years": 0} for pid in role_profiles}
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, empty_career
        )
        # ka9 works on all 10 anime (years 2015-2024), so span = 10
        assert features["ka9"]["career_years"] == 10

    def test_avg_anime_score(self, results_list, credits_list, anime_map, role_profiles, career_data):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        # ka0 only works on a0 (score=60)
        assert features["ka0"]["avg_anime_score"] == 60.0


class TestPeerPercentile:
    def test_basic_percentile(self, results_list, credits_list, anime_map, role_profiles, career_data):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        result = compute_peer_percentile(features)
        assert len(result) == 15
        # Everyone should have a result
        for pid in features:
            assert pid in result

    def test_highest_in_cohort_gets_high_percentile(self, results_list, credits_list, anime_map, role_profiles, career_data):
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
                "composite": 50 + i,
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
                "composite": 50 + i,
                "primary_role": f"unique_role_{i}",
                "career_band": "0-4y",
            }
            for i in range(3)
        }
        result = compute_peer_percentile(features)
        # Each "unique_role" has only 1 person, can't merge
        assert all(r["peer_percentile"] is None for r in result.values())


class TestOpportunityResidual:
    def test_basic_regression(self, results_list, credits_list, anime_map, role_profiles, career_data):
        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        residuals, r_squared = compute_opportunity_residual(features)
        assert len(residuals) == 15
        assert r_squared is not None
        assert 0 <= r_squared <= 1.0

    def test_residuals_are_z_scores(self, results_list, credits_list, anime_map, role_profiles, career_data):
        """Standardized residuals should have mean ~0 and std ~1."""
        import numpy as np

        features = _build_person_features(
            results_list, credits_list, anime_map, role_profiles, career_data
        )
        residuals, _ = compute_opportunity_residual(features)
        vals = [v for v in residuals.values() if v is not None]
        assert abs(np.mean(vals)) < 0.1
        assert abs(np.std(vals) - 1.0) < 0.2

    def test_insufficient_data(self):
        """With <10 persons, should return None values."""
        features = {
            f"p{i}": {
                "composite": 50,
                "primary_role": "ka",
                "career_years": 5,
                "avg_anime_score": 70,
                "unique_studios": 2,
            }
            for i in range(5)
        }
        residuals, r_squared = compute_opportunity_residual(features)
        assert r_squared is None
        assert all(v is None for v in residuals.values())


class TestConsistency:
    def test_consistent_person(self):
        """Person who only works on similarly-scored anime should be consistent."""
        features = {"p1": {"composite": 60}}
        anime_map = {f"a{i}": _make_anime(f"a{i}", score=75) for i in range(6)}
        credits = [_make_credit("p1", f"a{i}") for i in range(6)]

        result = compute_consistency(features, credits, anime_map)
        assert result["p1"] is not None
        assert result["p1"] > 0.9  # Very consistent

    def test_inconsistent_person(self):
        """Person who works on wildly varied anime should have lower consistency."""
        features = {"p1": {"composite": 60}}
        anime_map = {}
        credits = []
        for i in range(6):
            score = 30 + i * 12  # 30, 42, 54, 66, 78, 90
            anime_map[f"a{i}"] = _make_anime(f"a{i}", score=score)
            credits.append(_make_credit("p1", f"a{i}"))

        result = compute_consistency(features, credits, anime_map)
        assert result["p1"] is not None
        assert result["p1"] < 0.8

    def test_insufficient_works(self):
        """Person with fewer than MIN_WORKS_FOR_CONSISTENCY anime should get None."""
        features = {"p1": {"composite": 60}}
        anime_map = {f"a{i}": _make_anime(f"a{i}", score=75) for i in range(3)}
        credits = [_make_credit("p1", f"a{i}") for i in range(3)]

        result = compute_consistency(features, credits, anime_map)
        assert result["p1"] is None


class TestIndependentValue:
    def test_basic_computation(self):
        """Person X works on high-score anime; collaborators also on lower anime without X."""
        features = {f"p{i}": {"composite": 50 + i * 5} for i in range(6)}
        anime_map = {
            "good": _make_anime("good", score=90),
            "ok1": _make_anime("ok1", score=60),
            "ok2": _make_anime("ok2", score=65),
            "ok3": _make_anime("ok3", score=55),
        }
        # p0 works on "good"; p1-p5 work on "good" + their own "ok" anime
        credits = [_make_credit("p0", "good")]
        for i in range(1, 4):
            credits.append(_make_credit(f"p{i}", "good"))  # With p0
            credits.append(_make_credit(f"p{i}", f"ok{i}"))  # Without p0
        # p4, p5 only on separate anime (not enough collaborators for them)
        credits.append(_make_credit("p4", "ok1"))
        credits.append(_make_credit("p5", "ok2"))

        result = compute_independent_value(features, credits, anime_map)
        # p0 is on "good" (90); collaborators p1-p3 have "good"(90) with p0 vs "ok"(55-65) without p0
        # So p0's independent_value should be positive (collaborators do better with p0)
        assert result["p0"] is not None
        assert result["p0"] > 0

    def test_too_few_collaborators(self):
        """Should return None if < MIN_COLLABORATORS."""
        features = {"p0": {"composite": 50}, "p1": {"composite": 40}}
        anime_map = {"a1": _make_anime("a1", score=70)}
        credits = [_make_credit("p0", "a1"), _make_credit("p1", "a1")]

        result = compute_independent_value(features, credits, anime_map)
        assert result["p0"] is None


class TestComputeIndividualProfiles:
    def test_full_integration(self, results_list, credits_list, anime_map, role_profiles, career_data):
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

    def test_profiles_have_all_fields(self, results_list, credits_list, anime_map, role_profiles, career_data):
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

    def test_r_squared_reasonable(self, results_list, credits_list, anime_map, role_profiles, career_data):
        result = compute_individual_profiles(
            results=results_list,
            credits=credits_list,
            anime_map=anime_map,
            role_profiles=role_profiles,
            career_data=career_data,
        )
        if result.model_r_squared is not None:
            assert 0 <= result.model_r_squared <= 1.0

    def test_serializable(self, results_list, credits_list, anime_map, role_profiles, career_data):
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
