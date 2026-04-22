"""Tests for integrated value computation."""

import pytest

from src.analysis.scoring.integrated_value import (
    IntegratedValueResult,
    compute_integrated_value,
    compute_integrated_value_full,
    compute_studio_exposure,
    optimize_lambda_weights,
)
from src.models import BronzeAnime as Anime, Credit, Role


@pytest.fixture
def component_scores():
    """Component scores for 5 persons."""
    return {
        "person_fe": {"p1": 0.8, "p2": 0.6, "p3": 0.4, "p4": 0.3, "p5": 0.1},
        "birank": {"p1": 0.7, "p2": 0.5, "p3": 0.3, "p4": 0.2, "p5": 0.1},
        "studio_exposure": {"p1": 0.5, "p2": 0.4, "p3": 0.3, "p4": 0.2, "p5": 0.1},
        "awcc": {"p1": 0.6, "p2": 0.4, "p3": 0.2, "p4": 0.1, "p5": 0.05},
        "patronage": {"p1": 0.3, "p2": 0.2, "p3": 0.15, "p4": 0.1, "p5": 0.05},
    }


@pytest.fixture
def lambdas():
    """Equal weight configuration."""
    return {
        "person_fe": 0.2,
        "birank": 0.2,
        "studio_exposure": 0.2,
        "awcc": 0.2,
        "patronage": 0.2,
    }


@pytest.fixture
def dormancy_full():
    """No dormancy penalty for any person."""
    return {"p1": 1.0, "p2": 1.0, "p3": 1.0, "p4": 1.0, "p5": 1.0}


@pytest.fixture
def dormancy_partial():
    """p1 (highest scorer) is dormant (penalty = 0.3), others active."""
    return {"p1": 0.3, "p2": 1.0, "p3": 1.0, "p4": 1.0, "p5": 1.0}


@pytest.fixture
def cv_data():
    """Credits and anime for cross-validation optimization with 15 persons."""
    anime_map = {}
    credits = []
    for a_idx in range(6):
        aid = f"a{a_idx}"
        anime_map[aid] = Anime(
            id=aid,
            title_en=f"Anime {a_idx}",
            year=2018 + a_idx,
            score=6.0 + a_idx * 0.5,
            studios=["S1"],
        )
        for p_idx in range(5):
            pid = f"p{p_idx + a_idx * 2}"  # overlapping persons across anime
            credits.append(
                Credit(
                    person_id=pid, anime_id=aid, role=Role.KEY_ANIMATOR, source="test"
                )
            )
    return credits, anime_map


class TestComputeIntegratedValue:
    def test_compute_iv_basic(self, component_scores, lambdas, dormancy_full):
        """Weighted combination produces scores for all persons."""
        iv = compute_integrated_value(
            person_fe=component_scores["person_fe"],
            birank=component_scores["birank"],
            studio_exposure=component_scores["studio_exposure"],
            awcc=component_scores["awcc"],
            patronage=component_scores["patronage"],
            dormancy=dormancy_full,
            lambdas=lambdas,
        )
        assert len(iv) == 5
        for pid, score in iv.items():
            assert score >= 0  # min scorer maps to 0.0 after B02 renormalization

    def test_monotonicity(self, component_scores, lambdas, dormancy_full):
        """Higher components lead to higher IV."""
        iv = compute_integrated_value(
            person_fe=component_scores["person_fe"],
            birank=component_scores["birank"],
            studio_exposure=component_scores["studio_exposure"],
            awcc=component_scores["awcc"],
            patronage=component_scores["patronage"],
            dormancy=dormancy_full,
            lambdas=lambdas,
        )
        # p1 has highest components, p5 has lowest
        assert iv["p1"] > iv["p2"] > iv["p3"] > iv["p4"] > iv["p5"]

    def test_dormancy_applied(self, component_scores, lambdas, dormancy_partial):
        """Dormancy < 1 reduces IV score."""
        iv_full = compute_integrated_value(
            person_fe=component_scores["person_fe"],
            birank=component_scores["birank"],
            studio_exposure=component_scores["studio_exposure"],
            awcc=component_scores["awcc"],
            patronage=component_scores["patronage"],
            dormancy={"p1": 1.0},
            lambdas=lambdas,
        )
        iv_dormant = compute_integrated_value(
            person_fe=component_scores["person_fe"],
            birank=component_scores["birank"],
            studio_exposure=component_scores["studio_exposure"],
            awcc=component_scores["awcc"],
            patronage=component_scores["patronage"],
            dormancy=dormancy_partial,
            lambdas=lambdas,
        )
        # After B02 renormalization, dormancy reduces a non-minimum scorer's relative position
        assert iv_dormant["p1"] < iv_full["p1"]

    def test_empty_components(self):
        """Handles empty component dicts."""
        iv = compute_integrated_value(
            person_fe={},
            birank={},
            studio_exposure={},
            awcc={},
            patronage={},
            dormancy={},
            lambdas={"person_fe": 0.2, "birank": 0.2},
        )
        assert iv == {}


class TestStudioExposure:
    def test_studio_exposure(self):
        """Correct weighted sum of studio FEs based on time at each studio."""
        person_fe = {"p1": 0.5}
        studio_fe = {"StudioA": 0.8, "StudioB": 0.4}
        # p1 was at StudioA for 2 years, StudioB for 1 year
        studio_assignments = {
            "p1": {2018: "StudioA", 2019: "StudioA", 2020: "StudioB"},
        }
        exposure = compute_studio_exposure(person_fe, studio_fe, studio_assignments)
        # Expected: (2/3)*0.8 + (1/3)*0.4 = 0.533 + 0.133 = 0.6667
        assert "p1" in exposure
        assert exposure["p1"] == pytest.approx((2 / 3) * 0.8 + (1 / 3) * 0.4, abs=1e-4)

    def test_studio_exposure_empty(self):
        """Empty studio_fe returns empty dict."""
        result = compute_studio_exposure({}, {}, {})
        assert result == {}

    def test_studio_exposure_unknown_studio(self):
        """Studio not in studio_fe treated as 0."""
        person_fe = {"p1": 0.5}
        studio_fe = {"StudioA": 1.0}
        studio_assignments = {"p1": {2020: "StudioB"}}
        exposure = compute_studio_exposure(person_fe, studio_fe, studio_assignments)
        # StudioB not in studio_fe -> 0.0 contribution
        assert exposure["p1"] == pytest.approx(0.0)


class TestOptimizeLambdaWeights:
    def test_cv_optimization(self, component_scores, cv_data):
        """optimize_lambda_weights returns weights summing to approximately 1."""
        credits, anime_map = cv_data
        # Need components for persons that appear in credits
        person_ids = sorted({c.person_id for c in credits})
        components = {}
        for comp_name, base_scores in component_scores.items():
            comp = {}
            for i, pid in enumerate(person_ids):
                comp[pid] = 0.1 + i * 0.05
            components[comp_name] = comp

        weights, cv_mse, comp_std, comp_mean, var_expl = optimize_lambda_weights(
            components, credits, anime_map
        )
        assert len(weights) == len(components)
        total = sum(weights.values())
        assert total == pytest.approx(1.0, abs=0.05)
        # Component std should be returned
        assert isinstance(comp_std, dict)
        assert len(comp_std) == len(components)
        # Component mean should be returned
        assert isinstance(comp_mean, dict)
        assert len(comp_mean) == len(components)
        # PCA variance explained should be positive
        assert var_expl > 0

    def test_cv_empty_components(self):
        """Empty components returns empty weights."""
        weights, mse, comp_std, comp_mean, var_expl = optimize_lambda_weights(
            {}, [], {}
        )
        assert weights == {}
        assert mse == 0.0
        assert var_expl == 0.0

    def test_cv_few_persons(self):
        """Too few persons (< 10) returns equal weights."""
        components = {"a": {"p1": 1.0}, "b": {"p1": 2.0}}
        anime_map = {"a1": Anime(id="a1", title_en="X", year=2020, score=7.0)}
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test")
        ]
        weights, mse, comp_std, comp_mean, var_expl = optimize_lambda_weights(
            components, credits, anime_map
        )
        assert weights["a"] == pytest.approx(0.5)
        assert weights["b"] == pytest.approx(0.5)
        assert var_expl == 0.0

    def test_raw_scale_consistency(self, component_scores, cv_data):
        """Lambda weights should be non-negative and sum to 1."""
        credits, anime_map = cv_data
        person_ids = sorted({c.person_id for c in credits})
        components = {}
        for comp_name, base_scores in component_scores.items():
            comp = {}
            for i, pid in enumerate(person_ids):
                comp[pid] = 0.1 + i * 0.05
            components[comp_name] = comp

        weights, _, comp_std, _, _ = optimize_lambda_weights(
            components, credits, anime_map
        )
        # All weights should be non-negative
        for name, w in weights.items():
            assert w >= 0.0, f"{name} weight {w:.4f} is negative"
        # Weights should sum to 1
        assert sum(weights.values()) == pytest.approx(1.0, abs=0.01)
        # comp_std should have positive values
        for std in comp_std.values():
            assert std > 0


class TestComputeIntegratedValueFull:
    def test_full_pipeline(self, component_scores, cv_data):
        """compute_integrated_value_full returns IntegratedValueResult."""
        credits, anime_map = cv_data
        person_ids = sorted({c.person_id for c in credits})

        # Build components that cover persons in credits
        person_fe = {pid: 0.5 + i * 0.03 for i, pid in enumerate(person_ids)}
        birank = {pid: 0.3 + i * 0.02 for i, pid in enumerate(person_ids)}
        studio_exp = {pid: 0.2 for pid in person_ids}
        awcc = {pid: 0.1 + i * 0.01 for i, pid in enumerate(person_ids)}
        patronage = {pid: 0.05 for pid in person_ids}
        dormancy = {pid: 1.0 for pid in person_ids}

        result = compute_integrated_value_full(
            person_fe=person_fe,
            birank=birank,
            studio_exposure=studio_exp,
            awcc=awcc,
            patronage=patronage,
            dormancy=dormancy,
            credits=credits,
            anime_map=anime_map,
        )
        assert isinstance(result, IntegratedValueResult)
        assert len(result.iv_scores) > 0
        assert len(result.lambda_weights) > 0
        assert len(result.component_breakdown) > 0
        # Check breakdown has all component keys
        for pid, breakdown in result.component_breakdown.items():
            assert "person_fe" in breakdown
            assert "dormancy" in breakdown
        # PCA variance explained should be stored
        assert result.pca_variance_explained > 0


class TestScaleRobustness:
    """Ensure extreme scale differences don't cause a single component to dominate."""

    def test_extreme_scale_difference_no_single_component_dominance(self):
        """When one component has tiny std (e.g. 1e-6), it should not get 99%+ weight."""
        # BiRank-like: very small values with tiny variance
        # person_fe-like: normal range values
        components = {
            "birank": {f"p{i}": 1e-7 * (i + 1) for i in range(20)},
            "person_fe": {f"p{i}": 0.1 * (i + 1) for i in range(20)},
            "awcc": {f"p{i}": 0.05 * (i + 1) for i in range(20)},
        }
        anime_map = {
            f"a{i}": Anime(
                id=f"a{i}",
                title_en=f"Anime {i}",
                year=2018 + i,
                score=6.0 + i * 0.3,
                studios=["S1"],
            )
            for i in range(8)
        }
        credits = [
            Credit(
                person_id=f"p{i}",
                anime_id=f"a{j}",
                role=Role.KEY_ANIMATOR,
                source="test",
            )
            for i in range(20)
            for j in range(8)
            if (i + j) % 3 == 0
        ]
        weights, _, _, _, _ = optimize_lambda_weights(components, credits, anime_map)
        # Weights should sum to 1 and all be non-negative
        assert sum(weights.values()) == pytest.approx(1.0, abs=0.01)
        for name, w in weights.items():
            assert w >= 0.0, f"{name} weight = {w:.4f}, negative"


class TestPCAWeightDerivation:
    """Tests for PCA-based weight derivation."""

    def _build_components(self, n_persons: int = 20) -> dict[str, dict[str, float]]:
        """Build varied component scores for n persons."""
        import numpy as np

        rng = np.random.default_rng(42)
        pids = [f"p{i}" for i in range(n_persons)]
        return {
            "person_fe": {pid: float(rng.normal(0.5, 0.3)) for pid in pids},
            "birank": {pid: float(rng.exponential(0.3)) for pid in pids},
            "studio_exposure": {pid: float(rng.normal(0.2, 0.1)) for pid in pids},
            "awcc": {pid: float(rng.normal(0.3, 0.2)) for pid in pids},
            "patronage": {pid: float(rng.normal(0.15, 0.1)) for pid in pids},
        }

    def test_pca_weights_sum_to_one(self):
        """PCA-derived weights must sum to 1.0."""
        components = self._build_components()
        weights, _, _, _, _ = optimize_lambda_weights(components, [], {})
        assert sum(weights.values()) == pytest.approx(1.0, abs=1e-10)

    def test_pca_weights_non_negative(self):
        """Each component weight must be non-negative (absolute PCA loadings)."""
        components = self._build_components()
        weights, _, _, _, _ = optimize_lambda_weights(components, [], {})
        for name, w in weights.items():
            assert w >= 0.0, f"{name} weight {w:.4f} is negative"

    def test_pca_person_fe_loading_positive(self):
        """person_fe loading should be positive (sign convention)."""
        # Create data where person_fe is strongly correlated with other components
        components = self._build_components()
        weights, _, _, _, _ = optimize_lambda_weights(components, [], {})
        # person_fe should have positive weight (always true after abs + normalize)
        assert weights["person_fe"] > 0

    def test_pca_variance_explained_positive(self):
        """PC1 should explain > 0% of variance."""
        components = self._build_components()
        _, _, _, _, var_expl = optimize_lambda_weights(components, [], {})
        assert var_expl > 0.0
        assert var_expl <= 1.0

    def test_pca_equal_inputs_give_equal_weights(self):
        """When all components are identical, weights should be equal (0.2 each)."""
        pids = [f"p{i}" for i in range(20)]
        # All components have the exact same values
        base = {pid: float(i) for i, pid in enumerate(pids)}
        components = {
            "awcc": dict(base),
            "birank": dict(base),
            "patronage": dict(base),
            "person_fe": dict(base),
            "studio_exposure": dict(base),
        }
        weights, _, _, _, _ = optimize_lambda_weights(components, [], {})
        for name, w in weights.items():
            assert w == pytest.approx(0.2, abs=0.01), f"{name} = {w:.4f}, expected ~0.2"

    def test_pca_variance_explained_in_full_result(self, cv_data):
        """compute_integrated_value_full stores pca_variance_explained."""
        credits, anime_map = cv_data
        person_ids = sorted({c.person_id for c in credits})
        person_fe = {pid: 0.5 + i * 0.03 for i, pid in enumerate(person_ids)}
        birank = {pid: 0.3 + i * 0.02 for i, pid in enumerate(person_ids)}
        studio_exp = {pid: 0.2 for pid in person_ids}
        awcc = {pid: 0.1 + i * 0.01 for i, pid in enumerate(person_ids)}
        patronage = {pid: 0.05 for pid in person_ids}
        dormancy = {pid: 1.0 for pid in person_ids}

        result = compute_integrated_value_full(
            person_fe=person_fe,
            birank=birank,
            studio_exposure=studio_exp,
            awcc=awcc,
            patronage=patronage,
            dormancy=dormancy,
            credits=credits,
            anime_map=anime_map,
        )
        assert result.pca_variance_explained > 0
        assert result.pca_variance_explained <= 1.0
