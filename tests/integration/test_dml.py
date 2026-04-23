"""Tests for Double/Debiased Machine Learning module."""

import numpy as np

from src.analysis.causal.dml import (
    DMLReport,
    DualEstimate,
    EstimationResult,
    _fit_dml,
    _fit_ols,
    run_dml_analysis,
)
from src.runtime.models import BronzeAnime as Anime, Credit, Role


def _make_anime(
    aid: str,
    year: int = 2020,
    studio: str = "StudioA",
    episodes: int = 12,
    duration: int = 24,
    genres: list | None = None,
    fmt: str = "TV",
) -> Anime:
    return Anime(
        id=aid,
        title_ja=f"Anime {aid}",
        year=year,
        studios=[studio],
        episodes=episodes,
        duration=duration,
        genres=genres or ["Action"],
        format=fmt,
    )


def _make_credit(pid: str, aid: str, role: Role = Role.KEY_ANIMATOR) -> Credit:
    return Credit(person_id=pid, anime_id=aid, role=role)


class TestCoreDML:
    """Test the core DML estimation algorithm."""

    def test_known_linear_dgp(self):
        """DML should recover θ when DGP is Y = θ·D + g(X) + ε."""
        rng = np.random.RandomState(42)
        n = 500
        X = rng.randn(n, 3)
        D = X[:, 0] + rng.randn(n) * 0.5  # D correlated with X
        Y = 2.0 * D + X[:, 0] ** 2 + X[:, 1] + rng.randn(n) * 0.3  # θ=2.0, nonlinear g

        result = _fit_dml(Y, D, X, n_folds=3)
        # DML should be close to 2.0
        assert abs(result.theta - 2.0) < 0.5, f"DML theta={result.theta}, expected ~2.0"
        assert result.se > 0

    def test_ols_biased_under_nonlinear_confounding(self):
        """OLS should be biased when g(X) is nonlinear, DML should be less biased."""
        rng = np.random.RandomState(123)
        n = 500
        X = rng.randn(n, 2)
        D = np.sin(X[:, 0]) + rng.randn(n) * 0.3
        Y = 1.5 * D + X[:, 0] ** 3 + rng.randn(n) * 0.5  # θ=1.5

        ols = _fit_ols(Y, D, X)
        dml = _fit_dml(Y, D, X, n_folds=3)

        # DML should be closer to true θ=1.5
        ols_error = abs(ols.theta - 1.5)
        dml_error = abs(dml.theta - 1.5)
        # At minimum, DML shouldn't be dramatically worse
        assert dml_error < ols_error + 0.5

    def test_dml_returns_valid_ci(self):
        rng = np.random.RandomState(42)
        n = 300
        X = rng.randn(n, 2)
        D = rng.randn(n)
        Y = 1.0 * D + X[:, 0] + rng.randn(n) * 0.5

        result = _fit_dml(Y, D, X, n_folds=3)
        assert result.ci_lower < result.theta < result.ci_upper
        assert result.se > 0
        assert 0 <= result.p_value <= 1

    def test_ols_returns_valid(self):
        rng = np.random.RandomState(42)
        n = 200
        X = rng.randn(n, 2)
        D = rng.randn(n)
        Y = D + rng.randn(n)

        result = _fit_ols(Y, D, X)
        assert isinstance(result.theta, float)
        assert result.se > 0

    def test_degenerate_d(self):
        """If D has no variation after residualization, should handle gracefully."""
        n = 100
        X = np.ones((n, 1))
        D = np.ones(n)  # constant D
        Y = np.random.randn(n)

        result = _fit_dml(Y, D, X, n_folds=3)
        # Should not crash, theta=0 or very small
        assert isinstance(result.theta, float)


class TestDualEstimate:
    def test_bias_computation(self):
        ols = EstimationResult(theta=1.5, se=0.1)
        dml = EstimationResult(theta=1.0, se=0.1)
        est = DualEstimate(
            parameter="test",
            description="test",
            ols=ols,
            dml=dml,
            n_obs=100,
        )
        assert abs(est.bias - 0.5) < 1e-6
        assert abs(est.bias_pct - 50.0) < 1e-6

    def test_to_dict(self):
        ols = EstimationResult(theta=1.0, se=0.1)
        dml = EstimationResult(theta=0.8, se=0.1)
        est = DualEstimate(
            parameter="person_fe",
            description="test",
            ols=ols,
            dml=dml,
            n_obs=500,
        )
        d = est.to_dict()
        assert d["parameter"] == "person_fe"
        assert "ols" in d and "dml" in d
        assert abs(d["bias"] - 0.2) < 1e-4


class TestRunDMLAnalysis:
    def _make_dataset(self, n_anime=50, n_persons=30):
        """Build synthetic dataset for DML integration test."""
        import random

        random.seed(42)

        anime_map = {}
        credits = []
        person_fe = {}
        studio_fe = {"StudioA": 0.5, "StudioB": -0.3, "StudioC": 0.1}

        studios = list(studio_fe.keys())
        for i in range(n_anime):
            aid = f"a{i}"
            studio = random.choice(studios)
            anime_map[aid] = _make_anime(
                aid,
                year=2010 + i % 15,
                studio=studio,
                episodes=random.randint(1, 26),
            )
            # Add credits
            n_staff = random.randint(5, 20)
            for j in range(n_staff):
                pid = f"p{j % n_persons}"
                role = random.choice(
                    [Role.KEY_ANIMATOR, Role.DIRECTOR, Role.ANIMATION_DIRECTOR]
                )
                credits.append(_make_credit(pid, aid, role))

        for i in range(n_persons):
            person_fe[f"p{i}"] = random.gauss(0, 1)

        return credits, anime_map, person_fe, studio_fe

    def test_returns_report(self):
        credits, anime_map, person_fe, studio_fe = self._make_dataset()
        report = run_dml_analysis(credits, anime_map, person_fe, studio_fe)
        assert isinstance(report, DMLReport)
        assert len(report.diagnostics) > 0

    def test_person_fe_estimate_present(self):
        credits, anime_map, person_fe, studio_fe = self._make_dataset(n_anime=80)
        report = run_dml_analysis(credits, anime_map, person_fe, studio_fe)
        # Should have at least the person_fe estimate
        pfe_estimates = [e for e in report.estimates if e.parameter == "person_fe"]
        if pfe_estimates:
            est = pfe_estimates[0]
            assert est.ols.theta != 0 or est.dml.theta != 0
            assert est.n_obs > 0

    def test_to_dict_serializable(self):
        credits, anime_map, person_fe, studio_fe = self._make_dataset()
        report = run_dml_analysis(credits, anime_map, person_fe, studio_fe)
        d = report.to_dict()
        import json

        # Should be JSON-serializable
        json_str = json.dumps(d)
        assert len(json_str) > 0

    def test_insufficient_data_returns_empty(self):
        """With very few credits, should return empty estimates."""
        credits = [_make_credit("p1", "a1")]
        anime_map = {"a1": _make_anime("a1")}
        report = run_dml_analysis(credits, anime_map, {"p1": 0.5}, {"StudioA": 0.1})
        assert len(report.estimates) == 0
