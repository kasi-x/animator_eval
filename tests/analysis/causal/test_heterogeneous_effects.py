"""Tests for src/analysis/causal/heterogeneous_effects."""

from __future__ import annotations

import numpy as np
import pytest

from src.analysis.causal.heterogeneous_effects import (
    HTEDecomposition,
    IndividualCATE,
    estimate_cate_by_subgroup,
    estimate_individual_cate_t_learner,
)


# ---------------------------------------------------------------------------
# Subgroup CATE via interaction-term DiD
# ---------------------------------------------------------------------------


class TestSubgroupCATE:
    def _make_dataset(
        self,
        n_per_cell: int = 100,
        seed: int = 7,
        cate_a: float = 0.5,
        cate_b: float = 2.0,
    ):
        rng = np.random.default_rng(seed)
        # 2 subgroups × {treated, control} = 4 cells
        # CATE_A = cate_a, CATE_B = cate_b
        rows = []
        for sub, cate in [("A", cate_a), ("B", cate_b)]:
            for tr in (0, 1):
                noise = rng.normal(0, 0.3, n_per_cell)
                base = 1.0 if sub == "A" else 1.5
                y_cell = base + cate * tr + noise
                for v in y_cell:
                    rows.append((float(v), tr, sub))
        y = np.array([r[0] for r in rows])
        treated = np.array([r[1] for r in rows])
        subs = [r[2] for r in rows]
        return y, treated, subs

    def test_returns_decomposition(self):
        y, t, s = self._make_dataset()
        res = estimate_cate_by_subgroup(y, t, s)
        assert isinstance(res, HTEDecomposition)
        assert len(res.subgroups) == 2

    def test_recovers_subgroup_cate(self):
        y, t, s = self._make_dataset(cate_a=0.5, cate_b=2.0, n_per_cell=300, seed=11)
        res = estimate_cate_by_subgroup(y, t, s)
        by_label = {sg.subgroup_label: sg for sg in res.subgroups}
        assert abs(by_label["A"].cate - 0.5) < 0.2
        assert abs(by_label["B"].cate - 2.0) < 0.2

    def test_ate_between_subgroup_cates(self):
        y, t, s = self._make_dataset(cate_a=0.5, cate_b=2.0, n_per_cell=300, seed=13)
        res = estimate_cate_by_subgroup(y, t, s)
        # ATE should be the average of subgroup CATEs (roughly)
        avg_cate = np.mean([sg.cate for sg in res.subgroups])
        assert abs(res.ate - avg_cate) < 0.3

    def test_homogeneity_test_rejects_when_cates_differ(self):
        y, t, s = self._make_dataset(cate_a=0.5, cate_b=2.0, n_per_cell=200, seed=17)
        res = estimate_cate_by_subgroup(y, t, s)
        # CATEs differ significantly → homogeneity p small
        if res.homogeneity_test_p is not None:
            assert res.homogeneity_test_p < 0.01

    def test_homogeneity_test_does_not_reject_when_cates_equal(self):
        y, t, s = self._make_dataset(cate_a=1.0, cate_b=1.0, n_per_cell=300, seed=19)
        res = estimate_cate_by_subgroup(y, t, s)
        if res.homogeneity_test_p is not None:
            assert res.homogeneity_test_p > 0.05

    def test_significance_flag_true_when_ci_excludes_zero(self):
        y, t, s = self._make_dataset(cate_a=0.5, cate_b=3.0, n_per_cell=400, seed=23)
        res = estimate_cate_by_subgroup(y, t, s)
        by_label = {sg.subgroup_label: sg for sg in res.subgroups}
        # B has huge effect → significant
        assert by_label["B"].significant is True

    def test_subgroup_var_propagated(self):
        y, t, s = self._make_dataset(n_per_cell=100, seed=29)
        res = estimate_cate_by_subgroup(y, t, s, subgroup_var="cohort_decade")
        assert res.subgroup_var == "cohort_decade"

    def test_labels_applied(self):
        y, t, s = self._make_dataset(n_per_cell=100, seed=31)
        labels = {"A": "1990s", "B": "2010s"}
        res = estimate_cate_by_subgroup(y, t, s, subgroup_labels=labels)
        labels_seen = {sg.subgroup_label for sg in res.subgroups}
        assert labels_seen == {"1990s", "2010s"}

    def test_single_subgroup_raises(self):
        y = np.array([1.0, 2.0, 3.0, 4.0])
        t = np.array([0, 1, 0, 1])
        s = ["A", "A", "A", "A"]
        with pytest.raises(ValueError):
            estimate_cate_by_subgroup(y, t, s)

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            estimate_cate_by_subgroup(
                np.array([]), np.array([]), [],
            )


# ---------------------------------------------------------------------------
# T-learner individual CATE
# ---------------------------------------------------------------------------


class TestTLearner:
    def _make_tlearner_data(self, n=400, seed=41):
        rng = np.random.default_rng(seed)
        X = rng.normal(0, 1, (n, 3))
        treated = (rng.uniform(0, 1, n) > 0.5).astype(int)
        # CATE varies with X[:,0]: heterogeneity
        true_cate = 0.5 + 1.5 * X[:, 0]
        y = X[:, 1] + treated * true_cate + rng.normal(0, 0.5, n)
        return y, treated, X

    def test_returns_individual_cate(self):
        y, t, X = self._make_tlearner_data()
        res = estimate_individual_cate_t_learner(
            y, t, X, ["x1", "x2", "x3"], n_estimators=30,
        )
        assert isinstance(res, IndividualCATE)
        assert res.n == y.size

    def test_quantiles_present(self):
        y, t, X = self._make_tlearner_data(n=200, seed=53)
        res = estimate_individual_cate_t_learner(
            y, t, X, ["x1", "x2", "x3"], n_estimators=20,
        )
        assert 0.1 in res.cate_quantiles
        assert res.cate_quantiles[0.1] <= res.cate_quantiles[0.9]

    def test_top_features_identifies_heterogeneity_driver(self):
        # True heterogeneity is in X[:, 0]
        y, t, X = self._make_tlearner_data(n=500, seed=59)
        res = estimate_individual_cate_t_learner(
            y, t, X, ["x1", "x2", "x3"], n_estimators=50,
        )
        # x1 should rank high
        top_names = [name for name, _ in res.top_features_by_variance]
        assert "x1" in top_names[:2]

    def test_insufficient_treated_raises(self):
        y = np.array([1.0] * 10)
        t = np.array([0] * 10)  # no treated
        X = np.zeros((10, 2))
        with pytest.raises(ValueError):
            estimate_individual_cate_t_learner(y, t, X, ["a", "b"])

    def test_shape_mismatch_raises(self):
        y = np.array([1.0, 2.0])
        t = np.array([0, 1])
        X = np.zeros((3, 2))
        with pytest.raises(ValueError):
            estimate_individual_cate_t_learner(y, t, X, ["a", "b"])
