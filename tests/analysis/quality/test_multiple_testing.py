"""Tests for src/analysis/quality/multiple_testing."""

from __future__ import annotations

import numpy as np
import pytest

from src.analysis.quality.multiple_testing import (
    adjust,
    benjamini_hochberg,
    bonferroni,
    holm,
)


class TestBonferroni:
    def test_empty(self):
        r = bonferroni([])
        assert r.n_tests == 0
        assert r.results == ()

    def test_basic_adjustment(self):
        r = bonferroni([0.01, 0.02, 0.04], alpha=0.05)
        # n=3 → p_adj = min(p*3, 1)
        assert abs(r.results[0].p_adjusted - 0.03) < 1e-9
        assert abs(r.results[1].p_adjusted - 0.06) < 1e-9

    def test_all_reject(self):
        r = bonferroni([0.001, 0.001, 0.001], alpha=0.05)
        assert r.n_rejected_adjusted == 3

    def test_no_reject(self):
        r = bonferroni([0.04, 0.04, 0.04], alpha=0.05)
        # 0.04 * 3 = 0.12, all > 0.05
        assert r.n_rejected_adjusted == 0


class TestHolm:
    def test_empty(self):
        r = holm([])
        assert r.n_tests == 0

    def test_step_down(self):
        # 3 p-values: 0.01, 0.04, 0.03
        # sorted: 0.01, 0.03, 0.04
        # adj: 3*0.01=0.03, 2*0.03=0.06, 1*0.04=0.04
        # step-down running max: 0.03, 0.06, 0.06
        r = holm([0.01, 0.04, 0.03], alpha=0.05)
        adj_by_label = {res.label: res.p_adjusted for res in r.results}
        # H_0 (p=0.01) -> 0.03
        assert abs(adj_by_label["H_0"] - 0.03) < 1e-9
        # H_2 (p=0.03) -> 0.06 (running max preserves)
        assert abs(adj_by_label["H_2"] - 0.06) < 1e-9
        # H_1 (p=0.04) -> 0.06
        assert abs(adj_by_label["H_1"] - 0.06) < 1e-9

    def test_holm_at_least_as_powerful_as_bonferroni(self):
        # Holm rejects at least as many as Bonferroni
        pvals = [0.001, 0.01, 0.03, 0.05, 0.1]
        rb = bonferroni(pvals, alpha=0.05)
        rh = holm(pvals, alpha=0.05)
        assert rh.n_rejected_adjusted >= rb.n_rejected_adjusted


class TestBenjaminiHochberg:
    def test_empty(self):
        r = benjamini_hochberg([])
        assert r.n_tests == 0

    def test_basic(self):
        # 4 p-values: 0.01, 0.04, 0.05, 0.1
        # adj at rank: (4/1)*0.01=0.04, (4/2)*0.04=0.08, (4/3)*0.05≈0.0667, (4/4)*0.1=0.1
        # step-up min from right: 0.1, 0.0667, 0.0667, 0.04
        r = benjamini_hochberg([0.01, 0.04, 0.05, 0.1], alpha=0.05)
        adj = {res.label: res.p_adjusted for res in r.results}
        assert abs(adj["H_0"] - 0.04) < 1e-9
        # H_1 (0.04) -> step-up considers (4/2)*0.04=0.08 vs subsequent → 0.0667
        assert adj["H_1"] < adj["H_2"] + 1e-9

    def test_bh_at_least_as_powerful_as_bonferroni(self):
        pvals = [0.001, 0.01, 0.03, 0.05, 0.1]
        rb = bonferroni(pvals, alpha=0.05)
        rbh = benjamini_hochberg(pvals, alpha=0.05)
        assert rbh.n_rejected_adjusted >= rb.n_rejected_adjusted


class TestAdjustDispatcher:
    def test_bonferroni(self):
        r = adjust("bonferroni", [0.01], alpha=0.05)
        assert r.method == "bonferroni"

    def test_holm(self):
        r = adjust("holm", [0.01], alpha=0.05)
        assert r.method == "holm"

    def test_bh(self):
        r = adjust("bh", [0.01], alpha=0.05)
        assert r.method == "bh"

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            adjust("unknown", [0.01])  # type: ignore[arg-type]


class TestEdgeCases:
    def test_labels_length_mismatch(self):
        with pytest.raises(ValueError):
            bonferroni([0.01, 0.02], labels=["a"])

    def test_adjusted_clipped_at_one(self):
        # p=1.0, n=5 → adj = 5*1.0 = 5, clipped to 1.0
        r = bonferroni([1.0, 1.0, 1.0, 1.0, 1.0])
        for res in r.results:
            assert res.p_adjusted == 1.0

    def test_n_rejected_raw_vs_adjusted(self):
        # adjusted ≤ raw always
        rng = np.random.default_rng(7)
        pvals = rng.uniform(0, 0.1, 20).tolist()
        for method in ["bonferroni", "holm", "bh"]:
            r = adjust(method, pvals, alpha=0.05)  # type: ignore[arg-type]
            assert r.n_rejected_adjusted <= r.n_rejected_raw
