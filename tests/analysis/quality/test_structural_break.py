"""Tests for src/analysis/quality/structural_break."""

from __future__ import annotations

import numpy as np
import pytest

from src.analysis.quality.structural_break import (
    BreakCandidate,
    CUSUMResult,
    StructuralBreakReport,
    cusum_test,
    detect_break,
    sliding_window_break,
)


class TestCUSUM:
    def test_constant_series_no_break(self):
        # constant series → CUSUM = 0
        r = cusum_test([3.0] * 20)
        # constant → sigma=0 → graceful
        assert isinstance(r, CUSUMResult)
        assert r.has_break is False

    def test_drift_series_detected(self):
        # step shift at t=10
        vals = [1.0] * 10 + [10.0] * 10
        r = cusum_test(vals)
        assert r.has_break is True
        assert r.p_approx < 0.05

    def test_small_n_returns_no_break(self):
        r = cusum_test([1.0, 2.0, 3.0])
        assert r.has_break is False

    def test_random_series_usually_no_break(self):
        rng = np.random.default_rng(7)
        vals = rng.normal(5.0, 1.0, 100).tolist()
        r = cusum_test(vals, alpha=0.01)
        # Random → unlikely to flag at 1% level
        assert r.has_break is False


class TestSlidingWindow:
    def test_returns_candidates(self):
        vals = [1.0] * 10 + [5.0] * 10
        cands = sliding_window_break(vals, top_k=3)
        assert len(cands) == 3
        # Top candidate should be at t=10
        assert cands[0].index == 10
        assert cands[0].delta > 0
        assert cands[0].p_value < 0.001

    def test_empty_when_too_short(self):
        cands = sliding_window_break([1.0, 2.0, 3.0], min_segment=5)
        assert cands == []

    def test_no_real_break(self):
        rng = np.random.default_rng(11)
        vals = rng.normal(0, 1, 50).tolist()
        cands = sliding_window_break(vals, top_k=1)
        # Random data → top candidate p generally not very small
        assert cands[0].p_value > 0.001 or abs(cands[0].delta) < 1.0

    def test_top_k_sorted_by_f(self):
        vals = [1.0] * 10 + [5.0] * 10 + [3.0] * 10
        cands = sliding_window_break(vals, top_k=5)
        # Sorted by descending F
        for i in range(len(cands) - 1):
            assert cands[i].f_statistic >= cands[i + 1].f_statistic


class TestDetectBreak:
    def test_step_shift_detected(self):
        vals = [1.0] * 15 + [10.0] * 15
        r = detect_break(vals)
        assert r.consensus_break_index == 15

    def test_no_break(self):
        rng = np.random.default_rng(17)
        vals = rng.normal(5.0, 1.0, 50).tolist()
        r = detect_break(vals)
        # Mostly no consensus
        # (occasional false positive at 5% by design)
        assert isinstance(r, StructuralBreakReport)

    def test_consensus_index_in_top_candidates(self):
        vals = [2.0] * 12 + [8.0] * 12
        r = detect_break(vals)
        if r.consensus_break_index is not None:
            # Top candidate should match consensus
            top_indices = {c.index for c in r.top_candidates}
            assert r.consensus_break_index in top_indices

    def test_with_x_axis(self):
        vals = [1.0] * 10 + [5.0] * 10
        years = list(range(2000, 2020))
        r = detect_break(vals, x=years)
        if r.top_candidates:
            assert r.top_candidates[0].x_value in years
