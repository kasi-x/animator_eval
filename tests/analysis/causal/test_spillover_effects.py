"""Tests for src/analysis/causal/spillover_effects."""

from __future__ import annotations

import numpy as np
import pytest

from src.analysis.causal.spillover_effects import (
    PeerObservation,
    SpilloverEstimate,
    compute_peer_means,
    estimate_spillover_2sls,
)


class TestComputePeerMeans:
    def test_basic_three_anime(self):
        # 3 anime, persons overlapping
        credits = [
            ("p1", "a1", 2010), ("p2", "a1", 2010),
            ("p2", "a2", 2011), ("p3", "a2", 2011),
            ("p1", "a3", 2012), ("p3", "a3", 2012),
        ]
        thetas = {"p1": 1.0, "p2": 2.0, "p3": 3.0}
        obs = compute_peer_means(credits, thetas)
        # p1 in a1 with p2: peer_mean = 2.0
        # p1 in a3 with p3: peer_mean = 3.0
        # ...
        assert len(obs) > 0
        for o in obs:
            assert o.n_peers >= 1
            assert o.peer_mean_theta > 0

    def test_no_peer_excluded(self):
        # Single-person anime → no peer → excluded
        credits = [("p1", "a1", 2010)]
        thetas = {"p1": 1.0}
        obs = compute_peer_means(credits, thetas)
        assert obs == []

    def test_no_peer_of_peer_excluded(self):
        # Isolated pair: p1+p2 in a1, no other co-credits → no peer-of-peer
        credits = [("p1", "a1", 2010), ("p2", "a1", 2010)]
        thetas = {"p1": 1.0, "p2": 2.0}
        obs = compute_peer_means(credits, thetas)
        # No 2-hop neighbors → excluded
        assert obs == []

    def test_missing_theta_excluded(self):
        credits = [("p1", "a1", 2010), ("p2", "a1", 2010)]
        thetas = {"p1": 1.0}  # p2 missing
        obs = compute_peer_means(credits, thetas)
        # p1 has peer p2 with no theta → no peer with theta
        assert obs == []


class TestEstimateSpillover2SLS:
    def _synth_observations(self, n=200, true_beta=0.5, seed=7):
        rng = np.random.default_rng(seed)
        z = rng.normal(0, 1, n)  # peer-of-peer (instrument)
        # Stage 1: peer_mean = 0.7 z + e1
        peer_mean = 0.7 * z + rng.normal(0, 0.3, n)
        # True y = β * peer_mean + e2
        y = true_beta * peer_mean + rng.normal(0, 0.5, n)
        obs = [
            PeerObservation(
                person_id=f"p{i}", year=2010, anime_id=f"a{i}",
                own_theta=float(y[i]),
                peer_mean_theta=float(peer_mean[i]),
                peer_of_peer_mean_theta=float(z[i]),
                n_peers=3,
            )
            for i in range(n)
        ]
        return obs

    def test_returns_estimate(self):
        obs = self._synth_observations()
        est = estimate_spillover_2sls(obs)
        assert isinstance(est, SpilloverEstimate)
        assert est.n_obs == len(obs)

    def test_recovers_true_beta(self):
        # n=500 + strong instrument → β recovery
        obs = self._synth_observations(n=500, true_beta=0.5, seed=11)
        est = estimate_spillover_2sls(obs)
        assert abs(est.beta_peer - 0.5) < 0.1

    def test_ci_brackets_point(self):
        obs = self._synth_observations()
        est = estimate_spillover_2sls(obs)
        assert est.ci_low <= est.beta_peer <= est.ci_high

    def test_weak_iv_flagged_for_low_f(self):
        # Synthesize weak instrument
        rng = np.random.default_rng(17)
        n = 100
        z = rng.normal(0, 1, n)
        # Stage 1: peer ~ 0.05 z (very weak)
        peer = 0.05 * z + rng.normal(0, 1, n)
        y = 0.5 * peer + rng.normal(0, 1, n)
        obs = [
            PeerObservation(
                person_id=f"p{i}", year=2010, anime_id=f"a{i}",
                own_theta=float(y[i]),
                peer_mean_theta=float(peer[i]),
                peer_of_peer_mean_theta=float(z[i]),
                n_peers=2,
            )
            for i in range(n)
        ]
        est = estimate_spillover_2sls(obs)
        assert est.weak_iv_flag is True
        assert "weak IV" in " ".join(est.notes)

    def test_empty_returns_zero(self):
        est = estimate_spillover_2sls([])
        assert est.beta_peer == 0.0
        assert est.n_obs == 0
        assert est.weak_iv_flag is True
