"""Tests for src/analysis/career/mentor_effect."""

from __future__ import annotations


from src.analysis.career.mentor_effect import (
    MentorEventStudyRow,
    aggregate_mentor_effects,
    compute_pair_event_study,
    estimate_matched_did,
)


# ---------------------------------------------------------------------------
# Pair event study
# ---------------------------------------------------------------------------


class TestPairEventStudy:
    def test_finds_positive_delta(self):
        # theta steady around 1 before event, around 3 after
        series = (
            [(2000 + i, 1.0) for i in range(3)]
            + [(2003, 1.5), (2004, 2.0), (2005, 3.0), (2006, 3.0)]
        )
        row = compute_pair_event_study(("m", "e"), event_year=2003, mentee_year_theta=series)
        assert row is not None
        assert row.delta > 0

    def test_insufficient_pre_returns_none(self):
        series = [(2005, 1.0), (2006, 2.0)]
        row = compute_pair_event_study(("m", "e"), event_year=2003, mentee_year_theta=series)
        assert row is None

    def test_insufficient_post_returns_none(self):
        series = [(2000, 1.0), (2001, 1.0), (2002, 1.0)]
        row = compute_pair_event_study(("m", "e"), event_year=2003, mentee_year_theta=series)
        assert row is None

    def test_custom_windows(self):
        series = [(y, float(y - 1995)) for y in range(2000, 2015)]
        row = compute_pair_event_study(
            ("m", "e"), event_year=2005,
            mentee_year_theta=series,
            pre_window=(-2, -1), post_window=(1, 2),
        )
        assert row is not None
        # series is linearly increasing → post_mean > pre_mean
        assert row.delta > 0


# ---------------------------------------------------------------------------
# Aggregate effect
# ---------------------------------------------------------------------------


class TestAggregateEffect:
    def _make_rows(self, deltas):
        return [
            MentorEventStudyRow(
                mentor_id=f"m{i}", mentee_id=f"e{i}", event_year=2000 + i,
                pre_window=(-3, -1), post_window=(1, 5),
                pre_theta_mean=1.0, post_theta_mean=1.0 + d,
                delta=float(d), n_pre_obs=3, n_post_obs=5,
            )
            for i, d in enumerate(deltas)
        ]

    def test_empty_returns_zeros(self):
        res = aggregate_mentor_effects([])
        assert res.n_pairs == 0
        assert res.mean_delta == 0.0

    def test_positive_mean(self):
        rows = self._make_rows([0.5, 1.0, 1.5, 2.0])
        res = aggregate_mentor_effects(rows, bootstrap_n=200, rng_seed=7)
        assert abs(res.mean_delta - 1.25) < 1e-9
        assert res.n_pairs == 4

    def test_ci_brackets_mean(self):
        rows = self._make_rows([0.5, 1.0, 1.5, 2.0])
        res = aggregate_mentor_effects(rows, bootstrap_n=200, rng_seed=11)
        assert res.ci_low <= res.mean_delta <= res.ci_high

    def test_median_calculated(self):
        rows = self._make_rows([1.0, 2.0, 3.0])
        res = aggregate_mentor_effects(rows, bootstrap_n=100, rng_seed=13)
        assert res.median_delta == 2.0


# ---------------------------------------------------------------------------
# Matched DiD
# ---------------------------------------------------------------------------


class TestMatchedDiD:
    def test_zero_when_no_treated(self):
        res = estimate_matched_did([], {}, bootstrap_n=50)
        assert res.did_estimate == 0.0
        assert res.n_treated == 0

    def test_positive_did_when_treated_grew_more(self):
        treated = [
            MentorEventStudyRow(
                mentor_id="m1", mentee_id="e1", event_year=2005,
                pre_window=(-3, -1), post_window=(1, 5),
                pre_theta_mean=1.0, post_theta_mean=3.0,
                delta=2.0, n_pre_obs=3, n_post_obs=5,
            ),
            MentorEventStudyRow(
                mentor_id="m2", mentee_id="e2", event_year=2005,
                pre_window=(-3, -1), post_window=(1, 5),
                pre_theta_mean=1.0, post_theta_mean=2.5,
                delta=1.5, n_pre_obs=3, n_post_obs=5,
            ),
        ]
        # Controls: small delta (e.g., +0.5)
        controls = {
            f"c{i}": [(y, float(y - 2002) * 0.1 + 1.0) for y in range(2000, 2015)]
            for i in range(10)
        }
        res = estimate_matched_did(treated, controls, bootstrap_n=100, rng_seed=3)
        assert res.did_estimate > 0
        assert res.n_treated == 2
        assert res.n_control > 0

    def test_ci_brackets_did(self):
        treated = [
            MentorEventStudyRow(
                mentor_id="m1", mentee_id="e1", event_year=2005,
                pre_window=(-3, -1), post_window=(1, 5),
                pre_theta_mean=1.0, post_theta_mean=2.0,
                delta=1.0, n_pre_obs=3, n_post_obs=5,
            ),
        ]
        controls = {
            f"c{i}": [(y, 1.0) for y in range(2000, 2015)]
            for i in range(5)
        }
        res = estimate_matched_did(treated, controls, bootstrap_n=100, rng_seed=5)
        assert res.did_ci_low <= res.did_estimate <= res.did_ci_high
