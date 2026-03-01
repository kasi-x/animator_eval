"""confidence モジュールのテスト."""

import math

from src.analysis.confidence import (
    batch_compute_confidence,
    compute_confidence,
    compute_person_fe_ci,
    compute_score_range,
)


class TestComputeConfidence:
    def test_zero_credits(self):
        assert compute_confidence(0) == 0.0

    def test_one_credit(self):
        c = compute_confidence(1)
        assert 0.0 < c < 0.5

    def test_many_credits(self):
        c = compute_confidence(50)
        assert c > 0.9

    def test_monotonically_increasing(self):
        """More credits = higher confidence."""
        prev = 0.0
        for n in [1, 2, 5, 10, 20, 50, 100]:
            c = compute_confidence(n)
            assert c >= prev
            prev = c

    def test_source_diversity_bonus(self):
        single = compute_confidence(10, source_count=1)
        multi = compute_confidence(10, source_count=3)
        assert multi > single

    def test_year_span_bonus(self):
        short = compute_confidence(10, year_span=1)
        long_career = compute_confidence(10, year_span=10)
        assert long_career > short

    def test_max_confidence_is_one(self):
        c = compute_confidence(1000, source_count=5, year_span=20)
        assert c <= 1.0


class TestComputeScoreRange:
    def test_high_confidence_narrow_range(self):
        lower, upper = compute_score_range(50.0, 0.99)
        assert lower == 50.0
        assert upper == 50.0

    def test_low_confidence_wide_range(self):
        lower, upper = compute_score_range(50.0, 0.1)
        assert lower < 10.0
        assert upper > 90.0

    def test_range_within_bounds(self):
        lower, upper = compute_score_range(5.0, 0.0)
        assert lower >= 0.0
        assert upper <= 100.0

    def test_zero_confidence_max_range(self):
        lower, upper = compute_score_range(50.0, 0.0)
        assert lower == 0.0
        assert upper == 100.0


class TestComputePersonFeCi:
    """Tests for analytical person_fe confidence interval (B09 fix)."""

    def test_formula_correctness_95(self):
        """CI = theta +/- 1.96 * sigma / sqrt(n) for large n."""
        theta = 2.0
        sigma = 1.0
        n_obs = 100  # large enough for z-approximation
        lower, upper = compute_person_fe_ci(theta, n_obs, sigma, ci_level=0.95)
        expected_se = sigma / math.sqrt(n_obs)  # 0.1
        expected_half = 1.96 * expected_se  # 0.196
        assert abs(lower - (theta - expected_half)) < 0.001
        assert abs(upper - (theta + expected_half)) < 0.001

    def test_more_obs_narrower_ci(self):
        """More observations should produce narrower confidence intervals."""
        theta = 1.5
        sigma = 0.8
        _, upper_few = compute_person_fe_ci(theta, 5, sigma)
        _, upper_many = compute_person_fe_ci(theta, 50, sigma)
        width_few = upper_few - theta
        width_many = upper_many - theta
        assert width_few > width_many

    def test_higher_sigma_wider_ci(self):
        """Higher residual variance should produce wider intervals."""
        theta = 1.0
        n_obs = 30
        _, upper_low = compute_person_fe_ci(theta, n_obs, 0.5)
        _, upper_high = compute_person_fe_ci(theta, n_obs, 2.0)
        width_low = upper_low - theta
        width_high = upper_high - theta
        assert width_high > width_low

    def test_degenerate_single_obs(self):
        """With n_obs < 2, CI should collapse to point estimate."""
        theta = 3.0
        lower, upper = compute_person_fe_ci(theta, 1, 1.0)
        assert lower == theta
        assert upper == theta

    def test_degenerate_zero_sigma(self):
        """With zero sigma, CI should collapse to point estimate."""
        theta = 3.0
        lower, upper = compute_person_fe_ci(theta, 10, 0.0)
        assert lower == theta
        assert upper == theta

    def test_negative_sigma_degenerate(self):
        """Negative sigma (invalid) should return point estimate."""
        theta = 3.0
        lower, upper = compute_person_fe_ci(theta, 10, -1.0)
        assert lower == theta
        assert upper == theta

    def test_small_sample_uses_t_distribution(self):
        """For n < 30, t-distribution critical value > 1.96."""
        theta = 1.0
        sigma = 1.0
        n_small = 5  # df=4, t_{0.025,4} = 2.776
        lower_small, upper_small = compute_person_fe_ci(theta, n_small, sigma)
        # Width should be wider than z-based (1.96) interval
        se = sigma / math.sqrt(n_small)
        z_width = 1.96 * se
        actual_width = upper_small - theta
        assert actual_width > z_width, (
            f"t-based width {actual_width} should exceed z-based width {z_width}"
        )

    def test_large_sample_uses_z_approximation(self):
        """For n >= 30, should use z=1.96."""
        theta = 1.0
        sigma = 1.0
        n_large = 100
        lower, upper = compute_person_fe_ci(theta, n_large, sigma)
        se = sigma / math.sqrt(n_large)
        expected_half = 1.96 * se
        assert abs((upper - lower) / 2 - expected_half) < 0.001

    def test_ci_symmetric(self):
        """CI should be symmetric around theta."""
        theta = 2.5
        lower, upper = compute_person_fe_ci(theta, 20, 1.0)
        assert abs((upper - theta) - (theta - lower)) < 0.001

    def test_99_percent_wider_than_95(self):
        """99% CI should be wider than 95% CI."""
        theta = 1.0
        sigma = 1.0
        n_obs = 50
        _, upper_95 = compute_person_fe_ci(theta, n_obs, sigma, ci_level=0.95)
        _, upper_99 = compute_person_fe_ci(theta, n_obs, sigma, ci_level=0.99)
        assert upper_99 > upper_95


class TestBatchComputeConfidence:
    def test_adds_fields(self):
        results = [
            {
                "person_id": "p1",
                "birank": 80.0,
                "patronage": 70.0,
                "person_fe": 60.0,
                "iv_score": 71.0,
                "total_credits": 15,
                # Percentile fields (B08 fix: batch uses _pct for score_range)
                "birank_pct": 80.0,
                "patronage_pct": 70.0,
                "person_fe_pct": 60.0,
                "iv_score_pct": 71.0,
            },
        ]
        updated = batch_compute_confidence(results)
        assert "confidence" in updated[0]
        assert "score_range" in updated[0]
        assert "birank" in updated[0]["score_range"]
        assert "iv_score" in updated[0]["score_range"]
        assert "patronage" in updated[0]["score_range"]
        assert "person_fe" in updated[0]["score_range"]

    def test_uses_pct_for_heuristic_ranges(self):
        """B08 fix: score_range should use *_pct fields, not raw scores."""
        results = [
            {
                "person_id": "p1",
                "birank": 0.005,  # Raw birank is tiny (not on 0-100 scale)
                "patronage": 0.001,
                "person_fe": 2.3,  # Raw person_fe (log-scale)
                "iv_score": 0.8,
                "total_credits": 5,
                # Percentile values are on 0-100 scale
                "birank_pct": 75.0,
                "patronage_pct": 60.0,
                "person_fe_pct": 80.0,
                "iv_score_pct": 65.0,
            },
        ]
        updated = batch_compute_confidence(results)
        sr = updated[0]["score_range"]
        # With moderate confidence (~0.54 for 5 credits), ranges should be
        # centered around the _pct values (0-100), not the raw values
        # The lower bound of birank should be somewhere > 0 if pct=75
        lo_birank, hi_birank = sr["birank"]
        assert lo_birank > 20.0, (
            f"birank lower bound {lo_birank} should be > 20 for pct=75, "
            "not near 0 (which would mean raw score was used)"
        )

    def test_uses_career_data(self):
        results = [
            {
                "person_id": "p1",
                "birank": 50.0,
                "patronage": 50.0,
                "person_fe": 50.0,
                "iv_score": 50.0,
                "total_credits": 10,
                "career": {"active_years": 15, "first_year": 2010, "latest_year": 2025},
            },
        ]
        updated = batch_compute_confidence(results)
        assert updated[0]["confidence"] > 0

    def test_external_credit_counts(self):
        results = [
            {
                "person_id": "p1",
                "birank": 50.0,
                "patronage": 50.0,
                "person_fe": 50.0,
                "iv_score": 50.0,
                "total_credits": 0,
            },
        ]
        updated = batch_compute_confidence(results, credits_per_person={"p1": 30})
        assert updated[0]["confidence"] > 0.5

    def test_akm_residuals_analytical_ci(self):
        """B09 fix: When AKM residuals are provided, person_fe CI should be analytical."""
        results = [
            {
                "person_id": "p1",
                "person_fe": 1.5,
                "birank": 50.0,
                "patronage": 50.0,
                "iv_score": 50.0,
                "total_credits": 10,
                "person_fe_pct": 75.0,
                "birank_pct": 50.0,
                "patronage_pct": 50.0,
                "iv_score_pct": 50.0,
            },
            {
                "person_id": "p2",
                "person_fe": 0.5,
                "birank": 30.0,
                "patronage": 30.0,
                "iv_score": 30.0,
                "total_credits": 3,
                "person_fe_pct": 25.0,
                "birank_pct": 30.0,
                "patronage_pct": 30.0,
                "iv_score_pct": 30.0,
            },
        ]
        # p1 has 5 observations, p2 has 2 observations
        akm_residuals = {
            ("p1", "a1"): 0.1,
            ("p1", "a2"): -0.2,
            ("p1", "a3"): 0.15,
            ("p1", "a4"): -0.05,
            ("p1", "a5"): 0.0,
            ("p2", "a1"): 0.3,
            ("p2", "a6"): -0.1,
        }
        updated = batch_compute_confidence(results, akm_residuals=akm_residuals)

        # person_fe CI should be analytical (centered on person_fe value)
        sr_p1 = updated[0]["score_range"]["person_fe"]
        lower_p1, upper_p1 = sr_p1
        assert lower_p1 < 1.5
        assert upper_p1 > 1.5
        # CI should be centered on person_fe
        mid = (lower_p1 + upper_p1) / 2
        assert abs(mid - 1.5) < 0.001

        # p2 has fewer obs, so wider CI (relative to SE)
        sr_p2 = updated[1]["score_range"]["person_fe"]
        lower_p2, upper_p2 = sr_p2
        width_p1 = upper_p1 - lower_p1
        width_p2 = upper_p2 - lower_p2
        assert width_p2 > width_p1, (
            f"p2 width {width_p2} should be wider than p1 width {width_p1} "
            "because p2 has fewer observations"
        )

    def test_akm_residuals_single_obs_falls_back(self):
        """Person with only 1 observation should fall back to heuristic CI."""
        results = [
            {
                "person_id": "p1",
                "person_fe": 1.0,
                "birank": 50.0,
                "patronage": 50.0,
                "iv_score": 50.0,
                "total_credits": 5,
                "person_fe_pct": 50.0,
                "birank_pct": 50.0,
                "patronage_pct": 50.0,
                "iv_score_pct": 50.0,
            },
        ]
        # Only 1 observation for p1
        akm_residuals = {("p1", "a1"): 0.1}
        updated = batch_compute_confidence(results, akm_residuals=akm_residuals)
        sr = updated[0]["score_range"]["person_fe"]
        # Should fall back to heuristic (centered on person_fe_pct=50)
        lower, upper = sr
        # Heuristic CI on pct=50.0 with moderate confidence should be symmetric
        # around 50 and within [0, 100]
        assert lower >= 0.0
        assert upper <= 100.0

    def test_no_akm_residuals_uses_heuristic(self):
        """Without AKM residuals, person_fe should use heuristic CI on pct."""
        results = [
            {
                "person_id": "p1",
                "person_fe": 2.0,  # raw (not on 0-100 scale)
                "birank": 50.0,
                "patronage": 50.0,
                "iv_score": 50.0,
                "total_credits": 10,
                "person_fe_pct": 80.0,
                "birank_pct": 50.0,
                "patronage_pct": 50.0,
                "iv_score_pct": 50.0,
            },
        ]
        updated = batch_compute_confidence(results)
        sr = updated[0]["score_range"]["person_fe"]
        lower, upper = sr
        # Heuristic CI centered on pct=80, bounded by [0, 100]
        assert lower >= 0.0
        assert upper <= 100.0
        # Should be in the vicinity of 80, not 2.0
        mid = (lower + upper) / 2
        assert mid > 50.0, (
            f"Mid-point {mid} should be > 50 since person_fe_pct=80"
        )
