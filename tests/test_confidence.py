"""confidence モジュールのテスト."""

from src.analysis.confidence import (
    batch_compute_confidence,
    compute_confidence,
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


class TestBatchComputeConfidence:
    def test_adds_fields(self):
        results = [
            {
                "person_id": "p1",
                "authority": 80.0,
                "trust": 70.0,
                "skill": 60.0,
                "composite": 71.0,
                "total_credits": 15,
            },
        ]
        updated = batch_compute_confidence(results)
        assert "confidence" in updated[0]
        assert "score_range" in updated[0]
        assert "authority" in updated[0]["score_range"]

    def test_uses_career_data(self):
        results = [
            {
                "person_id": "p1",
                "authority": 50.0,
                "trust": 50.0,
                "skill": 50.0,
                "composite": 50.0,
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
                "authority": 50.0,
                "trust": 50.0,
                "skill": 50.0,
                "composite": 50.0,
                "total_credits": 0,
            },
        ]
        updated = batch_compute_confidence(results, credits_per_person={"p1": 30})
        assert updated[0]["confidence"] > 0.5
