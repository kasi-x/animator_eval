"""data_quality モジュールのテスト."""

from src.analysis.data_quality import compute_data_quality_score


class TestComputeDataQualityScore:
    def test_perfect_data(self):
        result = compute_data_quality_score(
            stats={"latest_year": 2025},
            credits_with_source=1000,
            total_credits=1000,
            persons_with_score=100,
            total_persons=100,
            anime_with_year=50,
            total_anime=50,
            anime_with_score=50,
            source_count=3,
        )
        assert result["overall_score"] >= 80

    def test_empty_data(self):
        result = compute_data_quality_score(
            stats={},
            total_credits=0,
            total_persons=0,
            total_anime=0,
        )
        assert result["overall_score"] < 30
        assert len(result["recommendations"]) > 0

    def test_dimensions_present(self):
        result = compute_data_quality_score(
            stats={"latest_year": 2024},
            credits_with_source=50,
            total_credits=100,
            persons_with_score=80,
            total_persons=100,
            anime_with_year=40,
            total_anime=50,
            anime_with_score=30,
            source_count=2,
        )
        for dim in ("completeness", "coverage", "diversity", "volume", "freshness"):
            assert dim in result["dimensions"]
            assert 0 <= result["dimensions"][dim]["score"] <= 100

    def test_low_source_coverage(self):
        result = compute_data_quality_score(
            stats={"latest_year": 2024},
            credits_with_source=10,
            total_credits=100,
            persons_with_score=50,
            total_persons=100,
            anime_with_year=40,
            total_anime=50,
            anime_with_score=30,
            source_count=1,
        )
        # Should recommend more sources
        assert any("ソース" in r or "source" in r.lower() for r in result["recommendations"])

    def test_stale_data(self):
        result = compute_data_quality_score(
            stats={"latest_year": 2015},
            credits_with_source=100,
            total_credits=100,
            persons_with_score=50,
            total_persons=50,
            anime_with_year=30,
            total_anime=30,
            anime_with_score=30,
            source_count=2,
        )
        assert result["dimensions"]["freshness"]["score"] <= 60

    def test_overall_score_range(self):
        result = compute_data_quality_score(
            stats={"latest_year": 2024},
            credits_with_source=500,
            total_credits=1000,
            persons_with_score=80,
            total_persons=100,
            anime_with_year=45,
            total_anime=50,
            anime_with_score=40,
            source_count=2,
        )
        assert 0 <= result["overall_score"] <= 100

    def test_high_volume_score(self):
        result = compute_data_quality_score(
            stats={"latest_year": 2024},
            credits_with_source=10000,
            total_credits=10000,
            persons_with_score=1000,
            total_persons=1000,
            anime_with_year=500,
            total_anime=500,
            anime_with_score=500,
            source_count=4,
        )
        assert result["dimensions"]["volume"]["score"] == 100
