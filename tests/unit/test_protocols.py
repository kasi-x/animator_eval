"""Tests for analysis function protocols and dataclass definitions."""

from dataclasses import asdict

from src.analysis.protocols import (
    GrowthMetrics,
    NetworkDensityMetrics,
    ProductivityMetrics,
    VersatilityMetrics,
)


class TestVersatilityMetrics:
    """Test VersatilityMetrics dataclass."""

    def test_versatility_metrics_defaults(self):
        """VersatilityMetrics should have sensible defaults."""
        metrics = VersatilityMetrics()
        assert metrics.categories == []
        assert metrics.category_count == 0
        assert metrics.versatility_score == 0.0

    def test_versatility_metrics_complete(self):
        """VersatilityMetrics should accept all expected fields."""
        metrics = VersatilityMetrics(
            categories=["animation", "direction"],
            category_count=2,
            roles=["KEY_ANIMATOR", "DIRECTOR"],
            role_count=2,
            versatility_score=75.5,
            category_credits={"animation": 10, "direction": 5},
        )
        assert metrics.category_count == 2
        assert metrics.versatility_score == 75.5
        assert metrics.category_credits["animation"] == 10

    def test_versatility_metrics_to_dict(self):
        """VersatilityMetrics should be serializable to dict."""
        metrics = VersatilityMetrics(
            categories=["animation"],
            category_count=1,
            versatility_score=50.0,
        )
        data = asdict(metrics)
        assert data["category_count"] == 1
        assert data["versatility_score"] == 50.0


class TestGrowthMetrics:
    """Test GrowthMetrics dataclass."""

    def test_growth_metrics_defaults(self):
        """GrowthMetrics should have sensible defaults."""
        metrics = GrowthMetrics()
        assert metrics.yearly_credits == {}
        assert metrics.trend == "new"
        assert metrics.total_credits == 0
        assert metrics.recent_avg_anime_score is None

    def test_growth_metrics_complete(self):
        """GrowthMetrics should accept all fields."""
        metrics = GrowthMetrics(
            yearly_credits={2020: 5, 2021: 8, 2022: 10},
            trend="rising",
            total_credits=23,
            recent_credits=10,
            total_years=3,
            career_span=3,
            activity_ratio=0.9,
            recent_avg_anime_score=7.5,
            career_avg_anime_score=7.2,
            current_score=85.0,
        )
        assert metrics.trend == "rising"
        assert metrics.activity_ratio == 0.9
        assert metrics.recent_avg_anime_score == 7.5

    def test_growth_metrics_without_optionals(self):
        """GrowthMetrics works without optional fields."""
        metrics = GrowthMetrics(
            yearly_credits={2020: 5},
            trend="stable",
            total_credits=5,
            recent_credits=3,
            total_years=1,
            career_span=1,
            activity_ratio=1.0,
        )
        assert metrics.trend == "stable"
        assert metrics.recent_avg_anime_score is None
        assert metrics.career_avg_anime_score is None


class TestNetworkDensityMetrics:
    """Test NetworkDensityMetrics dataclass."""

    def test_network_density_defaults(self):
        """NetworkDensityMetrics should have sensible defaults."""
        metrics = NetworkDensityMetrics()
        assert metrics.collaborator_count == 0
        assert metrics.hub_score == 0.0
        assert metrics.avg_collaborator_score is None

    def test_network_density_complete(self):
        """NetworkDensityMetrics should accept all fields."""
        metrics = NetworkDensityMetrics(
            collaborator_count=50,
            unique_anime=25,
            hub_score=2.0,
            avg_collaborator_score=70.5,
        )
        assert metrics.collaborator_count == 50
        assert metrics.hub_score == 2.0
        assert metrics.avg_collaborator_score == 70.5

    def test_network_density_with_none_avg(self):
        """NetworkDensityMetrics allows None for optional avg score."""
        metrics = NetworkDensityMetrics(
            collaborator_count=50,
            unique_anime=25,
            hub_score=2.0,
            avg_collaborator_score=None,
        )
        assert metrics.avg_collaborator_score is None


class TestProductivityMetrics:
    """Test ProductivityMetrics dataclass."""

    def test_productivity_metrics_defaults(self):
        """ProductivityMetrics should have sensible defaults."""
        metrics = ProductivityMetrics()
        assert metrics.total_credits == 0
        assert metrics.credits_per_year == 0.0
        assert metrics.peak_year is None

    def test_productivity_metrics_complete(self):
        """ProductivityMetrics should accept all fields."""
        metrics = ProductivityMetrics(
            total_credits=100,
            unique_anime=50,
            active_years=10,
            career_span=12,
            credits_per_year=10.0,
            peak_year=2020,
            peak_credits=15,
            consistency_score=85.5,
        )
        assert metrics.total_credits == 100
        assert metrics.consistency_score == 85.5
        assert metrics.peak_year == 2020

    def test_productivity_metrics_without_peak_year(self):
        """ProductivityMetrics allows None for peak_year."""
        metrics = ProductivityMetrics(
            total_credits=5,
            unique_anime=3,
            active_years=1,
            career_span=1,
            credits_per_year=5.0,
            peak_year=None,
            peak_credits=5,
            consistency_score=100.0,
        )
        assert metrics.peak_year is None
        assert metrics.peak_credits == 5


class TestDataclassSerialization:
    """Test that all metrics can be serialized to dict for JSON export."""

    def test_all_metrics_serializable(self):
        """All metrics dataclasses should be convertible to dict."""
        versatility = VersatilityMetrics(category_count=2, versatility_score=80.0)
        growth = GrowthMetrics(trend="rising", total_credits=50)
        network = NetworkDensityMetrics(collaborator_count=30, hub_score=1.5)
        productivity = ProductivityMetrics(total_credits=100, credits_per_year=10.0)

        # All should convert to dict without errors
        assert asdict(versatility)["category_count"] == 2
        assert asdict(growth)["trend"] == "rising"
        assert asdict(network)["collaborator_count"] == 30
        assert asdict(productivity)["total_credits"] == 100
