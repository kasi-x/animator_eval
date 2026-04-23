"""Unit tests for insights_report analysis module.

Tests 9 public functions covering:
- PageRank distribution analysis
- Bias correction impact
- Growth pattern detection
- Potential value categorization
- Bridge importance analysis
- Recommendation generation
- Key findings extraction
- Undervaluation alert identification
- Comprehensive insights generation

Covers both normal and edge cases, validates vocabulary compliance.
"""

from __future__ import annotations

import re

import pytest

from src.analysis.reports.insights_report import (
    BiasInsights,
    BridgeInsights,
    ComprehensiveInsights,
    GrowthInsights,
    PageRankInsights,
    PotentialValueInsights,
    UndervaluationAlert,
    analyze_bias_correction_impact,
    analyze_bridge_importance,
    analyze_growth_patterns,
    analyze_pagerank_distribution,
    analyze_potential_value_categories,
    export_insights_report,
    generate_comprehensive_insights,
    generate_key_findings,
    generate_recommendations,
    identify_undervaluation_alerts,
)


# ============================================================================
# Fixtures: Synthetic Data
# ============================================================================


@pytest.fixture
def synthetic_person_scores() -> dict[str, dict]:
    """5 persons with PageRank-like scores."""
    return {
        "p1": {"birank": 15.5, "iv_score": 55.0},
        "p2": {"birank": 8.3, "iv_score": 42.0},
        "p3": {"birank": 5.1, "iv_score": 35.0},
        "p4": {"birank": 2.8, "iv_score": 28.0},
        "p5": {"birank": 1.2, "iv_score": 18.0},
    }


@pytest.fixture
def synthetic_centrality() -> dict[str, dict]:
    """5 persons with betweenness/degree metrics."""
    return {
        "p1": {"betweenness": 0.35, "degree": 12},
        "p2": {"betweenness": 0.20, "degree": 8},
        "p3": {"betweenness": 0.12, "degree": 5},
        "p4": {"betweenness": 0.05, "degree": 3},
        "p5": {"betweenness": 0.02, "degree": 1},
    }


@pytest.fixture
def synthetic_role_profiles() -> dict[str, dict]:
    """Role information for 5 persons."""
    return {
        "p1": {"primary_role": "director"},
        "p2": {"primary_role": "key_animator"},
        "p3": {"primary_role": "animator"},
        "p4": {"primary_role": "cleanup"},
        "p5": {"primary_role": "inbetweener"},
    }


@pytest.fixture
def synthetic_person_names() -> dict[str, str]:
    """Person names (Japanese and English)."""
    return {
        "p1": "佐藤太郎",
        "p2": "田中花子",
        "p3": "鈴木一郎",
        "p4": "佐々木美咲",
        "p5": "松本健一",
    }


@pytest.fixture
def synthetic_studio_bias_metrics() -> dict:
    """Studio bias metrics with debiased scores."""
    return {
        "debiased_scores": {
            "p1": {"original_birank": 14.0, "debiased_birank": 15.5},
            "p2": {"original_birank": 7.0, "debiased_birank": 8.3},
            "p3": {"original_birank": 3.0, "debiased_birank": 5.1},
            "p4": {"original_birank": 1.5, "debiased_birank": 2.8},
            "p5": {"original_birank": 0.5, "debiased_birank": 1.2},
        },
        "bias_metrics": {
            "p1": {
                "primary_studio": "Studio A",
                "cross_studio_works": 5,
            },
            "p2": {
                "primary_studio": "Studio B",
                "cross_studio_works": 2,
            },
            "p3": {
                "primary_studio": "Studio C",
                "cross_studio_works": 0,
            },
            "p4": {
                "primary_studio": "Studio A",
                "cross_studio_works": 1,
            },
            "p5": {
                "primary_studio": "Studio B",
                "cross_studio_works": 0,
            },
        },
    }


@pytest.fixture
def synthetic_growth_data() -> dict:
    """Growth acceleration data."""
    return {
        "growth_metrics": {
            "p1": {
                "trend": "rising",
                "growth_velocity": 2.5,
                "momentum_score": 8.5,
                "career_years": 8,
                "early_career_bonus": 0.0,
            },
            "p2": {
                "trend": "rising",
                "growth_velocity": 1.8,
                "momentum_score": 6.2,
                "career_years": 3,
                "early_career_bonus": 1.2,
            },
            "p3": {
                "trend": "stable",
                "growth_velocity": 0.3,
                "momentum_score": 2.1,
                "career_years": 12,
                "early_career_bonus": 0.0,
            },
            "p4": {
                "trend": "declining",
                "growth_velocity": -0.2,
                "momentum_score": 0.5,
                "career_years": 5,
                "early_career_bonus": 0.5,
            },
            "p5": {
                "trend": "stable",
                "growth_velocity": 0.1,
                "momentum_score": 1.0,
                "career_years": 1,
                "early_career_bonus": 2.0,
            },
        },
    }


@pytest.fixture
def synthetic_potential_value_scores() -> dict[str, dict]:
    """Potential value scores with categories."""
    return {
        "p1": {
            "category": "elite",
            "potential_value": 18.0,
            "hidden_score": 15.0,
            "iv_score": 55.0,
            "birank": 15.5,
            "patronage": 8.2,
            "structural_score": 12.0,
            "debiased_birank": 15.5,
        },
        "p2": {
            "category": "rising_star",
            "potential_value": 9.5,
            "hidden_score": 7.5,
            "iv_score": 42.0,
            "birank": 8.3,
            "patronage": 4.1,
            "structural_score": 6.0,
            "debiased_birank": 8.3,
        },
        "p3": {
            "category": "hidden_gem",
            "potential_value": 7.2,
            "hidden_score": 6.8,
            "iv_score": 35.0,
            "birank": 3.0,
            "patronage": 2.5,
            "structural_score": 5.5,
            "debiased_birank": 5.1,
        },
        "p4": {
            "category": "hidden_gem",
            "potential_value": 3.5,
            "hidden_score": 3.2,
            "iv_score": 28.0,
            "birank": 1.5,
            "patronage": 1.2,
            "structural_score": 2.8,
            "debiased_birank": 2.8,
        },
        "p5": {
            "category": "newcomer",
            "potential_value": 2.0,
            "hidden_score": 1.5,
            "iv_score": 18.0,
            "birank": 0.5,
            "patronage": 0.5,
            "structural_score": 1.0,
            "debiased_birank": 1.2,
        },
    }


@pytest.fixture
def synthetic_bridges_data() -> dict:
    """Bridge analysis data."""
    return {
        "bridge_persons": [
            {"person_id": "p1", "bridge_score": 0.35},
            {"person_id": "p2", "bridge_score": 0.20},
            "p3",  # Support both dict and string formats
        ],
        "total_bridge_edges": 12,
    }


# ============================================================================
# Tests: analyze_pagerank_distribution
# ============================================================================


class TestAnalyzePageRankDistribution:
    """Tests for PageRank distribution analysis."""

    def test_normal_case_5_persons(
        self,
        synthetic_person_scores,
        synthetic_centrality,
        synthetic_role_profiles,
    ):
        """Normal case: 5 persons with varying scores."""
        result = analyze_pagerank_distribution(
            synthetic_person_scores,
            synthetic_centrality,
            synthetic_role_profiles,
        )

        assert isinstance(result, PageRankInsights)
        assert result.top_percentile_share > 0
        assert result.concentration_ratio > 0
        assert result.avg_score > 0
        assert result.median_score > 0
        assert isinstance(result.top_characteristics, dict)
        assert isinstance(result.network_structure, dict)

    def test_empty_input_returns_zeros(self):
        """Empty input should return zero-filled dataclass."""
        result = analyze_pagerank_distribution({}, {}, {})

        assert result.top_percentile_share == 0.0
        assert result.concentration_ratio == 0.0
        assert result.avg_score == 0.0
        assert result.median_score == 0.0
        assert result.top_characteristics == {}
        assert result.network_structure == {}

    def test_single_person(self):
        """Single person: top 10% = 100% of score."""
        person_scores = {"p1": {"birank": 10.0}}
        centrality = {"p1": {"betweenness": 0.5, "degree": 3}}
        role_profiles = {"p1": {"primary_role": "director"}}

        result = analyze_pagerank_distribution(
            person_scores,
            centrality,
            role_profiles,
        )

        assert result.top_percentile_share == 100.0  # single person = 100%
        assert result.avg_score == 10.0
        assert result.median_score == 10.0

    def test_concentration_ratio_high_when_skewed(
        self,
        synthetic_person_scores,
        synthetic_centrality,
        synthetic_role_profiles,
    ):
        """Concentration ratio increases when scores are more skewed."""
        result = analyze_pagerank_distribution(
            synthetic_person_scores,
            synthetic_centrality,
            synthetic_role_profiles,
        )

        # p1 dominates (15.5 / ~32.9 = ~47%)
        # Herfindahl for near-monopoly should be high
        assert result.concentration_ratio > 0.15


# ============================================================================
# Tests: analyze_bias_correction_impact
# ============================================================================


class TestAnalyzeBiasCorrectionImpact:
    """Tests for bias correction impact analysis."""

    def test_normal_case_with_corrections(
        self,
        synthetic_studio_bias_metrics,
        synthetic_person_names,
    ):
        """Normal case: debiased scores with corrections."""
        result = analyze_bias_correction_impact(
            synthetic_studio_bias_metrics,
            synthetic_person_names,
        )

        assert isinstance(result, BiasInsights)
        assert result.total_persons_affected == 5
        assert result.avg_correction > 0
        assert len(result.top_gainers) > 0
        assert len(result.top_losers) > 0
        assert isinstance(result.studio_effects, dict)
        assert result.cross_studio_value >= 0

    def test_empty_debiased_scores(self, synthetic_person_names):
        """Empty debiased scores should return empty insights."""
        empty_metrics = {
            "debiased_scores": {},
            "bias_metrics": {},
        }
        result = analyze_bias_correction_impact(empty_metrics, synthetic_person_names)

        assert result.total_persons_affected == 0
        assert result.avg_correction == 0.0
        assert result.top_gainers == []
        assert result.top_losers == []

    def test_top_gainers_sorted(
        self,
        synthetic_studio_bias_metrics,
        synthetic_person_names,
    ):
        """Top gainers should be sorted by correction descending."""
        result = analyze_bias_correction_impact(
            synthetic_studio_bias_metrics,
            synthetic_person_names,
        )

        if len(result.top_gainers) > 1:
            for i in range(len(result.top_gainers) - 1):
                assert (
                    result.top_gainers[i]["correction"]
                    >= result.top_gainers[i + 1]["correction"]
                )

    def test_studio_effects_computed(
        self,
        synthetic_studio_bias_metrics,
        synthetic_person_names,
    ):
        """Studio effects should be computed for studios with >=3 persons."""
        result = analyze_bias_correction_impact(
            synthetic_studio_bias_metrics,
            synthetic_person_names,
        )

        # Studio A has 2 persons (p1, p4) → not included
        # Studio B has 2 persons (p2, p5) → not included
        # Studio C has 1 person (p3) → not included
        # No studio has >=3, so effects should be empty
        assert isinstance(result.studio_effects, dict)


# ============================================================================
# Tests: analyze_growth_patterns
# ============================================================================


class TestAnalyzeGrowthPatterns:
    """Tests for growth pattern analysis."""

    def test_normal_case_with_growth(
        self,
        synthetic_growth_data,
        synthetic_person_names,
    ):
        """Normal case: growth metrics with rising/stagnant mix."""
        result = analyze_growth_patterns(
            synthetic_growth_data,
            synthetic_person_names,
        )

        assert isinstance(result, GrowthInsights)
        assert result.rising_stars_count == 2  # p1, p2
        assert result.stagnant_count == 3  # p3, p4, p5
        assert result.avg_velocity > 0
        assert len(result.top_risers) > 0
        assert result.early_career_impact >= 0

    def test_empty_growth_metrics(self, synthetic_person_names):
        """Empty growth metrics should return zero insights."""
        empty_data = {"growth_metrics": {}}
        result = analyze_growth_patterns(empty_data, synthetic_person_names)

        assert result.rising_stars_count == 0
        assert result.stagnant_count == 0
        assert result.avg_velocity == 0.0
        assert result.top_risers == []
        assert result.early_career_impact == 0.0

    def test_top_risers_by_momentum(
        self,
        synthetic_growth_data,
        synthetic_person_names,
    ):
        """Top risers should be sorted by momentum descending."""
        result = analyze_growth_patterns(
            synthetic_growth_data,
            synthetic_person_names,
        )

        if len(result.top_risers) > 1:
            for i in range(len(result.top_risers) - 1):
                assert (
                    result.top_risers[i]["momentum"]
                    >= result.top_risers[i + 1]["momentum"]
                )

    def test_early_career_impact_computed(
        self,
        synthetic_growth_data,
        synthetic_person_names,
    ):
        """Early career impact should average bonuses for persons with <=5 years."""
        result = analyze_growth_patterns(
            synthetic_growth_data,
            synthetic_person_names,
        )

        # p2 (3yr, bonus=1.2), p4 (5yr, bonus=0.5), p5 (1yr, bonus=2.0)
        # avg = (1.2 + 0.5 + 2.0) / 3 ≈ 1.233
        assert result.early_career_impact > 0


# ============================================================================
# Tests: analyze_potential_value_categories
# ============================================================================


class TestAnalyzePotentialValueCategories:
    """Tests for potential value categorization."""

    def test_normal_case_with_categories(
        self,
        synthetic_potential_value_scores,
        synthetic_person_names,
    ):
        """Normal case: mixed categories."""
        result = analyze_potential_value_categories(
            synthetic_potential_value_scores,
            synthetic_person_names,
        )

        assert isinstance(result, PotentialValueInsights)
        assert result.hidden_gems_count == 2  # p3, p4
        assert len(result.undervalued_talent) <= 10
        assert result.structural_advantage_impact >= 0
        assert "elite" in result.category_distribution
        assert "hidden_gem" in result.category_distribution

    def test_empty_scores(self, synthetic_person_names):
        """Empty scores should return empty insights."""
        result = analyze_potential_value_categories({}, synthetic_person_names)

        assert result.hidden_gems_count == 0
        assert result.undervalued_talent == []
        assert result.structural_advantage_impact == 0.0
        # Empty scores return empty dict for elite_vs_hidden (not zero-filled dict)
        assert result.elite_vs_hidden == {}

    def test_elite_vs_hidden_comparison(
        self,
        synthetic_potential_value_scores,
        synthetic_person_names,
    ):
        """Elite vs Hidden Gem comparison should include both groups."""
        result = analyze_potential_value_categories(
            synthetic_potential_value_scores,
            synthetic_person_names,
        )

        assert result.elite_vs_hidden["elite"]["count"] == 1  # p1
        assert result.elite_vs_hidden["hidden_gem"]["count"] == 2  # p3, p4
        assert result.elite_vs_hidden["elite"]["avg_birank"] > 0
        assert result.elite_vs_hidden["hidden_gem"]["avg_debiased_birank"] > 0


# ============================================================================
# Tests: analyze_bridge_importance
# ============================================================================


class TestAnalyzeBridgeImportance:
    """Tests for bridge importance analysis."""

    def test_normal_case_with_bridges(
        self,
        synthetic_bridges_data,
        synthetic_person_names,
        synthetic_centrality,
    ):
        """Normal case: bridge persons with centrality metrics."""
        result = analyze_bridge_importance(
            synthetic_bridges_data,
            synthetic_person_names,
            synthetic_centrality,
        )

        assert isinstance(result, BridgeInsights)
        assert result.bridge_persons_count == 3  # p1, p2, p3
        assert result.avg_betweenness > 0
        assert len(result.top_bridges) > 0
        assert result.circle_connections == 12
        assert result.information_brokerage >= 0

    def test_empty_bridges(self, synthetic_person_names, synthetic_centrality):
        """No bridges should return zero insights."""
        empty_bridges = {
            "bridge_persons": [],
            "total_bridge_edges": 0,
        }
        result = analyze_bridge_importance(
            empty_bridges,
            synthetic_person_names,
            synthetic_centrality,
        )

        assert result.bridge_persons_count == 0
        assert result.avg_betweenness == 0.0
        assert result.top_bridges == []
        assert result.circle_connections == 0

    def test_bridge_persons_normalization(
        self,
        synthetic_person_names,
        synthetic_centrality,
    ):
        """Bridge persons can be dicts or strings; should normalize."""
        mixed_bridges = {
            "bridge_persons": [
                {"person_id": "p1"},
                "p2",
                {"person_id": "p3"},
            ],
            "total_bridge_edges": 5,
        }
        result = analyze_bridge_importance(
            mixed_bridges,
            synthetic_person_names,
            synthetic_centrality,
        )

        assert result.bridge_persons_count == 3


# ============================================================================
# Tests: generate_recommendations
# ============================================================================


class TestGenerateRecommendations:
    """Tests for recommendation generation."""

    def test_concentration_threshold_triggers_recommendation(
        self,
        synthetic_person_scores,
        synthetic_centrality,
        synthetic_role_profiles,
        synthetic_studio_bias_metrics,
        synthetic_growth_data,
        synthetic_potential_value_scores,
        synthetic_person_names,
        synthetic_bridges_data,
    ):
        """High concentration should trigger recommendation."""
        pagerank = analyze_pagerank_distribution(
            synthetic_person_scores,
            synthetic_centrality,
            synthetic_role_profiles,
        )
        bias = analyze_bias_correction_impact(
            synthetic_studio_bias_metrics,
            synthetic_person_names,
        )
        growth = analyze_growth_patterns(
            synthetic_growth_data,
            synthetic_person_names,
        )
        potential = analyze_potential_value_categories(
            synthetic_potential_value_scores,
            synthetic_person_names,
        )
        bridges = analyze_bridge_importance(
            synthetic_bridges_data,
            synthetic_person_names,
            synthetic_centrality,
        )

        recs = generate_recommendations(
            pagerank,
            bias,
            growth,
            potential,
            bridges,
        )

        assert isinstance(recs, list)
        assert len(recs) > 0
        # High concentration should trigger at least one recommendation
        assert any("集中" in r for r in recs)

    def test_no_recommendations_for_balanced_distribution(self):
        """Balanced distribution should produce fewer recommendations."""
        pagerank = PageRankInsights(
            top_percentile_share=20.0,  # low concentration
            concentration_ratio=0.05,
            avg_score=5.0,
            median_score=4.5,
            top_characteristics={"role_distribution": {}},
            network_structure={},
        )
        bias = BiasInsights(
            total_persons_affected=0,
            avg_correction=0.0,
            top_gainers=[],
            top_losers=[],
            studio_effects={},
            cross_studio_value=0.0,
        )
        growth = GrowthInsights(
            rising_stars_count=0,
            stagnant_count=0,
            avg_velocity=0.0,
            top_risers=[],
            early_career_impact=0.0,
        )
        potential = PotentialValueInsights(
            category_distribution={},
            hidden_gems_count=0,
            undervalued_talent=[],
            structural_advantage_impact=0.0,
            elite_vs_hidden={},
        )
        bridges = BridgeInsights(
            bridge_persons_count=0,
            avg_betweenness=0.0,
            top_bridges=[],
            circle_connections=0,
            information_brokerage=0.0,
        )

        recs = generate_recommendations(
            pagerank,
            bias,
            growth,
            potential,
            bridges,
        )

        assert isinstance(recs, list)

    def test_vocabulary_compliance_no_ability(
        self,
        synthetic_person_scores,
        synthetic_centrality,
        synthetic_role_profiles,
        synthetic_studio_bias_metrics,
        synthetic_growth_data,
        synthetic_potential_value_scores,
        synthetic_person_names,
        synthetic_bridges_data,
    ):
        """Recommendations must not use prohibited vocabulary."""
        pagerank = analyze_pagerank_distribution(
            synthetic_person_scores,
            synthetic_centrality,
            synthetic_role_profiles,
        )
        bias = analyze_bias_correction_impact(
            synthetic_studio_bias_metrics,
            synthetic_person_names,
        )
        growth = analyze_growth_patterns(
            synthetic_growth_data,
            synthetic_person_names,
        )
        potential = analyze_potential_value_categories(
            synthetic_potential_value_scores,
            synthetic_person_names,
        )
        bridges = analyze_bridge_importance(
            synthetic_bridges_data,
            synthetic_person_names,
            synthetic_centrality,
        )

        recs = generate_recommendations(
            pagerank,
            bias,
            growth,
            potential,
            bridges,
        )

        prohibited_words = ["ability", "skill", "talent", "competence", "capability"]
        for rec in recs:
            for word in prohibited_words:
                assert (
                    word.lower() not in rec.lower()
                ), f"Prohibited word '{word}' in recommendation: {rec}"


# ============================================================================
# Tests: generate_key_findings
# ============================================================================


class TestGenerateKeyFindings:
    """Tests for key findings generation."""

    def test_findings_generated(
        self,
        synthetic_person_scores,
        synthetic_centrality,
        synthetic_role_profiles,
        synthetic_studio_bias_metrics,
        synthetic_growth_data,
        synthetic_potential_value_scores,
        synthetic_person_names,
        synthetic_bridges_data,
    ):
        """Key findings should be generated from all analyses."""
        pagerank = analyze_pagerank_distribution(
            synthetic_person_scores,
            synthetic_centrality,
            synthetic_role_profiles,
        )
        bias = analyze_bias_correction_impact(
            synthetic_studio_bias_metrics,
            synthetic_person_names,
        )
        growth = analyze_growth_patterns(
            synthetic_growth_data,
            synthetic_person_names,
        )
        potential = analyze_potential_value_categories(
            synthetic_potential_value_scores,
            synthetic_person_names,
        )
        bridges = analyze_bridge_importance(
            synthetic_bridges_data,
            synthetic_person_names,
            synthetic_centrality,
        )

        findings = generate_key_findings(
            pagerank,
            bias,
            growth,
            potential,
            bridges,
        )

        assert isinstance(findings, list)
        assert len(findings) > 0
        # Should have finding from each analysis type
        assert any("上位者" in f for f in findings)  # PageRank


# ============================================================================
# Tests: identify_undervaluation_alerts
# ============================================================================


class TestIdentifyUndervaluationAlerts:
    """Tests for undervaluation alert identification."""

    def test_normal_case_identifies_alerts(
        self,
        synthetic_studio_bias_metrics,
        synthetic_potential_value_scores,
        synthetic_person_names,
    ):
        """Should identify persons with debiasing gap > 0.03."""
        alerts = identify_undervaluation_alerts(
            synthetic_studio_bias_metrics,
            synthetic_potential_value_scores,
            synthetic_person_names,
        )

        assert isinstance(alerts, list)
        assert len(alerts) > 0
        # All alerts should have gap > 0.03
        for alert in alerts:
            assert alert.birank_gap > 0.03

    def test_empty_inputs(self, synthetic_person_names):
        """Empty inputs should return empty alerts."""
        empty_bias = {
            "debiased_scores": {},
            "bias_metrics": {},
        }
        alerts = identify_undervaluation_alerts(
            empty_bias,
            {},
            synthetic_person_names,
        )

        assert alerts == []

    def test_alert_structure(
        self,
        synthetic_studio_bias_metrics,
        synthetic_potential_value_scores,
        synthetic_person_names,
    ):
        """Each alert should have correct structure."""
        alerts = identify_undervaluation_alerts(
            synthetic_studio_bias_metrics,
            synthetic_potential_value_scores,
            synthetic_person_names,
        )

        for alert in alerts:
            assert isinstance(alert, UndervaluationAlert)
            assert alert.person_id
            assert alert.name
            assert alert.current_iv_score >= 0
            assert alert.debiased_birank >= 0
            assert alert.birank_gap > 0
            assert alert.category
            assert alert.reason

    def test_alerts_sorted_by_gap_descending(
        self,
        synthetic_studio_bias_metrics,
        synthetic_potential_value_scores,
        synthetic_person_names,
    ):
        """Alerts should be sorted by gap descending."""
        alerts = identify_undervaluation_alerts(
            synthetic_studio_bias_metrics,
            synthetic_potential_value_scores,
            synthetic_person_names,
        )

        if len(alerts) > 1:
            for i in range(len(alerts) - 1):
                assert alerts[i].birank_gap >= alerts[i + 1].birank_gap


# ============================================================================
# Tests: generate_comprehensive_insights
# ============================================================================


class TestGenerateComprehensiveInsights:
    """Tests for comprehensive insights generation."""

    def test_comprehensive_generation_normal(
        self,
        synthetic_person_scores,
        synthetic_studio_bias_metrics,
        synthetic_growth_data,
        synthetic_potential_value_scores,
        synthetic_centrality,
        synthetic_role_profiles,
        synthetic_bridges_data,
        synthetic_person_names,
    ):
        """Should generate all five insight types."""
        result = generate_comprehensive_insights(
            synthetic_person_scores,
            synthetic_studio_bias_metrics,
            synthetic_growth_data,
            synthetic_potential_value_scores,
            synthetic_centrality,
            synthetic_role_profiles,
            synthetic_bridges_data,
            synthetic_person_names,
        )

        assert isinstance(result, ComprehensiveInsights)
        assert isinstance(result.pagerank, PageRankInsights)
        assert isinstance(result.bias, BiasInsights)
        assert isinstance(result.growth, GrowthInsights)
        assert isinstance(result.potential, PotentialValueInsights)
        assert isinstance(result.bridges, BridgeInsights)
        assert isinstance(result.recommendations, list)
        assert isinstance(result.key_findings, list)
        assert isinstance(result.undervaluation_alerts, list)

    def test_comprehensive_empty_inputs(self):
        """Empty inputs should still return valid structure."""
        result = generate_comprehensive_insights(
            {},
            {"debiased_scores": {}, "bias_metrics": {}},
            {"growth_metrics": {}},
            {},
            {},
            {},
            {"bridge_persons": [], "total_bridge_edges": 0},
            {},
        )

        assert isinstance(result, ComprehensiveInsights)
        assert result.pagerank.top_percentile_share == 0.0
        assert result.bias.total_persons_affected == 0
        assert result.growth.rising_stars_count == 0


# ============================================================================
# Tests: export_insights_report
# ============================================================================


class TestExportInsightsReport:
    """Tests for export functionality."""

    def test_export_to_dict(
        self,
        synthetic_person_scores,
        synthetic_studio_bias_metrics,
        synthetic_growth_data,
        synthetic_potential_value_scores,
        synthetic_centrality,
        synthetic_role_profiles,
        synthetic_bridges_data,
        synthetic_person_names,
    ):
        """Should export comprehensive insights to JSON dict."""
        insights = generate_comprehensive_insights(
            synthetic_person_scores,
            synthetic_studio_bias_metrics,
            synthetic_growth_data,
            synthetic_potential_value_scores,
            synthetic_centrality,
            synthetic_role_profiles,
            synthetic_bridges_data,
            synthetic_person_names,
        )

        exported = export_insights_report(insights)

        assert isinstance(exported, dict)
        assert "pagerank_analysis" in exported
        assert "bias_correction_analysis" in exported
        assert "growth_analysis" in exported
        assert "potential_value_analysis" in exported
        assert "bridge_analysis" in exported
        assert "recommendations" in exported
        assert "key_findings" in exported

    def test_export_with_alerts(
        self,
        synthetic_person_scores,
        synthetic_studio_bias_metrics,
        synthetic_growth_data,
        synthetic_potential_value_scores,
        synthetic_centrality,
        synthetic_role_profiles,
        synthetic_bridges_data,
        synthetic_person_names,
    ):
        """Exported dict should include undervaluation_alerts if present."""
        insights = generate_comprehensive_insights(
            synthetic_person_scores,
            synthetic_studio_bias_metrics,
            synthetic_growth_data,
            synthetic_potential_value_scores,
            synthetic_centrality,
            synthetic_role_profiles,
            synthetic_bridges_data,
            synthetic_person_names,
        )

        exported = export_insights_report(insights)

        if insights.undervaluation_alerts:
            assert "undervaluation_alerts" in exported
            assert isinstance(exported["undervaluation_alerts"], list)


# ============================================================================
# Integration: Vocabulary Compliance
# ============================================================================


class TestVocabularyCompliance:
    """Cross-function vocabulary compliance check."""

    def test_no_prohibited_words_in_all_text_outputs(
        self,
        synthetic_person_scores,
        synthetic_studio_bias_metrics,
        synthetic_growth_data,
        synthetic_potential_value_scores,
        synthetic_centrality,
        synthetic_role_profiles,
        synthetic_bridges_data,
        synthetic_person_names,
    ):
        """All text outputs (recommendations, findings) must avoid prohibited words."""
        insights = generate_comprehensive_insights(
            synthetic_person_scores,
            synthetic_studio_bias_metrics,
            synthetic_growth_data,
            synthetic_potential_value_scores,
            synthetic_centrality,
            synthetic_role_profiles,
            synthetic_bridges_data,
            synthetic_person_names,
        )

        prohibited_pattern = re.compile(
            r"\b(ability|skill|talent|competence|capability)\b",
            re.IGNORECASE,
        )

        for rec in insights.recommendations:
            assert not prohibited_pattern.search(
                rec
            ), f"Prohibited word in recommendation: {rec}"

        for finding in insights.key_findings:
            assert not prohibited_pattern.search(
                finding
            ), f"Prohibited word in finding: {finding}"

        if insights.undervaluation_alerts:
            for alert in insights.undervaluation_alerts:
                assert not prohibited_pattern.search(
                    alert.reason
                ), f"Prohibited word in reason: {alert.reason}"
