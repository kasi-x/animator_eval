"""visualize モジュールのテスト."""

import networkx as nx
import pytest

from src.analysis.visualize import (
    plot_anime_stats,
    plot_bridge_analysis,
    plot_collaboration_network,
    plot_collaboration_strength,
    plot_crossval_stability,
    plot_decade_comparison,
    plot_genre_affinity,
    plot_growth_trends,
    plot_influence_tree,
    plot_milestone_summary,
    plot_network_evolution,
    plot_outlier_summary,
    plot_person_timeline,
    plot_productivity_distribution,
    plot_role_flow_sankey,
    plot_score_distribution,
    plot_seasonal_trends,
    plot_studio_comparison,
    plot_tag_summary,
    plot_time_series,
    plot_top_persons_radar,
    plot_transition_heatmap,
)


@pytest.fixture
def sample_scores():
    return {
        "p1": {"authority": 80.0, "trust": 60.0, "skill": 70.0, "composite": 71.5},
        "p2": {"authority": 50.0, "trust": 90.0, "skill": 40.0, "composite": 61.5},
        "p3": {"authority": 30.0, "trust": 20.0, "skill": 95.0, "composite": 42.75},
    }


@pytest.fixture
def sample_results(sample_scores):
    return [
        {"person_id": k, "name": f"Person {k}", **v}
        for k, v in sample_scores.items()
    ]


class TestPlotScoreDistribution:
    def test_creates_file(self, tmp_path, sample_scores):
        out = tmp_path / "dist.png"
        plot_score_distribution(sample_scores, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_scores(self, tmp_path):
        out = tmp_path / "dist.png"
        plot_score_distribution({}, output_path=out)
        assert not out.exists()


class TestPlotTopPersonsRadar:
    def test_creates_file(self, tmp_path, sample_results):
        out = tmp_path / "radar.png"
        plot_top_persons_radar(sample_results, top_n=3, output_path=out)
        assert out.exists()

    def test_empty(self, tmp_path):
        out = tmp_path / "radar.png"
        plot_top_persons_radar([], output_path=out)
        assert not out.exists()


class TestPlotCollaborationNetwork:
    def test_creates_file(self, tmp_path):
        g = nx.Graph()
        g.add_node("p1", name="Alice")
        g.add_node("p2", name="Bob")
        g.add_edge("p1", "p2", weight=3.0, shared_works=2)
        scores = {"p1": 80.0, "p2": 50.0}
        out = tmp_path / "net.png"
        plot_collaboration_network(g, scores, top_n=10, output_path=out)
        assert out.exists()

    def test_empty_graph(self, tmp_path):
        g = nx.Graph()
        out = tmp_path / "net.png"
        plot_collaboration_network(g, output_path=out)
        assert not out.exists()


class TestPlotPersonTimeline:
    def test_creates_file(self, tmp_path):
        credits_by_year = {
            2020: [
                {"anime_title": "Show A", "role": "in_between", "score": 7.0},
                {"anime_title": "Show B", "role": "in_between", "score": 6.5},
            ],
            2021: [
                {"anime_title": "Show C", "role": "key_animator", "score": 8.0},
            ],
            2023: [
                {"anime_title": "Show D", "role": "animation_director", "score": 8.5},
                {"anime_title": "Show E", "role": "director", "score": 9.0},
            ],
        }
        career_stages = {2020: 1, 2021: 3, 2023: 6}
        out = tmp_path / "timeline.png"
        plot_person_timeline(
            "p1", credits_by_year, career_stages, "Test Person", output_path=out
        )
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_credits(self, tmp_path):
        out = tmp_path / "timeline.png"
        plot_person_timeline("p1", {}, output_path=out)
        assert not out.exists()

    def test_no_career_stages(self, tmp_path):
        credits_by_year = {
            2022: [{"anime_title": "Show A", "role": "other", "score": 7.0}],
        }
        out = tmp_path / "timeline.png"
        plot_person_timeline("p1", credits_by_year, person_name="Test", output_path=out)
        assert out.exists()


class TestPlotGrowthTrends:
    def test_creates_file(self, tmp_path):
        data = {"trend_summary": {"rising": 10, "stable": 20, "declining": 5, "inactive": 3}}
        out = tmp_path / "growth.png"
        plot_growth_trends(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "growth.png"
        plot_growth_trends({}, output_path=out)
        assert not out.exists()


class TestPlotNetworkEvolution:
    def test_creates_file(self, tmp_path):
        data = {
            "years": [2018, 2019, 2020],
            "snapshots": {
                "2018": {"cumulative_persons": 10, "cumulative_edges": 5, "new_persons": 10, "density": 0.1},
                "2019": {"cumulative_persons": 20, "cumulative_edges": 15, "new_persons": 10, "density": 0.08},
                "2020": {"cumulative_persons": 30, "cumulative_edges": 30, "new_persons": 10, "density": 0.07},
            },
        }
        out = tmp_path / "net_evo.png"
        plot_network_evolution(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "net_evo.png"
        plot_network_evolution({"years": [], "snapshots": {}}, output_path=out)
        assert not out.exists()


class TestPlotDecadeComparison:
    def test_creates_file(self, tmp_path):
        data = {
            "decades": {
                "2000s": {"credit_count": 100, "unique_persons": 30, "unique_anime": 20, "avg_anime_score": 7.2},
                "2010s": {"credit_count": 300, "unique_persons": 80, "unique_anime": 50, "avg_anime_score": 7.5},
                "2020s": {"credit_count": 200, "unique_persons": 60, "unique_anime": 40, "avg_anime_score": 7.8},
            },
        }
        out = tmp_path / "decades.png"
        plot_decade_comparison(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "decades.png"
        plot_decade_comparison({"decades": {}}, output_path=out)
        assert not out.exists()


class TestPlotRoleFlowSankey:
    def test_creates_file(self, tmp_path):
        data = {
            "links": [
                {"source": "Stage 1", "target": "Stage 3", "value": 10},
                {"source": "Stage 3", "target": "Stage 4", "value": 5},
                {"source": "Stage 1", "target": "Stage 2", "value": 8},
            ],
        }
        out = tmp_path / "flow.png"
        plot_role_flow_sankey(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "flow.png"
        plot_role_flow_sankey({"links": []}, output_path=out)
        assert not out.exists()


class TestPlotTimeSeries:
    def test_creates_file(self, tmp_path):
        data = {
            "years": [2018, 2019, 2020],
            "series": {
                "credit_count": {2018: 50, 2019: 80, 2020: 100},
                "active_persons": {2018: 10, 2019: 20, 2020: 25},
                "unique_anime": {2018: 5, 2019: 8, 2020: 12},
            },
        }
        out = tmp_path / "ts.png"
        plot_time_series(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "ts.png"
        plot_time_series({"years": [], "series": {}}, output_path=out)
        assert not out.exists()


class TestPlotProductivityDistribution:
    def test_creates_file(self, tmp_path):
        data = {
            "p1": {"credits_per_year": 5.0, "consistency_score": 0.8},
            "p2": {"credits_per_year": 3.2, "consistency_score": 0.5},
            "p3": {"credits_per_year": 8.1, "consistency_score": 0.95},
        }
        out = tmp_path / "prod.png"
        plot_productivity_distribution(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "prod.png"
        plot_productivity_distribution({}, output_path=out)
        assert not out.exists()


class TestPlotInfluenceTree:
    def test_creates_file(self, tmp_path):
        data = {
            "mentors": {
                "d1": {"name": "Director A", "mentees": ["a1", "a2", "a3"]},
                "d2": {"name": "Director B", "mentees": ["a4"]},
            },
            "total_mentors": 2,
            "total_mentees": 4,
        }
        out = tmp_path / "influence.png"
        plot_influence_tree(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "influence.png"
        plot_influence_tree({"mentors": {}}, output_path=out)
        assert not out.exists()


class TestPlotMilestoneSummary:
    def test_creates_file(self, tmp_path):
        data = {
            "p1": [
                {"type": "career_start", "year": 2018, "anime_title": "Show A"},
                {"type": "promotion", "year": 2020, "from_stage": 1, "to_stage": 3},
            ],
            "p2": [
                {"type": "career_start", "year": 2015, "anime_title": "Show B"},
                {"type": "first_director", "year": 2022, "anime_title": "Show C"},
            ],
        }
        out = tmp_path / "ms.png"
        plot_milestone_summary(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "ms.png"
        plot_milestone_summary({}, output_path=out)
        assert not out.exists()


class TestPlotSeasonalTrends:
    def test_creates_file(self, tmp_path):
        data = {
            "by_season": {
                "winter": {"credit_count": 100, "person_count": 30, "avg_anime_score": 7.2},
                "spring": {"credit_count": 150, "person_count": 45, "avg_anime_score": 7.5},
                "summer": {"credit_count": 80, "person_count": 20, "avg_anime_score": 7.0},
                "fall": {"credit_count": 120, "person_count": 35, "avg_anime_score": 7.8},
            },
        }
        out = tmp_path / "seasonal.png"
        plot_seasonal_trends(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "seasonal.png"
        plot_seasonal_trends({"by_season": {}}, output_path=out)
        assert not out.exists()


class TestPlotBridgeAnalysis:
    def test_creates_file(self, tmp_path):
        data = {
            "bridge_persons": [
                {"person_id": "p1", "bridge_score": 80, "communities_connected": 3, "cross_community_edges": 5},
                {"person_id": "p2", "bridge_score": 45, "communities_connected": 2, "cross_community_edges": 3},
                {"person_id": "p3", "bridge_score": 25, "communities_connected": 2, "cross_community_edges": 1},
            ],
        }
        out = tmp_path / "bridges.png"
        plot_bridge_analysis(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "bridges.png"
        plot_bridge_analysis({"bridge_persons": []}, output_path=out)
        assert not out.exists()


class TestPlotCollaborationStrength:
    def test_creates_file(self, tmp_path):
        data = [
            {"person_a": "p1", "person_b": "p2", "strength_score": 85, "shared_works": 5, "longevity": 4},
            {"person_a": "p1", "person_b": "p3", "strength_score": 40, "shared_works": 2, "longevity": 1},
            {"person_a": "p2", "person_b": "p3", "strength_score": 60, "shared_works": 3, "longevity": 2},
        ]
        out = tmp_path / "collab.png"
        plot_collaboration_strength(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "collab.png"
        plot_collaboration_strength([], output_path=out)
        assert not out.exists()


class TestPlotTagSummary:
    def test_creates_file(self, tmp_path):
        data = {
            "tag_summary": {
                "veteran": 15,
                "rising_star": 10,
                "high_authority": 8,
                "top_talent": 5,
            },
        }
        out = tmp_path / "tags.png"
        plot_tag_summary(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "tags.png"
        plot_tag_summary({"tag_summary": {}}, output_path=out)
        assert not out.exists()


class TestPlotStudioComparison:
    def test_creates_file(self, tmp_path):
        data = {
            "Studio A": {"person_count": 50, "avg_person_score": 65.3},
            "Studio B": {"person_count": 30, "avg_person_score": 72.1},
            "Studio C": {"person_count": 80, "avg_person_score": 58.0},
        }
        out = tmp_path / "studios.png"
        plot_studio_comparison(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "studios.png"
        plot_studio_comparison({}, output_path=out)
        assert not out.exists()


class TestPlotOutlierSummary:
    def test_creates_file(self, tmp_path):
        data = {
            "axis_outliers": {
                "authority": {"high": [{"person_id": "p1"}], "low": []},
                "trust": {"high": [], "low": [{"person_id": "p2"}]},
                "composite": {"high": [{"person_id": "p1"}], "low": [{"person_id": "p3"}]},
            },
            "total_outliers": 3,
        }
        out = tmp_path / "outliers.png"
        plot_outlier_summary(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "outliers.png"
        plot_outlier_summary({"axis_outliers": {}}, output_path=out)
        assert not out.exists()


class TestPlotTransitionHeatmap:
    def test_creates_file(self, tmp_path):
        data = {
            "transitions": [
                {"from_stage": 1, "to_stage": 3, "count": 20},
                {"from_stage": 3, "to_stage": 4, "count": 10},
                {"from_stage": 1, "to_stage": 2, "count": 15},
                {"from_stage": 4, "to_stage": 6, "count": 3},
            ],
        }
        out = tmp_path / "trans.png"
        plot_transition_heatmap(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "trans.png"
        plot_transition_heatmap({"transitions": []}, output_path=out)
        assert not out.exists()


class TestPlotAnimeStats:
    def test_creates_file(self, tmp_path):
        data = {
            "a1": {"title": "Show A", "score": 8.0, "unique_persons": 20, "avg_person_score": 65.0},
            "a2": {"title": "Show B", "score": 7.5, "unique_persons": 15, "avg_person_score": 55.0},
            "a3": {"title": "Show C", "score": 6.0, "unique_persons": 10, "avg_person_score": 45.0},
        }
        out = tmp_path / "anime.png"
        plot_anime_stats(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "anime.png"
        plot_anime_stats({}, output_path=out)
        assert not out.exists()


class TestPlotGenreAffinity:
    def test_creates_file(self, tmp_path):
        data = {
            "p1": {"primary_tier": "high", "primary_era": "modern", "total_credits": 10},
            "p2": {"primary_tier": "mid", "primary_era": "2010s", "total_credits": 8},
            "p3": {"primary_tier": "low", "primary_era": "2000s", "total_credits": 5},
        }
        out = tmp_path / "genre.png"
        plot_genre_affinity(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "genre.png"
        plot_genre_affinity({}, output_path=out)
        assert not out.exists()


class TestPlotCrossvalStability:
    def test_creates_file(self, tmp_path):
        data = {
            "fold_results": [
                {"correlation": 0.85, "top10_overlap": 0.70},
                {"correlation": 0.90, "top10_overlap": 0.80},
                {"correlation": 0.82, "top10_overlap": 0.60},
            ],
            "avg_rank_correlation": 0.857,
            "avg_top10_overlap": 0.70,
        }
        out = tmp_path / "cv.png"
        plot_crossval_stability(data, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        out = tmp_path / "cv.png"
        plot_crossval_stability({"fold_results": []}, output_path=out)
        assert not out.exists()
