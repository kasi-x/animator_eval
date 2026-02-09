"""拡張エッジケーステスト — 境界値・異常値の網羅的テスト."""

from src.analysis.collaboration_strength import compute_collaboration_strength
from src.analysis.comparison_matrix import build_comparison_matrix
from src.analysis.decade_analysis import compute_decade_analysis
from src.analysis.ego_graph import extract_ego_graph
from src.analysis.growth import compute_growth_trends
from src.analysis.network_density import compute_network_density
from src.analysis.outliers import detect_outliers
from src.analysis.person_tags import compute_person_tags
from src.analysis.recommendation import recommend_for_team
from src.analysis.role_flow import compute_role_flow
from src.analysis.team_composition import analyze_team_patterns
from src.analysis.time_series import compute_time_series
from src.analysis.versatility import compute_versatility
from src.models import Anime, Credit, Role


class TestSinglePersonDataset:
    """1人だけのデータセットでの挙動."""

    def _make_single(self):
        anime_map = {"a1": Anime(id="a1", title_en="Solo", year=2022, score=7.0)}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR)]
        return credits, anime_map

    def test_network_density_single(self):
        credits, _ = self._make_single()
        result = compute_network_density(credits)
        assert result["p1"]["collaborator_count"] == 0
        assert result["p1"]["hub_score"] == 0

    def test_collaboration_strength_single(self):
        credits, anime_map = self._make_single()
        result = compute_collaboration_strength(credits, anime_map)
        assert result == []

    def test_growth_single(self):
        credits, anime_map = self._make_single()
        result = compute_growth_trends(credits, anime_map)
        assert result["p1"]["total_credits"] == 1

    def test_time_series_single(self):
        credits, anime_map = self._make_single()
        result = compute_time_series(credits, anime_map)
        assert len(result["years"]) == 1

    def test_decade_single(self):
        credits, anime_map = self._make_single()
        result = compute_decade_analysis(credits, anime_map)
        assert "2020s" in result["decades"]

    def test_role_flow_single(self):
        credits, anime_map = self._make_single()
        result = compute_role_flow(credits, anime_map)
        assert result["total_transitions"] == 0

    def test_ego_graph_single(self):
        credits, anime_map = self._make_single()
        result = extract_ego_graph("p1", credits, anime_map)
        assert result["total_nodes"] == 1

    def test_versatility_single(self):
        credits, _ = self._make_single()
        result = compute_versatility(credits)
        assert result["p1"]["role_count"] == 1


class TestAllSameScores:
    """全員同じスコアの場合."""

    def test_outliers_all_same(self):
        results = [
            {"person_id": f"p{i}", "authority": 50, "trust": 50, "skill": 50, "composite": 50}
            for i in range(20)
        ]
        out = detect_outliers(results)
        # IQR = 0, so no outliers by IQR method
        # All z-scores = 0
        assert out["total_outliers"] == 0

    def test_comparison_all_same(self):
        results = [
            {"person_id": f"p{i}", "name": f"P{i}", "authority": 50, "trust": 50, "skill": 50, "composite": 50}
            for i in range(3)
        ]
        cm = build_comparison_matrix(["p0", "p1"], results)
        dom = cm["pairwise_dominance"]["p0"]["p1"]
        assert dom["ties"] == 4

    def test_tags_all_same(self):
        results = [
            {
                "person_id": f"p{i}",
                "authority": 50, "trust": 50, "skill": 50, "composite": 50,
                "career": {"active_years": 5, "highest_stage": 3},
            }
            for i in range(10)
        ]
        tags = compute_person_tags(results)
        # No one should be "high" since all are at same level
        for pid_tags in tags.values():
            assert "top_talent" not in pid_tags


class TestNoYearData:
    """年情報がない場合のテスト."""

    def test_growth_no_years(self):
        anime_map = {"a1": Anime(id="a1", title_en="No Year")}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR)]
        result = compute_growth_trends(credits, anime_map)
        assert result == {}

    def test_time_series_no_years(self):
        anime_map = {"a1": Anime(id="a1", title_en="No Year")}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR)]
        result = compute_time_series(credits, anime_map)
        assert result["years"] == []

    def test_decade_no_years(self):
        anime_map = {"a1": Anime(id="a1", title_en="No Year")}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR)]
        result = compute_decade_analysis(credits, anime_map)
        assert result["decades"] == {}


class TestLargeTeam:
    """大人数チームのテスト."""

    def test_team_20_persons(self):
        anime_map = {"a1": Anime(id="a1", title_en="Big Show", year=2022, score=8.0)}
        credits = [
            Credit(person_id=f"p{i}", anime_id="a1", role=Role.KEY_ANIMATOR)
            for i in range(20)
        ]
        result = analyze_team_patterns(credits, anime_map, min_score=7.0)
        team = result["high_score_teams"][0]
        assert team["team_size"] == 20

    def test_ego_graph_dense(self):
        anime_map = {"a1": Anime(id="a1", title_en="Dense", year=2022)}
        credits = [
            Credit(person_id=f"p{i}", anime_id="a1", role=Role.KEY_ANIMATOR)
            for i in range(15)
        ]
        result = extract_ego_graph("p0", credits, anime_map, hops=1)
        assert result["total_nodes"] == 15
        # All 15 are connected to each other through a1

    def test_network_density_dense(self):
        credits = [
            Credit(person_id=f"p{i}", anime_id="a1", role=Role.KEY_ANIMATOR)
            for i in range(10)
        ]
        result = compute_network_density(credits)
        # Everyone has 9 collaborators
        for pid in result:
            assert result[pid]["collaborator_count"] == 9


class TestRecommendationEdgeCases:
    def test_recommend_all_team_members(self):
        """When all persons are in the team, no recommendations."""
        results = [
            {"person_id": "p1", "authority": 50, "trust": 50, "skill": 50, "composite": 50},
            {"person_id": "p2", "authority": 50, "trust": 50, "skill": 50, "composite": 50},
        ]
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
            Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        recs = recommend_for_team(["p1", "p2"], results, credits)
        assert recs == []

    def test_recommend_nonexistent_team(self):
        results = [{"person_id": "p1", "authority": 50, "trust": 50, "skill": 50, "composite": 50}]
        recs = recommend_for_team(["nonexistent"], results, [])
        assert recs == []
