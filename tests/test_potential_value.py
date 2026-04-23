"""potential_value.py coverage tests."""
from src.analysis.scoring.potential_value import (
    PotentialValueScore,
    ValueCategory,
    compute_potential_value_scores,
    compute_structural_advantage,
    export_potential_value_report,
    rank_by_potential_value,
)

import networkx as nx
import pytest

from src.runtime.models import BronzeAnime as Anime, Credit, Role


def _anime(
    aid: str,
    *,
    year: int = 2020,
    score: float | None = 75.0,
    studio: str | None = None,
    studios: list[str] | None = None,
    tags: list[dict] | None = None,
    genres: list[str] | None = None,
) -> Anime:
    resolved_studios = studios or ([studio] if studio else [])
    return Anime(
        id=aid,
        title_ja=f"Anime_{aid}",
        title_en=f"Anime_{aid}",
        year=year,
        score=score,
        studios=resolved_studios,
        tags=tags or [],
        genres=genres or [],
    )


def _credit(pid: str, aid: str, role: Role = Role.KEY_ANIMATOR) -> Credit:
    return Credit(person_id=pid, anime_id=aid, role=role, source="test")

class TestValueCategory:
    def test_enum_values(self):
        assert ValueCategory.ELITE.value == "elite"
        assert ValueCategory.RISING_STAR.value == "rising_star"
        assert ValueCategory.HIDDEN_GEM.value == "hidden_gem"
        assert ValueCategory.STRUCTURAL_PLAYER.value == "structural_player"
        assert ValueCategory.STEADY_PERFORMER.value == "steady_performer"
        assert ValueCategory.NEWCOMER.value == "newcomer"

    def test_all_categories_present(self):
        assert len(ValueCategory) == 6


class TestPotentialValueScore:
    def test_defaults(self):
        pv = PotentialValueScore(person_id="p1")
        assert pv.birank == 0.0
        assert pv.patronage == 0.0
        assert pv.person_fe == 0.0
        assert pv.potential_value == 0.0
        assert pv.category == ValueCategory.STEADY_PERFORMER

    def test_custom_values(self):
        pv = PotentialValueScore(
            person_id="p1",
            birank=0.8,
            patronage=0.7,
            potential_value=85.0,
            category=ValueCategory.ELITE,
        )
        assert pv.birank == 0.8
        assert pv.category == ValueCategory.ELITE


class TestComputeStructuralAdvantage:
    def test_node_not_in_graph(self):
        G = nx.Graph()
        G.add_node("p1")
        result = compute_structural_advantage(G, "p999")
        assert result == 0.0

    def test_node_with_no_neighbors(self):
        G = nx.Graph()
        G.add_node("p1")
        result = compute_structural_advantage(G, "p1")
        assert result == 0

    def test_node_with_neighbors_no_cache(self):
        G = nx.Graph()
        G.add_edge("p1", "p2", weight=1.0)
        G.add_edge("p1", "p3", weight=1.0)
        result = compute_structural_advantage(G, "p1")
        # No betweenness cache: betweenness=0, diversity=1.0 (all unique)
        # advantage = 0*0.6 + 1.0*0.4 = 0.4
        assert result == 0.4

    def test_node_with_betweenness_cache(self):
        G = nx.Graph()
        G.add_edge("p1", "p2", weight=1.0)
        G.add_edge("p1", "p3", weight=1.0)
        cache = {"p1": 0.5, "p2": 0.1, "p3": 0.1}
        result = compute_structural_advantage(G, "p1", betweenness_cache=cache)
        # 0.5*0.6 + 1.0*0.4 = 0.7
        assert result == 0.7


class TestComputePotentialValueScores:
    @pytest.fixture()
    def basic_setup(self):
        person_scores = {
            "p1": {"birank": 0.9, "patronage": 0.9, "person_fe": 0.8, "iv_score": 0.87},
            "p2": {"birank": 0.3, "patronage": 0.2, "person_fe": 0.7, "iv_score": 0.35},
            "p3": {"birank": 0.5, "patronage": 0.4, "person_fe": 0.5, "iv_score": 0.47},
        }
        debiased = {
            "p1": {"debiased_birank": 0.85},
            "p2": {"debiased_birank": 0.5},  # big improvement
            "p3": {"debiased_birank": 0.5},
        }
        growth = {
            "p1": {"growth_velocity": 0.5, "momentum_score": 0.3, "career_years": 15},
            "p2": {"growth_velocity": 3.0, "momentum_score": 2.0, "career_years": 2},
            "p3": {"growth_velocity": 0.1, "momentum_score": 0.0, "career_years": 8},
        }
        adjusted_skills = {"p1": 0.82, "p2": 0.9, "p3": 0.5}
        G = nx.Graph()
        G.add_edge("p1", "p2", weight=1.0)
        G.add_edge("p1", "p3", weight=1.0)
        G.add_edge("p2", "p3", weight=1.0)
        return person_scores, debiased, growth, adjusted_skills, G

    def test_returns_all_persons(self, basic_setup):
        ps, db, gr, adj, G = basic_setup
        result = compute_potential_value_scores(ps, db, gr, adj, G)
        assert set(result.keys()) == {"p1", "p2", "p3"}

    def test_result_type(self, basic_setup):
        ps, db, gr, adj, G = basic_setup
        result = compute_potential_value_scores(ps, db, gr, adj, G)
        for v in result.values():
            assert isinstance(v, PotentialValueScore)

    def test_elite_category_assignment(self, basic_setup):
        ps, db, gr, adj, G = basic_setup
        result = compute_potential_value_scores(ps, db, gr, adj, G)
        # p1: authority 0.9 > 0.8, trust 0.9 > 0.8 => ELITE
        assert result["p1"].category == ValueCategory.ELITE

    def test_newcomer_category(self, basic_setup):
        ps, db, gr, adj, G = basic_setup
        result = compute_potential_value_scores(ps, db, gr, adj, G)
        # p2: career_years 2 <= 3, momentum 2.0 > 1.0 => NEWCOMER
        assert result["p2"].category == ValueCategory.NEWCOMER

    def test_steady_performer_default(self, basic_setup):
        ps, db, gr, adj, G = basic_setup
        result = compute_potential_value_scores(ps, db, gr, adj, G)
        # p3 doesn't match any special category
        assert result["p3"].category == ValueCategory.STEADY_PERFORMER

    def test_potential_value_non_negative(self, basic_setup):
        ps, db, gr, adj, G = basic_setup
        result = compute_potential_value_scores(ps, db, gr, adj, G)
        for v in result.values():
            assert v.potential_value >= 0 or v.growth_score < 0

    def test_empty_inputs(self):
        result = compute_potential_value_scores({}, {}, {}, {}, nx.Graph())
        assert result == {}

    def test_missing_debiased_defaults_to_original(self):
        ps = {
            "p1": {"birank": 0.5, "patronage": 0.3, "person_fe": 0.4, "iv_score": 0.4}
        }
        result = compute_potential_value_scores(ps, {}, {}, {}, nx.Graph())
        assert result["p1"].debiased_birank == 0.5

    def test_betweenness_cache_used(self, basic_setup):
        ps, db, gr, adj, G = basic_setup
        cache = {"p1": 0.9, "p2": 0.1, "p3": 0.05}
        result = compute_potential_value_scores(
            ps, db, gr, adj, G, betweenness_cache=cache
        )
        # p1 should have higher structural advantage with cache
        assert result["p1"].structural_advantage > 0

    def test_hidden_gem_category(self):
        """Person with authority_improvement > 0.1 and skill > 0.6 => HIDDEN_GEM."""
        ps = {
            "p1": {"birank": 0.3, "patronage": 0.3, "person_fe": 0.7, "iv_score": 0.4}
        }
        debiased = {"p1": {"debiased_birank": 0.5}}  # improvement = 0.2 > 0.1
        growth = {
            "p1": {"growth_velocity": 0.0, "momentum_score": 0.0, "career_years": 10}
        }
        result = compute_potential_value_scores(
            ps, debiased, growth, {"p1": 0.7}, nx.Graph()
        )
        assert result["p1"].category == ValueCategory.HIDDEN_GEM

    def test_rising_star_category(self):
        """Person with velocity > 2.0 and authority > 0.5 => RISING_STAR."""
        ps = {
            "p1": {"birank": 0.6, "patronage": 0.5, "person_fe": 0.6, "iv_score": 0.57}
        }
        debiased = {"p1": {"debiased_birank": 0.6}}
        growth = {
            "p1": {"growth_velocity": 3.0, "momentum_score": 0.5, "career_years": 8}
        }
        result = compute_potential_value_scores(
            ps, debiased, growth, {"p1": 0.6}, nx.Graph()
        )
        assert result["p1"].category == ValueCategory.RISING_STAR


class TestRankByPotentialValue:
    def test_basic_ranking(self):
        scores = {
            "p1": PotentialValueScore(person_id="p1", potential_value=90),
            "p2": PotentialValueScore(person_id="p2", potential_value=50),
            "p3": PotentialValueScore(person_id="p3", potential_value=70),
        }
        ranked = rank_by_potential_value(scores)
        assert ranked[0][0] == "p1"
        assert ranked[1][0] == "p3"
        assert ranked[2][0] == "p2"

    def test_filter_by_category(self):
        scores = {
            "p1": PotentialValueScore(
                person_id="p1", potential_value=90, category=ValueCategory.ELITE
            ),
            "p2": PotentialValueScore(
                person_id="p2", potential_value=50, category=ValueCategory.NEWCOMER
            ),
        }
        ranked = rank_by_potential_value(scores, category=ValueCategory.ELITE)
        assert len(ranked) == 1
        assert ranked[0][0] == "p1"

    def test_top_n(self):
        scores = {
            f"p{i}": PotentialValueScore(person_id=f"p{i}", potential_value=float(i))
            for i in range(10)
        }
        ranked = rank_by_potential_value(scores, top_n=3)
        assert len(ranked) == 3

    def test_empty_input(self):
        assert rank_by_potential_value({}) == []


class TestExportPotentialValueReport:
    def test_report_structure(self):
        scores = {
            "p1": PotentialValueScore(
                person_id="p1",
                potential_value=80,
                category=ValueCategory.ELITE,
                birank=0.9,
                patronage=0.8,
                person_fe=0.7,
            ),
        }
        names = {"p1": "Alice"}
        report = export_potential_value_report(scores, names)
        assert "total_persons" in report
        assert "category_distribution" in report
        assert "overall_ranking" in report
        assert "category_rankings" in report
        assert "detailed_scores" in report
        assert report["total_persons"] == 1

    def test_category_distribution(self):
        scores = {
            "p1": PotentialValueScore(
                person_id="p1", potential_value=80, category=ValueCategory.ELITE
            ),
            "p2": PotentialValueScore(
                person_id="p2", potential_value=50, category=ValueCategory.NEWCOMER
            ),
        }
        report = export_potential_value_report(scores, {})
        assert report["category_distribution"]["elite"] == 1
        assert report["category_distribution"]["newcomer"] == 1

    def test_person_name_fallback(self):
        scores = {
            "p1": PotentialValueScore(person_id="p1", potential_value=80),
        }
        report = export_potential_value_report(scores, {})
        # Name should fall back to person_id
        assert report["overall_ranking"][0]["name"] == "p1"


# ============================================================
# 2. contribution_attribution.py
# ============================================================
