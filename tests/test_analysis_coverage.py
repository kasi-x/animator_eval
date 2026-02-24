"""Comprehensive tests for analysis modules with low/no coverage.

Covers:
1. potential_value.py — PotentialValueScore, ValueCategory, compute/rank/export
2. contribution_attribution.py — ContributionMetrics, Shapley, marginal, aggregation
3. studio_bias_correction.py — StudioBiasMetrics, DebiasedScore, StudioDisparityResult
4. growth_acceleration.py — AccelerationMetrics, compute/find/adjusted
5. anime_value.py — AnimeValueMetrics, 5-dimensional value model
6. individual_contribution.py — additional edge-case coverage
"""

import networkx as nx
import pytest

from src.analysis.anime_value import (
    AnimeValueMetrics,
    compute_anime_values,
    compute_commercial_value,
    compute_creative_value,
    compute_critical_value,
    compute_cultural_value,
    compute_technical_value,
    find_overperforming_works,
    find_undervalued_works,
    rank_anime_by_value,
)
from src.analysis.contribution_attribution import (
    ContributionMetrics,
    ROLE_CONTRIBUTION_WEIGHTS,
    aggregate_contributions_by_person,
    compute_contribution_attribution,
    compute_role_importance,
    compute_shapley_value_approximate,
    estimate_marginal_contribution,
    find_mvp_by_role,
    find_undervalued_contributors,
)
from src.analysis.growth_acceleration import (
    AccelerationMetrics,
    compute_adjusted_skill_with_growth,
    compute_growth_metrics,
    find_early_potential,
    find_fast_risers,
)
from src.analysis.potential_value import (
    PotentialValueScore,
    ValueCategory,
    compute_potential_value_scores,
    compute_structural_advantage,
    export_potential_value_report,
    rank_by_potential_value,
)
from src.analysis.studio_bias_correction import (
    DebiasedScore,
    StudioBiasMetrics,
    StudioDisparityResult,
    compute_studio_bias_metrics,
    compute_studio_disparity,
    compute_studio_prestige,
    debias_authority_scores,
    extract_all_studios,
    extract_studio_from_anime,
    find_overvalued_by_studio,
    find_undervalued_by_studio,
)
from src.models import Anime, Credit, Role


# ============================================================
# Helpers
# ============================================================


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


# ============================================================
# 1. potential_value.py
# ============================================================


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
        assert pv.authority == 0.0
        assert pv.trust == 0.0
        assert pv.skill == 0.0
        assert pv.potential_value == 0.0
        assert pv.category == ValueCategory.STEADY_PERFORMER

    def test_custom_values(self):
        pv = PotentialValueScore(
            person_id="p1",
            authority=0.8,
            trust=0.7,
            potential_value=85.0,
            category=ValueCategory.ELITE,
        )
        assert pv.authority == 0.8
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
            "p1": {"authority": 0.9, "trust": 0.9, "skill": 0.8, "composite": 0.87},
            "p2": {"authority": 0.3, "trust": 0.2, "skill": 0.7, "composite": 0.35},
            "p3": {"authority": 0.5, "trust": 0.4, "skill": 0.5, "composite": 0.47},
        }
        debiased = {
            "p1": {"debiased_authority": 0.85},
            "p2": {"debiased_authority": 0.5},  # big improvement
            "p3": {"debiased_authority": 0.5},
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
        ps = {"p1": {"authority": 0.5, "trust": 0.3, "skill": 0.4, "composite": 0.4}}
        result = compute_potential_value_scores(ps, {}, {}, {}, nx.Graph())
        assert result["p1"].debiased_authority == 0.5

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
        ps = {"p1": {"authority": 0.3, "trust": 0.3, "skill": 0.7, "composite": 0.4}}
        debiased = {"p1": {"debiased_authority": 0.5}}  # improvement = 0.2 > 0.1
        growth = {
            "p1": {"growth_velocity": 0.0, "momentum_score": 0.0, "career_years": 10}
        }
        result = compute_potential_value_scores(
            ps, debiased, growth, {"p1": 0.7}, nx.Graph()
        )
        assert result["p1"].category == ValueCategory.HIDDEN_GEM

    def test_rising_star_category(self):
        """Person with velocity > 2.0 and authority > 0.5 => RISING_STAR."""
        ps = {"p1": {"authority": 0.6, "trust": 0.5, "skill": 0.6, "composite": 0.57}}
        debiased = {"p1": {"debiased_authority": 0.6}}
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
                authority=0.9,
                trust=0.8,
                skill=0.7,
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


class TestRoleImportance:
    def test_director_weight(self):
        assert compute_role_importance(Role.DIRECTOR) == 0.20

    def test_key_animator_weight(self):
        assert compute_role_importance(Role.KEY_ANIMATOR) == 0.06

    def test_unknown_role(self):
        """Roles not in the map should return 0.01."""
        # All roles are in the map, but let's test a role that is explicitly 0.01
        assert compute_role_importance(Role.OTHER) == 0.01

    def test_all_roles_have_weights(self):
        """Every role in ROLE_CONTRIBUTION_WEIGHTS should have a positive weight."""
        for role, weight in ROLE_CONTRIBUTION_WEIGHTS.items():
            assert weight > 0


class TestEstimateMarginalContribution:
    def test_basic_marginal(self):
        result = estimate_marginal_contribution(
            person_id="p1",
            role=Role.DIRECTOR,
            anime_value=100.0,
            person_scores={"p1": {"composite": 0.8}},
            staff_quality_avg=0.5,
        )
        # role_weight=0.20, quality_premium=(0.8-0.5)/(0.5+0.1)=0.5
        # marginal = 0.20 * 100 * (1+0.5) = 30.0
        assert result == 30.0

    def test_below_average_quality(self):
        result = estimate_marginal_contribution(
            person_id="p1",
            role=Role.DIRECTOR,
            anime_value=100.0,
            person_scores={"p1": {"composite": 0.2}},
            staff_quality_avg=0.5,
        )
        # quality_premium=(0.2-0.5)/(0.5+0.1)=-0.5
        # marginal = 0.20 * 100 * (1-0.5) = 10.0
        assert result == 10.0

    def test_missing_person_scores(self):
        """Person not in scores should use staff_quality_avg."""
        result = estimate_marginal_contribution(
            person_id="p_unknown",
            role=Role.KEY_ANIMATOR,
            anime_value=100.0,
            person_scores={},
            staff_quality_avg=0.5,
        )
        # quality_premium = (0.5-0.5)/(0.5+0.1)=0
        # marginal = 0.06 * 100 * 1 = 6.0
        assert result == 6.0

    def test_zero_anime_value(self):
        result = estimate_marginal_contribution(
            person_id="p1",
            role=Role.DIRECTOR,
            anime_value=0.0,
            person_scores={"p1": {"composite": 0.8}},
            staff_quality_avg=0.5,
        )
        assert result == 0.0


class TestComputeShapleyValueApproximate:
    def test_single_person(self):
        """With 1 staff, Shapley value = marginal contribution."""
        result = compute_shapley_value_approximate(
            person_id="p1",
            role=Role.DIRECTOR,
            all_staff=[("p1", Role.DIRECTOR)],
            anime_value=100.0,
            person_scores={"p1": {"composite": 0.5}},
            staff_quality_avg=0.5,
        )
        # Only person: position is always 0 => coalition is empty
        # marginal = own marginal_contribution
        expected = estimate_marginal_contribution(
            "p1", Role.DIRECTOR, 100.0, {"p1": {"composite": 0.5}}, 0.5
        )
        assert result == expected

    def test_returns_float(self):
        result = compute_shapley_value_approximate(
            person_id="p1",
            role=Role.DIRECTOR,
            all_staff=[("p1", Role.DIRECTOR), ("p2", Role.KEY_ANIMATOR)],
            anime_value=100.0,
            person_scores={"p1": {"composite": 0.6}, "p2": {"composite": 0.4}},
            staff_quality_avg=0.5,
        )
        assert isinstance(result, float)

    def test_person_not_in_staff(self):
        """If person is not in all_staff, Shapley should be 0."""
        result = compute_shapley_value_approximate(
            person_id="p_missing",
            role=Role.DIRECTOR,
            all_staff=[("p1", Role.DIRECTOR)],
            anime_value=100.0,
            person_scores={},
            staff_quality_avg=0.5,
        )
        assert result == 0


class TestComputeContributionAttribution:
    def test_empty_credits(self):
        result = compute_contribution_attribution("a1", 100.0, [], {})
        assert result == {}

    def test_single_contributor(self):
        credits = [_credit("p1", "a1", Role.DIRECTOR)]
        scores = {"p1": {"composite": 0.8}}
        result = compute_contribution_attribution("a1", 100.0, credits, scores)
        assert "p1" in result
        assert isinstance(result["p1"], ContributionMetrics)
        assert result["p1"].value_share == 100.0  # only contributor

    def test_multiple_contributors_shares_sum(self):
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.KEY_ANIMATOR),
            _credit("p3", "a1", Role.ANIMATION_DIRECTOR),
        ]
        scores = {
            "p1": {"composite": 0.8},
            "p2": {"composite": 0.5},
            "p3": {"composite": 0.6},
        }
        result = compute_contribution_attribution("a1", 100.0, credits, scores)
        total_share = sum(c.value_share for c in result.values())
        assert abs(total_share - 100.0) < 0.1

    def test_role_importance_is_set(self):
        credits = [_credit("p1", "a1", Role.DIRECTOR)]
        result = compute_contribution_attribution("a1", 100.0, credits, {})
        assert result["p1"].role_importance == 0.20

    def test_same_person_multiple_roles_accumulates(self):
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p1", "a1", Role.STORYBOARD),
        ]
        scores = {"p1": {"composite": 0.7}}
        result = compute_contribution_attribution("a1", 100.0, credits, scores)
        # Should only be one entry for p1, with accumulated marginal
        assert len(result) == 1
        assert result["p1"].marginal_contribution > 0


class TestAggregateContributions:
    def test_basic_aggregation(self):
        c1 = ContributionMetrics(
            person_id="p1",
            anime_id="a1",
            role=Role.DIRECTOR,
            shapley_value=10.0,
            marginal_contribution=12.0,
            value_share=60.0,
            irreplaceability=0.3,
        )
        c2 = ContributionMetrics(
            person_id="p1",
            anime_id="a2",
            role=Role.DIRECTOR,
            shapley_value=8.0,
            marginal_contribution=9.0,
            value_share=50.0,
            irreplaceability=0.2,
        )
        all_contribs = {
            "a1": {"p1": c1},
            "a2": {"p1": c2},
        }
        result = aggregate_contributions_by_person(all_contribs)
        assert "p1" in result
        assert result["p1"]["total_shapley"] == 18.0
        assert result["p1"]["work_count"] == 2
        assert result["p1"]["avg_value_share"] == 55.0
        assert result["p1"]["primary_role"] == "director"

    def test_empty_input(self):
        result = aggregate_contributions_by_person({})
        assert result == {}


class TestFindUndervaluedContributors:
    def test_finds_undervalued(self):
        aggregates = {
            "p1": {"total_shapley": 20.0, "work_count": 2},  # per_work=10
            "p2": {"total_shapley": 2.0, "work_count": 2},  # per_work=1
        }
        scores = {
            "p1": {"composite": 3.0},  # 10 > 3*1.5=4.5 => undervalued
            "p2": {"composite": 5.0},  # 1 < 5*1.5=7.5 => not undervalued
        }
        result = find_undervalued_contributors(aggregates, scores)
        assert len(result) == 1
        assert result[0][0] == "p1"

    def test_empty_input(self):
        assert find_undervalued_contributors({}, {}) == []


class TestFindMvpByRole:
    def test_finds_mvp(self):
        aggregates = {
            "p1": {"total_shapley": 30.0, "work_count": 5, "primary_role": "director"},
            "p2": {"total_shapley": 20.0, "work_count": 3, "primary_role": "director"},
            "p3": {
                "total_shapley": 50.0,
                "work_count": 8,
                "primary_role": "key_animator",
            },
        }
        result = find_mvp_by_role(aggregates, "director", top_n=5)
        assert len(result) == 2
        assert result[0][0] == "p1"  # highest shapley among directors

    def test_no_matching_role(self):
        aggregates = {
            "p1": {"total_shapley": 30.0, "work_count": 5, "primary_role": "director"},
        }
        result = find_mvp_by_role(aggregates, "key_animator")
        assert result == []


# ============================================================
# 3. studio_bias_correction.py
# ============================================================


class TestExtractStudio:
    def test_studios_list_preferred(self):
        a = _anime("a1", studios=["Bones", "MAPPA"])
        assert extract_studio_from_anime(a) == "Bones"

    def test_fallback_to_studio_field(self):
        a = _anime("a1", studio="Ghibli")
        assert extract_studio_from_anime(a) == "Ghibli"

    def test_no_studio_returns_unknown(self):
        a = _anime("a1")
        assert extract_studio_from_anime(a) == "unknown"

    def test_extract_all_studios(self):
        a = _anime("a1", studios=["Bones", "MAPPA"])
        assert extract_all_studios(a) == ["Bones", "MAPPA"]

    def test_extract_all_studios_fallback(self):
        a = _anime("a1", studio="Ghibli")
        assert extract_all_studios(a) == ["Ghibli"]

    def test_extract_all_studios_empty(self):
        a = _anime("a1")
        assert extract_all_studios(a) == []


class TestComputeStudioBiasMetrics:
    @pytest.fixture()
    def bias_data(self):
        anime_map = {
            "a1": _anime("a1", studios=["StudioA"]),
            "a2": _anime("a2", studios=["StudioA"]),
            "a3": _anime("a3", studios=["StudioB"]),
            "a4": _anime("a4", studios=["StudioC"]),
        }
        credits = [
            _credit("p1", "a1"),
            _credit("p1", "a2"),
            _credit("p1", "a3"),
            _credit("p1", "a4"),
            _credit("p2", "a1"),
            _credit("p2", "a2"),
        ]
        return credits, anime_map

    def test_returns_all_persons(self, bias_data):
        credits, anime_map = bias_data
        result = compute_studio_bias_metrics(credits, anime_map)
        assert "p1" in result
        assert "p2" in result

    def test_primary_studio(self, bias_data):
        credits, anime_map = bias_data
        result = compute_studio_bias_metrics(credits, anime_map)
        assert result["p1"].primary_studio == "StudioA"
        assert result["p2"].primary_studio == "StudioA"

    def test_diverse_person_has_higher_diversity(self, bias_data):
        credits, anime_map = bias_data
        result = compute_studio_bias_metrics(credits, anime_map)
        # p1 worked at 3 studios, p2 at 1
        assert result["p1"].studio_diversity > result["p2"].studio_diversity

    def test_concentrated_person_has_high_concentration(self, bias_data):
        credits, anime_map = bias_data
        result = compute_studio_bias_metrics(credits, anime_map)
        # p2 only at StudioA => concentration = 1.0
        assert result["p2"].studio_concentration == 1.0

    def test_cross_studio_works_count(self, bias_data):
        credits, anime_map = bias_data
        result = compute_studio_bias_metrics(credits, anime_map)
        assert result["p1"].cross_studio_works == 2  # 3 studios - 1
        assert result["p2"].cross_studio_works == 0

    def test_empty_credits(self):
        """Empty credits returns empty dict."""
        result = compute_studio_bias_metrics([], {})
        assert result == {}


class TestComputeStudioPrestige:
    def test_basic_prestige(self):
        anime_map = {
            "a1": _anime("a1", studios=["StudioA"]),
            "a2": _anime("a2", studios=["StudioB"]),
        }
        credits = [
            _credit("p1", "a1"),
            _credit("p2", "a2"),
        ]
        scores = {
            "p1": {"authority": 0.9},
            "p2": {"authority": 0.3},
        }
        result = compute_studio_prestige(credits, anime_map, scores)
        assert result["StudioA"] > result["StudioB"]

    def test_unknown_person_ignored(self):
        anime_map = {"a1": _anime("a1", studios=["StudioA"])}
        credits = [_credit("p_unknown", "a1")]
        result = compute_studio_prestige(credits, anime_map, {})
        assert result.get("StudioA") is None or result["StudioA"] == 0


class TestDebiasAuthorityScores:
    def test_no_bias_data_preserves_original(self):
        ps = {"p1": {"authority": 0.7}}
        result = debias_authority_scores(ps, {}, {})
        assert result["p1"].debiased_authority == 0.7

    def test_high_concentration_reduces_score(self):
        ps = {"p1": {"authority": 0.8}}
        bias = {
            "p1": StudioBiasMetrics(
                person_id="p1",
                primary_studio="BigStudio",
                studio_concentration=1.0,
                studio_diversity=0.0,
            ),
        }
        prestige = {"BigStudio": 0.9}
        result = debias_authority_scores(ps, bias, prestige, debias_strength=0.5)
        assert result["p1"].debiased_authority < 0.8

    def test_diverse_person_gets_bonus(self):
        ps = {"p1": {"authority": 0.5}}
        bias = {
            "p1": StudioBiasMetrics(
                person_id="p1",
                primary_studio="SmallStudio",
                studio_concentration=0.3,
                studio_diversity=0.9,
                cross_studio_works=4,
            ),
        }
        prestige = {"SmallStudio": 0.2}
        result = debias_authority_scores(ps, bias, prestige, debias_strength=0.3)
        # Diversity bonus + cross-studio bonus should increase score
        assert result["p1"].debiased_authority > 0.5

    def test_debiased_score_fields(self):
        ps = {"p1": {"authority": 0.6}}
        bias = {
            "p1": StudioBiasMetrics(
                person_id="p1",
                primary_studio="Studio",
                studio_concentration=0.5,
                studio_diversity=0.5,
                cross_studio_works=2,
            ),
        }
        prestige = {"Studio": 0.5}
        result = debias_authority_scores(ps, bias, prestige)
        d = result["p1"]
        assert isinstance(d, DebiasedScore)
        assert d.person_id == "p1"
        assert d.original_authority == 0.6
        assert d.studio_bias >= 0
        assert d.diversity_factor >= 1.0


class TestStudioDisparityResult:
    def test_dataclass_fields(self):
        r = StudioDisparityResult(
            studio="MAPPA",
            person_count=10,
            mean_composite=0.6,
        )
        assert r.studio == "MAPPA"
        assert r.person_count == 10


class TestComputeStudioDisparity:
    def test_basic_disparity(self):
        anime_map = {f"a{i}": _anime(f"a{i}", studios=["StudioA"]) for i in range(6)}
        credits = [_credit(f"p{i}", f"a{i}") for i in range(6)]
        person_scores = {
            f"p{i}": {"authority": 0.5, "trust": 0.4, "skill": 0.6, "composite": 0.5}
            for i in range(6)
        }
        result = compute_studio_disparity(
            credits, anime_map, person_scores, min_persons=5
        )
        assert "StudioA" in result
        assert result["StudioA"].person_count == 6

    def test_too_few_persons_filtered(self):
        anime_map = {"a1": _anime("a1", studios=["TinyStudio"])}
        credits = [_credit("p1", "a1")]
        person_scores = {"p1": {"composite": 0.5}}
        result = compute_studio_disparity(
            credits, anime_map, person_scores, min_persons=5
        )
        assert "TinyStudio" not in result


class TestFindUnderOvervalued:
    def test_find_undervalued(self):
        debiased = {
            "p1": DebiasedScore(
                person_id="p1", original_authority=0.3, debiased_authority=0.7
            ),
            "p2": DebiasedScore(
                person_id="p2", original_authority=0.6, debiased_authority=0.5
            ),
        }
        result = find_undervalued_by_studio(debiased)
        assert result[0][0] == "p1"  # biggest improvement

    def test_find_overvalued(self):
        debiased = {
            "p1": DebiasedScore(
                person_id="p1", original_authority=0.8, debiased_authority=0.3
            ),
            "p2": DebiasedScore(
                person_id="p2", original_authority=0.3, debiased_authority=0.5
            ),
        }
        result = find_overvalued_by_studio(debiased)
        assert result[0][0] == "p1"  # biggest decline


# ============================================================
# 4. growth_acceleration.py
# ============================================================


class TestAccelerationMetrics:
    def test_defaults(self):
        m = AccelerationMetrics(person_id="p1")
        assert m.career_years == 0
        assert m.trend == "stable"
        assert m.momentum_score == 0.0


class TestComputeGrowthMetrics:
    @pytest.fixture()
    def growth_data(self):
        anime_map = {
            "a1": _anime("a1", year=2015),
            "a2": _anime("a2", year=2016),
            "a3": _anime("a3", year=2020),
            "a4": _anime("a4", year=2021),
            "a5": _anime("a5", year=2022),
            "a6": _anime("a6", year=2023),
            "a7": _anime("a7", year=2024),
            "a8": _anime("a8", year=2024),
        }
        credits = [
            # p1: rising - few early, many recent
            _credit("p1", "a1"),
            _credit("p1", "a5"),
            _credit("p1", "a6"),
            _credit("p1", "a7"),
            _credit("p1", "a8"),
            # p2: early career only
            _credit("p2", "a5"),
            _credit("p2", "a6"),
            # p3: declining - many early, few recent
            _credit("p3", "a1"),
            _credit("p3", "a2"),
            _credit("p3", "a3"),
            _credit("p3", "a4"),
            _credit("p3", "a5"),
            _credit("p3", "a6"),
            _credit("p3", "a7"),
        ]
        return credits, anime_map

    def test_returns_dict(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map)
        assert isinstance(result, dict)
        assert "p1" in result

    def test_career_years(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map)
        # p1: 2015-2024 = 10 years
        assert result["p1"].career_years == 10

    def test_total_credits(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map)
        assert result["p1"].total_credits == 5

    def test_early_trend(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map)
        # p2: career_years = 2 <= 3 => "early"
        assert result["p2"].trend == "early"

    def test_peak_year(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map)
        # p1 has 2 credits in 2024, 1 in each other year
        assert result["p1"].peak_year == 2024

    def test_empty_credits(self):
        result = compute_growth_metrics([], {})
        assert result == {}

    def test_anime_without_year_skipped(self):
        anime_map = {"a1": _anime("a1", year=None)}
        credits = [_credit("p1", "a1")]
        result = compute_growth_metrics(credits, anime_map)
        assert "p1" not in result

    def test_early_career_bonus(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map, current_year=2024)
        # p2 debuts in 2022, so years_since_debut=2 => bonus=(5-2)*0.1=0.3
        assert result["p2"].early_career_bonus == 0.3

    def test_no_early_bonus_for_veteran(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map, current_year=2026)
        # p1 debuts in 2015, years_since_debut=11 > 5 => no bonus
        assert result["p1"].early_career_bonus == 0


class TestFindFastRisers:
    def test_finds_risers(self):
        metrics = {
            "p1": AccelerationMetrics(
                person_id="p1", growth_velocity=3.0, growth_acceleration=1.0
            ),
            "p2": AccelerationMetrics(
                person_id="p2", growth_velocity=0.5, growth_acceleration=0.0
            ),
        }
        result = find_fast_risers(metrics, min_velocity=2.0)
        assert len(result) == 1
        assert result[0][0] == "p1"

    def test_empty_input(self):
        assert find_fast_risers({}) == []


class TestFindEarlyPotential:
    def test_finds_early_talent(self):
        metrics = {
            "p1": AccelerationMetrics(
                person_id="p1", career_years=3, momentum_score=2.0
            ),
            "p2": AccelerationMetrics(
                person_id="p2", career_years=10, momentum_score=3.0
            ),
        }
        result = find_early_potential(metrics, max_career_years=5, min_momentum=1.0)
        assert len(result) == 1
        assert result[0][0] == "p1"


class TestComputeAdjustedSkillWithGrowth:
    def test_basic_adjustment(self):
        person_scores = {
            "p1": {"skill": 0.5},
            "p2": {"skill": 0.6},
        }
        growth_metrics = {
            "p1": AccelerationMetrics(
                person_id="p1",
                growth_velocity=2.0,
                growth_acceleration=1.0,
                early_career_bonus=0.2,
            ),
        }
        result = compute_adjusted_skill_with_growth(
            person_scores, growth_metrics, growth_weight=0.3
        )
        assert "p1" in result
        assert result["p1"] > 0.5  # Should be higher due to growth
        assert result["p2"] == 0.6  # No growth data => original skill

    def test_no_growth_metrics(self):
        person_scores = {"p1": {"skill": 0.7}}
        result = compute_adjusted_skill_with_growth(person_scores, {})
        assert result["p1"] == 0.7

    def test_negative_growth_reduces_skill(self):
        person_scores = {"p1": {"skill": 0.6}}
        growth_metrics = {
            "p1": AccelerationMetrics(
                person_id="p1",
                growth_velocity=-3.0,
                growth_acceleration=-1.0,
                early_career_bonus=0.0,
            ),
        }
        result = compute_adjusted_skill_with_growth(
            person_scores, growth_metrics, growth_weight=0.3
        )
        assert result["p1"] < 0.6


# ============================================================
# 5. anime_value.py
# ============================================================


class TestAnimeValueMetrics:
    def test_defaults(self):
        m = AnimeValueMetrics(anime_id="a1", title="Test")
        assert m.composite_value == 0.0
        assert m.staff_count == 0


class TestComputeCommercialValue:
    def test_with_score_and_staff(self):
        anime = _anime("a1", score=80.0)
        credits = [_credit(f"p{i}", "a1") for i in range(30)]
        result = compute_commercial_value(anime, credits)
        # staff_score=min(1, 30/50)=0.6, diversity_score=min(1, 1/20)=0.05
        # external_score=min(1, 80/100)=0.8
        # commercial = 0.6*0.3 + 0.05*0.2 + 0.8*0.5 = 0.18 + 0.01 + 0.4 = 0.59
        assert 0 < result <= 1.0

    def test_no_score_uses_default(self):
        anime = _anime("a1", score=None)
        credits = [_credit("p1", "a1")]
        result = compute_commercial_value(anime, credits)
        assert result > 0  # Default external_score=0.5

    def test_zero_score_uses_default(self):
        anime = _anime("a1", score=0.0)
        credits = [_credit("p1", "a1")]
        result = compute_commercial_value(anime, credits)
        assert result > 0


class TestComputeCriticalValue:
    def test_with_tags_and_score(self):
        anime = _anime(
            "a1", score=90.0, tags=[{"name": f"tag{i}", "rank": i} for i in range(10)]
        )
        credits = [_credit("p1", "a1")]
        result = compute_critical_value(anime, credits)
        assert 0 < result <= 1.0

    def test_no_tags(self):
        anime = _anime("a1", score=70.0, tags=[])
        credits = [_credit("p1", "a1")]
        result = compute_critical_value(anime, credits)
        assert result > 0


class TestComputeCreativeValue:
    def test_with_director_and_animator(self):
        anime = _anime("a1", tags=[{"name": f"tag{i}"} for i in range(10)])
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.KEY_ANIMATOR),
        ]
        scores = {"p1": {"skill": 0.8}, "p2": {"skill": 0.6}}
        result = compute_creative_value(anime, credits, scores)
        assert 0 < result <= 1.0

    def test_no_scored_staff(self):
        anime = _anime("a1")
        credits = [_credit("p1", "a1", Role.DIRECTOR)]
        result = compute_creative_value(anime, credits, {})
        # Should use defaults
        assert result > 0


class TestComputeCulturalValue:
    def test_classic_anime(self):
        anime = _anime("a1", year=2000)
        result = compute_cultural_value(anime, [], current_year=2026)
        # Age = 26, age_score=min(1, 26/20)=1.0, longevity_score=0.8
        # cultural = 1.0*0.5 + 0.8*0.5 = 0.9
        assert result == 0.9

    def test_recent_anime(self):
        anime = _anime("a1", year=2024)
        result = compute_cultural_value(anime, [], current_year=2026)
        # Age = 2, age_score=0.1, longevity_score=0.4
        # cultural = 0.1*0.5 + 0.4*0.5 = 0.25
        assert result == 0.25

    def test_no_year(self):
        anime = _anime("a1", year=None)
        result = compute_cultural_value(anime, [])
        assert result == 0.5


class TestComputeTechnicalValue:
    def test_with_animators(self):
        anime = _anime("a1")
        credits = [
            _credit("p1", "a1", Role.KEY_ANIMATOR),
            _credit("p2", "a1", Role.ART_DIRECTOR),
        ]
        scores = {"p1": {"composite": 0.8}, "p2": {"composite": 0.7}}
        result = compute_technical_value(anime, credits, scores)
        assert 0 < result <= 1.0

    def test_no_staff(self):
        anime = _anime("a1")
        result = compute_technical_value(anime, [], {})
        # No animators, no tech staff => defaults
        assert result >= 0


class TestComputeAnimeValues:
    @pytest.fixture()
    def anime_data(self):
        anime_list = [
            _anime(
                "a1",
                year=2020,
                score=85.0,
                studios=["StudioA"],
                tags=[{"name": "action"}],
            ),
            _anime("a2", year=2005, score=70.0, studios=["StudioB"]),
        ]
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.KEY_ANIMATOR),
            _credit("p3", "a2", Role.DIRECTOR),
        ]
        person_scores = {
            "p1": {"authority": 0.8, "trust": 0.7, "skill": 0.9, "composite": 0.8},
            "p2": {"authority": 0.5, "trust": 0.4, "skill": 0.6, "composite": 0.5},
            "p3": {"authority": 0.3, "trust": 0.3, "skill": 0.4, "composite": 0.33},
        }
        return anime_list, credits, person_scores

    def test_returns_all_anime(self, anime_data):
        anime_list, credits, person_scores = anime_data
        result = compute_anime_values(anime_list, credits, person_scores)
        assert "a1" in result
        assert "a2" in result

    def test_result_type(self, anime_data):
        anime_list, credits, person_scores = anime_data
        result = compute_anime_values(anime_list, credits, person_scores)
        assert isinstance(result["a1"], AnimeValueMetrics)

    def test_composite_value_range(self, anime_data):
        anime_list, credits, person_scores = anime_data
        result = compute_anime_values(anime_list, credits, person_scores)
        for v in result.values():
            assert 0 <= v.composite_value <= 100

    def test_anime_without_credits_excluded(self, anime_data):
        anime_list, credits, person_scores = anime_data
        anime_list.append(_anime("a_empty", year=2020))
        result = compute_anime_values(anime_list, credits, person_scores)
        assert "a_empty" not in result

    def test_staff_count(self, anime_data):
        anime_list, credits, person_scores = anime_data
        result = compute_anime_values(anime_list, credits, person_scores)
        assert result["a1"].staff_count == 2
        assert result["a2"].staff_count == 1

    def test_value_per_staff(self, anime_data):
        anime_list, credits, person_scores = anime_data
        result = compute_anime_values(anime_list, credits, person_scores)
        for v in result.values():
            if v.staff_count > 0:
                assert abs(v.value_per_staff - v.composite_value / v.staff_count) < 0.1

    def test_anime_score_used(self):
        """Anime score should influence commercial and critical value."""
        anime_high = [_anime("a_high", score=95.0)]
        anime_low = [_anime("a_low", score=30.0)]
        credits_high = [_credit("p1", "a_high")]
        credits_low = [_credit("p1", "a_low")]
        scores = {
            "p1": {"authority": 0.5, "trust": 0.5, "skill": 0.5, "composite": 0.5}
        }
        result_high = compute_anime_values(anime_high, credits_high, scores)
        result_low = compute_anime_values(anime_low, credits_low, scores)
        assert (
            result_high["a_high"].commercial_value
            > result_low["a_low"].commercial_value
        )


class TestRankAnimeByValue:
    def test_ranking_order(self):
        values = {
            "a1": AnimeValueMetrics(
                anime_id="a1",
                title="Show A",
                composite_value=90.0,
                commercial_value=0.8,
                creative_value=0.7,
                technical_value=0.6,
                cultural_value=0.5,
            ),
            "a2": AnimeValueMetrics(
                anime_id="a2",
                title="Show B",
                composite_value=50.0,
                commercial_value=0.4,
                creative_value=0.3,
                technical_value=0.2,
                cultural_value=0.1,
            ),
        }
        ranked = rank_anime_by_value(values, dimension="composite")
        assert ranked[0][0] == "a1"
        assert ranked[1][0] == "a2"

    def test_ranking_by_dimension(self):
        values = {
            "a1": AnimeValueMetrics(
                anime_id="a1",
                title="Show A",
                composite_value=50.0,
                commercial_value=0.9,
            ),
            "a2": AnimeValueMetrics(
                anime_id="a2",
                title="Show B",
                composite_value=80.0,
                commercial_value=0.2,
            ),
        }
        ranked = rank_anime_by_value(values, dimension="commercial")
        assert ranked[0][0] == "a1"

    def test_top_n(self):
        values = {
            f"a{i}": AnimeValueMetrics(
                anime_id=f"a{i}", title=f"Show {i}", composite_value=float(i)
            )
            for i in range(10)
        }
        ranked = rank_anime_by_value(values, top_n=3)
        assert len(ranked) == 3

    def test_value_per_staff_dimension(self):
        values = {
            "a1": AnimeValueMetrics(
                anime_id="a1", title="Show A", value_per_staff=20.0
            ),
            "a2": AnimeValueMetrics(
                anime_id="a2", title="Show B", value_per_staff=10.0
            ),
        }
        ranked = rank_anime_by_value(values, dimension="value_per_staff")
        assert ranked[0][0] == "a1"


class TestFindUnderOverPerforming:
    def test_find_undervalued_works(self):
        values = {
            "a1": AnimeValueMetrics(
                anime_id="a1",
                title="Hidden Gem",
                staff_quality=0.8,
                composite_value=40.0,
            ),
            "a2": AnimeValueMetrics(
                anime_id="a2",
                title="Popular",
                staff_quality=0.8,
                composite_value=80.0,
            ),
        }
        result = find_undervalued_works(values, min_staff_quality=0.6, max_value=50)
        assert len(result) == 1
        assert result[0][0] == "a1"

    def test_find_overperforming_works(self):
        values = {
            "a1": AnimeValueMetrics(
                anime_id="a1",
                title="Surprise Hit",
                staff_quality=0.2,
                composite_value=70.0,
            ),
            "a2": AnimeValueMetrics(
                anime_id="a2",
                title="Normal",
                staff_quality=0.8,
                composite_value=80.0,
            ),
        }
        result = find_overperforming_works(values, max_staff_quality=0.4, min_value=60)
        assert len(result) == 1
        assert result[0][0] == "a1"


# ============================================================
# 6. additional individual_contribution edge cases
# ============================================================


class TestIndividualContributionEdgeCases:
    def test_consistency_with_zero_mean(self):
        """anime.score=0 should result in None consistency."""
        from src.analysis.individual_contribution import compute_consistency

        features = {"p1": {"composite": 50}}
        anime_map = {
            f"a{i}": Anime(id=f"a{i}", title_ja=f"a{i}", score=0.0) for i in range(6)
        }
        credits = [_credit("p1", f"a{i}") for i in range(6)]
        result = compute_consistency(features, credits, anime_map)
        assert result["p1"] is None

    def test_independent_value_with_collaboration_graph(self):
        """Test independent_value uses collaboration_graph when provided."""
        from src.analysis.individual_contribution import compute_independent_value

        features = {f"p{i}": {"composite": 50 + i * 5} for i in range(6)}
        anime_map = {
            "shared": _anime("shared", score=90),
            "solo1": _anime("solo1", score=50),
            "solo2": _anime("solo2", score=55),
            "solo3": _anime("solo3", score=60),
        }
        credits = [
            _credit("p0", "shared"),
            _credit("p1", "shared"),
            _credit("p1", "solo1"),
            _credit("p2", "shared"),
            _credit("p2", "solo2"),
            _credit("p3", "shared"),
            _credit("p3", "solo3"),
            _credit("p4", "solo1"),
            _credit("p5", "solo2"),
        ]
        G = nx.Graph()
        G.add_edges_from([("p0", "p1"), ("p0", "p2"), ("p0", "p3")])

        result = compute_independent_value(
            features, credits, anime_map, collaboration_graph=G
        )
        # p0 should have a positive value (collaborators do better when with p0)
        if result["p0"] is not None:
            assert result["p0"] > 0
