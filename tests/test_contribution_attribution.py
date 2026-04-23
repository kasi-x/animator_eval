"""contribution_attribution.py coverage tests."""
from src.analysis.contribution_attribution import (
    ContributionMetrics,
    aggregate_contributions_by_person,
    compute_contribution_attribution,
    compute_role_importance,
    compute_shapley_value_approximate,
    estimate_marginal_contribution,
    estimate_role_weights,
    find_mvp_by_role,
    find_undervalued_contributors,
    set_role_weights,
)


from src.models import BronzeAnime as Anime, Credit, Role


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

class TestRoleWeightEstimation:
    """OLS-based role weight estimation tests."""

    def _make_credits(self, n_anime=50):
        """Generate synthetic credits for OLS estimation testing."""
        import random

        random.seed(42)
        credits = []
        roles = [
            Role.DIRECTOR,
            Role.KEY_ANIMATOR,
            Role.ANIMATION_DIRECTOR,
            Role.SCREENPLAY,
            Role.EPISODE_DIRECTOR,
        ]
        for i in range(n_anime):
            aid = f"a{i}"
            # Bigger anime have more directors/animators
            n_staff = random.randint(5, 100)
            for j in range(n_staff):
                role = random.choice(roles)
                credits.append(_credit(f"p{j}_{i}", aid, role))
        return credits

    def test_ols_returns_weights(self):
        credits = self._make_credits(50)
        result = estimate_role_weights(credits)
        assert result.method == "ols"
        assert result.n_anime >= 30
        assert len(result.weights) > 0

    def test_weights_sum_to_one(self):
        credits = self._make_credits(50)
        result = estimate_role_weights(credits)
        assert abs(sum(result.weights.values()) - 1.0) < 1e-6

    def test_all_weights_positive(self):
        credits = self._make_credits(50)
        result = estimate_role_weights(credits)
        for role, w in result.weights.items():
            assert w > 0, f"Role {role} has non-positive weight {w}"

    def test_r_squared_reasonable(self):
        credits = self._make_credits(50)
        result = estimate_role_weights(credits)
        assert 0.0 <= result.r_squared <= 1.0

    def test_insufficient_anime_returns_uniform(self):
        """With <30 anime, should fall back to uniform weights."""
        credits = [_credit("p1", "a1", Role.DIRECTOR)]
        result = estimate_role_weights(credits)
        assert result.method == "uniform"

    def test_empty_credits_returns_uniform(self):
        result = estimate_role_weights([])
        assert result.method == "uniform"

    def test_coefficients_stored(self):
        credits = self._make_credits(50)
        result = estimate_role_weights(credits)
        assert len(result.coefficients) == len(result.weights)


class TestRoleImportance:
    def test_with_set_weights(self):
        """After set_role_weights, compute_role_importance uses those weights."""
        set_role_weights({"director": 0.30, "key_animator": 0.10})
        assert compute_role_importance(Role.DIRECTOR) == 0.30
        assert compute_role_importance(Role.KEY_ANIMATOR) == 0.10
        # Reset
        set_role_weights(None)

    def test_fallback_uniform(self):
        """Without set weights, all roles get uniform weight."""
        set_role_weights(None)
        w = compute_role_importance(Role.DIRECTOR)
        assert w > 0
        # Uniform: 1/n_roles
        assert abs(w - 1.0 / len(Role)) < 1e-6
        assert w == compute_role_importance(Role.SPECIAL)

    def test_unknown_role_in_weights(self):
        """Role not in weight dict should return 0.01."""
        set_role_weights({"director": 0.50})
        assert compute_role_importance(Role.KEY_ANIMATOR) == 0.01
        set_role_weights(None)


class TestEstimateMarginalContribution:
    def test_basic_marginal(self):
        set_role_weights({"director": 0.20})
        result = estimate_marginal_contribution(
            person_id="p1",
            role=Role.DIRECTOR,
            anime_value=100.0,
            person_scores={"p1": {"iv_score": 0.8}},
            staff_quality_avg=0.5,
        )
        # role_weight=0.20, quality_premium=(0.8-0.5)/(0.5+0.1)=0.5
        # marginal = 0.20 * 100 * (1+0.5) = 30.0
        assert result == 30.0
        set_role_weights(None)

    def test_below_average_quality(self):
        set_role_weights({"director": 0.20})
        result = estimate_marginal_contribution(
            person_id="p1",
            role=Role.DIRECTOR,
            anime_value=100.0,
            person_scores={"p1": {"iv_score": 0.2}},
            staff_quality_avg=0.5,
        )
        # quality_premium=(0.2-0.5)/(0.5+0.1)=-0.5
        # marginal = 0.20 * 100 * (1-0.5) = 10.0
        assert result == 10.0
        set_role_weights(None)

    def test_missing_person_scores(self):
        """Person not in scores should use staff_quality_avg."""
        set_role_weights({"key_animator": 0.06})
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
        set_role_weights(None)

    def test_zero_anime_value(self):
        result = estimate_marginal_contribution(
            person_id="p1",
            role=Role.DIRECTOR,
            anime_value=0.0,
            person_scores={"p1": {"iv_score": 0.8}},
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
            person_scores={"p1": {"iv_score": 0.5}},
            staff_quality_avg=0.5,
        )
        # Only person: position is always 0 => coalition is empty
        # marginal = own marginal_contribution
        expected = estimate_marginal_contribution(
            "p1", Role.DIRECTOR, 100.0, {"p1": {"iv_score": 0.5}}, 0.5
        )
        assert result == expected

    def test_returns_float(self):
        result = compute_shapley_value_approximate(
            person_id="p1",
            role=Role.DIRECTOR,
            all_staff=[("p1", Role.DIRECTOR), ("p2", Role.KEY_ANIMATOR)],
            anime_value=100.0,
            person_scores={"p1": {"iv_score": 0.6}, "p2": {"iv_score": 0.4}},
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
        scores = {"p1": {"iv_score": 0.8}}
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
            "p1": {"iv_score": 0.8},
            "p2": {"iv_score": 0.5},
            "p3": {"iv_score": 0.6},
        }
        result = compute_contribution_attribution("a1", 100.0, credits, scores)
        total_share = sum(c.value_share for c in result.values())
        assert abs(total_share - 100.0) < 0.1

    def test_role_importance_is_set(self):
        set_role_weights({"director": 0.20})
        credits = [_credit("p1", "a1", Role.DIRECTOR)]
        result = compute_contribution_attribution("a1", 100.0, credits, {})
        assert result["p1"].role_importance == 0.20
        set_role_weights(None)

    def test_same_person_multiple_roles_accumulates(self):
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p1", "a1", Role.EPISODE_DIRECTOR),
        ]
        scores = {"p1": {"iv_score": 0.7}}
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
            "p1": {"iv_score": 3.0},  # 10 > 3*1.5=4.5 => undervalued
            "p2": {"iv_score": 5.0},  # 1 < 5*1.5=7.5 => not undervalued
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


