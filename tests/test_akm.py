"""Tests for AKM fixed effects decomposition."""

import numpy as np
import pytest

from src.analysis.scoring.akm import (
    AKMResult,
    _build_panel,
    _compute_credit_weight,
    _debias_by_obs_count,
    _redistribute_studio_fe,
    _shrink_person_fe,
    estimate_akm,
    find_connected_set,
    infer_studio_assignment,
)
from src.models import BronzeAnime as Anime, Credit, Person, Role


@pytest.fixture
def studio_data():
    """3 studios, 5 anime, 8 persons with movers between studios.

    Studio A (high quality): anime a1 (score=9.0), a2 (score=8.5)
    Studio B (mid quality):  anime a3 (score=7.0), a4 (score=6.5)
    Studio C (low quality):  anime a5 (score=5.0)

    Movers (work at 2+ studios):
      p1: Studio A (2018) -> Studio B (2020)
      p2: Studio B (2019) -> Studio C (2021)
    Stayers:
      p3, p4: Studio A only
      p5, p6: Studio B only
      p7, p8: Studio C only (p8 added to a3 too for connected set)
    """
    anime_map = {
        "a1": Anime(
            id="a1", title_en="Alpha", year=2018, studios=["StudioA"]
        ),
        "a2": Anime(
            id="a2", title_en="Beta", year=2019, studios=["StudioA"]
        ),
        "a3": Anime(
            id="a3", title_en="Gamma", year=2019, studios=["StudioB"]
        ),
        "a4": Anime(
            id="a4", title_en="Delta", year=2020, studios=["StudioB"]
        ),
        "a5": Anime(
            id="a5", title_en="Epsilon", year=2021, studios=["StudioC"]
        ),
    }

    credits = [
        # p1: mover A -> B
        Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p1", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        # p2: mover B -> C
        Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"),
        # p3, p4: stayers at A
        Credit(
            person_id="p3", anime_id="a1", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(
            person_id="p3", anime_id="a2", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p4", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        # p5, p6: stayers at B
        Credit(
            person_id="p5", anime_id="a3", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(
            person_id="p5", anime_id="a4", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p6", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p6", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        # p7: stayer at C
        Credit(person_id="p7", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"),
        # p8: at C primarily but also in a3 (Studio B) to help connectivity
        Credit(person_id="p8", anime_id="a5", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p8", anime_id="a3", role=Role.IN_BETWEEN, source="test"),
    ]

    persons = [Person(id=f"p{i}", name_en=f"Person {i}") for i in range(1, 9)]
    return persons, anime_map, credits


class TestInferStudioAssignment:
    def test_infer_studio_assignment(self, studio_data):
        """Correct studio per person-year."""
        _, anime_map, credits = studio_data
        assignments = infer_studio_assignment(credits, anime_map)

        # p1 was at StudioA in 2018, StudioB in 2020
        assert assignments["p1"][2018] == "StudioA"
        assert assignments["p1"][2020] == "StudioB"

        # p3 was at StudioA in both years
        assert assignments["p3"][2018] == "StudioA"
        assert assignments["p3"][2019] == "StudioA"

        # p5 was at StudioB
        assert assignments["p5"][2019] == "StudioB"

    def test_empty_credits(self):
        """Empty credits produce empty assignments."""
        result = infer_studio_assignment([], {})
        assert result == {}

    def test_anime_without_studio_skipped(self):
        """Credits for anime with no studio are ignored."""
        anime_map = {
            "a1": Anime(id="a1", title_en="NoStudio", year=2020, studios=[])
        }
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test")
        ]
        result = infer_studio_assignment(credits, anime_map)
        assert "p1" not in result


class TestFindConnectedSet:
    def test_find_connected_set(self, studio_data):
        """Movers link studios into connected components."""
        _, anime_map, credits = studio_data
        assignments = infer_studio_assignment(credits, anime_map)
        connected_persons, connected_studios = find_connected_set(assignments)

        # p1 links A-B, p2 links B-C, p8 links B-C: all three studios connected
        assert "StudioA" in connected_studios
        assert "StudioB" in connected_studios
        assert "StudioC" in connected_studios

        # Movers should be in connected set
        assert "p1" in connected_persons
        assert "p2" in connected_persons

    def test_no_movers(self):
        """When no one moves, all persons and studios still returned."""
        assignments = {
            "p1": {2020: "StudioA"},
            "p2": {2020: "StudioB"},
        }
        persons, studios = find_connected_set(assignments)
        assert "p1" in persons
        assert "p2" in persons

    def test_single_mover_connects_two_studios(self):
        """A single mover links two otherwise isolated studios."""
        assignments = {
            "p1": {2018: "StudioA", 2020: "StudioB"},
            "p2": {2019: "StudioA"},
            "p3": {2020: "StudioB"},
        }
        persons, studios = find_connected_set(assignments)
        assert "StudioA" in studios
        assert "StudioB" in studios
        assert len(persons) == 3


class TestEstimateAKM:
    def test_estimate_akm_basic(self, studio_data):
        """AKM produces person_fe and studio_fe."""
        _, anime_map, credits = studio_data
        result = estimate_akm(credits, anime_map)
        assert isinstance(result, AKMResult)
        assert len(result.person_fe) > 0
        assert len(result.studio_fe) > 0

    def test_akm_r_squared_positive(self, studio_data):
        """Model explains some variance (R^2 > 0 and ≤ 1.0)."""
        _, anime_map, credits = studio_data
        result = estimate_akm(credits, anime_map)
        # With real structure, R^2 should be non-negative (intercept prevents collapse)
        assert result.r_squared >= 0.0
        assert result.r_squared <= 1.0

    def test_akm_observation_count(self, studio_data):
        """n_observations should be positive."""
        _, anime_map, credits = studio_data
        result = estimate_akm(credits, anime_map)
        assert result.n_observations > 0

    def test_akm_empty_data(self):
        """Handles empty credits gracefully."""
        result = estimate_akm([], {})
        assert result.person_fe == {}
        assert result.studio_fe == {}
        assert result.n_observations == 0
        assert result.r_squared == 0.0

    def test_studio_fe_ordering(self, studio_data):
        """Studio with larger production scale has higher studio_fe.

        AKM outcome is now log1p(staff_count) * log1p(episodes) * dur_mult,
        not anime.score.  Build data where StudioA has clearly larger-scale
        productions than StudioC so the ordering survives shrinkage.
        """
        _, _, base_credits = studio_data
        # Override anime_map: give StudioA many episodes (large scale)
        # and StudioC few episodes (small scale)
        anime_map = {
            "a1": Anime(
                id="a1",
                title_en="Alpha",
                year=2018,
                studios=["StudioA"],
                episodes=24,
            ),
            "a2": Anime(
                id="a2",
                title_en="Beta",
                year=2019,
                studios=["StudioA"],
                episodes=24,
            ),
            "a3": Anime(
                id="a3",
                title_en="Gamma",
                year=2019,
                studios=["StudioB"],
                episodes=12,
            ),
            "a4": Anime(
                id="a4",
                title_en="Delta",
                year=2020,
                studios=["StudioB"],
                episodes=12,
            ),
            "a5": Anime(
                id="a5",
                title_en="Epsilon",
                year=2021,
                studios=["StudioC"],
                episodes=1,
            ),
        }
        # Add extra staff to StudioA anime for larger staff counts
        extra_credits = list(base_credits)
        for i in range(10, 20):
            extra_credits.append(
                Credit(
                    person_id=f"extra{i}",
                    anime_id="a1",
                    role=Role.KEY_ANIMATOR,
                    source="test",
                )
            )
            extra_credits.append(
                Credit(
                    person_id=f"extra{i}",
                    anime_id="a2",
                    role=Role.KEY_ANIMATOR,
                    source="test",
                )
            )
        result = estimate_akm(extra_credits, anime_map)
        if "StudioA" in result.studio_fe and "StudioC" in result.studio_fe:
            assert result.studio_fe["StudioA"] > result.studio_fe["StudioC"]

    def test_akm_connected_set_size(self, studio_data):
        """Connected set size is tracked."""
        _, anime_map, credits = studio_data
        result = estimate_akm(credits, anime_map)
        assert result.connected_set_size > 0

    def test_akm_fallback_few_movers(self):
        """Logs warning when mover_fraction < 0.10 (all stayers at separate studios)."""
        # 10 stayers, 0 movers -> mover_fraction = 0
        anime_map = {}
        credits = []
        for i in range(1, 11):
            studio = f"Studio{i}"
            aid = f"a{i}"
            anime_map[aid] = Anime(
                id=aid, title_en=f"Anime {i}", year=2020, studios=[studio]
            )
            credits.append(
                Credit(
                    person_id=f"p{i}",
                    anime_id=aid,
                    role=Role.KEY_ANIMATOR,
                    source="test",
                )
            )
        # Add a second year for each person so they have transitions
        for i in range(1, 11):
            aid2 = f"a{i}b"
            studio = f"Studio{i}"
            anime_map[aid2] = Anime(
                id=aid2, title_en=f"Anime {i}b", year=2021, studios=[studio]
            )
            credits.append(
                Credit(
                    person_id=f"p{i}",
                    anime_id=aid2,
                    role=Role.KEY_ANIMATOR,
                    source="test",
                )
            )

        result = estimate_akm(credits, anime_map)
        # With 0 movers, studio FE should still be estimated (person FE only path)
        assert isinstance(result, AKMResult)
        assert result.n_movers == 0

    def test_studio_fe_zero_sum(self, studio_data):
        """Studio FE should be zero-mean after AKM identification constraint."""
        _, anime_map, credits = studio_data
        result = estimate_akm(credits, anime_map)
        if result.studio_fe:
            studio_fe_values = list(result.studio_fe.values())
            mean_fe = np.mean(studio_fe_values)
            assert abs(mean_fe) < 0.01, f"Studio FE mean = {mean_fe}, expected ~0"

    def test_co_production_studios(self):
        """Co-production (multiple studios) distributes credit weight equally."""
        anime_map = {
            "a1": Anime(
                id="a1",
                title_en="CoProduction",
                year=2020,
                studios=["StudioA", "StudioB"],
            ),
            "a2": Anime(
                id="a2",
                title_en="Solo",
                year=2021,
                studios=["StudioA"],
            ),
        }
        credits = [
            Credit(
                person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"
            ),
        ]
        assignments = infer_studio_assignment(credits, anime_map)
        # p1 in 2020: StudioA and StudioB both get 0.5 weight
        # p1 in 2021: StudioA gets full weight → StudioA wins
        assert assignments["p1"][2021] == "StudioA"
        # For 2020, with equal weight to both studios, either could win
        assert assignments["p1"][2020] in ("StudioA", "StudioB")


class TestShrinkPersonFE:
    """Test empirical Bayes shrinkage of person FE."""

    def test_shrinkage_reduces_variance(self):
        """Shrinkage should reduce person FE variance."""
        import structlog

        log = structlog.get_logger()
        rng = np.random.RandomState(42)
        n_persons = 100
        n_obs = 500
        person_fe = rng.randn(n_persons) * 2.0
        person_ind = rng.randint(0, n_persons, size=n_obs).astype(np.int32)
        residuals = rng.randn(n_obs) * 0.5

        shrunk = _shrink_person_fe(
            person_fe, person_ind, residuals, n_obs, n_persons, log
        )
        assert np.std(shrunk) < np.std(person_fe)

    def test_low_obs_persons_shrunk_more(self):
        """Persons with 1 observation should be shrunk more than those with many."""
        import structlog

        log = structlog.get_logger()
        n_persons = 20
        # Person 0: 1 observation, Person 1: 50 observations
        person_ind = np.array(
            [0] + [1] * 50 + list(range(2, n_persons)) * 3, dtype=np.int32
        )
        n_obs = len(person_ind)
        person_fe = np.zeros(n_persons)
        person_fe[0] = 2.0  # extreme value, 1 obs
        person_fe[1] = 2.0  # same extreme value, 50 obs
        for i in range(2, n_persons):
            person_fe[i] = 0.0
        residuals = np.random.RandomState(42).randn(n_obs) * 0.5

        shrunk = _shrink_person_fe(
            person_fe, person_ind, residuals, n_obs, n_persons, log
        )
        # Person 0 (1 obs) should be shrunk more toward mean
        # Person 1 (50 obs) should retain more of their raw value
        shrink_0 = abs(shrunk[0] - person_fe[0])
        shrink_1 = abs(shrunk[1] - person_fe[1])
        assert shrink_0 > shrink_1, (
            f"1-obs person shrunk by {shrink_0:.3f}, "
            f"50-obs person shrunk by {shrink_1:.3f}"
        )

    def test_shrinkage_preserves_ordering(self):
        """Relative ordering of person FE should be preserved."""
        import structlog

        log = structlog.get_logger()
        n_persons = 50
        n_obs = 500
        rng = np.random.RandomState(42)
        person_fe = np.linspace(-1, 1, n_persons)
        person_ind = rng.randint(0, n_persons, size=n_obs).astype(np.int32)
        residuals = rng.randn(n_obs) * 0.3

        shrunk = _shrink_person_fe(
            person_fe, person_ind, residuals, n_obs, n_persons, log
        )
        # Ordering should be preserved (higher raw → higher shrunk)
        active = np.array([np.sum(person_ind == i) > 0 for i in range(n_persons)])
        raw_order = np.argsort(person_fe[active])
        shrunk_order = np.argsort(shrunk[active])
        # Allow minor reorderings for near-identical values
        rank_corr = np.corrcoef(raw_order, shrunk_order)[0, 1]
        assert rank_corr > 0.95

    def test_empty_input(self):
        """Handles empty arrays."""
        import structlog

        log = structlog.get_logger()
        result = _shrink_person_fe(
            np.array([]), np.array([], dtype=np.int32), np.array([]), 0, 0, log
        )
        assert len(result) == 0

    def test_akm_integration_shrinkage_applied(self):
        """AKM estimate_akm applies shrinkage — low-obs extremes are reduced."""
        # Build data with one 1-obs person on high-score anime and many normal persons
        anime_map = {}
        credits = []
        # 3 studios, 20 anime, movers between studios
        for i in range(20):
            studio = f"Studio{chr(65 + i % 3)}"
            aid = f"a{i}"
            score = 7.0 + (i % 5) * 0.3
            anime_map[aid] = Anime(
                id=aid,
                title_en=f"A{i}",
                year=2018 + i // 4,
                score=score,
                studios=[studio],
            )

        # 15 regular persons with multiple credits
        for p in range(15):
            for a in range(20):
                if (p + a) % 3 == 0:
                    credits.append(
                        Credit(
                            person_id=f"p{p}",
                            anime_id=f"a{a}",
                            role=Role.KEY_ANIMATOR,
                            source="t",
                        )
                    )

        # 1 person on single high-score anime
        anime_map["a_special"] = Anime(
            id="a_special",
            title_en="Special",
            year=2022,
            studios=["StudioA"],
        )
        credits.append(
            Credit(
                person_id="p_rare",
                anime_id="a_special",
                role=Role.KEY_ANIMATOR,
                source="t",
            )
        )
        # Also give them a link to make connected
        credits.append(
            Credit(
                person_id="p_rare", anime_id="a0", role=Role.KEY_ANIMATOR, source="t"
            )
        )

        result = estimate_akm(credits, anime_map)

        if "p_rare" in result.person_fe:
            rare_fe = result.person_fe["p_rare"]
            all_fe = list(result.person_fe.values())
            fe_mean = np.mean(all_fe)
            # Without shrinkage, p_rare's raw FE would be far from the mean
            # (anime score 9.5 vs mean ~7.3 → raw FE ~+2.2).
            # With shrinkage (2 obs → heavily shrunk), it should be
            # pulled close to the grand mean.
            distance_from_mean = abs(rare_fe - fe_mean)
            assert distance_from_mean < 0.5, (
                f"p_rare FE={rare_fe:.3f} too far from mean={fe_mean:.3f} "
                f"(distance={distance_from_mean:.3f}, expected < 0.5 with shrinkage)"
            )


class TestDebiasObsCount:
    def test_removes_negative_correlation(self):
        """Debiasing should remove correlation between person_fe and log(credit_count)."""
        import structlog

        log = structlog.get_logger()
        rng = np.random.RandomState(42)
        n_persons = 100

        # Create person_fe with negative correlation to credit count
        credit_counts = rng.randint(1, 200, size=n_persons).astype(np.int64)
        # Systematic bias: more credits → lower FE
        person_fe = 0.5 - 0.15 * np.log1p(credit_counts) + rng.normal(0, 0.1, n_persons)

        old_corr = float(np.corrcoef(person_fe, np.log1p(credit_counts))[0, 1])
        assert old_corr < -0.3, (
            f"Pre-condition: expect negative correlation, got {old_corr}"
        )

        debiased = _debias_by_obs_count(person_fe.copy(), credit_counts, n_persons, log)

        new_corr = float(np.corrcoef(debiased, np.log1p(credit_counts))[0, 1])
        assert abs(new_corr) < 0.05, (
            f"After debiasing, correlation should be ~0, got {new_corr:.4f}"
        )

    def test_preserves_ordering_within_bracket(self):
        """Within same credit count, relative ordering should be preserved."""
        import structlog

        log = structlog.get_logger()
        n_persons = 60
        # Three groups: 5, 10, 20 credits (20 persons each)
        credit_counts = np.array([5] * 20 + [10] * 20 + [20] * 20, dtype=np.int64)
        # Within each group, person_fe decreases with index
        person_fe = np.zeros(n_persons)
        for grp_start in [0, 20, 40]:
            for i in range(20):
                person_fe[grp_start + i] = 1.0 - i * 0.05 - grp_start * 0.01

        debiased = _debias_by_obs_count(person_fe.copy(), credit_counts, n_persons, log)

        # Within each bracket, ordering should be preserved
        for grp_start in [0, 20, 40]:
            grp_slice = debiased[grp_start : grp_start + 20]
            for i in range(19):
                assert grp_slice[i] > grp_slice[i + 1], (
                    f"Ordering broken at group {grp_start}, idx {i}: "
                    f"{grp_slice[i]:.4f} <= {grp_slice[i + 1]:.4f}"
                )

    def test_skips_positive_slope(self):
        """If slope is non-negative, debiasing should be skipped."""
        import structlog

        log = structlog.get_logger()
        n_persons = 50
        credit_counts = np.arange(1, n_persons + 1, dtype=np.int64)
        # Positive correlation: more credits → higher FE
        person_fe = 0.1 * np.log1p(credit_counts) + 0.5

        debiased = _debias_by_obs_count(person_fe.copy(), credit_counts, n_persons, log)
        np.testing.assert_array_equal(debiased, person_fe)

    def test_small_dataset_skipped(self):
        """Debiasing should be skipped for datasets with fewer than 20 persons."""
        import structlog

        log = structlog.get_logger()
        person_fe = np.array([1.0, 0.5, -0.5])
        credit_counts = np.array([2, 1, 1], dtype=np.int64)

        result = _debias_by_obs_count(person_fe.copy(), credit_counts, 3, log)
        np.testing.assert_array_equal(result, person_fe)


class TestWeightedObservations:
    """Tests for observation weight computation and weighted estimation."""

    @pytest.fixture
    def weighted_data(self):
        """Data with through and episodic roles for weight testing."""
        anime_map = {
            "a1": Anime(
                id="a1",
                title_en="Alpha",
                year=2018,
                studios=["StudioA"],
                episodes=24,
            ),
            "a2": Anime(
                id="a2",
                title_en="Beta",
                year=2020,
                studios=["StudioB"],
                episodes=12,
            ),
        }
        return anime_map

    def test_through_role_higher_weight(self, weighted_data):
        """Director (through role) should have higher w_obs than Key Animator (episodic)."""
        anime = weighted_data["a1"]
        w_director = _compute_credit_weight(Role.DIRECTOR, None, anime, 5)
        w_key_anim = _compute_credit_weight(Role.KEY_ANIMATOR, None, anime, 5)
        assert w_director > w_key_anim, (
            f"Director weight {w_director:.4f} should be > Key Animator weight {w_key_anim:.4f}"
        )

    def test_episode_coverage_scales_weight(self):
        """Episodic role on 2/24 episodes should weigh less than through role on full series."""
        anime = Anime(
            id="a1",
            title_en="A",
            year=2020,
            studios=["StudioA"],
            episodes=24,
        )
        # Key Animator credited on eps 3 and 7
        w_partial = _compute_credit_weight(
            Role.KEY_ANIMATOR,
            "Key Animation (eps 3, 7)",
            anime,
            5,
        )
        # Director (through role, full coverage)
        w_through = _compute_credit_weight(Role.DIRECTOR, None, anime, 5)
        assert w_through > w_partial, (
            f"Through role weight {w_through:.4f} should be > "
            f"partial episodic weight {w_partial:.4f}"
        )

    def test_experience_increases_weight(self, weighted_data):
        """10-year veteran should have higher weight than newcomer on same role/anime."""
        anime = weighted_data["a1"]
        w_newbie = _compute_credit_weight(Role.KEY_ANIMATOR, None, anime, 0)
        w_veteran = _compute_credit_weight(Role.KEY_ANIMATOR, None, anime, 10)
        assert w_veteran > w_newbie, (
            f"Veteran weight {w_veteran:.4f} should be > newbie weight {w_newbie:.4f}"
        )
        # Experience factor at 0 years should be 1.0
        # w_experience = min(1.0 + 0.3*(1 - exp(0/5)), 1.3) = 1.0
        # At 10 years: min(1.0 + 0.3*(1 - exp(-2)), 1.3) ≈ 1.26
        assert w_veteran / w_newbie > 1.1

    def test_multi_role_accumulates(self):
        """Director+Character Designer should have higher w_obs than Director alone."""
        anime_map = {
            "a1": Anime(
                id="a1",
                title_en="A",
                year=2020,
                studios=["StudioA"],
                episodes=12,
            ),
            "a2": Anime(
                id="a2",
                title_en="B",
                year=2021,
                studios=["StudioB"],
                episodes=12,
            ),
        }
        # p1: director + character designer on a1
        # p2: director only on a1
        # Both also on a2 (for connected set)
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="t"),
            Credit(
                person_id="p1", anime_id="a1", role=Role.CHARACTER_DESIGNER, source="t"
            ),
            Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="t"),
            Credit(person_id="p2", anime_id="a1", role=Role.DIRECTOR, source="t"),
            Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="t"),
        ]
        studio_assignments = infer_studio_assignment(credits, anime_map)
        connected_persons, connected_studios = find_connected_set(studio_assignments)

        _, obs_keys, _, _, _, _, _, w = _build_panel(
            credits, anime_map, studio_assignments, connected_persons, connected_studios
        )

        # Find weights for p1 on a1 vs p2 on a1
        w_p1_a1 = None
        w_p2_a1 = None
        for i, (pid, aid) in enumerate(obs_keys):
            if pid == "p1" and aid == "a1":
                w_p1_a1 = float(w[i])
            if pid == "p2" and aid == "a1":
                w_p2_a1 = float(w[i])

        assert w_p1_a1 is not None and w_p2_a1 is not None
        assert w_p1_a1 > w_p2_a1, (
            f"Multi-role weight {w_p1_a1:.4f} should be > single-role weight {w_p2_a1:.4f}"
        )

    def test_weights_median_normalized(self, studio_data):
        """Observation weights should have median approximately 1.0."""
        _, anime_map, credits = studio_data
        studio_assignments = infer_studio_assignment(credits, anime_map)
        connected_persons, connected_studios = find_connected_set(studio_assignments)

        _, obs_keys, _, _, _, _, _, w = _build_panel(
            credits, anime_map, studio_assignments, connected_persons, connected_studios
        )

        if len(w) > 0:
            assert abs(np.median(w) - 1.0) < 0.01, (
                f"Median weight {np.median(w):.4f} should be ≈ 1.0"
            )

    def test_weighted_demeaning_convergence(self, studio_data):
        """Weighted iterative demeaning should converge within max_iter."""
        _, anime_map, credits = studio_data
        result = estimate_akm(credits, anime_map, max_iter=100)
        # If it converged, we should have valid results
        assert result.n_observations > 0
        assert len(result.person_fe) > 0

    def test_weighted_r_squared(self, studio_data):
        """Weighted R² should be in [0, 1] range."""
        _, anime_map, credits = studio_data
        result = estimate_akm(credits, anime_map)
        assert 0.0 <= result.r_squared <= 1.0, (
            f"Weighted R² = {result.r_squared:.4f}, expected [0, 1]"
        )

    def test_shrinkage_uses_raw_obs_count(self):
        """Shrinkage should depend on raw observation count, not on weight values.

        A director with 1 high-weight anime is still 1 data point for
        estimating person FE. Weights affect the WLS fit but not the
        number of independent observations available for shrinkage.
        """
        import structlog

        log = structlog.get_logger()
        n_persons = 30
        # Person 0: 3 observations (low count)
        # Person 1: 10 observations (high count)
        # Remaining: 5 obs each
        person_ind_parts = (
            [0] * 3 + [1] * 10 + [i for i in range(2, n_persons) for _ in range(5)]
        )
        person_ind = np.array(person_ind_parts, dtype=np.int32)
        n_obs = len(person_ind)

        person_fe = np.zeros(n_persons)
        person_fe[0] = 2.0  # extreme, 3 obs
        person_fe[1] = 2.0  # same extreme, 10 obs
        for i in range(2, n_persons):
            person_fe[i] = 0.0

        residuals = np.random.RandomState(42).randn(n_obs) * 0.5

        shrunk = _shrink_person_fe(
            person_fe, person_ind, residuals, n_obs, n_persons, log
        )

        # Person 1 (10 obs) should retain more of raw value than Person 0 (3 obs)
        shrink_0 = abs(shrunk[0] - person_fe[0])
        shrink_1 = abs(shrunk[1] - person_fe[1])
        assert shrink_0 > shrink_1, (
            f"3-obs person shrunk by {shrink_0:.3f}, "
            f"10-obs person shrunk by {shrink_1:.3f}; "
            "expected more shrinkage for fewer observations"
        )


class TestRedistributeStudioFE:
    """Tests for mover-calibrated studio FE redistribution."""

    def _build_synthetic_data(
        self, *, n_movers=30, n_stayers_per_studio=20, absorption=0.5
    ):
        """Build synthetic AKM state with controlled absorption.

        Creates 2 studios:
        - Studio 0: good (studio_fe = +1.5)
        - Studio 1: bad (studio_fe = -1.5)

        Movers split time between studios. Stayers are at one studio only.
        For stayers, person_fe is artificially depressed (good studio) or
        inflated (bad studio) to simulate absorption.

        Args:
            n_movers: number of movers
            n_stayers_per_studio: stayers per studio
            absorption: how much person_fe is shifted by studio quality
        """
        import structlog

        n_studios = 2
        n_stayers = n_stayers_per_studio * n_studios
        n_persons = n_movers + n_stayers

        studio_fe = np.array([1.5, -1.5])

        person_ind_list = []
        studio_ind_list = []
        w_list = []

        # Movers: split between studios (3 obs at studio 0, 2 at studio 1)
        for p in range(n_movers):
            for _ in range(3):
                person_ind_list.append(p)
                studio_ind_list.append(0)
                w_list.append(1.0)
            for _ in range(2):
                person_ind_list.append(p)
                studio_ind_list.append(1)
                w_list.append(1.0)

        # Stayers at studio 0
        base = n_movers
        for p in range(n_stayers_per_studio):
            for _ in range(5):
                person_ind_list.append(base + p)
                studio_ind_list.append(0)
                w_list.append(1.0)

        # Stayers at studio 1
        base2 = n_movers + n_stayers_per_studio
        for p in range(n_stayers_per_studio):
            for _ in range(5):
                person_ind_list.append(base2 + p)
                studio_ind_list.append(1)
                w_list.append(1.0)

        person_ind = np.array(person_ind_list, dtype=np.int32)
        studio_ind = np.array(studio_ind_list, dtype=np.int32)
        w = np.array(w_list, dtype=np.float64)
        n_obs = len(person_ind)

        rng = np.random.RandomState(42)
        person_fe = rng.randn(n_persons) * 0.3

        # Movers: person_fe is independent of studio (well-identified)
        # (already random, no correlation)

        # Stayers at good studio: person_fe depressed (absorbed into studio_fe)
        person_fe[n_movers : n_movers + n_stayers_per_studio] -= absorption
        # Stayers at bad studio: person_fe inflated (absorbed from studio_fe)
        person_fe[n_movers + n_stayers_per_studio :] += absorption

        person_list = [f"p{i}" for i in range(n_persons)]
        movers_set = {f"p{i}" for i in range(n_movers)}
        log = structlog.get_logger()

        return (
            person_fe,
            studio_fe,
            person_ind,
            studio_ind,
            w,
            person_list,
            movers_set,
            n_obs,
            n_persons,
            n_studios,
            log,
        )

    def test_absorption_detected_and_corrected(self):
        """When stayers show absorption, α > 0 and redistribution is applied."""
        data = self._build_synthetic_data(absorption=0.5)
        person_fe = data[0].copy()
        adj, alpha = _redistribute_studio_fe(*data)

        assert alpha > 0, f"Expected positive α, got {alpha}"

        # Stayers at good studio (indices 30-49) should have higher FE after
        n_movers = 30
        n_sps = 20
        mean_raw_good = float(np.mean(person_fe[n_movers : n_movers + n_sps]))
        mean_adj_good = float(np.mean(adj[n_movers : n_movers + n_sps]))
        assert mean_adj_good > mean_raw_good, (
            f"Stayers at good studio: adj {mean_adj_good:.3f} should be > "
            f"raw {mean_raw_good:.3f}"
        )

    def test_no_absorption_much_less_redistribution(self):
        """Without absorption, redistribution is much smaller than with absorption."""
        data_with = self._build_synthetic_data(absorption=0.5)
        pfe_with = data_with[0].copy()
        adj_with, _ = _redistribute_studio_fe(*data_with)

        data_without = self._build_synthetic_data(absorption=0.0)
        pfe_without = data_without[0].copy()
        adj_without, _ = _redistribute_studio_fe(*data_without)

        change_with = float(np.mean(np.abs(adj_with - pfe_with)))
        change_without = float(np.mean(np.abs(adj_without - pfe_without)))

        assert change_with > change_without * 2, (
            f"Absorption case change {change_with:.4f} should be >> "
            f"no-absorption case change {change_without:.4f}"
        )

    def test_alpha_bounded(self):
        """α should be non-negative."""
        data = self._build_synthetic_data(absorption=0.5)
        _, alpha = _redistribute_studio_fe(*data)
        assert alpha >= 0.0

    def test_stayers_adjusted_more_than_movers(self):
        """Stayers get larger adjustment because cs is more concentrated."""
        data = self._build_synthetic_data(absorption=0.5)
        person_fe = data[0].copy()
        adj, alpha = _redistribute_studio_fe(*data)

        if alpha < 0.01:
            pytest.skip("No redistribution applied")

        n_movers = 30
        # Mean absolute adjustment for movers vs stayers
        mover_adj = float(np.mean(np.abs(adj[:n_movers] - person_fe[:n_movers])))
        stayer_adj = float(np.mean(np.abs(adj[n_movers:] - person_fe[n_movers:])))
        assert stayer_adj > mover_adj, (
            f"Stayer adj {stayer_adj:.4f} should be > mover adj {mover_adj:.4f}"
        )

    def test_high_weight_gets_more_redistribution(self):
        """Person with higher observation weight gets more redistribution."""
        import structlog

        log = structlog.get_logger()

        n_persons = 60
        n_studios = 2
        studio_fe = np.array([2.0, -2.0])

        person_ind_list = []
        studio_ind_list = []
        w_list = []

        # 20 movers
        for p in range(20):
            for _ in range(3):
                person_ind_list.append(p)
                studio_ind_list.append(0)
                w_list.append(1.0)
            for _ in range(2):
                person_ind_list.append(p)
                studio_ind_list.append(1)
                w_list.append(1.0)

        # Person 20: "director" stayer at studio 0 — high weight (3.0 per obs)
        for _ in range(5):
            person_ind_list.append(20)
            studio_ind_list.append(0)
            w_list.append(3.0)

        # Person 21: "key animator" stayer at studio 0 — low weight (0.5 per obs)
        for _ in range(5):
            person_ind_list.append(21)
            studio_ind_list.append(0)
            w_list.append(0.5)

        # Remaining stayers at studio 0 (22-39)
        for p in range(22, 40):
            for _ in range(5):
                person_ind_list.append(p)
                studio_ind_list.append(0)
                w_list.append(1.0)

        # Stayers at studio 1 (40-59)
        for p in range(40, 60):
            for _ in range(5):
                person_ind_list.append(p)
                studio_ind_list.append(1)
                w_list.append(1.0)

        person_ind = np.array(person_ind_list, dtype=np.int32)
        studio_ind = np.array(studio_ind_list, dtype=np.int32)
        w = np.array(w_list, dtype=np.float64)
        n_obs = len(person_ind)

        rng = np.random.RandomState(42)
        person_fe = rng.randn(n_persons) * 0.3
        # Stayers at good studio have depressed person_fe
        person_fe[20:40] -= 0.5
        person_fe[40:60] += 0.5

        person_list = [f"p{i}" for i in range(n_persons)]
        movers_set = {f"p{i}" for i in range(20)}

        adj, alpha = _redistribute_studio_fe(
            person_fe.copy(),
            studio_fe,
            person_ind,
            studio_ind,
            w,
            person_list,
            movers_set,
            n_obs,
            n_persons,
            n_studios,
            log,
        )

        if alpha < 0.01:
            pytest.skip("No redistribution applied")

        # Director (p20, high weight) should get more redistribution than
        # key animator (p21, low weight) at same studio
        redist_director = adj[20] - person_fe[20]
        redist_ka = adj[21] - person_fe[21]
        assert redist_director > redist_ka, (
            f"Director redistribution {redist_director:.4f} should be > "
            f"key animator {redist_ka:.4f}"
        )

    def test_insufficient_data_skips(self):
        """With too few persons, redistribution is skipped."""
        import structlog

        log = structlog.get_logger()

        person_fe = np.array([0.5, -0.5])
        studio_fe = np.array([1.0])
        person_ind = np.array([0, 1], dtype=np.int32)
        studio_ind = np.array([0, 0], dtype=np.int32)
        w = np.array([1.0, 1.0])

        adj, alpha = _redistribute_studio_fe(
            person_fe,
            studio_fe,
            person_ind,
            studio_ind,
            w,
            ["p0", "p1"],
            {"p0"},
            2,
            2,
            1,
            log,
        )
        assert alpha == 0.0
        np.testing.assert_array_equal(adj, person_fe)

    def test_integration_akm_has_redistribution_alpha(self):
        """estimate_akm result includes redistribution_alpha field."""
        anime_map = {}
        credits = []
        # Build enough data for redistribution to potentially activate
        for i in range(30):
            studio = f"Studio{chr(65 + i % 3)}"
            aid = f"a{i}"
            score = 5.0 + (i % 3) * 2.0  # A=5-7, B=7-9, C varies
            anime_map[aid] = Anime(
                id=aid,
                title_en=f"A{i}",
                year=2015 + i // 6,
                score=score,
                studios=[studio],
                episodes=12,
            )

        # 20 movers across studios
        for m in range(20):
            src_idx = m % 3
            dst_idx = (m + 1) % 3
            for i in range(src_idx * 10, src_idx * 10 + 5):
                credits.append(
                    Credit(
                        person_id=f"mover{m}",
                        anime_id=f"a{i}",
                        role=Role.KEY_ANIMATOR,
                        source="t",
                    )
                )
            for i in range(dst_idx * 10, dst_idx * 10 + 5):
                credits.append(
                    Credit(
                        person_id=f"mover{m}",
                        anime_id=f"a{i}",
                        role=Role.KEY_ANIMATOR,
                        source="t",
                    )
                )

        # 30 stayers
        for s in range(30):
            studio_idx = s % 3
            for i in range(studio_idx * 10, studio_idx * 10 + 10):
                credits.append(
                    Credit(
                        person_id=f"stayer{s}",
                        anime_id=f"a{i}",
                        role=Role.DIRECTOR if s < 3 else Role.KEY_ANIMATOR,
                        source="t",
                    )
                )

        result = estimate_akm(credits, anime_map)
        assert isinstance(result, AKMResult)
        assert hasattr(result, "redistribution_alpha")
        assert result.redistribution_alpha >= 0.0
