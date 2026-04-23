"""Tests for career friction estimation."""

import pytest

from src.analysis.career_friction import CareerFrictionResult, estimate_career_friction
from src.models import BronzeAnime as Anime, Credit, Role


@pytest.fixture
def career_data():
    """Persons showing different career progressions.

    We need enough persons (>= 8) to make quartile-based friction meaningful.
    Persons are grouped into 4 quartiles by score.  Within each quartile the
    expected upgrade rate is the average of that quartile, so friction depends
    on the difference between a person's actual rate and their quartile's rate.

    Quartile 0 (low score): p7, p8 -- stayers (0 upgrades)
    Quartile 1:             p5, p6 -- stayers (0 upgrades)
    Quartile 2:             p3, p4 -- p3 stayer, p4 stayer
    Quartile 3 (high score): p1, p2 -- p1 upgrader, p2 stayer

    p1 (score=90) upgrades: in_between -> KA -> AD  (2/3 transitions are upgrades)
    p2 (score=85) stays at KA throughout             (0/3 transitions are upgrades)
    Both are in the same quartile (Q3), so expected rate = avg of both.
    p1's actual rate > expected -> friction(p1) < friction(p2)
    """
    anime_map = {
        "a1": Anime(id="a1", title_en="Early", year=2015, studios=["S1"]),
        "a2": Anime(id="a2", title_en="Mid", year=2017, studios=["S1"]),
        "a3": Anime(id="a3", title_en="Late", year=2019, studios=["S1"]),
        "a4": Anime(id="a4", title_en="Latest", year=2021, studios=["S1"]),
    }

    credits = [
        # p1 (Q3): in_between -> key_animator -> animation_director -> AD (2 upgrades / 3 trans)
        Credit(person_id="p1", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p1", anime_id="a3", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(
            person_id="p1", anime_id="a4", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        # p2 (Q3): key_animator throughout (0 upgrades / 3 transitions)
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        # p3 (Q2): AD -> AD -> KA -> KA (0 upgrades, 1 downgrade)
        Credit(
            person_id="p3", anime_id="a1", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(
            person_id="p3", anime_id="a2", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p3", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        # p4 (Q2): stayer at KA
        Credit(person_id="p4", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        # p5 (Q1): stayer at in_between
        Credit(person_id="p5", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p5", anime_id="a2", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p5", anime_id="a3", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p5", anime_id="a4", role=Role.IN_BETWEEN, source="test"),
        # p6 (Q1): stayer at in_between
        Credit(person_id="p6", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p6", anime_id="a2", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p6", anime_id="a3", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p6", anime_id="a4", role=Role.IN_BETWEEN, source="test"),
        # p7 (Q0): stayer at in_between
        Credit(person_id="p7", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p7", anime_id="a2", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p7", anime_id="a3", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p7", anime_id="a4", role=Role.IN_BETWEEN, source="test"),
        # p8 (Q0): stayer at in_between
        Credit(person_id="p8", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p8", anime_id="a2", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p8", anime_id="a3", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p8", anime_id="a4", role=Role.IN_BETWEEN, source="test"),
    ]

    # Scores determine quartile: Q0=[p7,p8], Q1=[p5,p6], Q2=[p3,p4], Q3=[p1,p2]
    person_scores = {
        "p1": 90.0,
        "p2": 85.0,
        "p3": 70.0,
        "p4": 65.0,
        "p5": 50.0,
        "p6": 45.0,
        "p7": 30.0,
        "p8": 25.0,
    }

    return anime_map, credits, person_scores


class TestCareerFriction:
    def test_friction_computation(self, career_data):
        """Produces friction_index for each person with transitions."""
        anime_map, credits, person_scores = career_data
        result = estimate_career_friction(credits, anime_map, person_scores)
        assert isinstance(result, CareerFrictionResult)
        assert len(result.friction_index) > 0
        # All friction values should be in [0, 1]
        for pid, friction in result.friction_index.items():
            assert 0.0 <= friction <= 1.0, (
                f"{pid} has friction {friction} outside [0, 1]"
            )

    def test_upgrade_reduces_friction(self, career_data):
        """Person who upgrades (p1) has lower friction than stayer (p2)."""
        anime_map, credits, person_scores = career_data
        result = estimate_career_friction(credits, anime_map, person_scores)
        # p1 has 2 upgrades out of 3 transitions, p2 has 0 upgrades out of 3
        # p1 should have lower friction than p2
        assert result.friction_index["p1"] < result.friction_index["p2"]

    def test_transition_matrix(self, career_data):
        """Correct stage transitions counted in the matrix."""
        anime_map, credits, person_scores = career_data
        result = estimate_career_friction(credits, anime_map, person_scores)
        assert len(result.transition_matrix) > 0
        # From stage 1 (in_between) -> stage 3 (key_animator) should appear
        # (p1's first transition)
        assert 1 in result.transition_matrix
        assert 3 in result.transition_matrix[1]
        assert result.transition_matrix[1][3] >= 1

    def test_era_friction(self, career_data):
        """Era friction computed per decade."""
        anime_map, credits, person_scores = career_data
        result = estimate_career_friction(credits, anime_map, person_scores)
        # Data spans 2015-2021 -> decade 2010 and 2020
        assert len(result.era_friction) > 0
        for decade, val in result.era_friction.items():
            assert 0.0 <= val <= 1.0

    def test_empty_credits(self):
        """Handles empty data gracefully."""
        result = estimate_career_friction([], {})
        assert result.friction_index == {}
        assert result.transition_matrix == {}
        assert result.role_friction == {}
        assert result.era_friction == {}
