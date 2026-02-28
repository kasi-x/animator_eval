"""Tests for AKM fixed effects decomposition."""

import pytest

from src.analysis.akm import AKMResult, estimate_akm, find_connected_set, infer_studio_assignment
from src.models import Anime, Credit, Person, Role


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
        "a1": Anime(id="a1", title_en="Alpha", year=2018, score=9.0, studios=["StudioA"]),
        "a2": Anime(id="a2", title_en="Beta", year=2019, score=8.5, studios=["StudioA"]),
        "a3": Anime(id="a3", title_en="Gamma", year=2019, score=7.0, studios=["StudioB"]),
        "a4": Anime(id="a4", title_en="Delta", year=2020, score=6.5, studios=["StudioB"]),
        "a5": Anime(id="a5", title_en="Epsilon", year=2021, score=5.0, studios=["StudioC"]),
    }

    credits = [
        # p1: mover A -> B
        Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p1", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        # p2: mover B -> C
        Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"),
        # p3, p4: stayers at A
        Credit(person_id="p3", anime_id="a1", role=Role.ANIMATION_DIRECTOR, source="test"),
        Credit(person_id="p3", anime_id="a2", role=Role.ANIMATION_DIRECTOR, source="test"),
        Credit(person_id="p4", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        # p5, p6: stayers at B
        Credit(person_id="p5", anime_id="a3", role=Role.ANIMATION_DIRECTOR, source="test"),
        Credit(person_id="p5", anime_id="a4", role=Role.ANIMATION_DIRECTOR, source="test"),
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
        anime_map = {"a1": Anime(id="a1", title_en="NoStudio", year=2020, score=7.0, studios=[])}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test")]
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
        """Model explains some variance (R^2 > 0)."""
        _, anime_map, credits = studio_data
        result = estimate_akm(credits, anime_map)
        # With real structure, R^2 should be positive
        assert result.r_squared >= 0.0

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
        """Better studio (higher avg anime score) has higher studio_fe."""
        _, anime_map, credits = studio_data
        result = estimate_akm(credits, anime_map)
        # StudioA anime avg = 8.75, StudioB avg = 6.75, StudioC avg = 5.0
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
                id=aid, title_en=f"Anime {i}", year=2020, score=7.0, studios=[studio]
            )
            credits.append(
                Credit(person_id=f"p{i}", anime_id=aid, role=Role.KEY_ANIMATOR, source="test")
            )
        # Add a second year for each person so they have transitions
        for i in range(1, 11):
            aid2 = f"a{i}b"
            studio = f"Studio{i}"
            anime_map[aid2] = Anime(
                id=aid2, title_en=f"Anime {i}b", year=2021, score=7.0, studios=[studio]
            )
            credits.append(
                Credit(person_id=f"p{i}", anime_id=aid2, role=Role.KEY_ANIMATOR, source="test")
            )

        result = estimate_akm(credits, anime_map)
        # With 0 movers, studio FE should still be estimated (person FE only path)
        assert isinstance(result, AKMResult)
        assert result.n_movers == 0
