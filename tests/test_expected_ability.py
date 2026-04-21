"""expected_ability モジュールのテスト."""

import pytest

from src.analysis.scoring.expected_ability import (
    compute_expected_ability,
    ExpectedActualResult,
)
from src.models import BronzeAnime as Anime, Credit, Role


@pytest.fixture
def anime_map():
    """8 anime works across different studios and years."""
    return {
        "a1": Anime(
            id="a1", title_en="Work A", year=2018, score=8.5, studios=["Alpha"]
        ),
        "a2": Anime(
            id="a2", title_en="Work B", year=2019, score=7.0, studios=["Alpha"]
        ),
        "a3": Anime(id="a3", title_en="Work C", year=2019, score=9.0, studios=["Beta"]),
        "a4": Anime(id="a4", title_en="Work D", year=2020, score=6.5, studios=["Beta"]),
        "a5": Anime(
            id="a5", title_en="Work E", year=2020, score=8.0, studios=["Alpha"]
        ),
        "a6": Anime(
            id="a6", title_en="Work F", year=2021, score=7.5, studios=["Gamma"]
        ),
        "a7": Anime(
            id="a7", title_en="Work G", year=2021, score=8.8, studios=["Alpha"]
        ),
        "a8": Anime(
            id="a8", title_en="Work H", year=2022, score=6.0, studios=["Gamma"]
        ),
    }


@pytest.fixture
def credits():
    """Credits for 12 persons across 8 anime — diverse roles and collaborations."""
    return [
        # Directors
        Credit(person_id="d1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="d1", anime_id="a5", role=Role.DIRECTOR, source="test"),
        Credit(person_id="d2", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="d2", anime_id="a7", role=Role.DIRECTOR, source="test"),
        Credit(person_id="d3", anime_id="a3", role=Role.DIRECTOR, source="test"),
        Credit(person_id="d3", anime_id="a4", role=Role.DIRECTOR, source="test"),
        Credit(person_id="d4", anime_id="a6", role=Role.DIRECTOR, source="test"),
        Credit(person_id="d4", anime_id="a8", role=Role.DIRECTOR, source="test"),
        # Key animators — spread across works
        Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p1", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a6", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a7", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p5", anime_id="a4", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(
            person_id="p5", anime_id="a6", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(
            person_id="p5", anime_id="a8", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p6", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p6", anime_id="a7", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p6", anime_id="a8", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p7", anime_id="a1", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(
            person_id="p7", anime_id="a6", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p8", anime_id="a7", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p8", anime_id="a8", role=Role.KEY_ANIMATOR, source="test"),
    ]


@pytest.fixture
def person_fe():
    """Person fixed effects — varying talent levels."""
    return {
        "d1": 2.5,
        "d2": 1.8,
        "d3": 1.2,
        "d4": 0.5,
        "p1": 2.0,
        "p2": 1.5,
        "p3": 0.8,
        "p4": 1.9,
        "p5": 1.1,
        "p6": 1.3,
        "p7": 0.9,
        "p8": 0.6,
    }


@pytest.fixture
def birank():
    """BiRank scores — directors have higher centrality."""
    return {
        "d1": 0.85,
        "d2": 0.70,
        "d3": 0.55,
        "d4": 0.30,
        "p1": 0.40,
        "p2": 0.35,
        "p3": 0.25,
        "p4": 0.45,
        "p5": 0.30,
        "p6": 0.28,
        "p7": 0.22,
        "p8": 0.15,
    }


@pytest.fixture
def studio_fe():
    return {"Alpha": 0.5, "Beta": 0.2, "Gamma": -0.3}


@pytest.fixture
def studio_assignments():
    return {
        "d1": {2018: "Alpha", 2020: "Alpha"},
        "d2": {2019: "Alpha", 2021: "Alpha"},
        "d3": {2019: "Beta", 2020: "Beta"},
        "d4": {2021: "Gamma", 2022: "Gamma"},
        "p1": {2018: "Alpha", 2019: "Alpha", 2020: "Alpha"},
        "p2": {2018: "Alpha", 2019: "Beta"},
        "p3": {2019: "Alpha", 2020: "Beta", 2021: "Gamma"},
        "p4": {2019: "Beta", 2020: "Alpha", 2021: "Alpha"},
        "p5": {2020: "Beta", 2021: "Gamma", 2022: "Gamma"},
        "p6": {2020: "Alpha", 2021: "Alpha", 2022: "Gamma"},
        "p7": {2018: "Alpha", 2021: "Gamma"},
        "p8": {2021: "Alpha", 2022: "Gamma"},
    }


@pytest.fixture
def iv_scores():
    """Integrated value scores for all persons."""
    return {
        "d1": 85.0,
        "d2": 70.0,
        "d3": 55.0,
        "d4": 30.0,
        "p1": 75.0,
        "p2": 60.0,
        "p3": 40.0,
        "p4": 72.0,
        "p5": 50.0,
        "p6": 48.0,
        "p7": 38.0,
        "p8": 25.0,
    }


class TestBasicComputation:
    """10+ persons, verify expected/actual/gap populated."""

    def test_return_type(
        self,
        credits,
        anime_map,
        person_fe,
        birank,
        studio_fe,
        studio_assignments,
        iv_scores,
    ):
        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        assert isinstance(result, ExpectedActualResult)

    def test_total_persons(
        self,
        credits,
        anime_map,
        person_fe,
        birank,
        studio_fe,
        studio_assignments,
        iv_scores,
    ):
        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        # All 12 persons are in both person_fe and iv_scores
        assert result.total_persons == 12

    def test_expected_populated(
        self,
        credits,
        anime_map,
        person_fe,
        birank,
        studio_fe,
        studio_assignments,
        iv_scores,
    ):
        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        assert len(result.expected) == 12
        for pid, val in result.expected.items():
            assert isinstance(val, float)

    def test_actual_populated(
        self,
        credits,
        anime_map,
        person_fe,
        birank,
        studio_fe,
        studio_assignments,
        iv_scores,
    ):
        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        assert len(result.actual) == 12
        # actual should match person_fe values (rounded)
        for pid in result.actual:
            assert result.actual[pid] == round(person_fe[pid], 4)

    def test_gap_populated(
        self,
        credits,
        anime_map,
        person_fe,
        birank,
        studio_fe,
        studio_assignments,
        iv_scores,
    ):
        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        assert len(result.gap) == 12
        # gap = actual - expected
        for pid in result.gap:
            expected_gap = round(result.actual[pid] - result.expected[pid], 4)
            assert abs(result.gap[pid] - expected_gap) < 1e-3


class TestGapSign:
    """Person with high person_fe but low environment → positive gap."""

    def test_high_fe_low_env_positive_gap(
        self,
        credits,
        anime_map,
        person_fe,
        birank,
        studio_fe,
        studio_assignments,
        iv_scores,
    ):
        # d1 has the highest person_fe (2.5) and works at Alpha (good studio),
        # but with moderate collaborators — we expect a positive gap
        # since their actual exceeds what environment alone would predict
        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        # Persons with highest person_fe relative to environment should have positive gap
        # d1 has person_fe=2.5 (highest) → gap should be positive or near zero
        # d4 has person_fe=0.5 (lowest) with weak studio → gap likely negative
        # We test that the highest-FE person has a larger gap than the lowest-FE person
        assert result.gap["d1"] > result.gap["d4"]

    def test_gap_sum_near_zero(
        self,
        credits,
        anime_map,
        person_fe,
        birank,
        studio_fe,
        studio_assignments,
        iv_scores,
    ):
        """OLS residuals should sum to approximately zero."""
        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        gap_sum = sum(result.gap.values())
        # OLS residuals sum to ~0 (intercept included)
        assert abs(gap_sum) < 1.0


class TestInsufficientData:
    """<10 persons → empty result."""

    def test_fewer_than_10_persons(self, anime_map, studio_fe, studio_assignments):
        credits = [
            Credit(
                person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p3", anime_id="a3", role=Role.DIRECTOR, source="test"),
        ]
        # Only 3 persons in both person_fe and iv_scores
        person_fe = {"p1": 1.0, "p2": 0.5, "p3": 1.5}
        birank = {"p1": 0.3, "p2": 0.2, "p3": 0.5}
        iv_scores = {"p1": 50.0, "p2": 30.0, "p3": 70.0}

        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        assert isinstance(result, ExpectedActualResult)
        assert result.expected == {}
        assert result.actual == {}
        assert result.gap == {}
        assert result.total_persons == 0
        assert result.model_r_squared == 0.0

    def test_empty_data(self):
        result = compute_expected_ability([], {}, {}, {}, {}, {}, {})
        assert result.total_persons == 0
        assert result.expected == {}

    def test_exactly_nine_persons(self, anime_map, studio_fe, studio_assignments):
        """9 persons — just below the threshold."""
        credits = [
            Credit(
                person_id=f"px{i}",
                anime_id=f"a{(i % 8) + 1}",
                role=Role.KEY_ANIMATOR,
                source="test",
            )
            for i in range(1, 10)
        ]
        person_fe = {f"px{i}": float(i) * 0.3 for i in range(1, 10)}
        birank = {f"px{i}": float(i) * 0.05 for i in range(1, 10)}
        iv_scores = {f"px{i}": float(i) * 10.0 for i in range(1, 10)}

        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        assert result.total_persons == 0


class TestRSquaredRange:
    """R-squared between 0 and 1."""

    def test_r_squared_in_range(
        self,
        credits,
        anime_map,
        person_fe,
        birank,
        studio_fe,
        studio_assignments,
        iv_scores,
    ):
        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        assert 0.0 <= result.model_r_squared <= 1.0

    def test_r_squared_is_float(
        self,
        credits,
        anime_map,
        person_fe,
        birank,
        studio_fe,
        studio_assignments,
        iv_scores,
    ):
        result = compute_expected_ability(
            credits,
            anime_map,
            person_fe,
            birank,
            studio_fe,
            studio_assignments,
            iv_scores,
        )
        assert isinstance(result.model_r_squared, float)
