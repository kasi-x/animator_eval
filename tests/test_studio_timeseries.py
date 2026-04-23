"""studio_timeseries モジュールのテスト."""

import pytest

from src.analysis.studio.timeseries import (
    compute_studio_timeseries,
    StudioTimeSeriesResult,
)
from src.runtime.models import BronzeAnime as Anime, Credit, Role


@pytest.fixture
def anime_map():
    return {
        # score= is display metadata (not used in scoring formulas);
        # kept to test avg_anime_score display output.
        "a1": Anime(
            id="a1", title_en="Show A", year=2020, studios=["Bones"], score=8.0
        ),
        "a2": Anime(
            id="a2", title_en="Show B", year=2021, studios=["Bones"], score=7.5
        ),
        "a3": Anime(
            id="a3", title_en="Show C", year=2022, studios=["Bones"], score=9.0
        ),
        "a4": Anime(
            id="a4", title_en="Show D", year=2020, studios=["MAPPA"]
        ),
        "a5": Anime(
            id="a5", title_en="Show E", year=2021, studios=["MAPPA"]
        ),
        "a6": Anime(
            id="a6", title_en="Show F", year=2022, studios=["MAPPA"]
        ),
    }


@pytest.fixture
def credits():
    return [
        # Bones staff 2020
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p3", anime_id="a1", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        # Bones staff 2021 (p1, p2 retained; p4 new)
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p4", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        # Bones staff 2022 (p1, p4 retained; p5 new)
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p4", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p5", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        # MAPPA staff 2020
        Credit(person_id="p6", anime_id="a4", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p7", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        # MAPPA staff 2021
        Credit(person_id="p6", anime_id="a5", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p8", anime_id="a5", role=Role.KEY_ANIMATOR, source="test"),
        # MAPPA staff 2022
        Credit(person_id="p9", anime_id="a6", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p10", anime_id="a6", role=Role.KEY_ANIMATOR, source="test"),
    ]


@pytest.fixture
def iv_scores():
    return {f"p{i}": float(i * 10) for i in range(1, 11)}


@pytest.fixture
def studio_assignments():
    return {
        "p1": {2020: "Bones", 2021: "Bones", 2022: "Bones"},
        "p2": {2020: "Bones", 2021: "Bones"},
        "p3": {2020: "Bones"},
        "p4": {2021: "Bones", 2022: "Bones"},
        "p5": {2022: "Bones"},
        "p6": {2020: "MAPPA", 2021: "MAPPA"},
        "p7": {2020: "MAPPA"},
        "p8": {2021: "MAPPA"},
        "p9": {2022: "MAPPA"},
        "p10": {2022: "MAPPA"},
    }


class TestBasicMetrics:
    """2 studios, 3 years — verify staff_count, avg_anime_score, retention."""

    def test_return_type(self, credits, anime_map, iv_scores, studio_assignments):
        result = compute_studio_timeseries(
            credits, anime_map, iv_scores, studio_assignments
        )
        assert isinstance(result, StudioTimeSeriesResult)

    def test_studios_analyzed(self, credits, anime_map, iv_scores, studio_assignments):
        result = compute_studio_timeseries(
            credits, anime_map, iv_scores, studio_assignments
        )
        assert result.studios_analyzed == 2
        assert "Bones" in result.studio_metrics
        assert "MAPPA" in result.studio_metrics

    def test_total_studio_years(
        self, credits, anime_map, iv_scores, studio_assignments
    ):
        result = compute_studio_timeseries(
            credits, anime_map, iv_scores, studio_assignments
        )
        # Bones: 3 years, MAPPA: 3 years → 6 studio-years
        assert result.total_studio_years == 6

    def test_bones_staff_count(self, credits, anime_map, iv_scores, studio_assignments):
        result = compute_studio_timeseries(
            credits, anime_map, iv_scores, studio_assignments
        )
        bones = result.studio_metrics["Bones"]
        # 2020: p1, p2, p3 → 3
        assert bones[0]["staff_count"] == 3
        # 2021: p1, p2, p4 → 3
        assert bones[1]["staff_count"] == 3
        # 2022: p1, p4, p5 → 3
        assert bones[2]["staff_count"] == 3

    def test_bones_avg_anime_score(
        self, credits, anime_map, iv_scores, studio_assignments
    ):
        result = compute_studio_timeseries(
            credits, anime_map, iv_scores, studio_assignments
        )
        bones = result.studio_metrics["Bones"]
        assert bones[0]["avg_anime_score"] == 8.0  # a1
        assert bones[1]["avg_anime_score"] == 7.5  # a2
        assert bones[2]["avg_anime_score"] == 9.0  # a3

    def test_mappa_staff_count(self, credits, anime_map, iv_scores, studio_assignments):
        result = compute_studio_timeseries(
            credits, anime_map, iv_scores, studio_assignments
        )
        mappa = result.studio_metrics["MAPPA"]
        assert mappa[0]["staff_count"] == 2  # p6, p7
        assert mappa[1]["staff_count"] == 2  # p6, p8
        assert mappa[2]["staff_count"] == 2  # p9, p10

    def test_year_values(self, credits, anime_map, iv_scores, studio_assignments):
        result = compute_studio_timeseries(
            credits, anime_map, iv_scores, studio_assignments
        )
        bones = result.studio_metrics["Bones"]
        assert [m["year"] for m in bones] == [2020, 2021, 2022]


class TestRetentionCalculation:
    """Retention = intersection/prev_staff; same staff → 1.0, complete turnover → 0."""

    def test_first_year_zero_retention(
        self, credits, anime_map, iv_scores, studio_assignments
    ):
        result = compute_studio_timeseries(
            credits, anime_map, iv_scores, studio_assignments
        )
        bones = result.studio_metrics["Bones"]
        # First year has no prior → retention=0
        assert bones[0]["talent_retention"] == 0.0

    def test_partial_retention(self, credits, anime_map, iv_scores, studio_assignments):
        result = compute_studio_timeseries(
            credits, anime_map, iv_scores, studio_assignments
        )
        bones = result.studio_metrics["Bones"]
        # 2020: {p1,p2,p3}, 2021: {p1,p2,p4}
        # Retention = |intersection| / |prev_staff| = |{p1,p2}| / |{p1,p2,p3}| = 2/3 ≈ 0.667
        assert bones[1]["talent_retention"] == pytest.approx(2 / 3, abs=1e-3)

    def test_high_retention_same_staff(self):
        """All same staff across years → retention=1.0."""
        anime_map = {
            "a1": Anime(
                id="a1", title_en="X1", year=2020, studios=["StudioX"]
            ),
            "a2": Anime(
                id="a2", title_en="X2", year=2021, studios=["StudioX"]
            ),
        }
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"
            ),
        ]
        iv_scores = {"p1": 50.0, "p2": 40.0}
        assignments = {
            "p1": {2020: "StudioX", 2021: "StudioX"},
            "p2": {2020: "StudioX", 2021: "StudioX"},
        }
        result = compute_studio_timeseries(credits, anime_map, iv_scores, assignments)
        metrics = result.studio_metrics["StudioX"]
        # Year 2 (index 1): same staff → retention = 2/2 = 1.0
        assert metrics[1]["talent_retention"] == 1.0

    def test_complete_turnover_zero_retention(self):
        """Completely different staff between years → retention=0."""
        anime_map = {
            "a1": Anime(
                id="a1", title_en="Y1", year=2020, studios=["StudioY"]
            ),
            "a2": Anime(
                id="a2", title_en="Y2", year=2021, studios=["StudioY"]
            ),
        }
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p3", anime_id="a2", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p4", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"
            ),
        ]
        iv_scores = {"p1": 50.0, "p2": 40.0, "p3": 60.0, "p4": 30.0}
        assignments = {
            "p1": {2020: "StudioY"},
            "p2": {2020: "StudioY"},
            "p3": {2021: "StudioY"},
            "p4": {2021: "StudioY"},
        }
        result = compute_studio_timeseries(credits, anime_map, iv_scores, assignments)
        metrics = result.studio_metrics["StudioY"]
        # Year 2: no overlap → retention = 0/2 = 0
        assert metrics[1]["talent_retention"] == 0.0
        assert metrics[1]["new_talent_ratio"] == 1.0


class TestEmptyData:
    """Empty credits/assignments → empty result."""

    def test_empty_everything(self):
        result = compute_studio_timeseries([], {}, {}, {})
        assert isinstance(result, StudioTimeSeriesResult)
        assert result.studio_metrics == {}
        assert result.studios_analyzed == 0
        assert result.total_studio_years == 0

    def test_empty_assignments(self, credits, anime_map, iv_scores):
        result = compute_studio_timeseries(credits, anime_map, iv_scores, {})
        assert result.studios_analyzed == 0
        assert result.total_studio_years == 0

    def test_empty_credits(self, anime_map, iv_scores, studio_assignments):
        result = compute_studio_timeseries([], anime_map, iv_scores, studio_assignments)
        # studio_assignments still provides data, but anime scores will be 0
        assert result.studios_analyzed > 0


class TestSingleStudio:
    """One studio across 3 years."""

    def test_single_studio_metrics(self):
        anime_map = {
            "a1": Anime(id="a1", title_en="S1", year=2020, studios=["Solo"], score=7.0),
            "a2": Anime(id="a2", title_en="S2", year=2021, studios=["Solo"], score=8.0),
            "a3": Anime(id="a3", title_en="S3", year=2022, studios=["Solo"], score=9.0),
        }
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"
            ),
            Credit(
                person_id="p3",
                anime_id="a2",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR, source="test"),
            Credit(
                person_id="p3",
                anime_id="a3",
                role=Role.ANIMATION_DIRECTOR,
                source="test",
            ),
            Credit(
                person_id="p4", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"
            ),
        ]
        iv_scores = {"p1": 80.0, "p2": 50.0, "p3": 60.0, "p4": 40.0}
        assignments = {
            "p1": {2020: "Solo", 2021: "Solo", 2022: "Solo"},
            "p2": {2020: "Solo", 2021: "Solo"},
            "p3": {2021: "Solo", 2022: "Solo"},
            "p4": {2022: "Solo"},
        }
        result = compute_studio_timeseries(credits, anime_map, iv_scores, assignments)

        assert result.studios_analyzed == 1
        assert "Solo" in result.studio_metrics
        assert result.total_studio_years == 3

        metrics = result.studio_metrics["Solo"]
        assert len(metrics) == 3
        assert metrics[0]["year"] == 2020
        assert metrics[1]["year"] == 2021
        assert metrics[2]["year"] == 2022

        # Staff counts
        assert metrics[0]["staff_count"] == 2  # p1, p2
        assert metrics[1]["staff_count"] == 3  # p1, p2, p3
        assert metrics[2]["staff_count"] == 3  # p1, p3, p4

        # Avg anime scores
        assert metrics[0]["avg_anime_score"] == 7.0
        assert metrics[1]["avg_anime_score"] == 8.0
        assert metrics[2]["avg_anime_score"] == 9.0

        # Avg staff IV
        assert metrics[0]["avg_staff_iv"] > 0
        assert metrics[1]["avg_staff_iv"] > 0

        # New talent ratio for first year is 1.0
        assert metrics[0]["new_talent_ratio"] == 1.0
