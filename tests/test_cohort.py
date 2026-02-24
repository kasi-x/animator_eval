"""cohort モジュールのテスト."""

from src.analysis.cohort import compute_cohort_analysis
from src.models import Anime, Credit, Role


def _make_multi_generation_data():
    """複数世代のテストデータ."""
    anime_map = {
        "a1": Anime(id="a1", title_en="Show 2005", year=2005),
        "a2": Anime(id="a2", title_en="Show 2008", year=2008),
        "a3": Anime(id="a3", title_en="Show 2012", year=2012),
        "a4": Anime(id="a4", title_en="Show 2015", year=2015),
        "a5": Anime(id="a5", title_en="Show 2021", year=2021),
    }
    credits = [
        # 2000s cohort: p1 (debut 2005)
        Credit(person_id="p1", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p1", anime_id="a3", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p1", anime_id="a4", role=Role.DIRECTOR, source="test"),
        # 2000s cohort: p2 (debut 2008)
        Credit(person_id="p2", anime_id="a2", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        # 2010s cohort: p3 (debut 2012)
        Credit(person_id="p3", anime_id="a3", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p3", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p3", anime_id="a5", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        # 2020s cohort: p4 (debut 2021)
        Credit(person_id="p4", anime_id="a5", role=Role.IN_BETWEEN, source="test"),
    ]
    return credits, anime_map


class TestComputeCohortAnalysis:
    def test_identifies_cohorts(self):
        credits, anime_map = _make_multi_generation_data()
        result = compute_cohort_analysis(credits, anime_map)
        assert "2000s" in result["cohorts"]
        assert "2010s" in result["cohorts"]
        assert "2020s" in result["cohorts"]

    def test_cohort_sizes(self):
        credits, anime_map = _make_multi_generation_data()
        result = compute_cohort_analysis(credits, anime_map)
        assert result["cohorts"]["2000s"]["size"] == 2  # p1, p2
        assert result["cohorts"]["2010s"]["size"] == 1  # p3
        assert result["cohorts"]["2020s"]["size"] == 1  # p4

    def test_career_span(self):
        credits, anime_map = _make_multi_generation_data()
        result = compute_cohort_analysis(credits, anime_map)
        # p1: 2005-2015 = 11 years, p2: 2008-2012 = 5 years
        assert result["cohorts"]["2000s"]["avg_career_span"] == 8.0

    def test_director_rate(self):
        credits, anime_map = _make_multi_generation_data()
        result = compute_cohort_analysis(credits, anime_map)
        # In 2000s: p1 reached director, p2 did not = 50%
        assert result["cohorts"]["2000s"]["director_rate"] == 50.0

    def test_with_scores(self):
        credits, anime_map = _make_multi_generation_data()
        scores = {"p1": 80.0, "p2": 40.0, "p3": 60.0, "p4": 20.0}
        result = compute_cohort_analysis(credits, anime_map, scores)
        assert result["cohorts"]["2000s"]["avg_score"] == 60.0

    def test_stage_by_cohort(self):
        credits, anime_map = _make_multi_generation_data()
        result = compute_cohort_analysis(credits, anime_map)
        stages_2000s = result["stage_by_cohort"]["2000s"]
        # p1 reached stage 6 (director), p2 reached stage 3 (key animator)
        assert 6 in stages_2000s
        assert 3 in stages_2000s

    def test_empty_data(self):
        result = compute_cohort_analysis([], {})
        assert result["total_persons"] == 0
        assert result["cohorts"] == {}

    def test_total_persons(self):
        credits, anime_map = _make_multi_generation_data()
        result = compute_cohort_analysis(credits, anime_map)
        assert result["total_persons"] == 4
