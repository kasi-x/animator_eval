"""career モジュールのテスト."""

from src.analysis.career import analyze_career, batch_career_analysis
from src.models import Anime, Credit, Role


def _make_career_data():
    """テスト用キャリアデータ: 動画→原画→作監の昇進パターン."""
    anime_map = {
        "a1": Anime(id="a1", title_en="Early Work", year=2018, score=6.0),
        "a2": Anime(id="a2", title_en="Growth Phase", year=2020, score=7.0),
        "a3": Anime(id="a3", title_en="Key Work", year=2022, score=8.0),
        "a4": Anime(id="a4", title_en="Latest Work", year=2024, score=8.5),
    }
    credits = [
        # 2018: 動画 (in-between)
        Credit(person_id="p1", anime_id="a1", role=Role.IN_BETWEEN),
        # 2020: 原画 (key animator)
        Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR),
        # 2022: 原画 + 作画監督
        Credit(person_id="p1", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a3", role=Role.ANIMATION_DIRECTOR),
        # 2024: 作画監督
        Credit(person_id="p1", anime_id="a4", role=Role.ANIMATION_DIRECTOR),
    ]
    return credits, anime_map


class TestAnalyzeCareer:
    def test_year_range(self):
        credits, anime_map = _make_career_data()
        result = analyze_career("p1", credits, anime_map)
        assert result.first_year == 2018
        assert result.latest_year == 2024

    def test_active_years(self):
        credits, anime_map = _make_career_data()
        result = analyze_career("p1", credits, anime_map)
        assert result.active_years == 4  # 2018, 2020, 2022, 2024

    def test_total_credits(self):
        credits, anime_map = _make_career_data()
        result = analyze_career("p1", credits, anime_map)
        assert result.total_credits == 5

    def test_yearly_activity(self):
        credits, anime_map = _make_career_data()
        result = analyze_career("p1", credits, anime_map)
        assert result.yearly_activity[2018] == 1
        assert result.yearly_activity[2022] == 2  # 2 credits in same year

    def test_role_progression(self):
        credits, anime_map = _make_career_data()
        result = analyze_career("p1", credits, anime_map)
        stages = [record.stage for record in result.role_progression]
        # Should show career growth: in_between(1) → key(3) → anim_dir(5) → anim_dir(5)
        assert stages[0] < stages[-1]

    def test_highest_stage(self):
        credits, anime_map = _make_career_data()
        result = analyze_career("p1", credits, anime_map)
        assert result.highest_stage == 5  # animation_director (+chief AD merged)
        assert "animation_director" in result.highest_roles

    def test_peak_year(self):
        credits, anime_map = _make_career_data()
        result = analyze_career("p1", credits, anime_map)
        # 2022 has 2 credits (key_animator + animation_director), others have 1
        assert result.peak_year == 2022
        assert result.peak_credits == 2

    def test_nonexistent_person(self):
        credits, anime_map = _make_career_data()
        result = analyze_career("nobody", credits, anime_map)
        assert result.total_credits == 0
        assert result.first_year is None

    def test_empty_credits(self):
        result = analyze_career("p1", [], {})
        assert result.total_credits == 0


class TestBatchCareerAnalysis:
    def test_analyzes_all(self):
        credits, anime_map = _make_career_data()
        results = batch_career_analysis(credits, anime_map)
        assert "p1" in results

    def test_filter_by_person_ids(self):
        credits, anime_map = _make_career_data()
        results = batch_career_analysis(credits, anime_map, person_ids={"p1"})
        assert len(results) == 1
        assert "p1" in results
