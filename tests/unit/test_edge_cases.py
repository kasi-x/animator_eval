"""エッジケーステスト — 極端な入力での動作確認."""

from src.analysis.scoring.normalize import normalize_scores
from src.analysis.career import analyze_career
from src.analysis.network.circles import find_director_circles
from src.analysis.anime_stats import compute_anime_stats
from src.runtime.models import BronzeAnime as Anime, Credit, Role


class TestNormalizationEdgeCases:
    def test_very_large_values(self):
        scores = {"p1": 1e10, "p2": 2e10}
        result = normalize_scores(scores)
        assert result["p1"] == 0.0
        assert result["p2"] == 100.0

    def test_very_small_values(self):
        scores = {"p1": 1e-10, "p2": 2e-10}
        result = normalize_scores(scores)
        assert result["p1"] == 0.0
        assert result["p2"] == 100.0

    def test_many_persons(self):
        scores = {f"p{i}": float(i) for i in range(1000)}
        result = normalize_scores(scores)
        assert result["p0"] == 0.0
        assert result["p999"] == 100.0
        assert len(result) == 1000


class TestCareerEdgeCases:
    def test_single_credit(self):
        anime_map = {"a1": Anime(id="a1", title_en="X", year=2024)}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR)]
        result = analyze_career("p1", credits, anime_map)
        assert result.total_credits == 1
        assert result.first_year == 2024
        assert result.latest_year == 2024
        assert result.active_years == 1

    def test_no_year_info(self):
        anime_map = {"a1": Anime(id="a1", title_en="X")}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR)]
        result = analyze_career("p1", credits, anime_map)
        assert result.total_credits == 1
        assert result.first_year is None
        assert result.active_years == 0

    def test_same_year_many_credits(self):
        anime_map = {
            f"a{i}": Anime(id=f"a{i}", title_en=f"X{i}", year=2024)
            for i in range(20)
        }
        credits = [
            Credit(person_id="p1", anime_id=f"a{i}", role=Role.KEY_ANIMATOR)
            for i in range(20)
        ]
        result = analyze_career("p1", credits, anime_map)
        assert result.active_years == 1
        assert result.yearly_activity[2024] == 20


class TestCirclesEdgeCases:
    def test_director_only(self):
        """監督だけでアニメーターがいない場合."""
        anime_map = {"a1": Anime(id="a1", year=2024)}
        credits = [Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR)]
        circles = find_director_circles(
            credits, anime_map, min_shared_works=1, min_director_works=1
        )
        # Should either not have dir1 or have empty members list
        if "dir1" in circles:
            assert circles["dir1"].members == []
        else:
            assert True  # Also acceptable to exclude directors with no circle members

    def test_one_work_director(self):
        """1作品しかない監督は min_director_works=2 で除外される."""
        anime_map = {"a1": Anime(id="a1", year=2024)}
        credits = [
            Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR),
            Credit(person_id="anim1", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        circles = find_director_circles(
            credits, anime_map, min_shared_works=1, min_director_works=2
        )
        assert "dir1" not in circles


class TestAnimeStatsEdgeCases:
    def test_anime_with_no_credits(self):
        anime_map = {"a1": Anime(id="a1", title_en="X", year=2024)}
        # No credits for a1
        stats = compute_anime_stats([], anime_map)
        assert stats == {}

    def test_credits_with_missing_anime(self):
        credits = [Credit(person_id="p1", anime_id="missing", role=Role.KEY_ANIMATOR)]
        stats = compute_anime_stats(credits, {})
        assert stats == {}

    def test_many_persons_one_anime(self):
        anime_map = {"a1": Anime(id="a1", title_en="Big Show", year=2024)}
        credits = [
            Credit(person_id=f"p{i}", anime_id="a1", role=Role.KEY_ANIMATOR)
            for i in range(100)
        ]
        stats = compute_anime_stats(credits, anime_map)
        assert stats["a1"]["unique_persons"] == 100
        assert stats["a1"]["credit_count"] == 100
