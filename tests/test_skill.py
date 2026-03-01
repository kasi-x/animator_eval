"""skill モジュールのテスト."""

from src.analysis.skill import compute_skill_scores
from src.models import Anime, Credit, Role


class TestComputeSkillScores:
    def test_empty(self):
        assert compute_skill_scores([], {}) == {}

    def test_no_scored_anime(self):
        # compute_skill_scores now uses staff count (not anime.score).
        # A single credit still yields staff_count=1. With only 1 anime
        # there are not enough teams to rate, but the person gets the
        # default rating normalized to 50.0.
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        anime_map = {"a1": Anime(id="a1", year=2020, score=None)}
        result = compute_skill_scores(credits, anime_map)
        assert "p1" in result
        assert result["p1"] == 50.0

    def test_basic_skill_scores(self):
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR),
            Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR),
        ]
        anime_map = {
            "a1": Anime(id="a1", year=2020, score=9.0),
            "a2": Anime(id="a2", year=2020, score=8.0),
            "a3": Anime(id="a3", year=2020, score=5.0),
        }
        result = compute_skill_scores(credits, anime_map)
        assert "p1" in result
        assert "p2" in result
        # p1 participated in higher rated anime overall
        assert result["p1"] > result["p2"]

    def test_non_skill_roles_excluded(self):
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        ]
        anime_map = {"a1": Anime(id="a1", year=2020, score=9.0)}
        result = compute_skill_scores(credits, anime_map)
        # DIRECTOR is not in SKILL_ROLES
        assert "p1" not in result
