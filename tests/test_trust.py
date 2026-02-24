"""trust モジュールのテスト."""

from src.analysis.trust import (
    _compute_time_weight,
    compute_trust_scores,
    detect_engagement_decay,
)
from src.models import Anime, Credit, Role


class TestTimeWeight:
    def test_current_year(self):
        assert _compute_time_weight(0) == 1.0

    def test_decay(self):
        w = _compute_time_weight(3.0)  # half-life
        assert abs(w - 0.5) < 0.01

    def test_monotonic_decrease(self):
        weights = [_compute_time_weight(t) for t in range(10)]
        for i in range(1, len(weights)):
            assert weights[i] < weights[i - 1]


class TestComputeTrustScores:
    def test_empty_credits(self):
        result = compute_trust_scores([], {})
        assert result == {}

    def test_basic_trust(self):
        credits = [
            Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR),
            Credit(person_id="anim1", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="dir1", anime_id="a2", role=Role.DIRECTOR),
            Credit(person_id="anim1", anime_id="a2", role=Role.KEY_ANIMATOR),
        ]
        anime_map = {
            "a1": Anime(id="a1", year=2023),
            "a2": Anime(id="a2", year=2024),
        }
        scores = compute_trust_scores(credits, anime_map, current_year=2025)
        # anim1 should have a trust score from being hired by dir1 twice
        assert "anim1" in scores
        assert scores["anim1"] > 0

    def test_repeat_engagement_higher_trust(self):
        # Animator hired by same director 3 times vs 1 time
        credits = [
            Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR),
            Credit(person_id="dir1", anime_id="a2", role=Role.DIRECTOR),
            Credit(person_id="dir1", anime_id="a3", role=Role.DIRECTOR),
            Credit(person_id="anim1", anime_id="a1", role=Role.KEY_ANIMATOR),
            Credit(person_id="anim1", anime_id="a2", role=Role.KEY_ANIMATOR),
            Credit(person_id="anim1", anime_id="a3", role=Role.KEY_ANIMATOR),
            Credit(person_id="anim2", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        anime_map = {
            "a1": Anime(id="a1", year=2023),
            "a2": Anime(id="a2", year=2024),
            "a3": Anime(id="a3", year=2025),
        }
        scores = compute_trust_scores(credits, anime_map, current_year=2025)
        assert scores["anim1"] > scores["anim2"]


class TestDetectEngagementDecay:
    def test_insufficient_data(self):
        credits = [
            Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR),
        ]
        anime_map = {"a1": Anime(id="a1", year=2020)}
        result = detect_engagement_decay("anim1", "dir1", credits, anime_map)
        assert result["status"] == "insufficient_data"

    def test_active_engagement(self):
        """アニメーターが監督の全作品に参加 → active."""
        credits = [
            Credit(person_id="dir1", anime_id=f"a{i}", role=Role.DIRECTOR)
            for i in range(1, 7)
        ] + [
            Credit(person_id="anim1", anime_id=f"a{i}", role=Role.KEY_ANIMATOR)
            for i in range(1, 7)
        ]
        anime_map = {f"a{i}": Anime(id=f"a{i}", year=2020 + i) for i in range(1, 7)}
        result = detect_engagement_decay("anim1", "dir1", credits, anime_map)
        assert result["status"] == "active"
        assert result["recent_rate"] == 1.0

    def test_decayed_engagement(self):
        """アニメーターが前半だけ参加して後半は不参加 → decayed."""
        credits = [
            Credit(person_id="dir1", anime_id=f"a{i}", role=Role.DIRECTOR)
            for i in range(1, 11)
        ] + [
            Credit(person_id="anim1", anime_id=f"a{i}", role=Role.KEY_ANIMATOR)
            for i in range(1, 6)  # 前半5作品のみ
        ]
        anime_map = {f"a{i}": Anime(id=f"a{i}", year=2015 + i) for i in range(1, 11)}
        result = detect_engagement_decay("anim1", "dir1", credits, anime_map)
        assert result["status"] == "decayed"
        assert result["recent_rate"] == 0.0

    def test_decay_rates_correct(self):
        """期待率と直近率が正しく計算される."""
        credits = [
            Credit(person_id="dir1", anime_id=f"a{i}", role=Role.DIRECTOR)
            for i in range(1, 11)
        ] + [
            Credit(person_id="anim1", anime_id=f"a{i}", role=Role.KEY_ANIMATOR)
            for i in range(1, 6)
        ]
        anime_map = {f"a{i}": Anime(id=f"a{i}", year=2015 + i) for i in range(1, 11)}
        result = detect_engagement_decay("anim1", "dir1", credits, anime_map)
        assert result["expected_rate"] == 0.5  # 10作品中5参加
        assert result["total_works"] == 10
        assert result["total_appearances"] == 5
