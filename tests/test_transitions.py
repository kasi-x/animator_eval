"""transitions モジュールのテスト."""

from src.analysis.transitions import compute_role_transitions
from src.models import BronzeAnime as Anime, Credit, Role


def _make_credits_and_anime():
    """テスト用のクレジットとアニメデータ."""
    anime_map = {
        "a1": Anime(id="a1", title_en="Show 2018", year=2018, score=7.0),
        "a2": Anime(id="a2", title_en="Show 2020", year=2020, score=7.5),
        "a3": Anime(id="a3", title_en="Show 2022", year=2022, score=8.0),
        "a4": Anime(id="a4", title_en="Show 2024", year=2024, score=8.5),
    }
    credits = [
        # Person 1: in-between → key_animator → animation_director
        Credit(person_id="p1", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p1", anime_id="a3", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        # Person 2: in-between → key_animator
        Credit(person_id="p2", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        # Person 3: key_animator → animation_director → director
        Credit(person_id="p3", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p3", anime_id="a2", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p3", anime_id="a4", role=Role.DIRECTOR, source="test"),
        # Person 4: only one credit (should be skipped)
        Credit(person_id="p4", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
    ]
    return credits, anime_map


class TestComputeRoleTransitions:
    def test_basic_transitions(self):
        credits, anime_map = _make_credits_and_anime()
        result = compute_role_transitions(credits, anime_map)
        assert result["total_persons_analyzed"] == 3  # p1, p2, p3

    def test_transition_counts(self):
        credits, anime_map = _make_credits_and_anime()
        result = compute_role_transitions(credits, anime_map)
        transitions = result["transitions"]

        # Find in-between → key_animator transition
        ib_to_ka = [t for t in transitions if t.from_stage == 1 and t.to_stage == 3]
        assert len(ib_to_ka) == 1
        assert ib_to_ka[0].count == 2  # p1 and p2 both do this

    def test_avg_years(self):
        credits, anime_map = _make_credits_and_anime()
        result = compute_role_transitions(credits, anime_map)
        transitions = result["transitions"]

        # in-between(2018) → key_animator: p1 takes 2 years (2018→2020), p2 takes 4 years (2018→2022)
        ib_to_ka = [t for t in transitions if t.from_stage == 1 and t.to_stage == 3]
        assert ib_to_ka[0].avg_years == 3.0  # (2 + 4) / 2

    def test_career_paths(self):
        credits, anime_map = _make_credits_and_anime()
        result = compute_role_transitions(credits, anime_map)
        paths = result["career_paths"]
        assert len(paths) > 0
        # All paths should have at least 2 stages
        for path_record in paths:
            assert len(path_record.path) >= 2

    def test_time_to_stage(self):
        credits, anime_map = _make_credits_and_anime()
        result = compute_role_transitions(credits, anime_map)
        avg_time = result["avg_time_to_stage"]
        # Stage 3 (key animator) should be reachable
        assert 3 in avg_time
        assert avg_time[3].sample_size > 0

    def test_empty_data(self):
        result = compute_role_transitions([], {})
        assert result["total_persons_analyzed"] == 0
        assert result["transitions"] == []
        assert result["career_paths"] == []

    def test_no_year_data_skipped(self):
        """アニメに年データがない場合はスキップされる."""
        anime_map = {
            "a1": Anime(id="a1", title_en="No Year"),  # year=None
        }
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        ]
        result = compute_role_transitions(credits, anime_map)
        assert result["total_persons_analyzed"] == 0
