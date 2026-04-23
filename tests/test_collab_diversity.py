"""collab_diversity モジュールのテスト."""

from src.analysis.collab_diversity import compute_collab_diversity
from src.runtime.models import BronzeAnime as Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Show 1", year=2020),
        "a2": Anime(id="a2", title_en="Show 2", year=2021),
        "a3": Anime(id="a3", title_en="Show 3", year=2022),
    }
    credits = [
        # p1 works with p2 on a1 and a2, with p3 on a3 (diverse)
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a3", role=Role.KEY_ANIMATOR),
    ]
    return credits, anime_map


class TestCollabDiversity:
    def test_empty(self):
        result = compute_collab_diversity([], {})
        assert result == {}

    def test_returns_persons(self):
        credits, anime_map = _make_data()
        result = compute_collab_diversity(credits, anime_map)
        assert "p1" in result

    def test_unique_collaborators(self):
        credits, anime_map = _make_data()
        result = compute_collab_diversity(credits, anime_map)
        assert result["p1"]["unique_collaborators"] == 2  # p2 and p3

    def test_diversity_index(self):
        credits, anime_map = _make_data()
        result = compute_collab_diversity(credits, anime_map)
        # p1 works with p2 twice and p3 once → not perfectly diverse
        assert 0 <= result["p1"]["diversity_index"] <= 1

    def test_repeat_rate(self):
        credits, anime_map = _make_data()
        result = compute_collab_diversity(credits, anime_map)
        # p2 is repeated
        assert result["p1"]["repeat_rate"] == 50.0  # 1 of 2 collaborators repeated

    def test_diversity_score(self):
        credits, anime_map = _make_data()
        result = compute_collab_diversity(credits, anime_map)
        assert 0 <= result["p1"]["diversity_score"] <= 100

    def test_single_collaborator(self):
        credits = [
            Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
            Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        ]
        anime_map = {"a1": Anime(id="a1", title_en="Solo", year=2020)}
        result = compute_collab_diversity(credits, anime_map)
        assert result["p1"]["unique_collaborators"] == 1

    def test_high_diversity(self):
        """Many unique collaborators → high diversity."""
        credits = []
        anime_map = {}
        for i in range(10):
            aid = f"a{i}"
            anime_map[aid] = Anime(id=aid, title_en=f"Show {i}", year=2020)
            credits.append(Credit(person_id="p1", anime_id=aid, role=Role.DIRECTOR))
            credits.append(
                Credit(person_id=f"p{i + 10}", anime_id=aid, role=Role.KEY_ANIMATOR)
            )

        result = compute_collab_diversity(credits, anime_map)
        assert result["p1"]["unique_collaborators"] == 10
        # Perfect diversity (each person once)
        assert result["p1"]["diversity_index"] == 1.0
