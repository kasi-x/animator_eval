"""explain モジュールのテスト."""

from src.analysis.explain import explain_authority, explain_skill, explain_trust
from src.models import BronzeAnime as Anime, Credit, Role


def _make_test_data():
    """テストデータ: 監督1人、アニメーター2人、作品3作."""
    anime_map = {
        "a1": Anime(id="a1", title_ja="作品A", title_en="Work A", year=2020),
        "a2": Anime(id="a2", title_ja="作品B", title_en="Work B", year=2022),
        "a3": Anime(id="a3", title_ja="作品C", title_en="Work C", year=2024),
    }
    credits = [
        # dir1 directs a1, a2, a3
        Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="dir1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="dir1", anime_id="a3", role=Role.DIRECTOR),
        # anim1 works on a1, a2, a3 (all dir1's works)
        Credit(person_id="anim1", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="anim1", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="anim1", anime_id="a3", role=Role.ANIMATION_DIRECTOR),
        # anim2 works on a1 only
        Credit(person_id="anim2", anime_id="a1", role=Role.IN_BETWEEN),
    ]
    return credits, anime_map


class TestExplainAuthority:
    def test_returns_works_sorted_by_score(self):
        credits, anime_map = _make_test_data()
        result = explain_authority("anim1", credits, anime_map)
        assert len(result) == 3
        assert result[0]["score"] >= result[1]["score"]

    def test_empty_for_nonexistent(self):
        credits, anime_map = _make_test_data()
        assert explain_authority("nobody", credits, anime_map) == []

    def test_includes_role(self):
        credits, anime_map = _make_test_data()
        result = explain_authority("anim1", credits, anime_map)
        roles = {r["role"] for r in result}
        assert "key_animator" in roles


class TestExplainTrust:
    def test_identifies_director_collabs(self):
        credits, anime_map = _make_test_data()
        result = explain_trust("anim1", credits, anime_map)
        assert len(result) >= 1
        dir_entry = next(r for r in result if r["director_id"] == "dir1")
        assert dir_entry["shared_works"] == 3

    def test_empty_for_nonexistent(self):
        credits, anime_map = _make_test_data()
        assert explain_trust("nobody", credits, anime_map) == []

    def test_single_work_collab(self):
        credits, anime_map = _make_test_data()
        result = explain_trust("anim2", credits, anime_map)
        assert len(result) == 1
        assert result[0]["shared_works"] == 1


class TestExplainSkill:
    def test_returns_works_sorted_by_year(self):
        credits, anime_map = _make_test_data()
        result = explain_skill("anim1", credits, anime_map)
        years = [r["year"] for r in result]
        assert years == sorted(years, reverse=True)

    def test_no_duplicate_anime(self):
        credits, anime_map = _make_test_data()
        result = explain_skill("anim1", credits, anime_map)
        anime_ids = [r["anime_id"] for r in result]
        assert len(anime_ids) == len(set(anime_ids))

    def test_empty_for_nonexistent(self):
        credits, anime_map = _make_test_data()
        assert explain_skill("nobody", credits, anime_map) == []
