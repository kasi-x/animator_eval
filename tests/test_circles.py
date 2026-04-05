"""circles モジュールのテスト."""

from src.analysis.network.circles import find_director_circles, get_person_circles
from src.models import Anime, Credit, Role


def _make_test_data():
    """監督2人、アニメーター3人、作品5作のテストデータ."""
    anime_map = {
        f"a{i}": Anime(
            id=f"a{i}", title_en=f"Anime {i}", year=2020 + i, score=7.0 + i * 0.2
        )
        for i in range(1, 6)
    }

    credits = [
        # dir1 → a1, a2, a3, a4 (4作品)
        Credit(person_id="dir1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="dir1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="dir1", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="dir1", anime_id="a4", role=Role.DIRECTOR),
        # dir2 → a3, a4, a5 (3作品)
        Credit(person_id="dir2", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="dir2", anime_id="a4", role=Role.DIRECTOR),
        Credit(person_id="dir2", anime_id="a5", role=Role.DIRECTOR),
        # anim1: dir1 の常連 (a1, a2, a3)
        Credit(person_id="anim1", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="anim1", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="anim1", anime_id="a3", role=Role.KEY_ANIMATOR),
        # anim2: dir1 に1回、dir2 に3回
        Credit(person_id="anim2", anime_id="a1", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="anim2", anime_id="a3", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="anim2", anime_id="a4", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="anim2", anime_id="a5", role=Role.ANIMATION_DIRECTOR),
        # anim3: dir1 に1回のみ
        Credit(person_id="anim3", anime_id="a4", role=Role.IN_BETWEEN),
    ]
    return credits, anime_map


class TestFindDirectorCircles:
    def test_basic_circles(self):
        credits, anime_map = _make_test_data()
        circles = find_director_circles(
            credits, anime_map, min_shared_works=2, min_director_works=3
        )
        assert "dir1" in circles
        assert "dir2" in circles

    def test_dir1_has_anim1(self):
        credits, anime_map = _make_test_data()
        circles = find_director_circles(
            credits, anime_map, min_shared_works=2, min_director_works=3
        )
        member_ids = [m.person_id for m in circles["dir1"].members]
        assert "anim1" in member_ids

    def test_anim3_not_in_circle(self):
        """1回しか共演していないアニメーターはサークルに含まれない."""
        credits, anime_map = _make_test_data()
        circles = find_director_circles(
            credits, anime_map, min_shared_works=2, min_director_works=3
        )
        member_ids = [m.person_id for m in circles["dir1"].members]
        assert "anim3" not in member_ids

    def test_hit_rate_calculation(self):
        credits, anime_map = _make_test_data()
        circles = find_director_circles(
            credits, anime_map, min_shared_works=2, min_director_works=3
        )
        anim1_entry = next(m for m in circles["dir1"].members if m.person_id == "anim1")
        # anim1: 3 shared works / 4 total dir1 works = 0.75
        assert anim1_entry.hit_rate == 0.75

    def test_total_works(self):
        credits, anime_map = _make_test_data()
        circles = find_director_circles(
            credits, anime_map, min_shared_works=2, min_director_works=3
        )
        assert circles["dir1"].total_works == 4
        assert circles["dir2"].total_works == 3

    def test_members_sorted_by_shared_works(self):
        credits, anime_map = _make_test_data()
        circles = find_director_circles(
            credits, anime_map, min_shared_works=2, min_director_works=3
        )
        works = [m.shared_works for m in circles["dir1"].members]
        assert works == sorted(works, reverse=True)

    def test_empty_credits(self):
        circles = find_director_circles([], {})
        assert circles == {}

    def test_min_director_works_filter(self):
        credits, anime_map = _make_test_data()
        # dir2 has 3 works, so requiring 4 should exclude it
        circles = find_director_circles(
            credits, anime_map, min_shared_works=2, min_director_works=4
        )
        assert "dir2" not in circles


class TestGetPersonCircles:
    def test_anim2_in_two_circles(self):
        credits, anime_map = _make_test_data()
        circles = find_director_circles(
            credits, anime_map, min_shared_works=2, min_director_works=3
        )
        person_circles = get_person_circles("anim2", circles)
        director_ids = {c.director_id for c in person_circles}
        assert "dir1" in director_ids
        assert "dir2" in director_ids

    def test_nonexistent_person(self):
        credits, anime_map = _make_test_data()
        circles = find_director_circles(
            credits, anime_map, min_shared_works=2, min_director_works=3
        )
        result = get_person_circles("nobody", circles)
        assert result == []

    def test_sorted_by_shared_works(self):
        credits, anime_map = _make_test_data()
        circles = find_director_circles(
            credits, anime_map, min_shared_works=2, min_director_works=3
        )
        person_circles = get_person_circles("anim2", circles)
        works = [c.shared_works for c in person_circles]
        assert works == sorted(works, reverse=True)
