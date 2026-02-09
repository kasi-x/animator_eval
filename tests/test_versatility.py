"""versatility モジュールのテスト."""

from src.analysis.versatility import ROLE_CATEGORY, compute_versatility
from src.models import Credit, Role


def _make_credits():
    return [
        # p1: director + key_animator + screenplay → 3 categories
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.SCREENPLAY, source="test"),
        # p2: only key_animator → 1 category
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        # p3: many categories
        Credit(person_id="p3", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p3", anime_id="a1", role=Role.CHARACTER_DESIGNER, source="test"),
        Credit(person_id="p3", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a1", role=Role.EFFECTS, source="test"),
        Credit(person_id="p3", anime_id="a1", role=Role.SCREENPLAY, source="test"),
    ]


class TestComputeVersatility:
    def test_returns_all_persons(self):
        credits = _make_credits()
        result = compute_versatility(credits)
        assert set(result.keys()) == {"p1", "p2", "p3"}

    def test_category_count(self):
        credits = _make_credits()
        result = compute_versatility(credits)
        assert result["p1"]["category_count"] == 3  # direction, animation, writing
        assert result["p2"]["category_count"] == 1  # animation only

    def test_versatility_score_single_category(self):
        credits = _make_credits()
        result = compute_versatility(credits)
        assert result["p2"]["versatility_score"] == 25.0  # 1/4 * 100

    def test_versatility_score_multi_category(self):
        credits = _make_credits()
        result = compute_versatility(credits)
        # p3 has 5 categories: direction, design, animation, technical, writing
        assert result["p3"]["versatility_score"] == 100.0  # capped at 100

    def test_roles_list(self):
        credits = _make_credits()
        result = compute_versatility(credits)
        assert "director" in result["p1"]["roles"]
        assert "key_animator" in result["p1"]["roles"]

    def test_filter_person_ids(self):
        credits = _make_credits()
        result = compute_versatility(credits, person_ids={"p1"})
        assert set(result.keys()) == {"p1"}

    def test_empty_credits(self):
        result = compute_versatility([])
        assert result == {}

    def test_category_credits(self):
        credits = _make_credits()
        result = compute_versatility(credits)
        # p2 has 2 key_animator credits → animation: 2
        assert result["p2"]["category_credits"]["animation"] == 2

    def test_role_category_mapping(self):
        assert ROLE_CATEGORY[Role.DIRECTOR] == "direction"
        assert ROLE_CATEGORY[Role.KEY_ANIMATOR] == "animation"
        assert ROLE_CATEGORY[Role.SCREENPLAY] == "writing"
