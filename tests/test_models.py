"""models モジュールのテスト."""

from src.models import Anime, Person, Role, ScoreResult, parse_role


class TestParseRole:
    def test_english_lowercase(self):
        assert parse_role("director") == Role.DIRECTOR

    def test_english_mixed_case(self):
        assert parse_role("Key Animation") == Role.KEY_ANIMATOR

    def test_japanese(self):
        assert parse_role("監督") == Role.DIRECTOR
        assert parse_role("作画監督") == Role.ANIMATION_DIRECTOR
        assert parse_role("原画") == Role.KEY_ANIMATOR

    def test_unknown_role(self):
        assert parse_role("unknown_role_xyz") == Role.OTHER

    def test_whitespace_handling(self):
        assert parse_role("  director  ") == Role.DIRECTOR


class TestPerson:
    def test_display_name_ja(self):
        p = Person(id="test:1", name_ja="宮崎駿", name_en="Hayao Miyazaki")
        assert p.display_name == "宮崎駿"

    def test_display_name_en_fallback(self):
        p = Person(id="test:1", name_en="Hayao Miyazaki")
        assert p.display_name == "Hayao Miyazaki"

    def test_display_name_id_fallback(self):
        p = Person(id="test:1")
        assert p.display_name == "test:1"


class TestAnime:
    def test_display_title_ja(self):
        a = Anime(id="test:1", title_ja="千と千尋の神隠し", title_en="Spirited Away")
        assert a.display_title == "千と千尋の神隠し"

    def test_display_title_en_fallback(self):
        a = Anime(id="test:1", title_en="Spirited Away")
        assert a.display_title == "Spirited Away"


class TestScoreResult:
    def test_composite_calculation(self):
        s = ScoreResult(person_id="test:1", authority=100.0, trust=100.0, skill=100.0)
        # 0.4 * 100 + 0.35 * 100 + 0.25 * 100 = 100.0
        assert s.composite == 100.0

    def test_composite_weighted(self):
        s = ScoreResult(person_id="test:1", authority=50.0, trust=0.0, skill=0.0)
        assert s.composite == 50.0 * 0.4

    def test_composite_zero(self):
        s = ScoreResult(person_id="test:1")
        assert s.composite == 0.0
