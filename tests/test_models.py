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
        assert parse_role("unknown_role_xyz") == Role.SPECIAL

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
    def test_iv_score_default(self):
        s = ScoreResult(person_id="test:1")
        assert s.iv_score == 0.0

    def test_iv_score_set(self):
        s = ScoreResult(person_id="test:1", iv_score=75.0)
        assert s.iv_score == 75.0

    def test_all_structural_fields(self):
        s = ScoreResult(
            person_id="test:1",
            person_fe=0.5,
            studio_fe_exposure=0.3,
            birank=0.7,
            patronage=0.4,
            dormancy=0.95,
            awcc=0.6,
            ndi=0.2,
            iv_score=85.0,
        )
        assert s.iv_score == 85.0
        assert s.person_fe == 0.5
        assert s.dormancy == 0.95
        assert s.ndi == 0.2
