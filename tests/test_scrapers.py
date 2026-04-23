"""スクレイパーのパーサーテスト（APIは呼ばない）."""

from src.models import Role
from src.scrapers.anilist_scraper import parse_anilist_anime, parse_anilist_staff
from src.scrapers.mal_scraper import parse_anime_data, parse_staff_data


class TestAniListParser:
    def test_parse_anime(self):
        raw = {
            "id": 16498,
            "title": {
                "romaji": "Shingeki no Kyojin",
                "english": "Attack on Titan",
                "native": "進撃の巨人",
            },
            "seasonYear": 2013,
            "season": "SPRING",
            "episodes": 25,
            "averageScore": 84,
        }
        anime = parse_anilist_anime(raw)
        assert anime.id == "anilist:16498"
        assert anime.title_ja == "進撃の巨人"
        assert anime.title_en == "Attack on Titan"
        assert anime.year == 2013
        assert anime.season == "spring"
        assert anime.score == 8.4

    def test_parse_anime_null_native(self):
        raw = {
            "id": 999,
            "title": {"romaji": "Test", "english": None, "native": None},
            "averageScore": None,
        }
        anime = parse_anilist_anime(raw)
        assert anime.title_ja == ""
        assert anime.title_en == "Test"
        assert anime.score is None

    def test_parse_staff(self):
        edges = [
            {
                "role": "Director",
                "node": {
                    "id": 100,
                    "name": {"full": "Tetsuro Araki", "native": "荒木哲郎"},
                    "homeTown": "東京都",  # hometown needed: pure kanji is zh_or_ja ambiguous
                },
            },
            {
                "role": "Key Animation",
                "node": {"id": 200, "name": {"full": "Arifumi Imai", "native": None}},
            },
        ]
        persons, credits = parse_anilist_staff(edges, "anilist:16498")
        assert len(persons) == 2
        assert persons[0].name_ja == "荒木哲郎"
        assert persons[1].name_ja == ""  # None → ""
        assert credits[0].role == Role.DIRECTOR
        assert credits[1].role == Role.KEY_ANIMATOR


class TestMALParser:
    def test_parse_anime(self):
        raw = {
            "mal_id": 5114,
            "titles": [
                {"type": "Default", "title": "Fullmetal Alchemist: Brotherhood"},
                {"type": "Japanese", "title": "鋼の錬金術師 FULLMETAL ALCHEMIST"},
            ],
            "title": "Fullmetal Alchemist: Brotherhood",
            "year": 2009,
            "season": "spring",
            "episodes": 64,
            "score": 9.1,
            "aired": {"prop": {"from": {"year": 2009}}},
        }
        anime = parse_anime_data(raw)
        assert anime.id == "mal:5114"
        assert anime.title_ja == "鋼の錬金術師 FULLMETAL ALCHEMIST"
        assert anime.year == 2009

    def test_parse_staff(self):
        staff_list = [
            {
                "person": {"mal_id": 1, "name": "Irie, Yasuhiro"},
                "positions": ["Director", "Episode Director"],
            },
            {
                "person": {"mal_id": 2, "name": "Takahashi, Rumiko"},
                "positions": ["Original Creator"],
            },
        ]
        persons, credits = parse_staff_data(staff_list, "mal:5114")
        assert len(persons) == 2
        assert persons[0].name_en == "Yasuhiro Irie"
        assert credits[0].role == Role.DIRECTOR
        assert credits[1].role == Role.EPISODE_DIRECTOR

    def test_parse_staff_missing_mal_id(self):
        staff_list = [{"person": {}, "positions": ["Director"]}]
        persons, credits = parse_staff_data(staff_list, "mal:1")
        assert len(persons) == 0
        assert len(credits) == 0
