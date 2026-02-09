"""database モジュールのテスト."""


import pytest

from src.database import (
    get_connection,
    init_db,
    insert_credit,
    load_all_anime,
    load_all_credits,
    load_all_persons,
    load_all_scores,
    upsert_anime,
    upsert_person,
    upsert_score,
)
from src.models import Anime, Credit, Person, Role, ScoreResult


@pytest.fixture
def db_conn(tmp_path):
    """テスト用の一時DB接続."""
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    yield conn
    conn.close()


class TestInitDb:
    def test_creates_tables(self, db_conn):
        tables = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = {row["name"] for row in tables}
        assert "persons" in table_names
        assert "anime" in table_names
        assert "credits" in table_names
        assert "scores" in table_names


class TestPersonCrud:
    def test_upsert_and_load(self, db_conn):
        person = Person(
            id="mal:p1", name_ja="宮崎駿", name_en="Hayao Miyazaki", mal_id=1
        )
        upsert_person(db_conn, person)
        db_conn.commit()

        persons = load_all_persons(db_conn)
        assert len(persons) == 1
        assert persons[0].name_ja == "宮崎駿"
        assert persons[0].mal_id == 1

    def test_upsert_update(self, db_conn):
        p1 = Person(id="mal:p1", name_en="Miyazaki")
        upsert_person(db_conn, p1)
        db_conn.commit()

        p2 = Person(id="mal:p1", name_ja="宮崎駿", name_en="")
        upsert_person(db_conn, p2)
        db_conn.commit()

        persons = load_all_persons(db_conn)
        assert len(persons) == 1
        assert persons[0].name_ja == "宮崎駿"
        assert persons[0].name_en == "Miyazaki"  # 空文字は上書きしない


class TestAnimeCrud:
    def test_upsert_and_load(self, db_conn):
        anime = Anime(
            id="mal:1", title_ja="千と千尋の神隠し", year=2001, mal_id=199
        )
        upsert_anime(db_conn, anime)
        db_conn.commit()

        anime_list = load_all_anime(db_conn)
        assert len(anime_list) == 1
        assert anime_list[0].title_ja == "千と千尋の神隠し"
        assert anime_list[0].year == 2001


class TestCreditCrud:
    def test_insert_and_load(self, db_conn):
        upsert_person(db_conn, Person(id="mal:p1"))
        upsert_anime(db_conn, Anime(id="mal:1"))
        insert_credit(
            db_conn,
            Credit(
                person_id="mal:p1",
                anime_id="mal:1",
                role=Role.DIRECTOR,
                source="mal",
            ),
        )
        db_conn.commit()

        credits = load_all_credits(db_conn)
        assert len(credits) == 1
        assert credits[0].role == Role.DIRECTOR

    def test_duplicate_ignored(self, db_conn):
        upsert_person(db_conn, Person(id="mal:p1"))
        upsert_anime(db_conn, Anime(id="mal:1"))
        credit = Credit(
            person_id="mal:p1",
            anime_id="mal:1",
            role=Role.DIRECTOR,
            source="mal",
        )
        insert_credit(db_conn, credit)
        insert_credit(db_conn, credit)
        db_conn.commit()

        credits = load_all_credits(db_conn)
        assert len(credits) == 1


class TestScoreCrud:
    def test_upsert_and_load(self, db_conn):
        upsert_person(db_conn, Person(id="mal:p1"))
        upsert_score(
            db_conn,
            ScoreResult(person_id="mal:p1", authority=80.0, trust=60.0, skill=70.0),
        )
        db_conn.commit()

        scores = load_all_scores(db_conn)
        assert len(scores) == 1
        assert scores[0].authority == 80.0
