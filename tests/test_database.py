"""database モジュールのテスト."""

import pytest

from src.database import (
    db_connection,
    ensure_calc_execution_records,
    get_calc_execution_hashes,
    get_connection,
    init_db,
    insert_credit,
    load_all_anime,
    load_all_credits,
    load_all_persons,
    load_all_scores,
    record_calc_execution,
    register_meta_lineage,
    search_persons,
    upsert_meta_entity_resolution_audit,
    upsert_anime,
    upsert_person,
    upsert_score,
)
from src.models import BronzeAnime as Anime, Credit, Person, Role, ScoreResult


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
        assert "person_scores" in table_names


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
        anime = Anime(id="mal:1", title_ja="千と千尋の神隠し", year=2001, mal_id=199)
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
            ScoreResult(
                person_id="mal:p1", birank=80.0, patronage=60.0, person_fe=70.0
            ),
        )
        db_conn.commit()

        scores = load_all_scores(db_conn)
        assert len(scores) == 1
        assert scores[0].birank == 80.0


class TestDbConnection:
    """db_connection() コンテキストマネージャのテスト."""

    def test_auto_commit_on_success(self, tmp_path):
        db_path = tmp_path / "ctx_test.db"
        with db_connection(db_path) as conn:
            init_db(conn)
            upsert_person(conn, Person(id="mal:p1", name_ja="テスト"))

        # Verify data persisted by opening a new connection
        conn2 = get_connection(db_path)
        row = conn2.execute(
            "SELECT name_ja FROM persons WHERE id = 'mal:p1'"
        ).fetchone()
        conn2.close()
        assert row is not None
        assert row["name_ja"] == "テスト"

    def test_rollback_on_exception(self, tmp_path):
        db_path = tmp_path / "ctx_test.db"

        # First: create table
        with db_connection(db_path) as conn:
            init_db(conn)

        # Then: insert that fails mid-way
        with pytest.raises(ValueError, match="intentional"):
            with db_connection(db_path) as conn:
                upsert_person(conn, Person(id="mal:p2", name_ja="ロールバック"))
                raise ValueError("intentional")

        # Verify data was rolled back
        conn2 = get_connection(db_path)
        row = conn2.execute("SELECT * FROM persons WHERE id = 'mal:p2'").fetchone()
        conn2.close()
        assert row is None

    def test_connection_closed_after_context(self, tmp_path):
        db_path = tmp_path / "ctx_test.db"
        with db_connection(db_path) as conn:
            init_db(conn)

        # Connection should be closed — executing should raise
        with pytest.raises(Exception):
            conn.execute("SELECT 1")


class TestMetaLineageAndAudit:
    def test_register_meta_lineage_persists_extended_fields(self, db_conn):
        register_meta_lineage(
            db_conn,
            table_name="meta_policy_attrition",
            audience="policy",
            source_silver_tables=["credits", "persons"],
            formula_version="v2.1",
            description="Policy attrition summary table.",
            ci_method="bootstrap_n1000",
            row_count=12,
            rng_seed=42,
            git_sha="deadbeef",
            inputs_hash="abc123",
            notes="test lineage row",
        )
        db_conn.commit()

        row = db_conn.execute(
            "SELECT description, rng_seed, git_sha, inputs_hash "
            "FROM ops_lineage WHERE table_name = 'meta_policy_attrition'"
        ).fetchone()
        assert row is not None
        assert row["description"] == "Policy attrition summary table."
        assert row["rng_seed"] == 42
        assert row["git_sha"] == "deadbeef"
        assert row["inputs_hash"] == "abc123"

    def test_upsert_meta_entity_resolution_audit_writes_rows_and_lineage(self, db_conn):
        upsert_person(db_conn, Person(id="p_canon", name_ja="正規名"))
        count = upsert_meta_entity_resolution_audit(
            db_conn,
            [
                {
                    "person_id": "p_canon",
                    "canonical_name": "正規名",
                    "merge_method": "exact_match",
                    "merge_confidence": 0.98,
                    "merged_from_keys": '["p_dup1","p_dup2"]',
                    "merge_evidence": "2 aliases merged",
                    "reviewed_by": None,
                    "reviewed_at": None,
                }
            ],
        )
        db_conn.commit()

        assert count == 1
        audit_row = db_conn.execute(
            "SELECT canonical_name, merge_method, merge_confidence "
            "FROM ops_entity_resolution_audit WHERE person_id = 'p_canon'"
        ).fetchone()
        assert audit_row is not None
        assert audit_row["canonical_name"] == "正規名"
        assert audit_row["merge_method"] == "exact_match"
        assert audit_row["merge_confidence"] == 0.98

        lineage_row = db_conn.execute(
            "SELECT table_name FROM ops_lineage "
            "WHERE table_name = 'ops_entity_resolution_audit'"
        ).fetchone()
        assert lineage_row is not None


class TestCalcExecutionRecords:
    def test_record_and_read_hashes(self, db_conn):
        ensure_calc_execution_records(db_conn)
        record_calc_execution(
            db_conn,
            scope="phase9_analysis_modules",
            calc_name="anime_stats",
            input_hash="abc123",
            output_path="result/json/anime_stats.json",
        )
        db_conn.commit()

        hashes = get_calc_execution_hashes(db_conn, "phase9_analysis_modules")
        assert hashes["anime_stats"] == "abc123"

    def test_upsert_overwrites_hash(self, db_conn):
        record_calc_execution(
            db_conn,
            scope="phase9_analysis_modules",
            calc_name="anime_stats",
            input_hash="h1",
        )
        record_calc_execution(
            db_conn,
            scope="phase9_analysis_modules",
            calc_name="anime_stats",
            input_hash="h2",
        )
        db_conn.commit()

        hashes = get_calc_execution_hashes(db_conn, "phase9_analysis_modules")
        assert hashes["anime_stats"] == "h2"


class TestSearchPersons:
    @pytest.fixture
    def populated_conn(self, db_conn):
        persons = [
            Person(id="p1", name_ja="宮崎駿", name_en="Hayao Miyazaki"),
            Person(id="p2", name_ja="김철수", name_ko="김철수", name_en="Kim Cheolsu"),
            Person(id="p3", name_zh="张三", name_en="Zhang San"),
            Person(id="p4", name_ja="田中太郎", name_en="Taro Tanaka", aliases=["たなかたろう"]),
        ]
        for p in persons:
            upsert_person(db_conn, p)
        db_conn.commit()
        return db_conn

    def test_search_by_name_ja(self, populated_conn):
        results = search_persons(populated_conn, "宮崎")
        assert any(r["id"] == "p1" for r in results)

    def test_search_by_name_en(self, populated_conn):
        results = search_persons(populated_conn, "Miyazaki")
        assert any(r["id"] == "p1" for r in results)

    def test_search_by_name_ko(self, populated_conn):
        results = search_persons(populated_conn, "김철수")
        assert any(r["id"] == "p2" for r in results)

    def test_search_by_name_zh(self, populated_conn):
        results = search_persons(populated_conn, "张三")
        assert any(r["id"] == "p3" for r in results)

    def test_search_by_alias(self, populated_conn):
        results = search_persons(populated_conn, "たなかたろう")
        assert any(r["id"] == "p4" for r in results)

    def test_result_includes_multilang_fields(self, populated_conn):
        results = search_persons(populated_conn, "김철수")
        assert len(results) >= 1
        row = next(r for r in results if r["id"] == "p2")
        assert "name_ko" in row
        assert "name_zh" in row

    def test_no_match_returns_empty(self, populated_conn):
        results = search_persons(populated_conn, "존재하지않는이름xyz")
        assert results == []
