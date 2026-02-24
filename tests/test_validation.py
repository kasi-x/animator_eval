"""validation モジュールのテスト."""

import sqlite3

import pytest

from src.database import init_db, insert_credit, upsert_anime, upsert_person
from src.models import Anime, Credit, Person, Role
from src.validation import (
    ValidationResult,
    validate_all,
    validate_credit_distribution,
    validate_data_completeness,
    validate_data_freshness,
    validate_referential_integrity,
)


@pytest.fixture
def test_conn(tmp_path):
    """テスト用DB接続."""
    db_path = tmp_path / "test_validation.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=OFF")  # テスト用に外部キー無効化
    init_db(conn)
    return conn


@pytest.fixture
def populated_conn(test_conn):
    """データ入りのテスト用DB."""
    persons = [
        Person(id="p1", name_en="Director A", name_ja="監督A"),
        Person(id="p2", name_en="Animator B", name_ja=""),
        Person(id="p3", name_en="", name_ja=""),  # 名前なし
    ]
    anime_list = [
        Anime(id="a1", title_en="Anime One", title_ja="アニメ1", year=2023, score=8.0),
        Anime(id="a2", title_en="Anime Two", title_ja="", year=None, score=None),
        Anime(id="a3", title_en="", title_ja="", year=2024, score=7.0),  # タイトルなし
    ]
    credits_data = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
    ]
    for p in persons:
        upsert_person(test_conn, p)
    for a in anime_list:
        upsert_anime(test_conn, a)
    for c in credits_data:
        insert_credit(test_conn, c)
    test_conn.commit()
    return test_conn


class TestValidationResult:
    def test_default_passed(self):
        r = ValidationResult()
        assert r.passed is True

    def test_add_error_sets_failed(self):
        r = ValidationResult()
        r.add_error("something broke")
        assert r.passed is False
        assert len(r.errors) == 1

    def test_add_warning_keeps_passed(self):
        r = ValidationResult()
        r.add_warning("minor issue")
        assert r.passed is True
        assert len(r.warnings) == 1


class TestReferentialIntegrity:
    def test_clean_data_passes(self, populated_conn):
        result = validate_referential_integrity(populated_conn)
        assert result.passed

    def test_orphan_person_credit(self, populated_conn):
        populated_conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, source) VALUES ('ghost', 'a1', 'other', 'test')"
        )
        populated_conn.commit()
        result = validate_referential_integrity(populated_conn)
        assert not result.passed
        assert result.stats["orphan_person_credits"] == 1

    def test_orphan_anime_credit(self, populated_conn):
        populated_conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, source) VALUES ('p1', 'ghost_anime', 'other', 'test')"
        )
        populated_conn.commit()
        result = validate_referential_integrity(populated_conn)
        assert not result.passed
        assert result.stats["orphan_anime_credits"] == 1


class TestDataCompleteness:
    def test_detects_nameless_persons(self, populated_conn):
        result = validate_data_completeness(populated_conn)
        assert result.stats["nameless_persons"] == 1  # p3 has no name

    def test_detects_titleless_anime(self, populated_conn):
        result = validate_data_completeness(populated_conn)
        assert result.stats["titleless_anime"] == 1  # a3 has no title

    def test_detects_no_year(self, populated_conn):
        result = validate_data_completeness(populated_conn)
        assert result.stats["no_year_anime"] == 1  # a2 has no year


class TestCreditDistribution:
    def test_detects_persons_without_credits(self, populated_conn):
        result = validate_credit_distribution(populated_conn)
        # p3 has no credits
        assert result.stats["persons_without_credits"] == 1

    def test_detects_anime_without_credits(self, populated_conn):
        result = validate_credit_distribution(populated_conn)
        # a3 has no credits
        assert result.stats["anime_without_credits"] == 1


class TestCreditQuality:
    def test_clean_data_no_warnings(self, populated_conn):
        from src.validation import validate_credit_quality

        result = validate_credit_quality(populated_conn)
        assert result.stats["multi_role_pairs"] == 0

    def test_detects_multi_role(self, populated_conn):
        from src.validation import validate_credit_quality

        # Add 5 different roles for same person-anime pair
        for role in [
            "director",
            "storyboard",
            "screenplay",
            "key_animator",
            "animation_director",
        ]:
            populated_conn.execute(
                "INSERT OR IGNORE INTO credits (person_id, anime_id, role, raw_role, source) VALUES ('p1', 'a1', ?, ?, 'test')",
                (role, role),
            )
        populated_conn.commit()
        result = validate_credit_quality(populated_conn)
        assert result.stats["multi_role_pairs"] >= 1


class TestDataFreshness:
    def test_fresh_data_no_warnings(self, populated_conn):
        # populated_conn has anime from 2023/2024 — fresh enough
        result = validate_data_freshness(populated_conn, stale_years=5)
        stale_warns = [w for w in result.warnings if "stale_data" in w]
        assert len(stale_warns) == 0

    def test_stale_data_detected(self, test_conn):
        upsert_anime(test_conn, Anime(id="old1", title_en="Old Show", year=2010))
        insert_credit(
            test_conn,
            Credit(
                person_id="p1", anime_id="old1", role=Role.DIRECTOR, source="old_src"
            ),
        )
        upsert_person(test_conn, Person(id="p1", name_en="Old Director"))
        test_conn.commit()
        result = validate_data_freshness(test_conn, stale_years=5)
        stale_warns = [w for w in result.warnings if "stale_data" in w]
        assert len(stale_warns) == 1

    def test_latest_anime_year_tracked(self, populated_conn):
        result = validate_data_freshness(populated_conn)
        assert result.stats["latest_anime_year"] == 2024

    def test_empty_db(self, test_conn):
        result = validate_data_freshness(test_conn)
        assert result.passed


class TestValidateAll:
    def test_combines_results(self, populated_conn):
        result = validate_all(populated_conn)
        # Should have warnings but pass (no orphan credits)
        assert result.passed
        assert len(result.warnings) > 0

    def test_empty_db_passes(self, test_conn):
        result = validate_all(test_conn)
        assert result.passed
