"""Tests for source-aware upsert_person and normalize_primary_names_by_credits."""

import json

import pytest

from src.db import (
    get_connection,
    init_db,
    normalize_primary_names_by_credits,
    upsert_person,
)
from src.runtime.models import Person


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "test.db"
    c = get_connection(db_path)
    init_db(c)
    yield c
    c.close()


def _row(conn, pid: str) -> dict:
    row = conn.execute(
        "SELECT name_ja, name_en, name_ko, name_zh, names_alt, aliases, name_priority "
        "FROM persons WHERE id = ?",
        (pid,),
    ).fetchone()
    assert row is not None, f"Person {pid!r} not found"
    return dict(row)


class TestUpsertPersonNew:
    def test_new_record_stores_priority(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="宮崎駿"), source="anilist")
        conn.commit()
        row = _row(conn, "p1")
        assert row["name_ja"] == "宮崎駿"
        assert row["name_priority"] == 3

    def test_new_record_no_source_priority_zero(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="テスト"))
        conn.commit()
        row = _row(conn, "p1")
        assert row["name_priority"] == 0

    def test_aliases_stored_on_insert(self, conn):
        p = Person(id="p1", name_ja="田中宏", aliases=["田中博"])
        upsert_person(conn, p, source="mal")
        conn.commit()
        row = _row(conn, "p1")
        aliases = json.loads(row["aliases"])
        assert "田中博" in aliases


class TestUpsertPersonUpdateHigherPriority:
    def test_higher_priority_updates_name(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="古い名前"), source="ann")
        conn.commit()
        upsert_person(conn, Person(id="p1", name_ja="新しい名前"), source="anilist")
        conn.commit()
        row = _row(conn, "p1")
        assert row["name_ja"] == "新しい名前"
        assert row["name_priority"] == 3

    def test_displaced_name_goes_to_aliases(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="旧名義"), source="ann")
        conn.commit()
        upsert_person(conn, Person(id="p1", name_ja="新名義"), source="anilist")
        conn.commit()
        row = _row(conn, "p1")
        aliases = json.loads(row["aliases"])
        assert "旧名義" in aliases
        assert row["name_ja"] == "新名義"

    def test_same_priority_updates_name(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="A"), source="mal")
        conn.commit()
        upsert_person(conn, Person(id="p1", name_ja="B"), source="seesaawiki")
        conn.commit()
        row = _row(conn, "p1")
        assert row["name_ja"] == "B"


class TestUpsertPersonUpdateLowerPriority:
    def test_lower_priority_does_not_overwrite_name(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="AniList名"), source="anilist")
        conn.commit()
        upsert_person(conn, Person(id="p1", name_ja="ANN名"), source="ann")
        conn.commit()
        row = _row(conn, "p1")
        assert row["name_ja"] == "AniList名"
        assert row["name_priority"] == 3

    def test_lower_priority_name_goes_to_aliases(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="AniList名"), source="anilist")
        conn.commit()
        upsert_person(conn, Person(id="p1", name_ja="ANN名"), source="ann")
        conn.commit()
        row = _row(conn, "p1")
        aliases = json.loads(row["aliases"])
        assert "ANN名" in aliases

    def test_lower_priority_metadata_merged(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="名前"), source="anilist")
        conn.commit()
        upsert_person(
            conn,
            Person(id="p1", blood_type="A", description="説明文"),
            source="ann",
        )
        conn.commit()
        row = conn.execute(
            "SELECT blood_type, description FROM persons WHERE id = ?", ("p1",)
        ).fetchone()
        assert row["blood_type"] == "A"
        assert row["description"] == "説明文"

    def test_no_source_never_overwrites_priority_name(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="確定名"), source="anilist")
        conn.commit()
        upsert_person(conn, Person(id="p1", name_ja="上書き試み"))
        conn.commit()
        row = _row(conn, "p1")
        assert row["name_ja"] == "確定名"


class TestUpsertPersonAliasAccumulation:
    def test_aliases_accumulate_across_updates(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="名前A", aliases=["別名1"]), source="ann")
        conn.commit()
        upsert_person(conn, Person(id="p1", name_ja="名前B", aliases=["別名2"]), source="anilist")
        conn.commit()
        row = _row(conn, "p1")
        aliases = json.loads(row["aliases"])
        # Both extra aliases and displaced old primary should be present
        assert "別名1" in aliases
        assert "別名2" in aliases
        assert "名前A" in aliases

    def test_no_duplicate_aliases(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="重複名"), source="ann")
        conn.commit()
        upsert_person(conn, Person(id="p1", name_ja="重複名"), source="anilist")
        conn.commit()
        row = _row(conn, "p1")
        aliases = json.loads(row["aliases"])
        assert aliases.count("重複名") <= 1


class TestNormalizePrimaryNamesByCredits:
    def _insert_anilist_person(self, conn, anilist_id: int, name_ja: str, name_en: str) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO src_anilist_persons (anilist_id, name_ja, name_en) "
            "VALUES (?, ?, ?)",
            (anilist_id, name_ja, name_en),
        )

    def _link_person(self, conn, person_id: str, source: str, external_id: str) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO person_external_ids (person_id, source, external_id) "
            "VALUES (?, ?, ?)",
            (person_id, source, external_id),
        )

    def _add_credits(self, conn, person_id: str, source: str, n: int) -> None:
        for i in range(n):
            conn.execute(
                "INSERT OR IGNORE INTO credits "
                "(person_id, anime_id, role, evidence_source) VALUES (?, ?, 'ANIMATOR', ?)",
                (person_id, f"anime_{source}_{i}", source),
            )

    def test_no_credits_returns_zero(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="名前"), source="anilist")
        conn.commit()
        result = normalize_primary_names_by_credits(conn)
        assert result == 0

    def test_updates_name_when_most_credited_source_differs(self, conn):
        upsert_person(conn, Person(id="p1", name_ja="古い名", anilist_id=101), source="ann")
        conn.commit()
        self._insert_anilist_person(conn, 101, "新しい名", "New Name")
        self._link_person(conn, "p1", "anilist", "101")
        self._add_credits(conn, "p1", "anilist", 5)
        conn.commit()
        updated = normalize_primary_names_by_credits(conn)
        assert updated == 1
        row = _row(conn, "p1")
        assert row["name_ja"] == "新しい名"
        aliases = json.loads(row["aliases"])
        assert "古い名" in aliases

    def test_no_update_when_name_already_matches(self, conn):
        upsert_person(
            conn,
            Person(id="p1", name_ja="同じ名", name_en="Same Name", anilist_id=101),
            source="anilist",
        )
        conn.commit()
        self._insert_anilist_person(conn, 101, "同じ名", "Same Name")
        self._link_person(conn, "p1", "anilist", "101")
        self._add_credits(conn, "p1", "anilist", 1)
        conn.commit()
        updated = normalize_primary_names_by_credits(conn)
        assert updated == 0
