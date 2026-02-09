"""database.get_db_stats のテスト."""

import sqlite3

import pytest

from src.database import (
    get_data_sources,
    get_db_stats,
    get_last_pipeline_run,
    get_schema_version,
    get_score_history,
    init_db,
    insert_credit,
    record_pipeline_run,
    save_score_history,
    update_data_source,
    upsert_anime,
    upsert_person,
    SCHEMA_VERSION,
)
from src.models import Anime, Credit, Person, Role, ScoreResult


@pytest.fixture
def stats_conn(tmp_path):
    """統計テスト用DB."""
    db_path = tmp_path / "stats.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn)

    persons = [
        Person(id="p1", name_en="Person One", name_ja="人物1"),
        Person(id="p2", name_en="Person Two", name_ja="人物2"),
    ]
    anime_list = [
        Anime(id="a1", title_en="Anime One", year=2022, score=8.0),
        Anime(id="a2", title_en="Anime Two", year=2024, score=7.0),
    ]
    credits_data = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="anilist"),
    ]

    for p in persons:
        upsert_person(conn, p)
    for a in anime_list:
        upsert_anime(conn, a)
    for c in credits_data:
        insert_credit(conn, c)
    conn.commit()
    return conn


class TestGetDbStats:
    def test_table_counts(self, stats_conn):
        stats = get_db_stats(stats_conn)
        assert stats["persons_count"] == 2
        assert stats["anime_count"] == 2
        assert stats["credits_count"] == 3
        assert stats["scores_count"] == 0

    def test_year_range(self, stats_conn):
        stats = get_db_stats(stats_conn)
        assert stats["year_min"] == 2022
        assert stats["year_max"] == 2024

    def test_source_breakdown(self, stats_conn):
        stats = get_db_stats(stats_conn)
        assert stats["credits_source_test"] == 2
        assert stats["credits_source_anilist"] == 1

    def test_avg_credits(self, stats_conn):
        stats = get_db_stats(stats_conn)
        assert stats["avg_credits_per_person"] == 1.5

    def test_distinct_roles(self, stats_conn):
        stats = get_db_stats(stats_conn)
        assert stats["distinct_roles"] == 2

    def test_empty_db(self, tmp_path):
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        init_db(conn)
        stats = get_db_stats(conn)
        assert stats["persons_count"] == 0
        assert stats["credits_count"] == 0
        conn.close()


class TestDataSources:
    def test_update_and_get(self, stats_conn):
        update_data_source(stats_conn, "anilist", 100)
        stats_conn.commit()
        sources = get_data_sources(stats_conn)
        assert len(sources) == 1
        assert sources[0]["source"] == "anilist"
        assert sources[0]["item_count"] == 100
        assert sources[0]["status"] == "ok"

    def test_update_existing(self, stats_conn):
        update_data_source(stats_conn, "anilist", 100)
        update_data_source(stats_conn, "anilist", 200, status="partial")
        stats_conn.commit()
        sources = get_data_sources(stats_conn)
        assert len(sources) == 1
        assert sources[0]["item_count"] == 200
        assert sources[0]["status"] == "partial"

    def test_multiple_sources(self, stats_conn):
        update_data_source(stats_conn, "anilist", 100)
        update_data_source(stats_conn, "mal", 50)
        stats_conn.commit()
        sources = get_data_sources(stats_conn)
        assert len(sources) == 2

    def test_empty(self, tmp_path):
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        init_db(conn)
        sources = get_data_sources(conn)
        assert sources == []
        conn.close()


class TestSchemaMigration:
    def test_schema_version_set(self, stats_conn):
        version = get_schema_version(stats_conn)
        assert version == SCHEMA_VERSION

    def test_score_history_table_exists(self, stats_conn):
        """Migration v1 creates score_history table."""
        stats_conn.execute("SELECT COUNT(*) FROM score_history")

    def test_score_history_index_exists(self, stats_conn):
        """Migration v2 creates indices on score_history."""
        indices = stats_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='score_history'"
        ).fetchall()
        names = {r["name"] for r in indices}
        assert "idx_score_history_person" in names
        assert "idx_score_history_run" in names

    def test_idempotent_init(self, tmp_path):
        """init_db can be called multiple times safely."""
        db_path = tmp_path / "idem.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        init_db(conn)
        init_db(conn)
        assert get_schema_version(conn) == SCHEMA_VERSION
        conn.close()


class TestScoreHistory:
    def test_save_and_retrieve(self, stats_conn):
        score = ScoreResult(person_id="p1", authority=80.0, trust=70.0, skill=60.0)
        save_score_history(stats_conn, score)
        stats_conn.commit()

        history = get_score_history(stats_conn, "p1")
        assert len(history) == 1
        assert history[0]["authority"] == 80.0
        assert history[0]["composite"] == score.composite

    def test_multiple_runs(self, stats_conn):
        for auth in [50.0, 60.0, 70.0]:
            save_score_history(
                stats_conn,
                ScoreResult(person_id="p1", authority=auth, trust=40.0, skill=30.0),
            )
        stats_conn.commit()

        history = get_score_history(stats_conn, "p1")
        assert len(history) == 3
        # All three values present
        values = {h["authority"] for h in history}
        assert values == {50.0, 60.0, 70.0}

    def test_empty_history(self, stats_conn):
        history = get_score_history(stats_conn, "nonexistent")
        assert history == []

    def test_limit(self, stats_conn):
        for i in range(10):
            save_score_history(
                stats_conn,
                ScoreResult(person_id="p2", authority=float(i), trust=0, skill=0),
            )
        stats_conn.commit()

        history = get_score_history(stats_conn, "p2", limit=3)
        assert len(history) == 3


class TestPipelineRuns:
    def test_record_and_retrieve(self, stats_conn):
        run_id = record_pipeline_run(stats_conn, 100, 50, 5.5, "full")
        stats_conn.commit()
        assert run_id > 0

        last = get_last_pipeline_run(stats_conn)
        assert last is not None
        assert last["credit_count"] == 100
        assert last["person_count"] == 50
        assert last["elapsed_seconds"] == 5.5
        assert last["mode"] == "full"

    def test_multiple_runs_returns_latest(self, stats_conn):
        record_pipeline_run(stats_conn, 50, 20, 2.0, "full")
        record_pipeline_run(stats_conn, 100, 40, 4.0, "full")
        stats_conn.commit()

        last = get_last_pipeline_run(stats_conn)
        assert last["credit_count"] == 100

    def test_no_runs(self, tmp_path):
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        init_db(conn)
        assert get_last_pipeline_run(conn) is None
        conn.close()
