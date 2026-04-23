"""pipeline モジュールの統合テスト."""

import time
from pathlib import Path

import pytest

from src.database import (
    get_connection,
    has_credits_changed_since_last_run,
    init_db,
    insert_credit,
    record_pipeline_run,
    upsert_anime,
    upsert_person,
)
from src.models import BronzeAnime as Anime, Credit, Person, Role
from src.pipeline import run_scoring_pipeline


@pytest.fixture
def populated_db(tmp_path, monkeypatch):
    """テスト用のデータが入ったDB."""
    db_path = tmp_path / "test_pipeline.db"
    json_dir = tmp_path / "json"

    conn = get_connection(db_path)
    init_db(conn)

    # テストデータ投入 (studios added for AKM)
    persons = [
        Person(id="p1", name_en="Director Alpha", name_ja="監督A"),
        Person(id="p2", name_en="Animator Beta", name_ja="アニメーターB"),
        Person(id="p3", name_en="Animator Gamma", name_ja="アニメーターC"),
        Person(id="p4", name_en="Key Animator Delta", name_ja="原画D"),
    ]
    anime_list = [
        Anime(
            id="a1",
            title_en="Great Anime",
            title_ja="すごいアニメ",
            year=2022,
            studios=["Studio A"],
        ),
        Anime(
            id="a2",
            title_en="Good Anime",
            title_ja="いいアニメ",
            year=2023,
            studios=["Studio B"],
        ),
        Anime(
            id="a3",
            title_en="Average Anime",
            title_ja="普通のアニメ",
            year=2024,
            studios=["Studio A"],
        ),
    ]
    credits_data = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(
            person_id="p3", anime_id="a1", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(
            person_id="p3", anime_id="a2", role=Role.ANIMATION_DIRECTOR, source="test"
        ),
        Credit(person_id="p4", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
    ]

    for p in persons:
        upsert_person(conn, p)
    for a in anime_list:
        upsert_anime(conn, a)
    for c in credits_data:
        insert_credit(conn, c)
    conn.commit()
    conn.close()

    # get_connection をパッチしてテストDBを使う
    import src.database

    monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)

    import src.pipeline
    import src.utils.config
    import src.utils.json_io

    monkeypatch.setattr(src.pipeline, "JSON_DIR", json_dir)
    # export_and_viz.py が実行時に src.utils.config.JSON_DIR を import するため
    # 本番ディレクトリへの書き込みを防ぐために必須
    monkeypatch.setattr(src.utils.config, "JSON_DIR", json_dir)
    monkeypatch.setattr(src.utils.json_io, "JSON_DIR", json_dir)

    return db_path


class TestScoringPipeline:
    def test_produces_results(self, populated_db):
        results = run_scoring_pipeline()
        assert len(results) > 0

    def test_iv_scores_ordered(self, populated_db):
        results = run_scoring_pipeline()
        iv_scores = [r["iv_score"] for r in results]
        assert iv_scores == sorted(iv_scores, reverse=True)

    def test_all_persons_scored(self, populated_db):
        results = run_scoring_pipeline()
        person_ids = {r["person_id"] for r in results}
        assert "p2" in person_ids

    def test_structural_fields_present(self, populated_db):
        results = run_scoring_pipeline()
        for r in results:
            assert "iv_score" in r
            assert "person_fe" in r
            assert "birank" in r
            assert "dormancy" in r

    def test_records_meta_quality_snapshot(self, populated_db):
        run_scoring_pipeline()
        conn = get_connection()
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM meta_quality_snapshot"
            ).fetchone()[0]
            assert count > 0
        finally:
            conn.close()

    def test_phase9_skips_recompute_when_hash_unchanged(self, populated_db):
        run_scoring_pipeline()

        conn = get_connection()
        try:
            first = conn.execute(
                """
                SELECT input_hash, computed_at, output_path
                FROM calc_execution_records
                WHERE scope = 'phase9_analysis_modules'
                  AND calc_name = 'anime_stats'
                """
            ).fetchone()
            assert first is not None
        finally:
            conn.close()

        output_path = Path(first["output_path"])
        assert output_path.exists()
        first_mtime = output_path.stat().st_mtime

        time.sleep(1.1)
        run_scoring_pipeline()

        conn = get_connection()
        try:
            second = conn.execute(
                """
                SELECT input_hash, computed_at, output_path
                FROM calc_execution_records
                WHERE scope = 'phase9_analysis_modules'
                  AND calc_name = 'anime_stats'
                """
            ).fetchone()
            assert second is not None
        finally:
            conn.close()

        second_mtime = output_path.stat().st_mtime
        assert second["input_hash"] == first["input_hash"]
        assert second["computed_at"] == first["computed_at"]
        assert second_mtime == first_mtime


class TestIncrementalPipeline:
    """incremental モードのテスト."""

    def test_incremental_first_run_executes_full(self, populated_db):
        """初回実行時はフルパイプラインが走る."""
        results = run_scoring_pipeline(incremental=True)
        assert len(results) > 0

    def test_incremental_skip_when_no_changes(self, populated_db):
        """データ変化なしなら2回目はキャッシュを返す."""
        # First run: full pipeline
        results1 = run_scoring_pipeline(incremental=True)
        assert len(results1) > 0

        # Second run: should skip and return cached
        results2 = run_scoring_pipeline(incremental=True)
        assert len(results2) > 0
        # Same number of results
        assert len(results2) == len(results1)

    def test_incremental_runs_after_new_credits(self, populated_db):
        """新クレジット追加後はフルパイプラインが走る."""
        run_scoring_pipeline(incremental=True)

        # Add a new credit
        conn = get_connection()
        insert_credit(
            conn,
            Credit(person_id="p4", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        )
        conn.commit()
        conn.close()

        results2 = run_scoring_pipeline(incremental=True)
        assert len(results2) > 0

    def test_non_incremental_always_runs(self, populated_db):
        """incremental=False なら常にフルパイプラインが走る."""
        results1 = run_scoring_pipeline(incremental=False)
        results2 = run_scoring_pipeline(incremental=False)
        assert len(results1) > 0
        assert len(results2) > 0


class TestHasCreditsChanged:
    """has_credits_changed_since_last_run のユニットテスト."""

    def test_no_previous_run_returns_true(self, populated_db):
        conn = get_connection()
        assert has_credits_changed_since_last_run(conn) is True
        conn.close()

    def test_same_counts_returns_false(self, populated_db):
        conn = get_connection()
        # Record a run matching current state
        credit_count = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
        person_count = conn.execute(
            "SELECT COUNT(DISTINCT person_id) FROM credits"
        ).fetchone()[0]
        record_pipeline_run(conn, credit_count, person_count, 1.0)
        conn.commit()

        assert has_credits_changed_since_last_run(conn) is False
        conn.close()

    def test_new_credit_returns_true(self, populated_db):
        conn = get_connection()
        # Record current state
        credit_count = conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0]
        person_count = conn.execute(
            "SELECT COUNT(DISTINCT person_id) FROM credits"
        ).fetchone()[0]
        record_pipeline_run(conn, credit_count, person_count, 1.0)
        conn.commit()

        # Add new credit
        insert_credit(
            conn,
            Credit(
                person_id="p4", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"
            ),
        )
        conn.commit()

        assert has_credits_changed_since_last_run(conn) is True
        conn.close()
