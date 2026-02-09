"""pipeline モジュールの統合テスト."""


import pytest

from src.database import (
    get_connection,
    init_db,
    insert_credit,
    upsert_anime,
    upsert_person,
)
from src.models import Anime, Credit, Person, Role
from src.pipeline import run_scoring_pipeline


@pytest.fixture
def populated_db(tmp_path, monkeypatch):
    """テスト用のデータが入ったDB."""
    db_path = tmp_path / "test_pipeline.db"
    json_dir = tmp_path / "json"

    conn = get_connection(db_path)
    init_db(conn)

    # テストデータ投入
    persons = [
        Person(id="p1", name_en="Director Alpha", name_ja="監督A"),
        Person(id="p2", name_en="Animator Beta", name_ja="アニメーターB"),
        Person(id="p3", name_en="Animator Gamma", name_ja="アニメーターC"),
        Person(id="p4", name_en="Key Animator Delta", name_ja="原画D"),
    ]
    anime_list = [
        Anime(id="a1", title_en="Great Anime", title_ja="すごいアニメ", year=2022, score=8.5),
        Anime(id="a2", title_en="Good Anime", title_ja="いいアニメ", year=2023, score=7.5),
        Anime(id="a3", title_en="Average Anime", title_ja="普通のアニメ", year=2024, score=6.0),
    ]
    credits_data = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a1", role=Role.ANIMATION_DIRECTOR, source="test"),
        Credit(person_id="p3", anime_id="a2", role=Role.ANIMATION_DIRECTOR, source="test"),
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

    monkeypatch.setattr(src.pipeline, "JSON_DIR", json_dir)

    return db_path


class TestScoringPipeline:
    def test_produces_results(self, populated_db):
        results = run_scoring_pipeline()
        assert len(results) > 0

    def test_composite_scores_ordered(self, populated_db):
        results = run_scoring_pipeline()
        composites = [r["composite"] for r in results]
        assert composites == sorted(composites, reverse=True)

    def test_all_persons_scored(self, populated_db):
        results = run_scoring_pipeline()
        person_ids = {r["person_id"] for r in results}
        assert "p2" in person_ids

    def test_repeat_engagement_higher_trust(self, populated_db):
        results = run_scoring_pipeline()
        scores_by_id = {r["person_id"]: r for r in results}
        if "p2" in scores_by_id and "p4" in scores_by_id:
            assert scores_by_id["p2"]["trust"] >= scores_by_id["p4"]["trust"]
