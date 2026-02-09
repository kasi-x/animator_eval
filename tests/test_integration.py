"""統合テスト — 合成データでパイプライン全体を通す."""

import pytest

from src.database import (
    get_connection,
    init_db,
    insert_credit,
    upsert_anime,
    upsert_person,
)
from src.synthetic import generate_synthetic_data


@pytest.fixture()
def synthetic_db(monkeypatch, tmp_path):
    """合成データ入りのテストDBを作成する."""
    db_path = tmp_path / "integration.db"
    json_dir = tmp_path / "json"

    # DEFAULT_DB_PATH をパッチして get_connection() がテストDBを使うようにする
    import src.database
    import src.pipeline

    monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)
    monkeypatch.setattr(src.pipeline, "JSON_DIR", json_dir)

    persons, anime_list, credits = generate_synthetic_data(
        n_directors=5, n_animators=30, n_anime=15, seed=42
    )

    conn = get_connection()
    init_db(conn)
    for p in persons:
        upsert_person(conn, p)
    for a in anime_list:
        upsert_anime(conn, a)
    for c in credits:
        insert_credit(conn, c)
    conn.commit()
    conn.close()

    return db_path


class TestFullPipeline:
    def test_pipeline_produces_scores(self, synthetic_db):
        from src.pipeline import run_scoring_pipeline

        results = run_scoring_pipeline()
        assert len(results) > 0

    def test_all_three_axes_nonzero(self, synthetic_db):
        """少なくとも一部の人物が全3軸で非ゼロスコアを持つ."""
        from src.pipeline import run_scoring_pipeline

        results = run_scoring_pipeline()
        has_all_three = any(
            r["authority"] > 0 and r["trust"] > 0 and r["skill"] > 0
            for r in results
        )
        assert has_all_three

    def test_composite_ordering(self, synthetic_db):
        """composite スコアが降順にソートされている."""
        from src.pipeline import run_scoring_pipeline

        results = run_scoring_pipeline()
        composites = [r["composite"] for r in results]
        assert composites == sorted(composites, reverse=True)

    def test_scores_in_db(self, synthetic_db):
        """スコアがDBに保存される."""
        from src.pipeline import run_scoring_pipeline

        run_scoring_pipeline()

        conn = get_connection()
        scores = conn.execute("SELECT COUNT(*) FROM scores").fetchone()[0]
        conn.close()
        assert scores > 0

    def test_report_generation(self, synthetic_db, tmp_path):
        """レポート生成が正常に完了する."""
        from src.pipeline import run_scoring_pipeline
        from src.report import generate_csv_report, generate_json_report, generate_text_report

        results = run_scoring_pipeline()

        json_path = generate_json_report(results, output_path=tmp_path / "r.json")
        text_path = generate_text_report(results, output_path=tmp_path / "r.txt")
        csv_path = generate_csv_report(results, output_path=tmp_path / "r.csv")

        assert json_path.exists()
        assert text_path.exists()
        assert csv_path.exists()

    def test_centrality_metrics_included(self, synthetic_db):
        """中心性指標が結果に含まれる."""
        from src.pipeline import run_scoring_pipeline

        results = run_scoring_pipeline()
        has_centrality = any("centrality" in r for r in results)
        assert has_centrality

    def test_percentile_ranks_included(self, synthetic_db):
        """パーセンタイルランクが結果に含まれる."""
        from src.pipeline import run_scoring_pipeline

        results = run_scoring_pipeline()
        for r in results:
            assert "composite_pct" in r
            assert 0 <= r["composite_pct"] <= 100

    def test_role_classification_included(self, synthetic_db):
        """役職分類が結果に含まれる."""
        from src.pipeline import run_scoring_pipeline

        results = run_scoring_pipeline()
        has_role = any("primary_role" in r for r in results)
        assert has_role
        for r in results:
            if "primary_role" in r:
                assert r["primary_role"] in ("director", "animator", "designer", "technical", "production", "writing", "other")

    def test_dry_run_returns_empty(self, synthetic_db):
        """dry-run モードではスコア計算を行わず空リストを返す."""
        from src.pipeline import run_scoring_pipeline

        results = run_scoring_pipeline(dry_run=True)
        assert results == []

        # DB にスコアは書き込まれていない
        from src.database import get_connection

        conn = get_connection()
        count = conn.execute("SELECT COUNT(*) FROM scores").fetchone()[0]
        conn.close()
        assert count == 0

    def test_career_data_included(self, synthetic_db):
        """キャリアデータが結果に含まれる."""
        from src.pipeline import run_scoring_pipeline

        results = run_scoring_pipeline()
        has_career = any("career" in r for r in results)
        assert has_career
        for r in results:
            if "career" in r:
                c = r["career"]
                assert "first_year" in c
                assert "latest_year" in c
                assert "active_years" in c
                assert "highest_stage" in c
                assert c["highest_stage"] >= 1

    def test_score_breakdown_included(self, synthetic_db):
        """スコアブレークダウンが結果に含まれる."""
        from src.pipeline import run_scoring_pipeline

        results = run_scoring_pipeline()
        has_breakdown = any("breakdown" in r for r in results)
        assert has_breakdown
        for r in results:
            if "breakdown" in r:
                bd = r["breakdown"]
                # At least one axis should have factors
                assert any(k in bd for k in ("authority", "trust", "skill"))
                # Authority factors should have title and role
                if "authority" in bd:
                    for factor in bd["authority"]:
                        assert "title" in factor
                        assert "role" in factor
                # Trust factors should have director_id and shared_works
                if "trust" in bd:
                    for factor in bd["trust"]:
                        assert "director_id" in factor
                        assert "shared_works" in factor
