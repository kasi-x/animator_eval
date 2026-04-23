"""統合テスト — 合成データでパイプライン全体を通す."""

import pytest

from src.db import (
    get_connection,
    init_db,
    insert_credit,
    upsert_anime,
    upsert_person,
)
from src.synthetic import generate_synthetic_data


# ---------------------------------------------------------------------------
# Class-scoped shared fixture — pipeline runs ONCE for the whole class
# ---------------------------------------------------------------------------

@pytest.fixture(scope="class")
def synthetic_db(tmp_path_factory):
    """合成データ入りのテストDB (class-scoped)。各クラスで1回だけ初期化される."""
    tmp_path = tmp_path_factory.mktemp("integration")
    db_path = tmp_path / "integration.db"
    json_dir = tmp_path / "json"

    import src.database
    import src.pipeline
    import src.utils.config
    import src.utils.json_io

    orig_db = src.database.DEFAULT_DB_PATH
    orig_pipeline = src.pipeline.JSON_DIR
    orig_config = src.utils.config.JSON_DIR
    orig_json_io = src.utils.json_io.JSON_DIR

    src.database.DEFAULT_DB_PATH = db_path
    src.pipeline.JSON_DIR = json_dir
    src.utils.config.JSON_DIR = json_dir
    src.utils.json_io.JSON_DIR = json_dir

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

    from tests.conftest import build_silver_duckdb
    import src.analysis.silver_reader
    import src.analysis.gold_writer

    silver_path = tmp_path / "silver.duckdb"
    gold_path = tmp_path / "gold.duckdb"
    build_silver_duckdb(silver_path, persons, anime_list, credits)

    orig_silver = src.analysis.silver_reader.DEFAULT_SILVER_PATH
    orig_gold = src.analysis.gold_writer.DEFAULT_GOLD_DB_PATH
    src.analysis.silver_reader.DEFAULT_SILVER_PATH = silver_path
    src.analysis.gold_writer.DEFAULT_GOLD_DB_PATH = gold_path

    yield db_path

    src.database.DEFAULT_DB_PATH = orig_db
    src.pipeline.JSON_DIR = orig_pipeline
    src.utils.config.JSON_DIR = orig_config
    src.utils.json_io.JSON_DIR = orig_json_io
    src.analysis.silver_reader.DEFAULT_SILVER_PATH = orig_silver
    src.analysis.gold_writer.DEFAULT_GOLD_DB_PATH = orig_gold


@pytest.fixture(scope="class")
def pipeline_results(synthetic_db):
    """パイプラインを1回だけ実行し、結果をクラス全体でキャッシュする."""
    from src.pipeline import run_scoring_pipeline

    return run_scoring_pipeline()


# ---------------------------------------------------------------------------
# Function-scoped fixture for tests that need a pristine (no-run) DB
# ---------------------------------------------------------------------------

@pytest.fixture()
def synthetic_db_fresh(monkeypatch, tmp_path):
    """合成データ入りのテストDB (function-scoped)。パイプライン未実行の状態が必要なテスト用."""
    db_path = tmp_path / "integration.db"
    json_dir = tmp_path / "json"

    import src.database
    import src.pipeline
    import src.utils.config
    import src.utils.json_io

    monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)
    monkeypatch.setattr(src.pipeline, "JSON_DIR", json_dir)
    monkeypatch.setattr(src.utils.config, "JSON_DIR", json_dir)
    monkeypatch.setattr(src.utils.json_io, "JSON_DIR", json_dir)

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

    from tests.conftest import build_silver_duckdb
    import src.analysis.silver_reader
    import src.analysis.gold_writer

    silver_path = tmp_path / "silver.duckdb"
    gold_path = tmp_path / "gold.duckdb"
    build_silver_duckdb(silver_path, persons, anime_list, credits)
    monkeypatch.setattr(src.analysis.silver_reader, "DEFAULT_SILVER_PATH", silver_path)
    monkeypatch.setattr(src.analysis.gold_writer, "DEFAULT_GOLD_DB_PATH", gold_path)

    return db_path


# ---------------------------------------------------------------------------
# Main test class — all tests share one pipeline run (~15s total, not 12×15s)
# ---------------------------------------------------------------------------

class TestFullPipeline:
    def test_pipeline_produces_scores(self, pipeline_results):
        assert len(pipeline_results) > 0

    def test_multiple_components_nonzero(self, pipeline_results):
        """少なくとも一部の人物が複数コンポーネントで非ゼロスコアを持つ."""
        has_multiple = any(r["birank"] > 0 and r["person_fe"] != 0 for r in pipeline_results)
        assert has_multiple

    def test_iv_score_ordering(self, pipeline_results):
        """iv_score が降順にソートされている."""
        iv_scores = [r["iv_score"] for r in pipeline_results]
        assert iv_scores == sorted(iv_scores, reverse=True)

    def test_scores_in_db(self, synthetic_db, pipeline_results):
        """スコアがgold.duckdbに保存される."""
        from src.analysis.gold_writer import gold_connect

        with gold_connect() as conn:
            scores = conn.execute("SELECT COUNT(*) FROM person_scores").fetchone()[0]
        assert scores > 0

    def test_report_generation(self, pipeline_results, tmp_path):
        """レポート生成が正常に完了する."""
        from src.report import (
            generate_csv_report,
            generate_json_report,
            generate_text_report,
        )

        json_path = generate_json_report(pipeline_results, output_path=tmp_path / "r.json")
        text_path = generate_text_report(pipeline_results, output_path=tmp_path / "r.txt")
        csv_path = generate_csv_report(pipeline_results, output_path=tmp_path / "r.csv")

        assert json_path.exists()
        assert text_path.exists()
        assert csv_path.exists()

    def test_centrality_metrics_included(self, pipeline_results):
        """中心性指標が結果に含まれる."""
        has_centrality = any("centrality" in r for r in pipeline_results)
        assert has_centrality

    def test_percentile_ranks_included(self, pipeline_results):
        """パーセンタイルランクが結果に含まれる."""
        for r in pipeline_results:
            assert "iv_score_pct" in r
            assert 0 <= r["iv_score_pct"] <= 100

    def test_role_classification_included(self, pipeline_results):
        """役職分類が結果に含まれる."""
        has_role = any("primary_role" in r for r in pipeline_results)
        assert has_role
        for r in pipeline_results:
            if "primary_role" in r:
                assert r["primary_role"] in (
                    "director",
                    "animator",
                    "designer",
                    "technical",
                    "production",
                    "writing",
                    "other",
                )

    def test_career_data_included(self, pipeline_results):
        """キャリアデータが結果に含まれる."""
        has_career = any("career" in r for r in pipeline_results)
        assert has_career
        for r in pipeline_results:
            if "career" in r:
                c = r["career"]
                assert "first_year" in c
                assert "latest_year" in c
                assert "active_years" in c
                assert "highest_stage" in c
                assert c["highest_stage"] >= 1

    def test_score_breakdown_included(self, pipeline_results):
        """スコアブレークダウンが結果に含まれる."""
        has_breakdown = any("breakdown" in r for r in pipeline_results)
        assert has_breakdown
        for r in pipeline_results:
            if "breakdown" in r:
                bd = r["breakdown"]
                assert any(k in bd for k in ("birank", "patronage", "person_fe"))
                if "birank" in bd:
                    for factor in bd["birank"]:
                        assert "title" in factor
                        assert "role" in factor
                if "patronage" in bd:
                    for factor in bd["patronage"]:
                        assert "director_id" in factor
                        assert "shared_works" in factor

    def test_structural_components_included(self, pipeline_results):
        """新しい構造推定コンポーネントが結果に含まれる."""
        for r in pipeline_results:
            assert "iv_score" in r
            assert "birank" in r
            assert "person_fe" in r
            assert "dormancy" in r
            assert "awcc" in r
            assert 0 <= r["dormancy"] <= 1.0

    def test_iv_score_percentiles_included(self, pipeline_results):
        """IVスコアのパーセンタイルが結果に含まれる."""
        for r in pipeline_results:
            assert "iv_score_pct" in r
            assert 0 <= r["iv_score_pct"] <= 100


# ---------------------------------------------------------------------------
# Dry-run test — needs pristine DB (no prior pipeline run)
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_returns_empty(self, synthetic_db_fresh):
        """dry-run モードではスコア計算を行わず空リストを返す."""
        from src.pipeline import run_scoring_pipeline

        results = run_scoring_pipeline(dry_run=True)
        assert results == []

        conn = get_connection()
        count = conn.execute("SELECT COUNT(*) FROM person_scores").fetchone()[0]
        conn.close()
        assert count == 0
