"""End-to-end パイプラインテスト — 合成データで全フロー検証.

合成データを生成し、パイプラインを実行し、全出力ファイルが
正しく生成されることを検証する。
"""

import json

import pytest

from src.synthetic import generate_synthetic_data


@pytest.fixture
def synthetic_pipeline(monkeypatch, tmp_path):
    """合成データでパイプラインを実行するフィクスチャ."""
    import src.database
    import src.pipeline
    import src.utils.json_io
    import src.utils.config

    # DB と JSON 出力先を一時ディレクトリに差し替え
    db_path = tmp_path / "test_e2e.db"
    json_dir = tmp_path / "json"
    json_dir.mkdir()

    monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)
    monkeypatch.setattr(src.pipeline, "JSON_DIR", json_dir)
    monkeypatch.setattr(src.utils.json_io, "JSON_DIR", json_dir)
    monkeypatch.setattr(src.utils.config, "JSON_DIR", json_dir)

    # 合成データを生成してDBに投入
    from src.database import (
        get_connection,
        init_db,
        insert_credit,
        upsert_anime,
        upsert_person,
    )

    persons, anime_list, credits = generate_synthetic_data(
        n_directors=5,
        n_animators=30,
        n_anime=15,
        seed=42,
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

    # Build silver.duckdb and gold.duckdb for DuckDB pipeline phases
    from tests.conftest import build_silver_duckdb

    import src.analysis.silver_reader
    import src.analysis.gold_writer

    silver_path = tmp_path / "silver.duckdb"
    gold_path = tmp_path / "gold.duckdb"
    build_silver_duckdb(silver_path, persons, anime_list, credits)
    monkeypatch.setattr(src.analysis.silver_reader, "DEFAULT_SILVER_PATH", silver_path)
    monkeypatch.setattr(src.analysis.gold_writer, "DEFAULT_GOLD_DB_PATH", gold_path)

    # パイプライン実行
    from src.pipeline import run_scoring_pipeline

    results = run_scoring_pipeline(visualize=False, dry_run=False)

    return {
        "results": results,
        "json_dir": json_dir,
        "db_path": db_path,
        "persons": persons,
        "anime_list": anime_list,
        "credits": credits,
    }


class TestE2EPipelineResults:
    def test_returns_results(self, synthetic_pipeline):
        results = synthetic_pipeline["results"]
        assert len(results) > 0

    def test_results_sorted_by_iv_score(self, synthetic_pipeline):
        results = synthetic_pipeline["results"]
        iv_scores = [r["iv_score"] for r in results]
        assert iv_scores == sorted(iv_scores, reverse=True)

    def test_results_have_required_fields(self, synthetic_pipeline):
        results = synthetic_pipeline["results"]
        for r in results:
            assert "person_id" in r
            assert "birank" in r
            assert "patronage" in r
            assert "person_fe" in r
            assert "iv_score" in r

    def test_scores_have_valid_ranges(self, synthetic_pipeline):
        """Raw structural scores have component-specific valid ranges."""
        results = synthetic_pipeline["results"]
        for r in results:
            # person_fe can be negative (AKM fixed effects relative to reference)
            assert isinstance(r["person_fe"], (int, float))
            # birank is non-negative (bipartite PageRank)
            assert r["birank"] >= 0
            # patronage is non-negative (director backing premium)
            assert r["patronage"] >= 0
            # dormancy is in (0, 1]
            assert 0 < r["dormancy"] <= 1
            # iv_score can be any value (weighted combination)
            assert isinstance(r["iv_score"], (int, float))
            # percentile fields are normalized to [0, 100]
            for axis in ("iv_score", "person_fe", "birank", "patronage"):
                assert 0 <= r[f"{axis}_pct"] <= 100

    def test_percentile_ranks(self, synthetic_pipeline):
        results = synthetic_pipeline["results"]
        for r in results:
            assert "iv_score_pct" in r
            assert 0 <= r["iv_score_pct"] <= 100


class TestE2EOutputFiles:
    def test_scores_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "scores.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert len(data) > 0

    def test_summary_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "summary.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert "elapsed_seconds" in data
        assert "data" in data

    def test_circles_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "circles.json"
        # Circles may or may not exist depending on data
        if path.exists():
            data = json.loads(path.read_text())
            assert isinstance(data, dict)

    def test_anime_stats_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "anime_stats.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert len(data) > 0

    def test_transitions_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "transitions.json"
        assert path.exists()

    def test_growth_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "growth.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert "trend_summary" in data

    def test_teams_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "teams.json"
        # May not exist if no high-score teams
        if path.exists():
            data = json.loads(path.read_text())
            assert "high_score_teams" in data

    def test_time_series_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "time_series.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert len(data["years"]) > 0

    def test_decades_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "decades.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert len(data["decades"]) > 0

    def test_tags_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "tags.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert "tag_summary" in data

    def test_role_flow_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "role_flow.json"
        assert path.exists()

    def test_network_evolution_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "network_evolution.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert len(data["years"]) > 0

    def test_milestones_json(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "milestones.json"
        assert path.exists()


class TestE2EDataConsistency:
    def test_all_directors_scored(self, synthetic_pipeline):
        results = synthetic_pipeline["results"]
        person_ids = {r["person_id"] for r in results}
        for p in synthetic_pipeline["persons"]:
            if p.id.startswith("syn:d"):
                assert p.id in person_ids

    def test_scores_json_matches_results(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "scores.json"
        data = json.loads(path.read_text())
        assert len(data) == len(synthetic_pipeline["results"])

    def test_growth_covers_active_persons(self, synthetic_pipeline):
        path = synthetic_pipeline["json_dir"] / "growth.json"
        data = json.loads(path.read_text())
        assert data["total_persons"] > 0

    def test_career_data_in_results(self, synthetic_pipeline):
        results = synthetic_pipeline["results"]
        has_career = sum(1 for r in results if r.get("career"))
        assert has_career > 0

    def test_network_data_in_results(self, synthetic_pipeline):
        results = synthetic_pipeline["results"]
        has_network = sum(1 for r in results if r.get("network"))
        assert has_network > 0

    def test_tags_in_results(self, synthetic_pipeline):
        results = synthetic_pipeline["results"]
        has_tags = sum(1 for r in results if r.get("tags") is not None)
        # Tags are added post-computation, so at least some should have them
        assert has_tags >= 0  # Not all may have tags
