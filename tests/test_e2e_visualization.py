"""End-to-end 可視化テスト — パイプライン + 可視化.

合成データで可視化付きパイプラインを実行し、
PNG ファイルが正しく生成されることを検証する。
"""

import pytest

from src.synthetic import generate_synthetic_data


@pytest.fixture
def viz_pipeline(monkeypatch, tmp_path):
    """可視化付きパイプラインを実行するフィクスチャ."""
    import src.analysis.visualize
    import src.database
    import src.pipeline
    import src.utils.config

    # DB/JSON/result を一時ディレクトリに差し替え
    db_path = tmp_path / "viz.db"
    json_dir = tmp_path / "json"
    json_dir.mkdir()
    result_dir = tmp_path / "result"
    result_dir.mkdir()

    monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", db_path)
    monkeypatch.setattr(src.pipeline, "JSON_DIR", json_dir)
    # visualize.py もモジュールレベルで JSON_DIR をインポートしているため直接差し替え
    monkeypatch.setattr(src.utils.config, "JSON_DIR", json_dir)
    monkeypatch.setattr(src.analysis.visualize, "JSON_DIR", json_dir)

    # 合成データをDBに投入
    from src.database import get_connection, init_db, insert_credit, upsert_anime, upsert_person

    persons, anime_list, credits = generate_synthetic_data(
        n_directors=5, n_animators=30, n_anime=15, seed=42,
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

    # 可視化付きでパイプライン実行
    from src.pipeline import run_scoring_pipeline

    results = run_scoring_pipeline(visualize=True, dry_run=False)

    return {
        "results": results,
        "json_dir": json_dir,
    }


class TestVisualizationOutputs:
    def test_score_distribution_png(self, viz_pipeline):
        # visualize.py saves to JSON_DIR.parent / "*.png"
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "score_distribution.png"
        assert png.exists(), f"Expected {png} to exist"
        assert png.stat().st_size > 0

    def test_top_radar_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "top_radar.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_collaboration_network_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "collaboration_network.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_growth_trends_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "growth_trends.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_network_evolution_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "network_evolution.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_decade_comparison_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "decade_comparison.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_role_flow_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "role_flow.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_time_series_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "time_series.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_productivity_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "productivity.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_influence_tree_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "influence_tree.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_milestones_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "milestones.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_collaborations_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "collaborations.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_tags_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "tags.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_transitions_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "transitions.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_anime_stats_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "anime_stats.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_genre_affinity_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "genre_affinity.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_crossval_png(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        png = parent / "crossval.png"
        assert png.exists()
        assert png.stat().st_size > 0

    def test_dashboard_html(self, viz_pipeline):
        json_dir = viz_pipeline["json_dir"]
        parent = json_dir.parent
        dashboard = parent / "dashboard.html"
        assert dashboard.exists(), f"Expected {dashboard} to exist"
        html = dashboard.read_text()
        assert "Dashboard" in html
        assert "data:image/png;base64," in html
        assert "ネットワーク" in html
