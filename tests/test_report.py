"""report モジュールのテスト."""

import json

import pytest

from src.report import (
    generate_csv_report,
    generate_html_report,
    generate_json_report,
    generate_text_report,
    generate_visual_dashboard,
)


@pytest.fixture
def sample_results():
    return [
        {
            "person_id": "p1",
            "name": "Director Alpha",
            "name_ja": "監督A",
            "name_en": "Director Alpha",
            "authority": 85.0,
            "trust": 70.0,
            "skill": 60.0,
            "composite": 73.5,
        },
        {
            "person_id": "p2",
            "name": "Animator Beta",
            "name_ja": "アニメーターB",
            "name_en": "Animator Beta",
            "authority": 50.0,
            "trust": 90.0,
            "skill": 40.0,
            "composite": 61.5,
        },
    ]


class TestGenerateJsonReport:
    def test_creates_file(self, tmp_path, sample_results):
        out = tmp_path / "report.json"
        result = generate_json_report(sample_results, output_path=out)
        assert result == out
        assert out.exists()

    def test_contains_disclaimer(self, tmp_path, sample_results):
        out = tmp_path / "report.json"
        generate_json_report(sample_results, output_path=out)
        data = json.loads(out.read_text())
        assert "disclaimer_ja" in data["metadata"]
        assert "disclaimer_en" in data["metadata"]
        assert "ネットワーク" in data["metadata"]["disclaimer_ja"]

    def test_contains_rankings(self, tmp_path, sample_results):
        out = tmp_path / "report.json"
        generate_json_report(sample_results, output_path=out)
        data = json.loads(out.read_text())
        assert len(data["rankings"]) == 2

    def test_empty_results(self, tmp_path):
        out = tmp_path / "report.json"
        generate_json_report([], output_path=out)
        data = json.loads(out.read_text())
        assert data["metadata"]["total_persons"] == 0


class TestGenerateTextReport:
    def test_creates_file(self, tmp_path, sample_results):
        out = tmp_path / "report.txt"
        result = generate_text_report(sample_results, output_path=out)
        assert result == out
        assert out.exists()

    def test_contains_disclaimer(self, tmp_path, sample_results):
        out = tmp_path / "report.txt"
        generate_text_report(sample_results, output_path=out)
        text = out.read_text()
        assert "ネットワーク" in text

    def test_contains_rankings(self, tmp_path, sample_results):
        out = tmp_path / "report.txt"
        generate_text_report(sample_results, output_path=out)
        text = out.read_text()
        assert "Director Alpha" in text
        assert "Animator Beta" in text


class TestGenerateCsvReport:
    def test_creates_file(self, tmp_path, sample_results):
        out = tmp_path / "scores.csv"
        result = generate_csv_report(sample_results, output_path=out)
        assert result == out
        assert out.exists()

    def test_has_header_and_rows(self, tmp_path, sample_results):
        out = tmp_path / "scores.csv"
        generate_csv_report(sample_results, output_path=out)
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 3  # header + 2 rows
        assert "rank" in lines[0]
        assert "composite" in lines[0]
        assert "primary_role" in lines[0]
        assert "composite_pct" in lines[0]

    def test_utf8_bom(self, tmp_path, sample_results):
        """Excel 互換のための UTF-8 BOM が付いていることを確認."""
        out = tmp_path / "scores.csv"
        generate_csv_report(sample_results, output_path=out)
        raw = out.read_bytes()
        assert raw[:3] == b"\xef\xbb\xbf"  # UTF-8 BOM

    def test_career_columns_in_csv(self, tmp_path):
        """CSV にキャリアデータ列が含まれる."""
        results = [
            {
                "person_id": "p1",
                "name": "Test Person",
                "name_ja": "テスト",
                "name_en": "Test",
                "authority": 50.0,
                "trust": 50.0,
                "skill": 50.0,
                "composite": 50.0,
                "career": {
                    "first_year": 2018,
                    "latest_year": 2024,
                    "active_years": 4,
                    "highest_stage": 4,
                    "highest_roles": ["animation_director"],
                },
            },
        ]
        out = tmp_path / "scores.csv"
        generate_csv_report(results, output_path=out)
        lines = out.read_text().strip().split("\n")
        header = lines[0]
        assert "first_year" in header
        assert "latest_year" in header
        assert "highest_stage" in header
        assert "2018" in lines[1]
        assert "2024" in lines[1]
        assert "animation_director" in lines[1]


class TestTextReportCareer:
    def test_career_summary_in_text_report(self, tmp_path):
        """テキストレポートにキャリアサマリーが含まれる."""
        results = [
            {
                "person_id": "p1",
                "name": "Test Person",
                "name_ja": "テスト",
                "name_en": "Test",
                "authority": 50.0,
                "trust": 50.0,
                "skill": 50.0,
                "composite": 50.0,
                "career": {
                    "first_year": 2018,
                    "latest_year": 2024,
                    "active_years": 4,
                    "highest_stage": 4,
                    "highest_roles": ["animation_director"],
                },
            },
        ]
        out = tmp_path / "report.txt"
        generate_text_report(results, output_path=out)
        text = out.read_text()
        assert "キャリアサマリー" in text
        assert "2018" in text

    def test_no_career_data_no_section(self, tmp_path, sample_results):
        """キャリアデータがない場合、セクションは表示されない."""
        out = tmp_path / "report.txt"
        generate_text_report(sample_results, output_path=out)
        text = out.read_text()
        assert "キャリアサマリー" not in text


class TestGenerateHtmlReport:
    def test_creates_file(self, tmp_path, sample_results):
        out = tmp_path / "report.html"
        result = generate_html_report(sample_results, output_path=out)
        assert result == out
        assert out.exists()

    def test_contains_disclaimer(self, tmp_path, sample_results):
        out = tmp_path / "report.html"
        generate_html_report(sample_results, output_path=out)
        html = out.read_text()
        assert "ネットワーク" in html
        assert "network position" in html

    def test_contains_table(self, tmp_path, sample_results):
        out = tmp_path / "report.html"
        generate_html_report(sample_results, output_path=out)
        html = out.read_text()
        assert "Director Alpha" in html
        assert "Animator Beta" in html
        assert "<table>" in html

    def test_contains_svg_chart(self, tmp_path, sample_results):
        out = tmp_path / "report.html"
        generate_html_report(sample_results, output_path=out)
        html = out.read_text()
        assert "<svg" in html
        assert "</svg>" in html

    def test_empty_results(self, tmp_path):
        out = tmp_path / "report.html"
        generate_html_report([], output_path=out)
        html = out.read_text()
        assert "<!DOCTYPE html>" in html

    def test_html_escapes_names(self, tmp_path):
        """HTML special characters in names are escaped."""
        results = [
            {
                "person_id": "p1",
                "name": '<script>alert("xss")</script>',
                "authority": 50.0, "trust": 50.0, "skill": 50.0, "composite": 50.0,
            },
        ]
        out = tmp_path / "report.html"
        generate_html_report(results, output_path=out)
        html = out.read_text()
        assert "<script>" not in html
        assert "&lt;script&gt;" in html


class TestGenerateVisualDashboard:
    def test_creates_file_with_pngs(self, tmp_path, sample_results):
        """PNGファイルがある場合ダッシュボードが生成される."""
        # Create a fake PNG
        png_dir = tmp_path / "charts"
        png_dir.mkdir()
        fake_png = png_dir / "score_distribution.png"
        fake_png.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)

        out = tmp_path / "dashboard.html"
        result = generate_visual_dashboard(
            sample_results, png_dir=png_dir, output_path=out,
        )
        assert result == out
        assert out.exists()
        html = out.read_text()
        assert "data:image/png;base64," in html
        assert "Score Distribution" in html
        assert "Dashboard" in html

    def test_contains_ranking_table(self, tmp_path, sample_results):
        """ダッシュボードにランキングテーブルが含まれる."""
        png_dir = tmp_path / "charts"
        png_dir.mkdir()
        (png_dir / "score_distribution.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 50)

        out = tmp_path / "dashboard.html"
        generate_visual_dashboard(sample_results, png_dir=png_dir, output_path=out)
        html = out.read_text()
        assert "Director Alpha" in html
        assert "Animator Beta" in html

    def test_contains_disclaimer(self, tmp_path, sample_results):
        """ダッシュボードに免責事項が含まれる."""
        png_dir = tmp_path / "charts"
        png_dir.mkdir()
        (png_dir / "top_radar.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 50)

        out = tmp_path / "dashboard.html"
        generate_visual_dashboard(sample_results, png_dir=png_dir, output_path=out)
        html = out.read_text()
        assert "ネットワーク" in html
        assert "network position" in html

    def test_no_pngs_still_returns(self, tmp_path, sample_results):
        """PNGがない場合もエラーにならない."""
        png_dir = tmp_path / "empty"
        png_dir.mkdir()
        out = tmp_path / "dashboard.html"
        generate_visual_dashboard(sample_results, png_dir=png_dir, output_path=out)

    def test_html_escapes_names(self, tmp_path):
        """ダッシュボードでもXSSエスケープされる."""
        results = [
            {
                "person_id": "p1",
                "name": '<script>alert("xss")</script>',
                "authority": 50.0, "trust": 50.0, "skill": 50.0, "composite": 50.0,
            },
        ]
        png_dir = tmp_path / "charts"
        png_dir.mkdir()
        (png_dir / "score_distribution.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 50)

        out = tmp_path / "dashboard.html"
        generate_visual_dashboard(results, png_dir=png_dir, output_path=out)
        html = out.read_text()
        assert "<script>" not in html
        assert "&lt;script&gt;" in html

    def test_multiple_chart_types(self, tmp_path, sample_results):
        """複数のチャートタイプが正しく埋め込まれる."""
        png_dir = tmp_path / "charts"
        png_dir.mkdir()
        for name in ["score_distribution.png", "top_radar.png", "collaboration_network.png"]:
            (png_dir / name).write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 50)

        out = tmp_path / "dashboard.html"
        generate_visual_dashboard(sample_results, png_dir=png_dir, output_path=out)
        html = out.read_text()
        assert html.count("data:image/png;base64,") == 3
        assert "Score Distribution" in html
        assert "Top Persons Radar" in html
        assert "Collaboration Network" in html
