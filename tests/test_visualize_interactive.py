"""visualize_interactive モジュールのテスト."""

from src.analysis.visualize_interactive import (
    generate_interactive_dashboard,
    plot_interactive_network,
    plot_interactive_radar,
    plot_interactive_scatter,
    plot_interactive_score_distribution,
    plot_interactive_timeline,
)


def _sample_scores():
    """テスト用スコアデータ."""
    return [
        {
            "person_id": "p1",
            "name": "Person 1",
            "authority": 85.5,
            "trust": 70.2,
            "skill": 90.1,
            "composite": 82.0,
        },
        {
            "person_id": "p2",
            "name": "Person 2",
            "authority": 60.3,
            "trust": 80.5,
            "skill": 65.0,
            "composite": 68.5,
        },
        {
            "person_id": "p3",
            "name": "Person 3",
            "authority": 75.0,
            "trust": 85.0,
            "skill": 70.0,
            "composite": 76.7,
        },
    ]


def _sample_timeline():
    """テスト用タイムラインデータ."""
    return {
        "years": [2018, 2019, 2020, 2021, 2022],
        "credit_counts": [10, 15, 20, 18, 25],
    }


def _sample_collaboration():
    """テスト用コラボレーションデータ."""
    return [
        {
            "person1_id": "p1",
            "person2_id": "p2",
            "person1_name": "Person 1",
            "person2_name": "Person 2",
            "weight": 10.0,
        },
        {
            "person1_id": "p2",
            "person2_id": "p3",
            "person1_name": "Person 2",
            "person2_name": "Person 3",
            "weight": 8.0,
        },
        {
            "person1_id": "p1",
            "person2_id": "p3",
            "person1_name": "Person 1",
            "person2_name": "Person 3",
            "weight": 12.0,
        },
    ]


class TestInteractiveScoreDistribution:
    def test_creates_html_file(self, tmp_path):
        scores = _sample_scores()
        output = tmp_path / "scores.html"
        plot_interactive_score_distribution(scores, output_path=output)
        assert output.exists()
        assert output.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        output = tmp_path / "empty.html"
        plot_interactive_score_distribution([], output_path=output)
        assert not output.exists()  # Should not create file for empty data

    def test_html_contains_plotly(self, tmp_path):
        scores = _sample_scores()
        output = tmp_path / "scores.html"
        plot_interactive_score_distribution(scores, output_path=output)
        content = output.read_text()
        assert "plotly" in content.lower()
        assert "Authority Score" in content


class TestInteractiveRadar:
    def test_creates_html_file(self, tmp_path):
        scores = _sample_scores()
        output = tmp_path / "radar.html"
        plot_interactive_radar(scores, top_n=3, output_path=output)
        assert output.exists()
        assert output.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        output = tmp_path / "empty.html"
        plot_interactive_radar([], output_path=output)
        assert not output.exists()

    def test_top_n_limit(self, tmp_path):
        scores = _sample_scores()
        output = tmp_path / "radar.html"
        plot_interactive_radar(scores, top_n=2, output_path=output)
        content = output.read_text()
        assert "Person 1" in content
        assert "Person 2" in content
        # Top 2 only, so Person 3 might not be in the data section


class TestInteractiveScatter:
    def test_creates_html_file(self, tmp_path):
        scores = _sample_scores()
        output = tmp_path / "scatter.html"
        plot_interactive_scatter(
            scores, x_axis="authority", y_axis="trust", output_path=output
        )
        assert output.exists()
        assert output.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        output = tmp_path / "empty.html"
        plot_interactive_scatter([], output_path=output)
        assert not output.exists()

    def test_different_axes(self, tmp_path):
        scores = _sample_scores()
        output = tmp_path / "scatter.html"
        plot_interactive_scatter(
            scores, x_axis="skill", y_axis="trust", output_path=output
        )
        content = output.read_text()
        assert "Skill Score" in content or "skill" in content.lower()
        assert "Trust Score" in content or "trust" in content.lower()


class TestInteractiveTimeline:
    def test_creates_html_file(self, tmp_path):
        timeline = _sample_timeline()
        output = tmp_path / "timeline.html"
        plot_interactive_timeline(timeline, output_path=output)
        assert output.exists()
        assert output.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        output = tmp_path / "empty.html"
        plot_interactive_timeline({}, output_path=output)
        assert not output.exists()

    def test_html_contains_years(self, tmp_path):
        timeline = _sample_timeline()
        output = tmp_path / "timeline.html"
        plot_interactive_timeline(timeline, output_path=output)
        content = output.read_text()
        assert "2018" in content or "2022" in content


class TestInteractiveNetwork:
    def test_creates_html_file(self, tmp_path):
        collab = _sample_collaboration()
        output = tmp_path / "network.html"
        plot_interactive_network(collab, top_n=3, output_path=output)
        assert output.exists()
        assert output.stat().st_size > 0

    def test_empty_data(self, tmp_path):
        output = tmp_path / "empty.html"
        plot_interactive_network([], output_path=output)
        assert not output.exists()

    def test_network_structure(self, tmp_path):
        collab = _sample_collaboration()
        output = tmp_path / "network.html"
        plot_interactive_network(collab, top_n=3, output_path=output)
        content = output.read_text()
        assert "Collaboration Network" in content


class TestGenerateInteractiveDashboard:
    def test_creates_multiple_files(self, tmp_path):
        scores = _sample_scores()
        timeline = _sample_timeline()
        generate_interactive_dashboard(scores, timeline, output_dir=tmp_path)

        # Check that files were created
        expected_files = [
            "interactive_scores.html",
            "interactive_radar.html",
            "interactive_scatter_authority_trust.html",
            "interactive_scatter_authority_skill.html",
            "interactive_scatter_trust_skill.html",
            "interactive_timeline.html",
        ]

        for filename in expected_files:
            filepath = tmp_path / filename
            assert filepath.exists(), f"{filename} should be created"
            assert filepath.stat().st_size > 0

    def test_creates_directory_if_missing(self, tmp_path):
        scores = _sample_scores()
        output_dir = tmp_path / "new_dir" / "interactive"
        generate_interactive_dashboard(scores, output_dir=output_dir)
        assert output_dir.exists()
        assert (output_dir / "interactive_scores.html").exists()

    def test_without_timeline(self, tmp_path):
        scores = _sample_scores()
        generate_interactive_dashboard(scores, timeline_data=None, output_dir=tmp_path)
        # Should still create score visualizations
        assert (tmp_path / "interactive_scores.html").exists()
        # But not timeline
        assert not (tmp_path / "interactive_timeline.html").exists()
