"""batch_compare モジュールのテスト."""

from src.analysis.batch_compare import compare_groups


def _make_groups():
    group_a = [
        {"person_id": "p1", "authority": 80, "trust": 70, "skill": 60, "composite": 70},
        {"person_id": "p2", "authority": 90, "trust": 80, "skill": 70, "composite": 80},
    ]
    group_b = [
        {"person_id": "p3", "authority": 40, "trust": 50, "skill": 55, "composite": 48},
        {"person_id": "p4", "authority": 30, "trust": 40, "skill": 45, "composite": 38},
    ]
    return group_a, group_b


class TestCompareGroups:
    def test_basic_comparison(self):
        a, b = _make_groups()
        result = compare_groups(a, b, "Veterans", "Newcomers")
        assert result["group_a"]["label"] == "Veterans"
        assert result["group_b"]["label"] == "Newcomers"

    def test_group_counts(self):
        a, b = _make_groups()
        result = compare_groups(a, b)
        assert result["group_a"]["count"] == 2
        assert result["group_b"]["count"] == 2

    def test_comparison_by_axis(self):
        a, b = _make_groups()
        result = compare_groups(a, b)
        assert "composite" in result["comparison_by_axis"]
        assert "authority" in result["comparison_by_axis"]

    def test_mean_diff_calculated(self):
        a, b = _make_groups()
        result = compare_groups(a, b)
        comp = result["comparison_by_axis"]["composite"]
        # Group A avg = 75, Group B avg = 43
        assert comp["mean_diff"] > 0

    def test_winner_determined(self):
        a, b = _make_groups()
        result = compare_groups(a, b, "Veterans", "Newcomers")
        assert result["summary"]["overall_winner"] == "Veterans"
        assert result["summary"]["a_wins"] == 4  # All axes

    def test_empty_groups(self):
        result = compare_groups([], [], "A", "B")
        assert result["group_a"]["count"] == 0
        assert result["summary"]["overall_winner"] == "tie"

    def test_equal_groups(self):
        a = [
            {
                "person_id": "p1",
                "authority": 50,
                "trust": 50,
                "skill": 50,
                "composite": 50,
            }
        ]
        b = [
            {
                "person_id": "p2",
                "authority": 50,
                "trust": 50,
                "skill": 50,
                "composite": 50,
            }
        ]
        result = compare_groups(a, b)
        assert result["summary"]["overall_winner"] == "tie"
        assert result["summary"]["ties"] == 4

    def test_custom_axes(self):
        a, b = _make_groups()
        result = compare_groups(a, b, axes=["composite"])
        assert len(result["comparison_by_axis"]) == 1
        assert "composite" in result["comparison_by_axis"]

    def test_mixed_winner(self):
        a = [
            {
                "person_id": "p1",
                "authority": 90,
                "trust": 20,
                "skill": 50,
                "composite": 50,
            }
        ]
        b = [
            {
                "person_id": "p2",
                "authority": 20,
                "trust": 90,
                "skill": 50,
                "composite": 50,
            }
        ]
        result = compare_groups(a, b, "A", "B")
        assert result["summary"]["a_wins"] == 1
        assert result["summary"]["b_wins"] == 1
        assert result["summary"]["ties"] == 2
