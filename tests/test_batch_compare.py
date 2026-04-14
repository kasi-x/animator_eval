"""batch_compare モジュールのテスト."""

from src.analysis.batch_compare import compare_groups


def _make_groups():
    group_a = [
        {
            "person_id": "p1",
            "birank": 80,
            "patronage": 70,
            "person_fe": 60,
            "iv_score": 70,
        },
        {
            "person_id": "p2",
            "birank": 90,
            "patronage": 80,
            "person_fe": 70,
            "iv_score": 80,
        },
    ]
    group_b = [
        {
            "person_id": "p3",
            "birank": 40,
            "patronage": 50,
            "person_fe": 55,
            "iv_score": 48,
        },
        {
            "person_id": "p4",
            "birank": 30,
            "patronage": 40,
            "person_fe": 45,
            "iv_score": 38,
        },
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
        assert "iv_score" in result["comparison_by_axis"]
        assert "birank" in result["comparison_by_axis"]

    def test_mean_diff_calculated(self):
        a, b = _make_groups()
        result = compare_groups(a, b)
        comp = result["comparison_by_axis"]["iv_score"]
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
                "birank": 50,
                "patronage": 50,
                "person_fe": 50,
                "iv_score": 50,
            }
        ]
        b = [
            {
                "person_id": "p2",
                "birank": 50,
                "patronage": 50,
                "person_fe": 50,
                "iv_score": 50,
            }
        ]
        result = compare_groups(a, b)
        assert result["summary"]["overall_winner"] == "tie"
        assert result["summary"]["ties"] == 4

    def test_custom_axes(self):
        a, b = _make_groups()
        result = compare_groups(a, b, axes=["iv_score"])
        assert len(result["comparison_by_axis"]) == 1
        assert "iv_score" in result["comparison_by_axis"]

    def test_mixed_winner(self):
        a = [
            {
                "person_id": "p1",
                "birank": 90,
                "patronage": 20,
                "person_fe": 50,
                "iv_score": 50,
            }
        ]
        b = [
            {
                "person_id": "p2",
                "birank": 20,
                "patronage": 90,
                "person_fe": 50,
                "iv_score": 50,
            }
        ]
        result = compare_groups(a, b, "A", "B")
        assert result["summary"]["a_wins"] == 1
        assert result["summary"]["b_wins"] == 1
        assert result["summary"]["ties"] == 2
