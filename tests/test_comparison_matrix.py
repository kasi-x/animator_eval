"""comparison_matrix モジュールのテスト."""

from src.analysis.comparison_matrix import build_comparison_matrix


def _make_results():
    return [
        {
            "person_id": "p1",
            "name": "Alice",
            "birank": 80,
            "patronage": 70,
            "person_fe": 60,
            "iv_score": 71,
        },
        {
            "person_id": "p2",
            "name": "Bob",
            "birank": 50,
            "patronage": 90,
            "person_fe": 40,
            "iv_score": 58,
        },
        {
            "person_id": "p3",
            "name": "Carol",
            "birank": 60,
            "patronage": 60,
            "person_fe": 80,
            "iv_score": 65,
        },
    ]


class TestBuildComparisonMatrix:
    def test_returns_requested_persons(self):
        result = build_comparison_matrix(["p1", "p2"], _make_results())
        assert len(result["persons"]) == 2
        pids = {p["person_id"] for p in result["persons"]}
        assert pids == {"p1", "p2"}

    def test_axis_rankings(self):
        result = build_comparison_matrix(["p1", "p2", "p3"], _make_results())
        # p1 has highest birank
        assert result["axis_rankings"]["birank"][0] == "p1"
        # p2 has highest patronage
        assert result["axis_rankings"]["patronage"][0] == "p2"
        # p3 has highest person_fe
        assert result["axis_rankings"]["person_fe"][0] == "p3"

    def test_pairwise_dominance(self):
        result = build_comparison_matrix(["p1", "p2"], _make_results())
        dom = result["pairwise_dominance"]
        # p1 vs p2: auth(win), trust(loss), skill(win), composite(win) = 3 wins, 1 loss
        assert dom["p1"]["p2"]["wins"] == 3
        assert dom["p1"]["p2"]["losses"] == 1

    def test_missing_person(self):
        result = build_comparison_matrix(["p1", "nonexistent"], _make_results())
        assert len(result["persons"]) == 1

    def test_empty(self):
        result = build_comparison_matrix([], _make_results())
        assert result["persons"] == []

    def test_custom_axes(self):
        result = build_comparison_matrix(
            ["p1", "p2"],
            _make_results(),
            axes=("birank", "patronage"),
        )
        assert "birank" in result["axis_rankings"]
        assert "person_fe" not in result["axis_rankings"]

    def test_single_person(self):
        result = build_comparison_matrix(["p1"], _make_results())
        assert len(result["persons"]) == 1
        assert result["pairwise_dominance"]["p1"] == {}
