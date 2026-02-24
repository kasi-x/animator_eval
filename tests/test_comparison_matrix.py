"""comparison_matrix モジュールのテスト."""

from src.analysis.comparison_matrix import build_comparison_matrix


def _make_results():
    return [
        {
            "person_id": "p1",
            "name": "Alice",
            "authority": 80,
            "trust": 70,
            "skill": 60,
            "composite": 71,
        },
        {
            "person_id": "p2",
            "name": "Bob",
            "authority": 50,
            "trust": 90,
            "skill": 40,
            "composite": 58,
        },
        {
            "person_id": "p3",
            "name": "Carol",
            "authority": 60,
            "trust": 60,
            "skill": 80,
            "composite": 65,
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
        # p1 has highest authority
        assert result["axis_rankings"]["authority"][0] == "p1"
        # p2 has highest trust
        assert result["axis_rankings"]["trust"][0] == "p2"
        # p3 has highest skill
        assert result["axis_rankings"]["skill"][0] == "p3"

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
            axes=("authority", "trust"),
        )
        assert "authority" in result["axis_rankings"]
        assert "skill" not in result["axis_rankings"]

    def test_single_person(self):
        result = build_comparison_matrix(["p1"], _make_results())
        assert len(result["persons"]) == 1
        assert result["pairwise_dominance"]["p1"] == {}
