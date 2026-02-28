"""aggregate_stats モジュールのテスト."""

from src.analysis.aggregate_stats import compute_aggregate_stats


def _make_results():
    return [
        {
            "person_id": "p1",
            "birank": 80,
            "patronage": 70,
            "person_fe": 60,
            "iv_score": 71,
            "primary_role": "director",
            "career": {"active_years": 15, "highest_stage": 6},
            "network": {"hub_score": 90, "collaborators": 50},
        },
        {
            "person_id": "p2",
            "birank": 50,
            "patronage": 90,
            "person_fe": 40,
            "iv_score": 58,
            "primary_role": "animator",
            "career": {"active_years": 8, "highest_stage": 3},
            "network": {"hub_score": 60, "collaborators": 20},
        },
        {
            "person_id": "p3",
            "birank": 30,
            "patronage": 30,
            "person_fe": 80,
            "iv_score": 43,
            "primary_role": "animator",
            "career": {"active_years": 5, "highest_stage": 4},
            "network": {"hub_score": 30, "collaborators": 10},
        },
        {
            "person_id": "p4",
            "birank": 60,
            "patronage": 60,
            "person_fe": 60,
            "iv_score": 60,
            "primary_role": "designer",
            "career": {"active_years": 10, "highest_stage": 4},
            "network": {"hub_score": 50, "collaborators": 25},
        },
    ]


class TestComputeAggregateStats:
    def test_total_persons(self):
        result = compute_aggregate_stats(_make_results())
        assert result["total_persons"] == 4

    def test_score_distribution(self):
        result = compute_aggregate_stats(_make_results())
        for axis in ("birank", "patronage", "person_fe", "iv_score"):
            assert axis in result["score_distribution"]
            dist = result["score_distribution"][axis]
            assert dist["min"] <= dist["mean"] <= dist["max"]
            assert dist["p25"] <= dist["median"] <= dist["p75"]

    def test_role_breakdown(self):
        result = compute_aggregate_stats(_make_results())
        rb = result["role_breakdown"]
        assert "director" in rb
        assert rb["director"]["count"] == 1
        assert rb["animator"]["count"] == 2

    def test_career_stats(self):
        result = compute_aggregate_stats(_make_results())
        cs = result["career_stats"]
        assert cs["avg_active_years"] > 0
        assert cs["max_active_years"] == 15

    def test_network_stats(self):
        result = compute_aggregate_stats(_make_results())
        ns = result["network_stats"]
        assert ns["avg_hub_score"] > 0
        assert ns["max_collaborators"] == 50

    def test_empty(self):
        result = compute_aggregate_stats([])
        assert result == {}

    def test_std_deviation(self):
        result = compute_aggregate_stats(_make_results())
        for axis in ("birank", "patronage", "person_fe", "iv_score"):
            assert result["score_distribution"][axis]["std"] >= 0
