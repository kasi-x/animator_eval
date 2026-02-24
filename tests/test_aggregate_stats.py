"""aggregate_stats モジュールのテスト."""

from src.analysis.aggregate_stats import compute_aggregate_stats


def _make_results():
    return [
        {
            "person_id": "p1",
            "authority": 80,
            "trust": 70,
            "skill": 60,
            "composite": 71,
            "primary_role": "director",
            "career": {"active_years": 15, "highest_stage": 6},
            "network": {"hub_score": 90, "collaborators": 50},
        },
        {
            "person_id": "p2",
            "authority": 50,
            "trust": 90,
            "skill": 40,
            "composite": 58,
            "primary_role": "animator",
            "career": {"active_years": 8, "highest_stage": 3},
            "network": {"hub_score": 60, "collaborators": 20},
        },
        {
            "person_id": "p3",
            "authority": 30,
            "trust": 30,
            "skill": 80,
            "composite": 43,
            "primary_role": "animator",
            "career": {"active_years": 5, "highest_stage": 4},
            "network": {"hub_score": 30, "collaborators": 10},
        },
        {
            "person_id": "p4",
            "authority": 60,
            "trust": 60,
            "skill": 60,
            "composite": 60,
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
        for axis in ("authority", "trust", "skill", "composite"):
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
        for axis in ("authority", "trust", "skill", "composite"):
            assert result["score_distribution"][axis]["std"] >= 0
