"""network_evolution モジュールのテスト."""

from src.analysis.network_evolution import compute_network_evolution
from src.models import Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Show 1", year=2018, score=7.0),
        "a2": Anime(id="a2", title_en="Show 2", year=2019, score=7.5),
        "a3": Anime(id="a3", title_en="Show 3", year=2020, score=8.0),
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p3", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p4", anime_id="a3", role=Role.IN_BETWEEN),
    ]
    return credits, anime_map


class TestNetworkEvolution:
    def test_empty(self):
        result = compute_network_evolution([], {})
        assert result["years"] == []

    def test_returns_years(self):
        credits, anime_map = _make_data()
        result = compute_network_evolution(credits, anime_map)
        assert result["years"] == [2018, 2019, 2020]

    def test_cumulative_persons_grow(self):
        credits, anime_map = _make_data()
        result = compute_network_evolution(credits, anime_map)
        snapshots = result["snapshots"]
        assert snapshots[2018]["cumulative_persons"] == 2
        assert snapshots[2019]["cumulative_persons"] == 3
        assert snapshots[2020]["cumulative_persons"] == 4

    def test_new_persons_tracked(self):
        credits, anime_map = _make_data()
        result = compute_network_evolution(credits, anime_map)
        assert result["snapshots"][2018]["new_persons"] == 2  # p1, p2
        assert result["snapshots"][2019]["new_persons"] == 1  # p3
        assert result["snapshots"][2020]["new_persons"] == 1  # p4

    def test_edges_grow(self):
        credits, anime_map = _make_data()
        result = compute_network_evolution(credits, anime_map)
        # 2018: p1-p2 (1 edge)
        # 2019: p1-p3 (1 new)
        # 2020: p1-p2, p1-p3, p1-p4, p2-p3, p2-p4, p3-p4 (3 new)
        assert result["snapshots"][2018]["cumulative_edges"] == 1
        assert (
            result["snapshots"][2020]["cumulative_edges"]
            > result["snapshots"][2018]["cumulative_edges"]
        )

    def test_density_computed(self):
        credits, anime_map = _make_data()
        result = compute_network_evolution(credits, anime_map)
        for snap in result["snapshots"].values():
            assert 0 <= snap["density"] <= 1

    def test_trends_computed(self):
        credits, anime_map = _make_data()
        result = compute_network_evolution(credits, anime_map)
        assert "person_growth" in result["trends"]
        assert "edge_growth" in result["trends"]
        assert result["trends"]["person_growth"] == 2  # 4 - 2

    def test_no_year_data(self):
        anime_map = {"a1": Anime(id="a1", title_en="No Year")}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR)]
        result = compute_network_evolution(credits, anime_map)
        assert result["years"] == []

    def test_single_year(self):
        anime_map = {"a1": Anime(id="a1", title_en="Solo", year=2022)}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR)]
        result = compute_network_evolution(credits, anime_map)
        assert result["years"] == [2022]
        # No trends for single year
        assert result["trends"] == {}
