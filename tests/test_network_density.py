"""network_density モジュールのテスト."""

from src.analysis.network_density import compute_network_density
from src.models import Credit, Role


def _make_credits():
    return [
        # a1: p1, p2, p3 work together
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a1", role=Role.IN_BETWEEN, source="test"),
        # a2: p1, p2 work together
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        # a3: p4 works alone
        Credit(person_id="p4", anime_id="a3", role=Role.DIRECTOR, source="test"),
    ]


class TestComputeNetworkDensity:
    def test_returns_all_persons(self):
        credits = _make_credits()
        result = compute_network_density(credits)
        assert set(result.keys()) == {"p1", "p2", "p3", "p4"}

    def test_collaborator_count(self):
        credits = _make_credits()
        result = compute_network_density(credits)
        assert result["p1"]["collaborator_count"] == 2  # p2, p3
        assert result["p2"]["collaborator_count"] == 2  # p1, p3
        assert result["p3"]["collaborator_count"] == 2  # p1, p2
        assert result["p4"]["collaborator_count"] == 0  # alone

    def test_unique_anime(self):
        credits = _make_credits()
        result = compute_network_density(credits)
        assert result["p1"]["unique_anime"] == 2  # a1, a2
        assert result["p3"]["unique_anime"] == 1  # a1 only

    def test_hub_score(self):
        credits = _make_credits()
        result = compute_network_density(credits)
        # p1 has 2 collabs, max is 2 → hub_score = 100.0
        assert result["p1"]["hub_score"] == 100.0
        # p4 has 0 collabs → hub_score = 0
        assert result["p4"]["hub_score"] == 0.0

    def test_with_scores(self):
        credits = _make_credits()
        scores = {"p1": 80.0, "p2": 60.0, "p3": 40.0, "p4": 20.0}
        result = compute_network_density(credits, person_scores=scores)
        # p1's collaborators are p2(60) and p3(40) → avg = 50.0
        assert result["p1"]["avg_collaborator_score"] == 50.0

    def test_empty(self):
        result = compute_network_density([])
        assert result == {}
