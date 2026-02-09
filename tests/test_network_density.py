"""network_density モジュールのテスト."""

from src.analysis.network_density import compute_network_density
from src.models import Credit, Role


def _make_credits():
    return [
        # a1: p1, p2, p3 (3-way collaboration)
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p3", anime_id="a1", role=Role.CHARACTER_DESIGNER),
        # a2: p1, p2 (2-way)
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR),
        # a3: p4 only (isolated)
        Credit(person_id="p4", anime_id="a3", role=Role.DIRECTOR),
    ]


class TestComputeNetworkDensity:
    def test_collaborator_count(self):
        credits = _make_credits()
        result = compute_network_density(credits)
        assert result["p1"].collaborator_count == 2  # p2, p3
        assert result["p2"].collaborator_count == 2  # p1, p3
        assert result["p3"].collaborator_count == 2  # p1, p2
        assert result["p4"].collaborator_count == 0  # alone

    def test_unique_anime(self):
        credits = _make_credits()
        result = compute_network_density(credits)
        assert result["p1"].unique_anime == 2  # a1, a2
        assert result["p3"].unique_anime == 1  # a1 only

    def test_hub_score(self):
        credits = _make_credits()
        result = compute_network_density(credits)
        # max_collabs = 2, so 2/2 * 100 = 100.0
        assert result["p1"].hub_score == 100.0
        # 0/2 * 100 = 0.0
        assert result["p4"].hub_score == 0.0

    def test_avg_collaborator_score(self):
        credits = _make_credits()
        scores = {"p1": 90.0, "p2": 50.0, "p3": 50.0}
        result = compute_network_density(credits, person_scores=scores)
        # p2's collaborators: p1 (90.0), p3 (50.0) → avg = 70.0 but rounds to 70.0
        # Actually p2 collaborates with p1 and p3, so avg = (90 + 50) / 2 = 70.0
        assert result["p1"].avg_collaborator_score == 50.0

    def test_empty_credits(self):
        result = compute_network_density([])
        assert result == {}
