"""role_flow モジュールのテスト."""

from src.analysis.role_flow import compute_role_flow
from src.runtime.models import BronzeAnime as Anime, Credit, Role


def _make_data():
    anime_map = {
        "a1": Anime(id="a1", title_en="Early", year=2018),
        "a2": Anime(id="a2", title_en="Mid", year=2020),
        "a3": Anime(id="a3", title_en="Late", year=2022),
    }
    credits = [
        # p1: in_between → key_animator → animation_director
        Credit(person_id="p1", anime_id="a1", role=Role.IN_BETWEEN),
        Credit(person_id="p1", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p1", anime_id="a3", role=Role.ANIMATION_DIRECTOR),
        # p2: key_animator → key_animator (no change)
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR),
        # p3: in_between → key_animator
        Credit(person_id="p3", anime_id="a1", role=Role.IN_BETWEEN),
        Credit(person_id="p3", anime_id="a2", role=Role.KEY_ANIMATOR),
    ]
    return credits, anime_map


class TestComputeRoleFlow:
    def test_returns_nodes_and_links(self):
        credits, anime_map = _make_data()
        result = compute_role_flow(credits, anime_map)
        assert "nodes" in result
        assert "links" in result
        assert "total_transitions" in result

    def test_total_transitions(self):
        credits, anime_map = _make_data()
        result = compute_role_flow(credits, anime_map)
        # p1: 2 transitions, p2: 0, p3: 1 = total 3
        assert result["total_transitions"] == 3

    def test_nodes_include_stages(self):
        credits, anime_map = _make_data()
        result = compute_role_flow(credits, anime_map)
        node_ids = {n["id"] for n in result["nodes"]}
        assert any("Stage 1" in n for n in node_ids)
        assert any("Stage 3" in n for n in node_ids)

    def test_links_have_value(self):
        credits, anime_map = _make_data()
        result = compute_role_flow(credits, anime_map)
        for link in result["links"]:
            assert link["value"] >= 1
            assert "source" in link
            assert "target" in link

    def test_empty(self):
        result = compute_role_flow([], {})
        assert result["total_transitions"] == 0
        assert result["nodes"] == []
        assert result["links"] == []

    def test_no_year_data(self):
        anime_map = {"a1": Anime(id="a1", title_en="No Year")}
        credits = [Credit(person_id="p1", anime_id="a1", role=Role.KEY_ANIMATOR)]
        result = compute_role_flow(credits, anime_map)
        assert result["total_transitions"] == 0

    def test_common_transition_counted(self):
        credits, anime_map = _make_data()
        result = compute_role_flow(credits, anime_map)
        # Stage 1 → Stage 3 should appear (p1 and p3 both do this)
        s1_to_s3 = [
            link
            for link in result["links"]
            if "Stage 1" in link["source"] and "Stage 3" in link["target"]
        ]
        assert len(s1_to_s3) == 1
        assert s1_to_s3[0]["value"] == 2  # p1 + p3
