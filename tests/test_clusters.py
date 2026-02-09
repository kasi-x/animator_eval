"""clusters モジュールのテスト."""

from src.analysis.clusters import compute_cluster_stats, detect_collaboration_clusters
from src.models import Anime, Credit, Role


def _make_test_data():
    """2つの明確なクラスターを持つテストデータ."""
    anime_map = {
        "a1": Anime(id="a1", title_en="Show 1", year=2020),
        "a2": Anime(id="a2", title_en="Show 2", year=2021),
        "a3": Anime(id="a3", title_en="Show 3", year=2022),
        "a4": Anime(id="a4", title_en="Show 4", year=2023),
    }
    # Cluster 1: p1, p2, p3 work together on a1, a2
    # Cluster 2: p4, p5, p6 work together on a3, a4
    credits = [
        # Cluster 1
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a1", role=Role.ANIMATION_DIRECTOR, source="test"),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a2", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p3", anime_id="a2", role=Role.ANIMATION_DIRECTOR, source="test"),
        # Cluster 2
        Credit(person_id="p4", anime_id="a3", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p5", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p6", anime_id="a3", role=Role.ANIMATION_DIRECTOR, source="test"),
        Credit(person_id="p4", anime_id="a4", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p5", anime_id="a4", role=Role.KEY_ANIMATOR, source="test"),
        Credit(person_id="p6", anime_id="a4", role=Role.ANIMATION_DIRECTOR, source="test"),
    ]
    return credits, anime_map


class TestDetectCollaborationClusters:
    def test_finds_clusters(self):
        credits, anime_map = _make_test_data()
        result = detect_collaboration_clusters(credits, anime_map)
        assert result["total_clusters"] >= 2

    def test_all_members_assigned(self):
        credits, anime_map = _make_test_data()
        result = detect_collaboration_clusters(credits, anime_map)
        assigned = set(result["person_to_cluster"].keys())
        # All persons with >= 2 shared works should be in a cluster
        assert len(assigned) == 6

    def test_cluster_structure(self):
        credits, anime_map = _make_test_data()
        result = detect_collaboration_clusters(credits, anime_map)
        for cluster in result["clusters"]:
            assert cluster["size"] >= 2
            assert "members" in cluster
            assert "avg_shared_works" in cluster

    def test_empty_data(self):
        result = detect_collaboration_clusters([], {})
        assert result["total_clusters"] == 0

    def test_min_shared_works_filter(self):
        credits, anime_map = _make_test_data()
        # With min_shared_works=3, no pairs have 3 shared works
        result = detect_collaboration_clusters(credits, anime_map, min_shared_works=3)
        assert result["total_clusters"] == 0


class TestComputeClusterStats:
    def test_with_scores(self):
        credits, anime_map = _make_test_data()
        clusters = detect_collaboration_clusters(credits, anime_map)
        scores = {f"p{i}": float(i * 10) for i in range(1, 7)}
        stats = compute_cluster_stats(clusters, scores)
        assert len(stats) >= 2
        for s in stats:
            assert "avg_score" in s
            assert "max_score" in s

    def test_without_scores(self):
        credits, anime_map = _make_test_data()
        clusters = detect_collaboration_clusters(credits, anime_map)
        stats = compute_cluster_stats(clusters)
        assert len(stats) >= 2
        for s in stats:
            assert "avg_score" not in s

    def test_empty_clusters(self):
        stats = compute_cluster_stats({"clusters": []})
        assert stats == []
