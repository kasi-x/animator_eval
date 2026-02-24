"""similarity モジュールのテスト."""

from src.analysis.similarity import _cosine_similarity, find_similar_persons


class TestCosineSimilarity:
    def test_identical_vectors(self):
        assert _cosine_similarity((1, 2, 3), (1, 2, 3)) > 0.99

    def test_orthogonal_vectors(self):
        assert abs(_cosine_similarity((1, 0, 0), (0, 1, 0))) < 0.01

    def test_zero_vector(self):
        assert _cosine_similarity((0, 0, 0), (1, 2, 3)) == 0.0

    def test_similar_direction(self):
        # Same direction, different magnitude
        sim = _cosine_similarity((1, 2, 3), (2, 4, 6))
        assert sim > 0.99


class TestFindSimilarPersons:
    def test_basic_similarity(self):
        results = [
            {
                "person_id": "p1",
                "name": "A",
                "authority": 80,
                "trust": 60,
                "skill": 70,
                "composite": 70,
            },
            {
                "person_id": "p2",
                "name": "B",
                "authority": 78,
                "trust": 62,
                "skill": 68,
                "composite": 69,
            },
            {
                "person_id": "p3",
                "name": "C",
                "authority": 10,
                "trust": 90,
                "skill": 20,
                "composite": 40,
            },
        ]
        similar = find_similar_persons("p1", results, top_n=2)
        assert len(similar) == 2
        # p2 should be most similar to p1
        assert similar[0]["person_id"] == "p2"
        assert similar[0]["similarity"] > 0.9

    def test_nonexistent_target(self):
        results = [
            {
                "person_id": "p1",
                "name": "A",
                "authority": 80,
                "trust": 60,
                "skill": 70,
                "composite": 70,
            },
        ]
        similar = find_similar_persons("nobody", results)
        assert similar == []

    def test_single_person(self):
        results = [
            {
                "person_id": "p1",
                "name": "A",
                "authority": 80,
                "trust": 60,
                "skill": 70,
                "composite": 70,
            },
        ]
        similar = find_similar_persons("p1", results)
        assert similar == []

    def test_top_n_limit(self):
        results = [
            {
                "person_id": f"p{i}",
                "name": f"P{i}",
                "authority": i * 10,
                "trust": i * 5,
                "skill": i * 8,
                "composite": i * 7,
            }
            for i in range(10)
        ]
        similar = find_similar_persons("p5", results, top_n=3)
        assert len(similar) == 3

    def test_similarity_sorted_desc(self):
        results = [
            {
                "person_id": "p1",
                "name": "A",
                "authority": 50,
                "trust": 50,
                "skill": 50,
                "composite": 50,
            },
            {
                "person_id": "p2",
                "name": "B",
                "authority": 49,
                "trust": 51,
                "skill": 50,
                "composite": 50,
            },
            {
                "person_id": "p3",
                "name": "C",
                "authority": 10,
                "trust": 90,
                "skill": 10,
                "composite": 40,
            },
        ]
        similar = find_similar_persons("p1", results)
        sims = [s["similarity"] for s in similar]
        assert sims == sorted(sims, reverse=True)
