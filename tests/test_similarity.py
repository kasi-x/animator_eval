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
                "birank": 80,
                "patronage": 60,
                "person_fe": 70,
                "iv_score": 70,
            },
            {
                "person_id": "p2",
                "name": "B",
                "birank": 78,
                "patronage": 62,
                "person_fe": 68,
                "iv_score": 69,
            },
            {
                "person_id": "p3",
                "name": "C",
                "birank": 10,
                "patronage": 90,
                "person_fe": 20,
                "iv_score": 40,
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
                "birank": 80,
                "patronage": 60,
                "person_fe": 70,
                "iv_score": 70,
            },
        ]
        similar = find_similar_persons("nobody", results)
        assert similar == []

    def test_single_person(self):
        results = [
            {
                "person_id": "p1",
                "name": "A",
                "birank": 80,
                "patronage": 60,
                "person_fe": 70,
                "iv_score": 70,
            },
        ]
        similar = find_similar_persons("p1", results)
        assert similar == []

    def test_top_n_limit(self):
        results = [
            {
                "person_id": f"p{i}",
                "name": f"P{i}",
                "birank": i * 10,
                "patronage": i * 5,
                "person_fe": i * 8,
                "iv_score": i * 7,
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
                "birank": 50,
                "patronage": 50,
                "person_fe": 50,
                "iv_score": 50,
            },
            {
                "person_id": "p2",
                "name": "B",
                "birank": 49,
                "patronage": 51,
                "person_fe": 50,
                "iv_score": 50,
            },
            {
                "person_id": "p3",
                "name": "C",
                "birank": 10,
                "patronage": 90,
                "person_fe": 10,
                "iv_score": 40,
            },
        ]
        similar = find_similar_persons("p1", results)
        sims = [s["similarity"] for s in similar]
        assert sims == sorted(sims, reverse=True)
