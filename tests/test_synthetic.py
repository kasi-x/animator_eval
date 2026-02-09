"""合成データ生成のテスト."""

from src.synthetic import generate_synthetic_data


class TestGenerateSyntheticData:
    def test_generates_correct_counts(self):
        persons, anime_list, credits = generate_synthetic_data(
            n_directors=5, n_animators=20, n_anime=10
        )
        assert len(persons) == 25  # 5 directors + 20 animators
        assert len(anime_list) == 10
        assert len(credits) > 0

    def test_directors_have_director_role(self):
        persons, anime_list, credits = generate_synthetic_data(
            n_directors=3, n_animators=10, n_anime=5
        )
        director_ids = {p.id for p in persons if p.id.startswith("syn:d")}
        director_credits = [c for c in credits if c.person_id in director_ids and c.role.value == "director"]
        assert len(director_credits) >= 5  # at least one director per anime

    def test_deterministic_with_seed(self):
        p1, a1, c1 = generate_synthetic_data(seed=123)
        p2, a2, c2 = generate_synthetic_data(seed=123)
        assert len(c1) == len(c2)
        assert [p.id for p in p1] == [p.id for p in p2]

    def test_different_seeds_different_data(self):
        _, _, c1 = generate_synthetic_data(seed=1)
        _, _, c2 = generate_synthetic_data(seed=2)
        # Credits should differ
        roles1 = [(c.person_id, c.anime_id) for c in c1[:10]]
        roles2 = [(c.person_id, c.anime_id) for c in c2[:10]]
        assert roles1 != roles2

    def test_credits_reference_valid_ids(self):
        persons, anime_list, credits = generate_synthetic_data(
            n_directors=3, n_animators=10, n_anime=5
        )
        person_ids = {p.id for p in persons}
        anime_ids = {a.id for a in anime_list}
        for c in credits:
            assert c.person_id in person_ids
            assert c.anime_id in anime_ids
            assert c.source == "synthetic"
