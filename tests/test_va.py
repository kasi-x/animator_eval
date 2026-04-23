"""Tests for VA graph modules."""

from src.runtime.models import Role

# Fixtures (anime_map, anime_list, va_credits, production_credits, characters,
# person_fe) are provided by tests/conftest.py


# ============================================================
# VA Graph Tests
# ============================================================


class TestVAGraph:
    def test_build_va_anime_graph(self, va_credits, anime_map):
        from src.analysis.va.graph import build_va_anime_graph

        g = build_va_anime_graph(va_credits, anime_map)
        assert g.number_of_nodes() > 0
        assert g.number_of_edges() > 0
        # VA1 should be connected to a1, a2, a3, a4
        assert g.has_edge("va1", "a1")
        assert g.has_edge("va1", "a2")

    def test_build_va_collaboration_graph(self, va_credits, anime_map):
        from src.analysis.va.graph import build_va_collaboration_graph

        g = build_va_collaboration_graph(va_credits, anime_map)
        assert g.number_of_nodes() > 0
        # VA1 and VA4 co-star in a1 and a2 (both MAIN)
        edge = (min("va1", "va4"), max("va1", "va4"))
        assert g.has_edge(*edge)

    def test_build_va_sd_graph(self, va_credits, production_credits, anime_map):
        from src.analysis.va.graph import build_va_sound_director_graph

        g = build_va_sound_director_graph(va_credits, production_credits, anime_map)
        assert g.number_of_nodes() > 0
        # VA1 in a1 -> sd1
        assert g.has_edge("va1", "sd1")

    def test_franchise_bonus(self, va_credits):
        from src.analysis.va.graph import _compute_franchise_bonus

        bonuses = _compute_franchise_bonus(va_credits)
        # VA1 voices c1 in a1 and a2 -> franchise bonus > 1.0
        assert bonuses.get(("va1", "c1"), 1.0) > 1.0
        # VA3 voices c9 in only a1 -> no bonus
        assert bonuses.get(("va3", "c9"), 1.0) == 1.0


# ============================================================
# VA AKM Tests
# ============================================================


class TestVAAKM:
    def test_estimate_va_akm(self, va_credits, production_credits, anime_map):
        from src.analysis.va.akm import estimate_va_akm

        result = estimate_va_akm(va_credits, production_credits, anime_map)
        assert len(result.person_fe) > 0
        # SD FE may be empty if not enough movers
        assert result.n_observations > 0

    def test_va_akm_returns_dataclass(self, va_credits, production_credits, anime_map):
        from src.analysis.va.akm import VAAKMResult, estimate_va_akm

        result = estimate_va_akm(va_credits, production_credits, anime_map)
        assert isinstance(result, VAAKMResult)


# ============================================================
# VA Trust Tests
# ============================================================


class TestVATrust:
    def test_compute_va_trust(self, va_credits, production_credits, anime_map):
        from src.analysis.va.trust import compute_va_trust

        trust = compute_va_trust(va_credits, production_credits, anime_map)
        assert isinstance(trust, dict)
        # VA1 works with sd1 (a1,a2) -> should have trust
        if "va1" in trust:
            assert trust["va1"] >= 0.0

    def test_compute_va_patronage(self, va_credits, production_credits, anime_map):
        from src.analysis.va.trust import compute_va_patronage

        sd_birank = {"sd1": 0.8, "sd2": 0.5}
        patronage = compute_va_patronage(
            va_credits, production_credits, anime_map, sd_birank
        )
        assert isinstance(patronage, dict)


# ============================================================
# VA Integrated Value Tests
# ============================================================


class TestVAIntegratedValue:
    def test_compute_va_iv(self):
        from src.analysis.va.integrated_value import compute_va_integrated_value

        person_fe = {"va1": 1.0, "va2": 0.5, "va3": -0.3}
        birank = {"va1": 0.8, "va2": 0.6, "va3": 0.2}
        sd_exposure = {"va1": 0.5, "va2": 0.3}
        awcc = {"va1": 0.0, "va2": 0.0, "va3": 0.0}
        patronage = {"va1": 0.7, "va2": 0.4}
        dormancy = {"va1": 1.0, "va2": 0.9, "va3": 0.5}

        iv = compute_va_integrated_value(
            person_fe, birank, sd_exposure, awcc, patronage, dormancy
        )
        assert isinstance(iv, dict)
        assert len(iv) > 0
        # VA1 should score higher than VA3
        assert iv.get("va1", 0) > iv.get("va3", 0)


# ============================================================
# VA Character Diversity Tests
# ============================================================


class TestCharacterDiversity:
    def test_compute_character_diversity(self, va_credits, anime_map, characters):
        from src.analysis.va.character_diversity import compute_character_diversity

        diversity = compute_character_diversity(va_credits, anime_map, characters)
        assert isinstance(diversity, dict)
        if diversity:
            # Check that CDI is in [0, 1]
            for pid, m in diversity.items():
                assert 0.0 <= m.cdi <= 1.0
                assert m.casting_tier in (
                    "lead_specialist",
                    "versatile",
                    "ensemble",
                    "newcomer",
                )

    def test_casting_tier_classification(self):
        from src.analysis.va.character_diversity import _classify_casting_tier

        assert _classify_casting_tier(15, 10, 30) == "lead_specialist"
        assert _classify_casting_tier(8, 15, 40) == "versatile"
        assert _classify_casting_tier(3, 18, 25) == "ensemble"
        assert _classify_casting_tier(1, 2, 5) == "newcomer"


# ============================================================
# VA Ensemble Synergy Tests
# ============================================================


class TestEnsembleSynergy:
    def test_compute_synergy(self, va_credits, anime_map):
        from src.analysis.va.ensemble_synergy import compute_va_ensemble_synergy

        # Lower min_shared for small test data
        synergy = compute_va_ensemble_synergy(va_credits, anime_map, min_shared=2)
        assert isinstance(synergy, list)
        # VA1 and VA4 share a1, a2, a3 -> should appear
        if synergy:
            pair_ids = {(s.va_a, s.va_b) for s in synergy}
            # Check order-independent
            va1_va4 = ("va1", "va4") if "va1" < "va4" else ("va4", "va1")
            assert va1_va4 in pair_ids

    def test_synergy_sorted_descending(self, va_credits, anime_map):
        from src.analysis.va.ensemble_synergy import compute_va_ensemble_synergy

        synergy = compute_va_ensemble_synergy(va_credits, anime_map, min_shared=2)
        if len(synergy) >= 2:
            for i in range(len(synergy) - 1):
                assert synergy[i].synergy_score >= synergy[i + 1].synergy_score


# ============================================================
# VA Replacement Difficulty Tests
# ============================================================


class TestReplacementDifficulty:
    def test_compute_rdi(self, va_credits, anime_map):
        from src.analysis.va.replacement_difficulty import (
            compute_replacement_difficulty,
        )

        rdi = compute_replacement_difficulty(va_credits, anime_map, min_characters=2)
        assert isinstance(rdi, dict)
        for pid, rd in rdi.items():
            assert 0.0 <= rd.rdi <= 1.0


# ============================================================
# Production Analysis Tests
# ============================================================


class TestProductionAnalysis:
    def test_compute_studio_talent_density(
        self, production_credits, anime_map, person_fe
    ):
        from src.analysis.production_analysis import compute_studio_talent_density

        density = compute_studio_talent_density(
            production_credits, anime_map, person_fe
        )
        assert isinstance(density, dict)
        for studio, td in density.items():
            assert 0.0 <= td.gini_coefficient <= 1.0
            assert td.staff_count > 0


# ============================================================
# Studio Network Tests
# ============================================================


class TestSyntheticVAData:
    def test_generate_synthetic_va_data(self):
        from src.testing.fixtures import generate_synthetic_data, generate_synthetic_va_data

        _, anime_list, _ = generate_synthetic_data(
            n_directors=3, n_animators=10, n_anime=10, seed=42
        )
        persons, chars, va_credits, sd_credits = generate_synthetic_va_data(
            anime_list, n_voice_actors=10, n_characters=20, n_sound_directors=3, seed=42
        )
        assert len(persons) == 13  # 10 VAs + 3 SDs
        assert len(chars) == 20
        assert len(va_credits) > 0
        assert len(sd_credits) == 10  # 1 per anime
        # All credits reference valid anime
        anime_ids = {a.id for a in anime_list}
        for cva in va_credits:
            assert cva.anime_id in anime_ids
        for c in sd_credits:
            assert c.role == Role.SOUND_DIRECTOR


# ============================================================
# Studio Clustering Tests
# ============================================================


class TestVAScoreResult:
    def test_va_score_result_creation(self):
        from src.runtime.models import VAScoreResult

        result = VAScoreResult(
            person_id="va1",
            person_fe=1.0,
            va_iv_score=0.8,
            casting_tier="versatile",
        )
        assert result.person_id == "va1"
        assert result.va_iv_score == 0.8
        assert result.casting_tier == "versatile"
        assert result.dormancy == 1.0  # default
