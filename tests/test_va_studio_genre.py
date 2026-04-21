"""Tests for VA evaluation, studio analysis, and genre ecosystem modules."""

import pytest

from src.models import (
    BronzeAnime as Anime,
    Character,
    CharacterVoiceActor,
    Credit,
    Role,
)


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def anime_map():
    """Minimal anime map for testing."""
    return {
        "a1": Anime(
            id="a1",
            title_ja="作品1",
            year=2020,
            episodes=12,
            duration=24,
            genres=["Action", "Adventure"],
            studios=["StudioA"],
            season="winter",
        ),
        "a2": Anime(
            id="a2",
            title_ja="作品2",
            year=2021,
            episodes=24,
            duration=24,
            genres=["Action", "Drama"],
            studios=["StudioA", "StudioB"],
            season="spring",
        ),
        "a3": Anime(
            id="a3",
            title_ja="作品3",
            year=2022,
            episodes=12,
            duration=24,
            genres=["Drama", "Romance"],
            studios=["StudioB"],
            season="summer",
        ),
        "a4": Anime(
            id="a4",
            title_ja="作品4",
            year=2023,
            episodes=13,
            duration=24,
            genres=["Action", "Sci-Fi"],
            studios=["StudioC"],
            season="fall",
        ),
        "a5": Anime(
            id="a5",
            title_ja="作品5",
            year=2024,
            episodes=12,
            duration=24,
            genres=["Comedy", "Romance"],
            studios=["StudioA"],
            season="winter",
        ),
    }


@pytest.fixture
def anime_list(anime_map):
    return list(anime_map.values())


@pytest.fixture
def va_credits():
    """Voice actor credits for testing."""
    return [
        # VA1 = main star (multiple MAIN roles across anime)
        CharacterVoiceActor(
            character_id="c1", person_id="va1", anime_id="a1", character_role="MAIN"
        ),
        CharacterVoiceActor(
            character_id="c1", person_id="va1", anime_id="a2", character_role="MAIN"
        ),  # same char = franchise
        CharacterVoiceActor(
            character_id="c2", person_id="va1", anime_id="a3", character_role="MAIN"
        ),
        CharacterVoiceActor(
            character_id="c3",
            person_id="va1",
            anime_id="a4",
            character_role="SUPPORTING",
        ),
        # VA2 = supporting specialist
        CharacterVoiceActor(
            character_id="c4",
            person_id="va2",
            anime_id="a1",
            character_role="SUPPORTING",
        ),
        CharacterVoiceActor(
            character_id="c5",
            person_id="va2",
            anime_id="a2",
            character_role="SUPPORTING",
        ),
        CharacterVoiceActor(
            character_id="c6",
            person_id="va2",
            anime_id="a3",
            character_role="SUPPORTING",
        ),
        CharacterVoiceActor(
            character_id="c7",
            person_id="va2",
            anime_id="a4",
            character_role="SUPPORTING",
        ),
        CharacterVoiceActor(
            character_id="c8", person_id="va2", anime_id="a5", character_role="MAIN"
        ),
        # VA3 = background with few credits
        CharacterVoiceActor(
            character_id="c9",
            person_id="va3",
            anime_id="a1",
            character_role="BACKGROUND",
        ),
        CharacterVoiceActor(
            character_id="c10",
            person_id="va3",
            anime_id="a2",
            character_role="BACKGROUND",
        ),
        # VA4 = co-stars with VA1 often
        CharacterVoiceActor(
            character_id="c11", person_id="va4", anime_id="a1", character_role="MAIN"
        ),
        CharacterVoiceActor(
            character_id="c11", person_id="va4", anime_id="a2", character_role="MAIN"
        ),
        CharacterVoiceActor(
            character_id="c12",
            person_id="va4",
            anime_id="a3",
            character_role="SUPPORTING",
        ),
    ]


@pytest.fixture
def production_credits():
    """Production credits including sound directors."""
    return [
        Credit(person_id="sd1", anime_id="a1", role=Role.SOUND_DIRECTOR),
        Credit(person_id="sd1", anime_id="a2", role=Role.SOUND_DIRECTOR),
        Credit(person_id="sd2", anime_id="a3", role=Role.SOUND_DIRECTOR),
        Credit(person_id="sd2", anime_id="a4", role=Role.SOUND_DIRECTOR),
        Credit(person_id="sd1", anime_id="a5", role=Role.SOUND_DIRECTOR),
        # Also add some production staff for studio analysis
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR),
        Credit(person_id="p1", anime_id="a2", role=Role.DIRECTOR),
        Credit(person_id="p2", anime_id="a3", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a4", role=Role.DIRECTOR),
        Credit(person_id="p3", anime_id="a5", role=Role.DIRECTOR),
        Credit(person_id="p4", anime_id="a1", role=Role.KEY_ANIMATOR),
        Credit(person_id="p4", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p4", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p5", anime_id="a2", role=Role.KEY_ANIMATOR),
        Credit(person_id="p5", anime_id="a3", role=Role.KEY_ANIMATOR),
        Credit(person_id="p5", anime_id="a4", role=Role.KEY_ANIMATOR),
        Credit(person_id="p6", anime_id="a1", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="p6", anime_id="a5", role=Role.ANIMATION_DIRECTOR),
        Credit(person_id="p7", anime_id="a3", role=Role.IN_BETWEEN),
        Credit(person_id="p7", anime_id="a4", role=Role.KEY_ANIMATOR),
    ]


@pytest.fixture
def characters():
    """Characters for testing."""
    return {
        f"c{i}": Character(
            id=f"c{i}",
            name_ja=f"キャラ{i}",
            gender="Male" if i % 2 == 0 else "Female",
        )
        for i in range(1, 13)
    }


@pytest.fixture
def person_fe():
    """Person FE for testing."""
    return {
        "p1": 1.5,
        "p2": 0.8,
        "p3": 1.2,
        "p4": 0.5,
        "p5": 0.3,
        "p6": 0.9,
        "p7": -0.2,
    }


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


class TestStudioNetwork:
    def test_build_talent_sharing(self, production_credits, anime_map):
        from src.analysis.studio.network import build_talent_sharing_network

        g = build_talent_sharing_network(production_credits, anime_map)
        assert g.number_of_nodes() >= 0

    def test_build_coproduction(self, anime_map):
        from src.analysis.studio.network import build_coproduction_network

        g = build_coproduction_network(anime_map)
        assert g.number_of_nodes() >= 0
        # a2 has StudioA and StudioB -> should create edge
        if g.number_of_edges() > 0:
            assert g.has_edge("StudioA", "StudioB")

    def test_compute_studio_network(self, production_credits, anime_map):
        from src.analysis.studio.network import compute_studio_network

        result = compute_studio_network(production_credits, anime_map)
        assert result.talent_sharing_graph is not None
        assert result.coproduction_graph is not None


# ============================================================
# Talent Pipeline Tests
# ============================================================


class TestTalentPipeline:
    def test_compute_talent_pipeline(self, production_credits, anime_map, person_fe):
        from src.analysis.talent_pipeline import compute_talent_pipeline

        result = compute_talent_pipeline(production_credits, anime_map, person_fe)
        assert isinstance(result.flow_matrix, dict)
        assert isinstance(result.brain_drain_index, dict)
        assert isinstance(result.retention_rates, dict)


# ============================================================
# Genre Ecosystem Tests
# ============================================================


class TestGenreEcosystem:
    def test_compute_genre_ecosystem(self, production_credits, anime_map):
        from src.analysis.genre.ecosystem import compute_genre_ecosystem

        result = compute_genre_ecosystem(production_credits, anime_map)
        assert isinstance(result.trends, dict)
        assert isinstance(result.staffing, dict)
        # Action appears in 3 anime -> should have a trend
        if "Action" in result.trends:
            assert result.trends["Action"].trend_class != ""


# ============================================================
# Genre Network Tests
# ============================================================


class TestGenreNetwork:
    def test_compute_pmi(self, anime_list):
        from src.analysis.genre.network import _compute_pmi

        pmi = _compute_pmi(anime_list, min_count=1)
        assert isinstance(pmi, dict)
        # Action and Adventure co-occur in a1
        for pair, val in pmi.items():
            assert isinstance(val, float)

    def test_compute_genre_network(self, anime_list):
        from src.analysis.genre.network import compute_genre_network

        result = compute_genre_network(anime_list)
        assert result.pmi_graph is not None
        assert isinstance(result.genre_families, dict)


# ============================================================
# Genre Quality Tests
# ============================================================


class TestGenreQuality:
    def test_compute_genre_quality(self, production_credits, anime_map, person_fe):
        from src.analysis.genre.quality import compute_genre_quality

        result = compute_genre_quality(production_credits, anime_map, person_fe)
        assert isinstance(result.quality, dict)
        assert isinstance(result.saturation, dict)
        assert isinstance(result.mobility, dict)


# ============================================================
# Synthetic VA Data Tests
# ============================================================


class TestSyntheticVAData:
    def test_generate_synthetic_va_data(self):
        from src.synthetic import generate_synthetic_data, generate_synthetic_va_data

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


class TestStudioClustering:
    def test_name_clusters_by_rank(self):
        import numpy as np
        from src.analysis.studio.clustering import _name_clusters_by_rank

        centers = np.array(
            [
                [1.0, 10.0],
                [3.0, 5.0],
                [2.0, 1.0],
            ]
        )
        specs = [(0, ["high", "mid", "low"]), (1, ["big", "medium", "small"])]
        names = _name_clusters_by_rank(centers, specs)
        assert len(names) == 3
        # Cluster 1 (feat 0 = 3.0, highest) should be "high"
        assert "high" in names[1]


# ============================================================
# Model Tests
# ============================================================


class TestVAScoreResult:
    def test_va_score_result_creation(self):
        from src.models import VAScoreResult

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
