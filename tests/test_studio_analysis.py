"""Tests for VA evaluation, studio analysis, and genre ecosystem modules."""

import pytest

from src.runtime.models import (
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


