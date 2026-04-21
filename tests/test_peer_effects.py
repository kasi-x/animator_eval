"""Tests for peer effects estimation (2SLS)."""

import networkx as nx
import pytest

from src.analysis.network.peer_effects import (
    PeerEffectResult,
    estimate_peer_effects_2sls,
)
from src.models import BronzeAnime as Anime, Credit, Role


@pytest.fixture
def peer_data():
    """3 anime, 10 persons with known peer structure.

    Creates enough data for 2SLS estimation (>= 20 observations).
    Each anime has multiple persons so leave-one-out peer means work.
    """
    anime_map = {
        "a1": Anime(id="a1", title_en="Anime 1", year=2018, score=8.5, studios=["S1"]),
        "a2": Anime(id="a2", title_en="Anime 2", year=2019, score=7.0, studios=["S1"]),
        "a3": Anime(id="a3", title_en="Anime 3", year=2020, score=6.0, studios=["S2"]),
    }

    credits = []
    # Put persons p1-p7 in anime a1 (7 persons)
    for i in range(1, 8):
        role = Role.KEY_ANIMATOR if i > 1 else Role.DIRECTOR
        credits.append(
            Credit(person_id=f"p{i}", anime_id="a1", role=role, source="test")
        )

    # Put persons p3-p10 in anime a2 (8 persons)
    for i in range(3, 11):
        role = Role.ANIMATION_DIRECTOR if i == 3 else Role.KEY_ANIMATOR
        credits.append(
            Credit(person_id=f"p{i}", anime_id="a2", role=role, source="test")
        )

    # Put persons p5-p10 in anime a3 (6 persons)
    for i in range(5, 11):
        credits.append(
            Credit(
                person_id=f"p{i}", anime_id="a3", role=Role.KEY_ANIMATOR, source="test"
            )
        )

    # Person scores
    person_scores = {f"p{i}": 50.0 + i * 5.0 for i in range(1, 11)}

    # Collaboration graph (persons who co-appear in anime)
    collab_graph = nx.Graph()
    # a1 connects p1-p7
    for i in range(1, 8):
        for j in range(i + 1, 8):
            collab_graph.add_edge(f"p{i}", f"p{j}")
    # a2 connects p3-p10
    for i in range(3, 11):
        for j in range(i + 1, 11):
            collab_graph.add_edge(f"p{i}", f"p{j}")
    # a3 connects p5-p10
    for i in range(5, 11):
        for j in range(i + 1, 11):
            collab_graph.add_edge(f"p{i}", f"p{j}")

    return credits, anime_map, person_scores, collab_graph


@pytest.fixture
def tiny_data():
    """Too few observations to estimate peer effects (< 20)."""
    anime_map = {
        "a1": Anime(id="a1", title_en="Small", year=2020, score=7.0, studios=["S1"]),
    }
    credits = [
        Credit(person_id="p1", anime_id="a1", role=Role.DIRECTOR, source="test"),
        Credit(person_id="p2", anime_id="a1", role=Role.KEY_ANIMATOR, source="test"),
    ]
    person_scores = {"p1": 80.0, "p2": 60.0}
    collab_graph = nx.Graph()
    collab_graph.add_edge("p1", "p2")
    return credits, anime_map, person_scores, collab_graph


class TestPeerEffects:
    def test_peer_effects_produces_result(self, peer_data):
        """Returns a PeerEffectResult with valid fields."""
        credits, anime_map, person_scores, collab_graph = peer_data
        result = estimate_peer_effects_2sls(
            credits, anime_map, person_scores, collab_graph
        )
        assert isinstance(result, PeerEffectResult)
        assert result.n_observations > 0

    def test_peer_boost_dict(self, peer_data):
        """person_peer_boost has entries for persons who appear in the data."""
        credits, anime_map, person_scores, collab_graph = peer_data
        result = estimate_peer_effects_2sls(
            credits, anime_map, person_scores, collab_graph
        )
        assert len(result.person_peer_boost) > 0
        # At least some persons from our fixture should have a boost value
        for pid, boost in result.person_peer_boost.items():
            assert isinstance(boost, float)

    def test_endogenous_effect_bounded(self, peer_data):
        """Endogenous effect coefficient is finite."""
        credits, anime_map, person_scores, collab_graph = peer_data
        result = estimate_peer_effects_2sls(
            credits, anime_map, person_scores, collab_graph
        )
        import math

        assert math.isfinite(result.endogenous_effect)
        assert math.isfinite(result.exogenous_effect)
        assert math.isfinite(result.own_effect)

    def test_too_few_observations(self, tiny_data):
        """Returns empty result when observations < 20."""
        credits, anime_map, person_scores, collab_graph = tiny_data
        result = estimate_peer_effects_2sls(
            credits, anime_map, person_scores, collab_graph
        )
        assert result.n_observations == 0
        assert result.person_peer_boost == {}
        assert result.identified is False

    def test_weak_instruments_flagged(self):
        """identified=False when instruments are weak (F < 10).

        Construct data where distance-2 neighbors have no predictive power
        for peer outcomes (all identical scores -> no instrument variation).
        """
        anime_map = {}
        credits = []
        # 5 anime, each with 5 persons -> 25 person-anime pairs >= 20
        for a_idx in range(5):
            aid = f"a{a_idx}"
            anime_map[aid] = Anime(
                id=aid, title_en=f"Anime {a_idx}", year=2020, score=7.0, studios=["S1"]
            )
            for p_idx in range(5):
                pid = f"p{a_idx}_{p_idx}"
                credits.append(
                    Credit(
                        person_id=pid,
                        anime_id=aid,
                        role=Role.KEY_ANIMATOR,
                        source="test",
                    )
                )

        # All persons have identical scores -> instrument has no variation
        person_scores = {c.person_id: 50.0 for c in credits}

        # Minimal graph (no distance-2 neighbors)
        collab_graph = nx.Graph()
        for a_idx in range(5):
            pids = [f"p{a_idx}_{p}" for p in range(5)]
            for i in range(len(pids)):
                for j in range(i + 1, len(pids)):
                    collab_graph.add_edge(pids[i], pids[j])

        result = estimate_peer_effects_2sls(
            credits, anime_map, person_scores, collab_graph
        )
        # With identical scores, instruments are weak
        assert not result.identified
