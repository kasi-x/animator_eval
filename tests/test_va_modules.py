"""Tests for Voice Actor analysis modules (§6.3)."""

import pytest

# VA modules import AnimeAnalysis but duck-type genres; use BronzeAnime so
# character_diversity and replacement_difficulty can access anime.genres.
from src.runtime.models import BronzeAnime as Anime, Character, CharacterVoiceActor, Credit, Role


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _anime(aid, year=2020, genres=None):
    return Anime(id=aid, title_en=f"Anime {aid}", year=year, genres=genres or [])


def _anime_analysis(aid, year=2020, episodes=12, duration=24):
    # Use BronzeAnime so that modules accessing .genres don't crash.
    # BronzeAnime has all fields AnimeAnalysis has (year, episodes, duration) plus genres.
    return Anime(id=aid, title_en=f"Anime {aid}", year=year,
                 episodes=episodes, duration=duration)


def _char(cid, gender=None):
    return Character(id=cid, name_en=f"Char {cid}", gender=gender)


def _cva(person_id, anime_id, char_id="c1", role="MAIN"):
    return CharacterVoiceActor(
        person_id=person_id, character_id=char_id,
        anime_id=anime_id, character_role=role,
    )


def _credit(pid, aid, role):
    return Credit(person_id=pid, anime_id=aid, role=role, raw_role=role.value)


# ---------------------------------------------------------------------------
# §6.3-A: compute_va_integrated_value
# ---------------------------------------------------------------------------

class TestVaIntegratedValue:
    def test_empty_dicts_return_empty(self):
        from src.analysis.va.integrated_value import compute_va_integrated_value
        result = compute_va_integrated_value({}, {}, {}, {}, {}, {})
        assert result == {}

    def test_two_persons_get_scores(self):
        from src.analysis.va.integrated_value import compute_va_integrated_value
        result = compute_va_integrated_value(
            person_fe={"va1": 1.0, "va2": 0.5},
            birank={"va1": 0.8, "va2": 0.4},
            sd_exposure={"va1": 0.5, "va2": 0.2},
            awcc={"va1": 0.6, "va2": 0.3},
            patronage={"va1": 0.4, "va2": 0.1},
            dormancy={"va1": 1.0, "va2": 1.0},
        )
        assert "va1" in result
        assert "va2" in result

    def test_dormancy_zero_gives_zero_score(self):
        from src.analysis.va.integrated_value import compute_va_integrated_value
        result = compute_va_integrated_value(
            person_fe={"va1": 1.0},
            birank={"va1": 0.8},
            sd_exposure={},
            awcc={},
            patronage={},
            dormancy={"va1": 0.0},
        )
        assert result["va1"] == pytest.approx(0.0)

    def test_relative_ordering_preserved(self):
        """Person with higher components should rank higher."""
        from src.analysis.va.integrated_value import compute_va_integrated_value
        result = compute_va_integrated_value(
            person_fe={"va1": 1.0, "va2": 0.1},
            birank={"va1": 1.0, "va2": 0.1},
            sd_exposure={"va1": 1.0, "va2": 0.1},
            awcc={"va1": 1.0, "va2": 0.1},
            patronage={"va1": 1.0, "va2": 0.1},
            dormancy={"va1": 1.0, "va2": 1.0},
        )
        assert result["va1"] > result["va2"]

    def test_missing_components_treated_as_zero(self):
        """VA with no birank still gets a score from other components."""
        from src.analysis.va.integrated_value import compute_va_integrated_value
        result = compute_va_integrated_value(
            person_fe={"va1": 1.0},
            birank={},  # va1 absent
            sd_exposure={},
            awcc={},
            patronage={},
            dormancy={"va1": 1.0},
        )
        assert "va1" in result


# ---------------------------------------------------------------------------
# §6.3-B: character_diversity
# ---------------------------------------------------------------------------

class TestCharacterDiversity:
    def test_empty_returns_empty(self):
        from src.analysis.va.character_diversity import compute_character_diversity
        result = compute_character_diversity([], {}, {})
        assert result == {}

    def test_single_character_newcomer_tier(self):
        """VA with one character gets newcomer casting tier."""
        from src.analysis.va.character_diversity import compute_character_diversity
        anime_map = {"a1": _anime("a1", genres=["Action"])}
        char_map = {"c1": _char("c1")}
        va_credits = [_cva("va1", "a1", char_id="c1", role="MAIN")]
        result = compute_character_diversity(va_credits, anime_map, char_map)
        assert "va1" in result
        assert result["va1"].casting_tier == "newcomer"

    def test_diverse_portfolio_higher_genre_entropy(self):
        """VA with diverse genres has higher entropy than specialist."""
        from src.analysis.va.character_diversity import compute_character_diversity
        diverse_genres = ["Action", "Romance", "Comedy", "Horror", "Sci-Fi",
                          "Fantasy", "Slice of Life", "Mystery"]
        specialist_genres = ["Action"] * 8

        anime_diverse = {f"a{i}": _anime(f"a{i}", genres=[diverse_genres[i]])
                         for i in range(8)}
        anime_special = {f"b{i}": _anime(f"b{i}", genres=[specialist_genres[i]])
                         for i in range(8)}
        char_map_d = {f"c{i}": _char(f"c{i}") for i in range(8)}
        char_map_s = {f"d{i}": _char(f"d{i}") for i in range(8)}

        credits_diverse = [_cva("va_diverse", f"a{i}", f"c{i}", "MAIN") for i in range(8)]
        credits_special = [_cva("va_spec", f"b{i}", f"d{i}", "MAIN") for i in range(8)]

        result_d = compute_character_diversity(credits_diverse, anime_diverse, char_map_d)
        result_s = compute_character_diversity(credits_special, anime_special, char_map_s)

        if "va_diverse" in result_d and "va_spec" in result_s:
            assert result_d["va_diverse"].genre_entropy >= result_s["va_spec"].genre_entropy

    def test_metrics_dataclass_fields(self):
        """Result has all expected fields."""
        from src.analysis.va.character_diversity import (
            CharacterDiversityMetrics,
            compute_character_diversity,
        )
        anime_map = {f"a{i}": _anime(f"a{i}", genres=[f"Genre{i}"]) for i in range(6)}
        char_map = {f"c{i}": _char(f"c{i}") for i in range(6)}
        va_credits = [_cva("va1", f"a{i}", f"c{i}", "MAIN") for i in range(6)]
        result = compute_character_diversity(va_credits, anime_map, char_map)
        if "va1" in result:
            m = result["va1"]
            assert isinstance(m, CharacterDiversityMetrics)
            assert 0.0 <= m.cdi <= 1.0
            assert m.casting_tier in ("lead_specialist", "versatile", "ensemble", "newcomer")


# ---------------------------------------------------------------------------
# §6.3-C: va_trust
# ---------------------------------------------------------------------------

class TestVaTrust:
    def test_empty_returns_empty(self):
        from src.analysis.va.trust import compute_va_trust
        result = compute_va_trust([], [], {})
        assert result == {}

    def test_va_with_sound_director_gets_trust(self):
        from src.analysis.va.trust import compute_va_trust
        anime_map = {"a1": _anime("a1", year=2022)}
        va_credits = [_cva("va1", "a1", "c1", "MAIN")]
        prod_credits = [_credit("sd1", "a1", Role.SOUND_DIRECTOR)]
        result = compute_va_trust(va_credits, prod_credits, anime_map, current_year=2025)
        assert "va1" in result
        assert result["va1"] > 0.0

    def test_no_sound_director_no_trust(self):
        from src.analysis.va.trust import compute_va_trust
        anime_map = {"a1": _anime("a1", year=2022)}
        va_credits = [_cva("va1", "a1", "c1", "MAIN")]
        # No sound director credits
        result = compute_va_trust(va_credits, [], anime_map, current_year=2025)
        assert "va1" not in result

    def test_more_collabs_more_trust(self):
        from src.analysis.va.trust import compute_va_trust
        anime_map = {f"a{i}": _anime(f"a{i}", year=2020 + i) for i in range(4)}
        # va1: 4 works with sd1
        va_credits_many = [_cva("va1", f"a{i}", f"c{i}", "MAIN") for i in range(4)]
        # va2: 1 work with sd1
        va_credits_few = [_cva("va2", "a0", "cx", "MAIN")]

        prod = [_credit("sd1", f"a{i}", Role.SOUND_DIRECTOR) for i in range(4)]

        result = compute_va_trust(
            va_credits_many + va_credits_few, prod, anime_map, current_year=2025
        )
        assert result.get("va1", 0) > result.get("va2", 0)


# ---------------------------------------------------------------------------
# §6.3-D: va_patronage
# ---------------------------------------------------------------------------

class TestVaPatronage:
    def test_empty_returns_empty(self):
        from src.analysis.va.trust import compute_va_patronage
        result = compute_va_patronage([], [], {}, {})
        assert result == {}

    def test_patronage_scales_with_sd_birank(self):
        from src.analysis.va.trust import compute_va_patronage
        anime_map = {"a1": _anime("a1"), "a2": _anime("a2")}
        prod = [
            _credit("sd_high", "a1", Role.SOUND_DIRECTOR),
            _credit("sd_low", "a2", Role.SOUND_DIRECTOR),
        ]
        va_credits = [
            _cva("va1", "a1", "c1", "MAIN"),
            _cva("va2", "a2", "c2", "MAIN"),
        ]
        birank = {"sd_high": 1.0, "sd_low": 0.1}
        result = compute_va_patronage(va_credits, prod, anime_map, birank)
        assert result.get("va1", 0) > result.get("va2", 0)


# ---------------------------------------------------------------------------
# §6.3-E: replacement_difficulty
# ---------------------------------------------------------------------------

class TestReplacementDifficulty:
    def test_empty_returns_empty(self):
        from src.analysis.va.replacement_difficulty import compute_replacement_difficulty
        result = compute_replacement_difficulty([], {})
        assert result == {}

    def test_rdi_between_0_and_1(self):
        from src.analysis.va.replacement_difficulty import compute_replacement_difficulty
        anime_map = {f"a{i}": _anime(f"a{i}", genres=[f"Genre{i % 3}"]) for i in range(8)}
        va_credits = [_cva("va1", f"a{i}", f"c{i}", "MAIN") for i in range(8)]
        result = compute_replacement_difficulty(va_credits, anime_map, min_characters=5)
        if "va1" in result:
            assert 0.0 <= result["va1"].rdi <= 1.0

    def test_min_characters_filter(self):
        """VAs with fewer than min_characters are excluded."""
        from src.analysis.va.replacement_difficulty import compute_replacement_difficulty
        anime_map = {"a1": _anime("a1")}
        va_credits = [_cva("va1", "a1", "c1", "MAIN")]
        result = compute_replacement_difficulty(va_credits, anime_map, min_characters=5)
        assert "va1" not in result


# ---------------------------------------------------------------------------
# §6.3-F: VA AKM
# ---------------------------------------------------------------------------

class TestVaAkm:
    def _make_data(self):
        """Synthetic: 3 VAs, 2 sound directors, 3 anime."""
        anime_map = {
            f"a{i}": _anime_analysis(f"a{i}", year=2020 + i, episodes=12)
            for i in range(3)
        }
        va_credits = [
            _cva("va0", "a0", "c0", "MAIN"),
            _cva("va0", "a1", "c0b", "MAIN"),
            _cva("va1", "a0", "c1", "MAIN"),
            _cva("va1", "a2", "c1b", "MAIN"),
            _cva("va2", "a1", "c2", "SUPPORTING"),
            _cva("va2", "a2", "c2b", "MAIN"),
        ]
        production_credits = [
            Credit(person_id="sd1", anime_id="a0", role=Role.SOUND_DIRECTOR,
                   raw_role="sound_director"),
            Credit(person_id="sd1", anime_id="a1", role=Role.SOUND_DIRECTOR,
                   raw_role="sound_director"),
            Credit(person_id="sd2", anime_id="a2", role=Role.SOUND_DIRECTOR,
                   raw_role="sound_director"),
        ]
        return dict(va_credits=va_credits, production_credits=production_credits,
                    anime_map=anime_map)

    def test_returns_result_with_person_fe(self):
        from src.analysis.va.akm import estimate_va_akm
        result = estimate_va_akm(**self._make_data())
        assert hasattr(result, "person_fe")
        assert hasattr(result, "sd_fe")
        assert len(result.person_fe) > 0

    def test_person_fe_range(self):
        from src.analysis.va.akm import estimate_va_akm
        result = estimate_va_akm(**self._make_data())
        for _, fe in result.person_fe.items():
            assert -15 < fe < 15, f"person FE {fe} out of expected range"

    def test_single_va_does_not_crash(self):
        """Edge case: only 1 VA — no movers, falls back to VA-only means."""
        from src.analysis.va.akm import estimate_va_akm
        anime_map = {"a0": _anime_analysis("a0", year=2022)}
        va_credits = [_cva("va_solo", "a0", "c0", "MAIN")]
        prod = [Credit(person_id="sd1", anime_id="a0", role=Role.SOUND_DIRECTOR,
                       raw_role="sound_director")]
        result = estimate_va_akm(va_credits, prod, anime_map)
        assert isinstance(result.person_fe, dict)
        assert result.n_observations >= 0

    def test_empty_credits_returns_empty_result(self):
        from src.analysis.va.akm import estimate_va_akm
        result = estimate_va_akm([], [], {})
        assert result.person_fe == {}
        assert result.n_observations == 0


# ---------------------------------------------------------------------------
# §6.3-G: VA Collaboration Graph
# ---------------------------------------------------------------------------

class TestVaGraph:
    def _make_collab_data(self):
        anime_map = {f"a{i}": _anime_analysis(f"a{i}", year=2020 + i) for i in range(3)}
        va_credits = [
            _cva("va1", "a0", "c1a", "MAIN"),
            _cva("va2", "a0", "c2a", "MAIN"),
            _cva("va1", "a1", "c1b", "MAIN"),
            _cva("va2", "a1", "c2b", "SUPPORTING"),
            _cva("va3", "a2", "c3", "MAIN"),
        ]
        return va_credits, anime_map

    def test_collab_graph_has_va_nodes(self):
        from src.analysis.va.graph import build_va_collaboration_graph
        va_credits, anime_map = self._make_collab_data()
        g = build_va_collaboration_graph(va_credits, anime_map)
        assert g.number_of_nodes() > 0

    def test_collab_graph_edge_weight_positive(self):
        from src.analysis.va.graph import build_va_collaboration_graph
        va_credits, anime_map = self._make_collab_data()
        g = build_va_collaboration_graph(va_credits, anime_map)
        for _, _, d in g.edges(data=True):
            assert d.get("weight", 0) > 0

    def test_anime_bipartite_graph_nodes(self):
        from src.analysis.va.graph import build_va_anime_graph
        va_credits, anime_map = self._make_collab_data()
        g = build_va_anime_graph(va_credits, anime_map)
        assert g.number_of_nodes() > 0
        # VA nodes have bipartite=0
        va_nodes = [n for n, d in g.nodes(data=True) if d.get("bipartite") == 0]
        assert len(va_nodes) > 0

    def test_empty_credits_returns_empty_graph(self):
        from src.analysis.va.graph import build_va_collaboration_graph
        g = build_va_collaboration_graph([], {})
        assert g.number_of_nodes() == 0


# ---------------------------------------------------------------------------
# §6.3-H: Ensemble Synergy
# ---------------------------------------------------------------------------

class TestEnsembleSynergy:
    def test_pair_below_min_shared_excluded(self):
        from src.analysis.va.ensemble_synergy import compute_va_ensemble_synergy
        # Only 2 shared anime — below default min_shared=3
        anime_map = {f"a{i}": _anime_analysis(f"a{i}", year=2020 + i) for i in range(2)}
        va_credits = [
            _cva("va1", "a0", "c1a", "MAIN"),
            _cva("va2", "a0", "c2a", "MAIN"),
            _cva("va1", "a1", "c1b", "MAIN"),
            _cva("va2", "a1", "c2b", "MAIN"),
        ]
        result = compute_va_ensemble_synergy(va_credits, anime_map, min_shared=3)
        assert len(result) == 0

    def test_pair_above_min_shared_included(self):
        from src.analysis.va.ensemble_synergy import compute_va_ensemble_synergy
        genres = ["Action", "Comedy", "Drama", "Fantasy"]
        anime_map = {
            f"a{i}": _anime(f"a{i}", year=2020 + i, genres=[genres[i % len(genres)]])
            for i in range(4)
        }
        va_credits = [
            _cva("va1", f"a{i}", f"c1_{i}", "MAIN") for i in range(4)
        ] + [
            _cva("va2", f"a{i}", f"c2_{i}", "MAIN") for i in range(4)
        ]
        result = compute_va_ensemble_synergy(va_credits, anime_map, min_shared=3)
        assert len(result) >= 1
        assert result[0].synergy_score > 0

    def test_synergy_sorted_descending(self):
        from src.analysis.va.ensemble_synergy import compute_va_ensemble_synergy
        genres = ["Action", "Comedy", "Drama", "Fantasy", "Horror", "Sci-Fi"]
        anime_map = {
            f"a{i}": _anime(f"a{i}", year=2020 + i, genres=[genres[i % len(genres)]])
            for i in range(6)
        }
        va_credits = (
            [_cva("va1", f"a{i}", f"c1_{i}", "MAIN") for i in range(6)]
            + [_cva("va2", f"a{i}", f"c2_{i}", "MAIN") for i in range(6)]
            + [_cva("va3", f"a{i}", f"c3_{i}", "MAIN") for i in range(3)]
            + [_cva("va4", f"a{i}", f"c4_{i}", "MAIN") for i in range(3)]
        )
        result = compute_va_ensemble_synergy(va_credits, anime_map, min_shared=3)
        scores = [s.synergy_score for s in result]
        assert scores == sorted(scores, reverse=True)
