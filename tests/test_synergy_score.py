"""Tests for synergy_score module."""

import json
import math

from src.analysis.synergy_score import (
    _build_sequel_chains,
    _compute_pair_synergy,
    _compute_quality_factor,
    _compute_group_synergy,
    _aggregate_person_synergy,
    _extract_senior_staff,
    _track_pair_occurrences,
    compute_synergy_scores,
    PairHistory,
)
from src.models import Anime, Credit, Role


def _make_chain_anime(
    n: int = 3,
    base_score: float = 7.0,
    score_delta: float = 0.5,
) -> dict[str, Anime]:
    """Create a chain of anime with sequel relations.

    Returns anime_map with a1..aN linked by SEQUEL/PREQUEL.
    """
    anime_map = {}
    for i in range(1, n + 1):
        relations = []
        if i > 1:
            relations.append({
                "related_anime_id": f"a{i - 1}",
                "relation_type": "PREQUEL",
            })
        if i < n:
            relations.append({
                "related_anime_id": f"a{i + 1}",
                "relation_type": "SEQUEL",
            })
        anime_map[f"a{i}"] = Anime(
            id=f"a{i}",
            title_en=f"Franchise Part {i}",
            year=2018 + i,
            score=base_score + (i - 1) * score_delta,
            relations_json=json.dumps(relations),
        )
    return anime_map


def _credit(pid: str, aid: str, role: Role = Role.DIRECTOR) -> Credit:
    return Credit(person_id=pid, anime_id=aid, role=role, source="test")


# ============================================================
# Chain Detection
# ============================================================

class TestBuildSequelChains:
    def test_simple_trilogy(self):
        """Three anime linked by SEQUEL/PREQUEL → one chain."""
        anime_map = _make_chain_anime(3)
        chains = _build_sequel_chains(anime_map)
        assert len(chains) == 1
        assert len(chains[0]) == 3

    def test_two_separate_franchises(self):
        """Two independent franchises → two chains."""
        anime_map = _make_chain_anime(2)
        # Add second franchise
        for i in range(3, 5):
            relations = []
            if i > 3:
                relations.append({"related_anime_id": f"a{i - 1}", "relation_type": "PREQUEL"})
            if i < 4:
                relations.append({"related_anime_id": f"a{i + 1}", "relation_type": "SEQUEL"})
            anime_map[f"a{i}"] = Anime(
                id=f"a{i}",
                title_en=f"Other Series {i}",
                year=2020 + i,
                score=7.0,
                relations_json=json.dumps(relations),
            )
        chains = _build_sequel_chains(anime_map)
        assert len(chains) == 2

    def test_single_anime_no_chain(self):
        """Single anime with no relations → no chains."""
        anime_map = {
            "a1": Anime(id="a1", title_en="Standalone", year=2020, score=8.0),
        }
        chains = _build_sequel_chains(anime_map)
        assert len(chains) == 0

    def test_alternative_relation_excluded(self):
        """ALTERNATIVE relation type should not create chain links."""
        anime_map = {
            "a1": Anime(
                id="a1", title_en="Original", year=2020, score=8.0,
                relations_json=json.dumps([{"related_anime_id": "a2", "relation_type": "ALTERNATIVE"}]),
            ),
            "a2": Anime(
                id="a2", title_en="Alt Version", year=2021, score=7.0,
                relations_json=json.dumps([{"related_anime_id": "a1", "relation_type": "ALTERNATIVE"}]),
            ),
        }
        chains = _build_sequel_chains(anime_map)
        assert len(chains) == 0

    def test_chain_sorted_by_year(self):
        """Chain members should be sorted by year."""
        anime_map = _make_chain_anime(3)
        chains = _build_sequel_chains(anime_map)
        years = [anime_map[aid].year for aid in chains[0]]
        assert years == sorted(years)


# ============================================================
# Senior Staff Extraction
# ============================================================

class TestExtractSeniorStaff:
    def test_filters_to_cooccurrence_roles(self):
        """Only COOCCURRENCE_ROLES are extracted."""
        anime_map = {"a1": Anime(id="a1", title_en="Test", year=2020)}
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.KEY_ANIMATOR),  # Not in COOCCURRENCE_ROLES
            _credit("p3", "a1", Role.CHARACTER_DESIGNER),
        ]
        staff = _extract_senior_staff(credits, anime_map)
        assert "p1" in staff["a1"]
        assert "p2" not in staff["a1"]
        assert "p3" in staff["a1"]

    def test_multiple_roles_per_person(self):
        """Person with multiple senior roles gets all of them."""
        anime_map = {"a1": Anime(id="a1", title_en="Test", year=2020)}
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p1", "a1", Role.SCREENPLAY),
        ]
        staff = _extract_senior_staff(credits, anime_map)
        assert "director" in staff["a1"]["p1"]
        assert "screenplay" in staff["a1"]["p1"]


# ============================================================
# Pair Tracking
# ============================================================

class TestTrackPairOccurrences:
    def test_pair_across_chain(self):
        """Pair co-occurring across all 3 works in a chain → tracked."""
        chains = [["a1", "a2", "a3"]]
        staff_map = {
            "a1": {"p1": {"director"}, "p2": {"character_designer"}},
            "a2": {"p1": {"director"}, "p2": {"character_designer"}},
            "a3": {"p1": {"director"}, "p2": {"character_designer"}},
        }
        histories = _track_pair_occurrences(chains, staff_map)
        pair_key = frozenset({"p1", "p2"})
        assert pair_key in histories
        assert histories[pair_key][0].collab_count == 3

    def test_single_collab_not_tracked(self):
        """Pair meeting only once in a chain → not tracked (need 2+)."""
        chains = [["a1", "a2"]]
        staff_map = {
            "a1": {"p1": {"director"}, "p2": {"character_designer"}},
            "a2": {"p1": {"director"}},  # p2 not in a2
        }
        histories = _track_pair_occurrences(chains, staff_map)
        pair_key = frozenset({"p1", "p2"})
        assert pair_key not in histories

    def test_pair_across_multiple_chains(self):
        """Pair working together in two separate chains → two PairHistory entries."""
        chains = [["a1", "a2"], ["a3", "a4"]]
        staff_map = {
            "a1": {"p1": {"director"}, "p2": {"character_designer"}},
            "a2": {"p1": {"director"}, "p2": {"character_designer"}},
            "a3": {"p1": {"director"}, "p2": {"character_designer"}},
            "a4": {"p1": {"director"}, "p2": {"character_designer"}},
        }
        histories = _track_pair_occurrences(chains, staff_map)
        pair_key = frozenset({"p1", "p2"})
        assert len(histories[pair_key]) == 2


# ============================================================
# Pair Synergy Scoring
# ============================================================

class TestComputePairSynergy:
    def test_single_collab_zero(self):
        """1 collaboration → 0 synergy."""
        anime_map = _make_chain_anime(1)
        h = PairHistory(anime_ids=["a1"], collab_count=1)
        assert _compute_pair_synergy(h, anime_map) == 0.0

    def test_two_collabs(self):
        """2 collaborations → 0.3 × quality_factor."""
        anime_map = _make_chain_anime(2)
        h = PairHistory(anime_ids=["a1", "a2"], collab_count=2)
        quality = _compute_quality_factor(["a1", "a2"], anime_map)
        expected = 0.3 * quality
        assert abs(_compute_pair_synergy(h, anime_map) - expected) < 0.001

    def test_three_collabs_unified_formula(self):
        """3 collaborations → log1p(3)/log1p(2) × 0.3 × quality_factor."""
        anime_map = _make_chain_anime(3)
        h = PairHistory(anime_ids=["a1", "a2", "a3"], collab_count=3)
        quality = _compute_quality_factor(["a1", "a2", "a3"], anime_map)
        expected = math.log1p(3) / math.log1p(2) * 0.3 * quality
        assert abs(_compute_pair_synergy(h, anime_map) - expected) < 0.001

    def test_n2_n3_no_large_jump(self):
        """n=2 → n=3 should be a smooth increase, not a discontinuous jump.

        Previously n=2 gave 0.3×q and n=3 gave log1p(3)×q ≈ 1.386×q (4.6x jump).
        The unified formula yields n=2: 0.3×q, n=3: ~0.379×q (ratio ≈ 1.26).
        """
        anime_map = _make_chain_anime(3)
        h2 = PairHistory(anime_ids=["a1", "a2"], collab_count=2)
        h3 = PairHistory(anime_ids=["a1", "a2", "a3"], collab_count=3)
        s2 = _compute_pair_synergy(h2, anime_map)
        s3 = _compute_pair_synergy(h3, anime_map)
        # s3 > s2 (monotonic)
        assert s3 > s2
        # The ratio should be modest (< 2x), not the old 4.6x jump
        ratio = s3 / s2 if s2 > 0 else float("inf")
        assert ratio < 2.0, f"n=2→n=3 ratio {ratio:.2f} is too large (should be ~1.26)"

    def test_higher_collab_count_higher_synergy(self):
        """More collaborations → higher synergy (monotonic)."""
        anime_map = _make_chain_anime(5)
        h2 = PairHistory(anime_ids=["a1", "a2"], collab_count=2)
        h5 = PairHistory(anime_ids=["a1", "a2", "a3", "a4", "a5"], collab_count=5)
        s2 = _compute_pair_synergy(h2, anime_map)
        s5 = _compute_pair_synergy(h5, anime_map)
        assert s5 > s2

    def test_improving_scores_boost_synergy(self):
        """Rising anime scores increase quality_factor → higher synergy."""
        # Rising scores
        anime_rising = _make_chain_anime(3, base_score=6.0, score_delta=1.5)
        # Flat scores
        anime_flat = _make_chain_anime(3, base_score=6.0, score_delta=0.0)

        h = PairHistory(anime_ids=["a1", "a2", "a3"], collab_count=3)
        s_rising = _compute_pair_synergy(h, anime_rising)
        s_flat = _compute_pair_synergy(h, anime_flat)
        assert s_rising > s_flat

    def test_declining_scores_reduce_synergy(self):
        """Declining anime scores → quality_factor < 1.0 → lower synergy."""
        anime_declining = _make_chain_anime(3, base_score=9.0, score_delta=-2.0)
        quality = _compute_quality_factor(["a1", "a2", "a3"], anime_declining)
        assert quality < 1.0

    def test_no_scores_defaults_to_1(self):
        """Anime without scores → quality_factor = 1.0."""
        anime_map = {
            "a1": Anime(id="a1", title_en="NoScore 1", year=2020),
            "a2": Anime(id="a2", title_en="NoScore 2", year=2021),
        }
        quality = _compute_quality_factor(["a1", "a2"], anime_map)
        assert quality == 1.0

    def test_quality_factor_clamped_to_2(self):
        """Quality factor can't exceed 2.0."""
        anime_map = {
            "a1": Anime(id="a1", title_en="Low", year=2020, score=1.0),
            "a2": Anime(id="a2", title_en="High", year=2021, score=10.0),
        }
        quality = _compute_quality_factor(["a1", "a2"], anime_map)
        assert quality == 2.0


# ============================================================
# Group Synergy
# ============================================================

class TestGroupSynergy:
    def test_trio_multiplier(self):
        """Trio gets 1.1× multiplier."""
        pair_synergies = {
            frozenset({"p1", "p2"}): 1.0,
            frozenset({"p1", "p3"}): 1.0,
            frozenset({"p2", "p3"}): 1.0,
        }
        group = frozenset({"p1", "p2", "p3"})
        result = _compute_group_synergy(group, pair_synergies)
        assert abs(result - 3.0 * 1.1) < 0.001

    def test_quartet_multiplier(self):
        """Quartet gets 1.2× multiplier."""
        pair_synergies = {
            frozenset({f"p{i}", f"p{j}"}): 1.0
            for i in range(1, 5) for j in range(i + 1, 5)
        }
        group = frozenset({"p1", "p2", "p3", "p4"})
        result = _compute_group_synergy(group, pair_synergies)
        # 6 pairs × 1.0 × 1.2 = 7.2
        assert abs(result - 6.0 * 1.2) < 0.001

    def test_pair_no_multiplier(self):
        """A pair (2 members) gets no multiplier."""
        pair_synergies = {frozenset({"p1", "p2"}): 1.0}
        group = frozenset({"p1", "p2"})
        result = _compute_group_synergy(group, pair_synergies)
        assert abs(result - 1.0) < 0.001

    def test_missing_pair_treated_as_zero(self):
        """If a pair in the group has no synergy, it contributes 0."""
        pair_synergies = {
            frozenset({"p1", "p2"}): 1.0,
            frozenset({"p1", "p3"}): 1.0,
            # p2-p3 pair missing
        }
        group = frozenset({"p1", "p2", "p3"})
        result = _compute_group_synergy(group, pair_synergies)
        assert abs(result - 2.0 * 1.1) < 0.001


# ============================================================
# Person Aggregation
# ============================================================

class TestAggregatePersonSynergy:
    def test_basic_aggregation(self):
        """Person participating in 2 pairs → sum of synergies."""
        pair_synergies = {
            frozenset({"p1", "p2"}): 0.5,
            frozenset({"p1", "p3"}): 0.3,
        }
        pair_histories = {
            frozenset({"p1", "p2"}): [PairHistory(anime_ids=["a1", "a2"], collab_count=2)],
            frozenset({"p1", "p3"}): [PairHistory(anime_ids=["a1", "a2"], collab_count=2)],
        }
        anime_map = _make_chain_anime(2)
        result = _aggregate_person_synergy(pair_synergies, anime_map, pair_histories)
        assert "p1" in result
        assert abs(result["p1"].total_synergy - 0.8) < 0.001
        assert result["p1"].pair_count == 2

    def test_zero_synergy_excluded(self):
        """Pairs with 0 synergy don't appear in aggregation."""
        pair_synergies = {
            frozenset({"p1", "p2"}): 0.0,
        }
        pair_histories = {}
        result = _aggregate_person_synergy(pair_synergies, {}, pair_histories)
        assert "p1" not in result

    def test_top_pairs_limited_to_5(self):
        """top_pairs should have at most 5 entries."""
        pair_synergies = {
            frozenset({"p1", f"p{i}"}): 0.1 * i for i in range(2, 10)
        }
        pair_histories = {
            k: [PairHistory(anime_ids=["a1", "a2"], collab_count=2)]
            for k in pair_synergies
        }
        anime_map = _make_chain_anime(2)
        result = _aggregate_person_synergy(pair_synergies, anime_map, pair_histories)
        assert len(result["p1"].top_pairs) <= 5

    def test_top_pairs_sorted_by_synergy(self):
        """top_pairs should be sorted by synergy_value descending."""
        pair_synergies = {
            frozenset({"p1", "p2"}): 0.3,
            frozenset({"p1", "p3"}): 0.8,
            frozenset({"p1", "p4"}): 0.1,
        }
        pair_histories = {
            k: [PairHistory(anime_ids=["a1", "a2"], collab_count=2)]
            for k in pair_synergies
        }
        anime_map = _make_chain_anime(2)
        result = _aggregate_person_synergy(pair_synergies, anime_map, pair_histories)
        values = [p["synergy_value"] for p in result["p1"].top_pairs]
        assert values == sorted(values, reverse=True)


# ============================================================
# Full Pipeline (compute_synergy_scores)
# ============================================================

class TestComputeSynergyScores:
    def test_output_structure(self):
        """Output has required top-level keys."""
        anime_map = _make_chain_anime(3)
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.CHARACTER_DESIGNER),
            _credit("p1", "a2", Role.DIRECTOR),
            _credit("p2", "a2", Role.CHARACTER_DESIGNER),
            _credit("p1", "a3", Role.DIRECTOR),
            _credit("p2", "a3", Role.CHARACTER_DESIGNER),
        ]
        result = compute_synergy_scores(credits, anime_map)
        assert "person_synergy_boosts" in result
        assert "top_synergy_pairs" in result
        assert "summary" in result

    def test_summary_counts(self):
        """Summary has correct count fields."""
        anime_map = _make_chain_anime(3)
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.CHARACTER_DESIGNER),
            _credit("p1", "a2", Role.DIRECTOR),
            _credit("p2", "a2", Role.CHARACTER_DESIGNER),
            _credit("p1", "a3", Role.DIRECTOR),
            _credit("p2", "a3", Role.CHARACTER_DESIGNER),
        ]
        result = compute_synergy_scores(credits, anime_map)
        summary = result["summary"]
        assert summary["franchise_count"] >= 1
        assert summary["synergy_active_pairs"] >= 1

    def test_no_chains_returns_empty(self):
        """No sequel relations → empty result."""
        anime_map = {
            "a1": Anime(id="a1", title_en="Standalone", year=2020, score=7.0),
        }
        credits = [_credit("p1", "a1", Role.DIRECTOR)]
        result = compute_synergy_scores(credits, anime_map)
        assert result["person_synergy_boosts"] == {}
        assert result["top_synergy_pairs"] == []
        assert result["summary"]["franchise_count"] == 0

    def test_non_senior_roles_excluded(self):
        """KEY_ANIMATOR (non-senior) pairs should not generate synergy."""
        anime_map = _make_chain_anime(3)
        credits = [
            _credit("p1", "a1", Role.KEY_ANIMATOR),
            _credit("p2", "a1", Role.KEY_ANIMATOR),
            _credit("p1", "a2", Role.KEY_ANIMATOR),
            _credit("p2", "a2", Role.KEY_ANIMATOR),
        ]
        result = compute_synergy_scores(credits, anime_map)
        assert result["summary"]["synergy_active_pairs"] == 0


# ============================================================
# Edge Cases
# ============================================================

class TestEdgeCases:
    def test_empty_credits(self):
        """Empty credits list → empty result."""
        result = compute_synergy_scores([], {})
        assert result["summary"]["franchise_count"] == 0

    def test_empty_anime_map(self):
        """Empty anime_map → empty result."""
        credits = [_credit("p1", "a1", Role.DIRECTOR)]
        result = compute_synergy_scores(credits, {})
        assert result["summary"]["franchise_count"] == 0

    def test_malformed_relations_json(self):
        """Malformed JSON in relations_json should not crash."""
        anime_map = {
            "a1": Anime(
                id="a1", title_en="Bad JSON", year=2020,
                relations_json="not valid json{",
            ),
        }
        chains = _build_sequel_chains(anime_map)
        assert len(chains) == 0

    def test_side_story_included_in_chain(self):
        """SIDE_STORY relation should be included in chains."""
        anime_map = {
            "a1": Anime(
                id="a1", title_en="Main", year=2020, score=7.0,
                relations_json=json.dumps([{"related_anime_id": "a2", "relation_type": "SIDE_STORY"}]),
            ),
            "a2": Anime(
                id="a2", title_en="Side Story", year=2021, score=7.5,
                relations_json=json.dumps([{"related_anime_id": "a1", "relation_type": "PARENT"}]),
            ),
        }
        chains = _build_sequel_chains(anime_map)
        assert len(chains) == 1

    def test_parent_relation_included(self):
        """PARENT relation should be included in chains."""
        anime_map = {
            "a1": Anime(
                id="a1", title_en="Parent", year=2020, score=8.0,
                relations_json=json.dumps([{"related_anime_id": "a2", "relation_type": "PARENT"}]),
            ),
            "a2": Anime(
                id="a2", title_en="Child", year=2021, score=7.0,
                relations_json=json.dumps([{"related_anime_id": "a1", "relation_type": "PARENT"}]),
            ),
        }
        chains = _build_sequel_chains(anime_map)
        assert len(chains) == 1
