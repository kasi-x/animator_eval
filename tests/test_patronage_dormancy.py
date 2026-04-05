"""Tests for patronage_dormancy module — patronage premium and dormancy penalty."""

import math

import pytest

from src.analysis.patronage_dormancy import (
    compute_career_aware_dormancy,
    compute_dormancy_penalty,
    compute_patronage_and_dormancy,
    compute_patronage_premium,
)
from src.models import Anime, Credit, Role


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_anime(aid: str, year: int = 2020, season: str | None = None) -> Anime:
    return Anime(id=aid, title_ja=f"Anime {aid}", year=year, season=season)


def _make_credit(pid: str, aid: str, role: Role = Role.KEY_ANIMATOR) -> Credit:
    return Credit(person_id=pid, anime_id=aid, role=role, source="test")


@pytest.fixture
def simple_credits_and_maps():
    """Director D1 on anime A1, A2. Animator P1 on A1, P2 on A1+A2."""
    anime_map = {
        "A1": _make_anime("A1", 2020),
        "A2": _make_anime("A2", 2022),
    }
    credits = [
        _make_credit("D1", "A1", Role.DIRECTOR),
        _make_credit("D1", "A2", Role.DIRECTOR),
        _make_credit("P1", "A1", Role.KEY_ANIMATOR),
        _make_credit("P2", "A1", Role.KEY_ANIMATOR),
        _make_credit("P2", "A2", Role.KEY_ANIMATOR),
    ]
    director_birank = {"D1": 2.0}
    return credits, anime_map, director_birank


# ---------------------------------------------------------------------------
# Patronage Premium
# ---------------------------------------------------------------------------

class TestPatronagePremium:
    def test_basic_computation(self, simple_credits_and_maps):
        credits, anime_map, director_birank = simple_credits_and_maps
        result = compute_patronage_premium(credits, anime_map, director_birank)

        # P1 worked with D1 on 1 anime → 2.0 * log(1+1)
        assert result["P1"] == pytest.approx(2.0 * math.log1p(1))
        # P2 worked with D1 on 2 anime → 2.0 * log(1+2)
        assert result["P2"] == pytest.approx(2.0 * math.log1p(2))

    def test_senior_directors_excluded(self, simple_credits_and_maps):
        """DIRECTOR and EPISODE_DIRECTOR receive patronage=0 (D17 — circularity)."""
        credits, anime_map, director_birank = simple_credits_and_maps
        result = compute_patronage_premium(credits, anime_map, director_birank)
        assert "D1" not in result

    def test_animation_director_receives_patronage(self):
        """D12 fix: animation directors receive patronage from senior directors."""
        anime_map = {"A1": _make_anime("A1")}
        credits = [
            _make_credit("D1", "A1", Role.DIRECTOR),
            _make_credit("AD1", "A1", Role.ANIMATION_DIRECTOR),
            _make_credit("KA1", "A1", Role.KEY_ANIMATOR),
        ]
        birank = {"D1": 2.0, "AD1": 1.0}
        result = compute_patronage_premium(credits, anime_map, birank)
        # AD1 receives from D1 (senior director): 2.0 * log(1+1)
        assert "AD1" in result
        assert result["AD1"] == pytest.approx(2.0 * math.log1p(1))
        # D1 (senior director) never receives
        assert "D1" not in result
        # KA1 receives from both D1 and AD1: 2.0*log(2) + 1.0*log(2)
        assert "KA1" in result
        assert result["KA1"] == pytest.approx(3.0 * math.log1p(1))

    def test_animation_director_no_self_patronage(self):
        """Animation director cannot give patronage to themselves."""
        anime_map = {"A1": _make_anime("A1")}
        credits = [
            _make_credit("AD1", "A1", Role.ANIMATION_DIRECTOR),
        ]
        birank = {"AD1": 1.5}
        result = compute_patronage_premium(credits, anime_map, birank)
        # AD1 has no senior director on this anime → patronage = 0
        assert "AD1" not in result

    def test_unknown_director_birank_defaults_zero(self):
        anime_map = {"A1": _make_anime("A1")}
        credits = [
            _make_credit("D1", "A1", Role.DIRECTOR),
            _make_credit("P1", "A1", Role.KEY_ANIMATOR),
        ]
        # D1 not in birank → PR_d = 0 → patronage = 0
        result = compute_patronage_premium(credits, anime_map, {})
        assert result.get("P1", 0.0) == 0.0

    def test_multiple_directors(self):
        """Person collaborates with multiple directors."""
        anime_map = {
            "A1": _make_anime("A1"),
            "A2": _make_anime("A2"),
        }
        credits = [
            _make_credit("D1", "A1", Role.DIRECTOR),
            _make_credit("D2", "A2", Role.DIRECTOR),
            _make_credit("P1", "A1", Role.KEY_ANIMATOR),
            _make_credit("P1", "A2", Role.KEY_ANIMATOR),
        ]
        birank = {"D1": 1.0, "D2": 3.0}
        result = compute_patronage_premium(credits, anime_map, birank)
        expected = 1.0 * math.log1p(1) + 3.0 * math.log1p(1)
        assert result["P1"] == pytest.approx(expected)

    def test_empty_credits(self):
        result = compute_patronage_premium([], {}, {})
        assert result == {}


# ---------------------------------------------------------------------------
# Dormancy Penalty
# ---------------------------------------------------------------------------

class TestDormancyPenalty:
    def test_active_person_no_penalty(self):
        """Person active in current year → dormancy = 1.0."""
        anime_map = {"A1": _make_anime("A1", 2024)}
        credits = [_make_credit("P1", "A1")]
        result = compute_dormancy_penalty(credits, anime_map, current_year=2024)
        assert result["P1"] == pytest.approx(1.0)

    def test_within_grace_period(self):
        """Gap <= grace_period → no decay.

        Quarter-level: current_ref=2024.75(Q4), anime=2023+Q2=2023.25.
        gap=1.5 < grace=2.0 → no decay.
        """
        anime_map = {"A1": _make_anime("A1", 2023)}
        credits = [_make_credit("P1", "A1")]
        result = compute_dormancy_penalty(
            credits, anime_map, current_year=2024, grace_period=2.0
        )
        assert result["P1"] == pytest.approx(1.0)

    def test_exponential_decay_after_grace(self):
        """Quarter-level gap calculation.

        current_ref=2024.75(Q4), anime=2019+Q2=2019.25.
        gap=5.5, effective=5.5-2.0=3.5, D=exp(-0.5*3.5).
        """
        anime_map = {"A1": _make_anime("A1", 2019)}
        credits = [_make_credit("P1", "A1")]
        result = compute_dormancy_penalty(
            credits, anime_map, current_year=2024, decay_rate=0.5, grace_period=2.0
        )
        # gap = 2024.75 - 2019.25 = 5.5, effective = 3.5
        expected = math.exp(-0.5 * 3.5)
        assert result["P1"] == pytest.approx(expected)

    def test_large_gap_approaches_zero(self):
        """Very long inactivity → dormancy near 0."""
        anime_map = {"A1": _make_anime("A1", 2000)}
        credits = [_make_credit("P1", "A1")]
        result = compute_dormancy_penalty(
            credits, anime_map, current_year=2024, decay_rate=0.5, grace_period=2.0
        )
        # gap=24, effective=22, exp(-11) ≈ 1.7e-5
        assert result["P1"] < 0.001

    def test_uses_latest_year(self):
        """Multiple credits → uses max year."""
        anime_map = {
            "A1": _make_anime("A1", 2015),
            "A2": _make_anime("A2", 2023),
        }
        credits = [
            _make_credit("P1", "A1"),
            _make_credit("P1", "A2"),
        ]
        result = compute_dormancy_penalty(
            credits, anime_map, current_year=2024, grace_period=2.0
        )
        # Should use 2023 as last year → gap=1 < grace=2 → 1.0
        assert result["P1"] == pytest.approx(1.0)

    def test_custom_decay_rate(self):
        anime_map = {"A1": _make_anime("A1", 2020)}
        credits = [_make_credit("P1", "A1")]
        result = compute_dormancy_penalty(
            credits, anime_map, current_year=2024, decay_rate=1.0, grace_period=0.0
        )
        # gap = 2024.75 - 2020.25 = 4.5, grace=0, effective=4.5, D=exp(-4.5)
        assert result["P1"] == pytest.approx(math.exp(-4.5))

    def test_empty_credits(self):
        result = compute_dormancy_penalty([], {}, current_year=2024)
        assert result == {}

    def test_anime_without_year_skipped(self):
        anime_map = {"A1": Anime(id="A1", title_ja="Test")}  # year=None
        credits = [_make_credit("P1", "A1")]
        result = compute_dormancy_penalty(credits, anime_map, current_year=2024)
        assert "P1" not in result


# ---------------------------------------------------------------------------
# Combined
# ---------------------------------------------------------------------------

class TestPatronageAndDormancy:
    def test_combined_result(self, simple_credits_and_maps):
        credits, anime_map, director_birank = simple_credits_and_maps
        result = compute_patronage_and_dormancy(
            credits, anime_map, director_birank, current_year=2024
        )
        assert "P1" in result.patronage_premium
        assert "P2" in result.patronage_premium
        assert "P1" in result.dormancy_penalty
        assert "D1" in result.dormancy_penalty  # dormancy applies to everyone
        assert isinstance(result.patronage_details, dict)


# ---------------------------------------------------------------------------
# Career-Aware Dormancy
# ---------------------------------------------------------------------------

class TestCareerAwareDormancy:
    def test_veteran_protected(self):
        """High career capital → dormancy floored at 0.5."""
        raw_dormancy = {"P1": 0.1}
        iv_historical = {"P1": 10.0}
        career_data = {"P1": {"active_years": 30, "highest_stage": 6}}
        result = compute_career_aware_dormancy(
            raw_dormancy, iv_historical, career_data,
            career_capital_threshold=0.7, dormancy_floor=0.5,
        )
        assert result["P1"] == 0.5  # max(0.1, 0.5)

    def test_junior_not_protected(self):
        """Low career capital → raw dormancy preserved."""
        raw_dormancy = {"P1": 0.1}
        iv_historical = {"P1": 1.0}
        career_data = {"P1": {"active_years": 2, "highest_stage": 1}}
        result = compute_career_aware_dormancy(
            raw_dormancy, iv_historical, career_data,
            career_capital_threshold=0.7, dormancy_floor=0.5,
        )
        assert result["P1"] == 0.1  # not protected

    def test_missing_career_data(self):
        """Person not in career_data → raw dormancy preserved."""
        raw_dormancy = {"P1": 0.3}
        iv_historical = {"P1": 10.0}
        career_data = {}
        result = compute_career_aware_dormancy(
            raw_dormancy, iv_historical, career_data,
        )
        assert result["P1"] == 0.3

    def test_empty_inputs_returns_raw(self):
        raw_dormancy = {"P1": 0.5}
        result = compute_career_aware_dormancy(raw_dormancy, {}, {})
        assert result == raw_dormancy

    def test_active_veteran_unaffected(self):
        """Veteran with dormancy > floor → unchanged."""
        raw_dormancy = {"P1": 0.9}
        iv_historical = {"P1": 10.0}
        career_data = {"P1": {"active_years": 25, "highest_stage": 5}}
        result = compute_career_aware_dormancy(
            raw_dormancy, iv_historical, career_data,
            career_capital_threshold=0.7, dormancy_floor=0.5,
        )
        assert result["P1"] == 0.9  # max(0.9, 0.5) = 0.9
