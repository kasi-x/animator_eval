"""growth_acceleration.py coverage tests."""
from src.analysis.growth_acceleration import (
    AccelerationMetrics,
    compute_adjusted_person_fe_with_growth,
    compute_growth_metrics,
    find_early_potential,
    find_fast_risers,
)

import networkx as nx
import pytest

from src.models import BronzeAnime as Anime, Credit, Role


def _anime(
    aid: str,
    *,
    year: int = 2020,
    score: float | None = 75.0,
    studio: str | None = None,
    studios: list[str] | None = None,
    tags: list[dict] | None = None,
    genres: list[str] | None = None,
) -> Anime:
    resolved_studios = studios or ([studio] if studio else [])
    return Anime(
        id=aid,
        title_ja=f"Anime_{aid}",
        title_en=f"Anime_{aid}",
        year=year,
        score=score,
        studios=resolved_studios,
        tags=tags or [],
        genres=genres or [],
    )


def _credit(pid: str, aid: str, role: Role = Role.KEY_ANIMATOR) -> Credit:
    return Credit(person_id=pid, anime_id=aid, role=role, source="test")

class TestAccelerationMetrics:
    def test_defaults(self):
        m = AccelerationMetrics(person_id="p1")
        assert m.career_years == 0
        assert m.trend == "stable"
        assert m.momentum_score == 0.0


class TestComputeGrowthMetrics:
    @pytest.fixture()
    def growth_data(self):
        anime_map = {
            "a1": _anime("a1", year=2015),
            "a2": _anime("a2", year=2016),
            "a3": _anime("a3", year=2020),
            "a4": _anime("a4", year=2021),
            "a5": _anime("a5", year=2022),
            "a6": _anime("a6", year=2023),
            "a7": _anime("a7", year=2024),
            "a8": _anime("a8", year=2024),
        }
        credits = [
            # p1: rising - few early, many recent
            _credit("p1", "a1"),
            _credit("p1", "a5"),
            _credit("p1", "a6"),
            _credit("p1", "a7"),
            _credit("p1", "a8"),
            # p2: early career only
            _credit("p2", "a5"),
            _credit("p2", "a6"),
            # p3: declining - many early, few recent
            _credit("p3", "a1"),
            _credit("p3", "a2"),
            _credit("p3", "a3"),
            _credit("p3", "a4"),
            _credit("p3", "a5"),
            _credit("p3", "a6"),
            _credit("p3", "a7"),
        ]
        return credits, anime_map

    def test_returns_dict(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map)
        assert isinstance(result, dict)
        assert "p1" in result

    def test_career_years(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map)
        # p1: 2015-2024 = 10 years
        assert result["p1"].career_years == 10

    def test_total_credits(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map)
        assert result["p1"].total_credits == 5

    def test_early_trend(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map)
        # p2: career_years = 2 <= 3 => "early"
        assert result["p2"].trend == "early"

    def test_peak_year(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map)
        # p1 has 2 credits in 2024, 1 in each other year
        assert result["p1"].peak_year == 2024

    def test_empty_credits(self):
        result = compute_growth_metrics([], {})
        assert result == {}

    def test_anime_without_year_skipped(self):
        anime_map = {"a1": _anime("a1", year=None)}
        credits = [_credit("p1", "a1")]
        result = compute_growth_metrics(credits, anime_map)
        assert "p1" not in result

    def test_early_career_bonus(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map, current_year=2024)
        # p2 debuts in 2022, so years_since_debut=2 => bonus=(5-2)*0.1=0.3
        assert result["p2"].early_career_bonus == 0.3

    def test_no_early_bonus_for_veteran(self, growth_data):
        credits, anime_map = growth_data
        result = compute_growth_metrics(credits, anime_map, current_year=2026)
        # p1 debuts in 2015, years_since_debut=11 > 5 => no bonus
        assert result["p1"].early_career_bonus == 0


class TestFindFastRisers:
    def test_finds_risers(self):
        metrics = {
            "p1": AccelerationMetrics(
                person_id="p1", growth_velocity=3.0, growth_acceleration=1.0
            ),
            "p2": AccelerationMetrics(
                person_id="p2", growth_velocity=0.5, growth_acceleration=0.0
            ),
        }
        result = find_fast_risers(metrics, min_velocity=2.0)
        assert len(result) == 1
        assert result[0][0] == "p1"

    def test_empty_input(self):
        assert find_fast_risers({}) == []


class TestFindEarlyPotential:
    def test_finds_early_talent(self):
        metrics = {
            "p1": AccelerationMetrics(
                person_id="p1", career_years=3, momentum_score=2.0
            ),
            "p2": AccelerationMetrics(
                person_id="p2", career_years=10, momentum_score=3.0
            ),
        }
        result = find_early_potential(metrics, max_career_years=5, min_momentum=1.0)
        assert len(result) == 1
        assert result[0][0] == "p1"


class TestComputeAdjustedSkillWithGrowth:
    def test_basic_adjustment(self):
        person_scores = {
            "p1": {"person_fe": 0.5},
            "p2": {"person_fe": 0.6},
        }
        growth_metrics = {
            "p1": AccelerationMetrics(
                person_id="p1",
                growth_velocity=2.0,
                growth_acceleration=1.0,
                early_career_bonus=0.2,
            ),
        }
        result = compute_adjusted_person_fe_with_growth(
            person_scores, growth_metrics, growth_weight=0.3
        )
        assert "p1" in result
        assert result["p1"] > 0.5  # Should be higher due to growth
        assert result["p2"] == 0.6  # No growth data => original skill

    def test_no_growth_metrics(self):
        person_scores = {"p1": {"person_fe": 0.7}}
        result = compute_adjusted_person_fe_with_growth(person_scores, {})
        assert result["p1"] == 0.7

    def test_negative_growth_reduces_skill(self):
        person_scores = {"p1": {"person_fe": 0.6}}
        growth_metrics = {
            "p1": AccelerationMetrics(
                person_id="p1",
                growth_velocity=-3.0,
                growth_acceleration=-1.0,
                early_career_bonus=0.0,
            ),
        }
        result = compute_adjusted_person_fe_with_growth(
            person_scores, growth_metrics, growth_weight=0.3
        )
        assert result["p1"] < 0.6


# ============================================================
# 5. anime_value.py
# ============================================================


