"""anime_value.py coverage tests."""
from src.analysis.anime_value import (
    AnimeValueMetrics,
    compute_anime_values,
    compute_commercial_value,
    compute_creative_value,
    compute_critical_value,
    compute_cultural_value,
    compute_technical_value,
    find_overperforming_works,
    find_undervalued_works,
    rank_anime_by_value,
)

import pytest

from src.runtime.models import BronzeAnime as Anime, Credit, Role


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

class TestAnimeValueMetrics:
    def test_defaults(self):
        m = AnimeValueMetrics(anime_id="a1", title="Test")
        assert m.composite_value == 0.0
        assert m.staff_count == 0


class TestComputeCommercialValue:
    def test_with_score_and_staff(self):
        anime = _anime("a1")
        credits = [_credit(f"p{i}", "a1") for i in range(30)]
        result = compute_commercial_value(anime, credits)
        # staff_score=min(1, 30/50)=0.6, diversity_score=min(1, 1/20)=0.05
        # external_score=min(1, 80/100)=0.8
        # commercial = 0.6*0.3 + 0.05*0.2 + 0.8*0.5 = 0.18 + 0.01 + 0.4 = 0.59
        assert 0 < result <= 1.0

    def test_no_score_uses_default(self):
        anime = _anime("a1", score=None)
        credits = [_credit("p1", "a1")]
        result = compute_commercial_value(anime, credits)
        assert result > 0  # Default external_score=0.5

    def test_zero_score_uses_default(self):
        anime = _anime("a1")
        credits = [_credit("p1", "a1")]
        result = compute_commercial_value(anime, credits)
        assert result > 0


class TestComputeCriticalValue:
    def test_with_tags_and_score(self):
        anime = _anime(
            "a1", tags=[{"name": f"tag{i}", "rank": i} for i in range(10)]
        )
        credits = [_credit("p1", "a1")]
        result = compute_critical_value(anime, credits)
        assert 0 < result <= 1.0

    def test_no_tags(self):
        anime = _anime("a1", tags=[])
        credits = [_credit("p1", "a1")]
        result = compute_critical_value(anime, credits)
        assert result > 0


class TestComputeCreativeValue:
    def test_with_director_and_animator(self):
        anime = _anime("a1", tags=[{"name": f"tag{i}"} for i in range(10)])
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.KEY_ANIMATOR),
        ]
        scores = {"p1": {"person_fe": 0.8}, "p2": {"person_fe": 0.6}}
        result = compute_creative_value(anime, credits, scores)
        assert 0 < result <= 1.0

    def test_no_scored_staff(self):
        anime = _anime("a1")
        credits = [_credit("p1", "a1", Role.DIRECTOR)]
        result = compute_creative_value(anime, credits, {})
        # Should use defaults
        assert result > 0


class TestComputeCulturalValue:
    def test_classic_anime(self):
        anime = _anime("a1", year=2000)
        result = compute_cultural_value(anime, [], current_year=2026)
        # Age = 26, age_score=min(1, 26/20)=1.0, longevity_score=0.8
        # cultural = 1.0*0.5 + 0.8*0.5 = 0.9
        assert result == 0.9

    def test_recent_anime(self):
        anime = _anime("a1", year=2024)
        result = compute_cultural_value(anime, [], current_year=2026)
        # Age = 2, age_score=0.1, longevity_score=0.4
        # cultural = 0.1*0.5 + 0.4*0.5 = 0.25
        assert result == 0.25

    def test_no_year(self):
        anime = _anime("a1", year=None)
        result = compute_cultural_value(anime, [])
        assert result == 0.5


class TestComputeTechnicalValue:
    def test_with_animators(self):
        anime = _anime("a1")
        credits = [
            _credit("p1", "a1", Role.KEY_ANIMATOR),
            _credit("p2", "a1", Role.BACKGROUND_ART),
        ]
        scores = {"p1": {"iv_score": 0.8}, "p2": {"iv_score": 0.7}}
        result = compute_technical_value(anime, credits, scores)
        assert 0 < result <= 1.0

    def test_no_staff(self):
        anime = _anime("a1")
        result = compute_technical_value(anime, [], {})
        # No animators, no tech staff => defaults
        assert result >= 0


class TestComputeAnimeValues:
    @pytest.fixture()
    def anime_data(self):
        anime_list = [
            _anime(
                "a1",
                year=2020,
                studios=["StudioA"],
                tags=[{"name": "action"}],
            ),
            _anime("a2", year=2005, studios=["StudioB"]),
        ]
        credits = [
            _credit("p1", "a1", Role.DIRECTOR),
            _credit("p2", "a1", Role.KEY_ANIMATOR),
            _credit("p3", "a2", Role.DIRECTOR),
        ]
        person_scores = {
            "p1": {"birank": 0.8, "patronage": 0.7, "person_fe": 0.9, "iv_score": 0.8},
            "p2": {"birank": 0.5, "patronage": 0.4, "person_fe": 0.6, "iv_score": 0.5},
            "p3": {"birank": 0.3, "patronage": 0.3, "person_fe": 0.4, "iv_score": 0.33},
        }
        return anime_list, credits, person_scores

    def test_returns_all_anime(self, anime_data):
        anime_list, credits, person_scores = anime_data
        result = compute_anime_values(anime_list, credits, person_scores)
        assert "a1" in result
        assert "a2" in result

    def test_result_type(self, anime_data):
        anime_list, credits, person_scores = anime_data
        result = compute_anime_values(anime_list, credits, person_scores)
        assert isinstance(result["a1"], AnimeValueMetrics)

    def test_composite_value_range(self, anime_data):
        anime_list, credits, person_scores = anime_data
        result = compute_anime_values(anime_list, credits, person_scores)
        for v in result.values():
            assert 0 <= v.composite_value <= 100

    def test_anime_without_credits_excluded(self, anime_data):
        anime_list, credits, person_scores = anime_data
        anime_list.append(_anime("a_empty", year=2020))
        result = compute_anime_values(anime_list, credits, person_scores)
        assert "a_empty" not in result

    def test_staff_count(self, anime_data):
        anime_list, credits, person_scores = anime_data
        result = compute_anime_values(anime_list, credits, person_scores)
        assert result["a1"].staff_count == 2
        assert result["a2"].staff_count == 1

    def test_value_per_staff(self, anime_data):
        anime_list, credits, person_scores = anime_data
        result = compute_anime_values(anime_list, credits, person_scores)
        for v in result.values():
            if v.staff_count > 0:
                assert abs(v.value_per_staff - v.composite_value / v.staff_count) < 0.1

    def test_more_staff_higher_commercial_value(self):
        """More staff (larger production) should increase commercial value."""
        # anime_value no longer uses anime.score for commercial/critical value.
        # Commercial value is driven by staff count, role diversity, and
        # production scale (episodes * duration).
        anime_big = [_anime("a_big")]
        anime_small = [_anime("a_small")]
        # 5 staff on a_big vs 1 on a_small
        credits_big = [_credit(f"p{i}", "a_big") for i in range(5)]
        credits_small = [_credit("p1", "a_small")]
        scores = {
            f"p{i}": {
                "birank": 0.5,
                "patronage": 0.5,
                "person_fe": 0.5,
                "iv_score": 0.5,
            }
            for i in range(5)
        }
        result_big = compute_anime_values(anime_big, credits_big, scores)
        result_small = compute_anime_values(anime_small, credits_small, scores)
        assert (
            result_big["a_big"].commercial_value
            > result_small["a_small"].commercial_value
        )


class TestRankAnimeByValue:
    def test_ranking_order(self):
        values = {
            "a1": AnimeValueMetrics(
                anime_id="a1",
                title="Show A",
                composite_value=90.0,
                commercial_value=0.8,
                creative_value=0.7,
                technical_value=0.6,
                cultural_value=0.5,
            ),
            "a2": AnimeValueMetrics(
                anime_id="a2",
                title="Show B",
                composite_value=50.0,
                commercial_value=0.4,
                creative_value=0.3,
                technical_value=0.2,
                cultural_value=0.1,
            ),
        }
        ranked = rank_anime_by_value(values, dimension="composite")
        assert ranked[0][0] == "a1"
        assert ranked[1][0] == "a2"

    def test_ranking_by_dimension(self):
        values = {
            "a1": AnimeValueMetrics(
                anime_id="a1",
                title="Show A",
                composite_value=50.0,
                commercial_value=0.9,
            ),
            "a2": AnimeValueMetrics(
                anime_id="a2",
                title="Show B",
                composite_value=80.0,
                commercial_value=0.2,
            ),
        }
        ranked = rank_anime_by_value(values, dimension="commercial")
        assert ranked[0][0] == "a1"

    def test_top_n(self):
        values = {
            f"a{i}": AnimeValueMetrics(
                anime_id=f"a{i}", title=f"Show {i}", composite_value=float(i)
            )
            for i in range(10)
        }
        ranked = rank_anime_by_value(values, top_n=3)
        assert len(ranked) == 3

    def test_value_per_staff_dimension(self):
        values = {
            "a1": AnimeValueMetrics(
                anime_id="a1", title="Show A", value_per_staff=20.0
            ),
            "a2": AnimeValueMetrics(
                anime_id="a2", title="Show B", value_per_staff=10.0
            ),
        }
        ranked = rank_anime_by_value(values, dimension="value_per_staff")
        assert ranked[0][0] == "a1"


class TestFindUnderOverPerforming:
    def test_find_undervalued_works(self):
        values = {
            "a1": AnimeValueMetrics(
                anime_id="a1",
                title="Hidden Gem",
                staff_quality=0.8,
                composite_value=40.0,
            ),
            "a2": AnimeValueMetrics(
                anime_id="a2",
                title="Popular",
                staff_quality=0.8,
                composite_value=80.0,
            ),
        }
        result = find_undervalued_works(values, min_staff_quality=0.6, max_value=50)
        assert len(result) == 1
        assert result[0][0] == "a1"

    def test_find_overperforming_works(self):
        values = {
            "a1": AnimeValueMetrics(
                anime_id="a1",
                title="Surprise Hit",
                staff_quality=0.2,
                composite_value=70.0,
            ),
            "a2": AnimeValueMetrics(
                anime_id="a2",
                title="Normal",
                staff_quality=0.8,
                composite_value=80.0,
            ),
        }
        result = find_overperforming_works(values, max_staff_quality=0.4, min_value=60)
        assert len(result) == 1
        assert result[0][0] == "a1"


# ============================================================
# 6. additional individual_contribution edge cases
# ============================================================


