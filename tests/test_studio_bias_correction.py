"""studio bias_correction.py coverage tests."""
from src.analysis.studio.bias_correction import (
    DebiasedScore,
    StudioBiasMetrics,
    StudioDisparityResult,
    compute_studio_bias_metrics,
    compute_studio_disparity,
    compute_studio_prestige,
    debias_birank_scores,
    extract_all_studios,
    extract_studio_from_anime,
    find_overvalued_by_studio,
    find_undervalued_by_studio,
)

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

class TestExtractStudio:
    def test_studios_list_preferred(self):
        a = _anime("a1", studios=["Bones", "MAPPA"])
        assert extract_studio_from_anime(a) == "Bones"

    def test_fallback_to_studio_field(self):
        a = _anime("a1", studio="Ghibli")
        assert extract_studio_from_anime(a) == "Ghibli"

    def test_no_studio_returns_unknown(self):
        a = _anime("a1")
        assert extract_studio_from_anime(a) == "unknown"

    def test_extract_all_studios(self):
        a = _anime("a1", studios=["Bones", "MAPPA"])
        assert extract_all_studios(a) == ["Bones", "MAPPA"]

    def test_extract_all_studios_fallback(self):
        a = _anime("a1", studio="Ghibli")
        assert extract_all_studios(a) == ["Ghibli"]

    def test_extract_all_studios_empty(self):
        a = _anime("a1")
        assert extract_all_studios(a) == []


class TestComputeStudioBiasMetrics:
    @pytest.fixture()
    def bias_data(self):
        anime_map = {
            "a1": _anime("a1", studios=["StudioA"]),
            "a2": _anime("a2", studios=["StudioA"]),
            "a3": _anime("a3", studios=["StudioB"]),
            "a4": _anime("a4", studios=["StudioC"]),
        }
        credits = [
            _credit("p1", "a1"),
            _credit("p1", "a2"),
            _credit("p1", "a3"),
            _credit("p1", "a4"),
            _credit("p2", "a1"),
            _credit("p2", "a2"),
        ]
        return credits, anime_map

    def test_returns_all_persons(self, bias_data):
        credits, anime_map = bias_data
        result = compute_studio_bias_metrics(credits, anime_map)
        assert "p1" in result
        assert "p2" in result

    def test_primary_studio(self, bias_data):
        credits, anime_map = bias_data
        result = compute_studio_bias_metrics(credits, anime_map)
        assert result["p1"].primary_studio == "StudioA"
        assert result["p2"].primary_studio == "StudioA"

    def test_diverse_person_has_higher_diversity(self, bias_data):
        credits, anime_map = bias_data
        result = compute_studio_bias_metrics(credits, anime_map)
        # p1 worked at 3 studios, p2 at 1
        assert result["p1"].studio_diversity > result["p2"].studio_diversity

    def test_concentrated_person_has_high_concentration(self, bias_data):
        credits, anime_map = bias_data
        result = compute_studio_bias_metrics(credits, anime_map)
        # p2 only at StudioA => concentration = 1.0
        assert result["p2"].studio_concentration == 1.0

    def test_cross_studio_works_count(self, bias_data):
        credits, anime_map = bias_data
        result = compute_studio_bias_metrics(credits, anime_map)
        assert result["p1"].cross_studio_works == 2  # 3 studios - 1
        assert result["p2"].cross_studio_works == 0

    def test_empty_credits(self):
        """Empty credits returns empty dict."""
        result = compute_studio_bias_metrics([], {})
        assert result == {}


class TestComputeStudioPrestige:
    def test_basic_prestige(self):
        anime_map = {
            "a1": _anime("a1", studios=["StudioA"]),
            "a2": _anime("a2", studios=["StudioB"]),
        }
        credits = [
            _credit("p1", "a1"),
            _credit("p2", "a2"),
        ]
        scores = {
            "p1": {"birank": 0.9},
            "p2": {"birank": 0.3},
        }
        result = compute_studio_prestige(credits, anime_map, scores)
        assert result["StudioA"] > result["StudioB"]

    def test_unknown_person_ignored(self):
        anime_map = {"a1": _anime("a1", studios=["StudioA"])}
        credits = [_credit("p_unknown", "a1")]
        result = compute_studio_prestige(credits, anime_map, {})
        assert result.get("StudioA") is None or result["StudioA"] == 0


class TestDebiasBirankScores:
    def test_no_bias_data_preserves_original(self):
        ps = {"p1": {"birank": 0.7}}
        result = debias_birank_scores(ps, {}, {})
        assert result["p1"].debiased_birank == 0.7

    def test_high_concentration_reduces_score(self):
        ps = {"p1": {"birank": 0.8}}
        bias = {
            "p1": StudioBiasMetrics(
                person_id="p1",
                primary_studio="BigStudio",
                studio_concentration=1.0,
                studio_diversity=0.0,
            ),
        }
        prestige = {"BigStudio": 0.9}
        result = debias_birank_scores(ps, bias, prestige, debias_strength=0.5)
        assert result["p1"].debiased_birank < 0.8

    def test_diverse_person_gets_bonus(self):
        ps = {"p1": {"birank": 0.5}}
        bias = {
            "p1": StudioBiasMetrics(
                person_id="p1",
                primary_studio="SmallStudio",
                studio_concentration=0.3,
                studio_diversity=0.9,
                cross_studio_works=4,
            ),
        }
        prestige = {"SmallStudio": 0.2}
        result = debias_birank_scores(ps, bias, prestige, debias_strength=0.3)
        # Diversity bonus + cross-studio bonus should increase score
        assert result["p1"].debiased_birank > 0.5

    def test_debiased_score_fields(self):
        ps = {"p1": {"birank": 0.6}}
        bias = {
            "p1": StudioBiasMetrics(
                person_id="p1",
                primary_studio="Studio",
                studio_concentration=0.5,
                studio_diversity=0.5,
                cross_studio_works=2,
            ),
        }
        prestige = {"Studio": 0.5}
        result = debias_birank_scores(ps, bias, prestige)
        d = result["p1"]
        assert isinstance(d, DebiasedScore)
        assert d.person_id == "p1"
        assert d.original_birank == 0.6
        assert d.studio_bias >= 0
        assert d.diversity_factor >= 1.0


class TestStudioDisparityResult:
    def test_dataclass_fields(self):
        r = StudioDisparityResult(
            studio="MAPPA",
            person_count=10,
            mean_iv_score=0.6,
        )
        assert r.studio == "MAPPA"
        assert r.person_count == 10


class TestComputeStudioDisparity:
    def test_basic_disparity(self):
        anime_map = {f"a{i}": _anime(f"a{i}", studios=["StudioA"]) for i in range(6)}
        credits = [_credit(f"p{i}", f"a{i}") for i in range(6)]
        person_scores = {
            f"p{i}": {
                "birank": 0.5,
                "patronage": 0.4,
                "person_fe": 0.6,
                "iv_score": 0.5,
            }
            for i in range(6)
        }
        result = compute_studio_disparity(
            credits, anime_map, person_scores, min_persons=5
        )
        assert "StudioA" in result
        assert result["StudioA"].person_count == 6

    def test_too_few_persons_filtered(self):
        anime_map = {"a1": _anime("a1", studios=["TinyStudio"])}
        credits = [_credit("p1", "a1")]
        person_scores = {"p1": {"iv_score": 0.5}}
        result = compute_studio_disparity(
            credits, anime_map, person_scores, min_persons=5
        )
        assert "TinyStudio" not in result


class TestFindUnderOvervalued:
    def test_find_undervalued(self):
        debiased = {
            "p1": DebiasedScore(
                person_id="p1", original_birank=0.3, debiased_birank=0.7
            ),
            "p2": DebiasedScore(
                person_id="p2", original_birank=0.6, debiased_birank=0.5
            ),
        }
        result = find_undervalued_by_studio(debiased)
        assert result[0][0] == "p1"  # biggest improvement

    def test_find_overvalued(self):
        debiased = {
            "p1": DebiasedScore(
                person_id="p1", original_birank=0.8, debiased_birank=0.3
            ),
            "p2": DebiasedScore(
                person_id="p2", original_birank=0.3, debiased_birank=0.5
            ),
        }
        result = find_overvalued_by_studio(debiased)
        assert result[0][0] == "p1"  # biggest decline


# ============================================================
# 4. growth_acceleration.py
# ============================================================


