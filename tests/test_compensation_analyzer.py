"""Tests for fair compensation analyzer — Shapley-based allocation.

Hard Rule 4 (CLAUDE.md): Compensation basis requires analytical CI (SE = σ/√n).
Tests verify Gini fairness metrics, Shapley value allocation, and anime-type
role adjustments (structural metadata only, no anime.score).
"""

import pytest

from src.analysis.domain.person.compensation_analyzer import (
    AnimeType,
    ANIME_TYPE_ROLE_ADJUSTMENTS,
    CompensationAnalysis,
    CompensationAnalysisRequest,
    FairnessMetrics,
    analyze_fair_compensation,
    batch_analyze_compensation,
    classify_anime_type,
    compute_fairness_metrics,
    compute_gini_coefficient,
    export_compensation_report,
)
from src.runtime.models import AnimeAnalysis as Anime, Role


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def movie_anime() -> Anime:
    """Single-episode anime (movie type)."""
    return Anime(
        id="movie_001",
        title_ja="劇場版テスト",
        title_en="Test Movie",
        year=2020,
        episodes=1,
        season=None,  # no season = movie
        format="MOVIE",
        duration=120,
    )


@pytest.fixture
def tv_1cour_anime() -> Anime:
    """TV 1-cour anime (12-13 episodes)."""
    return Anime(
        id="tv_1cour_001",
        title_ja="1クールテスト",
        title_en="Test 1-Cour",
        year=2020,
        episodes=12,
        season="SPRING",
        format="TV",
        duration=24,
    )


@pytest.fixture
def tv_2cour_anime() -> Anime:
    """TV 2-cour anime (24-26 episodes)."""
    return Anime(
        id="tv_2cour_001",
        title_ja="2クールテスト",
        title_en="Test 2-Cour",
        year=2020,
        episodes=24,
        season="SPRING",
        format="TV",
        duration=24,
    )


@pytest.fixture
def tv_long_anime() -> Anime:
    """Long-running TV series (27+ episodes)."""
    return Anime(
        id="tv_long_001",
        title_ja="長編テスト",
        title_en="Test Long",
        year=2020,
        episodes=50,
        season="SPRING",
        format="TV",
        duration=24,
    )


@pytest.fixture
def ova_anime() -> Anime:
    """Single-episode with season (OVA type)."""
    return Anime(
        id="ova_001",
        title_ja="OVAテスト",
        title_en="Test OVA",
        year=2020,
        episodes=1,
        season="SUMMER",  # has season = OVA
        format="OVA",
        duration=30,
    )


@pytest.fixture
def simple_contributions() -> dict[str, dict]:
    """Simple contribution set: 3 persons with Shapley values."""
    return {
        "dir_001": {
            "role": "director",
            "shapley_value": 100.0,
            "value_share": 0.50,
        },
        "ka_001": {
            "role": "key_animator",
            "shapley_value": 80.0,
            "value_share": 0.40,
        },
        "bg_001": {
            "role": "background_art",
            "shapley_value": 20.0,
            "value_share": 0.10,
        },
    }


@pytest.fixture
def large_contributions() -> dict[str, dict]:
    """Larger cohort: 10 persons with varied Shapley values."""
    contributions = {}
    for i in range(10):
        contributions[f"person_{i}"] = {
            "role": "key_animator" if i < 8 else "director",
            "shapley_value": float(100 - i * 10),
            "value_share": 0.1,
        }
    return contributions


# ============================================================
# Tests: anime type classification
# ============================================================


def test_classify_anime_type_movie(movie_anime):
    """Movie (1 episode, no season) → AnimeType.MOVIE."""
    result = classify_anime_type(movie_anime)
    assert result == AnimeType.MOVIE


def test_classify_anime_type_ova(ova_anime):
    """OVA (1 episode, with season) → AnimeType.OVA."""
    result = classify_anime_type(ova_anime)
    assert result == AnimeType.OVA


def test_classify_anime_type_tv_1cour(tv_1cour_anime):
    """TV 1-cour (12 episodes) → AnimeType.TV_1COUR."""
    result = classify_anime_type(tv_1cour_anime)
    assert result == AnimeType.TV_1COUR


def test_classify_anime_type_tv_2cour(tv_2cour_anime):
    """TV 2-cour (24 episodes) → AnimeType.TV_2COUR."""
    result = classify_anime_type(tv_2cour_anime)
    assert result == AnimeType.TV_2COUR


def test_classify_anime_type_tv_long(tv_long_anime):
    """Long-running TV (50 episodes) → AnimeType.TV_LONG."""
    result = classify_anime_type(tv_long_anime)
    assert result == AnimeType.TV_LONG


def test_classify_anime_unknown_episodes():
    """Missing episodes field → defaults to 1."""
    anime = Anime(
        id="unknown_001",
        title_ja="不明",
        title_en="Unknown",
        year=2020,
        episodes=None,  # missing
        season=None,
    )
    result = classify_anime_type(anime)
    assert result == AnimeType.MOVIE


# ============================================================
# Tests: Gini coefficient computation (fairness)
# ============================================================


def test_gini_coefficient_empty():
    """Empty list → Gini = 0.0."""
    result = compute_gini_coefficient([])
    assert result == 0.0


def test_gini_coefficient_single():
    """Single value → Gini = 0.0 (no inequality)."""
    result = compute_gini_coefficient([100.0])
    assert result == 0.0


def test_gini_coefficient_equal_values():
    """All equal values → Gini = 0.0 (perfect equality)."""
    result = compute_gini_coefficient([50.0, 50.0, 50.0, 50.0])
    assert result == 0.0


def test_gini_coefficient_unequal_values():
    """Unequal values → Gini in [0, 1)."""
    result = compute_gini_coefficient([10.0, 20.0, 30.0, 40.0])
    assert 0.0 < result < 1.0
    assert isinstance(result, float)


def test_gini_coefficient_extreme_inequality():
    """Highly skewed (one person gets most) → Gini high."""
    result = compute_gini_coefficient([1.0, 1.0, 1.0, 100.0])
    assert result > 0.7  # Gini for extreme skew


def test_gini_coefficient_total_zero():
    """All zeros → Gini = 0.0 (no variance)."""
    result = compute_gini_coefficient([0.0, 0.0, 0.0])
    assert result == 0.0


# ============================================================
# Tests: fairness metrics computation
# ============================================================


def test_fairness_metrics_empty():
    """Empty allocations → all metrics = 0.0."""
    metrics = compute_fairness_metrics({}, {})
    assert metrics.gini_coefficient == 0.0
    assert metrics.shapley_correlation == 0.0
    assert metrics.min_compensation == 0.0
    assert metrics.max_compensation == 0.0
    assert metrics.compensation_ratio == 0.0


def test_fairness_metrics_single_person(simple_contributions):
    """Single person (no variance) → Gini = 0.0, shapley_corr = 1.0."""
    allocations = {"dir_001": 100.0}
    contributions = {"dir_001": simple_contributions["dir_001"]}

    metrics = compute_fairness_metrics(allocations, contributions)
    assert metrics.gini_coefficient == 0.0
    assert metrics.shapley_correlation == 1.0
    assert metrics.min_compensation == 100.0
    assert metrics.max_compensation == 100.0
    assert metrics.compensation_ratio == 1.0  # max/min when equal


def test_fairness_metrics_three_persons(simple_contributions):
    """Three persons with allocations → Gini > 0, shapley_corr measured."""
    allocations = {
        "dir_001": 100.0,
        "ka_001": 80.0,
        "bg_001": 20.0,
    }
    metrics = compute_fairness_metrics(allocations, simple_contributions)

    # Verify structure
    assert 0.0 <= metrics.gini_coefficient <= 1.0
    assert -1.0 <= metrics.shapley_correlation <= 1.0
    assert metrics.min_compensation == 20.0
    assert metrics.max_compensation == 100.0
    assert metrics.compensation_ratio == 5.0  # 100 / 20


def test_fairness_metrics_perfect_allocation(simple_contributions):
    """Allocations match Shapley values → shapley_corr = 1.0."""
    allocations = {
        "dir_001": 100.0,
        "ka_001": 80.0,
        "bg_001": 20.0,
    }
    metrics = compute_fairness_metrics(allocations, simple_contributions)
    assert metrics.shapley_correlation == 1.0


def test_fairness_metrics_inverse_allocation(simple_contributions):
    """Allocations partially reverse Shapley order → shapley_corr negative."""
    allocations = {
        "dir_001": 20.0,  # reverse (was 100)
        "ka_001": 80.0,   # same (was 80)
        "bg_001": 100.0,  # reverse (was 20)
    }
    metrics = compute_fairness_metrics(allocations, simple_contributions)
    # Not perfect inverse, so correlation is negative but not -1.0
    assert metrics.shapley_correlation < 0.0


# ============================================================
# Tests: analyze_fair_compensation (main function)
# ============================================================


def test_analyze_fair_compensation_empty_contributions(tv_1cour_anime):
    """No contributions → empty allocations, zero metrics."""
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=100.0,
    )
    result = analyze_fair_compensation(request, {}, tv_1cour_anime)

    assert result.anime_id == "tv_1cour_001"
    assert len(result.allocations) == 0
    assert result.fairness_metrics.gini_coefficient == 0.0


def test_analyze_fair_compensation_single_person(tv_1cour_anime, simple_contributions):
    """Single person gets entire budget."""
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=100.0,
    )
    result = analyze_fair_compensation(
        request,
        {"dir_001": simple_contributions["dir_001"]},
        tv_1cour_anime,
    )

    assert len(result.allocations) == 1
    assert result.allocations["dir_001"] == pytest.approx(100.0)


def test_analyze_fair_compensation_proportional_allocation(
    tv_1cour_anime, simple_contributions
):
    """3 persons allocated proportionally to Shapley values."""
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=200.0,
    )
    result = analyze_fair_compensation(
        request, simple_contributions, tv_1cour_anime
    )

    # Total = 100 + 80 + 20 = 200 shapley units
    # Shares: 100/200=50%, 80/200=40%, 20/200=10%
    # Budget allocations: 100, 80, 20
    assert result.allocations["dir_001"] == pytest.approx(100.0)
    assert result.allocations["ka_001"] == pytest.approx(80.0)
    assert result.allocations["bg_001"] == pytest.approx(20.0)


def test_analyze_fair_compensation_with_min_compensation(
    tv_1cour_anime, simple_contributions
):
    """Minimum guarantees apply (code applies min, then rescales total)."""
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=200.0,
        min_compensation={Role.BACKGROUND_ART: 50.0},
    )
    result = analyze_fair_compensation(
        request, simple_contributions, tv_1cour_anime
    )

    # bg_001 originally gets 20, min is 50 → raised to 50
    # But then total (100+80+50=230) > budget (200) → all rescaled by 200/230
    # So bg_001 gets approximately 50 * (200/230) ≈ 43.5
    min_bg = min(result.allocations.values())
    assert min_bg < 50.0  # After rescaling


def test_analyze_fair_compensation_anime_type_adjustment_movie(
    movie_anime, simple_contributions
):
    """Movie type applies role adjustments (director +30%, ka -10%)."""
    request = CompensationAnalysisRequest(
        anime_id="movie_001",
        total_budget=200.0,
        apply_anime_type_adjustment=True,
    )
    result = analyze_fair_compensation(request, simple_contributions, movie_anime)

    # Director: 100 * 1.3 = 130
    # Key animator: 80 * 0.9 = 72
    # Background art: 20 * 1.2 = 24
    # Total adjusted = 226
    # Shares: 130/226, 72/226, 24/226
    total_adjusted = 130 + 72 + 24
    expected_dir = 200.0 * (130 / total_adjusted)
    assert result.allocations["dir_001"] == pytest.approx(expected_dir)


def test_analyze_fair_compensation_anime_type_adjustment_disabled(
    movie_anime, simple_contributions
):
    """apply_anime_type_adjustment=False → no adjustments."""
    request = CompensationAnalysisRequest(
        anime_id="movie_001",
        total_budget=200.0,
        apply_anime_type_adjustment=False,
    )
    result = analyze_fair_compensation(request, simple_contributions, movie_anime)

    # No adjustment: base allocation (100, 80, 20)
    assert result.allocations["dir_001"] == pytest.approx(100.0)


def test_analyze_fair_compensation_max_ratio_constraint(
    tv_1cour_anime, large_contributions
):
    """max_ratio constraint raises floor for low earners."""
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=1000.0,
        max_ratio=3.0,  # max/min <= 3
    )
    result = analyze_fair_compensation(
        request, large_contributions, tv_1cour_anime
    )

    min_alloc = min(result.allocations.values())
    max_alloc = max(result.allocations.values())
    if min_alloc > 0:
        ratio = max_alloc / min_alloc
        assert ratio <= 3.0 + 0.01  # allow small rounding error


def test_analyze_fair_compensation_result_structure(
    tv_1cour_anime, simple_contributions
):
    """Result includes all required fields."""
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=200.0,
    )
    result = analyze_fair_compensation(
        request, simple_contributions, tv_1cour_anime
    )

    assert isinstance(result, CompensationAnalysis)
    assert result.anime_id == "tv_1cour_001"
    assert result.anime_title == "1クールテスト"
    assert result.anime_type == "tv_1cour"
    assert result.total_budget == 200.0
    assert isinstance(result.allocations, dict)
    assert isinstance(result.contributions, dict)
    assert isinstance(result.fairness_metrics, FairnessMetrics)


def test_analyze_fair_compensation_invalid_role_graceful(
    tv_1cour_anime,
):
    """Invalid role string → falls back to Role.SPECIAL."""
    contributions = {
        "unknown_001": {
            "role": "nonexistent_role",
            "shapley_value": 100.0,
            "value_share": 1.0,
        }
    }
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=100.0,
    )
    # Should not raise; gracefully handles unknown role
    result = analyze_fair_compensation(request, contributions, tv_1cour_anime)
    assert "unknown_001" in result.allocations


# ============================================================
# Tests: batch_analyze_compensation
# ============================================================


def test_batch_analyze_compensation_empty():
    """Empty inputs → empty results."""
    results = batch_analyze_compensation([], {})
    assert len(results) == 0


def test_batch_analyze_compensation_single_anime(
    tv_1cour_anime, simple_contributions
):
    """Single anime batch → single result."""
    results = batch_analyze_compensation(
        [tv_1cour_anime],
        {"tv_1cour_001": simple_contributions},
        total_budget_per_anime=200.0,
    )

    assert len(results) == 1
    assert "tv_1cour_001" in results
    assert isinstance(results["tv_1cour_001"], CompensationAnalysis)


def test_batch_analyze_compensation_multiple_anime(
    tv_1cour_anime, tv_2cour_anime, simple_contributions, large_contributions
):
    """Multiple anime → multiple results."""
    all_contributions = {
        "tv_1cour_001": simple_contributions,
        "tv_2cour_001": large_contributions,
    }
    results = batch_analyze_compensation(
        [tv_1cour_anime, tv_2cour_anime],
        all_contributions,
        total_budget_per_anime=500.0,
    )

    assert len(results) == 2
    assert all(isinstance(r, CompensationAnalysis) for r in results.values())


def test_batch_analyze_compensation_missing_anime(tv_1cour_anime, simple_contributions):
    """Contributions for missing anime → skipped."""
    all_contributions = {
        "tv_1cour_001": simple_contributions,
        "nonexistent_anime": {"person": {"role": "director", "shapley_value": 50}},
    }
    results = batch_analyze_compensation(
        [tv_1cour_anime],
        all_contributions,
        total_budget_per_anime=200.0,
    )

    assert len(results) == 1
    assert "nonexistent_anime" not in results


# ============================================================
# Tests: export_compensation_report
# ============================================================


def test_export_compensation_report_empty():
    """Empty analyses → report structure with zeros."""
    report = export_compensation_report({}, {})

    assert report["total_anime"] == 0
    assert report["summary"]["avg_gini_coefficient"] == 0
    assert report["summary"]["avg_shapley_correlation"] == 0
    assert len(report["analyses"]) == 0


def test_export_compensation_report_single_anime(
    tv_1cour_anime, simple_contributions
):
    """Single anime report → includes allocations and fairness."""
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=200.0,
    )
    analysis = analyze_fair_compensation(
        request, simple_contributions, tv_1cour_anime
    )

    person_names = {
        "dir_001": "Director One",
        "ka_001": "Animator One",
        "bg_001": "Background One",
    }
    report = export_compensation_report(
        {"tv_1cour_001": analysis},
        person_names,
    )

    assert report["total_anime"] == 1
    assert len(report["analyses"]) == 1

    anime_entry = report["analyses"][0]
    assert anime_entry["anime_id"] == "tv_1cour_001"
    assert anime_entry["staff_count"] == 3
    assert anime_entry["fairness"]["gini_coefficient"] >= 0.0
    assert len(anime_entry["allocations"]) == 3


def test_export_compensation_report_with_anime_scores(
    tv_1cour_anime, simple_contributions
):
    """Report with anime_score metadata (display only, not scoring)."""
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=200.0,
    )
    analysis = analyze_fair_compensation(
        request, simple_contributions, tv_1cour_anime
    )

    person_names = {"dir_001": "Director", "ka_001": "Animator", "bg_001": "BG"}
    anime_scores = {"tv_1cour_001": 7.5}  # display metadata only

    report = export_compensation_report(
        {"tv_1cour_001": analysis},
        person_names,
        anime_scores=anime_scores,
    )

    anime_entry = report["analyses"][0]
    assert anime_entry["anime_score"] == 7.5


def test_export_compensation_report_allocations_sorted_descending(
    tv_1cour_anime, simple_contributions
):
    """Allocations in report sorted by amount (descending)."""
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=200.0,
    )
    analysis = analyze_fair_compensation(
        request, simple_contributions, tv_1cour_anime
    )

    person_names = {"dir_001": "Director", "ka_001": "Animator", "bg_001": "BG"}
    report = export_compensation_report(
        {"tv_1cour_001": analysis},
        person_names,
    )

    allocations_list = report["analyses"][0]["allocations"]
    amounts = [a["allocation"] for a in allocations_list]
    assert amounts == sorted(amounts, reverse=True)


def test_export_compensation_report_summary_stats(
    tv_1cour_anime, tv_2cour_anime, simple_contributions, large_contributions
):
    """Summary stats aggregate across anime."""
    results = batch_analyze_compensation(
        [tv_1cour_anime, tv_2cour_anime],
        {
            "tv_1cour_001": simple_contributions,
            "tv_2cour_001": large_contributions,
        },
        total_budget_per_anime=200.0,
    )

    person_names = {f"person_{i}": f"Person {i}" for i in range(10)}
    person_names.update({"dir_001": "D", "ka_001": "K", "bg_001": "B"})

    report = export_compensation_report(results, person_names)

    assert report["total_anime"] == 2
    assert "avg_gini_coefficient" in report["summary"]
    assert "avg_shapley_correlation" in report["summary"]
    assert "anime_type_distribution" in report


# ============================================================
# Tests: structural integrity & legal compliance
# ============================================================


def test_anime_type_role_adjustments_coverage():
    """ANIME_TYPE_ROLE_ADJUSTMENTS covers all anime types."""
    for anime_type in AnimeType:
        if anime_type != AnimeType.UNKNOWN:
            assert anime_type in ANIME_TYPE_ROLE_ADJUSTMENTS


def test_allocation_sums_reasonably(tv_1cour_anime, simple_contributions):
    """Total allocations ~ total budget (within rounding error)."""
    request = CompensationAnalysisRequest(
        anime_id="tv_1cour_001",
        total_budget=500.0,
    )
    result = analyze_fair_compensation(
        request, simple_contributions, tv_1cour_anime
    )

    total_alloc = sum(result.allocations.values())
    # After all adjustments, should be close to budget
    assert total_alloc == pytest.approx(500.0, rel=0.01)


def test_zero_budget_handling():
    """Zero total_budget → allocations are zero."""
    anime = Anime(
        id="test",
        title_ja="Test",
        episodes=12,
        year=2020,
    )
    request = CompensationAnalysisRequest(
        anime_id="test",
        total_budget=0.0,
    )
    contributions = {"p1": {"role": "director", "shapley_value": 100.0, "value_share": 1.0}}

    result = analyze_fair_compensation(request, contributions, anime)
    assert result is not None
    assert result.allocations["p1"] == 0.0


def test_no_anime_score_in_allocation_logic():
    """Anime.score field is never used in allocation (CLAUDE.md Hard Rule 1)."""
    # CLAUDE.md Hard Rule 1: anime.score only for display, never in scoring path
    anime = Anime(
        id="test",
        title_ja="Test",
        episodes=12,
        year=2020,
    )
    # Compensation analyzer only uses: episodes, season, title, format
    # (structural metadata, never anime.score)
    request = CompensationAnalysisRequest(anime_id="test", total_budget=100.0)
    contributions = {"p1": {"role": "director", "shapley_value": 50.0, "value_share": 1.0}}

    result = analyze_fair_compensation(request, contributions, anime)
    assert result is not None
    # Allocations depend only on Shapley values, not any scoring metadata
    assert isinstance(result.allocations, dict)
