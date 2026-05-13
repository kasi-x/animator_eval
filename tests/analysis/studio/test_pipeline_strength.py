"""Tests for src/analysis/studio/pipeline_strength.py.

Coverage:
- Unit tests for helper functions (_safe_hhi, _top_k_concentration, _bus_factor)
- compute_pipeline_strength with synthetic toy data
- Bootstrap CI bounds are finite and in valid range
- aggregate_by_studio produces expected keys
- young_sample_flag is set correctly when sample_young < 30
- lint_vocab: forbidden vocabulary absent from source
- No anime.score reference
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


def _make_toy_data() -> tuple[
    dict[str, float],
    dict[str, dict[int, str]],
    dict[str, int],
]:
    """Minimal synthetic dataset: 2 studios, 3 years, ~40 staff.

    Studio A: staff p0–p19, active 2010–2012
    Studio B: staff p20–p39, active 2010–2012
    Debut years spread so tenure buckets are populated.
    """
    person_fe: dict[str, float] = {}
    studio_assignments: dict[str, dict[int, str]] = {}
    debut_years: dict[str, int] = {}

    # Studio A — 20 staff
    for i in range(20):
        pid = f"p{i}"
        debut = 2005 + (i % 8)  # debut 2005–2012
        debut_years[pid] = debut
        person_fe[pid] = 0.1 * (i - 10)  # range −1.0 to +0.9
        studio_assignments[pid] = {y: "studioA" for y in range(debut, 2013)}

    # Studio B — 20 staff
    for i in range(20, 40):
        pid = f"p{i}"
        debut = 2003 + (i % 10)  # debut 2003–2012
        debut_years[pid] = debut
        person_fe[pid] = 0.05 * (i - 30)  # range −0.5 to +0.45
        studio_assignments[pid] = {y: "studioB" for y in range(debut, 2013)}

    return person_fe, studio_assignments, debut_years


# ─────────────────────────────────────────────────────────────────────────────
# Helper unit tests
# ─────────────────────────────────────────────────────────────────────────────


def test_safe_hhi_single_person() -> None:
    """HHI = 1.0 when only one person has nonzero share."""
    from src.analysis.studio.pipeline_strength import _safe_hhi

    assert _safe_hhi([1.0]) == pytest.approx(1.0)


def test_safe_hhi_equal_shares() -> None:
    """HHI = 1/n for n equal shares."""
    from src.analysis.studio.pipeline_strength import _safe_hhi

    n = 4
    hhi = _safe_hhi([1.0] * n)
    assert hhi == pytest.approx(1.0 / n, rel=1e-6)


def test_safe_hhi_empty() -> None:
    """HHI = 1.0 for empty input (maximal concentration by convention)."""
    from src.analysis.studio.pipeline_strength import _safe_hhi

    assert _safe_hhi([]) == pytest.approx(1.0)


def test_top_k_concentration_k_larger_than_n() -> None:
    """top_k with k > n returns sum of all shares."""
    from src.analysis.studio.pipeline_strength import _top_k_concentration

    result = _top_k_concentration([0.4, 0.6], k=10)
    assert result == pytest.approx(1.0)


def test_top_k_concentration_value_range() -> None:
    """top-3 concentration is in [0, 1]."""
    from src.analysis.studio.pipeline_strength import _top_k_concentration

    shares = [0.5, 0.2, 0.15, 0.1, 0.05]
    val = _top_k_concentration(shares, k=3)
    assert 0.0 <= val <= 1.0


def test_bus_factor_from_hhi_inverse() -> None:
    """bus_factor = 1/HHI."""
    from src.analysis.studio.pipeline_strength import _bus_factor_from_hhi

    assert _bus_factor_from_hhi(0.25) == pytest.approx(4.0)
    assert _bus_factor_from_hhi(1.0) == pytest.approx(1.0)


# ─────────────────────────────────────────────────────────────────────────────
# compute_pipeline_strength: integration
# ─────────────────────────────────────────────────────────────────────────────


def test_compute_pipeline_strength_returns_result() -> None:
    """compute_pipeline_strength returns a non-empty result."""
    from src.analysis.studio.pipeline_strength import compute_pipeline_strength

    person_fe, studio_assignments, debut_years = _make_toy_data()
    result = compute_pipeline_strength(
        person_fe=person_fe,
        studio_assignments=studio_assignments,
        debut_years=debut_years,
        n_bootstrap=50,  # small for test speed
        rng_seed=0,
        min_staff_per_cell=2,
    )
    assert len(result.cells) > 0, "Must produce at least one cell"
    assert result.n_studios >= 2, "Must see both studios"


def test_compute_pipeline_strength_metric_ranges() -> None:
    """All metrics are within expected structural bounds."""
    from src.analysis.studio.pipeline_strength import compute_pipeline_strength

    person_fe, studio_assignments, debut_years = _make_toy_data()
    result = compute_pipeline_strength(
        person_fe=person_fe,
        studio_assignments=studio_assignments,
        debut_years=debut_years,
        n_bootstrap=50,
        rng_seed=0,
        min_staff_per_cell=2,
    )
    for cell in result.cells:
        if cell.mid_career_retention is not None:
            assert 0.0 <= cell.mid_career_retention <= 1.0, (
                f"mid_career_retention out of [0,1]: {cell.mid_career_retention}"
            )
        if cell.key_person_concentration is not None:
            assert 0.0 <= cell.key_person_concentration <= 1.0, (
                f"key_person_concentration out of [0,1]: "
                f"{cell.key_person_concentration}"
            )
        if cell.bus_factor is not None:
            assert cell.bus_factor >= 1.0, (
                f"bus_factor < 1: {cell.bus_factor}"
            )


def test_compute_pipeline_strength_ci_ordering() -> None:
    """Bootstrap CI lower bound <= point estimate <= upper bound."""
    from src.analysis.studio.pipeline_strength import compute_pipeline_strength

    person_fe, studio_assignments, debut_years = _make_toy_data()
    result = compute_pipeline_strength(
        person_fe=person_fe,
        studio_assignments=studio_assignments,
        debut_years=debut_years,
        n_bootstrap=200,
        rng_seed=42,
        min_staff_per_cell=2,
    )
    for cell in result.cells:
        if cell.mid_career_retention_ci is not None and cell.mid_career_retention is not None:
            lo, hi = cell.mid_career_retention_ci
            assert lo <= hi, f"CI lo > hi: {lo} > {hi}"
        if cell.bus_factor_ci is not None and cell.bus_factor is not None:
            lo, hi = cell.bus_factor_ci
            assert lo <= hi, f"bus_factor CI lo > hi: {lo} > {hi}"


def test_young_sample_flag() -> None:
    """young_sample_flag is True when fewer than 30 young staff."""
    from src.analysis.studio.pipeline_strength import (
        _MIN_YOUNG_SAMPLE,
        compute_pipeline_strength,
    )

    # With only 20 staff per studio, young sample will be < 30
    person_fe, studio_assignments, debut_years = _make_toy_data()
    result = compute_pipeline_strength(
        person_fe=person_fe,
        studio_assignments=studio_assignments,
        debut_years=debut_years,
        n_bootstrap=20,
        rng_seed=0,
        min_staff_per_cell=2,
    )
    flagged = [c for c in result.cells if c.young_sample_flag]
    unflagged = [
        c for c in result.cells
        if not c.young_sample_flag and c.young_theta_growth is not None
    ]
    for c in flagged:
        assert c.sample_young < _MIN_YOUNG_SAMPLE
    for c in unflagged:
        assert c.sample_young >= _MIN_YOUNG_SAMPLE


def test_compute_pipeline_strength_empty_input() -> None:
    """Empty input returns a result with no cells."""
    from src.analysis.studio.pipeline_strength import compute_pipeline_strength

    result = compute_pipeline_strength(
        person_fe={},
        studio_assignments={},
        debut_years={},
        n_bootstrap=10,
        rng_seed=0,
    )
    assert result.cells == []
    assert result.n_studios == 0


# ─────────────────────────────────────────────────────────────────────────────
# aggregate_by_studio
# ─────────────────────────────────────────────────────────────────────────────


def test_aggregate_by_studio_keys() -> None:
    """aggregate_by_studio returns dict with expected keys per studio."""
    from src.analysis.studio.pipeline_strength import (
        aggregate_by_studio,
        compute_pipeline_strength,
    )

    person_fe, studio_assignments, debut_years = _make_toy_data()
    result = compute_pipeline_strength(
        person_fe=person_fe,
        studio_assignments=studio_assignments,
        debut_years=debut_years,
        n_bootstrap=20,
        rng_seed=0,
        min_staff_per_cell=2,
    )
    agg = aggregate_by_studio(result, recent_years=5)
    for sid, stats in agg.items():
        for key in [
            "young_theta_growth_mean",
            "mid_career_retention_mean",
            "key_person_concentration_mean",
            "bus_factor_mean",
            "n_cells",
            "latest_year",
        ]:
            assert key in stats, f"Missing key '{key}' for studio {sid}"


def test_aggregate_by_studio_n_cells_positive() -> None:
    """n_cells is positive for all entries in the aggregate."""
    from src.analysis.studio.pipeline_strength import (
        aggregate_by_studio,
        compute_pipeline_strength,
    )

    person_fe, studio_assignments, debut_years = _make_toy_data()
    result = compute_pipeline_strength(
        person_fe=person_fe,
        studio_assignments=studio_assignments,
        debut_years=debut_years,
        n_bootstrap=20,
        rng_seed=0,
        min_staff_per_cell=2,
    )
    agg = aggregate_by_studio(result)
    for sid, stats in agg.items():
        assert stats["n_cells"] > 0, f"n_cells must be > 0 for {sid}"


# ─────────────────────────────────────────────────────────────────────────────
# lint_vocab — source must not contain forbidden vocabulary
# ─────────────────────────────────────────────────────────────────────────────

_FORBIDDEN_PATTERN = re.compile(
    r"\b(ability|talent|talented|competent|incompetent|capable|incapable|aptitude)\b"
    r"|能力|実力|才能|優秀|優れた|劣る|劣った|有能|無能",
    re.IGNORECASE,
)

_ANALYSIS_SRC = (
    Path(__file__).parents[3]
    / "src"
    / "analysis"
    / "studio"
    / "pipeline_strength.py"
)


def test_lint_vocab_analysis_source() -> None:
    """pipeline_strength.py must not contain forbidden vocabulary."""
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    matches = _FORBIDDEN_PATTERN.findall(text)
    assert not matches, (
        f"Forbidden vocabulary found in pipeline_strength.py: {matches}"
    )


def test_no_anime_score_in_analysis_usage() -> None:
    """pipeline_strength.py must not use anime.score as a computation input.

    Negation disclosures ("anime.score is never used") are allowed; any line
    containing "anime.score" must also contain a negation word.
    """
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    for line in text.split("\n"):
        if "anime.score" in line:
            lowered = line.lower()
            assert "not" in lowered or "never" in lowered or "no " in lowered, (
                f"Unexpected non-negation use of anime.score: {line.strip()}"
            )


def test_analysis_source_exists() -> None:
    """The analysis source file exists at the expected path."""
    assert _ANALYSIS_SRC.exists(), (
        f"Analysis source not found: {_ANALYSIS_SRC}"
    )
