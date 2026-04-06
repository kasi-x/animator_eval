"""Tests for src/analysis/robustness.py."""

from __future__ import annotations

import numpy as np

from src.analysis.robustness import (
    STANDARD_SUBSAMPLES,
    SubsampleSpec,
    run_robustness_grid,
)
from src.analysis.uncertainty import bootstrap_ci


def _sample_data(n: int = 100) -> list[dict]:
    rng = np.random.default_rng(42)
    return [
        {
            "person_id": f"p{i}",
            "value": float(rng.normal(50, 10)),
            "first_year": int(rng.integers(1990, 2025)),
        }
        for i in range(n)
    ]


def test_grid_all_pass() -> None:
    """All subsamples produce results when data covers all eras."""
    data = _sample_data(500)
    grid = run_robustness_grid(
        data=data,
        value_field="value",
        statistic=np.mean,
        subsamples=STANDARD_SUBSAMPLES,
    )
    # At least "全体" should always be present
    assert any(r["name"] == "全体" for r in grid)
    # Each entry has the required keys
    for r in grid:
        assert "estimate" in r
        assert "n" in r


def test_grid_with_ci() -> None:
    data = _sample_data(200)

    def _ci(arr: np.ndarray):
        return bootstrap_ci(arr, np.mean, seed=0, n_bootstrap=100)

    grid = run_robustness_grid(
        data=data,
        value_field="value",
        statistic=np.mean,
        subsamples=STANDARD_SUBSAMPLES,
        ci_fn=_ci,
    )
    for r in grid:
        assert "ci_lower" in r
        assert "ci_upper" in r
        assert r["ci_lower"] <= r["estimate"] <= r["ci_upper"]


def test_grid_min_n_filter() -> None:
    """Subsamples below min_n are skipped."""
    data = [{"value": 1.0, "first_year": 2020}] * 5
    grid = run_robustness_grid(
        data=data,
        value_field="value",
        statistic=np.mean,
        subsamples=STANDARD_SUBSAMPLES,
        min_n=10,
    )
    # All should be skipped because n=5 < 10
    assert len(grid) == 0


def test_grid_custom_subsample() -> None:
    data = [
        {"value": 10.0, "role": "director"},
        {"value": 20.0, "role": "director"},
        {"value": 30.0, "role": "animator"},
    ] * 10  # 30 rows total

    subs = (
        SubsampleSpec(name="director", filter_fn=lambda r: r["role"] == "director"),
        SubsampleSpec(name="animator", filter_fn=lambda r: r["role"] == "animator"),
    )
    grid = run_robustness_grid(
        data=data,
        value_field="value",
        statistic=np.mean,
        subsamples=subs,
    )
    names = {r["name"] for r in grid}
    assert "director" in names
    assert "animator" in names

    director_row = next(r for r in grid if r["name"] == "director")
    assert abs(director_row["estimate"] - 15.0) < 0.01


def test_standard_subsamples_tuple() -> None:
    assert isinstance(STANDARD_SUBSAMPLES, tuple)
    assert len(STANDARD_SUBSAMPLES) >= 5
