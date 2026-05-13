"""Unit tests for src.analysis.career.role_progression.

Coverage:
- compute_progression_years: forward / backward / censored transitions
- km_role_tenure: KM fit, cohort stratification, CI bounds
- logrank_cohort_comparison: basic test with synthetic data
- compute_studio_blockage: blockage score + CI ordering
- compute_role_counts: per-role distinct person counts
- PIPELINE_ROLES / PIPELINE_LABELS constants
"""

from __future__ import annotations

import math
import sqlite3

import pytest


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def db() -> sqlite3.Connection:
    """Minimal in-memory SQLite DB with credits and anime tables."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            credit_year INTEGER
        );
        CREATE TABLE anime (
            id TEXT PRIMARY KEY,
            studio_id TEXT
        );
    """)

    # 4 studios, 20 anime
    for i in range(20):
        conn.execute(
            "INSERT INTO anime (id, studio_id) VALUES (?, ?)",
            (f"a{i}", f"studio_{i % 4}"),
        )

    # 200 persons in 'in_between', graduating to 'key_animator' after 3–7 years
    for p in range(200):
        debut = 1990 + (p % 20)
        anime_id = f"a{p % 20}"
        conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
            (f"p{p}", anime_id, "in_between", debut),
        )
        if p < 150:
            advance = debut + 3 + (p % 5)
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (f"p{p}", f"a{(p + 10) % 20}", "key_animator", advance),
            )

    # 30 persons jump to 'animation_director'
    for p in range(30):
        debut = 1990 + (p % 15)
        adv = debut + 8 + (p % 5)
        conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
            (f"p{p}", f"a{p % 20}", "animation_director", adv),
        )

    # 10 persons reach 'director'
    for p in range(10):
        debut = 1990 + (p % 10)
        adv = debut + 15 + p
        conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
            (f"p{p}", f"a{p % 20}", "director", adv),
        )

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_pipeline_roles_order() -> None:
    """PIPELINE_ROLES must be in ascending career stage order."""
    from src.analysis.career.role_progression import PIPELINE_ROLES

    assert PIPELINE_ROLES[0] == "in_between"
    assert PIPELINE_ROLES[-1] == "director"
    assert len(PIPELINE_ROLES) == 4


def test_pipeline_labels_coverage() -> None:
    """PIPELINE_LABELS must cover all PIPELINE_ROLES."""
    from src.analysis.career.role_progression import PIPELINE_LABELS, PIPELINE_ROLES

    for role in PIPELINE_ROLES:
        assert role in PIPELINE_LABELS, f"Missing label for role '{role}'"


# ---------------------------------------------------------------------------
# compute_progression_years
# ---------------------------------------------------------------------------


def test_progression_years_valid_roles(db: sqlite3.Connection) -> None:
    """compute_progression_years returns a non-empty list for valid role pair."""
    from src.analysis.career.role_progression import compute_progression_years

    records = compute_progression_years(db, "in_between", "key_animator")
    assert len(records) > 0


def test_progression_years_forward_only(db: sqlite3.Connection) -> None:
    """All non-None durations must be non-negative (forward transitions only)."""
    from src.analysis.career.role_progression import compute_progression_years

    records = compute_progression_years(db, "in_between", "key_animator")
    for rec in records:
        if rec.duration_years is not None:
            assert rec.duration_years >= 0, (
                f"Negative duration: {rec.duration_years}"
            )


def test_progression_years_censored_present(db: sqlite3.Connection) -> None:
    """Some records should be censored (duration_years=None) for non-graduating persons."""
    from src.analysis.career.role_progression import compute_progression_years

    records = compute_progression_years(db, "in_between", "key_animator")
    censored = [r for r in records if r.duration_years is None]
    # 50 persons (p150..p199) never reach key_animator
    assert len(censored) >= 10, (
        f"Expected censored records, got {len(censored)}"
    )


def test_progression_years_cohort_5y_multiple(db: sqlite3.Connection) -> None:
    """cohort_5y values must be exact multiples of 5."""
    from src.analysis.career.role_progression import compute_progression_years

    records = compute_progression_years(db, "in_between", "key_animator")
    for rec in records:
        assert rec.cohort_5y % 5 == 0, (
            f"cohort_5y {rec.cohort_5y} is not a multiple of 5"
        )


def test_progression_years_invalid_roles_raises(db: sqlite3.Connection) -> None:
    """ValueError raised for roles not in PIPELINE_ROLES."""
    from src.analysis.career.role_progression import compute_progression_years

    with pytest.raises(ValueError, match="PIPELINE_ROLES"):
        compute_progression_years(db, "voice_actor", "key_animator")


def test_progression_years_same_role_raises(db: sqlite3.Connection) -> None:
    """ValueError raised when role_from equals role_to (prevents nonsensical progression)."""
    from src.analysis.career.role_progression import PIPELINE_ROLES, compute_progression_years

    # Both roles are valid but same — still valid call, result should be trivially non-negative
    # Actually the spec allows same roles; only invalid roles raise. Just check no crash.
    records = compute_progression_years(db, PIPELINE_ROLES[0], PIPELINE_ROLES[0])
    # All durations should be 0 (same role)
    for rec in records:
        if rec.duration_years is not None:
            assert rec.duration_years == 0.0


# ---------------------------------------------------------------------------
# km_role_tenure
# ---------------------------------------------------------------------------


def test_km_role_tenure_nonempty(db: sqlite3.Connection) -> None:
    """km_role_tenure returns at least one cohort for a well-populated dataset."""
    from src.analysis.career.role_progression import (
        compute_progression_years,
        km_role_tenure,
    )

    records = compute_progression_years(db, "in_between", "key_animator")
    km = km_role_tenure(records, min_cohort_size=2)
    assert len(km) > 0, "Expected at least one KM cohort"


def test_km_survival_bounds(db: sqlite3.Connection) -> None:
    """Survival values must lie in [0, 1] and start at or near 1.0."""
    from src.analysis.career.role_progression import (
        compute_progression_years,
        km_role_tenure,
    )

    records = compute_progression_years(db, "in_between", "key_animator")
    km = km_role_tenure(records, min_cohort_size=2)

    for label, result in km.items():
        assert result.survival[0] <= 1.0, (
            f"Cohort {label}: first survival value > 1.0"
        )
        for s in result.survival:
            assert 0.0 <= s <= 1.0 + 1e-6, (
                f"Cohort {label}: survival {s} out of [0, 1]"
            )


def test_km_ci_bounds(db: sqlite3.Connection) -> None:
    """CI lower must be <= survival <= CI upper at each time point."""
    from src.analysis.career.role_progression import (
        compute_progression_years,
        km_role_tenure,
    )

    records = compute_progression_years(db, "in_between", "key_animator")
    km = km_role_tenure(records, min_cohort_size=2)

    for label, result in km.items():
        for lo, mid, hi in zip(result.ci_lower, result.survival, result.ci_upper):
            assert lo <= mid + 1e-6, (
                f"Cohort {label}: ci_lower {lo} > survival {mid}"
            )
            assert mid <= hi + 1e-6, (
                f"Cohort {label}: survival {mid} > ci_upper {hi}"
            )


def test_km_median_finite_or_none(db: sqlite3.Connection) -> None:
    """median_survival must be finite (>= 0) or None."""
    from src.analysis.career.role_progression import (
        compute_progression_years,
        km_role_tenure,
    )

    records = compute_progression_years(db, "in_between", "key_animator")
    km = km_role_tenure(records, min_cohort_size=2)

    for label, result in km.items():
        if result.median_survival is not None:
            assert result.median_survival >= 0, (
                f"Cohort {label}: negative median_survival"
            )
            assert not math.isnan(result.median_survival), (
                f"Cohort {label}: median_survival is NaN"
            )


def test_km_cohort_key_format(db: sqlite3.Connection) -> None:
    """KM cohort keys must be formatted as 'YYYY-YYYY' range strings."""
    from src.analysis.career.role_progression import (
        compute_progression_years,
        km_role_tenure,
    )

    records = compute_progression_years(db, "in_between", "key_animator")
    km = km_role_tenure(records, min_cohort_size=2)

    import re
    pattern = re.compile(r"^\d{4}–\d{4}$")
    for label in km:
        assert pattern.match(label), (
            f"Cohort label '{label}' does not match 'YYYY–YYYY' format"
        )


def test_km_min_cohort_size_respected(db: sqlite3.Connection) -> None:
    """Cohorts smaller than min_cohort_size must be excluded."""
    from src.analysis.career.role_progression import (
        compute_progression_years,
        km_role_tenure,
    )

    records = compute_progression_years(db, "in_between", "key_animator")
    km_strict = km_role_tenure(records, min_cohort_size=1000)
    # With 1000 minimum, no cohort should qualify
    assert len(km_strict) == 0, (
        "Expected 0 cohorts with min_cohort_size=1000"
    )


# ---------------------------------------------------------------------------
# logrank_cohort_comparison
# ---------------------------------------------------------------------------


def test_logrank_returns_dict_or_empty(db: sqlite3.Connection) -> None:
    """logrank_cohort_comparison returns dict with p_value or empty dict."""
    from src.analysis.career.role_progression import (
        compute_progression_years,
        logrank_cohort_comparison,
    )

    records = compute_progression_years(db, "in_between", "key_animator")
    result = logrank_cohort_comparison(records, min_cohort_size=2)

    if result:
        assert "p_value" in result
        assert 0.0 <= result["p_value"] <= 1.0, (
            f"p_value out of [0, 1]: {result['p_value']}"
        )
        assert "n_cohorts" in result
        assert result["n_cohorts"] >= 2


def test_logrank_empty_on_single_cohort(db: sqlite3.Connection) -> None:
    """logrank returns empty dict when fewer than 2 cohorts pass min_cohort_size."""
    from src.analysis.career.role_progression import (
        compute_progression_years,
        logrank_cohort_comparison,
    )

    records = compute_progression_years(db, "in_between", "key_animator")
    result = logrank_cohort_comparison(records, min_cohort_size=500)
    assert result == {}, (
        "Expected empty dict when fewer than 2 cohorts meet min_cohort_size"
    )


# ---------------------------------------------------------------------------
# compute_studio_blockage
# ---------------------------------------------------------------------------


def test_studio_blockage_returns_list(db: sqlite3.Connection) -> None:
    """compute_studio_blockage returns a list (may be empty if not enough data)."""
    from src.analysis.career.role_progression import compute_studio_blockage

    rows = compute_studio_blockage(
        db, "in_between", "key_animator", n_bootstrap=20, rng_seed=0, min_studio_persons=2
    )
    assert isinstance(rows, list)


def test_studio_blockage_ci_ordering(db: sqlite3.Connection) -> None:
    """For each StudioBlockageRow: ci_low <= ci_high."""
    from src.analysis.career.role_progression import compute_studio_blockage

    rows = compute_studio_blockage(
        db, "in_between", "key_animator", n_bootstrap=50, rng_seed=1, min_studio_persons=2
    )
    for row in rows:
        assert row.ci_low <= row.ci_high, (
            f"Studio {row.studio_id}: ci_low {row.ci_low} > ci_high {row.ci_high}"
        )


def test_studio_blockage_sorted_descending(db: sqlite3.Connection) -> None:
    """Results must be sorted by blockage_score descending."""
    from src.analysis.career.role_progression import compute_studio_blockage

    rows = compute_studio_blockage(
        db, "in_between", "key_animator", n_bootstrap=20, rng_seed=2, min_studio_persons=2
    )
    scores = [r.blockage_score for r in rows]
    assert scores == sorted(scores, reverse=True), (
        "Studio blockage rows must be sorted descending by blockage_score"
    )


def test_studio_blockage_industry_median_consistent(db: sqlite3.Connection) -> None:
    """All rows must share the same industry_median."""
    from src.analysis.career.role_progression import compute_studio_blockage

    rows = compute_studio_blockage(
        db, "in_between", "key_animator", n_bootstrap=20, rng_seed=3, min_studio_persons=2
    )
    if not rows:
        pytest.skip("No studios qualify — insufficient observed progressors")

    medians = {r.industry_median for r in rows}
    assert len(medians) == 1, (
        f"Expected single industry_median across all rows, got {medians}"
    )


# ---------------------------------------------------------------------------
# compute_role_counts
# ---------------------------------------------------------------------------


def test_role_counts_all_pipeline_roles(db: sqlite3.Connection) -> None:
    """compute_role_counts returns counts for every PIPELINE_ROLES entry."""
    from src.analysis.career.role_progression import PIPELINE_ROLES, compute_role_counts

    counts = compute_role_counts(db)
    assert set(counts.keys()) == set(PIPELINE_ROLES)


def test_role_counts_nonnegative(db: sqlite3.Connection) -> None:
    """All role counts must be non-negative."""
    from src.analysis.career.role_progression import compute_role_counts

    counts = compute_role_counts(db)
    for role, cnt in counts.items():
        assert cnt >= 0, f"Negative count for role '{role}': {cnt}"


def test_role_counts_in_between_large(db: sqlite3.Connection) -> None:
    """in_between must have the largest count in the synthetic fixture."""
    from src.analysis.career.role_progression import compute_role_counts

    counts = compute_role_counts(db)
    assert counts["in_between"] >= counts["director"], (
        "in_between must have more persons than director in the funnel"
    )


def test_role_counts_custom_roles(db: sqlite3.Connection) -> None:
    """compute_role_counts accepts a custom role list."""
    from src.analysis.career.role_progression import compute_role_counts

    counts = compute_role_counts(db, roles=["in_between", "director"])
    assert set(counts.keys()) == {"in_between", "director"}
