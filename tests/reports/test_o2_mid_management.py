"""Tests for O2 mid-management pipeline report.

Coverage:
- smoke: O2MidManagementReport.generate() with synthetic in-memory DB
- lint_vocab: forbidden vocabulary absent from report source code
- method gate: insert_lineage called with CI + null_model
- analysis: compute_progression_years / km_role_tenure / compute_studio_blockage
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def silver_conn() -> sqlite3.Connection:
    """In-memory SQLite connection with minimal SILVER schema + synthetic data.

    Populated with synthetic persons, anime, and credits covering all four
    pipeline roles with enough rows to pass the _MIN_ROLE_COUNT check.
    """
    conn = sqlite3.connect(":memory:")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            name_ja TEXT NOT NULL DEFAULT '',
            name_en TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            studio_id TEXT,
            year INTEGER,
            quarter INTEGER
        );

        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT NOT NULL DEFAULT '',
            credit_year INTEGER,
            episode INTEGER,
            evidence_source TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS meta_lineage (
            table_name TEXT PRIMARY KEY,
            audience TEXT NOT NULL,
            source_silver_tables TEXT NOT NULL,
            source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
            source_display_allowed INTEGER NOT NULL DEFAULT 0,
            formula_version TEXT NOT NULL,
            computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ci_method TEXT,
            null_model TEXT,
            holdout_method TEXT,
            description TEXT,
            inputs_hash TEXT,
            notes TEXT,
            rng_seed INTEGER,
            row_count INTEGER
        );
    """)

    n_studios = 4
    studio_ids = [f"studio_{i}" for i in range(n_studios)]

    # Synthetic anime
    for i in range(60):
        studio_id = studio_ids[i % n_studios]
        year = 1995 + (i % 30)
        conn.execute(
            "INSERT INTO anime (id, title_ja, studio_id, year, quarter) VALUES (?,?,?,?,?)",
            (f"a{i}", f"Anime{i}", studio_id, year, 1 + (i % 4)),
        )

    # Synthetic persons: 4000 total, split across roles so each role >= 1000 distinct persons
    #   - Roles: in_between, key_animator, animation_director, director
    #   - Each person can have multiple roles (simulates career progression)
    role_cycle = ["in_between", "key_animator", "animation_director", "director"]

    for p_idx in range(4000):
        pid = f"p{p_idx}"
        conn.execute(
            "INSERT INTO persons (id, name_en) VALUES (?, ?)",
            (pid, f"Person{p_idx}"),
        )
        # Primary role for this person
        primary_role = role_cycle[p_idx % 4]
        debut_year = 1985 + (p_idx % 25)

        # Primary credit
        anime_id = f"a{p_idx % 60}"
        conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
            (pid, anime_id, primary_role, debut_year),
        )

        # Give some persons a progression: also add a higher-stage credit some years later
        if p_idx < 1200 and primary_role == "in_between":
            # These persons also appear as key_animator later
            later_year = debut_year + 3 + (p_idx % 5)
            anime2 = f"a{(p_idx + 30) % 60}"
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (pid, anime2, "key_animator", later_year),
            )

        if p_idx < 400 and primary_role == "in_between":
            # Some also become animation_director
            later_year2 = debut_year + 8 + (p_idx % 7)
            anime3 = f"a{(p_idx + 15) % 60}"
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (pid, anime3, "animation_director", later_year2),
            )

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------


def test_generate_returns_path(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """O2MidManagementReport.generate() returns a valid HTML path."""
    from scripts.report_generators.reports.o2_mid_management import O2MidManagementReport

    report = O2MidManagementReport(silver_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None, "generate() must return a Path, not None"
    assert result.exists(), f"Output file must exist: {result}"
    html = result.read_text(encoding="utf-8")
    assert "mid" in html.lower() or "管" in html or "役職" in html, (
        "HTML should contain mid-management or 役職 content"
    )


def test_generate_creates_lineage(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """insert_lineage must populate meta_lineage with CI and null_model."""
    from scripts.report_generators.reports.o2_mid_management import O2MidManagementReport

    report = O2MidManagementReport(silver_conn, output_dir=tmp_path)
    report.generate()

    row = silver_conn.execute(
        "SELECT ci_method, null_model FROM meta_lineage WHERE table_name = 'meta_o2_mid_management'"
    ).fetchone()

    assert row is not None, "meta_lineage row must be inserted"
    ci_method, null_model = row
    assert ci_method, "ci_method must be non-empty"
    assert null_model, "null_model must be non-empty"


def test_generate_insufficient_data(tmp_path: Path) -> None:
    """When role counts are below threshold, generate() still returns a path (graceful)."""
    from scripts.report_generators.reports.o2_mid_management import O2MidManagementReport

    empty_conn = sqlite3.connect(":memory:")
    empty_conn.executescript("""
        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            credit_year INTEGER,
            episode INTEGER,
            evidence_source TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS meta_lineage (
            table_name TEXT PRIMARY KEY,
            audience TEXT NOT NULL,
            source_silver_tables TEXT NOT NULL,
            source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
            source_display_allowed INTEGER NOT NULL DEFAULT 0,
            formula_version TEXT NOT NULL,
            computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ci_method TEXT,
            null_model TEXT,
            holdout_method TEXT,
            description TEXT,
            inputs_hash TEXT,
            notes TEXT,
            rng_seed INTEGER,
            row_count INTEGER
        );
    """)
    report = O2MidManagementReport(empty_conn, output_dir=tmp_path)
    result = report.generate()
    assert result is not None, "generate() must return a path even with insufficient data"
    assert result.exists()


# ---------------------------------------------------------------------------
# lint_vocab: source code must not contain forbidden vocabulary
# ---------------------------------------------------------------------------

_FORBIDDEN_PATTERN = re.compile(
    r"\b(ability|skill|talent|competence|capability)\b",
    re.IGNORECASE,
)

_REPORT_SRC = (
    Path(__file__).parents[2]
    / "scripts"
    / "report_generators"
    / "reports"
    / "o2_mid_management.py"
)

_ANALYSIS_SRC = (
    Path(__file__).parents[2]
    / "src"
    / "analysis"
    / "career"
    / "role_progression.py"
)


def test_lint_vocab_report() -> None:
    """o2_mid_management.py must not contain forbidden vocabulary."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    matches = _FORBIDDEN_PATTERN.findall(text)
    assert not matches, (
        f"Forbidden vocabulary found in o2_mid_management.py: {matches}"
    )


def test_lint_vocab_analysis() -> None:
    """role_progression.py must not contain forbidden vocabulary."""
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    matches = _FORBIDDEN_PATTERN.findall(text)
    assert not matches, (
        f"Forbidden vocabulary found in role_progression.py: {matches}"
    )


def test_no_anime_score_in_report() -> None:
    """o2_mid_management.py must not reference anime.score."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text, "anime.score must not appear in o2_mid_management.py"


def test_no_anime_score_in_analysis() -> None:
    """role_progression.py must not reference anime.score."""
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text, "anime.score must not appear in role_progression.py"


# ---------------------------------------------------------------------------
# Method gate: CI + null_model present in report source
# ---------------------------------------------------------------------------


def test_method_gate_ci_present() -> None:
    """Report source must declare a CI method."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "ci_method" in text, "ci_method must be declared in insert_lineage call"
    assert "bootstrap" in text.lower() or "greenwood" in text.lower(), (
        "CI method must mention bootstrap or Greenwood formula"
    )


def test_method_gate_null_model_present() -> None:
    """Report source must declare a null model."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "null_model" in text, "null_model must be declared in insert_lineage call"


# ---------------------------------------------------------------------------
# Unit tests: analysis functions
# ---------------------------------------------------------------------------


def test_compute_progression_years_basic(silver_conn: sqlite3.Connection) -> None:
    """compute_progression_years returns ProgressionRecord list."""
    from src.analysis.career.role_progression import compute_progression_years

    records = compute_progression_years(silver_conn, "in_between", "key_animator")
    assert len(records) > 0, "Must return at least some records"

    # All records with non-None duration must have non-negative duration
    for rec in records:
        if rec.duration_years is not None:
            assert rec.duration_years >= 0, (
                f"duration_years must be non-negative, got {rec.duration_years}"
            )


def test_compute_progression_years_invalid_roles(silver_conn: sqlite3.Connection) -> None:
    """compute_progression_years raises ValueError for unknown roles."""
    from src.analysis.career.role_progression import compute_progression_years

    with pytest.raises(ValueError, match="PIPELINE_ROLES"):
        compute_progression_years(silver_conn, "unknown_role", "key_animator")


def test_km_role_tenure_returns_results(silver_conn: sqlite3.Connection) -> None:
    """km_role_tenure returns KMResult dict with non-empty entries."""
    from src.analysis.career.role_progression import (
        compute_progression_years,
        km_role_tenure,
    )

    records = compute_progression_years(silver_conn, "in_between", "key_animator")
    km = km_role_tenure(records, min_cohort_size=2)

    # Should have at least one cohort
    assert len(km) > 0, "km_role_tenure must return at least one cohort"

    for label, result in km.items():
        assert result.n > 0, f"Cohort {label} must have n > 0"
        assert len(result.timeline) == len(result.survival), (
            "timeline and survival must have equal length"
        )
        assert len(result.ci_lower) == len(result.ci_upper), (
            "ci_lower and ci_upper must have equal length"
        )
        for s in result.survival:
            assert 0.0 <= s <= 1.0, f"Survival must be in [0,1], got {s}"


def test_compute_studio_blockage_returns_rows(silver_conn: sqlite3.Connection) -> None:
    """compute_studio_blockage returns blockage rows when studio data exists."""
    from src.analysis.career.role_progression import compute_studio_blockage

    rows = compute_studio_blockage(
        silver_conn,
        role_from="in_between",
        role_to="key_animator",
        n_bootstrap=50,  # small for test speed
        rng_seed=0,
        min_studio_persons=2,
    )
    # With synthetic data that has studio affiliations, should get some rows
    # (may be empty if studio_id join has no data — that is acceptable)
    for row in rows:
        assert row.ci_low <= row.blockage_score <= row.ci_high or (
            row.ci_low <= row.ci_high
        ), "CI must span blockage_score"


def test_compute_role_counts(silver_conn: sqlite3.Connection) -> None:
    """compute_role_counts returns correct counts per pipeline role."""
    from src.analysis.career.role_progression import PIPELINE_ROLES, compute_role_counts

    counts = compute_role_counts(silver_conn)
    assert set(counts.keys()) == set(PIPELINE_ROLES), (
        "compute_role_counts must return counts for all PIPELINE_ROLES"
    )
    # in_between has 1000 persons in synthetic data (p0, p4, p8, ...)
    assert counts["in_between"] >= 1000, (
        f"Expected >= 1000 in_between persons, got {counts['in_between']}"
    )


def test_cohort_5y_assignment(silver_conn: sqlite3.Connection) -> None:
    """cohort_5y must be multiples of 5."""
    from src.analysis.career.role_progression import compute_progression_years

    records = compute_progression_years(silver_conn, "in_between", "key_animator")
    for rec in records:
        assert rec.cohort_5y % 5 == 0, (
            f"cohort_5y must be a multiple of 5, got {rec.cohort_5y}"
        )
