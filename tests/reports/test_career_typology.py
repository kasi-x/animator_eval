"""Tests for CareerTypologyReport.

Coverage:
- smoke: CareerTypologyReport.generate() with synthetic in-memory DB
- stop-if path: empty DB produces stop-if report
- lint_vocab: forbidden vocabulary absent from report source
- method gate: CI + null_model declared in insert_lineage call
- registered in V2_REPORT_CLASSES
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Synthetic DB fixtures
# ---------------------------------------------------------------------------


def _create_base_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT NOT NULL DEFAULT '',
            credit_year INTEGER,
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


@pytest.fixture()
def empty_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    _create_base_schema(conn)
    return conn


@pytest.fixture()
def populated_conn() -> sqlite3.Connection:
    """60 persons × 7 years, 3 structured trajectory patterns."""
    conn = sqlite3.connect(":memory:")
    _create_base_schema(conn)

    patterns = [
        ["in_between", "key_animator", "key_animator", "animation_director",
         "animation_director", "animation_director", "director"],
        ["key_animator", "key_animator", "key_animator",
         "key_animator", "key_animator", "key_animator", "key_animator"],
        ["in_between", "in_between", "in_between", "in_between",
         "in_between", "key_animator", "key_animator"],
    ]

    for p_idx in range(60):
        pattern = patterns[p_idx % 3]
        for year_offset, role in enumerate(pattern):
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) "
                "VALUES (?, ?, ?, ?)",
                (f"p{p_idx}", f"a{p_idx % 20}", role, 2000 + year_offset),
            )

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


def test_generate_returns_path_empty_db(empty_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """CareerTypologyReport.generate() with empty DB returns a valid path."""
    from scripts.report_generators.reports.career_typology import CareerTypologyReport

    report = CareerTypologyReport(empty_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None, "generate() must return a Path"
    assert result.exists(), f"Output file must exist: {result}"


def test_generate_stop_if_in_html_for_empty_db(
    empty_conn: sqlite3.Connection, tmp_path: Path
) -> None:
    """Empty DB causes stop-if path; HTML contains stop-if marker."""
    from scripts.report_generators.reports.career_typology import CareerTypologyReport

    report = CareerTypologyReport(empty_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None
    html = result.read_text(encoding="utf-8")
    assert "stop" in html.lower() or "insufficient" in html.lower() or "不足" in html, (
        "Stop-if HTML must mention stop/insufficient/不足"
    )


def test_generate_with_populated_db(
    populated_conn: sqlite3.Connection, tmp_path: Path
) -> None:
    """CareerTypologyReport.generate() with populated DB returns valid HTML."""
    from scripts.report_generators.reports.career_typology import CareerTypologyReport

    report = CareerTypologyReport(populated_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None, "generate() must return a Path"
    assert result.exists(), f"Output file must exist: {result}"
    html = result.read_text(encoding="utf-8")
    # Should contain typology-related content
    assert len(html) > 500, "HTML must have substantial content"


def test_generate_creates_lineage(
    populated_conn: sqlite3.Connection, tmp_path: Path
) -> None:
    """generate() inserts a row in meta_lineage with CI and null_model."""
    from scripts.report_generators.reports.career_typology import CareerTypologyReport

    report = CareerTypologyReport(populated_conn, output_dir=tmp_path)
    report.generate()

    row = populated_conn.execute(
        "SELECT ci_method, null_model FROM meta_lineage "
        "WHERE table_name = 'meta_career_typology'"
    ).fetchone()

    assert row is not None, "meta_lineage row must be inserted"
    ci_method, null_model = row
    assert ci_method, "ci_method must be non-empty"
    assert null_model, "null_model must be non-empty"


# ---------------------------------------------------------------------------
# Lint vocab
# ---------------------------------------------------------------------------

_REPORT_SRC = (
    Path(__file__).parents[2]
    / "scripts"
    / "report_generators"
    / "reports"
    / "career_typology.py"
)

_ANALYSIS_SRC = (
    Path(__file__).parents[2]
    / "src"
    / "analysis"
    / "career"
    / "trajectory_typology.py"
)


def _lint_vocab_violations(path: Path) -> list[str]:
    import sys

    lint_vocab_module = (
        Path(__file__).parents[2] / "scripts" / "report_generators" / "lint_vocab.py"
    )
    if str(lint_vocab_module.parent) not in sys.path:
        sys.path.insert(0, str(lint_vocab_module.parent))

    from scripts.report_generators.lint_vocab import (
        _compile_patterns,
        _is_definitional,
        _is_excepted,
        lint_file,
        load_exceptions,
        load_vocab,
    )

    terms = load_vocab()
    exceptions = load_exceptions()
    patterns = _compile_patterns(terms)
    findings = lint_file(path, patterns, {})
    real_findings = [
        f for f in findings
        if not _is_definitional(f) and not _is_excepted(f, exceptions)
    ]
    return [f.format() for f in real_findings]


def test_lint_vocab_report() -> None:
    """career_typology.py must not contain forbidden vocabulary in string literals."""
    violations = _lint_vocab_violations(_REPORT_SRC)
    assert not violations, (
        "Forbidden vocabulary found in career_typology.py:\n" + "\n".join(violations)
    )


def test_lint_vocab_analysis() -> None:
    """trajectory_typology.py must not contain forbidden vocabulary in string literals."""
    violations = _lint_vocab_violations(_ANALYSIS_SRC)
    assert not violations, (
        "Forbidden vocabulary found in trajectory_typology.py:\n" + "\n".join(violations)
    )


def test_no_anime_score_in_report() -> None:
    """career_typology.py must not reference anime.score."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text, "anime.score must not appear in career_typology.py"


# ---------------------------------------------------------------------------
# Method gate
# ---------------------------------------------------------------------------


def test_method_gate_ci_present() -> None:
    """Report source must declare a CI method in insert_lineage."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "ci_method" in text, "ci_method must be declared in insert_lineage call"
    assert "silhouette" in text.lower(), "CI method must mention silhouette"


def test_method_gate_null_model_present() -> None:
    """Report source must declare a null_model in insert_lineage."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "null_model" in text, "null_model must be declared in insert_lineage call"
    assert "threshold" in text.lower() or "stop" in text.lower(), (
        "null_model must mention threshold or stop-if"
    )


# ---------------------------------------------------------------------------
# Registration + metadata
# ---------------------------------------------------------------------------


def test_report_registered_in_init() -> None:
    """CareerTypologyReport must be in V2_REPORT_CLASSES."""
    from scripts.report_generators.reports import V2_REPORT_CLASSES
    from scripts.report_generators.reports.career_typology import CareerTypologyReport

    assert CareerTypologyReport in V2_REPORT_CLASSES, (
        "CareerTypologyReport must be in V2_REPORT_CLASSES"
    )


def test_report_name_and_filename() -> None:
    """CareerTypologyReport must have correct name and filename."""
    from scripts.report_generators.reports.career_typology import CareerTypologyReport

    assert CareerTypologyReport.name == "career_typology"
    assert CareerTypologyReport.filename == "career_typology.html"


def test_report_src_exists() -> None:
    """career_typology.py source file must exist."""
    assert _REPORT_SRC.exists(), f"Report source not found: {_REPORT_SRC}"


def test_analysis_src_exists() -> None:
    """trajectory_typology.py source file must exist."""
    assert _ANALYSIS_SRC.exists(), f"Analysis source not found: {_ANALYSIS_SRC}"
