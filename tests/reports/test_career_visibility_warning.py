"""Tests for scripts/report_generators/reports/career_visibility_warning.py.

Coverage:
- smoke: CareerVisibilityWarningReport.generate() with synthetic in-memory DB
- empty DB: graceful fallback
- lint_vocab: forbidden vocabulary absent from report source
- method gate: AUC gate / subgroup diff gate / CI declared
- SPEC: registered in V2_REPORT_CLASSES + correct name/filename
- no anime.score in source
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Source path constants
# ---------------------------------------------------------------------------

_REPORT_SRC = (
    Path(__file__).parents[2]
    / "scripts"
    / "report_generators"
    / "reports"
    / "career_visibility_warning.py"
)

_ANALYSIS_SRC = (
    Path(__file__).parents[2]
    / "src"
    / "analysis"
    / "career"
    / "visibility_loss.py"
)


# ---------------------------------------------------------------------------
# Synthetic DB fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def silver_conn() -> sqlite3.Connection:
    """In-memory SQLite with minimal schema for report smoke tests."""
    conn = sqlite3.connect(":memory:")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            name_en TEXT NOT NULL DEFAULT '',
            gender TEXT
        );
        CREATE TABLE IF NOT EXISTS anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            studio_id TEXT,
            year INTEGER,
            episodes INTEGER DEFAULT 12,
            duration INTEGER DEFAULT 24
        );
        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'in_between',
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

    for i in range(20):
        conn.execute(
            "INSERT INTO anime (id, title_ja, studio_id, year) VALUES (?,?,?,?)",
            (f"a{i}", f"Anime{i}", f"studio_{i % 3}", 2010 + (i % 10)),
        )

    for p in range(30):
        gender = "female" if p < 15 else "male"
        conn.execute(
            "INSERT INTO persons (id, name_en, gender) VALUES (?,?,?)",
            (f"p{p}", f"Person{p}", gender),
        )

    for p in range(30):
        for yr in range(2010, 2022):
            anime_id = f"a{(p + yr) % 20}"
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (f"p{p}", anime_id, "in_between", yr),
            )

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


def test_generate_returns_path(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """CareerVisibilityWarningReport.generate() must return a valid Path."""
    from scripts.report_generators.reports.career_visibility_warning import (
        CareerVisibilityWarningReport,
    )

    report = CareerVisibilityWarningReport(silver_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None, "generate() must return a Path, not None"
    assert result.exists(), f"Output file must exist: {result}"

    html = result.read_text(encoding="utf-8")
    assert len(html) > 100, "HTML must have non-trivial content"


def test_generate_empty_db(tmp_path: Path) -> None:
    """CareerVisibilityWarningReport.generate() with empty DB returns path gracefully."""
    from scripts.report_generators.reports.career_visibility_warning import (
        CareerVisibilityWarningReport,
    )

    empty_conn = sqlite3.connect(":memory:")
    report = CareerVisibilityWarningReport(empty_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None, "generate() must return a path even with empty data"
    assert result.exists()


def test_html_contains_gate_info(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """Generated HTML must mention the AUC gate or gate condition."""
    from scripts.report_generators.reports.career_visibility_warning import (
        CareerVisibilityWarningReport,
    )

    report = CareerVisibilityWarningReport(silver_conn, output_dir=tmp_path)
    result = report.generate()
    assert result is not None

    html = result.read_text(encoding="utf-8")
    assert "0.65" in html or "AUC" in html, "HTML must mention AUC gate value"


def test_html_no_anime_score(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """Generated HTML must not contain anime.score."""
    from scripts.report_generators.reports.career_visibility_warning import (
        CareerVisibilityWarningReport,
    )

    report = CareerVisibilityWarningReport(silver_conn, output_dir=tmp_path)
    result = report.generate()
    assert result is not None

    html = result.read_text(encoding="utf-8")
    assert "anime.score" not in html, "anime.score must not appear in generated HTML"


# ---------------------------------------------------------------------------
# Lint vocab tests
# ---------------------------------------------------------------------------


def _lint_vocab_violations(path: Path) -> list[str]:
    """Run lint_vocab on a file and return list of violation lines."""
    import sys

    lint_vocab_module = (
        Path(__file__).parents[2] / "scripts" / "report_generators" / "lint_vocab.py"
    )
    if str(lint_vocab_module.parent) not in sys.path:
        sys.path.insert(0, str(lint_vocab_module.parent))

    from scripts.report_generators.lint_vocab import (
        load_vocab,
        load_replacements,
        load_exceptions,
        _compile_patterns,
        lint_file,
        _is_definitional,
        _is_excepted,
    )

    terms = load_vocab()
    replacements = load_replacements()
    exceptions = load_exceptions()
    patterns = _compile_patterns(terms)
    findings = lint_file(path, patterns, replacements)
    real_findings = [
        f
        for f in findings
        if not _is_definitional(f) and not _is_excepted(f, exceptions)
    ]
    return [f.format() for f in real_findings]


def test_lint_vocab_report() -> None:
    """career_visibility_warning.py must not contain forbidden vocabulary."""
    violations = _lint_vocab_violations(_REPORT_SRC)
    assert not violations, (
        "Forbidden vocabulary found in career_visibility_warning.py:\n"
        + "\n".join(violations)
    )


def test_lint_vocab_analysis() -> None:
    """visibility_loss.py must not contain forbidden vocabulary."""
    violations = _lint_vocab_violations(_ANALYSIS_SRC)
    assert not violations, (
        "Forbidden vocabulary found in visibility_loss.py:\n"
        + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# Method gate: AUC gate / subgroup diff gate / CI declarations in source
# ---------------------------------------------------------------------------


def test_auc_gate_declared() -> None:
    """Report source must declare AUC gate value."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "_AUC_GATE" in text, "_AUC_GATE must be declared in career_visibility_warning.py"
    assert "0.65" in text, "AUC gate value 0.65 must appear in source"


def test_subgroup_diff_gate_declared() -> None:
    """Report source must declare subgroup AUC diff gate."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "_SUBGROUP_DIFF_GATE" in text or "subgroup_max_diff" in text, (
        "Subgroup diff gate must be declared in career_visibility_warning.py"
    )


def test_holdout_method_declared_in_spec() -> None:
    """SPEC must declare holdout method."""
    from scripts.report_generators.reports.career_visibility_warning import SPEC

    assert SPEC.method_gate.holdout is not None, "SPEC must declare holdout"
    assert SPEC.method_gate.holdout.method == "time-split", (
        "Holdout method must be time-split"
    )


def test_null_model_declared_in_spec() -> None:
    """SPEC must declare at least one null model."""
    from scripts.report_generators.reports.career_visibility_warning import SPEC

    assert len(SPEC.null_model) >= 1, "SPEC must declare at least one null model"


def test_ci_method_declared_in_spec() -> None:
    """SPEC must declare a CI method."""
    from scripts.report_generators.reports.career_visibility_warning import SPEC

    assert SPEC.method_gate.ci.estimator, "SPEC must declare a CI estimator"


# ---------------------------------------------------------------------------
# Source file existence + anime.score guard
# ---------------------------------------------------------------------------


def test_report_src_exists() -> None:
    """career_visibility_warning.py source file must exist."""
    assert _REPORT_SRC.exists(), f"Report source not found: {_REPORT_SRC}"


def test_analysis_src_exists() -> None:
    """visibility_loss.py source file must exist."""
    assert _ANALYSIS_SRC.exists(), f"Analysis source not found: {_ANALYSIS_SRC}"


def test_no_anime_score_in_report_src() -> None:
    """career_visibility_warning.py must not reference anime.score."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text, (
        "anime.score must not appear in career_visibility_warning.py"
    )


def test_no_anime_score_in_analysis_src() -> None:
    """visibility_loss.py must not reference anime.score."""
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text, (
        "anime.score must not appear in visibility_loss.py"
    )


# ---------------------------------------------------------------------------
# Registration in V2_REPORT_CLASSES
# ---------------------------------------------------------------------------


def test_report_registered_in_v2_classes() -> None:
    """CareerVisibilityWarningReport must be in V2_REPORT_CLASSES."""
    from scripts.report_generators.reports import V2_REPORT_CLASSES
    from scripts.report_generators.reports.career_visibility_warning import (
        CareerVisibilityWarningReport,
    )

    assert CareerVisibilityWarningReport in V2_REPORT_CLASSES, (
        "CareerVisibilityWarningReport must be in V2_REPORT_CLASSES"
    )


def test_report_name_and_filename() -> None:
    """CareerVisibilityWarningReport must have correct name and filename."""
    from scripts.report_generators.reports.career_visibility_warning import (
        CareerVisibilityWarningReport,
    )

    assert CareerVisibilityWarningReport.name == "career_visibility_warning"
    assert CareerVisibilityWarningReport.filename == "career_visibility_warning.html"


def test_report_doc_type_is_brief() -> None:
    """doc_type must be 'brief' for HR brief section."""
    from scripts.report_generators.reports.career_visibility_warning import (
        CareerVisibilityWarningReport,
    )

    assert CareerVisibilityWarningReport.doc_type == "brief"
