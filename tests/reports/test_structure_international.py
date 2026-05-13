"""Tests for scripts.report_generators.reports.structure_international.

Coverage:
- smoke: StructureInternationalReport.generate() with synthetic in-memory DB
- empty DB: degrades gracefully
- lineage: meta_lineage row has ci_method and null_model
- HTML: contains disclaimer
- lint_vocab: no forbidden vocabulary in report source and analysis source
- method gate: CI + null_model + no anime.score declared
- invariants: report registered in V2_REPORT_CLASSES, correct name/filename
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

# Source paths for lint checks
_REPORT_SRC = (
    Path(__file__).parents[2]
    / "scripts"
    / "report_generators"
    / "reports"
    / "structure_international.py"
)
_ANALYSIS_SRC = (
    Path(__file__).parents[2]
    / "src"
    / "analysis"
    / "network"
    / "international_collab.py"
)

# Forbidden pattern (subset matched directly in tests; full lint_vocab used in dedicated tests)
_FORBIDDEN_PATTERN = re.compile(
    r"\b(ability|skill|competence|capability)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Synthetic DB fixture
# ---------------------------------------------------------------------------


def _build_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            name_en TEXT NOT NULL DEFAULT '',
            name_zh TEXT,
            name_ko TEXT,
            country_of_origin TEXT
        );
        CREATE TABLE IF NOT EXISTS anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            studio_id TEXT,
            year INTEGER,
            quarter INTEGER,
            episodes INTEGER DEFAULT 12,
            duration INTEGER DEFAULT 24
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


@pytest.fixture()
def silver_conn() -> sqlite3.Connection:
    """In-memory DB: 200 JP, 50 CN, 30 KR, 20 SE_ASIA, 20 unknown persons."""
    conn = sqlite3.connect(":memory:")
    _build_schema(conn)

    for i in range(30):
        conn.execute(
            "INSERT INTO anime (id, title_ja, studio_id, year) VALUES (?,?,?,?)",
            (f"a{i}", f"Anime{i}", f"studio_{i % 3}", 2000 + i % 20),
        )

    configs = [
        ("JP", 200, None, None),
        ("CN", 50, None, None),
        ("KR", 30, None, None),
        ("TH", 20, None, None),
        (None, 20, None, None),
    ]
    p_idx = 0
    for country, count, zh, ko in configs:
        for _ in range(count):
            conn.execute(
                "INSERT INTO persons (id, name_en, country_of_origin, name_zh, name_ko) "
                "VALUES (?,?,?,?,?)",
                (f"p{p_idx}", f"Person{p_idx}", country, zh, ko),
            )
            role = "in_between" if p_idx % 3 == 0 else "key_animator"
            anime_id = f"a{p_idx % 30}"
            credit_year = 2000 + (p_idx % 20)
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (f"p{p_idx}", anime_id, role, credit_year),
            )
            p_idx += 1

    conn.commit()
    return conn


@pytest.fixture()
def empty_conn() -> sqlite3.Connection:
    """Minimal schema with no data rows."""
    conn = sqlite3.connect(":memory:")
    _build_schema(conn)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


def test_generate_returns_path(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """StructureInternationalReport.generate() returns a valid HTML path."""
    from scripts.report_generators.reports.structure_international import (
        StructureInternationalReport,
    )

    report = StructureInternationalReport(silver_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None, "generate() must return a Path, not None"
    assert result.exists(), f"Output file must exist: {result}"
    html = result.read_text(encoding="utf-8")
    assert "国際" in html or "international" in html.lower(), (
        "HTML should contain international-related content"
    )


def test_generate_empty_db(empty_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """StructureInternationalReport.generate() degrades gracefully with empty DB."""
    from scripts.report_generators.reports.structure_international import (
        StructureInternationalReport,
    )

    report = StructureInternationalReport(empty_conn, output_dir=tmp_path)
    result = report.generate()
    assert result is not None, "generate() must return a path even with empty data"
    assert result.exists()


def test_generate_creates_lineage(
    silver_conn: sqlite3.Connection, tmp_path: Path
) -> None:
    """insert_lineage must populate meta_lineage with ci_method and null_model."""
    from scripts.report_generators.reports.structure_international import (
        StructureInternationalReport,
    )

    report = StructureInternationalReport(silver_conn, output_dir=tmp_path)
    report.generate()

    row = silver_conn.execute(
        "SELECT ci_method, null_model FROM meta_lineage "
        "WHERE table_name = 'meta_structure_international'"
    ).fetchone()

    assert row is not None, "meta_lineage row must be inserted"
    ci_method, null_model = row
    assert ci_method, "ci_method must be non-empty"
    assert null_model, "null_model must be non-empty"


def test_generate_html_has_disclaimer(
    silver_conn: sqlite3.Connection, tmp_path: Path
) -> None:
    """Generated HTML must contain the disclaimer (JA + EN)."""
    from scripts.report_generators.reports.structure_international import (
        StructureInternationalReport,
    )

    report = StructureInternationalReport(silver_conn, output_dir=tmp_path)
    result = report.generate()
    html = result.read_text(encoding="utf-8")
    assert "Disclaimer" in html or "免責" in html, "Report must contain a disclaimer"


# ---------------------------------------------------------------------------
# Lint vocab
# ---------------------------------------------------------------------------


def _lint_vocab_violations(path: Path) -> list[str]:
    """Run lint_vocab on a file and return violation strings."""
    import sys

    lint_path = Path(__file__).parents[2] / "scripts" / "report_generators"
    if str(lint_path) not in sys.path:
        sys.path.insert(0, str(lint_path))

    from scripts.report_generators.lint_vocab import (
        _compile_patterns,
        _is_definitional,
        _is_excepted,
        lint_file,
        load_exceptions,
        load_replacements,
        load_vocab,
    )

    terms = load_vocab()
    replacements = load_replacements()
    exceptions = load_exceptions()
    patterns = _compile_patterns(terms)
    findings = lint_file(path, patterns, replacements)
    return [
        f.format()
        for f in findings
        if not _is_definitional(f) and not _is_excepted(f, exceptions)
    ]


def test_lint_vocab_report() -> None:
    """structure_international.py must not contain forbidden vocabulary."""
    violations = _lint_vocab_violations(_REPORT_SRC)
    assert not violations, (
        "Forbidden vocabulary found in structure_international.py:\n"
        + "\n".join(violations)
    )


def test_lint_vocab_analysis() -> None:
    """international_collab.py must not contain forbidden vocabulary."""
    violations = _lint_vocab_violations(_ANALYSIS_SRC)
    assert not violations, (
        "Forbidden vocabulary found in international_collab.py:\n"
        + "\n".join(violations)
    )


def test_no_anime_score_in_report() -> None:
    """structure_international.py must not reference anime.score."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text


def test_no_anime_score_in_analysis() -> None:
    """international_collab.py must not reference anime.score."""
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text


def test_no_ability_framing_in_report() -> None:
    """structure_international.py must not use ability framing."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    matches = _FORBIDDEN_PATTERN.findall(text)
    assert not matches, (
        f"Forbidden vocabulary in structure_international.py: {matches}"
    )


def test_no_ability_framing_in_analysis() -> None:
    """international_collab.py must not use ability framing."""
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    matches = _FORBIDDEN_PATTERN.findall(text)
    assert not matches, (
        f"Forbidden vocabulary in international_collab.py: {matches}"
    )


# ---------------------------------------------------------------------------
# Method gate checks
# ---------------------------------------------------------------------------


def test_method_gate_ci_present() -> None:
    """Report source must declare a CI method."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "ci_method" in text, "ci_method must be declared in insert_lineage call"
    assert "95%" in text or "wilson" in text.lower() or "analytical" in text.lower(), (
        "CI method must mention 95%, wilson, or analytical"
    )


def test_method_gate_null_model_present() -> None:
    """Report source must declare a null model."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "null_model" in text, "null_model must be declared in insert_lineage call"
    assert "permutation" in text.lower(), (
        "null_model must mention permutation test"
    )


def test_method_gate_no_framing_violation() -> None:
    """Report source must not use 空洞化 or 下請け framing."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "空洞化" not in text, "空洞化 framing is prohibited (card §Hard constraints)"
    assert "下請け" not in text, "下請け framing is prohibited (card §Hard constraints)"


# ---------------------------------------------------------------------------
# Invariant checks
# ---------------------------------------------------------------------------


def test_report_src_exists() -> None:
    """structure_international.py source file must exist."""
    assert _REPORT_SRC.exists(), f"Report source not found: {_REPORT_SRC}"


def test_analysis_src_exists() -> None:
    """international_collab.py source file must exist."""
    assert _ANALYSIS_SRC.exists(), f"Analysis source not found: {_ANALYSIS_SRC}"


def test_report_registered_in_init() -> None:
    """StructureInternationalReport must be in V2_REPORT_CLASSES."""
    from scripts.report_generators.reports import V2_REPORT_CLASSES
    from scripts.report_generators.reports.structure_international import (
        StructureInternationalReport,
    )

    assert StructureInternationalReport in V2_REPORT_CLASSES, (
        "StructureInternationalReport must be in V2_REPORT_CLASSES"
    )


def test_report_name_and_filename() -> None:
    """StructureInternationalReport must have correct name and filename."""
    from scripts.report_generators.reports.structure_international import (
        StructureInternationalReport,
    )

    assert StructureInternationalReport.name == "structure_international"
    assert StructureInternationalReport.filename == "structure_international.html"
