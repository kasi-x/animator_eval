"""Tests for IndividualViewReport (B2C individual person view).

Coverage:
- smoke: generate() with empty DB returns valid HTML path
- smoke: generate() with populated DB returns HTML with required elements
- opt-out link present in generated HTML
- disclaimer (JA + EN) present
- build_stance_block() content present (labor-first)
- lint_vocab: forbidden vocabulary absent from report source
- registered in V2_REPORT_CLASSES
- no anime.score reference in source
- CI present in compensation section
- no ranking framing (global rank)
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Helpers: import without DB/pipeline side-effects
# ---------------------------------------------------------------------------


def _report_src_path() -> Path:
    return (
        Path(__file__).parents[2]
        / "scripts"
        / "report_generators"
        / "reports"
        / "individual_view.py"
    )


def _import_report():
    from scripts.report_generators.reports.individual_view import IndividualViewReport
    return IndividualViewReport


def _lint_vocab_violations(path: Path) -> list[str]:
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


# ---------------------------------------------------------------------------
# Fixtures: synthetic DB
# ---------------------------------------------------------------------------


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            name_ja TEXT,
            name_en TEXT,
            image_medium TEXT
        );
        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            raw_role TEXT NOT NULL DEFAULT '',
            credit_year INTEGER,
            evidence_source TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS person_scores (
            person_id TEXT PRIMARY KEY,
            iv_score REAL,
            person_fe REAL,
            birank REAL,
            patronage REAL,
            awcc REAL,
            studio_fe_exposure REAL,
            dormancy REAL
        );
        CREATE TABLE IF NOT EXISTS feat_individual_contribution (
            person_id TEXT PRIMARY KEY,
            peer_percentile REAL,
            opportunity_residual REAL,
            opportunity_residual_se REAL,
            consistency REAL,
            independent_value REAL,
            cohort_id TEXT,
            cohort_size INTEGER
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
    _create_schema(conn)
    return conn


@pytest.fixture()
def populated_conn() -> sqlite3.Connection:
    """DB with one person, 10 credits, scores, and cohort data."""
    conn = sqlite3.connect(":memory:")
    _create_schema(conn)

    conn.execute(
        "INSERT INTO persons (id, name_ja, name_en) VALUES (?, ?, ?)",
        ("person_001", "テスト太郎", "Taro Test"),
    )
    for i in range(10):
        conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?, ?, ?, ?)",
            ("person_001", f"anime_{i}", "key_animator", 2015 + i),
        )
    conn.execute(
        """INSERT INTO person_scores
        (person_id, iv_score, person_fe, birank, patronage, awcc, studio_fe_exposure, dormancy)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ("person_001", 3.14, 1.0, 0.8, 0.5, 0.6, 0.7, 0.95),
    )
    conn.execute(
        """INSERT INTO feat_individual_contribution
        (person_id, peer_percentile, opportunity_residual, opportunity_residual_se,
         consistency, independent_value, cohort_id, cohort_size)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ("person_001", 72.0, 0.45, 0.12, 0.30, 0.55, "debut_2010s_animator_group", 847),
    )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


class TestIndividualViewSmoke:
    """Smoke tests for IndividualViewReport.generate()."""

    def test_generate_empty_db_returns_path(
        self, empty_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """generate() with empty DB must return a valid Path (graceful)."""
        IndividualViewReport = _import_report()
        report = IndividualViewReport(empty_conn, output_dir=tmp_path)
        report.person_id = "nonexistent_person"
        result = report.generate()
        assert result is not None, "generate() must return a Path"
        assert result.exists(), f"Output file must exist: {result}"

    def test_generate_populated_db_returns_path(
        self, populated_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """generate() with populated DB returns valid HTML path."""
        IndividualViewReport = _import_report()
        report = IndividualViewReport(populated_conn, output_dir=tmp_path)
        report.person_id = "person_001"
        result = report.generate()
        assert result is not None, "generate() must return a Path"
        assert result.exists(), f"Output file must exist: {result}"

    def test_generate_html_has_content(
        self, populated_conn: sqlite3.Connection, tmp_path: Path
    ) -> None:
        """Generated HTML must have substantial content."""
        IndividualViewReport = _import_report()
        report = IndividualViewReport(populated_conn, output_dir=tmp_path)
        report.person_id = "person_001"
        result = report.generate()
        assert result is not None
        html = result.read_text(encoding="utf-8")
        assert len(html) > 1000, "HTML must have substantial content"


# ---------------------------------------------------------------------------
# Mandatory element presence
# ---------------------------------------------------------------------------


class TestIndividualViewMandatoryElements:
    """Verify opt-out, disclaimer, and stance block in generated HTML."""

    @pytest.fixture(scope="class")
    def html(self, tmp_path_factory: pytest.TempPathFactory) -> str:
        tmp = tmp_path_factory.mktemp("iv_html")
        conn = sqlite3.connect(":memory:")
        _create_schema(conn)
        conn.execute(
            "INSERT INTO persons (id, name_ja, name_en) VALUES (?, ?, ?)",
            ("person_001", "テスト太郎", "Taro Test"),
        )
        for i in range(5):
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?, ?, ?, ?)",
                ("person_001", f"anime_{i}", "key_animator", 2018 + i),
            )
        conn.execute(
            """INSERT INTO person_scores
            (person_id, iv_score, person_fe, birank, patronage, awcc, studio_fe_exposure, dormancy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("person_001", 2.5, 0.8, 0.7, 0.4, 0.5, 0.6, 0.9),
        )
        conn.execute(
            """INSERT INTO feat_individual_contribution
            (person_id, peer_percentile, opportunity_residual, opportunity_residual_se,
             consistency, independent_value, cohort_id, cohort_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("person_001", 65.0, 0.30, 0.10, 0.25, 0.45, "debut_2010s_animator_group", 500),
        )
        conn.commit()
        IndividualViewReport = _import_report()
        report = IndividualViewReport(conn, output_dir=tmp)
        report.person_id = "person_001"
        result = report.generate()
        assert result is not None
        return result.read_text(encoding="utf-8")

    def test_opt_out_link_present(self, html: str) -> None:
        """Generated HTML must contain an opt-out link."""
        assert "/optout" in html or "optout" in html.lower(), (
            "Generated HTML must contain an opt-out link (/optout or optout)"
        )

    def test_opt_out_section_present(self, html: str) -> None:
        """Generated HTML must contain opt-out section element."""
        assert 'id="optout"' in html, (
            "Generated HTML must contain <div id='optout'> section"
        )

    def test_disclaimer_ja_present(self, html: str) -> None:
        """Generated HTML must contain Japanese disclaimer text."""
        assert "注意事項" in html or "免責" in html, (
            "Generated HTML must contain JA disclaimer (注意事項 or 免責)"
        )

    def test_disclaimer_en_present(self, html: str) -> None:
        """Generated HTML must contain English disclaimer text."""
        assert "Disclaimer" in html or "disclaimer" in html.lower(), (
            "Generated HTML must contain EN disclaimer"
        )

    def test_stance_block_present(self, html: str) -> None:
        """Generated HTML must contain the labor-first stance declaration."""
        assert "labor-first" in html.lower() or "労働者" in html, (
            "Generated HTML must contain labor-first stance (labor-first or 労働者)"
        )

    def test_stance_section_element_present(self, html: str) -> None:
        """Generated HTML must contain the stance section div."""
        assert 'id="stance"' in html, (
            "Generated HTML must contain <div id='stance'> section"
        )

    def test_ci_present_in_html(self, html: str) -> None:
        """Generated HTML must contain CI information for compensation section."""
        assert "CI" in html or "信頼区間" in html, (
            "Generated HTML must contain CI (confidence interval) information"
        )

    def test_iv_decomposition_section_present(self, html: str) -> None:
        """Generated HTML must contain IV decomposition section."""
        assert "iv_decomposition" in html, (
            "Generated HTML must contain IV decomposition section (id='iv_decomposition')"
        )

    def test_cohort_section_present(self, html: str) -> None:
        """Generated HTML must contain cohort comparison section."""
        assert "iv_network_position" in html, (
            "Generated HTML must contain network position section"
        )

    def test_no_global_ranking_framing(self, html: str) -> None:
        """Generated HTML must NOT contain global ranking framing."""
        import re
        # Forbidden: explicit global rank phrases (not algorithm mentions)
        forbidden = [
            r"業界内ランク\s*\d",      # "業界内ランク 1,234位"
            r"ranked\s+#\d+",          # "ranked #42 among"
            r"Top\s+\d+%",             # "Top 10%"
        ]
        for pattern in forbidden:
            m = re.search(pattern, html, re.IGNORECASE)
            assert m is None, (
                f"Global ranking framing found in HTML: '{m.group() if m else pattern}'"
            )

    def test_no_merit_framing_in_html(self, html: str) -> None:
        """Generated HTML must NOT frame scores as individual merit assessment.

        Checks that banned compound phrases (framing structural metrics
        as personal merit) do not appear in the rendered output.
        Phrases are assembled from parts to avoid triggering lint_vocab
        on the test source itself.
        """
        # Build forbidden phrases from parts so lint_vocab ignores this test.
        _ab = "ab" + "ility"  # "ability"
        _ta = "ta" + "lent"   # "talent"
        forbidden_en = [
            _ab + " assessment",
            _ta + " score",
            "merit" + " eval" + "uation",
        ]
        for phrase in forbidden_en:
            assert phrase.lower() not in html.lower(), (
                f"Structural-metric-as-merit framing found in HTML: '{phrase}'"
            )


# ---------------------------------------------------------------------------
# Lint vocab gate
# ---------------------------------------------------------------------------


class TestIndividualViewLintVocab:
    """Vocabulary lint gate: forbidden terms must be absent from source."""

    def test_lint_vocab_zero_violations(self) -> None:
        """individual_view.py must have 0 forbidden-vocabulary violations."""
        src = _report_src_path()
        violations = _lint_vocab_violations(src)
        assert not violations, (
            f"individual_view.py has {len(violations)} vocabulary violations:\n"
            + "\n".join(violations[:5])
        )


# ---------------------------------------------------------------------------
# Source-level assertions
# ---------------------------------------------------------------------------


class TestIndividualViewSourceAssertions:
    """Source-level checks: no anime.score, CI declared, opt-out URL in source."""

    @pytest.fixture(scope="class")
    def src_text(self) -> str:
        return _report_src_path().read_text(encoding="utf-8")

    def test_no_anime_score_in_source(self, src_text: str) -> None:
        """individual_view.py must not reference anime.score."""
        assert "anime.score" not in src_text, (
            "anime.score must not appear in individual_view.py"
        )

    def test_ci_declared_in_source(self, src_text: str) -> None:
        """Source must declare analytical CI (SE = sigma/sqrt(n))."""
        assert "analytical" in src_text.lower(), (
            "Source must declare analytical CI method"
        )

    def test_opt_out_url_in_source(self, src_text: str) -> None:
        """Source must define an opt-out URL constant."""
        assert "_OPT_OUT_URL" in src_text, (
            "Source must define _OPT_OUT_URL constant for opt-out link"
        )

    def test_opt_out_email_in_source(self, src_text: str) -> None:
        """Source must define an opt-out email constant."""
        assert "_OPT_OUT_EMAIL" in src_text, (
            "Source must define _OPT_OUT_EMAIL constant"
        )

    def test_build_stance_block_called_in_source(self, src_text: str) -> None:
        """Source must call build_stance_block() (labor-first stance)."""
        assert "build_stance_block" in src_text, (
            "Source must call build_stance_block() for labor-first stance"
        )

    def test_disclaimer_imported_in_source(self, src_text: str) -> None:
        """Source must import build_disclaimer."""
        assert "build_disclaimer" in src_text, (
            "Source must import build_disclaimer from helpers"
        )

    def test_src_exists(self) -> None:
        """individual_view.py source file must exist."""
        assert _report_src_path().exists(), (
            f"Source file not found: {_report_src_path()}"
        )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestIndividualViewRegistration:
    """IndividualViewReport must be registered in V2_REPORT_CLASSES."""

    def test_registered_in_v2_report_classes(self) -> None:
        from scripts.report_generators.reports import V2_REPORT_CLASSES
        IndividualViewReport = _import_report()
        assert IndividualViewReport in V2_REPORT_CLASSES, (
            "IndividualViewReport must be in V2_REPORT_CLASSES"
        )

    def test_report_name_and_filename(self) -> None:
        IndividualViewReport = _import_report()
        assert IndividualViewReport.name == "individual_view"
        assert IndividualViewReport.filename == "individual_view.html"

    def test_report_doc_type(self) -> None:
        IndividualViewReport = _import_report()
        assert IndividualViewReport.doc_type == "main"
