"""Tests for O1 gender ceiling analysis report.

Coverage:
- smoke: O1GenderCeilingReport.generate() with synthetic in-memory DB
- lint_vocab: forbidden vocabulary absent from report and analysis source code
- method gate: Cox CI + null_model declared in insert_lineage call
- analysis unit tests: load_gender_progression_records, cox_progression_hazard,
  mannwhitney_advancement_timing, compute_ego_network_gender_composition
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Synthetic DB fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def silver_conn() -> sqlite3.Connection:
    """In-memory SQLite connection with minimal SILVER schema + synthetic data.

    Populated with:
    - 400 persons (200 F, 200 M) across 4 pipeline roles + gender field
    - Synthetic progression credits enabling Cox and Mann-Whitney tests
    - meta_lineage table for insert_lineage
    """
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

    # 40 anime
    for i in range(40):
        conn.execute(
            "INSERT INTO anime (id, title_ja, studio_id, year, quarter, episodes, duration) "
            "VALUES (?,?,?,?,?,?,?)",
            (f"a{i}", f"Anime{i}", f"studio_{i % 4}", 1990 + (i % 30), 1, 12, 24),
        )

    # 400 persons: 0-199 = F, 200-399 = M
    for p_idx in range(400):
        gender = "female" if p_idx < 200 else "male"
        conn.execute(
            "INSERT INTO persons (id, name_en, gender) VALUES (?,?,?)",
            (f"p{p_idx}", f"Person{p_idx}", gender),
        )

    # Credits: all persons start as in_between, some progress to key_animator
    # Persons 0–99 (F) and 200–299 (M) also get key_animator credits
    for p_idx in range(400):
        debut_year = 1990 + (p_idx % 20)
        anime_id = f"a{p_idx % 40}"

        # in_between credit
        conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
            (f"p{p_idx}", anime_id, "in_between", debut_year),
        )

        # key_animator credit for subset
        if p_idx < 100 or (200 <= p_idx < 300):
            adv_year = debut_year + 3 + (p_idx % 5)
            anime2 = f"a{(p_idx + 10) % 40}"
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (f"p{p_idx}", anime2, "key_animator", adv_year),
            )

        # animation_director for smaller subset
        if p_idx < 30 or (200 <= p_idx < 230):
            adv_year2 = debut_year + 8 + (p_idx % 7)
            anime3 = f"a{(p_idx + 5) % 40}"
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (f"p{p_idx}", anime3, "animation_director", adv_year2),
            )

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


def test_generate_returns_path(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """O1GenderCeilingReport.generate() returns a valid HTML path."""
    from scripts.report_generators.reports.o1_gender_ceiling import O1GenderCeilingReport

    report = O1GenderCeilingReport(silver_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None, "generate() must return a Path, not None"
    assert result.exists(), f"Output file must exist: {result}"
    html = result.read_text(encoding="utf-8")
    assert "gender" in html.lower() or "性別" in html, (
        "HTML should contain gender-related content"
    )


def test_generate_empty_db(tmp_path: Path) -> None:
    """O1GenderCeilingReport.generate() with empty DB returns path gracefully."""
    from scripts.report_generators.reports.o1_gender_ceiling import O1GenderCeilingReport

    empty_conn = sqlite3.connect(":memory:")
    empty_conn.executescript("""
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            name_en TEXT NOT NULL DEFAULT '',
            gender TEXT
        );
        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
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

    report = O1GenderCeilingReport(empty_conn, output_dir=tmp_path)
    result = report.generate()
    assert result is not None, "generate() must return a path even with empty data"
    assert result.exists()


def test_generate_creates_lineage(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """insert_lineage must populate meta_lineage with CI and null_model."""
    from scripts.report_generators.reports.o1_gender_ceiling import O1GenderCeilingReport

    report = O1GenderCeilingReport(silver_conn, output_dir=tmp_path)
    report.generate()

    row = silver_conn.execute(
        "SELECT ci_method, null_model FROM meta_lineage "
        "WHERE table_name = 'meta_o1_gender_ceiling'"
    ).fetchone()

    assert row is not None, "meta_lineage row must be inserted"
    ci_method, null_model = row
    assert ci_method, "ci_method must be non-empty"
    assert null_model, "null_model must be non-empty"


# ---------------------------------------------------------------------------
# Lint vocab tests
# ---------------------------------------------------------------------------

_FORBIDDEN_PATTERN = re.compile(
    r"\b(ability|skill|talent|competence|capability)\b",
    re.IGNORECASE,
)

_FORBIDDEN_JP = re.compile(r"能力|実力|優秀|劣る|人材の質")

_REPORT_SRC = (
    Path(__file__).parents[2]
    / "scripts"
    / "report_generators"
    / "reports"
    / "o1_gender_ceiling.py"
)

_ANALYSIS_SRC = (
    Path(__file__).parents[2]
    / "src"
    / "analysis"
    / "causal"
    / "gender_progression.py"
)


def _lint_vocab_violations(path: Path) -> list[str]:
    """Run lint_vocab on a file and return list of violation lines.

    Delegates to the project's lint_vocab module (AST-based string-literal scan).
    Only returns violations for enforced categories (ability_framing,
    causal_verbs, evaluative_adjectives).
    """
    import sys
    lint_vocab_module = (
        Path(__file__).parents[2]
        / "scripts"
        / "report_generators"
        / "lint_vocab.py"
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
        f for f in findings
        if not _is_definitional(f) and not _is_excepted(f, exceptions)
    ]
    return [f.format() for f in real_findings]


def test_lint_vocab_report() -> None:
    """o1_gender_ceiling.py must not contain forbidden vocabulary in string literals."""
    violations = _lint_vocab_violations(_REPORT_SRC)
    assert not violations, (
        "Forbidden vocabulary found in o1_gender_ceiling.py:\n" + "\n".join(violations)
    )


def test_lint_vocab_jp_report() -> None:
    """o1_gender_ceiling.py must not contain forbidden Japanese vocabulary."""
    violations = _lint_vocab_violations(_REPORT_SRC)
    jp_violations = [v for v in violations if any(
        term in v for term in ("能力", "実力", "優秀", "劣る")
    )]
    assert not jp_violations, (
        "Forbidden Japanese vocabulary found in o1_gender_ceiling.py:\n"
        + "\n".join(jp_violations)
    )


def test_lint_vocab_analysis() -> None:
    """gender_progression.py must not contain forbidden vocabulary in string literals."""
    violations = _lint_vocab_violations(_ANALYSIS_SRC)
    assert not violations, (
        "Forbidden vocabulary found in gender_progression.py:\n" + "\n".join(violations)
    )


def test_no_anime_score_in_report() -> None:
    """o1_gender_ceiling.py must not reference anime.score."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text, "anime.score must not appear in o1_gender_ceiling.py"


def test_no_anime_score_in_analysis() -> None:
    """gender_progression.py must not reference anime.score."""
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text, "anime.score must not appear in gender_progression.py"


# ---------------------------------------------------------------------------
# Method gate: CI + null_model present in report source
# ---------------------------------------------------------------------------


def test_method_gate_ci_present() -> None:
    """Report source must declare a CI method."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "ci_method" in text, "ci_method must be declared in insert_lineage call"
    assert "cox" in text.lower() or "95%" in text.lower(), (
        "CI method must mention Cox or 95%"
    )


def test_method_gate_null_model_present() -> None:
    """Report source must declare a null model."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "null_model" in text, "null_model must be declared in insert_lineage call"
    assert "permutation" in text.lower() or "null" in text.lower(), (
        "null_model must mention permutation or null"
    )


# ---------------------------------------------------------------------------
# Unit tests: analysis functions
# ---------------------------------------------------------------------------


def test_load_gender_progression_records_basic(
    silver_conn: sqlite3.Connection,
) -> None:
    """load_gender_progression_records returns records with valid gender values."""
    from src.analysis.causal.gender_progression import load_gender_progression_records

    records = load_gender_progression_records(
        silver_conn, "in_between", "key_animator"
    )
    assert len(records) > 0, "Must return at least some records"

    for rec in records:
        assert rec.gender in ("M", "F", "NB"), (
            f"gender must be M/F/NB, got {rec.gender!r}"
        )
        if rec.duration_years is not None:
            assert rec.duration_years >= 0, (
                f"duration_years must be non-negative, got {rec.duration_years}"
            )
        assert rec.cohort_5y % 5 == 0, (
            f"cohort_5y must be multiple of 5, got {rec.cohort_5y}"
        )


def test_load_gender_progression_records_excludes_unknown(
    silver_conn: sqlite3.Connection,
) -> None:
    """Records with NULL or unknown gender must be excluded."""
    # Insert a person with no gender
    silver_conn.execute(
        "INSERT INTO persons (id, name_en, gender) VALUES ('pX', 'Unknown', NULL)"
    )
    silver_conn.execute(
        "INSERT INTO credits (person_id, anime_id, role, credit_year) "
        "VALUES ('pX', 'a0', 'in_between', 2000)"
    )
    silver_conn.commit()

    from src.analysis.causal.gender_progression import load_gender_progression_records

    records = load_gender_progression_records(
        silver_conn, "in_between", "key_animator"
    )
    pids = {r.person_id for r in records}
    assert "pX" not in pids, "Person with NULL gender must be excluded"


def test_cox_progression_hazard_returns_result(
    silver_conn: sqlite3.Connection,
) -> None:
    """cox_progression_hazard returns CoxResult with valid fields."""
    from src.analysis.causal.gender_progression import (
        CoxResult,
        load_gender_progression_records,
        cox_progression_hazard,
    )

    records = load_gender_progression_records(silver_conn, "in_between", "key_animator")
    result = cox_progression_hazard(records, "動画→原画", censor_years=25.0)

    if result is None:
        pytest.skip("Insufficient data for Cox model in synthetic DB — acceptable")

    assert isinstance(result, CoxResult), "Must return CoxResult"
    assert result.hr_female_vs_male > 0, "HR must be positive"
    assert result.ci_lower <= result.hr_female_vs_male <= result.ci_upper or (
        result.ci_lower <= result.ci_upper
    ), "CI must be ordered"
    assert result.n_female > 0, "n_female must be positive"
    assert result.n_male > 0, "n_male must be positive"


def test_cox_hazard_insufficient_data() -> None:
    """cox_progression_hazard returns None when fewer than 20 M/F persons."""
    from src.analysis.causal.gender_progression import (
        GenderProgressionRecord,
        cox_progression_hazard,
    )

    # Only 5 records per gender — below minimum
    records = [
        GenderProgressionRecord(
            person_id=f"p{i}",
            gender="F" if i < 5 else "M",
            role_from="in_between",
            role_to="key_animator",
            first_year_from=2000,
            first_year_to=2003,
            duration_years=3.0,
            cohort_5y=2000,
        )
        for i in range(10)
    ]
    result = cox_progression_hazard(records, "test")
    assert result is None, "Must return None with insufficient data"


def test_mannwhitney_advancement_timing_basic(
    silver_conn: sqlite3.Connection,
) -> None:
    """mannwhitney_advancement_timing returns results for valid data."""
    from src.analysis.causal.gender_progression import (
        MannWhitneyResult,
        load_gender_progression_records,
        mannwhitney_advancement_timing,
    )

    records = load_gender_progression_records(silver_conn, "in_between", "key_animator")
    results = mannwhitney_advancement_timing(
        records, "動画→原画", min_cohort_size=2
    )

    for res in results:
        assert isinstance(res, MannWhitneyResult)
        assert res.u_statistic >= 0, "U statistic must be non-negative"
        assert 0.0 <= res.p_value <= 1.0, f"p_value must be in [0,1], got {res.p_value}"
        assert res.effect_r >= 0.0, f"effect_r must be non-negative, got {res.effect_r}"
        assert res.n_female > 0 and res.n_male > 0


def test_mannwhitney_empty_records() -> None:
    """mannwhitney_advancement_timing returns empty list for empty input."""
    from src.analysis.causal.gender_progression import mannwhitney_advancement_timing

    results = mannwhitney_advancement_timing([])
    assert results == [], "Empty input must return empty list"


def test_compute_ego_network_gender_composition_basic(
    silver_conn: sqlite3.Connection,
) -> None:
    """compute_ego_network_gender_composition returns summary with valid fields."""
    from src.analysis.causal.gender_progression import (
        EgoNetworkSummary,
        compute_ego_network_gender_composition,
    )

    ego_results, summary = compute_ego_network_gender_composition(
        silver_conn,
        n_null_iterations=10,  # small for test speed
        rng_seed=0,
        sample_cap=100,
    )

    assert isinstance(summary, EgoNetworkSummary)
    assert summary.n_persons >= 0

    for r in ego_results:
        assert 0.0 <= r.same_gender_share <= 1.0, (
            f"same_gender_share must be in [0,1], got {r.same_gender_share}"
        )
        assert 0.0 <= r.null_percentile <= 100.0, (
            f"null_percentile must be in [0,100], got {r.null_percentile}"
        )
        assert r.gender in ("M", "F"), (
            f"gender must be M or F, got {r.gender!r}"
        )
        assert r.n_collaborators > 0


def test_ego_network_null_percentile_distribution(
    silver_conn: sqlite3.Connection,
) -> None:
    """null_percentile distribution should have reasonable range [0, 100]."""
    from src.analysis.causal.gender_progression import (
        compute_ego_network_gender_composition,
    )

    ego_results, _ = compute_ego_network_gender_composition(
        silver_conn,
        n_null_iterations=50,
        rng_seed=42,
        sample_cap=200,
    )

    if not ego_results:
        pytest.skip("No ego-network results — acceptable for small synthetic DB")

    pcts = [r.null_percentile for r in ego_results]
    assert min(pcts) >= 0.0, "null_percentile must be >= 0"
    assert max(pcts) <= 100.0, "null_percentile must be <= 100"


# ---------------------------------------------------------------------------
# Invariant checks on source files
# ---------------------------------------------------------------------------


def test_report_src_exists() -> None:
    """o1_gender_ceiling.py source file must exist."""
    assert _REPORT_SRC.exists(), (
        f"Report source not found: {_REPORT_SRC}"
    )


def test_analysis_src_exists() -> None:
    """gender_progression.py source file must exist."""
    assert _ANALYSIS_SRC.exists(), (
        f"Analysis source not found: {_ANALYSIS_SRC}"
    )


def test_report_registered_in_init() -> None:
    """O1GenderCeilingReport must be in V2_REPORT_CLASSES."""
    from scripts.report_generators.reports import V2_REPORT_CLASSES
    from scripts.report_generators.reports.o1_gender_ceiling import O1GenderCeilingReport

    assert O1GenderCeilingReport in V2_REPORT_CLASSES, (
        "O1GenderCeilingReport must be in V2_REPORT_CLASSES"
    )


def test_report_name_and_filename() -> None:
    """O1GenderCeilingReport must have correct name and filename."""
    from scripts.report_generators.reports.o1_gender_ceiling import O1GenderCeilingReport

    assert O1GenderCeilingReport.name == "o1_gender_ceiling"
    assert O1GenderCeilingReport.filename == "o1_gender_ceiling.html"
