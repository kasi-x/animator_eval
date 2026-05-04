"""Tests for O4 海外人材ポジション分析 report.

Coverage:
- smoke: O4ForeignTalentReport.generate() with synthetic in-memory DB
- lint_vocab: forbidden vocabulary absent from report and analysis source code
- method gate: CI + null_model declared in insert_lineage call
- analysis unit tests: resolve_nationality, load_nationality_records,
  NationalitySummary, person_fe_by_nationality, studio_foreign_share
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Synthetic DB fixtures
# ---------------------------------------------------------------------------


def _build_base_schema(conn: sqlite3.Connection) -> None:
    """Create minimal SILVER schema tables."""
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
    """In-memory SILVER SQLite with nationality + credit data.

    Persons:
    - 300 JP persons (country_of_origin = 'JP')
    - 60 CN persons (country_of_origin = 'CN')
    - 40 KR persons (country_of_origin = 'KR')
    - 20 SE_ASIA persons (country_of_origin = 'TH')
    - 20 unknown (no country_of_origin, no name_zh/ko)

    Credits: all persons have in_between credits; subset have key_animator.
    Anime: 20 anime across 4 studios.
    """
    conn = sqlite3.connect(":memory:")
    _build_base_schema(conn)

    # Anime
    for i in range(20):
        conn.execute(
            "INSERT INTO anime (id, title_ja, studio_id, year, episodes, duration) "
            "VALUES (?,?,?,?,?,?)",
            (f"a{i}", f"Anime{i}", f"studio_{i % 4}", 2000 + (i % 20), 12, 24),
        )

    # Persons
    configs = [
        ("JP", 300, None, None),
        ("CN", 60, None, None),
        ("KR", 40, None, None),
        ("TH", 20, None, None),   # SE_ASIA
        (None, 20, None, None),   # unknown
    ]
    p_idx = 0
    for country, count, zh, ko in configs:
        for _ in range(count):
            conn.execute(
                "INSERT INTO persons (id, name_en, country_of_origin, name_zh, name_ko) "
                "VALUES (?,?,?,?,?)",
                (f"p{p_idx}", f"Person{p_idx}", country, zh, ko),
            )
            # in_between credit
            debut = 1995 + (p_idx % 20)
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (f"p{p_idx}", f"a{p_idx % 20}", "in_between", debut),
            )
            # Some get key_animator
            if p_idx % 3 == 0:
                conn.execute(
                    "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                    (f"p{p_idx}", f"a{(p_idx + 5) % 20}", "key_animator", debut + 4),
                )
            p_idx += 1

    conn.commit()
    return conn


@pytest.fixture()
def empty_conn() -> sqlite3.Connection:
    """Minimal schema, no data rows."""
    conn = sqlite3.connect(":memory:")
    _build_base_schema(conn)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


def test_generate_returns_path(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """O4ForeignTalentReport.generate() returns a valid HTML path."""
    from scripts.report_generators.reports.o4_foreign_talent import O4ForeignTalentReport

    report = O4ForeignTalentReport(silver_conn, output_dir=tmp_path)
    result = report.generate()

    assert result is not None, "generate() must return a Path, not None"
    assert result.exists(), f"Output file must exist: {result}"
    html = result.read_text(encoding="utf-8")
    assert "国籍" in html or "foreign" in html.lower(), (
        "HTML should contain nationality-related content"
    )


def test_generate_empty_db(empty_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """O4ForeignTalentReport.generate() with empty DB returns path gracefully."""
    from scripts.report_generators.reports.o4_foreign_talent import O4ForeignTalentReport

    report = O4ForeignTalentReport(empty_conn, output_dir=tmp_path)
    result = report.generate()
    assert result is not None, "generate() must return a path even with empty data"
    assert result.exists()


def test_generate_creates_lineage(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """insert_lineage must populate meta_lineage with CI and null_model."""
    from scripts.report_generators.reports.o4_foreign_talent import O4ForeignTalentReport

    report = O4ForeignTalentReport(silver_conn, output_dir=tmp_path)
    report.generate()

    row = silver_conn.execute(
        "SELECT ci_method, null_model FROM meta_lineage "
        "WHERE table_name = 'meta_o4_foreign_talent'"
    ).fetchone()

    assert row is not None, "meta_lineage row must be inserted"
    ci_method, null_model = row
    assert ci_method, "ci_method must be non-empty"
    assert null_model, "null_model must be non-empty"


def test_generate_html_has_disclaimer(silver_conn: sqlite3.Connection, tmp_path: Path) -> None:
    """Generated HTML must contain the disclaimer (JA + EN)."""
    from scripts.report_generators.reports.o4_foreign_talent import O4ForeignTalentReport

    report = O4ForeignTalentReport(silver_conn, output_dir=tmp_path)
    result = report.generate()
    html = result.read_text(encoding="utf-8")
    assert "Disclaimer" in html or "免責" in html, (
        "Report must contain a disclaimer"
    )


# ---------------------------------------------------------------------------
# Lint vocab tests
# ---------------------------------------------------------------------------

_FORBIDDEN_PATTERN = re.compile(
    r"\b(ability|skill|competence|capability)\b",
    re.IGNORECASE,
)

_REPORT_SRC = (
    Path(__file__).parents[2]
    / "scripts"
    / "report_generators"
    / "reports"
    / "o4_foreign_talent.py"
)

_ANALYSIS_SRC = (
    Path(__file__).parents[2]
    / "src"
    / "analysis"
    / "network"
    / "nationality_resolver.py"
)


def _lint_vocab_violations(path: Path) -> list[str]:
    """Run lint_vocab on a file and return list of violation lines."""
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
        load_replacements,
        load_vocab,
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
    """o4_foreign_talent.py must not contain forbidden vocabulary in string literals."""
    violations = _lint_vocab_violations(_REPORT_SRC)
    assert not violations, (
        "Forbidden vocabulary found in o4_foreign_talent.py:\n" + "\n".join(violations)
    )


def test_lint_vocab_analysis() -> None:
    """nationality_resolver.py must not contain forbidden vocabulary."""
    violations = _lint_vocab_violations(_ANALYSIS_SRC)
    assert not violations, (
        "Forbidden vocabulary found in nationality_resolver.py:\n" + "\n".join(violations)
    )


def test_no_anime_score_in_report() -> None:
    """o4_foreign_talent.py must not reference anime.score."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text, "anime.score must not appear in o4_foreign_talent.py"


def test_no_anime_score_in_analysis() -> None:
    """nationality_resolver.py must not reference anime.score."""
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text, "anime.score must not appear in nationality_resolver.py"


def test_no_ability_framing_in_report() -> None:
    """o4_foreign_talent.py must not use ability framing."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    matches = _FORBIDDEN_PATTERN.findall(text)
    assert not matches, f"Forbidden vocabulary in o4_foreign_talent.py: {matches}"


# ---------------------------------------------------------------------------
# Method gate
# ---------------------------------------------------------------------------


def test_method_gate_ci_present() -> None:
    """Report source must declare a CI method."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "ci_method" in text, "ci_method must be declared in insert_lineage call"
    assert "95%" in text or "bootstrap" in text.lower() or "analytical" in text.lower(), (
        "CI method must mention 95% or bootstrap or analytical"
    )


def test_method_gate_null_model_present() -> None:
    """Report source must declare a null model."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "null_model" in text, "null_model must be declared in insert_lineage call"
    assert "mann-whitney" in text.lower() or "log-rank" in text.lower(), (
        "null_model must mention Mann-Whitney or log-rank"
    )


def test_method_gate_limited_mobility_bias() -> None:
    """Report source must mention limited mobility bias."""
    text = _REPORT_SRC.read_text(encoding="utf-8")
    assert "limited mobility" in text.lower() or "andrews" in text.lower(), (
        "Report must mention limited mobility bias (Andrews et al. 2008)"
    )


# ---------------------------------------------------------------------------
# Unit tests: nationality_resolver
# ---------------------------------------------------------------------------


def test_resolve_nationality_high_confidence() -> None:
    """resolve_nationality returns high confidence for country_of_origin."""
    from src.analysis.network.nationality_resolver import (
        CONF_HIGH,
        GROUP_CN,
        GROUP_DOMESTIC,
        GROUP_KR,
        GROUP_SE_ASIA,
        resolve_nationality,
    )

    jp = resolve_nationality("p1", "JP", None, None)
    assert jp.group == GROUP_DOMESTIC
    assert jp.confidence == CONF_HIGH

    cn = resolve_nationality("p2", "CN", None, None)
    assert cn.group == GROUP_CN
    assert cn.confidence == CONF_HIGH

    kr = resolve_nationality("p3", "KR", "홍길동", None)
    assert kr.group == GROUP_KR
    assert kr.confidence == CONF_HIGH  # country_of_origin takes priority

    th = resolve_nationality("p4", "TH", None, None)
    assert th.group == GROUP_SE_ASIA
    assert th.confidence == CONF_HIGH


def test_resolve_nationality_medium_confidence_zh() -> None:
    """resolve_nationality returns medium confidence for name_zh fallback."""
    from src.analysis.network.nationality_resolver import (
        CONF_MEDIUM,
        GROUP_CN,
        resolve_nationality,
    )

    rec = resolve_nationality("p5", None, "张三", None)
    assert rec.group == GROUP_CN
    assert rec.confidence == CONF_MEDIUM


def test_resolve_nationality_medium_confidence_ko() -> None:
    """resolve_nationality returns medium confidence for name_ko fallback."""
    from src.analysis.network.nationality_resolver import (
        CONF_MEDIUM,
        GROUP_KR,
        resolve_nationality,
    )

    rec = resolve_nationality("p6", None, None, "홍길동")
    assert rec.group == GROUP_KR
    assert rec.confidence == CONF_MEDIUM


def test_resolve_nationality_unknown() -> None:
    """resolve_nationality returns unknown when no data available."""
    from src.analysis.network.nationality_resolver import (
        CONF_LOW,
        GROUP_UNKNOWN,
        resolve_nationality,
    )

    rec = resolve_nationality("p7", None, None, None)
    assert rec.group == GROUP_UNKNOWN
    assert rec.confidence == CONF_LOW


def test_resolve_nationality_empty_strings() -> None:
    """resolve_nationality treats empty strings as missing."""
    from src.analysis.network.nationality_resolver import (
        CONF_LOW,
        GROUP_UNKNOWN,
        resolve_nationality,
    )

    rec = resolve_nationality("p8", "", "", "")
    assert rec.group == GROUP_UNKNOWN
    assert rec.confidence == CONF_LOW


def test_load_nationality_records_basic(silver_conn: sqlite3.Connection) -> None:
    """load_nationality_records returns one record per person."""
    from src.analysis.network.nationality_resolver import load_nationality_records

    records = load_nationality_records(silver_conn)
    assert len(records) == 440, f"Expected 440 records, got {len(records)}"


def test_load_nationality_records_groups(silver_conn: sqlite3.Connection) -> None:
    """load_nationality_records assigns correct groups."""
    from src.analysis.network.nationality_resolver import (
        GROUP_CN,
        GROUP_DOMESTIC,
        GROUP_KR,
        GROUP_SE_ASIA,
        load_nationality_records,
    )

    records = load_nationality_records(silver_conn)
    by_group: dict[str, int] = {}
    for r in records:
        by_group[r.group] = by_group.get(r.group, 0) + 1

    assert by_group.get(GROUP_DOMESTIC, 0) == 300
    assert by_group.get(GROUP_CN, 0) == 60
    assert by_group.get(GROUP_KR, 0) == 40
    assert by_group.get(GROUP_SE_ASIA, 0) == 20


def test_load_nationality_records_missing_columns() -> None:
    """load_nationality_records handles missing columns gracefully."""
    from src.analysis.network.nationality_resolver import load_nationality_records

    minimal = sqlite3.connect(":memory:")
    minimal.execute(
        "CREATE TABLE persons (id TEXT PRIMARY KEY, name_en TEXT NOT NULL DEFAULT '')"
    )
    minimal.execute("INSERT INTO persons (id, name_en) VALUES ('p1', 'Test')")
    minimal.commit()

    records = load_nationality_records(minimal)
    assert len(records) == 1
    assert records[0].group == "UNKNOWN"


def test_nationality_summary(silver_conn: sqlite3.Connection) -> None:
    """NationalitySummary.from_records computes correct stats."""
    from src.analysis.network.nationality_resolver import (
        NationalitySummary,
        load_nationality_records,
    )

    records = load_nationality_records(silver_conn)
    summary = NationalitySummary.from_records(records)

    assert summary.total_persons == 440
    assert summary.n_high_confidence == 420  # 300+60+40+20 = 420
    assert summary.n_low_confidence == 20     # unknown group
    assert summary.coverage_pct == pytest.approx(95.45, abs=0.5)


def test_studio_foreign_share(silver_conn: sqlite3.Connection) -> None:
    """studio_foreign_share returns rows sorted by foreign_share desc."""
    from src.analysis.network.nationality_resolver import (
        load_nationality_records,
        studio_foreign_share,
    )

    records = load_nationality_records(silver_conn)
    rows = studio_foreign_share(silver_conn, records, min_credits=1)

    # Should have entries for up to 4 studios
    assert len(rows) <= 4
    if len(rows) >= 2:
        # Sorted descending by foreign_share
        shares = [r["foreign_share"] for r in rows]
        assert shares == sorted(shares, reverse=True), "Should be sorted desc by foreign_share"

    for row in rows:
        assert 0.0 <= row["foreign_share"] <= 1.0, "foreign_share must be in [0,1]"
        assert row["total_credits"] > 0


def test_person_fe_by_nationality_empty() -> None:
    """person_fe_by_nationality returns empty dict when feat_person_scores absent."""
    from src.analysis.network.nationality_resolver import (
        load_nationality_records,
        person_fe_by_nationality,
    )

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE persons (id TEXT PRIMARY KEY, name_en TEXT NOT NULL DEFAULT '')")
    conn.execute("INSERT INTO persons VALUES ('p1', 'Test')")
    conn.commit()

    records = load_nationality_records(conn)
    result = person_fe_by_nationality(conn, records)
    assert result == {}


# ---------------------------------------------------------------------------
# Invariant checks
# ---------------------------------------------------------------------------


def test_report_src_exists() -> None:
    """o4_foreign_talent.py source file must exist."""
    assert _REPORT_SRC.exists(), f"Report source not found: {_REPORT_SRC}"


def test_analysis_src_exists() -> None:
    """nationality_resolver.py source file must exist."""
    assert _ANALYSIS_SRC.exists(), f"Analysis source not found: {_ANALYSIS_SRC}"


def test_report_registered_in_init() -> None:
    """O4ForeignTalentReport must be in V2_REPORT_CLASSES."""
    from scripts.report_generators.reports import V2_REPORT_CLASSES
    from scripts.report_generators.reports.o4_foreign_talent import O4ForeignTalentReport

    assert O4ForeignTalentReport in V2_REPORT_CLASSES, (
        "O4ForeignTalentReport must be in V2_REPORT_CLASSES"
    )


def test_report_name_and_filename() -> None:
    """O4ForeignTalentReport must have correct name and filename."""
    from scripts.report_generators.reports.o4_foreign_talent import O4ForeignTalentReport

    assert O4ForeignTalentReport.name == "o4_foreign_talent"
    assert O4ForeignTalentReport.filename == "o4_foreign_talent.html"
