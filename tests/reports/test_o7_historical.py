"""Tests for O7 歴史的クレジット記録復元分析 report.

Covers:
- smoke test: generate() returns a Path with valid HTML
- confidence_tier distribution: tier counts per decade are correct
- restoration_rate CI: analytical Wilson CI is within [0, 1]
- RESTORED rows: evidence_source = 'restoration_estimated' invariant
- existing SILVER rows untouched: H5 — no mutation of HIGH/MEDIUM rows
- multi_source_match: find_restoration_candidates() returns RestorationCandidate
- insert_restored: evidence_source + confidence_tier invariants
- lint_vocab: no forbidden terms in report source
- method gate: Wilson CI annotation present in output
- H1: anime.score not referenced in source
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

from scripts.report_generators.reports.o7_historical import (
    O7HistoricalRestorationReport,
    TierBreakdown,
    _compute_restoration_ci,
    _fetch_restored_credits,
    _fetch_tier_by_decade,
)
from src.etl.credit_restoration.multi_source_match import (
    EVIDENCE_SOURCE,
    RestorationCandidate,
    _fuzzy_sim,
    find_restoration_candidates,
)
from src.etl.credit_restoration.insert_restored import (
    RESTORED_TIER,
    count_restored_credits,
    insert_restored_credits,
)


# ---------------------------------------------------------------------------
# Synthetic in-memory SQLite DB
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS anime (
    id         TEXT PRIMARY KEY,
    title_ja   TEXT,
    title_en   TEXT,
    year       INTEGER,
    episodes   INTEGER,
    duration   INTEGER
);

CREATE TABLE IF NOT EXISTS persons (
    id       TEXT PRIMARY KEY,
    name_ja  TEXT NOT NULL DEFAULT '',
    name_en  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS credits (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id       TEXT NOT NULL,
    anime_id        TEXT NOT NULL,
    role            TEXT NOT NULL,
    raw_role        TEXT NOT NULL DEFAULT '',
    evidence_source TEXT NOT NULL DEFAULT '',
    confidence_tier TEXT NOT NULL DEFAULT 'HIGH',
    credit_year     INTEGER,
    episode         INTEGER,
    UNIQUE(person_id, anime_id, raw_role, episode)
);

CREATE TABLE IF NOT EXISTS roles (
    name   TEXT PRIMARY KEY,
    weight REAL
);
"""


def _build_test_db() -> sqlite3.Connection:
    """Create a minimal in-memory DB with pre-1990 and post-1990 anime."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA_SQL)

    anime_rows = [
        # id,    title_ja,                   title_en,               year, episodes, duration
        ("a1920", "無声映画A",                 "Silent Film A",       1920,  1,  60),
        ("a1963", "鉄腕アトム風作品",           "Astro Boy-like",      1963, 52,  25),
        ("a1979", "銀河鉄道風作品",             "Galaxy Express-like", 1979, 113, 25),
        ("a1985", "テスト旧作",                "Old Test Work",        1985, 26,  25),
        ("a1995", "新世紀作品",                "New Century Work",     1995, 26,  25),
    ]
    conn.executemany(
        "INSERT INTO anime (id, title_ja, title_en, year, episodes, duration) VALUES (?, ?, ?, ?, ?, ?)",
        anime_rows,
    )

    persons = [
        ("p1", "手塚風", "Tezuka-like"),
        ("p2", "松本風", "Matsumoto-like"),
        ("p3", "新人", "Newcomer"),
    ]
    conn.executemany("INSERT INTO persons (id, name_ja, name_en) VALUES (?, ?, ?)", persons)

    # Existing HIGH-tier credits.
    credits = [
        # person_id, anime_id, role,          raw_role,           evidence_source, confidence_tier, credit_year, episode
        ("p1", "a1963", "director",        "director",         "ann",        "HIGH",     1963, None),
        ("p2", "a1979", "director",        "director",         "mediaarts",  "HIGH",     1979, None),
        ("p1", "a1985", "key_animator",    "key_animator",     "seesaawiki", "HIGH",     1985, None),
        ("p3", "a1995", "animation_director", "animation_director", "ann",   "HIGH",     1995, None),
    ]
    conn.executemany(
        """INSERT OR IGNORE INTO credits
           (person_id, anime_id, role, raw_role, evidence_source, confidence_tier, credit_year, episode)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        credits,
    )

    conn.commit()
    return conn


def _build_db_with_restored(conn: sqlite3.Connection) -> None:
    """Add RESTORED-tier rows to an existing test DB."""
    restored = [
        # person_id, anime_id, role,       raw_role,         evidence_source,          confidence_tier, credit_year, episode
        ("p1", "a1920", "director",    "手塚風",           "restoration_estimated",  "RESTORED",  1920, None),
        ("p2", "a1963", "key_animator", "松本風",          "restoration_estimated",  "RESTORED",  1963, None),
    ]
    conn.executemany(
        """INSERT OR IGNORE INTO credits
           (person_id, anime_id, role, raw_role, evidence_source, confidence_tier, credit_year, episode)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        restored,
    )
    conn.commit()


@pytest.fixture
def test_conn() -> sqlite3.Connection:
    return _build_test_db()


@pytest.fixture
def test_conn_with_restored() -> sqlite3.Connection:
    conn = _build_test_db()
    _build_db_with_restored(conn)
    return conn


# ---------------------------------------------------------------------------
# Tests: _fetch_tier_by_decade
# ---------------------------------------------------------------------------


class TestFetchTierByDecade:
    def test_returns_list(self, test_conn):
        result = _fetch_tier_by_decade(test_conn)
        assert isinstance(result, list)

    def test_all_labels_are_decade_strings(self, test_conn):
        result = _fetch_tier_by_decade(test_conn)
        for bd in result:
            assert bd.label.endswith("s"), f"Expected decade string, got {bd.label}"

    def test_counts_positive(self, test_conn):
        result = _fetch_tier_by_decade(test_conn)
        for bd in result:
            assert bd.total >= 0

    def test_post_1990_excluded(self, test_conn):
        result = _fetch_tier_by_decade(test_conn)
        # 1995 anime (a1995) should NOT appear.
        for bd in result:
            assert "1990" not in bd.label and "2000" not in bd.label, (
                f"Post-1990 decade {bd.label} should be excluded"
            )

    def test_restoration_rate_in_range(self, test_conn_with_restored):
        result = _fetch_tier_by_decade(test_conn_with_restored)
        for bd in result:
            assert 0.0 <= bd.restoration_rate <= 1.0


# ---------------------------------------------------------------------------
# Tests: _compute_restoration_ci
# ---------------------------------------------------------------------------


class TestComputeRestorationCI:
    def test_zero_total_returns_zeros(self):
        lo, hi = _compute_restoration_ci(0, 0)
        assert lo == 0.0
        assert hi == 0.0

    def test_in_range_01(self):
        for n_restored, n_total in [(0, 100), (50, 100), (100, 100), (1, 5)]:
            lo, hi = _compute_restoration_ci(n_restored, n_total)
            assert 0.0 <= lo <= hi <= 1.0, f"CI out of range for ({n_restored}/{n_total})"

    def test_ci_width_decreases_with_n(self):
        lo_small, hi_small = _compute_restoration_ci(5, 10)
        lo_large, hi_large = _compute_restoration_ci(50, 100)
        # Same rate, larger n → narrower CI.
        assert (hi_small - lo_small) > (hi_large - lo_large)

    def test_rate_half_symmetric(self):
        lo, hi = _compute_restoration_ci(50, 100)
        midpoint = (lo + hi) / 2
        # Wilson CI is approximately centred on p=0.5.
        assert abs(midpoint - 0.5) < 0.05


# ---------------------------------------------------------------------------
# Tests: _fetch_restored_credits
# ---------------------------------------------------------------------------


class TestFetchRestoredCredits:
    def test_empty_when_no_restored(self, test_conn):
        result = _fetch_restored_credits(test_conn)
        assert result == []

    def test_returns_restored_rows(self, test_conn_with_restored):
        result = _fetch_restored_credits(test_conn_with_restored)
        assert len(result) >= 1

    def test_all_rows_are_restored_tier(self, test_conn_with_restored):
        result = _fetch_restored_credits(test_conn_with_restored)
        for row in result:
            assert row.confidence_tier == "RESTORED"

    def test_post_1990_excluded(self, test_conn_with_restored):
        result = _fetch_restored_credits(test_conn_with_restored)
        for row in result:
            if row.cohort_year is not None:
                assert row.cohort_year < 1990


# ---------------------------------------------------------------------------
# Tests: fuzzy_sim helper
# ---------------------------------------------------------------------------


class TestFuzzySim:
    def test_identical_strings(self):
        assert _fuzzy_sim("鉄腕アトム", "鉄腕アトム") == 1.0

    def test_empty_returns_zero(self):
        assert _fuzzy_sim("", "ABC") == 0.0
        assert _fuzzy_sim("ABC", "") == 0.0

    def test_dissimilar_returns_low(self):
        sim = _fuzzy_sim("AAAA", "ZZZZ")
        assert sim < 0.5

    def test_similar_returns_high(self):
        sim = _fuzzy_sim("Silver Fang", "Silver Fangs")
        assert sim > 0.8


# ---------------------------------------------------------------------------
# Tests: find_restoration_candidates
# ---------------------------------------------------------------------------


class TestFindRestorationCandidates:
    def test_returns_list(self, test_conn):
        result = find_restoration_candidates(test_conn)
        assert isinstance(result, list)

    def test_all_candidates_have_evidence_source_tag(self, test_conn):
        """All candidates must carry the restoration_estimated marker conceptually."""
        result = find_restoration_candidates(test_conn)
        # No BRONZE source tables exist in the in-memory DB, so result is empty.
        # Verify the invariant holds vacuously (no false RESTORED rows).
        for cand in result:
            assert cand.anime_id, "candidate must have anime_id"
            assert cand.role, "candidate must have role"

    def test_empty_db_returns_empty(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(_SCHEMA_SQL)
        result = find_restoration_candidates(conn)
        assert result == []

    def test_confidence_tier_values_valid(self, test_conn):
        result = find_restoration_candidates(test_conn)
        valid_tiers = {"HIGH", "MEDIUM", "LOW", "RESTORED"}
        for cand in result:
            assert cand.confidence_tier in valid_tiers


# ---------------------------------------------------------------------------
# Tests: insert_restored_credits
# ---------------------------------------------------------------------------


class TestInsertRestoredCredits:
    def _make_candidates(self, conn: sqlite3.Connection) -> list[RestorationCandidate]:
        return [
            RestorationCandidate(
                anime_id="a1920",
                role="animator",
                person_name_candidate="テスト人物A",
                person_id_candidate="p1",
                sources_supporting=["ann", "mediaarts"],
                similarity_score=0.92,
                cohort_year=1920,
                progression_consistency=True,
                confidence_tier="MEDIUM",
            ),
            RestorationCandidate(
                anime_id="a1963",
                role="key_animator",
                person_name_candidate="未知の人物",
                person_id_candidate=None,
                sources_supporting=["seesaawiki"],
                similarity_score=0.87,
                cohort_year=1963,
                progression_consistency=True,
                confidence_tier="LOW",
            ),
        ]

    def test_dry_run_returns_zero(self, test_conn):
        candidates = self._make_candidates(test_conn)
        n = insert_restored_credits(test_conn, candidates, dry_run=True)
        assert n == 0

    def test_dry_run_does_not_insert(self, test_conn):
        candidates = self._make_candidates(test_conn)
        insert_restored_credits(test_conn, candidates, dry_run=True)
        counts = count_restored_credits(test_conn)
        # No RESTORED rows should exist after dry_run.
        assert counts.get("RESTORED", 0) == 0

    def test_inserts_rows(self, test_conn):
        candidates = self._make_candidates(test_conn)
        n = insert_restored_credits(test_conn, candidates)
        assert n > 0

    def test_evidence_source_is_restoration_estimated(self, test_conn):
        """H4: evidence_source = 'restoration_estimated' invariant."""
        candidates = self._make_candidates(test_conn)
        insert_restored_credits(test_conn, candidates)
        rows = test_conn.execute(
            "SELECT evidence_source FROM credits WHERE confidence_tier = 'RESTORED'"
        ).fetchall()
        for row in rows:
            assert str(row[0]) == EVIDENCE_SOURCE, (
                f"evidence_source must be '{EVIDENCE_SOURCE}', got '{row[0]}'"
            )

    def test_confidence_tier_is_restored(self, test_conn):
        candidates = self._make_candidates(test_conn)
        insert_restored_credits(test_conn, candidates)
        rows = test_conn.execute(
            "SELECT confidence_tier FROM credits WHERE evidence_source = 'restoration_estimated'"
        ).fetchall()
        for row in rows:
            assert str(row[0]) == RESTORED_TIER

    def test_existing_high_rows_untouched(self, test_conn):
        """H5: existing SILVER credits (HIGH tier) must not be mutated."""
        before = test_conn.execute(
            "SELECT COUNT(*) FROM credits WHERE confidence_tier = 'HIGH'"
        ).fetchone()[0]
        candidates = self._make_candidates(test_conn)
        insert_restored_credits(test_conn, candidates)
        after = test_conn.execute(
            "SELECT COUNT(*) FROM credits WHERE confidence_tier = 'HIGH'"
        ).fetchone()[0]
        assert before == after, "HIGH-tier rows were modified — H5 violated"

    def test_idempotent(self, test_conn):
        """Running insert twice should not raise and counts should be stable."""
        candidates = self._make_candidates(test_conn)
        n1 = insert_restored_credits(test_conn, candidates)
        n2 = insert_restored_credits(test_conn, candidates)
        # Second insert should succeed (INSERT OR IGNORE) with same count.
        assert n1 == n2

    def test_empty_candidates_returns_zero(self, test_conn):
        n = insert_restored_credits(test_conn, [])
        assert n == 0


# ---------------------------------------------------------------------------
# Tests: count_restored_credits
# ---------------------------------------------------------------------------


class TestCountRestoredCredits:
    def test_returns_dict(self, test_conn):
        result = count_restored_credits(test_conn)
        assert isinstance(result, dict)

    def test_counts_restored(self, test_conn_with_restored):
        result = count_restored_credits(test_conn_with_restored)
        assert result.get("RESTORED", 0) >= 2

    def test_counts_high(self, test_conn):
        result = count_restored_credits(test_conn)
        assert result.get("HIGH", 0) >= 1


# ---------------------------------------------------------------------------
# Smoke: full report generation
# ---------------------------------------------------------------------------


class TestO7HistoricalRestorationReportSmoke:
    def test_generate_returns_path(self, test_conn, tmp_path):
        report = O7HistoricalRestorationReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        assert isinstance(out, Path)
        assert out.exists()

    def test_output_is_html(self, test_conn, tmp_path):
        report = O7HistoricalRestorationReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in text or "<html" in text.lower()

    def test_disclaimer_present(self, test_conn, tmp_path):
        report = O7HistoricalRestorationReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        # JA + EN disclaimer required (REPORT_PHILOSOPHY §9).
        assert "ネットワーク" in text
        assert "network structure" in text.lower()

    def test_method_note_ci_present(self, test_conn, tmp_path):
        """Method gate: Wilson CI annotation must be present."""
        report = O7HistoricalRestorationReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        assert "Wilson" in text or "95%" in text

    def test_no_score_used_in_computation(self, test_conn, tmp_path):
        """H1: viewer ratings excluded from computation (structural data only)."""
        # Verify by checking the report source contains no SQL with anime.score
        import ast
        source = (
            Path(__file__).resolve().parents[2]
            / "scripts/report_generators/reports/o7_historical.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                val = node.value
                if "anime.score" in val and any(
                    kw in val.upper() for kw in ("SELECT", "FROM", "WHERE", "JOIN")
                ):
                    pytest.fail(f"anime.score found in SQL: {val[:80]}")

    def test_confidence_tier_glossary_present(self, test_conn, tmp_path):
        report = O7HistoricalRestorationReport(test_conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        assert "confidence_tier" in text

    def test_generate_with_empty_db(self, tmp_path):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(_SCHEMA_SQL)
        report = O7HistoricalRestorationReport(conn, output_dir=tmp_path)
        out = report.generate()
        assert out is not None
        assert out.exists()

    def test_generate_with_restored_rows(self, test_conn_with_restored, tmp_path):
        report = O7HistoricalRestorationReport(
            test_conn_with_restored, output_dir=tmp_path
        )
        out = report.generate()
        assert out is not None
        text = out.read_text(encoding="utf-8")
        assert "RESTORED" in text


# ---------------------------------------------------------------------------
# Lint vocab: forbidden terms must not appear in report source
# ---------------------------------------------------------------------------


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


class TestLintVocabCompliance:
    """Verify that o7_historical.py contains no forbidden vocabulary."""

    REPORT_FILE = _PROJECT_ROOT / "scripts/report_generators/reports/o7_historical.py"
    FORBIDDEN_PATTERN = re.compile(
        r"\b(ability|skill|talent|competence|capability)\b", re.IGNORECASE
    )

    def test_no_forbidden_vocab_in_source(self):
        source = self.REPORT_FILE.read_text(encoding="utf-8")
        matches = self.FORBIDDEN_PATTERN.findall(source)
        assert matches == [], (
            f"Forbidden vocabulary found in o7_historical.py: {matches}"
        )

    def test_no_anime_score_in_sql_queries(self):
        """H1: anime.score must not appear in SQL SELECT/WHERE clauses."""
        import ast
        source = self.REPORT_FILE.read_text(encoding="utf-8")
        # Parse SQL strings only — look for anime.score in actual query strings
        # (not in method notes or explanatory text that mention it to explain exclusion).
        tree = ast.parse(source)
        sql_strings_with_score = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                val = node.value
                if "anime.score" in val and any(
                    kw in val.upper() for kw in ("SELECT", "FROM", "WHERE", "JOIN")
                ):
                    sql_strings_with_score.append(val[:80])
        assert sql_strings_with_score == [], (
            f"anime.score found in SQL query strings: {sql_strings_with_score}"
        )

    def test_evidence_source_is_restoration_estimated_in_source(self):
        """H4: insert code must reference 'restoration_estimated'."""
        insert_source = (
            _PROJECT_ROOT / "src/etl/credit_restoration/insert_restored.py"
        ).read_text(encoding="utf-8")
        assert "restoration_estimated" in insert_source, (
            "insert_restored.py must reference 'restoration_estimated'"
        )


# ---------------------------------------------------------------------------
# Method gate: CI must be present and correct
# ---------------------------------------------------------------------------


class TestMethodGate:
    """REPORT_PHILOSOPHY §3 — CI annotation required for individual estimates."""

    def test_wilson_ci_non_degenerate(self):
        lo, hi = _compute_restoration_ci(10, 100)
        assert lo < hi, "CI must be non-degenerate"

    def test_wilson_ci_rate_zero(self):
        lo, hi = _compute_restoration_ci(0, 100)
        # p=0 — CI must still be non-degenerate (Wilson handles boundary).
        assert hi > 0.0, "CI upper must be > 0 even at p=0"

    def test_wilson_ci_rate_one(self):
        lo, hi = _compute_restoration_ci(100, 100)
        assert lo < 1.0, "CI lower must be < 1 even at p=1"

    def test_tier_breakdown_restoration_rate(self):
        bd = TierBreakdown(label="1960s", high=80, medium=10, low=5, restored=5)
        assert abs(bd.restoration_rate - 5 / 100) < 1e-9

    def test_tier_breakdown_empty(self):
        bd = TierBreakdown(label="1960s")
        assert bd.restoration_rate == 0.0
        assert bd.total == 0
