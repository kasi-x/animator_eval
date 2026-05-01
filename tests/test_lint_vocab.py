"""Tests for lint_vocab.py contextual 2-gram patterns (X3 — x_cross_cutting).

Covers:
- CONTEXTUAL_BIGRAMS: 4 prohibited 2-gram combinations
- _scan_bigrams(): proximity-window detection logic
- lint_file_bigrams(): file-level scanning (Python string literals and raw text)
- Zero false-positives when terms appear in isolation
- Zero false-positives on existing reports/ directory
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.report_generators.lint_vocab import (
    CONTEXTUAL_BIGRAMS,
    BigramFinding,
    _scan_bigrams,
    lint_file_bigrams,
)


# ---------------------------------------------------------------------------
# CONTEXTUAL_BIGRAMS structural checks
# ---------------------------------------------------------------------------


class TestContextualBigramsDefinition:
    """Verify CONTEXTUAL_BIGRAMS has the expected 4 patterns."""

    def test_four_patterns_defined(self):
        assert len(CONTEXTUAL_BIGRAMS) == 4

    def test_all_entries_are_4_tuples(self):
        for entry in CONTEXTUAL_BIGRAMS:
            assert len(entry) == 4, f"Entry {entry!r} must be a 4-tuple"

    def test_expected_term_pairs(self):
        pairs = {(a, b) for a, b, *_ in CONTEXTUAL_BIGRAMS}
        assert ("失われた", "人材") in pairs
        assert ("不在", "能力") in pairs
        assert ("埋もれた", "才能") in pairs
        assert ("眠っている", "実力") in pairs

    def test_messages_reference_both_terms(self):
        """Each violation message must name both terms."""
        for a, b, msg, _ in CONTEXTUAL_BIGRAMS:
            assert a in msg, f"Message for ({a!r},{b!r}) does not contain {a!r}"
            assert b in msg, f"Message for ({a!r},{b!r}) does not contain {b!r}"

    def test_suggestions_non_empty(self):
        for a, b, _, suggestion in CONTEXTUAL_BIGRAMS:
            assert suggestion, f"Suggestion for ({a!r},{b!r}) must not be empty"


# ---------------------------------------------------------------------------
# _scan_bigrams: detection when both terms are close
# ---------------------------------------------------------------------------


class TestScanBigramsDetection:
    """_scan_bigrams must flag co-occurring pairs within BIGRAM_WINDOW."""

    def _bigrams(self):
        return CONTEXTUAL_BIGRAMS

    def _scan(self, text: str) -> list[BigramFinding]:
        return _scan_bigrams(text, Path("test.txt"), self._bigrams())

    def test_flag_ushinawareta_jinzai(self):
        findings = self._scan("失われた人材が業界から消えた。")
        assert len(findings) >= 1
        assert any(f.term_a == "失われた" and f.term_b == "人材" for f in findings)

    def test_flag_fuzai_noryoku(self):
        findings = self._scan("不在の能力が問題だ。")
        assert len(findings) >= 1
        assert any(f.term_a == "不在" and f.term_b == "能力" for f in findings)

    def test_flag_umoreta_saino(self):
        findings = self._scan("埋もれた才能を発掘する。")
        assert len(findings) >= 1
        assert any(f.term_a == "埋もれた" and f.term_b == "才能" for f in findings)

    def test_flag_nemutte_jitsuryoku(self):
        findings = self._scan("眠っている実力を活かす。")
        assert len(findings) >= 1
        assert any(f.term_a == "眠っている" and f.term_b == "実力" for f in findings)


# ---------------------------------------------------------------------------
# _scan_bigrams: no false-positive for isolated terms
# ---------------------------------------------------------------------------


class TestScanBigramsIsolation:
    """Single-term occurrences must NOT be flagged."""

    def _scan(self, text: str) -> list[BigramFinding]:
        return _scan_bigrams(text, Path("test.txt"), CONTEXTUAL_BIGRAMS)

    def test_no_flag_ushinawareta_alone(self):
        # "失われた" is legitimate in O7 (historical gap context)
        findings = self._scan("クレジットが失われたケースを調査した。")
        assert findings == []

    def test_no_flag_jinzai_alone(self):
        # "人材" is a neutral structural term
        findings = self._scan("人材の流動性を定量化した。")
        assert findings == []

    def test_no_flag_fuzai_alone(self):
        findings = self._scan("データ欠落による不在期間を計測した。")
        assert findings == []

    def test_no_flag_noryoku_alone(self):
        # "能力" in isolation triggers the standard unigram gate in forbidden_vocab,
        # but not the bigram gate — confirm bigram returns no hit here.
        findings = self._scan("能力という語が含まれる。")
        assert all(f.term_a != "不在" for f in findings)

    def test_no_flag_saino_alone(self):
        findings = self._scan("才能という語は本データでは使用しない。")
        assert all(f.term_a != "埋もれた" for f in findings)

    def test_no_flag_jitsuryoku_alone(self):
        findings = self._scan("実力という語が含まれる。")
        assert all(f.term_a != "眠っている" for f in findings)


# ---------------------------------------------------------------------------
# _scan_bigrams: proximity window boundary
# ---------------------------------------------------------------------------


class TestScanBigramWindowBoundary:
    """Terms just inside the window are flagged; just outside are not."""

    def _scan(self, text: str) -> list[BigramFinding]:
        return _scan_bigrams(text, Path("test.txt"), CONTEXTUAL_BIGRAMS)

    def test_inside_window_is_flagged(self):
        # 30 chars between terms — well within BIGRAM_WINDOW (60)
        gap = "あ" * 30
        text = f"失われた{gap}人材がいる。"
        findings = self._scan(text)
        assert len(findings) >= 1

    def test_outside_window_not_flagged(self):
        # 70 chars between terms — exceeds BIGRAM_WINDOW (60)
        gap = "あ" * 70
        text = f"失われた{gap}人材がいる。"
        findings = self._scan(text)
        assert findings == []


# ---------------------------------------------------------------------------
# lint_file_bigrams: Python string literal scanning
# ---------------------------------------------------------------------------


class TestLintFileBigramsPython:
    """lint_file_bigrams must scan Python string literals, not comments."""

    def test_detects_bigram_in_string_literal(self, tmp_path):
        py_file = tmp_path / "test_module.py"
        # Write bigram inside a string literal
        py_file.write_text(
            'text = "失われた人材が問題です"\n',
            encoding="utf-8",
        )
        findings = lint_file_bigrams(py_file, CONTEXTUAL_BIGRAMS)
        assert len(findings) >= 1

    def test_ignores_bigram_in_comment(self, tmp_path):
        py_file = tmp_path / "test_module.py"
        # Bigram only in a comment — must NOT flag
        py_file.write_text(
            "# 失われた人材の分析 (コメント)\nx = 1\n",
            encoding="utf-8",
        )
        findings = lint_file_bigrams(py_file, CONTEXTUAL_BIGRAMS)
        assert findings == []

    def test_ignores_bigram_in_identifier(self, tmp_path):
        # Identifiers are not string literals; no flagging
        py_file = tmp_path / "test_module.py"
        py_file.write_text(
            "def 失われた人材_func():\n    pass\n",
            encoding="utf-8",
        )
        findings = lint_file_bigrams(py_file, CONTEXTUAL_BIGRAMS)
        assert findings == []

    def test_clean_file_returns_empty(self, tmp_path):
        py_file = tmp_path / "clean.py"
        py_file.write_text(
            'text = "クレジット可視性喪失者数を計測した。"\n',
            encoding="utf-8",
        )
        findings = lint_file_bigrams(py_file, CONTEXTUAL_BIGRAMS)
        assert findings == []


# ---------------------------------------------------------------------------
# Zero false-positives on existing reports/
# ---------------------------------------------------------------------------


class TestNoFalsePositivesOnExistingReports:
    """Existing report sources must not trigger any bigram violations."""

    REPORTS_DIR = (
        Path(__file__).resolve().parent.parent
        / "scripts"
        / "report_generators"
        / "reports"
    )

    def test_no_bigram_hits_in_reports(self):
        if not self.REPORTS_DIR.exists():
            pytest.skip("reports/ directory not found")
        py_files = list(self.REPORTS_DIR.rglob("*.py"))
        if not py_files:
            pytest.skip("No .py files in reports/")
        all_findings: list[BigramFinding] = []
        for f in py_files:
            if "archived" in f.parts:
                continue
            all_findings.extend(lint_file_bigrams(f, CONTEXTUAL_BIGRAMS))
        assert all_findings == [], (
            "Bigram violations in existing reports:\n"
            + "\n".join(f.format() for f in all_findings)
        )


# ---------------------------------------------------------------------------
# BigramFinding.format()
# ---------------------------------------------------------------------------


class TestBigramFindingFormat:
    def test_format_includes_location_and_message(self):
        finding = BigramFinding(
            path=Path("foo.py"),
            line=10,
            col=5,
            term_a="埋もれた",
            term_b="才能",
            message="ability_framing (2-gram): 'buried'",
            replacement="露出機会ギャップ人材プール",
            context="埋もれた才能を発掘する。",
        )
        formatted = finding.format()
        assert "foo.py:10:5" in formatted
        assert "ability_framing" in formatted
        assert "露出機会ギャップ人材プール" in formatted
        assert "埋もれた才能を発掘する。" in formatted
