"""v2 section structure enforcement tests (Phase 4-2).

Tests that SectionBuilder.validate_findings() and ReportSection
comply with the v2 report philosophy gates.
"""

from __future__ import annotations

import pytest

from scripts.report_generators.section_builder import ReportSection, SectionBuilder


class TestValidateFindings:
    """validate_findings() must detect all prohibited vocabulary."""

    def setup_method(self):
        self.sb = SectionBuilder()

    def test_clean_findings_passes(self):
        text = "2022年のクレジット数は 1,234 件（95% CI: 1,200–1,268）であった。"
        assert self.sb.validate_findings(text) == []

    def test_ability_framing_ja(self):
        violations = self.sb.validate_findings("このスタッフの能力は高い。")
        assert any("能力" in v or "ability" in v.lower() for v in violations)

    def test_evaluative_adj_ja(self):
        violations = self.sb.validate_findings("このスタジオは優秀なスタッフを擁する。")
        assert any("優秀" in v for v in violations)

    def test_causal_verb_en(self):
        violations = self.sb.validate_findings(
            "High patronage scores cause better retention."
        )
        assert any("cause" in v.lower() for v in violations)

    def test_causal_verb_ja(self):
        violations = self.sb.validate_findings("高い信頼スコアが離職を引き起こす。")
        assert any("引き起こす" in v for v in violations)

    def test_normative_expression_en(self):
        violations = self.sb.validate_findings("Studios should hire more animators.")
        assert violations  # "should" is prohibited normative

    def test_multiple_violations(self):
        text = "優秀な人材が能力を発揮して素晴らしい結果をもたらす。"
        violations = self.sb.validate_findings(text)
        assert len(violations) >= 2

    def test_numbers_and_ci_allowed(self):
        text = (
            "平均クレジット数は 4.2 本 (SD=2.1, n=3,412)。"
            "上位10%は 8 本以上のクレジットを保有。"
        )
        assert self.sb.validate_findings(text) == []


class TestMethodNoteFromLineage:
    """method_note_from_lineage() must raise on missing lineage."""

    def test_raises_on_missing_table(self):
        import sqlite3

        conn = sqlite3.connect(":memory:")
        conn.execute(
            """CREATE TABLE meta_lineage (
                table_name TEXT PRIMARY KEY, audience TEXT NOT NULL,
                source_silver_tables TEXT NOT NULL,
                source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
                source_display_allowed INTEGER NOT NULL DEFAULT 0,
                formula_version TEXT NOT NULL, computed_at TIMESTAMP NOT NULL,
                ci_method TEXT, null_model TEXT, holdout_method TEXT,
                row_count INTEGER, notes TEXT)"""
        )
        sb = SectionBuilder()
        with pytest.raises(ValueError, match="No lineage registered"):
            sb.method_note_from_lineage("nonexistent_table", conn)

    def test_generates_html_with_lineage(self):
        import sqlite3

        conn = sqlite3.connect(":memory:")
        conn.execute(
            """CREATE TABLE meta_lineage (
                table_name TEXT PRIMARY KEY, audience TEXT NOT NULL,
                source_silver_tables TEXT NOT NULL,
                source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
                source_display_allowed INTEGER NOT NULL DEFAULT 0,
                formula_version TEXT NOT NULL, computed_at TIMESTAMP NOT NULL,
                ci_method TEXT, null_model TEXT, holdout_method TEXT,
                row_count INTEGER, notes TEXT)"""
        )
        conn.execute(
            "INSERT INTO meta_lineage VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "meta_test",
                "policy",
                '["anime_analysis","credits"]',
                1,
                0,
                "v1.0",
                "2026-01-01",
                "bootstrap_n1000",
                None,
                None,
                100,
                "test",
            ),
        )
        sb = SectionBuilder()
        html = sb.method_note_from_lineage("meta_test", conn)
        assert "anime_analysis" in html
        assert "anime.score" in html  # score-prohibition confirmation
        assert "bootstrap_n1000" in html


class TestBuildSection:
    """build_section() must produce required HTML structure."""

    def test_renders_findings(self):
        sb = SectionBuilder()
        section = ReportSection(
            title="Test Section",
            findings_html="<p>年間 1,234 件のクレジット。</p>",
        )
        html = sb.build_section(section)
        assert "Test Section" in html
        assert "1,234 件" in html
        assert 'class="findings"' in html

    def test_method_note_in_details(self):
        sb = SectionBuilder()
        section = ReportSection(
            title="Test",
            findings_html="<p>データ。</p>",
            method_note="bootstrap n=1000",
        )
        html = sb.build_section(section)
        assert "<details>" in html
        assert "bootstrap n=1000" in html

    def test_interpretation_optional(self):
        sb = SectionBuilder()
        s_without = ReportSection(title="T", findings_html="<p>x</p>")
        s_with = ReportSection(
            title="T",
            findings_html="<p>x</p>",
            interpretation_html="<p>alternative: ...</p>",
        )
        html_without = sb.build_section(s_without)
        html_with = sb.build_section(s_with)
        assert "Interpretation" not in html_without
        assert "Interpretation" in html_with


class TestValidateSectionStructure:
    def test_validate_passes_with_required_sections(self):
        sb = SectionBuilder()
        sb.validate(
            has_overview=True,
            has_findings=True,
            has_method_note=True,
            has_data_statement=True,
            has_disclaimers=True,
            interpretation_html="<p>代替解釈: 別の説明可能性がある。</p>",
            method_note_auto_generated=True,
        )

    def test_validate_raises_on_missing_required(self):
        sb = SectionBuilder()
        with pytest.raises(ValueError, match="Missing required sections"):
            sb.validate(
                has_overview=False,
                has_findings=True,
                has_method_note=True,
                has_data_statement=True,
                has_disclaimers=True,
            )

    def test_validate_requires_alternative_interpretation(self):
        sb = SectionBuilder()
        with pytest.raises(ValueError, match="alternative interpretation"):
            sb.validate(
                has_overview=True,
                has_findings=True,
                has_method_note=True,
                has_data_statement=True,
                has_disclaimers=True,
                interpretation_html="<p>解釈のみ</p>",
            )

    def test_validate_requires_auto_generated_method_note(self):
        sb = SectionBuilder()
        with pytest.raises(ValueError, match="auto-generated"):
            sb.validate(
                has_overview=True,
                has_findings=True,
                has_method_note=True,
                has_data_statement=True,
                has_disclaimers=True,
                method_note_auto_generated=False,
            )
