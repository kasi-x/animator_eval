"""Tests for Business Brief generator (labor-aware industry observation framing).

Tests cover:
- generate_business_brief() returns non-empty dict
- Brief has audience BUSINESS_INNOVATION
- Sections are framed as structural observation, not investment recommendations
- Each section has labor-structural impact content
- stance_and_disclaimer section present
- Labor-first caveat blocks present
- Forbidden vocabulary 0 violations (source file)
"""

import pytest
import re
from pathlib import Path


def _import_generate_business_brief():
    """Import generate_business_brief without triggering DB/pipeline calls."""
    import sys
    worktree_root = Path(__file__).parent.parent.parent
    scripts_dir = str(worktree_root / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from report_generators.briefs.business_brief import generate_business_brief
    return generate_business_brief


def _import_lint_vocab():
    """Import lint_vocab for vocabulary checks."""
    import sys
    worktree_root = Path(__file__).parent.parent.parent
    scripts_dir = str(worktree_root / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from report_generators.lint_vocab import (
        load_vocab, load_replacements, load_exceptions,
        _compile_patterns, lint_file, _is_definitional, _is_excepted,
    )
    return load_vocab, load_replacements, load_exceptions, _compile_patterns, lint_file, _is_definitional, _is_excepted


class TestBusinessBriefGeneration:
    """Business Brief generation and structure tests."""

    @pytest.fixture(scope="class")
    def brief_dict(self):
        generate = _import_generate_business_brief()
        result = generate()
        assert result, "generate_business_brief() returned empty result"
        return result

    def test_returns_non_empty_dict(self, brief_dict):
        assert isinstance(brief_dict, dict)
        assert len(brief_dict) > 0

    def test_metadata_present(self, brief_dict):
        meta = brief_dict.get("metadata", {})
        assert meta.get("audience") == "business_innovation"
        assert meta.get("title")
        assert meta.get("version")

    def test_method_gates_present(self, brief_dict):
        gates = brief_dict.get("method_gates", [])
        assert len(gates) >= 3
        for gate in gates:
            assert gate.get("method_name")
            assert gate.get("algorithm")
            assert gate.get("confidence_interval_method")

    def test_lineage_present(self, brief_dict):
        lineage = brief_dict.get("lineage")
        assert lineage is not None
        assert lineage.get("pipeline_version")
        assert lineage.get("data_cutoff_date")

    def test_sections_present(self, brief_dict):
        sections = brief_dict.get("sections", {})
        assert len(sections) >= 5, "Business brief requires at least 5 sections"

    def test_stance_and_disclaimer_section_present(self, brief_dict):
        sections = brief_dict.get("sections", {})
        assert "stance_and_disclaimer" in sections
        findings = sections["stance_and_disclaimer"].get("findings", "")
        assert "labor-first" in findings.lower() or "労働者" in findings

    def test_labor_structural_impact_in_sections(self, brief_dict):
        """Each substantive section should include labor-structural impact content."""
        sections = brief_dict.get("sections", {})
        exempt = {"stance_and_disclaimer"}
        labor_impact_count = 0
        for section_id, section in sections.items():
            if section_id in exempt:
                continue
            combined = (
                (section.get("findings") or "") + " " +
                (section.get("interpretation") or "")
            )
            if any(w in combined.lower() for w in ["labor", "worker", "structural impact", "労働"]):
                labor_impact_count += 1
        assert labor_impact_count >= 3, (
            "Business brief should have labor-structural framing in at least 3 sections"
        )

    def test_labor_first_caveat_blocks_present(self, brief_dict):
        """Business brief sections should include labor-first caveat blocks."""
        sections = brief_dict.get("sections", {})
        exempt = {"stance_and_disclaimer"}
        caveat_count = 0
        for section_id, section in sections.items():
            if section_id in exempt:
                continue
            combined = (
                (section.get("findings") or "") + " " +
                (section.get("interpretation") or "")
            )
            if "labor-first caveat" in combined.lower() or "labor-structural caveat" in combined.lower():
                caveat_count += 1
        assert caveat_count >= 2, (
            "Business brief should have explicit labor-first caveat blocks in at least 2 sections"
        )

    def test_substantive_sections_have_findings_and_interpretation(self, brief_dict):
        sections = brief_dict.get("sections", {})
        exempt = {"stance_and_disclaimer"}
        for section_id, section in sections.items():
            if section_id in exempt:
                continue
            assert section.get("findings"), f"Section '{section_id}' missing findings"
            assert section.get("interpretation"), f"Section '{section_id}' missing interpretation"

    def test_no_pure_investment_recommendation_framing(self, brief_dict):
        """Business brief should not contain pure investment pitch language."""
        sections = brief_dict.get("sections", {})
        investment_pitch_phrases = [
            "investment recommendation",
            "acquisition target",
            "recruitment targets",
            "staff poaching",
        ]
        for section_id, section in sections.items():
            combined = (
                (section.get("findings") or "") + " " +
                (section.get("interpretation") or "")
            ).lower()
            for phrase in investment_pitch_phrases:
                assert phrase not in combined, (
                    f"Section '{section_id}' contains investment pitch language: '{phrase}'"
                )

    def test_no_prohibited_vocabulary_in_section_content(self, brief_dict):
        prohibited = {
            r'\bability\b', r'\bskill\b', r'\btalent\b',
            r'\bcompetence\b', r'\bcapability\b',
        }
        sections = brief_dict.get("sections", {})
        for section_id, section in sections.items():
            combined = (
                (section.get("findings") or "") + " " +
                (section.get("interpretation") or "")
            ).lower()
            for pattern in prohibited:
                m = re.search(pattern, combined)
                assert m is None, (
                    f"Section '{section_id}' contains prohibited term '{m.group() if m else ''}'"
                )

    def test_opportunity_residual_is_structural_observation(self, brief_dict):
        """opportunity_residual section should describe structural gaps, not recruitment."""
        sections = brief_dict.get("sections", {})
        if "opportunity_residual" in sections:
            section = sections["opportunity_residual"]
            combined = (
                (section.get("findings") or "") + " " +
                (section.get("interpretation") or "")
            ).lower()
            # Should use structural observation language
            assert "structural" in combined, (
                "opportunity_residual section should use structural observation framing"
            )
            # Should NOT be pure recruitment targeting
            assert "recruitment targets" not in combined, (
                "opportunity_residual should not contain 'recruitment targets' framing"
            )

    def test_generated_at_timestamp_present(self, brief_dict):
        assert "generated_at" in brief_dict


class TestBusinessBriefLintVocab:
    """Vocabulary lint gate test for business_brief.py source file."""

    def test_lint_vocab_zero_violations(self):
        load_vocab, load_replacements, load_exceptions, _compile_patterns, lint_file, _is_definitional, _is_excepted = _import_lint_vocab()
        brief_path = (
            Path(__file__).parent.parent.parent
            / "scripts/report_generators/briefs/business_brief.py"
        )
        terms = load_vocab()
        replacements = load_replacements()
        exceptions = load_exceptions()
        patterns = _compile_patterns(terms)
        findings = lint_file(brief_path, patterns, replacements)
        findings = [f for f in findings if not _is_definitional(f) and not _is_excepted(f, exceptions)]
        assert len(findings) == 0, (
            f"business_brief.py has {len(findings)} vocabulary violations: "
            + "; ".join(f.format() for f in findings[:3])
        )
