"""Tests for Policy Brief generator (labor-first framing).

Tests cover:
- generate_policy_brief() returns non-empty dict
- Brief validates (method gates, lineage, sections present)
- stance_and_disclaimer section present
- Each substantive section has findings and interpretation
- build_disclaimer() and build_stance_block() are embedded
- AudienceType is POLICY
- Forbidden vocabulary 0 violations
"""

import pytest
import re
from pathlib import Path


def _import_generate_policy_brief():
    """Import generate_policy_brief without triggering DB/pipeline calls."""
    import sys
    worktree_root = Path(__file__).parent.parent.parent
    scripts_dir = str(worktree_root / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from report_generators.briefs.policy_brief import generate_policy_brief
    return generate_policy_brief


def _import_lint_vocab():
    """Import lint_vocab for vocabulary checks."""
    import sys
    worktree_root = Path(__file__).parent.parent.parent
    scripts_dir = str(worktree_root / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from report_generators.lint_vocab import load_vocab, load_replacements, load_exceptions, _compile_patterns, lint_file, iter_target_files, _is_definitional, _is_excepted
    return load_vocab, load_replacements, load_exceptions, _compile_patterns, lint_file, iter_target_files, _is_definitional, _is_excepted


class TestPolicyBriefGeneration:
    """Policy Brief generation and structure tests."""

    @pytest.fixture(scope="class")
    def brief_dict(self):
        generate = _import_generate_policy_brief()
        result = generate()
        assert result, "generate_policy_brief() returned empty result"
        return result

    def test_returns_non_empty_dict(self, brief_dict):
        assert isinstance(brief_dict, dict)
        assert len(brief_dict) > 0

    def test_metadata_present(self, brief_dict):
        assert "metadata" in brief_dict
        meta = brief_dict["metadata"]
        assert meta.get("audience") == "policy"
        assert meta.get("title")
        assert meta.get("version")

    def test_method_gates_present(self, brief_dict):
        gates = brief_dict.get("method_gates", [])
        assert len(gates) >= 3, "Policy brief requires at least 3 method gates"
        for gate in gates:
            assert gate.get("method_name")
            assert gate.get("algorithm")
            assert gate.get("confidence_interval_method")

    def test_lineage_present(self, brief_dict):
        lineage = brief_dict.get("lineage")
        assert lineage is not None
        assert lineage.get("pipeline_version")
        assert lineage.get("data_cutoff_date")
        assert lineage.get("source_tables")
        assert len(lineage["source_tables"]) >= 5

    def test_sections_present(self, brief_dict):
        sections = brief_dict.get("sections", {})
        assert len(sections) >= 6, "Policy brief requires at least 6 sections"

    def test_stance_and_disclaimer_section_present(self, brief_dict):
        sections = brief_dict.get("sections", {})
        assert "stance_and_disclaimer" in sections, (
            "stance_and_disclaimer section required by STANCE.md / REPORT_PHILOSOPHY v2"
        )
        findings = sections["stance_and_disclaimer"].get("findings", "")
        assert len(findings) > 100, "Stance/disclaimer section should have substantive content"

    def test_stance_block_content_present(self, brief_dict):
        """build_stance_block() content should appear in the brief."""
        sections = brief_dict.get("sections", {})
        stance_findings = sections.get("stance_and_disclaimer", {}).get("findings", "")
        # Check for key labor-first framing text
        assert "labor-first" in stance_findings.lower() or "労働者" in stance_findings, (
            "Stance block should contain labor-first framing language"
        )

    def test_disclaimer_content_present(self, brief_dict):
        """build_disclaimer() content should appear in the brief."""
        sections = brief_dict.get("sections", {})
        stance_findings = sections.get("stance_and_disclaimer", {}).get("findings", "")
        assert "注意事項" in stance_findings or "Note:" in stance_findings, (
            "Disclaimer block should contain bilingual notice"
        )

    def test_substantive_sections_have_findings_and_interpretation(self, brief_dict):
        sections = brief_dict.get("sections", {})
        exempt = {"stance_and_disclaimer"}
        for section_id, section in sections.items():
            if section_id in exempt:
                continue
            assert section.get("findings"), f"Section '{section_id}' missing findings"
            assert section.get("interpretation"), f"Section '{section_id}' missing interpretation"

    def test_policy_sections_include_caveat_blocks(self, brief_dict):
        """Each finding section should have a caveat block."""
        sections = brief_dict.get("sections", {})
        exempt = {"stance_and_disclaimer", "policy_recommendations"}
        caveat_found = 0
        for section_id, section in sections.items():
            if section_id in exempt:
                continue
            findings = section.get("findings", "")
            if "Caveat" in findings or "caveat" in findings:
                caveat_found += 1
        assert caveat_found >= 3, (
            "Policy brief should have caveat blocks in at least 3 finding sections"
        )

    def test_no_prohibited_vocabulary_in_section_content(self, brief_dict):
        """Check that sections don't contain prohibited vocabulary."""
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

    def test_generated_at_timestamp_present(self, brief_dict):
        assert "generated_at" in brief_dict


class TestPolicyBriefLintVocab:
    """Vocabulary lint gate test for policy_brief.py source file."""

    def test_lint_vocab_zero_violations(self):
        load_vocab, load_replacements, load_exceptions, _compile_patterns, lint_file, iter_target_files, _is_definitional, _is_excepted = _import_lint_vocab()
        brief_path = (
            Path(__file__).parent.parent.parent
            / "scripts/report_generators/briefs/policy_brief.py"
        )
        terms = load_vocab()
        replacements = load_replacements()
        exceptions = load_exceptions()
        patterns = _compile_patterns(terms)
        findings = lint_file(brief_path, patterns, replacements)
        findings = [f for f in findings if not _is_definitional(f) and not _is_excepted(f, exceptions)]
        assert len(findings) == 0, (
            f"policy_brief.py has {len(findings)} vocabulary violations: "
            + "; ".join(f.format() for f in findings[:3])
        )
