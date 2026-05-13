"""Tests for Workers Brief generator (labor-first rebrand of HR brief).

Tests cover:
- generate_hr_brief() / generate_workers_brief() returns non-empty dict
- Brief type is WorkersBrief (AudienceType.HR_OPERATIONS)
- Title is "Workers Brief" (not "Studio Operations & HR Brief")
- Target readers are worker-facing (not studio management)
- stance_and_disclaimer section present
- Worker-view sections present: structural_position, cohort_comparison,
  credit_visibility, opportunity_gap
- No studio HR optimization language
- Forbidden vocabulary 0 violations (source file)
"""

import pytest
import re
from pathlib import Path


def _import_hr_brief():
    """Import hr_brief without triggering DB/pipeline calls."""
    import sys
    worktree_root = Path(__file__).parent.parent.parent
    scripts_dir = str(worktree_root / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from report_generators.briefs.hr_brief import generate_hr_brief, generate_workers_brief
    return generate_hr_brief, generate_workers_brief


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


class TestWorkersBriefGeneration:
    """Workers Brief generation and structure tests."""

    @pytest.fixture(scope="class")
    def brief_dict(self):
        generate_hr_brief, _ = _import_hr_brief()
        result = generate_hr_brief()
        assert result, "generate_hr_brief() returned empty result"
        return result

    def test_returns_non_empty_dict(self, brief_dict):
        assert isinstance(brief_dict, dict)
        assert len(brief_dict) > 0

    def test_title_is_workers_brief(self, brief_dict):
        meta = brief_dict.get("metadata", {})
        title = meta.get("title", "")
        assert "Workers Brief" in title, (
            f"HR brief title should be 'Workers Brief', got: '{title}'"
        )

    def test_target_readers_are_worker_facing(self, brief_dict):
        meta = brief_dict.get("metadata", {})
        readers = meta.get("target_readers", [])
        readers_text = " ".join(readers).lower()
        # Should mention animators or workers or union
        assert any(w in readers_text for w in ["animator", "worker", "union", "jani"]), (
            "Workers Brief target readers should include animators, workers, or union representatives"
        )
        # Should NOT be studio management focused
        studio_mgmt_words = ["studio manager", "hr team", "compensation committee", "executive team"]
        for w in studio_mgmt_words:
            assert w.lower() not in readers_text, (
                f"Workers Brief should not target studio management (found: '{w}')"
            )

    def test_decision_points_are_worker_focused(self, brief_dict):
        meta = brief_dict.get("metadata", {})
        decision_points = meta.get("decision_points", [])
        points_text = " ".join(decision_points).lower()
        # Should mention compensation negotiation or worker advocacy
        worker_focus = ["compensation negotiation", "worker", "structural position", "credit"]
        assert any(w in points_text for w in worker_focus), (
            "Workers Brief decision points should be worker-focused"
        )

    def test_metadata_present(self, brief_dict):
        meta = brief_dict["metadata"]
        assert meta.get("audience") == "hr_operations"
        assert meta.get("version")

    def test_method_gates_present(self, brief_dict):
        gates = brief_dict.get("method_gates", [])
        assert len(gates) >= 3, "Workers brief requires at least 3 method gates"

    def test_lineage_present(self, brief_dict):
        lineage = brief_dict.get("lineage")
        assert lineage is not None
        assert lineage.get("pipeline_version")
        assert lineage.get("data_cutoff_date")

    def test_stance_and_disclaimer_section_present(self, brief_dict):
        sections = brief_dict.get("sections", {})
        assert "stance_and_disclaimer" in sections
        findings = sections["stance_and_disclaimer"].get("findings", "")
        assert "labor-first" in findings.lower() or "労働者" in findings, (
            "stance_and_disclaimer should contain labor-first framing"
        )

    def test_worker_view_sections_present(self, brief_dict):
        sections = brief_dict.get("sections", {})
        expected_sections = {
            "structural_position",
            "cohort_comparison",
            "credit_visibility",
            "opportunity_gap",
        }
        for expected in expected_sections:
            assert expected in sections, (
                f"Workers Brief missing section '{expected}' (worker-view section)"
            )

    def test_substantive_sections_have_findings_and_interpretation(self, brief_dict):
        sections = brief_dict.get("sections", {})
        exempt = {"stance_and_disclaimer"}
        for section_id, section in sections.items():
            if section_id in exempt:
                continue
            assert section.get("findings"), f"Section '{section_id}' missing findings"
            assert section.get("interpretation"), f"Section '{section_id}' missing interpretation"

    def test_labor_structural_caveats_in_interpretations(self, brief_dict):
        """Worker-view sections should include structural caveats."""
        sections = brief_dict.get("sections", {})
        caveat_or_structural = 0
        exempt = {"stance_and_disclaimer"}
        for section_id, section in sections.items():
            if section_id in exempt:
                continue
            interp = section.get("interpretation", "") or ""
            findings = section.get("findings", "") or ""
            combined = interp + findings
            if any(w in combined.lower() for w in ["caveat", "structural", "labor"]):
                caveat_or_structural += 1
        assert caveat_or_structural >= 4, (
            "Workers Brief should have labor-structural framing in at least 4 sections"
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

    def test_workers_brief_alias_function(self):
        """generate_workers_brief should be identical to generate_hr_brief."""
        generate_hr_brief, generate_workers_brief = _import_hr_brief()
        result_hr = generate_hr_brief()
        result_workers = generate_workers_brief()
        assert result_hr.get("metadata", {}).get("title") == result_workers.get("metadata", {}).get("title"), (
            "generate_workers_brief and generate_hr_brief should produce the same brief"
        )


class TestWorkersBriefLintVocab:
    """Vocabulary lint gate test for hr_brief.py source file."""

    def test_lint_vocab_zero_violations(self):
        load_vocab, load_replacements, load_exceptions, _compile_patterns, lint_file, _is_definitional, _is_excepted = _import_lint_vocab()
        brief_path = (
            Path(__file__).parent.parent.parent
            / "scripts/report_generators/briefs/hr_brief.py"
        )
        terms = load_vocab()
        replacements = load_replacements()
        exceptions = load_exceptions()
        patterns = _compile_patterns(terms)
        findings = lint_file(brief_path, patterns, replacements)
        findings = [f for f in findings if not _is_definitional(f) and not _is_excepted(f, exceptions)]
        assert len(findings) == 0, (
            f"hr_brief.py has {len(findings)} vocabulary violations: "
            + "; ".join(f.format() for f in findings[:3])
        )
