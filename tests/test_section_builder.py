"""Tests for section_builder.py method template extensions (X4 — x_cross_cutting).

Covers:
- METHOD_NOTE_TEMPLATES: all 8 required method keys present
- _render_method_templates(): HTML output and error handling
- method_note_from_lineage(method_keys=...): integration with lineage row
- Backward compatibility: method_keys=None still works
- v2 philosophy compliance: no causal/evaluative terms in templates
"""

from __future__ import annotations

import sqlite3

import pytest

from scripts.report_generators.section_builder import (
    METHOD_NOTE_TEMPLATES,
    SectionBuilder,
)

# Required method keys per X4 task card
REQUIRED_KEYS = {
    "cox",
    "mwu",
    "km",
    "counterfactual",
    "louvain",
    "propensity",
    "did",
    "weighted_pagerank",
}


# ---------------------------------------------------------------------------
# METHOD_NOTE_TEMPLATES definition checks
# ---------------------------------------------------------------------------


class TestMethodNoteTemplatesDefinition:
    """Verify all 8 required keys exist in METHOD_NOTE_TEMPLATES."""

    def test_all_required_keys_present(self):
        missing = REQUIRED_KEYS - set(METHOD_NOTE_TEMPLATES)
        assert not missing, f"Missing method template keys: {missing}"

    def test_all_templates_non_empty_html(self):
        for key, template in METHOD_NOTE_TEMPLATES.items():
            assert template.strip(), f"Template for {key!r} is empty"
            assert "<p>" in template, f"Template for {key!r} must contain a <p> element"

    def test_templates_contain_limitations_section(self):
        """Each template should mention known limitations."""
        for key, template in METHOD_NOTE_TEMPLATES.items():
            assert "限界" in template or "limitation" in template.lower(), (
                f"Template for {key!r} must reference known limitations"
            )

    def test_cox_template_mentions_proportional_hazard(self):
        assert "比例ハザード" in METHOD_NOTE_TEMPLATES["cox"]

    def test_mwu_template_mentions_rank_biserial(self):
        assert "rank-biserial" in METHOD_NOTE_TEMPLATES["mwu"]

    def test_km_template_mentions_greenwood(self):
        assert "Greenwood" in METHOD_NOTE_TEMPLATES["km"]

    def test_counterfactual_template_mentions_bootstrap(self):
        assert "Bootstrap" in METHOD_NOTE_TEMPLATES["counterfactual"] or \
               "bootstrap" in METHOD_NOTE_TEMPLATES["counterfactual"]

    def test_louvain_template_mentions_modularity(self):
        assert "モジュラリティ" in METHOD_NOTE_TEMPLATES["louvain"]

    def test_propensity_template_mentions_smd(self):
        assert "SMD" in METHOD_NOTE_TEMPLATES["propensity"]

    def test_did_template_mentions_parallel_trend(self):
        assert "平行トレンド" in METHOD_NOTE_TEMPLATES["did"]

    def test_weighted_pagerank_template_mentions_role_weight(self):
        assert "role_weight" in METHOD_NOTE_TEMPLATES["weighted_pagerank"]


# ---------------------------------------------------------------------------
# v2 philosophy compliance in templates
# ---------------------------------------------------------------------------


class TestTemplateV2Compliance:
    """Templates must not contain causal verbs or normative claims in Findings context."""

    CAUSAL_EN = ["causes", "cause", "leads to", "results in", "triggers", "drives"]
    NORMATIVE_EN = ["should", "must", "need to", "ought to"]

    def test_no_causal_verbs_in_templates(self):
        violations: list[str] = []
        for key, template in METHOD_NOTE_TEMPLATES.items():
            tl = template.lower()
            for v in self.CAUSAL_EN:
                if f" {v} " in tl or tl.startswith(v):
                    violations.append(f"{key}: contains causal verb '{v}'")
        assert not violations, "\n".join(violations)

    def test_no_normative_claims_in_templates(self):
        violations: list[str] = []
        for key, template in METHOD_NOTE_TEMPLATES.items():
            tl = template.lower()
            for v in self.NORMATIVE_EN:
                # Allow "should" only inside suggested/recommended phrases
                # that are clearly caveated; strict check for standalone usage
                if f" {v} " in tl:
                    violations.append(f"{key}: contains normative '{v}'")
        assert not violations, "\n".join(violations)


# ---------------------------------------------------------------------------
# _render_method_templates
# ---------------------------------------------------------------------------


class TestRenderMethodTemplates:
    """SectionBuilder._render_method_templates() must produce correct HTML."""

    def test_single_key_renders(self):
        html = SectionBuilder._render_method_templates(["cox"])
        assert "Cox" in html
        assert "method-templates" in html

    def test_multiple_keys_renders(self):
        html = SectionBuilder._render_method_templates(["cox", "km", "mwu"])
        assert "Cox" in html
        assert "Kaplan-Meier" in html
        assert "Mann-Whitney" in html

    def test_unknown_key_raises(self):
        with pytest.raises(ValueError, match="Unknown method_key"):
            SectionBuilder._render_method_templates(["nonexistent_key"])

    def test_unknown_key_error_lists_valid_keys(self):
        with pytest.raises(ValueError, match="Valid keys"):
            SectionBuilder._render_method_templates(["bad_key"])

    def test_all_keys_renders_without_error(self):
        html = SectionBuilder._render_method_templates(list(REQUIRED_KEYS))
        for key in REQUIRED_KEYS:
            assert METHOD_NOTE_TEMPLATES[key][:20] in html or key in html.lower()


# ---------------------------------------------------------------------------
# method_note_from_lineage(method_keys=...) integration
# ---------------------------------------------------------------------------


def _make_lineage_conn(table_name: str = "meta_test") -> sqlite3.Connection:
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
            table_name,
            "policy",
            '["credits","persons"]',
            1,
            0,
            "v1.0",
            "2026-01-01",
            "bootstrap_n1000",
            None,
            None,
            100,
            None,
        ),
    )
    return conn


class TestMethodNoteFromLineageWithMethodKeys:
    """method_note_from_lineage(method_keys=...) must include method template blocks."""

    def test_no_method_keys_backward_compat(self):
        """method_keys=None must still work (backward compatibility)."""
        conn = _make_lineage_conn()
        sb = SectionBuilder()
        html = sb.method_note_from_lineage("meta_test", conn, method_keys=None)
        assert "credits" in html
        assert "method-templates" not in html

    def test_method_keys_appended(self):
        conn = _make_lineage_conn()
        sb = SectionBuilder()
        html = sb.method_note_from_lineage("meta_test", conn, method_keys=["cox"])
        assert "Cox" in html
        assert "method-templates" in html

    def test_multiple_method_keys(self):
        conn = _make_lineage_conn()
        sb = SectionBuilder()
        html = sb.method_note_from_lineage(
            "meta_test", conn, method_keys=["mwu", "km"]
        )
        assert "Mann-Whitney" in html
        assert "Kaplan-Meier" in html

    def test_unknown_method_key_raises(self):
        conn = _make_lineage_conn()
        sb = SectionBuilder()
        with pytest.raises(ValueError, match="Unknown method_key"):
            sb.method_note_from_lineage("meta_test", conn, method_keys=["bad"])

    def test_lineage_metadata_still_present_with_keys(self):
        """Lineage source tables should still appear even when method_keys is set."""
        conn = _make_lineage_conn()
        sb = SectionBuilder()
        html = sb.method_note_from_lineage(
            "meta_test", conn, method_keys=["did"]
        )
        assert "credits" in html
        assert "persons" in html
        assert "DID" in html or "差分の差分" in html


# ---------------------------------------------------------------------------
# Empty method_keys list
# ---------------------------------------------------------------------------


class TestMethodNoteEmptyList:
    def test_empty_list_does_not_render_block(self):
        """method_keys=[] should not append a method-templates div."""
        conn = _make_lineage_conn()
        sb = SectionBuilder()
        html = sb.method_note_from_lineage("meta_test", conn, method_keys=[])
        # Empty list is falsy in Python; templates block should not appear
        assert "method-templates" not in html
