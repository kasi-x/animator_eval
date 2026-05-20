"""Tests for scripts/report_generators/briefs/executive_summary."""

from __future__ import annotations

import pytest

from scripts.report_generators.briefs.executive_summary import (
    ExecutiveSummary,
    KeyFinding,
    build_executive_summary,
    filter_findings_passing_coverage,
    filter_findings_with_ci_excludes_zero,
    format_value_with_ci,
    rank_findings_by_abs_value,
    render_executive_summary_html,
)


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------


class TestFormatValueWithCI:
    def test_positive_value_with_ci(self):
        f = KeyFinding(
            metric_label="gap", value=0.25, unit="log credits",
            ci_low=0.1, ci_high=0.4, direction="+",
        )
        s = format_value_with_ci(f)
        assert "+0.250" in s
        assert "[+0.100, +0.400]" in s

    def test_no_ci(self):
        f = KeyFinding(metric_label="g", value=0.5, unit="HR")
        s = format_value_with_ci(f)
        assert "0.500" in s
        assert "HR" in s
        assert "CI" not in s

    def test_negative_value(self):
        f = KeyFinding(
            metric_label="g", value=-0.3, unit="d",
            ci_low=-0.5, ci_high=-0.1, direction="-",
        )
        s = format_value_with_ci(f)
        assert "-0.300" in s


# ---------------------------------------------------------------------------
# build_executive_summary
# ---------------------------------------------------------------------------


class TestBuildSummary:
    def test_empty_findings(self):
        s = build_executive_summary("policy", "policymakers", [])
        assert s.brief_id == "policy"
        assert s.findings == ()

    def test_aggregates_method_gates(self):
        findings = [
            KeyFinding(metric_label="x", value=1.0, unit="d", method_gate="bootstrap CI"),
            KeyFinding(metric_label="y", value=2.0, unit="d", method_gate="bootstrap CI"),
            KeyFinding(metric_label="z", value=3.0, unit="HR", method_gate="permutation null"),
        ]
        s = build_executive_summary("hr", "managers", findings)
        assert s.method_gate_summary == {"bootstrap CI": 2, "permutation null": 1}

    def test_aggregates_coverage_caveats(self):
        findings = [
            KeyFinding(metric_label="x", value=1.0, unit="d", coverage_caveat="low gender"),
            KeyFinding(metric_label="y", value=2.0, unit="d"),  # no caveat
        ]
        s = build_executive_summary("biz", "investors", findings)
        assert len(s.coverage_warnings) == 1
        assert "low gender" in s.coverage_warnings[0]


# ---------------------------------------------------------------------------
# Render HTML
# ---------------------------------------------------------------------------


class TestRenderHTML:
    def test_empty_findings_message(self):
        s = build_executive_summary("policy", "policymakers", [])
        html = render_executive_summary_html(s)
        assert "headline finding を抽出可能な水準" in html

    def test_findings_listed(self):
        findings = [
            KeyFinding(
                metric_label="gender gap", value=-0.3, unit="log credits",
                ci_low=-0.5, ci_high=-0.1, source_report="equity_oaxaca",
                method_gate="bootstrap CI", direction="-",
            ),
        ]
        s = build_executive_summary("policy", "policymakers", findings)
        html = render_executive_summary_html(s)
        assert "gender gap" in html
        assert "equity_oaxaca" in html
        assert "bootstrap CI" in html

    def test_html_includes_section_id(self):
        s = build_executive_summary("policy", "policymakers", [])
        html = render_executive_summary_html(s)
        assert 'id="exec-policy"' in html


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------


class TestFilters:
    def test_rank_by_abs_value_returns_top_k(self):
        findings = [
            KeyFinding(metric_label=f"f{i}", value=float(i - 5), unit="d")
            for i in range(10)
        ]
        top3 = rank_findings_by_abs_value(findings, top_k=3)
        assert len(top3) == 3
        # Largest absolute values: 5 (i=0 → -5), 4 (i=9 → 4), 4 (i=1 → -4)
        abs_values = [abs(f.value) for f in top3]
        assert abs_values[0] >= abs_values[1] >= abs_values[2]

    def test_ci_excludes_zero_filter(self):
        findings = [
            KeyFinding(metric_label="a", value=0.3, unit="d", ci_low=0.1, ci_high=0.5),  # significant
            KeyFinding(metric_label="b", value=0.1, unit="d", ci_low=-0.1, ci_high=0.3),  # crosses zero
            KeyFinding(metric_label="c", value=-0.5, unit="d", ci_low=-0.8, ci_high=-0.2),  # significant
            KeyFinding(metric_label="d", value=0.5, unit="d"),  # no CI
        ]
        filtered = filter_findings_with_ci_excludes_zero(findings)
        labels = {f.metric_label for f in filtered}
        assert labels == {"a", "c"}

    def test_coverage_pass_filter(self):
        findings = [
            KeyFinding(metric_label="a", value=0.3, unit="d", coverage_caveat=""),
            KeyFinding(metric_label="b", value=0.1, unit="d", coverage_caveat="low"),
        ]
        passing = filter_findings_passing_coverage(findings)
        assert len(passing) == 1
        assert passing[0].metric_label == "a"
