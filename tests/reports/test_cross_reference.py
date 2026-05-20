"""Tests for scripts/report_generators/cross_reference."""

from __future__ import annotations

import pytest

from scripts.report_generators.cross_reference import (
    REPORT_LINKS,
    build_cross_reference_block,
    find_reports_without_cross_refs,
)


def test_returns_empty_for_unknown_report():
    assert build_cross_reference_block("does_not_exist") == ""


def test_returns_html_for_known_report():
    html = build_cross_reference_block("equity_oaxaca")
    assert "Cross-references" in html
    # Contains at least 1 link
    assert "<a href=" in html


def test_grouping_by_kind():
    html = build_cross_reference_block("equity_oaxaca")
    assert "関連レポート" in html
    # equity_oaxaca has both related and opposing
    assert "反対視点" in html


def test_audit_finds_unregistered():
    all_reports = ["equity_oaxaca", "new_unregistered", "another_unregistered"]
    missing = find_reports_without_cross_refs(all_reports)
    assert missing == ["new_unregistered", "another_unregistered"]


def test_audit_empty_when_all_registered():
    all_reports = ["equity_oaxaca", "bridge_analysis"]
    missing = find_reports_without_cross_refs(all_reports)
    assert missing == []


def test_html_includes_section_id():
    html = build_cross_reference_block("equity_oaxaca")
    assert 'id="cross-ref"' in html


def test_caveat_kind_renders():
    html = build_cross_reference_block("compensation_fairness")
    # has caveat link
    assert "注意" in html


def test_all_link_targets_well_formed():
    # All REPORT_LINKS targets should be valid report-name slugs
    for src, links in REPORT_LINKS.items():
        for link in links:
            assert link.target, f"empty target in {src}"
            assert link.kind in ("related", "opposing", "caveat")
            assert link.reason, f"empty reason in {src} -> {link.target}"
