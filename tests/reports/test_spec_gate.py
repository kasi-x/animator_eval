"""Tests for Phase 5 ReportSpec strict mode gate and make_default_spec.

Coverage:
- STRICT_REPORT_SPEC=1 causes assert_valid() to raise ValueError for
  an invalid spec.
- Non-strict mode (env absent / "0") lets the same invalid spec pass.
- make_default_spec provides correct audience-specific forbidden_framing
  defaults for all four audience types.
- BriefArc.to_html() emits all four 段 sections.
"""

from __future__ import annotations

import pytest

from scripts.report_generators._spec import (
    BriefArc,
    CIMethod,
    DataLineage,
    InterpretationGuard,
    Interpretation,
    LimitationBlock,
    MethodGate,
    NullContrast,
    ReportSpec,
    assert_valid,
    is_strict_mode,
    make_default_spec,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _invalid_spec() -> ReportSpec:
    """Spec that fails validation: claim missing, no null model, <3 limitations."""
    return ReportSpec(
        name="test_invalid",
        audience="policy",
        claim="",                       # violation: claim missing
        identifying_assumption="ok",
        null_model=[],                  # violation: null_model required
        method_gate=MethodGate(
            name="test_invalid",
            estimator="descriptive",
            ci=CIMethod(estimator="analytical_se"),
            rng_seed=42,
            null=[],
            limitations=["only one"],  # violation: <3 limitations
        ),
        sensitivity_grid=[],
        interpretation_guard=InterpretationGuard(
            forbidden_framing=[],
            required_alternatives=1,
        ),
        data_lineage=DataLineage(
            sources=["credits"],
            meta_table="meta_test",
            snapshot_date="2026-05-01",
            pipeline_version="v55",
        ),
    )


def _valid_spec() -> ReportSpec:
    """Minimal spec that passes validation."""
    return make_default_spec(
        name="test_valid",
        audience="policy",
        claim="テスト主張: 構造的観測事実X",
        sources=["credits", "persons"],
        meta_table="meta_test_valid",
        null_model=["N3"],
        identifying_assumption="クレジット可視性が参加の代理指標となる。",
    )


# ---------------------------------------------------------------------------
# Strict mode toggle
# ---------------------------------------------------------------------------


def test_strict_mode_on_with_env_1(monkeypatch):
    monkeypatch.setenv("STRICT_REPORT_SPEC", "1")
    assert is_strict_mode() is True


def test_strict_mode_off_without_env(monkeypatch):
    monkeypatch.delenv("STRICT_REPORT_SPEC", raising=False)
    assert is_strict_mode() is False


def test_strict_mode_off_with_env_0(monkeypatch):
    monkeypatch.setenv("STRICT_REPORT_SPEC", "0")
    assert is_strict_mode() is False


# ---------------------------------------------------------------------------
# assert_valid: strict mode raises for invalid spec
# ---------------------------------------------------------------------------


def test_assert_valid_strict_raises_for_invalid(monkeypatch):
    """STRICT_REPORT_SPEC=1 + invalid spec → ValueError."""
    monkeypatch.setenv("STRICT_REPORT_SPEC", "1")
    spec = _invalid_spec()
    with pytest.raises(ValueError, match=r"ReportSpec\[test_invalid\]"):
        assert_valid(spec)


def test_assert_valid_strict_passes_for_valid(monkeypatch):
    """STRICT_REPORT_SPEC=1 + valid spec → no exception."""
    monkeypatch.setenv("STRICT_REPORT_SPEC", "1")
    spec = _valid_spec()
    assert_valid(spec)  # must not raise


# ---------------------------------------------------------------------------
# assert_valid: non-strict mode never raises
# ---------------------------------------------------------------------------


def test_assert_valid_non_strict_does_not_raise_for_invalid(monkeypatch):
    """Non-strict mode (env absent) must not raise even for invalid spec."""
    monkeypatch.delenv("STRICT_REPORT_SPEC", raising=False)
    spec = _invalid_spec()
    assert_valid(spec)  # must not raise


def test_assert_valid_non_strict_env_0(monkeypatch):
    """STRICT_REPORT_SPEC=0 is non-strict, must not raise."""
    monkeypatch.setenv("STRICT_REPORT_SPEC", "0")
    spec = _invalid_spec()
    assert_valid(spec)  # must not raise


# ---------------------------------------------------------------------------
# make_default_spec: audience-specific forbidden_framing defaults
# ---------------------------------------------------------------------------


def test_make_default_spec_policy_forbidden_framing():
    spec = make_default_spec(
        name="p", audience="policy", claim="claim",
        sources=["credits"], meta_table="meta_p",
    )
    forbidden = spec.interpretation_guard.forbidden_framing
    assert "離職率の悪化" in forbidden
    assert "業界の危機" in forbidden


def test_make_default_spec_hr_forbidden_framing():
    spec = make_default_spec(
        name="h", audience="hr", claim="claim",
        sources=["credits"], meta_table="meta_h",
    )
    forbidden = spec.interpretation_guard.forbidden_framing
    assert "能力不足" in forbidden
    assert "優秀人材" in forbidden


def test_make_default_spec_biz_forbidden_framing():
    spec = make_default_spec(
        name="b", audience="biz", claim="claim",
        sources=["credits"], meta_table="meta_b",
    )
    forbidden = spec.interpretation_guard.forbidden_framing
    assert "過小評価" in forbidden
    assert "原石" in forbidden


def test_make_default_spec_technical_appendix_forbidden_framing():
    spec = make_default_spec(
        name="t", audience="technical_appendix", claim="claim",
        sources=["credits"], meta_table="meta_t",
    )
    forbidden = spec.interpretation_guard.forbidden_framing
    assert "ground truth" in forbidden
    assert "正解" in forbidden


def test_make_default_spec_custom_forbidden_framing_overrides_default():
    """Explicitly passed forbidden_framing replaces the audience default."""
    spec = make_default_spec(
        name="c", audience="policy", claim="claim",
        sources=["credits"], meta_table="meta_c",
        forbidden_framing=["custom_term"],
    )
    assert spec.interpretation_guard.forbidden_framing == ["custom_term"]


def test_make_default_spec_default_limitations_count():
    """Default spec has ≥ 3 limitations so validate() passes."""
    spec = _valid_spec()
    violations = spec.validate()
    assert violations == []


# ---------------------------------------------------------------------------
# BriefArc.to_html: all four 段 sections emitted
# ---------------------------------------------------------------------------


def _sample_brief_arc() -> BriefArc:
    return BriefArc(
        audience="policy",
        presenting_phenomena=["policy_attrition", "policy_monopsony"],
        null_contrast=[
            NullContrast(
                section_id="attrition",
                observed=0.35,
                null_lo=0.10,
                null_hi=0.20,
                note="outside null",
            )
        ],
        limitation_block=LimitationBlock(
            identifying_assumption_validity="可視クレジットのみ、離職の直接観測はない",
            sensitivity_caveats=["window 1yr: 結論変化なし", "window 3yr: 方向維持"],
            shrinkage_order_changes="top10 順序変化 2 件",
        ),
        interpretation=Interpretation(
            primary_claim="クレジット喪失率は null より高密度で存在する",
            primary_subject="本レポートの著者は",
            alternatives=["景気循環の可能性", "制作形態変化の可能性"],
            recommendation="3年ウィンドウでの追跡分析",
            recommendation_alt_value="短期視点では政策不要とも読める",
        ),
    )


def test_brief_arc_to_html_emits_all_four_sections():
    arc = _sample_brief_arc()
    html = arc.to_html()
    # 段 1
    assert 'id="arc-phenomena"' in html
    assert "段 1" in html
    # 段 2
    assert 'id="arc-null-contrast"' in html
    assert "段 2" in html
    # 段 3
    assert 'id="arc-limitations"' in html
    assert "段 3" in html
    # 段 4
    assert 'id="arc-interpretation"' in html
    assert "段 4" in html


def test_brief_arc_to_html_phenomena_links():
    arc = _sample_brief_arc()
    html = arc.to_html()
    assert "policy_attrition.html" in html
    assert "policy_monopsony.html" in html


def test_brief_arc_to_html_null_contrast_row():
    arc = _sample_brief_arc()
    html = arc.to_html()
    assert "0.3500" in html
    assert "0.1000" in html
    assert "外側" in html


def test_brief_arc_to_html_alternatives():
    arc = _sample_brief_arc()
    html = arc.to_html()
    assert "景気循環の可能性" in html
    assert "制作形態変化の可能性" in html


def test_brief_arc_to_html_recommendation():
    arc = _sample_brief_arc()
    html = arc.to_html()
    assert "3年ウィンドウでの追跡分析" in html
    assert "短期視点では政策不要とも読める" in html
