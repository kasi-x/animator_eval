"""Tests for the static ReportSpec validation rules."""

from __future__ import annotations

from src.reporting.specs import (
    DataScopeInfo,
    ExplanationMeta,
    FindingSpec,
    MethodsInfo,
    ReportSpec,
    ReportType,
    ReproducibilityInfo,
    ScatterSpec,
    SectionKind,
    SectionSpec,
    StrengthLevel,
    UncertaintyInfo,
    validate,
)
from src.reporting.specs.validation import errors_only, warnings_only


def _explanation() -> ExplanationMeta:
    return ExplanationMeta(question="Q?", reading_guide="G.")


def _scatter(slug: str = "scatter1") -> ScatterSpec:
    return ScatterSpec(
        slug=slug,
        title="散布図",
        data_key="scatter",
        explanation=_explanation(),
        x_field="x",
        y_field="y",
    )


def _data_scope_section() -> SectionSpec:
    return SectionSpec(
        slug="data_scope",
        kind=SectionKind.DATA_SCOPE,
        title="データ範囲",
        data_scope_info=DataScopeInfo(
            original_n=100,
            final_n=80,
            filter_steps=(("drop", 80),),
            source_json_files=("scores.json",),
        ),
    )


def _methods_section() -> SectionSpec:
    return SectionSpec(
        slug="methods",
        kind=SectionKind.METHODS,
        title="方法",
        methods_info=MethodsInfo(
            code_references=("src/analysis/scoring/akm.py:45",),
        ),
    )


def _repro_section() -> SectionSpec:
    return SectionSpec(
        slug="repro",
        kind=SectionKind.REPRODUCIBILITY,
        title="再現性",
        reproducibility_info=ReproducibilityInfo(inputs=("scores.json",)),
    )


def _findings_section(chart_slug: str = "scatter1") -> SectionSpec:
    return SectionSpec(
        slug="findings",
        kind=SectionKind.FINDINGS,
        title="主要な知見",
        findings=(
            FindingSpec(
                slug="F1",
                claim="テスト主張である。",
                strength=StrengthLevel.STRONG,
                evidence_chart_refs=(chart_slug,),
                competing_interpretations=("代替解釈A",),
                uncertainty=UncertaintyInfo(
                    estimate=0.5, ci_lower=0.4, ci_upper=0.6
                ),
            ),
        ),
    )


def _minimal_argumentative() -> ReportSpec:
    return ReportSpec(
        slug="test_argumentative",
        title="T",
        subtitle="S",
        report_type=ReportType.ARGUMENTATIVE,
        intro="intro",
        audience="researchers",
        sections=(
            _data_scope_section(),
            _methods_section(),
            SectionSpec(
                slug="stats",
                kind=SectionKind.DESCRIPTIVE_STATS,
                title="分布",
                charts=(_scatter(),),
            ),
            _findings_section(),
            SectionSpec(
                slug="limits",
                kind=SectionKind.LIMITATIONS,
                title="限界",
                narrative="既知の限界。",
            ),
            SectionSpec(
                slug="impl",
                kind=SectionKind.IMPLICATIONS,
                title="活用",
                narrative="活用方針。",
            ),
            _repro_section(),
        ),
    )


def test_minimal_argumentative_passes() -> None:
    results = validate(_minimal_argumentative())
    assert errors_only(results) == []


# ---------------------------------------------------------------------------
# R-1: section order
# ---------------------------------------------------------------------------
def test_r1_section_order_violation() -> None:
    spec = _minimal_argumentative()
    # Swap METHODS and FINDINGS to violate order
    sections = list(spec.sections)
    i_methods = next(i for i, s in enumerate(sections) if s.kind is SectionKind.METHODS)
    i_findings = next(i for i, s in enumerate(sections) if s.kind is SectionKind.FINDINGS)
    sections[i_methods], sections[i_findings] = sections[i_findings], sections[i_methods]
    bad = ReportSpec(**{**spec.__dict__, "sections": tuple(sections)})
    results = validate(bad)
    assert any(r.rule == "R-1" and r.is_error() for r in results)


# ---------------------------------------------------------------------------
# R-2: findings present
# ---------------------------------------------------------------------------
def test_r2_no_findings_in_argumentative() -> None:
    spec = _minimal_argumentative()
    sections = tuple(s for s in spec.sections if s.kind is not SectionKind.FINDINGS)
    bad = ReportSpec(**{**spec.__dict__, "sections": sections})
    results = validate(bad)
    assert any(r.rule == "R-2" and r.is_error() for r in results)


# ---------------------------------------------------------------------------
# R-3: finding field requirements
# ---------------------------------------------------------------------------
def test_r3_evidence_ref_missing_chart() -> None:
    spec = _minimal_argumentative()
    bad_findings = SectionSpec(
        slug="findings",
        kind=SectionKind.FINDINGS,
        title="主要な知見",
        findings=(
            FindingSpec(
                slug="F1",
                claim="テスト主張である。",
                strength=StrengthLevel.STRONG,
                evidence_chart_refs=("does_not_exist",),
                competing_interpretations=("代替解釈A",),
                uncertainty=UncertaintyInfo(
                    estimate=0.5, ci_lower=0.4, ci_upper=0.6
                ),
            ),
        ),
    )
    sections = tuple(
        bad_findings if s.kind is SectionKind.FINDINGS else s for s in spec.sections
    )
    bad = ReportSpec(**{**spec.__dict__, "sections": sections})
    results = validate(bad)
    assert any(
        r.rule == "R-3"
        and r.is_error()
        and "does_not_exist" in r.message
        for r in results
    )


def test_r3_strong_requires_competing_interpretations() -> None:
    f = FindingSpec(
        slug="F1",
        claim="主張である。",
        strength=StrengthLevel.STRONG,
        evidence_chart_refs=("scatter1",),
        # missing competing_interpretations
    )
    spec = _minimal_argumentative()
    sections = list(spec.sections)
    for i, s in enumerate(sections):
        if s.kind is SectionKind.FINDINGS:
            sections[i] = SectionSpec(
                slug=s.slug, kind=s.kind, title=s.title, findings=(f,)
            )
    bad = ReportSpec(**{**spec.__dict__, "sections": tuple(sections)})
    results = validate(bad)
    assert any(
        r.rule == "R-3" and r.is_error() and "competing" in r.message
        for r in results
    )


def test_r3_claim_empty_is_error() -> None:
    f = FindingSpec(
        slug="F1",
        claim="",
        strength=StrengthLevel.SUGGESTIVE,
        evidence_chart_refs=("scatter1",),
    )
    spec = _minimal_argumentative()
    sections = list(spec.sections)
    for i, s in enumerate(sections):
        if s.kind is SectionKind.FINDINGS:
            sections[i] = SectionSpec(
                slug=s.slug, kind=s.kind, title=s.title, findings=(f,)
            )
    bad = ReportSpec(**{**spec.__dict__, "sections": tuple(sections)})
    results = validate(bad)
    assert any(r.rule == "R-3" and r.is_error() and "claim" in r.message for r in results)


# ---------------------------------------------------------------------------
# R-4: compensation CI required
# ---------------------------------------------------------------------------
def test_r4_compensation_requires_ci() -> None:
    spec = _minimal_argumentative()
    # Rename slug to trigger R-4
    sections = list(spec.sections)
    for i, s in enumerate(sections):
        if s.kind is SectionKind.FINDINGS:
            sections[i] = SectionSpec(
                slug=s.slug,
                kind=s.kind,
                title=s.title,
                findings=(
                    FindingSpec(
                        slug="F1",
                        claim="主張である。",
                        strength=StrengthLevel.STRONG,
                        evidence_chart_refs=("scatter1",),
                        competing_interpretations=("代替",),
                        uncertainty=None,  # no CI
                    ),
                ),
            )
    bad = ReportSpec(
        **{
            **spec.__dict__,
            "slug": "compensation_fairness",
            "sections": tuple(sections),
        }
    )
    results = validate(bad)
    assert any(r.rule == "R-4" and r.is_error() for r in results)


# ---------------------------------------------------------------------------
# R-5: DATA_SCOPE requirements
# ---------------------------------------------------------------------------
def test_r5_data_scope_anime_score_used_is_error() -> None:
    bad_section = SectionSpec(
        slug="data_scope",
        kind=SectionKind.DATA_SCOPE,
        title="データ範囲",
        data_scope_info=DataScopeInfo(
            original_n=10,
            final_n=8,
            filter_steps=(("drop", 8),),
            source_json_files=("scores.json",),
            anime_score_used=True,  # forbidden
        ),
    )
    spec = _minimal_argumentative()
    sections = tuple(
        bad_section if s.kind is SectionKind.DATA_SCOPE else s for s in spec.sections
    )
    bad = ReportSpec(**{**spec.__dict__, "sections": sections})
    results = validate(bad)
    assert any(r.rule == "R-5" and r.is_error() for r in results)


# ---------------------------------------------------------------------------
# R-6: METHODS requirements
# ---------------------------------------------------------------------------
def test_r6_methods_without_code_refs() -> None:
    bad_section = SectionSpec(
        slug="methods",
        kind=SectionKind.METHODS,
        title="方法",
        methods_info=MethodsInfo(),  # no code_references
    )
    spec = _minimal_argumentative()
    sections = tuple(
        bad_section if s.kind is SectionKind.METHODS else s for s in spec.sections
    )
    bad = ReportSpec(**{**spec.__dict__, "sections": sections})
    results = validate(bad)
    assert any(r.rule == "R-6" and r.is_error() for r in results)


# ---------------------------------------------------------------------------
# R-8: descriptive envelope
# ---------------------------------------------------------------------------
def test_r8_descriptive_missing_envelope() -> None:
    spec = ReportSpec(
        slug="d",
        title="T",
        subtitle="S",
        report_type=ReportType.DESCRIPTIVE,
        intro="",
        audience="",
        sections=(),  # missing all 4 required kinds
    )
    results = validate(spec)
    r8_errors = [r for r in results if r.rule == "R-8"]
    assert len(r8_errors) == 4


# ---------------------------------------------------------------------------
# R-9: slug uniqueness
# ---------------------------------------------------------------------------
def test_r9_duplicate_chart_slug() -> None:
    spec = _minimal_argumentative()
    duplicate_scatter = _scatter("scatter1")
    new_section = SectionSpec(
        slug="extra",
        kind=SectionKind.DESCRIPTIVE_STATS,
        title="追加",
        charts=(duplicate_scatter,),
    )
    sections = spec.sections + (new_section,)
    bad = ReportSpec(**{**spec.__dict__, "sections": sections})
    results = validate(bad)
    # R-1 may also fire because DESCRIPTIVE_STATS comes after GLOSSARY; that's
    # fine. We care that R-9 detects the duplicate.
    assert any(r.rule == "R-9" and r.is_error() for r in results)


# ---------------------------------------------------------------------------
# W-1 / W-2: recommended sections
# ---------------------------------------------------------------------------
def test_w1_missing_limitations() -> None:
    spec = _minimal_argumentative()
    sections = tuple(s for s in spec.sections if s.kind is not SectionKind.LIMITATIONS)
    spec2 = ReportSpec(**{**spec.__dict__, "sections": sections})
    results = validate(spec2)
    warnings = warnings_only(results)
    assert any(r.rule == "W-1" for r in warnings)


# ---------------------------------------------------------------------------
# L-1: forbidden phrases
# ---------------------------------------------------------------------------
def test_l1_forbidden_phrase_in_finding_claim() -> None:
    spec = _minimal_argumentative()
    sections = list(spec.sections)
    for i, s in enumerate(sections):
        if s.kind is SectionKind.FINDINGS:
            sections[i] = SectionSpec(
                slug=s.slug,
                kind=s.kind,
                title=s.title,
                findings=(
                    FindingSpec(
                        slug="F1",
                        claim="この人物の能力が低い傾向がある。",
                        strength=StrengthLevel.STRONG,
                        evidence_chart_refs=("scatter1",),
                        competing_interpretations=("代替",),
                        uncertainty=UncertaintyInfo(
                            estimate=0.5, ci_lower=0.4, ci_upper=0.6
                        ),
                    ),
                ),
            )
    bad = ReportSpec(**{**spec.__dict__, "sections": tuple(sections)})
    results = validate(bad)
    assert any(r.rule == "L-1" and r.is_error() for r in results)
