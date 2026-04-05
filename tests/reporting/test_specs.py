"""Smoke tests: every dataclass can be constructed and round-trips its fields."""

from __future__ import annotations

import pytest

from src.reporting.specs import (
    BarSpec,
    BoxSpec,
    DataScopeInfo,
    ExplanationMeta,
    FindingSpec,
    ForestSpec,
    HeatmapSpec,
    HistogramSpec,
    LineSpec,
    MethodsInfo,
    ReportSpec,
    ReportType,
    ReproducibilityInfo,
    RidgeSpec,
    SankeySpec,
    ScatterSpec,
    SectionKind,
    SectionSpec,
    StatCardSpec,
    StrengthLevel,
    TableSpec,
    UncertaintyInfo,
    ViolinSpec,
)


def _minimal_explanation() -> ExplanationMeta:
    return ExplanationMeta(
        question="何を見せるチャートか",
        reading_guide="x 軸=〜、y 軸=〜。色=〜。",
    )


def test_explanation_meta_defaults() -> None:
    exp = _minimal_explanation()
    assert exp.question
    assert exp.reading_guide
    assert exp.key_findings == ()
    assert exp.caveats == ()
    assert exp.utilization == ()


def test_uncertainty_has_interval() -> None:
    u_no = UncertaintyInfo(estimate=0.42)
    u_yes = UncertaintyInfo(estimate=0.42, ci_lower=0.39, ci_upper=0.45)
    assert not u_no.has_interval()
    assert u_yes.has_interval()


def test_finding_spec_construction() -> None:
    f = FindingSpec(
        slug="F1",
        claim="テスト主張である。",
        strength=StrengthLevel.STRONG,
        evidence_chart_refs=("chart_a",),
        uncertainty=UncertaintyInfo(
            estimate=0.5, ci_lower=0.4, ci_upper=0.6, method="bootstrap"
        ),
        competing_interpretations=("反対解釈A",),
    )
    assert f.strength is StrengthLevel.STRONG
    assert f.uncertainty is not None
    assert f.uncertainty.has_interval()


@pytest.mark.parametrize(
    "spec_cls,kwargs",
    [
        (
            ScatterSpec,
            {
                "slug": "s1",
                "title": "散布図",
                "data_key": "scatter",
                "x_field": "x",
                "y_field": "y",
            },
        ),
        (
            BarSpec,
            {
                "slug": "b1",
                "title": "棒",
                "data_key": "bar",
                "category_field": "name",
                "value_field": "v",
            },
        ),
        (
            ForestSpec,
            {"slug": "f1", "title": "フォレスト", "data_key": "forest"},
        ),
        (
            ViolinSpec,
            {
                "slug": "v1",
                "title": "バイオリン",
                "data_key": "violin",
                "group_field": "g",
                "value_field": "v",
            },
        ),
        (
            RidgeSpec,
            {
                "slug": "r1",
                "title": "リッジ",
                "data_key": "ridge",
                "group_field": "g",
                "value_field": "v",
            },
        ),
        (
            HeatmapSpec,
            {"slug": "h1", "title": "ヒート", "data_key": "heat"},
        ),
        (
            LineSpec,
            {
                "slug": "l1",
                "title": "ライン",
                "data_key": "line",
                "x_field": "t",
                "y_field": "v",
            },
        ),
        (
            SankeySpec,
            {"slug": "snk1", "title": "サンキー", "data_key": "sankey"},
        ),
        (
            BoxSpec,
            {
                "slug": "box1",
                "title": "箱ひげ",
                "data_key": "box",
                "group_field": "g",
                "value_field": "v",
            },
        ),
        (
            HistogramSpec,
            {
                "slug": "hist1",
                "title": "ヒスト",
                "data_key": "hist",
                "value_field": "v",
            },
        ),
    ],
)
def test_chart_specs_construct(spec_cls, kwargs) -> None:
    spec = spec_cls(explanation=_minimal_explanation(), **kwargs)
    assert spec.slug == kwargs["slug"]
    assert spec.title


def test_stat_card_and_table() -> None:
    card = StatCardSpec(label="人数", value_field="total_persons")
    table = TableSpec(
        slug="t1",
        title="一覧",
        data_key="rows",
        columns=(("name", "名前"), ("score", "スコア")),
    )
    assert card.value_format == "{:,.0f}"
    assert table.sortable is True
    assert table.searchable is False


def test_data_scope_info_defaults() -> None:
    ds = DataScopeInfo(
        original_n=100,
        final_n=80,
        filter_steps=(("drop < 2", 90), ("drop < 5", 80)),
        source_json_files=("scores.json",),
    )
    assert ds.anime_score_used is False
    assert ds.time_range is None


def test_methods_info_and_reproducibility() -> None:
    m = MethodsInfo(
        equations=(("Eq1", r"y = x"),),
        code_references=("src/analysis/scoring/akm.py:45",),
    )
    r = ReproducibilityInfo(inputs=("scores.json",))
    assert m.code_references
    assert r.inputs


def test_section_spec_kind_dispatch() -> None:
    data_scope_section = SectionSpec(
        slug="data_scope",
        kind=SectionKind.DATA_SCOPE,
        title="データ範囲",
        data_scope_info=DataScopeInfo(
            original_n=10,
            final_n=8,
            filter_steps=(("drop", 8),),
            source_json_files=("x.json",),
        ),
    )
    assert data_scope_section.kind is SectionKind.DATA_SCOPE
    assert data_scope_section.data_scope_info is not None


def test_report_spec_construction() -> None:
    spec = ReportSpec(
        slug="test_report",
        title="テスト",
        subtitle="サブ",
        report_type=ReportType.DESCRIPTIVE,
        intro="hi",
        audience="researchers",
        sections=(),
        access_layer=1,
    )
    assert spec.slug == "test_report"
    assert spec.report_type is ReportType.DESCRIPTIVE
    assert spec.access_layer == 1
