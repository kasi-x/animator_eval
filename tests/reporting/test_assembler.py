"""Tests for the HTML assembler.

These tests verify that ``assemble()`` produces a valid HTML document from
a minimal ReportSpec + data dict, without hitting any legacy report code
that requires an actual ``result/json/`` directory.
"""

from __future__ import annotations

import pytest

from src.reporting.assemblers.html import assemble
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
    StatCardSpec,
    StrengthLevel,
    TableSpec,
    UncertaintyInfo,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _explanation() -> ExplanationMeta:
    return ExplanationMeta(question="何を見せるか?", reading_guide="x=横, y=縦。")


def _scatter(slug: str = "scatter1") -> ScatterSpec:
    return ScatterSpec(
        slug=slug,
        title="散布図",
        data_key="scatter",
        explanation=_explanation(),
        x_field="x",
        y_field="y",
    )


def _minimal_spec() -> ReportSpec:
    """An ARGUMENTATIVE spec that passes validation."""
    return ReportSpec(
        slug="test_asm",
        title="テスト",
        subtitle="サブタイトル",
        report_type=ReportType.ARGUMENTATIVE,
        intro="導入文。",
        audience="研究者",
        sections=(
            SectionSpec(
                slug="data_scope",
                kind=SectionKind.DATA_SCOPE,
                title="データ範囲",
                data_scope_info=DataScopeInfo(
                    original_n=100,
                    final_n=80,
                    filter_steps=(("除外", 80),),
                    source_json_files=("scores.json",),
                ),
            ),
            SectionSpec(
                slug="methods",
                kind=SectionKind.METHODS,
                title="方法",
                methods_info=MethodsInfo(
                    code_references=("src/analysis/scoring/akm.py:45",),
                ),
            ),
            SectionSpec(
                slug="stats",
                kind=SectionKind.DESCRIPTIVE_STATS,
                title="分布",
                charts=(_scatter(),),
            ),
            SectionSpec(
                slug="findings",
                kind=SectionKind.FINDINGS,
                title="主要な知見",
                findings=(
                    FindingSpec(
                        slug="F1",
                        claim="分析対象の分布は正規分布に従う。",
                        strength=StrengthLevel.STRONG,
                        evidence_chart_refs=("scatter1",),
                        competing_interpretations=("代替解釈A",),
                        uncertainty=UncertaintyInfo(
                            estimate=0.5,
                            ci_lower=0.4,
                            ci_upper=0.6,
                            method="bootstrap",
                        ),
                    ),
                ),
            ),
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
            SectionSpec(
                slug="repro",
                kind=SectionKind.REPRODUCIBILITY,
                title="再現性",
                reproducibility_info=ReproducibilityInfo(
                    inputs=("scores.json",),
                ),
            ),
        ),
    )


def _sample_data() -> dict:
    return {
        "scatter": [
            {"x": 1, "y": 2},
            {"x": 3, "y": 4},
            {"x": 5, "y": 6},
        ],
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_assemble_minimal() -> None:
    """A minimal valid spec produces an HTML document."""
    html = assemble(_minimal_spec(), _sample_data())
    assert "<!DOCTYPE html>" in html
    assert "<html" in html
    assert "テスト" in html
    assert "サブタイトル" in html


def test_html_contains_section_ids() -> None:
    html = assemble(_minimal_spec(), _sample_data())
    assert 'id="section-data_scope"' in html
    assert 'id="section-methods"' in html
    assert 'id="section-findings"' in html


def test_html_contains_finding_card() -> None:
    html = assemble(_minimal_spec(), _sample_data())
    assert "finding-card" in html
    assert "F1" in html
    assert "分析対象の分布は正規分布に従う" in html


def test_html_contains_uncertainty() -> None:
    html = assemble(_minimal_spec(), _sample_data())
    # CI values
    assert "0.4000" in html
    assert "0.6000" in html
    assert "bootstrap" in html


def test_html_contains_katex() -> None:
    html = assemble(_minimal_spec(), _sample_data())
    assert "katex" in html.lower()


def test_html_contains_data_scope_flow() -> None:
    html = assemble(_minimal_spec(), _sample_data())
    assert "100" in html  # original_n
    assert "分析対象" in html


def test_html_contains_methods_code_ref() -> None:
    html = assemble(_minimal_spec(), _sample_data())
    assert "akm.py" in html


def test_html_contains_chart() -> None:
    html = assemble(_minimal_spec(), _sample_data())
    # plotly_div_safe embeds a div with the chart id
    assert "chart-scatter1" in html


def test_assemble_validation_error() -> None:
    """A spec with validation errors raises ValueError."""
    bad = ReportSpec(
        slug="bad",
        title="T",
        subtitle="S",
        report_type=ReportType.ARGUMENTATIVE,
        intro="x",
        audience="x",
        sections=(),  # Missing required sections
    )
    with pytest.raises(ValueError, match="validation errors"):
        assemble(bad, {})


def test_assemble_skip_validation() -> None:
    """skip_validation=True allows invalid specs through."""
    bad = ReportSpec(
        slug="bad",
        title="T",
        subtitle="S",
        report_type=ReportType.ARGUMENTATIVE,
        intro="x",
        audience="x",
        sections=(),
    )
    html = assemble(bad, {}, skip_validation=True)
    assert "<!DOCTYPE html>" in html


def test_hidden_section_omitted() -> None:
    spec = _minimal_spec()
    # Make a new section list with a hidden section
    sections = list(spec.sections) + [
        SectionSpec(
            slug="hidden_sec",
            kind=SectionKind.NARRATIVE,
            title="隠しセクション",
            narrative="見えない。",
            hidden=True,
        ),
    ]
    new_spec = ReportSpec(**{**spec.__dict__, "sections": tuple(sections)})
    html = assemble(new_spec, _sample_data())
    assert "隠しセクション" not in html


def test_accordion_section() -> None:
    spec = _minimal_spec()
    sections = list(spec.sections) + [
        SectionSpec(
            slug="acc_sec",
            kind=SectionKind.NARRATIVE,
            title="折りたたみ",
            narrative="内容。",
            accordion=True,
        ),
    ]
    new_spec = ReportSpec(**{**spec.__dict__, "sections": tuple(sections)})
    html = assemble(new_spec, _sample_data())
    assert "<details" in html
    assert "<summary>" in html


def test_stat_cards_rendered() -> None:
    spec = _minimal_spec()
    sections = list(spec.sections) + [
        SectionSpec(
            slug="cards_sec",
            kind=SectionKind.CARD_GROUP,
            title="カード",
            cards=(StatCardSpec(label="人数", value_field="total_persons"),),
        ),
    ]
    new_spec = ReportSpec(**{**spec.__dict__, "sections": tuple(sections)})
    data = {**_sample_data(), "total_persons": 12345}
    html = assemble(new_spec, data)
    assert "12,345" in html
    assert "人数" in html


def test_l2_disclaimer_present() -> None:
    """L-2: the DISCLAIMER text must appear in the final HTML."""
    from src.reporting.renderers.html_primitives import DISCLAIMER

    html = assemble(_minimal_spec(), _sample_data())
    assert DISCLAIMER in html


def test_table_rendered() -> None:
    spec = _minimal_spec()
    sections = list(spec.sections) + [
        SectionSpec(
            slug="tbl_sec",
            kind=SectionKind.TABLE_GROUP,
            title="一覧",
            tables=(
                TableSpec(
                    slug="t1",
                    title="テスト表",
                    data_key="rows",
                    columns=(("name", "名前"), ("score", "スコア")),
                ),
            ),
        ),
    ]
    new_spec = ReportSpec(**{**spec.__dict__, "sections": tuple(sections)})
    data = {**_sample_data(), "rows": [{"name": "太郎", "score": 99}]}
    html = assemble(new_spec, data)
    assert "太郎" in html
    assert "99" in html
    assert "テスト表" in html
