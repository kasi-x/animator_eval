"""Smoke tests for the reporting CLI."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from src.reporting.cli import app
from src.reporting.registry import clear_registry, register
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
)

runner = CliRunner()


@pytest.fixture(autouse=True)
def _clean_registry():
    clear_registry()
    yield
    clear_registry()


def _valid_spec() -> ReportSpec:
    return ReportSpec(
        slug="cli_test",
        title="CLI テスト",
        subtitle="サブ",
        report_type=ReportType.ARGUMENTATIVE,
        intro="導入",
        audience="開発者",
        sections=(
            SectionSpec(
                slug="data_scope",
                kind=SectionKind.DATA_SCOPE,
                title="DS",
                data_scope_info=DataScopeInfo(
                    original_n=10, final_n=8,
                    filter_steps=(("drop", 8),),
                    source_json_files=("x.json",),
                ),
            ),
            SectionSpec(
                slug="methods",
                kind=SectionKind.METHODS,
                title="M",
                methods_info=MethodsInfo(code_references=("src/x.py:1",)),
            ),
            SectionSpec(
                slug="stats",
                kind=SectionKind.DESCRIPTIVE_STATS,
                title="S",
                charts=(
                    ScatterSpec(
                        slug="sc1", title="scatter", data_key="sc",
                        explanation=ExplanationMeta(question="Q", reading_guide="G"),
                        x_field="x", y_field="y",
                    ),
                ),
            ),
            SectionSpec(
                slug="findings",
                kind=SectionKind.FINDINGS,
                title="F",
                findings=(
                    FindingSpec(
                        slug="F1", claim="主張。",
                        strength=StrengthLevel.SUGGESTIVE,
                        evidence_chart_refs=("sc1",),
                    ),
                ),
            ),
            SectionSpec(slug="lim", kind=SectionKind.LIMITATIONS, title="L", narrative="制約。"),
            SectionSpec(slug="imp", kind=SectionKind.IMPLICATIONS, title="I", narrative="活用。"),
            SectionSpec(
                slug="repro",
                kind=SectionKind.REPRODUCIBILITY,
                title="R",
                reproducibility_info=ReproducibilityInfo(inputs=("x.json",)),
            ),
        ),
    )


def _valid_provide(json_dir: Path) -> dict:
    return {"sc": [{"x": 1, "y": 2}]}


# ---------------------------------------------------------------------------
# list-reports
# ---------------------------------------------------------------------------

def test_list_reports_empty() -> None:
    result = runner.invoke(app, ["list-reports"])
    assert result.exit_code == 0
    assert "No reports registered" in result.output


def test_list_reports_with_entry() -> None:
    register("my_report", _valid_spec, _valid_provide)
    result = runner.invoke(app, ["list-reports"])
    assert result.exit_code == 0
    assert "my_report" in result.output


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

def test_validate_ok() -> None:
    register("cli_test", _valid_spec, _valid_provide)
    result = runner.invoke(app, ["validate", "cli_test"])
    assert result.exit_code == 0
    assert "OK" in result.output


# ---------------------------------------------------------------------------
# validate-all
# ---------------------------------------------------------------------------

def test_validate_all_empty() -> None:
    result = runner.invoke(app, ["validate-all"])
    assert result.exit_code == 0
    assert "No reports registered" in result.output


def test_validate_all_ok() -> None:
    register("cli_test", _valid_spec, _valid_provide)
    result = runner.invoke(app, ["validate-all"])
    assert result.exit_code == 0
    assert "cli_test" in result.output


# ---------------------------------------------------------------------------
# generate
# ---------------------------------------------------------------------------

def test_generate_creates_file(tmp_path: Path) -> None:
    register("cli_test", _valid_spec, _valid_provide)
    out_dir = tmp_path / "reports"
    result = runner.invoke(app, [
        "generate", "cli_test",
        "--json-dir", str(tmp_path),
        "--output-dir", str(out_dir),
    ])
    assert result.exit_code == 0
    assert (out_dir / "cli_test.html").exists()


# ---------------------------------------------------------------------------
# generate-all
# ---------------------------------------------------------------------------

def test_generate_all(tmp_path: Path) -> None:
    register("cli_test", _valid_spec, _valid_provide)
    out_dir = tmp_path / "reports"
    result = runner.invoke(app, [
        "generate-all",
        "--json-dir", str(tmp_path),
        "--output-dir", str(out_dir),
    ])
    assert result.exit_code == 0
    assert "1 generated" in result.output
