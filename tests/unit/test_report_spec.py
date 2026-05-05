"""Tests for the v3 ReportSpec / MethodGate / BriefArc skeleton."""

from __future__ import annotations

import pytest

from scripts.report_generators._spec import (
    NULL_MODEL_CATALOGUE,
    BriefArc,
    CIMethod,
    DataLineage,
    Interpretation,
    InterpretationGuard,
    LimitationBlock,
    MethodGate,
    NullContrast,
    ReportSpec,
    SensitivityAxis,
    assert_valid,
    is_strict_mode,
)


def _good_method_gate() -> MethodGate:
    return MethodGate(
        name="Cox PH",
        estimator="cox_ph",
        ci=CIMethod(estimator="delta"),
        rng_seed=42,
        null=["N3", "N5"],
        limitations=[
            "right-censoring at observation end",
            "credit visibility loss confounded with overseas subcontracting",
            "credit granularity differs by era",
        ],
    )


def _good_spec(name: str = "policy_attrition") -> ReportSpec:
    return ReportSpec(
        name=name,
        audience="policy",
        claim="cohort-level visibility-loss rates differ monotonically across decades",
        identifying_assumption="visibility loss != employment exit",
        null_model=["N3", "N5"],
        method_gate=_good_method_gate(),
        sensitivity_grid=[
            SensitivityAxis(name="exit definition window",
                            values=["1y", "3y", "5y"]),
        ],
        interpretation_guard=InterpretationGuard(
            forbidden_framing=["離職率の悪化", "若手定着の課題"],
            required_alternatives=2,
        ),
        data_lineage=DataLineage(
            sources=["credits", "persons", "anime"],
            meta_table="meta_policy_attrition",
            snapshot_date="2026-04-30",
            pipeline_version="v55",
        ),
    )


# ---- catalogue ----------------------------------------------------------


def test_null_catalogue_has_seven_entries():
    assert set(NULL_MODEL_CATALOGUE) == {f"N{i}" for i in range(1, 8)}


# ---- happy path ---------------------------------------------------------


def test_valid_spec_returns_no_violations():
    spec = _good_spec()
    assert spec.validate() == []


def test_assert_valid_does_not_raise_in_default_mode(monkeypatch):
    monkeypatch.delenv("STRICT_REPORT_SPEC", raising=False)
    bad = ReportSpec(
        name="missing_claim",
        audience="policy",
        claim="",
        identifying_assumption="x",
        null_model=["N3"],
        method_gate=_good_method_gate(),
        sensitivity_grid=[],
        interpretation_guard=InterpretationGuard(
            forbidden_framing=[], required_alternatives=1,
        ),
        data_lineage=DataLineage(
            sources=["a"], meta_table="m",
            snapshot_date="2026-01-01", pipeline_version="v1",
        ),
    )
    # No raise in non-strict mode.
    assert_valid(bad)


# ---- validation errors --------------------------------------------------


def test_missing_claim_is_violation():
    spec = _good_spec()
    bad = ReportSpec(
        name=spec.name,
        audience=spec.audience,
        claim="",
        identifying_assumption=spec.identifying_assumption,
        null_model=spec.null_model,
        method_gate=spec.method_gate,
        sensitivity_grid=spec.sensitivity_grid,
        interpretation_guard=spec.interpretation_guard,
        data_lineage=spec.data_lineage,
    )
    assert any("claim" in v for v in bad.validate())


def test_missing_null_model_is_violation():
    spec = _good_spec()
    bad = ReportSpec(
        name=spec.name,
        audience=spec.audience,
        claim=spec.claim,
        identifying_assumption=spec.identifying_assumption,
        null_model=[],
        method_gate=spec.method_gate,
        sensitivity_grid=spec.sensitivity_grid,
        interpretation_guard=spec.interpretation_guard,
        data_lineage=spec.data_lineage,
    )
    assert any("null_model" in v for v in bad.validate())


def test_unknown_null_model_id_is_violation():
    gate = MethodGate(
        name="x",
        estimator="x",
        ci=CIMethod(estimator="delta"),
        rng_seed=42,
        null=["N99"],
        limitations=["a", "b", "c"],
    )
    assert any("unknown null model" in v for v in gate.validate())


def test_too_few_limitations_is_violation():
    gate = MethodGate(
        name="x",
        estimator="x",
        ci=CIMethod(estimator="delta"),
        rng_seed=42,
        null=["N3"],
        limitations=["only one"],
    )
    assert any("limitations" in v for v in gate.validate())


def test_bootstrap_without_n_resamples_is_violation():
    gate = MethodGate(
        name="x",
        estimator="x",
        ci=CIMethod(estimator="bootstrap"),  # n_resamples=None
        rng_seed=42,
        null=["N3"],
        limitations=["a", "b", "c"],
    )
    assert any("n_resamples" in v for v in gate.validate())


# ---- strict mode --------------------------------------------------------


def test_strict_mode_raises_on_invalid_spec(monkeypatch):
    monkeypatch.setenv("STRICT_REPORT_SPEC", "1")
    bad = ReportSpec(
        name="bad",
        audience="policy",
        claim="",
        identifying_assumption="x",
        null_model=["N3"],
        method_gate=_good_method_gate(),
        sensitivity_grid=[],
        interpretation_guard=InterpretationGuard(
            forbidden_framing=[], required_alternatives=1,
        ),
        data_lineage=DataLineage(
            sources=["a"], meta_table="m",
            snapshot_date="2026-01-01", pipeline_version="v1",
        ),
    )
    with pytest.raises(ValueError):
        assert_valid(bad)


def test_is_strict_mode_default_off(monkeypatch):
    monkeypatch.delenv("STRICT_REPORT_SPEC", raising=False)
    assert not is_strict_mode()


def test_is_strict_mode_truthy_values(monkeypatch):
    for v in ("1", "true", "yes"):
        monkeypatch.setenv("STRICT_REPORT_SPEC", v)
        assert is_strict_mode()


# ---- BriefArc -----------------------------------------------------------


def test_brief_arc_round_trip():
    arc = BriefArc(
        audience="policy",
        presenting_phenomena=["policy_attrition", "policy_monopsony"],
        null_contrast=[
            NullContrast(
                section_id="hhi_trends",
                observed=0.38,
                null_lo=0.001,
                null_hi=0.001,
                note="HHI baseline = uniform across studios",
            ),
        ],
        limitation_block=LimitationBlock(
            identifying_assumption_validity="visibility ≈ employment unverified",
            sensitivity_caveats=["window-width sensitivity moderate"],
        ),
        interpretation=Interpretation(
            primary_claim="market shows moderate concentration",
            primary_subject="本レポートの著者は",
            alternatives=["concentration is artifact of credit-data coverage"],
            recommendation="monitor HHI trend annually",
            recommendation_alt_value="alternatively, focus on labor mobility",
        ),
    )
    assert arc.audience == "policy"
    assert len(arc.interpretation.alternatives) >= 1
