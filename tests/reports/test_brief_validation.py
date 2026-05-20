"""Brief 統合済 sections の整合性テスト。

各 brief generate 関数が:
- section count を正しく出力
- 必須 section (stance / executive_summary) を含む
- SPEC 等の cross-link が壊れていない

を検証する。
"""

from __future__ import annotations

import pytest

from scripts.report_generators.briefs.business_brief import generate_business_brief
from scripts.report_generators.briefs.hr_brief import generate_hr_brief
from scripts.report_generators.briefs.policy_brief import generate_policy_brief


@pytest.fixture(scope="module")
def policy_dict():
    return generate_policy_brief()


@pytest.fixture(scope="module")
def hr_dict():
    return generate_hr_brief()


@pytest.fixture(scope="module")
def business_dict():
    return generate_business_brief()


class TestBriefSectionCounts:
    def test_policy_has_executive_summary(self, policy_dict):
        sections = policy_dict.get("sections", {})
        assert "executive_summary" in sections

    def test_hr_has_executive_summary(self, hr_dict):
        sections = hr_dict.get("sections", {})
        assert "executive_summary" in sections

    def test_business_has_executive_summary(self, business_dict):
        sections = business_dict.get("sections", {})
        assert "executive_summary" in sections

    def test_policy_has_stance(self, policy_dict):
        assert "stance_and_disclaimer" in policy_dict.get("sections", {})

    def test_hr_has_stance(self, hr_dict):
        assert "stance_and_disclaimer" in hr_dict.get("sections", {})

    def test_business_has_stance(self, business_dict):
        assert "stance_and_disclaimer" in business_dict.get("sections", {})


class TestBriefAdditions:
    """Session 2 後半で追加された section が brief に存在することの確認。"""

    def test_policy_has_structural_fragility(self, policy_dict):
        assert "structural_fragility" in policy_dict.get("sections", {})

    def test_policy_has_opportunity_decomposition(self, policy_dict):
        assert "opportunity_decomposition" in policy_dict.get("sections", {})

    def test_hr_has_cohort_inequality(self, hr_dict):
        assert "cohort_structural_inequality" in hr_dict.get("sections", {})


class TestBriefSectionsMinCount:
    def test_policy_min_sections(self, policy_dict):
        assert len(policy_dict.get("sections", {})) >= 8

    def test_hr_min_sections(self, hr_dict):
        assert len(hr_dict.get("sections", {})) >= 6

    def test_business_min_sections(self, business_dict):
        assert len(business_dict.get("sections", {})) >= 5


class TestCrossReferenceAuditCoverage:
    """Cross-reference dict が V2 reports をすべて網羅していること。"""

    def test_no_missing_cross_refs(self):
        from scripts.report_generators.cross_reference import find_reports_without_cross_refs
        from scripts.report_generators.reports import V2_REPORT_CLASSES
        names = [c.name for c in V2_REPORT_CLASSES if getattr(c, "name", None)]
        missing = find_reports_without_cross_refs(names)
        assert not missing, f"Missing cross-ref: {missing}"


class TestSpecCoverage:
    """全 v2 report が SPEC を宣言していることの監査。"""

    def test_no_module_missing_spec(self):
        import inspect

        from scripts.report_generators._spec import assert_valid
        from scripts.report_generators.reports import V2_REPORT_CLASSES

        seen: set[str] = set()
        missing: list[str] = []
        invalid: list[tuple[str, str]] = []
        for cls in V2_REPORT_CLASSES:
            mod = inspect.getmodule(cls)
            if mod is None or mod.__name__ in seen:
                continue
            seen.add(mod.__name__)
            spec = getattr(mod, "SPEC", None)
            if spec is None:
                missing.append(mod.__name__)
                continue
            try:
                assert_valid(spec)
            except Exception as exc:
                invalid.append((mod.__name__, str(exc)))
        assert not missing, f"Missing SPEC: {missing}"
        assert not invalid, f"Invalid SPEC: {invalid}"


class TestKeyfindingsLoader:
    def test_load_returns_empty_when_absent(self, tmp_path):
        from scripts.report_generators.briefs._keyfindings_loader import load_keyfindings

        non_existent = tmp_path / "no_such_file.json"
        result = load_keyfindings("policy", path=non_existent)
        assert result == []

    def test_load_filters_invalid_entries(self, tmp_path):
        import json

        from scripts.report_generators.briefs._keyfindings_loader import (
            load_keyfindings,
        )

        p = tmp_path / "kf.json"
        p.write_text(json.dumps({
            "policy": [
                {"metric_label": "ok", "value": 0.1, "unit": "d"},
                {"value": "not-a-float"},  # invalid → skipped
            ]
        }), encoding="utf-8")
        result = load_keyfindings("policy", path=p)
        assert len(result) == 1
        assert result[0].metric_label == "ok"

    def test_write_then_load_roundtrip(self, tmp_path):

        from scripts.report_generators.briefs._keyfindings_loader import (
            load_keyfindings,
            write_keyfindings,
        )

        p = tmp_path / "kf.json"
        write_keyfindings(
            {
                "policy": [
                    dict(
                        metric_label="gap", value=0.5, unit="log",
                        ci_low=0.3, ci_high=0.7,
                        source_report="oaxaca", method_gate="bootstrap",
                        direction="+",
                    )
                ],
                "hr": [], "business": [],
            },
            path=p,
        )
        result = load_keyfindings("policy", path=p)
        assert len(result) == 1
        assert result[0].value == 0.5
        assert result[0].ci_low == 0.3
