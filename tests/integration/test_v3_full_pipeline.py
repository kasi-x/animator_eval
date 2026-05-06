"""Integration test: verify all 45 report modules carry a valid v3 SPEC.

Each parametrized case:
  1. imports the module
  2. asserts SPEC constant exists
  3. asserts SPEC.validate() returns no violations
  4. asserts SPEC.method_gate.limitations has at least 3 entries
  5. asserts SPEC.null_model contains at least one valid N1–N7 id
"""

from __future__ import annotations

import importlib

import pytest

from scripts.report_generators._spec import NULL_MODEL_CATALOGUE

# ---------------------------------------------------------------------------
# Module names — one per file in scripts/report_generators/reports/ that
# exports a SPEC constant (verified: 45 modules as of 2026-05).
# ---------------------------------------------------------------------------

_REPORT_MODULES = [
    "akm_diagnostics",
    "bias_detection",
    "biz_brief_index",
    "biz_exposure_gap",
    "biz_genre_whitespace",
    "biz_independent_unit",
    "biz_team_template",
    "biz_trust_entry",
    "bridge_analysis",
    "cohort_animation",
    "compensation_fairness",
    "cooccurrence_groups",
    "derived_params",
    "dml_causal_inference",
    "growth_scores",
    "hr_brief_index",
    "index_page",
    "industry_overview",
    "knowledge_network",
    "longitudinal_analysis",
    "madb_coverage",
    "mgmt_attrition_risk",
    "mgmt_director_mentor",
    "mgmt_studio_benchmark",
    "mgmt_succession",
    "mgmt_team_chemistry",
    "ml_clustering",
    "network_analysis",
    "network_evolution",
    "network_graph",
    "o1_gender_ceiling",
    "o2_mid_management",
    "o3_ip_dependency",
    "o4_foreign_talent",
    "o7_historical",
    "o8_soft_power",
    "person_parameter_card",
    "policy_attrition",
    "policy_brief_index",
    "policy_gender_bottleneck",
    "policy_generational_health",
    "policy_monopsony",
    "score_layers_analysis",
    "shap_explanation",
    "temporal_foresight",
]

assert len(_REPORT_MODULES) == 45, (
    f"Expected 45 report modules, got {len(_REPORT_MODULES)}"
)

_VALID_NULL_IDS = set(NULL_MODEL_CATALOGUE)


@pytest.mark.parametrize("module_name", _REPORT_MODULES)
def test_report_spec_valid(module_name: str) -> None:
    """Each report module must have a valid SPEC with no violations."""
    mod = importlib.import_module(
        f"scripts.report_generators.reports.{module_name}"
    )

    # 1. SPEC constant exists
    assert hasattr(mod, "SPEC"), (
        f"{module_name}: missing SPEC constant"
    )
    spec = mod.SPEC

    # 2. validate() returns no violations
    violations = spec.validate()
    assert violations == [], (
        f"{module_name}: SPEC.validate() violations: {violations}"
    )

    # 3. method_gate.limitations has >= 3 entries
    n_limitations = len(spec.method_gate.limitations)
    assert n_limitations >= 3, (
        f"{module_name}: SPEC.method_gate.limitations has {n_limitations} entries, need >= 3"
    )

    # 4. null_model contains at least one valid N1–N7 id
    valid_nulls = [nid for nid in spec.null_model if nid in _VALID_NULL_IDS]
    assert valid_nulls, (
        f"{module_name}: SPEC.null_model {spec.null_model!r} contains no valid N1–N7 id"
    )
