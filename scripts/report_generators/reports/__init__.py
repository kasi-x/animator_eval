"""v2-compliant report generators.

Each module in this package implements a single report as a class
inheriting from BaseReportGenerator.

Phase 3-5 (2026-04-19) consolidation:
  - 12 reports moved into scripts/report_generators/reports/archived/ and
    removed from V2_REPORT_CLASSES. See docs/REPORT_INVENTORY.md for the
    full merge map. Archived modules remain importable for regeneration
    but are not run by default.

Usage:
    from scripts.report_generators.reports import V2_REPORT_CLASSES
    from src.db import get_connection

    with get_connection() as conn:
        for cls in V2_REPORT_CLASSES:
            report = cls(conn)
            out = report.generate()
            if out:
                print(f"  -> {out}")
"""

from .index_page import IndexPageReport
from .industry_overview import IndustryOverviewReport
# Phase 2: Network reports
from .bridge_analysis import BridgeAnalysisReport
from .network_analysis import NetworkAnalysisReport
from .network_graph import NetworkGraphReport
from .network_evolution import NetworkEvolutionReport
from .knowledge_network import KnowledgeNetworkReport
# Phase 3: Career & longitudinal reports
from .longitudinal_analysis import LongitudinalAnalysisReport
from .cohort_animation import CohortAnimationReport
from .growth_scores import GrowthScoresReport
# Phase 4: Scoring & fairness reports
from .compensation_fairness import CompensationFairnessReport
from .bias_detection import BiasDetectionReport
from .score_layers_analysis import ScoreLayersAnalysisReport
from .ml_clustering import MLClusteringReport
# Phase 5: Technical & specialized reports
from .akm_diagnostics import AKMDiagnosticsReport
from .dml_causal_inference import DMLCausalInferenceReport
from .shap_explanation import SHAPExplanationReport
from .temporal_foresight import TemporalForesightReport
from .cooccurrence_groups import CooccurrenceGroupsReport
from .derived_params import DerivedParamsReport
from .madb_coverage import MADBCoverageReport
# Phase 7: Policy briefs
from .policy_attrition import PolicyAttritionReport
from .policy_monopsony import PolicyMonopsonyReport
from .policy_gender_bottleneck import PolicyGenderBottleneckReport
from .policy_generational_health import PolicyGenerationalHealthReport
# Phase 8: Management (hr) briefs
from .mgmt_studio_benchmark import MgmtStudioBenchmarkReport
from .mgmt_director_mentor import MgmtDirectorMentorReport
from .mgmt_attrition_risk import MgmtAttritionRiskReport
from .mgmt_succession import MgmtSuccessionReport
from .mgmt_team_chemistry import MgmtTeamChemistryReport
# Phase 9: Biz briefs
from .biz_undervalued_talent import BizUndervaluedTalentReport
from .biz_genre_whitespace import BizGenreWhitespaceReport
from .biz_team_template import BizTeamTemplateReport
from .biz_trust_entry import BizTrustEntryReport
from .biz_independent_unit import BizIndependentUnitReport
# Extension reports (15_extension_reports card group)
from .o3_ip_dependency import O3IpDependencyReport
# Phase 10: Person parameter card + 3 audience brief indices
from .person_parameter_card import PersonParameterCardReport
from .policy_brief_index import PolicyBriefIndexReport
from .hr_brief_index import HrBriefIndexReport
from .biz_brief_index import BizBriefIndexReport

#: All v2 report classes, in generation order
#:
#: Audience brief body count (policy + hr + biz) = 16, total active = 37.
#: Target per detailed_todo §4.6: ≤20 audience-brief reports — met.
V2_REPORT_CLASSES = [
    # common: audience-agnostic landing pages
    IndexPageReport,
    IndustryOverviewReport,
    BiasDetectionReport,
    PersonParameterCardReport,
    # common: brief index pages (audience entry points)
    PolicyBriefIndexReport,
    HrBriefIndexReport,
    BizBriefIndexReport,
    # policy brief (5)
    PolicyAttritionReport,
    PolicyMonopsonyReport,
    PolicyGenderBottleneckReport,
    PolicyGenerationalHealthReport,
    CompensationFairnessReport,
    # hr brief (6)
    MgmtStudioBenchmarkReport,
    MgmtDirectorMentorReport,
    MgmtAttritionRiskReport,
    MgmtSuccessionReport,
    MgmtTeamChemistryReport,
    GrowthScoresReport,
    # biz brief (5 + O3 extension)
    BizGenreWhitespaceReport,
    BizUndervaluedTalentReport,
    BizTrustEntryReport,
    BizTeamTemplateReport,
    BizIndependentUnitReport,
    O3IpDependencyReport,
    # technical appendix
    AKMDiagnosticsReport,
    DMLCausalInferenceReport,
    ScoreLayersAnalysisReport,
    SHAPExplanationReport,
    LongitudinalAnalysisReport,
    MLClusteringReport,
    NetworkAnalysisReport,
    NetworkGraphReport,
    NetworkEvolutionReport,
    CooccurrenceGroupsReport,
    MADBCoverageReport,
    DerivedParamsReport,
    CohortAnimationReport,
    KnowledgeNetworkReport,
    TemporalForesightReport,
    BridgeAnalysisReport,
]

__all__ = [
    "IndexPageReport",
    "IndustryOverviewReport",
    "BridgeAnalysisReport",
    "NetworkAnalysisReport",
    "NetworkGraphReport",
    "NetworkEvolutionReport",
    "KnowledgeNetworkReport",
    "LongitudinalAnalysisReport",
    "CohortAnimationReport",
    "GrowthScoresReport",
    "CompensationFairnessReport",
    "BiasDetectionReport",
    "ScoreLayersAnalysisReport",
    "MLClusteringReport",
    "AKMDiagnosticsReport",
    "DMLCausalInferenceReport",
    "SHAPExplanationReport",
    "TemporalForesightReport",
    "CooccurrenceGroupsReport",
    "DerivedParamsReport",
    "MADBCoverageReport",
    "PolicyAttritionReport",
    "PolicyMonopsonyReport",
    "PolicyGenderBottleneckReport",
    "PolicyGenerationalHealthReport",
    "MgmtStudioBenchmarkReport",
    "MgmtDirectorMentorReport",
    "MgmtAttritionRiskReport",
    "MgmtSuccessionReport",
    "MgmtTeamChemistryReport",
    "BizUndervaluedTalentReport",
    "BizGenreWhitespaceReport",
    "BizTeamTemplateReport",
    "BizTrustEntryReport",
    "BizIndependentUnitReport",
    "PersonParameterCardReport",
    "PolicyBriefIndexReport",
    "HrBriefIndexReport",
    "BizBriefIndexReport",
    "O3IpDependencyReport",
    "V2_REPORT_CLASSES",
]
