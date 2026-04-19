"""v2-compliant report generators.

Each module in this package implements a single report as a class
inheriting from BaseReportGenerator.

Usage:
    from scripts.report_generators.reports import V2_REPORT_CLASSES
    from src.database import get_connection

    with get_connection() as conn:
        for cls in V2_REPORT_CLASSES:
            report = cls(conn)
            out = report.generate()
            if out:
                print(f"  -> {out}")
"""

from .index_page import IndexPageReport
from .industry_overview import IndustryOverviewReport
# Phase 1 additional
from .career_transitions import CareerTransitionsReport
from .structural_career import StructuralCareerReport
from .industry_analysis import IndustryAnalysisReport
# Phase 2: Network reports
from .bridge_analysis import BridgeAnalysisReport
from .network_analysis import NetworkAnalysisReport
from .network_graph import NetworkGraphReport
from .network_evolution import NetworkEvolutionReport
from .knowledge_network import KnowledgeNetworkReport
from .team_analysis import TeamAnalysisReport
# Phase 3: Career & longitudinal reports
from .longitudinal_analysis import LongitudinalAnalysisReport
from .career_dynamics import CareerDynamicsReport
from .cohort_animation import CohortAnimationReport
from .growth_scores import GrowthScoresReport
from .career_friction_report import CareerFrictionReport
# Phase 4: Scoring & fairness reports
from .compensation_fairness import CompensationFairnessReport
from .bias_detection import BiasDetectionReport
from .score_layers_analysis import ScoreLayersAnalysisReport
from .ml_clustering import MLClusteringReport
from .genre_analysis import GenreAnalysisReport
from .credit_statistics import CreditStatisticsReport
# Phase 5: Technical & specialized reports
from .akm_diagnostics import AKMDiagnosticsReport
from .dml_causal_inference import DMLCausalInferenceReport
from .studio_impact import StudioImpactReport
from .shap_explanation import SHAPExplanationReport
from .temporal_foresight import TemporalForesightReport
from .compatibility import CompatibilityReport
from .cooccurrence_groups import CooccurrenceGroupsReport
from .derived_params import DerivedParamsReport
from .madb_coverage import MADBCoverageReport
from .studio_timeseries import StudioTimeseriesReport
# Phase 6: Exit & return analysis
from .exit_analysis import ExitAnalysisReport
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
# Phase 10: Person parameter card + 3 audience brief indices
from .person_parameter_card import PersonParameterCardReport
from .policy_brief_index import PolicyBriefIndexReport
from .hr_brief_index import HrBriefIndexReport
from .biz_brief_index import BizBriefIndexReport

#: All v2 report classes, in generation order
V2_REPORT_CLASSES = [
    # common: audience-agnostic landing pages
    IndexPageReport,
    IndustryOverviewReport,
    CreditStatisticsReport,
    BiasDetectionReport,
    PersonParameterCardReport,
    # common: brief index pages (audience entry points)
    PolicyBriefIndexReport,
    HrBriefIndexReport,
    BizBriefIndexReport,
    # policy brief
    PolicyAttritionReport,
    PolicyMonopsonyReport,
    PolicyGenderBottleneckReport,
    PolicyGenerationalHealthReport,
    CompensationFairnessReport,
    IndustryAnalysisReport,
    CareerTransitionsReport,
    CareerFrictionReport,
    ExitAnalysisReport,
    # hr brief
    MgmtStudioBenchmarkReport,
    MgmtDirectorMentorReport,
    MgmtAttritionRiskReport,
    MgmtSuccessionReport,
    MgmtTeamChemistryReport,
    GrowthScoresReport,
    StructuralCareerReport,
    CareerDynamicsReport,
    StudioImpactReport,
    StudioTimeseriesReport,
    TeamAnalysisReport,
    CompatibilityReport,
    # biz brief
    BizGenreWhitespaceReport,
    BizUndervaluedTalentReport,
    BizTrustEntryReport,
    BizTeamTemplateReport,
    BizIndependentUnitReport,
    GenreAnalysisReport,
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
    "CareerTransitionsReport",
    "StructuralCareerReport",
    "IndustryAnalysisReport",
    "BridgeAnalysisReport",
    "NetworkAnalysisReport",
    "NetworkGraphReport",
    "NetworkEvolutionReport",
    "KnowledgeNetworkReport",
    "TeamAnalysisReport",
    "LongitudinalAnalysisReport",
    "CareerDynamicsReport",
    "CohortAnimationReport",
    "GrowthScoresReport",
    "CareerFrictionReport",
    "CompensationFairnessReport",
    "BiasDetectionReport",
    "ScoreLayersAnalysisReport",
    "MLClusteringReport",
    "GenreAnalysisReport",
    "CreditStatisticsReport",
    "AKMDiagnosticsReport",
    "DMLCausalInferenceReport",
    "StudioImpactReport",
    "SHAPExplanationReport",
    "TemporalForesightReport",
    "CompatibilityReport",
    "CooccurrenceGroupsReport",
    "DerivedParamsReport",
    "MADBCoverageReport",
    "StudioTimeseriesReport",
    "ExitAnalysisReport",
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
    "V2_REPORT_CLASSES",
]
