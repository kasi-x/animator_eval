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
from .person_ranking import PersonRankingReport
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
from .expected_ability_report import ExpectedAbilityReport
from .anime_value_report import AnimeValueReport
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

#: All v2 report classes, in generation order
V2_REPORT_CLASSES = [
    # Phase 1: High-visibility
    IndexPageReport,
    IndustryOverviewReport,
    PersonRankingReport,
    CareerTransitionsReport,
    StructuralCareerReport,
    IndustryAnalysisReport,
    # Phase 2: Network
    BridgeAnalysisReport,
    NetworkAnalysisReport,
    NetworkGraphReport,
    NetworkEvolutionReport,
    KnowledgeNetworkReport,
    TeamAnalysisReport,
    # Phase 3: Career & longitudinal
    LongitudinalAnalysisReport,
    CareerDynamicsReport,
    CohortAnimationReport,
    GrowthScoresReport,
    CareerFrictionReport,
    # Phase 4: Scoring & fairness
    CompensationFairnessReport,
    BiasDetectionReport,
    ScoreLayersAnalysisReport,
    ExpectedAbilityReport,
    AnimeValueReport,
    MLClusteringReport,
    GenreAnalysisReport,
    CreditStatisticsReport,
    # Phase 5: Technical & specialized
    AKMDiagnosticsReport,
    DMLCausalInferenceReport,
    StudioImpactReport,
    SHAPExplanationReport,
    TemporalForesightReport,
    CompatibilityReport,
    CooccurrenceGroupsReport,
    DerivedParamsReport,
    MADBCoverageReport,
    StudioTimeseriesReport,
    # Phase 6: Exit & return
    ExitAnalysisReport,
]

__all__ = [
    "IndexPageReport",
    "IndustryOverviewReport",
    "PersonRankingReport",
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
    "ExpectedAbilityReport",
    "AnimeValueReport",
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
    "V2_REPORT_CLASSES",
]
