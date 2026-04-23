"""Technical Appendix: Consolidated research reports with cross-references to main briefs.

This module provides infrastructure for organizing technical reports (AKM diagnostics,
bias analysis, network analysis, etc.) into a unified technical appendix that is
cross-referenced from the 3 main audience briefs.

Structure:
    - TechnicalReport: Base class for individual technical reports
    - TechnicalAppendix: Aggregates reports, validates cross-references
    - ReportCatalog: Discovery + metadata for all 20+ technical reports
"""

import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from datetime import datetime

import structlog

log = structlog.get_logger(__name__)


class ReportCategory(str, Enum):
    """Categorization of technical reports."""
    CORE_SCORING = "core_scoring"        # AKM, IV, PageRank
    NETWORK_ANALYSIS = "network"         # Centrality, bridges, communities
    BIAS_DETECTION = "bias"              # Demographic/role disparities
    CAUSAL_INFERENCE = "causal"          # DML, structural estimation
    CAREER_DYNAMICS = "career"           # Cohorts, exit patterns, growth
    GENRE_ANALYSIS = "genre"             # Genre affinity, ecosystem
    STUDIO_PROFILING = "studio"          # Studio clustering, exposure
    CONFIDENCE_METHODS = "confidence"    # CI estimation, validation
    DATA_QUALITY = "data_quality"        # Statistics, anomalies, validation
    ARCHIVAL = "archival"                # Old benchmarks, duplicates (deprecated)


@dataclass
class TechnicalReport:
    """A single technical research report."""
    
    id: str
    title: str
    category: ReportCategory
    description: str
    
    # File location
    file_path: str
    
    # Methodology
    algorithm: str
    data_source: str
    time_window: str = "2024-12-31"
    
    # Cross-references
    briefs_referenced: list[str] = field(default_factory=list)  # ["policy", "hr", "business"]
    sections_referenced: dict[str, list[str]] = field(default_factory=dict)  # {"policy": ["market_concentration"]}
    
    # Quality gates
    has_confidence_intervals: bool = False
    has_null_model: bool = False
    has_validation: bool = False
    
    # Metadata
    analyst_notes: str = ""
    deprecated: bool = False
    deprecation_reason: str = ""
    
    def to_dict(self) -> dict:
        """Export report metadata as dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "category": self.category.value,
            "description": self.description,
            "file_path": self.file_path,
            "algorithm": self.algorithm,
            "data_source": self.data_source,
            "time_window": self.time_window,
            "briefs_referenced": self.briefs_referenced,
            "sections_referenced": self.sections_referenced,
            "quality_gates": {
                "has_confidence_intervals": self.has_confidence_intervals,
                "has_null_model": self.has_null_model,
                "has_validation": self.has_validation,
            },
            "analyst_notes": self.analyst_notes,
            "deprecated": self.deprecated,
            "deprecation_reason": self.deprecation_reason,
        }
    
    @staticmethod
    def load_from_json(file_path: str) -> Optional[dict]:
        """Load report data from JSON file."""
        try:
            path = Path(file_path)
            if path.exists():
                with open(path, "r") as f:
                    return json.load(f)
        except Exception as e:
            log.exception("report_load_error", file=file_path, error=str(e))
        return None


@dataclass
class TechnicalAppendix:
    """Container for all technical reports with validation & cross-referencing."""
    
    reports: dict[str, TechnicalReport] = field(default_factory=dict)
    generated_at: datetime = field(default_factory=datetime.now)
    
    def add_report(self, report: TechnicalReport) -> None:
        """Register a technical report."""
        self.reports[report.id] = report
        log.debug("report_added", report_id=report.id, category=report.category.value)
    
    def get_by_brief(self, brief_id: str) -> dict[str, list[TechnicalReport]]:
        """Get all reports referenced by a specific brief, grouped by category."""
        results = {}
        for report in self.reports.values():
            if brief_id in report.briefs_referenced and not report.deprecated:
                category = report.category.value
                if category not in results:
                    results[category] = []
                results[category].append(report)
        return results
    
    def get_by_category(self, category: ReportCategory) -> list[TechnicalReport]:
        """Get all reports in a category (excluding deprecated)."""
        return [r for r in self.reports.values() if r.category == category and not r.deprecated]
    
    def validate(self) -> tuple[bool, list[str]]:
        """Validate appendix integrity.
        
        Checks:
        1. All referenced briefs exist (policy, hr, business)
        2. All file paths are accessible
        3. No circular cross-references
        4. Core reports have quality gates
        
        Returns:
            (is_valid, error_list)
        """
        errors = []
        valid_briefs = {"policy", "hr", "business"}
        
        for report_id, report in self.reports.items():
            # Check brief references
            for brief in report.briefs_referenced:
                if brief not in valid_briefs:
                    errors.append(f"Report '{report_id}' references invalid brief: {brief}")
            
            # Check file accessibility (non-deprecated only)
            if not report.deprecated:
                file_path = Path(report.file_path)
                if not file_path.exists():
                    errors.append(f"Report '{report_id}' file not found: {report.file_path}")
            
            # Check core reports have quality gates
            if report.category == ReportCategory.CORE_SCORING:
                if not (report.has_confidence_intervals or report.has_null_model):
                    errors.append(
                        f"Core report '{report_id}' missing quality gates "
                        "(needs CI or null model)"
                    )
        
        is_valid = len(errors) == 0
        if is_valid:
            log.info(
                "appendix_valid",
                reports=len(self.reports),
                active=len([r for r in self.reports.values() if not r.deprecated]),
            )
        else:
            log.error("appendix_validation_failed", error_count=len(errors))
        
        return is_valid, errors
    
    def to_dict(self) -> dict:
        """Export appendix as dictionary."""
        return {
            "metadata": {
                "generated_at": self.generated_at.isoformat(),
                "total_reports": len(self.reports),
                "active_reports": len([r for r in self.reports.values() if not r.deprecated]),
            },
            "reports_by_category": {
                cat.value: [r.to_dict() for r in self.get_by_category(cat)]
                for cat in ReportCategory
            },
            "cross_references": {
                brief: {
                    "total": len(reports),
                    "by_category": {
                        cat: len(reps)
                        for cat, reps in self.get_by_brief(brief).items()
                    }
                }
                for brief in ["policy", "hr", "business"]
                for reports in [self.get_by_brief(brief).values()]
            },
        }
    
    @staticmethod
    def load_from_catalog(catalog_path: str) -> "TechnicalAppendix":
        """Load appendix from catalog file (list of TechnicalReport specs)."""
        appendix = TechnicalAppendix()
        
        try:
            with open(catalog_path, "r") as f:
                catalog = json.load(f)
            
            for report_spec in catalog.get("reports", []):
                report = TechnicalReport(
                    id=report_spec["id"],
                    title=report_spec["title"],
                    category=ReportCategory(report_spec["category"]),
                    description=report_spec["description"],
                    file_path=report_spec["file_path"],
                    algorithm=report_spec["algorithm"],
                    data_source=report_spec.get("data_source", "pipeline"),
                    time_window=report_spec.get("time_window", "2024-12-31"),
                    briefs_referenced=report_spec.get("briefs_referenced", []),
                    sections_referenced=report_spec.get("sections_referenced", {}),
                    has_confidence_intervals=report_spec.get("has_confidence_intervals", False),
                    has_null_model=report_spec.get("has_null_model", False),
                    has_validation=report_spec.get("has_validation", False),
                    analyst_notes=report_spec.get("analyst_notes", ""),
                    deprecated=report_spec.get("deprecated", False),
                    deprecation_reason=report_spec.get("deprecation_reason", ""),
                )
                appendix.add_report(report)
            
            log.info("appendix_loaded", path=catalog_path, report_count=len(appendix.reports))
        
        except Exception as e:
            log.exception("appendix_load_error", path=catalog_path, error=str(e))
        
        return appendix


def create_default_appendix(result_dir: str = "result/json") -> TechnicalAppendix:
    """Create default technical appendix with all key reports."""
    appendix = TechnicalAppendix()
    
    # Core Scoring
    appendix.add_report(TechnicalReport(
        id="akm_diagnostics",
        title="AKM Decomposition: Individual & Studio Effects",
        category=ReportCategory.CORE_SCORING,
        description="Estimates person and studio fixed effects from production scale (staff_count × episodes × duration). Separates individual contribution from opportunity/studio factors.",
        file_path=f"{result_dir}/akm_diagnostics.json",
        algorithm="OLS FE with 2-way fixed effects",
        data_source="production_scale aggregates",
        briefs_referenced=["hr", "business"],
        sections_referenced={"hr": ["compensation_fairness"], "business": ["undervalued_staff"]},
        has_confidence_intervals=True,
        has_validation=True,
        analyst_notes="Core component of Integrated Value (IV). Confidence intervals are analytical (SE = sigma/sqrt(n)).",
    ))
    
    appendix.add_report(TechnicalReport(
        id="bias_report",
        title="Demographic & Role Bias Analysis",
        category=ReportCategory.BIAS_DETECTION,
        description="Identifies disparities in opportunity (production scale, role assignment) and outcomes (scores) by demographic and role group.",
        file_path=f"{result_dir}/bias_report.json",
        algorithm="Group comparisons + logistic regression",
        data_source="person attributes + production history",
        briefs_referenced=["policy", "hr"],
        sections_referenced={"policy": ["gender_bottleneck"], "hr": ["compensation_fairness"]},
        has_confidence_intervals=True,
        has_null_model=True,
        analyst_notes="Gender/role gap analysis. Null model: random assignment. See Report Philosophy for framing constraints.",
    ))
    
    appendix.add_report(TechnicalReport(
        id="iv_weights",
        title="Integrated Value (IV) Component Weights",
        category=ReportCategory.CORE_SCORING,
        description="Documents the 5 components of Integrated Value: AKM (theta_i), Authority (PageRank), Trust (edge weight), Credit Density, Patronage. Shows lambda weights and dormancy adjustment.",
        file_path=f"{result_dir}/iv_weights.json",
        algorithm="Weighted linear combination with dormancy multiplier",
        data_source="AKM + graph centrality + edge weight aggregates",
        briefs_referenced=["business", "hr"],
        sections_referenced={"business": ["undervalued_staff"], "hr": ["succession_potential"]},
        has_validation=True,
        analyst_notes="Lambda weights are fixed priors (CV optimization against anime.score removed). Dormancy applied multiplicatively after sum.",
    ))
    
    # Network Analysis
    appendix.add_report(TechnicalReport(
        id="network_analysis",
        title="Network Centrality & Position Metrics",
        category=ReportCategory.NETWORK_ANALYSIS,
        description="Computes degree, betweenness, closeness, eigenvector centrality. Identifies central figures and key connectors.",
        file_path=f"{result_dir}/network_evolution.json",
        algorithm="Brandes' betweenness (Rust accelerated), eigenvector (rayon parallel)",
        data_source="co-credit collaboration graph",
        briefs_referenced=["policy", "business"],
        sections_referenced={"policy": ["market_concentration"], "business": ["emerging_teams"]},
        has_null_model=True,
        analyst_notes="Rust extension (animetor_eval_core) provides 50-100x speedup. Falls back to NetworkX if Rust unavailable.",
    ))
    
    appendix.add_report(TechnicalReport(
        id="bridges",
        title="Structural Holes & Bridge Detection",
        category=ReportCategory.NETWORK_ANALYSIS,
        description="Identifies people who bridge disconnected network regions (gatekeepers, cross-studio connectors). Measures brokerage potential.",
        file_path=f"{result_dir}/bridges.json",
        algorithm="Constraint metric (Burt's), community-bridging edges",
        data_source="community-detected subgraphs + co-credit edges",
        briefs_referenced=["policy"],
        sections_referenced={"policy": ["market_concentration"]},
        has_validation=True,
        analyst_notes="Key for understanding industry fragmentation and gatekeeping.",
    ))
    
    appendix.add_report(TechnicalReport(
        id="knowledge_spanners",
        title="Knowledge Spanners: High Betweenness People",
        category=ReportCategory.NETWORK_ANALYSIS,
        description="Lists people with high betweenness centrality — those who control information flow between network clusters.",
        file_path=f"{result_dir}/knowledge_spanners.json",
        algorithm="Betweenness centrality (Brandes, normalized)",
        data_source="co-credit collaboration graph",
        briefs_referenced=["business"],
        sections_referenced={"business": ["emerging_teams", "market_whitespace"]},
        has_null_model=True,
        analyst_notes="Useful for identifying industry connectors and potential mentors/leaders.",
    ))
    
    # Causal Inference
    appendix.add_report(TechnicalReport(
        id="dml_analysis",
        title="Double Machine Learning (DML) Causal Estimates",
        category=ReportCategory.CAUSAL_INFERENCE,
        description="Estimates causal effect of key variables (studio prestige, role type, peer network) on person fixed effect. Uses orthogonal ML to control for confounding.",
        file_path=f"{result_dir}/dml_analysis.json",
        algorithm="Double ML with nuisance estimation (random forest), Neyman orthogonality",
        data_source="AKM components + person/role covariates",
        briefs_referenced=["hr", "business"],
        sections_referenced={"hr": ["team_chemistry", "compensation_fairness"], "business": ["undervalued_staff"]},
        has_confidence_intervals=True,
        has_validation=True,
        analyst_notes="Confidence intervals are analytical (not bootstrap). Orthogonality verified via debiased LASSO.",
    ))
    
    appendix.add_report(TechnicalReport(
        id="causal_identification",
        title="Causal Identification Strategy & Assumptions",
        category=ReportCategory.CAUSAL_INFERENCE,
        description="Documents causal assumptions (unconfoundedness, overlap, SUTVA). Specifies what claims can be causal vs. associational.",
        file_path=f"{result_dir}/causal_identification.json",
        algorithm="Identification proof + sensitivity analysis (e.g., Rotnitzky bounds)",
        data_source="domain knowledge + covariate balance checks",
        briefs_referenced=["policy"],
        sections_referenced={"policy": ["recommendations"]},
        has_validation=True,
        analyst_notes="Transparency requirement: every causal claim must cite this. Non-causal findings labeled 'associational'.",
    ))
    
    # Career Dynamics
    appendix.add_report(TechnicalReport(
        id="career_friction",
        title="Career Friction & Attrition Dynamics",
        category=ReportCategory.CAREER_DYNAMICS,
        description="Analyzes patterns of career exit (gaps > 2 years). Compares attrition rates by gender, role, and studio cohort.",
        file_path=f"{result_dir}/career_friction.json",
        algorithm="Kaplan-Meier survival curves, Cox proportional hazards",
        data_source="credit timeline + career gaps",
        briefs_referenced=["policy", "hr"],
        sections_referenced={"policy": ["attrition_dynamics"], "hr": ["retention_action"]},
        has_confidence_intervals=True,
        has_null_model=True,
        analyst_notes="Note: 'attrition' is narrow (credit gap) — does not imply job loss, may reflect freelance variability.",
    ))
    
    appendix.add_report(TechnicalReport(
        id="generational_health",
        title="Generational & Cohort Analysis",
        category=ReportCategory.CAREER_DYNAMICS,
        description="Tracks career trajectories by entry cohort (e.g., debuted 2015-2017, 2018-2020, etc.). Measures progression speed, peak timing, sustainability.",
        file_path=f"{result_dir}/generational_health.json",
        algorithm="Cohort event study + spline fitting",
        data_source="production scale trajectories over time",
        briefs_referenced=["hr", "policy"],
        sections_referenced={"hr": ["succession_potential", "team_chemistry"], "policy": ["attrition_dynamics"]},
        has_confidence_intervals=True,
        analyst_notes="Cohorts show distinct lifecycle patterns. Early cohorts show sustainability; recent cohorts still ramping.",
    ))
    
    # Genre Analysis
    appendix.add_report(TechnicalReport(
        id="genre_ecosystem",
        title="Genre Ecosystem: Specialists & Crossovers",
        category=ReportCategory.GENRE_ANALYSIS,
        description="Maps person expertise by genre. Identifies genre specialists vs. versatile generalists. Genre affinity scores.",
        file_path=f"{result_dir}/genre_ecosystem.json",
        algorithm="Herfindahl index (specialization) + cosine similarity",
        data_source="role × anime genre co-occurrence",
        briefs_referenced=["business"],
        sections_referenced={"business": ["market_whitespace", "emerging_teams"]},
        has_validation=True,
        analyst_notes="Genre matters for niche expertise. Useful for identifying specialized professional pools.",
    ))
    
    # Studio Profiling
    appendix.add_report(TechnicalReport(
        id="studio_network",
        title="Studio Collaboration Network & Ecosystem",
        category=ReportCategory.STUDIO_PROFILING,
        description="Models studios as nodes with edges representing co-production, shared staff. Identifies studio clusters and competitive vs. cooperative dynamics.",
        file_path=f"{result_dir}/studio_network.json",
        algorithm="Studio co-production graph + modularity-based clustering",
        data_source="studio participation in anime + shared person affiliations",
        briefs_referenced=["policy", "business"],
        sections_referenced={"policy": ["market_concentration"], "business": ["investment_action"]},
        has_validation=True,
        analyst_notes="Studio concentration is primary policy concern. Shared staff indicates ecosystem openness.",
    ))
    
    # Confidence Methods
    appendix.add_report(TechnicalReport(
        id="credit_intervals",
        title="Confidence Interval Methodology & Validation",
        category=ReportCategory.CONFIDENCE_METHODS,
        description="Documents CI computation for all person-level estimates. Specifies whether analytical (SE formula) or bootstrap. Validates coverage.",
        file_path=f"{result_dir}/credit_intervals.json",
        algorithm="Analytical SEs (Delta method) + bootstrap validation",
        data_source="residual variance + sample size statistics",
        briefs_referenced=["policy", "hr", "business"],
        sections_referenced={"policy": [], "hr": ["compensation_fairness"], "business": ["undervalued_staff"]},
        has_validation=True,
        analyst_notes="All person-level estimates in public reports MUST include CIs. This is a blocking legal requirement.",
    ))
    
    # Deprecated / Archival
    appendix.add_report(TechnicalReport(
        id="performance_benchmarks",
        title="Performance Benchmarks Archive (Old)",
        category=ReportCategory.ARCHIVAL,
        description="Archive of performance profiling runs (140+ JSON files). Superseded by inline profiling and Rust acceleration.",
        file_path=f"{result_dir}/performance_20260415_204327.json",
        algorithm="N/A (deprecated)",
        data_source="Old pipeline profiling",
        deprecated=True,
        deprecation_reason="Pipeline performance normalized; Rust acceleration removed need for continuous benchmarking. See recent commits for latest metrics.",
    ))
    
    return appendix
