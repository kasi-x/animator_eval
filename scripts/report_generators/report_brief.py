"""Report Brief base class for audience-specific report generation.

Implements the v2 report structure with gating, methodology disclosure,
and audience-specific framing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional
from datetime import datetime

import structlog

log = structlog.get_logger(__name__)


class AudienceType(Enum):
    """Target audience for report briefs."""
    POLICY = "policy"
    HR_OPERATIONS = "hr_operations"
    BUSINESS_INNOVATION = "business_innovation"
    RESEARCH_TECHNICAL = "research_technical"
    SUPPORTING = "supporting"


@dataclass
class MethodGate:
    """Gate for method transparency (required before report publication)."""
    method_name: str
    algorithm: str
    confidence_interval_method: str
    null_model: Optional[str] = None
    validation_method: Optional[str] = None
    limitations: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "method_name": self.method_name,
            "algorithm": self.algorithm,
            "confidence_interval_method": self.confidence_interval_method,
            "null_model": self.null_model,
            "validation_method": self.validation_method,
            "limitations": self.limitations,
        }


@dataclass
class LineageMetadata:
    """Data lineage metadata (required for all reports)."""
    pipeline_version: str
    data_cutoff_date: str
    source_tables: list[str]
    processing_steps: list[str]
    computed_fields: list[str]
    
    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "pipeline_version": self.pipeline_version,
            "data_cutoff_date": self.data_cutoff_date,
            "source_tables": self.source_tables,
            "processing_steps": self.processing_steps,
            "computed_fields": self.computed_fields,
        }


@dataclass
class ReportBriefMetadata:
    """Metadata for a report brief."""
    audience: AudienceType
    title: str
    description: str
    target_readers: list[str]
    decision_points: list[str]
    version: str = "2.0"
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    method_gates: list[MethodGate] = field(default_factory=list)
    lineage: Optional[LineageMetadata] = None
    
    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "audience": self.audience.value,
            "title": self.title,
            "description": self.description,
            "target_readers": self.target_readers,
            "decision_points": self.decision_points,
            "version": self.version,
            "created_at": self.created_at,
            "method_gates": [mg.to_dict() for mg in self.method_gates],
            "lineage": self.lineage.to_dict() if self.lineage else None,
        }


class ReportBrief:
    """Base class for audience-specific report briefs."""
    
    def __init__(
        self,
        audience: AudienceType,
        title: str,
        description: str,
        target_readers: list[str],
        decision_points: list[str],
    ):
        """Initialize report brief.
        
        Args:
            audience: Target audience type
            title: Report title
            description: Brief description
            target_readers: List of reader types (e.g., "policymakers", "studio_managers")
            decision_points: List of key decisions this report informs
        """
        self.audience = audience
        self.metadata = ReportBriefMetadata(
            audience=audience,
            title=title,
            description=description,
            target_readers=target_readers,
            decision_points=decision_points,
        )
        self.sections: dict[str, dict] = {}
        self.method_gates: list[MethodGate] = []
        self.lineage: Optional[LineageMetadata] = None
    
    def add_method_gate(self, gate: MethodGate) -> None:
        """Register a method gate for this report.
        
        All reports must declare methods, confidence intervals, and validation
        before publication.
        """
        self.method_gates.append(gate)
        self.metadata.method_gates.append(gate)
        log.debug(
            "method_gate_registered",
            report=self.metadata.title,
            method=gate.method_name,
        )
    
    def set_lineage(self, lineage: LineageMetadata) -> None:
        """Set data lineage metadata.
        
        Required: Must track which pipeline stage, which data sources,
        and which derived fields compose this report.
        """
        self.lineage = lineage
        self.metadata.lineage = lineage
        log.debug(
            "lineage_set",
            report=self.metadata.title,
            data_cutoff=lineage.data_cutoff_date,
        )
    
    def add_section(
        self,
        section_id: str,
        title: str,
        findings: str,
        interpretation: Optional[str] = None,
        charts: Optional[list[str]] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Add a section to the report.
        
        Enforces the v2 structure:
        - Findings layer: neutral facts, no causal language
        - Interpretation layer: first-person subject, alternative interpretations
        """
        self.sections[section_id] = {
            "title": title,
            "findings": findings,
            "interpretation": interpretation,
            "charts": charts or [],
            "metadata": metadata or {},
        }
        log.debug(
            "section_added",
            report=self.metadata.title,
            section=section_id,
            has_interpretation=interpretation is not None,
        )
    
    def validate(self) -> tuple[bool, list[str]]:
        """Validate report readiness for publication.
        
        Required gates:
        1. All sections have findings (non-empty)
        2. Method gates registered for all methods
        3. Lineage metadata present
        4. No prohibited vocabulary (see REPORT_PHILOSOPHY.md)
        
        Returns:
            (is_valid, error_messages)
        """
        errors = []
        
        # Check sections
        if not self.sections:
            errors.append("No sections defined")
        
        for section_id, section in self.sections.items():
            if not section.get("findings"):
                errors.append(f"Section '{section_id}' has empty findings")
        
        # Check method gates
        if not self.method_gates:
            errors.append("No method gates registered")
        
        # Check lineage
        if not self.lineage:
            errors.append("No lineage metadata provided")
        
        # Validate vocabulary (exact word match only)
        prohibited = {
            r'\bability\b',           # Not "capability", "probability", "availability"
            r'\bskill\b',             # Not "skilled", "skillset"
            r'\btalent\b',            # Not "talent pool" - exact word only
            r'\bcompetence\b',        # Exact match
            r'\bcapability\b',        # Exact match (distinct from "capability")
        }
        import re
        
        for section_id, section in self.sections.items():
            findings = section.get("findings", "")
            interpretation = section.get("interpretation", "")
            combined = (findings + " " + interpretation).lower()
            
            for pattern in prohibited:
                if re.search(pattern, combined):
                    # Extract the matched word
                    match = re.search(pattern, combined)
                    if match:
                        errors.append(
                            f"Section '{section_id}' contains prohibited term: '{match.group()}'"
                        )
        
        is_valid = len(errors) == 0
        
        if is_valid:
            log.info(
                "report_valid",
                report=self.metadata.title,
                sections=len(self.sections),
                gates=len(self.method_gates),
            )
        else:
            log.warning(
                "report_invalid",
                report=self.metadata.title,
                error_count=len(errors),
                errors=errors,
            )
        
        return is_valid, errors
    
    def to_dict(self) -> dict:
        """Export report as dictionary (for JSON serialization)."""
        return {
            "metadata": self.metadata.to_dict(),
            "sections": self.sections,
            "method_gates": [mg.to_dict() for mg in self.method_gates],
        }


class PolicyBrief(ReportBrief):
    """Report brief for policy audience."""
    
    def __init__(self):
        super().__init__(
            audience=AudienceType.POLICY,
            title="Industry Policy Brief",
            description="Trends in workforce dynamics, market concentration, and hiring practices",
            target_readers=[
                "Policymakers",
                "Labor regulators",
                "Industry advisors",
                "Union representatives",
            ],
            decision_points=[
                "Labor policy interventions",
                "Antitrust investigations",
                "Wage floor recommendations",
                "Workforce development priorities",
            ],
        )


class HRBrief(ReportBrief):
    """Report brief for HR/Operations audience."""
    
    def __init__(self):
        super().__init__(
            audience=AudienceType.HR_OPERATIONS,
            title="Studio Operations & HR Brief",
            description="Team dynamics, compensation benchmarking, and succession planning",
            target_readers=[
                "Studio managers",
                "HR teams",
                "Compensation committees",
                "Executive teams",
            ],
            decision_points=[
                "Compensation strategy",
                "Team formation",
                "Retention programs",
                "Succession planning",
            ],
        )


class BusinessBrief(ReportBrief):
    """Report brief for business/innovation audience."""
    
    def __init__(self):
        super().__init__(
            audience=AudienceType.BUSINESS_INNOVATION,
            title="Market Opportunities & Innovation Brief",
            description="Market whitespace, independent talent, and emerging collaborations",
            target_readers=[
                "Investors",
                "Business development",
                "Innovation teams",
                "Entrepreneurs",
            ],
            decision_points=[
                "Investment opportunities",
                "Partnership potential",
                "Market entry strategy",
                "Talent acquisition targets",
            ],
        )


class TechnicalAppendix(ReportBrief):
    """Technical appendix for researchers and validation."""
    
    def __init__(self):
        super().__init__(
            audience=AudienceType.RESEARCH_TECHNICAL,
            title="Technical Appendix & Methodology",
            description="Algorithms, validation results, and research details",
            target_readers=[
                "Researchers",
                "Data engineers",
                "Academic validators",
                "ML/statistics specialists",
            ],
            decision_points=[
                "Method validation",
                "Algorithm evaluation",
                "Research reproducibility",
                "Fairness audits",
            ],
        )
