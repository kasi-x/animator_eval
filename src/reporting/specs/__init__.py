"""Report specification dataclasses.

Public API re-exports. Internal modules may import from the concrete files
directly if they need to avoid circular imports.
"""

from src.reporting.specs.card import StatCardSpec, TableSpec
from src.reporting.specs.chart import (
    BarSpec,
    BoxSpec,
    ChartSpec,
    ForestSpec,
    HeatmapSpec,
    HistogramSpec,
    LineSpec,
    RidgeSpec,
    SankeySpec,
    ScatterSpec,
    ViolinSpec,
)
from src.reporting.specs.explanation import ExplanationMeta
from src.reporting.specs.finding import (
    FindingSpec,
    StrengthLevel,
    UncertaintyInfo,
)
from src.reporting.specs.info import (
    DataScopeInfo,
    MethodsInfo,
    ReproducibilityInfo,
)
from src.reporting.specs.report import ReportSpec, ReportType
from src.reporting.specs.section import SectionKind, SectionSpec
from src.reporting.specs.validation import ValidationError, validate

__all__ = [
    "BarSpec",
    "BoxSpec",
    "ChartSpec",
    "DataScopeInfo",
    "ExplanationMeta",
    "FindingSpec",
    "ForestSpec",
    "HeatmapSpec",
    "HistogramSpec",
    "LineSpec",
    "MethodsInfo",
    "ReportSpec",
    "ReportType",
    "ReproducibilityInfo",
    "RidgeSpec",
    "SankeySpec",
    "ScatterSpec",
    "SectionKind",
    "SectionSpec",
    "StatCardSpec",
    "StrengthLevel",
    "TableSpec",
    "UncertaintyInfo",
    "ValidationError",
    "ViolinSpec",
    "validate",
]
