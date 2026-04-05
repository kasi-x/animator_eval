"""SectionKind enum and SectionSpec dataclass.

Sections are typed: the ``kind`` field determines which content fields are
meaningful and which are required (enforced by ``validation.py``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from src.reporting.specs.card import StatCardSpec, TableSpec
from src.reporting.specs.chart import ChartSpec
from src.reporting.specs.finding import FindingSpec

if TYPE_CHECKING:
    from src.reporting.specs.info import (
        DataScopeInfo,
        MethodsInfo,
        ReproducibilityInfo,
    )


class SectionKind(str, Enum):
    """Section types.

    The first block is the fixed ARGUMENTATIVE order; the second block is for
    free-form DESCRIPTIVE / REFERENCE sections that have no ordering
    constraint.
    """

    # Fixed order for ARGUMENTATIVE reports
    ABSTRACT = "abstract"
    RESEARCH_QUESTION = "research_question"
    HYPOTHESES = "hypotheses"
    DATA_SCOPE = "data_scope"
    METHODS = "methods"
    DESCRIPTIVE_STATS = "descriptive_stats"
    FINDINGS = "findings"
    ROBUSTNESS = "robustness"
    INTERPRETATION = "interpretation"
    LIMITATIONS = "limitations"
    IMPLICATIONS = "implications"
    OPEN_QUESTIONS = "open_questions"
    REPRODUCIBILITY = "reproducibility"
    GLOSSARY = "glossary"

    # Free-form / descriptive
    NARRATIVE = "narrative"
    CHART_GROUP = "chart_group"
    TABLE_GROUP = "table_group"
    CARD_GROUP = "card_group"


#: Canonical order of ARGUMENTATIVE section kinds. Reports may omit
#: intermediate sections (validation rule W-1/W-2 warns for missing
#: ``LIMITATIONS`` / ``IMPLICATIONS``), but may not reorder them.
ARGUMENTATIVE_SECTION_ORDER: tuple[SectionKind, ...] = (
    SectionKind.ABSTRACT,
    SectionKind.RESEARCH_QUESTION,
    SectionKind.HYPOTHESES,
    SectionKind.DATA_SCOPE,
    SectionKind.METHODS,
    SectionKind.DESCRIPTIVE_STATS,
    SectionKind.FINDINGS,
    SectionKind.ROBUSTNESS,
    SectionKind.INTERPRETATION,
    SectionKind.LIMITATIONS,
    SectionKind.IMPLICATIONS,
    SectionKind.OPEN_QUESTIONS,
    SectionKind.REPRODUCIBILITY,
    SectionKind.GLOSSARY,
)


@dataclass(frozen=True)
class SectionSpec:
    """A single section inside a ReportSpec.

    The ``kind`` dictates which of the content fields (``narrative``,
    ``findings``, ``charts``, etc.) are meaningful. The validation module
    enforces these kind/content invariants.

    Display controls:
        hidden: Skip the section in HTML output (useful for L1/L3 access
            layer splits where some sections are research-only).
        accordion: Render as a collapsible block.
    """

    slug: str
    kind: SectionKind
    title: str

    narrative: str = ""
    findings: tuple[FindingSpec, ...] = ()
    charts: tuple[ChartSpec, ...] = field(default_factory=tuple)
    tables: tuple[TableSpec, ...] = ()
    cards: tuple[StatCardSpec, ...] = ()

    # kind-specific structured content (resolved at validation time)
    data_scope_info: "DataScopeInfo | None" = None
    methods_info: "MethodsInfo | None" = None
    reproducibility_info: "ReproducibilityInfo | None" = None

    hidden: bool = False
    accordion: bool = False
