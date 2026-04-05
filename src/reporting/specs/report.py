"""ReportType enum and ReportSpec dataclass — the top-level report object."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from src.reporting.specs.section import SectionSpec


class ReportType(str, Enum):
    """Report type.

    ARGUMENTATIVE:
        A report making testable claims: a causal/attribution story that
        demands Findings, robustness checks, and competing interpretations.
        Examples: compensation_fairness, bias_detection.

    DESCRIPTIVE:
        A report that shows distributions, cross-tabs, or timelines without
        making a load-bearing causal claim. Findings are optional; only the
        minimal "academic envelope" (data_scope / methods / limitations /
        reproducibility) is required.

    REFERENCE:
        Supporting documentation (methodology compendium, glossary,
        cross-report index). Has no Findings.

    DATASET:
        A dataset card / schema specification for researchers.
    """

    ARGUMENTATIVE = "argumentative"
    DESCRIPTIVE = "descriptive"
    REFERENCE = "reference"
    DATASET = "dataset"


@dataclass(frozen=True)
class ReportSpec:
    """Top-level declarative spec for a single report.

    All display logic lives in the HTML assembler. The ``sections`` tuple is
    the source of truth for report content; validation runs over this
    structure before any rendering happens.

    Fields:
        slug: Registry identifier and filename stem
            (``result/reports/{slug}.html``).
        report_type: Drives validation strictness (see ``ReportType``).
        access_layer: 1 public, 2 person-only, 3 research, 4 restricted.
            Mirrors ``positioning_governance.md`` §2.1.
        robustness_subsamples: Subsample slugs declared by this report for
            ``run_robustness_grid`` in ``src/analysis/robustness.py``.
        data_provider_name: Dotted module path (without ``.provide``) for
            the report's data provider. Resolved by the registry.
    """

    slug: str
    title: str
    subtitle: str
    report_type: ReportType

    intro: str
    audience: str
    sections: tuple[SectionSpec, ...]

    glossary_terms: tuple[str, ...] = ()
    disclaimer_override: str = ""

    access_layer: int = 1
    robustness_subsamples: tuple[str, ...] = ()
    data_provider_name: str = ""
