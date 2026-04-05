"""Finding specification: the atomic unit of an academic-style claim.

Holds the strength level enum, the uncertainty info dataclass, and the
``FindingSpec`` dataclass itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class StrengthLevel(str, Enum):
    """Strength of a Finding's claim.

    STRONG:       Causal claim, multiple methods agree, CI is tight.
    SUGGESTIVE:   Correlational claim, single method, interpretable.
    EXPLORATORY:  Pattern-spotting, requires follow-up.
    """

    STRONG = "strong"
    SUGGESTIVE = "suggestive"
    EXPLORATORY = "exploratory"


@dataclass(frozen=True)
class UncertaintyInfo:
    """Uncertainty attached to a Finding.

    An interval (``ci_lower`` / ``ci_upper``) is required when the report is
    compensation-related (see validation rule R-4), but optional otherwise.
    """

    # Point estimate + interval
    estimate: float | None = None
    ci_lower: float | None = None
    ci_upper: float | None = None
    ci_level: float = 0.95

    # Alternative uncertainty expressions
    standard_error: float | None = None
    p_value: float | None = None

    # Sample information
    n: int | None = None
    n_bootstrap: int | None = None

    # Derivation metadata (traceability)
    method: str = ""                # e.g. "analytic_normal", "bootstrap", "delta_method"
    source_code_ref: str = ""       # e.g. "src/analysis/scoring/akm.py:234"

    def has_interval(self) -> bool:
        """Return True if both CI bounds are populated."""
        return self.ci_lower is not None and self.ci_upper is not None


@dataclass(frozen=True)
class FindingSpec:
    """A single academic-style claim with evidence, uncertainty, and falsification.

    Required:
        slug: Report-unique identifier (e.g. "F1" or "finding_studio_effect").
        claim: One-sentence propositional claim.
        strength: Strength level.
        evidence_chart_refs: Slugs of charts in the same report that serve as
            evidence.

    Strongly recommended (enforced conditionally by validation):
        justification: Short description of the metric, sample, method,
            identification assumptions.
        uncertainty: CI / SE / p / n. Required for compensation reports (R-4).

    Recommended:
        competing_interpretations: Alternative explanations.
        falsification: What observation would invalidate the claim.
        robustness_chart_refs: Slugs of charts showing robustness checks.

    Reproducibility:
        source_code_ref: Primary computation path:line.
    """

    slug: str
    claim: str
    strength: StrengthLevel
    evidence_chart_refs: tuple[str, ...]

    justification: str = ""
    uncertainty: UncertaintyInfo | None = None

    competing_interpretations: tuple[str, ...] = ()
    falsification: str = ""
    robustness_chart_refs: tuple[str, ...] = ()

    source_code_ref: str = ""
