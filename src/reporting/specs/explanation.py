"""Chart-level explanation metadata.

``ExplanationMeta`` is intentionally distinct from ``FindingSpec``:

- ``FindingSpec`` is the unit of a report-wide logical claim.
- ``ExplanationMeta`` is per-chart reading guidance (how to read this chart).

A chart may be cited as evidence for a Finding, but the explanation is kept
independent so charts can be reused across reports without rewriting claims.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ExplanationMeta:
    """Metadata describing how to read a single chart.

    Fields:
        question: The question this chart answers (required).
        reading_guide: How to read the axes / encoding (required).
        key_findings: Bullet points of notable observations.
        caveats: Reading caveats.
        context: Why this chart is needed.
        significance: The analytical significance.
        utilization: ``({"role": "...", "how": "..."}, ...)`` — role-specific
            utilization suggestions.
        glossary_keys: Keys into ``COMMON_GLOSSARY_TERMS`` that should be shown
            in the glossary of any report that embeds this chart.
    """

    question: str
    reading_guide: str
    key_findings: tuple[str, ...] = ()
    caveats: tuple[str, ...] = ()
    context: str = ""
    significance: str = ""
    utilization: tuple[dict, ...] = field(default_factory=tuple)
    glossary_keys: tuple[str, ...] = ()
