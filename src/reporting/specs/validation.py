"""Static validation rules for ``ReportSpec`` objects.

Rule numbering follows ``schema_design.md`` §3:

Errors (R-x):
    R-1  Section order for ARGUMENTATIVE reports
    R-2  ARGUMENTATIVE must contain at least one Finding
    R-3  Each Finding has claim / strength / evidence_chart_refs;
         STRONG findings must have at least one competing interpretation;
         all evidence_chart_refs must resolve to charts in the same report.
    R-4  Compensation-related reports require CI on every Finding
    R-5  DATA_SCOPE must carry a DataScopeInfo; anime_score_used must be False
    R-6  METHODS must carry a MethodsInfo with at least one code reference
    R-7  REPRODUCIBILITY must carry a ReproducibilityInfo with inputs
    R-8  DESCRIPTIVE reports must carry the minimal academic envelope
    R-9  Slugs (chart / finding / table) must be unique within the report

Warnings (W-x):
    W-1  LIMITATIONS recommended for ARGUMENTATIVE reports
    W-2  IMPLICATIONS recommended for ARGUMENTATIVE reports
    W-3  SUGGESTIVE / EXPLORATORY findings should offer competing interpretations
    W-4  Every chart should have a non-empty question and reading_guide

Lint (L-x):
    L-1  Forbidden phrases in narrative / claim / explanation strings
    L-2  (assembler-time) DISCLAIMER present in final HTML
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from src.reporting.specs.finding import FindingSpec, StrengthLevel
from src.reporting.specs.forbidden_phrases import find_forbidden
from src.reporting.specs.report import ReportSpec, ReportType
from src.reporting.specs.section import (
    ARGUMENTATIVE_SECTION_ORDER,
    SectionKind,
    SectionSpec,
)


Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class ValidationError:
    """A single validation finding."""

    rule: str
    severity: Severity
    message: str
    section_slug: str = ""
    finding_slug: str = ""

    def is_error(self) -> bool:
        return self.severity == "error"


def validate(spec: ReportSpec) -> list[ValidationError]:
    """Run all static validation rules and return every finding.

    The caller is responsible for deciding what to do with warnings (log,
    ignore) and errors (typically abort rendering).
    """
    errors: list[ValidationError] = []
    errors.extend(_check_section_order(spec))
    errors.extend(_check_findings_present(spec))
    errors.extend(_check_finding_fields(spec))
    errors.extend(_check_compensation_ci(spec))
    errors.extend(_check_structured_sections(spec))
    errors.extend(_check_descriptive_envelope(spec))
    errors.extend(_check_slug_uniqueness(spec))
    errors.extend(_check_recommendations(spec))
    errors.extend(_check_forbidden_phrases(spec))
    return errors


# ---------------------------------------------------------------------------
# R-1: section order
# ---------------------------------------------------------------------------
def _check_section_order(spec: ReportSpec) -> list[ValidationError]:
    if spec.report_type is not ReportType.ARGUMENTATIVE:
        return []

    canonical_index = {kind: i for i, kind in enumerate(ARGUMENTATIVE_SECTION_ORDER)}
    errors: list[ValidationError] = []
    last_rank = -1
    last_seen_kind: SectionKind | None = None
    for section in spec.sections:
        rank = canonical_index.get(section.kind)
        if rank is None:
            # Free-form sections are allowed inside an ARGUMENTATIVE report
            # (e.g. a CHART_GROUP inside DESCRIPTIVE_STATS); they do not
            # advance the canonical cursor.
            continue
        if rank < last_rank:
            errors.append(
                ValidationError(
                    rule="R-1",
                    severity="error",
                    message=(
                        f"section '{section.slug}' (kind={section.kind.value}) "
                        f"appears after '{last_seen_kind.value if last_seen_kind else '?'}', "
                        f"violating canonical ARGUMENTATIVE order"
                    ),
                    section_slug=section.slug,
                )
            )
        else:
            last_rank = rank
            last_seen_kind = section.kind
    return errors


# ---------------------------------------------------------------------------
# R-2: findings present
# ---------------------------------------------------------------------------
def _check_findings_present(spec: ReportSpec) -> list[ValidationError]:
    if spec.report_type is not ReportType.ARGUMENTATIVE:
        return []
    for section in spec.sections:
        if section.kind is SectionKind.FINDINGS and section.findings:
            return []
    return [
        ValidationError(
            rule="R-2",
            severity="error",
            message=(
                "ARGUMENTATIVE report must contain at least one FINDINGS section "
                "with at least one FindingSpec"
            ),
        )
    ]


# ---------------------------------------------------------------------------
# R-3: per-finding required fields
# ---------------------------------------------------------------------------
def _collect_chart_slugs(spec: ReportSpec) -> set[str]:
    slugs: set[str] = set()
    for section in spec.sections:
        for chart in section.charts:
            slugs.add(chart.slug)
    return slugs


def _check_finding_fields(spec: ReportSpec) -> list[ValidationError]:
    errors: list[ValidationError] = []
    chart_slugs = _collect_chart_slugs(spec)

    for section in spec.sections:
        for finding in section.findings:
            errors.extend(_validate_single_finding(finding, section, chart_slugs))
    return errors


def _validate_single_finding(
    finding: FindingSpec,
    section: SectionSpec,
    chart_slugs: set[str],
) -> list[ValidationError]:
    errors: list[ValidationError] = []

    if not finding.claim or not finding.claim.strip():
        errors.append(
            ValidationError(
                rule="R-3",
                severity="error",
                message="FindingSpec.claim must be a non-empty string",
                section_slug=section.slug,
                finding_slug=finding.slug,
            )
        )
    elif not finding.claim.rstrip().endswith(("。", ".", "！", "!", "？", "?")):
        # Softer check: claims should read as complete sentences.
        errors.append(
            ValidationError(
                rule="R-3",
                severity="warning",
                message="FindingSpec.claim should end with a terminating punctuation mark",
                section_slug=section.slug,
                finding_slug=finding.slug,
            )
        )

    if not finding.evidence_chart_refs:
        errors.append(
            ValidationError(
                rule="R-3",
                severity="error",
                message="FindingSpec.evidence_chart_refs must contain at least one chart slug",
                section_slug=section.slug,
                finding_slug=finding.slug,
            )
        )
    else:
        for ref in finding.evidence_chart_refs:
            if ref not in chart_slugs:
                errors.append(
                    ValidationError(
                        rule="R-3",
                        severity="error",
                        message=(
                            f"evidence chart ref '{ref}' does not match any chart "
                            f"slug in the report"
                        ),
                        section_slug=section.slug,
                        finding_slug=finding.slug,
                    )
                )

    if finding.strength is StrengthLevel.STRONG and not finding.competing_interpretations:
        errors.append(
            ValidationError(
                rule="R-3",
                severity="error",
                message=(
                    "STRONG findings must enumerate at least one competing interpretation"
                ),
                section_slug=section.slug,
                finding_slug=finding.slug,
            )
        )

    return errors


# ---------------------------------------------------------------------------
# R-4: compensation CI requirement
# ---------------------------------------------------------------------------
_COMPENSATION_SLUG_MARKERS = ("compensation", "counterfactual", "fair_")


def _is_compensation_report(spec: ReportSpec) -> bool:
    slug = spec.slug.lower()
    return any(marker in slug for marker in _COMPENSATION_SLUG_MARKERS)


def _check_compensation_ci(spec: ReportSpec) -> list[ValidationError]:
    if not _is_compensation_report(spec):
        return []
    errors: list[ValidationError] = []
    for section in spec.sections:
        for finding in section.findings:
            if finding.uncertainty is None or not finding.uncertainty.has_interval():
                errors.append(
                    ValidationError(
                        rule="R-4",
                        severity="error",
                        message=(
                            "compensation-related reports require a confidence "
                            "interval on every Finding (CLAUDE.md legal requirement)"
                        ),
                        section_slug=section.slug,
                        finding_slug=finding.slug,
                    )
                )
    return errors


# ---------------------------------------------------------------------------
# R-5, R-6, R-7: structured sections
# ---------------------------------------------------------------------------
def _check_structured_sections(spec: ReportSpec) -> list[ValidationError]:
    errors: list[ValidationError] = []
    for section in spec.sections:
        if section.kind is SectionKind.DATA_SCOPE:
            if section.data_scope_info is None:
                errors.append(
                    ValidationError(
                        rule="R-5",
                        severity="error",
                        message="DATA_SCOPE section must carry a DataScopeInfo instance",
                        section_slug=section.slug,
                    )
                )
            elif section.data_scope_info.anime_score_used:
                errors.append(
                    ValidationError(
                        rule="R-5",
                        severity="error",
                        message=(
                            "DataScopeInfo.anime_score_used must be False "
                            "(CLAUDE.md hard constraint)"
                        ),
                        section_slug=section.slug,
                    )
                )
        elif section.kind is SectionKind.METHODS:
            info = section.methods_info
            if info is None:
                errors.append(
                    ValidationError(
                        rule="R-6",
                        severity="error",
                        message="METHODS section must carry a MethodsInfo instance",
                        section_slug=section.slug,
                    )
                )
            elif not info.code_references:
                errors.append(
                    ValidationError(
                        rule="R-6",
                        severity="error",
                        message="MethodsInfo.code_references must contain at least one entry",
                        section_slug=section.slug,
                    )
                )
        elif section.kind is SectionKind.REPRODUCIBILITY:
            info = section.reproducibility_info
            if info is None:
                errors.append(
                    ValidationError(
                        rule="R-7",
                        severity="error",
                        message="REPRODUCIBILITY section must carry a ReproducibilityInfo instance",
                        section_slug=section.slug,
                    )
                )
            elif not info.inputs:
                errors.append(
                    ValidationError(
                        rule="R-7",
                        severity="error",
                        message="ReproducibilityInfo.inputs must contain at least one entry",
                        section_slug=section.slug,
                    )
                )
    return errors


# ---------------------------------------------------------------------------
# R-8: descriptive envelope
# ---------------------------------------------------------------------------
_DESCRIPTIVE_REQUIRED_KINDS: tuple[SectionKind, ...] = (
    SectionKind.DATA_SCOPE,
    SectionKind.METHODS,
    SectionKind.LIMITATIONS,
    SectionKind.REPRODUCIBILITY,
)


def _check_descriptive_envelope(spec: ReportSpec) -> list[ValidationError]:
    if spec.report_type is not ReportType.DESCRIPTIVE:
        return []
    present = {section.kind for section in spec.sections}
    errors: list[ValidationError] = []
    for required in _DESCRIPTIVE_REQUIRED_KINDS:
        if required not in present:
            errors.append(
                ValidationError(
                    rule="R-8",
                    severity="error",
                    message=(
                        f"DESCRIPTIVE report is missing required section kind "
                        f"'{required.value}'"
                    ),
                )
            )
    return errors


# ---------------------------------------------------------------------------
# R-9: slug uniqueness
# ---------------------------------------------------------------------------
def _check_slug_uniqueness(spec: ReportSpec) -> list[ValidationError]:
    errors: list[ValidationError] = []
    chart_slugs: dict[str, str] = {}
    finding_slugs: dict[str, str] = {}
    table_slugs: dict[str, str] = {}
    section_slugs: dict[str, str] = {}

    for section in spec.sections:
        if section.slug in section_slugs:
            errors.append(
                ValidationError(
                    rule="R-9",
                    severity="error",
                    message=f"duplicate section slug '{section.slug}'",
                    section_slug=section.slug,
                )
            )
        else:
            section_slugs[section.slug] = section.slug

        for chart in section.charts:
            if chart.slug in chart_slugs:
                errors.append(
                    ValidationError(
                        rule="R-9",
                        severity="error",
                        message=(
                            f"duplicate chart slug '{chart.slug}' "
                            f"(also in section '{chart_slugs[chart.slug]}')"
                        ),
                        section_slug=section.slug,
                    )
                )
            else:
                chart_slugs[chart.slug] = section.slug

        for finding in section.findings:
            if finding.slug in finding_slugs:
                errors.append(
                    ValidationError(
                        rule="R-9",
                        severity="error",
                        message=f"duplicate finding slug '{finding.slug}'",
                        section_slug=section.slug,
                        finding_slug=finding.slug,
                    )
                )
            else:
                finding_slugs[finding.slug] = section.slug

        for table in section.tables:
            if table.slug in table_slugs:
                errors.append(
                    ValidationError(
                        rule="R-9",
                        severity="error",
                        message=f"duplicate table slug '{table.slug}'",
                        section_slug=section.slug,
                    )
                )
            else:
                table_slugs[table.slug] = section.slug

    return errors


# ---------------------------------------------------------------------------
# W-1 / W-2 / W-3 / W-4: recommendations
# ---------------------------------------------------------------------------
def _check_recommendations(spec: ReportSpec) -> list[ValidationError]:
    warnings: list[ValidationError] = []

    if spec.report_type is ReportType.ARGUMENTATIVE:
        present = {section.kind for section in spec.sections}
        if SectionKind.LIMITATIONS not in present:
            warnings.append(
                ValidationError(
                    rule="W-1",
                    severity="warning",
                    message="ARGUMENTATIVE report should include a LIMITATIONS section",
                )
            )
        if SectionKind.IMPLICATIONS not in present:
            warnings.append(
                ValidationError(
                    rule="W-2",
                    severity="warning",
                    message="ARGUMENTATIVE report should include an IMPLICATIONS section",
                )
            )

    for section in spec.sections:
        # W-3: suggestive / exploratory findings without competing interpretations
        for finding in section.findings:
            if (
                finding.strength in (StrengthLevel.SUGGESTIVE, StrengthLevel.EXPLORATORY)
                and not finding.competing_interpretations
            ):
                warnings.append(
                    ValidationError(
                        rule="W-3",
                        severity="warning",
                        message=(
                            f"{finding.strength.value} findings should enumerate at "
                            f"least one competing interpretation"
                        ),
                        section_slug=section.slug,
                        finding_slug=finding.slug,
                    )
                )

        # W-4: chart explanation completeness
        for chart in section.charts:
            exp = chart.explanation
            if not exp.question or not exp.reading_guide:
                warnings.append(
                    ValidationError(
                        rule="W-4",
                        severity="warning",
                        message=(
                            f"chart '{chart.slug}' must have non-empty "
                            f"explanation.question and explanation.reading_guide"
                        ),
                        section_slug=section.slug,
                    )
                )

    return warnings


# ---------------------------------------------------------------------------
# L-1: forbidden phrases
# ---------------------------------------------------------------------------
def _check_forbidden_phrases(spec: ReportSpec) -> list[ValidationError]:
    errors: list[ValidationError] = []

    def _flag(text: str, section_slug: str, finding_slug: str = "") -> None:
        for phrase, suggestion in find_forbidden(text):
            errors.append(
                ValidationError(
                    rule="L-1",
                    severity="error",
                    message=(
                        f"forbidden phrase '{phrase}' found — suggested rephrasing: "
                        f"{suggestion}"
                    ),
                    section_slug=section_slug,
                    finding_slug=finding_slug,
                )
            )

    _flag(spec.intro, section_slug="<report.intro>")

    for section in spec.sections:
        _flag(section.narrative, section_slug=section.slug)
        for finding in section.findings:
            _flag(finding.claim, section.slug, finding.slug)
            _flag(finding.justification, section.slug, finding.slug)
            _flag(finding.falsification, section.slug, finding.slug)
            for alt in finding.competing_interpretations:
                _flag(alt, section.slug, finding.slug)
        for chart in section.charts:
            exp = chart.explanation
            _flag(exp.question, section.slug)
            _flag(exp.reading_guide, section.slug)
            for kf in exp.key_findings:
                _flag(kf, section.slug)
            for cv in exp.caveats:
                _flag(cv, section.slug)
            _flag(exp.context, section.slug)
            _flag(exp.significance, section.slug)

    return errors


# ---------------------------------------------------------------------------
# Helpers for callers
# ---------------------------------------------------------------------------
def errors_only(results: list[ValidationError]) -> list[ValidationError]:
    """Convenience filter: keep only ``severity == 'error'``."""
    return [r for r in results if r.is_error()]


def warnings_only(results: list[ValidationError]) -> list[ValidationError]:
    """Convenience filter: keep only warnings."""
    return [r for r in results if not r.is_error()]
