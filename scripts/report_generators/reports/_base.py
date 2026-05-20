"""Base class for v2-compliant report generators.

All report generators inherit from BaseReportGenerator, which provides:
- Connection and provider injection
- Common write_report() method with v2 wrapping
- Abstract generate() method for subclass implementation
- Abstract SNS generation: to_sns_post() / to_note_post()
"""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel, field_validator

from ..html_templates import COMMON_GLOSSARY_TERMS, REPORTS_DIR, wrap_html_v2
from ..section_builder import DataStatementParams, SectionBuilder
from ..stratified_loader import StratifiedDataProvider
from .._coverage_block import coverage_block_html as _coverage_block_html

log = structlog.get_logger()

if TYPE_CHECKING:
    from ..section_builder import ReportSection
    from src.analysis.quality.coverage_matrix import CoverageMatrix


# ---------------------------------------------------------------------------
# Coverage caveat block helper (27_methodology/01_missingness_disclosure)
# ---------------------------------------------------------------------------


def _build_coverage_block_html(
    matrix: "CoverageMatrix | None",
) -> str:
    """Return coverage caveat block HTML for injection into every report.

    When matrix is None, compute_coverage_matrix() is called with the
    default resolved.duckdb path. This degrades gracefully when the DB
    is absent (returns an empty-matrix notice).

    Args:
        matrix: Pre-computed CoverageMatrix, or None to auto-compute.

    Returns:
        HTML string for the coverage_block_html parameter of wrap_html_v2().
    """
    if matrix is None:
        try:
            from src.analysis.quality.coverage_matrix import compute_coverage_matrix
            matrix = compute_coverage_matrix()
        except Exception as exc:
            log.warning("coverage_block_compute_failed", error=str(exc))
            from src.analysis.quality.coverage_matrix import CoverageMatrix
            matrix = CoverageMatrix()

    return _coverage_block_html(matrix)


# ---------------------------------------------------------------------------
# SNS post data models (Pydantic v2)
# ---------------------------------------------------------------------------

_X_CHAR_LIMIT = 280
_NOTE_CHAR_MIN = 1500
_NOTE_CHAR_MAX = 3000


class SnsPost(BaseModel):
    """X (Twitter) short-form post.

    text must be <= 280 characters (platform limit).
    figure_path is optional (relative to output dir or absolute).
    url is the link to the full report.
    hashtags are included in text — callers must count chars accordingly.
    """

    platform: str = "x"
    text: str
    figure_path: str = ""
    url: str = ""

    @field_validator("text")
    @classmethod
    def text_within_x_limit(cls, v: str) -> str:
        if len(v) > _X_CHAR_LIMIT:
            raise ValueError(
                f"SnsPost.text exceeds {_X_CHAR_LIMIT} chars: {len(v)} chars. "
                "Truncate or rewrite."
            )
        return v


class NotePost(BaseModel):
    """note.com long-form article.

    body must be 1500-3000 characters.
    title is the article heading.
    """

    platform: str = "note"
    title: str
    body: str
    figure_paths: list[str] = []
    url: str = ""

    @field_validator("body")
    @classmethod
    def body_within_note_range(cls, v: str) -> str:
        length = len(v)
        if length < _NOTE_CHAR_MIN:
            raise ValueError(
                f"NotePost.body is too short: {length} chars (min {_NOTE_CHAR_MIN})."
            )
        if length > _NOTE_CHAR_MAX:
            raise ValueError(
                f"NotePost.body exceeds {_NOTE_CHAR_MAX} chars: {length} chars."
            )
        return v


def append_validation_warnings(findings: str, sb: SectionBuilder) -> str:
    """Append validation warnings to findings HTML if violations exist."""
    violations = sb.validate_findings(findings)
    if violations:
        findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'
    return findings


class BaseReportGenerator(ABC):
    """Abstract base for v2-compliant report generators.

    Subclasses implement generate() which returns the output file path
    or None if the report could not be generated.

    Usage:
        class MyReport(BaseReportGenerator):
            name = "my_report"
            title = "My Report Title"
            subtitle = "Descriptive subtitle"

            def generate(self) -> Path | None:
                sections = [...]
                body = "\\n".join(self.builder.build_section(s) for s in sections)
                return self.write_report(body)
    """

    #: Short identifier used for filename and logging
    name: str = ""
    #: Report title (displayed in <h1>)
    title: str = ""
    #: Report subtitle
    subtitle: str = ""
    #: Document type: 'main', 'brief', or 'appendix'
    doc_type: str = "main"
    #: Output filename (defaults to {name}.html)
    filename: str = ""
    #: Glossary terms to include (defaults to common terms)
    glossary_terms: dict[str, str] | None = None

    def __init__(
        self,
        conn: sqlite3.Connection,
        provider: StratifiedDataProvider | None = None,
        builder: SectionBuilder | None = None,
        *,
        output_dir: Path | None = None,
    ) -> None:
        self.conn = conn
        self.provider = provider or StratifiedDataProvider(conn)
        self.builder = builder or SectionBuilder()
        self.output_dir = output_dir or REPORTS_DIR

    @abstractmethod
    def generate(self) -> Path | None:
        """Generate the report. Returns output path or None on failure."""
        ...

    def to_sns_post(self) -> SnsPost:
        """Generate an X (Twitter) Tier-A short-form post from this report.

        Default implementation raises NotImplementedError.
        Subclasses that want SNS export must override this method.

        Returns:
            SnsPost with text <= 280 chars, optional figure_path, and url.

        Raises:
            NotImplementedError: when the report has not implemented SNS export.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} has not implemented to_sns_post(). "
            "Override this method in the report subclass."
        )

    def to_note_post(self) -> NotePost:
        """Generate a note.com Tier-C long-form article from this report.

        Default implementation raises NotImplementedError.
        Subclasses that want SNS export must override this method.

        Returns:
            NotePost with body 1500-3000 chars.

        Raises:
            NotImplementedError: when the report has not implemented note export.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} has not implemented to_note_post(). "
            "Override this method in the report subclass."
        )

    def write_report(
        self,
        body: str,
        *,
        intro_html: str = "",
        data_statement_params: DataStatementParams | None = None,
        extra_glossary: dict[str, str] | None = None,
        coverage_matrix: "CoverageMatrix | None" = None,
    ) -> Path:
        """Write a v2-compliant HTML report file.

        Args:
            body: rendered HTML body content (sections, charts, etc.)
            intro_html: optional intro block before the body
            data_statement_params: params for data statement (uses defaults if None)
            extra_glossary: additional glossary terms merged with common ones
            coverage_matrix: optional CoverageMatrix for the caveat block.
                When None, compute_coverage_matrix() is called automatically
                so every report gets the mandatory coverage disclosure per
                TASK_CARDS/27_methodology/01_missingness_disclosure.

        Returns:
            Path to the written file.
        """
        # Build data statement + disclaimer
        ds_html = self.builder.build_data_statement(data_statement_params)
        disclaimer_html = self.builder.build_disclaimer()

        # Merge glossary terms
        terms = dict(COMMON_GLOSSARY_TERMS)
        if self.glossary_terms:
            terms.update(self.glossary_terms)
        if extra_glossary:
            terms.update(extra_glossary)

        # Build mandatory coverage caveat block (27_methodology/01_missingness_disclosure)
        cov_block = _build_coverage_block_html(coverage_matrix)

        # Append cross-reference block (Session 2 後半: report 連鎖性)
        try:
            from ..cross_reference import build_cross_reference_block
            xref_html = build_cross_reference_block(self.name)
        except Exception as exc:
            log.debug("cross_ref_build_failed", report=self.name, error=str(exc))
            xref_html = ""
        body_with_xref = body + xref_html if xref_html else body

        # Reproducibility footer (Session 2 ラウンド 4: 誰でも再現可能)
        try:
            import inspect

            from ..reproducibility_footer import (
                build_metadata,
                register,
                render_footer_html,
            )

            mod = inspect.getmodule(self.__class__)
            spec_obj = getattr(mod, "SPEC", None) if mod is not None else None
            repro_meta = build_metadata(self.name, spec_obj=spec_obj)
            register(repro_meta)
            footer_html = render_footer_html(repro_meta)
            body_with_xref = body_with_xref + footer_html
        except Exception as exc:
            log.debug("repro_footer_failed", report=self.name, error=str(exc))

        html = wrap_html_v2(
            title=self.title,
            subtitle=self.subtitle,
            body=body_with_xref,
            doc_type=self.doc_type,
            intro_html=intro_html,
            glossary_terms=terms,
            data_statement_html=ds_html,
            disclaimer_html=disclaimer_html,
            coverage_block_html=cov_block,
        )

        fname = self.filename or f"{self.name}.html"
        out_path = self.output_dir / fname
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(html, encoding="utf-8")

        log.info("report_written", report=self.name, path=str(out_path))
        return out_path

    def render_unified_structure(
        self,
        sections: list["ReportSection"],
        meta_table: str | None = None,
        *,
        overview_html: str = "",
        interpretation_html: str = "",
        data_statement_params: "DataStatementParams | None" = None,
        extra_glossary: "dict[str, str] | None" = None,
        coverage_matrix: "CoverageMatrix | None" = None,
    ) -> "Path":
        """Render a report using the mandatory v2 unified section structure.

        Structure enforced:
          1. 概要 (overview)
          2. Findings (each section in `sections`)
          3. Method Note (auto-generated from meta_lineage if meta_table given)
          4. Interpretation (optional, must have at least one alternative)
          5. Data Statement
          6. Disclaimers

        Args:
            sections: List of ReportSection (Findings content).
            meta_table: meta_lineage table name for auto Method Note.
                        If None, no Method Note is generated.
            overview_html: Optional intro paragraph (the 概要 section).
            interpretation_html: Optional interpretation block (must be labelled,
                                  contain at least one alternative interpretation).
            data_statement_params: Override defaults for the data statement.
            extra_glossary: Additional glossary terms.
            coverage_matrix: optional CoverageMatrix for the caveat block.
                Passed through to write_report(). When None, auto-computed.

        Returns:
            Path to the written HTML file.
        """
        self.builder.validate(
            has_overview=bool(overview_html and overview_html.strip()),
            has_findings=bool(sections),
            has_method_note=bool(meta_table),
            has_data_statement=True,
            has_disclaimers=True,
            interpretation_html=interpretation_html,
            method_note_auto_generated=True,
        )

        parts: list[str] = []

        # 1. 概要
        if overview_html:
            parts.append(
                f'<div class="card" id="overview">'
                f"<h2>概要 / Overview</h2>{overview_html}</div>"
            )

        # 2. Findings
        for section in sections:
            violations = self.builder.validate_findings(section.findings_html)
            if violations:
                log.warning(
                    "findings_violations",
                    report=self.name,
                    section=section.title,
                    violations=violations,
                )
            parts.append(self.builder.build_section(section))

        # 3. Method Note (auto-generated from meta_lineage)
        if meta_table:
            try:
                method_html = self.builder.method_note_from_lineage(meta_table, self.conn)
                parts.append(
                    f'<div class="card method-note-auto" id="method-note">'
                    f"<h2>Method Note</h2>{method_html}</div>"
                )
            except ValueError as e:
                log.warning("method_note_lineage_missing", meta_table=meta_table, error=str(e))

        # 4. Interpretation (optional)
        if interpretation_html:
            parts.append(
                '<div class="card interpretation" id="interpretation"'
                ' style="border-left:3px solid #c0a0d0;">'
                '<h2>Interpretation / 解釈</h2>'
                '<p style="font-size:0.8rem;color:#9090b0;">'
                "以下は分析者の解釈であり、代替解釈が存在する。 / "
                "The following reflects the analyst's interpretation; "
                "alternative interpretations exist.</p>"
                f"{interpretation_html}</div>"
            )

        body = "\n".join(parts)
        return self.write_report(
            body,
            data_statement_params=data_statement_params,
            extra_glossary=extra_glossary,
            coverage_matrix=coverage_matrix,
        )
