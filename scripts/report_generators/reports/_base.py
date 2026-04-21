"""Base class for v2-compliant report generators.

All report generators inherit from BaseReportGenerator, which provides:
- Connection and provider injection
- Common write_report() method with v2 wrapping
- Abstract generate() method for subclass implementation
"""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from ..html_templates import COMMON_GLOSSARY_TERMS, REPORTS_DIR, wrap_html_v2
from ..section_builder import DataStatementParams, SectionBuilder
from ..stratified_loader import StratifiedDataProvider

log = structlog.get_logger()

if TYPE_CHECKING:
    from ..section_builder import ReportSection


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

    def write_report(
        self,
        body: str,
        *,
        intro_html: str = "",
        data_statement_params: DataStatementParams | None = None,
        extra_glossary: dict[str, str] | None = None,
    ) -> Path:
        """Write a v2-compliant HTML report file.

        Args:
            body: rendered HTML body content (sections, charts, etc.)
            intro_html: optional intro block before the body
            data_statement_params: params for data statement (uses defaults if None)
            extra_glossary: additional glossary terms merged with common ones

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

        html = wrap_html_v2(
            title=self.title,
            subtitle=self.subtitle,
            body=body,
            doc_type=self.doc_type,
            intro_html=intro_html,
            glossary_terms=terms,
            data_statement_html=ds_html,
            disclaimer_html=disclaimer_html,
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
        )
