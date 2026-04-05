"""HtmlReportAssembler — ReportSpec → 完全なHTMLページ.

PlotlyRenderer + explanation_widgets + html_templates を組み合わせて
ReportSpec を単一の HTML ファイルとして出力する。
"""

from __future__ import annotations

from src.viz.assemblers.explanation_widgets import render_explanation
from src.viz.assemblers.html_templates import (
    COMMON_GLOSSARY_TERMS,
    wrap_html,
)
from src.viz.renderers.plotly_renderer import PlotlyRenderer
from src.viz.report_spec import (
    ReportSpec,
    SectionSpec,
    StatCardSpec,
    TableSpec,
    slugify,
)


class HtmlReportAssembler:
    """ReportSpec → 完全 HTML ページ."""

    def __init__(self, renderer: PlotlyRenderer | None = None) -> None:
        self._renderer = renderer or PlotlyRenderer()
        self._table_counter = 0

    def assemble(self, spec: ReportSpec, *, footer_stats: str = "") -> str:
        """ReportSpec → HTML 文字列."""
        self._table_counter = 0

        intro_html = ""
        if spec.description or spec.audience:
            intro_html = (
                '<div class="report-intro">'
                f"<h2>{spec.title}</h2>"
                f"<p>{spec.description}</p>"
            )
            if spec.audience:
                intro_html += f'<p class="audience">対象読者: {spec.audience}</p>'
            intro_html += "</div>"

        # TOC
        toc = self._build_toc(spec.sections)

        # Sections
        sections_html = ""
        for section in spec.sections:
            sections_html += self._render_section(section)

        body = toc + sections_html

        # Merge glossary
        glossary = {**COMMON_GLOSSARY_TERMS, **spec.glossary}

        return wrap_html(
            title=spec.title,
            subtitle=spec.subtitle,
            body=body,
            intro_html=intro_html,
            glossary_terms=glossary,
            footer_stats=footer_stats,
        )

    # ── private ──

    def _build_toc(self, sections: tuple[SectionSpec, ...]) -> str:
        if not sections:
            return ""
        links = "".join(
            f'<a href="#{slugify(s.title)}">{s.title}</a>'
            for s in sections
        )
        return f'<nav class="toc">{links}</nav>'

    def _render_section(self, section: SectionSpec) -> str:
        slug = slugify(section.title)
        parts: list[str] = []

        parts.append(f'<div class="card" id="{slug}">')
        parts.append(f"<h2>{section.title}</h2>")

        if section.description:
            parts.append(f'<p class="section-desc">{section.description}</p>')

        self._render_content_block(parts, section)

        # Subsections
        for sub in section.subsections:
            parts.append(f"<h3>{sub.title}</h3>")
            if sub.description:
                parts.append(f'<p class="section-desc">{sub.description}</p>')
            self._render_content_block(parts, sub)

        parts.append("</div>")
        return "\n".join(parts)

    def _render_content_block(self, parts: list[str], section: SectionSpec) -> None:
        """stats + tables + charts を共通パターンでレンダリング."""
        if section.stats:
            parts.append(self._render_stats(section.stats))
        for table in section.tables:
            parts.append(self._render_table(table))
        for chart in section.charts:
            before, after = render_explanation(chart.explanation)
            parts.append(before)
            parts.append(self._renderer.render_to_html_div(chart))
            parts.append(after)

    def _render_stats(self, stats: tuple[StatCardSpec, ...]) -> str:
        cards = ""
        for s in stats:
            badge = f' <span class="badge {s.badge_class}"></span>' if s.badge_class else ""
            cards += (
                '<div class="stat-card">'
                f'<div class="value">{s.value}{badge}</div>'
                f'<div class="label">{s.label}</div>'
                "</div>"
            )
        return f'<div class="stats-grid">{cards}</div>'

    def _render_table(self, table: TableSpec) -> str:
        self._table_counter += 1
        tid = f"tbl_{self._table_counter}"

        caption = f"<caption>{table.caption}</caption>" if table.caption else ""

        # Headers
        ths = ""
        for ci, h in enumerate(table.headers):
            if table.sortable:
                onclick = f'sortTable("{tid}", {ci}, false)'
                ths += f'<th class="sortable-th" onclick=\'{onclick}\'>{h} ↕</th>'
            else:
                ths += f"<th>{h}</th>"

        # Rows
        trs = ""
        for row in table.rows:
            tds = "".join(f"<td>{cell}</td>" for cell in row)
            trs += f"<tr>{tds}</tr>"

        return (
            f'<table id="{tid}">'
            f"{caption}"
            f"<thead><tr>{ths}</tr></thead>"
            f"<tbody>{trs}</tbody>"
            "</table>"
        )
