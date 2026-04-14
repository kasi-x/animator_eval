"""Assemble a ``ReportSpec`` + provider data into a complete HTML string.

The assembler walks each ``SectionSpec`` and delegates chart rendering to
``chart_renderers.render_chart`` and HTML layout to ``html_primitives``.

Flow::

    assemble(spec, data)
      → validate(spec)   (raises on error)
      → for section in spec.sections:
            _render_section(section, data, all_chart_slugs)
      → wrap_html_with_katex(...)
"""

from __future__ import annotations

import html
from typing import Any

from src.reporting.renderers.chart_renderers import render_chart
from src.reporting.renderers.html_primitives import (
    COMMON_GLOSSARY_TERMS,
    DISCLAIMER,
    caveat_box,
    chart_guide,
    plotly_div_safe,
    report_intro,
    section_desc,
    wrap_html_with_katex,
)
from src.reporting.specs.finding import FindingSpec
from src.reporting.specs.info import DataScopeInfo, MethodsInfo, ReproducibilityInfo
from src.reporting.specs.report import ReportSpec
from src.reporting.specs.section import SectionKind, SectionSpec
from src.reporting.specs.validation import errors_only, validate


# ---------------------------------------------------------------------------
# Section renderers (one per SectionKind)
# ---------------------------------------------------------------------------


def _esc(text: str) -> str:
    """Escape HTML entities."""
    return html.escape(text)


def _render_narrative(section: SectionSpec, _data: dict[str, Any]) -> str:
    if not section.narrative:
        return ""
    return section_desc(section.narrative)


def _render_data_scope(section: SectionSpec, _data: dict[str, Any]) -> str:
    info: DataScopeInfo | None = section.data_scope_info
    if info is None:
        return section_desc(section.narrative) if section.narrative else ""

    parts: list[str] = []
    # Sample flow
    parts.append('<div class="data-scope">')
    parts.append('<div class="sample-flow">')
    parts.append(f'<div class="flow-step">元データ: {info.original_n:,} 件</div>')
    for desc, n in info.filter_steps:
        parts.append('<div class="flow-arrow">↓</div>')
        parts.append(f'<div class="flow-step">{_esc(desc)} → {n:,} 件</div>')
    parts.append('<div class="flow-arrow">↓</div>')
    parts.append(f'<div class="flow-step final">分析対象: {info.final_n:,} 件</div>')
    parts.append("</div>")  # sample-flow

    # Source files
    if info.source_json_files:
        parts.append('<dl class="data-sources">')
        parts.append("<dt>入力 JSON</dt>")
        parts.append(f"<dd>{', '.join(info.source_json_files)}</dd>")
        if info.source_db_tables:
            parts.append("<dt>DB テーブル</dt>")
            parts.append(f"<dd>{', '.join(info.source_db_tables)}</dd>")
        if info.time_range:
            parts.append("<dt>期間</dt>")
            parts.append(f"<dd>{info.time_range[0]}–{info.time_range[1]}</dd>")
        parts.append("</dl>")

    if info.known_biases:
        parts.append("<h4>既知のバイアス</h4><ul>")
        for bias in info.known_biases:
            parts.append(f"<li>{_esc(bias)}</li>")
        parts.append("</ul>")

    parts.append("</div>")  # data-scope
    return "\n".join(parts)


def _render_methods(section: SectionSpec, _data: dict[str, Any]) -> str:
    info: MethodsInfo | None = section.methods_info
    if info is None:
        return section_desc(section.narrative) if section.narrative else ""

    parts: list[str] = ['<div class="methods-section">']

    if info.estimator_description:
        parts.append(f"<p>{_esc(info.estimator_description)}</p>")

    for label, eq in info.equations:
        parts.append(
            f'<div class="equation">'
            f'<span class="eq-label">{_esc(label)}</span>'
            f'<span class="eq-body">{eq}</span>'
            f"</div>"
        )

    if info.identification_assumptions:
        parts.append("<h4>識別仮定</h4><ol>")
        for a in info.identification_assumptions:
            parts.append(f"<li>{_esc(a)}</li>")
        parts.append("</ol>")

    if info.rejected_alternatives:
        parts.append("<h4>棄却した代替手法</h4><dl>")
        for alt, reason in info.rejected_alternatives:
            parts.append(f"<dt>{_esc(alt)}</dt><dd>{_esc(reason)}</dd>")
        parts.append("</dl>")

    if info.code_references:
        parts.append('<p class="code-ref">実装参照: ')
        parts.append(
            ", ".join(f"<code>{_esc(ref)}</code>" for ref in info.code_references)
        )
        parts.append("</p>")

    parts.append("</div>")
    return "\n".join(parts)


def _render_reproducibility(section: SectionSpec, _data: dict[str, Any]) -> str:
    info: ReproducibilityInfo | None = section.reproducibility_info
    if info is None:
        return section_desc(section.narrative) if section.narrative else ""

    parts: list[str] = ['<div class="reproducibility-section"><dl>']
    parts.append(f"<dt>入力</dt><dd>{', '.join(info.inputs)}</dd>")
    if info.entry_points:
        parts.append(
            f"<dt>エントリポイント</dt><dd>{', '.join(info.entry_points)}</dd>"
        )
    if info.seeds:
        seeds_str = ", ".join(f"{k}={v}" for k, v in info.seeds)
        parts.append(f"<dt>シード</dt><dd>{seeds_str}</dd>")
    if info.parameters:
        params_str = ", ".join(f"{k}={v}" for k, v in info.parameters)
        parts.append(f"<dt>パラメータ</dt><dd>{params_str}</dd>")
    if info.pipeline_run_id:
        parts.append(
            f"<dt>実行 ID</dt><dd><code>{_esc(info.pipeline_run_id)}</code></dd>"
        )
    if info.data_snapshot_date:
        parts.append(
            f"<dt>スナップショット</dt><dd>{_esc(info.data_snapshot_date)}</dd>"
        )
    parts.append("</dl>")

    if info.db_queries:
        parts.append("<h4>SQL クエリ</h4>")
        for label, query in info.db_queries:
            parts.append(f"<p><strong>{_esc(label)}</strong></p>")
            parts.append(f"<pre>{_esc(query)}</pre>")

    parts.append("</div>")
    return "\n".join(parts)


def _render_finding_card(f: FindingSpec) -> str:
    """Render one FindingSpec as a ``finding-card`` block."""
    strength_cls = f"finding-strength-{f.strength.value}"

    parts: list[str] = [f'<div class="finding-card {strength_cls}">']

    # Header: slug + strength badge
    strength_label = {
        "strong": "★★★ Strong",
        "suggestive": "★★☆ Suggestive",
        "exploratory": "★☆☆ Exploratory",
    }
    parts.append(
        f'<div class="finding-header">'
        f'<span class="finding-slug">{_esc(f.slug)}</span>'
        f'<span class="finding-strength">{strength_label.get(f.strength.value, f.strength.value)}</span>'
        f"</div>"
    )

    # Claim
    parts.append(f'<div class="finding-claim">{_esc(f.claim)}</div>')

    # Uncertainty block
    if f.uncertainty:
        u = f.uncertainty
        parts.append('<div class="finding-uncertainty">')
        if u.estimate is not None:
            parts.append(
                '<div><span class="uncertainty-label">推定値</span>'
                f'<span class="uncertainty-value">{u.estimate:.4f}</span></div>'
            )
        if u.has_interval():
            parts.append(
                f'<div><span class="uncertainty-label">{int(u.ci_level * 100)}% CI</span>'
                f'<span class="uncertainty-value">[{u.ci_lower:.4f}, {u.ci_upper:.4f}]</span></div>'
            )
        if u.n is not None:
            parts.append(
                f'<div><span class="uncertainty-label">n</span>'
                f'<span class="uncertainty-value">{u.n:,}</span></div>'
            )
        if u.method:
            parts.append(f'<span class="uncertainty-method">{_esc(u.method)}</span>')
        parts.append("</div>")  # finding-uncertainty

    # Justification
    if f.justification:
        parts.append(
            '<details class="finding-justification">'
            "<summary>根拠と手法</summary>"
            f"<p>{_esc(f.justification)}</p>"
        )
        if f.source_code_ref:
            parts.append(
                f'<p class="code-ref">参照: <code>{_esc(f.source_code_ref)}</code></p>'
            )
        parts.append("</details>")

    # Competing interpretations
    if f.competing_interpretations:
        parts.append(
            '<details class="finding-robustness"><summary>競合解釈</summary><ul>'
        )
        for alt in f.competing_interpretations:
            parts.append(f"<li>{_esc(alt)}</li>")
        parts.append("</ul></details>")

    # Falsification
    if f.falsification:
        parts.append(
            '<details class="finding-robustness">'
            "<summary>反証条件</summary>"
            f"<p>{_esc(f.falsification)}</p>"
            "</details>"
        )

    parts.append("</div>")  # finding-card
    return "\n".join(parts)


def _render_findings(section: SectionSpec, _data: dict[str, Any]) -> str:
    if not section.findings:
        return ""
    return "\n".join(_render_finding_card(f) for f in section.findings)


def _render_charts(section: SectionSpec, data: dict[str, Any]) -> str:
    """Render all charts in a section."""
    parts: list[str] = []
    for chart_spec in section.charts:
        fig = render_chart(chart_spec, data)
        div_id = f"chart-{chart_spec.slug}"
        height = chart_spec.height if chart_spec.height is not None else 500
        parts.append(plotly_div_safe(fig, div_id, height))

        # Explanation block below chart
        exp = chart_spec.explanation
        if exp.question:
            parts.append(chart_guide(exp.question))
        if exp.reading_guide:
            parts.append(section_desc(exp.reading_guide))
    return "\n".join(parts)


def _render_stat_cards(section: SectionSpec, data: dict[str, Any]) -> str:
    if not section.cards:
        return ""
    parts: list[str] = ['<div class="stats-grid">']
    for card in section.cards:
        val = data.get(card.value_field, 0)
        try:
            formatted = card.value_format.format(val)
        except (ValueError, TypeError):
            formatted = str(val)
        badge_cls = f"badge-{card.badge}" if card.badge else ""
        parts.append(
            f'<div class="stat-card {badge_cls}">'
            f'<div class="value">{formatted}</div>'
            f'<div class="label">{_esc(card.label)}</div>'
        )
        if card.sublabel:
            parts.append(f'<div class="sublabel">{_esc(card.sublabel)}</div>')
        parts.append("</div>")
    parts.append("</div>")
    return "\n".join(parts)


def _render_tables(section: SectionSpec, data: dict[str, Any]) -> str:
    if not section.tables:
        return ""
    parts: list[str] = []
    for tbl in section.tables:
        rows = data.get(tbl.data_key, [])
        if tbl.max_rows and len(rows) > tbl.max_rows:
            rows = rows[: tbl.max_rows]

        parts.append(f"<h4>{_esc(tbl.title)}</h4>")
        parts.append("<table><thead><tr>")
        for _field, display in tbl.columns:
            parts.append(f"<th>{_esc(display)}</th>")
        parts.append("</tr></thead><tbody>")
        for row in rows:
            parts.append("<tr>")
            for field_name, _display in tbl.columns:
                parts.append(f"<td>{_esc(str(row.get(field_name, '')))}</td>")
            parts.append("</tr>")
        parts.append("</tbody></table>")
    return "\n".join(parts)


def _render_limitations(section: SectionSpec, _data: dict[str, Any]) -> str:
    if section.narrative:
        return caveat_box(section.narrative)
    return ""


# ---------------------------------------------------------------------------
# Section dispatcher
# ---------------------------------------------------------------------------

_SECTION_RENDERERS: dict[SectionKind, Any] = {
    SectionKind.ABSTRACT: _render_narrative,
    SectionKind.RESEARCH_QUESTION: _render_narrative,
    SectionKind.HYPOTHESES: _render_narrative,
    SectionKind.DATA_SCOPE: _render_data_scope,
    SectionKind.METHODS: _render_methods,
    SectionKind.DESCRIPTIVE_STATS: _render_narrative,
    SectionKind.FINDINGS: _render_findings,
    SectionKind.ROBUSTNESS: _render_narrative,
    SectionKind.INTERPRETATION: _render_narrative,
    SectionKind.LIMITATIONS: _render_limitations,
    SectionKind.IMPLICATIONS: _render_narrative,
    SectionKind.OPEN_QUESTIONS: _render_narrative,
    SectionKind.REPRODUCIBILITY: _render_reproducibility,
    SectionKind.GLOSSARY: lambda s, d: "",  # handled by wrap_html
    SectionKind.NARRATIVE: _render_narrative,
    SectionKind.CHART_GROUP: _render_narrative,
    SectionKind.TABLE_GROUP: _render_narrative,
    SectionKind.CARD_GROUP: _render_narrative,
}


def _render_section(section: SectionSpec, data: dict[str, Any]) -> str:
    """Render a single SectionSpec to an HTML card."""
    if section.hidden:
        return ""

    parts: list[str] = []

    # Open card
    tag = "details" if section.accordion else "div"
    parts.append(f'<{tag} class="card" id="section-{section.slug}">')
    if section.accordion:
        parts.append(f"<summary><h2>{_esc(section.title)}</h2></summary>")
    else:
        parts.append(f"<h2>{_esc(section.title)}</h2>")

    # Kind-specific body
    renderer = _SECTION_RENDERERS.get(section.kind, _render_narrative)
    body = renderer(section, data)
    if body:
        parts.append(body)

    # Stat cards
    parts.append(_render_stat_cards(section, data))

    # Charts
    parts.append(_render_charts(section, data))

    # Tables
    parts.append(_render_tables(section, data))

    # Close card
    parts.append(f"</{tag}>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def assemble(
    spec: ReportSpec, data: dict[str, Any], *, skip_validation: bool = False
) -> str:
    """Assemble a complete HTML report from a spec and provider data.

    Parameters
    ----------
    spec:
        The declarative report specification.
    data:
        A flat dict produced by the report's data provider.
        Chart specs look up their data via ``data[chart.data_key]``.
        Stat cards look up their data via ``data[card.value_field]``.
    skip_validation:
        If True, skip the validation step (useful for testing partial specs).

    Returns
    -------
    str
        A complete, self-contained HTML document.

    Raises
    ------
    ValueError
        If the spec has validation errors (and ``skip_validation`` is False).
    """
    if not skip_validation:
        results = validate(spec)
        errs = errors_only(results)
        if errs:
            msgs = "; ".join(f"[{e.rule}] {e.message}" for e in errs)
            raise ValueError(f"ReportSpec '{spec.slug}' has validation errors: {msgs}")

    # Build intro
    intro_html = (
        report_intro(spec.title, spec.intro, spec.audience) if spec.intro else ""
    )

    # Render all sections
    section_html_parts: list[str] = []
    for section in spec.sections:
        rendered = _render_section(section, data)
        if rendered:
            section_html_parts.append(rendered)
    body = "\n\n".join(section_html_parts)

    # Merge glossary terms (spec-specific + common)
    glossary: dict[str, str] = (
        dict(COMMON_GLOSSARY_TERMS) if isinstance(COMMON_GLOSSARY_TERMS, dict) else {}
    )
    for term in spec.glossary_terms:
        if ":" in term:
            k, v = term.split(":", 1)
            glossary[k.strip()] = v.strip()

    result = wrap_html_with_katex(
        title=spec.title,
        subtitle=spec.subtitle,
        body=body,
        intro_html=intro_html,
        glossary_terms=glossary if glossary else None,
    )

    # L-2: verify DISCLAIMER is present in final HTML
    if DISCLAIMER and DISCLAIMER not in result:
        raise ValueError(
            f"[L-2] DISCLAIMER text not found in final HTML for '{spec.slug}'. "
            f"wrap_html should inject it automatically — this indicates a template bug."
        )

    return result
