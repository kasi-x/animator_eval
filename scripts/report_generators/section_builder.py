"""REPORT_PHILOSOPHY v2 section structure enforcer.

Provides:
- ReportSection dataclass (title / findings / viz / method note / interpretation)
- SectionBuilder: validates findings text, renders v2-compliant HTML sections
- Data statement and disclaimer generation

Every section rendered by this module follows the mandatory v2 structure:
  1. Section Title (noun phrase, not a conclusion)
  2. Findings (1-3 paragraphs, purely descriptive)
  3. Primary Visualization
  4. Method Note
  5. Interpretation (optional, labeled, authored, with alternatives)
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass


# =========================================================================
# Prohibited expressions per REPORT_PHILOSOPHY v2 Section 2.1 / 7
# =========================================================================

_PROHIBITED_ADJECTIVES_EN = {
    "remarkable", "surprising", "alarming", "concerning", "healthy",
    "robust", "weak", "impressive", "excellent", "outstanding",
    "significant",  # when used evaluatively, not statistically
    "notable", "striking", "dramatic", "exceptional", "extraordinary",
    "promising", "worrying", "encouraging", "disappointing", "critical",
}

_PROHIBITED_ADJECTIVES_JA = {
    "驚くべき", "注目すべき", "特に重要", "素晴らしい", "優れた",
    "懸念される", "健全な", "堅調な", "印象的な", "期待できる",
    "残念な", "著しい", "顕著な", "飛躍的な",
    # ability framing (v2 philosophy gate)
    "能力", "優秀", "実力", "才能", "センス", "技能水準",
}

_PROHIBITED_CAUSAL_VERBS_EN = {
    "cause", "causes", "caused", "causing",
    "drive", "drives", "driven", "driving",
    "result in", "results in", "resulted in",
    "lead to", "leads to", "led to", "leading to",
    "trigger", "triggers", "triggered", "triggering",
    "produce", "produces", "produced",
    "generate", "generates", "generated",
    "determine", "determines", "determined",
}

_PROHIBITED_CAUSAL_JA = {
    "引き起こす", "もたらす", "起因する", "原因となる",
    "結果として", "～の結果",
}

_PROHIBITED_NORMATIVE_EN = {
    "should", "need to", "needs to", "must", "ought to",
    "have to", "has to", "require", "requires", "necessary",
    "essential", "important to", "crucial",
}

_PROHIBITED_NORMATIVE_JA = {
    "べき", "必要がある", "しなければならない", "すべき",
}

_PROHIBITED_SELECTION_JA = {
    "注目すべきは", "驚くべきことに", "特筆すべきは", "重要なのは",
    "興味深いことに", "見逃せないのは",
}

_PROHIBITED_SELECTION_EN = {
    "notably", "importantly", "interestingly", "strikingly",
    "it is worth noting", "it should be noted",
    "of particular interest",
}


@dataclass
class ReportSection:
    """A single section of a v2-compliant report.

    Attributes:
        title: Noun phrase (not a conclusion). e.g. "Score Distribution by Tier"
        findings_html: 1-3 paragraphs of purely descriptive text.
            Must include n, CI or distribution width, shape if skewed.
            ZERO evaluative adjectives, ZERO causal verbs.
        visualization_html: Plotly div or HTML table.
        method_note: Metric definition, parameter choices, known limitations.
        interpretation_html: Optional. If present, must be labeled,
            state authorship, present min 1 alternative, disclose premises.
        section_id: HTML id attribute for anchor links.
    """

    title: str
    findings_html: str
    visualization_html: str = ""
    method_note: str = ""
    interpretation_html: str | None = None
    section_id: str = ""


@dataclass
class DataStatementParams:
    """Parameters for the mandatory data statement (v2 Section 4)."""

    data_source: str = (
        "Animetor Eval database (SQLite), aggregating credit data from "
        "SeesaaWiki, Keyframe, AniList, MediaArts DB, and MAL."
    )
    snapshot_date: str = ""
    schema_version: str = ""
    coverage_notes: str = (
        "Temporal recording density varies: 1980s credits are sparser than 2010s+. "
        "Missing segments include: overseas subcontracting, uncredited work, "
        "assistant roles, some OVA/streaming originals. "
        "TV-format works are overrepresented relative to all production formats."
    )
    name_resolution_notes: str = (
        "5-step entity resolution: exact match, cross-source, romaji, "
        "similarity (Jaro-Winkler, threshold=0.95), AI-assisted (Ollama/Qwen3). "
        "False merge/split rates are not precisely quantified; "
        "romanization ambiguity is a known risk."
    )
    missing_value_handling: str = (
        "Persons with zero credits are excluded from scoring. "
        "Missing duration/episodes default to format-based estimates. "
        "NULL gender values are retained as a separate category in stratified analyses."
    )


class SectionBuilder:
    """Builds v2-compliant report sections and validates findings text."""

    _ALT_INTERPRETATION_TOKENS = (
        "代替解釈",
        "別の解釈",
        "もう一つの解釈",
        "alternative interpretation",
        "another interpretation",
    )

    def validate_findings(self, text: str) -> list[str]:
        """Check findings text for REPORT_PHILOSOPHY v2 violations.

        Returns list of violation descriptions. Empty list = compliant.
        """
        violations: list[str] = []
        text_lower = text.lower()

        # Check prohibited adjectives (EN)
        for adj in _PROHIBITED_ADJECTIVES_EN:
            if re.search(rf"\b{re.escape(adj)}\b", text_lower):
                violations.append(f"Evaluative adjective in findings: '{adj}'")

        # Check prohibited adjectives (JA)
        for adj in _PROHIBITED_ADJECTIVES_JA:
            if adj in text:
                violations.append(f"Evaluative adjective in findings (JA): '{adj}'")

        # Check prohibited causal verbs (EN)
        for verb in _PROHIBITED_CAUSAL_VERBS_EN:
            if re.search(rf"\b{re.escape(verb)}\b", text_lower):
                violations.append(f"Causal verb in findings: '{verb}'")

        # Check prohibited causal verbs (JA)
        for verb in _PROHIBITED_CAUSAL_JA:
            if verb in text:
                violations.append(f"Causal verb in findings (JA): '{verb}'")

        # Check prohibited normative verbs (EN)
        for norm in _PROHIBITED_NORMATIVE_EN:
            if re.search(rf"\b{re.escape(norm)}\b", text_lower):
                violations.append(f"Normative expression in findings: '{norm}'")

        # Check prohibited normative verbs (JA)
        for norm in _PROHIBITED_NORMATIVE_JA:
            if norm in text:
                violations.append(f"Normative expression in findings (JA): '{norm}'")

        # Check prohibited selection rhetoric (EN)
        for sel in _PROHIBITED_SELECTION_EN:
            if sel.lower() in text_lower:
                violations.append(f"Selection rhetoric in findings: '{sel}'")

        # Check prohibited selection rhetoric (JA)
        for sel in _PROHIBITED_SELECTION_JA:
            if sel in text:
                violations.append(f"Selection rhetoric in findings (JA): '{sel}'")

        return violations

    def validate(
        self,
        *,
        has_overview: bool,
        has_findings: bool,
        has_method_note: bool,
        has_data_statement: bool,
        has_disclaimers: bool,
        interpretation_html: str | None = None,
        method_note_auto_generated: bool = True,
    ) -> None:
        """Validate required v2 section structure before rendering.

        Raises:
            ValueError: if required sections are missing or constraints are violated.
        """
        missing: list[str] = []
        if not has_overview:
            missing.append("概要 / Overview")
        if not has_findings:
            missing.append("Findings")
        if not has_method_note:
            missing.append("Method Note")
        if not has_data_statement:
            missing.append("Data Statement")
        if not has_disclaimers:
            missing.append("Disclaimers")
        if missing:
            raise ValueError(f"Missing required sections: {', '.join(missing)}")

        if has_method_note and not method_note_auto_generated:
            raise ValueError("Method Note must be auto-generated from meta_lineage")

        if interpretation_html:
            interp_lower = interpretation_html.lower()
            has_alt = any(
                token.lower() in interp_lower for token in self._ALT_INTERPRETATION_TOKENS
            )
            if not has_alt:
                raise ValueError(
                    "Interpretation section must contain at least one "
                    "alternative interpretation"
                )

    def build_section(self, section: ReportSection) -> str:
        """Render a ReportSection to v2-compliant HTML."""
        sid = section.section_id or section.title.lower().replace(" ", "-")[:40]

        parts: list[str] = []
        parts.append(f'<div class="card report-section" id="sec-{sid}">')
        parts.append(f"  <h2>{section.title}</h2>")

        # Findings
        parts.append('  <div class="findings">')
        parts.append(f"    {section.findings_html}")
        parts.append("  </div>")

        # Visualization
        if section.visualization_html:
            parts.append('  <div class="chart-container">')
            parts.append(f"    {section.visualization_html}")
            parts.append("  </div>")

        # Method note
        if section.method_note:
            parts.append('  <div class="method-note">')
            parts.append("    <details>")
            parts.append('      <summary style="cursor:pointer;color:#7b8794;'
                         'font-size:0.85rem;">Method Note</summary>')
            parts.append(f'      <div style="margin-top:0.5rem;font-size:0.82rem;'
                         f'color:#8a94a0;line-height:1.5;">{section.method_note}</div>')
            parts.append("    </details>")
            parts.append("  </div>")

        # Interpretation (optional)
        if section.interpretation_html:
            parts.append('  <div class="interpretation"'
                         ' style="border-top:1px solid #3a3a5c;'
                         'margin-top:1.5rem;padding-top:1rem;">')
            parts.append('    <h3 style="color:#c0a0d0;font-size:1rem;">'
                         'Interpretation / \u89e3\u91c8</h3>')
            parts.append(f"    {section.interpretation_html}")
            parts.append("  </div>")

        parts.append("</div>")
        return "\n".join(parts)

    def build_data_statement(
        self, params: DataStatementParams | None = None,
    ) -> str:
        """Render the mandatory data statement (v2 Section 4).

        Reports without data statements are NOT published.
        """
        p = params or DataStatementParams()
        return f"""
<div class="card" id="data-statement"
     style="border-left:3px solid #5a5a8a;margin-top:2rem;">
  <h2>Data Statement / \u30c7\u30fc\u30bf\u58f0\u660e</h2>
  <table style="width:100%;border-collapse:collapse;font-size:0.85rem;">
    <tr>
      <td style="padding:0.5rem;color:#9a9ab0;vertical-align:top;width:25%;">
        <strong>Data Source</strong></td>
      <td style="padding:0.5rem;">{p.data_source}
        {f'<br>Snapshot: {p.snapshot_date}' if p.snapshot_date else ''}
        {f'<br>Schema: v{p.schema_version}' if p.schema_version else ''}</td>
    </tr>
    <tr>
      <td style="padding:0.5rem;color:#9a9ab0;vertical-align:top;">
        <strong>Coverage &amp; Known Biases</strong></td>
      <td style="padding:0.5rem;">{p.coverage_notes}</td>
    </tr>
    <tr>
      <td style="padding:0.5rem;color:#9a9ab0;vertical-align:top;">
        <strong>Name Resolution</strong></td>
      <td style="padding:0.5rem;">{p.name_resolution_notes}</td>
    </tr>
    <tr>
      <td style="padding:0.5rem;color:#9a9ab0;vertical-align:top;">
        <strong>Missing Value Handling</strong></td>
      <td style="padding:0.5rem;">{p.missing_value_handling}</td>
    </tr>
  </table>
</div>
"""

    def build_disclaimer(self) -> str:
        """Render the mandatory bilingual disclaimer (v2 Section 9)."""
        return """
<div class="card" id="disclaimer"
     style="border-left:3px solid #e05080;margin-top:1rem;">
  <h2>Disclaimer / \u6ce8\u610f\u4e8b\u9805</h2>
  <div style="font-size:0.82rem;line-height:1.7;color:#b0b0c0;">
    <p><strong>\u3010\u6ce8\u610f\u4e8b\u9805\u3011</strong><br>
    \u672c\u30ec\u30dd\u30fc\u30c8\u306b\u542b\u307e\u308c\u308b\u6570\u5024\u306f\u3001\u516c\u958b\u30af\u30ec\u30b8\u30c3\u30c8\u30c7\u30fc\u30bf\u306b\u57fa\u3065\u304f\u30cd\u30c3\u30c8\u30ef\u30fc\u30af\u69cb\u9020
    \u304a\u3088\u3073\u5354\u696d\u5bc6\u5ea6\u306e\u8a18\u8ff0\u7684\u6307\u6a19\u3067\u3042\u308b\u3002\u3053\u308c\u3089\u306f\u500b\u4eba\u306e\u80fd\u529b\u3001\u6280\u91cf\u3001\u82b8\u8853\u6027\u3001\u307e\u305f\u306f
    \u8077\u696d\u7684\u4fa1\u5024\u306e\u8a55\u4fa1\u3067\u306f\u306a\u304f\u3001\u305d\u306e\u3088\u3046\u306a\u8a55\u4fa1\u3068\u3057\u3066\u89e3\u91c8\u3055\u308c\u308b\u3079\u304d\u3067\u306f\u306a\u3044\u3002</p>
    <p>\u672c\u6307\u6a19\u306f\u6e2c\u5b9a\u8005\u304c\u9078\u629e\u3057\u305f\u5b9a\u7fa9\u30fb\u96c6\u8a08\u5358\u4f4d\u30fb\u6642\u4ee3\u7a93\u306b\u4f9d\u5b58\u3057\u3066\u304a\u308a\u3001\u5225\u306e\u9078\u629e\u304b\u3089\u306f
    \u5225\u306e\u6570\u5024\u304c\u5f97\u3089\u308c\u308b\u3002\u672c\u30ec\u30dd\u30fc\u30c8\u306f\u300c\u5ba2\u89b3\u7684\u771f\u5b9f\u306e\u958b\u793a\u300d\u3067\u306f\u306a\u304f\u300c\u660e\u793a\u3055\u308c\u305f
    \u9078\u629e\u306b\u57fa\u3065\u304f\u8a18\u8ff0\u300d\u3067\u3042\u308b\u3002</p>
    <p>\u672c\u6307\u6a19\u3092\u63a1\u7528\u30fb\u5831\u916c\u30fb\u5951\u7d04\u30fb\u4eba\u4e8b\u8a55\u4fa1\u306e\u5358\u4e00\u307e\u305f\u306f\u4e3b\u8981\u306a\u6839\u62e0\u3068\u3057\u3066\u4f7f\u7528\u3059\u308b\u3053\u3068\u3092
    \u904b\u55b6\u8005\u306f\u63a8\u5968\u305b\u305a\u3001\u305d\u306e\u3088\u3046\u306a\u4f7f\u7528\u306e\u7d50\u679c\u306b\u3064\u3044\u3066\u8cac\u4efb\u3092\u8ca0\u308f\u306a\u3044\u3002</p>

    <p style="margin-top:1rem;"><strong>Note:</strong><br>
    All figures in this report are descriptive metrics of network structure and
    collaboration density, derived from publicly available credit data. They do
    not constitute and should not be interpreted as assessments of individual
    ability, skill, artistry, or professional worth.</p>
    <p>These metrics depend on definitional, aggregational, and temporal choices
    made by the analyst; alternative choices would yield different figures. This
    report is not an "objective disclosure of truth" but a "description under
    stated choices."</p>
    <p>The operators do not endorse the use of these metrics as the sole or primary
    basis for hiring, compensation, contract, or personnel decisions, and
    disclaim responsibility for outcomes of such use.</p>
  </div>
</div>
"""

    def method_note_from_lineage(
        self, table_name: str, conn: sqlite3.Connection
    ) -> str:
        """meta_lineage を読んで Method Note HTML を自動生成する.

        手書き method_note は禁止。このメソッド経由でのみ生成すること。

        Args:
            table_name: meta_lineage に登録されたテーブル名 (例: 'meta_policy_attrition')
            conn: SQLite 接続

        Returns:
            HTML 文字列

        Raises:
            ValueError: table_name が meta_lineage に未登録の場合
        """
        import json as _json
        row = conn.execute(
            "SELECT * FROM meta_lineage WHERE table_name = ?", (table_name,)
        ).fetchone()
        if row is None:
            raise ValueError(
                f"No lineage registered for '{table_name}'. "
                "Call register_meta_lineage() before generating method notes."
            )

        col_names = [d[0] for d in conn.execute(
            "SELECT * FROM meta_lineage WHERE 0"
        ).description or []]
        if not col_names:
            col_names = [
                "table_name", "audience", "source_silver_tables",
                "source_bronze_forbidden", "source_display_allowed",
                "formula_version", "computed_at",
                "ci_method", "null_model", "holdout_method",
                "row_count", "notes",
            ]
        data = dict(zip(col_names, row))

        # Parse silver source tables
        try:
            silver_tables: list[str] = _json.loads(data.get("source_silver_tables", "[]"))
        except (ValueError, TypeError):
            silver_tables = []

        parts: list[str] = ['<div style="font-size:0.82rem;line-height:1.6;color:#8a94a0;">']

        # Silver sources
        if silver_tables:
            tbl_html = ", ".join(f"<code>{t}</code>" for t in silver_tables)
            parts.append(f"<p><strong>データソース (silver 層):</strong> {tbl_html}</p>")

        if data.get("description"):
            parts.append(f"<p><strong>指標の説明:</strong> {data['description']}</p>")

        # Score prohibition confirmation
        if data.get("source_bronze_forbidden", 1):
            parts.append(
                "<p><strong>スコア非使用確認:</strong> "
                "このテーブルの算出に <code>anime.score</code>（視聴者評価）は使用していない。"
                " / <em>anime.score (viewer ratings) was not used in this computation.</em></p>"
            )

        # CI method
        if data.get("ci_method"):
            parts.append(f"<p><strong>信頼区間:</strong> {data['ci_method']}</p>")

        # Null model
        if data.get("null_model"):
            parts.append(f"<p><strong>Null モデル:</strong> {data['null_model']}</p>")

        # Holdout method
        if data.get("holdout_method"):
            parts.append(f"<p><strong>検証手法:</strong> {data['holdout_method']}</p>")

        # Formula version + computed_at
        fv = data.get("formula_version", "")
        ca = data.get("computed_at", "")
        if fv or ca:
            meta_parts = []
            if fv:
                meta_parts.append(f"formula_version={fv}")
            if ca:
                meta_parts.append(f"computed_at={ca}")
            if data.get("git_sha"):
                meta_parts.append(f"git_sha={data['git_sha']}")
            if data.get("rng_seed") is not None:
                meta_parts.append(f"rng_seed={data['rng_seed']}")
            if data.get("inputs_hash"):
                meta_parts.append(f"inputs_hash={data['inputs_hash'][:12]}…")
            parts.append(f"<p><strong>メタ:</strong> {', '.join(meta_parts)}</p>")

        # Row count
        if data.get("row_count") is not None:
            parts.append(f"<p><strong>行数:</strong> {data['row_count']:,}</p>")

        # Notes
        if data.get("notes"):
            parts.append(f"<p><strong>備考:</strong> {data['notes']}</p>")

        parts.append("</div>")
        return "\n".join(parts)
