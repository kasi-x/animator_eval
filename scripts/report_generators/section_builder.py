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

import json
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


# =========================================================================
# Module-level HTML templates (unescaped Japanese for readability)
# =========================================================================

_INTERPRETATION_HEADER_HTML = (
    '  <div class="interpretation" style="border-top:1px solid #3a3a5c;margin-top:1.5rem;padding-top:1rem;">\n'
    '    <h3 style="color:#c0a0d0;font-size:1rem;">Interpretation / 解釈</h3>\n'
)

_DATA_STATEMENT_TEMPLATE = """
<div class="card" id="data-statement"
     style="border-left:3px solid #5a5a8a;margin-top:2rem;">
  <h2>Data Statement / データ声明</h2>
  <table style="width:100%;border-collapse:collapse;font-size:0.85rem;">
    <tr>
      <td style="padding:0.5rem;color:#9a9ab0;vertical-align:top;width:25%;">
        <strong>Data Source</strong></td>
      <td style="padding:0.5rem;">{data_source}
        {snapshot}
        {schema}</td>
    </tr>
    <tr>
      <td style="padding:0.5rem;color:#9a9ab0;vertical-align:top;">
        <strong>Coverage &amp; Known Biases</strong></td>
      <td style="padding:0.5rem;">{coverage_notes}</td>
    </tr>
    <tr>
      <td style="padding:0.5rem;color:#9a9ab0;vertical-align:top;">
        <strong>Name Resolution</strong></td>
      <td style="padding:0.5rem;">{name_resolution_notes}</td>
    </tr>
    <tr>
      <td style="padding:0.5rem;color:#9a9ab0;vertical-align:top;">
        <strong>Missing Value Handling</strong></td>
      <td style="padding:0.5rem;">{missing_value_handling}</td>
    </tr>
  </table>
</div>
"""

_DISCLAIMER_HTML = """
<div class="card" id="disclaimer"
     style="border-left:3px solid #e05080;margin-top:1rem;">
  <h2>Disclaimer / 注意事項</h2>
  <div style="font-size:0.82rem;line-height:1.7;color:#b0b0c0;">
    <p><strong>【注意事項】</strong><br>
    本レポートに含まれる数値は、公開クレジットデータに基づくネットワーク構造
    および協業密度の記述的指標である。これらは個人の能力、技量、芸術性、または
    職業的価値の評価ではなく、そのような評価として解釈されるべきではない。</p>
    <p>本指標は測定者が選択した定義・集計単位・時代窓に依存しており、別の選択からは
    別の数値が得られる。本レポートは「客観的真実の開示」ではなく「明示された
    選択に基づく記述」である。</p>
    <p>本指標を採用・報酬・契約・人事評価の単一または主要な根拠として使用することを
    運営者は推奨せず、そのような使用の結果について責任を負わない。</p>

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


# =========================================================================
# Method Note Templates (X4 — x_cross_cutting, 2026-05-02)
# =========================================================================
# Standardized HTML blocks for statistical methods used across O1-O8
# reports. Referenced via SectionBuilder.method_note_from_lineage(method_keys=…).
#
# Authoring rules (v2 REPORT_PHILOSOPHY):
#   - No causal verbs in assumption descriptions.
#   - No evaluative adjectives.
#   - No normative "should" constructions.
#   - Interpretation guidance is phrased associationally ("is associated with",
#     "co-occurs with"), not causally.
# =========================================================================

METHOD_NOTE_TEMPLATES: dict[str, str] = {
    # ------------------------------------------------------------------
    # Cox proportional hazards regression
    # ------------------------------------------------------------------
    "cox": (
        '<div class="method-template" style="margin:0.5rem 0;">'
        "<p><strong>Cox 比例ハザード回帰 (Cox Proportional Hazards):</strong><br>"
        "<em>前提:</em> ハザード比は観測期間を通じて一定 (比例ハザード仮定)。"
        "Schoenfeld 残差検定で仮定の充足を確認する。"
        "<br><em>結果変数:</em> クレジット可視性喪失までの時間 (打ち切り含む)。"
        "<br><em>係数の読み方:</em> ハザード比 HR が 1 を超える場合、共変量が高い群では"
        "クレジット可視性喪失イベントの発生率が高い期間と共起する。"
        "HR は因果効果ではなく観察上の関連を示す。"
        "<br><em>既知の限界:</em> 比例ハザード仮定の違反は係数を歪める。"
        "観測されない交絡 (unobserved confounding) を制御しない。</p>"
        "</div>"
    ),
    # ------------------------------------------------------------------
    # Mann-Whitney U test
    # ------------------------------------------------------------------
    "mwu": (
        '<div class="method-template" style="margin:0.5rem 0;">'
        "<p><strong>Mann-Whitney U 検定 (Mann-Whitney U Test):</strong><br>"
        "<em>前提:</em> 2 群が独立。分布の形状は比較群間で類似。"
        "<br><em>用途:</em> 中央値・分布の位置を比較する非パラメトリック検定。"
        "サンプルサイズが小さい場合、または分布が非正規の場合に適用。"
        "<br><em>結果の読み方:</em> 有意な U 統計量は 2 群の分布位置の差と共起する。"
        "効果量として rank-biserial correlation r を併記する。"
        "<br><em>既知の限界:</em> 同順位 (ties) が多い場合は補正が必要。"
        "効果の方向性は示すが因果関係の根拠にはならない。</p>"
        "</div>"
    ),
    # ------------------------------------------------------------------
    # Kaplan-Meier estimator
    # ------------------------------------------------------------------
    "km": (
        '<div class="method-template" style="margin:0.5rem 0;">'
        "<p><strong>Kaplan-Meier 推定量 (Kaplan-Meier Estimator):</strong><br>"
        "<em>前提:</em> 打ち切り (censoring) は生存時間と独立。"
        "<br><em>用途:</em> クレジット可視性の維持率 (生存関数) を時系列で推定する。"
        "群間比較には log-rank 検定を補完する。"
        "<br><em>結果の読み方:</em> KM 曲線は各時点での「まだクレジットが可視の割合」を示す。"
        "95% CI は Greenwood の式またはブートストラップで算出する。"
        "<br><em>既知の限界:</em> 共変量を調整しない記述統計。"
        "群間差の因果的解釈には追加の識別戦略が必要。</p>"
        "</div>"
    ),
    # ------------------------------------------------------------------
    # Counterfactual estimation with bootstrap CI
    # ------------------------------------------------------------------
    "counterfactual": (
        '<div class="method-template" style="margin:0.5rem 0;">'
        "<p><strong>反事実推定 + Bootstrap CI (Counterfactual + Bootstrap CI):</strong><br>"
        "<em>前提:</em> 反事実シナリオ (key person 不在) を観測データから近似する。"
        "モデルの定式化を明示し、CI で推定の不確実性を表現する。"
        "<br><em>用途:</em> 特定個人が関与しなかった場合の production_scale 変化を推定する。"
        "Bootstrap 回数 B=1,000 (デフォルト)、seed は meta_lineage.rng_seed 参照。"
        "<br><em>結果の読み方:</em> 推定値と 95% CI は「モデルが想定する反事実との差」を示す。"
        "CI の幅が大きい場合、推定の信頼性は低い。"
        "<br><em>既知の限界:</em> 反事実は仮定に依存し観察不可能。"
        "モデルの誤定式化がバイアスを生じる。交絡を完全には除去しない。</p>"
        "</div>"
    ),
    # ------------------------------------------------------------------
    # Louvain community detection
    # ------------------------------------------------------------------
    "louvain": (
        '<div class="method-template" style="margin:0.5rem 0;">'
        "<p><strong>Louvain コミュニティ検出 (Louvain Community Detection):</strong><br>"
        "<em>前提:</em> グラフのモジュラリティ最適化。解は確率的に変動する (乱数シード依存)。"
        "<br><em>用途:</em> 共クレジットグラフから密結合な制作集団を抽出する。"
        "コミュニティ境界は resolution パラメータに依存する (default=1.0)。"
        "<br><em>結果の読み方:</em> 同一コミュニティ内のノードは互いに高密度な共クレジット関係にある。"
        "コミュニティ ID は実行ごとに変わる可能性があり、番号に意味はない。"
        "<br><em>既知の限界:</em> resolution limit (大規模グラフでの小コミュニティ融合)。"
        "結果の安定性を複数 seed で確認することを推奨する。</p>"
        "</div>"
    ),
    # ------------------------------------------------------------------
    # Propensity score matching / IPW
    # ------------------------------------------------------------------
    "propensity": (
        '<div class="method-template" style="margin:0.5rem 0;">'
        "<p><strong>傾向スコアマッチング / IPW (Propensity Score Matching / IPW):</strong><br>"
        "<em>前提:</em> 強い無視可能性 (strong ignorability): 処置割り当ては観測済み共変量で"
        "条件付けると独立。観測されない交絡がないことを仮定。"
        "<br><em>用途:</em> 観測研究で「処置群」と「対照群」の共変量分布を均衡させ、"
        "ATT (処置群への平均処置効果) を推定する。"
        "<br><em>結果の読み方:</em> マッチング後の推定値は観察上の関連を調整したもの。"
        "標準化差 (SMD) でバランス診断を行い、SMD &lt; 0.1 を良好の目安とする。"
        "<br><em>既知の限界:</em> 観測されない交絡には対処しない。"
        "傾向スコアモデルの誤定式化がバランス不良を生じる。</p>"
        "</div>"
    ),
    # ------------------------------------------------------------------
    # Difference-in-Differences (DID)
    # ------------------------------------------------------------------
    "did": (
        '<div class="method-template" style="margin:0.5rem 0;">'
        "<p><strong>差分の差分法 (Difference-in-Differences, DID):</strong><br>"
        "<em>前提:</em> 平行トレンド仮定: 処置前の処置群・対照群のトレンドが平行。"
        "介入前期間のプロット・統計検定で仮定の妥当性を示す。"
        "<br><em>用途:</em> 自然実験的な介入 (政策変更、スタジオ閉鎖等) が"
        "クレジット可視性・ネットワーク指標に与えた変化と共起する差を推定する。"
        "<br><em>結果の読み方:</em> DID 推定量は「介入による差の差」を表す。"
        "強い平行トレンド仮定のもとで介入効果の推定値として解釈できる。"
        "<br><em>既知の限界:</em> 平行トレンド仮定の違反は推定量を歪める。"
        "処置タイミングの不均一性がある場合は staggered DID (Callaway-Sant'Anna 等) を検討する。</p>"
        "</div>"
    ),
    # ------------------------------------------------------------------
    # Weighted PageRank
    # ------------------------------------------------------------------
    "weighted_pagerank": (
        '<div class="method-template" style="margin:0.5rem 0;">'
        "<p><strong>重み付き PageRank (Weighted PageRank):</strong><br>"
        "<em>前提:</em> 共クレジットグラフはエッジ重み付き有向グラフ。"
        "エッジ重み = role_weight × episode_coverage × duration_mult (構造的のみ)。"
        "<br><em>用途:</em> 人物ノードのネットワーク内での位置的重要度を算出する。"
        "収束条件: tol=1e-6, max_iter=100 (デフォルト)。"
        "<br><em>結果の読み方:</em> PageRank 値が高いノードは、"
        "エッジ重みと接続パターンから見てネットワーク内で中心的な位置を占める。"
        "この値は個人の評価ではなく、観測グラフにおける構造的位置の記述である。"
        "<br><em>既知の限界:</em> dangling nodes (孤立ノード) は personalization vector で処理。"
        "グラフ密度・時代窓の選択によって結果は変動する。</p>"
        "</div>"
    ),
}


@dataclass
class KPICard:
    """One headline KPI shown above the section findings.

    The label is short noun phrase (not a conclusion). The value is the
    rendered string ("12,345", "0.42", "n/a"). The ``hint`` is an optional
    one-line caption beneath the value (e.g. "対象期間 1990–2024").
    """

    label: str
    value: str
    hint: str = ""


# v3 helpers — used by reports without a curated KPI list to extract
# headline numbers from their findings text.

_NUMBER_PATTERN = re.compile(
    r"(?P<num>[\d,]+(?:\.\d+)?)\s*(?P<unit>%|名|人|件|回|年|スタジオ|社|ペア|本|作)?"
)


def auto_kpis_from_findings(
    findings_html: str, *, max_cards: int = 4
) -> list[KPICard]:
    """Heuristic: extract up to ``max_cards`` KPI cards from a findings string.

    Looks for ``<strong>...</strong>`` blocks and pulls the first numeric
    token + unit out of each. Returns an empty list when nothing matches.
    The order preserves the document order. Use this only when a report
    has no curated KPI list — curated KPIs always win.
    """
    if not findings_html:
        return []
    cards: list[KPICard] = []
    seen_values: set[str] = set()
    for m in re.finditer(r"<strong[^>]*>([^<]+)</strong>", findings_html):
        block = m.group(1)
        nm = _NUMBER_PATTERN.search(block)
        if not nm:
            continue
        value = nm.group("num") + (nm.group("unit") or "")
        if value in seen_values:
            continue
        seen_values.add(value)
        # Label = the non-numeric prefix of the strong block, trimmed.
        label = _NUMBER_PATTERN.sub("", block).strip(":：. 　")
        if not label:
            label = "値"
        cards.append(KPICard(label=label[:32], value=value))
        if len(cards) >= max_cards:
            break
    return cards


@dataclass
class ReportSection:
    """A single section of a v2/v3-compliant report.

    v3 (2026-05-05): Added ``kpi_cards`` (要点先出し) and ``chart_caption``
    (図 1 文説明). Both are optional but encouraged for every section that
    produces a chart, matching the dense, scannable layout used in
    ``industry_overview.html``.

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
        kpi_cards: 2-6 KPIs rendered above findings as a stats-grid strip.
            Use this to surface the section's headline numbers (n, CI width,
            null-model deviation, etc.) so the reader sees the answer first.
        chart_caption: Single sentence directly under the chart explaining
            "what this chart shows" (axes, units, color encoding). The chart
            title carries the WHAT; the caption carries HOW TO READ IT.
    """

    title: str
    findings_html: str
    visualization_html: str = ""
    method_note: str = ""
    interpretation_html: str | None = None
    section_id: str = ""
    kpi_cards: list[KPICard] | None = None
    chart_caption: str = ""


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

    @staticmethod
    def _check_prohibited_wordlist_en(text_lower: str, terms: list[str], category: str) -> list[str]:
        return [
            f"{category}: '{t}'"
            for t in terms
            if re.search(rf"\b{re.escape(t)}\b", text_lower)
        ]

    @staticmethod
    def _check_prohibited_wordlist_ja(text: str, terms: list[str], category: str) -> list[str]:
        return [f"{category}: '{t}'" for t in terms if t in text]

    @staticmethod
    def _check_prohibited_phrase_en(text_lower: str, terms: list[str], category: str) -> list[str]:
        return [f"{category}: '{t}'" for t in terms if t.lower() in text_lower]

    def validate_findings(self, text: str) -> list[str]:
        """Check findings text for REPORT_PHILOSOPHY v2 violations.

        Returns list of violation descriptions. Empty list = compliant.
        """
        tl = text.lower()
        return (
            self._check_prohibited_wordlist_en(tl, _PROHIBITED_ADJECTIVES_EN, "Evaluative adjective in findings")
            + self._check_prohibited_wordlist_ja(text, _PROHIBITED_ADJECTIVES_JA, "Evaluative adjective in findings (JA)")
            + self._check_prohibited_wordlist_en(tl, _PROHIBITED_CAUSAL_VERBS_EN, "Causal verb in findings")
            + self._check_prohibited_wordlist_ja(text, _PROHIBITED_CAUSAL_JA, "Causal verb in findings (JA)")
            + self._check_prohibited_wordlist_en(tl, _PROHIBITED_NORMATIVE_EN, "Normative expression in findings")
            + self._check_prohibited_wordlist_ja(text, _PROHIBITED_NORMATIVE_JA, "Normative expression in findings (JA)")
            + self._check_prohibited_phrase_en(tl, _PROHIBITED_SELECTION_EN, "Selection rhetoric in findings")
            + self._check_prohibited_wordlist_ja(text, _PROHIBITED_SELECTION_JA, "Selection rhetoric in findings (JA)")
        )

    def _check_required_sections(
        self,
        has_overview: bool,
        has_findings: bool,
        has_method_note: bool,
        has_data_statement: bool,
        has_disclaimers: bool,
    ) -> None:
        missing = []
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

    @staticmethod
    def _check_method_note_source(has_method_note: bool, method_note_auto_generated: bool) -> None:
        if has_method_note and not method_note_auto_generated:
            raise ValueError("Method Note must be auto-generated from meta_lineage")

    def _check_interpretation_has_alt(self, interpretation_html: str | None) -> None:
        if not interpretation_html:
            return
        interp_lower = interpretation_html.lower()
        has_alt = any(t.lower() in interp_lower for t in self._ALT_INTERPRETATION_TOKENS)
        if not has_alt:
            raise ValueError(
                "Interpretation section must contain at least one alternative interpretation"
            )

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
        self._check_required_sections(
            has_overview, has_findings, has_method_note, has_data_statement, has_disclaimers
        )
        self._check_method_note_source(has_method_note, method_note_auto_generated)
        self._check_interpretation_has_alt(interpretation_html)

    @staticmethod
    def _render_kpi_strip_block(section: "ReportSection") -> str:
        """v3: KPI 要点先出しストリップ (industry_overview.html 風).

        Curated ``section.kpi_cards`` 優先。空のときは findings から
        ``auto_kpis_from_findings`` で抽出して表示する。
        """
        cards_data = section.kpi_cards
        if not cards_data:
            cards_data = auto_kpis_from_findings(section.findings_html)
        if not cards_data:
            return ""
        cards: list[str] = []
        for kpi in cards_data:
            hint = (
                f'<div class="hint" style="font-size:0.72rem;color:#7a7a92;'
                f'margin-top:0.25rem;">{kpi.hint}</div>'
                if kpi.hint else ""
            )
            cards.append(
                '<div class="stat-card">'
                f'<div class="value">{kpi.value}</div>'
                f'<div class="label">{kpi.label}</div>'
                f"{hint}"
                "</div>"
            )
        return (
            '  <div class="stats-grid" style="margin-bottom:1rem;">\n    '
            + "\n    ".join(cards)
            + "\n  </div>"
        )

    @staticmethod
    def _render_findings_block(section: "ReportSection") -> str:
        return f'  <div class="findings">\n    {section.findings_html}\n  </div>'

    @staticmethod
    def _default_chart_caption(section: "ReportSection") -> str:
        """v3: chart_caption 未指定時のデフォルト 1 文。

        section.title から「この図は X を示す。横軸/縦軸/凡例は本文と
        Method Note を参照。」を生成する。ReportSection 側で
        chart_caption を明示指定すれば override される。
        """
        if not section.title:
            return ""
        return (
            f"この図は「{section.title}」を可視化する。"
            "軸ラベル / 凡例 / 信頼区間の意味は本文と Method Note を参照。"
        )

    @classmethod
    def _render_visualization_block(cls, section: "ReportSection") -> str:
        if not section.visualization_html:
            return ""
        caption = section.chart_caption or cls._default_chart_caption(section)
        caption_html = (
            '<p class="chart-caption" style="font-size:0.82rem;color:#9a9ab0;'
            f'margin-top:0.5rem;line-height:1.5;">{caption}</p>'
            if caption else ""
        )
        return (
            f'  <div class="chart-container">\n    {section.visualization_html}\n'
            f"  </div>{caption_html}"
        )

    @staticmethod
    def _render_method_note_block(section: "ReportSection") -> str:
        if not section.method_note:
            return ""
        return (
            '  <div class="method-note">\n    <details>\n'
            '      <summary style="cursor:pointer;color:#7b8794;font-size:0.85rem;">Method Note</summary>\n'
            f'      <div style="margin-top:0.5rem;font-size:0.82rem;color:#8a94a0;line-height:1.5;">{section.method_note}</div>\n'
            "    </details>\n  </div>"
        )

    @staticmethod
    def _render_interpretation_block(section: "ReportSection") -> str:
        if not section.interpretation_html:
            return ""
        return f"{_INTERPRETATION_HEADER_HTML}    {section.interpretation_html}\n  </div>"

    def build_section(self, section: ReportSection) -> str:
        """Render a ReportSection to v2/v3-compliant HTML.

        v3 layout (top → bottom):
          1. <h2> section title
          2. KPI strip (optional, ``kpi_cards``) — 要点先出し
          3. Findings — descriptive paragraphs
          4. Chart + chart_caption (optional, ``chart_caption``) — 図 1 文説明
          5. Method note (folded)
          6. Interpretation (optional, labelled)
        """
        sid = section.section_id or section.title.lower().replace(" ", "-")[:40]
        blocks = [
            f'<div class="card report-section" id="sec-{sid}">',
            f"  <h2>{section.title}</h2>",
            self._render_kpi_strip_block(section),
            self._render_findings_block(section),
            self._render_visualization_block(section),
            self._render_method_note_block(section),
            self._render_interpretation_block(section),
            "</div>",
        ]
        return "\n".join(b for b in blocks if b)

    def build_data_statement(
        self, params: DataStatementParams | None = None,
    ) -> str:
        """Render the mandatory data statement (v2 Section 4).

        Reports without data statements are NOT published.
        """
        p = params or DataStatementParams()
        snapshot = f"<br>Snapshot: {p.snapshot_date}" if p.snapshot_date else ""
        schema = f"<br>Schema: v{p.schema_version}" if p.schema_version else ""
        return _DATA_STATEMENT_TEMPLATE.format(
            data_source=p.data_source,
            snapshot=snapshot,
            schema=schema,
            coverage_notes=p.coverage_notes,
            name_resolution_notes=p.name_resolution_notes,
            missing_value_handling=p.missing_value_handling,
        )

    def build_disclaimer(self) -> str:
        """Render the mandatory bilingual disclaimer (v2 Section 9)."""
        return _DISCLAIMER_HTML

    def method_note_from_lineage(
        self,
        table_name: str,
        conn: sqlite3.Connection,
        method_keys: list[str] | None = None,
    ) -> str:
        """meta_lineage を読んで Method Note HTML を自動生成する.

        手書き method_note は禁止。このメソッド経由でのみ生成すること。

        Args:
            table_name: meta_lineage テーブルの table_name 値。
            conn: SQLite 接続。
            method_keys: 追加で挿入する手法テンプレートのキーリスト。
                使用可能なキー: "cox", "mwu", "km", "counterfactual",
                "louvain", "propensity", "did", "weighted_pagerank"。
                None の場合はテンプレートを挿入しない (後方互換)。

        Raises:
            ValueError: table_name が meta_lineage に未登録の場合、または
                        method_keys に未知のキーが含まれる場合。
        """
        data = self._load_lineage_row(table_name, conn)
        silver_tables = self._parse_silver_tables(data)

        parts: list[str] = ['<div style="font-size:0.82rem;line-height:1.6;color:#8a94a0;">']
        if silver_tables:
            parts.append(self._render_silver_sources_p(silver_tables))
        parts.append(self._render_simple_p("指標の説明", data.get("description")))
        if data.get("source_bronze_forbidden", 1):
            parts.append(self._render_score_prohibition_p())
        parts.append(self._render_simple_p("信頼区間", data.get("ci_method")))
        parts.append(self._render_simple_p("Null モデル", data.get("null_model")))
        parts.append(self._render_simple_p("検証手法", data.get("holdout_method")))
        if method_keys:
            parts.append(self._render_method_templates(method_keys))
        parts.append(self._render_meta_paragraph(data))
        parts.append(self._render_row_count_p(data.get("row_count")))
        parts.append(self._render_simple_p("備考", data.get("notes")))
        parts.append("</div>")
        return "\n".join(p for p in parts if p)

    @staticmethod
    def _render_method_templates(method_keys: list[str]) -> str:
        """Render standardized method-note blocks for the given method keys.

        Each method key maps to an HTML block describing the statistical
        method's assumptions, interpretation guidance, and known limitations.
        Blocks are v2-philosophy compliant: no causal verbs, no normative
        claims, no evaluative adjectives in Findings context.

        Raises:
            ValueError: if any key in method_keys is not in METHOD_NOTE_TEMPLATES.
        """
        unknown = [k for k in method_keys if k not in METHOD_NOTE_TEMPLATES]
        if unknown:
            raise ValueError(
                f"Unknown method_key(s): {unknown}. "
                f"Valid keys: {sorted(METHOD_NOTE_TEMPLATES)}"
            )
        blocks = [METHOD_NOTE_TEMPLATES[k] for k in method_keys]
        inner = "\n".join(blocks)
        return (
            '<div class="method-templates" '
            'style="border-top:1px solid #2a2a4a;margin-top:0.8rem;padding-top:0.8rem;">'
            "\n<p><strong>手法詳細 / Method Details:</strong></p>"
            f"\n{inner}\n</div>"
        )

    @staticmethod
    def _load_lineage_row(table_name: str, conn: sqlite3.Connection) -> dict:
        """Fetch the meta_lineage row for table_name as a column→value dict."""
        cursor = conn.execute(
            "SELECT * FROM meta_lineage WHERE table_name = ?", (table_name,)
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError(
                f"No lineage registered for '{table_name}'. "
                "Call register_meta_lineage() before generating method notes."
            )
        col_names = [d[0] for d in cursor.description]
        return dict(zip(col_names, row))

    @staticmethod
    def _parse_silver_tables(data: dict) -> list[str]:
        """JSON-decode source_silver_tables, defaulting to [] on bad data."""
        try:
            return json.loads(data.get("source_silver_tables", "[]"))
        except (ValueError, TypeError):
            return []

    @staticmethod
    def _render_silver_sources_p(silver_tables: list[str]) -> str:
        """データソース paragraph with <code>-wrapped table names."""
        inner = ", ".join(f"<code>{t}</code>" for t in silver_tables)
        return f"<p><strong>データソース (silver 層):</strong> {inner}</p>"

    @staticmethod
    def _render_simple_p(label: str, value: str | None) -> str:
        """`<p><strong>label:</strong> value</p>`; empty string when value is falsy."""
        if not value:
            return ""
        return f"<p><strong>{label}:</strong> {value}</p>"

    @staticmethod
    def _render_score_prohibition_p() -> str:
        """Bilingual confirmation that anime.score was not used (H1 invariant)."""
        return (
            "<p><strong>スコア非使用確認:</strong> "
            "このテーブルの算出に <code>anime.score</code>（視聴者評価）は使用していない。"
            " / <em>anime.score (viewer ratings) was not used in this computation.</em></p>"
        )

    @staticmethod
    def _render_meta_paragraph(data: dict) -> str:
        """Comma-joined formula_version / computed_at / git_sha / rng_seed / inputs_hash."""
        pieces: list[str] = []
        if data.get("formula_version"):
            pieces.append(f"formula_version={data['formula_version']}")
        if data.get("computed_at"):
            pieces.append(f"computed_at={data['computed_at']}")
        if data.get("git_sha"):
            pieces.append(f"git_sha={data['git_sha']}")
        if data.get("rng_seed") is not None:
            pieces.append(f"rng_seed={data['rng_seed']}")
        if data.get("inputs_hash"):
            pieces.append(f"inputs_hash={data['inputs_hash'][:12]}…")
        if not pieces:
            return ""
        return f"<p><strong>メタ:</strong> {', '.join(pieces)}</p>"

    @staticmethod
    def _render_row_count_p(row_count: int | None) -> str:
        """`<p>行数: N,NNN</p>`; empty when row_count is None (0 is valid)."""
        if row_count is None:
            return ""
        return f"<p><strong>行数:</strong> {row_count:,}</p>"
