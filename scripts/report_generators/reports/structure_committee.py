"""制作委員会 (production committee) bipartite influence centrality レポート — v2 compliant.

出資者 × アニメ の bipartite graph を構築し、出資者間 co-investment ネットワークの
構造を可視化する。 視聴者評価は使用しない。

Method overview:
- bipartite graph: nodes = anime / company、edges = 委員会クレジット記録。
  edge weight = 1 + log1p(episodes)。
- company–company projection: 共通 anime あたり min(weight) 加算。
- eigenvector centrality (numpy 実装、power-iteration / weighted-degree fallback)。
- HHI = Σ(share)² × 10000、配信プラットフォーム拡大前後 (2017 境界) で descriptive 比較。

Honest gaps (REPORT_PHILOSOPHY):
- 委員会クレジットの取得は seesaawiki / madb に依存し、coverage は anime 全集合の
  およそ 1.7% (5,679 / 332,941 resolved anime)。
- 出資者 entity resolution 未実施 (株式会社プレフィックス揺れ、子会社統合等)。
- 2017 boundary は配信拡大期の便宜的基準であり、因果推論 (event-study) は別カード
  25-01 にて実施。

Framing (H2 compliance):
  "出資者間 co-investment 集中度" "中心性" "HHI" のみ使用。
  「支配」「独占」「優劣」は禁止 (カード _hard_constraints.md §H2)。

Audience: policy (primary)。
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.analysis.network.committee_centrality import CommitteeCentralityResult

import plotly.graph_objects as go
import structlog

from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator, append_validation_warnings

log = structlog.get_logger(__name__)

# Cap the number of companies plotted in the centrality bar chart.  More
# than this becomes unreadable on a single A4 page.
_TOP_N_CENTRALITY = 20


_PERIOD_LABELS: dict[str, str] = {
    "pre": "前期 (2017 年未満)",
    "post": "後期 (2017 年以降)",
}


class StructureCommitteeReport(BaseReportGenerator):
    """制作委員会 bipartite influence centrality レポート.

    出資者 × アニメ bipartite ネットワークの構造的記述。
    配信プラットフォーム拡大期 (2017 境界) の前後で eigenvector centrality と
    HHI を比較する。  個人・企業への主観的評価は行わない。
    """

    name = "structure_committee"
    title = "制作委員会 bipartite 中心性分析"
    subtitle = (
        "出資者 × アニメ bipartite projection / "
        "eigenvector centrality / "
        "HHI 集中度 (配信拡大期前後の構造比較)"
    )
    filename = "structure_committee.html"
    doc_type = "main"

    def generate(self) -> Path | None:
        from src.analysis.network.committee_centrality import (
            CommitteeCentralityResult,  # noqa: F401 — used in type hints below
            analyze_committee_centrality,
        )

        sb = SectionBuilder()

        result: CommitteeCentralityResult = analyze_committee_centrality(self.conn)

        if not result.memberships:
            log.warning(
                "structure_committee_no_data",
                note=result.coverage_note,
            )
            # Even with no data, emit a thin report so the pipeline registers
            # the document.  Empty-state sections explain the data gap.

        coverage_note = self._build_coverage_note(result)

        sections = [
            self._build_centrality_section(sb, result, coverage_note),
            self._build_hhi_section(sb, result, coverage_note),
            self._build_coverage_section(sb, result, coverage_note),
        ]

        overview_html = self._build_overview(result)
        interpretation_html = self._build_interpretation(result)

        insert_lineage(
            self.conn,
            table_name="meta_structure_committee",
            audience="policy",
            source_silver_tables=["anime", "anime_production_committee"],
            formula_version="v1.0",
            ci_method=(
                "中心性: 点推定のみ (eigenvector / weighted-degree fallback)。 "
                "HHI: 点推定のみ、n_companies < 10 のセルは非表示。"
            ),
            null_model=(
                "Not implemented in this card.  Future work: degree-preserving "
                "bipartite null (chung-lu / configuration model) for "
                "co-investment edge weight significance."
            ),
            holdout_method="Not applicable (descriptive analysis of observed committee memberships)",
            description=(
                "Production committee bipartite influence centrality analysis: "
                "anime × company bipartite graph, projected onto a company–company "
                "co-investment graph with episode-log-weighted edges. "
                "Eigenvector centrality (numpy / power iteration / weighted-degree "
                "fallback) and Herfindahl-Hirschman Index (HHI) reported per "
                "period.  Period split at year=2017 ('delivery-platform expansion' "
                "reference; descriptive contrast only, no causal claim — "
                "event-study causal analysis lives in card 25-01). "
                "No viewer ratings used. "
                "Framing: 'co-investment concentration', 'centrality', 'HHI'; "
                "not 'dominance' or 'monopoly'. "
                "Company-side entity resolution not implemented in this scope."
            ),
            rng_seed=42,
        )

        return self.render_unified_structure(
            sections=sections,
            overview_html=overview_html,
            interpretation_html=interpretation_html,
            meta_table="meta_structure_committee",
            extra_glossary=_GLOSSARY,
        )

    # ------------------------------------------------------------------
    # Coverage note
    # ------------------------------------------------------------------

    def _build_coverage_note(self, result: CommitteeCentralityResult) -> str:
        warning = ""
        if result.low_coverage_warning:
            warning = (
                '<span style="color:#e05080;font-weight:bold;">'
                "⚠ 制作委員会データのカバレッジが低い可能性あり。"
                "出資構造の集中度指標は標本選択バイアスを受ける可能性がある。"
                "</span> "
            )
        return (
            f'<p style="color:#e09050;font-size:0.85rem;">'
            f"[データ品質] {warning}"
            f"{result.coverage_note}"
            f"</p>"
        )

    # ------------------------------------------------------------------
    # Overview
    # ------------------------------------------------------------------

    def _build_overview(self, result: CommitteeCentralityResult) -> str:
        n_periods = len({h.period for h in result.period_hhi if h.hhi is not None})
        n_companies = result.n_unique_companies
        n_anime = result.n_unique_anime

        return (
            "<p>本レポートは、アニメーション業界の制作委員会クレジット記録から "
            "出資者 × アニメの bipartite ネットワークを構築し、"
            "出資者間 co-investment 構造を中心性および HHI で記述する。</p>"
            f"<p>対象 anime: {n_anime:,} 件、対象企業: {n_companies:,} 社。"
            f"配信プラットフォーム拡大期 (2017 境界) 前後の比較: "
            f"HHI 算出可能期間 {n_periods} 期。</p>"
            "<p>全指標は公開クレジット記録に基づく構造的記述であり、"
            "個別企業の評価や市場支配性に関する主張ではない。"
            "因果的解釈 (event-study) は別カード 25-01 にて扱う。</p>"
        )

    # ------------------------------------------------------------------
    # Section 1: top centrality companies (pre/post)
    # ------------------------------------------------------------------

    def _build_centrality_section(
        self,
        sb: SectionBuilder,
        result: CommitteeCentralityResult,
        coverage_note: str,
    ) -> ReportSection:
        if not result.centralities:
            return ReportSection(
                title="出資者間 co-investment 中心性 (pre / post 2017)",
                findings_html=(
                    "<p>中心性データが取得できませんでした。"
                    "委員会クレジットおよび Resolved 層 anime の充足が必要です。</p>"
                    + coverage_note
                ),
                method_note=(
                    "Eigenvector centrality on the company–company projection "
                    "of the anime↔company bipartite graph.  "
                    "Edge weight = 1 + log1p(episodes), min-weight "
                    "co-investment aggregation.  No viewer ratings used."
                ),
                section_id="centrality",
            )

        fig = go.Figure()
        any_data = False

        for period in ("pre", "post"):
            rows = [r for r in result.centralities if r.period == period]
            if not rows:
                continue
            top = sorted(rows, key=lambda r: r.eigenvector_centrality, reverse=True)[
                :_TOP_N_CENTRALITY
            ]
            if not top:
                continue
            any_data = True
            fig.add_trace(go.Bar(
                x=[r.company_name for r in top],
                y=[r.eigenvector_centrality for r in top],
                name=_PERIOD_LABELS.get(period, period),
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    "中心性=%{y:.4f}<br>"
                    f"期間={_PERIOD_LABELS.get(period, period)}<extra></extra>"
                ),
            ))

        if not any_data:
            return ReportSection(
                title="出資者間 co-investment 中心性 (pre / post 2017)",
                findings_html=(
                    "<p>有効な中心性ペアがありませんでした (n_too_small 等)。</p>"
                    + coverage_note
                ),
                method_note=(
                    "Eigenvector centrality on the company–company projection. "
                    "視聴者評価不使用。"
                ),
                section_id="centrality",
            )

        fig.update_layout(
            title=f"出資者間 co-investment 中心性 上位 {_TOP_N_CENTRALITY} 社 (期間別)",
            xaxis_title="企業",
            yaxis_title="Eigenvector centrality",
            barmode="group",
            height=500,
            xaxis=dict(tickangle=-30),
        )

        # Findings — top company per period.
        findings_parts: list[str] = []
        for period in ("pre", "post"):
            rows = [r for r in result.centralities if r.period == period]
            if not rows:
                continue
            top = max(rows, key=lambda r: r.eigenvector_centrality)
            findings_parts.append(
                f"<li><strong>{_PERIOD_LABELS.get(period, period)}</strong>: "
                f"最高中心性 <em>{top.company_name}</em> "
                f"(score={top.eigenvector_centrality:.4f}, "
                f"n_anime_in_period={top.n_anime_in_period})</li>"
            )

        method_note = (
            "中心性算出: networkx.eigenvector_centrality_numpy "
            "(weight='weight') を第一選択、numpy 失敗時は power-iteration "
            f"(max_iter=1000, tol=1e-08)、共に未収束時は重み付き次数中心性に fallback。 "
            f"内部 method note: {result.centrality_note}。 "
            "Projection edge weight = 共通 anime 各々の min(weight_A, weight_B) の和。 "
            "min(weight) 採用は標準的な bipartite 投影流儀 (lighter side bottleneck)。 "
            f"min_anime_per_company={2} 未満の企業は projection から除外。 "
            "視聴者評価不使用。"
        )

        findings_html = (
            "<p>出資者間 co-investment ネットワーク上の eigenvector centrality を "
            "期間別 (2017 境界) に算出し、上位 20 社を並べて比較する。 "
            "中心性は同 graph 内での隣接ノード重要度の伝播強度を表し、"
            "企業の単独優先度評価ではない。</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="出資者間 co-investment 中心性 (pre / post 2017)",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_committee_centrality", height=500),
            method_note=method_note,
            section_id="centrality",
        )

    # ------------------------------------------------------------------
    # Section 2: HHI concentration
    # ------------------------------------------------------------------

    def _build_hhi_section(
        self,
        sb: SectionBuilder,
        result: CommitteeCentralityResult,
        coverage_note: str,
    ) -> ReportSection:
        rows = [h for h in result.period_hhi if h.hhi is not None]
        if not rows:
            return ReportSection(
                title="HHI 集中度 (pre / post 2017)",
                findings_html=(
                    "<p>HHI 算出に十分な企業数 (≥10) を持つ期間がありませんでした。</p>"
                    + coverage_note
                ),
                method_note=(
                    "HHI = Σ(share_i)² × 10000、share_i = 企業 i の anime-membership / 全 membership。 "
                    "n_companies < 10 のセルは非表示。視聴者評価不使用。"
                ),
                section_id="hhi",
            )

        labels = [_PERIOD_LABELS.get(r.period, r.period) for r in rows]
        hhi_vals = [r.hhi for r in rows]
        top10 = [r.top10_share for r in rows]
        n_companies_per = [r.n_companies for r in rows]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=labels,
            y=hhi_vals,
            text=[f"HHI={v:.0f}" for v in hhi_vals],
            textposition="outside",
            customdata=list(zip(top10, n_companies_per)),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "HHI=%{y:.0f}<br>"
                "top-10 share=%{customdata[0]:.3f}<br>"
                "n_companies=%{customdata[1]}<extra></extra>"
            ),
            marker_color=["#7CC8F2", "#E09BC2"],
        ))
        fig.update_layout(
            title="制作委員会 membership 集中度 (HHI, 0–10000 スケール)",
            xaxis_title="期間",
            yaxis_title="HHI",
            height=420,
        )

        findings_parts: list[str] = []
        for r in rows:
            findings_parts.append(
                f"<li><strong>{_PERIOD_LABELS.get(r.period, r.period)}</strong>: "
                f"HHI={r.hhi:.0f}、top-10 share={r.top10_share:.3f}、"
                f"n_companies={r.n_companies:,}、n_memberships={r.n_memberships:,}</li>"
            )

        findings_html = (
            "<p>制作委員会 membership share の Herfindahl-Hirschman Index (HHI) を "
            "期間別に示す。 share_i = 企業 i の anime-membership 数 / 全 membership 数。 "
            "HHI は 0–10000 (10000 = 単一企業独占、0 = 完全分散) の業界標準スケール。</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="HHI 集中度 (pre / post 2017)",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_committee_hhi", height=420),
            method_note=(
                "HHI = Σ(share_i)² × 10000。 share_i は当該期間の "
                "(anime, company) membership 数 / 全 membership 数。 "
                "n_companies < 10 または n_memberships < 5 のセルは非表示 "
                "(統計的に意味薄)。 "
                "top-10 share = 上位 10 社の share 合計 (補助指標)。 "
                "視聴者評価不使用。 "
                "委員会出資者の entity resolution は本スコープ未実施 "
                "(株式会社プレフィックスや表記揺れの可能性)。"
            ),
            section_id="hhi",
        )

    # ------------------------------------------------------------------
    # Section 3: coverage / data lineage transparency
    # ------------------------------------------------------------------

    def _build_coverage_section(
        self,
        sb: SectionBuilder,
        result: CommitteeCentralityResult,
        coverage_note: str,
    ) -> ReportSection:
        findings_html = (
            f"<p>本分析の対象データは Conformed 層 "
            f"<code>anime_production_committee</code> テーブル (主に seesaawiki / "
            f"madb 由来) と Resolved 層 anime の結合を経由する。 "
            f"全 resolved anime のうち委員会クレジットを持つ件数は限定的であり、"
            f"近年の TV シリーズに偏る既知の coverage 偏在がある。</p>"
            f"<ul>"
            f"<li>unique anime with committee record: {result.n_unique_anime:,}</li>"
            f"<li>unique companies: {result.n_unique_companies:,}</li>"
            f"<li>period boundary: {result.boundary_year}</li>"
            f"</ul>"
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="データカバレッジと既知制約",
            findings_html=findings_html,
            method_note=(
                "データ経路: Resolved.anime ⨝ Conformed.anime_production_committee "
                "(SOURCE_IDS_JSON containment / unqualified anime_id join / "
                "standalone fallback の三段)。 委員会データは映画・古典作品ほど "
                "欠落傾向が強く、本指標は近年 TV シリーズ偏重の標本選択を含む。 "
                "出資者社名表記揺れ (株式会社プレフィックス、子会社統合) は "
                "本スコープでは正規化していない。"
            ),
            section_id="coverage",
        )

    # ------------------------------------------------------------------
    # Interpretation
    # ------------------------------------------------------------------

    def _build_interpretation(self, result: CommitteeCentralityResult) -> str:
        hhi_rows = {h.period: h for h in result.period_hhi if h.hhi is not None}
        lines: list[str] = []

        if "pre" in hhi_rows and "post" in hhi_rows:
            pre = hhi_rows["pre"]
            post = hhi_rows["post"]
            delta = (post.hhi or 0) - (pre.hhi or 0)
            direction = "上昇" if delta > 0 else "低下" if delta < 0 else "横ばい"
            lines.append(
                f"HHI は {result.boundary_year} 年境界を挟んで "
                f"{pre.hhi:.0f} → {post.hhi:.0f} ({direction}, Δ={delta:+.0f})。"
            )

        if not lines:
            return ""

        return (
            f"<p>本分析の著者は、以下の構造的パターンを観察する: "
            f"{'　'.join(lines)}</p>"
            f"<p>代替解釈: "
            f"(a) HHI の変化は委員会クレジット記録のカバレッジ拡大／縮小を "
            f"反映している可能性がある (seesaawiki / madb の収録方針が時期で異なる場合)。"
            f"(b) 2017 境界は配信プラットフォーム拡大期の便宜的基準であり、"
            f"製作慣行の構造変化を識別する境界ではない。"
            f"(c) 出資者 entity resolution 未実施のため、子会社統合や"
            f"表記揺れが share 計算に影響している可能性がある。</p>"
            f"<p>因果的解釈 (Netflix 配信参入の effect-study) は別カード 25-01 にて、"
            f"出資者 entity resolution と null model 拡張は今後の課題。</p>"
        )


# ---------------------------------------------------------------------------
# Glossary
# ---------------------------------------------------------------------------

_GLOSSARY: dict[str, str] = {
    "制作委員会 (production committee)": (
        "アニメ作品の出資・権利保有を共同で行う複数企業の集合体。"
        "クレジットに「○○製作委員会」または個別出資企業名として記載される。"
    ),
    "bipartite projection": (
        "anime × company の bipartite graph を company-only graph に射影する操作。"
        "投影 edge weight = 共通 anime 各々の min(weight_A, weight_B) の総和。"
    ),
    "eigenvector_centrality": (
        "隣接ノードの重要度を再帰的に反映するノード重要度指標。"
        "「中心の隣接ほど中心」という関係式の固有値解。"
        "個別企業の市場支配性優先度評価ではない。"
    ),
    "HHI_Herfindahl_Hirschman_Index": (
        "市場集中度の業界標準指標。Σ(share_i)² × 10000。"
        "本レポートでは委員会 membership share に対して算出。"
        "10000 = 完全独占、0 = 完全分散。"
    ),
    "co_investment_concentration": (
        "出資者間の共同出資ネットワークにおける集中度。"
        "個別企業の意思決定支配や交渉力評価ではなく、"
        "観測される構造的共出資パターンの統計記述。"
    ),
    "delivery_platform_expansion_boundary": (
        "Netflix 等配信プラットフォームの日本アニメ大量投資を始めた "
        "2017 年前後を便宜的境界として、descriptive な構造比較に使用。"
        "因果推論ではない (event-study causal analysis は card 25-01)。"
    ),
}


# v3 minimal SPEC
from .._spec import SensitivityAxis, make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="structure_committee",
    audience="policy",
    claim=(
        "制作委員会 (出資者 × アニメ) bipartite ネットワークの "
        "company–company projection 上で、 eigenvector centrality と HHI に "
        "配信プラットフォーム拡大期 (2017 境界) 前後で descriptive な差異が観察される"
    ),
    identifying_assumption=(
        "委員会クレジット (seesaawiki / madb) は出資構造の合理的サンプル代理。 "
        "出資者社名は表記揺れ未補正の生データであり、表記揺れは独立に分布する。 "
        "2017 境界は descriptive contrast 用であり、 causal cut-off ではない。"
        "出資者 entity resolution が未実施で表記揺れ (株式会社プレフィックス等) が "
        "centrality / HHI 計算に bias を入れる可能性が残存する。sensitivity grid で "
        "name-normalization 適用前後の結果差を確認する。"
    ),
    null_model=["N6"],
    sources=["anime", "anime_production_committee"],
    meta_table="meta_structure_committee",
    estimator="bipartite projection + eigenvector centrality + HHI",
    ci_estimator="analytical_se",
    n_resamples=None,
    sensitivity_grid=[
        SensitivityAxis(name="name_normalization", values=["raw", "strip_prefix", "fuzzy_merge_0.9"]),
        SensitivityAxis(name="time_split_year", values=[2015, 2017, 2019]),
        SensitivityAxis(name="min_anime_per_committee", values=[1, 3, 5]),
    ],
    extra_limitations=[
        "委員会クレジットカバレッジは限定的 (近年 TV シリーズ偏重)",
        "出資者 entity resolution 未実施 (株式会社プレフィックス等の表記揺れ)",
        "2017 境界は descriptive contrast 用、 causal cut-off ではない",
        "null model 未実装 (将来課題)",
    ],
    alternative_interpretations=(
        "観察された 2017 前後の差異は配信拡大ではなく seesaawiki / madb の出資者 credit 充実度の年代偏り (近年ほど詳細) を反映している可能性。coverage 補正後の再評価要。",
        "eigenvector centrality の高値は構造的影響力ではなく "
        "表記揺れによる同一企業の複数 node 化が降下しないままの artifact である可能性。fuzzy merge 適用後の再計算要。",
        "company–company projection の HHI は per-anime cap (1 アニメに最大 N 出資者) に依存し、tail 委員会 (8 社以上) を打ち切ると集中度が見かけ上低下する可能性。",
    ),
)
