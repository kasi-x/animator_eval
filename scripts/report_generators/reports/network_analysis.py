"""Network Analysis report — v2 compliant, DB-driven.

Reads pre-computed feat_network and related tables only.
No graph construction, no betweenness computation from scratch.

Sections (each has 2 charts — overview + deeper insight):
- Section 1: Centrality Distribution Overview
- Section 2: Hub vs Broker Analysis
- Section 3: Network Structure by Scale Tier
- Section 4: AKM Mobility Diagnostics
"""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..color_utils import hex_to_rgba
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ..sql_fragments import person_display_name_sql
from ._base import BaseReportGenerator, append_validation_warnings

_TRACK_PALETTE = ["#667eea", "#f093fb", "#FFD166", "#06D6A0", "#f5576c", "#aaaaaa"]

_TIER_COLORS = {
    "large": "#667eea",
    "medium": "#06D6A0",
    "small": "#FFD166",
    "micro": "#f5576c",
    "unknown": "#aaaaaa",
}


class NetworkAnalysisReport(BaseReportGenerator):
    name = "network_analysis"
    title = "ネットワーク分析（DB駆動版）"
    subtitle = "中心性分布 / Hub vs Broker / スケール階層別構造 / AKMモビリティ診断"
    filename = "network_analysis.html"

    glossary_terms = {
        "Betweenness Centrality（媒介中心性）": (
            "ネットワーク上の最短経路にどれだけ多く含まれるかを示す指標。"
            "高い値を持つ人物は異なるグループ間の「橋渡し」的位置にある可能性がある。"
        ),
        "Degree Centrality（次数中心性）": (
            "直接繋がっている協業者数をノード総数で正規化した値。"
            "高いほど多くの人物と直接協業している。"
        ),
        "Hub Score": (
            "HITSアルゴリズムにおけるhubスコア。"
            "多くの高 authority スコアのノードへリンクするノードが高い値を持つ。"
        ),
        "AKM Limited Mobility Bias": (
            "Abowd-Kramarz-Margolisモデルで、スタジオ間移動が少ない場合に"
            "person FE θ が studio FE ψ と交絡して過大推定されるバイアス。"
        ),
        "Scale Tier": (
            "作品の規模階層（large/medium/small/micro）。"
            "スタッフ数・話数・放送形式から導出した feat_work_context.scale_tier。"
        ),
    }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []

        for build_fn in [
            self._build_centrality_overview,
            self._build_centrality_deeper,
            self._build_hub_broker_overview,
            self._build_hub_broker_deeper,
            self._build_tier_structure_overview,
            self._build_tier_structure_deeper,
            self._build_akm_mobility_overview,
            self._build_akm_mobility_deeper,
        ]:
            try:
                sec = build_fn(sb)
            except Exception as exc:
                sec = ReportSection(
                    title="(エラー)",
                    findings_html=f"<p>セクション生成中にエラーが発生しました: {exc}</p>",
                    section_id=f"err_{build_fn.__name__}",
                )
            sections.append(sb.build_section(sec))

        if not sections:
            return None

        return self.write_report(
            "\n".join(sections),
            extra_glossary=self.glossary_terms,
        )

    # ==================================================================
    # Section 1a helpers
    # ==================================================================

    def _fetch_centrality_overview_rows(self) -> list:
        return self.conn.execute("""
            SELECT fn.betweenness_centrality,
                   fn.degree_centrality,
                   fn.eigenvector_centrality,
                   COALESCE(fcm.career_track, 'unknown') AS career_track
            FROM feat_network fn
            LEFT JOIN feat_cluster_membership fcm ON fn.person_id = fcm.person_id
            WHERE fn.degree_centrality > 0
            ORDER BY RANDOM()
            LIMIT 5000
        """).fetchall()

    def _findings_centrality_overview_summary(
        self, rows: list, bc_summ: dict, dc_summ: dict, n_tracks: int
    ) -> str:
        return (
            f"<p>サンプル {len(rows):,} 名（degree_centrality > 0）のうち、"
            f"キャリアトラック別に3指標（媒介中心性・次数中心性・固有ベクトル中心性）の分布を示す。</p>"
            f"<p>媒介中心性: {format_distribution_inline(bc_summ)}、{format_ci((bc_summ['ci_lower'], bc_summ['ci_upper']))}。</p>"
            f"<p>次数中心性: {format_distribution_inline(dc_summ)}、{format_ci((dc_summ['ci_lower'], dc_summ['ci_upper']))}。</p>"
            f"<p>キャリアトラック数: {n_tracks} 種類。</p>"
        )

    def _make_centrality_violin_figure(self, rows: list, tracks: list, color_map: dict) -> go.Figure:
        metrics = [
            ("betweenness_centrality", "媒介中心性"),
            ("degree_centrality", "次数中心性"),
            ("eigenvector_centrality", "固有ベクトル中心性"),
        ]
        fig = go.Figure()
        for track in tracks:
            track_rows = [r for r in rows if r["career_track"] == track]
            if not track_rows:
                continue
            color = color_map[track]
            for metric_key, metric_label in metrics:
                vals = [r[metric_key] or 0.0 for r in track_rows]
                fig.add_trace(go.Violin(
                    x=[metric_label] * len(vals),
                    y=vals,
                    name=track,
                    legendgroup=track,
                    showlegend=(metric_key == "betweenness_centrality"),
                    box_visible=True,
                    meanline_visible=True,
                    line_color=color,
                    fillcolor=hex_to_rgba(color, 0.25),
                    points=False,
                ))
        fig.update_layout(
            title=f"3指標の分布（キャリアトラック別）— サンプル {len(rows):,} 名",
            violinmode="group",
            xaxis_title="指標",
            yaxis_title="値",
            legend_title="キャリアトラック",
            height=480,
        )
        return fig

    # ==================================================================
    # Section 1a: Centrality Distribution Overview (Violin by career_track)
    # ==================================================================

    def _build_centrality_overview(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_centrality_overview_rows()

        if not rows:
            return ReportSection(
                title="中心性分布概要",
                findings_html="<p>feat_networkにデータが存在しません。</p>",
                section_id="centrality_overview",
            )

        tracks = sorted({r["career_track"] for r in rows})
        color_map = {t: _TRACK_PALETTE[i % len(_TRACK_PALETTE)] for i, t in enumerate(tracks)}

        bc_vals = [r["betweenness_centrality"] or 0.0 for r in rows]
        dc_vals = [r["degree_centrality"] or 0.0 for r in rows]
        bc_summ = distribution_summary(bc_vals, label="betweenness_centrality")
        dc_summ = distribution_summary(dc_vals, label="degree_centrality")

        findings = self._findings_centrality_overview_summary(rows, bc_summ, dc_summ, len(tracks))
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="中心性分布概要（キャリアトラック別バイオリン）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_centrality_violin_figure(rows, tracks, color_map),
                "chart_centrality_violin", height=480,
            ),
            method_note=(
                "データ: feat_network（betweenness_centrality, degree_centrality, eigenvector_centrality）を "
                "feat_cluster_membership.career_track と結合。"
                "サンプル: degree_centrality > 0 の人物から最大5,000名をランダム抽出。"
                "バイオリンの幅はカーネル密度推定、箱は四分位範囲、線は平均を示す。"
            ),
            interpretation_html=(
                "<p>各指標の分布形状はトラック間で異なる可能性があるが、キャリアトラックの割り当て自体が"
                "ネットワーク構造から派生しているため、循環的な解釈には注意が必要である。"
                "代替解釈として、分布の差異は単にサンプルサイズやデータ密度の違いを反映している可能性がある。</p>"
            ),
            section_id="centrality_overview",
        )

    # ==================================================================
    # Section 1b helpers
    # ==================================================================

    def _fetch_centrality_deeper_rows(self) -> list:
        return self.conn.execute(f"""
            SELECT fn.person_id,
                   fn.betweenness_centrality,
                   fn.degree_centrality,
                   fn.n_collaborators,
                   fps.iv_score,
                   fps.awcc,
                   COALESCE(fcm.career_track, 'unknown') AS career_track,
                   p.gender,
                   {person_display_name_sql('fn.person_id')}
            FROM feat_network fn
            LEFT JOIN feat_person_scores fps ON fn.person_id = fps.person_id
            LEFT JOIN feat_cluster_membership fcm ON fn.person_id = fcm.person_id
            LEFT JOIN persons p ON fn.person_id = p.id
            WHERE fn.degree_centrality > 0
            ORDER BY RANDOM()
            LIMIT 5000
        """).fetchall()

    def _compute_bridge_quadrant(
        self, bc: np.ndarray, dc: np.ndarray
    ) -> tuple[float, float, int]:
        bc_75 = float(np.percentile(bc, 75))
        dc_50 = float(np.percentile(dc, 50))
        n_bridges = int(((bc >= bc_75) & (dc <= dc_50)).sum())
        return bc_75, dc_50, n_bridges

    def _findings_centrality_deeper_overview(
        self, n_rows: int, n_bridges: int, bc_75: float, dc_50: float
    ) -> str:
        return (
            f"<p>媒介中心性（x軸）と次数中心性（y軸）の散布図。"
            f"点の色は iv_score（濃いほど高い）、サイズは n_collaborators に比例。</p>"
            f"<p>「構造的ブリッジ」（betweenness ≥ 75th pctile かつ degree ≤ 中央値）に該当する人物: "
            f"{n_bridges:,} 名（全体の {n_bridges/n_rows*100:.1f}%）。</p>"
        )

    def _make_centrality_deeper_scatter(
        self, rows: list, tracks: list, color_map: dict, bc_75: float, dc_50: float
    ) -> go.Figure:
        fig = go.Figure()
        for track in tracks:
            sub_rows = [r for r in rows if r["career_track"] == track]
            if not sub_rows:
                continue
            sub_iv = [r["iv_score"] or 0.0 for r in sub_rows]
            sub_nc = [max(r["n_collaborators"] or 1, 1) for r in sub_rows]
            fig.add_trace(go.Scattergl(
                x=[r["betweenness_centrality"] or 0.0 for r in sub_rows],
                y=[r["degree_centrality"] or 0.0 for r in sub_rows],
                mode="markers",
                name=track,
                marker=dict(
                    color=sub_iv,
                    colorscale="Viridis",
                    size=[max(4, int(nc ** 0.4)) for nc in sub_nc],
                    opacity=0.6,
                    showscale=(track == tracks[0]),
                    colorbar=dict(title="iv_score", thickness=12) if track == tracks[0] else None,
                    line=dict(width=0),
                ),
                text=[r["name"] for r in sub_rows],
                hovertemplate=(
                    "%{text}<br>betweenness=%{x:.6f}<br>"
                    "degree=%{y:.6f}<br>iv_score=%{marker.color:.3f}<extra></extra>"
                ),
            ))

        fig.add_shape(type="line", x0=bc_75, x1=bc_75, y0=0, y1=1,
                      xref="x", yref="paper", line=dict(color="#f5576c", dash="dot", width=1))
        fig.add_shape(type="line", x0=0, x1=1, y0=dc_50, y1=dc_50,
                      xref="paper", yref="y", line=dict(color="#667eea", dash="dot", width=1))
        fig.update_layout(
            title="媒介中心性 vs 次数中心性（iv_scoreで色付け）",
            xaxis_title="媒介中心性",
            yaxis_title="次数中心性",
            height=500,
            legend_title="キャリアトラック",
        )
        return fig

    # ==================================================================
    # Section 1b: Centrality Deeper (Betweenness vs Degree scatter)
    # ==================================================================

    def _build_centrality_deeper(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_centrality_deeper_rows()

        if not rows:
            return ReportSection(
                title="媒介中心性 vs 次数中心性 散布図",
                findings_html="<p>データが利用不可。</p>",
                section_id="centrality_deeper",
            )

        bc = np.array([r["betweenness_centrality"] or 0.0 for r in rows])
        dc = np.array([r["degree_centrality"] or 0.0 for r in rows])
        bc_75, dc_50, n_bridges = self._compute_bridge_quadrant(bc, dc)

        tracks = sorted({r["career_track"] for r in rows})
        color_map = {t: _TRACK_PALETTE[i % len(_TRACK_PALETTE)] for i, t in enumerate(tracks)}

        findings = self._findings_centrality_deeper_overview(len(rows), n_bridges, bc_75, dc_50)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="媒介中心性 vs 次数中心性：構造的ブリッジの特定",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_centrality_deeper_scatter(rows, tracks, color_map, bc_75, dc_50),
                "chart_bc_dc_scatter", height=500,
            ),
            method_note=(
                "散布図: x=feat_network.betweenness_centrality, y=feat_network.degree_centrality。"
                "色=feat_person_scores.iv_score, サイズは n_collaborators^0.4 に比例。"
                "ブリッジ象限: betweenness ≥ 75パーセンタイル かつ degree ≤ 中央値（破線で表示）。"
                "degree > 0 の人物から5,000名をランダム抽出。"
            ),
            interpretation_html=(
                "<p>媒介中心性が高く次数中心性が低い人物（右下象限）は、少数の直接接続でも多くの"
                "最短経路上に位置する「純粋ブリッジ」候補である。ただし、この分類はサンプリングの影響を受けるため、"
                "代替解釈として、希少な長距離協業を持つ人物が統計的に有利になっている可能性がある。</p>"
            ),
            section_id="centrality_deeper",
        )

    # ==================================================================
    # Section 2a helpers
    # ==================================================================

    def _fetch_hub_broker_overview_rows(self) -> list:
        return self.conn.execute(f"""
            SELECT fn.person_id,
                   fn.betweenness_centrality,
                   fn.degree_centrality,
                   fn.hub_score,
                   fn.n_collaborators,
                   {person_display_name_sql('fn.person_id')},
                   COALESCE(fcm.career_track, 'unknown') AS career_track
            FROM feat_network fn
            LEFT JOIN persons p ON fn.person_id = p.id
            LEFT JOIN feat_cluster_membership fcm ON fn.person_id = fcm.person_id
            WHERE fn.betweenness_centrality IS NOT NULL
              AND fn.betweenness_centrality > 0
            ORDER BY fn.betweenness_centrality DESC
            LIMIT 100
        """).fetchall()

    def _findings_hub_broker_overview(self, rows: list, summ: dict) -> str:
        findings = (
            f"<p>媒介中心性が正の値を持つ上位100名のプロフィール。"
            f"上位100名の中央値: {summ['median']:.6f}、{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
        )
        findings += "<p>上位10名:</p><ol>"
        for r in rows[:10]:
            findings += (
                f"<li>{r['name']} — betweenness={r['betweenness_centrality']:.6f}、"
                f"degree={r['degree_centrality']:.6f}、hub_score={r['hub_score'] or 0:.4f}、"
                f"n_collaborators={r['n_collaborators'] or 0}</li>"
            )
        findings += "</ol>"
        return findings

    def _make_hub_broker_overview_bar(
        self, top30: list, color_map: dict
    ) -> go.Figure:
        names = [r["name"] for r in top30]
        bc = [r["betweenness_centrality"] for r in top30]
        colors = [color_map.get(r["career_track"], "#aaaaaa") for r in top30]

        fig = go.Figure(go.Bar(
            x=bc,
            y=names,
            orientation="h",
            marker_color=colors,
            text=[f"{v:.6f}" for v in bc],
            textposition="outside",
            hovertemplate="%{y}: %{x:.6f}<extra></extra>",
        ))
        fig.update_layout(
            title="媒介中心性 上位30名",
            xaxis_title="媒介中心性",
            yaxis=dict(autorange="reversed", tickfont=dict(size=10)),
            height=max(400, 18 * len(top30)),
        )
        return fig

    # ==================================================================
    # Section 2a: Hub vs Broker Overview (Top 30 by betweenness bar)
    # ==================================================================

    def _build_hub_broker_overview(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_hub_broker_overview_rows()

        if not rows:
            return ReportSection(
                title="媒介中心性上位100名",
                findings_html="<p>媒介中心性が正の値を持つ人物のデータが存在しません。</p>",
                section_id="hub_broker_overview",
            )

        top30 = rows[:30]
        bc_vals = [r["betweenness_centrality"] for r in rows]
        summ = distribution_summary(bc_vals, label="betweenness_centrality (top 100)")

        all_tracks = sorted({r["career_track"] for r in rows})
        color_map = {t: _TRACK_PALETTE[i % len(_TRACK_PALETTE)] for i, t in enumerate(all_tracks)}

        findings = self._findings_hub_broker_overview(rows, summ)
        findings = append_validation_warnings(findings, sb)

        chart_height = max(400, 18 * len(top30))
        return ReportSection(
            title="媒介中心性上位30名（棒グラフ）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_hub_broker_overview_bar(top30, color_map),
                "chart_top_betweenness_bar", height=chart_height,
            ),
            method_note=(
                "データ: feat_network.betweenness_centrality を降順に並べ、上位100名を取得。"
                "棒グラフは上位30名を表示。色は feat_cluster_membership.career_track を示す。"
                "媒介中心性はパイプラインのPhase 5/6で事前計算（Brandes, Rust または NetworkX）。"
            ),
            interpretation_html=(
                "<p>上位に位置する人物は協業ネットワークの多くの最短経路を通過する位置にある。"
                "これが実際の業務上の影響力を示すか否かは、協業グラフの定義（エッジ重み・対象期間）に依存する。"
                "代替解釈：高い値は、特定の時期・ジャンルへの集中参加の結果として生じる可能性がある。</p>"
            ),
            section_id="hub_broker_overview",
        )

    # ==================================================================
    # Section 2b helpers
    # ==================================================================

    def _fetch_hub_broker_deeper_rows(self) -> list:
        return self.conn.execute(f"""
            SELECT fn.betweenness_centrality,
                   fn.hub_score,
                   fn.degree_centrality,
                   {person_display_name_sql('fn.person_id')},
                   COALESCE(fcm.career_track, 'unknown') AS career_track
            FROM feat_network fn
            LEFT JOIN persons p ON fn.person_id = p.id
            LEFT JOIN feat_cluster_membership fcm ON fn.person_id = fcm.person_id
            WHERE fn.betweenness_centrality IS NOT NULL
              AND fn.hub_score IS NOT NULL
              AND fn.degree_centrality > 0
            ORDER BY RANDOM()
            LIMIT 3000
        """).fetchall()

    def _compute_hub_broker_quadrants(
        self, bc_arr: np.ndarray, hs_arr: np.ndarray
    ) -> tuple[float, float, int, int]:
        bc_75 = float(np.percentile(bc_arr, 75))
        hs_50 = float(np.percentile(hs_arr, 50))
        n_pure_broker = int(((bc_arr >= bc_75) & (hs_arr <= hs_50)).sum())
        n_hub_broker = int(((bc_arr >= bc_75) & (hs_arr > hs_50)).sum())
        return bc_75, hs_50, n_pure_broker, n_hub_broker

    def _findings_hub_broker_deeper(
        self, n_rows: int, n_pure_broker: int, n_hub_broker: int
    ) -> str:
        return (
            f"<p>媒介中心性（x軸）vs Hub Score（y軸）の散布図（{n_rows:,} 名サンプル）。</p>"
            f"<p>「純粋ブリッジ」（betweenness ≥ 75th pctile、hub_score ≤ 中央値）: {n_pure_broker:,} 名。</p>"
            f"<p>「ハブ兼ブリッジ」（betweenness ≥ 75th pctile、hub_score > 中央値）: {n_hub_broker:,} 名。</p>"
        )

    def _make_hub_broker_deeper_scatter(
        self,
        rows: list,
        tracks: list,
        color_map: dict,
        bc_75: float,
        hs_50: float,
        hs_arr: np.ndarray,
    ) -> go.Figure:
        fig = go.Figure()
        for track in tracks:
            sub = [r for r in rows if r["career_track"] == track]
            if not sub:
                continue
            fig.add_trace(go.Scattergl(
                x=[r["betweenness_centrality"] or 0.0 for r in sub],
                y=[r["hub_score"] or 0.0 for r in sub],
                mode="markers",
                name=track,
                marker=dict(
                    color=color_map.get(track, "#aaaaaa"),
                    size=5,
                    opacity=0.55,
                    line=dict(width=0),
                ),
                text=[r["name"] for r in sub],
                hovertemplate="%{text}<br>betweenness=%{x:.6f}<br>hub_score=%{y:.4f}<extra></extra>",
            ))

        fig.add_shape(type="line", x0=bc_75, x1=bc_75, y0=0, y1=1,
                      xref="x", yref="paper", line=dict(color="#f5576c", dash="dot", width=1))
        fig.add_shape(type="line", x0=0, x1=1, y0=hs_50, y1=hs_50,
                      xref="paper", yref="y", line=dict(color="#667eea", dash="dot", width=1))
        fig.add_annotation(x=bc_75, y=float(hs_arr.max()), text="Betweenness 75th",
                           showarrow=False, font=dict(color="#f5576c", size=10))
        fig.update_layout(
            title="媒介中心性 vs Hub Score（キャリアトラック別）",
            xaxis_title="媒介中心性",
            yaxis_title="Hubスコア",
            height=480,
            legend_title="キャリアトラック",
        )
        return fig

    # ==================================================================
    # Section 2b: Hub vs Broker Deeper (betweenness vs hub_score scatter)
    # ==================================================================

    def _build_hub_broker_deeper(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_hub_broker_deeper_rows()

        if not rows:
            return ReportSection(
                title="媒介中心性 vs Hub Score",
                findings_html="<p>hub_scoreデータが利用不可。</p>",
                section_id="hub_broker_deeper",
            )

        bc_arr = np.array([r["betweenness_centrality"] or 0.0 for r in rows])
        hs_arr = np.array([r["hub_score"] or 0.0 for r in rows])
        bc_75, hs_50, n_pure_broker, n_hub_broker = self._compute_hub_broker_quadrants(bc_arr, hs_arr)

        tracks = sorted({r["career_track"] for r in rows})
        color_map = {t: _TRACK_PALETTE[i % len(_TRACK_PALETTE)] for i, t in enumerate(tracks)}

        findings = self._findings_hub_broker_deeper(len(rows), n_pure_broker, n_hub_broker)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="純粋ブリッジ vs ハブ兼ブリッジの識別",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_hub_broker_deeper_scatter(rows, tracks, color_map, bc_75, hs_50, hs_arr),
                "chart_betweenness_hub_scatter", height=480,
            ),
            method_note=(
                "散布図: x=feat_network.betweenness_centrality, y=feat_network.hub_score。"
                "色は feat_cluster_membership.career_track を示す。"
                "純粋ブリッジ象限: betweenness ≥ 75パーセンタイル かつ hub_score ≤ 中央値。"
                "ハブ兼ブリッジ象限: 双方が閾値以上。最大3,000名をランダム抽出。"
            ),
            interpretation_html=(
                "<p>純粋ブリッジは少数の接続先を通じて多くの最短経路を持つ人物であり、"
                "ハブ兼ブリッジは多くの接続を持ちつつも橋渡し機能を果たす人物である。"
                "いずれの分類も、使用したグラフの時間窓と重み付けスキームに依存する点に留意が必要である。</p>"
            ),
            section_id="hub_broker_deeper",
        )

    # ==================================================================
    # Section 3a helpers
    # ==================================================================

    def _fetch_tier_structure_overview_rows(self) -> list:
        return self.conn.execute("""
            SELECT fn.person_id,
                   fn.degree_centrality,
                   fwc.scale_tier
            FROM feat_network fn
            JOIN feat_credit_contribution fcc ON fn.person_id = fcc.person_id
            JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
            WHERE fn.degree_centrality > 0
              AND fwc.scale_tier IS NOT NULL
        """).fetchall()

    def _compute_modal_tier_dc(
        self, rows: list
    ) -> tuple[dict[str, list[float]], dict[str, float]]:
        """Compute per-person modal scale_tier and return tier→DC-list mapping."""
        person_tiers: dict[str, list[str]] = {}
        pid_to_dc: dict[str, float] = {}
        for r in rows:
            person_tiers.setdefault(r["person_id"], []).append(r["scale_tier"])
            if r["person_id"] not in pid_to_dc:
                pid_to_dc[r["person_id"]] = r["degree_centrality"] or 0.0

        person_modal_tier = {
            pid: Counter(tiers).most_common(1)[0][0]
            for pid, tiers in person_tiers.items()
        }
        tier_dc: dict[str, list[float]] = {}
        for pid, tier in person_modal_tier.items():
            tier_dc.setdefault(tier, []).append(pid_to_dc.get(pid, 0.0))
        return tier_dc, pid_to_dc

    def _findings_tier_structure_overview(self, tier_dc: dict, n_persons: int) -> str:
        findings = f"<p>作品スケール階層（modal）別の次数中心性分布（{n_persons:,} 名）。</p>"
        findings += "<ul>"
        for tier in sorted(tier_dc):
            vals = tier_dc[tier]
            s = distribution_summary(vals, label=tier)
            findings += (
                f"<li><strong>{tier}</strong> (n={s['n']:,}): "
                f"median={s['median']:.4f}、{format_ci((s['ci_lower'], s['ci_upper']))}</li>"
            )
        findings += "</ul>"
        return findings

    def _make_tier_structure_overview_box(self, tier_dc: dict) -> go.Figure:
        fig = go.Figure()
        for tier in sorted(tier_dc.keys()):
            color = _TIER_COLORS.get(tier, "#aaaaaa")
            fig.add_trace(go.Box(
                y=tier_dc[tier],
                name=tier,
                marker_color=color,
                boxpoints="outliers",
                jitter=0.3,
            ))
        fig.update_layout(
            title="スケール階層別 次数中心性（箱ひげ図）",
            yaxis_title="次数中心性",
            xaxis_title="スケール階層（最頻作品規模）",
            height=440,
        )
        return fig

    # ==================================================================
    # Section 3a: Network Structure by Scale Tier (Box plots)
    # ==================================================================

    def _build_tier_structure_overview(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_tier_structure_overview_rows()

        if not rows:
            return ReportSection(
                title="スケール階層別 次数中心性",
                findings_html="<p>スケール階層データが利用不可。feat_work_contextにデータが存在しません。</p>",
                section_id="tier_structure_overview",
            )

        tier_dc, pid_to_dc = self._compute_modal_tier_dc(rows)
        findings = self._findings_tier_structure_overview(tier_dc, len(pid_to_dc))
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="スケール階層別ネットワーク構造（箱ひげ図）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_tier_structure_overview_box(tier_dc),
                "chart_tier_degree_box", height=440,
            ),
            method_note=(
                "データ: feat_network.degree_centrality を feat_credit_contribution → feat_work_context.scale_tier と結合。"
                "人物ごとに最頻 scale_tier を割り当て（クレジットのある全作品で最も頻出の階層）。"
                "箱: 四分位範囲、ひげ: 1.5×IQR、外れ値も表示。"
            ),
            interpretation_html=(
                "<p>大規模作品（large tier）に多く参加する人物ほど次数中心性が高い傾向がある場合、"
                "それはスタッフ規模の大きさ（同一作品で出会う人数）を反映している可能性がある。"
                "代替解釈：スケール階層自体が次数中心性の原因ではなく、両者がキャリアステージと交絡している可能性がある。</p>"
            ),
            section_id="tier_structure_overview",
        )

    # ==================================================================
    # Section 3b helpers
    # ==================================================================

    def _fetch_tier_structure_deeper_rows(self) -> list:
        return self.conn.execute("""
            SELECT fn.person_id,
                   fn.betweenness_centrality,
                   fwc.scale_tier,
                   COALESCE(fcm.career_track, 'unknown') AS career_track
            FROM feat_network fn
            JOIN feat_credit_contribution fcc ON fn.person_id = fcc.person_id
            JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
            LEFT JOIN feat_cluster_membership fcm ON fn.person_id = fcm.person_id
            WHERE fn.betweenness_centrality IS NOT NULL
              AND fwc.scale_tier IS NOT NULL
        """).fetchall()

    def _compute_track_tier_heatmap_data(
        self, rows: list
    ) -> tuple[list[str], list[str], list[list[float]], int]:
        """Aggregate mean betweenness by (career_track, modal_scale_tier)."""
        person_data: dict[str, dict] = {}
        for r in rows:
            pid = r["person_id"]
            if pid not in person_data:
                person_data[pid] = {
                    "betweenness": r["betweenness_centrality"] or 0.0,
                    "tiers": [],
                    "career_track": r["career_track"],
                }
            person_data[pid]["tiers"].append(r["scale_tier"])

        cell_vals: dict[tuple[str, str], list[float]] = defaultdict(list)
        for pid, data in person_data.items():
            modal_tier = Counter(data["tiers"]).most_common(1)[0][0]
            cell_vals[(data["career_track"], modal_tier)].append(data["betweenness"])

        tracks = sorted({r["career_track"] for r in rows})
        tiers = sorted({r["scale_tier"] for r in rows})

        z = [
            [float(np.mean(cell_vals.get((track, tier), []))) if cell_vals.get((track, tier)) else 0.0
             for tier in tiers]
            for track in tracks
        ]
        return tracks, tiers, z, len(person_data)

    def _findings_tier_structure_deeper(
        self, n_tracks: int, n_tiers: int, n_persons: int
    ) -> str:
        return (
            f"<p>キャリアトラック（{n_tracks} 種類）× スケール階層（{n_tiers} 種類）の"
            f"セル別平均媒介中心性ヒートマップ（{n_persons:,} 名）。</p>"
        )

    def _make_tier_structure_deeper_heatmap(
        self, tracks: list, tiers: list, z: list[list[float]]
    ) -> go.Figure:
        fig = go.Figure(go.Heatmap(
            z=z,
            x=tiers,
            y=tracks,
            colorscale="Viridis",
            colorbar=dict(title="平均 Betweenness"),
            hovertemplate="track=%{y}<br>tier=%{x}<br>avg_betweenness=%{z:.6f}<extra></extra>",
            zmin=0,
        ))
        fig.update_layout(
            title="平均媒介中心性：キャリアトラック × スケール階層",
            xaxis_title="スケール階層",
            yaxis_title="キャリアトラック",
            height=360,
        )
        return fig

    # ==================================================================
    # Section 3b: Network Structure Deeper (heatmap: career_track × scale_tier)
    # ==================================================================

    def _build_tier_structure_deeper(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_tier_structure_deeper_rows()

        if not rows:
            return ReportSection(
                title="キャリアトラック × スケール階層 ヒートマップ",
                findings_html="<p>データが利用不可。</p>",
                section_id="tier_structure_deeper",
            )

        tracks, tiers, z, n_persons = self._compute_track_tier_heatmap_data(rows)
        findings = self._findings_tier_structure_deeper(len(tracks), len(tiers), n_persons)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="キャリアトラック × スケール階層 平均媒介中心性（ヒートマップ）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_tier_structure_deeper_heatmap(tracks, tiers, z),
                "chart_track_tier_heatmap", height=360,
            ),
            method_note=(
                "ヒートマップの各セルは（最頻 scale_tier, career_track）でグループ化した人物の "
                "平均 betweenness_centrality を示す。最頻階層はクレジットのある全作品で最も頻出の scale_tier。"
                "データは feat_network, feat_credit_contribution, feat_work_context, feat_cluster_membership より。"
            ),
            interpretation_html=(
                "<p>特定のキャリアトラックと作品規模の組み合わせで媒介中心性が高い場合、"
                "そのセルの人物が構造的に重要な位置にいる可能性を示唆する。"
                "ただし、セルのサンプルサイズが小さい場合は平均値が不安定であり、"
                "信頼区間の計算が困難であるため解釈には注意を要する。</p>"
            ),
            section_id="tier_structure_deeper",
        )

    # ==================================================================
    # Section 4a helpers
    # ==================================================================

    def _fetch_akm_mobility_overview_rows(self) -> list:
        return self.conn.execute("""
            SELECT fps.person_fe,
                   fps.studio_fe_exposure,
                   fps.person_fe_n_obs,
                   fn.n_unique_anime,
                   fps.iv_score
            FROM feat_person_scores fps
            JOIN feat_network fn ON fps.person_id = fn.person_id
            WHERE fps.person_fe_n_obs IS NOT NULL
              AND fps.person_fe_n_obs > 0
              AND fn.n_unique_anime IS NOT NULL
        """).fetchall()

    def _compute_akm_mobility_stats(
        self, n_anime: np.ndarray, person_fe: np.ndarray, n_rows: int
    ) -> tuple[float, int]:
        corr = float(np.corrcoef(n_anime, person_fe)[0, 1]) if n_rows >= 10 else float("nan")
        n_single = int((n_anime == 1).sum())
        return corr, n_single

    def _findings_akm_mobility_overview(
        self, n_rows: int, corr: float, n_single: int
    ) -> str:
        return (
            f"<p>AKMモビリティ診断: {n_rows:,} 名を対象に、"
            f"n_unique_anime（作品数 = モビリティの代理変数）と person_fe（個人固定効果）の関係を示す。</p>"
            f"<p>Pearson相関係数（n_unique_anime vs person_fe）: r = {corr:.4f}。</p>"
            f"<p>作品数が1の人物（Limited mobility候補）: {n_single:,} 名"
            f"（全体の {n_single/n_rows*100:.1f}%）。</p>"
        )

    def _make_akm_mobility_overview_scatter(
        self, sample_rows: list, n_total: int
    ) -> go.Figure:
        fig = go.Figure(go.Scattergl(
            x=[r["n_unique_anime"] or 0 for r in sample_rows],
            y=[r["person_fe"] or 0.0 for r in sample_rows],
            mode="markers",
            marker=dict(
                color=[r["studio_fe_exposure"] or 0.0 for r in sample_rows],
                colorscale="Plasma",
                size=4,
                opacity=0.5,
                colorbar=dict(title="studio_fe_exposure", thickness=12),
                line=dict(width=0),
            ),
            hovertemplate=(
                "n_unique_anime=%{x}<br>person_fe=%{y:.4f}<br>"
                "studio_fe_exposure=%{marker.color:.4f}<extra></extra>"
            ),
        ))
        fig.update_layout(
            title=f"n_unique_anime vs person_fe（色: studio_fe_exposure）— {len(sample_rows):,} 名",
            xaxis_title="n_unique_anime（作品数）",
            yaxis_title="person_fe（個人固定効果）",
            height=460,
        )
        return fig

    # ==================================================================
    # Section 4a: AKM Mobility Diagnostics (n_unique_anime vs person_fe scatter)
    # ==================================================================

    def _build_akm_mobility_overview(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_akm_mobility_overview_rows()

        if not rows:
            return ReportSection(
                title="AKMモビリティ診断",
                findings_html="<p>person_fe / n_unique_animeデータが利用不可。</p>",
                section_id="akm_mobility_overview",
            )

        n_anime = np.array([r["n_unique_anime"] or 0 for r in rows], dtype=float)
        person_fe = np.array([r["person_fe"] or 0.0 for r in rows])
        corr, n_single = self._compute_akm_mobility_stats(n_anime, person_fe, len(rows))

        sample_rows = rows if len(rows) <= 4000 else list(rows[:4000])

        findings = self._findings_akm_mobility_overview(len(rows), corr, n_single)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="AKMモビリティ診断：n_unique_anime vs person_fe",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_akm_mobility_overview_scatter(sample_rows, len(rows)),
                "chart_akm_mobility_scatter", height=460,
            ),
            method_note=(
                "散布図: x=feat_network.n_unique_anime（モビリティの代理変数 — クレジットのある作品数）、"
                "y=feat_person_scores.person_fe（AKM 個人固定効果）。"
                "色=studio_fe_exposure。データは person_fe_n_obs > 0 でフィルタ。"
                "Pearson r は全サンプル（サブサンプルではなく）で計算。"
            ),
            interpretation_html=(
                "<p>AKMモデルは同一の連結集合内でスタジオ間を移動する人物が存在することで識別される。"
                "n_unique_anime が少ない人物（Limited mobility）では person_fe が "
                "studio FE と交絡しやすく、推定値の信頼区間が実質的に広い。"
                "負の相関（多作品参加者の person_fe が低い）はスタジオ変動の吸収に伴う"
                "識別改善の副作用である可能性があり、person_fe は構造的な個人寄与の推定値であって"
                "個人寄与の水準を示すものではない。代替解釈として、ネットワーク位置の差異が同値の"
                "個人寄与指標に異なる値を与える可能性も残る。</p>"
            ),
            section_id="akm_mobility_overview",
        )

    # ==================================================================
    # Section 4b helpers
    # ==================================================================

    def _fetch_akm_mobility_deeper_rows(self) -> list:
        return self.conn.execute("""
            SELECT fps.person_fe_n_obs,
                   fps.person_fe,
                   fps.confidence
            FROM feat_person_scores fps
            WHERE fps.person_fe_n_obs IS NOT NULL
              AND fps.person_fe_n_obs > 0
        """).fetchall()

    def _compute_akm_obs_thresholds(
        self, n_obs_arr: np.ndarray, n_rows: int
    ) -> tuple[int, int, int]:
        n_lt5 = int((n_obs_arr < 5).sum())
        n_5to20 = int(((n_obs_arr >= 5) & (n_obs_arr < 20)).sum())
        n_ge20 = int((n_obs_arr >= 20).sum())
        return n_lt5, n_5to20, n_ge20

    def _findings_akm_mobility_deeper(
        self, n_rows: int, summ: dict, n_lt5: int, n_5to20: int, n_ge20: int
    ) -> str:
        return (
            f"<p>AKM推定における観測数（person_fe_n_obs）の分布（{n_rows:,} 名）。</p>"
            f"<p>分布: {format_distribution_inline(summ)}、{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
            f"<p>観測数 &lt; 5（信頼性低）: {n_lt5:,} 名（{n_lt5/n_rows*100:.1f}%）。</p>"
            f"<p>観測数 5–19: {n_5to20:,} 名（{n_5to20/n_rows*100:.1f}%）。</p>"
            f"<p>観測数 ≥ 20（信頼性高）: {n_ge20:,} 名（{n_ge20/n_rows*100:.1f}%）。</p>"
        )

    def _make_akm_mobility_deeper_hist(self, n_obs: list[int]) -> go.Figure:
        cap = 100
        capped = [min(n, cap) for n in n_obs]
        fig = go.Figure(go.Bar(
            x=list(range(cap + 1)),
            y=[capped.count(i) for i in range(cap + 1)],
            marker_color="#667eea",
            opacity=0.75,
            hovertemplate="n_obs=%{x}<br>count=%{y}<extra></extra>",
        ))
        for threshold, label, color in [(5, "n=5", "#f5576c"), (20, "n=20", "#FFD166")]:
            fig.add_shape(type="line", x0=threshold, x1=threshold, y0=0, y1=1,
                          xref="x", yref="paper", line=dict(color=color, dash="dash", width=1.5))
            fig.add_annotation(x=threshold, y=1, yref="paper",
                                text=label, showarrow=False,
                                font=dict(color=color, size=10), yanchor="top")
        fig.update_layout(
            title=f"person_fe_n_obs 分布（x軸: {cap}でキャップ、超過分は {cap} に集約）",
            xaxis_title="person_fe_n_obs（AKM観測数）",
            yaxis_title="人物数",
            height=400,
        )
        return fig

    # ==================================================================
    # Section 4b: AKM Mobility Deeper (histogram of person_fe_n_obs)
    # ==================================================================

    def _build_akm_mobility_deeper(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_akm_mobility_deeper_rows()

        if not rows:
            return ReportSection(
                title="AKM観測数分布",
                findings_html="<p>person_fe_n_obsデータが利用不可。</p>",
                section_id="akm_mobility_deeper",
            )

        n_obs = [r["person_fe_n_obs"] for r in rows]
        n_obs_arr = np.array(n_obs, dtype=float)
        summ = distribution_summary(list(n_obs_arr), label="person_fe_n_obs")
        n_lt5, n_5to20, n_ge20 = self._compute_akm_obs_thresholds(n_obs_arr, len(rows))

        findings = self._findings_akm_mobility_deeper(len(rows), summ, n_lt5, n_5to20, n_ge20)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="AKM観測数分布（Limited Mobility診断）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_akm_mobility_deeper_hist(n_obs),
                "chart_akm_n_obs_hist", height=400,
            ),
            method_note=(
                "feat_person_scores.person_fe_n_obs のヒストグラム（AKM モデルで個人固定効果を推定する際に使用された観測数）。"
                "表示の見やすさのため値は100でキャップ、統計には全値を使用。"
                "n=5 と n=20 の閾値線は一般的な信頼性カットオフを示す。"
            ),
            interpretation_html=(
                "<p>観測数が少ない人物（n &lt; 5）の person_fe 推定値は統計的に不安定であり、"
                "補償根拠として使用する際には信頼区間の明示が必要である。"
                "観測数の分布はデータ収集の偏り（特定年代・スタジオの記録密度の差）を反映している可能性があり、"
                "信頼区間は観測数の平方根に反比例して増大する。</p>"
            ),
            section_id="akm_mobility_deeper",
        )
