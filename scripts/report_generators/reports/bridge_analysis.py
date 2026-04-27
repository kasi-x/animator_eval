"""Bridge Analysis report — v2 compliant.

Computes bridge-like metrics inline from available DB columns since
feat_network.bridge_score and feat_cluster_membership.community_id
are NULL for all persons in the current pipeline state.

Sections (each has overview chart + deeper insight chart):
- Section 1: Betweenness centrality distribution (proxy for bridge importance)
- Section 2: Cross-studio-cluster bridge persons
- Section 3: Career track structure (community proxy)
- Section 4: Betweenness by work scale tier
"""

from __future__ import annotations

import random
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..color_utils import TIER_PALETTE as _TIER_COLORS, hex_to_rgba
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ..sql_fragments import person_display_name_sql
from ._base import BaseReportGenerator, append_validation_warnings

_TRACK_COLORS = {
    "rising": "#06D6A0",
    "stable": "#667eea",
    "declining": "#f5576c",
    "late_bloomer": "#FFD166",
    "one_shot": "#a0d2db",
    "veteran": "#f093fb",
}
_CLUSTER_COLORS = ["#f093fb", "#06D6A0", "#fda085", "#a0d2db", "#FFD166",
                   "#667eea", "#f5576c", "#b8c0ff"]


class BridgeAnalysisReport(BaseReportGenerator):
    name = "bridge_analysis"
    title = "ネットワークブリッジ分析"
    subtitle = "コミュニティ間を接続する人材の分布・スタジオクラスタ横断・構造的特性"
    filename = "bridge_analysis.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_betweenness_distribution(sb)))
        sections.append(sb.build_section(self._build_betweenness_deeper(sb)))
        sections.append(sb.build_section(self._build_cross_studio_bridges(sb)))
        sections.append(sb.build_section(self._build_cross_studio_deeper(sb)))
        sections.append(sb.build_section(self._build_career_track_structure(sb)))
        sections.append(sb.build_section(self._build_career_track_deeper(sb)))
        sections.append(sb.build_section(self._build_tier_betweenness(sb)))
        sections.append(sb.build_section(self._build_tier_betweenness_deeper(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1a helpers ─────────────────────────────────────

    def _fetch_betweenness_distribution_rows(self) -> list:
        try:
            return self.conn.execute(f"""
                SELECT fn.person_id, fn.betweenness_centrality, fn.degree_centrality,
                       {person_display_name_sql('fn.person_id')},
                       fc.first_year, p.gender
                FROM feat_network fn
                JOIN persons p ON fn.person_id = p.id
                LEFT JOIN feat_career fc ON fn.person_id = fc.person_id
                WHERE fn.betweenness_centrality IS NOT NULL
                  AND fn.betweenness_centrality > 0
            """).fetchall()
        except Exception:
            return []

    def _findings_betweenness_overview(self, summ: dict) -> str:
        return (
            f"<p>媒介中心性が正の値を持つ人物は{summ['n']:,}名。"
            f"分布: {format_distribution_inline(summ)}、"
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
        )

    def _findings_gender_strata(self, rows: list, value_key: str) -> str:
        gender_groups: dict[str, list[float]] = {}
        for r in rows:
            g = r["gender"] or "不明"
            gender_groups.setdefault(g, []).append(r[value_key])

        if len(gender_groups) <= 1:
            return ""

        out = "<p>性別別:</p><ul>"
        for g, gvals in sorted(gender_groups.items()):
            gs = distribution_summary(gvals, label=g)
            out += (
                f"<li><strong>{g}</strong> (n={gs['n']:,}): "
                f"median={gs['median']:.6f}、{format_ci((gs['ci_lower'], gs['ci_upper']))}</li>"
            )
        out += "</ul>"
        return out

    def _findings_betweenness_top10(self, rows_sorted: list) -> str:
        out = "<p>媒介中心性上位10名:</p><ol>"
        for r in rows_sorted[:10]:
            out += (
                f"<li>{r['name']} — betweenness={r['betweenness_centrality']:.6f}、"
                f"degree={r['degree_centrality']:.6f}</li>"
            )
        out += "</ol>"
        return out

    def _make_betweenness_violin(self, vals: list[float], summ: dict) -> go.Figure:
        fig = go.Figure()
        fig.add_trace(go.Violin(
            y=vals, name="媒介中心性",
            box_visible=True, meanline_visible=True,
            line_color="#f093fb", fillcolor="rgba(240,147,251,0.3)",
            points="outliers",
        ))
        fig.update_layout(
            title=f"媒介中心性分布 — {len(vals):,}名 (バイオリン)",
            yaxis_title="媒介中心性",
        )
        fig.add_annotation(
            x=0, y=summ["median"],
            text=f"median={summ['median']:.6f}",
            showarrow=True, arrowhead=2, ax=60, ay=0,
            font=dict(color="#f093fb", size=11),
        )
        return fig

    # ── Section 1a: Betweenness Centrality Distribution ────────

    def _build_betweenness_distribution(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_betweenness_distribution_rows()

        if not rows:
            return ReportSection(
                title="媒介中心性分布",
                findings_html="<p>媒介中心性データが利用不可。feat_networkテーブルにデータが存在しない。</p>",
                section_id="betweenness_dist",
            )

        vals = [r["betweenness_centrality"] for r in rows]
        summ = distribution_summary(vals, label="betweenness_centrality")
        rows_sorted = sorted(rows, key=lambda r: r["betweenness_centrality"], reverse=True)

        findings = self._findings_betweenness_overview(summ)
        findings += self._findings_gender_strata(rows, "betweenness_centrality")
        findings += self._findings_betweenness_top10(rows_sorted)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="媒介中心性分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_betweenness_violin(vals, summ),
                "chart_betweenness_violin", height=420,
            ),
            method_note=(
                "媒介中心性は feat_network.betweenness_centrality 由来"
                "（Phase 5/6: Rust拡張またはNetworkX経由のBrandesアルゴリズム）。"
                "現在のパイプライン状態では feat_network.bridge_score が"
                "全員 NULL のため、ブリッジ重要度の代理指標として使用。"
                "バイオリン幅は密度、箱 = IQR、白線 = 中央値、破線 = 平均。"
            ),
            interpretation_html=(
                "<p>媒介中心性は、ネットワーク上の最短経路にどれだけ多く含まれるかを示す指標である。"
                "高い値を持つ人物は異なるコミュニティ間の情報・協業の「橋渡し」的位置にいる可能性がある。"
                "ただし、この指標はグラフ構造のみに依存しており、実際の業務上の仲介機能を直接測定して"
                "いるわけではない。</p>"
            ),
            section_id="betweenness_dist",
        )

    # ── Section 1b helpers ─────────────────────────────────────

    def _fetch_betweenness_deeper_rows(self) -> list:
        try:
            return self.conn.execute(f"""
                SELECT fn.person_id, fn.betweenness_centrality, fn.degree_centrality,
                       fn.n_collaborators,
                       {person_display_name_sql('fn.person_id')},
                       fps.iv_score
                FROM feat_network fn
                JOIN persons p ON fn.person_id = p.id
                LEFT JOIN feat_person_scores fps ON fn.person_id = fps.person_id
                WHERE fn.betweenness_centrality IS NOT NULL
                  AND fn.degree_centrality IS NOT NULL
                  AND fn.betweenness_centrality > 0
            """).fetchall()
        except Exception:
            return []

    def _compute_structural_bridges(
        self, rows: list
    ) -> tuple[float, float, list]:
        all_betw = [r["betweenness_centrality"] for r in rows]
        all_deg = [r["degree_centrality"] for r in rows]
        p75_betw = float(np.percentile(all_betw, 75))
        p25_deg = float(np.percentile(all_deg, 25))
        structural_bridges = [
            r for r in rows
            if r["betweenness_centrality"] >= p75_betw
            and r["degree_centrality"] <= p25_deg
        ]
        return p75_betw, p25_deg, structural_bridges

    def _findings_deeper_overview(self, rows: list, corr: float) -> str:
        return (
            f"<p>{len(rows):,}名の媒介中心性と次数中心性の関係を分析。"
            f"両指標のPearson相関係数は{corr:.4f}。</p>"
        )

    def _findings_structural_bridges(self, structural_bridges: list) -> str:
        out = (
            f"<p>構造的ブリッジ候補（媒介中心性≥75パーセンタイル かつ "
            f"次数中心性≤25パーセンタイル）は{len(structural_bridges):,}名。"
            f"これらの人物は少数の協業者で異なるコミュニティ間を接続している。</p>"
        )
        if structural_bridges:
            sb_sorted = sorted(structural_bridges,
                               key=lambda r: r["betweenness_centrality"], reverse=True)[:5]
            out += "<p>構造的ブリッジ上位5名:</p><ol>"
            for r in sb_sorted:
                out += (
                    f"<li>{r['name']} — betweenness={r['betweenness_centrality']:.6f}、"
                    f"degree={r['degree_centrality']:.6f}、"
                    f"collaborators={r['n_collaborators'] or 0}</li>"
                )
            out += "</ol>"
        return out

    def _make_betweenness_degree_scatter(
        self,
        display_rows: list,
        all_rows: list,
        structural_bridges: list,
        p75_betw: float,
        p25_deg: float,
    ) -> go.Figure:
        betw = [r["betweenness_centrality"] for r in display_rows]
        deg = [r["degree_centrality"] for r in display_rows]
        names = [r["name"] for r in display_rows]
        iv_scores = [r["iv_score"] if r["iv_score"] is not None else 0 for r in display_rows]

        fig = go.Figure()
        fig.add_trace(go.Scattergl(
            x=deg, y=betw,
            mode="markers",
            marker=dict(
                size=[max(3, min(12, (iv or 0) * 50)) for iv in iv_scores],
                color=iv_scores,
                colorscale="Viridis",
                colorbar=dict(title="IVスコア"),
                opacity=0.6,
            ),
            text=names,
            hovertemplate=(
                "%{text}<br>"
                "次数中心性: %{x:.6f}<br>"
                "媒介中心性: %{y:.6f}<extra></extra>"
            ),
        ))

        if structural_bridges:
            sb_display = [r for r in display_rows
                          if r["betweenness_centrality"] >= p75_betw
                          and r["degree_centrality"] <= p25_deg]
            if sb_display:
                fig.add_trace(go.Scattergl(
                    x=[r["degree_centrality"] for r in sb_display],
                    y=[r["betweenness_centrality"] for r in sb_display],
                    mode="markers",
                    marker=dict(size=10, color="rgba(245,87,108,0.0)",
                                line=dict(width=2, color="#f5576c")),
                    name="構造的ブリッジ",
                    hoverinfo="skip",
                ))

        fig.add_hline(y=p75_betw, line_dash="dot", line_color="rgba(240,147,251,0.5)",
                      annotation_text="betweenness P75", annotation_position="top left")
        fig.add_vline(x=p25_deg, line_dash="dot", line_color="rgba(102,126,234,0.5)",
                      annotation_text="degree P25", annotation_position="top right")

        fig.update_layout(
            title=f"媒介中心性 × 次数中心性 ({len(display_rows):,}名)",
            xaxis_title="次数中心性",
            yaxis_title="媒介中心性",
            xaxis_type="log", yaxis_type="log",
            showlegend=True,
        )
        return fig

    # ── Section 1b: Betweenness vs Degree scatter ──────────────

    def _build_betweenness_deeper(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_betweenness_deeper_rows()

        if not rows:
            return ReportSection(
                title="媒介中心性 × 次数中心性",
                findings_html="<p>散布図に必要なデータが利用不可。</p>",
                section_id="betweenness_vs_degree",
            )

        rng = random.Random(42)
        display_rows = rng.sample(rows, min(3000, len(rows))) if len(rows) > 3000 else rows

        corr = float(np.corrcoef(
            [r["betweenness_centrality"] for r in rows],
            [r["degree_centrality"] for r in rows]
        )[0, 1]) if len(rows) >= 3 else float("nan")

        p75_betw, p25_deg, structural_bridges = self._compute_structural_bridges(rows)

        findings = self._findings_deeper_overview(rows, corr)
        findings += self._findings_structural_bridges(structural_bridges)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="媒介中心性 × 次数中心性",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_betweenness_degree_scatter(
                    display_rows, rows, structural_bridges, p75_betw, p25_deg
                ),
                "chart_betweenness_vs_degree", height=500,
            ),
            method_note=(
                "betweenness_centrality（Y）と degree_centrality（X）の散布図、"
                "両指標とも feat_network 由来。両対数スケール。"
                "マーカーサイズは IV スコア（feat_person_scores.iv_score）で表現。"
                "色 = Viridis スケールの IV スコア。"
                "構造的ブリッジ = 媒介中心性高（≥ P75）かつ 次数中心性低（≤ P25） — "
                "少数のリンクで異なるグループを繋ぐ人物。"
                "赤い円は構造的ブリッジ候補をハイライト。"
                "描画パフォーマンスのため3,000点までサブサンプリング。"
            ),
            interpretation_html=(
                "<p>媒介中心性が高く次数中心性が低い人物（左上象限）は「構造的穴」を埋める"
                "ブリッジ型の位置にいる。一方、両方高い人物（右上）はハブ型である。"
                "低い相関係数は、ネットワーク上の仲介者と単なる多接続者が異なる人物であることを"
                "示唆するが、グラフ構築方法（共同クレジットに基づくエッジ生成）のアーティファクト"
                "である可能性も排除できない。</p>"
            ),
            section_id="betweenness_vs_degree",
        )

    # ── Section 2a helpers ─────────────────────────────────────

    def _fetch_cross_studio_bridges_rows(self) -> list:
        try:
            return self.conn.execute(f"""
                SELECT
                    fsa.person_id,
                    {person_display_name_sql('fsa.person_id')},
                    COUNT(DISTINCT fcm_studio.studio_cluster_name) AS n_clusters,
                    COUNT(DISTINCT fsa.studio_id) AS n_studios,
                    SUM(fsa.n_works) AS total_works,
                    fn.betweenness_centrality,
                    fn.n_collaborators,
                    fps.iv_score
                FROM feat_studio_affiliation fsa
                JOIN persons p ON fsa.person_id = p.id
                LEFT JOIN feat_cluster_membership fcm_studio
                    ON fsa.person_id = fcm_studio.person_id
                LEFT JOIN feat_network fn ON fsa.person_id = fn.person_id
                LEFT JOIN feat_person_scores fps ON fsa.person_id = fps.person_id
                WHERE fcm_studio.studio_cluster_name IS NOT NULL
                GROUP BY fsa.person_id
                HAVING n_clusters >= 2
                ORDER BY n_clusters DESC, total_works DESC
            """).fetchall()
        except Exception:
            return []

    def _build_cluster_dist(self, rows: list) -> dict[int, int]:
        cluster_dist: dict[int, int] = {}
        for r in rows:
            nc = r["n_clusters"]
            cluster_dist[nc] = cluster_dist.get(nc, 0) + 1
        return cluster_dist

    def _findings_cluster_distribution(self, rows: list, cluster_dist: dict[int, int]) -> str:
        out = (
            f"<p>2つ以上のスタジオクラスタで活動する人物は{len(rows):,}名。</p>"
            "<p>横断クラスタ数別の人数分布:</p><ul>"
        )
        for nc in sorted(cluster_dist):
            out += f"<li><strong>{nc}クラスタ</strong>: {cluster_dist[nc]:,}名</li>"
        out += "</ul>"
        return out

    def _findings_cross_studio_top20(self, rows: list) -> str:
        out = "<p>スタジオクラスタ横断上位20名:</p><ol>"
        for r in rows[:20]:
            out += (
                f"<li>{r['name']} — {r['n_clusters']}クラスタ、"
                f"{r['n_studios']}スタジオ、{r['total_works']}作品</li>"
            )
        out += "</ol>"
        return out

    def _make_cross_studio_bar(self, cluster_dist: dict[int, int], total: int) -> go.Figure:
        fig = go.Figure(go.Bar(
            x=[str(nc) for nc in sorted(cluster_dist)],
            y=[cluster_dist[nc] for nc in sorted(cluster_dist)],
            marker_color="#f093fb",
            hovertemplate="%{x}クラスタ: %{y:,}名<extra></extra>",
        ))
        fig.update_layout(
            title=f"スタジオクラスタ横断数別人数 ({total:,}名)",
            xaxis_title="横断クラスタ数",
            yaxis_title="人数",
        )
        return fig

    # ── Section 2a: Cross-Studio-Cluster Bridges ───────────────

    def _build_cross_studio_bridges(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_cross_studio_bridges_rows()

        if not rows:
            return ReportSection(
                title="スタジオクラスタ横断ブリッジ",
                findings_html=(
                    "<p>スタジオクラスタ横断データが利用不可。"
                    "feat_studio_affiliationまたはfeat_cluster_membership.studio_cluster_name"
                    "にデータが存在しない。</p>"
                ),
                section_id="cross_studio_bridges",
            )

        cluster_dist = self._build_cluster_dist(rows)
        findings = self._findings_cluster_distribution(rows, cluster_dist)
        findings += self._findings_cross_studio_top20(rows)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="スタジオクラスタ横断ブリッジ",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_cross_studio_bar(cluster_dist, len(rows)),
                "chart_cross_studio_dist", height=400,
            ),
            method_note=(
                "スタジオクラスタ横断ブリッジはインラインで算出: "
                "feat_studio_affiliation と feat_cluster_membership.studio_cluster_name を JOIN し、"
                "人物ごとに COUNT DISTINCT studio_cluster_name を取り、≥ 2 でフィルタ。"
                "スタジオクラスタ名はスタジオ特徴量に対する K-Means クラスタリング"
                "（パイプライン Phase 9）由来。人物のスタジオ所属は"
                "feat_studio_affiliation（person_id × credit_year × studio_id）より取得。"
            ),
            interpretation_html=(
                "<p>スタジオクラスタを横断する人物は、異なるスタジオ文化や制作手法の間を移動している。"
                "横断クラスタ数が多い人物はフリーランスまたは業界内で幅広い経験を持つ可能性がある。"
                "ただし、スタジオクラスタの定義（K-Meansのクラスタ数・特徴量選択）に依存するため、"
                "異なるクラスタリング設定では異なる結果が得られる。</p>"
            ),
            section_id="cross_studio_bridges",
        )

    # ── Section 2b helpers ─────────────────────────────────────

    def _fetch_cross_studio_deeper_rows(self) -> list:
        try:
            return self.conn.execute(f"""
                SELECT
                    fsa.person_id,
                    {person_display_name_sql('fsa.person_id')},
                    COUNT(DISTINCT fcm_studio.studio_cluster_name) AS n_clusters,
                    COUNT(DISTINCT fsa.studio_id) AS n_studios,
                    fn.betweenness_centrality,
                    fps.iv_score
                FROM feat_studio_affiliation fsa
                JOIN persons p ON fsa.person_id = p.id
                LEFT JOIN feat_cluster_membership fcm_studio
                    ON fsa.person_id = fcm_studio.person_id
                LEFT JOIN feat_network fn ON fsa.person_id = fn.person_id
                LEFT JOIN feat_person_scores fps ON fsa.person_id = fps.person_id
                WHERE fcm_studio.studio_cluster_name IS NOT NULL
                  AND fn.betweenness_centrality IS NOT NULL
                GROUP BY fsa.person_id
                HAVING n_clusters >= 1
            """).fetchall()
        except Exception:
            return []

    def _group_by_n_clusters(self, rows: list) -> dict[int, list[float]]:
        cluster_groups: dict[int, list[float]] = {}
        for r in rows:
            nc = r["n_clusters"]
            if r["betweenness_centrality"] and r["betweenness_centrality"] > 0:
                cluster_groups.setdefault(nc, []).append(r["betweenness_centrality"])
        return cluster_groups

    def _findings_cross_studio_deeper(self, rows: list, cluster_groups: dict) -> str:
        out = (
            f"<p>{len(rows):,}名についてクラスタ横断数と媒介中心性の関係を分析。</p>"
            "<p>クラスタ横断数別の媒介中心性分布:</p><ul>"
        )
        for nc in sorted(cluster_groups):
            vals = cluster_groups[nc]
            if len(vals) >= 3:
                s = distribution_summary(vals, label=f"{nc}_clusters")
                out += (
                    f"<li><strong>{nc}クラスタ</strong> (n={s['n']:,}): "
                    f"median={s['median']:.6f}、{format_ci((s['ci_lower'], s['ci_upper']))}</li>"
                )
            else:
                out += f"<li><strong>{nc}クラスタ</strong>: n={len(vals)}（CI算出不可）</li>"
        out += "</ul>"
        return out

    def _make_cross_studio_box(self, cluster_groups: dict[int, list[float]]) -> go.Figure:
        fig = go.Figure()
        for idx, nc in enumerate(sorted(cluster_groups)):
            vals = cluster_groups[nc]
            if len(vals) >= 2:
                color = _CLUSTER_COLORS[idx % len(_CLUSTER_COLORS)]
                fig.add_trace(go.Box(
                    y=vals,
                    name=f"{nc}クラスタ (n={len(vals):,})",
                    marker_color=color,
                    boxpoints="outliers",
                ))
        fig.update_layout(
            title="クラスタ横断数別 媒介中心性分布",
            yaxis_title="媒介中心性",
            xaxis_title="横断クラスタ数",
            yaxis_type="log",
        )
        return fig

    # ── Section 2b: Cross-studio deeper — betweenness vs n_clusters ─

    def _build_cross_studio_deeper(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_cross_studio_deeper_rows()

        if not rows:
            return ReportSection(
                title="クラスタ横断数 × 媒介中心性 × IVスコア",
                findings_html="<p>横断分析に必要なデータが利用不可。</p>",
                section_id="cross_studio_deeper",
            )

        cluster_groups = self._group_by_n_clusters(rows)
        findings = self._findings_cross_studio_deeper(rows, cluster_groups)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="クラスタ横断数 × 媒介中心性",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_cross_studio_box(cluster_groups),
                "chart_cross_studio_box", height=460,
            ),
            method_note=(
                "betweenness_centrality を n_clusters（COUNT DISTINCT studio_cluster_name）"
                "でグルーピングしたボックスプロット。Y軸ログスケール。"
                "betweenness_centrality = 0 または NULL の人物は除外。"
                "これは、より多くのスタジオクラスタ間で仕事をすることが"
                "構造的仲介位置の高さと相関するかを検証する。"
            ),
            interpretation_html=(
                "<p>横断クラスタ数と媒介中心性に正の関係がある場合、スタジオ間を移動する人物が"
                "ネットワーク上でブリッジ的位置を占める傾向を示唆する。ただし、因果の方向は"
                "不明確である。高い媒介中心性を持つ人物がスタジオ間移動の機会を得やすい"
                "（選抜効果）可能性と、スタジオ間移動がネットワーク位置を向上させる"
                "（処遇効果）可能性の両方がある。</p>"
            ),
            section_id="cross_studio_deeper",
        )

    # ── Section 3a helpers ─────────────────────────────────────

    def _fetch_career_track_structure_rows(self) -> list:
        try:
            return self.conn.execute("""
                SELECT fcm.career_track, COUNT(*) AS n_persons
                FROM feat_cluster_membership fcm
                WHERE fcm.career_track IS NOT NULL AND fcm.career_track != ''
                GROUP BY fcm.career_track
                ORDER BY n_persons DESC
            """).fetchall()
        except Exception:
            return []

    def _findings_career_track_breakdown(self, rows: list, total_persons: int) -> str:
        out = (
            f"<p>{len(rows)}種類のキャリアトラックが検出され、合計{total_persons:,}名に割り当て。</p>"
            "<p>キャリアトラック別人数:</p><ul>"
        )
        for r in rows:
            pct = r["n_persons"] / max(total_persons, 1) * 100
            out += (
                f"<li><strong>{r['career_track']}</strong>: "
                f"{r['n_persons']:,}名 ({pct:.1f}%)</li>"
            )
        out += "</ul>"
        return out

    def _make_career_track_pie(self, rows: list, total_persons: int) -> go.Figure:
        labels = [r["career_track"] for r in rows]
        values = [r["n_persons"] for r in rows]
        colors = [_TRACK_COLORS.get(l, "#a0a0c0") for l in labels]

        fig = go.Figure(go.Pie(
            labels=labels,
            values=values,
            marker=dict(colors=colors),
            textinfo="label+percent",
            hovertemplate="%{label}: %{value:,}名 (%{percent})<extra></extra>",
        ))
        fig.update_layout(title=f"キャリアトラック構成 — {total_persons:,}名")
        return fig

    # ── Section 3a: Career Track Structure ─────────────────────

    def _build_career_track_structure(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_career_track_structure_rows()

        if not rows:
            return ReportSection(
                title="キャリアトラック構成",
                findings_html=(
                    "<p>キャリアトラックデータが利用不可。"
                    "feat_cluster_membership.career_trackにデータが存在しない。</p>"
                ),
                section_id="career_track_structure",
            )

        total_persons = sum(r["n_persons"] for r in rows)
        findings = self._findings_career_track_breakdown(rows, total_persons)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="キャリアトラック構成",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_career_track_pie(rows, total_persons),
                "chart_career_track_pie", height=420,
            ),
            method_note=(
                "キャリアトラックは feat_cluster_membership.career_track 由来"
                "（Phase 6 のルールベース分類）。"
                "feat_cluster_membership.community_id が全員 NULL のため、"
                "コミュニティの代理指標として使用。"
                "円グラフは相対比率を表示、ホバーで絶対件数を表示。"
            ),
            section_id="career_track_structure",
        )

    # ── Section 3b helpers ─────────────────────────────────────

    def _fetch_career_track_deeper_rows(self) -> list:
        try:
            return self.conn.execute("""
                SELECT fcm.career_track,
                       fn.betweenness_centrality,
                       fn.degree_centrality,
                       fn.n_collaborators,
                       fps.iv_score
                FROM feat_cluster_membership fcm
                JOIN feat_network fn ON fcm.person_id = fn.person_id
                LEFT JOIN feat_person_scores fps ON fcm.person_id = fps.person_id
                WHERE fcm.career_track IS NOT NULL AND fcm.career_track != ''
                  AND fn.betweenness_centrality IS NOT NULL
            """).fetchall()
        except Exception:
            return []

    def _group_by_career_track(self, rows: list) -> dict[str, list[dict]]:
        track_groups: dict[str, list[dict]] = {}
        for r in rows:
            track_groups.setdefault(r["career_track"], []).append(r)
        return track_groups

    def _findings_career_track_network(self, track_groups: dict, total: int) -> str:
        out = (
            f"<p>{total:,}名のキャリアトラック別ネットワーク指標を分析。</p>"
            "<p>トラック別の媒介中心性と協業者数:</p><ul>"
        )
        for track in sorted(track_groups, key=lambda t: len(track_groups[t]), reverse=True):
            members = track_groups[track]
            betw_vals = [m["betweenness_centrality"] for m in members
                         if m["betweenness_centrality"] and m["betweenness_centrality"] > 0]
            collab_vals = [m["n_collaborators"] for m in members
                           if m["n_collaborators"] is not None]
            if betw_vals:
                s = distribution_summary(betw_vals, label=track)
                avg_collab = sum(collab_vals) / max(len(collab_vals), 1)
                out += (
                    f"<li><strong>{track}</strong> (n={len(members):,}): "
                    f"betweenness median={s['median']:.6f}、"
                    f"平均協業者数={avg_collab:.0f}</li>"
                )
        out += "</ul>"
        return out

    def _make_career_track_betweenness_violin(self, track_groups: dict) -> go.Figure:
        fig = go.Figure()
        for idx, track in enumerate(sorted(track_groups,
                                           key=lambda t: len(track_groups[t]), reverse=True)):
            members = track_groups[track]
            betw_vals = [m["betweenness_centrality"] for m in members
                         if m["betweenness_centrality"] and m["betweenness_centrality"] > 0]
            if len(betw_vals) >= 2:
                color = _TRACK_COLORS.get(track, _CLUSTER_COLORS[idx % len(_CLUSTER_COLORS)])
                fig.add_trace(go.Violin(
                    y=betw_vals,
                    name=f"{track} (n={len(betw_vals):,})",
                    box_visible=True, meanline_visible=True,
                    line_color=color,
                    fillcolor=hex_to_rgba(color, 0.3),
                    points="outliers" if len(betw_vals) > 40 else "all",
                ))
        fig.update_layout(
            title="キャリアトラック別 媒介中心性分布",
            yaxis_title="媒介中心性",
            yaxis_type="log",
            violinmode="group",
        )
        return fig

    # ── Section 3b: Career Track Deeper — Cross-Track Collaboration ─

    def _build_career_track_deeper(self, sb: SectionBuilder) -> ReportSection:
        """Betweenness distribution by career track + cross-track collaboration matrix."""
        rows = self._fetch_career_track_deeper_rows()

        if not rows:
            return ReportSection(
                title="キャリアトラック別ネットワーク指標",
                findings_html="<p>キャリアトラック別ネットワーク分析に必要なデータが利用不可。</p>",
                section_id="career_track_deeper",
            )

        track_groups = self._group_by_career_track(rows)
        findings = self._findings_career_track_network(track_groups, len(rows))
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="キャリアトラック別ネットワーク指標",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_career_track_betweenness_violin(track_groups),
                "chart_career_track_betweenness", height=480,
            ),
            method_note=(
                "career_track 別の betweenness_centrality バイオリンプロット。"
                "Y軸ログスケール。betweenness = 0 の人物は"
                "ゼロに集中してしまうため、バイオリンから除外。"
                "キャリアトラックは feat_cluster_membership（ルールベース分類）由来。"
                "どのキャリア軌跡がよりブリッジ的なネットワーク位置を生み出すかを明らかにする。"
            ),
            interpretation_html=(
                "<p>特定のキャリアトラック（例: veteranやlate_bloomer）が高い媒介中心性を示す場合、"
                "長期のキャリアや転機がネットワーク上のブリッジ的位置と関連している可能性がある。"
                "ただし、キャリアトラックの分類自体がクレジット数・期間に基づくルールベースであるため、"
                "活動期間の長さが両方の指標に影響している交絡因子である可能性がある。</p>"
            ),
            section_id="career_track_deeper",
        )

    # ── Section 4a helpers ─────────────────────────────────────

    def _fetch_tier_betweenness_rows(self) -> list:
        try:
            return self.conn.execute("""
                SELECT
                    fwc.scale_tier AS tier,
                    fwc.scale_label AS tier_label,
                    AVG(fn.betweenness_centrality) AS avg_betweenness,
                    COUNT(DISTINCT fn.person_id) AS n_persons,
                    AVG(fn.degree_centrality) AS avg_degree,
                    AVG(fn.n_collaborators) AS avg_collaborators
                FROM feat_network fn
                JOIN feat_credit_contribution fcc ON fn.person_id = fcc.person_id
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fn.betweenness_centrality IS NOT NULL
                  AND fwc.scale_tier IS NOT NULL
                GROUP BY fwc.scale_tier, fwc.scale_label
                ORDER BY fwc.scale_tier
            """).fetchall()
        except Exception:
            return []

    def _findings_tier_betweenness(self, rows: list) -> str:
        out = "<p>作品規模Tier別の平均媒介中心性と協業者数:</p><ul>"
        for r in rows:
            label = r["tier_label"] or f"Tier {r['tier']}"
            out += (
                f"<li><strong>{label}</strong>: "
                f"平均betweenness={r['avg_betweenness']:.6f}、"
                f"平均degree={r['avg_degree']:.6f}、"
                f"平均協業者数={r['avg_collaborators']:.0f}、"
                f"n={r['n_persons']:,}</li>"
            )
        out += "</ul>"
        return out

    def _make_tier_betweenness_bar(self, rows: list) -> go.Figure:
        tiers = [r["tier"] for r in rows]
        tier_labels = [r["tier_label"] or f"T{r['tier']}" for r in rows]

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(
            x=tier_labels,
            y=[r["avg_betweenness"] for r in rows],
            name="平均媒介中心性",
            marker_color=[_TIER_COLORS.get(t, "#a0a0c0") for t in tiers],
            hovertemplate="%{x}: %{y:.6f}<extra></extra>",
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=tier_labels,
            y=[r["avg_collaborators"] for r in rows],
            name="平均協業者数",
            mode="lines+markers",
            line=dict(color="#FFD166", dash="dash"),
            marker=dict(size=8),
            hovertemplate="%{x}: %{y:.0f}名<extra></extra>",
        ), secondary_y=True)

        fig.update_layout(
            title="作品規模Tier別 平均媒介中心性・協業者数",
            xaxis_title="スケールTier",
        )
        fig.update_yaxes(title_text="平均媒介中心性", secondary_y=False)
        fig.update_yaxes(title_text="平均協業者数", secondary_y=True)
        return fig

    # ── Section 4a: Betweenness by Scale Tier ──────────────────

    def _build_tier_betweenness(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_tier_betweenness_rows()

        if not rows:
            return ReportSection(
                title="作品規模Tier別 媒介中心性",
                findings_html="<p>Tier別媒介中心性データが利用不可。</p>",
                section_id="tier_betweenness",
            )

        findings = self._findings_tier_betweenness(rows)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="作品規模Tier別 媒介中心性",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_tier_betweenness_bar(rows),
                "chart_tier_betweenness", height=420,
            ),
            method_note=(
                "Average betweenness_centrality per scale_tier. "
                "A person may appear in multiple tiers if credited on works of different tiers. "
                "scale_tier from feat_work_context (format + episodes + duration based). "
                "Secondary y-axis shows average n_collaborators. "
                "Uses feat_credit_contribution JOIN feat_work_context for tier assignment."
            ),
            interpretation_html=(
                "<p>大規模作品（高いTier）に参加する人物が高い媒介中心性を示す場合、"
                "大規模制作に参加すること自体が多くのスタッフとの接点を生み、"
                "ネットワーク上のブリッジ的位置を得やすくなる構造を示唆する。"
                "しかし、逆にネットワーク上で有利な位置にいる人物が大規模作品に"
                "選ばれやすいという選抜効果も同時に考えられる。</p>"
            ),
            section_id="tier_betweenness",
        )

    # ── Section 4b helpers ─────────────────────────────────────

    def _fetch_tier_betweenness_deeper_rows(self) -> list:
        try:
            return self.conn.execute("""
                SELECT
                    fwc.scale_tier AS tier,
                    fwc.scale_label AS tier_label,
                    fn.betweenness_centrality,
                    fn.person_id
                FROM feat_network fn
                JOIN feat_credit_contribution fcc ON fn.person_id = fcc.person_id
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fn.betweenness_centrality IS NOT NULL
                  AND fn.betweenness_centrality > 0
                  AND fwc.scale_tier IS NOT NULL
            """).fetchall()
        except Exception:
            return []

    def _deduplicate_tier_person_betweenness(
        self, rows: list
    ) -> tuple[dict[int, list[float]], dict[int, str]]:
        """Take max betweenness per (person, tier) pair; return grouped vals + label map."""
        tier_person: dict[tuple[int, str], float] = {}
        tier_labels_map: dict[int, str] = {}
        for r in rows:
            key = (r["tier"], r["person_id"])
            tier_labels_map[r["tier"]] = r["tier_label"] or f"Tier {r['tier']}"
            if key not in tier_person or r["betweenness_centrality"] > tier_person[key]:
                tier_person[key] = r["betweenness_centrality"]

        tier_groups: dict[int, list[float]] = {}
        for (tier, _pid), val in tier_person.items():
            tier_groups.setdefault(tier, []).append(val)

        return tier_groups, tier_labels_map

    def _findings_tier_betweenness_deeper(
        self, tier_groups: dict, tier_labels_map: dict
    ) -> str:
        out = "<p>Tier別の媒介中心性全分布（ユニーク人物単位）:</p><ul>"
        for tier in sorted(tier_groups):
            vals = tier_groups[tier]
            if len(vals) >= 3:
                s = distribution_summary(vals, label=f"tier_{tier}")
                label = tier_labels_map.get(tier, f"Tier {tier}")
                out += (
                    f"<li><strong>{label}</strong> (n={s['n']:,}): "
                    f"{format_distribution_inline(s)}、"
                    f"{format_ci((s['ci_lower'], s['ci_upper']))}</li>"
                )
        out += "</ul>"
        return out

    def _make_tier_betweenness_violin(
        self, tier_groups: dict, tier_labels_map: dict
    ) -> go.Figure:
        fig = go.Figure()
        for tier in sorted(tier_groups):
            vals = tier_groups[tier]
            if len(vals) >= 2:
                label = tier_labels_map.get(tier, f"Tier {tier}")
                color = _TIER_COLORS.get(tier, "#a0a0c0")
                fig.add_trace(go.Violin(
                    y=vals,
                    name=f"{label} (n={len(vals):,})",
                    box_visible=True, meanline_visible=True,
                    line_color=color,
                    fillcolor=hex_to_rgba(color, 0.3),
                    points="outliers" if len(vals) > 40 else "all",
                ))
        fig.update_layout(
            title="Tier別 媒介中心性全分布（バイオリン）",
            yaxis_title="媒介中心性",
            yaxis_type="log",
            violinmode="group",
        )
        return fig

    # ── Section 4b: Tier deeper — per-tier violin of betweenness ─

    def _build_tier_betweenness_deeper(self, sb: SectionBuilder) -> ReportSection:
        rows = self._fetch_tier_betweenness_deeper_rows()

        if not rows:
            return ReportSection(
                title="Tier別 媒介中心性分布（詳細）",
                findings_html="<p>Tier別媒介中心性分布の詳細データが利用不可。</p>",
                section_id="tier_betweenness_deeper",
            )

        tier_groups, tier_labels_map = self._deduplicate_tier_person_betweenness(rows)
        findings = self._findings_tier_betweenness_deeper(tier_groups, tier_labels_map)
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="Tier別 媒介中心性分布（詳細）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                self._make_tier_betweenness_violin(tier_groups, tier_labels_map),
                "chart_tier_betweenness_violin", height=480,
            ),
            method_note=(
                "scale_tier 別の betweenness_centrality 全分布をバイオリンプロットで表示。"
                "Tierごとに人物単位で重複排除（最大 betweenness を採用）。"
                "Y軸ログスケール。betweenness = 0 の人物は除外。"
                "概観チャートに表示される平均値だけでなく、"
                "Tierごとの分布の全体形状を示す。"
            ),
            section_id="tier_betweenness_deeper",
        )
