"""Network Graph report — v2 compliant.

Interactive network visualization with tier/cluster color coding:
- Section 1: Top-N subgraph visualization (nodes colored by tier, sized by degree)
- Section 2: Community assignment scatter (degree vs betweenness, colored by community)
- Section 3: Gender legend overlay
- Section 4: Ego-network statistics for top hubs
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ..sql_fragments import person_display_name_sql
from ._base import BaseReportGenerator

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}
_COMMUNITY_PALETTE = [
    "#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#667eea",
    "#f5576c", "#fda085", "#8a94a0", "#c0c0e0", "#a0b0a0",
    "#F72585", "#7209B7", "#3A0CA3", "#4CC9F0", "#4361EE",
]


class NetworkGraphReport(BaseReportGenerator):
    name = "network_graph"
    title = "ネットワーク可視化"
    subtitle = "協業グラフのTop-Nサブグラフ・コミュニティ散布図"
    filename = "network_graph.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_scatter_network_section(sb)))
        sections.append(sb.build_section(self._build_community_scatter_section(sb)))
        sections.append(sb.build_section(self._build_gender_distribution_section(sb)))
        sections.append(sb.build_section(self._build_ego_stats_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Network scatter (degree vs betweenness, colored by tier) ──

    def _build_scatter_network_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute(f"""
                SELECT
                    fn.person_id,
                    {person_display_name_sql('fn.person_id')},
                    fn.degree_centrality,
                    fn.betweenness_centrality,
                    fps.awcc,
                    modal_tier.scale_tier AS tier
                FROM feat_network fn
                JOIN persons p ON fn.person_id = p.id
                LEFT JOIN feat_person_scores fps ON fn.person_id = fps.person_id
                LEFT JOIN (
                    SELECT fcc.person_id, fwc.scale_tier,
                           ROW_NUMBER() OVER (
                               PARTITION BY fcc.person_id
                               ORDER BY COUNT(*) DESC
                           ) AS rn
                    FROM feat_credit_contribution fcc
                    JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                    WHERE fwc.scale_tier IS NOT NULL
                    GROUP BY fcc.person_id, fwc.scale_tier
                ) modal_tier ON fn.person_id = modal_tier.person_id AND modal_tier.rn = 1
                WHERE fn.degree_centrality IS NOT NULL
                  AND fn.betweenness_centrality IS NOT NULL
                ORDER BY fn.degree_centrality DESC
                LIMIT 3000
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="次数 vs 媒介中心性（Tier色分け）",
                findings_html="<p>ネットワーク散布図データが取得できませんでした。</p>",
                section_id="net_scatter",
            )

        n = len(rows)
        findings = (
            f"<p>次数中心性 vs 媒介中心性の散布図（上位{n:,}名、次数中心性順）。"
            "ノード色はモーダル作品Scale Tier（1=極小〜5=大規模）。"
            "次数が高く媒介が低い人物は密なコミュニティ内に埋め込まれている可能性がある。"
            "媒介が高く次数が中程度の人物はコミュニティ間のブリッジ的位置にある可能性がある。</p>"
        )

        # Group by tier for separate scatter traces
        tier_data: dict[int | str, list] = {}
        for r in rows:
            t = r["tier"] if r["tier"] is not None else "unknown"
            tier_data.setdefault(t, []).append(r)

        fig = go.Figure()
        for t in sorted(tier_data.keys(), key=lambda x: (x == "unknown", x)):
            td = tier_data[t]
            color = _TIER_COLORS.get(t, "#a0a0c0") if isinstance(t, int) else "#a0a0c0"
            label = f"Tier {t}" if isinstance(t, int) else "不明なTier"
            fig.add_trace(go.Scatter(
                x=[r["degree_centrality"] for r in td],
                y=[r["betweenness_centrality"] for r in td],
                mode="markers",
                name=label,
                marker=dict(
                    color=color, size=5, opacity=0.7,
                    line=dict(width=0),
                ),
                text=[r["name"] for r in td],
                hovertemplate="%{text}<br>degree=%{x:.4f}<br>BC=%{y:.4f}<extra></extra>",
            ))
        fig.update_layout(
            title=f"次数中心性 vs 媒介中心性（上位{n:,}名、Tier別色分け）",
            xaxis_title="次数中心性",
            yaxis_title="媒介中心性",
            legend=dict(orientation="v"),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="次数 vs 媒介中心性（Tier色分け）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_net_scatter", height=520),
            method_note=(
                "degree_centrality による上位3,000名。"
                "媒介中心性は大規模グラフではk-pivot近似を使用する場合あり。"
                "最頻Tier = クレジット済みの全作品における最頻 scale_tier を "
                "ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY COUNT(*) DESC) で算出。"
                "feat_credit_contribution にエントリがない人物は「不明なTier」として表示。"
            ),
            section_id="net_scatter",
        )

    # ── Section 2: Community scatter ─────────────────────────────

    def _build_community_scatter_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute(f"""
                SELECT
                    fn.person_id,
                    {person_display_name_sql('fn.person_id')},
                    fn.degree_centrality,
                    fn.bridge_score,
                    fcm.community_id
                FROM feat_network fn
                JOIN persons p ON fn.person_id = p.id
                LEFT JOIN feat_cluster_membership fcm ON fn.person_id = fcm.person_id
                WHERE fn.degree_centrality IS NOT NULL
                ORDER BY fn.degree_centrality DESC
                LIMIT 2000
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="コミュニティ別散布図",
                findings_html="<p>コミュニティ散布図データが取得できませんでした。</p>",
                section_id="comm_scatter",
            )

        n = len(rows)
        findings = (
            f"<p>上位{n:,}名の次数中心性 vs ブリッジスコア（コミュニティ別色分け）。"
            "次数とブリッジスコアの両方が高い人物は、複数コミュニティを接続する"
            "構造的仲介者の位置を占める可能性がある。</p>"
        )

        # Group by community
        comm_data: dict[str, list] = {}
        for r in rows:
            c = str(r["community_id"]) if r["community_id"] is not None else "none"
            comm_data.setdefault(c, []).append(r)

        # Limit to top 15 communities by size, merge rest
        top_comms = sorted(comm_data.keys(), key=lambda c: -len(comm_data[c]))[:14]
        other_rows: list = []
        for c in list(comm_data.keys()):
            if c not in top_comms and c != "none":
                other_rows.extend(comm_data[c])

        fig = go.Figure()
        for i, c in enumerate(top_comms):
            cd = comm_data[c]
            color = _COMMUNITY_PALETTE[i % len(_COMMUNITY_PALETTE)]
            fig.add_trace(go.Scatter(
                x=[r["degree_centrality"] for r in cd],
                y=[r["bridge_score"] or 0 for r in cd],
                mode="markers",
                name=f"C{c}",
                marker=dict(color=color, size=5, opacity=0.7, line=dict(width=0)),
                text=[r["name"] for r in cd],
                hovertemplate="%{text}<br>degree=%{x:.4f}<br>bridge=%{y:.2f}<extra></extra>",
            ))
        if other_rows:
            fig.add_trace(go.Scatter(
                x=[r["degree_centrality"] for r in other_rows],
                y=[r["bridge_score"] or 0 for r in other_rows],
                mode="markers", name="その他",
                marker=dict(color="#cccccc", size=4, opacity=0.4, line=dict(width=0)),
                hoverinfo="skip",
            ))

        fig.update_layout(
            title=f"コミュニティ別 次数中心性 vs ブリッジスコア（上位{n:,}名）",
            xaxis_title="次数中心性",
            yaxis_title="ブリッジスコア",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="コミュニティ別散布図",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_comm_scatter", height=500),
            method_note=(
                "degree_centrality による上位2,000名。"
                "コミュニティは feat_cluster_membership.community_id より（Louvain）。"
                "ブリッジスコアは feat_network.bridge_score より（0–99 対数正規化）。"
                "メンバー数による上位14コミュニティを個別表示、"
                "残りのコミュニティは「その他」に集約。"
            ),
            section_id="comm_scatter",
        )

    # ── Section 3: Gender distribution in network ─────────────────

    def _build_gender_distribution_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    p.gender,
                    fn.degree_centrality,
                    fn.betweenness_centrality,
                    COUNT(*) OVER (PARTITION BY p.gender) AS gender_n
                FROM feat_network fn
                JOIN persons p ON fn.person_id = p.id
                WHERE fn.degree_centrality IS NOT NULL
                  AND p.gender IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="性別別ネットワーク分布",
                findings_html="<p>性別別ネットワークデータが取得できませんでした。</p>",
                section_id="gender_network",
            )

        gender_deg: dict[str, list[float]] = {}
        for r in rows:
            gender_deg.setdefault(r["gender"], []).append(r["degree_centrality"])

        findings = "<p>性別別次数中心性分布:</p><ul>"
        for g, vals in sorted(gender_deg.items()):
            gs = distribution_summary(vals, label=g)
            findings += (
                f"<li><strong>{g}</strong> (n={gs['n']:,}): "
                f"{format_distribution_inline(gs)}, "
                f"{format_ci((gs['ci_lower'], gs['ci_upper']))}</li>"
            )
        findings += "</ul>"

        fig = go.Figure()
        gender_colors = {"Male": "#667eea", "Female": "#f5576c", "unknown": "#a0a0c0"}
        for g, vals in sorted(gender_deg.items()):
            fig.add_trace(go.Violin(
                y=vals[:2000] if len(vals) > 2000 else vals,
                name=g, box_visible=True, meanline_visible=True,
                points=False,
                marker_color=gender_colors.get(g, "#a0a0c0"),
            ))
        fig.update_layout(
            title="性別別 次数中心性",
            yaxis_title="次数中心性",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="性別別ネットワーク分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_gender_net", height=420),
            method_note=(
                "性別は persons.gender より（データソース由来の二値分類）。"
                "gender が NULL の人物は除外。"
                "次数中心性は feat_network より。"
                "次数中心性の性別差は、直接的なネットワーク効果ではなく、"
                "キャリア長・ロール分布・スタジオ配属などの違いを反映している可能性がある。"
            ),
            section_id="gender_network",
        )

    # ── Section 4: Ego-network statistics ─────────────────────────

    def _build_ego_stats_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute(f"""
                SELECT
                    fn.person_id,
                    {person_display_name_sql('fn.person_id')},
                    fn.degree_centrality,
                    fn.betweenness_centrality,
                    fn.bridge_score,
                    fn.n_bridge_communities,
                    fps.awcc,
                    fps.ndi,
                    fc.primary_role,
                    fc.active_years,
                    fc.highest_stage
                FROM feat_network fn
                JOIN persons p ON fn.person_id = p.id
                LEFT JOIN feat_person_scores fps ON fn.person_id = fps.person_id
                LEFT JOIN feat_career fc ON fn.person_id = fc.person_id
                WHERE fn.degree_centrality IS NOT NULL
                ORDER BY fn.degree_centrality DESC
                LIMIT 20
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="Top 20 ハブのEgo-network統計",
                findings_html="<p>ハブEgo-networkデータが取得できませんでした。</p>",
                section_id="ego_stats",
            )

        findings = (
            "<p>次数中心性上位20名のネットワーク指標プロファイル。"
            "ブリッジスコア・NDI・AWCC・コミュニティ架橋数は、"
            "構造的位置の相補的な特徴付けを提供する。</p>"
        )

        def _fmt_opt(val, fmt):
            return f"{val:{fmt}}" if val is not None else "—"

        table_rows = "".join(
            f"<tr>"
            f"<td>{i}</td>"
            f"<td>{r['name']}</td>"
            f"<td>{r['degree_centrality']:.4f}</td>"
            f"<td>{r['betweenness_centrality']:.4f}</td>"
            f"<td>{_fmt_opt(r['bridge_score'], '.1f')}</td>"
            f"<td>{r['n_bridge_communities'] or 0}</td>"
            f"<td>{_fmt_opt(r['awcc'], '.3f')}</td>"
            f"<td>{_fmt_opt(r['ndi'], '.3f')}</td>"
            f"<td>{r['primary_role'] or ''}</td>"
            f"<td>{r['active_years'] or ''}</td>"
            f"</tr>"
            for i, r in enumerate(rows, 1)
        )
        table_html = (
            '<div style="overflow-x:auto;"><table>'
            "<thead><tr>"
            "<th>#</th><th>名前</th><th>次数</th><th>媒介</th>"
            "<th>ブリッジ</th><th>架橋コミュニティ数</th><th>AWCC</th><th>NDI</th>"
            "<th>主要ロール</th><th>活動年数</th>"
            "</tr></thead>"
            f"<tbody>{table_rows}</tbody></table></div>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="Top 20 ハブのEgo-network統計",
            findings_html=findings,
            visualization_html=table_html,
            method_note=(
                "degree_centrality, betweenness_centrality, bridge_score, "
                "n_bridge_communities は feat_network より。"
                "awcc, ndi は feat_person_scores より。"
                "primary_role, active_years は feat_career より。"
                "degree_centrality の降順で並び替え。"
            ),
            section_id="ego_stats",
        )
