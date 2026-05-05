"""Network Evolution report — v2 compliant.

Covers temporal change in network structure:
  1. Network size growth (nodes + edges by year)
  2. Tier composition over time (era panels)
  3. Community stability across eras
  4. Hub turnover (new vs returning hubs per era)
  5. Centrality distribution (degree vs betweenness scatter)
  6. New-entrant rate and retention (area chart)
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..color_utils import TIER_PALETTE as _TIER_COLORS
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class NetworkEvolutionReport(BaseReportGenerator):
    name = "network_evolution"
    title = "ネットワーク時系列変化"
    subtitle = "協業グラフのノード・エッジ成長とTier構成の経年変化"
    filename = "network_evolution.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_size_growth_section(sb)))
        sections.append(sb.build_section(self._build_tier_era_section(sb)))
        sections.append(sb.build_section(self._build_community_stability_section(sb)))
        sections.append(sb.build_section(self._build_hub_turnover_section(sb)))
        sections.append(sb.build_section(self._build_centrality_scatter_section(sb)))
        sections.append(sb.build_section(self._build_entrant_rate_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Network size growth ───────────────────────────

    def _build_size_growth_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    a.year,
                    COUNT(DISTINCT c.person_id) AS active_persons,
                    COUNT(c.id) AS total_credits,
                    COUNT(DISTINCT a.id) AS n_works
                FROM conformed.anime a
                JOIN conformed.credits c ON c.anime_id = a.id
                WHERE a.year BETWEEN 1970 AND 2024
                  AND a.year IS NOT NULL
                GROUP BY a.year
                ORDER BY a.year
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="ネットワーク規模の成長",
                findings_html="<p>ネットワーク規模データが利用できません。</p>",
                section_id="size_growth",
            )

        years = [r["year"] for r in rows]
        persons = [r["active_persons"] for r in rows]
        credits_ = [r["total_credits"] for r in rows]

        findings = (
            f"<p>年間アクティブ人数とクレジット総数（1970–2024年, n={len(years)}年分）。"
            "ネットワークのノード数（アクティブ人数）とクレジット数を2軸で表示。</p>"
        )

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=years, y=persons, name="年間アクティブ人数",
            line=dict(color="#7CC8F2", width=2),
            hovertemplate="%{x}: %{y:,} 人<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=years, y=credits_, name="年間クレジット数",
            line=dict(color="#E09BC2", width=1.5, dash="dot"),
            yaxis="y2",
            hovertemplate="%{x}: %{y:,} 件<extra></extra>",
        ))
        fig.update_layout(
            title="ネットワーク規模の成長 (1970–2024)",
            xaxis_title="年",
            yaxis=dict(title="アクティブ人数", side="left"),
            yaxis2=dict(title="クレジット総数", side="right", overlaying="y"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )

        return ReportSection(
            title="ネットワーク規模の成長",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_size_growth", height=420),
            method_note=(
                "アクティブ人数 = 当該年の credits JOIN conformed.anime におけるユニーク person_id 数。"
                "クレジット数 = 当該年の credits 行数の合計。"
                "2023–2025年の値はクレジットが遡って追加されることで増加する可能性がある。"
            ),
            section_id="size_growth",
        )

    # ── Section 2: Tier composition over eras ────────────────────

    def _build_tier_era_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    (a.year / 10) * 10 AS decade,
                    fwc.scale_tier,
                    COUNT(DISTINCT a.id) AS n_works
                FROM conformed.anime a
                JOIN feat_work_context fwc ON fwc.anime_id = a.id
                WHERE a.year BETWEEN 1970 AND 2024
                  AND fwc.scale_tier IS NOT NULL
                GROUP BY decade, fwc.scale_tier
                ORDER BY decade, fwc.scale_tier
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="Tier構成の時系列変化",
                findings_html="<p>Tier×年代データが利用できません。</p>",
                section_id="tier_era",
            )

        decade_tier: dict[int, dict[int, int]] = {}
        for r in rows:
            decade_tier.setdefault(r["decade"], {})[r["scale_tier"]] = r["n_works"]

        decades = sorted(decade_tier.keys())
        findings = "<p>年代別のスケールTier構成比:</p><ul>"
        for d in decades:
            total = sum(decade_tier[d].values())
            tier_str = ", ".join(
                f"T{t}: {100*n/total:.0f}%"
                for t, n in sorted(decade_tier[d].items())
            )
            findings += f"<li><strong>{d}s</strong> ({total:,} works): {tier_str}</li>"
        findings += "</ul>"

        fig = go.Figure()
        for tier in sorted({t for d in decade_tier.values() for t in d}):
            fig.add_trace(go.Bar(
                x=[str(d) for d in decades],
                y=[decade_tier[d].get(tier, 0) for d in decades],
                name=f"Tier {tier}",
                marker_color=_TIER_COLORS.get(tier, "#a0a0c0"),
            ))
        fig.update_layout(
            title="年代別 作品スケール階層構成",
            barmode="stack",
            xaxis_title="年代", yaxis_title="作品数",
        )

        return ReportSection(
            title="Tier構成の時系列変化",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_tier_era", height=420),
            method_note=(
                "scale_tier は feat_work_context より（1=micro, 5=major）。"
                "階層分類は format + 話数 + 放送時間のみで決定（視聴者評価は使用しない）。"
            ),
            section_id="tier_era",
        )

    # ── Section 3: Community stability ────────────────────────────

    def _build_community_stability_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fcm.community_id,
                    (fc.first_year / 10) * 10 AS debut_decade,
                    COUNT(*) AS n
                FROM feat_cluster_membership fcm
                JOIN feat_career fc ON fcm.person_id = fc.person_id
                WHERE fc.first_year BETWEEN 1970 AND 2019
                GROUP BY fcm.community_id, debut_decade
                ORDER BY fcm.community_id, debut_decade
            """).fetchall()
            top_comms_row = self.conn.execute("""
                SELECT community_id, COUNT(*) AS n
                FROM feat_cluster_membership
                GROUP BY community_id
                ORDER BY n DESC
                LIMIT 10
            """).fetchall()
        except Exception:
            rows = []
            top_comms_row = []

        if not rows or not top_comms_row:
            return ReportSection(
                title="コミュニティ世代構成",
                findings_html="<p>コミュニティ安定性データが利用できません。</p>",
                section_id="community_stability",
            )

        top_comm_ids = {r["community_id"] for r in top_comms_row}

        comm_decade: dict[str, dict[int, int]] = {}
        for r in rows:
            cid = str(r["community_id"])
            if r["community_id"] not in top_comm_ids:
                continue
            comm_decade.setdefault(cid, {})[r["debut_decade"]] = r["n"]

        decades = sorted({r["debut_decade"] for r in rows})

        findings = (
            "<p>上位10コミュニティのデビュー年代構成。"
            "複数年代のメンバーを含むコミュニティは、時間を超えた安定的なメンバーシップ基準を持つ。</p>"
        )

        fig = go.Figure()
        decade_colors = ["#3593D2", "#7CC8F2", "#3BC494", "#F8EC6A", "#E09BC2",
                         "#E07532", "#FFB444", "#8a94a0"]
        for i, d in enumerate(decades):
            fig.add_trace(go.Bar(
                x=list(comm_decade.keys()),
                y=[comm_decade[c].get(d, 0) for c in comm_decade],
                name=f"{d}s",
                marker_color=decade_colors[i % len(decade_colors)],
            ))
        fig.update_layout(
            title="メンバーのデビュー年代別コミュニティ構成（上位10コミュニティ）",
            barmode="stack",
            xaxis_title="コミュニティID", yaxis_title="メンバー数",
        )

        return ReportSection(
            title="コミュニティ世代構成",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_comm_stability", height=420),
            method_note=(
                "コミュニティメンバーシップは feat_cluster_membership より。"
                "デビュー年代 = (feat_career.first_year / 10) * 10。"
                "総メンバー数による上位10コミュニティのみ表示。"
            ),
            section_id="community_stability",
        )

    # ── Section 4: Hub turnover ───────────────────────────────────

    def _build_hub_turnover_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    (fc.first_year / 10) * 10 AS debut_decade,
                    fn.degree_centrality,
                    fn.person_id
                FROM feat_network fn
                JOIN feat_career fc ON fn.person_id = fc.person_id
                WHERE fn.degree_centrality IS NOT NULL
                  AND fc.first_year BETWEEN 1970 AND 2019
                ORDER BY fn.degree_centrality DESC
                LIMIT 3000
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="ハブの世代交代",
                findings_html="<p>ハブ交代データが利用できません。</p>",
                section_id="hub_turnover",
            )

        decade_vals: dict[int, list[float]] = {}
        for r in rows:
            decade_vals.setdefault(r["debut_decade"], []).append(r["degree_centrality"])

        findings = "<p>上位3,000人の次数中心性分布（デビュー年代別）:</p><ul>"
        for d in sorted(decade_vals):
            ds = distribution_summary(decade_vals[d], label=str(d))
            findings += (
                f"<li><strong>{d}年代コホート</strong> (n={ds['n']:,}): "
                f"{format_distribution_inline(ds)}, "
                f"{format_ci((ds['ci_lower'], ds['ci_upper']))}</li>"
            )
        findings += (
            "</ul><p>注: このサンプルは現在の次数が高い人物に偏っている。</p>"
        )

        fig = go.Figure()
        decade_colors = ["#3593D2", "#7CC8F2", "#3BC494", "#F8EC6A", "#E09BC2",
                         "#E07532"]
        for i, d in enumerate(sorted(decade_vals)):
            vals = decade_vals[d]
            fig.add_trace(go.Violin(
                y=vals[:500] if len(vals) > 500 else vals,
                name=f"{d}s", box_visible=True, meanline_visible=True,
                points=False, marker_color=decade_colors[i % len(decade_colors)],
            ))
        fig.update_layout(
            title="デビュー年代別 次数中心性（次数上位3,000名）",
            xaxis_title="デビュー年代", yaxis_title="次数中心性",
        )

        return ReportSection(
            title="ハブの世代交代",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_hub_turnover", height=420),
            method_note=(
                "feat_network.degree_centrality による上位3,000名。"
                "選択バイアス: 現在の接続数が多い人物が過大表現される。"
            ),
            interpretation_html=(
                "<p>早期デビューコホートに高次数ノードが集中して見えるのは、"
                "2つの複合的な効果による想定内の現象である: (1) キャリアが長いほど"
                "共クレジットのエッジを蓄積する時間が長い、(2) 早期に開始し現在も活動している人物は"
                "生存バイアスのかかったサンプルとなる。"
                "代替分析として、デビューコホート間で固定キャリア年（例: career_year=10）での"
                "次数を比較する方法が考えられる。</p>"
            ),
            section_id="hub_turnover",
        )

    # ── Section 5: Centrality distribution scatter ───────────────

    def _build_centrality_scatter_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fn.degree_centrality,
                    fn.betweenness_centrality,
                    fn.eigenvector_centrality,
                    fn.hub_score,
                    fc.career_track
                FROM feat_network fn
                JOIN feat_career fc ON fn.person_id = fc.person_id
                WHERE fn.degree_centrality > 0
                  AND fn.betweenness_centrality > 0
                ORDER BY fn.degree_centrality DESC
                LIMIT 10000
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="中心性指標の関係",
                findings_html="<p>中心性散布図データが利用できません。</p>",
                section_id="centrality_scatter",
            )

        degree = [r["degree_centrality"] for r in rows]
        between = [r["betweenness_centrality"] for r in rows]
        eigen = [r["eigenvector_centrality"] or 0 for r in rows]

        findings = (
            f"<p>上位{len(rows):,}人（次数順）の次数中心性 vs 媒介中心性。"
            "高媒介性・中程度の次数を持つ人物はコミュニティ間のブリッジとして機能。"
            "色 = 固有ベクトル中心性。</p>"
        )

        fig = go.Figure(go.Scattergl(
            x=degree, y=between,
            mode="markers",
            marker=dict(
                size=4, opacity=0.5,
                color=eigen,
                colorscale="Viridis",
                colorbar=dict(title="固有ベクトル中心性"),
            ),
            hovertemplate=(
                "次数: %{x:.4f}<br>"
                "媒介: %{y:.6f}<br>"
                "固有ベクトル: %{marker.color:.4f}<extra></extra>"
            ),
        ))
        fig.update_layout(
            title="次数中心性 vs 媒介中心性",
            xaxis_title="次数中心性",
            yaxis_title="媒介中心性",
            xaxis_type="log", yaxis_type="log",
            height=500,
        )

        return ReportSection(
            title="中心性指標の関係",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_centrality_scatter", height=500),
            method_note=(
                "中心性指標は feat_network より（パイプライン Phase 6）。"
                "両対数スケール。次数上位10,000名を表示。"
                "固有ベクトル中心性を色で表現。"
            ),
            section_id="centrality_scatter",
        )

    # ── Section 6: New-entrant rate ──────────────────────────────

    def _build_entrant_rate_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fc.first_year AS year,
                    COUNT(*) AS new_entrants
                FROM feat_career fc
                WHERE fc.first_year BETWEEN 1970 AND 2024
                GROUP BY fc.first_year
                ORDER BY fc.first_year
            """).fetchall()
            exit_rows = self.conn.execute("""
                SELECT
                    fc.latest_year AS year,
                    COUNT(*) AS exits
                FROM feat_career fc
                WHERE fc.latest_year BETWEEN 1970 AND 2020
                GROUP BY fc.latest_year
                ORDER BY fc.latest_year
            """).fetchall()
        except Exception:
            rows = []
            exit_rows = []

        if not rows:
            return ReportSection(
                title="人材参入・退出レート",
                findings_html="<p>新規参入率データが利用できません。</p>",
                section_id="entrant_rate",
            )

        years = [r["year"] for r in rows]
        entrants = [r["new_entrants"] for r in rows]

        exit_by_year: dict[int, int] = {}
        for r in exit_rows:
            exit_by_year[r["year"]] = r["exits"]
        exits = [exit_by_year.get(y, 0) for y in years]

        # Net flow
        net = [e - x for e, x in zip(entrants, exits)]

        findings = (
            f"<p>年間新規参入者と退出者（{years[0]}–{years[-1]}年）。"
            "新規参入 = その年にfirst_yearを持つ人物。"
            "退出 = latest_yearがその年に該当する人物（2021年以降は右打ち切り軽減のため除外"
            " — 退出判定は5年以上クレジットなし）。</p>"
        )

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=years, y=entrants, name="新規参入者",
            fill="tozeroy",
            line=dict(color="#3BC494", width=1.5),
            fillcolor="rgba(6,214,160,0.3)",
        ))
        fig.add_trace(go.Scatter(
            x=years, y=[-e for e in exits], name="退出者",
            fill="tozeroy",
            line=dict(color="#E07532", width=1.5),
            fillcolor="rgba(245,87,108,0.3)",
        ))
        fig.add_trace(go.Scatter(
            x=years, y=net, name="ネットフロー",
            line=dict(color="#F8EC6A", width=2, dash="dash"),
        ))
        fig.update_layout(
            title="人材参入・退出の推移",
            xaxis_title="年", yaxis_title="人数",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=450,
        )

        return ReportSection(
            title="人材参入・退出レート",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_entrant_rate", height=450),
            method_note=(
                "新規参入は feat_career.first_year より。"
                "退出は feat_career.latest_year より（2020年以前のみ — 退出 = 5年以上クレジットなし）。"
                "退出定義は近似であり、latest_year が2020年の人物が将来再び活動する可能性がある。"
            ),
            interpretation_html=(
                "<p>近年に退出者の急増が見える現象は、右打ち切りの影響の一部である:"
                "2020年まで活動し将来再び作業する人物が退出として表示される。"
                "5年間のギャップ閾値はこれを軽減するが完全には排除しない。</p>"
            ),
            section_id="entrant_rate",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='network_evolution',
    audience='technical_appendix',
    claim='ネットワーク時系列変化 に関する記述的指標 (subtitle: 協業グラフのノード・エッジ成長とTier構成の経年変化)',
    sources=["credits", "persons", "anime"],
    meta_table='meta_network_evolution',
)
