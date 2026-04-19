# DEPRECATED (Phase 3-5, 2026-04-19): merged into industry_overview. Data Statement 章に吸収.
# This module is retained in archived/ for regeneration and audit only.
# It is NOT in V2_REPORT_CLASSES and will not run in default generation.
"""Credit Statistics report — v2 compliant.

Credit distribution patterns with rich visualizations:
  1. Credits per person (histogram + tier/gender/decade tabs)
  2. Credits per work by tier (box plots)
  3. Role distribution (top 20 bar, log scale)
  4. Role diversity per person (histogram)
  5. Credit density over time (dual-axis line)
  6. Role Flow Sankey (from role_flow.json)
  7. Productivity vs Consistency scatter (from productivity.json)
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..helpers import load_json
from ..html_templates import plotly_div_safe, stratification_tabs, strat_panel
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}


class CreditStatisticsReport(BaseReportGenerator):
    name = "credit_statistics"
    title = "クレジット統計分析"
    subtitle = "クレジット分布・役割多様性・Tier/性別/年代別クレジット密度"
    filename = "credit_statistics.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_credits_per_person_section(sb)))
        sections.append(sb.build_section(self._build_credits_per_work_section(sb)))
        sections.append(sb.build_section(self._build_role_distribution_section(sb)))
        sections.append(sb.build_section(self._build_role_diversity_section(sb)))
        sections.append(sb.build_section(self._build_credit_density_section(sb)))
        sec = self._build_sankey_section(sb)
        if sec:
            sections.append(sb.build_section(sec))
        sec = self._build_productivity_section(sb)
        if sec:
            sections.append(sb.build_section(sec))
        return self.write_report("\n".join(sections))

    # ── Section 1: Credits per person ────────────────────────────

    def _build_credits_per_person_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    c.person_id,
                    COUNT(*) AS total_credits,
                    p.gender,
                    (fc.first_year / 10) * 10 AS debut_decade,
                    modal_tier.scale_tier AS tier
                FROM credits c
                JOIN persons p ON c.person_id = p.id
                LEFT JOIN feat_career fc ON c.person_id = fc.person_id
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
                ) modal_tier ON c.person_id = modal_tier.person_id AND modal_tier.rn = 1
                GROUP BY c.person_id, p.gender, debut_decade, tier
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="人物別クレジット数分布",
                findings_html="<p>人物別クレジット数データがありません。</p>",
                section_id="credits_per_person",
            )

        all_credits = [r["total_credits"] for r in rows]
        summ = distribution_summary(all_credits, label="total_credits")

        findings = (
            f"<p>人物別クレジット数の分布（n={summ['n']:,}人）: "
            f"{format_distribution_inline(summ)}、"
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
        )

        # Gender
        gender_credits: dict[str, list[float]] = {}
        for r in rows:
            gender_credits.setdefault(r["gender"] or "unknown", []).append(r["total_credits"])

        gender_html = "<p>性別ごとの人物別クレジット数:</p><ul>"
        for g, gv in sorted(gender_credits.items()):
            gs = distribution_summary(gv, label=g)
            gender_html += (
                f"<li><strong>{g}</strong>（n={gs['n']:,}）: "
                f"{format_distribution_inline(gs)}、"
                f"{format_ci((gs['ci_lower'], gs['ci_upper']))}</li>"
            )
        gender_html += "</ul>"

        # Tier
        tier_credits: dict[int, list[float]] = {}
        for r in rows:
            if r["tier"] is not None:
                tier_credits.setdefault(r["tier"], []).append(r["total_credits"])

        tier_html = "<p>最頻Tier別の人物別クレジット数:</p><ul>"
        for t in sorted(tier_credits):
            ts = distribution_summary(tier_credits[t], label=f"tier{t}")
            tier_html += (
                f"<li><strong>Tier {t}</strong>（n={ts['n']:,}）: "
                f"{format_distribution_inline(ts)}、"
                f"{format_ci((ts['ci_lower'], ts['ci_upper']))}</li>"
            )
        tier_html += "</ul>"

        # Decade
        decade_credits: dict[int, list[float]] = {}
        for r in rows:
            if r["debut_decade"] is not None:
                decade_credits.setdefault(r["debut_decade"], []).append(r["total_credits"])

        decade_html = "<p>デビュー年代別の人物別クレジット数:</p><ul>"
        for d in sorted(decade_credits):
            ds = distribution_summary(decade_credits[d], label=str(d))
            decade_html += (
                f"<li><strong>{d}年代</strong>（n={ds['n']:,}）: "
                f"{format_distribution_inline(ds)}</li>"
            )
        decade_html += "</ul>"

        fig = go.Figure(go.Histogram(
            x=all_credits, nbinsx=50, marker_color="#a0d2db",
            hovertemplate="%{x}件: %{y:,}人<extra></extra>",
        ))
        fig.update_layout(title="人物別クレジット数", xaxis_title="総クレジット数", yaxis_title="人数")

        tabs_html = stratification_tabs(
            "cpp_tabs",
            {"overall": "全体", "gender": "性別", "tier": "Tier", "decade": "年代"},
            active="overall",
        )
        panels = (
            strat_panel("cpp_tabs", "overall",
                        plotly_div_safe(fig, "chart_cpp_overall", height=380), active=True) +
            strat_panel("cpp_tabs", "gender", gender_html) +
            strat_panel("cpp_tabs", "tier", tier_html) +
            strat_panel("cpp_tabs", "decade", decade_html)
        )

        return ReportSection(
            title="人物別クレジット数分布",
            findings_html=findings,
            visualization_html=tabs_html + panels,
            method_note=(
                "Total credits = COUNT(*) in credits table per person. "
                "One row = one role on one work. Multiple roles on same work = multiple rows."
            ),
            section_id="credits_per_person",
        )

    # ── Section 2: Credits per work by tier ──────────────────────

    def _build_credits_per_work_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fwc.scale_tier AS tier,
                    COUNT(c.id) AS n_credits,
                    fwc.anime_id
                FROM feat_work_context fwc
                JOIN credits c ON c.anime_id = fwc.anime_id
                WHERE fwc.scale_tier IS NOT NULL
                GROUP BY fwc.scale_tier, fwc.anime_id
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="作品別クレジット数（Tier別）",
                findings_html="<p>作品別クレジット数データがありません。</p>",
                section_id="credits_per_work",
            )

        tier_credits: dict[int, list[int]] = {}
        for r in rows:
            tier_credits.setdefault(r["tier"], []).append(r["n_credits"])

        findings = "<p>規模Tier別の作品あたりクレジット数:</p><ul>"
        for t in sorted(tier_credits):
            ts = distribution_summary(tier_credits[t], label=f"tier{t}")
            findings += (
                f"<li><strong>Tier {t}</strong>（n={ts['n']:,}作品）: "
                f"{format_distribution_inline(ts)}、"
                f"{format_ci((ts['ci_lower'], ts['ci_upper']))}</li>"
            )
        findings += "</ul>"

        fig = go.Figure()
        for t in sorted(tier_credits):
            fig.add_trace(go.Box(
                y=tier_credits[t],
                name=f"Tier {t}",
                marker_color=_TIER_COLORS.get(t, "#a0a0c0"),
                boxpoints=False,
            ))
        fig.update_layout(
            title="規模Tier別 作品あたりクレジット数",
            xaxis_title="規模Tier", yaxis_title="作品あたりクレジット数",
        )

        return ReportSection(
            title="作品別クレジット数（Tier別）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cpw_tier", height=420),
            method_note=(
                "Credits = COUNT(credits.id) per anime_id. "
                "scale_tier from feat_work_context."
            ),
            section_id="credits_per_work",
        )

    # ── Section 3: Role distribution ─────────────────────────────

    def _build_role_distribution_section(self, sb: SectionBuilder) -> ReportSection:
        credit_stats = load_json("credit_stats.json")
        if not credit_stats:
            # Fallback: query DB directly
            try:
                rows = self.conn.execute("""
                    SELECT role, COUNT(*) AS n
                    FROM credits
                    WHERE role IS NOT NULL
                    GROUP BY role
                    ORDER BY n DESC
                    LIMIT 20
                """).fetchall()
                role_dist = {r["role"]: r["n"] for r in rows}
            except Exception:
                role_dist = {}
        else:
            role_dist = credit_stats.get("role_distribution", {})

        if not role_dist:
            return ReportSection(
                title="役職分布",
                findings_html="<p>役職分布データがありません。</p>",
                section_id="role_distribution",
            )

        sorted_roles = sorted(role_dist.items(), key=lambda x: x[1], reverse=True)[:20]
        total = sum(v for _, v in sorted_roles)

        findings = f"<p>クレジット数上位20役職（表示合計: {total:,}）:</p>"

        fig = go.Figure(go.Bar(
            x=[r[0] for r in sorted_roles],
            y=[r[1] for r in sorted_roles],
            marker_color="#f093fb",
            hovertemplate="%{x}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            title="役職分布（上位20）",
            xaxis_title="役職", yaxis_title="クレジット数",
            xaxis_tickangle=-45, yaxis_type="log",
        )

        return ReportSection(
            title="役職分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_role_dist", height=450),
            method_note=(
                "役職分布は credit_stats.json または credits テーブル由来。"
                "Y軸はログスケール。上位20役職を表示。"
            ),
            section_id="role_distribution",
        )

    # ── Section 4: Role diversity per person ─────────────────────

    def _build_role_diversity_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT person_id, COUNT(DISTINCT role) AS n_distinct_roles
                FROM credits
                WHERE role IS NOT NULL
                GROUP BY person_id
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="人物別役割多様性",
                findings_html="<p>役割多様性データがありません。</p>",
                section_id="role_diversity",
            )

        vals = [r["n_distinct_roles"] for r in rows]
        summ = distribution_summary(vals, label="n_distinct_roles")

        findings = (
            f"<p>人物別の異なる役割数の分布（n={summ['n']:,}人）: "
            f"{format_distribution_inline(summ)}、"
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
        )

        fig = go.Figure(go.Histogram(
            x=vals, nbinsx=30, marker_color="#FFD166",
            hovertemplate="%{x}役職: %{y:,}人<extra></extra>",
        ))
        fig.update_layout(
            title="人物別 異なる役職数",
            xaxis_title="異なる役職数", yaxis_title="人数",
        )

        return ReportSection(
            title="人物別役割多様性",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_role_diversity", height=380),
            method_note=(
                "n_distinct_roles = 人物ごとの COUNT(DISTINCT credits.role)。"
                "役職文字列は生値（正規化なし）。"
            ),
            section_id="role_diversity",
        )

    # ── Section 5: Credit density over time ──────────────────────

    def _build_credit_density_section(self, sb: SectionBuilder) -> ReportSection:
        # Try JSON first (richer), then fallback to DB
        credit_stats = load_json("credit_stats.json")
        timeline = credit_stats.get("timeline_stats", {}) if credit_stats else {}
        by_year = timeline.get("by_year", [])

        if by_year:
            by_year_sorted = sorted(by_year, key=lambda x: x.get("year", 0))
            years = [y["year"] for y in by_year_sorted]
            credits_yr = [y.get("credits", 0) for y in by_year_sorted]
            persons_yr = [y.get("person_count", 0) for y in by_year_sorted]
            anime_yr = [y.get("anime_count", 0) for y in by_year_sorted]
        else:
            try:
                rows = self.conn.execute("""
                    SELECT a.year,
                           COUNT(c.id) AS total_credits,
                           COUNT(DISTINCT a.id) AS n_works,
                           COUNT(DISTINCT c.person_id) AS n_persons
                    FROM anime a
                    JOIN credits c ON c.anime_id = a.id
                    WHERE a.year BETWEEN 1980 AND 2024
                    GROUP BY a.year ORDER BY a.year
                """).fetchall()
                years = [r["year"] for r in rows]
                credits_yr = [r["total_credits"] for r in rows]
                persons_yr = [r["n_persons"] for r in rows]
                anime_yr = [r["n_works"] for r in rows]
            except Exception:
                years, credits_yr, persons_yr, anime_yr = [], [], [], []

        if not years:
            return ReportSection(
                title="クレジット密度の推移",
                findings_html="<p>クレジット密度データがありません。</p>",
                section_id="credit_density",
            )

        findings = (
            f"<p>年次クレジット統計（{years[0]}–{years[-1]}）。"
            "3パネル: 年間クレジット総数、年間活動人物数、年間アニメ作品数。</p>"
        )

        from plotly.subplots import make_subplots
        fig = make_subplots(
            rows=1, cols=3,
            subplot_titles=("年間クレジット数", "年間活動人数", "年間作品数"),
        )
        fig.add_trace(go.Bar(x=years, y=credits_yr, marker_color="#f093fb"), row=1, col=1)
        fig.add_trace(go.Bar(x=years, y=persons_yr, marker_color="#a0d2db"), row=1, col=2)
        fig.add_trace(go.Bar(x=years, y=anime_yr, marker_color="#fda085"), row=1, col=3)
        fig.update_layout(
            title=f"クレジット推移 ({years[0]}–{years[-1]})",
            showlegend=False, height=400,
        )

        return ReportSection(
            title="クレジット密度の推移",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_credit_timeline", height=400),
            method_note=(
                "タイムラインデータは credit_stats.json または credits JOIN anime 由来。"
                "トレンドはソースデータの記録慣行の影響を受ける。"
            ),
            section_id="credit_density",
        )

    # ── Section 6: Role Flow Sankey ──────────────────────────────

    def _build_sankey_section(self, sb: SectionBuilder) -> ReportSection | None:
        role_flow = load_json("role_flow.json")
        if not role_flow:
            return None

        nodes = role_flow.get("nodes", [])
        links = role_flow.get("links", [])
        if not nodes or not links:
            return None

        node_labels = [n["label"] for n in nodes]
        node_map = {n["id"]: i for i, n in enumerate(nodes)}
        sorted_links = sorted(links, key=lambda x: x["value"], reverse=True)[:40]
        valid_links = [
            lk for lk in sorted_links
            if lk["source"] in node_map and lk["target"] in node_map
        ]

        if not valid_links:
            return None

        total_transitions = role_flow.get("total_transitions", 0)

        pastel = [
            "#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#667eea",
            "#f5576c", "#fda085", "#8a94a0", "#c0c0e0",
        ]

        fig = go.Figure(go.Sankey(
            node=dict(
                pad=15, thickness=20, label=node_labels,
                color=[pastel[i % len(pastel)] for i in range(len(node_labels))],
            ),
            link=dict(
                source=[node_map[lk["source"]] for lk in valid_links],
                target=[node_map[lk["target"]] for lk in valid_links],
                value=[lk["value"] for lk in valid_links],
            ),
        ))
        fig.update_layout(
            title=f"役職フロー（全{total_transitions:,}遷移中 上位40件）",
            height=600,
        )

        findings = (
            f"<p>役職遷移フロー図: {len(nodes)}のキャリアステージにおける"
            f"合計{total_transitions:,}回の遷移のうち上位40件を表示。"
            "各フローの幅はステージ間を遷移した人数に比例する。"
            "双方向のフロー（例: 作画監督→原画 および 原画→作画監督）は"
            "降格ではなく、年度をまたぐ役職の兼任を反映している。"
            "例えばN年に作画監督、N+1年に原画としてクレジットされた人物は、"
            "両役職を同時に担当している可能性がある。</p>"
        )

        return ReportSection(
            title="役職フロー（Sankey図）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_sankey", height=600),
            method_note=(
                "役職フローは role_flow.json 由来（パイプラインPhase 9）。"
                "ステージ遷移は人物ごとにキャリア全体で集計。"
                "件数上位40の遷移を表示。"
            ),
            section_id="role_flow",
        )

    # ── Section 7: Productivity vs Consistency ───────────────────

    def _build_productivity_section(self, sb: SectionBuilder) -> ReportSection | None:
        productivity = load_json("productivity.json")
        if not productivity or not isinstance(productivity, dict):
            return None

        cpy_vals: list[float] = []
        con_vals: list[float] = []
        for _pid, pdata in productivity.items():
            if not isinstance(pdata, dict):
                continue
            c = pdata.get("credits_per_year", 0)
            s = pdata.get("consistency_score", 0)
            if c > 0:
                cpy_vals.append(c)
                con_vals.append(s)

        if len(cpy_vals) < 10:
            return None

        fig = go.Figure(go.Scattergl(
            x=cpy_vals, y=con_vals, mode="markers",
            marker=dict(size=4, color="#f5576c", opacity=0.4),
            hovertemplate=(
                "年間クレジット数: %{x:.1f}<br>"
                "一貫性: %{y:.2f}<extra></extra>"
            ),
        ))
        fig.update_layout(
            title="生産性 vs 一貫性",
            xaxis_title="年間クレジット数",
            yaxis_title="一貫性スコア",
            xaxis_type="log",
            height=450,
        )

        findings = (
            f"<p>生産性（クレジット数/年）vs 一貫性スコア（{len(cpy_vals):,}人）。"
            "X軸は対数スケール。各点は1人を表す。</p>"
        )

        return ReportSection(
            title="生産性 vs 一貫性",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_productivity", height=450),
            method_note=(
                "データは productivity.json（パイプライン出力）由来。"
                "credits_per_year = total_credits / active_years。"
                "consistency_score = 正規化された変動係数。"
            ),
            section_id="productivity",
        )
