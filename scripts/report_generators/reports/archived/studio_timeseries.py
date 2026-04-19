# DEPRECATED (Phase 3-5, 2026-04-19): merged into mgmt_studio_benchmark. 時系列章に統合.
# This module is retained in archived/ for regeneration and audit only.
# It is NOT in V2_REPORT_CLASSES and will not run in default generation.
"""Studio Timeseries report — v2 compliant.

Studio longitudinal analysis:
- Section 1: Studio cluster production volume over time
- Section 2: Staff composition trends by studio cluster
- Section 3: Studio FE stability across eras
- Section 4: New vs established studio activity
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}


class StudioTimeseriesReport(BaseReportGenerator):
    name = "studio_timeseries"
    title = "スタジオ時系列分析"
    subtitle = "スタジオクラスタ別制作量推移・スタッフ構成変化・FE安定性"
    filename = "studio_timeseries.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_cluster_volume_section(sb)))
        sections.append(sb.build_section(self._build_staff_trend_section(sb)))
        sections.append(sb.build_section(self._build_fe_stability_section(sb)))
        sections.append(sb.build_section(self._build_new_studio_section(sb)))
        return self.write_report("\n".join(sections))

    def _build_cluster_volume_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    COALESCE(fcm.studio_cluster_name, 'C' || fcm.studio_cluster_id) AS cluster_label,
                    (fwc.credit_year / 5) * 5 AS period,
                    COUNT(DISTINCT fcc.anime_id) AS n_works
                FROM feat_cluster_membership fcm
                JOIN feat_credit_contribution fcc ON fcm.person_id = fcc.person_id
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fwc.credit_year BETWEEN 1990 AND 2024
                  AND fcm.studio_cluster_id IS NOT NULL
                GROUP BY cluster_label, period
                ORDER BY cluster_label, period
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スタジオクラスタ別制作量推移",
                findings_html="<p>スタジオクラスタ別制作量データが取得できませんでした。</p>",
                section_id="cluster_volume",
            )

        cluster_period: dict[str, dict[int, int]] = {}
        for r in rows:
            cluster_period.setdefault(r["cluster_label"], {})[r["period"]] = r["n_works"]

        top_clusters = sorted(cluster_period, key=lambda c: -sum(cluster_period[c].values()))[:6]
        periods = sorted({r["period"] for r in rows})

        findings = "<p>スタジオクラスタ別制作量（5年周期、1990–2024年）:</p><ul>"
        for c in top_clusters:
            total = sum(cluster_period[c].values())
            findings += f"<li><strong>{c}</strong>: 計{total:,}作品</li>"
        findings += "</ul>"

        fig = go.Figure()
        colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#667eea", "#f5576c"]
        for i, c in enumerate(top_clusters):
            fig.add_trace(go.Scatter(
                x=periods,
                y=[cluster_period[c].get(p, 0) for p in periods],
                name=c,
                mode="lines+markers",
                line=dict(color=colors[i % len(colors)]),
            ))
        fig.update_layout(
            title="スタジオクラスタ別 制作量（5年周期）",
            xaxis_title="期間", yaxis_title="作品数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオクラスタ別制作量推移",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cluster_vol", height=420),
            method_note=(
                "スタジオクラスタは feat_cluster_membership（スタジオ特徴量に対するK-Means）より取得。"
                "作品数はクラスタ・期間別に person → credits → anime 経由で集計。"
                "同一作品に複数クラスタ所属のスタッフがクレジットされている場合、複数クラスタで重複計上される。"
                "総作品数上位6クラスタを表示。"
            ),
            section_id="cluster_volume",
        )

    def _build_staff_trend_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    COALESCE(fcm.studio_cluster_name, 'C' || fcm.studio_cluster_id) AS cluster_label,
                    (fwc.credit_year / 10) * 10 AS decade,
                    COUNT(DISTINCT fcc.person_id) AS n_staff,
                    AVG(CASE WHEN p.gender = 'Female' THEN 1.0 ELSE 0.0 END) AS female_rate
                FROM feat_cluster_membership fcm
                JOIN feat_credit_contribution fcc ON fcm.person_id = fcc.person_id
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                JOIN persons p ON fcc.person_id = p.id
                WHERE fwc.credit_year BETWEEN 1980 AND 2024
                  AND fcm.studio_cluster_id IS NOT NULL
                GROUP BY cluster_label, decade
                ORDER BY cluster_label, decade
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スタジオクラスタ別スタッフ構成推移",
                findings_html="<p>スタッフ構成推移データが取得できませんでした。</p>",
                section_id="staff_trend",
            )

        cluster_decade_female: dict[str, dict[int, float]] = {}
        for r in rows:
            cluster_decade_female.setdefault(r["cluster_label"], {})[r["decade"]] = r["female_rate"]

        top_clusters = sorted(
            cluster_decade_female, key=lambda c: -sum(1 for _ in cluster_decade_female[c])
        )[:6]
        decades = sorted({r["decade"] for r in rows})

        findings = "<p>スタジオクラスタ × 年代別 女性スタッフ比率:</p>"

        fig = go.Figure()
        colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#667eea", "#f5576c"]
        for i, c in enumerate(top_clusters):
            fig.add_trace(go.Scatter(
                x=decades,
                y=[100 * cluster_decade_female[c].get(d, 0) for d in decades],
                name=c,
                mode="lines+markers",
                line=dict(color=colors[i % len(colors)]),
            ))
        fig.update_layout(
            title="スタジオクラスタ × 年代別 女性スタッフ比率",
            xaxis_title="年代", yaxis_title="女性スタッフ比率 (%)",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオクラスタ別スタッフ構成推移",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_staff_trend", height=420),
            method_note=(
                "Female rate = fraction of credited persons per cluster-decade with gender='female'. "
                "gender from persons.gender. Persons with NULL gender excluded from rate calculation."
            ),
            section_id="staff_trend",
        )

    def _build_fe_stability_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fsa.studio_id,
                    COALESCE(fsa.studio_name, fsa.studio_id) AS studio_label,
                    AVG(CASE WHEN fsa.credit_year < 2010 THEN fps.iv_score END) AS avg_iv_early,
                    AVG(CASE WHEN fsa.credit_year >= 2010 THEN fps.iv_score END) AS avg_iv_late,
                    COUNT(DISTINCT fsa.person_id) AS n_staff
                FROM feat_studio_affiliation fsa
                JOIN feat_person_scores fps ON fsa.person_id = fps.person_id
                WHERE fps.iv_score IS NOT NULL
                GROUP BY fsa.studio_id, studio_label
                HAVING avg_iv_early IS NOT NULL AND avg_iv_late IS NOT NULL
                   AND n_staff >= 5
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スタジオ平均IVスコア安定性（前期 vs 後期）",
                findings_html=(
                    "<p>スタジオ別前期・後期IVスコアデータが取得できませんでした。"
                    "feat_studio_affiliation × feat_person_scores から、2010年を境に"
                    "前期/後期平均IVスコアを算出し安定性を確認する。</p>"
                ),
                section_id="fe_stability",
            )

        early = [r["avg_iv_early"] for r in rows]
        late = [r["avg_iv_late"] for r in rows]
        diffs = [r["avg_iv_late"] - r["avg_iv_early"] for r in rows]
        diff_summ = distribution_summary(diffs, label="iv_change")

        findings = (
            f"<p>スタジオ別平均IVスコアの変化量（後期 − 前期）、{len(rows):,}スタジオ: "
            f"{format_distribution_inline(diff_summ)}, "
            f"{format_ci((diff_summ['ci_lower'], diff_summ['ci_upper']))}。"
            "前期=2010年より前、後期=2010年以降。</p>"
        )

        fig = go.Figure(go.Scatter(
            x=early, y=late,
            mode="markers",
            marker=dict(color="#a0d2db", size=6, opacity=0.7),
            text=[r["studio_label"] for r in rows],
            hovertemplate="%{text}<br>前期=%{x:.3f}, 後期=%{y:.3f}<extra></extra>",
        ))
        emin, emax = min(early), max(early)
        fig.add_trace(go.Scatter(
            x=[emin, emax], y=[emin, emax],
            mode="lines", line=dict(color="#f5576c", dash="dash"),
            name="y=x",
        ))
        fig.update_layout(
            title="スタジオ平均IVスコア: 前期 vs 後期",
            xaxis_title="前期 平均IVスコア（〜2009年）",
            yaxis_title="後期 平均IVスコア（2010年〜）",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオ平均IVスコア安定性（前期 vs 後期）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_fe_stability", height=420),
            method_note=(
                "feat_studio_affiliation × feat_person_scores のJOINにより、"
                "スタジオ別・前期（2010年未満）/後期（2010年以降）の平均IVスコアを算出。"
                "y=x線に近いスタジオは期間間で安定した水準を示す。"
                "最低5名以上のスタッフを有するスタジオのみ対象。"
            ),
            section_id="fe_stability",
        )

    def _build_new_studio_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    asj.studio_id,
                    (MIN(a.year) / 10) * 10 AS debut_decade
                FROM anime_studios asj
                JOIN anime a ON asj.anime_id = a.id
                WHERE a.year IS NOT NULL
                GROUP BY asj.studio_id
                HAVING debut_decade IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スタジオデビュー年代分布",
                findings_html="<p>スタジオデビューデータが取得できませんでした。</p>",
                section_id="new_studio",
            )

        decade_studios: dict[int, int] = {}
        for r in rows:
            d = r["debut_decade"]
            decade_studios[d] = decade_studios.get(d, 0) + 1

        decades = sorted(decade_studios.keys())
        findings = "<p>スタジオの初作品年代別分布（スタジオ設立年の近似値）:</p><ul>"
        for d in decades:
            findings += f"<li><strong>{d}年代</strong>: {decade_studios[d]:,}スタジオ</li>"
        findings += "</ul>"

        fig = go.Figure(go.Bar(
            x=[str(d) for d in decades],
            y=[decade_studios[d] for d in decades],
            marker_color="#f093fb",
            hovertemplate="%{x}年代: %{y:,}スタジオ<extra></extra>",
        ))
        fig.update_layout(
            title="スタジオ初作品年代分布",
            xaxis_title="デビュー年代", yaxis_title="スタジオ数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオデビュー年代分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_new_studio", height=380),
            method_note=(
                "anime_studios × anime のJOINにより、各スタジオの最初の作品年を特定。"
                "デビュー年代=初作品の年を10年単位に丸めた値。"
                "実際の設立年は初クレジット作品より前の場合がある。"
            ),
            section_id="new_studio",
        )
