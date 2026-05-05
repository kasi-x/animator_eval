"""Growth Scores report — v2 compliant.

Growth trajectory metrics:
- Section 1: Growth score distribution (overall + by gender/tier/cluster tabs)
  + scatter of growth_score vs iv_score colored by growth_trend
- Section 2: Growth trend types (acceleration, stable, deceleration)
  + growth_score distribution by debut decade (first_year)
- Section 3: Growth by debut decade cohort
  + growth_score by career_track
- Section 4: Correlation between growth and tier attainment
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..color_utils import TIER_PALETTE as _TIER_COLORS
from ..html_templates import plotly_div_safe, stratification_tabs, strat_panel
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_TREND_COLORS = {
    "accelerating": "#3BC494",
    "stable": "#3593D2",
    "decelerating": "#E07532",
    "volatile": "#F8EC6A",
}

_TRACK_COLORS = [
    "#E09BC2", "#7CC8F2", "#3BC494", "#F8EC6A", "#3593D2",
    "#E07532", "#FFB444", "#8a94a0",
]


class GrowthScoresReport(BaseReportGenerator):
    name = "growth_scores"
    title = "成長スコア分析"
    subtitle = "IV成長軌跡・トレンドタイプ・コホート別・Tier到達との関連"
    filename = "growth_scores.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_growth_dist_section(sb)))
        sections.append(sb.build_section(self._build_trend_type_section(sb)))
        sections.append(sb.build_section(self._build_growth_cohort_section(sb)))
        sections.append(sb.build_section(self._build_growth_tier_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Growth score distribution ─────────────────────

    def _build_growth_dist_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fc.growth_score, fc.growth_trend, p.gender, fc.career_track,
                       fps.iv_score,
                       modal_tier.scale_tier AS tier
                FROM feat_career fc
                JOIN conformed.persons p ON fc.person_id = p.id
                LEFT JOIN feat_person_scores fps ON fc.person_id = fps.person_id
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
                ) modal_tier ON fc.person_id = modal_tier.person_id AND modal_tier.rn = 1
                WHERE fc.growth_score IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="成長スコアの分布",
                findings_html="<p>成長スコアデータが取得できません（feat_career.growth_score）。</p>",
                section_id="growth_dist",
            )

        vals = [r["growth_score"] for r in rows]
        summ = distribution_summary(vals, label="growth_score")

        findings = (
            f"<p>成長スコア分布（n={summ['n']:,}）: "
            f"{format_distribution_inline(summ)}, "
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
        )

        # Gender
        gender_groups: dict[str, list[float]] = {}
        for r in rows:
            g = r["gender"] or "不明"
            gender_groups.setdefault(g, []).append(r["growth_score"])

        gender_html = "<p>性別ごとの分布:</p><ul>"
        for g, gv in sorted(gender_groups.items()):
            gs = distribution_summary(gv, label=g)
            gender_html += (
                f"<li><strong>{g}</strong>（n={gs['n']:,}）: "
                f"{format_distribution_inline(gs)}, {format_ci((gs['ci_lower'], gs['ci_upper']))}</li>"
            )
        gender_html += "</ul>"

        # Tier
        tier_groups: dict[int, list[float]] = {}
        for r in rows:
            if r["tier"] is not None:
                tier_groups.setdefault(r["tier"], []).append(r["growth_score"])

        tier_html = "<p>最頻作品規模Tier別:</p><ul>"
        for t in sorted(tier_groups):
            ts = distribution_summary(tier_groups[t], label=f"tier{t}")
            tier_html += (
                f"<li><strong>Tier {t}</strong>（n={ts['n']:,}）: "
                f"{format_distribution_inline(ts)}, {format_ci((ts['ci_lower'], ts['ci_upper']))}</li>"
            )
        tier_html += "</ul>"

        # Career track
        track_groups: dict[str, list[float]] = {}
        for r in rows:
            t = r["career_track"] or "不明"
            track_groups.setdefault(t, []).append(r["growth_score"])

        # Main histogram
        fig = go.Figure(go.Histogram(
            x=vals, nbinsx=40, marker_color="#3BC494",
            hovertemplate="score=%{x:.3f}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            title="成長スコア分布",
            xaxis_title="成長スコア", yaxis_title="人数",
        )

        # Tier box plot
        fig_tier = go.Figure()
        for t in sorted(tier_groups):
            fig_tier.add_trace(go.Box(
                y=tier_groups[t], name=f"Tier {t}",
                marker_color=_TIER_COLORS.get(t, "#a0a0c0"), boxpoints=False,
            ))
        fig_tier.update_layout(
            title="Tier別成長スコア",
            yaxis_title="成長スコア",
        )

        # Career track violin
        fig_track = go.Figure()
        for i, (t, tv) in enumerate(sorted(track_groups.items(), key=lambda x: -len(x[1]))[:8]):
            fig_track.add_trace(go.Violin(
                y=tv[:500] if len(tv) > 500 else tv,
                name=t[:20], box_visible=True, meanline_visible=True,
                points=False, marker_color=_TRACK_COLORS[i % len(_TRACK_COLORS)],
            ))
        fig_track.update_layout(
            title="キャリアトラック別成長スコア",
            yaxis_title="成長スコア",
        )

        # Deeper chart: growth_score vs iv_score scatter, colored by growth_trend
        scatter_rows = [r for r in rows if r["iv_score"] is not None and r["growth_trend"] is not None]
        fig_scatter = go.Figure()
        if scatter_rows:
            trend_scatter: dict[str, tuple[list[float], list[float]]] = {}
            for r in scatter_rows:
                trend = r["growth_trend"]
                gs_list, iv_list = trend_scatter.setdefault(trend, ([], []))
                gs_list.append(r["growth_score"])
                iv_list.append(r["iv_score"])
            for trend_name in sorted(trend_scatter):
                gs_list, iv_list = trend_scatter[trend_name]
                # Sample down to 2000 per trend for performance
                n_pts = len(gs_list)
                if n_pts > 2000:
                    step = n_pts // 2000
                    gs_list = gs_list[::step][:2000]
                    iv_list = iv_list[::step][:2000]
                fig_scatter.add_trace(go.Scattergl(
                    x=gs_list, y=iv_list,
                    mode="markers",
                    marker=dict(
                        size=3,
                        color=_TREND_COLORS.get(trend_name, "#a0a0c0"),
                        opacity=0.5,
                    ),
                    name=trend_name,
                    hovertemplate="growth=%{x:.2f}, iv=%{y:.2f}<extra>%{fullData.name}</extra>",
                ))
            fig_scatter.update_layout(
                title="成長スコア vs IVスコア（トレンドタイプ別）",
                xaxis_title="成長スコア",
                yaxis_title="IVスコア",
                legend_title="トレンドタイプ",
            )

        tabs_html = stratification_tabs(
            "growth_tabs",
            {"overall": "全体", "gender": "性別", "tier": "Tier", "track": "トラック", "scatter": "成長×IV散布図"},
            active="overall",
        )
        panels = (
            strat_panel("growth_tabs", "overall",
                        plotly_div_safe(fig, "chart_growth_overall", height=380), active=True)
            + strat_panel("growth_tabs", "gender",
                          gender_html)
            + strat_panel("growth_tabs", "tier",
                          tier_html + plotly_div_safe(fig_tier, "chart_growth_tier", height=400))
            + strat_panel("growth_tabs", "track",
                          plotly_div_safe(fig_track, "chart_growth_track", height=420))
            + strat_panel("growth_tabs", "scatter",
                          plotly_div_safe(fig_scatter, "chart_growth_vs_iv", height=460))
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="成長スコアの分布",
            findings_html=findings,
            visualization_html=tabs_html + panels,
            method_note=(
                "growth_scoreはfeat_careerから取得（Phase 9: growthモジュール）。"
                "定義: キャリア年ごとの年間IVスコアに対する線形回帰の傾きを0-99に正規化した値。"
                "負の値は年間IVの低下を示す。"
                "年間IVスコア観測値が3件以上ある人物のみスコアリング対象。"
                "散布図はgrowth_scoreとiv_scoreの関係をgrowth_trend別に色分けして表示。"
            ),
            section_id="growth_dist",
        )

    # ── Section 2: Trend types ────────────────────────────────────

    def _build_trend_type_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fc.growth_trend AS trend, p.gender, COUNT(*) AS n
                FROM feat_career fc
                JOIN conformed.persons p ON fc.person_id = p.id
                WHERE fc.growth_trend IS NOT NULL
                GROUP BY fc.growth_trend, p.gender
                ORDER BY fc.growth_trend, p.gender
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="成長トレンドタイプ",
                findings_html="<p>トレンドタイプデータが取得できません（feat_career.growth_trend）。</p>",
                section_id="trend_type",
            )

        trend_totals: dict[str, int] = {}
        trend_gender: dict[str, dict[str, int]] = {}
        for r in rows:
            t = r["trend"]
            g = r["gender"] or "不明"
            trend_totals[t] = trend_totals.get(t, 0) + r["n"]
            trend_gender.setdefault(t, {})[g] = r["n"]

        grand_total = sum(trend_totals.values())
        findings = "<p>成長トレンドタイプの分布:</p><ul>"
        for t, cnt in sorted(trend_totals.items(), key=lambda x: -x[1]):
            findings += f"<li><strong>{t}</strong>: {cnt:,}人（{100*cnt/grand_total:.1f}%）</li>"
        findings += "</ul>"

        trends = sorted(trend_totals.keys())
        fig = go.Figure(go.Bar(
            x=trends, y=[trend_totals[t] for t in trends],
            marker_color=[_TREND_COLORS.get(t, "#a0a0c0") for t in trends],
            hovertemplate="%{x}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            title="成長トレンドタイプ分布",
            xaxis_title="トレンドタイプ", yaxis_title="人数",
        )

        # Deeper chart: growth_score distribution by debut decade per trend
        try:
            decade_rows = self.conn.execute("""
                SELECT fc.growth_trend, fc.growth_score,
                       (fc.first_year / 10) * 10 AS debut_decade
                FROM feat_career fc
                WHERE fc.growth_trend IS NOT NULL
                  AND fc.growth_score IS NOT NULL
                  AND fc.first_year BETWEEN 1970 AND 2029
            """).fetchall()
        except Exception:
            decade_rows = []

        fig_decade = go.Figure()
        if decade_rows:
            # Group by decade
            decade_groups: dict[int, list[float]] = {}
            for r in decade_rows:
                d = r["debut_decade"]
                decade_groups.setdefault(d, []).append(r["growth_score"])

            decade_colors = ["#3593D2", "#7CC8F2", "#3BC494", "#F8EC6A", "#E09BC2", "#E07532"]
            for i, d in enumerate(sorted(decade_groups)):
                vals = decade_groups[d]
                fig_decade.add_trace(go.Box(
                    y=vals,
                    name=f"{d}年代",
                    marker_color=decade_colors[i % len(decade_colors)],
                    boxpoints=False,
                ))
            fig_decade.update_layout(
                title="デビュー年代別 成長スコア分布",
                xaxis_title="デビュー年代",
                yaxis_title="成長スコア",
            )

        viz_html = (
            plotly_div_safe(fig, "chart_trend_type", height=380)
            + plotly_div_safe(fig_decade, "chart_trend_decade", height=420)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="成長トレンドタイプ",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "growth_trendはfeat_careerから取得: growth_scoreと軌跡形状から導出されたカテゴリラベル。"
                "典型的な値: accelerating, stable, decelerating, volatile。"
                "定義はPhase 9 growthモジュールの実装に依存する。"
                "年代別チャートはデビュー年（first_year）を10年単位で集計し、"
                "成長スコアの分布を箱ひげ図で表示。"
            ),
            section_id="trend_type",
        )

    # ── Section 3: Growth by debut decade ─────────────────────────

    def _build_growth_cohort_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    (fc.first_year / 10) * 10 AS debut_decade,
                    fc.growth_score,
                    fc.career_track
                FROM feat_career fc
                WHERE fc.growth_score IS NOT NULL
                  AND fc.first_year BETWEEN 1970 AND 2029
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="デビュー年代別成長スコア",
                findings_html="<p>コホートデータが取得できません（feat_career.growth_score, feat_career.first_year）。</p>",
                section_id="growth_cohort",
            )

        decade_vals: dict[int, list[float]] = {}
        for r in rows:
            decade_vals.setdefault(r["debut_decade"], []).append(r["growth_score"])

        findings = "<p>デビュー年代コホート別の成長スコア:</p><ul>"
        for d in sorted(decade_vals):
            ds = distribution_summary(decade_vals[d], label=str(d))
            findings += (
                f"<li><strong>{d}年代</strong>（n={ds['n']:,}）: "
                f"{format_distribution_inline(ds)}, "
                f"{format_ci((ds['ci_lower'], ds['ci_upper']))}</li>"
            )
        findings += "</ul>"

        # Main violin by decade
        fig = go.Figure()
        decade_colors = ["#3593D2", "#7CC8F2", "#3BC494", "#F8EC6A", "#E09BC2", "#E07532"]
        for i, d in enumerate(sorted(decade_vals)):
            vals = decade_vals[d]
            fig.add_trace(go.Violin(
                y=vals[:1000] if len(vals) > 1000 else vals,
                name=f"{d}年代", box_visible=True, meanline_visible=True,
                points=False, marker_color=decade_colors[i % len(decade_colors)],
            ))
        fig.update_layout(
            title="デビュー年代別成長スコア",
            xaxis_title="デビュー年代", yaxis_title="成長スコア",
        )

        # Deeper chart: growth_score by career_track
        track_groups: dict[str, list[float]] = {}
        for r in rows:
            t = r["career_track"] or "不明"
            track_groups.setdefault(t, []).append(r["growth_score"])

        fig_track = go.Figure()
        for i, (t, tv) in enumerate(sorted(track_groups.items(), key=lambda x: -len(x[1]))[:10]):
            fig_track.add_trace(go.Box(
                y=tv,
                name=t[:20],
                marker_color=_TRACK_COLORS[i % len(_TRACK_COLORS)],
                boxpoints=False,
            ))
        fig_track.update_layout(
            title="キャリアトラック別成長スコア",
            xaxis_title="キャリアトラック",
            yaxis_title="成長スコア",
        )

        viz_html = (
            plotly_div_safe(fig, "chart_growth_cohort", height=420)
            + plotly_div_safe(fig_track, "chart_growth_by_track", height=420)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="デビュー年代別成長スコア",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "growth_scoreとfirst_yearはfeat_careerから取得。"
                "コホート比較はキャリア長で制御されていない点に注意 — 後発コホートは"
                "観測期間が短く、成長スコア推定にバイアスが生じる可能性がある。"
                "キャリアトラック別チャートはcareer_track（feat_career）ごとの"
                "成長スコア分布を箱ひげ図で表示。"
            ),
            section_id="growth_cohort",
        )

    # ── Section 4: Growth vs tier attainment ─────────────────────

    def _build_growth_tier_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fc.growth_score,
                    MAX(fwc.scale_tier) AS max_tier
                FROM feat_career fc
                JOIN feat_credit_contribution fcc ON fc.person_id = fcc.person_id
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fc.growth_score IS NOT NULL
                  AND fwc.scale_tier IS NOT NULL
                GROUP BY fc.person_id, fc.growth_score
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="成長スコアと最高Tier到達の関連",
                findings_html="<p>成長スコア×Tierデータが取得できません（feat_career, feat_work_context）。</p>",
                section_id="growth_tier",
            )

        tier_growth: dict[int, list[float]] = {}
        for r in rows:
            tier_growth.setdefault(r["max_tier"], []).append(r["growth_score"])

        findings = "<p>到達した最高作品規模Tier別の成長スコア:</p><ul>"
        for t in sorted(tier_growth):
            ts = distribution_summary(tier_growth[t], label=f"tier{t}")
            findings += (
                f"<li><strong>最高Tier {t}</strong>（n={ts['n']:,}）: "
                f"{format_distribution_inline(ts)}, "
                f"{format_ci((ts['ci_lower'], ts['ci_upper']))}</li>"
            )
        findings += (
            "</ul><p>注: max_tierとgrowth_scoreはともにクレジット履歴から導出されており、"
            "共通の入力データを持つため独立した指標ではない。</p>"
        )

        fig = go.Figure()
        for t in sorted(tier_growth):
            fig.add_trace(go.Box(
                y=tier_growth[t], name=f"最高Tier {t}",
                marker_color=_TIER_COLORS.get(t, "#a0a0c0"), boxpoints=False,
            ))
        fig.update_layout(
            title="到達最高Tier別成長スコア",
            xaxis_title="最高Tier", yaxis_title="成長スコア",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="成長スコアと最高Tier到達の関連",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_growth_max_tier", height=400),
            method_note=(
                "growth_scoreはfeat_careerから取得。"
                "max_tier = feat_credit_contribution x feat_work_contextの結合による"
                "全作品のMAX(scale_tier)。"
                "両指標は同一のクレジット履歴から導出されるため、"
                "構成上の相関が期待される。"
            ),
            section_id="growth_tier",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='growth_scores',
    audience='hr',
    claim='成長スコア分析 に関する記述的指標 (subtitle: IV成長軌跡・トレンドタイプ・コホート別・Tier到達との関連)',
    sources=["credits", "persons", "anime"],
    meta_table='meta_growth_scores',
)
