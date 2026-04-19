# DEPRECATED (Phase 3-5, 2026-04-19): merged into policy_attrition. KM/Cox を付録として統合.
# This module is retained in archived/ for regeneration and audit only.
# It is NOT in V2_REPORT_CLASSES and will not run in default generation.
"""Exit Analysis report -- career gap / return statistics.

Analyzes the reliability of exit/semi-exit classifications using two data
sources at complementary granularities:

  - **feat_career_gaps** (year-level): gap_type, gap_length, returned
  - **feat_credit_activity** (quarter-level): density, n_gaps, mean_gap_quarters,
    median_gap_quarters, consecutive_quarters, n_hiatuses

Sections:
  1. Gap overview (yearly + quarterly stats)
  2. Return rate by gap length + quarterly gap distribution
  3. Return rate by era + density comparison (returners vs non-returners)
  4. Semi-exit -> true exit progression + quarterly activity patterns
  5. Return timing distribution + consecutive-quarter analysis
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_TYPE_COLORS = {"semi_exit": "#FFD166", "exit": "#f5576c"}
_Q_COLOR = "#7B68EE"  # quarterly accent (medium-slate-blue)
_Q_COLOR2 = "#20B2AA"  # quarterly secondary (light-sea-green)


class ExitAnalysisReport(BaseReportGenerator):
    name = "exit_analysis"
    title = "退職・復職分析"
    subtitle = (
        "キャリアギャップの信頼性検証 -- "
        "年単位 (feat_career_gaps) と四半期単位 (feat_credit_activity) の2視点"
    )
    filename = "exit_analysis.html"

    # ------------------------------------------------------------------ helpers

    def _has_table(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            return bool(row and row[0] > 0)
        except Exception:
            return False

    # ------------------------------------------------------------------ entry

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_overview_section(sb)))
        sections.append(sb.build_section(self._build_return_rate_section(sb)))
        sections.append(sb.build_section(self._build_era_section(sb)))
        sections.append(sb.build_section(self._build_progression_section(sb)))
        sections.append(sb.build_section(self._build_return_timing_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Overview ─────────────────────────────────────────

    def _build_overview_section(self, sb: SectionBuilder) -> ReportSection:
        # --- yearly gap stats ---
        try:
            rows = self.conn.execute("""
                SELECT gap_type, returned, COUNT(*) AS n, AVG(gap_length) AS avg_gap
                FROM feat_career_gaps
                GROUP BY gap_type, returned
                ORDER BY gap_type, returned
            """).fetchall()
        except Exception:
            rows = []

        # --- quarterly stats ---
        q_stats: dict | None = None
        if self._has_table("feat_credit_activity"):
            try:
                q_row = self.conn.execute("""
                    SELECT
                        COUNT(*)              AS n_persons,
                        AVG(density)          AS avg_density,
                        AVG(n_gaps)           AS avg_n_gaps,
                        AVG(mean_gap_quarters) AS avg_mean_gap_q,
                        AVG(median_gap_quarters) AS avg_median_gap_q,
                        AVG(n_hiatuses)       AS avg_n_hiatuses,
                        AVG(activity_span_quarters) AS avg_span_q,
                        AVG(active_quarters)  AS avg_active_q,
                        AVG(consecutive_quarters) AS avg_consec_q,
                        AVG(consecutive_rate) AS avg_consec_rate
                    FROM feat_credit_activity
                """).fetchone()
                if q_row and q_row["n_persons"]:
                    q_stats = dict(q_row)
            except Exception:
                pass

        if not rows and not q_stats:
            return ReportSection(
                title="ギャップ概要",
                findings_html=(
                    "<p>feat_career_gaps / feat_credit_activity のいずれも利用不可。"
                    "<code>compute_feat_career_gaps()</code> を実行してください。</p>"
                ),
                section_id="gap_overview",
            )

        # --- aggregate yearly data ---
        type_stats: dict[str, dict] = {}
        for r in rows:
            gt = r["gap_type"]
            if gt not in type_stats:
                type_stats[gt] = {"total": 0, "returned": 0, "avg_gap": 0}
            type_stats[gt]["total"] += r["n"]
            if r["returned"]:
                type_stats[gt]["returned"] += r["n"]
                type_stats[gt]["avg_gap"] = r["avg_gap"]

        grand_total = sum(s["total"] for s in type_stats.values())
        grand_returned = sum(s["returned"] for s in type_stats.values())

        # --- findings (Japanese) ---
        findings_parts: list[str] = []

        if grand_total > 0:
            findings_parts.append(
                "<h3>年単位分析 (feat_career_gaps)</h3>"
                f"<p>休止期間分析（年単位と四半期単位の2視点）。"
                f"feat_career_gaps (n={grand_total:,}件) の年単位分析"
            )
            if q_stats:
                findings_parts[-1] += (
                    f"に加え、feat_credit_activity (n={q_stats['n_persons']:,}件) "
                    "による四半期精度の活動密度分析を実施。"
                )
            else:
                findings_parts[-1] += "を実施。"
            findings_parts[-1] += "</p>"

            rate_pct = 100 * grand_returned / grand_total
            findings_parts.append(
                f"<p>検出されたキャリアギャップ総数: {grand_total:,}件。"
                f"全体復帰率: {grand_returned:,} / {grand_total:,} "
                f"({rate_pct:.1f}%)。</p><ul>"
            )
            for gt in ("semi_exit", "exit"):
                s = type_stats.get(gt)
                if not s:
                    continue
                label = "準退職 (3-4年gap)" if gt == "semi_exit" else "退職 (5年以上gap)"
                rate = 100 * s["returned"] / s["total"] if s["total"] else 0
                avg = s["avg_gap"] or 0
                findings_parts.append(
                    f"<li><strong>{label}</strong>: {s['total']:,}件、"
                    f"うち復帰 {s['returned']:,}件 ({rate:.1f}%)、"
                    f"復帰者平均gap長 {avg:.1f}年</li>"
                )
            findings_parts.append("</ul>")

        if q_stats:
            findings_parts.append(
                "<h3>四半期単位分析 (feat_credit_activity)</h3>"
                f"<p>対象人数: {q_stats['n_persons']:,}名。"
                f"平均活動スパン: {q_stats['avg_span_q']:.1f}四半期 "
                f"({q_stats['avg_span_q'] / 4:.1f}年相当)。"
                f"平均アクティブ四半期数: {q_stats['avg_active_q']:.1f}。"
                f"平均活動密度 (density): {q_stats['avg_density']:.3f}。"
                f"平均ギャップ回数: {q_stats['avg_n_gaps']:.1f}回。"
                f"平均ギャップ長 (四半期): {q_stats['avg_mean_gap_q']:.1f}。"
                f"中央値ギャップ長 (四半期): {q_stats['avg_median_gap_q']:.1f}。"
                f"平均休止回数 (hiatus): {q_stats['avg_n_hiatuses']:.1f}回。"
                f"平均連続活動四半期: {q_stats['avg_consec_q']:.1f}。"
                f"連続率: {q_stats['avg_consec_rate']:.3f}。</p>"
            )

        findings = "\n".join(findings_parts)

        # --- chart: 2-panel (pie + quarterly summary bar) ---
        n_cols = 2 if q_stats else 2
        specs = [[{"type": "domain"}, {"type": "xy"}]]
        subtitles = ("ギャップ種別構成", "復帰率比較")
        if q_stats:
            specs = [[{"type": "domain"}, {"type": "xy"}],
                     [{"type": "xy"}, {"type": "xy"}]]
            subtitles = (
                "ギャップ種別構成 (年単位)",
                "復帰率比較 (年単位)",
                "四半期活動密度分布",
                "四半期ギャップ長分布",
            )
            n_rows = 2
        else:
            n_rows = 1

        fig = make_subplots(
            rows=n_rows, cols=2,
            specs=specs,
            subplot_titles=subtitles,
        )

        # -- row 1 col 1: pie --
        if grand_total > 0:
            labels = []
            values = []
            colors = []
            for gt in ("semi_exit", "exit"):
                s = type_stats.get(gt, {"total": 0, "returned": 0})
                label = "準退職" if gt == "semi_exit" else "退職"
                labels.extend([f"{label} 復帰", f"{label} 未復帰"])
                values.extend([s["returned"], s["total"] - s["returned"]])
                base = _TYPE_COLORS[gt]
                colors.extend([base, base + "60"])
            fig.add_trace(go.Pie(
                labels=labels, values=values,
                marker=dict(colors=colors),
                hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
            ), row=1, col=1)

        # -- row 1 col 2: return rate bar --
        if grand_total > 0:
            bar_labels = []
            bar_rates = []
            bar_colors = []
            for gt in ("semi_exit", "exit"):
                s = type_stats.get(gt, {"total": 0, "returned": 0})
                label = "準退職\n(3-4yr)" if gt == "semi_exit" else "退職\n(5+yr)"
                bar_labels.append(label)
                bar_rates.append(100 * s["returned"] / s["total"] if s["total"] else 0)
                bar_colors.append(_TYPE_COLORS[gt])
            fig.add_trace(go.Bar(
                x=bar_labels, y=bar_rates, marker_color=bar_colors,
                hovertemplate="%{x}: %{y:.1f}%<extra></extra>",
                showlegend=False,
            ), row=1, col=2)
            fig.update_yaxes(title_text="復帰率 (%)", row=1, col=2)

        # -- row 2: quarterly charts --
        if q_stats and n_rows == 2:
            # row 2 col 1: density distribution
            try:
                density_rows = self.conn.execute("""
                    SELECT density FROM feat_credit_activity
                    WHERE density IS NOT NULL
                """).fetchall()
                density_vals = [r["density"] for r in density_rows]
                if density_vals:
                    fig.add_trace(go.Histogram(
                        x=density_vals, nbinsx=50,
                        marker_color=_Q_COLOR, opacity=0.75,
                        name="活動密度",
                        hovertemplate="density=%{x:.2f}: %{y:,}名<extra></extra>",
                    ), row=2, col=1)
                    fig.update_xaxes(title_text="活動密度 (density)", row=2, col=1)
                    fig.update_yaxes(title_text="人数", row=2, col=1)
            except Exception:
                pass

            # row 2 col 2: mean_gap_quarters histogram
            try:
                gap_q_rows = self.conn.execute("""
                    SELECT mean_gap_quarters FROM feat_credit_activity
                    WHERE mean_gap_quarters IS NOT NULL AND n_gaps > 0
                """).fetchall()
                gap_q_vals = [r["mean_gap_quarters"] for r in gap_q_rows]
                if gap_q_vals:
                    fig.add_trace(go.Histogram(
                        x=gap_q_vals, nbinsx=50,
                        marker_color=_Q_COLOR2, opacity=0.75,
                        name="平均gap(四半期)",
                        hovertemplate="mean_gap=%{x:.1f}Q: %{y:,}名<extra></extra>",
                    ), row=2, col=2)
                    fig.update_xaxes(title_text="平均ギャップ長 (四半期)", row=2, col=2)
                    fig.update_yaxes(title_text="人数", row=2, col=2)
            except Exception:
                pass

        chart_height = 750 if n_rows == 2 else 400
        fig.update_layout(height=chart_height)

        return ReportSection(
            title="ギャップ概要",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_gap_overview", height=chart_height),
            method_note=(
                "feat_career_gaps: credits x anime year データから算出。"
                "semi_exit = 連続クレジット間3-4年のギャップ。"
                "exit = 5年以上のギャップ。returned = 1 は後続クレジットが存在。"
                "進行中ギャップ (最終クレジット年 + gap >= reliable_max_year) は returned=0 "
                "とされるが、将来のデータで returned=1 に転換する可能性がある。"
                "<br>"
                "feat_credit_activity: person_id ごとの四半期単位活動記録。"
                "density = active_quarters / activity_span_quarters。"
                "n_gaps = クレジットのない四半期の連続区間数。"
                "mean/median_gap_quarters = ギャップ長の平均/中央値 (四半期単位)。"
                "n_hiatuses = 4四半期以上の長期ギャップ数。"
            ),
            section_id="gap_overview",
        )

    # ── Section 2: Return rate by gap length ────────────────────────

    def _build_return_rate_section(self, sb: SectionBuilder) -> ReportSection:
        # --- yearly: return rate by gap length ---
        try:
            rows = self.conn.execute("""
                SELECT gap_length, returned, COUNT(*) AS n
                FROM feat_career_gaps
                GROUP BY gap_length, returned
                ORDER BY gap_length
            """).fetchall()
        except Exception:
            rows = []

        # --- quarterly: gap distribution ---
        q_gap_stats: list[dict] | None = None
        if self._has_table("feat_credit_activity"):
            try:
                q_rows = self.conn.execute("""
                    SELECT
                        mean_gap_quarters,
                        median_gap_quarters,
                        max_gap_quarters,
                        n_gaps,
                        n_hiatuses
                    FROM feat_credit_activity
                    WHERE n_gaps > 0 AND mean_gap_quarters IS NOT NULL
                """).fetchall()
                if q_rows:
                    q_gap_stats = [dict(r) for r in q_rows]
            except Exception:
                pass

        if not rows and not q_gap_stats:
            return ReportSection(
                title="Gap長別 復帰率",
                findings_html="<p>Gap長データが利用不可。</p>",
                section_id="return_by_gap",
            )

        # Aggregate by gap_length (yearly)
        gap_data: dict[int, dict[str, int]] = {}
        for r in rows:
            gl = r["gap_length"]
            gap_data.setdefault(gl, {"total": 0, "returned": 0})
            gap_data[gl]["total"] += r["n"]
            if r["returned"]:
                gap_data[gl]["returned"] += r["n"]

        gap_lengths = sorted(g for g in gap_data if g <= 20)

        total_per_gap = [gap_data[g]["total"] for g in gap_lengths]
        returned_per_gap = [gap_data[g]["returned"] for g in gap_lengths]
        rate_per_gap = [
            100 * r / t if t > 0 else 0
            for r, t in zip(returned_per_gap, total_per_gap)
        ]

        findings_parts: list[str] = []

        if gap_lengths:
            findings_parts.append(
                "<h3>年単位: Gap長別復帰率</h3>"
                "<p>gap長 (年) 別の復帰率:</p><ul>"
            )
            for gl in [3, 4, 5, 6, 7, 10, 15, 20]:
                if gl in gap_data:
                    d = gap_data[gl]
                    rate = 100 * d["returned"] / d["total"] if d["total"] else 0
                    findings_parts.append(
                        f"<li><strong>{gl}年gap</strong>: "
                        f"{d['total']:,}件、復帰率 {rate:.1f}%</li>"
                    )
            findings_parts.append("</ul>")

        if q_gap_stats:
            mean_vals = [r["mean_gap_quarters"] for r in q_gap_stats]
            median_vals = [r["median_gap_quarters"] for r in q_gap_stats
                          if r["median_gap_quarters"] is not None]
            ds_mean = distribution_summary(mean_vals, label="mean_gap_quarters")
            ds_median = distribution_summary(median_vals, label="median_gap_quarters")
            findings_parts.append(
                "<h3>四半期単位: ギャップ長分布</h3>"
                f"<p>ギャップを持つ人材 n={len(q_gap_stats):,}名。"
                f"平均ギャップ長 (四半期): {format_distribution_inline(ds_mean)}。"
                f"中央値ギャップ長 (四半期): {format_distribution_inline(ds_median)}。</p>"
            )
            hiatus_counts = [r["n_hiatuses"] for r in q_gap_stats]
            n_with_hiatus = sum(1 for h in hiatus_counts if h and h > 0)
            findings_parts.append(
                f"<p>長期休止 (4四半期以上) 経験者: {n_with_hiatus:,}名 "
                f"({100 * n_with_hiatus / len(q_gap_stats):.1f}%)。</p>"
            )

        findings = "\n".join(findings_parts)

        # --- charts ---
        has_quarterly = q_gap_stats is not None and len(q_gap_stats or []) > 0
        n_rows = 2 if has_quarterly else 1

        if n_rows == 2:
            fig = make_subplots(
                rows=2, cols=1,
                specs=[[{"secondary_y": True}], [{"secondary_y": False}]],
                subplot_titles=(
                    "年単位: Gap長別 復帰率・件数",
                    "四半期単位: 平均ギャップ長と中央値ギャップ長の分布",
                ),
                vertical_spacing=0.15,
            )
        else:
            fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Row 1: yearly bar + line
        if gap_lengths:
            fig.add_trace(go.Bar(
                x=gap_lengths, y=total_per_gap,
                name="ギャップ件数",
                marker_color="rgba(160,210,219,0.5)",
                hovertemplate="%{x}年: %{y:,}件<extra></extra>",
            ), row=1, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(
                x=gap_lengths, y=rate_per_gap,
                name="復帰率 (%)",
                mode="lines+markers",
                line=dict(color="#06D6A0", width=3),
                marker=dict(size=8),
                hovertemplate="%{x}年gap: %{y:.1f}%復帰<extra></extra>",
            ), row=1, col=1, secondary_y=True)
            fig.add_vline(x=3, line_dash="dot", line_color="#FFD166",
                          annotation_text="準退職 (3yr)", annotation_position="top left",
                          row=1, col=1)
            fig.add_vline(x=5, line_dash="dash", line_color="#f5576c",
                          annotation_text="退職 (5yr)", annotation_position="top right",
                          row=1, col=1)
            fig.update_yaxes(title_text="件数", row=1, col=1, secondary_y=False)
            fig.update_yaxes(title_text="復帰率 (%)", row=1, col=1, secondary_y=True)
            fig.update_xaxes(title_text="ギャップ長 (年)", row=1, col=1)

        # Row 2: quarterly histogram (mean + median overlaid)
        if has_quarterly and n_rows == 2:
            mean_vals_plot = [r["mean_gap_quarters"] for r in q_gap_stats
                             if r["mean_gap_quarters"] is not None]
            median_vals_plot = [r["median_gap_quarters"] for r in q_gap_stats
                               if r["median_gap_quarters"] is not None]
            # Cap at 40 quarters (10 years) for readability
            mean_vals_plot = [min(v, 40) for v in mean_vals_plot]
            median_vals_plot = [min(v, 40) for v in median_vals_plot]

            fig.add_trace(go.Histogram(
                x=mean_vals_plot, nbinsx=40,
                name="平均gap長 (四半期)",
                marker_color=_Q_COLOR, opacity=0.6,
                hovertemplate="平均gap=%{x:.0f}Q: %{y:,}名<extra></extra>",
            ), row=2, col=1)
            fig.add_trace(go.Histogram(
                x=median_vals_plot, nbinsx=40,
                name="中央値gap長 (四半期)",
                marker_color=_Q_COLOR2, opacity=0.6,
                hovertemplate="中央値gap=%{x:.0f}Q: %{y:,}名<extra></extra>",
            ), row=2, col=1)
            fig.update_layout(barmode="overlay")
            fig.update_xaxes(title_text="ギャップ長 (四半期、上限40Q)", row=2, col=1)
            fig.update_yaxes(title_text="人数", row=2, col=1)

        chart_height = 800 if n_rows == 2 else 450
        fig.update_layout(height=chart_height)

        return ReportSection(
            title="Gap長別 復帰率",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_return_by_gap", height=chart_height),
            method_note=(
                "年単位: feat_career_gaps の gap_length ごとに復帰率を算出。"
                "gap_length > 20年は表示上カット。"
                "縦線は準退職 (3年) と退職 (5年) の閾値。"
                "四半期単位: feat_credit_activity の mean_gap_quarters / "
                "median_gap_quarters のヒストグラム。40四半期 (10年) で打ち切り。"
                "n_gaps > 0 の人材のみ対象。"
            ),
            interpretation_html=(
                "<p>年単位で3年から5年にかけて復帰率が急低下する場合、5年退職閾値の妥当性を"
                "支持する。四半期単位の分析は、年単位では捉えられない短期ギャップの存在を"
                "明らかにする。mean_gap_quarters と median_gap_quarters に大きな乖離がある場合、"
                "ギャップ長分布は右に裾が長い (少数の超長期ギャップが平均を引き上げている) "
                "ことを示唆する。別の解釈として、四半期単位のギャップが年単位閾値未満に"
                "収まるケースが多い場合、年単位分析は休止期間を過大推定している可能性がある。</p>"
            ),
            section_id="return_by_gap",
        )

    # ── Section 3: Return rate by era ───────────────────────────────

    def _build_era_section(self, sb: SectionBuilder) -> ReportSection:
        # --- yearly: era × type return rates ---
        try:
            rows = self.conn.execute("""
                SELECT
                    (gap_start_year / 10) * 10 AS decade,
                    gap_type,
                    returned,
                    COUNT(*) AS n
                FROM feat_career_gaps
                WHERE gap_start_year BETWEEN 1970 AND 2019
                GROUP BY decade, gap_type, returned
                ORDER BY decade
            """).fetchall()
        except Exception:
            rows = []

        # --- quarterly: density for returners vs non-returners ---
        density_comparison: dict[str, list[float]] | None = None
        if self._has_table("feat_credit_activity"):
            try:
                # Join feat_credit_activity with feat_career_gaps to compare
                # density of persons who returned vs those who did not.
                # A person is a "returner" if they have ANY returned=1 gap.
                cmp_rows = self.conn.execute("""
                    SELECT
                        CASE WHEN returner.person_id IS NOT NULL THEN 'returner'
                             ELSE 'non_returner' END AS grp,
                        ca.density
                    FROM feat_credit_activity ca
                    LEFT JOIN (
                        SELECT DISTINCT person_id
                        FROM feat_career_gaps
                        WHERE returned = 1
                    ) returner ON ca.person_id = returner.person_id
                    WHERE ca.density IS NOT NULL
                      AND ca.person_id IN (SELECT DISTINCT person_id FROM feat_career_gaps)
                """).fetchall()
                if cmp_rows:
                    density_comparison = {"returner": [], "non_returner": []}
                    for r in cmp_rows:
                        density_comparison[r["grp"]].append(r["density"])
            except Exception:
                pass

        if not rows and not density_comparison:
            return ReportSection(
                title="年代別 復帰率",
                findings_html="<p>年代別データが利用不可。</p>",
                section_id="return_by_era",
            )

        # Build decade x type return rates
        decade_type: dict[int, dict[str, dict[str, int]]] = {}
        for r in rows:
            d = r["decade"]
            gt = r["gap_type"]
            decade_type.setdefault(d, {}).setdefault(gt, {"total": 0, "returned": 0})
            decade_type[d][gt]["total"] += r["n"]
            if r["returned"]:
                decade_type[d][gt]["returned"] += r["n"]

        decades = sorted(decade_type.keys())

        findings_parts: list[str] = []

        if decades:
            findings_parts.append(
                "<h3>年単位: 年代別復帰率</h3>"
                "<p>ギャップ開始年代 x ギャップ種別ごとの復帰率:</p><ul>"
            )
            for d in decades:
                parts = []
                for gt in ("semi_exit", "exit"):
                    s = decade_type[d].get(gt, {"total": 0, "returned": 0})
                    if s["total"] > 0:
                        rate = 100 * s["returned"] / s["total"]
                        label = "準退職" if gt == "semi_exit" else "退職"
                        parts.append(f"{label}: {rate:.0f}% ({s['total']:,}件)")
                findings_parts.append(f"<li><strong>{d}年代</strong>: {', '.join(parts)}</li>")
            findings_parts.append("</ul>")

        if density_comparison:
            ret_vals = density_comparison.get("returner", [])
            non_vals = density_comparison.get("non_returner", [])
            if ret_vals and non_vals:
                ds_ret = distribution_summary(ret_vals, label="returner_density")
                ds_non = distribution_summary(non_vals, label="non_returner_density")
                findings_parts.append(
                    "<h3>四半期活動密度: 復帰者 vs 未復帰者</h3>"
                    "<p>ギャップ経験者のうち、復帰者と未復帰者の四半期活動密度を比較。</p>"
                    f"<p>復帰者 (n={ds_ret['n']:,}): "
                    f"{format_distribution_inline(ds_ret)}、"
                    f"{format_ci((ds_ret['ci_lower'], ds_ret['ci_upper']))}。</p>"
                    f"<p>未復帰者 (n={ds_non['n']:,}): "
                    f"{format_distribution_inline(ds_non)}、"
                    f"{format_ci((ds_non['ci_lower'], ds_non['ci_upper']))}。</p>"
                )

        findings = "\n".join(findings_parts)

        # --- charts ---
        has_density = (density_comparison is not None
                       and len(density_comparison.get("returner", [])) > 0)
        n_rows = 2 if has_density else 1

        if n_rows == 2:
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=("年代別復帰率推移", "四半期活動密度: 復帰者 vs 未復帰者"),
                vertical_spacing=0.15,
            )
        else:
            fig = go.Figure()

        # Row 1: era line chart
        for gt, label, color in [
            ("semi_exit", "準退職 (3-4yr)", "#FFD166"),
            ("exit", "退職 (5+yr)", "#f5576c"),
        ]:
            rates = []
            for d in decades:
                s = decade_type[d].get(gt, {"total": 0, "returned": 0})
                rates.append(100 * s["returned"] / s["total"] if s["total"] else None)
            if n_rows == 2:
                fig.add_trace(go.Scatter(
                    x=decades, y=rates, name=label,
                    mode="lines+markers", line=dict(color=color, width=2),
                    hovertemplate=f"{label} %{{x}}年代: %{{y:.1f}}%<extra></extra>",
                ), row=1, col=1)
            else:
                fig.add_trace(go.Scatter(
                    x=decades, y=rates, name=label,
                    mode="lines+markers", line=dict(color=color, width=2),
                    hovertemplate=f"{label} %{{x}}年代: %{{y:.1f}}%<extra></extra>",
                ))

        if n_rows == 2:
            fig.update_xaxes(title_text="ギャップ開始年代", row=1, col=1)
            fig.update_yaxes(title_text="復帰率 (%)", row=1, col=1)
        else:
            fig.update_layout(
                xaxis_title="ギャップ開始年代", yaxis_title="復帰率 (%)",
            )

        # Row 2: density box plot (returner vs non-returner)
        if has_density and n_rows == 2:
            for grp, label, color in [
                ("returner", "復帰者", "#06D6A0"),
                ("non_returner", "未復帰者", "#f5576c"),
            ]:
                vals = density_comparison.get(grp, [])  # type: ignore[union-attr]
                if vals:
                    fig.add_trace(go.Box(
                        y=vals, name=label,
                        marker_color=color,
                        boxmean="sd",
                        hovertemplate=f"{label}: %{{y:.3f}}<extra></extra>",
                    ), row=2, col=1)
            fig.update_yaxes(title_text="活動密度 (density)", row=2, col=1)

        chart_height = 750 if n_rows == 2 else 420
        fig.update_layout(height=chart_height)

        viz_html = plotly_div_safe(fig, "chart_return_by_era", height=chart_height)

        return ReportSection(
            title="年代別 復帰率",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "年代 = (gap_start_year / 10) * 10。"
                "2020年代は右打ち切りの影響が大きいため除外 "
                "(復帰判定に十分な時間が経過していない)。"
                "四半期密度比較: feat_career_gaps に登場する person_id のみを対象とし、"
                "feat_credit_activity の density を復帰者/未復帰者で比較。"
                "density = active_quarters / activity_span_quarters。"
            ),
            section_id="return_by_era",
        )

    # ── Section 4: Semi-exit -> exit progression ─────────────────────

    def _build_progression_section(self, sb: SectionBuilder) -> ReportSection:
        """準退職 (3-4yr gap) から退職 (5+yr gap) への遷移率分析。

        四半期活動パターン (ギャップ前後の密度変化) も分析。
        """
        # --- yearly progression ---
        try:
            rows = self.conn.execute("""
                SELECT
                    semi.person_id,
                    semi.gap_start_year AS semi_year,
                    later_exit.gap_start_year AS exit_year
                FROM feat_career_gaps semi
                LEFT JOIN feat_career_gaps later_exit
                    ON semi.person_id = later_exit.person_id
                    AND later_exit.gap_type = 'exit'
                    AND later_exit.gap_start_year > semi.gap_start_year
                WHERE semi.gap_type = 'semi_exit'
                  AND semi.returned = 1
            """).fetchall()
        except Exception:
            rows = []

        # --- quarterly: activity patterns for semi-exit persons ---
        q_pattern: dict[str, list[float]] | None = None
        if self._has_table("feat_credit_activity"):
            try:
                # Compare quarterly stats between semi-exit -> exit persons vs
                # semi-exit -> continued persons
                semi_exit_pids = set()
                later_exit_pids = set()
                for r in rows:
                    semi_exit_pids.add(r["person_id"])
                    if r["exit_year"] is not None:
                        later_exit_pids.add(r["person_id"])
                continued_pids = semi_exit_pids - later_exit_pids

                if later_exit_pids and continued_pids:
                    # Fetch quarterly stats for both groups
                    all_pids = later_exit_pids | continued_pids
                    placeholders = ",".join("?" * len(all_pids))
                    q_rows = self.conn.execute(f"""
                        SELECT person_id, density, consecutive_rate,
                               n_hiatuses, mean_gap_quarters
                        FROM feat_credit_activity
                        WHERE person_id IN ({placeholders})
                    """, list(all_pids)).fetchall()

                    q_pattern = {
                        "exit_density": [],
                        "continued_density": [],
                        "exit_consec_rate": [],
                        "continued_consec_rate": [],
                        "exit_mean_gap_q": [],
                        "continued_mean_gap_q": [],
                    }
                    for r in q_rows:
                        pid = r["person_id"]
                        prefix = "exit" if pid in later_exit_pids else "continued"
                        if r["density"] is not None:
                            q_pattern[f"{prefix}_density"].append(r["density"])
                        if r["consecutive_rate"] is not None:
                            q_pattern[f"{prefix}_consec_rate"].append(r["consecutive_rate"])
                        if r["mean_gap_quarters"] is not None:
                            q_pattern[f"{prefix}_mean_gap_q"].append(r["mean_gap_quarters"])
            except Exception:
                pass

        if not rows and not q_pattern:
            return ReportSection(
                title="準退職 -> 退職 遷移率",
                findings_html="<p>遷移データが利用不可。</p>",
                section_id="semi_to_exit",
            )

        n_semi_returned = len(set(r["person_id"] for r in rows))
        n_later_exit = len(set(
            r["person_id"] for r in rows if r["exit_year"] is not None
        ))
        rate = 100 * n_later_exit / n_semi_returned if n_semi_returned else 0

        # Time to exit after semi-exit return
        time_to_exit = [
            r["exit_year"] - r["semi_year"]
            for r in rows
            if r["exit_year"] is not None
        ]

        findings_parts: list[str] = []

        findings_parts.append(
            "<h3>年単位: 準退職からの遷移</h3>"
            f"<p>準退職 (3-4年gap) から復帰した {n_semi_returned:,}名のうち、"
            f"{n_later_exit:,}名 ({rate:.1f}%) が後に退職 (5年以上gap) を経験。</p>"
        )
        if time_to_exit:
            ts = distribution_summary(time_to_exit, label="years_to_exit")
            findings_parts.append(
                "<p>準退職から退職までの期間: "
                f"{format_distribution_inline(ts)}、"
                f"{format_ci((ts['ci_lower'], ts['ci_upper']))}。</p>"
            )
        findings_parts.append(
            f"<p>逆に、{n_semi_returned - n_later_exit:,}名 "
            f"({100 - rate:.1f}%) は準退職後も5年以上gapなく活動を継続。</p>"
        )

        if q_pattern:
            exit_d = q_pattern.get("exit_density", [])
            cont_d = q_pattern.get("continued_density", [])
            exit_cr = q_pattern.get("exit_consec_rate", [])
            cont_cr = q_pattern.get("continued_consec_rate", [])
            if exit_d and cont_d:
                ds_exit = distribution_summary(exit_d, label="exit_density")
                ds_cont = distribution_summary(cont_d, label="continued_density")
                findings_parts.append(
                    "<h3>四半期活動パターン: 遷移者 vs 継続者</h3>"
                    f"<p>後に退職した人材の活動密度: {format_distribution_inline(ds_exit)}。</p>"
                    f"<p>活動継続した人材の活動密度: {format_distribution_inline(ds_cont)}。</p>"
                )
            if exit_cr and cont_cr:
                ds_exit_cr = distribution_summary(exit_cr, label="exit_consec_rate")
                ds_cont_cr = distribution_summary(cont_cr, label="continued_consec_rate")
                findings_parts.append(
                    f"<p>遷移者の連続活動率: {format_distribution_inline(ds_exit_cr)}。</p>"
                    f"<p>継続者の連続活動率: {format_distribution_inline(ds_cont_cr)}。</p>"
                )

        findings = "\n".join(findings_parts)

        # --- charts ---
        has_q = q_pattern is not None and len(q_pattern.get("exit_density", [])) > 0
        n_rows = 2 if has_q else 1

        if n_rows == 2:
            fig = make_subplots(
                rows=2, cols=1,
                specs=[[{"type": "xy"}], [{"type": "xy"}]],
                subplot_titles=(
                    "準退職後の転帰 (年単位)",
                    "四半期活動パターン: 後に退職 vs 活動継続",
                ),
                vertical_spacing=0.15,
            )
        else:
            fig = go.Figure()

        # Row 1: outcome bar
        bar_x = ["復帰後も継続", "後に退職"]
        bar_y = [n_semi_returned - n_later_exit, n_later_exit]
        bar_colors = ["#06D6A0", "#f5576c"]
        if n_rows == 2:
            fig.add_trace(go.Bar(
                x=bar_x, y=bar_y, marker_color=bar_colors,
                hovertemplate="%{x}: %{y:,}名<extra></extra>",
                showlegend=False,
            ), row=1, col=1)
            fig.update_yaxes(title_text="人数", row=1, col=1)
        else:
            fig.add_trace(go.Bar(
                x=bar_x, y=bar_y, marker_color=bar_colors,
                hovertemplate="%{x}: %{y:,}名<extra></extra>",
            ))
            fig.update_layout(yaxis_title="人数")

        # Row 2: quarterly box plots for density and consecutive_rate
        if has_q and n_rows == 2:
            for grp, label, color in [
                ("exit", "後に退職", "#f5576c"),
                ("continued", "活動継続", "#06D6A0"),
            ]:
                d_vals = q_pattern.get(f"{grp}_density", [])  # type: ignore[union-attr]
                if d_vals:
                    fig.add_trace(go.Box(
                        y=d_vals, name=f"{label}\n(密度)",
                        marker_color=color,
                        boxmean="sd",
                    ), row=2, col=1)
                cr_vals = q_pattern.get(f"{grp}_consec_rate", [])  # type: ignore[union-attr]
                if cr_vals:
                    fig.add_trace(go.Box(
                        y=cr_vals, name=f"{label}\n(連続率)",
                        marker_color=color,
                        boxmean="sd",
                    ), row=2, col=1)
            fig.update_yaxes(title_text="値 (0-1)", row=2, col=1)

        chart_height = 750 if n_rows == 2 else 380
        fig.update_layout(height=chart_height)

        return ReportSection(
            title="準退職 -> 退職 遷移率",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_semi_to_exit", height=chart_height),
            method_note=(
                "対象: feat_career_gaps で gap_type='semi_exit' かつ returned=1 の人材。"
                "同一person_idでgap_start_year がより後の gap_type='exit' レコードが"
                "存在すれば「後に退職」と判定。1人が複数回準退職した場合も1人として計上。"
                "四半期パターン: 上記2群それぞれについて feat_credit_activity の "
                "density (活動密度) と consecutive_rate (連続活動率) を比較。"
            ),
            interpretation_html=(
                "<p>遷移率が高い場合、3-4年のギャップは退職の前兆シグナルである可能性がある。"
                "遷移率が低い場合、一時的な休止と永続的な退職は異なる現象であり、"
                "5年閾値は有効に機能していると解釈できる。"
                "四半期分析で遷移者の活動密度/連続率が有意に低い場合、四半期指標は "
                "年単位閾値より早期に退職リスクを検知できる可能性がある。"
                "ただし、四半期データの欠損パターン (データソースの記録頻度) が "
                "活動密度の計測を歪めている可能性も考慮する。</p>"
            ),
            section_id="semi_to_exit",
        )

    # ── Section 5: Return timing distribution ───────────────────────

    def _build_return_timing_section(self, sb: SectionBuilder) -> ReportSection:
        # --- yearly: gap length distribution for returners ---
        try:
            rows = self.conn.execute("""
                SELECT gap_length, gap_type, COUNT(*) AS n
                FROM feat_career_gaps
                WHERE returned = 1
                GROUP BY gap_length, gap_type
                ORDER BY gap_length
            """).fetchall()
        except Exception:
            rows = []

        # --- quarterly: consecutive_quarters distribution ---
        consec_vals: list[float] | None = None
        consec_by_hiatus: dict[str, list[float]] | None = None
        if self._has_table("feat_credit_activity"):
            try:
                c_rows = self.conn.execute("""
                    SELECT consecutive_quarters, n_hiatuses
                    FROM feat_credit_activity
                    WHERE consecutive_quarters IS NOT NULL
                """).fetchall()
                if c_rows:
                    consec_vals = [r["consecutive_quarters"] for r in c_rows]
                    consec_by_hiatus = {"hiatus_0": [], "hiatus_1_2": [], "hiatus_3plus": []}
                    for r in c_rows:
                        nh = r["n_hiatuses"] or 0
                        if nh == 0:
                            consec_by_hiatus["hiatus_0"].append(r["consecutive_quarters"])
                        elif nh <= 2:
                            consec_by_hiatus["hiatus_1_2"].append(r["consecutive_quarters"])
                        else:
                            consec_by_hiatus["hiatus_3plus"].append(r["consecutive_quarters"])
            except Exception:
                pass

        if not rows and not consec_vals:
            return ReportSection(
                title="復帰タイミング分布",
                findings_html="<p>復帰タイミングデータが利用不可。</p>",
                section_id="return_timing",
            )

        semi_data: dict[int, int] = {}
        exit_data: dict[int, int] = {}
        for r in rows:
            if r["gap_type"] == "semi_exit":
                semi_data[r["gap_length"]] = r["n"]
            else:
                exit_data[r["gap_length"]] = r["n"]

        all_gaps = sorted(set(semi_data) | set(exit_data))
        all_gaps = [g for g in all_gaps if g <= 25]

        findings_parts: list[str] = []

        if all_gaps:
            total_semi = sum(semi_data.values())
            total_exit = sum(exit_data.values())
            findings_parts.append(
                "<h3>年単位: 復帰者のギャップ長分布</h3>"
                f"<p>復帰者のギャップ長分布: 準退職復帰 {total_semi:,}件、"
                f"退職復帰 {total_exit:,}件。</p>"
            )
            # Distribution summary for returner gap lengths
            all_gap_lengths = []
            for gl, n in semi_data.items():
                all_gap_lengths.extend([gl] * n)
            for gl, n in exit_data.items():
                all_gap_lengths.extend([gl] * n)
            if all_gap_lengths:
                ds_gl = distribution_summary(all_gap_lengths, label="return_gap_length")
                findings_parts.append(
                    f"<p>復帰者ギャップ長: {format_distribution_inline(ds_gl)}、"
                    f"{format_ci((ds_gl['ci_lower'], ds_gl['ci_upper']))}。</p>"
                )

        if consec_vals:
            ds_consec = distribution_summary(consec_vals, label="consecutive_quarters")
            findings_parts.append(
                "<h3>四半期単位: 連続活動四半期数の分布</h3>"
                f"<p>全人材の最大連続活動四半期数: {format_distribution_inline(ds_consec)}、"
                f"{format_ci((ds_consec['ci_lower'], ds_consec['ci_upper']))}。</p>"
            )
            if consec_by_hiatus:
                for key, label in [
                    ("hiatus_0", "休止0回"),
                    ("hiatus_1_2", "休止1-2回"),
                    ("hiatus_3plus", "休止3回以上"),
                ]:
                    vals = consec_by_hiatus.get(key, [])
                    if vals:
                        ds = distribution_summary(vals, label=key)
                        findings_parts.append(
                            f"<p>{label} (n={ds['n']:,}): "
                            f"連続活動四半期 {format_distribution_inline(ds)}。</p>"
                        )

        findings = "\n".join(findings_parts)

        # --- charts ---
        has_consec = consec_vals is not None and len(consec_vals or []) > 0
        n_rows = 2 if has_consec else 1

        if n_rows == 2:
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=(
                    "復帰者ギャップ長分布 (年単位)",
                    "連続活動四半期数の分布 (休止回数別)",
                ),
                vertical_spacing=0.15,
            )
        else:
            fig = go.Figure()

        # Row 1: yearly stacked bar
        if all_gaps:
            if n_rows == 2:
                fig.add_trace(go.Bar(
                    x=[g for g in all_gaps if g in semi_data],
                    y=[semi_data[g] for g in all_gaps if g in semi_data],
                    name="準退職 (3-4yr)", marker_color="#FFD166",
                    hovertemplate="%{x}年gap: %{y:,}件<extra></extra>",
                ), row=1, col=1)
                fig.add_trace(go.Bar(
                    x=[g for g in all_gaps if g in exit_data],
                    y=[exit_data[g] for g in all_gaps if g in exit_data],
                    name="退職 (5+yr)", marker_color="#f5576c",
                    hovertemplate="%{x}年gap: %{y:,}件<extra></extra>",
                ), row=1, col=1)
                fig.update_xaxes(title_text="ギャップ長 (年)", row=1, col=1)
                fig.update_yaxes(title_text="件数", row=1, col=1)
            else:
                fig.add_trace(go.Bar(
                    x=[g for g in all_gaps if g in semi_data],
                    y=[semi_data[g] for g in all_gaps if g in semi_data],
                    name="準退職 (3-4yr)", marker_color="#FFD166",
                    hovertemplate="%{x}年gap: %{y:,}件<extra></extra>",
                ))
                fig.add_trace(go.Bar(
                    x=[g for g in all_gaps if g in exit_data],
                    y=[exit_data[g] for g in all_gaps if g in exit_data],
                    name="退職 (5+yr)", marker_color="#f5576c",
                    hovertemplate="%{x}年gap: %{y:,}件<extra></extra>",
                ))
                fig.update_layout(
                    xaxis_title="ギャップ長 (年)", yaxis_title="件数",
                )

        # Row 2: consecutive quarters box by hiatus group
        if has_consec and n_rows == 2 and consec_by_hiatus:
            colors_hiatus = {
                "hiatus_0": "#06D6A0",
                "hiatus_1_2": _Q_COLOR,
                "hiatus_3plus": "#f5576c",
            }
            labels_hiatus = {
                "hiatus_0": "休止0回",
                "hiatus_1_2": "休止1-2回",
                "hiatus_3plus": "休止3回以上",
            }
            for key in ("hiatus_0", "hiatus_1_2", "hiatus_3plus"):
                vals = consec_by_hiatus.get(key, [])
                if vals:
                    # Cap at 80 quarters for display
                    capped = [min(v, 80) for v in vals]
                    fig.add_trace(go.Box(
                        y=capped,
                        name=labels_hiatus[key],
                        marker_color=colors_hiatus[key],
                        boxmean="sd",
                        hovertemplate=(
                            f"{labels_hiatus[key]}: %{{y:.0f}}Q<extra></extra>"
                        ),
                    ), row=2, col=1)
            fig.update_yaxes(
                title_text="連続活動四半期数 (上限80Q)", row=2, col=1,
            )

        fig.update_layout(barmode="stack", height=750 if n_rows == 2 else 400)
        chart_height = 750 if n_rows == 2 else 400

        return ReportSection(
            title="復帰タイミング分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_return_timing", height=chart_height),
            method_note=(
                "年単位: returned=1 の人材のみを対象としたギャップ長分布。"
                "準退職: 3-4年gap、退職: 5年以上gap。長期gapでの復帰はまれだが存在する。"
                "四半期単位: feat_credit_activity の consecutive_quarters "
                "(最大連続活動四半期数) を n_hiatuses (4Q以上ギャップ回数) 別に比較。"
                "連続活動四半期が短く休止回数が多い人材は断続的キャリアパターンを示す。"
                "80四半期 (20年) で表示を打ち切り。"
            ),
            interpretation_html=(
                "<p>年単位で準退職gapからの復帰が退職gapよりも圧倒的に多い場合、"
                "閾値設計の妥当性を支持する。四半期分析では、休止回数0の人材が長い"
                "連続活動期間を持ち、休止回数3回以上の人材が短い連続活動期間を持つなら、"
                "断続的参加パターンが明確に識別可能であることを示す。"
                "ただし、連続活動四半期は活動スパン (キャリア長) と相関するため、"
                "キャリア長が異なる群を直接比較する際にはこの交絡に留意する。</p>"
            ),
            section_id="return_timing",
        )
