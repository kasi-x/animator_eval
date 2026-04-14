"""Anime Value Report — v2 compliant.

Work-level value metrics (no viewer ratings):
  1. Work production_scale distribution by scale tier (violin)
  2. Staff count vs production_scale scatter
  3. Production scale by format and decade (line)
  4. Scale tier distribution (pie + bar)
  5. Top-50 works by production_scale
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}


class AnimeValueReport(BaseReportGenerator):
    name = "anime_value_report"
    title = "作品価値指標分析"
    subtitle = "スケールTier別・フォーマット別・年代別の作品構造指標"
    filename = "anime_value_report.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_value_tier_section(sb)))
        sections.append(sb.build_section(self._build_staff_value_section(sb)))
        sections.append(sb.build_section(self._build_format_decade_section(sb)))
        sections.append(sb.build_section(self._build_tier_distribution_section(sb)))
        sections.append(sb.build_section(self._build_top_works_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Production scale by tier (violin) ────────────

    def _build_value_tier_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fwc.scale_tier, fwc.production_scale, fwc.n_staff
                FROM feat_work_context fwc
                WHERE fwc.scale_tier IS NOT NULL
                  AND fwc.production_scale IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スケールTier別 production_scale分布",
                findings_html="<p>production_scaleデータが利用できません。</p>",
                section_id="value_tier",
            )

        tier_vals: dict[int, list[float]] = {}
        for r in rows:
            tier_vals.setdefault(r["scale_tier"], []).append(r["production_scale"])

        findings = "<p>スケールTier別のproduction_scale（feat_work_context由来、視聴者評価不使用）:</p><ul>"
        for t in sorted(tier_vals):
            ts = distribution_summary(tier_vals[t], label=f"tier{t}")
            findings += (
                f"<li><strong>Tier {t}</strong> (n={ts['n']:,} works): "
                f"{format_distribution_inline(ts)}, "
                f"{format_ci((ts['ci_lower'], ts['ci_upper']))}</li>"
            )
        findings += "</ul>"

        fig = go.Figure()
        for t in sorted(tier_vals):
            vals = tier_vals[t]
            fig.add_trace(go.Violin(
                y=vals[:2000] if len(vals) > 2000 else vals,
                name=f"Tier {t}",
                box_visible=True, meanline_visible=True,
                points=False,
                line_color=_TIER_COLORS.get(t, "#a0a0c0"),
            ))
        fig.update_layout(
            title="Tier別 Production Scale（バイオリン）",
            yaxis_title="production_scale",
        )

        return ReportSection(
            title="スケールTier別 production_scale分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_value_tier", height=450),
            method_note=(
                "production_scale = staff_count × episodes × duration_mult（構造的）。"
                "視聴者評価は使用しない。"
                "scale_tier は scale_raw（format + episodes + duration）より導出され、"
                "production_scale（staff × episodes × duration）とは集計方法が異なる。"
                "両者は相関するが同一ではない — "
                "フォーマットや話数が小さい場合でもスタッフ数が多ければ、"
                "Tier 1 作品が Tier 5 平均を上回る production_scale を取り得る。"
            ),
            section_id="value_tier",
        )

    # ── Section 2: Staff count vs production_scale ──────────────

    def _build_staff_value_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fwc.n_staff, fwc.production_scale, fwc.scale_tier
                FROM feat_work_context fwc
                WHERE fwc.n_staff IS NOT NULL
                  AND fwc.production_scale IS NOT NULL
                  AND fwc.scale_tier IS NOT NULL
                ORDER BY RANDOM()
                LIMIT 5000
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スタッフ数 vs production_scale",
                findings_html="<p>スタッフ数×production_scale散布図データが利用できません。</p>",
                section_id="staff_value",
            )

        findings = (
            f"<p>スタッフ数 vs production_scaleの散布図（n={len(rows):,}作品、"
            "ランダムサンプル最大5,000、スケールTier別色分け）。</p>"
        )

        fig = go.Figure()
        tier_data: dict[int, list] = {}
        for r in rows:
            tier_data.setdefault(r["scale_tier"], []).append(r)
        for t, td in sorted(tier_data.items()):
            fig.add_trace(go.Scattergl(
                x=[r["n_staff"] for r in td],
                y=[r["production_scale"] for r in td],
                mode="markers",
                name=f"Tier {t}",
                marker=dict(color=_TIER_COLORS.get(t, "#a0a0c0"), size=4, opacity=0.5),
                hovertemplate="staff=%{x}, scale=%{y:.1f}<extra></extra>",
            ))
        fig.update_layout(
            title="スタッフ数 × Production Scale",
            xaxis_title="スタッフ数", yaxis_title="production_scale",
            xaxis_type="log", yaxis_type="log",
        )

        return ReportSection(
            title="スタッフ数 vs production_scale",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_staff_value", height=460),
            method_note=(
                "n_staff and production_scale from feat_work_context. "
                "Log-log scale. Correlation expected (n_staff is a component of production_scale)."
            ),
            section_id="staff_value",
        )

    # ── Section 3: Format × decade ──────────────────────────────

    def _build_format_decade_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    (fwc.credit_year / 10) * 10 AS decade,
                    fwc.format_group,
                    AVG(fwc.production_scale) AS avg_scale,
                    COUNT(*) AS n
                FROM feat_work_context fwc
                WHERE fwc.credit_year BETWEEN 1970 AND 2024
                  AND fwc.format_group IS NOT NULL
                  AND fwc.production_scale IS NOT NULL
                GROUP BY decade, fwc.format_group
                ORDER BY decade, avg_scale DESC
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="フォーマット×年代別 production_scale",
                findings_html="<p>フォーマット×年代データが利用できません。</p>",
                section_id="format_decade",
            )

        format_total: dict[str, int] = {}
        for r in rows:
            format_total[r["format_group"]] = format_total.get(r["format_group"], 0) + r["n"]
        top_formats = sorted(format_total, key=lambda f: -format_total[f])[:6]

        decade_fmt: dict[int, dict[str, float]] = {}
        for r in rows:
            if r["format_group"] in top_formats:
                decade_fmt.setdefault(r["decade"], {})[r["format_group"]] = r["avg_scale"]

        decades = sorted(decade_fmt.keys())
        findings = "<p>フォーマット × 年代別のproduction_scale平均値（上位6フォーマット）。</p>"

        fig = go.Figure()
        fmt_colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#667eea", "#f5576c"]
        for i, fmt in enumerate(top_formats):
            fig.add_trace(go.Scatter(
                x=decades,
                y=[decade_fmt[d].get(fmt) for d in decades],
                name=fmt, mode="lines+markers",
                line=dict(color=fmt_colors[i % len(fmt_colors)]),
            ))
        fig.update_layout(
            title="フォーマット × 年代別 平均Production Scale",
            xaxis_title="年代", yaxis_title="平均 production_scale",
        )

        return ReportSection(
            title="フォーマット×年代別 production_scale",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_format_decade", height=420),
            method_note=(
                "production_scale from feat_work_context. "
                "format_group from feat_work_context. Top 6 formats shown."
            ),
            section_id="format_decade",
        )

    # ── Section 4: Tier distribution ────────────────────────────

    def _build_tier_distribution_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT scale_tier, scale_label, COUNT(*) AS n
                FROM feat_work_context
                WHERE scale_tier IS NOT NULL
                GROUP BY scale_tier, scale_label
                ORDER BY scale_tier
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スケールTier分布",
                findings_html="<p>Tier分布データが利用できません。</p>",
                section_id="tier_dist",
            )

        total = sum(r["n"] for r in rows)
        findings = f"<p>全{total:,}作品のスケールTier分布:</p><ul>"
        for r in rows:
            label = r["scale_label"] or f"Tier {r['scale_tier']}"
            pct = 100 * r["n"] / total
            findings += f"<li><strong>{label}</strong>: {r['n']:,} ({pct:.1f}%)</li>"
        findings += "</ul>"

        labels = [r["scale_label"] or f"T{r['scale_tier']}" for r in rows]
        values = [r["n"] for r in rows]
        colors = [_TIER_COLORS.get(r["scale_tier"], "#a0a0c0") for r in rows]

        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "domain"}, {"type": "xy"}]],
            subplot_titles=("Tier構成比", "Tier件数"),
        )
        fig.add_trace(go.Pie(
            labels=labels, values=values,
            marker=dict(colors=colors),
            hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
        ), row=1, col=1)
        fig.add_trace(go.Bar(
            x=labels, y=values, marker_color=colors,
            hovertemplate="%{x}: %{y:,}<extra></extra>",
        ), row=1, col=2)
        fig.update_layout(height=400)

        return ReportSection(
            title="スケールTier分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_tier_dist", height=400),
            method_note=(
                "scale_tier と scale_label は feat_work_context より取得。"
                "format + episodes + duration から導出される。"
            ),
            section_id="tier_dist",
        )

    # ── Section 5: Top 50 works ─────────────────────────────────

    def _build_top_works_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    a.id,
                    COALESCE(NULLIF(a.title_ja,''), NULLIF(a.title_en,''), CAST(a.id AS TEXT)) AS title,
                    fwc.production_scale,
                    fwc.scale_tier,
                    fwc.n_staff,
                    fwc.credit_year AS year,
                    fwc.format_group
                FROM feat_work_context fwc
                JOIN anime a ON a.id = fwc.anime_id
                WHERE fwc.production_scale IS NOT NULL
                ORDER BY fwc.production_scale DESC
                LIMIT 50
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="Top 50 作品（production_scale順）",
                findings_html="<p>作品ランキングデータが利用できません。</p>",
                section_id="top_works",
            )

        findings = (
            "<p>production_scale上位50作品"
            "（staff_count × episodes × duration_mult）。"
            "視聴者評価は一切使用していない。</p>"
        )

        table_rows = "".join(
            f"<tr>"
            f"<td>{i}</td>"
            f"<td>{r['title']}</td>"
            f"<td>{r['year'] or ''}</td>"
            f"<td>{r['format_group'] or ''}</td>"
            f"<td>T{r['scale_tier']}</td>"
            f"<td>{r['n_staff'] or ''}</td>"
            f"<td>{r['production_scale']:.1f}</td>"
            f"</tr>"
            for i, r in enumerate(rows, 1)
        )
        table_html = (
            '<div style="overflow-x:auto;"><table>'
            "<thead><tr><th>#</th><th>タイトル</th><th>年</th><th>フォーマット</th>"
            "<th>Tier</th><th>スタッフ数</th><th>Prod. Scale</th></tr></thead>"
            f"<tbody>{table_rows}</tbody></table></div>"
        )

        return ReportSection(
            title="Top 50 作品（production_scale順）",
            findings_html=findings,
            visualization_html=table_html,
            method_note=(
                "production_scale from feat_work_context. "
                "Ranked descending. No viewer rating used."
            ),
            section_id="top_works",
        )
