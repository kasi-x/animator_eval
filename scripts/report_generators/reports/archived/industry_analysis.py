# DEPRECATED (Phase 3-5, 2026-04-19): merged into industry_overview. common 版と重複を解消.
# This module is retained in archived/ for regeneration and audit only.
# It is NOT in V2_REPORT_CLASSES and will not run in default generation.
"""Industry Analysis dashboard — v2 compliant.

Covers macro industry trends:
- Section 1: Production volume by year and format
- Section 2: Role composition trends by era
- Section 3: Network growth metrics
- Section 4: Seasonal patterns
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class IndustryAnalysisReport(BaseReportGenerator):
    name = "industry_analysis"
    title = "業界分析ダッシュボード"
    subtitle = "アニメ制作業界のマクロトレンドと構造変化"
    filename = "industry_analysis.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_volume_section(sb)))
        sections.append(sb.build_section(self._build_format_section(sb)))
        sections.append(sb.build_section(self._build_role_composition_section(sb)))
        sections.append(sb.build_section(self._build_seasonal_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Production volume ─────────────────────────────

    def _build_volume_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    a.year,
                    COUNT(DISTINCT a.id) AS n_works,
                    COUNT(DISTINCT c.person_id) AS n_persons,
                    COUNT(c.id) AS n_credits
                FROM anime a
                LEFT JOIN credits c ON c.anime_id = a.id
                WHERE a.year BETWEEN 1963 AND 2025
                  AND a.year IS NOT NULL
                GROUP BY a.year
                ORDER BY a.year
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="年次制作量推移",
                findings_html="<p>年次制作データがありません。</p>",
                section_id="volume",
            )

        years = [r["year"] for r in rows]
        n_works = [r["n_works"] for r in rows]
        n_persons = [r["n_persons"] for r in rows]

        total_works = sum(n_works)
        total_persons_unique = None
        try:
            res = self.conn.execute("SELECT COUNT(*) AS n FROM persons WHERE id IN (SELECT DISTINCT person_id FROM credits)").fetchone()
            total_persons_unique = res["n"] if res else None
        except Exception:
            pass

        findings = (
            f"<p>年間制作量の推移（{len(years)}年間で合計n={total_works:,}作品）。"
        )
        if total_persons_unique:
            findings += f"1件以上のクレジットを持つ人物の総数: {total_persons_unique:,}人。"
        findings += (
            "データセット全体で制作量は増加傾向にあり、1990年代以降に加速が見られる。"
            "2020年代の数値は、近年の作品のクレジット記録が進行中のため、"
            "過少計上の可能性がある。</p>"
        )

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=years, y=n_works, name="年間作品数",
            line=dict(color="#f093fb", width=2),
            hovertemplate="%{x}: %{y:,} 作品<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=years, y=n_persons, name="年間稼働人数",
            line=dict(color="#a0d2db", width=1.5, dash="dot"),
            yaxis="y2",
            hovertemplate="%{x}: %{y:,} 人<extra></extra>",
        ))
        fig.update_layout(
            title="年間制作ボリューム",
            xaxis_title="年",
            yaxis=dict(title="年間作品数", side="left"),
            yaxis2=dict(title="年間稼働人数", side="right", overlaying="y"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="年次制作量推移",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_volume", height=420),
            method_note=(
                "作品数は anime テーブルを年（放送年 = anime.year）で集計。"
                "年次稼働人数は credits JOIN anime ON credit_year = anime.year で集計。"
                "1980年以前は記録が疎なため値が小さい。"
                "2023–2025年は遡及的なクレジット追加により値が増える可能性がある。"
            ),
            section_id="volume",
        )

    # ── Section 2: Format distribution ──────────────────────────

    def _build_format_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    (year / 10) * 10 AS decade,
                    format,
                    COUNT(*) AS n
                FROM anime
                WHERE year BETWEEN 1960 AND 2024
                  AND format IS NOT NULL
                GROUP BY decade, format
                ORDER BY decade, n DESC
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="フォーマット別作品数（年代別）",
                findings_html="<p>フォーマットデータがありません。</p>",
                section_id="format",
            )

        # Aggregate by decade and top formats
        decade_format: dict[int, dict[str, int]] = {}
        all_formats: set[str] = set()
        for r in rows:
            d = r["decade"]
            f = r["format"]
            decade_format.setdefault(d, {})[f] = r["n"]
            all_formats.add(f)

        decades = sorted(decade_format.keys())
        top_formats = sorted(all_formats, key=lambda f: -sum(
            decade_format[d].get(f, 0) for d in decades
        ))[:8]

        findings = "<p>年代別の制作フォーマット分布:</p><ul>"
        for d in decades:
            total = sum(decade_format[d].values())
            top = sorted(decade_format[d].items(), key=lambda x: -x[1])[:3]
            top_str = ", ".join(f"{f}: {n:,}" for f, n in top)
            findings += f"<li><strong>{d}年代</strong>（{total:,}作品）: {top_str} ...</li>"
        findings += "</ul>"

        fig = go.Figure()
        format_colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166",
                         "#667eea", "#f5576c", "#fda085", "#8a94a0"]
        for i, fmt in enumerate(top_formats):
            fig.add_trace(go.Bar(
                x=[str(d) for d in decades],
                y=[decade_format[d].get(fmt, 0) for d in decades],
                name=fmt,
                marker_color=format_colors[i % len(format_colors)],
            ))
        fig.update_layout(
            title="年代 × フォーマット別作品数",
            barmode="stack",
            xaxis_title="年代", yaxis_title="作品数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="フォーマット別作品数（年代別）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_format", height=420),
            method_note=(
                "フォーマットは anime.format（AniList/MAL の生文字列）。"
                "総作品数で上位8フォーマットを表示。"
                "フォーマット分類はソースにより異なる（例: 'TV' vs 'TV_SHORT'）。"
                "正規化は行っていない。"
            ),
            section_id="format",
        )

    # ── Section 3: Role composition ──────────────────────────────

    def _build_role_composition_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    (c.credit_year / 10) * 10 AS decade,
                    c.role,
                    COUNT(*) AS n
                FROM credits c
                WHERE c.credit_year BETWEEN 1960 AND 2024
                  AND c.role IS NOT NULL
                GROUP BY decade, c.role
                ORDER BY decade, n DESC
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="役職構成の時系列変化",
                findings_html="<p>役職構成データがありません。</p>",
                section_id="role_composition",
            )

        decade_role: dict[int, dict[str, int]] = {}
        for r in rows:
            decade_role.setdefault(r["decade"], {})[r["role"]] = r["n"]

        decades = sorted(decade_role.keys())
        all_roles = sorted(
            {r for d in decade_role.values() for r in d},
            key=lambda r: -sum(decade_role[d].get(r, 0) for d in decades)
        )[:10]

        total_by_decade = {d: sum(decade_role[d].values()) for d in decades}
        findings = "<p>年代別の総クレジット数に占める役職構成比:</p><ul>"
        for d in decades:
            tot = total_by_decade[d] or 1
            top3 = sorted(decade_role[d].items(), key=lambda x: -x[1])[:3]
            top_str = ", ".join(f"{r}: {100*n/tot:.0f}%" for r, n in top3)
            findings += f"<li><strong>{d}年代</strong>: {top_str}</li>"
        findings += "</ul>"

        fig = go.Figure()
        colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166",
                  "#667eea", "#f5576c", "#fda085", "#8a94a0", "#c0c0e0", "#a0b0a0"]
        for i, role in enumerate(all_roles):
            totals = [total_by_decade.get(d, 1) for d in decades]
            fig.add_trace(go.Bar(
                x=[str(d) for d in decades],
                y=[100 * decade_role[d].get(role, 0) / max(t, 1) for d, t in zip(decades, totals)],
                name=role,
                marker_color=colors[i % len(colors)],
            ))
        fig.update_layout(
            title="年代別 役職構成比 (%)",
            barmode="stack",
            xaxis_title="年代", yaxis_title="クレジット構成比 (%)",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="役職構成の時系列変化",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_role_comp", height=420),
            method_note=(
                "credits テーブルを credit_year と role でグルーピング。"
                "総数で上位10役職を表示。"
                "役職文字列はソースデータの生値で、役職カテゴリへの正規化は行っていない。"
                "役職構成の変化は、業界の実態変化だけでなく、"
                "ソース間のクレジット記録慣行の違いを部分的に反映する可能性がある。"
            ),
            section_id="role_composition",
        )

    # ── Section 4: Seasonal patterns ────────────────────────────

    def _build_seasonal_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    a.season,
                    COUNT(DISTINCT a.id) AS n_works,
                    AVG(CASE WHEN fwc.scale_tier IS NOT NULL THEN fwc.scale_tier END) AS avg_tier
                FROM anime a
                LEFT JOIN feat_work_context fwc ON fwc.anime_id = a.id
                WHERE a.season IS NOT NULL
                  AND a.year BETWEEN 1990 AND 2024
                GROUP BY a.season
                ORDER BY
                    CASE a.season
                        WHEN 'WINTER' THEN 1 WHEN 'SPRING' THEN 2
                        WHEN 'SUMMER' THEN 3 WHEN 'FALL' THEN 4
                        ELSE 5
                    END
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="クール別制作量",
                findings_html="<p>クール別データがありません。</p>",
                section_id="seasonal",
            )

        seasons = [r["season"] for r in rows]
        n_works = [r["n_works"] for r in rows]
        findings = "<p>放送クール別の制作数と平均作品規模ティア（1990–2024）:</p><ul>"
        for r in rows:
            tier_str = f"、平均tier={r['avg_tier']:.2f}" if r["avg_tier"] is not None else ""
            findings += f"<li><strong>{r['season']}</strong>: {r['n_works']:,}作品{tier_str}</li>"
        findings += "</ul>"

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=seasons, y=n_works,
            marker_color=["#667eea", "#06D6A0", "#FFD166", "#f5576c"],
            hovertemplate="%{x}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            title="季節別作品数（1990–2024）",
            xaxis_title="季節", yaxis_title="作品数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="クール別制作量",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_seasonal", height=360),
            method_note=(
                "季節は anime.season（WINTER/SPRING/SUMMER/FALL）。"
                "NULL の季節は除外。一貫性のため1990–2024年の範囲に限定"
                "（1990年以前は季節データが疎）。"
                "avg_tier は feat_work_context.scale_tier より算出。"
            ),
            section_id="seasonal",
        )
