"""Industry Overview report -- v2 compliant rewrite.

Ports ALL 20 visualizations from the original generate_industry_overview()
in generate_all_reports.py and adds gender/tier stratification.

Sections:
  1. Industry Scale (pipeline summary, annual production volume)
  2. Workforce Stock (stacked area by career stage over time) [Chart A]
  3. Entry/Exit/Career-Up Rates by stage [Chart B]
  4. Career Stage Milestones (debut seasons by era) [Chart C]
  5. Collaborator IV mean x Individual IV percentile 4-group (scatter + bar) [Chart D]
  6. Role-Specific Annual Trends [Chart E]
  7. Value Flow (area), Loss Type (bar), Role Composition (pie) [Charts F1-F3]
  8. Studio-Level Flow [Chart G]
  9. Country-Level Flow [Chart H]
  10. Blank/Return Analysis (DB full population) [Chart I]
  11. Cluster-Specific Flow [Chart J]
  12. Decade Comparison (demand/supply dual line)
  13. Seasonal Analysis (grouped bar + debut panels)
  14. Growth Trends (debut_decade x trend stacked bar)

All sections follow v2 structure:
  Findings (descriptive only) -> Visualization -> Method Note -> [Interpretation]

Gender/tier stratification added to Charts A, B, D, I.
"""

from __future__ import annotations

import bisect
import random
import statistics
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..html_templates import plotly_div_safe, stratification_tabs, strat_panel
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RELIABLE_MAX_YEAR = 2025
STAT_MAX_YEAR = 2024
EXIT_CUTOFF_YEAR = 2020       # 退職: 5年以上クレジットなし (2025 - 5)
SEMI_EXIT_CUTOFF_YEAR = 2022  # 準退職: 3年以上クレジットなし (2025 - 3)
FLOW_START_YEAR = 1970
_EXP_HIGH_PCTILE = 70.0

STAGE_GROUPS_DEF = [
    ("初級ランク", 0, 2, "#a0d2db"),
    ("中級ランク", 3, 4, "#06D6A0"),
    ("上級ランク", 5, 99, "#f093fb"),
]

EXP_GROUPS_DEF = [
    ("1年目", 0, 0, "#FF6B6B"),
    ("2年目", 1, 1, "#FFA94D"),
    ("3年目", 2, 2, "#FFD43B"),
    ("若手 (4~9年)", 3, 9, "#69DB7C"),
    ("中堅 (10~19年)", 10, 19, "#4DABF7"),
    ("ベテラン (20年+)", 20, 999, "#DA77F2"),
]

ROLE_TYPE_DEF: dict[str, tuple[str, str]] = {
    "animator": ("動画/原画", "#a0d2db"),
    "director": ("演出/監督", "#f093fb"),
    "designer": ("デザイナー", "#FFD166"),
    "production": ("制作", "#fda085"),
    "writing": ("脚本/構成", "#06D6A0"),
    "technical": ("技術/CG", "#667eea"),
    "other": ("その他", "#606070"),
}

_EXP_TIER_DEFS = [
    ("高期待・高実績群", "#F72585"),
    ("高期待・低実績群", "#FFD166"),
    ("低期待・高実績群", "#06D6A0"),
    ("低期待・低実績群", "#a0a0c0"),
]

LOSS_TYPES = ["エース離脱", "上級ランク引退", "中級ランク離脱", "初級ランク早期離脱"]
LOSS_TYPE_COLORS = {
    "エース離脱": "#FFD166",
    "上級ランク引退": "#f093fb",
    "中級ランク離脱": "#06D6A0",
    "初級ランク早期離脱": "#a0d2db",
}

_TIER_COLORS = {
    1: "#667eea",
    2: "#a0d2db",
    3: "#06D6A0",
    4: "#FFD166",
    5: "#f5576c",
}
_TIER_NAMES = {
    1: "Micro (T1)",
    2: "Small (T2)",
    3: "Standard (T3)",
    4: "Large (T4)",
    5: "Major (T5)",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stage_group(stage: int) -> str:
    for sg, lo, hi, _ in STAGE_GROUPS_DEF:
        if lo <= stage <= hi:
            return sg
    return "初級ランク"


def _exp_group(years_since_debut: int) -> str:
    for label, lo, hi, _ in EXP_GROUPS_DEF:
        if lo <= years_since_debut <= hi:
            return label
    return EXP_GROUPS_DEF[-1][0]


def _role_type(primary_role: str | None) -> str:
    return primary_role if primary_role in ROLE_TYPE_DEF else "other"


def _hex_rgba(hex_color: str, alpha: float = 0.6) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _fmt_num(n: int | float) -> str:
    if isinstance(n, float):
        return f"{n:,.2f}"
    return f"{n:,}"


# ---------------------------------------------------------------------------
# Report class
# ---------------------------------------------------------------------------

class IndustryOverviewReport(BaseReportGenerator):
    """Industry Overview dashboard -- REPORT_PHILOSOPHY v2 compliant."""

    name = "industry_overview"
    title = "業界概観ダッシュボード"
    subtitle = "アニメ制作業界の記述的統計と構造的分布"
    filename = "industry_overview.html"

    _EXTRA_CSS = """
<style>
.tier-chip {
    display: inline-block; padding: 0.2rem 0.6rem;
    border-radius: 10px; font-size: 0.78rem; font-weight: 700;
    margin: 0.15rem;
}
.stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem; margin: 1.5rem 0;
}
.stat-card {
    background: linear-gradient(135deg, rgba(240,147,251,0.15), rgba(245,87,108,0.1));
    border: 1px solid rgba(240,147,251,0.2);
    border-radius: 12px; padding: 1.5rem; text-align: center;
}
.stat-card .value {
    font-size: 2.2rem; font-weight: 800;
    background: linear-gradient(135deg, #f093fb, #f5576c);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
}
.stat-card .label { font-size: 0.85rem; color: #a0a0c0; margin-top: 0.3rem; }
</style>
"""

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []

        # ----------------------------------------------------------
        # Load shared data
        # ----------------------------------------------------------
        data = self._load_all_data()
        if not data:
            return None

        # Section 1: Industry Scale
        sec = self._build_scale_section(sb, data)
        sections.append(sb.build_section(sec))

        # Section 2: Workforce Stock (Chart A)
        sec = self._build_stock_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 3: Entry/Exit (Chart B)
        sec = self._build_entry_exit_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 4: Career Milestones (Chart C)
        sec = self._build_milestones_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 5: 4-tier classification (Chart D)
        sec = self._build_four_tier_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 6: Role-specific trends (Chart E)
        sec = self._build_role_trends_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 7: Value flow (Charts F1-F3)
        sec = self._build_value_flow_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 8: Studio-level flow (Chart G)
        sec = self._build_studio_flow_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 9: Country-level flow (Chart H)
        sec = self._build_country_flow_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 10: Blank/Return analysis (Chart I)
        sec = self._build_blank_return_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 11: Cluster-specific flow (Chart J)
        sec = self._build_cluster_flow_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 12: Decade comparison
        sec = self._build_decade_comparison_section(sb)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 13: Seasonal analysis
        sec = self._build_seasonal_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        # Section 14: Growth trends
        sec = self._build_growth_trends_section(sb, data)
        if sec:
            sections.append(sb.build_section(sec))

        body = self._EXTRA_CSS + "\n".join(sections)
        return self.write_report(body)

    # ==================================================================
    # Data loading
    # ==================================================================

    def _load_all_data(self) -> dict[str, Any] | None:
        """Load all shared data for the report."""
        data: dict[str, Any] = {}

        # --- Summary stats ---
        try:
            r = self.conn.execute("SELECT COUNT(*) AS n FROM persons").fetchone()
            data["n_persons"] = r["n"] if r else 0
        except Exception:
            data["n_persons"] = 0
        try:
            r = self.conn.execute("SELECT COUNT(*) AS n FROM anime").fetchone()
            data["n_anime"] = r["n"] if r else 0
        except Exception:
            data["n_anime"] = 0
        try:
            r = self.conn.execute("SELECT COUNT(*) AS n FROM credits").fetchone()
            data["n_credits"] = r["n"] if r else 0
        except Exception:
            data["n_credits"] = 0

        # --- Scores data (person-level) ---
        pid_first_year: dict[str, int] = {}
        pid_latest_year: dict[str, int] = {}
        pid_stage: dict[str, int] = {}
        pid_iv: dict[str, float] = {}
        pid_role_type_map: dict[str, str] = {}
        pid_trend: dict[str, str] = {}
        pid_gender: dict[str, str] = {}
        pid_total_credits: dict[str, int] = {}
        scores_list: list[dict] = []

        try:
            rows = self.conn.execute("""
                SELECT fps.person_id, fps.iv_score, fps.birank, fps.patronage,
                       fps.person_fe, fc.primary_role, fc.total_credits,
                       fc.first_year, fc.latest_year, fc.highest_stage,
                       fc.active_years,
                       p.gender
                FROM feat_person_scores fps
                LEFT JOIN feat_career fc ON fps.person_id = fc.person_id
                LEFT JOIN persons p ON fps.person_id = p.id
            """).fetchall()
            for r in rows:
                pid = r["person_id"]
                if not pid:
                    continue
                fy = r["first_year"]
                ly = r["latest_year"]
                st = r["highest_stage"] or 0
                iv = r["iv_score"] or 0.0
                if fy:
                    pid_first_year[pid] = fy
                if ly:
                    pid_latest_year[pid] = ly
                pid_stage[pid] = st
                pid_iv[pid] = iv
                pid_role_type_map[pid] = _role_type(r["primary_role"])
                pid_total_credits[pid] = r["total_credits"] or 0
                gender = r["gender"]
                if gender:
                    pid_gender[pid] = (
                        "F" if gender == "Female" else
                        "M" if gender == "Male" else
                        "other"
                    )
                scores_list.append({
                    "person_id": pid,
                    "iv_score": iv,
                    "birank": r["birank"] or 0.0,
                    "patronage": r["patronage"] or 0.0,
                    "person_fe": r["person_fe"] or 0.0,
                    "total_credits": r["total_credits"] or 0,
                    "career": {
                        "first_year": fy,
                        "latest_year": ly,
                        "highest_stage": st,
                        "active_years": r["active_years"] or 0,
                    },
                })
        except Exception:
            pass

        # --- Growth trend data ---
        try:
            rows_g = self.conn.execute("""
                SELECT person_id, growth_trend
                FROM feat_cluster_membership
                WHERE growth_trend IS NOT NULL
            """).fetchall()
            for r in rows_g:
                pid_trend[r["person_id"]] = r["growth_trend"]
        except Exception:
            pass

        # --- Milestones (promotion events) ---
        pid_stage_timeline: dict[str, list[tuple[int, int]]] = {}
        try:
            rows_m = self.conn.execute("""
                SELECT person_id, event_type, event_year, from_stage, to_stage
                FROM feat_milestones
                WHERE event_type IN ('promotion', 'career_start')
                  AND event_year IS NOT NULL
            """).fetchall()
            for r in rows_m:
                pid = r["person_id"]
                yr = r["event_year"]
                if r["event_type"] == "promotion":
                    to_s = r["to_stage"] or 0
                    if to_s:
                        pid_stage_timeline.setdefault(pid, []).append((yr, to_s))
                elif r["event_type"] == "career_start":
                    pid_stage_timeline.setdefault(pid, []).append((yr, 1))
            for pid in pid_stage_timeline:
                pid_stage_timeline[pid].sort(key=lambda x: x[0])
        except Exception:
            pass

        # --- IV percentiles ---
        iv_vals_sorted = sorted(pid_iv.values()) if pid_iv else [0.0]
        p90_idx = int(len(iv_vals_sorted) * 0.90)
        high_iv_threshold = iv_vals_sorted[p90_idx] if iv_vals_sorted else 0.0

        # --- Annual time series ---
        annual_series: list[dict] = []
        try:
            rows_a = self.conn.execute("""
                SELECT a.year,
                       COUNT(DISTINCT a.id) AS n_anime,
                       COUNT(c.id) AS n_credits,
                       COUNT(DISTINCT c.person_id) AS n_persons_active
                FROM anime a
                LEFT JOIN credits c ON c.anime_id = a.id AND c.credit_year = a.year
                WHERE a.year >= 1963 AND a.year <= 2025
                GROUP BY a.year ORDER BY a.year
            """).fetchall()
            annual_series = [dict(r) for r in rows_a]
        except Exception:
            pass

        # Stage at year function
        def _stage_at_year(pid: str, year: int) -> int:
            tl = pid_stage_timeline.get(pid)
            if not tl:
                return pid_stage.get(pid, 0)
            stage = 0
            for yr, st in tl:
                if yr <= year:
                    stage = st
                else:
                    break
            return stage if stage > 0 else pid_stage.get(pid, 0)

        if not pid_first_year:
            return None

        data.update({
            "pid_first_year": pid_first_year,
            "pid_latest_year": pid_latest_year,
            "pid_stage": pid_stage,
            "pid_iv": pid_iv,
            "pid_role_type": pid_role_type_map,
            "pid_trend": pid_trend,
            "pid_gender": pid_gender,
            "pid_total_credits": pid_total_credits,
            "pid_stage_timeline": pid_stage_timeline,
            "stage_at_year": _stage_at_year,
            "high_iv_threshold": high_iv_threshold,
            "scores_list": scores_list,
            "annual_series": annual_series,
        })
        return data

    # ==================================================================
    # Section 1: Industry Scale
    # ==================================================================

    def _build_scale_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection:
        n_p = data.get("n_persons", 0)
        n_a = data.get("n_anime", 0)
        n_c = data.get("n_credits", 0)
        n_scored = len(data.get("pid_iv", {}))

        findings = (
            f"<p>The database contains {n_p:,} persons, {n_a:,} anime works, "
            f"and {n_c:,} credit records. Of these, {n_scored:,} persons "
            f"have IV scores in feat_person_scores.</p>"
        )

        # Stats grid
        findings += '<div class="stats-grid">'
        for label, val in [
            ("Total Persons", _fmt_num(n_p)),
            ("Total Anime", _fmt_num(n_a)),
            ("Total Credits", _fmt_num(n_c)),
            ("Scored Persons", _fmt_num(n_scored)),
        ]:
            findings += (
                f'<div class="stat-card">'
                f'<div class="value">{val}</div>'
                f'<div class="label">{label}</div></div>'
            )
        findings += "</div>"

        # Annual volume chart
        annual = data.get("annual_series", [])
        if annual:
            years = [r["year"] for r in annual]
            n_anime_by_year = [r["n_anime"] for r in annual]
            n_credits_by_year = [r["n_credits"] for r in annual]

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=years, y=n_anime_by_year,
                name="年間作品数", line=dict(color="#f093fb", width=2),
                hovertemplate="%{x}: %{y:,} works<extra></extra>",
            ))
            fig.add_trace(go.Bar(
                x=years, y=n_credits_by_year,
                name="年間クレジット数", marker_color="rgba(160,210,219,0.3)",
                yaxis="y2",
                hovertemplate="%{x}: %{y:,} credits<extra></extra>",
            ))
            fig.update_layout(
                title="年間制作ボリューム",
                xaxis_title="年",
                yaxis=dict(title="年間作品数", side="left"),
                yaxis2=dict(title="年間クレジット数", side="right", overlaying="y"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                barmode="overlay",
            )
            # Add annotation for data reliability boundary
            fig.add_vline(
                x=RELIABLE_MAX_YEAR + 0.5, line_dash="dash",
                line_color="rgba(239,71,111,0.5)",
                annotation_text=f"データ境界（{RELIABLE_MAX_YEAR}）",
                annotation_position="top left",
            )
            viz_html = plotly_div_safe(fig, "chart_annual_volume", height=420)
        else:
            viz_html = '<p style="color:#8a94a0">年次系列データが利用できません。</p>'

        return ReportSection(
            title="業界規模 -- データベースカバレッジ",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "集計は SQLite から直接取得。feat_person_scores は協業グラフの"
                "連結集合に含まれる人物のみを含む。年次系列は anime.year を"
                "放送年として使用している。"
            ),
            section_id="scale",
        )

    # ==================================================================
    # Section 2: Workforce Stock (Chart A)
    # ==================================================================

    def _build_stock_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        pid_fy = data["pid_first_year"]
        pid_ly = data["pid_latest_year"]
        stage_at_year = data["stage_at_year"]
        pid_gender = data["pid_gender"]

        flow_years = list(range(FLOW_START_YEAR, EXIT_CUTOFF_YEAR + 1))
        stage_groups = [d[0] for d in STAGE_GROUPS_DEF]
        sg_color = {d[0]: d[3] for d in STAGE_GROUPS_DEF}

        # Stock by stage group
        stock_by_sg: dict[str, dict[int, int]] = {
            sg: {yr: 0 for yr in flow_years} for sg in stage_groups
        }
        stock_total: dict[int, int] = {yr: 0 for yr in flow_years}

        # Gender-stratified stock
        gender_stock: dict[str, dict[str, dict[int, int]]] = {}
        for g_label in ("F", "M"):
            gender_stock[g_label] = {
                sg: {yr: 0 for yr in flow_years} for sg in stage_groups
            }

        for pid, fy in pid_fy.items():
            ly = pid_ly.get(pid, RELIABLE_MAX_YEAR)
            g = pid_gender.get(pid)
            for yr in flow_years:
                if fy <= yr <= ly:
                    sg = _stage_group(stage_at_year(pid, yr))
                    stock_by_sg[sg][yr] += 1
                    stock_total[yr] += 1
                    if g in gender_stock:
                        gender_stock[g][sg][yr] += 1

        # Build stacked area figure
        fig = go.Figure()
        for sg in reversed(stage_groups):
            c = sg_color[sg]
            fig.add_trace(go.Scatter(
                x=flow_years,
                y=[stock_by_sg[sg].get(yr, 0) for yr in flow_years],
                name=sg, mode="lines", stackgroup="stock",
                line=dict(width=0.5, color=c),
                fillcolor=_hex_rgba(c, 0.6),
                hovertemplate=f"{sg}: %{{y:,}}<extra></extra>",
            ))
        fig.update_layout(
            title=f"A. キャリアランク別 人材ストック（{FLOW_START_YEAR}"
                  f"--{EXIT_CUTOFF_YEAR}）",
            xaxis_title="年", yaxis_title="稼働人数",
            height=480,
        )
        fig.add_annotation(
            text="first_year <= year <= latest_year を満たす人数",
            xref="paper", yref="paper", x=0.0, y=-0.12,
            showarrow=False, font=dict(size=10, color="#a0a0c0"),
        )

        # Gender split tabs
        tabs_html = stratification_tabs(
            "stock_gender", {"all": "全体", "F": "女性", "M": "男性"},
            active="all",
        )
        panels: list[str] = []
        panels.append(strat_panel(
            "stock_gender", "all",
            plotly_div_safe(fig, "chart_stock_all", height=480),
            active=True,
        ))
        for g_label, g_display in [("F", "女性"), ("M", "男性")]:
            fig_g = go.Figure()
            for sg in reversed(stage_groups):
                c = sg_color[sg]
                fig_g.add_trace(go.Scatter(
                    x=flow_years,
                    y=[gender_stock[g_label][sg].get(yr, 0) for yr in flow_years],
                    name=sg, mode="lines", stackgroup="stock",
                    line=dict(width=0.5, color=c),
                    fillcolor=_hex_rgba(c, 0.6),
                    hovertemplate=f"{sg}: %{{y:,}}<extra></extra>",
                ))
            n_g = sum(
                1 for p in pid_fy
                if pid_gender.get(p) == g_label
            )
            fig_g.update_layout(
                title=f"A. 人材ストック -- {g_display}（n={n_g:,}）",
                xaxis_title="年", yaxis_title="稼働人数",
                height=480,
            )
            panels.append(strat_panel(
                "stock_gender", g_label,
                plotly_div_safe(fig_g, f"chart_stock_{g_label}", height=480),
            ))

        viz_html = tabs_html + "\n".join(panels)

        # A-combined: All / Female / Male total lines on one chart
        fig_combined = go.Figure()
        totals_all = [stock_total.get(yr, 0) for yr in flow_years]
        totals_f = [
            sum(gender_stock["F"][sg].get(yr, 0) for sg in stage_groups)
            for yr in flow_years
        ]
        totals_m = [
            sum(gender_stock["M"][sg].get(yr, 0) for sg in stage_groups)
            for yr in flow_years
        ]
        fig_combined.add_trace(go.Scatter(
            x=flow_years, y=totals_all, name="全体",
            mode="lines", line=dict(color="#c0c0d0", width=3),
            hovertemplate="全体: %{y:,}<extra></extra>",
        ))
        fig_combined.add_trace(go.Scatter(
            x=flow_years, y=totals_f, name="女性",
            mode="lines", line=dict(color="#f5576c", width=2),
            hovertemplate="女性: %{y:,}<extra></extra>",
        ))
        fig_combined.add_trace(go.Scatter(
            x=flow_years, y=totals_m, name="男性",
            mode="lines", line=dict(color="#667eea", width=2),
            hovertemplate="男性: %{y:,}<extra></extra>",
        ))
        fig_combined.update_layout(
            title="A（統合）. 稼働人数の推移 -- 全体・女性・男性",
            xaxis_title="年", yaxis_title="稼働人数",
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        )
        viz_html += plotly_div_safe(
            fig_combined, "chart_stock_combined", height=400
        )

        # Composition ratio chart (secondary)
        fig_pct = go.Figure()
        for sg in reversed(stage_groups):
            c = sg_color[sg]
            ratios = []
            for yr in flow_years:
                total = stock_total.get(yr, 0)
                ratios.append(
                    stock_by_sg[sg].get(yr, 0) / total * 100 if total > 0 else 0
                )
            fig_pct.add_trace(go.Scatter(
                x=flow_years, y=ratios,
                name=sg, mode="lines", stackgroup="pct",
                line=dict(width=0.5, color=c),
                fillcolor=_hex_rgba(c, 0.5),
                hovertemplate=f"{sg}: %{{y:.1f}}%<extra></extra>",
            ))
        # Female ratio overlay (secondary y-axis)
        f_ratio = []
        m_ratio = []
        for yr in flow_years:
            total = stock_total.get(yr, 0)
            f_n = sum(gender_stock["F"][sg].get(yr, 0) for sg in stage_groups)
            m_n = sum(gender_stock["M"][sg].get(yr, 0) for sg in stage_groups)
            f_ratio.append(f_n / total * 100 if total > 0 else 0)
            m_ratio.append(m_n / total * 100 if total > 0 else 0)
        fig_pct.add_trace(go.Scatter(
            x=flow_years, y=f_ratio, name="女性比率",
            mode="lines", yaxis="y2",
            line=dict(color="#f5576c", width=2, dash="dash"),
            hovertemplate="女性比率: %{y:.1f}%<extra></extra>",
        ))
        fig_pct.add_trace(go.Scatter(
            x=flow_years, y=m_ratio, name="男性比率",
            mode="lines", yaxis="y2",
            line=dict(color="#667eea", width=2, dash="dash"),
            hovertemplate="男性比率: %{y:.1f}%<extra></extra>",
        ))
        fig_pct.update_layout(
            title="A（構成比）. キャリアランク別構成比（性別比率オーバーレイ）",
            xaxis_title="年",
            yaxis=dict(title="キャリアランク構成比（%）", range=[0, 100]),
            yaxis2=dict(
                title="性別比率（%・点線）",
                overlaying="y", side="right", range=[0, 100],
                showgrid=False,
            ),
            height=420,
        )
        viz_html += plotly_div_safe(fig_pct, "chart_stock_pct", height=420)

        # Findings: latest year stats
        latest_yr = flow_years[-1] if flow_years else EXIT_CUTOFF_YEAR
        latest_total = stock_total.get(latest_yr, 0)
        findings_parts = [
            f"<p>{latest_yr}年の推定稼働人数は "
            f"{latest_total:,} 人。"
        ]
        for sg in stage_groups:
            cnt = stock_by_sg[sg].get(latest_yr, 0)
            pct = cnt / latest_total * 100 if latest_total > 0 else 0
            findings_parts.append(
                f"{sg}: {cnt:,} ({pct:.1f}%). "
            )
        findings_parts.append("</p>")

        # Gender breakdown for latest year
        for g_label, g_name in [("F", "女性"), ("M", "男性")]:
            g_total = sum(
                gender_stock[g_label][sg].get(latest_yr, 0)
                for sg in stage_groups
            )
            findings_parts.append(
                f"<p>{g_name}: {latest_yr}年に {g_total:,} 人が稼働。</p>"
            )

        findings = "".join(findings_parts)

        return ReportSection(
            title="キャリアランク別 人材ストック",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "ある年Yに『稼働中』と見なす条件は first_year <= Y <= latest_year。"
                "各年のキャリアランクは feat_milestones の昇進イベントから推定し、"
                "該当なしの場合は highest_stage をフォールバックとして使用する。"
                "右側打ち切りバイアスを避けるためストック計算は EXIT_CUTOFF_YEAR "
                f"（{EXIT_CUTOFF_YEAR}）で打ち切る。"
            ),
            interpretation_html=(
                "<p>初級ランク中心の構成から、中級・上級を含むバランスの取れた構成"
                "への変化は、業界の成熟（キャリア長期化・定着率向上による中級/上級"
                "比率の増加）と整合的である。別の解釈として、クレジット記録の"
                "生存バイアス（クレジット数の多い人ほどDBに捕捉されやすい）が"
                "長期キャリアを見かけ上押し上げている可能性がある。</p>"
            ),
            section_id="stock",
        )

    # ==================================================================
    # Section 3: Entry/Exit (Chart B)
    # ==================================================================

    def _build_entry_exit_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        pid_fy = data["pid_first_year"]
        pid_ly = data["pid_latest_year"]
        pid_gender = data["pid_gender"]
        stage_at_year = data["stage_at_year"]

        stage_groups = [d[0] for d in STAGE_GROUPS_DEF]
        sg_color = {d[0]: d[3] for d in STAGE_GROUPS_DEF}
        valid_years = list(range(FLOW_START_YEAR, RELIABLE_MAX_YEAR + 1))
        exit_xs = [yr for yr in valid_years if yr <= EXIT_CUTOFF_YEAR]

        # Entry/exit by stage group
        entry_by_sg: dict[str, dict[int, int]] = {sg: {} for sg in stage_groups}
        exit_by_sg: dict[str, dict[int, int]] = {sg: {} for sg in stage_groups}

        # Gender-stratified entry
        entry_by_gender: dict[str, dict[int, int]] = {"F": {}, "M": {}}
        exit_by_gender: dict[str, dict[int, int]] = {"F": {}, "M": {}}

        for pid, fy in pid_fy.items():
            if not (FLOW_START_YEAR <= fy <= RELIABLE_MAX_YEAR):
                continue
            entry_by_sg["初級ランク"][fy] = entry_by_sg["初級ランク"].get(fy, 0) + 1
            g = pid_gender.get(pid)
            if g in entry_by_gender:
                entry_by_gender[g][fy] = entry_by_gender[g].get(fy, 0) + 1

        # Semi-exit counts (3-4yr gap, for 2021-SEMI_EXIT_CUTOFF_YEAR)
        semi_exit_by_sg: dict[str, dict[int, int]] = {sg: {} for sg in stage_groups}

        for pid, ly in pid_ly.items():
            if FLOW_START_YEAR <= ly <= EXIT_CUTOFF_YEAR:
                # True exit (5+ years)
                sg = _stage_group(stage_at_year(pid, ly))
                exit_by_sg[sg][ly] = exit_by_sg[sg].get(ly, 0) + 1
                g = pid_gender.get(pid)
                if g in exit_by_gender:
                    exit_by_gender[g][ly] = exit_by_gender[g].get(ly, 0) + 1
            elif EXIT_CUTOFF_YEAR < ly <= SEMI_EXIT_CUTOFF_YEAR:
                # Semi-exit (3-4yr gap — may return)
                sg = _stage_group(stage_at_year(pid, ly))
                semi_exit_by_sg[sg][ly] = semi_exit_by_sg[sg].get(ly, 0) + 1

        semi_exit_xs = [yr for yr in valid_years
                        if EXIT_CUTOFF_YEAR < yr <= SEMI_EXIT_CUTOFF_YEAR]

        # Build chart B
        fig_b = go.Figure()
        # Exit stacked by stage
        for sg in stage_groups:
            c = sg_color[sg]
            fig_b.add_trace(go.Scatter(
                x=exit_xs,
                y=[exit_by_sg[sg].get(yr, 0) for yr in exit_xs],
                name=f"Exit: {sg}", mode="lines",
                line=dict(width=0.5, color=c),
                fillcolor=_hex_rgba(c, 0.65),
                stackgroup="exit",
                hovertemplate=f"{sg} exit %{{x}}: %{{y:,}}<extra></extra>",
            ))
        # Semi-exit dashed overlay (2021-2022)
        if semi_exit_xs:
            semi_total_by_yr = {
                yr: sum(semi_exit_by_sg[sg].get(yr, 0) for sg in stage_groups)
                for yr in semi_exit_xs
            }
            fig_b.add_trace(go.Scatter(
                x=semi_exit_xs,
                y=[semi_total_by_yr[yr] for yr in semi_exit_xs],
                name="準退職（3–4年ギャップ）",
                mode="lines+markers",
                line=dict(color="#FFD166", width=2.5, dash="dash"),
                marker=dict(size=7, symbol="diamond"),
                hovertemplate="Semi-exit %{x}: %{y:,} (may return)<extra></extra>",
            ))
        # Entry line
        fig_b.add_trace(go.Scatter(
            x=valid_years,
            y=[entry_by_sg["初級ランク"].get(yr, 0) for yr in valid_years],
            name="参入（全員初級ランクとして）", mode="lines",
            line=dict(color="#FFFFFF", width=3),
            hovertemplate="Entry %{x}: %{y:,}<extra></extra>",
        ))
        fig_b.add_vrect(
            x0=SEMI_EXIT_CUTOFF_YEAR + 0.5, x1=RELIABLE_MAX_YEAR,
            fillcolor="rgba(239,71,111,0.06)",
            line_color="rgba(239,71,111,0.4)", line_dash="dash",
            annotation_text=f"データなし（{SEMI_EXIT_CUTOFF_YEAR + 1}+）",
            annotation_position="top right",
        )
        # Semi-exit zone annotation
        fig_b.add_vrect(
            x0=EXIT_CUTOFF_YEAR + 0.5, x1=SEMI_EXIT_CUTOFF_YEAR + 0.5,
            fillcolor="rgba(255,209,102,0.08)",
            line_color="rgba(255,209,102,0.3)", line_dash="dot",
            annotation_text="準退職 zone",
            annotation_position="top left",
        )
        fig_b.update_layout(
            title=f"B. キャリアランク別 参入 vs 退職 "
                  f"（退職 ≤{EXIT_CUTOFF_YEAR}、準退職 {EXIT_CUTOFF_YEAR+1}–{SEMI_EXIT_CUTOFF_YEAR}）",
            height=520,
            xaxis_title="年", yaxis_title="人数",
        )

        # Gender tabs
        tabs_html = stratification_tabs(
            "entry_exit_gender",
            {"all": "全体", "F": "女性", "M": "男性"},
            active="all",
        )
        panels_b: list[str] = []
        panels_b.append(strat_panel(
            "entry_exit_gender", "all",
            plotly_div_safe(fig_b, "chart_b_all", height=520),
            active=True,
        ))
        for g_label, g_name in [("F", "女性"), ("M", "男性")]:
            fig_g = go.Figure()
            fig_g.add_trace(go.Scatter(
                x=valid_years,
                y=[entry_by_gender[g_label].get(yr, 0) for yr in valid_years],
                name=f"参入（{g_name}）", mode="lines",
                line=dict(color="#06D6A0", width=2),
                hovertemplate="Entry %{x}: %{y:,}<extra></extra>",
            ))
            fig_g.add_trace(go.Scatter(
                x=exit_xs,
                y=[exit_by_gender[g_label].get(yr, 0) for yr in exit_xs],
                name=f"退職（{g_name}）", mode="lines",
                line=dict(color="#EF476F", width=2, dash="dot"),
                hovertemplate="Exit %{x}: %{y:,}<extra></extra>",
            ))
            fig_g.update_layout(
                title=f"B. 参入 vs 退職 -- {g_name}",
                height=420, xaxis_title="年", yaxis_title="人数",
            )
            panels_b.append(strat_panel(
                "entry_exit_gender", g_label,
                plotly_div_safe(fig_g, f"chart_b_{g_label}", height=420),
            ))

        viz_html = tabs_html + "\n".join(panels_b)

        # Experience-based exit chart (B-2 equivalent)
        exit_by_dur: dict[str, dict[int, int]] = {d[0]: {} for d in EXP_GROUPS_DEF}
        for pid, ly in pid_ly.items():
            if not (FLOW_START_YEAR <= ly <= EXIT_CUTOFF_YEAR):
                continue
            fy = pid_fy.get(pid)
            if not fy:
                continue
            dur = ly - fy
            eg = _exp_group(dur)
            exit_by_dur[eg][ly] = exit_by_dur[eg].get(ly, 0) + 1

        fig_b2 = go.Figure()
        for label, _lo, _hi, color in EXP_GROUPS_DEF:
            fig_b2.add_trace(go.Scatter(
                x=exit_xs,
                y=[exit_by_dur[label].get(yr, 0) for yr in exit_xs],
                name=f"退職: {label}", mode="lines",
                line=dict(width=0.5, color=color),
                fillcolor=_hex_rgba(color, 0.65),
                stackgroup="exit_dur",
                hovertemplate=f"{label} exit %{{x}}: %{{y:,}}<extra></extra>",
            ))
        fig_b2.add_trace(go.Scatter(
            x=valid_years,
            y=[entry_by_sg["初級ランク"].get(yr, 0) for yr in valid_years],
            name="参入", mode="lines",
            line=dict(color="#FFFFFF", width=3),
            hovertemplate="Entry %{x}: %{y:,}<extra></extra>",
        ))
        fig_b2.update_layout(
            title=f"B-2. キャリア継続年数別 退職数 "
                  f"（積み上げ、{EXIT_CUTOFF_YEAR}年まで）",
            height=480, xaxis_title="年", yaxis_title="人数",
        )
        viz_html += plotly_div_safe(fig_b2, "chart_b2_dur", height=480)

        # B-3: same chart but excluding dur==0 (退職1年目)
        fig_b3 = go.Figure()
        for label, lo, _hi, color in EXP_GROUPS_DEF:
            if lo == 0:
                continue
            fig_b3.add_trace(go.Scatter(
                x=exit_xs,
                y=[exit_by_dur[label].get(yr, 0) for yr in exit_xs],
                name=f"退職: {label}", mode="lines",
                line=dict(width=0.5, color=color),
                fillcolor=_hex_rgba(color, 0.65),
                stackgroup="exit_dur_no1",
                hovertemplate=f"{label} exit %{{x}}: %{{y:,}}<extra></extra>",
            ))
        fig_b3.add_trace(go.Scatter(
            x=valid_years,
            y=[entry_by_sg["初級ランク"].get(yr, 0) for yr in valid_years],
            name="参入", mode="lines",
            line=dict(color="#FFFFFF", width=3),
            hovertemplate="Entry %{x}: %{y:,}<extra></extra>",
        ))
        fig_b3.add_trace(go.Scatter(
            x=valid_years,
            y=[
                entry_by_sg["初級ランク"].get(yr, 0)
                - exit_by_dur["1年目"].get(yr, 0)
                for yr in valid_years
            ],
            name="参入（1年以内退職を除く）", mode="lines",
            line=dict(color="#FFFFFF", width=2, dash="dot"),
            hovertemplate="Entry(ex-1yr) %{x}: %{y:,}<extra></extra>",
        ))
        fig_b3.update_layout(
            title=f"B-3. キャリア継続年数別 退職数（退職1年目を除く、"
                  f"{EXIT_CUTOFF_YEAR}年まで）",
            height=480, xaxis_title="年", yaxis_title="人数",
        )
        viz_html += plotly_div_safe(fig_b3, "chart_b3_dur_no1y", height=480)

        # Findings
        total_entry = sum(entry_by_sg["初級ランク"].values())
        total_exit = sum(
            sum(d.values()) for d in exit_by_sg.values()
        )
        total_semi = sum(
            sum(d.values()) for d in semi_exit_by_sg.values()
        )
        findings = (
            f"<p>{FLOW_START_YEAR}〜{RELIABLE_MAX_YEAR}年の間に "
            f"{total_entry:,} 人が業界に参入した。"
            f"{FLOW_START_YEAR}〜{EXIT_CUTOFF_YEAR}年の間に "
            f"{total_exit:,} 人が退職（5年以上クレジットなし）した。"
            f"加えて {total_semi:,} 人が準退職状態にある"
            f"（最終クレジット {EXIT_CUTOFF_YEAR+1}〜{SEMI_EXIT_CUTOFF_YEAR}年、"
            f"3〜4年ギャップ — 復帰の可能性あり）。</p>"
            f"<p>参入の性別内訳: 女性 {sum(entry_by_gender['F'].values()):,} 人、"
            f"男性 {sum(entry_by_gender['M'].values()):,} 人"
            f"（残りは性別不明）。</p>"
        )

        return ReportSection(
            title="キャリアランク別 参入・退職率",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "参入 = 初クレジット年。"
                f"退職 = 最終クレジット年 ≤ {EXIT_CUTOFF_YEAR}（5年以上クレジットなし）。"
                f"準退職 = 最終クレジット年が {EXIT_CUTOFF_YEAR+1}〜{SEMI_EXIT_CUTOFF_YEAR}"
                "（3〜4年ギャップ; 復帰の可能性あり — 破線で表示）。"
                "参入者は全員『初級ランク』に分類され、退職時のキャリアランクは退職年の "
                "milestones から推定する。性別は persons.gender を使用し、"
                "NULL 値は性別パネルから除外する。"
            ),
            interpretation_html=(
                "<p>近年の参入数と退職数のギャップ拡大は業界成長と整合的である。"
                "別の解釈: 2000年以降のクレジット記録カバレッジの向上が、"
                "クレジットデータが疎な過年度に対して参入数の見かけを押し上げて"
                "いる可能性がある。</p>"
            ),
            section_id="entry_exit",
        )

    # ==================================================================
    # Section 4: Career Milestones (Chart C)
    # ==================================================================

    def _build_milestones_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        pid_fy = data["pid_first_year"]

        # Debut season by era panels
        try:
            rows = self.conn.execute("""
                SELECT a.season, a.year, c.person_id,
                       CASE
                           WHEN a.episodes IS NULL OR a.episodes = 0 THEN 'unknown'
                           WHEN a.episodes <= 1 THEN 'movie_or_special'
                           WHEN a.episodes <= 14 THEN 'single_cour'
                           WHEN a.episodes <= 28 THEN 'multi_cour'
                           ELSE 'long_cour'
                       END AS cour_type
                FROM anime a
                JOIN credits c ON c.anime_id = a.id
                WHERE a.year BETWEEN 1990 AND ?
                  AND a.season IN ('winter','spring','summer','fall')
            """, (RELIABLE_MAX_YEAR,)).fetchall()
        except Exception:
            return None

        season_names = ["winter", "spring", "summer", "fall"]
        season_labels = [
            "Winter (1-3)", "Spring (4-6)", "Summer (7-9)", "Fall (10-12)",
        ]
        cour_type_order = ["single_cour", "multi_cour", "long_cour", "movie_or_special"]
        cour_labels = {
            "single_cour": "Single cour (<=14ep)",
            "multi_cour": "Multi cour (15-28ep)",
            "long_cour": "Long cour (29+ep)",
            "movie_or_special": "Movie/special (<=1ep)",
        }
        cour_colors = {
            "single_cour": "#a0d2db",
            "multi_cour": "#06D6A0",
            "long_cour": "#f093fb",
            "movie_or_special": "#fda085",
        }

        decades = [1990, 2000, 2010, 2020]
        debut_by_decade: dict[int, dict[str, dict[str, int]]] = {
            dec: {s: {ct: 0 for ct in cour_type_order} for s in season_names}
            for dec in decades
        }
        debut_seen: set[tuple[str, int, str, str]] = set()

        for row in rows:
            pid = row["person_id"]
            yr = row["year"]
            s = row["season"]
            ct = row["cour_type"]
            if not pid or not yr or s not in season_names:
                continue
            if pid_fy.get(pid) != yr:
                continue
            dec = (yr // 10) * 10
            if dec not in debut_by_decade:
                continue
            key = (pid, dec, s, ct)
            if key in debut_seen:
                continue
            debut_seen.add(key)
            if ct in debut_by_decade[dec][s]:
                debut_by_decade[dec][s][ct] += 1

        fig_c = make_subplots(
            rows=2, cols=2,
            subplot_titles=[f"{d}s" for d in decades],
            vertical_spacing=0.18, horizontal_spacing=0.10,
        )
        positions = [(1, 1), (1, 2), (2, 1), (2, 2)]
        for dec, (r, c) in zip(decades, positions):
            for ct in cour_type_order:
                fig_c.add_trace(go.Bar(
                    x=season_labels,
                    y=[debut_by_decade[dec][sn][ct] for sn in season_names],
                    name=cour_labels[ct],
                    marker_color=cour_colors[ct],
                    showlegend=(dec == decades[0]),
                    legendgroup=ct,
                    hovertemplate=f"{cour_labels[ct]}: %{{y:,}}<extra></extra>",
                ), row=r, col=c)
        fig_c.update_layout(
            barmode="group",
            title="C. 年代・季節別 デビュー人数",
            height=600,
        )

        viz_html = plotly_div_safe(fig_c, "chart_c_debut", height=600)

        total_debuts = len(debut_seen)
        findings = (
            f"<p>{len(decades)} 年代パネルにわたるデビューパターン: "
            f"{total_debuts:,} 件のデビューイベント（人物 × 年代 × 季節 × "
            f"cour_type の組み合わせごとに一意）。真の新規参入者とは "
            f"first_year が該当年度のクレジット年と一致する人物を指す。</p>"
        )

        return ReportSection(
            title="キャリア節目 -- 年代別デビュー季節",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "真の新規参入者 = feat_career.first_year がクレジット年と一致する人物。"
                "年代 × 季節 × cour_type の組み合わせごとに1人1回カウント。"
                "cour_type はエピソード数の閾値から判定。"
            ),
            section_id="milestones",
        )

    # ==================================================================
    # Section 5: 4-Tier Classification (Chart D)
    # ==================================================================

    def _build_four_tier_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        pid_iv = data["pid_iv"]
        pid_fy = data["pid_first_year"]
        pid_ly = data["pid_latest_year"]
        pid_gender = data["pid_gender"]
        pid_total_credits = data["pid_total_credits"]

        if len(pid_iv) < 50:
            return None

        exp_tier_names = [t[0] for t in _EXP_TIER_DEFS]
        exp_tier_color = {t[0]: t[1] for t in _EXP_TIER_DEFS}

        # Compute expected ability from DB
        exp_pid_expected: dict[str, float] = {}
        exp_pid_actual: dict[str, float] = {}
        exp_pid_tier: dict[str, str] = {}
        exp_tier_sizes: dict[str, int] = {t: 0 for t in exp_tier_names}
        computation_ok = False

        try:
            raw_rows = self.conn.execute("""
                SELECT c.anime_id, c.person_id
                FROM credits c JOIN anime a ON c.anime_id = a.id
                WHERE a.year BETWEEN 1980 AND 2025
            """).fetchall()

            # Group by anime -> avg collaborator iv
            anime_pids: dict[str, list[str]] = {}
            for row in raw_rows:
                anime_pids.setdefault(row["anime_id"], []).append(row["person_id"])

            avg_collab_iv: dict[str, float] = {}
            for aid, pids in anime_pids.items():
                ivs = [pid_iv.get(p, 0.0) for p in pids]
                if ivs:
                    avg_collab_iv[aid] = statistics.mean(ivs)

            # Per-person aggregation
            pid_animes: dict[str, list[str]] = {}
            for aid, pids in anime_pids.items():
                for p in pids:
                    pid_animes.setdefault(p, []).append(aid)

            person_collab_iv: dict[str, float] = {}
            for p, animes in pid_animes.items():
                civs = [avg_collab_iv.get(a, 0.0) for a in animes]
                if civs:
                    person_collab_iv[p] = statistics.mean(civs)

            # Normalize
            collab_max = max(person_collab_iv.values(), default=1.0) or 1.0

            exp_raw: dict[str, float] = {}
            for p in pid_iv:
                cv = person_collab_iv.get(p, 0.0) / collab_max
                exp_raw[p] = cv

            # Percentile rank
            exp_sorted = sorted(exp_raw.values())
            exp_n = max(len(exp_sorted) - 1, 1)
            for p, v in exp_raw.items():
                idx = bisect.bisect_left(exp_sorted, v)
                exp_pid_expected[p] = (idx / exp_n) * 100.0

            act_sorted = sorted(pid_iv.values())
            act_n = max(len(act_sorted) - 1, 1)
            for p, v in pid_iv.items():
                idx = bisect.bisect_left(act_sorted, v)
                exp_pid_actual[p] = (idx / act_n) * 100.0

            # Assign groups
            for p in pid_iv:
                hi_exp = exp_pid_expected.get(p, 0.0) >= _EXP_HIGH_PCTILE
                hi_act = exp_pid_actual.get(p, 0.0) >= _EXP_HIGH_PCTILE
                t = (
                    "高期待・高実績群" if hi_exp and hi_act else
                    "高期待・低実績群" if hi_exp else
                    "低期待・高実績群" if hi_act else
                    "低期待・低実績群"
                )
                exp_pid_tier[p] = t
                exp_tier_sizes[t] += 1
            computation_ok = True

        except Exception:
            # Fallback: 2-group only
            iv_sorted = sorted(pid_iv.values())
            thr = iv_sorted[int(len(iv_sorted) * 0.70)] if iv_sorted else 0.0
            for p, v in pid_iv.items():
                t = "高期待・高実績群" if v >= thr else "低期待・低実績群"
                exp_pid_tier[p] = t
                exp_tier_sizes[t] += 1

        # Entry/exit by tier
        valid_years = list(range(FLOW_START_YEAR, RELIABLE_MAX_YEAR + 1))
        exit_xs = [yr for yr in valid_years if yr <= EXIT_CUTOFF_YEAR]

        entry_by_tier: dict[str, dict[int, int]] = {t: {} for t in exp_tier_names}
        exit_by_tier: dict[str, dict[int, int]] = {t: {} for t in exp_tier_names}

        for p, fy in pid_fy.items():
            if FLOW_START_YEAR <= fy <= RELIABLE_MAX_YEAR:
                t = exp_pid_tier.get(p, "標準")
                entry_by_tier[t][fy] = entry_by_tier[t].get(fy, 0) + 1
        for p, ly in pid_ly.items():
            if FLOW_START_YEAR <= ly <= EXIT_CUTOFF_YEAR:
                t = exp_pid_tier.get(p, "標準")
                exit_by_tier[t][ly] = exit_by_tier[t].get(ly, 0) + 1

        # D-1: entry/exit per tier
        fig_d = make_subplots(
            rows=2, cols=1,
            subplot_titles=(
                "D-1. 群別 参入（実線）/ 翌年クレジット可視性喪失（点線）",
                "D-2. 協業者 IV 平均 vs 個人 IV パーセンタイル 分布",
            ),
            vertical_spacing=0.14,
            row_heights=[0.5, 0.5],
        )
        for t, tc in exp_tier_color.items():
            fig_d.add_trace(go.Scatter(
                x=valid_years,
                y=[entry_by_tier[t].get(yr, 0) for yr in valid_years],
                name=f"{t} 参入", mode="lines",
                line=dict(color=tc, width=2), legendgroup=t,
                hovertemplate=f"{t} entry %{{x}}: %{{y:,}}<extra></extra>",
            ), row=1, col=1)
            fig_d.add_trace(go.Scatter(
                x=exit_xs,
                y=[exit_by_tier[t].get(yr, 0) for yr in exit_xs],
                name=f"{t} 退職", mode="lines",
                line=dict(color=tc, width=2, dash="dot"),
                legendgroup=t, showlegend=False,
                hovertemplate=f"{t} exit %{{x}}: %{{y:,}}<extra></extra>",
            ), row=1, col=1)

        # D-2: scatter expected vs actual
        if computation_ok:
            scatter_pids = list(exp_pid_tier.keys())
            if len(scatter_pids) > 500:
                random.seed(42)
                scatter_pids = random.sample(scatter_pids, 500)
            for t, tc in exp_tier_color.items():
                pids_t = [p for p in scatter_pids if exp_pid_tier.get(p) == t]
                if not pids_t:
                    continue
                fig_d.add_trace(go.Scatter(
                    x=[exp_pid_expected.get(p, 0.0) for p in pids_t],
                    y=[exp_pid_actual.get(p, 0.0) for p in pids_t],
                    mode="markers", name=t, legendgroup=t, showlegend=False,
                    marker=dict(
                        color=tc,
                        size=[
                            max(4, min(20, int(
                                (pid_total_credits.get(p, 1) or 1) ** 0.4
                            )))
                            for p in pids_t
                        ],
                        opacity=0.55, line=dict(width=0),
                    ),
                    hovertemplate=(
                        f"{t}<br>"
                        "協業者IV平均: %{x:.1f}パーセンタイル<br>"
                        "個人IV: %{y:.1f}パーセンタイル<extra></extra>"
                    ),
                ), row=2, col=1)
            fig_d.add_hline(
                y=_EXP_HIGH_PCTILE, line_dash="dash",
                line_color="rgba(255,255,255,0.25)", row=2, col=1,
            )
            fig_d.add_vline(
                x=_EXP_HIGH_PCTILE, line_dash="dash",
                line_color="rgba(255,255,255,0.25)", row=2, col=1,
            )

        fig_d.update_xaxes(title_text="年", row=1, col=1)
        fig_d.update_yaxes(title_text="人数", row=1, col=1)
        fig_d.update_xaxes(
            title_text="協業者 IV 平均（パーセンタイル）", row=2, col=1,
        )
        fig_d.update_yaxes(
            title_text="個人 IV スコア（パーセンタイル）", row=2, col=1,
        )
        fig_d.update_layout(
            title="D. 協業者 IV 平均 × 個人 IV パーセンタイル 4群分類",
            height=820,
        )

        viz_html = plotly_div_safe(fig_d, "chart_d_tiers", height=820)

        # Gender stratification table for Chart D
        gender_tier_counts: dict[str, dict[str, int]] = {"F": {}, "M": {}}
        for p, t in exp_pid_tier.items():
            g = pid_gender.get(p)
            if g in gender_tier_counts:
                gender_tier_counts[g][t] = gender_tier_counts[g].get(t, 0) + 1

        gender_table = (
            '<table style="width:100%;font-size:0.85rem;margin:1rem 0;">'
            "<thead><tr><th>群</th><th>合計</th>"
            "<th>女性</th><th>男性</th></tr></thead><tbody>"
        )
        exp_total = sum(exp_tier_sizes.values()) or 1
        for t in exp_tier_names:
            cnt = exp_tier_sizes.get(t, 0)
            f_cnt = gender_tier_counts["F"].get(t, 0)
            m_cnt = gender_tier_counts["M"].get(t, 0)
            gender_table += (
                f'<tr><td style="color:{exp_tier_color[t]};font-weight:bold">'
                f'{t}</td>'
                f"<td>{cnt:,} ({cnt / exp_total * 100:.1f}%)</td>"
                f"<td>{f_cnt:,}</td>"
                f"<td>{m_cnt:,}</td></tr>"
            )
        gender_table += "</tbody></table>"

        findings = (
            f"<p>協業者 IV 平均（ネットワーク位置の代理変数）× 個人 IV パーセンタイル"
            f"に基づく 4 群分類、閾値は両軸とも {_EXP_HIGH_PCTILE:.0f} パーセンタイル。"
            f"本分類は構造的ネットワーク位置の記述であり、個人評価を目的としない。</p>"
            f"{gender_table}"
        )

        return ReportSection(
            title="協業者 IV 平均 × 個人 IV パーセンタイル -- 4 群分類",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "X 軸 = 協業者の IV 平均（ネットワーク上の位置の代理）。"
                "Y 軸 = 個人 IV スコアのパーセンタイル。閾値: "
                f"両軸とも {_EXP_HIGH_PCTILE:.0f} パーセンタイル。"
                "全構成要素は構造的データのみ（視聴者評価は使用しない）。"
                "散布図は500人のサンプル。バブルサイズ = total_credits^0.4。"
            ),
            interpretation_html=(
                "<p>『高期待・低実績群』象限は、高IV協業者ネットワーク上に位置しながら"
                "個人 IV スコアのパーセンタイルが相対的に低い集団を指す。"
                "キャリア初期でクレジット累積が少ない人物を含み得る。"
                "別の解釈: 同類選好（assortative mixing）により、"
                "高IV環境では周辺的な参加者も機械的にネットワーク位置上位へと分類される可能性がある。"
                "このラベルは個人評価を示すものではなく、ネットワーク位置と IV パーセンタイルの"
                "相対関係のみを記述する。</p>"
            ),
            section_id="four_group_network_vs_iv",
        )

    # ==================================================================
    # Section 6: Role-specific trends (Chart E)
    # ==================================================================

    def _build_role_trends_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        pid_fy = data["pid_first_year"]
        pid_ly = data["pid_latest_year"]
        pid_rt = data["pid_role_type"]

        valid_years = list(range(FLOW_START_YEAR, RELIABLE_MAX_YEAR + 1))
        exit_xs = [yr for yr in valid_years if yr <= EXIT_CUTOFF_YEAR]

        entry_by_role: dict[str, dict[int, int]] = {rt: {} for rt in ROLE_TYPE_DEF}
        exit_by_role: dict[str, dict[int, int]] = {rt: {} for rt in ROLE_TYPE_DEF}

        for pid, fy in pid_fy.items():
            if FLOW_START_YEAR <= fy <= RELIABLE_MAX_YEAR:
                rt = pid_rt.get(pid, "other")
                entry_by_role[rt][fy] = entry_by_role[rt].get(fy, 0) + 1

        for pid, ly in pid_ly.items():
            if FLOW_START_YEAR <= ly <= EXIT_CUTOFF_YEAR:
                rt = pid_rt.get(pid, "other")
                exit_by_role[rt][ly] = exit_by_role[rt].get(ly, 0) + 1

        fig_e = make_subplots(
            rows=2, cols=1,
            subplot_titles=(
                "役職別 参入（年次）",
                f"役職別 退職（年次、{EXIT_CUTOFF_YEAR}年まで）",
            ),
            vertical_spacing=0.14, shared_xaxes=True,
        )
        for rt, (rl, rc) in ROLE_TYPE_DEF.items():
            fig_e.add_trace(go.Scatter(
                x=valid_years,
                y=[entry_by_role[rt].get(yr, 0) for yr in valid_years],
                name=rl, mode="lines",
                line=dict(color=rc, width=2),
                legendgroup=rl,
                hovertemplate=f"{rl} entry %{{x}}: %{{y:,}}<extra></extra>",
            ), row=1, col=1)
            fig_e.add_trace(go.Scatter(
                x=exit_xs,
                y=[exit_by_role[rt].get(yr, 0) for yr in exit_xs],
                name=rl, mode="lines",
                line=dict(color=rc, width=2, dash="dot"),
                legendgroup=rl, showlegend=False,
                hovertemplate=f"{rl} exit %{{x}}: %{{y:,}}<extra></extra>",
            ), row=2, col=1)

        fig_e.update_layout(
            title="E. 役職別 年次参入/退職トレンド",
            height=580,
        )
        fig_e.update_yaxes(title_text="参入数", row=1, col=1)
        fig_e.update_yaxes(title_text="退職数", row=2, col=1)
        fig_e.update_xaxes(title_text="年", row=2, col=1)

        viz_html = plotly_div_safe(fig_e, "chart_e_role", height=580)

        findings = (
            "<p>役職別の年次参入・退職数。"
            "アニメーター（動画/原画）が件数で最大カテゴリを占める。</p>"
        )

        return ReportSection(
            title="役職別 年次参入/退職トレンド",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "主要役職は feat_person_scores.primary_role から取得。"
                "役職は7カテゴリにマップされる。参入 = 初クレジット年、"
                f"退職 = 最終クレジット年 <= {EXIT_CUTOFF_YEAR}。"
            ),
            section_id="role_trends",
        )

    # ==================================================================
    # Section 7: Value Flow (Charts F1-F3)
    # ==================================================================

    def _build_value_flow_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        pid_iv = data["pid_iv"]
        pid_fy = data["pid_first_year"]
        pid_ly = data["pid_latest_year"]
        pid_trend = data["pid_trend"]
        stage_at_year = data["stage_at_year"]
        pid_rt = data["pid_role_type"]
        high_iv = data["high_iv_threshold"]

        valid_years = list(range(FLOW_START_YEAR, RELIABLE_MAX_YEAR + 1))

        # F1: Value flow (area)
        lost_val: dict[int, float] = {}
        growth_val: dict[int, float] = {}
        entry_val: dict[int, float] = {}
        for pid, ly in pid_ly.items():
            if FLOW_START_YEAR <= ly <= EXIT_CUTOFF_YEAR:
                lost_val[ly] = lost_val.get(ly, 0.0) + pid_iv.get(pid, 0.0)
        for pid, trend in pid_trend.items():
            ly = pid_ly.get(pid)
            if ly and FLOW_START_YEAR <= ly <= RELIABLE_MAX_YEAR and trend == "rising":
                growth_val[ly] = growth_val.get(ly, 0.0) + pid_iv.get(pid, 0.0)
        for pid, fy in pid_fy.items():
            if FLOW_START_YEAR <= fy <= RELIABLE_MAX_YEAR:
                entry_val[fy] = entry_val.get(fy, 0.0) + pid_iv.get(pid, 0.0)

        fig_f1 = go.Figure()
        for vals, name, color, fill in [
            (lost_val, "Lost value (exits)", "#EF476F", "rgba(239,71,111,0.25)"),
            (growth_val, "Growth value (rising trend)", "#06D6A0",
             "rgba(6,214,160,0.25)"),
            (entry_val, "Entry value (new entrants)", "#a0d2db",
             "rgba(160,210,219,0.25)"),
        ]:
            fig_f1.add_trace(go.Scatter(
                x=valid_years,
                y=[vals.get(yr, 0.0) for yr in valid_years],
                name=name, mode="lines", fill="tozeroy",
                line=dict(color=color, width=1), fillcolor=fill,
                hovertemplate=f"{name} %{{x}}: %{{y:.1f}}<extra></extra>",
            ))
        fig_f1.update_layout(
            title="F-1. IV Score 価値フロー（年間合計）",
            xaxis_title="年", yaxis_title="IV Score 合計",
        )

        # F2: Loss type breakdown
        loss_by_type_yr: dict[str, dict[int, int]] = {t: {} for t in LOSS_TYPES}
        loss_by_role_total: dict[str, int] = {}
        for pid, ly in pid_ly.items():
            if not (FLOW_START_YEAR <= ly <= EXIT_CUTOFF_YEAR):
                continue
            iv = pid_iv.get(pid, 0.0)
            st = stage_at_year(pid, ly)
            lt = (
                "エース離脱" if iv > high_iv else
                "上級ランク引退" if st >= 5 else
                "中級ランク離脱" if st >= 3 else
                "初級ランク早期離脱"
            )
            loss_by_type_yr[lt][ly] = loss_by_type_yr[lt].get(ly, 0) + 1
            rt = pid_rt.get(pid, "other")
            loss_by_role_total[rt] = loss_by_role_total.get(rt, 0) + 1

        lt_years = sorted({
            yr for lt in loss_by_type_yr.values() for yr in lt
            if FLOW_START_YEAR <= yr <= EXIT_CUTOFF_YEAR
        })
        fig_f2 = go.Figure()
        for lt in LOSS_TYPES:
            fig_f2.add_trace(go.Bar(
                x=lt_years,
                y=[loss_by_type_yr[lt].get(yr, 0) for yr in lt_years],
                name=lt, marker_color=LOSS_TYPE_COLORS[lt],
                hovertemplate=f"{lt} %{{x}}: %{{y:,}}<extra></extra>",
            ))
        fig_f2.update_layout(
            barmode="stack",
            title=f"F-2. 退職タイプ別内訳（{EXIT_CUTOFF_YEAR}年まで）",
            xaxis_title="年", yaxis_title="退職数",
        )

        # F3: Role composition pie
        rt_ord = sorted(
            loss_by_role_total, key=lambda k: loss_by_role_total[k], reverse=True,
        )
        fig_f3 = go.Figure(go.Pie(
            labels=[ROLE_TYPE_DEF.get(rt, (rt, "#606070"))[0] for rt in rt_ord],
            values=[loss_by_role_total[rt] for rt in rt_ord],
            marker_colors=[
                ROLE_TYPE_DEF.get(rt, ("", "#606070"))[1] for rt in rt_ord
            ],
            hole=0.4, textinfo="label+percent",
            hovertemplate="%{label}: %{value:,}<extra></extra>",
        ))
        fig_f3.update_layout(
            title="F-3. 退職者の役職構成（全期間）",
            height=360, legend=dict(orientation="h"),
        )

        viz_html = (
            plotly_div_safe(fig_f1, "chart_f1_value", height=400)
            + plotly_div_safe(fig_f2, "chart_f2_loss", height=400)
            + plotly_div_safe(fig_f3, "chart_f3_role", height=360)
        )

        total_lost = sum(sum(d.values()) for d in loss_by_type_yr.values())
        findings = (
            f"<p>{FLOW_START_YEAR}〜{EXIT_CUTOFF_YEAR}年の間に "
            f"{total_lost:,} 人が退職し、内訳は以下の通り: "
        )
        for lt in LOSS_TYPES:
            cnt = sum(loss_by_type_yr[lt].values())
            findings += f"{lt} {cnt:,}, "
        findings = findings.rstrip(", ") + ".</p>"

        return ReportSection(
            title="価値フローと損失タイプ別内訳",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "IV Score 合計はパイプライン実行時点のネットワーク位置を反映する"
                "スナップショット値である。年次変動は人材フローだけでなく"
                "ネットワーク成長も部分的に反映する。"
                f"損失タイプ: エース離脱（IV > p90 = {high_iv:.4f}）、"
                "上級ランク引退（stage >= 5）、中級ランク離脱（stage 3-4）、"
                "初級ランク早期離脱（stage <= 2）。"
            ),
            section_id="value_flow",
        )

    # ==================================================================
    # Section 8: Studio Flow (Chart G)
    # ==================================================================

    def _build_studio_flow_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        try:
            rows = self.conn.execute("""
                SELECT je.value AS studio, a.year, c.person_id
                FROM anime a
                JOIN credits c ON c.anime_id = a.id,
                     json_each(
                         CASE WHEN json_valid(a.studios) THEN a.studios
                              ELSE '[]' END
                     ) AS je
                WHERE a.year BETWEEN ? AND ?
                  AND je.value != '' AND je.value IS NOT NULL
            """, (FLOW_START_YEAR, RELIABLE_MAX_YEAR)).fetchall()
        except Exception:
            return None

        studio_person_years: dict[str, dict[int, set[str]]] = {}
        for row in rows:
            st = str(row["studio"]).strip()
            yr = row["year"]
            pid = row["person_id"]
            if not st or not yr or not pid:
                continue
            studio_person_years.setdefault(st, {}).setdefault(yr, set()).add(pid)

        studio_total = {
            s: sum(len(v) for v in yrs.values())
            for s, yrs in studio_person_years.items()
        }
        top_studios = sorted(
            studio_total, key=lambda k: studio_total[k], reverse=True,
        )[:8]

        if not top_studios:
            return None

        palette = [
            "#f093fb", "#a0d2db", "#06D6A0", "#FFD166",
            "#fda085", "#667eea", "#EF476F", "#90BE6D",
        ]
        studio_yrs = sorted({
            yr for s in top_studios for yr in studio_person_years.get(s, {})
            if FLOW_START_YEAR <= yr <= EXIT_CUTOFF_YEAR
        })

        fig_g = go.Figure()
        for i, studio in enumerate(top_studios):
            sy = studio_person_years.get(studio, {})
            fig_g.add_trace(go.Scatter(
                x=studio_yrs,
                y=[len(sy.get(yr, set())) for yr in studio_yrs],
                name=studio[:28], mode="lines",
                line=dict(color=palette[i % len(palette)], width=2),
                hovertemplate=f"{studio[:28]}: %{{y:,}}<extra></extra>",
            ))
        fig_g.update_layout(
            title=f"G. 上位8スタジオ -- 年間ユニーク人数 "
                  f"（{EXIT_CUTOFF_YEAR}年まで）",
            xaxis_title="年", yaxis_title="ユニーク人数",
        )

        viz_html = plotly_div_safe(fig_g, "chart_g_studio", height=420)

        findings = (
            "<p>述べ参加人年で見た上位8スタジオ: "
            + ", ".join(f"{s} ({studio_total[s]:,})" for s in top_studios[:4])
            + f" 他 {len(top_studios) - 4} スタジオ。</p>"
        )

        return ReportSection(
            title="スタジオ別 人材動向",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "スタジオは anime.studios JSON フィールドから取得。"
                "各ラインはスタジオ別の年間ユニーククレジット人数を示す。"
            ),
            section_id="studio_flow",
        )

    # ==================================================================
    # Section 9: Country Flow (Chart H)
    # ==================================================================

    def _build_country_flow_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        try:
            rows = self.conn.execute("""
                SELECT COALESCE(a.country_of_origin,'Unknown') AS country,
                       a.year, COUNT(DISTINCT c.person_id) AS up
                FROM anime a JOIN credits c ON c.anime_id = a.id
                WHERE a.year BETWEEN ? AND ?
                GROUP BY country, a.year
            """, (FLOW_START_YEAR, RELIABLE_MAX_YEAR)).fetchall()
        except Exception:
            return None

        country_persons: dict[str, dict[int, int]] = {}
        for row in rows:
            country_persons.setdefault(row["country"], {})[row["year"]] = row["up"]

        country_total = {c: sum(v.values()) for c, v in country_persons.items()}
        top_countries = sorted(
            country_total, key=lambda k: country_total[k], reverse=True,
        )[:5]

        if not top_countries:
            return None

        palette = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#fda085"]
        ctry_yrs = sorted({
            yr for c in top_countries for yr in country_persons.get(c, {})
            if FLOW_START_YEAR <= yr <= RELIABLE_MAX_YEAR
        })

        fig_h = go.Figure()
        for i, ctry in enumerate(top_countries):
            cy = country_persons.get(ctry, {})
            fig_h.add_trace(go.Scatter(
                x=ctry_yrs, y=[cy.get(yr, 0) for yr in ctry_yrs],
                name=ctry or "不明", mode="lines",
                line=dict(color=palette[i % len(palette)], width=2),
                hovertemplate=f"{ctry}: %{{y:,}}<extra></extra>",
            ))
        fig_h.update_layout(
            title="H. 国別 年間ユニーク人数（上位5カ国）",
            xaxis_title="年", yaxis_title="ユニーク人数",
            yaxis_type="log",
        )

        viz_html = plotly_div_safe(fig_h, "chart_h_country", height=380)

        findings = (
            "<p>述べクレジット人年で見た上位5カ国: "
            + ", ".join(
                f"{c} ({country_total[c]:,})" for c in top_countries
            )
            + "。</p>"
        )

        return ReportSection(
            title="国別 人材動向",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "国は anime.country_of_origin から取得。Y軸は対数スケール。"
                "日本が桁違いに優位であり、対数スケールにより各国の"
                "成長軌道の比較が可能になる。"
            ),
            section_id="country_flow",
        )

    # ==================================================================
    # Section 10: Blank/Return Analysis (Chart I)
    # ==================================================================

    def _build_blank_return_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        pid_stage = data["pid_stage"]
        pid_iv = data["pid_iv"]
        pid_gender = data["pid_gender"]

        blank_categories = [
            ("短期 (3-4年)", 3, 4),
            ("中期 (5-9年)", 5, 9),
            ("長期 (10年以上)", 10, 99),
        ]

        def _returnee_stage_label(stage: int) -> str:
            if stage <= 3:
                return "原画以下（<=3）"
            if stage == 4:
                return "作画監督（4）"
            return "監督以上（>=5）"

        returnee_stats: list[dict] = []
        gender_returnees: dict[str, dict[str, int]] = {
            "F": {}, "M": {},
        }

        try:
            rows = self.conn.execute("""
                SELECT c.person_id, a.year
                FROM credits c
                JOIN anime a ON c.anime_id = a.id
                WHERE a.year BETWEEN 1980 AND ?
                  AND a.year IS NOT NULL
            """, (EXIT_CUTOFF_YEAR,)).fetchall()

            person_active_years: dict[str, set[int]] = {}
            for row in rows:
                if row["person_id"] and row["year"]:
                    person_active_years.setdefault(
                        row["person_id"], set()
                    ).add(int(row["year"]))

            for cat_label, min_blank, max_blank in blank_categories:
                returnees: list[dict] = []
                for pid, yr_set in person_active_years.items():
                    yrs = sorted(yr_set)
                    if len(yrs) < 2:
                        continue
                    max_gap = max(
                        yrs[i + 1] - yrs[i] - 1 for i in range(len(yrs) - 1)
                    )
                    if max_gap < min_blank or max_gap > max_blank:
                        continue
                    has_return = any(
                        min_blank <= (yrs[i + 1] - yrs[i] - 1) <= max_blank
                        and i + 1 < len(yrs)
                        for i in range(len(yrs) - 1)
                    )
                    if not has_return:
                        continue
                    stage = pid_stage.get(pid, 0)
                    returnees.append({
                        "stage_label": _returnee_stage_label(stage),
                        "iv": pid_iv.get(pid, 0.0),
                        "blank_years": max_gap,
                    })
                    g = pid_gender.get(pid)
                    if g in gender_returnees:
                        gender_returnees[g][cat_label] = (
                            gender_returnees[g].get(cat_label, 0) + 1
                        )

                returnee_stats.append({
                    "label": cat_label,
                    "count": len(returnees),
                    "avg_blank": (
                        sum(r["blank_years"] for r in returnees) / len(returnees)
                        if returnees else 0
                    ),
                    "avg_iv": (
                        sum(r["iv"] for r in returnees) / len(returnees)
                        if returnees else 0
                    ),
                    "by_stage": Counter(r["stage_label"] for r in returnees),
                })
        except Exception:
            for cat_label, _, _ in blank_categories:
                returnee_stats.append({
                    "label": cat_label, "count": 0,
                    "avg_blank": 0, "avg_iv": 0, "by_stage": Counter(),
                })

        stage_labels_i = ["原画以下（<=3）", "作画監督（4）", "監督以上（>=5）"]
        stage_colors_i = ["#a0d2db", "#06D6A0", "#f093fb"]

        fig_i = go.Figure()
        for sl, sc in zip(stage_labels_i, stage_colors_i):
            fig_i.add_trace(go.Bar(
                x=[rs["label"] for rs in returnee_stats],
                y=[rs["by_stage"].get(sl, 0) for rs in returnee_stats],
                name=sl, marker_color=sc,
                hovertemplate=f"{sl}: %{{y:,}}<extra></extra>",
            ))
        fig_i.update_layout(
            barmode="group",
            title=f"I. ブランク期間・段階別 復帰人数 "
                  f"（{EXIT_CUTOFF_YEAR}年まで）",
            xaxis_title="ブランク期間", yaxis_title="復帰人数",
        )

        # Gender panel
        tabs_html = stratification_tabs(
            "returnee_gender", {"all": "全体", "F": "女性", "M": "男性"},
            active="all",
        )
        panels: list[str] = []
        panels.append(strat_panel(
            "returnee_gender", "all",
            plotly_div_safe(fig_i, "chart_i_all", height=380),
            active=True,
        ))
        for g_label, g_name in [("F", "女性"), ("M", "男性")]:
            fig_ig = go.Figure()
            cats = [rs["label"] for rs in returnee_stats]
            vals = [gender_returnees[g_label].get(c, 0) for c in cats]
            fig_ig.add_trace(go.Bar(
                x=cats, y=vals,
                name=g_name, marker_color="#f093fb" if g_label == "F" else "#a0d2db",
                hovertemplate=f"{g_name}: %{{y:,}}<extra></extra>",
            ))
            fig_ig.update_layout(
                title=f"I. 復帰者 -- {g_name}",
                xaxis_title="ブランク期間", yaxis_title="件数",
                height=380,
            )
            panels.append(strat_panel(
                "returnee_gender", g_label,
                plotly_div_safe(fig_ig, f"chart_i_{g_label}", height=380),
            ))

        viz_html = tabs_html + "\n".join(panels)

        # Stats cards
        viz_html += '<div class="stats-grid">'
        for rs in returnee_stats:
            viz_html += (
                f'<div class="stat-card">'
                f'<div class="value">{_fmt_num(rs["count"])}</div>'
                f'<div class="label">{rs["label"]}<br>'
                f'平均ブランク: {rs["avg_blank"]:.1f}年<br>'
                f'平均 IV: {rs["avg_iv"]:.4f}</div></div>'
            )
        viz_html += "</div>"

        total_ret = sum(rs["count"] for rs in returnee_stats)
        findings = (
            f"<p>{total_ret:,} 人がブランク期間後に復帰した "
            f"（credits × anime、{EXIT_CUTOFF_YEAR}年まで）。"
        )
        for rs in returnee_stats:
            findings += f'{rs["label"]}: {rs["count"]:,}。'
        findings += "</p>"

        return ReportSection(
            title="ブランク期間・復帰分析",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "ブランク = 稼働年の連続区間における最大ギャップ。"
                "ギャップ後にクレジットが存在する人物のみ復帰者としてカウント。"
                "DB クエリは全人物を対象とする（スコア算出済みに限らない）。"
                "短いブランク（3〜4年）は劇場作品の制作サイクル"
                "（2〜4年）を反映している可能性がある。"
            ),
            section_id="blank_return",
        )

    # ==================================================================
    # Section 11: Cluster-Specific Flow (Chart J)
    # ==================================================================

    def _build_cluster_flow_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        scores_list = data["scores_list"]
        pid_fy = data["pid_first_year"]
        pid_ly = data["pid_latest_year"]

        if len(scores_list) < 50:
            return None

        try:
            from sklearn.cluster import KMeans
            from sklearn.preprocessing import StandardScaler
        except ImportError:
            return None

        K_J = 5
        cl_rows: list[list[float]] = []
        cl_pids: list[str] = []
        for e in scores_list:
            pid = e["person_id"]
            car = e.get("career") or {}
            cl_rows.append([
                float(e.get("iv_score") or 0.0),
                float(e.get("birank") or 0.0),
                float(e.get("patronage") or 0.0),
                float(e.get("person_fe") or 0.0),
                float(car.get("highest_stage") or 0),
                float(car.get("active_years") or 0),
                float(e.get("total_credits") or 0),
            ])
            cl_pids.append(pid)

        if len(cl_rows) < K_J * 5:
            return None

        X_j = np.array(cl_rows)
        sc_j = StandardScaler()
        X_js = sc_j.fit_transform(X_j)
        km_j = KMeans(n_clusters=K_J, n_init=20, random_state=42)
        lbl_j = km_j.fit_predict(X_js)
        centers_j = sc_j.inverse_transform(km_j.cluster_centers_)

        # Multi-axis dynamic naming: score tier × career length × rank tier
        # Feature indices: 0=iv_score, 4=highest_stage, 5=active_years
        iv_rank = np.argsort(-centers_j[:, 0])  # desc
        yr_rank = np.argsort(-centers_j[:, 5])  # desc
        iv_tier_labels = ["トップ", "高", "中", "中低", "低"]
        yr_tier_labels = ["超長期", "長期", "中期", "短期", "極短"]
        iv_tier: dict[int, str] = {
            int(iv_rank[i]): iv_tier_labels[min(i, K_J - 1)] for i in range(K_J)
        }
        yr_tier: dict[int, str] = {
            int(yr_rank[i]): yr_tier_labels[min(i, K_J - 1)] for i in range(K_J)
        }

        def _stage_rank_label(stage_val: float) -> str:
            if stage_val >= 5:
                return "上級ランク"
            if stage_val >= 3:
                return "中級ランク"
            return "初級ランク"

        cl_name: dict[int, str] = {}
        for cid in range(K_J):
            stage_v = float(centers_j[cid, 4])
            yr_v = float(centers_j[cid, 5])
            parts = [
                f"{iv_tier[cid]}スコア",
                _stage_rank_label(stage_v),
                f"活動{yr_tier[cid]}({yr_v:.0f}年)",
            ]
            cl_name[cid] = " / ".join(parts)
        pid_cl: dict[str, int] = {
            cl_pids[i]: int(lbl_j[i]) for i in range(len(cl_pids))
        }

        flow_years = list(range(FLOW_START_YEAR, EXIT_CUTOFF_YEAR + 1))
        valid_years = list(range(FLOW_START_YEAR, RELIABLE_MAX_YEAR + 1))
        exit_xs = [yr for yr in valid_years if yr <= EXIT_CUTOFF_YEAR]

        entry_cl: dict[int, dict[int, int]] = {k: {} for k in range(K_J)}
        exit_cl: dict[int, dict[int, int]] = {k: {} for k in range(K_J)}
        stock_cl: dict[int, dict[int, int]] = {
            k: {yr: 0 for yr in flow_years} for k in range(K_J)
        }

        for pid, fy in pid_fy.items():
            if FLOW_START_YEAR <= fy <= RELIABLE_MAX_YEAR:
                k = pid_cl.get(pid, -1)
                if k >= 0:
                    entry_cl[k][fy] = entry_cl[k].get(fy, 0) + 1
        for pid, ly in pid_ly.items():
            if FLOW_START_YEAR <= ly <= EXIT_CUTOFF_YEAR:
                k = pid_cl.get(pid, -1)
                if k >= 0:
                    exit_cl[k][ly] = exit_cl[k].get(ly, 0) + 1
        for pid, fy in pid_fy.items():
            ly = pid_ly.get(pid, RELIABLE_MAX_YEAR)
            k = pid_cl.get(pid, -1)
            if k >= 0:
                for yr in flow_years:
                    if fy <= yr <= ly:
                        stock_cl[k][yr] += 1

        cl_pal = ["#a0d2db", "#06D6A0", "#FFD166", "#f093fb", "#EF476F"]

        fig_j = make_subplots(
            rows=2, cols=1,
            subplot_titles=(
                "クラスタ別 参入/退職（年次）",
                "クラスタ別 ストック（積み上げ面グラフ）",
            ),
            vertical_spacing=0.14, shared_xaxes=True,
        )
        for k in range(K_J):
            c = cl_pal[k % len(cl_pal)]
            nm = cl_name.get(k, f"クラスタ {k}")
            fig_j.add_trace(go.Scatter(
                x=valid_years,
                y=[entry_cl[k].get(yr, 0) for yr in valid_years],
                name=f"{nm} 参入", mode="lines",
                line=dict(color=c, width=2), legendgroup=nm,
                hovertemplate=f"{nm} entry %{{x}}: %{{y:,}}<extra></extra>",
            ), row=1, col=1)
            fig_j.add_trace(go.Scatter(
                x=exit_xs,
                y=[exit_cl[k].get(yr, 0) for yr in exit_xs],
                name=f"{nm} 退職", mode="lines",
                line=dict(color=c, width=2, dash="dot"),
                legendgroup=nm, showlegend=False,
                hovertemplate=f"{nm} exit %{{x}}: %{{y:,}}<extra></extra>",
            ), row=1, col=1)
            fig_j.add_trace(go.Scatter(
                x=flow_years,
                y=[stock_cl[k].get(yr, 0) for yr in flow_years],
                name=nm, mode="lines", stackgroup="one",
                line=dict(color=c, width=1),
                legendgroup=nm, showlegend=False,
                hovertemplate=f"{nm} stock %{{x}}: %{{y:,}}<extra></extra>",
            ), row=2, col=1)

        fig_j.update_layout(
            title=f"J. クラスタ別（K={K_J}）参入/退職/ストック",
            height=680,
        )
        fig_j.update_yaxes(title_text="人数", row=1, col=1)
        fig_j.update_yaxes(title_text="ストック", row=2, col=1)
        fig_j.update_xaxes(title_text="年", row=2, col=1)

        viz_html = plotly_div_safe(fig_j, "chart_j_cluster", height=680)

        # Cluster centroid table
        feat_names = [
            "iv_score", "birank", "patronage", "person_fe",
            "stage", "active_yrs", "credits",
        ]
        table_html = (
            '<table style="width:100%;font-size:0.82rem;margin:1rem 0;">'
            "<thead><tr><th>クラスタ</th>"
            + "".join(f"<th>{n}</th>" for n in feat_names)
            + "</tr></thead><tbody>"
        )
        for k in range(K_J):
            table_html += (
                f"<tr><td>{cl_name.get(k, '?')}</td>"
                + "".join(f"<td>{centers_j[k, fi]:.2f}</td>" for fi in range(len(feat_names)))
                + "</tr>"
            )
        table_html += "</tbody></table>"

        findings = (
            f"<p>iv_score、birank、patronage、person_fe、stage、active_years、"
            f"total_credits を用いた K-Means クラスタリング（K={K_J}）。"
            f"クラスタ重心は iv_score の昇順:</p>{table_html}"
        )

        return ReportSection(
            title="スコア × キャリアクラスタ別フロー",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                f"K-Means、K={K_J}、n_init=20、random_state=42。"
                "特徴量は StandardScaler で標準化。"
                "クラスタ名は iv_score 重心の順位で割り当てる。"
            ),
            section_id="cluster_flow",
        )

    # ==================================================================
    # Section 12: Decade Comparison
    # ==================================================================

    def _build_decade_comparison_section(
        self, sb: SectionBuilder,
    ) -> ReportSection | None:
        try:
            rows = self.conn.execute("""
                SELECT a.year, a.format,
                    COUNT(DISTINCT a.id) AS anime_count,
                    COUNT(DISTINCT c.person_id) AS person_count
                FROM anime a
                LEFT JOIN credits c ON c.anime_id = a.id
                WHERE a.year BETWEEN 1980 AND ?
                  AND a.format IN ('TV','MOVIE','OVA','ONA','TV_SHORT')
                GROUP BY a.year, a.format
            """, (RELIABLE_MAX_YEAR,)).fetchall()
        except Exception:
            return None

        dec_formats = ["TV", "MOVIE", "OVA", "ONA", "TV_SHORT"]
        fmt_colors = {
            "TV": "#f093fb", "MOVIE": "#fda085",
            "OVA": "#a0d2db", "ONA": "#06D6A0", "TV_SHORT": "#FFD166",
        }
        anime_by_fmt: dict[str, dict[int, int]] = {f: {} for f in dec_formats}
        person_by_fmt: dict[str, dict[int, int]] = {f: {} for f in dec_formats}
        years_set: set[int] = set()

        for row in rows:
            yr = row["year"]
            fmt = row["format"]
            if fmt not in dec_formats:
                continue
            years_set.add(yr)
            anime_by_fmt[fmt][yr] = row["anime_count"]
            person_by_fmt[fmt][yr] = row["person_count"]

        years_sorted = sorted(years_set)
        if not years_sorted:
            return None

        fig_dec = make_subplots(
            rows=2, cols=1,
            subplot_titles=(
                "需要: フォーマット別 年間作品数",
                "供給: フォーマット別 年間ユニーク人数",
            ),
            vertical_spacing=0.12,
        )
        for fmt in dec_formats:
            c = fmt_colors[fmt]
            fig_dec.add_trace(go.Scatter(
                x=years_sorted,
                y=[anime_by_fmt[fmt].get(yr, 0) for yr in years_sorted],
                mode="lines", name=fmt, line=dict(color=c, width=2),
                hovertemplate=f"{fmt} %{{x}}: %{{y:,}} works<extra></extra>",
            ), row=1, col=1)
            fig_dec.add_trace(go.Scatter(
                x=years_sorted,
                y=[person_by_fmt[fmt].get(yr, 0) for yr in years_sorted],
                mode="lines", name=fmt, line=dict(color=c, width=2),
                showlegend=False,
                hovertemplate=f"{fmt} %{{x}}: %{{y:,}} persons<extra></extra>",
            ), row=2, col=1)

        fig_dec.update_layout(
            title=f"年代比較: 需要と供給 "
                  f"（1980〜{RELIABLE_MAX_YEAR}）",
            height=700,
        )

        viz_html = plotly_div_safe(fig_dec, "chart_decade", height=700)

        findings = (
            f"<p>1980〜{RELIABLE_MAX_YEAR}年のフォーマット別内訳。"
            f"TV フォーマットが作品数・クレジット人数ともに最大のシェアを占める。"
            f"ONA（web オリジナル）は2010年代から成長を見せている。</p>"
        )

        return ReportSection(
            title="年代比較 -- フォーマット別 需要と供給",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "フォーマットは anime.format から取得。集計: 作品数 = 異なる anime.id 数、"
                "人数 = それら作品にクレジットされた異なる person_id 数。"
                "(TV, MOVIE, OVA, ONA, TV_SHORT) 以外のフォーマットは除外する。"
            ),
            section_id="decade_comparison",
        )

    # ==================================================================
    # Section 13: Seasonal Analysis
    # ==================================================================

    def _build_seasonal_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        try:
            rows = self.conn.execute("""
                SELECT a.season, a.format,
                       CASE
                           WHEN a.episodes IS NULL OR a.episodes = 0 THEN 'unknown'
                           WHEN a.episodes <= 1 THEN 'movie_or_special'
                           WHEN a.episodes <= 14 THEN 'single_cour'
                           WHEN a.episodes <= 28 THEN 'multi_cour'
                           ELSE 'long_cour'
                       END AS cour_type,
                       COUNT(DISTINCT a.id) AS works,
                       COUNT(DISTINCT c.person_id) AS persons
                FROM anime a
                LEFT JOIN credits c ON c.anime_id = a.id
                WHERE a.year BETWEEN 1990 AND ?
                  AND a.season IN ('winter','spring','summer','fall')
                GROUP BY a.season, cour_type
            """, (RELIABLE_MAX_YEAR,)).fetchall()
        except Exception:
            return None

        season_names = ["winter", "spring", "summer", "fall"]
        season_labels = [
            "冬（1-3月）", "春（4-6月）", "夏（7-9月）", "秋（10-12月）",
        ]
        cour_order = ["single_cour", "multi_cour", "long_cour", "movie_or_special"]
        cour_labels = {
            "single_cour": "1クール",
            "multi_cour": "2クール",
            "long_cour": "長期",
            "movie_or_special": "映画/特番",
        }
        cour_colors = {
            "single_cour": "#a0d2db",
            "multi_cour": "#06D6A0",
            "long_cour": "#f093fb",
            "movie_or_special": "#fda085",
        }

        counts: dict[str, dict[str, int]] = {
            s: {ct: 0 for ct in cour_order} for s in season_names
        }
        for row in rows:
            s = row["season"]
            ct = row["cour_type"]
            if s in counts and ct in counts[s]:
                counts[s][ct] += row["works"]

        fig_sea = go.Figure()
        for ct in cour_order:
            fig_sea.add_trace(go.Bar(
                x=season_labels,
                y=[counts[sn][ct] for sn in season_names],
                name=cour_labels[ct], marker_color=cour_colors[ct],
                hovertemplate=f"{cour_labels[ct]}: %{{y:,}}<extra></extra>",
            ))
        fig_sea.update_layout(
            barmode="group",
            title="季節別: 季節 × クールタイプ別 作品数",
            xaxis_title="季節", yaxis_title="作品数",
        )

        viz_html = plotly_div_safe(fig_sea, "chart_seasonal", height=400)

        total_works = sum(
            sum(ct_d.values()) for ct_d in counts.values()
        )
        findings = (
            f"<p>分類済み {total_works:,} 作品の季節別制作パターン"
            f"（1990〜{RELIABLE_MAX_YEAR}年）。季節は anime.season から取得。</p>"
        )

        return ReportSection(
            title="季節パターン -- 季節・クールタイプ別 作品数",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "季節は anime.season から取得。クールタイプはエピソード数の閾値で判定: "
                "1クール (<=14)、2クール (15-28)、長期 (29+)、映画/特番 (<=1)。"
            ),
            section_id="seasonal",
        )

    # ==================================================================
    # Section 14: Growth Trends
    # ==================================================================

    def _build_growth_trends_section(
        self, sb: SectionBuilder, data: dict,
    ) -> ReportSection | None:
        pid_fy = data["pid_first_year"]
        pid_trend = data["pid_trend"]

        if not pid_trend:
            return None

        trend_categories = ["rising", "stable", "new", "declining", "inactive"]
        trend_labels = {
            "rising": "上昇",
            "stable": "安定",
            "new": "新規",
            "declining": "下降",
            "inactive": "非稼働",
        }
        trend_colors = {
            "rising": "#06D6A0",
            "stable": "#a0d2db",
            "new": "#667eea",
            "declining": "#fda085",
            "inactive": "#606070",
        }

        decades = [1970, 1980, 1990, 2000, 2010, 2020]
        cohort_trend: dict[int, dict[str, int]] = {
            d: {t: 0 for t in trend_categories} for d in decades
        }
        for pid, fy in pid_fy.items():
            decade = (fy // 10) * 10
            if decade not in cohort_trend:
                continue
            trend = pid_trend.get(pid, "")
            if trend in trend_categories:
                cohort_trend[decade][trend] += 1

        fig_gt = go.Figure()
        for trend in trend_categories:
            fig_gt.add_trace(go.Bar(
                orientation="h",
                name=trend_labels[trend],
                x=[
                    cohort_trend[d][trend]
                    / max(sum(cohort_trend[d].values()), 1) * 100
                    for d in decades
                ],
                y=[f"{d}s" for d in decades],
                marker_color=trend_colors[trend],
                customdata=[cohort_trend[d][trend] for d in decades],
                hovertemplate=(
                    f"{trend_labels[trend]}: %{{x:.1f}}%"
                    " (%{customdata:,})<extra></extra>"
                ),
            ))
        fig_gt.update_layout(
            barmode="stack",
            title="成長トレンド: デビュー年代別キャリア軌跡",
            xaxis_title="構成比（%）",
            yaxis_title="デビュー年代",
            xaxis=dict(range=[0, 100]),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02,
                xanchor="right", x=1,
            ),
        )

        viz_html = plotly_div_safe(fig_gt, "chart_growth", height=500)

        total_classified = sum(
            sum(d.values()) for d in cohort_trend.values()
        )
        findings = (
            f"<p>トレンドデータを持つ {total_classified:,} 人のキャリア軌跡分布を"
            f"デビュー年代別にグループ化した。"
            f"トレンドカテゴリ: rising（上昇）、stable（安定）、new（新規）、"
            f"declining（下降）、inactive（非稼働）"
            f"（feat_cluster_membership.growth_trend より）。</p>"
        )

        return ReportSection(
            title="成長トレンド -- デビュー年代別キャリア軌跡",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "トレンドは feat_cluster_membership.growth_trend から取得。"
                "デビュー年代 = (first_year / 10) * 10。"
                "水平積み上げ棒は件数ではなく構成比（%）を示す。"
            ),
            section_id="growth_trends",
        )
