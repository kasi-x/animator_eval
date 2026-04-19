# DEPRECATED (Phase 3-5, 2026-04-19): merged into policy_generational_health. キャリア段階遷移章に統合.
# This module is retained in archived/ for regeneration and audit only.
# It is NOT in V2_REPORT_CLASSES and will not run in default generation.
"""Career Transitions report — v2 compliant.

Ports ALL charts from the original generate_career_report() in
scripts/generate_all_reports.py into v2 ReportSection structure.

Sections:
 1. Transition Matrix (count + probability heatmaps)
 2. Role-to-Role Transition Times (horizontal bar)
 3. Time to Stage Distribution (ridge plot or bar fallback + summary table)
 4. Top Career Paths (horizontal bar)
 5. Role Flow Sankey
 6. Stage Median Arrival Years (horizontal bar)
 7. Career Speed Density (density scatter)
 8. Stage-based IV Distribution (ridge plot)
 9. Director Credit Ranking (top 50 bar)
10. Director Dominant Type Distribution (bar)
11. Director Scale Profile (stacked bar + heatmap)
12. Director Career Span by Dominant Type (violin)
13. Director Years to First Direction by Scale (ridge)
14. Director Scale Mobility (heatmap + sankey)
15. Director Experience vs Achieved Scale (density scatter)

Key v2 requirements:
- Findings text: NO evaluative adjectives, NO causal verbs
- distribution_summary + format_ci + format_distribution_inline for all stats
- sb.validate_findings on every section
- plotly_div_safe for all charts
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from src.analysis.career import (
    SCALE_KEY_LABELS,
    SCALE_KEYS_ORDERED,
    compute_director_scale_profiles,
    compute_director_trajectories,
)
from src.database import load_all_anime, load_all_credits, load_all_persons
from src.utils.json_io import (
    load_person_scores_from_json,
    load_role_flow_from_json,
    load_role_transitions_from_json,
)

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..helpers import density_scatter_2d, fmt_num, ridge_plot, safe_nested
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

import structlog

log = structlog.get_logger()

_ROLE_CATEGORY_LABELS = {
    "direction": "監督・演出",
    "animation_supervision": "作監・総作監",
    "animation": "原画・動画",
    "design": "キャラデザ・設定",
    "art": "美術・背景",
    "sound": "音楽・音響",
    "writing": "脚本・シリーズ構成",
    "production": "制作進行・プロデュース",
    "technical": "撮影・CG・技術",
    "other": "その他",
}

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}

_SCALE_COLORS_MAP = {
    "TV大": "#F72585",
    "TV中": "#7209b7",
    "TV小": "#4cc9f0",
    "単発大": "#f77f00",
    "単発中": "#fcbf49",
    "単発小": "#2ec4b6",
}

_SCALE_COLORS_KEY = {
    "tv_large": "#F72585",
    "tv_medium": "#7209b7",
    "tv_small": "#4cc9f0",
    "tanpatsu_large": "#f77f00",
    "tanpatsu_medium": "#fcbf49",
    "tanpatsu_small": "#2ec4b6",
}

_STAGE_COLORS_BAR = [
    "#f093fb",
    "#a0d2db",
    "#667eea",
    "#06D6A0",
    "#FFD166",
    "#f5576c",
    "#F72585",
]

_STAGE_NAMES = [
    "0:新人",
    "1:若手",
    "2:中堅",
    "3:熟練",
    "4:ベテラン",
    "5:マスター",
    "6:レジェンド",
]

_SCALE_RANK = {
    "tv_large": 5,
    "tanpatsu_large": 4,
    "tv_medium": 3,
    "tanpatsu_medium": 2,
    "tv_small": 1,
    "tanpatsu_small": 0,
}


class CareerTransitionsReport(BaseReportGenerator):
    """Career transitions & role progression — v2 compliant."""

    name = "career_transitions"
    title = "キャリア遷移分析"
    subtitle = "役職カテゴリ間の遷移と監督昇進のキャリア文脈"
    filename = "career_transitions.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []

        # Load shared data
        self._transitions = load_role_transitions_from_json()
        self._role_flow = load_role_flow_from_json()
        self._scores = load_person_scores_from_json()

        if not self._transitions and not self._role_flow:
            return None

        # Load director analysis data
        self._load_director_data()

        # Section 1: Transition Matrix (count + probability)
        sections.append(sb.build_section(self._build_transition_matrix_section(sb)))

        # Section 2: Role-to-Role Transition Times
        sections.append(sb.build_section(self._build_role_chain_times_section(sb)))

        # Section 3: Time to Stage Distribution
        sections.append(sb.build_section(self._build_time_to_stage_section(sb)))

        # Section 4: Top Career Paths
        sections.append(sb.build_section(self._build_career_paths_section(sb)))

        # Section 5: Role Flow Sankey
        sections.append(sb.build_section(self._build_role_flow_sankey_section(sb)))

        # Section 6: Stage Median Arrival Years
        sections.append(sb.build_section(self._build_stage_median_years_section(sb)))

        # Section 7: Career Speed Density
        sections.append(sb.build_section(self._build_career_speed_section(sb)))

        # Section 8: Stage-based IV Distribution
        sections.append(sb.build_section(self._build_stage_iv_section(sb)))

        # Section 9: Director Credit Ranking
        sections.append(sb.build_section(self._build_director_ranking_section(sb)))

        # Section 10: Director Dominant Type Distribution
        sections.append(
            sb.build_section(
                self._build_director_dominant_type_section(sb),
            )
        )

        # Section 11: Director Scale Profile (stacked bar + heatmap)
        sections.append(
            sb.build_section(self._build_director_scale_profile_section(sb))
        )

        # Section 12: Director Career Span by Dominant Type
        sections.append(
            sb.build_section(
                self._build_director_career_span_section(sb),
            )
        )

        # Section 13: Director Years to First Direction by Scale
        sections.append(
            sb.build_section(
                self._build_director_years_to_debut_section(sb),
            )
        )

        # Section 14: Director Scale Mobility (heatmap + sankey)
        sections.append(
            sb.build_section(
                self._build_director_mobility_section(sb),
            )
        )

        # Section 15: Director Experience vs Achieved Scale
        sections.append(
            sb.build_section(
                self._build_director_exp_vs_scale_section(sb),
            )
        )

        # Filter out empty placeholder sections
        sections = [s for s in sections if s.strip()]

        return self.write_report(
            "\n".join(sections),
            extra_glossary={
                "キャリアステージ (Career Stage)": (
                    "クレジット履歴の期間と役職の進行に基づく人物の段階分類"
                    "（新人・中堅・ベテラン・マスターなど）。"
                ),
                "遷移行列 (Transition Matrix)": (
                    "キャリアステージ間の遷移頻度を示すグリッド。"
                    "各セル(行,列)=行のステージから列のステージへ移った人数。"
                ),
                "サンキーダイアグラム (Sankey Diagram)": (
                    "帯の幅が役職間の遷移量を表すフロー可視化。"
                    "主要なキャリアパスを一目で把握できます。"
                ),
            },
        )

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_director_data(self) -> None:
        """Load director scale profiles and trajectories from DB."""
        self._dir_profiles: list = []
        self._dir_trajectories = None
        try:
            anime_list = load_all_anime(self.conn)
            credits_list = load_all_credits(self.conn)
            persons = load_all_persons(self.conn)

            anime_map = {a.id: a for a in anime_list}
            name_map = {p.id: (p.name_ja or p.name_en or p.id) for p in persons}

            self._dir_profiles = compute_director_scale_profiles(
                credits_list,
                anime_map,
                name_map,
                director_roles={"director"},
                min_credits=3,
            )

            if self._dir_profiles:
                self._dir_trajectories = compute_director_trajectories(
                    credits_list,
                    anime_map,
                    director_roles={"director"},
                    min_director_credits=3,
                )

            # Store for later use
            self._credits_list = credits_list
            self._anime_map = anime_map
        except Exception:
            log.warning("director_data_load_failed", exc_info=True)

    # ------------------------------------------------------------------
    # Section 1: Transition Matrix (count + probability)
    # ------------------------------------------------------------------

    def _build_transition_matrix_section(self, sb: SectionBuilder) -> ReportSection:
        """Career stage transition count matrix and probability matrix."""
        trans = self._transitions.get("transitions", []) if self._transitions else []
        if not trans:
            return ReportSection(
                title="キャリアステージ遷移行列",
                findings_html="<p>遷移データが利用できません。</p>",
                section_id="transition_matrix",
            )

        stages = sorted(
            set([t["from_label"] for t in trans] + [t["to_label"] for t in trans])
        )
        matrix: dict[tuple[str, str], int] = {}
        for t in trans:
            matrix[(t["from_label"], t["to_label"])] = t["count"]

        z = [[matrix.get((s1, s2), 0) for s2 in stages] for s1 in stages]
        total_transitions = sum(t["count"] for t in trans)

        # Distribution summary of counts
        all_counts = [t["count"] for t in trans if t["count"] > 0]
        count_summ = distribution_summary(all_counts, label="transition_counts")

        findings = (
            f"<p>遷移行列は{len(stages)}個のキャリアステージと{len(trans)}個の観測されたステージペアにわたり、"
            f"合計{total_transitions:,}件の遷移を含む。 "
            f"ペアごとの件数分布: {format_distribution_inline(count_summ)}, "
            f"{format_ci((count_summ['ci_lower'], count_summ['ci_upper']))}。</p>"
        )

        # Count heatmap
        fig_count = go.Figure(
            go.Heatmap(
                z=z,
                x=stages,
                y=stages,
                colorscale="Magma",
                text=[[str(v) if v > 0 else "" for v in row] for row in z],
                texttemplate="%{text}",
                hovertemplate="%{y} -> %{x}: %{z}<extra></extra>",
            )
        )
        fig_count.update_layout(
            title="キャリアステージ遷移行列（件数）",
            xaxis_title="遷移先ステージ",
            yaxis_title="遷移元ステージ",
        )

        # Probability heatmap
        z_prob = []
        for row in z:
            row_total = sum(row)
            if row_total > 0:
                z_prob.append([v / row_total * 100 for v in row])
            else:
                z_prob.append([0.0] * len(row))

        fig_prob = go.Figure(
            go.Heatmap(
                z=z_prob,
                x=stages,
                y=stages,
                colorscale="Viridis",
                texttemplate="%{z:.1f}%",
                hovertemplate="%{y} -> %{x}: %{z:.1f}%<extra></extra>",
            )
        )
        fig_prob.update_layout(
            title="キャリアステージ遷移確率行列 (row-normalized %)",
            xaxis_title="遷移先ステージ",
            yaxis_title="遷移元ステージ",
        )

        viz = plotly_div_safe(
            fig_count, "chart_trans_count", height=500
        ) + plotly_div_safe(fig_prob, "chart_trans_prob", height=500)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="キャリアステージ遷移行列",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "遷移データは transitions.json 由来（パイプライン Phase 9）。"
                "件数行列: 各セル = 行ステージから列ステージへ遷移が観測された人数。"
                "確率行列: 各行の合計が 100% になるよう正規化。"
                "対角値はステージ滞留率を示す。"
            ),
            interpretation_html=(
                "<p>対角値が大きい行は、人物が滞留しやすいステージを示す。"
                "対角より上側の非対角値は上方遷移に対応する。"
                "このパターンは段階的キャリア上昇と整合的だが、"
                "データ記録慣行（例: 役職カテゴリ境界）を反映している可能性もある。"
                "代替解釈として、中堅ステージの対角持続性は個人のキャリア選択ではなく、"
                "業界の需要パターンを部分的に反映している可能性もある。</p>"
            ),
            section_id="transition_matrix",
        )

    # ------------------------------------------------------------------
    # Section 2: Role-to-Role Transition Times
    # ------------------------------------------------------------------

    def _build_role_chain_times_section(self, sb: SectionBuilder) -> ReportSection:
        """Average years between consecutive upward stage transitions."""
        trans = self._transitions.get("transitions", []) if self._transitions else []
        consecutive = [t for t in trans if t["to_stage"] > t["from_stage"]]
        if not consecutive:
            return ReportSection(
                title="ロール間遷移時間",
                findings_html="<p>連続遷移データが利用できません。</p>",
                section_id="role_chain_times",
            )

        consecutive.sort(key=lambda t: (t["from_stage"], t["to_stage"]))
        labels = [f"{t['from_label']} -> {t['to_label']}" for t in consecutive]
        years = [t.get("avg_years", 0) for t in consecutive]
        counts = [t.get("count", 0) for t in consecutive]

        # Distribution summary of avg_years
        years_summ = distribution_summary(years, label="transition_years")

        findings = (
            f"<p>{len(consecutive)}件の上方遷移パスを観測。 "
            f"ステージ間平均遷移時間の分布: "
            f"{format_distribution_inline(years_summ)}, "
            f"{format_ci((years_summ['ci_lower'], years_summ['ci_upper']))}。</p>"
            "<p>各値は遷移元ステージから遷移先ステージまでの年数を表す"
            "（デビューからの累積年数ではない）。 "
            "上方遷移（to_stage > from_stage）のみを含む。</p>"
        )

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                y=labels,
                x=years,
                orientation="h",
                marker_color="#f093fb",
                text=[f"{y:.1f}yr (n={c:,})" for y, c in zip(years, counts)],
                textposition="auto",
                hovertemplate=(
                    "%{y}<br>Avg transition time: %{x:.1f} years<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="ステージ間遷移時間（上方遷移のみ）",
            xaxis_title="平均年数",
            yaxis_title="遷移経路",
            height=max(400, len(labels) * 40),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="ロール間遷移時間（因果チェーン）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_role_chain",
                height=max(400, len(labels) * 40),
            ),
            method_note=(
                "データは transitions.json 由来。to_stage > from_stage の"
                "遷移のみ表示。avg_years = 遷移を完了した人物の"
                "ステージ間平均年数。"
                "生存バイアス: 遷移を完了した人物のみ含まれ、"
                "昇進しなかった人物は除外される。"
            ),
            interpretation_html=(
                "<p>特定ステージ間の遷移時間が長いことは、参入障壁の高さや"
                "キャリアパス構造の違いを反映している可能性がある。"
                "代替解釈: (1) 生存バイアス — 遷移を完了した人物のみ計上されるため、"
                "平均は典型的な待機時間を過小評価する可能性がある; "
                "(2) 選抜効果 — 早く昇進する人物はゆっくり昇進する人物と"
                "系統的に異なる可能性; "
                "(3) 時代効果 — 業界構造の変化により、"
                "年代間で遷移時間が異なり得る。</p>"
            ),
            section_id="role_chain_times",
        )

    # ------------------------------------------------------------------
    # Section 3: Time to Stage Distribution (ridge / bar fallback)
    # ------------------------------------------------------------------

    def _build_time_to_stage_section(self, sb: SectionBuilder) -> ReportSection:
        """Ridge plot of active years by highest stage, with bar fallback."""
        avg_time = (
            self._transitions.get("avg_time_to_stage", {}) if self._transitions else {}
        )
        if not avg_time:
            return ReportSection(
                title="キャリアステージ別到達年数分布",
                findings_html="<p>ステージ到達時間データが利用できません。</p>",
                section_id="time_to_stage",
            )

        # Parse stage metadata
        stage_labels_t: list[str] = []
        avg_years: list[float] = []
        median_years: list[float] = []
        sample_sizes: list[int] = []
        for stage_id in sorted(avg_time.keys(), key=int):
            sd = avg_time[stage_id]
            stage_labels_t.append(sd.get("label", f"Stage {stage_id}"))
            avg_years.append(sd.get("avg_years", 0))
            median_years.append(sd.get("median_years", 0))
            sample_sizes.append(sd.get("sample_size", 0))

        # Try to build ridge from per-person scores data
        stage_time_groups: dict[str, list[float]] = {}
        scores = self._scores
        if scores and isinstance(scores, list):
            for p in scores:
                career = p.get("career", {})
                hs = career.get("highest_stage", 0)
                ay = career.get("active_years", 0)
                if hs > 0 and ay > 0:
                    label = (
                        stage_labels_t[min(hs, len(stage_labels_t) - 1)]
                        if hs < len(stage_labels_t)
                        else f"Stage {hs}"
                    )
                    stage_time_groups.setdefault(label, []).append(float(ay))

        # Build findings from distribution summaries
        stage_summaries: dict[str, dict] = {}
        for label, vals in stage_time_groups.items():
            if vals:
                stage_summaries[label] = distribution_summary(vals, label=label)

        findings = (
            f"<p>ステージ到達時間データ: {len(avg_time)}ステージ分が利用可能"
            f"（ステージ全体のサンプル合計: {sum(sample_sizes):,}）。</p>"
        )
        if stage_summaries:
            findings += "<p>ステージ別アクティブ年数の分布:</p><ul>"
            for label in stage_labels_t:
                if label in stage_summaries:
                    s = stage_summaries[label]
                    findings += (
                        f"<li><strong>{label}</strong> (n={s['n']:,}): "
                        f"{format_distribution_inline(s)}, "
                        f"{format_ci((s['ci_lower'], s['ci_upper']))}</li>"
                    )
            findings += "</ul>"

        # Summary table as method note addition
        table_rows = ""
        for lbl, avg, med, sz in zip(
            stage_labels_t,
            avg_years,
            median_years,
            sample_sizes,
        ):
            table_rows += (
                f"<tr><td>{lbl}</td><td>{avg:.1f}</td>"
                f"<td>{med:.0f}</td><td>{fmt_num(sz)}</td></tr>"
            )
        summary_table = (
            "<table><thead><tr>"
            "<th>ステージ</th><th>平均年数</th>"
            "<th>中央値年数</th><th>サンプルサイズ</th>"
            "</tr></thead><tbody>"
            f"{table_rows}</tbody></table>"
        )

        # Visualization
        if stage_time_groups:
            fig = ridge_plot(
                stage_time_groups,
                title="ステージ別 到達年数分布 (Ridge Plot)",
                xlabel="活動年数",
                height=max(400, len(stage_time_groups) * 70),
            )
            viz = plotly_div_safe(
                fig,
                "chart_time_to_stage",
                height=max(400, len(stage_time_groups) * 70),
            )
        else:
            # Fallback: grouped bar
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=stage_labels_t,
                    y=avg_years,
                    name="平均",
                    marker_color="#f093fb",
                )
            )
            fig.add_trace(
                go.Bar(
                    x=stage_labels_t,
                    y=median_years,
                    name="中央値",
                    marker_color="#a0d2db",
                )
            )
            fig.update_layout(
                title="各キャリアステージへの平均到達年数",
                barmode="group",
                xaxis_title="キャリアステージ",
                yaxis_title="年数",
            )
            viz = plotly_div_safe(fig, "chart_time_to_stage", height=400)

        # Append summary table below chart
        viz += f'<div style="margin-top:1rem;">{summary_table}</div>'

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="キャリアステージ別到達年数分布",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "avg_time_to_stage は transitions.json 由来の集計統計を提供する。"
                "リッジプロットは scores.json の人物別 active_years を"
                "highest_stage でグルーピングして構築。二峰分布は"
                "進行速度の異なるサブグループの存在を示唆する。"
                "デビューからの累積年数であり、ステージ間遷移時間ではない。"
            ),
            interpretation_html=(
                "<p>ステージ別の活動年数分布は、キャリア進行タイミングと"
                "データ記録密度の両方を反映している。"
                "低位ステージで分布が広い場合、そのステージにキャリア中ずっと"
                "留まっていた人物を含む可能性がある。"
                "代替解釈として、ステージ内の二峰パターンは、"
                "異なるキャリア軌跡（例: ステージをスキップする人と線形に進む人）を"
                "反映している可能性もある。</p>"
            ),
            section_id="time_to_stage",
        )

    # ------------------------------------------------------------------
    # Section 4: Top Career Paths
    # ------------------------------------------------------------------

    def _build_career_paths_section(self, sb: SectionBuilder) -> ReportSection:
        """Top 15 most common career paths."""
        paths = self._transitions.get("career_paths", []) if self._transitions else []
        if not paths:
            return ReportSection(
                title="代表的キャリアパス",
                findings_html="<p>キャリアパスデータが利用できません。</p>",
                section_id="career_paths",
            )

        top_paths = paths[:15]
        path_labels = [" -> ".join(p["path_labels"]) for p in top_paths]
        path_counts = [p["count"] for p in top_paths]

        total_paths = len(paths)
        top_count = sum(path_counts)
        all_count = sum(p["count"] for p in paths)

        findings = (
            f"<p>{total_paths:,}通りの異なるキャリアパスを観測。 "
            f"上位15パスは全{all_count:,}人分のパスのうち{top_count:,}人分を占める"
            f"（{top_count / all_count * 100:.1f}%、all_count > 0の場合）。 "
            f"最も一般的なパスは「{path_labels[0]}」"
            f"（{path_counts[0]:,}人）。</p>"
        )

        fig = go.Figure(
            go.Bar(
                y=path_labels[::-1],
                x=path_counts[::-1],
                orientation="h",
                marker_color="#fda085",
                hovertemplate="%{y}: %{x}<extra></extra>",
            )
        )
        fig.update_layout(
            title="代表的キャリアパス Top 15",
            xaxis_title="人数",
            xaxis_type="log",
            yaxis_title="",
            height=max(400, len(path_labels) * 30),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="代表的キャリアパス",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_career_paths",
                height=max(400, len(path_labels) * 30),
            ),
            method_note=(
                "キャリアパスは transitions.json 由来。各パスは人物が通過した"
                "キャリアステージの順序列。"
                "パス頻度のべき乗分布を受けて X 軸はログスケール。"
                "パスは頻度降順で並び替え。"
            ),
            section_id="career_paths",
        )

    # ------------------------------------------------------------------
    # Section 5: Role Flow Sankey
    # ------------------------------------------------------------------

    def _build_role_flow_sankey_section(self, sb: SectionBuilder) -> ReportSection:
        """Sankey diagram of role flow from role_flow.json."""
        role_flow = self._role_flow
        if not role_flow:
            return ReportSection(
                title="ロールフロー（Sankey）",
                findings_html="<p>ロールフローデータが利用できません。</p>",
                section_id="role_flow_sankey",
            )

        nodes = role_flow.get("nodes", [])
        links = role_flow.get("links", [])
        if not nodes or not links:
            return ReportSection(
                title="ロールフロー（Sankey）",
                findings_html="<p>ロールフローのノード/リンクが利用できません。</p>",
                section_id="role_flow_sankey",
            )

        node_labels = [n["label"] for n in nodes]
        node_map = {n["id"]: i for i, n in enumerate(nodes)}

        # Top 40 transitions for readability
        sorted_links = sorted(links, key=lambda x: x["value"], reverse=True)[:40]
        valid = [
            lk
            for lk in sorted_links
            if lk["source"] in node_map and lk["target"] in node_map
        ]
        src_indices = [node_map[lk["source"]] for lk in valid]
        tgt_indices = [node_map[lk["target"]] for lk in valid]
        values = [lk["value"] for lk in valid]

        total_transitions = role_flow.get("total_transitions", 0)
        top_value = sum(values)

        findings = (
            f"<p>ロールフローSankeyは全{len(links):,}種類のリンク"
            f"（遷移インスタンス合計{total_transitions:,}件）のうち、"
            f"上位40遷移を表示。 "
            f"表示された遷移は{top_value:,}件のインスタンスを占める。</p>"
        )

        node_colors = px.colors.qualitative.Pastel
        link_colors = []
        for s in src_indices:
            c = node_colors[s % len(node_colors)]
            if c.startswith("rgb("):
                link_colors.append(
                    c.replace("rgb(", "rgba(").replace(")", ",0.3)"),
                )
            else:
                link_colors.append("rgba(180,180,200,0.3)")

        fig = go.Figure(
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    label=node_labels,
                    color=[
                        node_colors[i % len(node_colors)]
                        for i in range(len(node_labels))
                    ],
                ),
                link=dict(
                    source=src_indices,
                    target=tgt_indices,
                    value=values,
                    color=link_colors,
                ),
            )
        )
        fig.update_layout(
            title=(
                f"Career Role Flow (top 40 transitions, "
                f"{fmt_num(total_transitions)} total)"
            ),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="ロールフロー（Sankey）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_role_sankey",
                height=600,
            ),
            method_note=(
                "データは role_flow.json 由来。ノード = 役職カテゴリ、"
                "リンク = 役職間の人物遷移。リンク幅は遷移件数に比例。"
                "可読性のため上位40リンクを表示。"
                "ノード色は Plotly Pastel パレット由来。"
            ),
            interpretation_html=(
                "<p>特定の役職ペア間で太いバンドが見られる場合、"
                "一般的なキャリア遷移パターンを示している。"
                "これは業界の役職進行慣習を反映する可能性もあるが、"
                "役職カテゴリの分類方法にも影響される。"
                "代替解釈として、2つの役職間の頻繁な遷移は、"
                "固定的なキャリアパスではなく役職の流動性を示す可能性もある。</p>"
            ),
            section_id="role_flow_sankey",
        )

    # ------------------------------------------------------------------
    # Section 6: Stage Median Arrival Years
    # ------------------------------------------------------------------

    def _build_stage_median_years_section(
        self,
        sb: SectionBuilder,
    ) -> ReportSection:
        """Horizontal bar of median years to each stage."""
        avg_time = (
            self._transitions.get("avg_time_to_stage", {}) if self._transitions else {}
        )
        if not avg_time:
            return ReportSection(
                title="ステージ別 中央値到達年数",
                findings_html="<p>ステージ到達時間データが利用できません。</p>",
                section_id="stage_median_years",
            )

        stage_labels: list[str] = []
        median_vals: list[float] = []
        sample_sizes: list[int] = []
        for stage_id in sorted(avg_time.keys(), key=int):
            sd = avg_time[stage_id]
            stage_labels.append(sd.get("label", f"Stage {stage_id}"))
            median_vals.append(sd.get("median_years", 0))
            sample_sizes.append(sd.get("sample_size", 0))

        if not stage_labels:
            return ReportSection(
                title="ステージ別 中央値到達年数",
                findings_html="<p>ステージラベルが見つかりません。</p>",
                section_id="stage_median_years",
            )

        med_summ = distribution_summary(median_vals, label="median_years")

        findings = (
            f"<p>{len(stage_labels)}ステージにわたる中央値到達年数。 "
            f"ステージ中央値の分布: {format_distribution_inline(med_summ)}, "
            f"{format_ci((med_summ['ci_lower'], med_summ['ci_upper']))}。</p>"
        )

        n_stages = len(stage_labels)
        colors = _STAGE_COLORS_BAR[:n_stages]

        fig = go.Figure(
            go.Bar(
                y=stage_labels[::-1],
                x=median_vals[::-1],
                orientation="h",
                marker_color=colors[:n_stages][::-1],
                text=[
                    f"{v:.1f}yr (n={n})"
                    for v, n in zip(median_vals[::-1], sample_sizes[::-1])
                ],
                textposition="outside",
                hovertemplate="%{y}: median %{x:.1f} years<extra></extra>",
            )
        )
        fig.update_layout(
            title="ステージ別 中央値到達年数",
            xaxis_title="中央値年数",
            height=max(350, n_stages * 50),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="ステージ別 中央値到達年数",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_stage_median",
                height=max(350, n_stages * 50),
            ),
            method_note=(
                "中央年数は transitions.json の avg_time_to_stage 由来。"
                "各バーはデビューから該当ステージ到達までの"
                "年数中央値を示す。サンプル数（n）をバーごとに表示。"
            ),
            section_id="stage_median_years",
        )

    # ------------------------------------------------------------------
    # Section 7: Career Speed Density
    # ------------------------------------------------------------------

    def _build_career_speed_section(self, sb: SectionBuilder) -> ReportSection:
        """Density scatter: credits/year vs stage progression speed."""
        scores = self._scores
        if not scores or not isinstance(scores, list):
            return ReportSection(
                title="キャリア速度分析",
                findings_html="<p>スコアデータが利用できません。</p>",
                section_id="career_speed",
            )

        speed_x: list[float] = []
        speed_y: list[float] = []
        speed_names: list[str] = []
        for p in scores:
            active_yrs = safe_nested(p, "career", "active_years", default=0)
            total_cred = float(p.get("total_credits", 0))
            highest_stage = safe_nested(p, "career", "highest_stage", default=0)
            if active_yrs >= 2 and total_cred > 0:
                speed_x.append(total_cred / active_yrs)
                speed_y.append(highest_stage / active_yrs)
                speed_names.append(p.get("name", p.get("person_id", "")))

        if len(speed_x) <= 10:
            return ReportSection(
                title="キャリア速度分析",
                findings_html=(
                    "<p>キャリア速度分析に必要なデータが不足しています"
                    f"（n={len(speed_x)}）。</p>"
                ),
                section_id="career_speed",
            )

        x_summ = distribution_summary(speed_x, label="credits_per_year")
        y_summ = distribution_summary(speed_y, label="stage_speed")

        findings = (
            f"<p>キャリア速度散布図: {len(speed_x):,}人"
            f"（active_years >= 2、total_credits > 0）。 "
            f"年間クレジット数: {format_distribution_inline(x_summ)}, "
            f"{format_ci((x_summ['ci_lower'], x_summ['ci_upper']))}。 "
            f"ステージ進行速度（ステージ/年）: "
            f"{format_distribution_inline(y_summ)}, "
            f"{format_ci((y_summ['ci_lower'], y_summ['ci_upper']))}。</p>"
        )

        fig = density_scatter_2d(
            speed_x,
            speed_y,
            xlabel="年間クレジット数",
            ylabel="ステージ進行速度（stage/年）",
            title="キャリア速度 -- 年間クレジット vs 年間ステージ進行",
            label_names=speed_names,
            label_top=10,
            height=520,
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="キャリア速度分析（密度）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_career_speed",
                height=520,
            ),
            method_note=(
                "X = total_credits / active_years。"
                "Y = highest_stage / active_years。"
                "フィルタ: active_years ≥ 2、total_credits > 0。"
                "n > 500 で密度等高線、小サンプルは散布図を表示。"
                "Y 上位10点にラベル付与。"
            ),
            interpretation_html=(
                "<p>制作活動量とステージ進行速度の関係は"
                "異なるキャリア戦略を反映している可能性がある。"
                "年間クレジット数が多いがステージ進行速度が遅い場合、"
                "1つの役職カテゴリ内での専門化を示唆する。"
                "代替解釈として、ステージ進行速度はステージ数（0〜6）で"
                "上限が定まっているため、長期キャリアの人物は"
                "実際の昇進とは関係なく stage/年 が低くなる。</p>"
            ),
            section_id="career_speed",
        )

    # ------------------------------------------------------------------
    # Section 8: Stage-based IV Distribution
    # ------------------------------------------------------------------

    def _build_stage_iv_section(self, sb: SectionBuilder) -> ReportSection:
        """Ridge plot of log1p(IV Score) by highest career stage."""
        scores = self._scores
        if not scores or not isinstance(scores, list):
            return ReportSection(
                title="ステージ別 IV Score分布",
                findings_html="<p>スコアデータが利用できません。</p>",
                section_id="stage_iv",
            )

        stage_iv: dict[str, list[float]] = {}
        for p in scores:
            hs = int(safe_nested(p, "career", "highest_stage", default=0))
            if 0 <= hs <= 6:
                label = _STAGE_NAMES[hs]
                stage_iv.setdefault(label, []).append(
                    float(np.log1p(p.get("iv_score", 0))),
                )

        if not stage_iv:
            return ReportSection(
                title="ステージ別 IV Score分布",
                findings_html="<p>ステージ別IVデータが利用できません。</p>",
                section_id="stage_iv",
            )

        findings = "<p>キャリアステージ別のIV score（log1p変換）:</p><ul>"
        for label in _STAGE_NAMES:
            if label in stage_iv:
                s = distribution_summary(stage_iv[label], label=label)
                findings += (
                    f"<li><strong>{label}</strong> (n={s['n']:,}): "
                    f"{format_distribution_inline(s)}, "
                    f"{format_ci((s['ci_lower'], s['ci_upper']))}</li>"
                )
        findings += "</ul>"

        fig = ridge_plot(
            stage_iv,
            title="キャリアステージ別 IV Score分布（log1p変換）",
            xlabel="log1p(IV Score)",
            height=450,
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="ステージ別 IV Score分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_stage_iv",
                height=450,
            ),
            method_note=(
                "IV scores from scores.json, grouped by highest_stage (0-6). "
                "log1p transformation applied to handle right-skewed "
                "distribution. Ridge plot uses KDE estimation per group. "
                "IV = Integrated Value (network-derived, no viewer ratings)."
            ),
            section_id="stage_iv",
        )

    # ------------------------------------------------------------------
    # Section 9: Director Credit Ranking (top 50)
    # ------------------------------------------------------------------

    def _build_director_ranking_section(
        self,
        sb: SectionBuilder,
    ) -> ReportSection:
        """Top 50 directors by credit count, colored by dominant scale."""
        profiles = self._dir_profiles
        if not profiles:
            return ReportSection(
                title="監督クレジット保有者 上位50名",
                findings_html="<p>監督プロフィールデータが利用できません。</p>",
                section_id="director_ranking",
            )

        top50 = profiles[:50]
        names = [p.name for p in top50]
        totals = [p.total_director_credits for p in top50]
        spans = [
            f"{p.first_year}-{p.latest_year}" if p.first_year else "N/A" for p in top50
        ]
        dominants = [
            SCALE_KEY_LABELS.get(p.dominant_type, p.dominant_type) for p in top50
        ]

        total_directors = len(profiles)
        total_summ = distribution_summary(
            [p.total_director_credits for p in profiles],
            label="director_credits",
        )

        findings = (
            f"<p>監督クレジット3件以上の監督: {total_directors:,}人。 "
            f"クレジット数の分布: {format_distribution_inline(total_summ)}, "
            f"{format_ci((total_summ['ci_lower'], total_summ['ci_upper']))}。 "
            f"最多監督: {names[0]}（{totals[0]:,}クレジット）。 "
            f"クレジット数上位50人を主要作品規模別に色分け表示。</p>"
        )

        bar_colors = []
        for d in dominants[::-1]:
            bar_colors.append(_SCALE_COLORS_MAP.get(d, "#888"))

        fig = go.Figure(
            go.Bar(
                y=names[::-1],
                x=totals[::-1],
                orientation="h",
                marker_color=bar_colors,
                customdata=list(zip(spans[::-1], dominants[::-1])),
                hovertemplate=(
                    "<b>%{y}</b><br>Director credits: %{x}<br>"
                    "Period: %{customdata[0]}<br>"
                    "Dominant scale: %{customdata[1]}<extra></extra>"
                ),
                text=dominants[::-1],
                textposition="outside",
            )
        )
        fig.update_layout(
            title="監督クレジット数 上位50名（色=主要作品規模）",
            xaxis_title="監督クレジット数",
            height=max(600, len(top50) * 18),
            margin=dict(l=160),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="「監督」クレジット保有者 上位50名",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_dir_rank",
                height=max(600, len(top50) * 18),
            ),
            method_note=(
                "監督クレジットは credits テーブルの "
                "role = 'director' で集計。最低3クレジット必要。"
                "支配スケール = 監督ごとに最もクレジットが多いスケールカテゴリ。"
                "スケールカテゴリ: TV 大/中/小、"
                "単発作品（tanpatsu）大/中/小。"
            ),
            section_id="director_ranking",
        )

    # ------------------------------------------------------------------
    # Section 10: Director Dominant Type Distribution
    # ------------------------------------------------------------------

    def _build_director_dominant_type_section(
        self,
        sb: SectionBuilder,
    ) -> ReportSection:
        """Distribution of dominant work scale across all directors."""
        profiles = self._dir_profiles
        if not profiles:
            return ReportSection(
                title="監督のドミナント規模 分布",
                findings_html="<p>監督プロフィールデータが利用できません。</p>",
                section_id="director_dominant_type",
            )

        dom_counts = Counter(
            SCALE_KEY_LABELS.get(p.dominant_type, p.dominant_type) for p in profiles
        )
        dom_labels = list(dom_counts.keys())
        dom_vals = [dom_counts[k] for k in dom_labels]

        findings = (
            f"<p>{len(profiles):,}人の監督におけるドミナント規模分布:</p>"
            "<ul>"
        )
        for label in dom_labels:
            cnt = dom_counts[label]
            pct = cnt / len(profiles) * 100
            findings += f"<li><strong>{label}</strong>: {cnt:,} ({pct:.1f}%)</li>"
        findings += "</ul>"

        fig = go.Figure(
            go.Bar(
                x=dom_labels,
                y=dom_vals,
                marker_color=[_SCALE_COLORS_MAP.get(lb, "#888") for lb in dom_labels],
                hovertemplate="%{x}: %{y} directors<extra></extra>",
                text=dom_vals,
                textposition="outside",
            )
        )
        fig.update_layout(
            title="監督の主要作品規模 分布（全監督）",
            xaxis_title="主要作品規模",
            yaxis_title="監督数",
            height=380,
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="監督のドミナント規模 分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_dir_dominant",
                height=380,
            ),
            method_note=(
                "Dominant type = scale category with the most director credits "
                "per person. Categories: TV large (long-run/large staff), "
                "TV medium, TV small, tanpatsu (movie/OVA) large, medium, small."
            ),
            section_id="director_dominant_type",
        )

    # ------------------------------------------------------------------
    # Section 11: Director Scale Profile (stacked bar + heatmap)
    # ------------------------------------------------------------------

    def _build_director_scale_profile_section(
        self,
        sb: SectionBuilder,
    ) -> ReportSection:
        """Stacked bar (top 60) + heatmap (top 40) of director scale profiles."""
        profiles = self._dir_profiles
        if not profiles:
            return ReportSection(
                title="規模別監督プロフィール",
                findings_html="<p>監督プロフィールデータが利用できません。</p>",
                section_id="director_scale_profile",
            )

        top60 = profiles[:60]
        top40 = profiles[:40]

        findings = (
            f"<p>上位{len(top60)}人の監督の規模構成"
            f"（積み上げ棒グラフ、規模別クレジット%）と上位{len(top40)}人"
            f"（ヒートマップ）。各棒/行は1人の監督のクレジットが"
            f"6つの規模カテゴリにわたる分布を表す。</p>"
        )

        # Stacked bar chart
        fig_stack = go.Figure()
        for sk in SCALE_KEYS_ORDERED:
            vals = [p.scale_fractions.get(sk, 0.0) * 100 for p in top60]
            fig_stack.add_trace(
                go.Bar(
                    name=SCALE_KEY_LABELS[sk],
                    y=[p.name for p in top60],
                    x=vals,
                    orientation="h",
                    marker_color=_SCALE_COLORS_KEY[sk],
                    hovertemplate=(
                        f"<b>%{{y}}</b> -- {SCALE_KEY_LABELS[sk]}: "
                        f"%{{x:.1f}}%<extra></extra>"
                    ),
                )
            )
        fig_stack.update_layout(
            barmode="stack",
            title="監督クレジットの作品規模内訳（%）上位60名",
            xaxis_title="割合 (%)",
            xaxis=dict(range=[0, 100]),
            height=max(700, len(top60) * 14),
            legend=dict(orientation="h", y=1.02),
            margin=dict(l=160),
        )

        # Heatmap
        hm_z = [
            [p.scale_fractions.get(sk, 0.0) * 100 for sk in SCALE_KEYS_ORDERED]
            for p in top40
        ]
        fig_hm = go.Figure(
            go.Heatmap(
                z=hm_z,
                x=[SCALE_KEY_LABELS[sk] for sk in SCALE_KEYS_ORDERED],
                y=[p.name for p in top40],
                colorscale="Plasma",
                text=[[f"{v:.0f}%" for v in row] for row in hm_z],
                texttemplate="%{text}",
                hovertemplate=("<b>%{y}</b><br>%{x}: %{z:.1f}%<extra></extra>"),
                zmin=0,
                zmax=100,
            )
        )
        fig_hm.update_layout(
            title="監督 x 作品規模 ヒートマップ（%）",
            height=max(600, len(top40) * 18),
            margin=dict(l=160),
        )

        viz = plotly_div_safe(
            fig_stack,
            "chart_dir_stack",
            height=max(700, len(top60) * 14),
        ) + plotly_div_safe(
            fig_hm,
            "chart_dir_heatmap",
            height=max(600, len(top40) * 18),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="規模別監督プロフィール",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "スケール比率はスケールカテゴリ別クレジット数を"
                "分類済み総クレジット数で除して算出。積み上げ棒: 各セグメント"
                "= 該当スケールのクレジット割合。ヒートマップ: 同一データを"
                "行列形式で表示。カテゴリ: tv_large、tv_medium、tv_small、"
                "tanpatsu_large、tanpatsu_medium、tanpatsu_small。"
            ),
            section_id="director_scale_profile",
        )

    # ------------------------------------------------------------------
    # Section 12: Director Career Span by Dominant Type
    # ------------------------------------------------------------------

    def _build_director_career_span_section(
        self,
        sb: SectionBuilder,
    ) -> ReportSection:
        """Violin of career span by dominant work scale."""
        profiles = self._dir_profiles
        if not profiles:
            return ReportSection(
                title="規模別 監督キャリアスパン",
                findings_html="<p>監督プロフィールデータが利用できません。</p>",
                section_id="director_career_span",
            )

        span_by_dom: dict[str, list[int]] = {}
        for p in profiles:
            if p.career_span:
                lb = SCALE_KEY_LABELS.get(p.dominant_type, p.dominant_type)
                span_by_dom.setdefault(lb, []).append(p.career_span)

        if len(span_by_dom) < 2:
            return ReportSection(
                title="規模別 監督キャリアスパン",
                findings_html=(
                    "<p>比較に必要な規模カテゴリが不足しています"
                    f"（{len(span_by_dom)}カテゴリ）。</p>"
                ),
                section_id="director_career_span",
            )

        findings = "<p>ドミナント作品規模別のキャリアスパン（年数）:</p><ul>"
        for lb, spans in sorted(span_by_dom.items()):
            s = distribution_summary(
                [float(x) for x in spans],
                label=lb,
            )
            findings += (
                f"<li><strong>{lb}</strong> (n={s['n']:,}): "
                f"{format_distribution_inline(s)}, "
                f"{format_ci((s['ci_lower'], s['ci_upper']))}</li>"
            )
        findings += "</ul>"

        fig = go.Figure()
        for lb, spans in sorted(span_by_dom.items()):
            fig.add_trace(
                go.Violin(
                    x=[lb] * len(spans),
                    y=spans,
                    name=lb,
                    box_visible=True,
                    meanline_visible=True,
                    fillcolor=_SCALE_COLORS_MAP.get(lb, "#888"),
                    line_color="#fff",
                    opacity=0.8,
                )
            )
        fig.update_layout(
            title="主要規模別 監督キャリアスパン分布",
            xaxis_title="主要作品規模",
            yaxis_title="キャリアスパン（年）",
            violinmode="group",
            height=420,
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="規模別 監督キャリアスパン",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_dir_span",
                height=420,
            ),
            method_note=(
                "キャリアスパン = latest_year - first_year + 1（監督クレジットのみ）。"
                "支配スケールカテゴリでグルーピング。バイオリンプロットは分布全体を表示し、"
                "ボックスプロットを重ね描画。平均線を表示。"
            ),
            interpretation_html=(
                "<p>特定スケールカテゴリでキャリアスパンが長い場合、"
                "そのカテゴリが持続的な仕事機会を提供している、あるいは"
                "長期キャリアの監督がそうしたカテゴリに集中する可能性がある。"
                "代替解釈として、小規模カテゴリの短いスパンは"
                "実際のキャリア終了ではなく、データ記録のギャップを"
                "反映している可能性もある。</p>"
            ),
            section_id="director_career_span",
        )

    # ------------------------------------------------------------------
    # Section 13: Director Years to First Direction by Scale (ridge)
    # ------------------------------------------------------------------

    def _build_director_years_to_debut_section(
        self,
        sb: SectionBuilder,
    ) -> ReportSection:
        """Ridge plot of years from first credit to first director credit by scale."""
        traj = self._dir_trajectories
        if not traj:
            return ReportSection(
                title="規模別 初監督デビューまでの経験年数",
                findings_html="<p>監督軌跡データが利用できません。</p>",
                section_id="director_years_to_debut",
            )

        groups = {
            SCALE_KEY_LABELS[sk]: traj.years_to_first_dir_by_scale.get(sk, [])
            for sk in SCALE_KEYS_ORDERED
            if traj.years_to_first_dir_by_scale.get(sk)
        }
        if not groups:
            return ReportSection(
                title="規模別 初監督デビューまでの経験年数",
                findings_html="<p>規模別の初監督デビュー年データが利用できません。</p>",
                section_id="director_years_to_debut",
            )

        findings = (
            "<p>業界初クレジットから監督初クレジットまでの年数"
            "（規模カテゴリ別）:</p><ul>"
        )
        for label, vals in groups.items():
            fvals = [float(v) for v in vals]
            s = distribution_summary(fvals, label=label)
            findings += (
                f"<li><strong>{label}</strong> (n={s['n']:,}): "
                f"{format_distribution_inline(s)}, "
                f"{format_ci((s['ci_lower'], s['ci_upper']))}</li>"
            )
        findings += "</ul>"

        fig = ridge_plot(
            {k: [float(v) for v in vs] for k, vs in groups.items()},
            title="規模別 初監督デビューまでの経験年数 (Ridge Plot)",
            xlabel="経験年数（初クレジットから）",
            height=max(450, len(groups) * 75),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="規模別 初監督デビューまでの経験年数",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_dir_debut_years",
                height=max(450, len(groups) * 75),
            ),
            method_note=(
                "初監督クレジット到達年数 = 該当スケールで監督役職が付いた初年 − "
                "何らかのクレジットが付いた初年。"
                "compute_director_trajectories() で算出。"
                "リッジプロットはグループごとに KDE 推定を使用。"
            ),
            interpretation_html=(
                "<p>初監督到達までの中央年数が短いスケールカテゴリは、"
                "参入障壁が低いか制作構造が異なる可能性がある。"
                "例: TVシリーズは話数監督の機会を早期に提供し得る。"
                "代替解釈として、小規模カテゴリで監督する人物の"
                "監督前キャリアが短く見えるのは、小規模作品に"
                "専門役職が少ないためであり、昇進自体が速いとは限らない。</p>"
            ),
            section_id="director_years_to_debut",
        )

    # ------------------------------------------------------------------
    # Section 14: Director Scale Mobility (heatmap + sankey)
    # ------------------------------------------------------------------

    def _build_director_mobility_section(
        self,
        sb: SectionBuilder,
    ) -> ReportSection:
        """Scale mobility heatmap and Sankey."""
        traj = self._dir_trajectories
        if not traj:
            return ReportSection(
                title="規模間モビリティ分析",
                findings_html="<p>監督軌跡データが利用できません。</p>",
                section_id="director_mobility",
            )

        # Mobility heatmap
        prob_z = [
            [
                traj.transition_probs.get((sk_f, sk_t), 0.0) * 100
                for sk_t in SCALE_KEYS_ORDERED
            ]
            for sk_f in SCALE_KEYS_ORDERED
        ]
        row_labels = [SCALE_KEY_LABELS[sk] for sk in SCALE_KEYS_ORDERED]
        row_ns = [
            sum(
                traj.transition_counts.get((sk_f, sk_t), 0)
                for sk_t in SCALE_KEYS_ORDERED
            )
            for sk_f in SCALE_KEYS_ORDERED
        ]
        ylabels_with_n = [
            f"{SCALE_KEY_LABELS[sk]}  (n={row_ns[i]})"
            for i, sk in enumerate(SCALE_KEYS_ORDERED)
        ]

        total_mobility = sum(row_ns)
        findings = (
            f"<p>規模間モビリティ行列: 6つの規模カテゴリにわたり"
            f"{total_mobility:,}件の監督-年遷移を観測。 "
            f"各行は翌年（3年以内）に各規模カテゴリで"
            f"監督する確率を示す。</p>"
        )

        fig_mob = go.Figure(
            go.Heatmap(
                z=prob_z,
                x=row_labels,
                y=ylabels_with_n,
                colorscale="Plasma",
                text=[[f"{v:.1f}%" for v in row] for row in prob_z],
                texttemplate="%{text}",
                hovertemplate=(
                    "<b>%{y} -> %{x}</b><br>Probability: %{z:.1f}%<extra></extra>"
                ),
                zmin=0,
                zmax=60,
            )
        )
        fig_mob.update_layout(
            title="規模間モビリティ行列 -- 翌年の監督作品規模",
            xaxis_title="翌年の規模",
            yaxis_title="当年の規模",
            height=420,
        )

        viz = plotly_div_safe(fig_mob, "chart_dir_mobility", height=420)

        # Sankey
        sankey_links = []
        for sk_f in SCALE_KEYS_ORDERED:
            for sk_t in SCALE_KEYS_ORDERED:
                cnt = traj.transition_counts.get((sk_f, sk_t), 0)
                if cnt >= 10:
                    sankey_links.append((sk_f, sk_t, cnt))

        if sankey_links:
            from_nodes = [
                f"{SCALE_KEY_LABELS[sk]}（前年）" for sk in SCALE_KEYS_ORDERED
            ]
            to_nodes = [f"{SCALE_KEY_LABELS[sk]}（翌年）" for sk in SCALE_KEYS_ORDERED]
            all_nodes = from_nodes + to_nodes
            node_idx = {n: i for i, n in enumerate(all_nodes)}
            sk_colors = [
                "#F72585",
                "#7209b7",
                "#4cc9f0",
                "#f77f00",
                "#fcbf49",
                "#2ec4b6",
            ]
            node_colors = sk_colors + sk_colors

            src = [
                node_idx[f"{SCALE_KEY_LABELS[sk_f]}（前年）"]
                for sk_f, _, _ in sankey_links
            ]
            tgt = [
                node_idx[f"{SCALE_KEY_LABELS[sk_t]}（翌年）"]
                for _, sk_t, _ in sankey_links
            ]
            vals = [cnt for _, _, cnt in sankey_links]

            fig_sankey = go.Figure(
                go.Sankey(
                    node=dict(
                        pad=15,
                        thickness=20,
                        label=all_nodes,
                        color=node_colors,
                    ),
                    link=dict(
                        source=src,
                        target=tgt,
                        value=vals,
                        color=["rgba(180,180,200,0.3)"] * len(sankey_links),
                    ),
                )
            )
            fig_sankey.update_layout(
                title=("監督の規模間フロー（前年→翌年、>= 10 transitions）"),
                height=480,
            )
            viz += plotly_div_safe(
                fig_sankey,
                "chart_dir_mobility_sankey",
                height=480,
            )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="規模間モビリティ分析",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "モビリティ行列は compute_director_trajectories() 由来。"
                "遷移 = 同一監督が年 T にスケール X で監督し、"
                "T+1 〜 T+3 にスケール Y で監督した場合。"
                "確率は行方向で正規化。Sankey は10件以上の遷移のみ表示。"
                "左 = 当年スケール、右 = 翌年以降スケール。"
            ),
            interpretation_html=(
                "<p>モビリティ行列で対角値が高いことはスケール持続性を示し、"
                "監督が同じスケールカテゴリに留まる傾向がある。"
                "非対角エントリはスケール間モビリティを定量化する。"
                "代替解釈として、見かけ上の持続性は監督の選好ではなく"
                "複数年契約など制作契約構造を反映している可能性がある。"
                "また、3年のギャップウィンドウはクレジット疎な監督の"
                "遷移を過剰カウントする可能性がある。</p>"
            ),
            section_id="director_mobility",
        )

    # ------------------------------------------------------------------
    # Section 15: Director Experience vs Achieved Scale (density scatter)
    # ------------------------------------------------------------------

    def _build_director_exp_vs_scale_section(
        self,
        sb: SectionBuilder,
    ) -> ReportSection:
        """Density scatter of director career span vs highest achieved scale."""
        profiles = self._dir_profiles
        if not profiles:
            return ReportSection(
                title="監督経験スパン x 達成規模",
                findings_html="<p>監督プロフィールデータが利用できません。</p>",
                section_id="director_exp_vs_scale",
            )

        scatter_x: list[float] = []
        scatter_y: list[float] = []
        scatter_label: list[str] = []
        for p in profiles:
            if not p.first_year or not p.scale_counts:
                continue
            best_scale = max(
                p.scale_counts,
                key=lambda k: _SCALE_RANK.get(k, -1),
            )
            best_rank = _SCALE_RANK.get(best_scale, -1)
            exp_years = (p.latest_year or p.first_year) - p.first_year
            if exp_years >= 0 and best_rank >= 0:
                scatter_x.append(float(exp_years))
                jitter = float(np.random.default_rng(42).uniform(-0.15, 0.15))
                scatter_y.append(best_rank + jitter)
                scatter_label.append(p.name)

        if len(scatter_x) <= 50:
            return ReportSection(
                title="監督経験スパン x 達成規模",
                findings_html=(
                    f"<p>経験 vs 規模散布図に必要なデータが不足しています"
                    f"（n={len(scatter_x)}）。</p>"
                ),
                section_id="director_exp_vs_scale",
            )

        x_summ = distribution_summary(scatter_x, label="experience_span")
        y_summ = distribution_summary(scatter_y, label="max_scale_rank")

        findings = (
            f"<p>{len(scatter_x):,}人の監督における経験スパン vs 達成規模。 "
            f"経験スパン: {format_distribution_inline(x_summ)}, "
            f"{format_ci((x_summ['ci_lower'], x_summ['ci_upper']))}。 "
            f"規模ランク（0=tanpatsu_small, 5=tv_large）: "
            f"{format_distribution_inline(y_summ)}, "
            f"{format_ci((y_summ['ci_lower'], y_summ['ci_upper']))}。</p>"
        )

        fig = density_scatter_2d(
            scatter_x,
            scatter_y,
            xlabel="監督キャリアスパン（年）",
            ylabel="最高スケールランク（0=tanpatsu_small、5=tv_large）",
            title="監督経験スパン vs 達成規模",
            label_names=scatter_label,
            label_top=15,
            height=500,
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="監督経験スパン x 達成規模",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_dir_exp_scale",
                height=500,
            ),
            method_note=(
                "X = latest_year - first_year（監督クレジットのみ）。"
                "Y = scale_counts の全キーの中の最大スケールランク。"
                "スケールランク: tanpatsu_small=0、tv_small=1、tanpatsu_medium=2、"
                "tv_medium=3、tanpatsu_large=4、tv_large=5。"
                "視認性のため Y 軸に ±0.15 のジッターを付与。"
                "n > 500 で密度等高線、小サンプルは散布図。"
            ),
            interpretation_html=(
                "<p>大規模作品の機会が業界経験を要するのであれば、"
                "キャリアスパンと達成スケールランクの正の相関は予想通り。"
                "ただし、スパンが短く高スケールランクに到達した監督は、"
                "上級ポジションや関連分野から直接参入した可能性がある。"
                "代替解釈として、この関連は部分的にトートロジーとも言える — "
                "監督年数が多いほど機械的に高スケール作品に到達する"
                "機会が増えるためである。</p>"
            ),
            section_id="director_exp_vs_scale",
        )
