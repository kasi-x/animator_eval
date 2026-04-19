# DEPRECATED (Phase 3-5, 2026-04-19): merged into mgmt_team_chemistry. チーム構造章に統合.
# This module is retained in archived/ for regeneration and audit only.
# It is NOT in V2_REPORT_CLASSES and will not run in default generation.
"""Team Analysis report — v2 compliant.

Covers production team structure by scale tier:
- Section 1: Team size distribution by scale tier
- Section 2: Role composition by scale tier
- Section 3: Crew repeat-collaboration rate
- Section 4: Team network cohesion metrics
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}


class TeamAnalysisReport(BaseReportGenerator):
    name = "team_analysis"
    title = "制作チーム構造分析"
    subtitle = "スケールTier別チームサイズ・役職構成・リピート率"
    filename = "team_analysis.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_team_size_section(sb)))
        sections.append(sb.build_section(self._build_role_composition_section(sb)))
        sections.append(sb.build_section(self._build_repeat_collab_section(sb)))
        sections.append(sb.build_section(self._build_cohesion_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Team size by tier ──────────────────────────────

    def _build_team_size_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fwc.scale_tier AS tier,
                    fwc.anime_id,
                    COUNT(DISTINCT c.person_id) AS team_size
                FROM feat_work_context fwc
                JOIN credits c ON c.anime_id = fwc.anime_id
                WHERE fwc.scale_tier IS NOT NULL
                GROUP BY fwc.scale_tier, fwc.anime_id
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="Tier別チームサイズ",
                findings_html="<p>チームサイズデータが取得できませんでした。</p>",
                section_id="team_size",
            )

        tier_sizes: dict[int, list[int]] = {}
        for r in rows:
            tier_sizes.setdefault(r["tier"], []).append(r["team_size"])

        findings = "<p>作品あたりのクレジット人数（スケールTier別）：</p><ul>"
        for t in sorted(tier_sizes):
            ts = distribution_summary(tier_sizes[t], label=f"tier{t}")
            findings += (
                f"<li><strong>Tier {t}</strong> (n={ts['n']:,} 作品): "
                f"{format_distribution_inline(ts)}, "
                f"{format_ci((ts['ci_lower'], ts['ci_upper']))}</li>"
            )
        findings += "</ul>"

        # Chart 1: Box plot — tier別チームサイズ分布
        fig_box = go.Figure()
        for t in sorted(tier_sizes):
            fig_box.add_trace(go.Box(
                y=tier_sizes[t][:1000] if len(tier_sizes[t]) > 1000 else tier_sizes[t],
                name=f"Tier {t}",
                marker_color=_TIER_COLORS.get(t, "#a0a0c0"),
                boxpoints=False,
            ))
        fig_box.update_layout(
            title="Tier別チームサイズ分布",
            xaxis_title="スケールTier", yaxis_title="チームサイズ（人数）",
        )

        # Chart 2: Trend — 年代別×Tier別 平均チームサイズ推移
        try:
            decade_rows = self.conn.execute("""
                SELECT
                    (a.year / 10) * 10 AS decade,
                    fwc.scale_tier AS tier,
                    AVG(team_counts.team_size) AS avg_size
                FROM feat_work_context fwc
                JOIN anime a ON a.id = fwc.anime_id
                JOIN (
                    SELECT anime_id, COUNT(DISTINCT person_id) AS team_size
                    FROM credits GROUP BY anime_id
                ) team_counts ON team_counts.anime_id = fwc.anime_id
                WHERE a.year BETWEEN 1970 AND 2024
                  AND fwc.scale_tier IS NOT NULL
                GROUP BY decade, fwc.scale_tier
                ORDER BY decade, fwc.scale_tier
            """).fetchall()
        except Exception:
            decade_rows = []

        fig_trend = go.Figure()
        if decade_rows:
            decade_tier: dict[tuple[int, int], float] = {}
            decades_set: set[int] = set()
            tiers_set: set[int] = set()
            for r in decade_rows:
                decade_tier[(r["decade"], r["tier"])] = r["avg_size"]
                decades_set.add(r["decade"])
                tiers_set.add(r["tier"])
            decades_sorted = sorted(decades_set)
            for t in sorted(tiers_set):
                fig_trend.add_trace(go.Scatter(
                    x=decades_sorted,
                    y=[decade_tier.get((d, t), None) for d in decades_sorted],
                    name=f"Tier {t}",
                    line=dict(color=_TIER_COLORS.get(t, "#a0a0c0")),
                    hovertemplate=f"T{t} %{{x}}s: %{{y:.1f}}<extra></extra>",
                ))
        fig_trend.update_layout(
            title="年代別×Tier別 平均チームサイズ推移",
            xaxis_title="年代", yaxis_title="平均チームサイズ",
        )

        chart_html = (
            plotly_div_safe(fig_box, "chart_team_size", height=420)
            + plotly_div_safe(fig_trend, "chart_team_trend", height=420)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="Tier別チームサイズ",
            findings_html=findings,
            visualization_html=chart_html,
            method_note=(
                "チームサイズ = 作品ごとの credits における COUNT(DISTINCT person_id)。"
                "スケールTierは feat_work_context 由来（1=マイクロ、5=メジャー）。"
                "1作品で複数役職を兼任する同一人物は1人として計上。"
                "credits.role が声優役職の場合は声優を除外 — "
                "解釈前に credits テーブルの role カラム網羅率を確認のこと。"
                "トレンドチャート: 年代 × Tier 別の AVG(team_size)。"
            ),
            interpretation_html=(
                "<p>スケールTierとチームサイズの正の関係は構造的に予想される。"
                "Tier分類自体がスタッフ数を入力の一つとして使用しているため、"
                "この関係はスケール効果の独立した証拠ではない。"
                "コア制作スタッフのみ（声優・音楽クレジットを除く）を用いた"
                "別のチームサイズ指標が、制作規模のより独立した尺度となる。</p>"
            ),
            section_id="team_size",
        )

    # ── Section 2: Role composition by tier ──────────────────────

    def _build_role_composition_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fwc.scale_tier AS tier,
                    c.role,
                    COUNT(*) AS n
                FROM feat_work_context fwc
                JOIN credits c ON c.anime_id = fwc.anime_id
                WHERE fwc.scale_tier IS NOT NULL
                  AND c.role IS NOT NULL
                GROUP BY fwc.scale_tier, c.role
                ORDER BY fwc.scale_tier, n DESC
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="Tier別役職構成",
                findings_html="<p>Tier別役職構成データが取得できませんでした。</p>",
                section_id="role_comp_tier",
            )

        tier_role: dict[int, dict[str, int]] = {}
        for r in rows:
            tier_role.setdefault(r["tier"], {})[r["role"]] = r["n"]

        findings = "<p>スケールTier別クレジット数上位5役職：</p><ul>"
        for t in sorted(tier_role):
            total = sum(tier_role[t].values())
            top5 = sorted(tier_role[t].items(), key=lambda x: -x[1])[:5]
            role_str = ", ".join(f"{role}: {100*n/total:.0f}%" for role, n in top5)
            findings += f"<li><strong>Tier {t}</strong> ({total:,} クレジット): {role_str}</li>"
        findings += "</ul>"

        # Chart 1: Stacked 100% bar — Tier別役職構成比
        all_roles: set[str] = set()
        for tier_d in tier_role.values():
            for role in list(tier_d.keys())[:8]:
                all_roles.add(role)
        top_roles = sorted(all_roles, key=lambda r: -sum(
            tier_role[t].get(r, 0) for t in tier_role
        ))[:8]

        fig = go.Figure()
        role_colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166",
                       "#667eea", "#f5576c", "#fda085", "#8a94a0"]
        tiers = sorted(tier_role.keys())
        tier_totals = {t: sum(tier_role[t].values()) for t in tiers}
        for i, role in enumerate(top_roles):
            fig.add_trace(go.Bar(
                x=[f"T{t}" for t in tiers],
                y=[100 * tier_role[t].get(role, 0) / max(tier_totals[t], 1) for t in tiers],
                name=role[:25],
                marker_color=role_colors[i % len(role_colors)],
            ))
        fig.update_layout(
            title="Tier別役職構成比 (%)",
            barmode="stack",
            xaxis_title="スケールTier", yaxis_title="クレジット比率 (%)",
        )

        # Chart 2: Role diversity — Tier別 ユニーク役職数の分布
        try:
            diversity_rows = self.conn.execute("""
                SELECT
                    fwc.scale_tier AS tier,
                    fwc.anime_id,
                    COUNT(DISTINCT c.role) AS n_roles
                FROM feat_work_context fwc
                JOIN credits c ON c.anime_id = fwc.anime_id
                WHERE fwc.scale_tier IS NOT NULL
                  AND c.role IS NOT NULL
                GROUP BY fwc.scale_tier, fwc.anime_id
            """).fetchall()
        except Exception:
            diversity_rows = []

        fig_diversity = go.Figure()
        if diversity_rows:
            tier_diversity: dict[int, list[int]] = {}
            for r in diversity_rows:
                tier_diversity.setdefault(r["tier"], []).append(r["n_roles"])

            for t in sorted(tier_diversity):
                fig_diversity.add_trace(go.Box(
                    y=tier_diversity[t][:1000] if len(tier_diversity[t]) > 1000 else tier_diversity[t],
                    name=f"Tier {t}",
                    marker_color=_TIER_COLORS.get(t, "#a0a0c0"),
                    boxpoints=False,
                ))

            # Add findings about diversity
            findings += "<p>Tier別作品あたりユニーク役職数：</p><ul>"
            for t in sorted(tier_diversity):
                ds = distribution_summary(tier_diversity[t], label=f"tier{t}")
                findings += (
                    f"<li><strong>Tier {t}</strong> (n={ds['n']:,} 作品): "
                    f"中央値={ds['median']:.0f} 役職, "
                    f"{format_ci((ds['ci_lower'], ds['ci_upper']))}</li>"
                )
            findings += "</ul>"

        fig_diversity.update_layout(
            title="Tier別 作品あたりユニーク役職数",
            xaxis_title="スケールTier", yaxis_title="ユニーク役職数",
        )

        chart_html = (
            plotly_div_safe(fig, "chart_role_comp_tier", height=420)
            + plotly_div_safe(fig_diversity, "chart_role_diversity", height=420)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="Tier別役職構成",
            findings_html=findings,
            visualization_html=chart_html,
            method_note=(
                "credits テーブル由来、role = 生の役職文字列（正規化なし）。"
                "全Tier合計で上位8役職を表示。"
                "役職文字列のタクソノミーはソースデータごとに異なり、正規化は適用していない。"
                "役職多様性 = 各Tier内でのアニメごとの COUNT(DISTINCT role)。"
            ),
            interpretation_html=(
                "<p>大規模Tierほど役職の多様性が高い傾向が見られる場合、"
                "これは制作工程の分業化を反映している可能性がある。"
                "ただし、クレジット記載の粒度がTierによって異なる"
                "（大規模作品ほど詳細にクレジットされる）可能性も排除できない。</p>"
            ),
            section_id="role_comp_tier",
        )

    # ── Section 3: Repeat collaboration rate ─────────────────────

    def _build_repeat_collab_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fwc.scale_tier AS tier,
                    fcc.person_id,
                    COUNT(DISTINCT fcc.anime_id) AS n_works
                FROM feat_credit_contribution fcc
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fwc.scale_tier IS NOT NULL
                GROUP BY fwc.scale_tier, fcc.person_id
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="リピート協業率",
                findings_html="<p>リピート協業データが取得できませんでした。</p>",
                section_id="repeat_collab",
            )

        tier_repeats: dict[int, list[int]] = {}
        for r in rows:
            tier_repeats.setdefault(r["tier"], []).append(r["n_works"])

        findings = "<p>Tier別 人物あたりの参加作品数（同一Tier内のクレジット数）：</p><ul>"
        for t in sorted(tier_repeats):
            vals = tier_repeats[t]
            ts = distribution_summary(vals, label=f"tier{t}")
            multi = sum(1 for v in vals if v > 1)
            pct_multi = 100 * multi / len(vals) if vals else 0
            findings += (
                f"<li><strong>Tier {t}</strong> (n={ts['n']:,} 人-Tierペア): "
                f"中央値={ts['median']:.1f} 作品, "
                f"2作品以上参加率={pct_multi:.0f}%</li>"
            )
        findings += "</ul>"

        # Chart 1: Box plot — Tier別参加作品数
        fig = go.Figure()
        for t in sorted(tier_repeats):
            vals = tier_repeats[t]
            fig.add_trace(go.Box(
                y=vals[:1000] if len(vals) > 1000 else vals,
                name=f"Tier {t}",
                marker_color=_TIER_COLORS.get(t, "#a0a0c0"),
                boxpoints=False,
            ))
        fig.update_layout(
            title="Tier別 人物あたり参加作品数",
            xaxis_title="スケールTier", yaxis_title="作品数（Tier内）",
        )

        # Chart 2: Core staff ratio — Tier別「コアスタッフ」（5作品以上）比率
        fig_core = go.Figure()
        core_data_tiers: list[str] = []
        core_data_pcts: list[float] = []
        core_data_colors: list[str] = []
        core_data_ns: list[int] = []
        for t in sorted(tier_repeats):
            vals = tier_repeats[t]
            n_core = sum(1 for v in vals if v >= 5)
            pct_core = 100 * n_core / len(vals) if vals else 0
            core_data_tiers.append(f"Tier {t}")
            core_data_pcts.append(pct_core)
            core_data_colors.append(_TIER_COLORS.get(t, "#a0a0c0"))
            core_data_ns.append(n_core)

        fig_core.add_trace(go.Bar(
            x=core_data_tiers,
            y=core_data_pcts,
            marker_color=core_data_colors,
            text=[f"{p:.1f}% (n={n})" for p, n in zip(core_data_pcts, core_data_ns)],
            textposition="auto",
            hovertemplate="%{x}: %{y:.1f}% コアスタッフ<extra></extra>",
        ))
        fig_core.update_layout(
            title="Tier別「コアスタッフ」比率（同一Tier内5作品以上参加者）",
            xaxis_title="スケールTier",
            yaxis_title="コアスタッフ比率 (%)",
        )

        # Add core staff findings
        findings += "<p>「コアスタッフ」（同一Tier内で5作品以上にクレジットされた人物）の比率：</p><ul>"
        for t_label, pct, n in zip(core_data_tiers, core_data_pcts, core_data_ns):
            findings += f"<li><strong>{t_label}</strong>: {pct:.1f}% (n={n:,} 人)</li>"
        findings += "</ul>"

        chart_html = (
            plotly_div_safe(fig, "chart_repeat_collab", height=400)
            + plotly_div_safe(fig_core, "chart_core_staff", height=400)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="リピート協業率",
            findings_html=findings,
            visualization_html=chart_html,
            method_note=(
                "n_works = scale_tier ごとに集計した、人物別のクレジット済みアニメのユニーク数。"
                "同一Tierの複数作品に登場する頻度を測るのであって、"
                "それらの作品で同じ仲間と働いたかどうかを測るものではない。"
                "真のリピートチーム測定にはペアワイズ共クレジット追跡が必要。"
                "コアスタッフ閾値: 同一Tier内で5作品以上。"
            ),
            interpretation_html=(
                "<p>コアスタッフ比率がTier間で異なる場合、高Tierほど固定チームでの制作が多い、"
                "あるいは高Tier作品に継続的に参加できる人材が限られている可能性がある。"
                "ただし、Tier分類が作品数と相関しているため、"
                "単に母数の違いが反映されている可能性もある。</p>"
            ),
            section_id="repeat_collab",
        )

    # ── Section 4: Team cohesion ──────────────────────────────────

    def _build_cohesion_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fwc.scale_tier AS tier,
                    AVG(fwc.n_staff) AS avg_staff,
                    AVG(fwc.n_distinct_roles) AS avg_roles,
                    COUNT(*) AS n_works
                FROM feat_work_context fwc
                WHERE fwc.scale_tier IS NOT NULL
                  AND fwc.n_staff IS NOT NULL
                GROUP BY fwc.scale_tier
                ORDER BY fwc.scale_tier
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="チーム凝集性指標",
                findings_html="<p>チーム凝集性データが取得できませんでした。</p>",
                section_id="team_cohesion",
            )

        findings = "<p>feat_work_contextに基づくTier別作品レベルチーム指標：</p><ul>"
        for r in rows:
            findings += (
                f"<li><strong>Tier {r['tier']}</strong> (n={r['n_works']:,} 作品): "
                f"平均スタッフ数={r['avg_staff']:.1f}, "
                f"平均ユニーク役職数={r['avg_roles']:.1f}</li>"
            )
        findings += (
            "</ul>"
            "<p>n_staffおよびn_distinct_rolesはfeat_work_contextの事前計算値であり、"
            "チーム凝集性の直接的な尺度ではなく制作規模の代理指標である点に留意。</p>"
        )

        # Chart 1: Bar — Tier別平均スタッフ数
        tiers = [r["tier"] for r in rows]
        avg_staff = [r["avg_staff"] for r in rows]
        colors = [_TIER_COLORS.get(t, "#a0a0c0") for t in tiers]

        fig = go.Figure(go.Bar(
            x=[f"T{t}" for t in tiers],
            y=avg_staff,
            marker_color=colors,
            hovertemplate="Tier %{x}: 平均スタッフ数=%{y:.1f}<extra></extra>",
        ))
        fig.update_layout(
            title="Tier別 平均スタッフ数 (n_staff)",
            xaxis_title="スケールTier", yaxis_title="平均スタッフ数",
        )

        # Chart 2: Scatter — n_staff vs n_distinct_roles by tier
        try:
            scatter_rows = self.conn.execute("""
                SELECT
                    fwc.scale_tier AS tier,
                    fwc.n_staff,
                    fwc.n_distinct_roles
                FROM feat_work_context fwc
                WHERE fwc.scale_tier IS NOT NULL
                  AND fwc.n_staff IS NOT NULL
                  AND fwc.n_distinct_roles IS NOT NULL
            """).fetchall()
        except Exception:
            scatter_rows = []

        fig_scatter = go.Figure()
        if scatter_rows:
            tier_scatter: dict[int, list[tuple[int, int]]] = {}
            for r in scatter_rows:
                tier_scatter.setdefault(r["tier"], []).append(
                    (r["n_staff"], r["n_distinct_roles"])
                )
            for t in sorted(tier_scatter):
                points = tier_scatter[t]
                # Sample if too many points
                if len(points) > 500:
                    import random
                    points = random.sample(points, 500)
                fig_scatter.add_trace(go.Scatter(
                    x=[p[0] for p in points],
                    y=[p[1] for p in points],
                    mode="markers",
                    name=f"Tier {t}",
                    marker=dict(
                        color=_TIER_COLORS.get(t, "#a0a0c0"),
                        size=5,
                        opacity=0.5,
                    ),
                    hovertemplate=(
                        f"Tier {t}<br>"
                        "スタッフ数: %{x}<br>"
                        "役職数: %{y}<extra></extra>"
                    ),
                ))
        fig_scatter.update_layout(
            title="スタッフ数 × ユニーク役職数（Tier別散布図）",
            xaxis_title="スタッフ数 (n_staff)",
            yaxis_title="ユニーク役職数 (n_distinct_roles)",
        )

        chart_html = (
            plotly_div_safe(fig, "chart_cohesion", height=380)
            + plotly_div_safe(fig_scatter, "chart_staff_roles_scatter", height=420)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="チーム凝集性指標",
            findings_html=findings,
            visualization_html=chart_html,
            method_note=(
                "n_staff は feat_work_context 由来（事前算出の COUNT(DISTINCT person_id)）。"
                "n_distinct_roles は feat_work_context 由来（事前算出の COUNT(DISTINCT role)）。"
                "真のチーム凝集性指標（例: 作品内共クレジット部分グラフのクラスタリング係数）は"
                "作品ごとのグラフ構築にかかる計算コストのため、本表では算出していない。"
                "散布図は描画パフォーマンス上、Tierあたり最大500点までサンプリングして表示。"
            ),
            interpretation_html=(
                "<p>スタッフ数と役職数の関係は、作品規模が大きいほど分業が進む構造を反映している"
                "可能性がある。ただし、クレジット記載の網羅性が作品規模と相関している場合、"
                "この関係は記録バイアスの影響を受けている可能性がある。"
                "散布図上のTier間の重なり・分離パターンは、Tier分類の妥当性検証の参考となる。</p>"
            ),
            section_id="team_cohesion",
        )
