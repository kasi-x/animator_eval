"""Career Dynamics report — v2 compliant.

Individual career profile cards and aggregate dynamics:
- Section 1: Person FE distribution with CI bands
- Section 2: Director profile: tier distribution + career stage at first direction
- Section 3: Role progression timing (career_year_at_credit by role_category)
- Section 4: Peak performance year distribution
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}


class CareerDynamicsReport(BaseReportGenerator):
    name = "career_dynamics"
    title = "キャリアダイナミクス"
    subtitle = "個人固定効果・監督Tierプロファイル・役職取得タイミング"
    filename = "career_dynamics.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_person_fe_section(sb)))
        sections.append(sb.build_section(self._build_director_profile_section(sb)))
        sections.append(sb.build_section(self._build_role_timing_section(sb)))
        sections.append(sb.build_section(self._build_peak_year_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Person FE with CI bands ───────────────────────

    def _build_person_fe_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fps.person_fe, fps.person_fe_se,
                       fc.primary_role, fc.career_track
                FROM feat_person_scores fps
                LEFT JOIN feat_career fc ON fps.person_id = fc.person_id
                WHERE fps.person_fe IS NOT NULL
                  AND fps.person_fe_se IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="個人固定効果（95% CI付き）",
                findings_html="<p>feat_person_scores のデータが取得できませんでした。</p>",
                section_id="person_fe",
            )

        fe_vals = [r["person_fe"] for r in rows]
        se_vals = [r["person_fe_se"] for r in rows]
        summ = distribution_summary(fe_vals, label="person_fe")

        # Narrow CI persons: persons with SE < median SE
        import statistics
        med_se = statistics.median(se_vals)
        narrow_ci = [r["person_fe"] for r in rows if r["person_fe_se"] <= med_se]
        narrow_summ = distribution_summary(narrow_ci, label="narrow_ci")

        findings = (
            f"<p>個人固定効果（AKM）の分布（n={summ['n']:,}）: "
            f"{format_distribution_inline(summ)}, "
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
            f"<p>SE中央値 = {med_se:.4f}。"
            f"SE &le; 中央値の人物（n={narrow_summ['n']:,}、推定精度が高いFE）: "
            f"{format_distribution_inline(narrow_summ)}, "
            f"{format_ci((narrow_summ['ci_lower'], narrow_summ['ci_upper']))}。</p>"
            "<p>各人物の95% CIは [FE − 1.96×SE, FE + 1.96×SE]。"
            "CIが広い場合、クレジット数の少なさまたは共同作業者の多様性の低さを示す。</p>"
        )

        # Track stratification
        track_fe: dict[str, list[float]] = {}
        for r in rows:
            t = r["career_track"] or "unknown"
            track_fe.setdefault(t, []).append(r["person_fe"])

        # Histogram + track violin
        fig = go.Figure(go.Histogram(
            x=fe_vals, nbinsx=50, marker_color="#f093fb",
            hovertemplate="FE=%{x:.3f}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            title="個人固定効果の分布",
            xaxis_title="個人FE（log scale production）", yaxis_title="人数",
        )

        fig2 = go.Figure()
        track_colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#667eea",
                        "#f5576c", "#fda085", "#8a94a0"]
        for i, (track, tvals) in enumerate(
            sorted(track_fe.items(), key=lambda x: -len(x[1]))[:8]
        ):
            fig2.add_trace(go.Violin(
                y=tvals[:1000] if len(tvals) > 1000 else tvals,
                name=track[:20],
                box_visible=True, meanline_visible=True,
                points=False, marker_color=track_colors[i % len(track_colors)],
            ))
        fig2.update_layout(
            title="キャリアトラック別 個人FE",
            yaxis_title="個人FE",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="個人固定効果（95% CI付き）",
            findings_html=findings,
            visualization_html=(
                plotly_div_safe(fig, "chart_fe_dist", height=380) +
                plotly_div_safe(fig2, "chart_fe_track", height=420)
            ),
            method_note=(
                "個人FEは feat_person_scores.person_fe（AKM分解: "
                "log(production_scale) = theta_i + psi_j + epsilon）より取得。"
                "SEは feat_person_scores.person_fe_se（OLS標準誤差）。"
                "95% CI = FE ± 1.96 × SE（解析的に導出、ブートストラップではない）。"
                "AKM二部グラフの連結成分に含まれない人物は除外される。"
            ),
            section_id="person_fe",
        )

    # ── Section 2: Director profile by tier ──────────────────────

    def _build_director_profile_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fwc.scale_tier,
                    AVG(fcc.career_year_at_credit) AS avg_career_yr,
                    COUNT(DISTINCT fcc.person_id) AS n_directors,
                    COUNT(DISTINCT fcc.anime_id) AS n_works
                FROM feat_credit_contribution fcc
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fcc.role IN ('director', 'episode_director')
                  AND fwc.scale_tier IS NOT NULL
                  AND fcc.career_year_at_credit IS NOT NULL
                GROUP BY fwc.scale_tier
                ORDER BY fwc.scale_tier
            """).fetchall()
            dist_rows = self.conn.execute("""
                SELECT
                    fwc.scale_tier,
                    fcc.career_year_at_credit AS career_yr
                FROM feat_credit_contribution fcc
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fcc.role IN ('director', 'episode_director')
                  AND fwc.scale_tier IS NOT NULL
                  AND fcc.career_year_at_credit IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []
            dist_rows = []

        if not rows:
            return ReportSection(
                title="監督のTierプロファイル（キャリア年付き）",
                findings_html="<p>監督Tierプロファイルのデータが取得できませんでした。</p>",
                section_id="director_profile",
            )

        findings = "<p>作品規模Tier別の監督クレジット集計。監督クレジット時点の平均キャリア年数:</p><ul>"
        for r in rows:
            findings += (
                f"<li><strong>Tier {r['scale_tier']}</strong>: "
                f"監督数={r['n_directors']:,}人, "
                f"作品数={r['n_works']:,}, "
                f"監督クレジット時の平均キャリア年数={r['avg_career_yr']:.1f}年</li>"
            )
        findings += (
            "</ul>"
            "<p>career_year_at_credit = 監督クレジット年 − デビュー年（feat_credit_contribution.career_year_at_credit）。</p>"
        )

        # Distribution of career year at direction by tier
        tier_cy: dict[int, list[float]] = {}
        for r in dist_rows:
            tier_cy.setdefault(r["scale_tier"], []).append(r["career_yr"])

        fig = go.Figure()
        for t in sorted(tier_cy):
            vals = tier_cy[t]
            fig.add_trace(go.Box(
                y=vals, name=f"Tier {t}",
                marker_color=_TIER_COLORS.get(t, "#a0a0c0"),
                boxpoints=False,
            ))
        fig.update_layout(
            title="スケールTier別 監督クレジット時のキャリア年数",
            xaxis_title="スケールTier", yaxis_title="監督時のキャリア年数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="監督のTierプロファイル（キャリア年付き）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_dir_profile", height=420),
            method_note=(
                "監督クレジット: feat_credit_contribution.role IN ('director', 'episode_director')。"
                "career_year_at_credit は feat_credit_contribution のカラム。"
                "scale_tier は feat_work_context から取得。"
                "複数作品で監督クレジットがある人物は作品ごとに1回カウントされる。"
            ),
            interpretation_html=(
                "<p>上位Tierで監督クレジット時の平均キャリア年数が高いことは、"
                "年功序列的な傾向と整合する: 高予算作品を監督する人物は、キャリアの後半で"
                "そうする傾向がある。"
                "代替解釈: 上位レベルで業界に参入した人物（例: 共同制作）は、"
                "高Tier作品をより早く監督する可能性があり、高Tierの分布を圧縮する。"
                "箱ひげ図は平均値だけでなく全分布を示す。</p>"
            ),
            section_id="director_profile",
        )

    # ── Section 3: Role progression timing ────────────────────────

    def _build_role_timing_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fprp.role_category,
                    fprp.career_year_first
                FROM feat_person_role_progression fprp
                WHERE fprp.role_category IS NOT NULL
                  AND fprp.career_year_first IS NOT NULL
                  AND fprp.career_year_first BETWEEN 0 AND 40
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="役職カテゴリ別初回取得タイミング",
                findings_html="<p>feat_person_role_progression のデータが取得できませんでした。</p>",
                section_id="role_timing",
            )

        role_cy: dict[str, list[float]] = {}
        for r in rows:
            role_cy.setdefault(r["role_category"], []).append(r["career_year_first"])

        findings = "<p>各役職カテゴリにおける初回クレジット時のキャリア年数（feat_person_role_progression.career_year_first）:</p><ul>"
        for rc in sorted(role_cy, key=lambda x: distribution_summary(role_cy[x], label=x)["median"]):
            rs = distribution_summary(role_cy[rc], label=rc)
            findings += (
                f"<li><strong>{rc}</strong> (n={rs['n']:,}): "
                f"{format_distribution_inline(rs)}, "
                f"{format_ci((rs['ci_lower'], rs['ci_upper']))}</li>"
            )
        findings += "</ul>"

        fig = go.Figure()
        role_colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166",
                       "#667eea", "#f5576c", "#fda085", "#8a94a0"]
        sorted_roles = sorted(role_cy, key=lambda x: distribution_summary(role_cy[x], label=x)["median"])
        for i, rc in enumerate(sorted_roles[:8]):
            vals = role_cy[rc]
            fig.add_trace(go.Box(
                y=vals,
                name=rc[:20],
                marker_color=role_colors[i % len(role_colors)],
                boxpoints=False,
            ))
        fig.update_layout(
            title="役職カテゴリ別 初回クレジット時のキャリア年数（中央値順）",
            xaxis_title="役職カテゴリ", yaxis_title="キャリア年数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="役職カテゴリ別初回取得タイミング",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_role_timing", height=440),
            method_note=(
                "career_year_first は feat_person_role_progression のカラム: "
                "人物×役職カテゴリ別の MIN(career_year_at_credit)。"
                "キャリア開始時に複数役職を同時に持つ人物は career_year_first = 0 が複数カテゴリに表示される。"
                "人数上位8カテゴリを表示。"
            ),
            section_id="role_timing",
        )

    # ── Section 4: Peak performance year ─────────────────────────

    def _build_peak_year_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fc.person_id,
                    fc.peak_year - fc.first_year AS peak_career_year,
                    fc.career_track
                FROM feat_career fc
                WHERE fc.peak_year IS NOT NULL
                  AND fc.first_year IS NOT NULL
                  AND fc.active_years >= 3
                  AND fc.peak_year - fc.first_year BETWEEN 0 AND 40
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="ピーク年次の分布",
                findings_html="<p>ピーク年次のデータが取得できませんでした。</p>",
                section_id="peak_year",
            )

        peak_vals = [r["peak_career_year"] for r in rows]
        summ = distribution_summary(peak_vals, label="peak_career_year")

        track_peaks: dict[str, list[float]] = {}
        for r in rows:
            t = r["career_track"] or "unknown"
            track_peaks.setdefault(t, []).append(r["peak_career_year"])

        findings = (
            f"<p>クレジット数が最大となったキャリア年次（ピーク年次）の分布（n={summ['n']:,}人）: "
            f"{format_distribution_inline(summ)}, "
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
            "<p>feat_career.peak_year − feat_career.first_year で算出。"
            "active_years &ge; 3 かつキャリア年数0〜40の範囲に限定。</p>"
        )

        fig = go.Figure(go.Histogram(
            x=peak_vals, nbinsx=40, marker_color="#FFD166",
            hovertemplate="キャリア年 %{x}: %{y:,}人<extra></extra>",
        ))
        fig.update_layout(
            title="ピーク年次の分布（クレジット数基準）",
            xaxis_title="ピーク時のキャリア年数", yaxis_title="人数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="ピーク年次の分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_peak_year", height=380),
            method_note=(
                "ピーク年次 = feat_career.peak_year（クレジット数が最大の暦年）− first_year で算出したキャリア年数。"
                "active_years < 3 の人物は除外。"
                "現役人物は右打ち切り: 真のピーク年が将来にある可能性がある。"
            ),
            section_id="peak_year",
        )
