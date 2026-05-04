"""AKM Diagnostics report — v2 compliant.

Method notes and diagnostics for the AKM fixed-effect decomposition:
- Section 1: Connected set size and mobility
- Section 2: Person FE standard error distribution
- Section 3: Studio FE distribution
- Section 4: Residual analysis
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class AKMDiagnosticsReport(BaseReportGenerator):
    name = "akm_diagnostics"
    title = "AKM固定効果診断"
    subtitle = "連結集合・個人FE SE分布・スタジオFE分布・残差分析"
    filename = "akm_diagnostics.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_connected_set_section(sb)))
        sections.append(sb.build_section(self._build_person_se_section(sb)))
        sections.append(sb.build_section(self._build_studio_fe_section(sb)))
        sections.append(sb.build_section(self._build_residual_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Connected set ──────────────────────────────────

    def _build_connected_set_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            total_persons = self.conn.execute(
                "SELECT COUNT(*) AS n FROM conformed.persons"
            ).fetchone()["n"]
            total_studios = self.conn.execute(
                "SELECT COUNT(DISTINCT studio_id) AS n FROM feat_studio_affiliation"
            ).fetchone()["n"]
            scored_persons = self.conn.execute(
                "SELECT COUNT(*) AS n FROM feat_person_scores WHERE person_fe IS NOT NULL"
            ).fetchone()["n"]
            mobility_row = self.conn.execute("""
                SELECT COUNT(DISTINCT studio_id) AS n_studios_per_person
                FROM feat_studio_affiliation
                GROUP BY person_id
                HAVING COUNT(DISTINCT studio_id) > 1
            """).fetchall()
        except Exception:
            total_persons = 0
            total_studios = 0
            scored_persons = 0
            mobility_row = []

        pct_in_set = 100 * scored_persons / max(total_persons, 1)
        n_movers = len(mobility_row)

        findings = (
            f"<p>AKM連結集合: "
            f"{total_persons:,}人中{scored_persons:,}人（{pct_in_set:.1f}%）がperson_fe推定対象。"
            "連結二部グラフ外の人物は除外（スタジオ移動者が不足）。</p>"
            f"<p>スタジオ数: {total_studios:,}。"
            f"複数スタジオでクレジットのある移動者: {n_movers:,}人。"
            "AKM固定効果の識別にはスタジオ間移動が必要。"
            "単一スタジオのみの人物のperson_feは、そのスタジオ内の残差分散のみから推定される。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="連結集合の規模と移動率",
            findings_html=findings,
            method_note=(
                "AKM = Abowd-Kramarz-Margolis 分解。"
                "log(production_scale_ij) = theta_i + psi_j + epsilon。"
                "推定には人物-スタジオ二部グラフの最大連結成分を使用する。"
                "この成分に含まれない人物は、個人FEとスタジオFEの同時識別ができない。"
                "連結集合のカバレッジ（{:.1f}%）が外的妥当性を規定する。".format(pct_in_set)
            ),
            section_id="connected_set",
        )

    # ── Section 2: Person FE SE distribution ─────────────────────

    def _build_person_se_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT person_fe, person_fe_se
                FROM feat_person_scores
                WHERE person_fe IS NOT NULL AND person_fe_se IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="個人FE 標準誤差分布",
                findings_html="<p>個人FE標準誤差データが利用できません。</p>",
                section_id="person_se",
            )

        se_vals = [r["person_fe_se"] for r in rows]
        fe_vals = [r["person_fe"] for r in rows]
        se_summ = distribution_summary(se_vals, label="person_fe_se")

        # CI width = 1.96 × 2 × SE = 3.92 × SE
        ci_widths = [3.92 * se for se in se_vals]
        ci_summ = distribution_summary(ci_widths, label="95pct_ci_width")

        findings = (
            f"<p>個人固定効果の標準誤差（n={se_summ['n']:,}人）: "
            f"{format_distribution_inline(se_summ)}, "
            f"{format_ci((se_summ['ci_lower'], se_summ['ci_upper']))}。</p>"
            f"<p>95%信頼区間幅 = 3.92 × SE: "
            f"{format_distribution_inline(ci_summ)}, "
            f"{format_ci((ci_summ['ci_lower'], ci_summ['ci_upper']))}。</p>"
            "<p>クレジット数が少ないほど、またスタジオ多様性が低いほどSEは大きくなる傾向。"
            "報酬の根拠として個人レベル推定を用いる場合、信頼区間の提示が必須（v2 §3.1）。</p>"
        )

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=fe_vals[:3000], y=se_vals[:3000],
            mode="markers",
            marker=dict(color="#667eea", size=3, opacity=0.5),
            hovertemplate="FE=%{x:.3f}, SE=%{y:.4f}<extra></extra>",
        ))
        fig.update_layout(
            title="個人FE × SE（サンプル3,000）",
            xaxis_title="個人FE", yaxis_title="個人FE SE",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="個人FE 標準誤差分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_person_se", height=420),
            method_note=(
                "person_fe_se from feat_person_scores (OLS SE from AKM). "
                "95% CI = FE ± 1.96 × SE (analytically derived, Gaussian assumption). "
                "SE depends on number of credits and studio diversity; "
                "persons with few credits have wider CIs."
            ),
            section_id="person_se",
        )

    # ── Section 3: Studio FE distribution ────────────────────────

    def _build_studio_fe_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fsa.studio_name,
                    AVG(fps.studio_fe_exposure) AS studio_fe,
                    COUNT(DISTINCT fsa.person_id) AS n_staff
                FROM feat_studio_affiliation fsa
                JOIN feat_person_scores fps ON fsa.person_id = fps.person_id
                WHERE fsa.is_main_studio = 1
                  AND fps.studio_fe_exposure IS NOT NULL
                GROUP BY fsa.studio_id
                HAVING n_staff >= 5
                ORDER BY studio_fe DESC
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スタジオ固定効果分布",
                findings_html="<p>スタジオFEデータなし。</p>",
                section_id="studio_fe_dist",
            )

        fe_vals = [r["studio_fe"] for r in rows]
        summ = distribution_summary(fe_vals, label="studio_fe")

        top5 = rows[:5]
        bottom5 = rows[-5:]
        top_str = "、".join(f"{r['studio_name']}({r['studio_fe']:.2f}, n={r['n_staff']})" for r in top5)
        bottom_str = "、".join(f"{r['studio_name']}({r['studio_fe']:.2f}, n={r['n_staff']})" for r in bottom5)

        findings = (
            f"<p>スタジオ固定効果の分布（n={summ['n']:,}スタジオ、スタッフ5人以上）: "
            f"{format_distribution_inline(summ)}, "
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
            f"<p>上位5: {top_str}</p>"
            f"<p>下位5: {bottom_str}</p>"
            "<p>studio_fe_exposure = 所属スタッフのstudio_fe_exposureの平均値。"
            "スタジオの制作規模への寄与を人物効果を除いて推定したもの。</p>"
        )

        fig = go.Figure(go.Histogram(
            x=fe_vals, nbinsx=40, marker_color="#a0d2db",
            hovertemplate="studio_fe=%{x:.3f}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(title="スタジオFE分布", xaxis_title="スタジオFE（平均）", yaxis_title="スタジオ数")

        # Deeper chart: Studio FE vs staff count scatter
        fig2 = go.Figure(go.Scatter(
            x=[r["n_staff"] for r in rows],
            y=[r["studio_fe"] for r in rows],
            mode="markers",
            marker=dict(size=5, color=fe_vals, colorscale="Viridis", showscale=True,
                        colorbar=dict(title="スタジオFE")),
            text=[r["studio_name"] for r in rows],
            hovertemplate="%{text}<br>staff=%{x}, FE=%{y:.3f}<extra></extra>",
        ))
        fig2.update_layout(
            title="スタジオFE × スタッフ数",
            xaxis_title="所属スタッフ数", yaxis_title="スタジオFE",
            xaxis_type="log",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオ固定効果分布",
            findings_html=findings,
            visualization_html=(
                plotly_div_safe(fig, "chart_studio_fe", height=380) +
                plotly_div_safe(fig2, "chart_studio_fe_scatter", height=400)
            ),
            method_note=(
                "スタジオFE = 該当スタジオに所属する人物（is_main_studio=1、n_staff >= 5）の "
                "studio_fe_exposure の平均値。"
                "studio_fe_exposure は feat_person_scores 由来（AKM分解におけるpsi_j）。"
                "識別は任意の正規化に対して相対的であり、"
                "同一実行内の比較のみ意味を持つ。"
            ),
            interpretation_html=(
                "<p>スタジオFEとスタッフ数の散布図は、大規模スタジオが必ずしも高いFEを持つわけではないことを示す。"
                "小規模で高FEのスタジオは、ニッチな高品質制作に特化している可能性がある。"
                "ただし、スタッフ5人未満のスタジオは除外されており、極小規模のサンプルバイアスに注意。</p>"
            ),
            section_id="studio_fe_dist",
        )

    # ── Section 4: Residual analysis ─────────────────────────────

    def _build_residual_section(self, sb: SectionBuilder) -> ReportSection:
        import math
        try:
            rows = self.conn.execute("""
                SELECT
                    fps.person_fe,
                    fps.studio_fe_exposure,
                    fpws.mean_production_scale
                FROM feat_person_scores fps
                JOIN feat_person_work_summary fpws ON fps.person_id = fpws.person_id
                WHERE fps.person_fe IS NOT NULL
                  AND fps.studio_fe_exposure IS NOT NULL
                  AND fpws.mean_production_scale IS NOT NULL
                  AND fpws.mean_production_scale > 0
                  AND fps.person_fe_n_obs >= 3
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="AKM残差分析",
                findings_html=(
                    "<p>AKM残差の近似計算に必要なデータが不足。"
                    "残差 ≈ log(production_scale) − person_fe − studio_fe_exposure。"
                    "正規分布に近い残差はOLS仮定と整合する。</p>"
                ),
                section_id="residual",
            )

        vals = []
        for r in rows:
            resid = math.log(r["mean_production_scale"]) - r["person_fe"] - r["studio_fe_exposure"]
            vals.append(resid)
        summ = distribution_summary(vals, label="akm_residual")

        findings = (
            f"<p>AKM残差の近似分布（n={summ['n']:,}人、n_obs≥3フィルタ）: "
            f"{format_distribution_inline(summ)}, "
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
            "<p>残差 ≈ log(mean_production_scale) − person_fe − studio_fe_exposure。"
            "平均が0に近く対称的な分布であればOLS仮定と整合する。</p>"
        )

        fig = go.Figure(go.Histogram(
            x=vals, nbinsx=50, marker_color="#06D6A0",
            hovertemplate="残差=%{x:.3f}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(title="AKM残差の近似分布", xaxis_title="残差", yaxis_title="人数")

        # Deeper: Q-Q style — residual vs person_fe scatter
        sample_idx = list(range(0, len(rows), max(1, len(rows) // 2000)))
        fig2 = go.Figure(go.Scatter(
            x=[rows[i]["person_fe"] for i in sample_idx],
            y=[vals[i] for i in sample_idx],
            mode="markers",
            marker=dict(size=3, color="rgba(6,214,160,0.3)"),
            hovertemplate="person_fe=%{x:.3f}, 残差=%{y:.3f}<extra></extra>",
        ))
        fig2.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
        fig2.update_layout(
            title="個人FE × 残差（ランダム性の確認）",
            xaxis_title="個人FE (θ_i)", yaxis_title="残差",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="AKM残差分析",
            findings_html=findings,
            visualization_html=(
                plotly_div_safe(fig, "chart_residual", height=380) +
                plotly_div_safe(fig2, "chart_residual_scatter", height=400)
            ),
            method_note=(
                "残差は log(mean_production_scale) − person_fe − studio_fe_exposure で近似。"
                "これは観測単位の production_scale ではなく個人平均を用いているため近似値である。"
                "person_fe との散布図で非ランダムパターンを確認する: "
                "ファネル形状は分散不均一性を示唆する。"
            ),
            interpretation_html=(
                "<p>残差がperson_feに対してランダムに散布していれば、加法モデルの仮定が支持される。"
                "ファネル型（大きいFEほど残差が広がる）は分散不均一性を示唆し、"
                "標準誤差の信頼性に影響する。"
                "右裾が重い分布は、特定の大型作品がモデルに適合していないことを示す。</p>"
            ),
            section_id="residual",
        )
