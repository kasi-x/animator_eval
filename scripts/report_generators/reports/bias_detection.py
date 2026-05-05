"""Bias Detection report — v2 compliant.

Statistical tests for differential score patterns:
- Section 1: Score distribution by gender (test statistics, not "bias" labels)
- Section 2: Score by tier × gender interaction
- Section 3: Role access patterns by gender and era
- Section 4: Studio effect distribution by gender
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe, stratification_tabs, strat_panel
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class BiasDetectionReport(BaseReportGenerator):
    name = "bias_detection"
    title = "スコア差異分析"
    subtitle = "性別・Tier・年代別スコアパターンの統計的記述"
    filename = "bias_detection.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_gender_score_section(sb)))
        sections.append(sb.build_section(self._build_tier_gender_section(sb)))
        sections.append(sb.build_section(self._build_role_access_section(sb)))
        sections.append(sb.build_section(self._build_studio_fe_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Score by gender ────────────────────────────────

    def _build_gender_score_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fps.iv_score, fps.person_fe, p.gender
                FROM feat_person_scores fps
                JOIN conformed.persons p ON fps.person_id = p.id
                WHERE fps.iv_score IS NOT NULL
                  AND p.gender IN ('Male', 'Female')
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="性別別スコア分布",
                findings_html="<p>性別別スコアデータが取得できませんでした。</p>",
                section_id="gender_score",
            )

        gender_iv: dict[str, list[float]] = {}
        gender_fe: dict[str, list[float]] = {}
        for r in rows:
            g = r["gender"]
            gender_iv.setdefault(g, []).append(r["iv_score"])
            if r["person_fe"] is not None:
                gender_fe.setdefault(g, []).append(r["person_fe"])

        findings = "<p>性別別IVスコア分布（統制なし・記述統計）:</p><ul>"
        for g, gv in sorted(gender_iv.items()):
            gs = distribution_summary(gv, label=g)
            findings += (
                f"<li><strong>{g}</strong> (n={gs['n']:,}): "
                f"{format_distribution_inline(gs)}, "
                f"{format_ci((gs['ci_lower'], gs['ci_upper']))}</li>"
            )
        findings += "</ul>"

        fe_html = "<p>性別別 個人固定効果（AKM person_fe）:</p><ul>"
        for g, fv in sorted(gender_fe.items()):
            fs = distribution_summary(fv, label=g)
            fe_html += (
                f"<li><strong>{g}</strong> (n={fs['n']:,}): "
                f"{format_distribution_inline(fs)}, "
                f"{format_ci((fs['ci_lower'], fs['ci_upper']))}</li>"
            )
        fe_html += (
            "</ul>"
            "<p>注: person_feはスタジオ効果(psi_j)を統制済みだが、"
            "役職構成・キャリア年数・作品Tierについては統制していない。</p>"
        )

        fig = go.Figure()
        gender_colors = {"Male": "#3593D2", "Female": "#E07532", "unknown": "#a0a0c0"}
        for g, gv in sorted(gender_iv.items()):
            fig.add_trace(go.Violin(
                y=gv[:2000] if len(gv) > 2000 else gv,
                name=g, box_visible=True, meanline_visible=True,
                points=False, marker_color=gender_colors.get(g, "#a0a0c0"),
            ))
        fig.update_layout(title="性別別 IVスコア", yaxis_title="IVスコア")

        tabs_html = stratification_tabs(
            "gbias_tabs", {"iv": "IVスコア", "fe": "個人FE"}, active="iv"
        )
        panels = (
            strat_panel("gbias_tabs", "iv",
                        plotly_div_safe(fig, "chart_gender_iv", height=420), active=True) +
            strat_panel("gbias_tabs", "fe", fe_html)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="性別別スコア分布",
            findings_html=findings,
            visualization_html=tabs_html + panels,
            method_note=(
                "iv_score は feat_person_scores 由来（視聴者評価の混入なし）。"
                "person_fe は feat_person_scores 由来（AKMによりスタジオ効果を統制）。"
                "本節では回帰による統制は適用していない。"
                "素点の差は役職構成、キャリア長、作品Tier、スタジオ所属などの差を反映している可能性があり、"
                "必ずしも個人レベルの差を意味しない。"
            ),
            interpretation_html=(
                "<p>性別間の素点差は記述的なものであり、因果的な解釈ではない。"
                "複数の交絡要因が統制されていない: 役職分布は性別で異なり"
                "（例: アニメーション職と監督職での構成比の違い）、"
                "キャリア年数の分布も異なり、スタジオ所属パターンも異なる。"
                "役職内・キャリア年次内での比較を行えば、"
                "交絡を統制したより正確なスコア差の記述が可能となる。"
                "AKM個人FEはスタジオ効果を統制するが、上記の他の交絡要因は統制していない。</p>"
            ),
            section_id="gender_score",
        )

    # ── Section 2: Tier × gender interaction ─────────────────────

    def _build_tier_gender_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fps.iv_score, p.gender, fwc.scale_tier
                FROM feat_person_scores fps
                JOIN conformed.persons p ON fps.person_id = p.id
                JOIN feat_credit_contribution fcc ON fps.person_id = fcc.person_id
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fps.iv_score IS NOT NULL
                  AND p.gender IN ('Male', 'Female')
                  AND fwc.scale_tier IS NOT NULL
                GROUP BY fps.person_id, p.gender, fwc.scale_tier
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="Tier × 性別インタラクション",
                findings_html="<p>Tier×性別スコアデータが取得できませんでした。</p>",
                section_id="tier_gender",
            )

        cell_scores: dict[tuple[int, str], list[float]] = {}
        for r in rows:
            cell_scores.setdefault((r["scale_tier"], r["gender"]), []).append(r["iv_score"])

        genders = sorted({r["gender"] for r in rows})
        tiers = sorted({r["scale_tier"] for r in rows})

        findings = "<p>Scale Tier × 性別別 IVスコア中央値（人×Tier×性別単位）:</p>"
        findings += "<ul>"
        for t in tiers:
            for g in genders:
                vals = cell_scores.get((t, g), [])
                if vals:
                    cs = distribution_summary(vals, label=f"T{t}_{g}")
                    findings += (
                        f"<li><strong>Tier {t} × {g}</strong> (n={cs['n']:,}): "
                        f"中央値={cs['median']:.3f}</li>"
                    )
        findings += "</ul>"

        fig = go.Figure()
        gender_colors = {"Male": "#3593D2", "Female": "#E07532", "unknown": "#a0a0c0"}
        for g in genders:
            medians = []
            tier_lbls = []
            for t in tiers:
                vals = cell_scores.get((t, g), [])
                if vals:
                    cs = distribution_summary(vals, label=f"T{t}_{g}")
                    medians.append(cs["median"])
                    tier_lbls.append(f"T{t}")
            fig.add_trace(go.Scatter(
                x=tier_lbls, y=medians, name=g,
                mode="lines+markers",
                line=dict(color=gender_colors.get(g, "#a0a0c0")),
                hovertemplate=f"{g}: %{{y:.3f}}<extra></extra>",
            ))
        fig.update_layout(
            title="Tier × 性別別 IVスコア中央値",
            xaxis_title="スケールTier", yaxis_title="IVスコア中央値",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="Tier × 性別インタラクション",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_tier_gender", height=400),
            method_note=(
                "Person-tier-gender observations: each distinct (person, scale_tier, gender) triple "
                "contributes one iv_score value. A person credited on works across tiers "
                "appears in multiple tier groups."
            ),
            section_id="tier_gender",
        )

    # ── Section 3: Role access by gender and era ─────────────────

    def _build_role_access_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fcc.role,
                    p.gender,
                    (fc.first_year / 10) * 10 AS debut_decade,
                    COUNT(*) AS n
                FROM feat_credit_contribution fcc
                JOIN conformed.persons p ON fcc.person_id = p.id
                LEFT JOIN feat_career fc ON fcc.person_id = fc.person_id
                WHERE fcc.role IS NOT NULL
                  AND p.gender IN ('Male', 'Female')
                  AND fc.first_year BETWEEN 1970 AND 2019
                GROUP BY fcc.role, p.gender, debut_decade
                ORDER BY fcc.role, debut_decade
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="役職別クレジット分布（性別・年代別）",
                findings_html="<p>役職別アクセスデータが取得できませんでした。</p>",
                section_id="role_access",
            )

        # Aggregate to role × gender shares
        role_gender: dict[str, dict[str, int]] = {}
        for r in rows:
            role_gender.setdefault(r["role"], {}).setdefault(r["gender"], 0)
            role_gender[r["role"]][r["gender"]] = (
                role_gender[r["role"]][r["gender"]] + r["n"]
            )

        genders = sorted({r["gender"] for r in rows})
        roles = sorted(role_gender.keys(), key=lambda rc: -sum(role_gender[rc].values()))[:10]

        findings = "<p>性別別クレジット数（上位10役職）:</p><ul>"
        for rc in roles:
            total = sum(role_gender[rc].values())
            g_str = ", ".join(
                f"{g}: {100*role_gender[rc].get(g, 0)/total:.0f}%"
                for g in genders
            )
            findings += f"<li><strong>{rc}</strong> ({total:,}): {g_str}</li>"
        findings += "</ul>"

        fig = go.Figure()
        gender_colors_list = ["#3593D2", "#E07532", "#a0a0c0"]
        for i, g in enumerate(genders):
            totals = {rc: sum(role_gender[rc].values()) for rc in roles}
            fig.add_trace(go.Bar(
                x=roles,
                y=[100 * role_gender[rc].get(g, 0) / max(totals[rc], 1) for rc in roles],
                name=g,
                marker_color=gender_colors_list[i % len(gender_colors_list)],
            ))
        fig.update_layout(
            title="性別別 役職カテゴリ構成比（%）",
            barmode="stack",
            xaxis_title="役職カテゴリ", yaxis_title="クレジット比率（%）",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="役職別クレジット分布（性別・年代別）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_role_access", height=420),
            method_note=(
                "feat_credit_contribution × persons のJOINにより性別別クレジット数を集計。"
                "roleは正規化済み役職名。"
                "上位10役職を表示。"
                "単位はクレジット数（1人が複数クレジットを持つ場合あり）。"
            ),
            section_id="role_access",
        )

    # ── Section 4: Studio FE by gender ────────────────────────────

    def _build_studio_fe_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fps.studio_fe_exposure, p.gender
                FROM feat_person_scores fps
                JOIN conformed.persons p ON fps.person_id = p.id
                WHERE fps.studio_fe_exposure IS NOT NULL AND p.gender IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スタジオ効果の性別分布",
                findings_html="<p>スタジオ効果データが取得できませんでした。</p>",
                section_id="studio_fe",
            )

        gender_se: dict[str, list[float]] = {}
        for r in rows:
            gender_se.setdefault(r["gender"], []).append(r["studio_fe_exposure"])

        findings = "<p>性別別スタジオ効果指標（studio_fe_exposure = 所属スタジオの平均固定効果）:</p><ul>"
        for g, gv in sorted(gender_se.items()):
            gs = distribution_summary(gv, label=g)
            findings += (
                f"<li><strong>{g}</strong> (n={gs['n']:,}): "
                f"{format_distribution_inline(gs)}, "
                f"{format_ci((gs['ci_lower'], gs['ci_upper']))}</li>"
            )
        findings += (
            "</ul>"
            "<p>studio_fe_exposureは各人が所属したスタジオの平均的な制作規模水準を示す。"
            "性別間の差異はスタジオ配置パターンの違いを反映する。</p>"
        )

        fig = go.Figure()
        gender_colors = {"Male": "#3593D2", "Female": "#E07532", "unknown": "#a0a0c0"}
        for g, gv in sorted(gender_se.items()):
            fig.add_trace(go.Violin(
                y=gv[:2000] if len(gv) > 2000 else gv,
                name=g, box_visible=True, meanline_visible=True,
                points=False, marker_color=gender_colors.get(g, "#a0a0c0"),
            ))
        fig.update_layout(title="性別別 スタジオ露出度", yaxis_title="スタジオ露出度")

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオ効果の性別分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_studio_fe", height=420),
            method_note=(
                "studio_exposure from feat_person_scores: "
                "average studio fixed effect (psi_j) across all studios where person was credited. "
                "psi_j from AKM decomposition. "
                "Interpretation: persons with higher studio_exposure have worked at studios "
                "that tend to produce higher-scale productions on average."
            ),
            section_id="studio_fe",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='bias_detection',
    audience='common',
    claim='スコア差異分析 に関する記述的指標 (subtitle: 性別・Tier・年代別スコアパターンの統計的記述)',
    sources=["credits", "persons", "anime"],
    meta_table='meta_bias_detection',
)
