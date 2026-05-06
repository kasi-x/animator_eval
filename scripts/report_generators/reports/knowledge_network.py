"""Knowledge Network report — v2 compliant.

Covers knowledge transfer via collaboration:
- Section 1: AWCC (Average Weighted Collaboration Centrality) distribution
- Section 2: NDI (Network Diversity Index) by tier
- Section 3: Mentorship network density
- Section 4: Knowledge reach by career stage
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..color_utils import TIER_PALETTE as _TIER_COLORS
from ..html_templates import plotly_div_safe, stratification_tabs, strat_panel
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class KnowledgeNetworkReport(BaseReportGenerator):
    name = "knowledge_network"
    title = "知識伝達ネットワーク"
    subtitle = "AWCC・NDI・メンタリング密度の分布とTier別特性"
    filename = "knowledge_network.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_awcc_section(sb)))
        sections.append(sb.build_section(self._build_ndi_section(sb)))
        sections.append(sb.build_section(self._build_mentorship_section(sb)))
        sections.append(sb.build_section(self._build_reach_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: AWCC distribution ─────────────────────────────

    def _build_awcc_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fps.awcc, fps.ndi, p.gender,
                       modal_tier.scale_tier AS tier
                FROM feat_person_scores fps
                JOIN conformed.persons p ON fps.person_id = p.id
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
                ) modal_tier ON fps.person_id = modal_tier.person_id AND modal_tier.rn = 1
                WHERE fps.awcc IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="AWCC（加重協業中心性）の分布",
                findings_html="<p>AWCCデータが利用できません。</p>",
                section_id="awcc_dist",
            )

        awcc_vals = [r["awcc"] for r in rows]
        summ = distribution_summary(awcc_vals, label="awcc")

        findings = (
            f"<p>AWCC分布（n={summ['n']:,}人）: "
            f"{format_distribution_inline(summ)}, "
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
            "<p>AWCC（加重協業中心性）= 直接協業者への平均エッジ重み。"
            "協業関係の強度と多様性を反映する指標。</p>"
        )

        # Gender breakdown
        gender_groups: dict[str, list[float]] = {}
        for r in rows:
            g = r["gender"] or "unknown"
            gender_groups.setdefault(g, []).append(r["awcc"])

        gender_html = ""
        if len(gender_groups) > 1:
            gender_html = "<p>性別:</p><ul>"
            for g, gv in sorted(gender_groups.items()):
                gs = distribution_summary(gv, label=g)
                gender_html += (
                    f"<li><strong>{g}</strong> (n={gs['n']:,}): "
                    f"{format_distribution_inline(gs)}, "
                    f"{format_ci((gs['ci_lower'], gs['ci_upper']))}</li>"
                )
            gender_html += "</ul>"

        # Tier breakdown
        tier_groups: dict[int, list[float]] = {}
        for r in rows:
            if r["tier"] is not None:
                tier_groups.setdefault(r["tier"], []).append(r["awcc"])

        tier_html = ""
        if tier_groups:
            tier_html = "<p>主要スケールTier別:</p><ul>"
            for t in sorted(tier_groups):
                ts = distribution_summary(tier_groups[t], label=f"tier{t}")
                tier_html += (
                    f"<li><strong>Tier {t}</strong> (n={ts['n']:,}): "
                    f"{format_distribution_inline(ts)}, "
                    f"{format_ci((ts['ci_lower'], ts['ci_upper']))}</li>"
                )
            tier_html += "</ul>"

        # Figures
        fig = go.Figure(go.Histogram(
            x=awcc_vals, nbinsx=40, marker_color="#3BC494",
            hovertemplate="AWCC=%{x:.3f}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(title="AWCC 分布", xaxis_title="AWCC", yaxis_title="人数")

        fig_tier = go.Figure()
        for t in sorted(tier_groups):
            fig_tier.add_trace(go.Box(
                y=tier_groups[t], name=f"Tier {t}",
                marker_color=_TIER_COLORS.get(t, "#a0a0c0"),
                boxpoints=False,
            ))
        fig_tier.update_layout(title="作品規模Tier別 AWCC", yaxis_title="AWCC")

        tabs_html = stratification_tabs(
            "awcc_tabs", {"overall": "全体", "gender": "性別", "tier": "Tier別"}, active="overall"
        )
        panel_overall = strat_panel(
            "awcc_tabs", "overall",
            plotly_div_safe(fig, "chart_awcc_overall", height=380), active=True,
        )
        panel_gender = strat_panel(
            "awcc_tabs", "gender",
            gender_html or "<p>性別内訳が利用できません。</p>",
        )
        panel_tier = strat_panel(
            "awcc_tabs", "tier",
            (tier_html or "<p>Tier内訳が利用できません。</p>") +
            plotly_div_safe(fig_tier, "chart_awcc_tier", height=400),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="AWCC（加重協業中心性）の分布",
            findings_html=findings,
            visualization_html=tabs_html + panel_overall + panel_gender + panel_tier,
            method_note=(
                "AWCC は feat_network 由来（Phase 9 スコアリング）。"
                "定義: 直接協業者全員に対するエッジ重みの平均。"
                "エッジ重み = role_weight × episode_coverage × duration_mult（視聴者評価不使用）。"
                "グラフ内に協業者のいない人物は AWCC = NULL となり、除外される。"
            ),
            section_id="awcc_dist",
        )

    # ── Section 2: NDI by tier ────────────────────────────────────

    def _build_ndi_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fps.ndi, modal_tier.scale_tier AS tier
                FROM feat_person_scores fps
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
                ) modal_tier ON fps.person_id = modal_tier.person_id AND modal_tier.rn = 1
                WHERE fps.ndi IS NOT NULL
                  AND modal_tier.scale_tier IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="NDI（ネットワーク多様性指標）のTier別分布",
                findings_html="<p>NDIデータが利用できません。</p>",
                section_id="ndi_tier",
            )

        tier_ndi: dict[int, list[float]] = {}
        for r in rows:
            tier_ndi.setdefault(r["tier"], []).append(r["ndi"])

        findings = "<p>NDI（ネットワーク多様性指標）主要スケールTier別:</p><ul>"
        for t in sorted(tier_ndi):
            ts = distribution_summary(tier_ndi[t], label=f"tier{t}")
            findings += (
                f"<li><strong>Tier {t}</strong> (n={ts['n']:,}): "
                f"{format_distribution_inline(ts)}, "
                f"{format_ci((ts['ci_lower'], ts['ci_upper']))}</li>"
            )
        findings += "</ul>"

        fig = go.Figure()
        for t in sorted(tier_ndi):
            fig.add_trace(go.Violin(
                y=tier_ndi[t][:1000] if len(tier_ndi[t]) > 1000 else tier_ndi[t],
                name=f"Tier {t}", box_visible=True, meanline_visible=True,
                points=False, marker_color=_TIER_COLORS.get(t, "#a0a0c0"),
            ))
        fig.update_layout(
            title="作品規模Tier別 NDI",
            xaxis_title="モーダルTier", yaxis_title="NDI",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="NDI（ネットワーク多様性指標）のTier別分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_ndi_tier", height=420),
            method_note=(
                "NDI = Network Diversity Index（feat_network より取得）。"
                "定義: 1 − Σ(p_c²)。p_c は協業者のうちコミュニティcに属する割合"
                "（ハーフィンダール指数の補数）。NDI = 0 は全協業者が単一コミュニティに属することを意味し、"
                "NDI = 1 は協業者が複数コミュニティに均等分布することを意味する。"
                "人物ごとの最頻Tierはウィンドウ関数で算出。"
            ),
            interpretation_html=(
                "<p>低Tier人物のNDIが高い傾向は、小規模予算の作品が多様なコミュニティから"
                "スタッフを集める一方、高Tier作品がより専門化・固定化したチームを用いることを反映している可能性がある。"
                "代替解釈として、高Tier作品はクルーが大規模であるため、"
                "物量的に多くのコミュニティから人材を集めてしまい、結果としてNDIが上振れする可能性もある。"
                "これらの仮説の検証にはクルー規模の統制が必要である。</p>"
            ),
            section_id="ndi_tier",
        )

    # ── Section 3: Mentorship network density ────────────────────

    def _build_mentorship_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    COUNT(*) AS n_pairs,
                    AVG(weight) AS avg_weight,
                    MAX(weight) AS max_weight,
                    MIN(weight) AS min_weight
                FROM feat_mentorships
            """).fetchone()
            degree_rows = self.conn.execute("""
                SELECT mentor_id AS person_id, COUNT(*) AS n_mentees
                FROM feat_mentorships
                GROUP BY mentor_id
                ORDER BY n_mentees DESC
                LIMIT 100
            """).fetchall()
        except Exception:
            rows = None
            degree_rows = []

        if not rows or rows["n_pairs"] == 0:
            return ReportSection(
                title="メンタリング関係の密度",
                findings_html="<p>メンタリングデータが利用できません（feat_mentorships）。</p>",
                section_id="mentorship",
            )

        n_pairs = rows["n_pairs"]
        avg_w = rows["avg_weight"]
        findings = (
            f"<p>feat_mentorship に記録されたメンター-メンティのペア数: {n_pairs:,}。"
            f"エッジ重み（メンタリング強度）の平均: {avg_w:.3f} "
            f"（最小={rows['min_weight']:.3f}、最大={rows['max_weight']:.3f}）。</p>"
        )

        if degree_rows:
            n_mentees = [r["n_mentees"] for r in degree_rows]
            ds = distribution_summary(n_mentees, label="n_mentees_per_mentor")
            findings += (
                f"<p>メンター1人あたりのメンティ数（上位100メンター）: "
                f"{format_distribution_inline(ds)}、"
                f"{format_ci((ds['ci_lower'], ds['ci_upper']))}。</p>"
            )
            fig = go.Figure(go.Histogram(
                x=n_mentees, nbinsx=20, marker_color="#FFB444",
                hovertemplate="メンティ%{x}人: メンター%{y:,}人<extra></extra>",
            ))
            fig.update_layout(
                title="メンター1人あたりのメンティ数（上位100メンター）",
                xaxis_title="メンティ数", yaxis_title="メンター数",
            )
            viz = plotly_div_safe(fig, "chart_mentorship", height=360)
        else:
            viz = "<p>メンタリング次数分布が利用できません。</p>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="メンタリング関係の密度",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "メンタリングペアは feat_mentorship テーブル（Phase 9: mentorship モジュール）由来。"
                "メンター-メンティ関係の操作的定義: 経験年数が上の人物（career_year が大きい）が"
                "経験年数の下の人物と同一作品で共クレジットされ、複数作品で繰り返される関係。"
                "エッジ重みは共クレジット頻度と役職の近接性を反映する。"
            ),
            section_id="mentorship",
        )

    # ── Section 4: Knowledge reach by career stage ───────────────

    def _build_reach_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fc.highest_stage,
                    fn.degree_centrality,
                    fps.awcc,
                    fps.ndi
                FROM feat_network fn
                JOIN feat_career fc ON fn.person_id = fc.person_id
                JOIN feat_person_scores fps ON fn.person_id = fps.person_id
                WHERE fc.highest_stage IS NOT NULL
                  AND fn.degree_centrality IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="キャリアステージ別ネットワーク到達範囲",
                findings_html="<p>ステージ別ネットワークデータが利用できません。</p>",
                section_id="knowledge_reach",
            )

        stage_deg: dict[int, list[float]] = {}
        stage_awcc: dict[int, list[float]] = {}
        for r in rows:
            s = r["highest_stage"]
            stage_deg.setdefault(s, []).append(r["degree_centrality"])
            if r["awcc"] is not None:
                stage_awcc.setdefault(s, []).append(r["awcc"])

        stage_labels = {0: "Unknown", 1: "Entry", 2: "Junior", 3: "Mid",
                        4: "Senior", 5: "Principal", 6: "Director/Lead"}

        findings = "<p>キャリアステージ別の次数中心性とAWCC（highest_stage基準）:</p><ul>"
        for s in sorted(stage_deg):
            ds = distribution_summary(stage_deg[s], label=f"stage{s}")
            awcc_str = ""
            if s in stage_awcc:
                aws = distribution_summary(stage_awcc[s], label="awcc")
                awcc_str = f", AWCC median={aws['median']:.3f}"
            findings += (
                f"<li><strong>Stage {s} ({stage_labels.get(s, s)})</strong> "
                f"(n={ds['n']:,}): degree median={ds['median']:.4f}{awcc_str}</li>"
            )
        findings += "</ul>"

        fig = go.Figure()
        stage_colors = ["#8a94a0", "#a0a0c0", "#7CC8F2", "#3BC494", "#F8EC6A", "#E07532", "#E09BC2"]
        for i, s in enumerate(sorted(stage_deg)):
            fig.add_trace(go.Box(
                y=stage_deg[s][:500] if len(stage_deg[s]) > 500 else stage_deg[s],
                name=f"S{s}",
                marker_color=stage_colors[i % len(stage_colors)],
                boxpoints=False,
            ))
        fig.update_layout(
            title="キャリア段階別 次数中心性",
            xaxis_title="最高段階", yaxis_title="次数中心性",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="キャリアステージ別ネットワーク到達範囲",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_reach_stage", height=420),
            method_note=(
                "highest_stage は feat_career 由来（ルールベースのステージ0〜6）。"
                "degree_centrality と awcc は feat_network 由来。"
                "ステージとネットワーク指標は同一のクレジット履歴から導出されており、"
                "独立ではない — クレジット数の多い人物ほどステージが高く、次数も高くなる傾向がある。"
            ),
            section_id="knowledge_reach",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='knowledge_network',
    audience='technical_appendix',
    claim=(
        'AWCC (能力加重協業中心性) / NDI (Network Diffusion Index) / メンタリング密度 '
        '(高経験者と低経験者の共クレジット比率) の Tier 別分布が、'
        'configuration model null 95% 区間外に位置する構造的パターンを示す'
    ),
    identifying_assumption=(
        'AWCC は協業者の IV スコアで重み付けした次数中心性 — 真の知識伝達効率'
        'ではなく構造的近接性。NDI は graph-based diffusion の理論モデル指標。'
        'メンタリング ≠ 共クレジット — 後者を operational proxy として使用。'
    ),
    null_model=['N1', 'N2'],
    sources=['credits', 'persons', 'anime', 'feat_person_scores'],
    meta_table='meta_knowledge_network',
    estimator='AWCC + NDI + Tier 別分布 (median + IQR)',
    ci_estimator='bootstrap', n_resamples=500,
    extra_limitations=[
        'AWCC は構造的近接性指標 — 知識伝達の実態とは別',
        'NDI 理論モデルの仮定 (homogeneous transmission) を満たさない可能性',
        'Tier 境界 (5 階層) は事前固定、別境界で結論変動可能',
    ],
)
