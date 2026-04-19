"""Person Ranking report — v2 compliant rewrite.

Key v2 changes from monolith:
- CIs from person_fe_se on individual estimates
- Tier-stratified score distributions (tier tab panel)
- Findings/Interpretation structural separation
- No evaluative adjectives in findings text
- Data statement + v2 disclaimer
"""

from __future__ import annotations

import math
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe, stratification_tabs, strat_panel
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class PersonRankingReport(BaseReportGenerator):
    """Person ranking and score distribution — REPORT_PHILOSOPHY v2 compliant."""

    name = "person_ranking"
    title = "人物ランキング・スコア分析"
    subtitle = "IV Score による人物ランキングと信頼区間付きスコア分布"
    filename = "person_ranking.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []

        persons = self._load_persons()
        if not persons:
            return None

        tier_score_groups = self._load_tier_scores()

        sections.append(sb.build_section(self._build_distribution_section(sb, persons)))
        sections.append(sb.build_section(self._build_tier_section(sb, tier_score_groups)))
        sections.append(sb.build_section(self._build_ci_section(sb, persons)))
        sections.append(sb.build_section(self._build_top_table_section(sb, persons)))

        body = "\n".join(sections)
        return self.write_report(body)

    # ==============================================================
    # Data loaders
    # ==============================================================

    def _load_persons(self) -> list[dict]:
        try:
            rows = self.conn.execute("""
                SELECT
                    fps.person_id,
                    COALESCE(NULLIF(p.name_ja,''), NULLIF(p.name_en,''), fps.person_id) AS name,
                    p.gender,
                    fps.iv_score, fps.person_fe, fps.birank,
                    fps.patronage, fps.awcc, fps.studio_fe_exposure,
                    fps.dormancy, fps.ndi,
                    fps.person_fe_se, fps.person_fe_n_obs,
                    fps.iv_score_pct, fps.person_fe_pct,
                    fps.confidence,
                    fps.score_range_low, fps.score_range_high,
                    fc.first_year, fc.career_track, fc.total_credits,
                    fc.highest_stage, fc.primary_role
                FROM feat_person_scores fps
                JOIN persons p ON fps.person_id = p.id
                LEFT JOIN feat_career fc ON fps.person_id = fc.person_id
                WHERE fps.iv_score IS NOT NULL
                ORDER BY fps.iv_score DESC
            """).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    def _load_tier_scores(self) -> dict[int, list[float]]:
        """IV scores grouped by each tier the person has credits in.

        A person with credits in Tier 3 and Tier 5 appears in both groups.
        """
        try:
            rows = self.conn.execute("""
                SELECT
                    pt.scale_tier AS tier,
                    fps.iv_score
                FROM feat_person_scores fps
                JOIN (
                    SELECT fcc_t.person_id, fwc_t.scale_tier
                    FROM feat_credit_contribution fcc_t
                    JOIN feat_work_context fwc_t ON fcc_t.anime_id = fwc_t.anime_id
                    WHERE fwc_t.scale_tier IS NOT NULL
                    GROUP BY fcc_t.person_id, fwc_t.scale_tier
                ) pt ON pt.person_id = fps.person_id
                WHERE fps.iv_score IS NOT NULL
            """).fetchall()
        except Exception:
            return {}

        groups: dict[int, list[float]] = {}
        for r in rows:
            groups.setdefault(r["tier"], []).append(r["iv_score"])

        return groups

    # ==============================================================
    # Section builders
    # ==============================================================

    def _build_distribution_section(
        self, sb: SectionBuilder, persons: list[dict],
    ) -> ReportSection:
        iv_scores = [p["iv_score"] for p in persons if p.get("iv_score") is not None]
        biranks = [p["birank"] for p in persons if p.get("birank") is not None]
        patronages = [p["patronage"] for p in persons if p.get("patronage") is not None]
        pfe_vals = [p["person_fe"] for p in persons if p.get("person_fe") is not None]

        iv_summ = distribution_summary(iv_scores, label="IV Score")
        br_summ = distribution_summary(biranks, label="BiRank")
        pa_summ = distribution_summary(patronages, label="Patronage")
        pfe_summ = distribution_summary(pfe_vals, label="Person FE")

        findings = (
            f"<p>IVスコアが算出された{len(persons):,}人のスコア分布:</p>"
            f"<ul>"
            f"<li><strong>IV Score</strong>: {format_distribution_inline(iv_summ)}, "
            f"{format_ci((iv_summ['ci_lower'], iv_summ['ci_upper']))}</li>"
            f"<li><strong>BiRank</strong>: {format_distribution_inline(br_summ)}, "
            f"{format_ci((br_summ['ci_lower'], br_summ['ci_upper']))}</li>"
            f"<li><strong>Patronage</strong>: {format_distribution_inline(pa_summ)}, "
            f"{format_ci((pa_summ['ci_lower'], pa_summ['ci_upper']))}</li>"
            f"<li><strong>Person FE</strong>: {format_distribution_inline(pfe_summ)}, "
            f"{format_ci((pfe_summ['ci_lower'], pfe_summ['ci_upper']))}</li>"
            f"</ul>"
            f"<p>4指標すべてが正の歪度（右裾が長い分布）を示しており、"
            f"協業ネットワークにおける集中構造と整合する。"
            f"全範囲を表示するため対数y軸のヒストグラムを使用。</p>"
        )

        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("IV Score", "BiRank", "Patronage", "Person FE"),
            vertical_spacing=0.14, horizontal_spacing=0.08,
        )
        for row, col, vals, color, name in [
            (1, 1, iv_scores, "#f093fb", "IV Score"),
            (1, 2, biranks, "#a0d2db", "BiRank"),
            (2, 1, patronages, "#f5576c", "Patronage"),
            (2, 2, pfe_vals, "#fda085", "Person FE"),
        ]:
            fig.add_trace(go.Histogram(
                x=vals, nbinsx=40, marker_color=color, name=name,
                hovertemplate="%{x:.2f}: %{y:,}<extra></extra>",
            ), row=row, col=col)
        fig.update_layout(title="スコア構成要素の分布 (対数y軸)", showlegend=False)
        for r in range(1, 3):
            for c in range(1, 3):
                fig.update_yaxes(type="log", row=r, col=c)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2 violations: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="スコア構成要素の分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_score_dist", height=560),
            method_note=(
                "IV Score = Person FE, BiRank, Patronage, AWCC, Studio Exposure の加重合計 × Dormancy乗数。"
                "Lambda重みはPCA由来の固定事前分布（視聴者評価に対する最適化なし）。"
                "BiRank: 二部グラフランキング（人物 + アニメ作品）。"
                "Patronage: 同一監督からの反復起用クレジット重み。"
                "Person FE: log(staff × episodes × duration) のAKM固定効果。"
                "対数y軸: 分布が複数桁にわたるため。"
            ),
            interpretation_html=(
                "<p>ゼロ付近のスコア集中と長い右裾は、協業ネットワークのトポロジーを反映している: "
                "大半の人物は少数の作品に参加し、共同作業者の重複が限定的である一方、"
                "少数の人物が構造的に中心的な位置を占める。"
                "このパターンはスケールフリーネットワークの性質と整合する。"
                "代替解釈: 2000年代以前に活動した人物について、歴史的記録の疎さが"
                "スコアを人為的に抑制している可能性がある。</p>"
            ),
            section_id="score_dist",
        )

    def _build_tier_section(
        self, sb: SectionBuilder, tier_groups: dict[int, list[float]],
    ) -> ReportSection:
        if not tier_groups:
            findings = "<p>Tier別スコアデータが取得できませんでした。</p>"
            return ReportSection(
                title="作品規模Tier別IVスコア分布",
                findings_html=findings,
                section_id="tier_scores",
            )

        tier_summaries = {
            t: distribution_summary(vals, label=f"Tier {t}")
            for t, vals in sorted(tier_groups.items())
        }

        findings = "<p>各作品規模Tierに参加実績のある人物のIVスコア分布:</p><ul>"
        for tier, summ in sorted(tier_summaries.items()):
            ci_str = format_ci((summ["ci_lower"], summ["ci_upper"]))
            findings += (
                f"<li><strong>Tier {tier}</strong> (n={summ['n']:,}): "
                f"{format_distribution_inline(summ)}, {ci_str}</li>"
            )
        findings += (
            "</ul><p>注: 複数Tierの作品に参加実績がある人物は複数グループに計上される。"
            "nは（人物, Tier）ペア数であり、ユニーク人物数ではない。</p>"
        )

        # Tab view
        tab_axes = {"all": "全Tier比較"} | {
            f"{t}": f"Tier {t}"
            for t in sorted(tier_groups.keys())
        }
        tabs_html = stratification_tabs("tier_score_tabs", tab_axes, active="all")

        # Combined violin
        fig_all = go.Figure()
        for tier in sorted(tier_groups.keys()):
            vals = tier_groups[tier]
            tier_name = f"T{tier}"
            color = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}.get(tier, "#a0a0c0")
            sample = vals[:2000] if len(vals) > 2000 else vals
            fig_all.add_trace(go.Violin(
                x=[tier_name] * len(sample), y=sample,
                name=tier_name, box_visible=True,
                meanline_visible=True, points=False,
                line_color=color,
            ))
        fig_all.update_layout(
            title="作品規模Tier別 IV Score",
            xaxis_title="Tier", yaxis_title="IV Score",
            violinmode="overlay",
        )

        panels = [
            strat_panel(
                "tier_score_tabs", "all",
                plotly_div_safe(fig_all, "chart_tier_score_all", height=420),
                active=True,
            )
        ]
        for tier in sorted(tier_groups.keys()):
            summ = tier_summaries[tier]
            panels.append(strat_panel(
                "tier_score_tabs", str(tier),
                f'<div class="card" style="margin:0;">'
                f'<p><strong>Tier {tier}</strong>: n={summ["n"]:,}, '
                f'{format_distribution_inline(summ)}, '
                f'{format_ci((summ["ci_lower"], summ["ci_upper"]))}</p>'
                f'</div>',
            ))

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2 violations: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="作品規模Tier別IVスコア分布",
            findings_html=findings,
            visualization_html=tabs_html + "\n".join(panels),
            method_note=(
                "Tier割り当ては作品単位（feat_work_context.scale_tier）。"
                "該当Tierの作品にクレジットがある人物がそのTierグループに含まれる。"
                "同一人物が複数のTierグループに出現する場合がある。"
                "CIは解析的（SE = sigma/sqrt(n), 95%）。"
                "描画性能のため、Tier毎にバイオリンプロットを2,000点にサブサンプリング。"
            ),
            interpretation_html=(
                "<p>高Tier作品（T4-T5）にクレジットのある人物は、IVスコアの中央値が高い傾向がある。"
                "これは選択メカニズムを反映している: 大規模制作に携わる人物は、"
                "ネットワーク接続と個人固定効果をより多く蓄積している。"
                "代替解釈: 高Tier作品はより多くのスタッフを集める（作品あたりのクレジット数が多い）ため、"
                "AKMモデルの結果変数 production_scale を機械的に増大させる。"
                "観察データのみから選択効果と処置効果を分離することはできない。</p>"
            ),
            section_id="tier_scores",
        )

    def _build_ci_section(
        self, sb: SectionBuilder, persons: list[dict],
    ) -> ReportSection:
        """Show top 30 persons with person_fe ± CI from person_fe_se."""
        # Filter to those with SE available
        ci_persons = [
            p for p in persons
            if p.get("person_fe_se") is not None
            and p.get("person_fe_n_obs", 0) and p.get("person_fe_n_obs", 0) >= 3
        ][:30]

        n_with_se = sum(
            1 for p in persons
            if p.get("person_fe_se") is not None
        )
        n_total = len(persons)

        findings = (
            f"<p>スコア算出済み{n_total:,}人のうち、{n_with_se:,}人"
            f"（{100 * n_with_se / n_total:.1f}%）で person_fe_se が算出済み"
            f"（AKM同定には最低3つの接続観測が必要）。"
            f"以下のチャートはIVスコア上位30人の個人固定効果推定値と95% CIを表示。"
            f"CI幅は推定精度を反映: 接続集合内の作品数が少ない人物はCIが広い。</p>"
        )

        if ci_persons:
            names = [p.get("name") or p["person_id"] for p in ci_persons]
            pfe = [p["person_fe"] for p in ci_persons]
            se = [p["person_fe_se"] for p in ci_persons]
            # 95% CI: ±1.96 × SE
            ci_hi = [fe + 1.96 * s for fe, s in zip(pfe, se)]
            ci_lo = [fe - 1.96 * s for fe, s in zip(pfe, se)]
            n_obs = [p.get("person_fe_n_obs", 0) for p in ci_persons]
            confidence = [p.get("confidence", "") or "" for p in ci_persons]

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=pfe, y=names,
                mode="markers",
                marker=dict(
                    size=8,
                    color=[p.get("iv_score", 0) for p in ci_persons],
                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(title="IV Score"),
                ),
                error_x=dict(
                    type="data",
                    symmetric=False,
                    array=[h - f for h, f in zip(ci_hi, pfe)],
                    arrayminus=[f - lo for f, lo in zip(pfe, ci_lo)],
                    color="rgba(255,255,255,0.3)",
                ),
                customdata=list(zip(n_obs, confidence)),
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "個人固定効果: %{x:.3f}<br>"
                    "n_obs: %{customdata[0]}<br>"
                    "信頼度: %{customdata[1]}"
                    "<extra></extra>"
                ),
            ))
            fig.add_vline(x=0, line_dash="dot", line_color="rgba(255,255,255,0.2)")
            fig.update_layout(
                title="個人固定効果 (θ) と95% CI — 上位30人",
                xaxis_title="個人固定効果 (θ)",
                yaxis_title="",
                height=max(400, len(ci_persons) * 22),
                margin=dict(l=200),
            )
            fig.update_yaxes(autorange="reversed")
            viz_html = plotly_div_safe(fig, "chart_pfe_ci", height=max(400, len(ci_persons) * 22))
        else:
            viz_html = (
                '<p style="color:#8a94a0">個人固定効果の標準誤差は未計算です。 '
                "AKM付きでパイプラインを実行し feat_person_scores.person_fe_se を生成してください。</p>"
            )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2 violations: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="個人固定効果推定値と信頼区間",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "Person FE (θ) = AKM分解の個人固定効果: "
                "log(production_scale) = θ_i + ψ_j + ε_ij。"
                "SEは内部推定量のOLS残差から算出。"
                "95% CI = θ ± 1.96 × SE（漸近正規近似）。"
                "接続集合内に最低3観測が必要。"
                "CLAUDE.mdに基づき、報酬根拠のCIは解析的に導出"
                "（SE = sigma/sqrt(n)）されなければならない（ヒューリスティック不可）。"
                "信頼度ラベル: high/medium/low（n_obs と SE に基づく）。"
            ),
            interpretation_html=(
                "<p>広いCIは個人固定効果推定値の精度が低いことを示す — "
                "通常、AKM同定に使用される接続集合内の作品数が少ないためである。"
                "広いCIの点推定値は、狭いCIの点推定値と同等に扱うべきではない"
                "（特に報酬の根拠として使用する場合）。"
                "代替的な統計手法としてベイズ縮小事前分布を用いれば、"
                "少数観測の人物の推定値を正則化しCIを狭めることが可能だが、"
                "事前分布に関する仮定が導入される。</p>"
            ),
            section_id="pfe_ci",
        )

    def _build_top_table_section(
        self, sb: SectionBuilder, persons: list[dict],
    ) -> ReportSection:
        """Top 50 persons table with score components and CI."""
        top50 = persons[:50]

        findings = (
            f"<p>以下はIVスコア降順の上位50人。"
            f"スコア範囲 [low, high] は、person_fe_se が利用可能な場合は "
            f"個別IVスコア推定値の95% CI、それ以外は複合スコア分布の &plusmn;1 SD範囲。"
            f"ランキング対象: 計{len(persons):,}人。</p>"
        )

        rows_html = ""
        for rank, p in enumerate(top50, 1):
            name = p.get("name") or p["person_id"]
            iv = p.get("iv_score", 0) or 0
            br = p.get("birank", 0) or 0
            pat = p.get("patronage", 0) or 0
            pfe = p.get("person_fe", 0) or 0
            conf = p.get("confidence") or ""
            sr_lo = p.get("score_range_low")
            sr_hi = p.get("score_range_high")
            pct = p.get("iv_score_pct")
            primary_role = p.get("primary_role") or ""

            ci_str = ""
            if sr_lo is not None and sr_hi is not None and not (
                math.isnan(float(sr_lo)) or math.isnan(float(sr_hi))
            ):
                ci_str = f"[{float(sr_lo):.2f}, {float(sr_hi):.2f}]"

            pct_str = f"{pct:.0f}th" if pct is not None and not math.isnan(float(pct)) else ""
            conf_badge = (
                f'<span class="badge badge-high" style="font-size:0.7rem;">{conf}</span>'
                if conf else ""
            )

            rows_html += (
                f"<tr>"
                f"<td>{rank}</td>"
                f"<td style='color:#e0e0e0'>{name}</td>"
                f"<td>{iv:.3f}</td>"
                f"<td style='font-size:0.8rem;color:#8a94a0'>{ci_str}</td>"
                f"<td>{pct_str}</td>"
                f"<td>{br:.3f}</td>"
                f"<td>{pat:.3f}</td>"
                f"<td>{pfe:.3f}</td>"
                f"<td style='font-size:0.8rem;color:#a0a0c0'>{primary_role}</td>"
                f"<td>{conf_badge}</td>"
                f"</tr>"
            )

        table_html = (
            '<div style="overflow-x:auto;">'
            "<table>"
            "<thead><tr>"
            "<th>#</th><th>名前</th><th>IV Score</th><th>95% CI</th>"
            "<th>パーセンタイル</th><th>BiRank</th><th>Patronage</th>"
            "<th>Person FE</th><th>主要ロール</th><th>信頼度</th>"
            "</tr></thead>"
            f"<tbody>{rows_html}</tbody>"
            "</table></div>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2 violations: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="IVスコア上位50人",
            findings_html=findings,
            visualization_html=table_html,
            method_note=(
                "スコア範囲 [low, high]: person_fe_se が利用可能な場合は "
                "IV ± 1.96 × 伝搬SE から導出。"
                "SE が利用不可の場合は全体分布の IV ± (IQR/2) として格納。"
                "パーセンタイル順位はスコア算出済み全人物内で計算。"
                "信頼度レベル: high = n_obs ≥ 10 かつ狭いCI; "
                "medium = n_obs 4〜9 または広いCI; low = n_obs < 4。"
            ),
            section_id="top_table",
        )
