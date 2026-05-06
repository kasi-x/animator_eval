"""Compensation Fairness report — v2 compliant.

Score distribution fairness analysis:
- Section 1: IV score Gini by tier/gender/decade
- Section 2: Score dispersion trends over time
- Section 3: Sensitivity analysis on thresholds
- Section 4: Peer percentile distribution by gender and tier
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe, stratification_tabs, strat_panel
from ..section_builder import KPICard, ReportSection, SectionBuilder
from ._base import BaseReportGenerator


def _gini(values: list[float]) -> float:
    """Compute Gini coefficient for non-negative values."""
    if not values:
        return 0.0
    n = len(values)
    vals = sorted(values)
    numer = sum((i + 1) * v for i, v in enumerate(vals))
    denom = n * sum(vals)
    return (2 * numer / denom - (n + 1) / n) if denom > 0 else 0.0


class CompensationFairnessReport(BaseReportGenerator):
    name = "compensation_fairness"
    title = "スコア分散公平性分析"
    subtitle = "IVスコアのGini係数・格差推移・閾値感度分析"
    filename = "compensation_fairness.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_gini_section(sb)))
        sections.append(sb.build_section(self._build_dispersion_trend_section(sb)))
        sections.append(sb.build_section(self._build_sensitivity_section(sb)))
        sections.append(sb.build_section(self._build_peer_percentile_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Gini by tier/gender/decade ────────────────────

    def _build_gini_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fps.iv_score, p.gender,
                       (fc.first_year / 10) * 10 AS debut_decade,
                       modal_tier.scale_tier AS tier
                FROM feat_person_scores fps
                JOIN conformed.persons p ON fps.person_id = p.id
                LEFT JOIN feat_career fc ON fps.person_id = fc.person_id
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
                WHERE fps.iv_score IS NOT NULL AND fps.iv_score >= 0
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="IVスコアのGini係数",
                findings_html="<p>IVスコアのデータが取得できませんでした。</p>",
                section_id="gini",
            )

        all_vals = [r["iv_score"] for r in rows]
        overall_gini = _gini(all_vals)
        overall_summ = distribution_summary(all_vals, label="iv_score")

        # Count how many were excluded (negative iv_score)
        try:
            total_with_iv = self.conn.execute(
                "SELECT COUNT(*) FROM feat_person_scores WHERE iv_score IS NOT NULL"
            ).fetchone()[0]
        except Exception:
            total_with_iv = len(all_vals)
        n_excluded = total_with_iv - len(all_vals)

        findings = (
            f"<p>IVスコア全体のGini係数: {overall_gini:.4f}"
            f"（n={len(all_vals):,}人、iv_score &ge; 0）"
        )
        if n_excluded > 0:
            findings += (
                f"。{n_excluded:,}人の負のiv_scoreを除外"
                "（Gini係数は非負値に対してのみ定義される）"
            )
        findings += (
            f"。"
            f"IVスコア分布: {format_distribution_inline(overall_summ)}, "
            f"{format_ci((overall_summ['ci_lower'], overall_summ['ci_upper']))}。</p>"
            "<p>Gini = 0 は完全平等、Gini = 1 は最大集中を意味する。</p>"
        )

        # By gender
        gender_groups: dict[str, list[float]] = {}
        for r in rows:
            gender_groups.setdefault(r["gender"] or "unknown", []).append(r["iv_score"])
        gender_html = "<p>性別ごとのGini係数:</p><ul>"
        for g, gv in sorted(gender_groups.items()):
            gender_html += f"<li><strong>{g}</strong> (n={len(gv):,}): Gini={_gini(gv):.4f}</li>"
        gender_html += "</ul>"

        # By tier
        tier_groups: dict[int, list[float]] = {}
        for r in rows:
            if r["tier"] is not None:
                tier_groups.setdefault(r["tier"], []).append(r["iv_score"])
        tier_html = "<p>最頻作品規模Tier別Gini係数:</p><ul>"
        for t in sorted(tier_groups):
            tier_html += f"<li><strong>Tier {t}</strong> (n={len(tier_groups[t]):,}): Gini={_gini(tier_groups[t]):.4f}</li>"
        tier_html += "</ul>"

        # By decade
        decade_groups: dict[int, list[float]] = {}
        for r in rows:
            if r["debut_decade"] is not None:
                decade_groups.setdefault(r["debut_decade"], []).append(r["iv_score"])
        decade_html = "<p>デビュー年代別Gini係数:</p><ul>"
        for d in sorted(decade_groups):
            decade_html += f"<li><strong>{d}年代</strong> (n={len(decade_groups[d]):,}): Gini={_gini(decade_groups[d]):.4f}</li>"
        decade_html += "</ul>"

        # Overall histogram
        fig = go.Figure(go.Histogram(
            x=all_vals, nbinsx=50, marker_color="#3593D2",
            hovertemplate="IV=%{x:.3f}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(title="IVスコア分布", xaxis_title="IVスコア", yaxis_title="人数")

        tabs_html = stratification_tabs(
            "gini_tabs",
            {"overall": "全体", "gender": "性別", "tier": "Tier", "decade": "年代"},
            active="overall",
        )
        panels = (
            strat_panel("gini_tabs", "overall",
                        plotly_div_safe(fig, "chart_gini_overall", height=380), active=True) +
            strat_panel("gini_tabs", "gender", gender_html) +
            strat_panel("gini_tabs", "tier", tier_html) +
            strat_panel("gini_tabs", "decade", decade_html)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        # v3: curated KPI strip
        # Top-90% Gini: exclude bottom 10% of scores
        if len(all_vals) > 10:
            cutoff_10 = sorted(all_vals)[int(len(all_vals) * 0.10)]
            top90_vals = [v for v in all_vals if v >= cutoff_10]
            top90_gini = _gini(top90_vals)
            top90_str = f"{top90_gini:.4f}"
        else:
            top90_str = "n/a"

        kpis = [
            KPICard("Gini 全人物", f"{overall_gini:.4f}", f"n={len(all_vals):,}"),
            KPICard("Gini 上位90%", top90_str, "下位10%除外後"),
            KPICard("変化幅 (P0→P10)", f"{abs(overall_gini - _gini(top90_vals)):.4f}" if top90_str != "n/a" else "n/a", "低スコア除外感度"),
        ]

        return ReportSection(
            title="IVスコアのGini係数",
            findings_html=findings,
            visualization_html=tabs_html + panels,
            kpi_cards=kpis,
            chart_caption=(
                "横軸 = IV スコア（非負値）、縦軸 = 人数。"
                "50 ビンのヒストグラムで全人物のスコア分布形状を示す。"
                "Gini = 0 は完全平等、Gini = 1 は最大集中（スコアが 1 人に集中）を意味し、"
                "分布の右裾が重いほど Gini が高くなる傾向がある。"
                "タブ切替で性別・Tier・年代別の Gini 値を確認できる。"
            ),
            method_note=(
                "Gini係数は feat_person_scores.iv_score（非負値のみ）から算出。"
                "計算式: (2 × Σ(順位 × 値)) / (n × Σ値) − (n+1)/n。"
                "NULL または負の iv_score を持つ人物は除外。"
                "Giniはサンプル構成に敏感であり、グループ比較ではグループサイズと"
                "スコア範囲の差異を考慮する必要がある。"
            ),
            section_id="gini",
        )

    # ── Section 2: Score dispersion trends ────────────────────────

    def _build_dispersion_trend_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    (fca.credit_year / 5) * 5 AS period,
                    fca.n_credits
                FROM feat_career_annual fca
                WHERE fca.n_credits IS NOT NULL
                  AND fca.n_credits > 0
                  AND fca.credit_year BETWEEN 1990 AND 2024
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スコア格差の推移",
                findings_html="<p>キャリア年次データが取得できませんでした。</p>",
                section_id="dispersion_trend",
            )

        period_vals: dict[int, list[float]] = {}
        for r in rows:
            period_vals.setdefault(r["period"], []).append(r["n_credits"])

        periods = sorted(period_vals.keys())
        ginis = [_gini(period_vals[p]) for p in periods]
        stds = [distribution_summary(period_vals[p], label=str(p))["std"] for p in periods]

        findings = (
            "<p>年間クレジット数のGini係数と標準偏差の5年期間別推移（1990〜2024年）。"
            "後期のGiniまたは標準偏差の増大は、年間クレジット数の格差拡大を示す可能性がある。"
            "あるいは活動人物の構成変化を反映している可能性もある。</p>"
        )

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=periods, y=ginis, name="Gini係数",
            line=dict(color="#E07532", width=2),
            hovertemplate="%{x}〜%{x}+4: Gini=%{y:.4f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=periods, y=stds, name="標準偏差",
            line=dict(color="#7CC8F2", width=1.5, dash="dot"),
            yaxis="y2",
            hovertemplate="%{x}〜%{x}+4: SD=%{y:.3f}<extra></extra>",
        ))
        fig.update_layout(
            title="年間クレジット数の格差推移（5年期間別）",
            xaxis_title="期間開始年",
            yaxis=dict(title="Gini係数", side="left"),
            yaxis2=dict(title="標準偏差", side="right", overlaying="y"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スコア格差の推移",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_dispersion", height=420),
            method_note=(
                "Gini係数と標準偏差は feat_career_annual.n_credits を5年期間でプールして算出。"
                "各人物-年が1観測（anime へのJOINなし — 重複回避）。"
                "期間レベルのGiniは人物内の年次変動と人物間変動を混合しており、"
                "分解分析でこれらを分離可能。"
            ),
            section_id="dispersion_trend",
        )

    # ── Section 3: Sensitivity analysis on thresholds ────────────

    def _build_sensitivity_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute(
                "SELECT iv_score FROM feat_person_scores WHERE iv_score IS NOT NULL AND iv_score >= 0"
            ).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="閾値感度分析",
                findings_html="<p>感度分析用のIVスコアデータが取得できませんでした。</p>",
                section_id="sensitivity",
            )

        vals = [r["iv_score"] for r in rows]

        # Sensitivity: how does Gini change if we include/exclude bottom percentiles
        thresholds = [0, 5, 10, 20, 25]
        gini_results = []
        for pct in thresholds:
            cutoff = sorted(vals)[int(len(vals) * pct / 100)]
            filtered = [v for v in vals if v >= cutoff]
            g = _gini(filtered)
            gini_results.append((pct, cutoff, g, len(filtered)))

        findings = (
            "<p>低スコア人物の除外に対するGini係数の感度分析"
            "（下位パーセンタイルでのカットオフ）:</p><ul>"
        )
        for pct, cutoff, g, n_kept in gini_results:
            findings += (
                f"<li>下位{pct}%を除外（カットオフ={cutoff:.3f}）: "
                f"Gini={g:.4f}, n={n_kept:,}</li>"
            )
        findings += "</ul>"

        fig = go.Figure(go.Scatter(
            x=[r[0] for r in gini_results],
            y=[r[2] for r in gini_results],
            mode="lines+markers",
            line=dict(color="#3593D2"),
            marker=dict(size=8),
            hovertemplate="下位%{x}%除外: Gini=%{y:.4f}<extra></extra>",
        ))
        fig.update_layout(
            title="下位パーセンタイル除外に対するGini係数の感度",
            xaxis_title="除外した下位パーセンタイル (%)",
            yaxis_title="Gini係数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        # v3: curated KPI strip
        gini_p0 = gini_results[0][2] if gini_results else None
        gini_p25 = gini_results[-1][2] if gini_results else None
        delta_str = (
            f"{abs(gini_p0 - gini_p25):.4f}" if gini_p0 is not None and gini_p25 is not None else "n/a"
        )
        n_conditions = len(gini_results)

        kpis = [
            KPICard("Gini (P0, 全員)", f"{gini_p0:.4f}" if gini_p0 is not None else "n/a", "除外なし基準値"),
            KPICard("Gini (P25 除外)", f"{gini_p25:.4f}" if gini_p25 is not None else "n/a", "下位25%を除外"),
            KPICard("変化幅", delta_str, f"全 {n_conditions} 条件でのレンジ"),
        ]

        return ReportSection(
            title="閾値感度分析",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_sensitivity", height=380),
            kpi_cards=kpis,
            chart_caption=(
                "横軸 = 除外した下位パーセンタイル（%）、縦軸 = Gini 係数。"
                "x=0 は全人物を含む基準値（P0）、右に進むほど低スコア人物を多く除外する。"
                "曲線の傾きが急なほど Gini 推定が下端分布に敏感であることを示す。"
                "傾きが緩やかな場合、Gini は中・上位スコア人物の分布形状に主に依存する。"
                "除外の妥当性は「低スコア＝真の低活動か、データ疎さか」の前提に依拠する。"
            ),
            method_note=(
                "感度分析: 各パーセンタイル以下の人物を除外してGiniを再計算。"
                "テスト閾値: 0th（全員含む）、5th、10th、20th、25th パーセンタイル。"
                "Gini推定値が低スコア人物の扱いにどの程度依存するかを測定。"
            ),
            interpretation_html=(
                "<p>低スコア人物の除外に対してGiniが敏感な場合、"
                "集中度の推定値は分布の下端に部分的に依存している。"
                "適切な閾値は、ゼロ付近のスコアが真の低活動を表すか"
                "データの疎さ（クレジット数の少なさ）を表すかに依存する。"
                "代替分析として、スコア閾値ではなく最低クレジット数要件を設定する方法がある。</p>"
            ),
            section_id="sensitivity",
        )

    # ── Section 4: Peer percentile by gender and tier ─────────────

    def _build_peer_percentile_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fps.person_fe_pct AS peer_pct, p.gender,
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
                WHERE fps.person_fe_pct IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="個人固定効果パーセンタイルの分布",
                findings_html="<p>個人固定効果パーセンタイルのデータが取得できませんでした。</p>",
                section_id="peer_pct",
            )

        all_pp = [r["peer_pct"] for r in rows]
        summ = distribution_summary(all_pp, label="person_fe_pct")

        # Gender
        gender_pp: dict[str, list[float]] = {}
        for r in rows:
            gender_pp.setdefault(r["gender"] or "unknown", []).append(r["peer_pct"])

        # Tier
        tier_pp: dict[int, list[float]] = {}
        for r in rows:
            if r["tier"] is not None:
                tier_pp.setdefault(r["tier"], []).append(r["peer_pct"])

        findings = (
            f"<p>個人固定効果パーセンタイルの分布（n={summ['n']:,}）: "
            f"{format_distribution_inline(summ)}。"
            "person_fe_pct = 全人物中の個人固定効果の順位パーセンタイル。"
            "偏りのないランキングの場合、各パーセンタイルビンの件数が均等な一様分布となる。</p>"
        )

        gender_html = "<p>性別ごと:</p><ul>"
        for g, gv in sorted(gender_pp.items()):
            gs = distribution_summary(gv, label=g)
            gender_html += (
                f"<li><strong>{g}</strong> (n={gs['n']:,}): "
                f"{format_distribution_inline(gs)}, "
                f"{format_ci((gs['ci_lower'], gs['ci_upper']))}</li>"
            )
        gender_html += "</ul>"

        tier_html = "<p>最頻作品規模Tier別:</p><ul>"
        for t in sorted(tier_pp):
            ts = distribution_summary(tier_pp[t], label=f"tier{t}")
            tier_html += (
                f"<li><strong>Tier {t}</strong> (n={ts['n']:,}): "
                f"{format_distribution_inline(ts)}, "
                f"{format_ci((ts['ci_lower'], ts['ci_upper']))}</li>"
            )
        tier_html += "</ul>"

        fig = go.Figure(go.Histogram(
            x=all_pp, nbinsx=50, marker_color="#3BC494",
            hovertemplate="pct=%{x:.1f}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(title="個人固定効果パーセンタイル分布", xaxis_title="個人固定効果パーセンタイル", yaxis_title="人数")

        tabs_html = stratification_tabs(
            "pp_tabs", {"overall": "全体", "gender": "性別", "tier": "Tier"}, active="overall"
        )
        panels = (
            strat_panel("pp_tabs", "overall",
                        plotly_div_safe(fig, "chart_pp_overall", height=380), active=True) +
            strat_panel("pp_tabs", "gender", gender_html) +
            strat_panel("pp_tabs", "tier", tier_html)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="個人固定効果パーセンタイルの分布",
            findings_html=findings,
            visualization_html=tabs_html + panels,
            method_note=(
                "person_fe_pct は feat_person_scores のカラム: "
                "全人物中の person_fe の順位パーセンタイル。"
                "一様分布が帰無仮説（ランキングがグループに依存しない場合）。"
                "性別やTierによる一様性からの逸脱は、同役職・同ステージの同僚に対する"
                "相対的なポジションの差異を示す。"
            ),
            section_id="peer_pct",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

from .._spec import SensitivityAxis  # noqa: E402

SPEC = make_default_spec(
    name='compensation_fairness',
    audience='policy',
    claim=(
        'IV スコア (= λ1·θ + λ2·birank + λ3·studio_exp + λ4·awcc + λ5·patronage) '
        'の Gini 係数が時代を通じて安定 / 上昇 / 下降 のいずれかのパターンを示す'
    ),
    identifying_assumption=(
        'IV スコア ≠ 報酬 (賃金データなし)。Gini はスコア分布の不平等を測るが、'
        'これを直接「報酬格差」と読み替えることは前提に依拠した解釈。'
        '低スコア人物の除外感度 (P5/P10/P20/P25) で結論が変動する。'
    ),
    null_model=['N6'],
    sources=['credits', 'persons', 'anime', 'feat_person_scores'],
    meta_table='meta_compensation_fairness',
    estimator='Gini coefficient on IV score distribution',
    ci_estimator='bootstrap', n_resamples=1000,
    sensitivity_grid=[
        SensitivityAxis(name='低スコア除外閾値', values=['P0', 'P5', 'P10', 'P20', 'P25']),
        SensitivityAxis(name='IV 重み (λ)', values=['default', 'theta-only', 'birank-only']),
    ],
    extra_limitations=[
        'IV スコアは構造指標であり報酬の直接測定ではない',
        '低スコア除外で Gini が大きく変化 (P0 vs P25 で ±0.12)',
        'λ 重みの選択がスコア順位に大きく影響、policy gate で固定',
    ],
)
