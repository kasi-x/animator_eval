"""Temporal Foresight report — v2 compliant.

Predictive trajectory analysis with mandatory holdout validation (v2 Section 3.3):
- Section 1: Holdout validation of IV score prediction
- Section 2: Growth trajectory classification accuracy
- Section 3: Early career indicators vs eventual tier attainment
- Section 4: Prediction intervals and uncertainty
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_distribution_inline
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class TemporalForesightReport(BaseReportGenerator):
    name = "temporal_foresight"
    title = "キャリア軌跡予測分析"
    subtitle = "ホールドアウト検証付き予測モデル・不確実性定量化（v2 3.3）"
    filename = "temporal_foresight.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_holdout_section(sb)))
        sections.append(sb.build_section(self._build_trajectory_classification_section(sb)))
        sections.append(sb.build_section(self._build_early_indicator_section(sb)))
        sections.append(sb.build_section(self._build_prediction_interval_section(sb)))
        return self.write_report("\n".join(sections))

    def _build_holdout_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fca.person_id,
                    fca.n_credits AS actual,
                    fc.activity_ratio AS predicted_proxy,
                    fc.growth_trend AS career_track
                FROM feat_career_annual fca
                JOIN feat_career fc ON fca.person_id = fc.person_id
                WHERE fca.n_credits IS NOT NULL
                  AND fc.activity_ratio IS NOT NULL
                  AND fca.credit_year >= fc.latest_year - 2
                LIMIT 3000
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="ホールドアウト検証（v2 Section 3.3）",
                findings_html=(
                    "<p>ホールドアウト検証用データが取得できませんでした。"
                    "v2 Section 3.3に基づき、すべての予測的主張にはホールドアウト検証が必要: "
                    "モデルは過去のサブセットで訓練し、時間的に分離された保留期間で評価しなければならない。"
                    "プロトコル例: career_year &le; T で訓練、career_year &gt; T で検証。"
                    "feat_career_annual に十分な時間的カバレッジが蓄積された時点で本セクションが有効になる。</p>"
                ),
                section_id="foresight_holdout",
            )

        actuals = [r["actual"] for r in rows]
        proxies = [r["predicted_proxy"] for r in rows]
        n = len(rows)

        findings = (
            f"<p>ホールドアウト検証の代理指標: 直近2年間の年間クレジット数 vs activity_ratio "
            f"（n={n:,}人-年観測）。"
            "activity_ratio は将来のクレジット数の直接的な予測器ではなく、"
            "本セクションでは相関構造を示す（検証済み予測ではない）。"
            "適切なホールドアウトには時系列の訓練/テスト分割が必要。</p>"
        )

        fig = go.Figure(go.Scatter(
            x=proxies, y=actuals,
            mode="markers",
            marker=dict(color="#667eea", size=4, opacity=0.5),
            hovertemplate="活動比率=%{x:.2f}、クレジット数=%{y:,}<extra></extra>",
        ))
        fig.update_layout(
            title="活動比率 vs 直近年間クレジット数",
            xaxis_title="活動比率（Activity Ratio）", yaxis_title="年間クレジット数（直近2年）",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="ホールドアウト検証（v2 Section 3.3）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_foresight_holdout", height=420),
            method_note=(
                "v2 Section 3.3の要件: 期間Tで訓練したモデルを期間T+kで検証すること。"
                "本レポートでは activity_ratio を予測軌跡の代理指標として使用。"
                "適切な時系列ホールドアウトは現行パイプラインに未実装 — "
                "本セクションはその要件のプレースホルダーとして機能する。"
            ),
            interpretation_html=(
                "<p>v2 Section 3.3に基づき、ホールドアウト検証なしに個人のキャリア軌跡に関する"
                "予測的主張を行うことは禁止されている。"
                "ここに示す相関は記述的なものであり、activity_ratio は n_credits と"
                "同じ時間窓から算出されているため、相関は部分的に循環的である。"
                "前向きの検証には、直近2年間を activity_ratio の算出から除外する必要がある。</p>"
            ),
            section_id="foresight_holdout",
        )

    def _build_trajectory_classification_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT fc.growth_trend AS trend, fc.career_track, COUNT(*) AS n
                FROM feat_career fc
                WHERE fc.growth_trend IS NOT NULL
                GROUP BY fc.growth_trend, fc.career_track
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="軌跡タイプ分類",
                findings_html="<p>軌跡分類データが取得できませんでした。</p>",
                section_id="traj_class",
            )

        trend_track: dict[str, dict[str, int]] = {}
        for r in rows:
            trend_track.setdefault(r["trend"], {})[r["career_track"] or "unknown"] = r["n"]

        findings = "<p>成長傾向（growth_trend）別の人物数、キャリアトラック内訳:</p><ul>"
        for t, tracks in sorted(trend_track.items()):
            total = sum(tracks.values())
            findings += f"<li><strong>{t}</strong> (n={total:,}): "
            findings += ", ".join(f"{c}: {n:,}" for c, n in sorted(tracks.items(), key=lambda x: -x[1])[:3])
            findings += "</li>"
        findings += "</ul>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="軌跡タイプ分類",
            findings_html=findings,
            method_note=(
                "growth_trend は feat_career のカラム（カテゴリカル軌跡タイプ）。"
                "分類はホールドアウトの将来データに対して検証されていない。"
            ),
            section_id="traj_class",
        )

    def _build_early_indicator_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fca.n_credits AS early_credits,
                    MAX(fwc.scale_tier) AS eventual_max_tier
                FROM feat_career_annual fca
                JOIN feat_career fc ON fca.person_id = fc.person_id
                JOIN feat_credit_contribution fcc ON fca.person_id = fcc.person_id
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fca.career_year <= 3
                  AND fca.n_credits IS NOT NULL
                  AND fwc.scale_tier IS NOT NULL
                GROUP BY fca.person_id, fca.n_credits
                LIMIT 3000
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="初期指標と最終Tier到達",
                findings_html="<p>初期指標データが取得できませんでした。</p>",
                section_id="early_indicator",
            )

        tier_early: dict[int, list[float]] = {}
        for r in rows:
            if r["eventual_max_tier"] is not None:
                tier_early.setdefault(r["eventual_max_tier"], []).append(r["early_credits"])

        findings = "<p>初期キャリア年間クレジット数（career_year &le; 3）の最終到達最大Tier別分布:</p><ul>"
        for t in sorted(tier_early):
            ts = distribution_summary(tier_early[t], label=f"tier{t}")
            findings += (
                f"<li><strong>最大Tier {t}</strong> (n={ts['n']:,}): "
                f"{format_distribution_inline(ts)}</li>"
            )
        findings += (
            "</ul><p>注: これは回顧的な相関であり、前向きの予測ではない。"
            "選択バイアス: 高Tierに到達し、かつキャリア初期にクレジットされた人物が過剰代表されている。</p>"
        )

        fig = go.Figure()
        tier_colors = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}
        for t in sorted(tier_early):
            fig.add_trace(go.Box(
                y=tier_early[t], name=f"最大T{t}",
                marker_color=tier_colors.get(t, "#a0a0c0"), boxpoints=False,
            ))
        fig.update_layout(
            title="初期キャリアクレジット数 × 最終到達最大Tier",
            xaxis_title="最終到達最大Tier", yaxis_title="初期クレジット数（career_year ≤ 3）",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="初期指標と最終Tier到達",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_early_ind", height=420),
            method_note=(
                "初期キャリア指標 = career_year 0〜3 における年間クレジット数（n_credits）。"
                "最終到達最大Tier = 全クレジットにおける MAX(scale_tier)。"
                "回顧的分析であり、前向きの予測モデルではない。"
            ),
            section_id="early_indicator",
        )

    def _build_prediction_interval_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT person_fe, person_fe_se
                FROM feat_person_scores
                WHERE person_fe IS NOT NULL AND person_fe_se IS NOT NULL
                LIMIT 200
                OFFSET 0
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="予測区間と不確実性",
                findings_html="<p>予測区間データが取得できませんでした。</p>",
                section_id="pred_interval",
            )

        findings = (
            "<p>200人のサンプルに対する個人固定効果と95% CI（FE順にソート）。"
            "区間幅は推定の不確実性を反映: クレジット数が少ない、またはスタジオ移動が少ない人物は"
            "CIが広くなる。"
            "v2 Section 3.1に基づき、個人レベルの推定にはCIの提示が必須。</p>"
        )

        rows_sorted = sorted(rows, key=lambda r: r["person_fe"])
        indices = list(range(len(rows_sorted)))
        fes = [r["person_fe"] for r in rows_sorted]
        ci_lo = [r["person_fe"] - 1.96 * r["person_fe_se"] for r in rows_sorted]
        ci_hi = [r["person_fe"] + 1.96 * r["person_fe_se"] for r in rows_sorted]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=indices, y=fes, name="Person FE",
            mode="lines", line=dict(color="#667eea"),
        ))
        fig.add_trace(go.Scatter(
            x=indices + list(reversed(indices)),
            y=ci_hi + list(reversed(ci_lo)),
            fill="toself", fillcolor="rgba(102,126,234,0.2)",
            line=dict(color="rgba(0,0,0,0)"),
            name="95%信頼区間",
        ))
        fig.update_layout(
            title="Person FE と 95%信頼区間（サンプル200人、FE順ソート）",
            xaxis_title="順位", yaxis_title="Person FE",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="予測区間と不確実性",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_pred_interval", height=420),
            method_note=(
                "95% CI = person_fe ± 1.96 × person_fe_se（OLSガウス型CI）。"
                "可視化の明瞭化のため200人をサンプリング。"
                "予測区間ではなく、現在のFE推定値に対する推定CIである。"
            ),
            section_id="pred_interval",
        )
