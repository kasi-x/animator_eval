"""Temporal Foresight report — v2 compliant.

Retrospective activity pattern analysis (v2 method gate compliance):
- Section 1: Activity ratio retrospective comparison
- Section 2: Growth trajectory classification patterns
- Section 3: Early career indicators retrospective view
- Section 4: Person FE estimation and confidence intervals
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
    title = "キャリア軌跡記述分析"
    subtitle = "活動パターンの回顧的記述・不確実性の定量化（v2 3.3）"
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
                title="活動比率の回顧的記述（v2 Section 3.3）",
                findings_html=(
                    "<p>活動比率比較用データが取得できませんでした。"
                    "v2 Section 3.3に基づき、個人のキャリア軌跡に関する前向き予測には "
                    "時間的に分離された保留期間の検証が必須です。"
                    "本セクションは、活動パターンの過去観測を記述するプレースホルダーとして機能します。"
                    "feat_career_annual に十分な時間的カバレッジが蓄積された時点で本セクションが有効になる。</p>"
                ),
                section_id="foresight_holdout",
            )

        actuals = [r["actual"] for r in rows]
        proxies = [r["predicted_proxy"] for r in rows]
        n = len(rows)

        findings = (
            f"<p>活動比率の回顧的比較: 直近2年間の年間クレジット数 vs activity_ratio "
            f"（n={n:,}人-年観測）。"
            "activity_ratio は同一時間窓から算出されたため、この相関は記述的であり、"
            "活動パターンの静的スナップショットを示すに過ぎない。"
            "本セクションは前向きの予測ではなく、過去観測の相関構造を表示する。</p>"
        )

        fig = go.Figure(go.Scatter(
            x=proxies, y=actuals,
            mode="markers",
            marker=dict(color="#3593D2", size=4, opacity=0.5),
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
            title="活動比率の回顧的記述（v2 Section 3.3）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_foresight_holdout", height=420),
            method_note=(
                "v2 Section 3.3の要件: 個人のキャリア軌跡に関する前向き予測には、"
                "期間Tで訓練したモデルを期間T+kで検証すること。"
                "本セクションでは、activity_ratio と実観測クレジット数の相関を記述的に表示。"
                "適切な時系列検証は現行パイプラインに未実装のため、"
                "本セクションは活動パターン記述のプレースホルダーとして機能する。"
            ),
            interpretation_html=(
                "<p>v2 Section 3.3に基づき、時間的分離のない個人のキャリア軌跡に関する"
                "前向き予測的主張は禁止されている。"
                "ここに示す相関は回顧的な記述であり、activity_ratio は n_credits と"
                "同じ時間窓から算出されているため、循環的である。"
                "真の前向き検証には、activity_ratio の算出時期と n_credits の観測時期を "
                "時間的に分離する必要がある。</p>"
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
                title="成長傾向分類の集計",
                findings_html="<p>軌跡分類データが取得できませんでした。</p>",
                section_id="traj_class",
            )

        trend_track: dict[str, dict[str, int]] = {}
        for r in rows:
            trend_track.setdefault(r["trend"], {})[r["career_track"] or "unknown"] = r["n"]

        findings = "<p>成長傾向（growth_trend）別の人物数、キャリアトラック内訳（記述的集計）:</p><ul>"
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
            title="成長傾向分類の集計",
            findings_html=findings,
            method_note=(
                "growth_trend は feat_career のカラム（カテゴリカル軌跡タイプ）。"
                "本セクションは過去の軌跡分類パターンの記述的集計である。"
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
                title="初期指標の回顧的記述",
                findings_html="<p>初期指標データが取得できませんでした。</p>",
                section_id="early_indicator",
            )

        tier_early: dict[int, list[float]] = {}
        for r in rows:
            if r["eventual_max_tier"] is not None:
                tier_early.setdefault(r["eventual_max_tier"], []).append(r["early_credits"])

        findings = "<p>初期キャリア年間クレジット数（career_year &le; 3）の最終到達最大Tier別分布（回顧的）:</p><ul>"
        for t in sorted(tier_early):
            ts = distribution_summary(tier_early[t], label=f"tier{t}")
            findings += (
                f"<li><strong>最大Tier {t}</strong> (n={ts['n']:,}): "
                f"{format_distribution_inline(ts)}</li>"
            )
        findings += (
            "</ul><p>注: これは回顧的な相関記述であり、個人の初期指標を用いた前向き予測ではない。"
            "選択バイアス: 高Tierに到達し、かつキャリア初期にクレジットされた人物が過剰代表されている。</p>"
        )

        fig = go.Figure()
        tier_colors = {1: "#3593D2", 2: "#7CC8F2", 3: "#3BC494", 4: "#F8EC6A", 5: "#E07532"}
        for t in sorted(tier_early):
            fig.add_trace(go.Box(
                y=tier_early[t], name=f"最大T{t}",
                marker_color=tier_colors.get(t, "#a0a0c0"), boxpoints=False,
            ))
        fig.update_layout(
            title="初期キャリアクレジット数 × 最終到達最大Tier（回顧的）",
            xaxis_title="最終到達最大Tier", yaxis_title="初期クレジット数（career_year ≤ 3）",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="初期指標の回顧的記述",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_early_ind", height=420),
            method_note=(
                "初期キャリア指標 = career_year 0〜3 における年間クレジット数（n_credits）。"
                "最終到達最大Tier = 全クレジットにおける MAX(scale_tier)。"
                "本セクションは回顧的な相関記述であり、前向きの予測モデルではない。"
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
                title="Person FE推定と信頼区間",
                findings_html="<p>推定CI データが取得できませんでした。</p>",
                section_id="pred_interval",
            )

        findings = (
            "<p>200人のサンプルに対する個人固定効果と95% CI（FE順にソート）。"
            "区間幅は現在の推定の不確実性を反映: クレジット数が少ない、またはスタジオ移動が少ない人物は"
            "CIが広くなる。"
            "v2 Section 3.1に基づき、個人レベルの推定にはCI提示が必須。</p>"
        )

        rows_sorted = sorted(rows, key=lambda r: r["person_fe"])
        indices = list(range(len(rows_sorted)))
        fes = [r["person_fe"] for r in rows_sorted]
        ci_lo = [r["person_fe"] - 1.96 * r["person_fe_se"] for r in rows_sorted]
        ci_hi = [r["person_fe"] + 1.96 * r["person_fe_se"] for r in rows_sorted]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=indices, y=fes, name="Person FE",
            mode="lines", line=dict(color="#3593D2"),
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
            title="Person FE推定と信頼区間",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_pred_interval", height=420),
            method_note=(
                "95% CI = person_fe ± 1.96 × person_fe_se（OLSガウス型CI）。"
                "可視化の明瞭化のため200人をサンプリング。"
                "これは現在のFE推定値に対する信頼区間であり、将来の予測区間ではない。"
            ),
            section_id="pred_interval",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='temporal_foresight',
    audience='technical_appendix',
    claim='キャリア軌跡記述分析 に関する記述的指標 (subtitle: 活動パターンの回顧的記述・不確実性の定量化（v2 3.3）)',
    sources=["credits", "persons", "anime"],
    meta_table='meta_temporal_foresight',
)
