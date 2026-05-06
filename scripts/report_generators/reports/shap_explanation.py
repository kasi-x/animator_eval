"""SHAP Explanation report — v2 compliant.

SHAP feature importance and model explanation:
- Section 1: Feature importance (mean |SHAP|)
- Section 2: SHAP value distribution by feature
- Section 3: Holdout validation (v2 Section 3.3 for predictive claims)
- Section 4: Partial dependence (top features)
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_distribution_inline
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class SHAPExplanationReport(BaseReportGenerator):
    name = "shap_explanation"
    title = "SHAP特徴量重要度"
    subtitle = "IVスコア予測モデルのSHAP値・ホールドアウト検証"
    filename = "shap_explanation.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_importance_section(sb)))
        sections.append(sb.build_section(self._build_shap_distribution_section(sb)))
        sections.append(sb.build_section(self._build_holdout_section(sb)))
        sections.append(sb.build_section(self._build_pdp_section(sb)))
        return self.write_report("\n".join(sections))

    def _build_importance_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT feature_name, mean_abs_shap, rank
                FROM feat_shap_importance
                WHERE mean_abs_shap IS NOT NULL
                ORDER BY mean_abs_shap DESC
                LIMIT 15
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="特徴量重要度（mean|SHAP|）",
                findings_html=(
                    "<p>SHAP重要度データが利用できません（feat_shap_importance）。"
                    "SHAP値の算出にはPhase 9 SHAP説明モジュールの実行が必要です。"
                    "モデル: GradientBoostingRegressor（IVスコア予測、13特徴量: "
                    "birank, patronage, person_fe, awcc, ndi, dormancy, career_friction等）。</p>"
                ),
                section_id="shap_importance",
            )

        findings = "<p>平均絶対SHAP値による特徴量重要度上位（IVスコア予測モデル）:</p><ul>"
        for r in rows:
            findings += (
                f"<li><strong>{r['feature_name']}</strong> "
                f"(rank #{r['rank']}): mean|SHAP|={r['mean_abs_shap']:.4f}</li>"
            )
        findings += "</ul>"

        fig = go.Figure(go.Bar(
            x=[r["mean_abs_shap"] for r in reversed(rows)],
            y=[r["feature_name"] for r in reversed(rows)],
            orientation="h",
            marker_color="#3593D2",
            hovertemplate="%{y}: %{x:.4f}<extra></extra>",
        ))
        fig.update_layout(
            title="特徴量重要度（mean |SHAP|）",
            xaxis_title="mean |SHAP|", yaxis_title="特徴量",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="特徴量重要度（mean|SHAP|）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_shap_imp", height=500),
            method_note=(
                "SHAP values from TreeExplainer (Phase 9: SHAP module). "
                "Model: GradientBoostingRegressor(n_estimators=200, max_depth=4) predicting iv_score. "
                "mean|SHAP| = mean of |shap_value| across all persons in training set. "
                "SHAP importance reflects feature usage in this specific model; "
                "it does not imply causal relevance of features."
            ),
            interpretation_html=(
                "<p>SHAP重要度は本予測モデルへの寄与度で特徴量をランク付けする。"
                "SHAP重要度が高い特徴量は、他の特徴量を所与としたときに予測上有用であることを示すが、"
                "因果的に重要とは限らない。"
                "代替的な分析として、因果的文脈（例: 因果グラフ）でShapley値を用いる方法があり、"
                "ML-SHAP値とは異なる結果となる。</p>"
            ),
            section_id="shap_importance",
        )

    def _build_shap_distribution_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT feature_name, shap_value, feature_value
                FROM feat_shap_values
                WHERE shap_value IS NOT NULL
                LIMIT 10000
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="SHAP値の分布",
                findings_html="<p>SHAP値分布データが利用できません（feat_shap_values）。</p>",
                section_id="shap_dist",
            )

        feature_shap: dict[str, list[float]] = {}
        for r in rows:
            feature_shap.setdefault(r["feature_name"], []).append(r["shap_value"])

        top_features = sorted(feature_shap, key=lambda f: -sum(abs(v) for v in feature_shap[f]))[:8]

        findings = "<p>上位8特徴量のSHAP値分布:</p><ul>"
        for f in top_features:
            fs = distribution_summary(feature_shap[f], label=f)
            findings += (
                f"<li><strong>{f}</strong> (n={fs['n']:,}): "
                f"{format_distribution_inline(fs)}</li>"
            )
        findings += "</ul>"

        fig = go.Figure()
        colors = ["#E09BC2", "#7CC8F2", "#3BC494", "#F8EC6A",
                  "#3593D2", "#E07532", "#FFB444", "#8a94a0"]
        for i, f in enumerate(top_features):
            fig.add_trace(go.Violin(
                y=feature_shap[f][:500] if len(feature_shap[f]) > 500 else feature_shap[f],
                name=f[:20], box_visible=True, meanline_visible=True,
                points=False, marker_color=colors[i % len(colors)],
            ))
        fig.update_layout(title="特徴量別 SHAP値の分布", yaxis_title="SHAP値")

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="SHAP値の分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_shap_dist", height=440),
            method_note=(
                "SHAP values from feat_shap_values (sample capped at 10,000). "
                "Positive SHAP: feature increases predicted iv_score. "
                "Negative SHAP: feature decreases predicted iv_score."
            ),
            section_id="shap_dist",
        )

    def _build_holdout_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            row = self.conn.execute("""
                SELECT train_r2, holdout_r2, train_rmse, holdout_rmse, n_train, n_holdout
                FROM feat_shap_model_metrics
                WHERE holdout_r2 IS NOT NULL
                LIMIT 1
            """).fetchone()
        except Exception:
            row = None

        if not row:
            return ReportSection(
                title="ホールドアウト検証（v2 Section 3.3）",
                findings_html=(
                    "<p>ホールドアウト検証指標が利用できません（feat_shap_model_metrics）。"
                    "v2 Section 3.3に基づき、予測的主張にはホールドアウト検証が必要です。"
                    "SHAPモジュールは学習R²、テストR²、RMSEを"
                    "20%テストセットで報告する必要があります。</p>"
                ),
                section_id="holdout",
            )

        findings = (
            f"<p>モデル検証（ホールドアウト=ランダム20%分割）: "
            f"学習R²={row['train_r2']:.4f}、ホールドアウトR²={row['holdout_r2']:.4f}。"
            f"学習RMSE={row['train_rmse']:.4f}、ホールドアウトRMSE={row['holdout_rmse']:.4f}。"
            f"n_train={row['n_train']:,}、n_holdout={row['n_holdout']:,}。</p>"
            "<p>R²ギャップ（学習 − ホールドアウト）は過学習の程度を示す。"
            "ギャップが大きいほど特徴量重要度推定の信頼性は低下する。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="ホールドアウト検証（v2 Section 3.3）",
            findings_html=findings,
            method_note=(
                "Holdout split: random 80/20 split stratified by career_track. "
                "R² and RMSE computed on holdout set only. "
                "SHAP values computed on training set (not holdout) — "
                "importance may be slightly inflated by training set overfitting."
            ),
            section_id="holdout",
        )

    def _build_pdp_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT feature_name, feature_value, pdp_value
                FROM feat_shap_pdp
                WHERE pdp_value IS NOT NULL
                ORDER BY feature_name, feature_value
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="部分依存プロット（Top 4特徴量）",
                findings_html="<p>PDPデータが利用できません（feat_shap_pdp）。</p>",
                section_id="pdp",
            )

        feature_pdp: dict[str, list[tuple[float, float]]] = {}
        for r in rows:
            feature_pdp.setdefault(r["feature_name"], []).append(
                (r["feature_value"], r["pdp_value"])
            )

        top4 = sorted(feature_pdp.keys())[:4]
        findings = "<p>上位特徴量の部分依存プロット: 他の特徴量の周辺分布で平均化した、各特徴量の関数としての予測iv_score。</p>"

        fig = go.Figure()
        colors = ["#3593D2", "#3BC494", "#F8EC6A", "#E07532"]
        for i, f in enumerate(top4):
            pts = sorted(feature_pdp[f], key=lambda x: x[0])
            fig.add_trace(go.Scatter(
                x=[p[0] for p in pts],
                y=[p[1] for p in pts],
                name=f[:20], mode="lines+markers",
                line=dict(color=colors[i % len(colors)]),
            ))
        fig.update_layout(
            title="部分依存プロット（上位4特徴量）",
            xaxis_title="特徴量の値", yaxis_title="予測IV（周辺）",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="部分依存プロット（Top 4特徴量）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_pdp", height=420),
            method_note=(
                "PDP from feat_shap_pdp: average predicted output as a function of one feature, "
                "marginalizing over all other features. "
                "PDPs can be misleading when features are correlated."
            ),
            section_id="pdp",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

from .._spec import HoldoutSpec  # noqa: E402

SPEC = make_default_spec(
    name='shap_explanation',
    audience='technical_appendix',
    claim=(
        'IV スコア予測 GBM モデルの SHAP 値 上位 10 特徴量 (5 IV 成分 + '
        'デモグラ + 経験) のうち上位 3 が SHAP |φ| 寄与の 50% 以上を占める'
    ),
    identifying_assumption=(
        'SHAP は Shapley 値の機械学習近似 — 特徴量間の依存性 (correlated features) '
        'で寄与配分が変動する。MDI (Mean Decrease Impurity) と一致しない場合あり。'
        'ホールドアウト検証で予測精度 (R² > 0.7) を gate とする。'
    ),
    null_model=['N3'],
    sources=['credits', 'persons', 'anime', 'feat_person_scores'],
    meta_table='meta_shap_explanation',
    estimator='SHAP TreeExplainer on GBM regressor',
    ci_estimator='bootstrap', n_resamples=200,
    holdout=HoldoutSpec(
        method='time-split',
        holdout_size='last 3 years (2022-2024)',
        metric='R² (IV score prediction)',
        naive_baseline='median IV score',
    ),
    extra_limitations=[
        'SHAP は correlated feature で寄与配分が不安定',
        'TreeExplainer は GBM 専用、他モデル (RF / linear) では実装変更必要',
        'ホールドアウト R² < 0.7 のときは特徴量重要度を非公開化',
    ],
)
