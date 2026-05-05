"""DML Causal Inference report — v2 compliant.

Double Machine Learning (DML) causal estimates:
- Section 1: DML estimate overview (treatment effects with CIs)
- Section 2: Treatment effects by tier
- Section 3: Sensitivity to nuisance model specification
- Section 4: Refutation tests
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class DMLCausalInferenceReport(BaseReportGenerator):
    name = "dml_causal_inference"
    title = "DML因果推定レポート"
    subtitle = "二重機械学習による処置効果推定（Tier別・感度分析付き）"
    filename = "dml_causal_inference.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_overview_section(sb)))
        sections.append(sb.build_section(self._build_tier_effect_section(sb)))
        sections.append(sb.build_section(self._build_sensitivity_section(sb)))
        sections.append(sb.build_section(self._build_refutation_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: DML estimates overview ────────────────────────

    def _build_overview_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT treatment, outcome, ate, ate_se, n_obs, method
                FROM feat_causal_estimates
                WHERE ate IS NOT NULL
                ORDER BY ABS(ate) DESC
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="DML処置効果推定値",
                findings_html=(
                    "<p>因果推定値が利用できません（feat_causal_estimates）。"
                    "DML（Double Machine Learning）推定にはPhase 9因果推論モジュールの実行が必要です。"
                    "ATE（平均処置効果）は処置変数がアウトカム変数に与える因果的効果を"
                    "交差適合法で交絡因子を制御して推定します。</p>"
                ),
                section_id="dml_overview",
            )

        findings = "<p>DML平均処置効果（ATE）推定値と95%信頼区間:</p><ul>"
        for r in rows:
            ci_lo = r["ate"] - 1.96 * r["ate_se"]
            ci_hi = r["ate"] + 1.96 * r["ate_se"]
            findings += (
                f"<li><strong>{r['treatment']} → {r['outcome']}</strong> "
                f"(n={r['n_obs']:,}, method={r['method'] or 'DML'}): "
                f"ATE={r['ate']:.4f}, 95% CI [{ci_lo:.4f}, {ci_hi:.4f}]</li>"
            )
        findings += "</ul>"

        # v3: CIScatter primitive — null reference (ATE=0) / null vs sig
        # 識別 / sort 入力順
        from src.viz import embed as viz_embed
        from src.viz.primitives import CIPoint, CIScatterSpec, render_ci_scatter

        ci_points = [
            CIPoint(
                label=f"{r['treatment']}→{r['outcome']}",
                x=r["ate"],
                ci_lo=r["ate"] - 1.96 * r["ate_se"],
                ci_hi=r["ate"] + 1.96 * r["ate_se"],
                # 95% CI が 0 を含めば非有意 (Wald 近似 p ≥ 0.05)
                p_value=0.04 if (
                    (r["ate"] - 1.96 * r["ate_se"]) > 0
                    or (r["ate"] + 1.96 * r["ate_se"]) < 0
                ) else 0.20,
                n=r["n_obs"],
            )
            for r in rows
        ]
        spec = CIScatterSpec(
            points=ci_points,
            x_label="ATE (95% CI, Wald)",
            title="DML ATE推定値と95%信頼区間",
            reference=0.0,
            reference_label="ATE",
            sort_by="input",
        )
        fig = render_ci_scatter(spec, theme="dark")

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="DML処置効果推定値",
            findings_html=findings,
            visualization_html=viz_embed(fig, "chart_dml_overview"),
            method_note=(
                "DML推定値はfeat_causal_estimates（Phase 9因果モジュール）由来。"
                "ATE = 交差適合法で推定された平均処置効果。"
                "ate_se = 交差適合の分散推定値による標準誤差。"
                "95% CI = ATE ± 1.96 × SE（ガウス近似、ブートストラップではない）。"
                "DMLに必要な手続き: (1) 処置モデルM(X)とアウトカムモデルQ(X)を"
                "ホールドアウトfoldで学習、(2) 残差を部分アウト、(3) 残差にOLS。"
            ),
            interpretation_html=(
                "<p>DMLのATEは非交絡性仮定（観測共変量で条件付けると処置が潜在アウトカムと独立）"
                "のもとで推定された母集団平均因果効果である。"
                "この仮定は観測データから検証不可能である。"
                "代替感度分析（Rosenbaum bounds）は限定的な未観測交絡のもとで"
                "推定値の範囲を与える。"
                "推定値は「因果効果」ではなく「観測交絡因子を制御した後の関連」として"
                "解釈するべきである。</p>"
            ),
            section_id="dml_overview",
        )

    # ── Section 2: Treatment effects by tier ─────────────────────

    def _build_tier_effect_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT treatment, outcome, scale_tier AS tier, ate, ate_se, n_obs
                FROM feat_causal_estimates
                WHERE scale_tier IS NOT NULL AND ate IS NOT NULL
                ORDER BY treatment, scale_tier
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="Tier別処置効果",
                findings_html="<p>Tier別因果推定値が利用できません。</p>",
                section_id="dml_tier",
            )

        findings = "<p>作品規模Tier別に層別化したDML ATE推定値:</p><ul>"
        for r in rows:
            ci_lo = r["ate"] - 1.96 * r["ate_se"]
            ci_hi = r["ate"] + 1.96 * r["ate_se"]
            findings += (
                f"<li><strong>Tier {r['tier']}: {r['treatment']} → {r['outcome']}</strong> "
                f"(n={r['n_obs']:,}): "
                f"ATE={r['ate']:.4f}, 95% CI [{ci_lo:.4f}, {ci_hi:.4f}]</li>"
            )
        findings += "</ul>"

        tiers = sorted({r["tier"] for r in rows})
        combos = sorted({(r["treatment"], r["outcome"]) for r in rows})

        fig = go.Figure()
        for t_tup in combos:
            tier_ates = {r["tier"]: (r["ate"], r["ate_se"]) for r in rows
                        if (r["treatment"], r["outcome"]) == t_tup}
            label = f"{t_tup[0]}→{t_tup[1]}"
            fig.add_trace(go.Scatter(
                x=[f"T{t}" for t in tiers],
                y=[tier_ates.get(t, (None, None))[0] for t in tiers],
                error_y=dict(
                    type="data",
                    array=[1.96 * tier_ates.get(t, (0, 0))[1] for t in tiers],
                ),
                mode="markers+lines",
                name=label,
            ))
        fig.add_hline(y=0, line_dash="dash", line_color="#a0a0a0")
        fig.update_layout(
            title="スケールTier別 ATE",
            xaxis_title="スケールTier", yaxis_title="ATE",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="Tier別処置効果",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_dml_tier", height=420),
            method_note=(
                "Tier層別化DML: scale_tierの各層で個別にモデルを学習。"
                "Tierあたりのnが小さいとSEが大きくなり、信頼区間が広がる。"
                "scale_tierはfeat_causal_estimates由来。"
            ),
            section_id="dml_tier",
        )

    # ── Section 3: Sensitivity analysis ──────────────────────────

    def _build_sensitivity_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT treatment, outcome, sensitivity_param, ate_sensitivity, n_obs
                FROM feat_causal_estimates
                WHERE sensitivity_param IS NOT NULL
                ORDER BY treatment, sensitivity_param
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="感度分析",
                findings_html=(
                    "<p>感度分析データが利用できません（feat_causal_estimates.sensitivity_param）。"
                    "感度分析はnuisanceモデル仕様の変更（正則化手法の変更、"
                    "傾向スコア推定のML手法変更等）がATE推定にどう影響するかを検証します。</p>"
                ),
                section_id="dml_sensitivity",
            )

        findings = "<p>nuisanceモデル仕様に対するDML ATEの感度:</p><ul>"
        for r in rows:
            findings += (
                f"<li><strong>{r['treatment']} → {r['outcome']}</strong>, "
                f"param={r['sensitivity_param']}: "
                f"ATE={r['ate_sensitivity']:.4f}</li>"
            )
        findings += "</ul>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="感度分析",
            findings_html=findings,
            method_note=(
                "sensitivity_param: nuisanceモデルの代替パラメータ値。"
                "代替仕様を通してATEが安定していれば推定への信頼が高まる。"
                "大きなばらつきはモデル選択への高い感度を示す。"
            ),
            section_id="dml_sensitivity",
        )

    # ── Section 4: Refutation tests ───────────────────────────────

    def _build_refutation_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT treatment, outcome, refutation_type, refutation_ate, refutation_p
                FROM feat_causal_estimates
                WHERE refutation_type IS NOT NULL
                ORDER BY treatment, refutation_type
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="反証テスト",
                findings_html=(
                    "<p>反証テストデータが利用できません。"
                    "推奨反証テスト: "
                    "(1) プラセボ処置: 処置をランダムノイズに置換→ATEが有意でないことを確認。"
                    "(2) データサブセット: 80%サンプリング→ATEが全体推定のCI内に収まることを確認。"
                    "(3) 未観測交絡因子: ランダム交絡因子を追加→ATEが安定していることを確認。</p>"
                ),
                section_id="dml_refutation",
            )

        findings = "<p>反証テスト結果:</p><ul>"
        for r in rows:
            p_str = f"p={r['refutation_p']:.3f}" if r["refutation_p"] is not None else "p=N/A"
            findings += (
                f"<li><strong>{r['treatment']} → {r['outcome']}</strong> "
                f"({r['refutation_type']}): "
                f"ATE={r['refutation_ate']:.4f}, {p_str}</li>"
            )
        findings += "</ul>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="反証テスト",
            findings_html=findings,
            method_note=(
                "反証テストはfeat_causal_estimates由来。"
                "プラセボ反証: 処置をランダムな並べ替えに置換 — "
                "ATEはゼロに近いことが期待される。"
                "ブートストラップサブセット: 80%サブサンプル — "
                "ATEが全体推定の2SE以内に収まることを確認。"
            ),
            section_id="dml_refutation",
        )
