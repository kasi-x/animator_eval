"""Policy Attrition report — v2 compliant.

新卒離職の因果分解:
- Section 1: デビューコホート別生存曲線 (KM)
- Section 2: 処置効果推定 (Cox HR + DML ATE)
- Section 3: exit定義別感度分析
"""

from __future__ import annotations

import json
from pathlib import Path

import plotly.graph_objects as go

from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_JSON_DIR = Path(__file__).parents[4] / "result" / "json"


def _load(name: str) -> dict | list:
    p = _JSON_DIR / f"{name}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


# Decade colour palette
_COHORT_COLORS = [
    "#667eea", "#f093fb", "#06D6A0", "#FFD166",
    "#f5576c", "#a0d2db", "#fda085", "#43e97b",
]


class PolicyAttritionReport(BaseReportGenerator):
    name = "policy_attrition"
    title = "新卒離職の因果分解"
    subtitle = "デビューコホート別生存分析 + DML因果推定"
    filename = "policy_attrition.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        data = _load("entry_cohort_attrition")

        sections: list[str] = [
            sb.build_section(self._build_km_curves(sb, data)),
            sb.build_section(self._build_treatment_effects(sb, data)),
            sb.build_section(self._build_sensitivity(sb, data)),
        ]
        insert_lineage(
            self.conn,
            table_name="meta_policy_attrition",
            audience="policy",
            source_silver_tables=["credits", "persons", "anime"],
            formula_version="v1.0",
            ci_method=(
                "Greenwood formula for KM survival curves (95% CI); "
                "analytical SE (sigma/sqrt(n)) for DML ATE"
            ),
            null_model=(
                "Random credit reassignment within role cohort (100 draws, seed=42); "
                "compares observed KM median to permuted distribution"
            ),
            holdout_method="Leave-one-year-out (last 3 years, 2022-2024)",
            description=(
                "Causal decomposition of new-entrant attrition by debut cohort. "
                "KM estimator: event = gap_type=='exit' and returned==0 (right-censored). "
                "DML: partial linear model with GBM nuisance, K=5 cross-fit. "
                "Attrition = credit visibility loss rate, not career exit."
            ),
            rng_seed=42,
        )
        return self.write_report("\n".join(sections))

    # ── Section 1: KM survival curves ────────────────────────────

    def _build_km_curves(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        km_by_cohort: dict = data.get("km_by_cohort", {}) if isinstance(data, dict) else {}

        if not km_by_cohort:
            return ReportSection(
                title="デビューコホート別生存曲線",
                findings_html=(
                    "<p>生存曲線データが利用できません（entry_cohort_attrition.km_by_cohort）。"
                    "DML離職分析モジュールの実行が必要です。"
                    "KM推定量はデビューコホートごとにgap_type==exitかつreturned==0を"
                    "イベントとして推定します。</p>"
                ),
                section_id="km_curves",
            )

        fig = go.Figure()
        findings_parts: list[str] = []

        for idx, (decade_label, decade_data) in enumerate(sorted(km_by_cohort.items())):
            if not isinstance(decade_data, dict):
                continue
            timeline = decade_data.get("timeline", [])
            survival = decade_data.get("survival", [])
            ci_lower = decade_data.get("ci_lower", [])
            ci_upper = decade_data.get("ci_upper", [])
            n = decade_data.get("n")
            median_survival = decade_data.get("median_survival")

            if not timeline or not survival:
                continue

            color = _COHORT_COLORS[idx % len(_COHORT_COLORS)]

            # CI shading
            if ci_upper and ci_lower and len(ci_upper) == len(timeline):
                x_fill = list(timeline) + list(reversed(timeline))
                y_fill = list(ci_upper) + list(reversed(ci_lower))
                fig.add_trace(go.Scatter(
                    x=x_fill, y=y_fill,
                    fill="toself",
                    fillcolor=f"rgba({_hex_to_rgb(color)},0.12)",
                    line=dict(width=0),
                    showlegend=False,
                    hoverinfo="skip",
                ))

            fig.add_trace(go.Scatter(
                x=timeline, y=survival,
                mode="lines",
                name=f"{decade_label}年代デビュー",
                line=dict(color=color, width=2),
                hovertemplate=f"{decade_label}: t=%{{x}}年, S(t)=%{{y:.3f}}<extra></extra>",
            ))

            # Findings text per decade
            n_str = f"n={n:,}" if n is not None else "n=不明"
            med_str = (
                f"中央生存時間={median_survival:.1f}年" if median_survival is not None
                else "中央生存時間=算出不能（打切）"
            )
            findings_parts.append(f"<li><strong>{decade_label}年代デビュー</strong>: {n_str}, {med_str}</li>")

        findings_html = (
            f"<p>デビューコホート別のカプラン–マイヤー生存曲線"
            f"（横軸: デビューからの経過年数, 縦軸: アクティブ継続率）:</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            if findings_parts
            else "<p>有効なコホートデータが見つかりませんでした。</p>"
        )

        fig.update_layout(
            title="デビューコホート別 KM生存曲線（95% CI）",
            xaxis_title="デビューからの経過年数",
            yaxis_title="生存率 S(t)",
            yaxis=dict(range=[0, 1.05]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="デビューコホート別生存曲線",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_km_curves", height=480),
            method_note=(
                "KM推定量（Kaplan–Meier）。"
                "イベント定義: gap_type == 'exit' かつ returned == 0。"
                "打切り: 観測期間末尾まで記録のある人物（右打切り）。"
                "Greenwood公式による95% CI。"
                "コホート区分はデビュー年の区切り（entry_cohort_attrition.km_by_cohort のキー）。"
            ),
            section_id="km_curves",
        )

    # ── Section 2: Treatment effects (Cox + DML) ─────────────────

    def _build_treatment_effects(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        cox_ph: dict = data.get("cox_ph", {}) if isinstance(data, dict) else {}
        dml_attrition: dict = data.get("dml_attrition", {}) if isinstance(data, dict) else {}

        if not cox_ph and not dml_attrition:
            return ReportSection(
                title="処置効果推定（Cox HR + DML ATE）",
                findings_html=(
                    "<p>処置効果推定データが利用できません"
                    "（entry_cohort_attrition.cox_ph / dml_attrition）。"
                    "CoxPHモデルはBreslow基底ハザード推定を使用し、"
                    "DMLは部分線形モデルをGBM交差適合（K=5）で推定します。</p>"
                ),
                section_id="treatment_effects",
            )

        # Forest plot data from Cox HR
        covariates: list[str] = []
        hr_vals: list[float] = []
        hr_lo: list[float] = []
        hr_hi: list[float] = []
        p_vals: list[float | None] = []

        for cov, stats in sorted(cox_ph.items()):
            if not isinstance(stats, dict):
                continue
            hr = stats.get("hr")
            ci_lo = stats.get("ci_lower")
            ci_hi = stats.get("ci_upper")
            pv = stats.get("p_value")
            if hr is None:
                continue
            covariates.append(cov)
            hr_vals.append(hr)
            hr_lo.append(ci_lo if ci_lo is not None else hr)
            hr_hi.append(ci_hi if ci_hi is not None else hr)
            p_vals.append(pv)

        # Findings: Cox
        cox_findings_items: list[str] = []
        for i, cov in enumerate(covariates):
            p_str = f"p={p_vals[i]:.3f}" if p_vals[i] is not None else "p=N/A"
            cox_findings_items.append(
                f"<li><strong>{cov}</strong>: HR={hr_vals[i]:.3f}, "
                f"95% CI [{hr_lo[i]:.3f}, {hr_hi[i]:.3f}], {p_str}</li>"
            )

        # Findings: DML
        dml_theta = dml_attrition.get("theta")
        dml_se = dml_attrition.get("se")
        dml_ci_lo = dml_attrition.get("ci_lower")
        dml_ci_hi = dml_attrition.get("ci_upper")
        dml_n = dml_attrition.get("n")
        dml_method = dml_attrition.get("method_note", "DML部分線形モデル")

        dml_text = ""
        if dml_theta is not None:
            ci_lo_str = f"{dml_ci_lo:.4f}" if dml_ci_lo is not None else "N/A"
            ci_hi_str = f"{dml_ci_hi:.4f}" if dml_ci_hi is not None else "N/A"
            se_str = f"SE={dml_se:.4f}" if dml_se is not None else ""
            n_str = f"n={dml_n:,}" if dml_n is not None else ""
            dml_text = (
                f"<p>DML ATE推定値: θ={dml_theta:.4f} {se_str}, "
                f"95% CI [{ci_lo_str}, {ci_hi_str}]"
                f"{', ' + n_str if n_str else ''}. "
                f"手法: {dml_method}</p>"
            )

        findings_html = (
            "<p>Cox比例ハザードモデルのハザード比（HR）と95% CI:</p>"
            f"<ul>{''.join(cox_findings_items)}</ul>"
            if cox_findings_items else ""
        ) + dml_text

        if not findings_html:
            findings_html = "<p>有効な処置効果推定値が見つかりませんでした。</p>"

        # Forest plot
        if covariates:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=hr_vals,
                y=covariates,
                mode="markers",
                marker=dict(color="#f093fb", size=10, symbol="square"),
                error_x=dict(
                    type="data",
                    symmetric=False,
                    array=[h - v for h, v in zip(hr_hi, hr_vals)],
                    arrayminus=[v - lo for v, lo in zip(hr_vals, hr_lo)],
                    color="rgba(255,255,255,0.5)",
                    thickness=2,
                    width=6,
                ),
                hovertemplate="<b>%{y}</b><br>HR=%{x:.3f}<extra></extra>",
            ))
            fig.add_vline(x=1.0, line_dash="dash", line_color="#a0a0a0",
                          annotation_text="HR=1 (無効果)", annotation_position="top right")
            fig.update_layout(
                title="CoxPH ハザード比 フォレストプロット",
                xaxis_title="ハザード比 (HR)",
                yaxis_title="",
                xaxis_type="log",
                margin=dict(l=180),
            )
            fig.update_yaxes(autorange="reversed")
            viz_html = plotly_div_safe(fig, "chart_cox_forest", height=max(360, len(covariates) * 32 + 100))
        else:
            viz_html = '<p style="color:#8a94a0">CoxPHフォレストプロットのデータが利用できません。</p>'

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="処置効果推定（Cox HR + DML ATE）",
            findings_html=findings_html,
            visualization_html=viz_html,
            method_note=(
                "CoxPHモデル: Breslow基底ハザード推定、比例ハザード仮定のもとで推定。"
                "HR > 1はイベント（離職）リスクの増加を示す。"
                "DML部分線形モデル: nuisance関数をGradient Boosted Machine (GBM) で推定、"
                "K=5折クロス適合で交絡因子を除去した後にOLS残差回帰。"
                "95% CI = 推定値 ± 1.96 × SE（漸近正規近似）。"
            ),
            section_id="treatment_effects",
        )

    # ── Section 3: Sensitivity analysis ──────────────────────────

    def _build_sensitivity(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        sensitivity: dict = data.get("sensitivity", {}) if isinstance(data, dict) else {}

        if not sensitivity:
            return ReportSection(
                title="exit定義別感度分析",
                findings_html=(
                    "<p>感度分析データが利用できません（entry_cohort_attrition.sensitivity）。"
                    "exit閾値を3/5/7年で変えてDML ATEの安定性を確認します。"
                    "閾値ごとにイベント数 n_events と5年生存率 km_5y も報告されます。</p>"
                ),
                section_id="sensitivity",
            )

        labels: list[str] = []
        thetas: list[float] = []
        n_events_list: list[int] = []
        km_5y_list: list[float] = []

        _display_map = {
            "exit_3y": "exit閾値3年",
            "exit_5y": "exit閾値5年",
            "exit_7y": "exit閾値7年",
        }

        for key in ("exit_3y", "exit_5y", "exit_7y"):
            entry = sensitivity.get(key)
            if not isinstance(entry, dict):
                continue
            theta = entry.get("dml_theta")
            if theta is None:
                continue
            labels.append(_display_map.get(key, key))
            thetas.append(theta)
            n_events_list.append(entry.get("n_events", 0))
            km_5y_list.append(entry.get("km_5y", 0.0))

        if not labels:
            return ReportSection(
                title="exit定義別感度分析",
                findings_html=(
                    "<p>有効な感度分析エントリが見つかりませんでした"
                    "（entry_cohort_attrition.sensitivity の各キーに dml_theta が必要）。</p>"
                ),
                section_id="sensitivity",
            )

        findings_items = []
        for i, label in enumerate(labels):
            km_str = f"{km_5y_list[i]:.3f}" if km_5y_list[i] else "N/A"
            findings_items.append(
                f"<li><strong>{label}</strong>: "
                f"DML θ={thetas[i]:.4f}, "
                f"イベント数={n_events_list[i]:,}, "
                f"KM 5年生存率={km_str}</li>"
            )

        findings_html = (
            "<p>exitの定義閾値を3年・5年・7年に変えた場合のDML ATE推定値:</p>"
            f"<ul>{''.join(findings_items)}</ul>"
        )

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=labels,
            y=thetas,
            marker_color=["#667eea", "#f093fb", "#06D6A0"][:len(labels)],
            hovertemplate="%{x}: θ=%{y:.4f}<extra></extra>",
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="#a0a0a0")
        fig.update_layout(
            title="exit定義別 DML ATE（感度分析）",
            xaxis_title="exit閾値定義",
            yaxis_title="DML ATE (θ)",
        )

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="exit定義別感度分析",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_sensitivity", height=380),
            method_note=(
                "感度分析: exitの定義閾値を3年・5年・7年に変えて、"
                "DML ATE推定値の安定性を検証。"
                "各閾値でKM推定とDML部分線形モデルを独立に推定。"
                "大きなばらつきはexit定義への高い感度を示す。"
                "km_5y = KM推定による5年時点の生存率（累積非exit率）。"
            ),
            section_id="sensitivity",
        )


def _hex_to_rgb(hex_color: str) -> str:
    """Convert #RRGGBB to 'R,G,B' string for rgba() use."""
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    r = int(h[0:2], 16)
    g = int(h[2:4], 16)
    b = int(h[4:6], 16)
    return f"{r},{g},{b}"
