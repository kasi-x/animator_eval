"""Policy Attrition report — v2 compliant.

新卒離職の因果分解:
- Section 1: デビューコホート別生存曲線 (KM)
- Section 2: 処置効果推定 (Cox HR + DML ATE)
- Section 3: exit定義別感度分析

v3 visualization: Section 2 forest plot を src.viz.primitives.CIScatter
経由で描画 (palette / CI / null reference 統一)。
"""

from __future__ import annotations

import json
from pathlib import Path

import plotly.graph_objects as go  # noqa: F401  (kept for backward-compat shims)

from src.viz import embed as viz_embed
from src.viz.primitives import (
    CIPoint,
    CIScatterSpec,
    KMCurveSpec,
    KMStratum,
    render_ci_scatter,
    render_km_curve,
)

from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe  # noqa: F401  (legacy shim still used elsewhere)
from ..section_builder import KPICard, ReportSection, SectionBuilder
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


# Decade colour palette — v3: Okabe-Ito CB-safe + cohort_decade fixed mapping
from src.viz.palettes import OKABE_ITO_DARK  # noqa: E402

_COHORT_COLORS = list(OKABE_ITO_DARK)


# v3 ReportSpec — see docs/REPORT_DESIGN_v3.md §7 (example A)
from .._spec import (  # noqa: E402
    CIMethod, DataLineage, HoldoutSpec, InterpretationGuard,
    MethodGate, ReportSpec, SensitivityAxis,
)

SPEC = ReportSpec(
    name="policy_attrition",
    audience="policy",
    claim=(
        "デビューコホート別の翌年クレジット可視性喪失率が "
        "5 年窓で単調に減少する"
    ),
    identifying_assumption=(
        "クレジット可視性喪失 = 雇用離脱 を仮定しない。"
        "クレジット可視性のみを観察対象とする。"
    ),
    null_model=["N3", "N5"],
    method_gate=MethodGate(
        name="Cox PH + Kaplan-Meier",
        estimator="cox_ph + km (Breslow baseline)",
        ci=CIMethod(estimator="greenwood", parametric_assumption="proportional hazards"),
        rng_seed=42,
        null=["N3", "N5"],
        holdout=HoldoutSpec(
            method="leave-one-year-out",
            holdout_size="last 3 years (2022-2024)",
            metric="C-index",
            naive_baseline="role-cohort marginal hazard",
        ),
        shrinkage=None,
        sensitivity_grid=[
            SensitivityAxis(name="exit definition window",
                            values=["1y", "3y", "5y"]),
            SensitivityAxis(name="cohort cut", values=["5y", "10y"]),
        ],
        limitations=[
            "右打切り (観測末年)",
            "可視性喪失は海外下請け / 無名義参加 / 産休 / 療養 を吸収",
            "クレジット粒度の時代差 (1980s vs 2010s) が hazard 推定に bias",
        ],
    ),
    sensitivity_grid=[
        SensitivityAxis(name="exit definition window",
                        values=["1y", "3y", "5y"]),
        SensitivityAxis(name="cohort cut", values=["5y", "10y"]),
    ],
    interpretation_guard=InterpretationGuard(
        forbidden_framing=["離職率の悪化", "若手定着の課題", "業界の危機"],
        required_alternatives=2,
    ),
    data_lineage=DataLineage(
        sources=["credits", "persons", "anime"],
        meta_table="meta_policy_attrition",
        snapshot_date="2026-04-30",
        pipeline_version="v55",
    ),
)


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

        # v3: KMCurve primitive 経由で描画 (Greenwood band + risk table + median marker)
        strata: list[KMStratum] = []
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
            n_at_risk = decade_data.get("n_at_risk")

            if not timeline or not survival:
                continue

            strata.append(
                KMStratum(
                    label=f"{decade_label}年代デビュー",
                    timeline=timeline,
                    survival=survival,
                    ci_lo=ci_lower if (ci_lower and len(ci_lower) == len(timeline)) else None,
                    ci_hi=ci_upper if (ci_upper and len(ci_upper) == len(timeline)) else None,
                    n_at_risk=n_at_risk,
                    median_survival=median_survival,
                    n=n,
                    color=_COHORT_COLORS[idx % len(_COHORT_COLORS)],
                )
            )

            n_str = f"n={n:,}" if n is not None else "n=不明"
            med_str = (
                f"中央生存時間={median_survival:.1f}年" if median_survival is not None
                else "中央生存時間=算出不能（打切）"
            )
            findings_parts.append(
                f"<li><strong>{decade_label}年代デビュー</strong>: {n_str}, {med_str}</li>"
            )

        findings_html = (
            f"<p>デビューコホート別のカプラン–マイヤー生存曲線"
            f"（横軸: デビューからの経過年数, 縦軸: アクティブ継続率）:</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            if findings_parts
            else "<p>有効なコホートデータが見つかりませんでした。</p>"
        )

        if strata:
            km_spec = KMCurveSpec(
                strata=strata,
                title="デビューコホート別 KM生存曲線（Greenwood 95% CI）",
                x_label="デビューからの経過年数",
                y_label="生存率 S(t)",
                risk_table=True,
                median_marker=True,
            )
            viz_html = viz_embed(render_km_curve(km_spec, theme="dark"), "chart_km_curves")
        else:
            viz_html = '<p style="color:#8a94a0">KM 描画用データが利用できません。</p>'

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        # v3: KPI strip (要点先出し)
        n_cohorts = len(strata)
        total_n = sum(s.n for s in strata if s.n is not None) if strata else 0
        medians = [s.median_survival for s in strata if s.median_survival is not None]
        kpis = [
            KPICard("コホート数", f"{n_cohorts}", "デビュー年代区分"),
            KPICard("対象人物数", f"{total_n:,}" if total_n else "n/a",
                    "全コホート合計"),
        ]
        if medians:
            kpis.append(KPICard(
                "中央生存時間", f"{min(medians):.1f}–{max(medians):.1f}年",
                "コホート間レンジ",
            ))

        return ReportSection(
            title="デビューコホート別生存曲線",
            findings_html=findings_html,
            visualization_html=viz_html,
            kpi_cards=kpis,
            chart_caption=(
                "横軸 = デビューからの経過年数、縦軸 = 翌年クレジットが観測される割合 "
                "S(t)。線色はデビューコホート、薄い帯は Greenwood 95% 信頼区間。"
                "下の at-risk 表が各時点でまだ観測されていない (打切り前) 人物数。"
                "中央値線 (点線) は S(t)=0.5 への到達時間。"
            ),
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

        # Forest plot — v3: CIScatter primitive 経由で描画
        if covariates:
            ci_points = [
                CIPoint(
                    label=cov,
                    x=hr_vals[i],
                    ci_lo=hr_lo[i],
                    ci_hi=hr_hi[i],
                    p_value=p_vals[i],
                )
                for i, cov in enumerate(covariates)
            ]
            spec = CIScatterSpec(
                points=ci_points,
                x_label="ハザード比 (HR, log scale)",
                title="CoxPH ハザード比 フォレストプロット",
                log_x=True,
                reference=1.0,
                reference_label="HR",
                sort_by="input",
                significance_threshold=0.05,
            )
            fig = render_ci_scatter(spec, theme="dark")
            viz_html = viz_embed(fig, "chart_cox_forest")
        else:
            viz_html = '<p style="color:#8a94a0">CoxPHフォレストプロットのデータが利用できません。</p>'

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        # v3: KPI strip
        n_sig = sum(
            1 for i, p in enumerate(p_vals)
            if p is not None and p < 0.05
        )
        kpis = [
            KPICard("共変量数", f"{len(covariates)}", "Cox PH 入力変数"),
            KPICard("有意 (p<0.05)", f"{n_sig} / {len(covariates)}",
                    "Wald 検定 (95% CI が HR=1 を跨がない)"),
        ]
        if dml_theta is not None:
            kpis.append(KPICard("DML ATE θ", f"{dml_theta:.4f}",
                                "GBM 5-fold cross-fit"))

        return ReportSection(
            title="処置効果推定（Cox HR + DML ATE）",
            findings_html=findings_html,
            visualization_html=viz_html,
            kpi_cards=kpis,
            chart_caption=(
                "横軸 = ハザード比 HR (対数スケール)、縦軸 = 共変量。"
                "塗り潰し四角 = 有意 (p<0.05)、中抜き = 非有意。"
                "誤差棒 = Wald 95% 信頼区間。点線 (HR=1) は無効果線。"
                "HR > 1 でイベント (クレジット可視性喪失) のハザードが増加する観察上の関連。"
            ),
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

        # v3: bar chart は palette の Okabe-Ito から取り、apply_theme で
        # dark theme を統一適用 (sensitivity は単発バーなので primitive 化せず
        # raw plotly のまま、theme/palette のみ統一)。
        from src.viz.palettes import OKABE_ITO_DARK
        from src.viz.theme import apply_theme

        bar_palette = OKABE_ITO_DARK[2:5]  # blue / green / yellow (CB-safe triplet)
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=labels,
            y=thetas,
            marker_color=bar_palette[:len(labels)],
            hovertemplate="%{x}: θ=%{y:.4f}<extra></extra>",
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="#a0a0a0",
                      annotation_text="θ = 0", annotation_position="top right")
        fig.update_layout(
            title="exit定義別 DML ATE（感度分析）",
            xaxis_title="exit閾値定義",
            yaxis_title="DML ATE (θ)",
        )
        apply_theme(fig, theme="dark", height=380)

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        # v3: KPI strip
        theta_range = (
            f"{min(thetas):.3f} – {max(thetas):.3f}" if thetas else "n/a"
        )
        kpis = [
            KPICard("感度軸", f"{len(labels)} 条件", "exit 閾値 3/5/7 年"),
            KPICard("θ レンジ", theta_range, "全感度条件のばらつき"),
        ]

        return ReportSection(
            title="exit定義別感度分析",
            findings_html=findings_html,
            visualization_html=viz_embed(fig, "chart_sensitivity", height=380),
            kpi_cards=kpis,
            chart_caption=(
                "横軸 = exit 閾値 (3/5/7 年)、縦軸 = DML ATE 推定値 θ。"
                "棒の高さ揃いが小さいほど exit 定義に対する推定の頑健性が高い。"
                "点線 (θ=0) は無効果線。"
            ),
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
