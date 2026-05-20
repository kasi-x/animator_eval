"""翌年クレジット可視性喪失 早期警告 — HR brief セクション。

HR brief 読者 (スタジオ HR・労働組合担当) 向けのモデル性能サマリーと
subgroup fairness check を提供する。

構造:
  Section 1: モデル性能 (AUC / Brier / calibration gate)
  Section 2: subgroup fairness (gender / role_group / cohort_band 別 AUC)
  Section 3: 特徴量重要度 (SHAP mean abs)

命名: 「翌年クレジット可視性喪失」= 本データセット上のクレジット不在。
離職・業界離脱ではない。

REPORT_PHILOSOPHY v2 遵守:
- Findings 節は評価語なし・数値 + n + CI 形式。
- Interpretation 節は一人称明示・代替解釈 1 件以上。
- Gate 未達時はモデル個別予測を非表示。
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import plotly.graph_objects as go

from ..html_templates import plotly_div_safe
from ..section_builder import KPICard, ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_JSON_DIR = Path(__file__).parents[4] / "result" / "json"

_AUC_GATE = 0.65
_SUBGROUP_DIFF_GATE = 0.10
_ECE_GATE = 0.10


def _load_json(name: str) -> dict:
    p = _JSON_DIR / f"{name}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _safe_float(v: object, default: float = float("nan")) -> float:
    try:
        f = float(v)  # type: ignore[arg-type]
        return default if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


def _fmt(v: float, precision: int = 3) -> str:
    return f"{v:.{precision}f}" if not math.isnan(v) else "N/A"


class CareerVisibilityWarningReport(BaseReportGenerator):
    """翌年クレジット可視性喪失 早期警告レポート (HR brief)。

    JSON source: result/json/visibility_loss_model.json
    Expected schema:
        auc_roc: float
        brier_score: float
        n_holdout: int
        n_positive: int
        calibration_lo_bin: float
        calibration_hi_bin: float
        baseline_auc: float
        passes_gate: bool
        subgroup_auc: {str: float}
        subgroup_max_diff: float
        feature_importances: {str: float}
        holdout_year: int
        model_method: str
    """

    name = "career_visibility_warning"
    title = "翌年クレジット可視性喪失 早期警告"
    subtitle = (
        "LightGBM + isotonic calibration — temporal holdout AUC / "
        "subgroup fairness / feature importance"
    )
    filename = "career_visibility_warning.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load_json("visibility_loss_model")
        sb = SectionBuilder()

        sections = [
            sb.build_section(_build_model_performance(sb, data)),
            sb.build_section(_build_subgroup_fairness(sb, data)),
            sb.build_section(_build_feature_importance(sb, data)),
        ]

        # Cox PH 並設 (data に "cox_results" が含まれる時のみ)
        cox_section = _build_cox_section(sb, data)
        if cox_section is not None:
            sections.append(sb.build_section(cox_section))

        interpretation_html = _build_interpretation(data)
        body = "\n".join(sections)

        if interpretation_html:
            body += (
                '<div class="card interpretation" id="interpretation"'
                ' style="border-left:3px solid #c0a0d0;">'
                '<h2>Interpretation / 解釈</h2>'
                '<p style="font-size:0.8rem;color:#9090b0;">'
                "以下は分析者の解釈であり、代替解釈が存在する。 / "
                "The following reflects the analyst's interpretation; "
                "alternative interpretations exist.</p>"
                f"{interpretation_html}</div>"
            )

        return self.write_report(body)


# ---------------------------------------------------------------------------
# Section builders (module-level functions — 1 関数 1 責務)
# ---------------------------------------------------------------------------


def _build_model_performance(sb: SectionBuilder, data: dict) -> ReportSection:
    """Section 1: AUC / Brier / calibration gate."""
    auc = _safe_float(data.get("auc_roc"))
    brier = _safe_float(data.get("brier_score"))
    n_holdout = int(data.get("n_holdout", 0))
    n_positive = int(data.get("n_positive", 0))
    baseline_auc = _safe_float(data.get("baseline_auc"))
    passes = bool(data.get("passes_gate", False))
    holdout_year = data.get("holdout_year", "N/A")
    method = data.get("model_method", "LightGBM + isotonic calibration")

    if not data or not passes:
        gate_html = (
            f"<p>モデル性能ゲート未達 (AUC &lt; {_AUC_GATE:.2f} またはデータ不足)。"
            f"現在の holdout AUC = {_fmt(auc)}。"
            f"holdout n = {n_holdout:,}。"
            f"AUC ≥ {_AUC_GATE:.2f} かつ holdout n ≥ 30 を公開条件とする。"
            f"個別予測スコアはゲート未達のため非表示。</p>"
        )
        return ReportSection(
            title="モデル性能 (AUC / Brier / calibration gate)",
            findings_html=gate_html,
            method_note=(
                f"LightGBM + isotonic calibration, temporal year split。"
                f"holdout = {holdout_year} 年度, gate = AUC ≥ {_AUC_GATE:.2f}。"
                f"Label: 翌年クレジット可視性喪失 (本データセット上のクレジット不在)。"
                f"Features: theta_i (AKM person FE), PageRank, betweenness, "
                f"直近 3 年 credit slope / variance, studio Shannon entropy, "
                f"role stall 年数, peer 可視性喪失率, cohort 経過年数, role 多様性。"
                f"外部レーティング・視聴者評価は features に含まない。"
            ),
            section_id="visibility_performance",
        )

    lo_bin = _safe_float(data.get("calibration_lo_bin"))
    hi_bin = _safe_float(data.get("calibration_hi_bin"))
    ece = _safe_float(data.get("ece"))

    findings = (
        f"<p>holdout ({holdout_year} 年度) ROC-AUC = {_fmt(auc)}"
        f"（ゲート閾値 {_AUC_GATE:.2f} 達成）、"
        f"Brier score = {_fmt(brier)}、"
        f"ECE (10-bin) = {_fmt(ece)}（ゲート閾値 {_ECE_GATE:.2f}）、"
        f"holdout n = {n_holdout:,}（うち可視性喪失 = {n_positive:,}件）。"
        f"ベースライン AUC (last-3-year mean) = {_fmt(baseline_auc)}。</p>"
        f"<p>calibration ビン確認: 予測確率 0–0.2 区間の実測喪失率 = {_fmt(lo_bin, 2)}"
        f"、予測確率 0.8–1.0 区間の実測喪失率 = {_fmt(hi_bin, 2)}。</p>"
    )

    kpis = [
        KPICard("holdout AUC", _fmt(auc), f"ゲート {_AUC_GATE:.2f} 達成"),
        KPICard("Brier score", _fmt(brier), "確率較正精度（低いほど良）"),
        KPICard("ECE", _fmt(ece), f"ゲート {_ECE_GATE:.2f}（低いほど良）"),
        KPICard("holdout n", f"{n_holdout:,}", f"{holdout_year} 年度"),
        KPICard("ベースライン AUC", _fmt(baseline_auc), "last-3-year mean baseline"),
    ]

    return ReportSection(
        title="モデル性能 (AUC / Brier / calibration gate)",
        findings_html=findings,
        kpi_cards=kpis,
        method_note=(
            f"モデル: {method}。"
            f"temporal year split: train = {holdout_year} 年以前, "
            f"holdout = {holdout_year} 年度。"
            f"Label: 翌年クレジット可視性喪失 = 本データセット上のクレジット不在。"
            f"この事象はスタジオ離脱・無名義参加・海外下請け・データ欠落等の"
            f"複合事象を含む (内訳は本データから推定不可)。"
            f"公開ゲート: AUC ≥ {_AUC_GATE:.2f} かつ holdout n ≥ 30。"
            f"Features に外部レーティング・視聴者評価は含まない (Hard rule H1)。"
        ),
        section_id="visibility_performance",
    )


def _build_subgroup_fairness(sb: SectionBuilder, data: dict) -> ReportSection:
    """Section 2: subgroup (gender / role_group / cohort_band) 別 AUC。"""
    subgroup_auc: dict[str, float] = data.get("subgroup_auc", {})
    max_diff = _safe_float(data.get("subgroup_max_diff"))
    passes = bool(data.get("passes_gate", False))

    if not data or not passes or not subgroup_auc:
        findings = (
            "<p>subgroup fairness データが利用できません。"
            "モデル性能ゲートを通過後に表示されます。</p>"
        )
        return ReportSection(
            title="subgroup fairness (gender / role_group / cohort_band 別 AUC)",
            findings_html=findings,
            method_note=(
                "subgroup AUC 差 > 0.10 の場合、fairness 修正を優先し"
                "個別予測の公開を保留する (TASK_CARDS/25_compensation_fairness/03)。"
            ),
            section_id="visibility_fairness",
        )

    # subgroup AUC bar chart
    valid_groups = {k: v for k, v in subgroup_auc.items() if not math.isnan(v)}
    sorted_groups = sorted(valid_groups.items(), key=lambda kv: kv[1], reverse=True)

    group_names = [kv[0] for kv in sorted_groups]
    group_aucs = [kv[1] for kv in sorted_groups]

    colors = ["#e05080" if abs(v - 0.5) < 0.05 else "#7CC8F2" for v in group_aucs]

    fig = go.Figure(
        go.Bar(
            x=group_names,
            y=group_aucs,
            marker_color=colors,
            hovertemplate="%{x}: AUC=%{y:.3f}<extra></extra>",
        )
    )
    fig.add_hline(
        y=_AUC_GATE,
        line_dash="dash",
        line_color="#e05080",
        annotation_text=f"gate={_AUC_GATE:.2f}",
    )
    fig.update_layout(
        title="subgroup 別 holdout AUC",
        xaxis_title="subgroup",
        yaxis_title="ROC-AUC",
        height=400,
        yaxis_range=[0.4, 1.0],
    )

    fairness_flag = (
        "⚠ AUC 差がゲート閾値を超過" if not math.isnan(max_diff) and max_diff > _SUBGROUP_DIFF_GATE
        else "AUC 差はゲート閾値内"
    )
    findings = (
        f"<p>subgroup 別 holdout AUC: {len(valid_groups)} グループ。"
        f"有効グループの AUC 最大差 = {_fmt(max_diff)}（上限 {_SUBGROUP_DIFF_GATE:.2f}）。"
        f"判定: {fairness_flag}。</p>"
        f"<p>サンプル数 &lt; 20 のサブグループは NaN (非表示)。"
        f"subgroup 定義: gender (M/F/NB), role_group (animation/direction/production/other), "
        f"cohort_band (debut 10 年区切り)。</p>"
    )

    kpis = [
        KPICard("最大 AUC 差", _fmt(max_diff), f"上限 {_SUBGROUP_DIFF_GATE:.2f}"),
        KPICard("有効 subgroup 数", str(len(valid_groups)), "n≥20 のみ"),
        KPICard("fairness 判定", fairness_flag[:20], ""),
    ]

    return ReportSection(
        title="subgroup fairness (gender / role_group / cohort_band 別 AUC)",
        findings_html=findings,
        visualization_html=plotly_div_safe(fig, "chart_visibility_fairness", height=400),
        kpi_cards=kpis,
        chart_caption=(
            "横軸 = subgroup ラベル、縦軸 = holdout ROC-AUC。"
            f"赤破線 = ゲート閾値 {_AUC_GATE:.2f}。"
            f"AUC 差 > {_SUBGROUP_DIFF_GATE:.2f} の場合は fairness 修正を優先する。"
            "赤色バー = AUC が 0.5 ± 0.05 以内 (ランダム予測と同等)。"
        ),
        method_note=(
            "subgroup fairness 基準: 全 subgroup 間の AUC 最大差 ≤ 0.10。"
            "差が 0.10 を超える場合は個別予測公開を保留し、"
            "モデル再設計 (features の再評価・post-hoc 較正) を先行させる。"
        ),
        section_id="visibility_fairness",
    )


def _build_feature_importance(sb: SectionBuilder, data: dict) -> ReportSection:
    """Section 3: SHAP mean abs feature 重要度。"""
    fi = data.get("feature_importances", {})
    passes = bool(data.get("passes_gate", False))

    if not data or not passes or not fi:
        findings = (
            "<p>特徴量重要度データが利用できません。"
            "モデル性能ゲートを通過後に表示されます。</p>"
        )
        return ReportSection(
            title="特徴量重要度 (SHAP mean abs)",
            findings_html=findings,
            section_id="visibility_features",
        )

    sorted_fi = sorted(fi.items(), key=lambda kv: float(kv[1]), reverse=True)
    top10 = sorted_fi[:10]
    feat_names = [kv[0] for kv in top10]
    feat_vals = [_safe_float(kv[1], 0.0) for kv in top10]

    top_feat = feat_names[0] if feat_names else "N/A"
    top_val = feat_vals[0] if feat_vals else 0.0

    fig = go.Figure(
        go.Bar(
            x=feat_vals,
            y=feat_names,
            orientation="h",
            marker_color="#7CC8F2",
            hovertemplate="%{y}: %{x:.4f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="翌年クレジット可視性喪失モデル 特徴量重要度 (Top 10)",
        xaxis_title="SHAP mean |value|",
        yaxis_title="",
        height=420,
        margin={"l": 200},
    )
    fig.update_yaxes(autorange="reversed")

    findings = (
        f"<p>特徴量重要度 (SHAP mean abs, Top 10): "
        f"最上位 = {top_feat}（{top_val:.4f}）。"
        f"全 feature 数: {len(fi):,}。</p>"
    )

    kpis = [
        KPICard("最上位 feature", top_feat[:24] if top_feat != "N/A" else "n/a", f"SHAP {top_val:.4f}"),
        KPICard("feature 総数", str(len(fi)), "全モデル入力変数"),
    ]

    return ReportSection(
        title="特徴量重要度 (SHAP mean abs)",
        findings_html=findings,
        visualization_html=plotly_div_safe(fig, "chart_visibility_fi", height=420),
        kpi_cards=kpis,
        chart_caption=(
            "横軸 = SHAP mean absolute value (大きいほど予測への寄与が大)、"
            "縦軸 = feature 名。"
            "SHAP は構造的指標のみに基づく — 外部レーティングは含まない。"
            "因果的寄与ではなく予測関連性を示す指標。"
        ),
        method_note=(
            "重要度指標: SHAP (SHapley Additive exPlanations) mean absolute value。"
            "MDI ではなく SHAP を使用することで高カーディナリティ変数のバイアスを軽減。"
            "SHAP でも非線形交互作用の解釈には限界があり、"
            "因果的解釈は別途 DML / RDD 等の識別戦略が必要。"
        ),
        section_id="visibility_features",
    )


def _build_cox_section(sb: SectionBuilder, data: dict) -> ReportSection | None:
    """Cox PH 並設 section (data.cox_results が存在する時のみレンダー)。

    cox_results schema:
        feature_names: list[str]
        hazard_ratios: list[float]
        hr_ci_low / hr_ci_high: list[float]
        p_values: list[float]
        concordance_index: float
        n_subjects: int
        n_events: int
        ph_violators: list[str]  # Schoenfeld test 違反 feature 列
        ph_global_p: float
        train_concordance / test_concordance: float (temporal holdout)

    Returns None if data unavailable → セクション挿入見送り。
    """
    cox = data.get("cox_results") if isinstance(data, dict) else None
    if not cox:
        return None

    feature_names = cox.get("feature_names", [])
    hrs = cox.get("hazard_ratios", [])
    hr_lo = cox.get("hr_ci_low", [])
    hr_hi = cox.get("hr_ci_high", [])
    pvals = cox.get("p_values", [])
    c_train = _safe_float(cox.get("train_concordance"))
    c_test = _safe_float(cox.get("test_concordance"))
    ph_p = _safe_float(cox.get("ph_global_p"))
    ph_violators = cox.get("ph_violators", []) or []

    rows = "".join(
        f"<tr><td>{name}</td>"
        f"<td>{_fmt(hr)}</td>"
        f"<td>[{_fmt(lo)}, {_fmt(hi)}]</td>"
        f"<td>{_fmt(p, 4)}</td></tr>"
        for name, hr, lo, hi, p in zip(feature_names, hrs, hr_lo, hr_hi, pvals)
    )

    findings = (
        "<p>Cox PH (lifelines) による hazard 推定。LightGBM の予測精度と並設し "
        "interpretation 容易性を強化する。</p>"
        f"<p>concordance (train): {_fmt(c_train)}, "
        f"concordance (test, debut_year holdout): {_fmt(c_test)}。"
        f"PH global p (Schoenfeld): {_fmt(ph_p, 4)}。</p>"
        + (
            f"<p>PH 仮定違反 feature: {', '.join(ph_violators)}。 "
            "時間相互作用項を追加した拡張 spec で再推定推奨。</p>"
            if ph_violators else
            "<p>PH 仮定: 全 feature で違反なし。</p>"
        )
        + "<table><thead><tr><th>feature</th><th>HR</th><th>95% CI</th>"
        "<th>p</th></tr></thead><tbody>" + rows + "</tbody></table>"
    )

    return ReportSection(
        title="Cox PH (並設、HR + Schoenfeld test)",
        section_id="visibility_cox",
        findings_html=findings,
        method_note=(
            "lifelines.CoxPHFitter で fit。HR は exp(β)。95% CI = exp(β ± 1.96 × SE)。"
            "Schoenfeld residual test で PH 仮定検証 (p < 0.05 で違反)。"
            "temporal holdout: debut_year 閾値で train/test 分割、"
            "concordance を両半分で計算し drift を観察。"
        ),
    )


def _build_interpretation(data: dict) -> str:
    """Interpretation 節 HTML を構築する。

    REPORT_PHILOSOPHY v2 §2.2 準拠:
    - 一人称明示
    - 代替解釈 1 件以上
    - 依拠する前提の明示
    """
    if not data:
        return ""

    auc = _safe_float(data.get("auc_roc"))
    passes = bool(data.get("passes_gate", False))
    max_diff = _safe_float(data.get("subgroup_max_diff"))

    if not passes:
        return ""

    auc_str = _fmt(auc)
    fairness_ok = math.isnan(max_diff) or max_diff <= _SUBGROUP_DIFF_GATE

    primary = (
        f"本レポートの分析者は、holdout AUC = {auc_str} を"
        "「翌年クレジット可視性喪失の構造的パターンが、ゼロ情報ベースラインを"
        "上回る程度に予測可能である」ことの証拠と解釈する。"
        "この解釈は、クレジット可視性の消失パターンが経済的構造"
        "(ネットワーク中心性・スタジオ依存度・コホート経過年数) と"
        "系統的に共変するという前提に依拠している。"
    )

    alt = (
        "代替解釈: この AUC は、モデルが「過去に可視性が低かった人物が"
        "将来も低い」という時間的自己相関を捉えているに過ぎず、"
        "構造的原因を反映していない可能性がある。"
        "この解釈では、モデルを早期警告として使うことの価値は限定的となる。"
    )

    fairness_note = (
        (
            "subgroup 間 AUC 差がゲート以内であることから、"
            "分析者はモデルが gender / role_group / cohort_band 間で"
            "おおむね均等な予測精度を持つと解釈する。"
            "ただし、サンプル数が少ないサブグループ (n &lt; 20) は検証対象外であり、"
            "これらのグループへの適用は注意が必要。"
        )
        if fairness_ok
        else (
            "subgroup 間 AUC 差がゲート閾値を超過しており、"
            "分析者は特定の subgroup でモデル精度が著しく低下していると解釈する。"
            "現状では個別予測の公開を保留し、fairness 修正を優先することを推奨する。"
        )
    )

    return (
        f"<p><strong>主解釈</strong>: {primary}</p>"
        f"<p><strong>代替解釈</strong>: {alt}</p>"
        f"<p><strong>fairness 解釈</strong>: {fairness_note}</p>"
        "<p><em>注意</em>: 本解釈は「クレジット可視性の消失 = 業界離脱」を含意しない。"
        "消失の内訳 (スタジオ移動・無名義・データ欠落・休業) は本データから推定できない。"
        "早期警告としての活用は、スタジオ HR による継続的なクレジット公開推進の"
        "トリガーとして位置づけるにとどめ、個別人事判断の根拠にしてはならない。</p>"
    )


# ---------------------------------------------------------------------------
# v3 SPEC
# ---------------------------------------------------------------------------

from .._spec import HoldoutSpec, SensitivityAxis, make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="career_visibility_warning",
    audience="hr",
    claim=(
        "LightGBM + isotonic calibration による翌年クレジット可視性喪失予測の "
        "holdout AUC が 0.65 以上かつ subgroup AUC 差 ≤ 0.10 の場合に "
        "早期警告スコアを HR brief に公開する。"
    ),
    identifying_assumption=(
        "クレジット可視性の喪失パターンが構造的変数 (ネットワーク位置・"
        "スタジオ多様性・コホート経過年数) と系統的に共変する。"
        "離職・業界離脱との対応は本データから検証不可。"
        "予測は確率的指標であり、個別人事判断の根拠として使用してはならない。"
    ),
    null_model=["N3", "N7"],
    sources=["credits", "persons", "anime", "mart.akm_person_fe"],
    meta_table="meta_career_visibility_warning",
    estimator="LightGBM + isotonic calibration (sklearn CalibratedClassifierCV)",
    ci_estimator="bootstrap",
    n_resamples=200,
    holdout=HoldoutSpec(
        method="time-split",
        holdout_size="最終 1 年 (holdout_year)",
        metric="ROC-AUC",
        naive_baseline="last-3-year mean loss rate per person",
    ),
    sensitivity_grid=[
        SensitivityAxis(name="AUC gate", values=[0.60, 0.65, 0.70]),
        SensitivityAxis(name="subgroup diff gate", values=[0.05, 0.10, 0.15]),
        SensitivityAxis(name="n_estimators", values=[100, 300, 500]),
        SensitivityAxis(name="window_years", values=[2, 3, 5]),
    ],
    extra_limitations=[
        "Label = クレジット可視性喪失 ≠ 業界離脱。内訳 (スタジオ移動・無名義・休業等) は不明",
        "in-between animator 等はクレジット可視性が構造的に低い — coverage bias",
        "subgroup AUC 差 > 0.10 の場合は公開保留 (fairness gate)",
        "theta_i / PageRank が未計算の場合は 0.0 で補完 — feature quality 低下",
    ],
)
