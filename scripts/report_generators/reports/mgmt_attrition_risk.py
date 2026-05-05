"""離職リスクスコア分析 — v2 compliant.

Management brief: attrition risk model performance and feature importance.
- Section 1: Model performance (C-index gate + tier risk chart)
- Section 2: Feature importance (Top 10)
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import plotly.graph_objects as go

from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_JSON_DIR = Path(__file__).parents[4] / "result" / "json"

_C_INDEX_GATE = 0.70


def _load(name: str) -> dict | list:
    p = _JSON_DIR / f"{name}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _safe_float(v: object, default: float = 0.0) -> float:
    try:
        f = float(v)  # type: ignore[arg-type]
        return default if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


_GATE_MSG = (
    "<p>モデル性能ゲート未達: C-index &lt; {gate:.2f}。"
    "C-indexは生存モデルの識別性能指標であり、ランダム予測と同等（0.5）から"
    "完全予測（1.0）の範囲をとる。"
    "このレポートは C-index ≥ {gate:.2f} を公開条件としている。"
    "現在のC-index={c_index:.3f}はゲート閾値を下回るため、"
    "個別スコアおよびTier別リスク推定を表示しない。</p>"
)


class MgmtAttritionRiskReport(BaseReportGenerator):
    name = "mgmt_attrition_risk"
    title = "離職リスクスコア分析"
    subtitle = "生存モデル C-index / キャリブレーション / 特徴量重要度"
    filename = "mgmt_attrition_risk.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load("attrition_risk_model")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(self._build_model_performance(sb, data)),
            sb.build_section(self._build_feature_importance(sb, data)),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: Model performance ────────────────────────────────

    def _build_model_performance(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        c_index = data.get("c_index")
        brier = data.get("brier_score")
        agg = data.get("aggregate_by_tier", {})

        if not isinstance(agg, dict):
            agg = {}

        c_val = _safe_float(c_index) if c_index is not None else 0.0

        # Gate check
        if not data or c_val < _C_INDEX_GATE:
            gate_html = _GATE_MSG.format(gate=_C_INDEX_GATE, c_index=c_val)
            violations = sb.validate_findings(gate_html)
            if violations:
                gate_html += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="モデル性能（C-index / キャリブレーション）",
                findings_html=gate_html,
                method_note=(
                    "Random Survival Forest, temporal split "
                    "2010-2015 train / 2017-2018 test, C-index gate=0.70。"
                    "C-index ≥ 0.70 が公開条件（REPORT_PHILOSOPHY v2 必須方法ゲート）。"
                ),
                section_id="attrition_performance",
            )

        brier_val = _safe_float(brier) if brier is not None else float("nan")
        brier_str = (
            f"{brier_val:.4f}" if not math.isnan(brier_val) else "N/A"
        )

        # Tier risk chart
        tiers = sorted(agg.keys())
        horizons = ["mean_risk_1y", "mean_risk_3y", "mean_risk_5y"]
        horizon_labels = ["1年", "3年", "5年"]
        colors_h = ["#3593D2", "#E09BC2", "#FFB444"]

        fig = go.Figure()
        for h, label, color in zip(horizons, horizon_labels, colors_h):
            x_tiers = []
            y_risks = []
            for tier in tiers:
                tier_data = agg.get(tier, {})
                if not isinstance(tier_data, dict):
                    continue
                risk = tier_data.get(h)
                if risk is not None:
                    x_tiers.append(str(tier))
                    y_risks.append(_safe_float(risk))
            if x_tiers:
                fig.add_trace(
                    go.Bar(
                        name=label,
                        x=x_tiers,
                        y=y_risks,
                        marker_color=color,
                        hovertemplate=(
                            f"Tier %{{x}} {label}: %{{y:.3f}}<extra></extra>"
                        ),
                    )
                )

        fig.update_layout(
            title="キャリア段階Tier別 平均離職リスク（1年/3年/5年）",
            xaxis_title="キャリア段階Tier",
            yaxis_title="平均離職リスク",
            barmode="group",
            height=420,
        )

        n_tiers = len(tiers)
        findings = (
            f"<p>モデル性能: C-index={c_val:.3f}"
            f"（ゲート閾値={_C_INDEX_GATE:.2f}、達成）、"
            f"Brier score={brier_str}。"
            f"Tier別リスク集計: {n_tiers:,}Tier。"
            f"バーチャートはTier別・時間軸別の平均離職リスク推定値を示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="モデル性能（C-index / キャリブレーション）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_attrition_perf", height=420
            ),
            method_note=(
                "Random Survival Forest, temporal split "
                "2010-2015 train / 2017-2018 test, C-index gate=0.70。"
                "C-index: 生存分析における識別性能指標"
                "（Harrell's C-statistic）。"
                "Brier score: 確率予測の較正精度指標（低いほど良）。"
                "aggregate_by_tier: カーネル分布内のキャリア段階Tierへの集計。"
            ),
            section_id="attrition_performance",
        )

    # ── Section 2: Feature importance ──────────────────────────────

    def _build_feature_importance(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        fi = data.get("feature_importances", {})
        c_val = _safe_float(data.get("c_index")) if data.get("c_index") else 0.0

        if not isinstance(fi, dict):
            fi = {}

        if not fi or c_val < _C_INDEX_GATE:
            findings = (
                "<p>特徴量重要度データが利用できません、"
                "またはC-indexゲート未達のため表示しません"
                "（attrition_risk_model.feature_importances）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="特徴量重要度（Top 10）",
                findings_html=findings,
                section_id="attrition_features",
            )

        sorted_fi = sorted(fi.items(), key=lambda kv: kv[1], reverse=True)
        top10 = sorted_fi[:10]

        feat_names = [kv[0] for kv in top10]
        feat_imps = [_safe_float(kv[1]) for kv in top10]

        top_feat = feat_names[0] if feat_names else "N/A"
        top_imp = feat_imps[0] if feat_imps else 0.0

        fig = go.Figure(
            go.Bar(
                x=feat_imps,
                y=feat_names,
                orientation="h",
                marker_color="#7CC8F2",
                hovertemplate="%{y}: 重要度=%{x:.4f}<extra></extra>",
            )
        )
        fig.update_layout(
            title="離職リスクモデル 特徴量重要度（Top 10）",
            xaxis_title="特徴量重要度",
            yaxis_title="",
            height=420,
            margin=dict(l=200),
        )
        fig.update_yaxes(autorange="reversed")

        findings = (
            f"<p>特徴量重要度（Top 10）: "
            f"最上位特徴量={top_feat}（重要度={top_imp:.4f}）。"
            f"全特徴量数: {len(fi):,}件。"
            f"重要度はRandom Survival Forestの特徴量重要度（MDI）。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="特徴量重要度（Top 10）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_attrition_fi", height=420
            ),
            method_note=(
                "特徴量重要度: Random Survival Forest の"
                "平均不純度減少（MDI）によるランキング。"
                "MDIは木ベースモデルに固有のバイアスを持つ"
                "（高カーディナリティ特徴量を過大評価する傾向）。"
                "permutation importanceとの比較が推奨される。"
            ),
            section_id="attrition_features",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='mgmt_attrition_risk',
    audience='hr',
    claim='離職リスクスコア分析 に関する記述的指標 (subtitle: 生存モデル C-index / キャリブレーション / 特徴量重要度)',
    sources=["credits", "persons", "anime"],
    meta_table='meta_mgmt_attrition_risk',
)
