"""Policy Gender Bottleneck report — v2 compliant.

ジェンダー・ボトルネック分析:
- Section 1: ステージ遷移別生存曲線（性別比較）
- Section 2: 昇進率ギャップのOaxaca-Blinder分解
- Section 3: スタジオ別ジェンダーギャップ (γ_j)
"""

from __future__ import annotations

import json
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

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


_COLOR_F = "#f093fb"  # female line color
_COLOR_M = "#a0d2db"  # male line color

_STAGE_LABELS = {
    "stage_0_to_2": "ステージ0→2",
    "stage_0_to_3": "ステージ0→3",
    "stage_0_to_4": "ステージ0→4",
}


class PolicyGenderBottleneckReport(BaseReportGenerator):
    name = "policy_gender_bottleneck"
    title = "ジェンダー・ボトルネック分析"
    subtitle = "昇進KM / Oaxaca-Blinder分解 / スタジオFE"
    filename = "policy_gender_bottleneck.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        data = _load("gender_bottleneck")

        sections: list[str] = [
            sb.build_section(self._build_survival_by_stage(sb, data)),
            sb.build_section(self._build_oaxaca(sb, data)),
            sb.build_section(self._build_studio_fe(sb, data)),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: Survival by stage (gender comparison) ─────────

    def _build_survival_by_stage(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        surv_by_stage: dict = data.get("survival_by_stage", {}) if isinstance(data, dict) else {}
        data_quality: dict = data.get("data_quality", {}) if isinstance(data, dict) else {}

        coverage_warning = ""
        coverage_pct = data_quality.get("coverage_pct")
        if coverage_pct is not None and coverage_pct < 50:
            coverage_warning = (
                f'<p style="color:#e09050;font-size:0.85rem;">'
                f"[データ品質] gender フィールドのカバレッジが{coverage_pct:.1f}%（50%未満）。"
                f"性別不明人物は分析から除外されています。</p>"
            )

        if not surv_by_stage:
            return ReportSection(
                title="ステージ遷移別生存曲線（性別比較）",
                findings_html=(
                    "<p>ステージ遷移別生存曲線データが利用できません"
                    "（gender_bottleneck.survival_by_stage）。"
                    "KM推定量はstage_0_to_2 / stage_0_to_3 / stage_0_to_4ごとに"
                    "F/M別に推定されます。</p>"
                    + coverage_warning
                ),
                section_id="survival_by_stage",
            )

        # Use stage_0_to_3 as main chart, others as supplementary text
        stage_keys = [k for k in ("stage_0_to_2", "stage_0_to_3", "stage_0_to_4")
                      if k in surv_by_stage]

        if not stage_keys:
            return ReportSection(
                title="ステージ遷移別生存曲線（性別比較）",
                findings_html=(
                    "<p>有効なステージデータが見つかりませんでした。</p>" + coverage_warning
                ),
                section_id="survival_by_stage",
            )

        findings_items: list[str] = []
        for stage_key in stage_keys:
            stage_data = surv_by_stage[stage_key]
            if not isinstance(stage_data, dict):
                continue
            n_f = stage_data.get("n_F")
            n_m = stage_data.get("n_M")
            lr_p = stage_data.get("logrank_p")
            label = _STAGE_LABELS.get(stage_key, stage_key)
            n_f_str = f"n_F={n_f:,}" if n_f is not None else "n_F=不明"
            n_m_str = f"n_M={n_m:,}" if n_m is not None else "n_M=不明"
            p_str = f"log-rank p={lr_p:.4f}" if lr_p is not None else "log-rank p=N/A"
            findings_items.append(f"<li><strong>{label}</strong>: {n_f_str}, {n_m_str}, {p_str}</li>")

        findings_html = (
            "<p>ステージ遷移別KM生存曲線（F: 女性, M: 男性）の概要:</p>"
            f"<ul>{''.join(findings_items)}</ul>"
            + coverage_warning
        )

        # Build subplot with one panel per stage key (max 3)
        n_panels = len(stage_keys)
        fig = make_subplots(
            rows=1, cols=n_panels,
            subplot_titles=[_STAGE_LABELS.get(k, k) for k in stage_keys],
            horizontal_spacing=0.08,
        )

        for col_idx, stage_key in enumerate(stage_keys, 1):
            stage_data = surv_by_stage[stage_key]
            if not isinstance(stage_data, dict):
                continue

            for gender, color, dash in [("F", _COLOR_F, "solid"), ("M", _COLOR_M, "dot")]:
                gd = stage_data.get(gender, {})
                if not isinstance(gd, dict):
                    continue
                timeline = gd.get("timeline", [])
                survival = gd.get("survival", [])
                ci_lower = gd.get("ci_lower", [])
                ci_upper = gd.get("ci_upper", [])

                if not timeline or not survival:
                    continue

                show_legend = col_idx == 1

                # CI shading
                if ci_upper and ci_lower and len(ci_upper) == len(timeline):
                    x_fill = list(timeline) + list(reversed(timeline))
                    y_fill = list(ci_upper) + list(reversed(ci_lower))
                    r, g_c, b = _hex_to_rgb_tuple(color)
                    fig.add_trace(go.Scatter(
                        x=x_fill, y=y_fill,
                        fill="toself",
                        fillcolor=f"rgba({r},{g_c},{b},0.1)",
                        line=dict(width=0),
                        showlegend=False,
                        hoverinfo="skip",
                    ), row=1, col=col_idx)

                fig.add_trace(go.Scatter(
                    x=timeline, y=survival,
                    mode="lines",
                    name=f"{'女性(F)' if gender == 'F' else '男性(M)'}",
                    line=dict(color=color, width=2, dash=dash),
                    showlegend=show_legend,
                    hovertemplate=f"{gender}: t=%{{x}}, S=%{{y:.3f}}<extra></extra>",
                ), row=1, col=col_idx)

        fig.update_layout(
            title="ステージ遷移別生存曲線（F vs M、95% CI）",
            legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1),
        )
        for col_idx in range(1, n_panels + 1):
            fig.update_yaxes(range=[0, 1.05], row=1, col=col_idx)
            fig.update_xaxes(title_text="経過年数", row=1, col=col_idx)

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="ステージ遷移別生存曲線（性別比較）",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_surv_stage", height=420),
            method_note=(
                "KM推定量。イベント: 各ステージ閾値への到達（stage_0_to_k = ステージk以上到達）。"
                "ログランク検定（logrank_p）: F/M生存曲線の同一性検定。"
                "性別不明（NULL）は分析から除外。"
                "Greenwood公式による95% CI。"
                "出典: gender_bottleneck.survival_by_stage。"
            ),
            section_id="survival_by_stage",
        )

    # ── Section 2: Oaxaca-Blinder decomposition ───────────────────

    def _build_oaxaca(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        oaxaca: dict = data.get("oaxaca_decomposition", {}) if isinstance(data, dict) else {}

        if not oaxaca:
            return ReportSection(
                title="昇進率ギャップのOaxaca-Blinder分解",
                findings_html=(
                    "<p>Oaxaca-Blinder分解データが利用できません"
                    "（gender_bottleneck.oaxaca_decomposition）。"
                    "raw_gap = F平均 - M平均を説明済み成分（特性差）と"
                    "未説明成分（係数差）に分解します。</p>"
                ),
                section_id="oaxaca",
            )

        raw_gap = oaxaca.get("raw_gap")
        explained = oaxaca.get("explained")
        unexplained = oaxaca.get("unexplained")
        explained_fraction = oaxaca.get("explained_fraction")
        components: dict = oaxaca.get("components", {})
        n_f = oaxaca.get("n_F")
        n_m = oaxaca.get("n_M")

        n_f_str = f"n_F={n_f:,}" if n_f is not None else "n_F=不明"
        n_m_str = f"n_M={n_m:,}" if n_m is not None else "n_M=不明"
        gap_str = f"{raw_gap:.4f}" if raw_gap is not None else "N/A"
        expl_pct = f"{explained_fraction * 100:.1f}%" if explained_fraction is not None else "N/A"
        unexpl_pct = (
            f"{(1 - explained_fraction) * 100:.1f}%" if explained_fraction is not None else "N/A"
        )

        findings_html = (
            f"<p>Oaxaca-Blinder分解（{n_f_str}, {n_m_str}）: "
            f"raw_gap={gap_str}, 説明済み割合={expl_pct}, 未説明割合={unexpl_pct}。</p>"
        )

        if not components and explained is None:
            viz_html = '<p style="color:#8a94a0">成分データが利用できません。</p>'
        else:
            # Stacked bar: explained vs unexplained (and sub-components if available)
            if components:
                comp_names = list(components.keys())
                comp_vals = [components[c] for c in comp_names]
                # Split positive (explained) and negative (unexplained) parts
                pos_vals = [max(v, 0) for v in comp_vals]
                neg_vals = [min(v, 0) for v in comp_vals]

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    name="正方向寄与（説明済み）",
                    x=comp_names,
                    y=pos_vals,
                    marker_color="#06D6A0",
                    hovertemplate="%{x}: %{y:.4f}<extra></extra>",
                ))
                fig.add_trace(go.Bar(
                    name="負方向寄与（未説明）",
                    x=comp_names,
                    y=neg_vals,
                    marker_color="#f5576c",
                    hovertemplate="%{x}: %{y:.4f}<extra></extra>",
                ))
                fig.update_layout(
                    title="昇進率ギャップのOaxaca-Blinder成分分解",
                    xaxis_title="成分",
                    yaxis_title="ギャップへの寄与",
                    barmode="relative",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
            else:
                # Simple explained vs unexplained bar
                explained_v = explained if explained is not None else 0.0
                unexplained_v = unexplained if unexplained is not None else 0.0
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=["説明済み", "未説明"],
                    y=[explained_v, unexplained_v],
                    marker_color=["#06D6A0", "#f5576c"],
                    hovertemplate="%{x}: %{y:.4f}<extra></extra>",
                ))
                fig.add_hline(y=0, line_dash="dash", line_color="#a0a0a0")
                fig.update_layout(
                    title="昇進率ギャップのOaxaca-Blinder分解",
                    xaxis_title="成分",
                    yaxis_title="ギャップへの寄与",
                )
            viz_html = plotly_div_safe(fig, "chart_oaxaca", height=400)

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="昇進率ギャップのOaxaca-Blinder分解",
            findings_html=findings_html,
            visualization_html=viz_html,
            method_note=(
                "Oaxaca-Blinder分解: raw_gap = F平均 − M平均。"
                "説明済み成分 = Mの係数ベクトルに対するF/M特性差の寄与。"
                "未説明成分 = 同一特性に対する係数差。"
                "推定は線形確率モデル（OLS）に基づく。"
                "出典: gender_bottleneck.oaxaca_decomposition。"
            ),
            section_id="oaxaca",
        )

    # ── Section 3: Studio gender FE ───────────────────────────────

    def _build_studio_fe(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        studio_fe: dict = data.get("studio_gender_fe", {}) if isinstance(data, dict) else {}

        if not studio_fe:
            return ReportSection(
                title="スタジオ別ジェンダーギャップ（γ_j）",
                findings_html=(
                    "<p>スタジオ別ジェンダーFEデータが利用できません"
                    "（gender_bottleneck.studio_gender_fe）。"
                    "γ_j はスタジオjにおける性別ギャップの固定効果推定値です。"
                    "正値はF優位、負値はM優位を示します。</p>"
                ),
                section_id="studio_fe",
            )

        n_studios = studio_fe.get("n_studios", 0)
        top10_f: list = studio_fe.get("top_10_F_favored", [])
        bottom10_m: list = studio_fe.get("bottom_10_M_favored", [])

        all_entries: list[dict] = list(top10_f) + list(bottom10_m)
        # De-duplicate by studio_id / studio_name
        seen: set[str] = set()
        deduped: list[dict] = []
        for entry in all_entries:
            key = str(entry.get("studio_id") or entry.get("studio_name") or id(entry))
            if key not in seen:
                seen.add(key)
                deduped.append(entry)

        # Sort by gamma descending
        deduped.sort(key=lambda e: e.get("gamma_j", 0), reverse=True)

        gamma_vals = [e.get("gamma_j", 0) for e in deduped]
        studio_names = [
            str(e.get("studio_name") or e.get("studio_id") or "不明")
            for e in deduped
        ]

        if gamma_vals:
            g_min = min(gamma_vals)
            g_max = max(gamma_vals)
            range_str = f"{g_min:.4f}〜{g_max:.4f}"
        else:
            range_str = "N/A"

        findings_html = (
            f"<p>スタジオ別ジェンダーFE（γ_j）: 対象スタジオ数={n_studios:,}。"
            f"γ_j の範囲: {range_str}（正値=F優位, 負値=M優位）。</p>"
            f"<p>上位10社（F優位）と下位10社（M優位）の合計{len(deduped)}社を表示。</p>"
        )

        if deduped:
            bar_colors = ["#f093fb" if v >= 0 else "#a0d2db" for v in gamma_vals]
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=gamma_vals,
                y=studio_names,
                orientation="h",
                marker_color=bar_colors,
                hovertemplate="%{y}: γ_j=%{x:.4f}<extra></extra>",
            ))
            fig.add_vline(x=0, line_dash="dash", line_color="#a0a0a0",
                          annotation_text="γ=0（ギャップなし）", annotation_position="top right")
            fig.update_layout(
                title="スタジオ別ジェンダーギャップ γ_j（上位10 / 下位10）",
                xaxis_title="γ_j（スタジオ別ジェンダーFE）",
                yaxis_title="",
                height=max(400, len(deduped) * 28 + 120),
                margin=dict(l=200),
            )
            fig.update_yaxes(autorange="reversed")
            viz_html = plotly_div_safe(fig, "chart_studio_fe", height=max(400, len(deduped) * 28 + 120))
        else:
            viz_html = '<p style="color:#8a94a0">スタジオ別ジェンダーFEデータが利用できません。</p>'

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオ別ジェンダーギャップ（γ_j）",
            findings_html=findings_html,
            visualization_html=viz_html,
            method_note=(
                "γ_j: スタジオ固定効果モデルによる推定値。"
                "回帰式: promotion_event ~ gender + γ_j + θ_i + ε。"
                "γ_j > 0: スタジオjでFの昇進率がMより相対的に高い。"
                "γ_j < 0: スタジオjでMの昇進率がFより相対的に高い。"
                "出典: gender_bottleneck.studio_gender_fe。"
            ),
            section_id="studio_fe",
        )


def _hex_to_rgb_tuple(hex_color: str) -> tuple[int, int, int]:
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
