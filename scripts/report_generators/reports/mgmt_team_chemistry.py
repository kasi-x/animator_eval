"""チーム化学反応分析 — v2 compliant.

Management brief: team chemistry via pair residuals.
- Section 1: Top 20 positive chemistry pairs (horizontal bar)
- Section 2: Chemistry network (circular node-link, significant pairs)
- Section 3: BH correction p-value scatter (p_raw vs p_bh)
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


class MgmtTeamChemistryReport(BaseReportGenerator):
    name = "mgmt_team_chemistry"
    title = "チーム化学反応分析"
    subtitle = "ペア残差 / BH補正検定 / Top20ポジティブペア"
    filename = "mgmt_team_chemistry.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load("team_chemistry")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(self._build_top_pairs(sb, data)),
            sb.build_section(self._build_chemistry_network(sb, data)),
            sb.build_section(self._build_null_comparison(sb, data)),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: Top 20 positive pairs ───────────────────────────

    def _build_top_pairs(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        top_pairs = data.get("top_20_positive_pairs", {})
        if not isinstance(top_pairs, dict):
            top_pairs = {}

        n_analyzed = int(_safe_float(data.get("n_pairs_analyzed")))
        n_significant = int(_safe_float(data.get("n_significant_pairs")))
        n_positive = int(_safe_float(data.get("n_positive_chemistry")))

        if not top_pairs:
            findings = (
                "<p>ポジティブ化学反応ペアデータが利用できません"
                "（team_chemistry.top_20_positive_pairs）。"
                "チーム化学反応分析モジュールの実行が必要です。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="ポジティブ化学反応ペア（Top20）",
                findings_html=findings,
                section_id="chemistry_top_pairs",
            )

        # Sort by mean_res desc
        pairs_sorted = sorted(
            top_pairs.items(),
            key=lambda kv: _safe_float(
                kv[1].get("mean_res") if isinstance(kv[1], dict) else 0
            ),
            reverse=True,
        )

        pair_labels = []
        mean_res_vals = []
        se_vals = []

        for key, pdata in pairs_sorted:
            if not isinstance(pdata, dict):
                continue
            pid_a = str(pdata.get("pid_a") or "")
            pid_b = str(pdata.get("pid_b") or "")
            # Use pair key or anonymised label
            label = (
                f"{pid_a[:8]}…/{pid_b[:8]}…"
                if pid_a and pid_b
                else str(key)[:20]
            )
            pair_labels.append(label)
            mean_res_vals.append(_safe_float(pdata.get("mean_res")))
            se_vals.append(_safe_float(pdata.get("se")))

        fig = go.Figure(
            go.Bar(
                x=mean_res_vals,
                y=pair_labels,
                orientation="h",
                marker_color="#06D6A0",
                error_x=dict(
                    type="data",
                    array=se_vals,
                    symmetric=True,
                ),
                hovertemplate="ペア %{y}: 残差=%{x:.4f}<extra></extra>",
            )
        )
        fig.add_vline(x=0, line_dash="dash", line_color="#a0a0a0")
        fig.update_layout(
            title="ポジティブ化学反応ペア Top20（平均ペア残差）",
            xaxis_title="平均ペア残差（mean_res）",
            yaxis_title="",
            height=560,
            margin=dict(l=180),
        )
        fig.update_yaxes(autorange="reversed")

        findings = (
            f"<p>分析ペア数: {n_analyzed:,}件。"
            f"BH補正後の有意ペア数: {n_significant:,}件。"
            f"ポジティブ化学反応ペア数: {n_positive:,}件。"
            f"グラフはmean_res上位20ペアを表示。"
            f"エラーバーは標準誤差（SE）。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="ポジティブ化学反応ペア（Top20）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_chemistry_top_pairs", height=560
            ),
            method_note=(
                "mean_res: 共同クレジット作品における"
                "ペアの実績値と線形予測値の差の平均（ペア残差）。"
                "正の値はペアの協働時の実績が予測を上回ることを示す。"
                "SE: 観測数に基づく標準誤差（解析的導出）。"
                "BH補正: Benjamini-Hochberg法による多重検定補正。"
            ),
            section_id="chemistry_top_pairs",
        )

    # ── Section 2: Chemistry network ───────────────────────────────

    def _build_chemistry_network(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        pair_residuals = data.get("pair_residuals", {})
        if not isinstance(pair_residuals, dict):
            pair_residuals = {}

        # Filter significant pairs
        sig_pairs = [
            (key, pdata)
            for key, pdata in pair_residuals.items()
            if isinstance(pdata, dict) and pdata.get("significant")
        ]

        if not sig_pairs:
            findings = (
                "<p>有意なペア残差データが利用できません"
                "（team_chemistry.pair_residuals、significant=True）。"
                "データが空か、有意なペアが存在しない可能性があります。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="化学反応ネットワーク（有意ペア）",
                findings_html=findings,
                section_id="chemistry_network",
            )

        # Limit to top 50 by |mean_res|
        sig_pairs_sorted = sorted(
            sig_pairs,
            key=lambda kp: abs(_safe_float(
                kp[1].get("mean_res") if isinstance(kp[1], dict) else 0
            )),
            reverse=True,
        )[:50]

        n_displayed = len(sig_pairs_sorted)

        # Collect unique person IDs
        person_ids: list[str] = []
        pid_set: set[str] = set()
        edge_list: list[tuple[str, str, float]] = []

        for _, pdata in sig_pairs_sorted:
            if not isinstance(pdata, dict):
                continue
            pa = str(pdata.get("pid_a") or "")
            pb = str(pdata.get("pid_b") or "")
            res = _safe_float(pdata.get("mean_res"))
            if pa and pb:
                if pa not in pid_set:
                    person_ids.append(pa)
                    pid_set.add(pa)
                if pb not in pid_set:
                    person_ids.append(pb)
                    pid_set.add(pb)
                edge_list.append((pa, pb, res))

        n_nodes = len(person_ids)

        # Circular layout
        node_x: dict[str, float] = {}
        node_y: dict[str, float] = {}
        for i, pid in enumerate(person_ids):
            angle = 2 * math.pi * i / max(n_nodes, 1)
            node_x[pid] = math.cos(angle)
            node_y[pid] = math.sin(angle)

        res_vals = [e[2] for e in edge_list]
        res_max = max(abs(v) for v in res_vals) if res_vals else 1.0

        fig = go.Figure()

        # Edge traces
        for pa, pb, res in edge_list:
            if pa not in node_x or pb not in node_x:
                continue
            opacity = 0.3 + 0.6 * abs(res) / res_max
            color = "#06D6A0" if res >= 0 else "#f5576c"
            fig.add_trace(
                go.Scatter(
                    x=[node_x[pa], node_x[pb], None],
                    y=[node_y[pa], node_y[pb], None],
                    mode="lines",
                    line=dict(color=color, width=1.5),
                    opacity=opacity,
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

        # Node trace
        node_x_list = [node_x[pid] for pid in person_ids]
        node_y_list = [node_y[pid] for pid in person_ids]
        node_labels = [pid[:10] for pid in person_ids]

        fig.add_trace(
            go.Scatter(
                x=node_x_list,
                y=node_y_list,
                mode="markers",
                marker=dict(
                    size=8,
                    color="#f093fb",
                    line=dict(color="#ffffff", width=1),
                ),
                text=node_labels,
                hovertemplate="%{text}<extra></extra>",
                name="人物ノード",
            )
        )
        fig.update_layout(
            title=(
                f"化学反応ネットワーク（有意ペア Top{n_displayed}、円形レイアウト）"
            ),
            xaxis=dict(visible=False, range=[-1.3, 1.3]),
            yaxis=dict(visible=False, range=[-1.3, 1.3]),
            showlegend=False,
            height=520,
        )

        findings = (
            f"<p>表示した有意ペア数: {n_displayed:,}件"
            f"（|mean_res|上位{n_displayed}件を選択）。"
            f"ノード数: {n_nodes:,}人。"
            f"エッジ色: 緑=正の残差、赤=負の残差。"
            f"エッジ透明度は|mean_res|の相対値に対応。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="化学反応ネットワーク（有意ペア）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_chemistry_network", height=520
            ),
            method_note=(
                "円形レイアウト: 全ノードを等間隔の角度で配置。"
                "エッジ: BH補正後有意ペア（p_bh < 0.05）を表示。"
                "可読性のため|mean_res|上位50件に絞り込み。"
                "ノードIDは先頭10文字のみ表示（個人識別を最小化）。"
            ),
            section_id="chemistry_network",
        )

    # ── Section 3: BH correction p-value scatter ───────────────────

    def _build_null_comparison(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        pair_residuals = data.get("pair_residuals", {})
        if not isinstance(pair_residuals, dict):
            pair_residuals = {}

        p_raw_list: list[float] = []
        p_bh_list: list[float] = []

        for pdata in pair_residuals.values():
            if not isinstance(pdata, dict):
                continue
            pr = pdata.get("p_raw")
            pb = pdata.get("p_bh")
            if pr is not None and pb is not None:
                p_raw_list.append(_safe_float(pr, default=1.0))
                p_bh_list.append(_safe_float(pb, default=1.0))

        if not p_raw_list:
            findings = (
                "<p>p値データが利用できません"
                "（team_chemistry.pair_residuals の p_raw / p_bh）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="BH補正前後のp値分布",
                findings_html=findings,
                section_id="chemistry_bh",
            )

        n_pairs = len(p_raw_list)
        sig_threshold = 0.05
        n_sig_bh = sum(1 for v in p_bh_list if v < sig_threshold)

        # Subsample for display if large
        max_display = 3000
        if n_pairs > max_display:
            step = n_pairs // max_display
            p_raw_disp = p_raw_list[::step]
            p_bh_disp = p_bh_list[::step]
        else:
            p_raw_disp = p_raw_list
            p_bh_disp = p_bh_list

        colors = [
            "#f5a623" if pb < sig_threshold else "#8a94a0"
            for pb in p_bh_disp
        ]

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=p_raw_disp,
                y=p_bh_disp,
                mode="markers",
                marker=dict(
                    color=colors,
                    size=4,
                    opacity=0.5,
                ),
                name="ペア（オレンジ=BH有意）",
                hovertemplate=(
                    "p_raw=%{x:.4f}<br>"
                    "p_bh=%{y:.4f}<extra></extra>"
                ),
            )
        )
        # Diagonal reference line
        fig.add_trace(
            go.Scatter(
                x=[0, 1],
                y=[0, 1],
                mode="lines",
                line=dict(color="#a0a0a0", dash="dash"),
                name="y=x（補正なし）",
                hoverinfo="skip",
            )
        )
        # BH threshold horizontal line
        fig.add_hline(
            y=sig_threshold,
            line_dash="dot",
            line_color="#f5576c",
            annotation_text=f"BH閾値={sig_threshold}",
        )
        fig.update_layout(
            title="BH補正前後のp値比較（p_raw vs p_bh）",
            xaxis_title="p_raw（補正前）",
            yaxis_title="p_bh（BH補正後）",
            xaxis=dict(range=[0, 1]),
            yaxis=dict(range=[0, 1]),
            height=460,
        )

        findings = (
            f"<p>p値比較対象ペア数: {n_pairs:,}件。"
            f"BH補正後の有意ペア数（p_bh<{sig_threshold}）: "
            f"{n_sig_bh:,}件。"
            f"破線（y=x）はBH補正なしの参照線。"
            f"補正後p値は補正前より保守的（上方）になる。"
            f"オレンジの点はBH補正後に有意なペア。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="BH補正前後のp値分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_chemistry_bh", height=460
            ),
            method_note=(
                "p_raw: ペア残差の t検定による補正前p値。"
                "p_bh: Benjamini-Hochberg法による偽発見率（FDR）制御後のp値。"
                "有意水準閾値: p_bh < 0.05。"
                "散布図は最大3,000点にサブサンプリング（大規模データ対応）。"
            ),
            section_id="chemistry_bh",
        )
