"""チーム組成テンプレート分析 — v2 compliant."""

from __future__ import annotations

import json
import math
from pathlib import Path

import plotly.graph_objects as go

from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_JSON_DIR = Path(__file__).parents[4] / "result" / "json"

_SILHOUETTE_GATE = 0.3


def _load(name: str) -> dict | list:
    p = _JSON_DIR / f"{name}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _sf(v: object, default: float = 0.0) -> float:
    try:
        f = float(v)  # type: ignore[arg-type]
        return default if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


def _normalize_axis(values: list[float]) -> list[float]:
    """Normalize values to [0, 1] across a list."""
    if not values:
        return values
    vmin = min(values)
    vmax = max(values)
    span = vmax - vmin
    if span == 0:
        return [0.5] * len(values)
    return [(v - vmin) / span for v in values]


class BizTeamTemplateReport(BaseReportGenerator):
    name = "biz_team_template"
    title = "チーム組成テンプレート"
    subtitle = "K=5クラスタ プロファイル / 成功率比較"
    filename = "biz_team_template.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load("team_templates")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(self._build_cluster_profiles(sb, data)),
            sb.build_section(self._build_success_rates(sb, data)),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: Cluster centroids radar ──────────────────────────

    def _build_cluster_profiles(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        archetypes = data.get("archetypes", {})
        if not isinstance(archetypes, dict):
            archetypes = {}

        n_teams_total = int(data.get("n_teams_total", 0))
        silhouette = _sf(data.get("silhouette_score", 0.0))
        gate_passed = bool(data.get("silhouette_gate_passed", False))

        axes = ["size", "role_entropy", "fe_mean", "fe_std"]
        axes_ja = ["チームサイズ", "ロールエントロピー", "FE平均", "FE標準偏差"]

        if not archetypes:
            findings = (
                "<p>アーキタイプデータが利用できません"
                "（team_templates.archetypes）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="K=5クラスタ・セントロイド（4軸）",
                findings_html=findings,
                method_note=(
                    f"K-means K=5 on (size, role_entropy, fe_mean, fe_std),"
                    f" silhouette gate > {_SILHOUETTE_GATE}"
                ),
                section_id="tt_cluster_profiles",
            )

        # Collect raw centroid values per axis for normalization
        raw: dict[str, list[float]] = {ax: [] for ax in axes}
        for name, arc in archetypes.items():
            centroid = arc.get("centroid", {})
            if not isinstance(centroid, dict):
                centroid = {}
            for ax in axes:
                raw[ax].append(_sf(centroid.get(ax, 0.0)))

        # Normalize each axis
        norm: dict[str, list[float]] = {
            ax: _normalize_axis(raw[ax]) for ax in axes
        }

        fig = go.Figure()
        colors = ["#667eea", "#764ba2", "#f093fb", "#f5576c", "#fda085"]
        for i, (arc_name, _) in enumerate(archetypes.items()):
            r_vals = [norm[ax][i] for ax in axes]
            # Close the polygon
            r_closed = r_vals + [r_vals[0]]
            theta_closed = axes_ja + [axes_ja[0]]
            fig.add_trace(
                go.Scatterpolar(
                    r=r_closed,
                    theta=theta_closed,
                    fill="toself",
                    name=arc_name,
                    line_color=colors[i % len(colors)],
                    opacity=0.7,
                )
            )

        fig.update_layout(
            title="K=5アーキタイプ セントロイド（4軸、0-1正規化）",
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 1])
            ),
            height=500,
        )

        gate_str = "達成" if gate_passed else "未達"
        findings = (
            f"<p>チーム総数: {n_teams_total:,}チーム。"
            f"シルエットスコア: {silhouette:.3f}"
            f"（ゲート閾値={_SILHOUETTE_GATE}、{gate_str}）。"
            f"レーダーチャートは各アーキタイプのセントロイドを"
            f"4軸（0-1正規化）で示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="K=5クラスタ・セントロイド（4軸）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_tt_radar", height=500
            ),
            method_note=(
                f"K-means K=5 on (size, role_entropy, fe_mean, fe_std),"
                f" silhouette gate > {_SILHOUETTE_GATE}。"
                "各軸は全アーキタイプ間で0-1正規化済み。"
                "role_entropy: チーム内ロール分布のシャノンエントロピー。"
                "fe_mean/fe_std: チーム内 person_fe のパーセンタイル平均・標準偏差。"
            ),
            section_id="tt_cluster_profiles",
        )

    # ── Section 2: Success rates ─────────────────────────────────────

    def _build_success_rates(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        archetypes = data.get("archetypes", {})
        if not isinstance(archetypes, dict):
            archetypes = {}

        if not archetypes:
            findings = (
                "<p>成功率データが利用できません"
                "（team_templates.archetypes）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="アーキタイプ別成功率（scale_tier >= 4）",
                findings_html=findings,
                method_note="成功定義: scale_tier >= 4 の作品で構成されるチーム",
                section_id="tt_success",
            )

        # Sort by success_rate desc
        sorted_arcs = sorted(
            archetypes.items(),
            key=lambda kv: _sf(kv[1].get("success_rate", 0.0)),
            reverse=True,
        )
        arc_names = [kv[0] for kv in sorted_arcs]
        rates = [_sf(kv[1].get("success_rate", 0.0)) for kv in sorted_arcs]
        n_teams_list = [int(kv[1].get("n_teams", 0)) for kv in sorted_arcs]

        colors = ["#fda085", "#f5576c", "#f093fb", "#764ba2", "#667eea"]

        fig = go.Figure(
            go.Bar(
                x=rates,
                y=arc_names,
                orientation="h",
                marker_color=[colors[i % len(colors)] for i in range(len(arc_names))],
                text=[f"n={n}" for n in n_teams_list],
                textposition="outside",
                hovertemplate=(
                    "%{y}<br>成功率=%{x:.3f}<br>チーム数=%{text}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="アーキタイプ別 成功率（scale_tier >= 4）、成功率降順",
            xaxis_title="成功率",
            yaxis_title="アーキタイプ",
            xaxis=dict(range=[0, 1.1]),
            height=420,
            margin=dict(l=180, r=80),
        )

        rate_max = max(rates) if rates else 0.0
        rate_min = min(rates) if rates else 0.0
        best_arc = arc_names[0] if arc_names else "N/A"
        worst_arc = arc_names[-1] if arc_names else "N/A"

        findings = (
            f"<p>成功率の範囲: {rate_min:.3f} ～ {rate_max:.3f}。"
            f"成功率上位アーキタイプ: {best_arc}。"
            f"成功率下位アーキタイプ: {worst_arc}。"
            f"バー外のテキストはアーキタイプ別チーム数を示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="アーキタイプ別成功率（scale_tier >= 4）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_tt_success", height=420
            ),
            method_note=(
                "成功定義: scale_tier >= 4 の作品で構成されるチーム。"
                "scale_tier: production_scale（スタッフ数×エピソード数×duration_mult）"
                "のパーセンタイル分位（Tier1-6）。"
                "成功率はチーム数が少ない場合に不安定になる"
                "（n_teams < 20のアーキタイプは信頼区間が広い）。"
            ),
            section_id="tt_success",
        )
