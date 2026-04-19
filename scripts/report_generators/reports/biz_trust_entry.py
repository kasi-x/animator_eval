"""信頼ネット参入経路分析 — v2 compliant."""

from __future__ import annotations

import json
import math
from pathlib import Path

import plotly.graph_objects as go

from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_JSON_DIR = Path(__file__).parents[4] / "result" / "json"

_HIST_BINS = 20


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


def _histogram(values: list[float], n_bins: int = _HIST_BINS) -> tuple[
    list[float], list[int]
]:
    """Compute simple histogram returning (bin_centers, counts)."""
    if not values:
        return [], []
    vmin = min(values)
    vmax = max(values)
    span = vmax - vmin
    if span == 0:
        return [vmin], [len(values)]
    bin_width = span / n_bins
    counts = [0] * n_bins
    for v in values:
        idx = min(int((v - vmin) / bin_width), n_bins - 1)
        counts[idx] += 1
    centers = [vmin + (i + 0.5) * bin_width for i in range(n_bins)]
    return centers, counts


class BizTrustEntryReport(BaseReportGenerator):
    name = "biz_trust_entry"
    title = "信頼ネット参入経路"
    subtitle = "ゲートキーパースコア / リーチ・フロンティア"
    filename = "biz_trust_entry.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load("trust_entry")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(self._build_gatekeeper_profiles(sb, data)),
            sb.build_section(self._build_reach_frontier(sb, data)),
            sb.build_section(self._build_historical_patterns(sb, data)),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: Gatekeeper score histogram ────────────────────────

    def _build_gatekeeper_profiles(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        n_gatekeepers = int(data.get("n_gatekeepers", 0))
        dist = data.get("gatekeeper_score_distribution", {})
        if not isinstance(dist, dict):
            dist = {}
        top100 = data.get("top_100", [])
        if not isinstance(top100, list):
            top100 = []

        p50 = _sf(dist.get("p50", 0.0))
        p75 = _sf(dist.get("p75", 0.0))

        if not data:
            findings = (
                "<p>ゲートキーパーデータが利用できません"
                "（trust_entry）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="ゲートキーパー集計（ロール・ステージ分布）",
                findings_html=findings,
                method_note=(
                    "ゲートキーパースコア = betweenness_z × 0.4 + "
                    "fe_pct_z × 0.3 + birank_z × 0.2 + bridge_z × 0.1"
                ),
                section_id="te_gatekeeper",
            )

        gk_scores = [_sf(row.get("gatekeeper_score", 0.0)) for row in top100]
        centers, counts = _histogram(gk_scores, n_bins=_HIST_BINS)

        fig = go.Figure(
            go.Bar(
                x=centers,
                y=counts,
                marker_color="#667eea",
                name="ゲートキーパースコア",
                hovertemplate="スコア≈%{x:.2f}: 人数=%{y}<extra></extra>",
            )
        )
        if dist.get("p50") is not None:
            fig.add_vline(
                x=p50,
                line_color="#f093fb",
                line_dash="dash",
                annotation_text=f"p50={p50:.2f}",
            )
        if dist.get("p75") is not None:
            fig.add_vline(
                x=p75,
                line_color="#fda085",
                line_dash="dot",
                annotation_text=f"p75={p75:.2f}",
            )
        fig.update_layout(
            title=f"ゲートキーパースコア分布（Top-100 表示、全体={n_gatekeepers:,}名）",
            xaxis_title="ゲートキーパースコア",
            yaxis_title="人数（Top-100 bin）",
            height=420,
        )

        findings = (
            f"<p>ゲートキーパー総数: {n_gatekeepers:,}名。"
            f"スコア分布（Top-100）: p50={p50:.3f}, p75={p75:.3f}。"
            f"ヒストグラムはTop-100のスコア分布を示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="ゲートキーパー集計（ロール・ステージ分布）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_te_gatekeeper", height=420
            ),
            method_note=(
                "ゲートキーパースコア = betweenness_z × 0.4 + "
                "fe_pct_z × 0.3 + birank_z × 0.2 + bridge_z × 0.1。"
                "z-scoreは全スコア付与対象者に対する標準化値。"
                "Top-100はスコア上位100名のサブセット。"
            ),
            section_id="te_gatekeeper",
        )

    # ── Section 2: Reach × gatekeeper score frontier ────────────────

    def _build_reach_frontier(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        top100 = data.get("top_100", [])
        if not isinstance(top100, list):
            top100 = []

        if not top100:
            findings = (
                "<p>リーチ・フロンティアデータが利用できません"
                "（trust_entry.top_100）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="リーチ × ゲートキーパースコア パレートフロンティア",
                findings_html=findings,
                section_id="te_reach",
            )

        gk_scores = [_sf(row.get("gatekeeper_score", 0.0)) for row in top100]
        reach_fracs = [_sf(row.get("reach_fraction", 0.0)) for row in top100]
        pids = [str(row.get("person_id", "")) for row in top100]

        fig = go.Figure(
            go.Scatter(
                x=gk_scores,
                y=reach_fracs,
                mode="markers+text",
                text=pids,
                textposition="top center",
                marker=dict(
                    size=8,
                    color=gk_scores,
                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(title="GKスコア"),
                ),
                hovertemplate=(
                    "ID=%{text}<br>"
                    "GKスコア=%{x:.3f}<br>"
                    "リーチ率=%{y:.3f}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="リーチ率 × ゲートキーパースコア（Top-100）",
            xaxis_title="ゲートキーパースコア",
            yaxis_title="リーチ率（reach_fraction）",
            height=480,
        )

        reach_min = min(reach_fracs) if reach_fracs else 0.0
        reach_max = max(reach_fracs) if reach_fracs else 0.0

        # Correlation direction (no causal language)
        if len(gk_scores) > 1 and len(reach_fracs) > 1:
            n = len(gk_scores)
            mean_x = sum(gk_scores) / n
            mean_y = sum(reach_fracs) / n
            cov = sum((x - mean_x) * (y - mean_y)
                      for x, y in zip(gk_scores, reach_fracs)) / n
            corr_dir = "正" if cov > 0 else "負" if cov < 0 else "無相関"
        else:
            corr_dir = "算出不可"

        findings = (
            f"<p>Top-100のリーチ率（reach_fraction）の範囲: "
            f"{reach_min:.3f} ～ {reach_max:.3f}。"
            f"ゲートキーパースコアとリーチ率の共変方向: {corr_dir}。"
            f"散布図は個人IDをラベルとして表示（集計値のみ）。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="リーチ × ゲートキーパースコア パレートフロンティア",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_te_reach", height=480
            ),
            method_note=(
                "reach_fraction: 当該ノードを除去したときにグラフの"
                "連結成分数が増加する割合（到達可能性への影響）。"
                "個人名は表示せず、person_idのみ表示。"
            ),
            section_id="te_reach",
        )

    # ── Section 3: Component z-score box plots ───────────────────────

    def _build_historical_patterns(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        top100 = data.get("top_100", [])
        if not isinstance(top100, list):
            top100 = []

        components = ["betweenness_z", "fe_pct_z", "birank_z", "bridge_z"]
        components_ja = [
            "媒介中心性_z",
            "person_fe_pct_z",
            "BiRank_z",
            "ブリッジ_z",
        ]
        colors = ["#667eea", "#f093fb", "#fda085", "#a0d2db"]

        if not top100:
            findings = (
                "<p>構成要素分布データが利用できません"
                "（trust_entry.top_100）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="ゲートキーパー構成要素分布",
                findings_html=findings,
                method_note=(
                    "各構成要素は全スコア付与対象者に対するz-score。"
                ),
                section_id="te_components",
            )

        fig = go.Figure()
        medians: list[tuple[str, float]] = []
        for comp, comp_ja, color in zip(components, components_ja, colors):
            vals = [_sf(row.get(comp, 0.0)) for row in top100]
            sorted_vals = sorted(vals)
            n = len(sorted_vals)
            med = (
                sorted_vals[n // 2]
                if n % 2 == 1
                else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
            )
            medians.append((comp_ja, med))
            fig.add_trace(
                go.Box(
                    y=vals,
                    name=comp_ja,
                    marker_color=color,
                    boxmean=True,
                    hovertemplate=f"{comp_ja}: %{{y:.3f}}<extra></extra>",
                )
            )

        fig.update_layout(
            title="ゲートキーパー構成要素 z-score 分布（Top-100）",
            yaxis_title="z-score",
            height=420,
        )

        highest_med = max(medians, key=lambda t: t[1]) if medians else ("N/A", 0.0)
        findings = (
            f"<p>Top-100の構成要素z-score分布を4軸で示す。"
            f"中央値が最も高い構成要素: {highest_med[0]}"
            f"（中央値={highest_med[1]:.3f}）。"
            f"箱ひげ図は四分位範囲および外れ値を表示する。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="ゲートキーパー構成要素分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_te_components", height=420
            ),
            method_note=(
                "各構成要素は全スコア付与対象者に対するz-score。"
                "betweenness_z: Brandes媒介中心性z-score。"
                "fe_pct_z: AKM person_fe パーセンタイルのz-score。"
                "birank_z: BiRankスコアのz-score。"
                "bridge_z: ブリッジスコアのz-score。"
                "boxmean=True: 菱形マーカーは平均値を示す。"
            ),
            section_id="te_components",
        )
