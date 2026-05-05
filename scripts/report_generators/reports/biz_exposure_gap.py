"""露出機会ギャップ人材プール分析 — v2 / v3 compliant.

v3 rename (2026-05-05): file biz_undervalued_talent.py → biz_exposure_gap.py、
class BizUndervaluedTalentReport → BizExposureGapReport、
report name biz_undervalued_talent → biz_exposure_gap、
filename biz_undervalued_talent.html → biz_exposure_gap.html。

Narrow label per REPORT_INVENTORY §Vocabulary concerns and
REPORT_DESIGN_v3.md §7 example C: removes evaluation framing
("過小評価", "タレント") in favor of structural label
("露出機会ギャップ人材プール" = high-θ_i / low-exposure pair candidates).

A backward-compatibility alias ``BizUndervaluedTalentReport`` is kept
for one release so existing V2_REPORT_CLASSES registries do not break.
The DB / Hamilton ``meta_biz_undervalued`` table name is unchanged
(rename requires a schema migration handled in a follow-up PR).
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

_THRESHOLD_UP = 30.0


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


class BizExposureGapReport(BaseReportGenerator):
    name = "biz_exposure_gap"
    title = "露出機会ギャップ人材プール"
    subtitle = "U_pスコア分布 / K=5アーキタイプ / 復帰兆候"
    filename = "biz_exposure_gap.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load("exposure_gap")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(self._build_undervalued_distribution(sb, data)),
            sb.build_section(self._build_archetypes(sb, data)),
            sb.build_section(self._build_recovery_signals(sb, data)),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: U_p score distribution ───────────────────────────

    def _build_undervalued_distribution(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        n_total = int(data.get("n_total_scored", 0))
        n_structural = int(data.get("n_structural_undervalued", 0))
        structural_rate = _sf(data.get("structural_rate", 0.0))
        dist = data.get("u_score_distribution", {})
        if not isinstance(dist, dict):
            dist = {}

        p25 = _sf(dist.get("p25", 0.0))
        p50 = _sf(dist.get("p50", 0.0))
        p75 = _sf(dist.get("p75", 0.0))

        if not data:
            findings = (
                "<p>exposure_gap.json が存在しないか空です。"
                "U_pスコア分布データが取得できません。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="U_pスコア分布（露出機会ギャップの閾値）",
                findings_html=findings,
                method_note=(
                    "U_p = percentile(person_fe) - percentile(total_credits),"
                    " 構造的閾値=30pt"
                ),
                section_id="up_distribution",
            )

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=["p25", "p50", "p75"],
                y=[p25, p50, p75],
                marker_color=["#3593D2", "#E09BC2", "#FFB444"],
                name="U_pパーセンタイル値",
                hovertemplate="%{x}: %{y:.1f}<extra></extra>",
            )
        )
        # Threshold line at 30
        fig.add_trace(
            go.Scatter(
                x=["p25", "p50", "p75"],
                y=[_THRESHOLD_UP] * 3,
                mode="lines",
                name=f"構造的閾値={_THRESHOLD_UP:.0f}pt",
                line=dict(color="#e05080", dash="dash", width=2),
                hovertemplate=f"閾値={_THRESHOLD_UP:.0f}<extra></extra>",
            )
        )
        fig.update_layout(
            title="U_pスコア 四分位値と露出機会ギャップ閾値",
            xaxis_title="パーセンタイル",
            yaxis_title="U_pスコア",
            height=420,
        )

        rate_str = f"{structural_rate * 100:.1f}" if structural_rate <= 1.0 else (
            f"{structural_rate:.1f}"
        )
        findings = (
            f"<p>スコア付与対象: {n_total:,}名。"
            f"露出機会ギャップ（U_p ≥ {_THRESHOLD_UP:.0f}）: {n_structural:,}名"
            f"（全体の{rate_str}%）。"
            f"U_pスコア分布: p25={p25:.1f}, p50={p50:.1f}, p75={p75:.1f}。"
            f"点線は構造的閾値={_THRESHOLD_UP:.0f}ptを示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="U_pスコア分布（露出機会ギャップの閾値）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_up_dist", height=420
            ),
            method_note=(
                "U_p = percentile(person_fe) - percentile(total_credits),"
                " 構造的閾値=30pt。"
                "person_fe はAKM分解による個人固定効果のパーセンタイル順位。"
                "total_credits は累積クレジット数のパーセンタイル順位。"
                "閾値の30ptは固定の事前設定値であり、データ駆動ではない。"
            ),
            section_id="up_distribution",
        )

    # ── Section 2: K=5 archetypes ───────────────────────────────────

    def _build_archetypes(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        archetypes = data.get("archetypes", {})
        if not isinstance(archetypes, dict):
            archetypes = {}

        if not archetypes:
            findings = (
                "<p>アーキタイプデータが利用できません"
                "（exposure_gap.archetypes）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="K=5アーキタイプ別集計",
                findings_html=findings,
                method_note="K-means K=5 on (person_fe_pct, activity_pct, dormancy)",
                section_id="up_archetypes",
            )

        # Sort by avg_u_score ascending for chart
        sorted_arcs = sorted(
            archetypes.items(),
            key=lambda kv: _sf(kv[1].get("avg_u_score", 0.0)),
        )
        names = [kv[0] for kv in sorted_arcs]
        counts = [int(kv[1].get("count", 0)) for kv in sorted_arcs]
        u_scores = [_sf(kv[1].get("avg_u_score", 0.0)) for kv in sorted_arcs]

        colors = ["#3593D2", "#E09BC2", "#E09BC2", "#E07532", "#FFB444"]
        bar_colors = [colors[i % len(colors)] for i in range(len(names))]

        fig = go.Figure(
            go.Bar(
                x=counts,
                y=names,
                orientation="h",
                marker_color=bar_colors,
                text=[f"avg_U_p={u:.1f}" for u in u_scores],
                textposition="outside",
                hovertemplate="%{y}: count=%{x}<extra></extra>",
            )
        )
        fig.update_layout(
            title="K=5アーキタイプ別 人数（avg_U_pスコア付）",
            xaxis_title="人数",
            yaxis_title="アーキタイプ",
            height=420,
            margin=dict(l=180, r=120),
        )

        u_vals = [_sf(v.get("avg_u_score", 0.0)) for v in archetypes.values()]
        u_min = min(u_vals) if u_vals else 0.0
        u_max = max(u_vals) if u_vals else 0.0
        arc_list = ", ".join(names)

        findings = (
            f"<p>アーキタイプ数: {len(archetypes):,}。"
            f"アーキタイプ名: {arc_list}。"
            f"avg_U_pスコアの範囲: {u_min:.1f} ～ {u_max:.1f}。"
            f"バーはアーキタイプ別の人数を示し、avg_U_pスコアの昇順で並べた。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="K=5アーキタイプ別集計",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_up_archetypes", height=420
            ),
            method_note=(
                "K-means K=5 on (person_fe_pct, activity_pct, dormancy,"
                " avg_fe_pct, avg_u_score)。"
                "アーキタイプ名はセントロイドの相対順位による命名（固定閾値なし）。"
            ),
            section_id="up_archetypes",
        )

    # ── Section 3: Recovery signals ─────────────────────────────────

    def _build_recovery_signals(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        recovery_count = int(data.get("recovery_signal_count", 0))
        n_structural = int(data.get("n_structural_undervalued", 0))

        ratio = recovery_count / n_structural if n_structural > 0 else 0.0

        if not data:
            findings = (
                "<p>復帰兆候データが利用できません"
                "（exposure_gap.recovery_signal_count）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="復帰兆候（直近クレジット保有者）",
                findings_html=findings,
                method_note=(
                    "復帰兆候: U_p >= 30 かつ fe_pct >= 60 かつ recent_credits > 0"
                ),
                section_id="up_recovery",
            )

        fig = go.Figure(
            go.Indicator(
                mode="gauge+number+delta",
                value=recovery_count,
                title={"text": "復帰兆候保有者数（露出機会ギャッププール内）"},
                gauge={
                    "axis": {"range": [0, max(n_structural, 1)]},
                    "bar": {"color": "#E09BC2"},
                    "steps": [
                        {"range": [0, n_structural * 0.25], "color": "#2a2a4a"},
                        {"range": [n_structural * 0.25, n_structural * 0.6],
                         "color": "#3a3a6a"},
                        {"range": [n_structural * 0.6, n_structural],
                         "color": "#4a4a8a"},
                    ],
                    "threshold": {
                        "line": {"color": "#FFB444", "width": 3},
                        "thickness": 0.75,
                        "value": n_structural * 0.5,
                    },
                },
                delta={
                    "reference": n_structural * 0.25,
                    "relative": False,
                },
            )
        )
        fig.update_layout(height=420)

        findings = (
            f"<p>露出機会ギャッププール: {n_structural:,}名。"
            f"復帰兆候保有者（U_p ≥ 30 かつ fe_pct ≥ 60 かつ"
            f" recent_credits &gt; 0）: {recovery_count:,}名"
            f"（プール比={ratio * 100:.1f}%）。"
            f"本集計は集計値のみを示し、個人の特定はしない。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="復帰兆候（直近クレジット保有者）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_up_recovery", height=420
            ),
            method_note=(
                "復帰兆候: U_p >= 30 かつ fe_pct >= 60 かつ recent_credits > 0。"
                "recent_credits は直近2年以内のクレジット件数。"
                "個人識別情報は表示しない（集計値のみ）。"
            ),
            section_id="up_recovery",
        )


# ─────────────────────────────────────────────────────────────────
# v3 rename backward-compatibility alias.
# Existing V2_REPORT_CLASSES registries and external callers may still
# reference ``BizUndervaluedTalentReport``; this alias keeps them
# working until the next major release.
# ─────────────────────────────────────────────────────────────────
BizUndervaluedTalentReport = BizExposureGapReport


# v3 curated SPEC — REPORT_DESIGN_v3 §7 example C.
from .._spec import (  # noqa: E402
    SensitivityAxis, ShrinkageSpec, make_default_spec,
)

SPEC = make_default_spec(
    name='biz_exposure_gap',
    audience='biz',
    claim=(
        'theta_i (構造スコア) パーセンタイルが total_credits パーセンタイルを '
        '30pt 以上上回る人物 (U_p ≥ 30) が、null product 分布より高密度で存在する'
    ),
    identifying_assumption=(
        '露出 = 主要スタジオ × メイン役職クレジット を仮定。'
        '別経路 (sakuga / SNS / 海外下請け / 配信オリジナル) の露出は'
        '露出ゼロとして扱われる — 実質露出は過小推定される可能性。'
    ),
    null_model=['N4', 'N7'],  # role-matched bootstrap + activity baseline
    sources=['credits', 'persons', 'anime', 'studios'],
    meta_table='meta_biz_undervalued',  # DB schema は未 rename (後続 PR)
    estimator='U_p = pct(theta_i) - pct(total_credits); 閾値 30pt',
    ci_estimator='bootstrap',
    n_resamples=1000,
    shrinkage=ShrinkageSpec(
        method='empirical_bayes_beta',
        n_threshold=30,
        prior='global U_p distribution (Beta fit)',
    ),
    sensitivity_grid=[
        SensitivityAxis(name='露出定義',
                        values=['mainstream studio', '+sakuga 引用', '全クレジット']),
        SensitivityAxis(name='theta_i 閾値',
                        values=['P75', 'P90', 'P95']),
        SensitivityAxis(name='U_p threshold',
                        values=[20, 30, 40]),
    ],
    extra_limitations=[
        '個人提示は theta_i CI 区間幅 < 1.0 のもののみ',
        '海外下請け露出は捕捉できない (国別 anime_studios 不完全)',
        'サンプル小 (n<30) は EB 縮小で中央方向に補正、生スコアと順位差圧縮',
    ],
    forbidden_framing=['過小評価', '発掘', '原石', '隠れた才能', '掘り出し物'],
    required_alternatives=2,
)
