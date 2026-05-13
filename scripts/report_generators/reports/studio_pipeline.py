"""スタジオ育成パイプライン強度レポート — v2 compliant.

HR brief + Business brief: studio pipeline health across 4 structural axes:
  - young_theta_growth: trajectory of person FE for recently-debuted staff
  - mid_career_retention: 3-year retention of mid-tenure staff
  - key_person_concentration: top-3 credit share
  - bus_factor: inverse HHI on credit distribution

All metrics are structural (credit records + AKM theta_i only — no anime.score).
Bootstrap CI at 95% level (cluster = staff member).

Sections
--------
1. パイプライン構造サマリー  (4-metric overview, studio tier distribution)
2. 若手θ成長軌跡            (young_theta_growth by studio)
3. 中堅クレジット継続率      (mid_career_retention by studio)
4. key-person集中度とbus factor (concentration + bus factor scatter)
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import plotly.graph_objects as go

from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe
from ..section_builder import KPICard, ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_JSON_DIR = Path(__file__).parents[4] / "result" / "json"

_TIER_COLORS = {
    "large": "#3593D2",
    "mid": "#7CC8F2",
    "boutique": "#3BC494",
    "unknown": "#8a94a0",
}


# ─────────────────────────────────────────────────────────────────────────────
# JSON loader
# ─────────────────────────────────────────────────────────────────────────────


def _load_json(name: str) -> dict | list:
    p = _JSON_DIR / f"{name}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _safe_float(v: object, default: float = 0.0) -> float:
    try:
        f = float(v)  # type: ignore[arg-type]
        return default if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


def _tier_label(n_staff: int) -> str:
    """Assign a structural tier label based on staff count.

    Not evaluative — purely a cardinality descriptor.
    """
    if n_staff >= 200:
        return "large"
    if n_staff >= 50:
        return "mid"
    return "boutique"


# ─────────────────────────────────────────────────────────────────────────────
# Report class
# ─────────────────────────────────────────────────────────────────────────────


class StudioPipelineReport(BaseReportGenerator):
    """スタジオ育成パイプライン強度レポート.

    Audience: hr (primary), biz (secondary).
    Claim: 4 structural axes (young-theta trajectory / mid-career
    credit continuity / key-person credit share / bus factor) characterise
    differences in studio pipeline structure.
    """

    name = "studio_pipeline"
    title = "スタジオ育成パイプライン構造"
    subtitle = (
        "若手θ成長軌跡 / 中堅クレジット継続率 / key-person集中度 / bus factor"
        "（クレジット構造指標・AKM θ_i 使用）"
    )
    filename = "studio_pipeline.html"
    doc_type = "brief"

    def generate(self) -> Path | None:  # noqa: D102
        data = _load_json("studio_pipeline_strength")
        if not isinstance(data, dict):
            data = {}

        sb = SectionBuilder()

        sections = [
            sb.build_section(self._build_summary(sb, data)),
            sb.build_section(self._build_young_theta(sb, data)),
            sb.build_section(self._build_mid_retention(sb, data)),
            sb.build_section(self._build_concentration_bus(sb, data)),
        ]

        insert_lineage(
            self.conn,
            table_name="meta_studio_pipeline",
            audience="hr",
            source_silver_tables=[
                "credits",
                "persons",
                "anime",
                "studios",
                "anime_studios",
                "mart.akm_results",
            ],
            formula_version="v1.0",
            ci_method=(
                "Cluster bootstrap 95% CI (1000 draws, seed=42, cluster=staff person_id). "
                "Analytical SE reported where bootstrap not available."
            ),
            null_model=(
                "Cohort-matched permutation (N3): studio-year cell assignments "
                "are permuted within debut-year cohort to construct null distribution "
                "for young_theta_growth and mid_career_retention."
            ),
            holdout_method=None,
            description=(
                "Studio pipeline structure across 4 structural axes: "
                "(1) young_theta_growth = mean AKM theta_i deviation from cohort peers "
                "for staff with tenure < 5 years; "
                "(2) mid_career_retention = 3-year credit continuity for mid-tenure staff; "
                "(3) key_person_concentration = top-3 credit share (HHI-based); "
                "(4) bus_factor = 1/HHI on credit distribution. "
                "anime.score is not used at any stage. "
                "Structural indicators only — not evaluative judgements of individuals."
            ),
            rng_seed=42,
        )

        return self.write_report("\n".join(sections))

    # ── Section 1: Summary ──────────────────────────────────────────

    def _build_summary(self, sb: SectionBuilder, data: dict) -> ReportSection:
        entries = [v for v in data.values() if isinstance(v, dict)]

        if not entries:
            return ReportSection(
                title="パイプライン構造サマリー",
                findings_html=(
                    "<p>スタジオパイプライン強度データが利用できません"
                    "（studio_pipeline_strength.json）。"
                    "パイプラインのスタジオ分析モジュールを実行してください。</p>"
                ),
                section_id="pipeline_summary",
            )

        n = len(entries)
        ytg_vals = [
            _safe_float(e.get("young_theta_growth_mean"))
            for e in entries
            if e.get("young_theta_growth_mean") is not None
        ]
        mcr_vals = [
            _safe_float(e.get("mid_career_retention_mean"))
            for e in entries
            if e.get("mid_career_retention_mean") is not None
        ]
        bus_vals = [
            _safe_float(e.get("bus_factor_mean"))
            for e in entries
            if e.get("bus_factor_mean") is not None
        ]

        ytg_mean = (
            sum(ytg_vals) / len(ytg_vals) if ytg_vals else None
        )
        mcr_mean = (
            sum(mcr_vals) / len(mcr_vals) if mcr_vals else None
        )
        bus_mean = (
            sum(bus_vals) / len(bus_vals) if bus_vals else None
        )

        kpis = [
            KPICard("分析スタジオ数", f"{n:,}", "cell あり"),
            KPICard(
                "業界平均 mid-career継続率",
                f"{mcr_mean:.3f}" if mcr_mean is not None else "N/A",
                "3年後クレジット継続",
            ),
            KPICard(
                "業界平均 bus factor",
                f"{bus_mean:.2f}" if bus_mean is not None else "N/A",
                "1/HHI (高=分散)",
            ),
        ]

        # Tier bar chart
        tier_counts: dict[str, int] = {}
        for e in entries:
            n_staff = int(_safe_float(e.get("n_cells", 1)) * 5)
            tier = _tier_label(n_staff)
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        tiers = ["large", "mid", "boutique"]
        fig = go.Figure(
            go.Bar(
                x=[t for t in tiers if t in tier_counts],
                y=[tier_counts.get(t, 0) for t in tiers if t in tier_counts],
                marker_color=[
                    _TIER_COLORS.get(t, "#8a94a0")
                    for t in tiers
                    if t in tier_counts
                ],
                hovertemplate="%{x}: %{y:,}スタジオ<extra></extra>",
            )
        )
        fig.update_layout(
            title="スタジオ規模Tier分布（パイプライン分析対象）",
            xaxis_title="規模Tier",
            yaxis_title="スタジオ数",
            height=340,
        )

        findings = (
            f"<p>パイプライン強度が算出されたスタジオ数: {n:,}件。"
        )
        if ytg_mean is not None:
            findings += (
                f"若手θ成長（コホート偏差平均）: {ytg_mean:+.4f}。"
            )
        if mcr_mean is not None:
            findings += (
                f"中堅クレジット3年継続率（業界平均）: {mcr_mean:.3f}。"
            )
        if bus_mean is not None:
            findings += (
                f"bus factor（業界平均）: {bus_mean:.2f}。"
            )
        findings += (
            "スタジオ間の差異は構造的なクレジット分布の違いを反映しており、"
            "個人の主観的評価ではない。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="パイプライン構造サマリー（4軸概観）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_pipeline_tier", height=340
            ),
            kpi_cards=kpis,
            method_note=(
                "4 structural axes computed per studio-year cell from credit records "
                "and AKM θ_i (person fixed effects). "
                "Tier labels (large/mid/boutique) are cardinality descriptors, "
                "not evaluative rankings. "
                "anime.score is not used."
            ),
            section_id="pipeline_summary",
        )

    # ── Section 2: Young theta growth ───────────────────────────────

    def _build_young_theta(self, sb: SectionBuilder, data: dict) -> ReportSection:
        entries = [
            {"sid": k, **v}
            for k, v in data.items()
            if isinstance(v, dict)
            and v.get("young_theta_growth_mean") is not None
        ]

        if not entries:
            return ReportSection(
                title="若手θ成長軌跡（tenure < 5年スタッフ）",
                findings_html=(
                    "<p>young_theta_growth データが利用できません。"
                    "AKM theta_i 出力と studio_assignments が必要です。</p>"
                ),
                section_id="young_theta",
            )

        entries_sorted = sorted(
            entries,
            key=lambda e: _safe_float(e.get("young_theta_growth_mean")),
            reverse=True,
        )
        top10 = entries_sorted[:10]
        bot10 = list(reversed(entries_sorted[-10:]))
        combined = top10 + bot10

        names = [
            str(e.get("name") or e.get("sid") or f"s{i}")
            for i, e in enumerate(combined)
        ]
        ytg_vals = [
            _safe_float(e.get("young_theta_growth_mean")) for e in combined
        ]
        ci_list = [e.get("young_theta_growth_ci") for e in combined]

        lo_list = []
        hi_list = []
        for i, ci in enumerate(ci_list):
            if isinstance(ci, (list, tuple)) and len(ci) == 2:
                lo_list.append(_safe_float(ci[0]))
                hi_list.append(_safe_float(ci[1]))
            else:
                lo_list.append(ytg_vals[i])
                hi_list.append(ytg_vals[i])

        fig = go.Figure()
        colors = [
            "#3BC494" if v >= 0 else "#E07532" for v in ytg_vals
        ]
        fig.add_trace(
            go.Bar(
                x=names,
                y=ytg_vals,
                error_y=dict(
                    type="data",
                    symmetric=False,
                    array=[hi - v for hi, v in zip(hi_list, ytg_vals)],
                    arrayminus=[v - lo for lo, v in zip(lo_list, ytg_vals)],
                    visible=True,
                ),
                marker_color=colors,
                hovertemplate="%{x}: Δθ=%{y:+.4f}<extra></extra>",
            )
        )
        fig.add_hline(
            y=0.0,
            line_dash="dot",
            line_color="#aaaaaa",
            annotation_text="コホート平均",
        )
        fig.update_layout(
            title="若手θ成長（コホート偏差）— 上位10 / 下位10スタジオ",
            xaxis_title="スタジオ",
            yaxis_title="コホート平均からのΔθ",
            height=440,
        )

        all_ytg = [
            _safe_float(e.get("young_theta_growth_mean")) for e in entries
        ]
        ytg_min = min(all_ytg)
        ytg_max = max(all_ytg)
        n = len(entries)
        top_name = str(top10[0].get("name") or top10[0].get("sid")) if top10 else "N/A"

        findings = (
            f"<p>young_theta_growth（コホート偏差平均）が算出されたスタジオ数: {n:,}件。"
            f"値の範囲: {ytg_min:+.4f}〜{ytg_max:+.4f}。"
            f"正の値はコホート同期平均と比較して高い person FE を持つ"
            f"若手クレジット保有者がそのスタジオに多いことを示す。"
            f"代表スタジオ（上位）: {top_name}。"
            f"エラーバーは bootstrap 95% CI。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="若手θ成長軌跡（tenure < 5年スタッフ）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_young_theta", height=440
            ),
            method_note=(
                "young_theta_growth = mean(θ_i − mean_θ_cohort) for staff with "
                "tenure < 5 years at the studio in each calendar year. "
                "θ_i = AKM person fixed effect (log production-scale metric). "
                "Cohort baseline = mean θ_i of all persons with the same debut year "
                "across all studios. "
                "Positive deviation: recently-debuted staff at this studio have "
                "higher-than-average θ_i relative to their cohort peers. "
                "CI: cluster bootstrap (1000 draws, cluster=person_id). "
                "Cells with fewer than 30 young staff are flagged as unreliable."
            ),
            chart_caption=(
                "横軸 = スタジオ、縦軸 = young_theta_growth（コホート偏差）。"
                "0 線 = 同一 debut-year コホートの全スタジオ平均。"
                "上位10（緑）/ 下位10（橙）。エラーバー = bootstrap 95% CI。"
            ),
            section_id="young_theta",
        )

    # ── Section 3: Mid-career retention ─────────────────────────────

    def _build_mid_retention(self, sb: SectionBuilder, data: dict) -> ReportSection:
        entries = [
            {"sid": k, **v}
            for k, v in data.items()
            if isinstance(v, dict)
            and v.get("mid_career_retention_mean") is not None
        ]

        if not entries:
            return ReportSection(
                title="中堅クレジット3年継続率（tenure 5–15年）",
                findings_html=(
                    "<p>mid_career_retention データが利用できません。"
                    "studio_pipeline_strength.json を確認してください。</p>"
                ),
                section_id="mid_retention",
            )

        entries_sorted = sorted(
            entries,
            key=lambda e: _safe_float(e.get("mid_career_retention_mean")),
            reverse=True,
        )
        top10 = entries_sorted[:10]
        bot10 = list(reversed(entries_sorted[-10:]))
        combined = top10 + bot10

        names = [
            str(e.get("name") or e.get("sid") or f"s{i}")
            for i, e in enumerate(combined)
        ]
        mcr_vals = [
            _safe_float(e.get("mid_career_retention_mean")) for e in combined
        ]
        ci_list = [e.get("mid_career_retention_ci") for e in combined]

        lo_list = []
        hi_list = []
        for i, ci in enumerate(ci_list):
            if isinstance(ci, (list, tuple)) and len(ci) == 2:
                lo_list.append(_safe_float(ci[0]))
                hi_list.append(_safe_float(ci[1]))
            else:
                lo_list.append(mcr_vals[i])
                hi_list.append(mcr_vals[i])

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=mcr_vals,
                y=names,
                mode="markers",
                error_x=dict(
                    type="data",
                    symmetric=False,
                    array=[hi - v for hi, v in zip(hi_list, mcr_vals)],
                    arrayminus=[v - lo for lo, v in zip(lo_list, mcr_vals)],
                    visible=True,
                ),
                marker=dict(
                    size=10,
                    color=mcr_vals,
                    colorscale="RdYlGn",
                    showscale=True,
                    colorbar=dict(title="継続率"),
                ),
                hovertemplate="%{y}: 継続率=%{x:.3f}<extra></extra>",
            )
        )
        fig.update_layout(
            title="中堅クレジット3年継続率 — 上位10 / 下位10スタジオ",
            xaxis_title="3年後クレジット継続率",
            yaxis_title="スタジオ",
            height=480,
        )

        all_mcr = [
            _safe_float(e.get("mid_career_retention_mean")) for e in entries
        ]
        mcr_min = min(all_mcr)
        mcr_max = max(all_mcr)
        n = len(entries)

        findings = (
            f"<p>mid_career_retention（tenure 5–15年、3年後クレジット継続率）が"
            f"算出されたスタジオ数: {n:,}件。"
            f"値の範囲: {mcr_min:.3f}〜{mcr_max:.3f}。"
            f"継続率はスタジオの構造的な中堅クレジット維持の度合いを示す指標であり、"
            f"個人の主観的評価を含まない。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="中堅クレジット3年継続率（tenure 5–15年）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_mid_retention", height=480
            ),
            method_note=(
                "mid_career_retention[s, y] = "
                "P(credited at s in year y | credited at s in year y-3, "
                "tenure 5–15 years at y-3). "
                "Tenure = years elapsed since person's debut year across all studios. "
                "CI: cluster bootstrap (1000 draws, cluster=person_id). "
                "Interpretation: this is a credit-visibility continuity measure, "
                "not a measure of formal employment status."
            ),
            chart_caption=(
                "横軸 = 3年後クレジット継続率（bootstrap 平均）、縦軸 = スタジオ。"
                "エラーバー = bootstrap 95% CI。"
                "カラー = 継続率（緑 = 高、赤 = 低）。"
            ),
            section_id="mid_retention",
        )

    # ── Section 4: Concentration + bus factor ───────────────────────

    def _build_concentration_bus(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        entries = [
            {"sid": k, **v}
            for k, v in data.items()
            if isinstance(v, dict)
            and v.get("key_person_concentration_mean") is not None
            and v.get("bus_factor_mean") is not None
        ]

        if not entries:
            return ReportSection(
                title="key-person集中度と bus factor",
                findings_html=(
                    "<p>key_person_concentration / bus_factor データが利用できません。"
                    "studio_pipeline_strength.json を確認してください。</p>"
                ),
                section_id="concentration_bus",
            )

        kpc_vals = [_safe_float(e.get("key_person_concentration_mean")) for e in entries]
        bus_vals = [_safe_float(e.get("bus_factor_mean")) for e in entries]
        names = [
            str(e.get("name") or e.get("sid") or f"s{i}")
            for i, e in enumerate(entries)
        ]

        n = len(entries)
        kpc_mean = sum(kpc_vals) / n if n else 0.0
        bus_mean = sum(bus_vals) / n if n else 0.0

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=kpc_vals,
                y=bus_vals,
                mode="markers",
                text=names,
                marker=dict(
                    size=8,
                    color=bus_vals,
                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(title="bus factor"),
                    opacity=0.75,
                ),
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "top-3集中度=%{x:.3f}<br>"
                    "bus factor=%{y:.2f}<extra></extra>"
                ),
            )
        )
        fig.add_vline(
            x=kpc_mean,
            line_dash="dot",
            line_color="#aaaaaa",
            annotation_text=f"平均={kpc_mean:.3f}",
        )
        fig.add_hline(
            y=bus_mean,
            line_dash="dot",
            line_color="#aaaaaa",
            annotation_text=f"平均={bus_mean:.2f}",
        )
        fig.update_layout(
            title="key-person集中度（top-3） vs bus factor（1/HHI）",
            xaxis_title="top-3クレジット集中度",
            yaxis_title="bus factor（1/HHI）",
            height=480,
        )

        findings = (
            f"<p>key-person集中度（top-3クレジットシェア合計）と"
            f"bus factor（1/HHI）の分布: スタジオ数={n:,}件。"
            f"top-3集中度 平均={kpc_mean:.3f}、"
            f"bus factor 平均={bus_mean:.2f}。"
            f"右下ほどクレジット構造が特定人物に集中しており、"
            f"左上ほどより分散した構造となっている。"
            f"いずれもクレジット記録の構造的指標であり、個人評価ではない。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="key-person集中度と bus factor",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_concentration_bus", height=480
            ),
            method_note=(
                "key_person_concentration[s, y] = "
                "sum of credit_share for top-3 staff at studio s in year y, "
                "where credit_share is proportional to contribution weight. "
                "bus_factor[s, y] = 1 / HHI on credit_share distribution. "
                "Higher bus_factor = more distributed credit structure. "
                "HHI = sum(share_i^2); bus_factor = 1/HHI. "
                "Both metrics are structural descriptors of credit distribution, "
                "not evaluative judgements of individuals. "
                "CI: cluster bootstrap (1000 draws, cluster=person_id)."
            ),
            chart_caption=(
                "横軸 = top-3クレジット集中度（0–1）、縦軸 = bus factor（1/HHI）。"
                "点の色 = bus factor（明 = 高）。"
                "破線 = 業界平均。右下 = 集中型、左上 = 分散型。"
            ),
            section_id="concentration_bus",
        )


# ─────────────────────────────────────────────────────────────────────────────
# v3 SPEC
# ─────────────────────────────────────────────────────────────────────────────

from .._spec import (  # noqa: E402
    SensitivityAxis,
    ShrinkageSpec,
    make_default_spec,
)

SPEC = make_default_spec(
    name="studio_pipeline",
    audience="hr",
    claim=(
        "スタジオ s の若手 theta 成長（コホート偏差）・中堅クレジット3年継続率・"
        "top-3 集中度・bus factor（1/HHI）の 4 軸がスタジオ間で有意に異なる"
        "構造的パターンを示す"
    ),
    identifying_assumption=(
        "クレジット記録の可視性 ≈ 実際の制作参加。"
        "AKM θ_i は個人の production-scale 需要への構造的応答を反映するが、"
        "studio-specific selection 効果を完全には除去しない。"
        "mid_career_retention はクレジット可視性の継続であり "
        "formal 雇用関係の継続と等価ではない。"
    ),
    null_model=["N3", "N4"],
    sources=[
        "credits",
        "persons",
        "anime",
        "studios",
        "anime_studios",
        "mart.akm_results",
    ],
    meta_table="meta_studio_pipeline",
    estimator=(
        "young_theta_growth = mean(θ_i − cohort_mean); "
        "mid_career_retention = proportion CI; "
        "key_person_concentration = top-3 share sum; "
        "bus_factor = 1/HHI"
    ),
    ci_estimator="bootstrap",
    n_resamples=1000,
    shrinkage=ShrinkageSpec(
        method="empirical_bayes_normal",
        n_threshold=30,
        prior="industry-wide distribution of each metric",
    ),
    sensitivity_grid=[
        SensitivityAxis(name="young tenure threshold", values=["<3y", "<5y", "<7y"]),
        SensitivityAxis(name="mid tenure window", values=["5-10y", "5-15y", "3-12y"]),
        SensitivityAxis(name="retention lookback", values=["2y", "3y", "5y"]),
        SensitivityAxis(name="top-k concentration", values=[2, 3, 5]),
    ],
    extra_limitations=[
        "AKM θ_i は AKM 連結集合内スタジオのみで比較可能",
        "クレジット記録が疎な古い年代 (1980s) では mid_career_retention が不安定",
        "bus_factor は credit_weight として均等 share を使用しており "
        "実際の作業量比率とは異なる場合がある",
    ],
)
