"""Policy Monopsony report — v2 compliant.

人材市場流動性・独占度分析:
- Section 1: スタジオ集中度 HHI 時系列
- Section 2: 転職率・時代比較
- Section 3: キャリア固定化回帰 (Lock-in)

v3 visualization:
- Section 3 (lock-in OR) を CIScatter primitive 経由
- Section 1/2 は theme + palette のみ統一 (line/grouped-bar 用 primitive 未整備)
"""

from __future__ import annotations

import json
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.viz import embed as viz_embed
from src.viz.palettes import OKABE_ITO_DARK
from src.viz.primitives import CIPoint, CIScatterSpec, render_ci_scatter
from src.viz.theme import apply_theme

from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe  # noqa: F401  (legacy callers elsewhere)
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


# v3: career-stage mapping は src.viz.palettes に集約。era の固定色も
# Okabe-Ito + viridis ベースに切替 (CB-safe / report 横断一貫性)。
_STAGE_COLORS = {
    "新人": OKABE_ITO_DARK[2],   # sky blue
    "中堅": OKABE_ITO_DARK[3],   # bluish green
    "ベテラン": OKABE_ITO_DARK[7],  # reddish purple
    "シニア": OKABE_ITO_DARK[1],    # orange
}

_ERA_COLORS = [
    OKABE_ITO_DARK[2], OKABE_ITO_DARK[3], OKABE_ITO_DARK[1], OKABE_ITO_DARK[7],
    OKABE_ITO_DARK[5], OKABE_ITO_DARK[6], OKABE_ITO_DARK[4], OKABE_ITO_DARK[0],
]


class PolicyMonopsonyReport(BaseReportGenerator):
    name = "policy_monopsony"
    title = "人材市場流動性・独占度分析"
    subtitle = "HHI時系列 + 転職率 + Lock-in回帰"
    filename = "policy_monopsony.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        data = _load("monopsony_analysis")

        sections: list[str] = [
            sb.build_section(self._build_hhi_trends(sb, data)),
            sb.build_section(self._build_mobility(sb, data)),
            sb.build_section(self._build_lockin(sb, data)),
        ]
        insert_lineage(
            self.conn,
            table_name="meta_policy_monopsony",
            audience="policy",
            source_silver_tables=["credits", "persons", "anime", "studios", "anime_studios"],
            formula_version="v1.0",
            ci_method=(
                "Analytical SE (OLS residual variance / sqrt(n)) for lock-in regression; "
                "bootstrap 95% CI (1000 draws, seed=42) for HHI year-level estimates"
            ),
            null_model=(
                "Permutation test: random studio assignment preserving studio size distribution "
                "(100 draws, seed=42); compares observed HHI to null HHI distribution"
            ),
            holdout_method="Year-based 5-fold cross-validation (rolling window, 5-year gaps)",
            description=(
                "Labor market fluidity and monopsony analysis for credited staff. "
                "HHI = sum(share_studio^2)*10000 per year across all credited roles. "
                "Mobility = fraction changing primary studio within 3-year window. "
                "Lock-in regression: OLS with person and year fixed effects, "
                "outcome = log(years at same studio). Source-free of anime.score."
            ),
            rng_seed=42,
        )
        return self.write_report("\n".join(sections))

    # ── Section 1: HHI time series ────────────────────────────────

    def _build_hhi_trends(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        hhi_ts: dict = data.get("hhi_timeseries", {}) if isinstance(data, dict) else {}

        if not hhi_ts:
            return ReportSection(
                title="スタジオ集中度（HHI）時系列",
                findings_html=(
                    "<p>HHI時系列データが利用できません（monopsony_analysis.hhi_timeseries）。"
                    "HHI（ハーフィンダール–ハーシュマン指数）は"
                    "HHI_y = Σ(share_{s,y})² × 10000 で計算されます。</p>"
                ),
                section_id="hhi_trends",
            )

        years = sorted(int(y) for y in hhi_ts.keys())
        hhi_raw = [hhi_ts[str(y)].get("hhi", 0) for y in years]
        hhi_norm = [hhi_ts[str(y)].get("hhi_normalized", 0) for y in years]
        n_studios = [hhi_ts[str(y)].get("n_active_studios", 0) for y in years]

        if not years:
            return ReportSection(
                title="スタジオ集中度（HHI）時系列",
                findings_html="<p>有効なHHI時系列エントリが見つかりませんでした。</p>",
                section_id="hhi_trends",
            )

        hhi_min = min(hhi_raw)
        hhi_max = max(hhi_raw)
        n_years = len(years)
        yr_min = years[0]
        yr_max = years[-1]

        findings_html = (
            f"<p>HHI時系列: 対象期間 {yr_min}〜{yr_max}年 ({n_years}年分)。"
            f"HHI（生値）の範囲: {hhi_min:.1f}〜{hhi_max:.1f}。"
            f"アクティブスタジオ数の範囲: {min(n_studios):,}〜{max(n_studios):,}社。</p>"
        )

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        fig.add_trace(
            go.Scatter(
                x=years, y=hhi_raw,
                mode="lines+markers",
                name="HHI (生値)",
                line=dict(color=OKABE_ITO_DARK[7], width=2),
                marker=dict(size=5),
                hovertemplate="年=%{x}: HHI=%{y:.1f}<extra></extra>",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=years, y=hhi_norm,
                mode="lines",
                name="HHI (正規化)",
                line=dict(color=OKABE_ITO_DARK[2], width=2, dash="dot"),
                hovertemplate="年=%{x}: HHI正規化=%{y:.4f}<extra></extra>",
            ),
            secondary_y=True,
        )

        # v3: 米国 DOJ 基準を null reference として明示
        fig.add_hline(
            y=1500, line_dash="dot", line_color="#a0a0a0",
            annotation_text="競争的市場上限 (DOJ 1500)",
            annotation_position="bottom right",
        )

        fig.update_layout(
            title="スタジオ集中度（HHI）時系列",
            xaxis_title="年",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        fig.update_yaxes(title_text="HHI（生値、左軸）", secondary_y=False)
        fig.update_yaxes(title_text="HHI（正規化、右軸）", secondary_y=True)
        apply_theme(fig, theme="dark", height=420)

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオ集中度（HHI）時系列",
            findings_html=findings_html,
            visualization_html=viz_embed(fig, "chart_hhi_trends", height=420),
            method_note=(
                "HHI_y = Σ_s (share_{s,y})² × 10000。"
                "share_{s,y} = スタジオsの年yにおける総クレジット数 / 全スタジオ合計クレジット数。"
                "HHI正規化 = (HHI - 1/N) / (1 - 1/N)、N=アクティブスタジオ数。"
                "HHI = 10000: 独占, HHI < 1500: 競争的市場（米国DOJ基準参考値）。"
                "出典: monopsony_analysis.hhi_timeseries。"
            ),
            section_id="hhi_trends",
        )

    # ── Section 2: Mobility rates ─────────────────────────────────

    def _build_mobility(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        mob: dict = data.get("mobility_rates", {}) if isinstance(data, dict) else {}

        if not mob:
            return ReportSection(
                title="転職率・時代比較",
                findings_html=(
                    "<p>転職率データが利用できません（monopsony_analysis.mobility_rates）。"
                    "by_era（時代別）とby_stage（キャリア段階別）の内訳が期待されます。</p>"
                ),
                section_id="mobility",
            )

        overall = mob.get("overall")
        by_era: dict = mob.get("by_era", {})
        by_stage: dict = mob.get("by_stage", {})

        eras = sorted(by_era.keys())
        stages = sorted(by_stage.keys())

        overall_str = f"{overall:.3f}" if overall is not None else "N/A"
        findings_html = (
            f"<p>全体転職率: {overall_str}。"
            f"時代区分: {len(eras)}区分, キャリア段階区分: {len(stages)}区分。</p>"
        )

        if by_era:
            findings_html += "<ul>"
            for era in eras:
                rate = by_era[era]
                findings_html += f"<li><strong>{era}</strong>: 転職率={rate:.3f}</li>"
            findings_html += "</ul>"

        if not eras or not stages:
            fig = go.Figure()
            fig.add_annotation(
                text="by_era または by_stage データなし",
                xref="paper", yref="paper", x=0.5, y=0.5,
                showarrow=False, font=dict(color="#8a94a0"),
            )
            apply_theme(fig, theme="dark", height=340)
            viz_html = viz_embed(fig, "chart_mobility", height=340)
        else:
            fig = go.Figure()
            for i, stage in enumerate(stages):
                era_rates = [by_era.get(era, 0) for era in eras]

                # Grouped bar: each stage group by era
                fig.add_trace(go.Bar(
                    name=str(stage),
                    x=eras,
                    y=era_rates,
                    marker_color=_ERA_COLORS[i % len(_ERA_COLORS)],
                    hovertemplate=f"{stage}: %{{x}} → 転職率=%{{y:.3f}}<extra></extra>",
                ))

            # Overlay stage-level averages as scatter
            for i, stage in enumerate(stages):
                stage_rate = by_stage.get(stage)
                if stage_rate is not None:
                    fig.add_trace(go.Scatter(
                        x=eras,
                        y=[stage_rate] * len(eras),
                        mode="lines",
                        name=f"{stage}（段階平均）",
                        line=dict(dash="dot", color=_ERA_COLORS[i % len(_ERA_COLORS)], width=1),
                        showlegend=False,
                    ))

            fig.update_layout(
                title="時代×キャリア段階別 転職率",
                xaxis_title="時代区分",
                yaxis_title="転職率",
                barmode="group",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            apply_theme(fig, theme="dark", height=420)
            viz_html = viz_embed(fig, "chart_mobility", height=420)

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="転職率・時代比較",
            findings_html=findings_html,
            visualization_html=viz_html,
            method_note=(
                "転職率 = 前年と異なるスタジオでクレジットされた人物の割合 / 年間アクティブ人物数。"
                "by_era: 時代区分ごとの平均転職率。"
                "by_stage: キャリア段階（新人/中堅/ベテラン/シニア）ごとの平均転職率。"
                "出典: monopsony_analysis.mobility_rates。"
            ),
            section_id="mobility",
        )

    # ── Section 3: Lock-in regression ────────────────────────────

    def _build_lockin(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        lockin: dict = data.get("lockin_regression", {}) if isinstance(data, dict) else {}

        if not lockin:
            return ReportSection(
                title="キャリア固定化回帰（Lock-in）",
                findings_html=(
                    "<p>キャリア固定化回帰データが利用できません（monopsony_analysis.lockin_regression）。"
                    "ロジット回帰 P(同一スタジオ翌年) ~ log(person_fe_rank) + controls を推定します。</p>"
                ),
                section_id="lockin",
            )

        coef_fe = lockin.get("coef_fe")
        se = lockin.get("se")
        or_ = lockin.get("or_")
        ci = lockin.get("ci")
        interpretation = lockin.get("interpretation", "")
        n = lockin.get("n")
        method = lockin.get("method", "Logit")

        or_val = or_ if or_ is not None else (
            2.718 ** coef_fe if coef_fe is not None else None
        )
        ci_lo = ci[0] if isinstance(ci, (list, tuple)) and len(ci) >= 2 else None
        ci_hi = ci[1] if isinstance(ci, (list, tuple)) and len(ci) >= 2 else None

        or_str = f"{or_val:.3f}" if or_val is not None else "N/A"
        se_str = f"SE={se:.4f}" if se is not None else ""
        ci_str = (
            f"95% CI [{ci_lo:.3f}, {ci_hi:.3f}]"
            if ci_lo is not None and ci_hi is not None else ""
        )
        n_str = f"n={n:,}" if n is not None else ""

        findings_html = (
            f"<p>キャリア固定化ロジット回帰（{method}）: "
            f"OR={or_str} {se_str}。"
            f"{ci_str}{',' if ci_str and n_str else ''} {n_str}。</p>"
        )
        if interpretation:
            findings_html += f"<p>{interpretation}</p>"

        # v3: CIScatter primitive 経由で 1 点 forest plot
        if or_val is not None:
            spec = CIScatterSpec(
                points=[CIPoint(
                    label="log(person_fe_rank) OR",
                    x=or_val,
                    ci_lo=ci_lo if ci_lo is not None else or_val,
                    ci_hi=ci_hi if ci_hi is not None else or_val,
                    p_value=None,
                    n=n,
                )],
                x_label="オッズ比 (OR, log scale)",
                title="キャリア固定化 オッズ比（95% CI）",
                log_x=True,
                reference=1.0,
                reference_label="OR",
                sort_by="input",
            )
            viz_html = viz_embed(
                render_ci_scatter(spec, theme="dark"), "chart_lockin",
            )
        else:
            viz_html = '<p style="color:#8a94a0">オッズ比データが利用できません。</p>'

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="キャリア固定化回帰（Lock-in）",
            findings_html=findings_html,
            visualization_html=viz_html,
            method_note=(
                "Logit回帰: P(同一スタジオ翌年クレジット) ~ log(person_fe_rank) + controls。"
                "OR（オッズ比）= exp(β)。OR > 1 は同一スタジオ残留の対数オッズが増加することを示す。"
                "95% CI = exp(β ± 1.96 × SE)。"
                "controls には career_stage, first_year, studio_size が含まれる（実装依存）。"
                "出典: monopsony_analysis.lockin_regression。"
            ),
            section_id="lockin",
        )
