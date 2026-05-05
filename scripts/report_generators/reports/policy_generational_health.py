"""Policy Generational Health report — v2 compliant.

世代交代健全性指標:
- Section 1: デビュー年代別生存率 (S5/S10/S15)
- Section 2: キャリア年数ピラミッド（積み上げ面グラフ）
- Section 3: 人材フロー会計（入退場・net flow）
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


_BIN_COLORS = {
    "bin_0_5": "#7CC8F2",    # 0-5年: 新人
    "bin_5_15": "#3593D2",   # 5-15年: 中堅
    "bin_15_plus": "#E09BC2", # 15年+: ベテラン/シニア
}

_SURVIVAL_COLORS = {
    "S5": "#3BC494",
    "S10": "#F8EC6A",
    "S15": "#E07532",
    "S20": "#FFB444",
}


class PolicyGenerationalHealthReport(BaseReportGenerator):
    name = "policy_generational_health"
    title = "世代交代健全性指標"
    subtitle = "コホート生存率 / 人材ピラミッド / フロー会計"
    filename = "policy_generational_health.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        data = _load("generational_health")

        sections: list[str] = [
            sb.build_section(self._build_cohort_survival(sb, data)),
            sb.build_section(self._build_generation_pyramid(sb, data)),
            sb.build_section(self._build_flow_accounting(sb, data)),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: Cohort survival S5/S10/S15 ────────────────────

    def _build_cohort_survival(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        cohort_surv: dict = data.get("cohort_survival", {}) if isinstance(data, dict) else {}

        if not cohort_surv:
            return ReportSection(
                title="デビュー年代別生存率（S5/S10/S15）",
                findings_html=(
                    "<p>コホート生存率データが利用できません（generational_health.cohort_survival）。"
                    "S(k) = デビューからk年後もアクティブ（クレジット記録あり）な人物の割合。</p>"
                ),
                method_note="S(k) = k年後もアクティブ（クレジット記録あり）の割合",
                section_id="cohort_survival",
            )

        decades = sorted(cohort_surv.keys())
        surv_keys = ("S5", "S10", "S15", "S20")

        # Build S5 range for findings
        s5_vals = [
            cohort_surv[d].get("S5") for d in decades
            if isinstance(cohort_surv[d], dict) and cohort_surv[d].get("S5") is not None
        ]
        s5_range_str = (
            f"{min(s5_vals):.3f}〜{max(s5_vals):.3f}" if s5_vals else "N/A"
        )

        findings_html = (
            f"<p>コホート別生存率（デビュー年代: {len(decades)}区分）。"
            f"S5（5年生存率）の範囲: {s5_range_str}。</p>"
            f"<ul>"
        )
        for decade in decades:
            entry = cohort_surv[decade]
            if not isinstance(entry, dict):
                continue
            s_parts = []
            for sk in surv_keys:
                sv = entry.get(sk)
                if sv is not None:
                    s_parts.append(f"{sk}={sv:.3f}")
            findings_html += f"<li><strong>{decade}年代</strong>: {', '.join(s_parts) or 'データなし'}</li>"
        findings_html += "</ul>"

        # Grouped bar chart
        fig = go.Figure()
        for sk in surv_keys:
            color = _SURVIVAL_COLORS.get(sk, "#a0a0a0")
            y_vals: list[float | None] = []
            valid = False
            for decade in decades:
                entry = cohort_surv[decade]
                v = entry.get(sk) if isinstance(entry, dict) else None
                y_vals.append(v)
                if v is not None:
                    valid = True
            if not valid:
                continue
            fig.add_trace(go.Bar(
                name=sk,
                x=decades,
                y=[v if v is not None else 0 for v in y_vals],
                marker_color=color,
                hovertemplate=f"{sk}: %{{x}} → %{{y:.3f}}<extra></extra>",
            ))

        fig.update_layout(
            title="デビュー年代別 コホート生存率（S5/S10/S15/S20）",
            xaxis_title="デビュー年代",
            yaxis_title="生存率 S(k)",
            yaxis=dict(range=[0, 1.0]),
            barmode="group",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="デビュー年代別生存率（S5/S10/S15）",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_cohort_surv", height=420),
            method_note=(
                "S(k) = デビューからk年後（k=5/10/15/20）もアクティブ"
                "（年間クレジット記録が存在する）人物の割合。"
                "コホート年代はデビュー年の10年区切り（または任意区切り）。"
                "打切り: 観測期間末尾（データ取得時点）での右打切り。"
                "出典: generational_health.cohort_survival。"
            ),
            section_id="cohort_survival",
        )

    # ── Section 2: Generation pyramid (area stack) ────────────────

    def _build_generation_pyramid(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        gen_pyramid: dict = data.get("generation_pyramid", {}) if isinstance(data, dict) else {}

        if not gen_pyramid:
            return ReportSection(
                title="キャリア年数ピラミッド（積み上げ面グラフ）",
                findings_html=(
                    "<p>人材ピラミッドデータが利用できません（generational_health.generation_pyramid）。"
                    "year別にbin_0_5（0-5年）, bin_5_15（5-15年）, bin_15_plus（15年以上）"
                    "のアクティブ人数が格納されます。</p>"
                ),
                section_id="generation_pyramid",
            )

        years = sorted(int(y) for y in gen_pyramid.keys())
        bin_0_5 = [gen_pyramid[str(y)].get("bin_0_5", 0) for y in years]
        bin_5_15 = [gen_pyramid[str(y)].get("bin_5_15", 0) for y in years]
        bin_15_plus = [gen_pyramid[str(y)].get("bin_15_plus", 0) for y in years]

        # Total headcount
        totals = [b0 + b5 + bp for b0, b5, bp in zip(bin_0_5, bin_5_15, bin_15_plus)]

        yr_min = years[0] if years else "N/A"
        yr_max = years[-1] if years else "N/A"
        tot_min = min(totals) if totals else 0
        tot_max = max(totals) if totals else 0

        # Recent proportions (last year)
        if totals and totals[-1] > 0:
            last_total = totals[-1]
            prop_0_5 = bin_0_5[-1] / last_total
            prop_5_15 = bin_5_15[-1] / last_total
            prop_15p = bin_15_plus[-1] / last_total
            recent_str = (
                f"直近年（{yr_max}年）の構成: "
                f"0-5年={prop_0_5:.1%}, 5-15年={prop_5_15:.1%}, 15年以上={prop_15p:.1%}。"
            )
        else:
            recent_str = ""

        findings_html = (
            f"<p>人材キャリア年数ピラミッド: 対象期間 {yr_min}〜{yr_max}年。"
            f"総アクティブ人数の範囲: {tot_min:,}〜{tot_max:,}人。"
            f"{recent_str}</p>"
        )

        fig = go.Figure()

        # Stacked area: bin_0_5 (bottom), then cumulative layers
        cumul_0_5 = bin_0_5
        cumul_5_15 = [a + b for a, b in zip(bin_0_5, bin_5_15)]
        cumul_all = [a + b + c for a, b, c in zip(bin_0_5, bin_5_15, bin_15_plus)]

        fig.add_trace(go.Scatter(
            x=years, y=cumul_all,
            fill="tozeroy",
            fillcolor="rgba(240,147,251,0.25)",
            line=dict(color=_BIN_COLORS["bin_15_plus"], width=1.5),
            name="15年以上",
            hovertemplate="年=%{x}: 15年以上=%{y:,}<extra></extra>",
            stackgroup=None,
        ))
        fig.add_trace(go.Scatter(
            x=years, y=cumul_5_15,
            fill="tozeroy",
            fillcolor="rgba(102,126,234,0.3)",
            line=dict(color=_BIN_COLORS["bin_5_15"], width=1.5),
            name="5-15年",
            hovertemplate="年=%{x}: 5-15年以下=%{y:,}<extra></extra>",
            stackgroup=None,
        ))
        fig.add_trace(go.Scatter(
            x=years, y=cumul_0_5,
            fill="tozeroy",
            fillcolor="rgba(160,210,219,0.4)",
            line=dict(color=_BIN_COLORS["bin_0_5"], width=1.5),
            name="0-5年",
            hovertemplate="年=%{x}: 0-5年=%{y:,}<extra></extra>",
            stackgroup=None,
        ))

        fig.update_layout(
            title="アクティブ人材のキャリア年数ピラミッド（積み上げ面）",
            xaxis_title="年",
            yaxis_title="アクティブ人数",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="キャリア年数ピラミッド（積み上げ面グラフ）",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_gen_pyramid", height=440),
            method_note=(
                "アクティブ = 当該年にクレジット記録が存在する人物。"
                "キャリア年数 = 当該年 − デビュー年。"
                "bin_0_5: キャリア年数 0〜5年, bin_5_15: 5〜15年, bin_15_plus: 15年以上。"
                "積み上げ面グラフは各ビンを下から順に重ねた累積値を表示。"
                "出典: generational_health.generation_pyramid。"
            ),
            section_id="generation_pyramid",
        )

    # ── Section 3: Flow accounting ────────────────────────────────

    def _build_flow_accounting(self, sb: SectionBuilder, data: dict | list) -> ReportSection:
        flow_acc: dict = data.get("flow_accounting", {}) if isinstance(data, dict) else {}

        if not flow_acc:
            return ReportSection(
                title="人材フロー会計（入退場・net flow）",
                findings_html=(
                    "<p>人材フロー会計データが利用できません（generational_health.flow_accounting）。"
                    "year別にentry（新規参入）, exit（離脱）, net_flow, "
                    "dependency_ratio（シニア/ジュニア比）が格納されます。</p>"
                ),
                method_note=(
                    "dependency_ratio = シニア(>=20y) / ジュニア(<=3y) アクティブ人数比"
                ),
                section_id="flow_accounting",
            )

        years = sorted(int(y) for y in flow_acc.keys())
        entries = [flow_acc[str(y)].get("entry", 0) or 0 for y in years]
        exits = [flow_acc[str(y)].get("exit", 0) or 0 for y in years]
        net_flows = [flow_acc[str(y)].get("net_flow", 0) or 0 for y in years]
        dep_ratios = [flow_acc[str(y)].get("dependency_ratio") for y in years]

        if not years:
            return ReportSection(
                title="人材フロー会計（入退場・net flow）",
                findings_html="<p>有効なフロー会計エントリが見つかりませんでした。</p>",
                method_note=(
                    "dependency_ratio = シニア(>=20y) / ジュニア(<=3y) アクティブ人数比"
                ),
                section_id="flow_accounting",
            )

        yr_min = years[0]
        yr_max = years[-1]

        # Recent entries/exits
        recent_n = min(3, len(years))
        recent_years = years[-recent_n:]
        recent_entries = entries[-recent_n:]
        recent_exits = exits[-recent_n:]
        recent_items = ", ".join(
            f"{y}年(入={e:,}/退={x:,})"
            for y, e, x in zip(recent_years, recent_entries, recent_exits)
        )

        dep_vals = [v for v in dep_ratios if v is not None]
        dep_str = (
            f"dependency_ratio の範囲: {min(dep_vals):.2f}〜{max(dep_vals):.2f}。"
            if dep_vals else ""
        )

        findings_html = (
            f"<p>人材フロー会計: 対象期間 {yr_min}〜{yr_max}年。"
            f"直近{recent_n}年の入退場: {recent_items}。"
            f"{dep_str}</p>"
        )

        # Combined bar + line chart
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        fig.add_trace(go.Bar(
            x=years, y=entries,
            name="入場（entry）",
            marker_color="rgba(6,214,160,0.7)",
            hovertemplate="年=%{x}: 入場=%{y:,}<extra></extra>",
        ), secondary_y=False)

        fig.add_trace(go.Bar(
            x=years, y=[-v for v in exits],
            name="退場（exit）",
            marker_color="rgba(245,87,108,0.7)",
            hovertemplate="年=%{x}: 退場=%{y:,}<extra></extra>",
        ), secondary_y=False)

        fig.add_trace(go.Scatter(
            x=years, y=net_flows,
            mode="lines+markers",
            name="net flow",
            line=dict(color="#F8EC6A", width=2),
            marker=dict(size=5),
            hovertemplate="年=%{x}: net=%{y:,}<extra></extra>",
        ), secondary_y=False)

        # Dependency ratio on secondary axis
        valid_dep_years = [years[i] for i, v in enumerate(dep_ratios) if v is not None]
        valid_dep_vals = [v for v in dep_ratios if v is not None]
        if valid_dep_years:
            fig.add_trace(go.Scatter(
                x=valid_dep_years, y=valid_dep_vals,
                mode="lines",
                name="dependency ratio（右軸）",
                line=dict(color="#E09BC2", width=1.5, dash="dot"),
                hovertemplate="年=%{x}: dep_ratio=%{y:.2f}<extra></extra>",
            ), secondary_y=True)

        fig.add_hline(y=0, line_dash="dash", line_color="#a0a0a0", secondary_y=False)

        fig.update_layout(
            title="人材フロー会計（入場/退場 棒グラフ + net flow 折れ線）",
            xaxis_title="年",
            barmode="relative",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        fig.update_yaxes(title_text="人数（入/退/net）", secondary_y=False)
        fig.update_yaxes(title_text="dependency ratio", secondary_y=True)

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="人材フロー会計（入退場・net flow）",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_flow_accounting", height=460),
            method_note=(
                "entry = 当該年に初めてクレジットが記録された人物数。"
                "exit = 前年アクティブかつ当該年以降クレジットなしの人物数（打切り除く）。"
                "net_flow = entry − exit。"
                "dependency_ratio = シニア（キャリア20年以上）アクティブ数 / "
                "ジュニア（キャリア3年以下）アクティブ数。"
                "退場棒グラフは負方向に表示（視認性のため）。"
                "出典: generational_health.flow_accounting。"
            ),
            section_id="flow_accounting",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

from .._spec import SensitivityAxis  # noqa: E402

SPEC = make_default_spec(
    name='policy_generational_health',
    audience='policy',
    claim=(
        'デビュー decade × career year bin の S(k) 曲線 (KM 生存率) と '
        '年次フロー (新規参入 / 退出) が世代間で異なるパターンを示す'
    ),
    identifying_assumption=(
        '世代 = デビュー decade を仮定。career_year_bin の打切りは観察窓末年。'
        '退出 = 翌年クレジット可視性喪失 と定義し、雇用実態とは区別する。'
        'フロー会計は credit-record の denominator が時代別に変動する点を考慮。'
    ),
    null_model=['N5'],
    sources=['credits', 'persons', 'anime'],
    meta_table='meta_policy_generation',
    estimator='Kaplan-Meier (Greenwood CI) + 年次新規参入 / 退出フロー集計',
    ci_estimator='greenwood',
    sensitivity_grid=[
        SensitivityAxis(name='decade cut', values=['10y', '5y']),
        SensitivityAxis(name='退出 gap 閾値', values=['1y', '3y', '5y']),
    ],
    extra_limitations=[
        'クレジット記録密度の世代間差 (1970s vs 2010s) で生存率推定に下方バイアス',
        '名前解決失敗 (~1-3%) を退出として誤計上する可能性',
        'フォーマット変化 (TV → 配信) の影響は別軸で分解必要',
    ],
)
