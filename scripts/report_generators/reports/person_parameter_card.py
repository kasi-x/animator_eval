"""個人パラメータカード — K=6アーキタイプ別パラメータ分布（10軸）— v2 compliant."""

from __future__ import annotations

import json
import math
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_JSON_DIR = Path(__file__).parents[4] / "result" / "json"

_MAX_PARAMS = 10
_RADAR_PARAMS = 10
_TOP_VARIANCE_PARAMS = 5


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


def _normalize_across(values_dict: dict[str, list[float]]) -> dict[str, list[float]]:
    """Normalize each key's list by the key's min/max across all values."""
    result: dict[str, list[float]] = {}
    for key, vals in values_dict.items():
        vmin = min(vals) if vals else 0.0
        vmax = max(vals) if vals else 0.0
        span = vmax - vmin
        if span == 0:
            result[key] = [0.5] * len(vals)
        else:
            result[key] = [(v - vmin) / span for v in vals]
    return result


class PersonParameterCardReport(BaseReportGenerator):
    name = "person_parameter_card"
    title = "個人パラメータカード"
    subtitle = "K=6アーキタイプ別パラメータ分布（10軸）"
    filename = "person_parameter_card.html"
    doc_type = "main"

    def generate(self) -> Path | None:
        data = _load("person_parameters")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(self._build_archetype_overview(sb, data)),
            sb.build_section(self._build_top_persons_per_archetype(sb, data)),
            sb.build_section(self._build_parameter_distributions(sb, data)),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: K=6 archetype radar ──────────────────────────────

    def _build_archetype_overview(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        archetypes = data.get("archetypes", {})
        if not isinstance(archetypes, dict):
            archetypes = {}
        n_total = int(data.get("n_total", 0))

        if not archetypes:
            findings = (
                "<p>アーキタイプデータが利用できません"
                "（person_parameters.archetypes）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="K=6アーキタイプ別人数・パラメータ分布",
                findings_html=findings,
                method_note="K-means K=6 on 10-parameter feature matrix",
                section_id="ppc_overview",
            )

        # Collect all parameter names from first archetype with data
        param_names: list[str] = []
        for arc_data in archetypes.values():
            params = arc_data.get("parameters", {})
            if isinstance(params, dict):
                param_names = list(params.keys())[:_RADAR_PARAMS]
                break

        if not param_names:
            findings = (
                "<p>パラメータ名が取得できません"
                "（archetypes.*.parameters が空）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="K=6アーキタイプ別人数・パラメータ分布",
                findings_html=findings,
                method_note="K-means K=6 on 10-parameter feature matrix",
                section_id="ppc_overview",
            )

        # Build raw mean values per param per archetype
        arc_names = list(archetypes.keys())
        raw_by_param: dict[str, list[float]] = {p: [] for p in param_names}
        for arc_name in arc_names:
            params = archetypes[arc_name].get("parameters", {})
            if not isinstance(params, dict):
                params = {}
            for p in param_names:
                pdata = params.get(p, {})
                mean_v = _sf(pdata.get("mean", 0.0) if isinstance(pdata, dict) else 0.0)
                raw_by_param[p].append(mean_v)

        norm_by_param = _normalize_across(raw_by_param)

        colors = [
            "#3593D2", "#E09BC2", "#E09BC2",
            "#E07532", "#FFB444", "#7CC8F2",
        ]

        fig = go.Figure()
        for i, arc_name in enumerate(arc_names):
            r_vals = [norm_by_param[p][i] for p in param_names]
            # Close polygon
            r_closed = r_vals + [r_vals[0]]
            theta_closed = param_names + [param_names[0]]
            fig.add_trace(
                go.Scatterpolar(
                    r=r_closed,
                    theta=theta_closed,
                    fill="toself",
                    name=arc_name,
                    line_color=colors[i % len(colors)],
                    opacity=0.65,
                )
            )

        fig.update_layout(
            title="K=6アーキタイプ セントロイド（10パラメータ軸、0-1正規化）",
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            height=540,
        )

        arc_count_strs = [
            f"{nm}（{int(archetypes[nm].get('count', 0)):,}名）"
            for nm in arc_names
        ]
        counts_summary = "、".join(arc_count_strs)

        findings = (
            f"<p>総対象者数: {n_total:,}名。"
            f"アーキタイプ別内訳: {counts_summary}。"
            f"レーダーチャートは各アーキタイプのパラメータ平均値を"
            f"0-1正規化して示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="K=6アーキタイプ別人数・パラメータ分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_ppc_radar", height=540
            ),
            method_note=(
                "K-means K=6 on 10-parameter feature matrix"
                "（authority, trust, credit_density, person_fe,"
                " peer_percentile, opportunity_residual, consistency,"
                " independent_value, birank, dormancy）。"
                "各軸は全アーキタイプ間で0-1正規化済み。"
                "アーキタイプ名はセントロイドの相対順位による命名。"
            ),
            section_id="ppc_overview",
        )

    # ── Section 2: Grouped bar by archetype (top-5 params) ──────────

    def _build_top_persons_per_archetype(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        archetypes = data.get("archetypes", {})
        if not isinstance(archetypes, dict):
            archetypes = {}

        if not archetypes:
            findings = (
                "<p>アーキタイプ別パラメータデータが利用できません"
                "（person_parameters.archetypes）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="アーキタイプ別Top-10 パラメータ分布",
                findings_html=findings,
                section_id="ppc_grouped",
            )

        # Collect param means per archetype
        arc_names = list(archetypes.keys())
        all_params: list[str] = []
        for arc_data in archetypes.values():
            params = arc_data.get("parameters", {})
            if isinstance(params, dict):
                all_params = list(params.keys())[:_MAX_PARAMS]
                break

        if not all_params:
            findings = (
                "<p>パラメータリストが空です"
                "（archetypes.*.parameters）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="アーキタイプ別Top-10 パラメータ分布",
                findings_html=findings,
                section_id="ppc_grouped",
            )

        # mean[arc_name][param] = mean value
        means: dict[str, dict[str, float]] = {}
        for arc_name in arc_names:
            params = archetypes[arc_name].get("parameters", {})
            if not isinstance(params, dict):
                params = {}
            means[arc_name] = {}
            for p in all_params:
                pdata = params.get(p, {})
                means[arc_name][p] = _sf(
                    pdata.get("mean", 0.0) if isinstance(pdata, dict) else 0.0
                )

        # Select top-5 params by cross-archetype variance
        param_variances: list[tuple[str, float]] = []
        for p in all_params:
            vals = [means[a][p] for a in arc_names]
            if len(vals) < 2:
                param_variances.append((p, 0.0))
                continue
            mean_v = sum(vals) / len(vals)
            var_v = sum((v - mean_v) ** 2 for v in vals) / len(vals)
            param_variances.append((p, var_v))

        param_variances.sort(key=lambda t: t[1], reverse=True)
        top_params = [pv[0] for pv in param_variances[:_TOP_VARIANCE_PARAMS]]

        colors = [
            "#3593D2", "#E09BC2", "#E09BC2",
            "#E07532", "#FFB444",
        ]

        fig = go.Figure()
        for i, p in enumerate(top_params):
            y_vals = [means[a][p] for a in arc_names]
            fig.add_trace(
                go.Bar(
                    name=p,
                    x=arc_names,
                    y=y_vals,
                    marker_color=colors[i % len(colors)],
                    hovertemplate=f"{p}: %{{y:.3f}}<extra></extra>",
                )
            )

        fig.update_layout(
            title="アーキタイプ × パラメータ（上位5 by 分散）",
            xaxis_title="アーキタイプ",
            yaxis_title="パラメータ平均値（元スケール）",
            barmode="group",
            height=460,
        )

        top_params_str = "、".join(top_params)

        # Which archetype has highest mean for each param
        param_desc_parts: list[str] = []
        for p in top_params:
            vals_by_arc = {a: means[a][p] for a in arc_names}
            best_arc = max(vals_by_arc, key=lambda a: vals_by_arc[a])
            param_desc_parts.append(f"{p}の最大アーキタイプ={best_arc}")

        findings = (
            f"<p>分散上位{_TOP_VARIANCE_PARAMS}パラメータ: {top_params_str}。"
            f"パラメータ別最大値アーキタイプ: "
            f"{'、'.join(param_desc_parts)}。"
            f"グループ棒グラフは各アーキタイプ × パラメータの平均値を示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="アーキタイプ別Top-10 パラメータ分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_ppc_grouped", height=460
            ),
            method_note=(
                f"表示パラメータ: アーキタイプ間分散上位{_TOP_VARIANCE_PARAMS}件。"
                "分散はアーキタイプ間の平均値に対するpopulation varianceで計算。"
                "値は元スケール（正規化なし）。"
            ),
            section_id="ppc_grouped",
        )

    # ── Section 3: 10-parameter box plot grid ───────────────────────

    def _build_parameter_distributions(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        param_dists = data.get("parameter_distributions", {})
        if not isinstance(param_dists, dict):
            param_dists = {}
        n_total = int(data.get("n_total", 0))

        if not param_dists:
            findings = (
                "<p>パラメータ分布データが利用できません"
                "（person_parameters.parameter_distributions）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="10パラメータ分布（ヒストグラム）",
                findings_html=findings,
                method_note="各パラメータの四分位値（p25/p50/p75）を使用",
                section_id="ppc_distributions",
            )

        param_names = list(param_dists.keys())[:_MAX_PARAMS]
        n_params = len(param_names)

        n_cols = 5
        n_rows = math.ceil(n_params / n_cols)

        fig = make_subplots(
            rows=n_rows,
            cols=n_cols,
            subplot_titles=param_names,
        )

        colors = [
            "#3593D2", "#E09BC2", "#E09BC2", "#E07532", "#FFB444",
            "#7CC8F2", "#4ecdc4", "#45b7d1", "#96ceb4", "#ffeaa7",
        ]

        for i, p in enumerate(param_names):
            row = i // n_cols + 1
            col = i % n_cols + 1
            pdata = param_dists[p]
            if not isinstance(pdata, dict):
                pdata = {}

            p25 = _sf(pdata.get("p25", 0.0))
            p50 = _sf(pdata.get("p50", 0.0))
            p75 = _sf(pdata.get("p75", 0.0))
            mean_v = _sf(pdata.get("mean", p50))

            # Build box from summary stats
            # Plotly go.Box with lowerfence/q1/median/q3/upperfence
            iqr = p75 - p25
            lower_fence = p25 - 1.5 * iqr
            upper_fence = p75 + 1.5 * iqr

            fig.add_trace(
                go.Box(
                    q1=[p25],
                    median=[p50],
                    q3=[p75],
                    mean=[mean_v],
                    lowerfence=[lower_fence],
                    upperfence=[upper_fence],
                    name=p,
                    marker_color=colors[i % len(colors)],
                    showlegend=False,
                    boxmean=True,
                    hovertemplate=(
                        f"{p}<br>"
                        "p25=%{q1:.3f}<br>"
                        "p50=%{median:.3f}<br>"
                        "p75=%{q3:.3f}<extra></extra>"
                    ),
                ),
                row=row,
                col=col,
            )

        fig.update_layout(
            title=f"10パラメータ箱ひげ図グリッド（n={n_total:,}）",
            height=280 * n_rows,
        )

        param_summary = "、".join(param_names)

        findings = (
            f"<p>表示パラメータ数: {n_params:,}。"
            f"総対象者数: {n_total:,}名。"
            f"パラメータ一覧: {param_summary}。"
            f"各サブプロットは四分位値（p25/p50/p75）と平均値（菱形）を示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="10パラメータ分布（ヒストグラム）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_ppc_dists", height=280 * n_rows
            ),
            method_note=(
                "各パラメータの四分位値（p25/p50/p75）を使用。"
                "箱ひげの上下ひげは IQR × 1.5 の仮定的フェンス値。"
                "実際の外れ値は個別値データが利用可能な場合のみ描画される"
                "（本実装では四分位サマリーのみ使用）。"
                "boxmean=True: 菱形マーカーは平均値を示す。"
            ),
            section_id="ppc_distributions",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='person_parameter_card',
    audience='common',
    claim=(
        '個人ごとの 10 軸パラメータ (theta_i / birank / studio_exp / patronage / '
        'AWCC / dormancy / tenure / role_diversity / network_reach / total_credits) を '
        'K=6 アーキタイプに分類し、各個人カードを集計値で提示'
    ),
    identifying_assumption=(
        'アーキタイプ = K-Means cluster の解釈ラベル — 客観的分類ではない。'
        '個別カードは集計値の表示であり、個人特定 / 評価判断には使用しない設計。'
        '10 軸の選択は事前固定、別の軸選択で異なるアーキタイプが得られる。'
    ),
    null_model=['N6'],
    sources=['credits', 'persons', 'anime', 'feat_person_scores'],
    meta_table='meta_common_person_parameters',
    estimator='K-Means (K=6) on z-normalized 10-axis vector',
    ci_estimator='bootstrap', n_resamples=500,
    extra_limitations=[
        '10 軸の選択は固定、軸セット変更でアーキタイプが変化',
        'K=6 は事前固定、silhouette 最適 K と異なる可能性',
        '個別カードは集計値表示、個人特定情報は除外',
    ],
)
