"""O1 ジェンダー天井効果分析 — v2 compliant.

役職進行ハザード率の性別差を Cox 回帰で推定し、共クレジット ego-network の
性別構成を null model と比較する。Policy brief 向け。

Method overview:
- Cox 回帰: 役職進行ハザード率の性別差（F vs M, HR + 95% CI）
- Mann-Whitney U: 同コホート内昇進タイミング差（非パラメトリック、効果量 r）
- ego-network 性別構成 vs. null model（permutation 1000 回）

Framing (H2 compliance):
  Results are described as "advancement hazard rate difference" and
  "network position difference". Viewer ratings are excluded.
  All framing uses narrow structural descriptors only.

Audience: policy (primary), hr (secondary)
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import structlog

from ..ci_utils import format_ci
from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator, append_validation_warnings

log = structlog.get_logger(__name__)

# Minimum persons per gender per pipeline pair to run Cox model
_MIN_GENDER_N = 20
# Minimum per cohort per gender for Mann-Whitney
_MIN_COHORT_N = 5
# Null model iterations for ego-network
_N_NULL_ITER = 1000
# Ego-network sample cap
_EGO_SAMPLE_CAP = 3000

_COLOR_F = "#E09BC2"   # female line color
_COLOR_M = "#7CC8F2"   # male line color
_COLOR_NB = "#F8EC6A"  # non-binary line color

_PIPELINE_PAIRS: list[tuple[str, str, str]] = [
    ("in_between", "key_animator", "動画→原画"),
    ("key_animator", "animation_director", "原画→作監"),
    ("animation_director", "director", "作監→監督"),
]

_ROLE_LABELS: dict[str, str] = {
    "in_between": "動画",
    "key_animator": "原画",
    "animation_director": "作監",
    "director": "監督",
}


def _hex_to_rgb(hex_color: str) -> str:
    """Convert #RRGGBB → 'R,G,B' for rgba() use."""
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"{r},{g},{b}"


def _fmt_hr(hr: float, ci_lo: float, ci_hi: float) -> str:
    """Format hazard ratio with 95% CI as a readable string."""
    return f"HR={hr:.2f} (95% CI: {ci_lo:.2f}–{ci_hi:.2f})"


class O1GenderCeilingReport(BaseReportGenerator):
    """O1: ジェンダー天井効果 — advancement hazard rate disparity analysis.

    Estimates role-advancement hazard rate differences by gender using Cox PH
    regression, within-cohort timing differences via Mann-Whitney U, and
    ego-network gender composition vs. permutation null model.

    Policy brief audience.  Structural data only; viewer ratings excluded.
    """

    name = "o1_gender_ceiling"
    title = "ジェンダー天井効果分析"
    subtitle = (
        "役職進行ハザード率の性別差 (Cox 回帰) "
        "/ 同コホート内昇進タイミング (Mann-Whitney U) "
        "/ ego-network 性別構成 vs. null model"
    )
    filename = "o1_gender_ceiling.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        from src.analysis.causal.gender_progression import (
            PIPELINE_PAIRS,
            CoxResult,
            MannWhitneyResult,
            EgoNetworkResult,
            EgoNetworkSummary,
            compute_ego_network_gender_composition,
            load_gender_progression_records,
            cox_progression_hazard,
            mannwhitney_advancement_timing,
        )

        sb = SectionBuilder()

        # Gender coverage check
        coverage = self._check_gender_coverage()
        coverage_note = self._build_coverage_note(coverage)

        # Collect results for all pipeline pairs
        cox_results: list[CoxResult] = []
        mw_results: list[MannWhitneyResult] = []

        for role_from, role_to, pair_label in PIPELINE_PAIRS:
            records = load_gender_progression_records(self.conn, role_from, role_to)
            mf = [r for r in records if r.gender in ("M", "F")]

            n_f = sum(1 for r in mf if r.gender == "F")
            n_m = sum(1 for r in mf if r.gender == "M")

            if n_f < _MIN_GENDER_N or n_m < _MIN_GENDER_N:
                log.info(
                    "gender_ceiling_pair_skipped",
                    pair=pair_label,
                    n_f=n_f,
                    n_m=n_m,
                )
                continue

            cox = cox_progression_hazard(records, pair_label)
            if cox is not None:
                cox_results.append(cox)

            mw = mannwhitney_advancement_timing(records, pair_label, min_cohort_size=_MIN_COHORT_N)
            mw_results.extend(mw)

        # Ego-network analysis (full graph, not per pair)
        ego_results, ego_summary = compute_ego_network_gender_composition(
            self.conn,
            n_null_iterations=_N_NULL_ITER,
            rng_seed=42,
            sample_cap=_EGO_SAMPLE_CAP,
        )

        sections: list[str] = [
            sb.build_section(self._build_cox_section(sb, cox_results, coverage_note)),
            sb.build_section(self._build_mw_section(sb, mw_results, coverage_note)),
            sb.build_section(self._build_ego_net_section(sb, ego_results, ego_summary, coverage_note)),
        ]

        interpretation_html = self._build_interpretation(cox_results, ego_summary)

        insert_lineage(
            self.conn,
            table_name="meta_o1_gender_ceiling",
            audience="policy",
            source_silver_tables=["credits", "persons", "anime"],
            formula_version="v1.0",
            ci_method=(
                "Cox PH model 95% CI on gender covariate hazard ratio "
                "(lifelines CoxPHFitter, cohort_5y covariate); "
                "Log-rank test F vs M for each pipeline pair"
            ),
            null_model=(
                "Ego-network gender composition: permutation null model "
                f"({_N_NULL_ITER} iterations, seed=42) preserving gender ratio "
                "of collaborator pool. "
                "Mann-Whitney U: rank-based test within debut cohort."
            ),
            holdout_method=(
                "Not applicable (descriptive analysis of observed credit records)"
            ),
            description=(
                "Gender ceiling analysis: role-advancement hazard rate differences "
                "by gender via Cox PH regression (in_between→key_animator, "
                "key_animator→animation_director, animation_director→director). "
                "Within-cohort timing differences via Mann-Whitney U. "
                "Ego-network same-gender collaboration share vs. permutation null. "
                "Viewer ratings not used. "
                "Results describe structural network position differences, "
                "not individual evaluations or subjective assessments."
            ),
            rng_seed=42,
        )

        return self.write_report(
            "\n".join(sections),
            intro_html=self._build_intro(coverage),
            extra_glossary=_GLOSSARY,
        )

    # ------------------------------------------------------------------
    # Coverage check
    # ------------------------------------------------------------------

    def _check_gender_coverage(self) -> dict[str, Any]:
        """Return gender coverage statistics from persons table."""
        try:
            rows = self.conn.execute(
                "SELECT gender, COUNT(*) as cnt FROM conformed.persons GROUP BY gender"
            ).fetchall()
        except Exception:
            return {"total": 0, "n_known": 0, "coverage_pct": 0.0}

        total = sum(r[1] for r in rows)
        n_known = sum(
            r[1] for r in rows
            if r[0] and r[0].lower() in ("male", "female", "non-binary")
        )
        return {
            "total": total,
            "n_known": n_known,
            "coverage_pct": 100.0 * n_known / total if total > 0 else 0.0,
            "rows": rows,
        }

    def _build_coverage_note(self, coverage: dict[str, Any]) -> str:
        """Build HTML note about gender coverage."""
        pct = coverage.get("coverage_pct", 0.0)
        n_known = coverage.get("n_known", 0)
        total = coverage.get("total", 0)
        return (
            f'<p style="color:#e09050;font-size:0.85rem;">'
            f"[データ品質] gender フィールドのカバレッジ: "
            f"{n_known:,} / {total:,} 人 ({pct:.1f}%)。"
            f"性別不明人物は分析から除外されています。"
            f"カバレッジ未満の分析結果は統計的に代表性が低い可能性があります。</p>"
        )

    # ------------------------------------------------------------------
    # Section 1: Cox regression
    # ------------------------------------------------------------------

    def _build_cox_section(
        self,
        sb: SectionBuilder,
        cox_results: list,
        coverage_note: str,
    ) -> ReportSection:
        if not cox_results:
            return ReportSection(
                title="役職進行ハザード率の性別差 (Cox 回帰)",
                findings_html=(
                    "<p>Cox 回帰に必要なデータが不足しています。"
                    "各役職ペアで F/M ともに最低 "
                    f"{_MIN_GENDER_N} 名が必要です。</p>"
                    + coverage_note
                ),
                method_note=(
                    "Cox PH 回帰。進行イベント: role_to の最初のクレジット。"
                    "打切り: 観察期間内に role_to 未到達 (25年上限)。"
                    "共変量: gender_f (F=1, M=0), cohort_5y (標準化)。"
                    "HR > 1 = F の進行ハザード率が M より高い。"
                    "結果は役職進行ハザード率の差であり、個人の主観的評価ではない。"
                ),
                section_id="cox_hazard",
            )

        # v3: CIScatter primitive — null reference (HR=1) / 有意差 marker /
        # log-x optional / sort 入力順
        from src.viz import embed as viz_embed
        from src.viz.primitives import CIPoint, CIScatterSpec, render_ci_scatter

        ci_points = [
            CIPoint(
                label=r.pair_label,
                x=r.hr_female_vs_male,
                ci_lo=r.ci_lower,
                ci_hi=r.ci_upper,
                p_value=r.p_value,
            )
            for r in cox_results
        ]
        spec = CIScatterSpec(
            points=ci_points,
            x_label="Hazard Ratio (F vs M, log scale)",
            title="役職進行ハザード率の性別差 HR (F vs M, 95% CI) — Cox PH 回帰",
            log_x=True,
            reference=1.0,
            reference_label="HR",
            sort_by="input",
            significance_threshold=0.05,
        )
        fig = render_ci_scatter(spec, theme="dark")

        # Build findings text
        findings_parts: list[str] = []
        for r in cox_results:
            hr_str = _fmt_hr(r.hr_female_vs_male, r.ci_lower, r.ci_upper)
            p_str = f"p={r.p_value:.4f}" if r.p_value is not None else "p=N/A"
            lr_str = f"log-rank p={r.logrank_p:.4f}" if r.logrank_p is not None else ""
            direction = (
                "F の進行ハザード率が M より高い" if r.hr_female_vs_male > 1
                else "F の進行ハザード率が M より低い"
            )
            findings_parts.append(
                f"<li><strong>{r.pair_label}</strong>: {hr_str}, {p_str}"
                f"{', ' + lr_str if lr_str else ''}. "
                f"n_F={r.n_female:,} (進行={r.n_events_female:,}), "
                f"n_M={r.n_male:,} (進行={r.n_events_male:,}). "
                f"{direction}。</li>"
            )

        findings_html = (
            f"<p>役職ペア {len(cox_results)} 組で Cox 比例ハザードモデルを推定した。"
            f"HR は F を M と比較したハザード比（進行ハザード率の差）:</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="役職進行ハザード率の性別差 (Cox 回帰)",
            findings_html=findings_html,
            visualization_html=viz_embed(fig, "chart_cox_hr"),
            method_note=(
                "Cox 比例ハザード回帰 (lifelines CoxPHFitter)。"
                "進行イベント = role_to での最初のクレジット年。"
                "打切り = 観察窓内に role_to 未到達 (25年上限)。"
                "共変量: gender_f (F=1, M=0), cohort_5y (中心化)。"
                "HR > 1: F の進行ハザード率が M より高い (より速い昇進)。"
                "HR < 1: F の進行ハザード率が M より低い (より遅い昇進)。"
                "本指標は役職進行ハザード率の差であり、"
                "個人の主観的評価ではない。"
                "gender カバレッジが低い場合、結果の代表性に注意が必要。"
                "外部視聴者評価は使用しない。"
            ),
            section_id="cox_hazard",
        )

    # ------------------------------------------------------------------
    # Section 2: Mann-Whitney U
    # ------------------------------------------------------------------

    def _build_mw_section(
        self,
        sb: SectionBuilder,
        mw_results: list,
        coverage_note: str,
    ) -> ReportSection:
        if not mw_results:
            return ReportSection(
                title="コホート内昇進タイミング差 (Mann-Whitney U)",
                findings_html=(
                    "<p>Mann-Whitney U に必要なデータが不足しています。"
                    "各コホートで F/M ともに最低 "
                    f"{_MIN_COHORT_N} 名の観測進行者が必要です。</p>"
                    + coverage_note
                ),
                method_note=(
                    "Mann-Whitney U (scipy.stats.mannwhitneyu, two-sided)。"
                    "観測進行者 (打切りなし) のみを使用。"
                    "効果量 r = |Z| / √n_total。"
                ),
                section_id="mw_timing",
            )

        # Scatter: effect_r by cohort, coloured by p_value
        fig = go.Figure()

        pairs_seen = list({r.pair_label for r in mw_results})
        palette = ["#E09BC2", "#7CC8F2", "#3BC494", "#F8EC6A"]

        for idx, pair_label in enumerate(pairs_seen):
            pair_rows = [r for r in mw_results if r.pair_label == pair_label]
            pair_rows.sort(key=lambda r: r.cohort_5y)

            color = palette[idx % len(palette)]
            cohorts = [str(r.cohort_5y) for r in pair_rows]
            effects = [r.effect_r for r in pair_rows]
            pvals = [r.p_value for r in pair_rows]
            hover_texts = [
                (
                    f"{pair_label} {r.cohort_5y}–{r.cohort_5y + 4}<br>"
                    f"effect_r={r.effect_r:.3f}<br>"
                    f"p={r.p_value:.4f}<br>"
                    f"median_F={r.median_years_female:.1f}yr, "
                    f"median_M={r.median_years_male:.1f}yr<br>"
                    f"n_F={r.n_female}, n_M={r.n_male}"
                )
                for r in pair_rows
            ]

            fig.add_trace(go.Scatter(
                x=cohorts,
                y=effects,
                mode="markers+lines",
                name=pair_label,
                line=dict(color=color, width=1.5),
                marker=dict(
                    size=10,
                    color=pvals,
                    colorscale="Reds_r",
                    cmin=0.0,
                    cmax=0.3,
                    showscale=idx == 0,
                    colorbar=dict(title="p値", thickness=12, len=0.6),
                ),
                hovertext=hover_texts,
                hoverinfo="text",
            ))

        fig.add_hline(y=0, line_dash="dash", line_color="#606070", line_width=1)
        fig.update_layout(
            title="コホート別昇進タイミング差の効果量 (Mann-Whitney effect_r)",
            xaxis_title="デビューコホート (5年区切り)",
            yaxis_title="効果量 r = |Z| / √n",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=440,
        )

        # Findings text: summarise per pair
        findings_parts: list[str] = []
        for pair_label in pairs_seen:
            pair_rows = [r for r in mw_results if r.pair_label == pair_label]
            n_sig = sum(1 for r in pair_rows if r.p_value < 0.05)
            effects = [r.effect_r for r in pair_rows]
            mean_eff = sum(effects) / len(effects) if effects else 0.0

            # Determine direction from median differences
            f_faster = sum(
                1 for r in pair_rows
                if r.median_years_female is not None
                and r.median_years_male is not None
                and r.median_years_female < r.median_years_male
            )
            direction_str = (
                f"F の方が中央値昇進年数が短いコホート: {f_faster}/{len(pair_rows)}"
            )

            findings_parts.append(
                f"<li><strong>{pair_label}</strong>: "
                f"コホート数={len(pair_rows)}, "
                f"有意差あり (p&lt;0.05) = {n_sig}コホート, "
                f"効果量 r 平均 = {mean_eff:.3f}。"
                f"{direction_str}。</li>"
            )

        findings_html = (
            f"<p>観測進行者のみを対象に、デビューコホート別 (5年区切り) で "
            f"Mann-Whitney U 検定を実施した:</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            f"<p>効果量 r の目安: 0.1 小, 0.3 中, 0.5 大 (Cohen 1988)。"
            f"進行タイミング差の有無を示す記述的指標であり、原因推定ではない。</p>"
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="コホート内昇進タイミング差 (Mann-Whitney U)",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_mw_timing", height=440),
            method_note=(
                "Mann-Whitney U 検定 (scipy.stats.mannwhitneyu, two-sided)。"
                "対象: 観測済み進行者のみ (right-censored 除外)。"
                "コホート: デビュー年の 5年区切り (cohort_5y = debut_year // 5 * 5)。"
                "効果量 r = |Z| / √(n_F + n_M)。Z は正規近似による。"
                "小コホート (各性別 n < 5) は除外。"
                "打切り観察を除外するため生存分析とは異なる母集団を扱う点に注意。"
                "本指標は昇進タイミングの分布差の記述であり、"
                "個人の主観的評価ではない。"
            ),
            section_id="mw_timing",
        )

    # ------------------------------------------------------------------
    # Section 3: Ego-network gender composition
    # ------------------------------------------------------------------

    def _build_ego_net_section(
        self,
        sb: SectionBuilder,
        ego_results: list,
        ego_summary: Any,
        coverage_note: str,
    ) -> ReportSection:
        if not ego_results or ego_summary.n_persons == 0:
            return ReportSection(
                title="共クレジット ego-network 性別構成 vs. null model",
                findings_html=(
                    "<p>ego-network 分析に必要な協働データが取得できませんでした。</p>"
                    + coverage_note
                ),
                method_note=(
                    "ego-network (1-hop): 同一作品クレジットによる共クレジット関係。"
                    f"null model: 協働者プール内性別比を保持した permutation ({_N_NULL_ITER} 回)。"
                    "null_percentile: 観測 same_gender_share が null 分布の何パーセンタイルか。"
                ),
                section_id="ego_net_gender",
            )

        f_results = [r for r in ego_results if r.gender == "F"]
        m_results = [r for r in ego_results if r.gender == "M"]

        # Histogram of null_percentile for F and M
        fig = go.Figure()

        if f_results:
            f_pcts = [r.null_percentile for r in f_results]
            fig.add_trace(go.Histogram(
                x=f_pcts,
                name="女性 (F)",
                marker_color=_COLOR_F,
                opacity=0.7,
                nbinsx=20,
                hovertemplate="null_percentile=%{x:.1f}, count=%{y}<extra></extra>",
            ))
        if m_results:
            m_pcts = [r.null_percentile for r in m_results]
            fig.add_trace(go.Histogram(
                x=m_pcts,
                name="男性 (M)",
                marker_color=_COLOR_M,
                opacity=0.7,
                nbinsx=20,
                hovertemplate="null_percentile=%{x:.1f}, count=%{y}<extra></extra>",
            ))

        fig.add_vline(
            x=95,
            line_dash="dash",
            line_color="#E07532",
            annotation_text="95th percentile (p=0.05)",
        )
        fig.add_vline(
            x=5,
            line_dash="dash",
            line_color="#3BC494",
            annotation_text="5th percentile",
        )
        fig.update_layout(
            title=(
                "ego-network 性別構成の null_percentile 分布"
                f" (null: permutation {_N_NULL_ITER}回)"
            ),
            xaxis_title="null_percentile (0=完全異性, 100=完全同性)",
            yaxis_title="人数",
            barmode="overlay",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=400,
        )

        # Findings text
        pct_share_f = ego_summary.mean_same_gender_share_female * 100
        pct_share_m = ego_summary.mean_same_gender_share_male * 100
        med_pct_f = ego_summary.median_null_percentile_female
        med_pct_m = ego_summary.median_null_percentile_male
        n95_f = ego_summary.n_above_95th_female
        n95_m = ego_summary.n_above_95th_male
        n_f = len(f_results)
        n_m = len(m_results)

        findings_html = (
            f"<p>ego-network 性別構成分析 (対象: {ego_summary.n_persons:,} 人):</p>"
            f"<ul>"
            f"<li><strong>女性 (F)</strong> n={n_f:,}: "
            f"同性協働者割合平均 = {pct_share_f:.1f}%, "
            f"null_percentile 中央値 = {med_pct_f:.1f}, "
            f"null_percentile ≥ 95 (同性集積が帰無より有意): {n95_f:,}人</li>"
            f"<li><strong>男性 (M)</strong> n={n_m:,}: "
            f"同性協働者割合平均 = {pct_share_m:.1f}%, "
            f"null_percentile 中央値 = {med_pct_m:.1f}, "
            f"null_percentile ≥ 95: {n95_m:,}人</li>"
            f"</ul>"
            f"<p>null_percentile が高い = 同性間での協働傾向 (homophily) が帰無より強い。"
            f"low null_percentile = 異性間での協働傾向 (heterophily)。</p>"
            + coverage_note
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="共クレジット ego-network 性別構成 vs. null model",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_ego_net_gender", height=400),
            method_note=(
                "ego-network (1-hop): 同一作品への共クレジット関係を 1-hop 隣接として扱う。"
                "same_gender_share = ego の直接協働者のうち ego と同性の割合。"
                f"null model: 協働者プール内の性別比率を保持した permutation を "
                f"{_N_NULL_ITER} 回繰り返し、帰無分布を構成する。"
                "null_percentile = 観測 same_gender_share が帰無分布の何パーセンタイルか。"
                "≥95 = 5% 有意水準で同性集積 (homophily) が有意。"
                "≤5 = 5% 有意水準で異性集積 (heterophily) が有意。"
                "role は ego が担当した最初の animation 役職を使用。"
                "本指標は協働ネットワーク位置の差であり、主観的評価ではない。"
                "外部視聴者評価は使用しない。"
            ),
            section_id="ego_net_gender",
        )

    # ------------------------------------------------------------------
    # Interpretation
    # ------------------------------------------------------------------

    def _build_interpretation(
        self,
        cox_results: list,
        ego_summary: Any,
    ) -> str:
        if not cox_results:
            return ""

        # Summarise direction of HR across pairs
        hr_below_1 = [r for r in cox_results if r.hr_female_vs_male < 1.0]
        hr_above_1 = [r for r in cox_results if r.hr_female_vs_male > 1.0]

        main_dir = (
            "F の役職進行ハザード率が M より低い" if len(hr_below_1) > len(hr_above_1)
            else "F の役職進行ハザード率が M より高い" if len(hr_above_1) > len(hr_below_1)
            else "役職ペアによって方向が異なる"
        )

        first = cox_results[0]
        first_hr_str = _fmt_hr(first.hr_female_vs_male, first.ci_lower, first.ci_upper)

        return (
            f"<p>本分析の著者は、{main_dir} という構造的パターンを観察する。"
            f"{first.pair_label}: {first_hr_str}。</p>"
            f"<p>代替解釈: ハザード率の差は機会の不均等を直接示すものではなく、"
            f"性別による役職選択・参加率・スタジオ帰属の差を反映する可能性がある。"
            f"gender カバレッジ ({ego_summary.n_persons:,} 名) が全体の一部であることも、"
            f"結果の代表性に影響する。</p>"
            f"<p>この解釈の前提: Cox モデルの比例ハザード仮定が成立すること、"
            f"および性別帰属データの精度。"
            f"これらが満たされない場合、HR 推定値は偏向する可能性がある。</p>"
            f"<p>ego-network 分析では同性協働傾向 (homophily) の有無を観察するが、"
            f"それが機会制約なのか選好なのかは本データからは特定できない。</p>"
        )

    # ------------------------------------------------------------------
    # Intro
    # ------------------------------------------------------------------

    def _build_intro(self, coverage: dict[str, Any]) -> str:
        n_known = coverage.get("n_known", 0)
        pct = coverage.get("coverage_pct", 0.0)
        return (
            "<p>本レポートは、アニメーション業界のクレジットデータから役職進行ハザード率の"
            "性別差を構造的に推定し、共クレジット ego-network の性別構成を null model と比較する。"
            "政策立案者・業界団体が労働市場の構造的パターンを把握するための参照情報を提供する。</p>"
            f"<p>分析対象: gender フィールドが記録された {n_known:,} 名 "
            f"(全体の {pct:.1f}%)。性別不明の人物は除外されている。</p>"
            "<p>すべての指標は公開クレジットデータに基づく構造的記述である。"
            "役職進行ハザード率の差は個人の主観的評価を意味しない。</p>"
            "<p>免責事項 / Disclaimer: "
            "指標は役職進行タイミングの構造的差異を記述するものであり、"
            "主観的評価や個人属性の定量化ではありません。"
            "This report describes structural differences in role-advancement timing; "
            "it does not evaluate individual performance or assess personal attributes.</p>"
        )


# ---------------------------------------------------------------------------
# Glossary
# ---------------------------------------------------------------------------

_GLOSSARY: dict[str, str] = {
    "advancement_hazard_rate": (
        "単位時間あたりの役職進行確率。Cox PH モデルで推定。"
        "「高ハザード率」= より速い進行。構造的指標であり個人評価ではない。"
    ),
    "HR (hazard ratio)": (
        "Cox 回帰のハザード比。HR(F vs M) > 1: F のハザード率が M より高い。"
        "HR < 1: F のハザード率が M より低い。"
    ),
    "null_percentile": (
        "観測値が帰無分布の何パーセンタイルに相当するか。"
        "≥95 = 帰無分布の 5% 有意水準で有意。"
    ),
    "same_gender_share": (
        "ego-network の直接協働者のうち ego と同性の割合。"
        "homophily の指標として使用。"
    ),
    "effect_r": (
        "Mann-Whitney U 効果量 r = |Z| / √n。"
        "0.1 小, 0.3 中, 0.5 大 (Cohen 1988)。"
    ),
    "cohort_5y": (
        "初 credit 年の 5年区切りコホート。"
        "例: 1990–1994 → cohort_5y = 1990。"
    ),
}


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='o1_gender_ceiling',
    audience='policy',
    claim='ジェンダー天井効果分析 に関する記述的指標 (subtitle: 役職進行ハザード率の性別差 (Cox 回帰) / 同コホート内昇進タイミング (Mann-Whitney U) / ego-network 性別構成 vs. null model)',
    sources=["credits", "persons", "anime"],
    meta_table='meta_o1_gender_ceiling',
)
