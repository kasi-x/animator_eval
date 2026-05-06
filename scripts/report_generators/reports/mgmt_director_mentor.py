"""監督共起後 5 年メンティー M̂ プロファイル — v2 compliant.

Management brief: director mentoring value-add.
- Section 1: M̂_d ranking (EB shrinkage), Top 20
- Section 2: Permutation null model vs observed values
- Section 3: Mentee career trajectory scatter (fe_before vs fe_after)
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


class MgmtDirectorMentorReport(BaseReportGenerator):
    name = "mgmt_director_mentor"
    title = "監督共起後 5 年メンティー M̂ プロファイル"
    subtitle = "M̂_d EB縮小推定 + 置換ヌルモデル"
    filename = "mgmt_director_mentor.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load("director_value_add")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(self._build_ranking(sb, data)),
            sb.build_section(self._build_null_model(sb, data)),
            sb.build_section(self._build_mentee_outcomes(sb, data)),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: Ranking ──────────────────────────────────────────

    def _build_ranking(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        rankings = data.get("rankings", [])
        if not isinstance(rankings, list):
            rankings = []

        if not rankings:
            findings = (
                "<p>監督共起後 5 年メンティー M̂ プロファイルデータが利用できません"
                "（director_value_add.rankings）。"
                "パイプラインのメンタリングモジュールの実行が必要です。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="監督共起後 5 年メンティー M̂ プロファイル（M̂_d）",
                findings_html=findings,
                section_id="director_ranking",
            )

        ranked = sorted(
            rankings,
            key=lambda r: _safe_float(r.get("m_shrunk")),
            reverse=True,
        )
        top20 = ranked[:20]

        n_total = len(rankings)
        n_significant = sum(
            1 for r in rankings
            if _safe_float(r.get("null_p_value"), 1.0) < 0.05
        )

        m_shrunk_vals = [_safe_float(r.get("m_shrunk")) for r in rankings]
        m_min = min(m_shrunk_vals) if m_shrunk_vals else 0.0
        m_max = max(m_shrunk_vals) if m_shrunk_vals else 0.0

        names = [
            str(r.get("director_id") or f"director_{i}")
            for i, r in enumerate(top20)
        ]
        m_vals = [_safe_float(r.get("m_shrunk")) for r in top20]
        ci_los = [_safe_float(r.get("ci_lower")) for r in top20]
        ci_his = [_safe_float(r.get("ci_upper")) for r in top20]
        p_vals = [
            _safe_float(r.get("null_p_value"), 1.0) for r in top20
        ]

        # v3: CIScatter (forest) primitive 経由 — 有意差 marker / null reference /
        # EB shrinkage badge / Okabe-Ito CB-safe palette を強制
        from src.viz import embed as viz_embed
        from src.viz.primitives import (
            CIPoint, CIScatterSpec, ShrinkageInfo, render_ci_scatter,
        )

        ci_points = [
            CIPoint(
                label=names[i],
                x=m_vals[i],
                ci_lo=ci_los[i],
                ci_hi=ci_his[i],
                p_value=p_vals[i],
            )
            for i in range(len(top20))
        ]
        spec = CIScatterSpec(
            points=ci_points,
            x_label="M̂_d（メンティー固定効果平均変化量）",
            title="監督共起後 5 年メンティー M̂ プロファイル（M̂_d、EB縮小推定値、上位20）",
            reference=0.0,
            reference_label="null",
            sort_by="x",
            significance_threshold=0.05,
            shrinkage=ShrinkageInfo(method="Empirical Bayes"),
        )
        fig = render_ci_scatter(spec, theme="dark")

        findings = (
            f"<p>M̂_d 推定値（M̂_d）が算出された監督数: {n_total:,}人。"
            f"置換ヌルモデルに対してp<0.05の監督数: {n_significant:,}人。"
            f"M̂_d範囲: {m_min:.3f}〜{m_max:.3f}。"
            f"オレンジ色のバーはp<0.05（ヌルモデル有意）、"
            f"グレーは非有意。エラーバーは95%信頼区間。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        # v3: KPI strip
        from ..section_builder import KPICard
        n_sig_pct = (
            f"{n_significant / n_total * 100:.1f}%"
            if n_total > 0 else "N/A"
        )
        kpis = [
            KPICard("分析監督数", f"{n_total:,}", "M̂_d 算出可能"),
            KPICard("ヌル有意 (p<0.05)", f"{n_significant:,}",
                    "permutation null model"),
            KPICard("M̂_d レンジ", f"{m_min:.3f} – {m_max:.3f}",
                    "EB 縮小推定値"),
            KPICard("null < 観測値 比率", n_sig_pct,
                    f"{n_significant}/{n_total} 監督"),
        ]

        return ReportSection(
            title="監督共起後 5 年メンティー M̂ プロファイル（M̂_d）",
            findings_html=findings,
            visualization_html=viz_embed(fig, "chart_director_ranking"),
            kpi_cards=kpis,
            chart_caption=(
                "横軸 = M̂_d (メンティー固定効果の平均変化量)、縦軸 = 監督。"
                "M̂_d は監督との共起後 5 年以内にデビューしたメンティー集団の"
                "AKM 個人固定効果 (θ_i) 平均変化量で、"
                "値が大きいほど接触後にメンティーのネットワーク位置が上方シフトした集団を指す。"
                "塗り潰し (オレンジ) = 置換ヌルモデルに対し p<0.05 で有意、"
                "中抜き (グレー) = null 95% 区間内 (ランダム割当と区別不能)。"
                "誤差棒 = 95% 信頼区間 (解析的導出)。"
                "点線 (M̂_d=0) はメンティー集団全体の平均線 (帰無仮説の基準)。"
                "EB (Empirical Bayes) 縮小推定により、メンティー数が少ない監督の推定値は"
                "全体平均方向に補正されるため、生の標本平均よりも順位間の差が圧縮される。"
                "有意な監督と非有意な監督の差は「指導効果の差」ではなく"
                "「null モデルとの構造的距離」として読む。"
            ),
            method_note=(
                "M̂_d: 監督の指導下でメンティーが経験した"
                "個人固定効果（AKM θ_i）の平均変化量。"
                "EB縮小推定によりメンティー数が少ない監督の推定値を補正。"
                "CI: 95%信頼区間（解析的導出）。"
                "null_p_value: 置換ヌルモデルにおけるp値"
                "（監督-メンティー割当をランダムシャッフルして算出）。"
            ),
            section_id="director_ranking",
        )

    # ── Section 2: Null model ───────────────────────────────────────

    def _build_null_model(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        null_dist = data.get("null_distribution", [])
        rankings = data.get("rankings", [])

        if not isinstance(null_dist, list):
            null_dist = []
        if not isinstance(rankings, list):
            rankings = []

        null_floats = [_safe_float(v) for v in null_dist]
        n_perm = len(null_floats)

        if not null_floats:
            findings = (
                "<p>置換ヌルモデルデータが利用できません"
                "（director_value_add.null_distribution）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="置換ヌルモデル vs 観測値",
                findings_html=findings,
                section_id="director_null",
            )

        null_mean = sum(null_floats) / n_perm if n_perm else 0.0
        null_var = (
            sum((v - null_mean) ** 2 for v in null_floats) / n_perm
            if n_perm > 1 else 0.0
        )
        null_std = math.sqrt(null_var)

        sorted_null = sorted(null_floats)
        p95_idx = int(0.95 * (n_perm - 1))
        null_p95 = sorted_null[p95_idx] if sorted_null else 0.0

        # Top 5 observed m_shrunk
        top5_obs = sorted(
            [_safe_float(r.get("m_shrunk")) for r in rankings],
            reverse=True,
        )[:5]

        n_above_p95 = sum(1 for v in top5_obs if v > null_p95)

        fig = go.Figure()
        fig.add_trace(
            go.Histogram(
                x=null_floats,
                nbinsx=40,
                name="ヌル分布（置換）",
                marker_color="#7CC8F2",
                opacity=0.7,
                hovertemplate="M=%{x:.3f}: %{y:,}<extra></extra>",
            )
        )
        for i, obs in enumerate(top5_obs):
            fig.add_vline(
                x=obs,
                line_color="#FFB444",
                line_dash="dot",
                annotation_text=f"観測値#{i+1} ({obs:.3f})",
                annotation_font_size=10,
            )
        fig.add_vline(
            x=null_p95,
            line_color="#E07532",
            line_dash="dash",
            annotation_text=f"95th({null_p95:.3f})",
        )
        fig.update_layout(
            title="置換ヌルモデル分布と観測上位値",
            xaxis_title="M̂_d（置換サンプル）",
            yaxis_title="頻度",
            height=420,
        )

        findings = (
            f"<p>置換ヌルモデル: n_permutations={n_perm:,}。"
            f"ヌル分布: 平均={null_mean:.3f}、標準偏差={null_std:.3f}、"
            f"95パーセンタイル={null_p95:.3f}。"
            f"観測上位5値のうちヌル95パーセンタイルを上回る数: "
            f"{n_above_p95}件。"
            f"垂直破線はヌル95パーセンタイル閾値を示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="置換ヌルモデル vs 観測値",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_director_null", height=420
            ),
            method_note=(
                "置換ヌルモデル: 監督-メンティー割当をランダムシャッフルし、"
                "M̂_dを再計算。これをn_permutations回繰り返して帰無分布を生成。"
                "95パーセンタイル閾値を上回る観測値は"
                "ランダム割当では生じにくいことを示す。"
            ),
            section_id="director_null",
        )

    # ── Section 3: Mentee outcomes ──────────────────────────────────

    def _build_mentee_outcomes(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        mentee_outcomes = data.get("mentee_outcomes", {})
        if not isinstance(mentee_outcomes, dict):
            mentee_outcomes = {}

        if not mentee_outcomes:
            findings = (
                "<p>メンティーキャリア軌跡データが利用できません"
                "（director_value_add.mentee_outcomes）。"
                "メンタリングモジュールによるメンティー追跡が必要です。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="メンティーキャリア軌跡（前後散布図）",
                findings_html=findings,
                section_id="director_mentee",
            )

        # Flatten all mentees, assign group index per mentor
        fe_before_all: list[float] = []
        fe_after_all: list[float] = []
        group_all: list[int] = []
        n_mentees = 0

        for g_idx, (_, mentees) in enumerate(mentee_outcomes.items()):
            if not isinstance(mentees, list):
                continue
            for m in mentees:
                if not isinstance(m, dict):
                    continue
                fb = m.get("fe_before")
                fa = m.get("fe_after")
                if fb is not None and fa is not None:
                    fe_before_all.append(_safe_float(fb))
                    fe_after_all.append(_safe_float(fa))
                    group_all.append(g_idx % 10)
                    n_mentees += 1

        if not fe_before_all:
            findings = (
                "<p>メンティーキャリア軌跡データの数値が取得できませんでした。"
                "fe_before / fe_after フィールドの確認が必要です。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="メンティーキャリア軌跡（前後散布図）",
                findings_html=findings,
                section_id="director_mentee",
            )

        improvements = [
            a - b for a, b in zip(fe_after_all, fe_before_all)
        ]
        mean_imp = sum(improvements) / len(improvements)

        all_vals = fe_before_all + fe_after_all
        v_min = min(all_vals)
        v_max = max(all_vals)

        color_scale = [
            "#3593D2", "#E09BC2", "#E07532", "#FFB444",
            "#3BC494", "#F8EC6A", "#7CC8F2", "#4ecdc4",
            "#ff6b6b", "#c7f464",
        ]
        colors = [color_scale[g % len(color_scale)] for g in group_all]

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=fe_before_all,
                y=fe_after_all,
                mode="markers",
                marker=dict(
                    color=colors,
                    size=6,
                    opacity=0.65,
                ),
                name="メンティー",
                hovertemplate=(
                    "指導前FE: %{x:.3f}<br>"
                    "指導後FE: %{y:.3f}<extra></extra>"
                ),
            )
        )
        # Reference line y = x
        fig.add_trace(
            go.Scatter(
                x=[v_min, v_max],
                y=[v_min, v_max],
                mode="lines",
                line=dict(color="#a0a0a0", dash="dash"),
                name="y = x（変化なし）",
                hoverinfo="skip",
            )
        )
        fig.update_layout(
            title="メンティーキャリア軌跡: 指導前後の個人固定効果",
            xaxis_title="指導前 Person FE (θ)",
            yaxis_title="指導後 Person FE (θ)",
            height=460,
        )

        findings = (
            f"<p>メンティーキャリア軌跡データ: メンター数="
            f"{len(mentee_outcomes):,}人、"
            f"メンティー総数={n_mentees:,}人。"
            f"平均固定効果変化量（指導後 − 指導前）: {mean_imp:.3f}。"
            f"散布図の各点は1メンティーを示し、"
            f"破線（y=x）は変化なしの参照線。"
            f"点の色は監督グループを区別する（個人識別なし）。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="メンティーキャリア軌跡（前後散布図）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_director_mentee", height=460
            ),
            method_note=(
                "fe_before / fe_after: メンター初回接触の前後期間における"
                "AKM個人固定効果推定値。"
                "期間区切りはメンタリングモジュールの定義に依存。"
                "色は監督グループ（集計レベル）を区別するが、"
                "個人IDは表示しない。"
            ),
            section_id="director_mentee",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

from .._spec import (  # noqa: E402
    ShrinkageSpec, SensitivityAxis,
)

SPEC = make_default_spec(
    name='mgmt_director_mentor',
    audience='hr',
    claim=(
        '監督ノード A の下流 5 年メンティー集団の theta_i 平均変化量 M̂_d が '
        '全監督下デビュー者の null 分布 (置換 1000 iter) を超える監督が存在する'
    ),
    identifying_assumption=(
        '監督下デビュー = 監督による機会割り当て を仮定しない。'
        '共起は機会割当の必要条件ではあるが十分条件ではない。'
        'メンティーの後続成長は監督効果と self-selection 効果の混合。'
    ),
    null_model=['N4', 'N5'],  # role-matched bootstrap + era-window resample
    sources=['credits', 'persons', 'anime'],
    meta_table='meta_hr_mentor_card',
    estimator='M̂_d = mean(Δθ for mentees within 5y of co-credit)',
    ci_estimator='bootstrap',
    n_resamples=1000,
    shrinkage=ShrinkageSpec(
        method='empirical_bayes_normal',
        n_threshold=30,
        prior='global mean of M̂_d distribution',
    ),
    sensitivity_grid=[
        SensitivityAxis(name='デビュー定義', values=['初クレジット', '初メイン役職']),
        SensitivityAxis(name='追跡窓', values=['5y', '10y']),
        SensitivityAxis(name='M ≥ 閾値', values=[3, 5, 10]),
    ],
    extra_limitations=[
        '「監督下デビュー」は初クレジット作品の監督との共起 — 別経路の指導関係は捕捉外',
        '監督個人の選好と機会割当の混在 — 因果効果としての解釈不可',
        'EB 縮小強度は global prior 強度に依存、上位 20 順位が ~25-40% 入れ替わる',
    ],
    forbidden_framing=['育成力', '弟子の質', '師匠の格', '優秀な指導者'],
    required_alternatives=2,
)
