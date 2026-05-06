"""後継計画マトリクス — v2 compliant.

Management brief: succession planning matrix.
- Section 1: Retirement risk score distribution (histogram)
- Section 2: Succession coverage rate (pie chart)
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import plotly.graph_objects as go

from ..html_templates import plotly_div_safe
from ..section_builder import KPICard, ReportSection, SectionBuilder
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


class MgmtSuccessionReport(BaseReportGenerator):
    name = "mgmt_succession"
    title = "後継計画マトリクス"
    subtitle = "退職リスクスコア + 後継候補カバレッジ"
    filename = "mgmt_succession.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load("succession_matrix")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(
                self._build_retirement_risk_distribution(sb, data)
            ),
            sb.build_section(
                self._build_succession_coverage(sb, data)
            ),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: Retirement risk distribution ─────────────────────

    def _build_retirement_risk_distribution(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        dist = data.get("retirement_risk_distribution", [])
        if not isinstance(dist, list):
            dist = []

        n_veterans = data.get("n_veterans")
        n_high_risk = data.get("n_high_risk")

        if not dist:
            findings = (
                "<p>退職リスク分布データが利用できません"
                "（succession_matrix.retirement_risk_distribution）。"
                "後継計画モジュールの実行が必要です。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="退職リスクスコア分布",
                findings_html=findings,
                section_id="succession_risk_dist",
            )

        risk_vals = [
            _safe_float(e.get("retire_risk"))
            for e in dist
            if isinstance(e, dict) and e.get("retire_risk") is not None
        ]
        n = len(risk_vals)

        if not risk_vals:
            findings = (
                "<p>退職リスクスコアの数値が取得できませんでした。"
                "retire_risk フィールドの確認が必要です。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="退職リスクスコア分布",
                findings_html=findings,
                section_id="succession_risk_dist",
            )

        mean_risk = sum(risk_vals) / n
        n_above_half = sum(1 for v in risk_vals if v > 0.5)

        n_vet_str = (
            f"{int(n_veterans):,}" if n_veterans is not None else str(n)
        )
        n_hr_str = (
            f"{int(n_high_risk):,}"
            if n_high_risk is not None
            else f"{n_above_half:,}"
        )

        fig = go.Figure(
            go.Histogram(
                x=risk_vals,
                nbinsx=25,
                marker_color="#E09BC2",
                opacity=0.8,
                xbins=dict(start=0.0, end=1.0, size=0.04),
                hovertemplate="リスク=%{x:.2f}: %{y:,}人<extra></extra>",
            )
        )
        fig.add_vline(
            x=0.5,
            line_dash="dash",
            line_color="#E07532",
            annotation_text="リスク閾値=0.5",
        )
        fig.add_vline(
            x=mean_risk,
            line_dash="dot",
            line_color="#FFB444",
            annotation_text=f"平均={mean_risk:.3f}",
        )
        fig.update_layout(
            title="退職リスクスコア分布（ベテラン人材）",
            xaxis_title="退職リスクスコア（0–1）",
            yaxis_title="人数",
            xaxis=dict(range=[0, 1]),
            height=420,
        )

        findings = (
            f"<p>退職リスクスコア分布: ベテラン人材数={n_vet_str}人"
            f"（リスクスコア取得済み: {n:,}人）。"
            f"退職リスク>0.5の人数: {n_hr_str}人。"
            f"平均退職リスク: {mean_risk:.3f}。"
            f"縦破線は閾値（0.5）、点線は平均値を示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        # v3: curated KPI strip
        kpis = [
            KPICard("ベテラン数", n_vet_str, "リスクスコア対象人物"),
            KPICard("候補者総数", f"{n:,}", "スコア取得済み人物数"),
            KPICard("平均退職リスク", f"{mean_risk:.3f}", "0–1、高いほど離脱リスク大"),
        ]

        return ReportSection(
            title="退職リスクスコア分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_succession_risk", height=420
            ),
            kpi_cards=kpis,
            chart_caption=(
                "横軸 = 退職リスクスコア（0–1、5 年以内のクレジット可視性喪失確率）、"
                "縦軸 = 人数（ベテラン人材対象）。"
                "橙の破線（閾値 0.5）はレポート表示用の参照値であり、"
                "個別意思決定の単一閾値として使用すべきでない。"
                "類似性指標（役職・ジャンルベクトルの cosine 類似度 × 共クレジット履歴）"
                "による近似であり、実際の後継選択は経営判断・個人意向に依存する。"
            ),
            method_note=(
                "退職リスクスコア: 生存モデル（Random Survival Forest）による"
                "5年以内の「翌年クレジット可視性喪失」確率推定値。"
                "閾値0.5はレポート表示用の参照値であり、"
                "意思決定の単一基準として使用すべきでない。"
                "ベテランの定義はmethod_notesフィールドに記載。"
            ),
            section_id="succession_risk_dist",
        )

    # ── Section 2: Succession coverage ─────────────────────────────

    def _build_succession_coverage(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        coverage = data.get("succession_coverage", {})
        if not isinstance(coverage, dict):
            coverage = {}

        if not coverage:
            findings = (
                "<p>後継カバレッジデータが利用できません"
                "（succession_matrix.succession_coverage）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="後継カバレッジ率",
                findings_html=findings,
                method_note=(
                    "後継カバレッジ = 退職リスク上位者のうち"
                    "上位k候補が存在する割合。"
                ),
                section_id="succession_coverage",
            )

        covered = int(_safe_float(coverage.get("covered")))
        uncovered = int(_safe_float(coverage.get("uncovered")))
        coverage_rate = _safe_float(coverage.get("coverage_rate"))

        total = covered + uncovered
        if total == 0:
            findings = (
                "<p>後継カバレッジ: covered=0、uncovered=0。"
                "データが空です。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="後継カバレッジ率",
                findings_html=findings,
                method_note=(
                    "後継カバレッジ = 退職リスク上位者のうち"
                    "上位k候補が存在する割合。"
                ),
                section_id="succession_coverage",
            )

        fig = go.Figure(
            go.Pie(
                labels=["カバー済み", "カバー未整備"],
                values=[covered, uncovered],
                marker=dict(colors=["#3BC494", "#E07532"]),
                textinfo="label+percent",
                hovertemplate=(
                    "%{label}: %{value:,}人 (%{percent})<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="後継カバレッジ率（退職リスク上位者）",
            height=400,
        )

        findings = (
            f"<p>後継カバレッジ率: {coverage_rate:.1%}。"
            f"カバー済み: {covered:,}人、"
            f"カバー未整備: {uncovered:,}人"
            f"（計{total:,}人）。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        # v3: curated KPI strip
        kpis = [
            KPICard("ベテラン数", f"{total:,}", "退職リスク上位対象人物"),
            KPICard("候補者総数", f"{covered + uncovered:,}", "カバー済み＋未整備"),
            KPICard("平均カバレッジ", f"{coverage_rate:.1%}", "後継候補が整備された割合"),
        ]

        return ReportSection(
            title="後継カバレッジ率",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_succession_coverage", height=400
            ),
            kpi_cards=kpis,
            chart_caption=(
                "円グラフは退職リスク上位者のうち後継候補が整備されている割合を示す。"
                "緑 = カバー済み（上位 k 候補が存在）、橙 = カバー未整備。"
                "類似性指標（役職・ジャンルベクトルの cosine 類似度 × 共クレジット履歴）"
                "による近似であり、実際の後継選択は経営判断・個人意向に依存する。"
                "個人特定を避けるため aggregate のみ公開し、個別ペアは表示しない。"
            ),
            method_note=(
                "後継カバレッジ = 退職リスク上位者のうち"
                "上位k候補が存在する割合。"
                "「カバー済み」: 退職リスク上位者の役割・技能に対して"
                "上位k人の後継候補が確認された人物。"
                "「カバー未整備」: 後継候補が不十分な人物。"
                "k の値はmethod_notesフィールドに記載。"
            ),
            section_id="succession_coverage",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

from .._spec import SensitivityAxis  # noqa: E402

SPEC = make_default_spec(
    name='mgmt_succession',
    audience='hr',
    claim=(
        'ベテラン (高 person FE θ_i, 5+ 年経験) × 候補者 (低-中 θ_i, 3+ 年経験) '
        'のペアの succession_score (cosine similarity of role/genre vector × '
        'co-credit history) が role-matched bootstrap null 95% 区間外'
    ),
    identifying_assumption=(
        '後継 = 役職 / ジャンル類似性 + 共同作業履歴を仮定。'
        '実際の後継選択は個人的関係 / 経営判断 / 本人意向に依存し本指標は近似のみ。'
        'aggregate 公開のみで個人特定はしない (legal risk 軽減)。'
    ),
    null_model=['N4', 'N5'],
    sources=['credits', 'persons', 'anime'],
    meta_table='meta_hr_succession',
    estimator='cosine_sim(role_vec_A, role_vec_B) × co_credit_count',
    ci_estimator='bootstrap', n_resamples=1000,
    sensitivity_grid=[
        SensitivityAxis(name='ベテラン経験閾値', values=['3y', '5y', '10y']),
        SensitivityAxis(name='類似性 metric',
                        values=['cosine', 'jaccard', 'weighted overlap']),
    ],
    extra_limitations=[
        '後継 ≠ 類似性 — 本指標は近似的表現',
        '個人的関係 / 経営判断 は捕捉外',
        'aggregate 公開のみ — 個別ペア提示はしない',
    ],
)
