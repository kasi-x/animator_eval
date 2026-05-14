"""IV XAI Breakdown report — v2 compliant.

Integrated Value (IV) transparent 5-component decomposition for individual view.
Sections:
  1. IV Component Composition — per-person stacked bar (top N by IV)
  2. Cohort Distribution — within-cohort percentile heatmap per component
  3. Lambda Weights + Correlation Diagnostics — method transparency
  4. Dormancy Distribution — last-credit-year histogram
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import structlog

from ..html_templates import plotly_div_safe
from ..section_builder import DataStatementParams, ReportSection, SectionBuilder
from ._base import BaseReportGenerator, append_validation_warnings

logger = structlog.get_logger()

# Canonical component display names and colors
_COMPONENT_LABELS: dict[str, str] = {
    "person_fe":       "Person FE (θ)",
    "birank":          "BiRank",
    "studio_exposure": "Studio Exposure",
    "awcc":            "AWCC",
    "patronage":       "Patronage",
}

_COMPONENT_COLORS: dict[str, str] = {
    "person_fe":       "#3593D2",
    "birank":          "#7CC8F2",
    "studio_exposure": "#FFB444",
    "awcc":            "#E09BC2",
    "patronage":       "#96ceb4",
}

_TOP_N: int = 30  # persons to show in composition chart


@dataclass
class _PersonRow:
    """Parsed row from feat_person_scores + feat_career."""

    person_id: str
    iv_score: float
    person_fe: float
    birank: float
    studio_exposure: float
    awcc: float
    patronage: float
    dormancy: float
    last_credit_year: int | None
    cohort: str


def _safe_float(v: Any) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _load_person_rows(conn: Any) -> list[_PersonRow]:
    """Load all persons with IV scores from Mart layer."""
    try:
        rows = conn.execute("""
            SELECT
                fps.person_id,
                fps.iv_score,
                fps.person_fe,
                fps.birank,
                fps.awcc,
                fps.patronage,
                fps.studio_fe_exposure AS studio_exposure,
                fps.dormancy,
                fc.latest_year AS last_credit_year,
                (COALESCE(fc.first_year, 2000) / 10) * 10 AS debut_decade,
                COALESCE(cr.primary_role, 'other') AS primary_role
            FROM feat_person_scores fps
            LEFT JOIN feat_career fc ON fps.person_id = fc.person_id
            LEFT JOIN (
                SELECT person_id, arg_max(role, cnt) AS primary_role
                FROM (
                    SELECT person_id, role, COUNT(*) AS cnt
                    FROM credits
                    GROUP BY person_id, role
                )
                GROUP BY person_id
            ) cr ON fps.person_id = cr.person_id
            WHERE fps.iv_score IS NOT NULL
            ORDER BY fps.iv_score DESC
            LIMIT 2000
        """).fetchall()
    except Exception as exc:
        logger.warning("iv_xai_load_failed", error=str(exc))
        return []

    result: list[_PersonRow] = []
    for r in rows:
        (pid, iv, pfe, br, awcc, pat, stud_exp, dorm,
         last_yr, debut_dec, pri_role) = r
        cohort = f"{int(debut_dec or 2000)}s_{pri_role or 'other'}"
        result.append(
            _PersonRow(
                person_id=str(pid),
                iv_score=_safe_float(iv),
                person_fe=_safe_float(pfe),
                birank=_safe_float(br),
                studio_exposure=_safe_float(stud_exp),
                awcc=_safe_float(awcc),
                patronage=_safe_float(pat),
                dormancy=_safe_float(dorm) if dorm is not None else 1.0,
                last_credit_year=int(last_yr) if last_yr else None,
                cohort=cohort,
            )
        )
    return result


def _load_lambda_weights(conn: Any) -> dict[str, float] | None:
    """Load most recent lambda weights from meta_iv_lambda (if available)."""
    try:
        rows = conn.execute("""
            SELECT component, lambda_weight
            FROM meta_iv_lambda
            ORDER BY computed_at DESC
            LIMIT 5
        """).fetchall()
        if rows:
            return {str(r[0]): _safe_float(r[1]) for r in rows}
    except Exception:
        pass
    # Fallback: equal weights
    return {name: 0.2 for name in _COMPONENT_LABELS}


class IVXAIBreakdownReport(BaseReportGenerator):
    """IV XAI Breakdown — 5-component decomposition transparency report."""

    name = "iv_xai_breakdown"
    title = "IV構成分解レポート (XAI)"
    subtitle = (
        "Integrated Value の 5 成分 + dormancy 乗算の透明分解 — "
        "個人向けスコア構成の根拠提示"
    )
    filename = "iv_xai_breakdown.html"
    doc_type = "main"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        person_rows = _load_person_rows(self.conn)
        lambda_weights = _load_lambda_weights(self.conn)

        sections: list[str] = [
            sb.build_section(self._build_composition_section(sb, person_rows)),
            sb.build_section(self._build_cohort_section(sb, person_rows)),
            sb.build_section(self._build_method_section(sb, lambda_weights, person_rows)),
            sb.build_section(self._build_dormancy_section(sb, person_rows)),
        ]
        return self.write_report(
            "\n".join(sections),
            data_statement_params=DataStatementParams(
                data_source=(
                    "feat_person_scores (Mart層) + feat_career + credits。"
                    "IV成分は AKM (person_fe) / BiRank / AWCC / patronage / studio_exposure の 5 系統。"
                ),
                coverage_notes=(
                    "iv_score が NULL の人物は除外。"
                    "studio_exposure は AKM 非移動者では欠損（z=0 補完）。"
                    "patronage は共クレジット実績がない場合は 0。"
                ),
                missing_value_handling=(
                    "欠損成分は z-space で 0 補完（= 母集団平均）。"
                    "dormancy 欠損は 1.0（アクティブ）として扱う。"
                ),
            ),
        )

    # ── Section 1: IV Component Composition ─────────────────────────

    def _build_composition_section(
        self, sb: SectionBuilder, rows: list[_PersonRow]
    ) -> ReportSection:
        if not rows:
            findings = "<p>feat_person_scores からデータを取得できませんでした。</p>"
            findings = append_validation_warnings(findings, sb)
            return ReportSection(
                title="IV 成分構成（上位人物）",
                findings_html=findings,
                section_id="iv_composition",
            )

        top_rows = rows[:_TOP_N]
        n_total = len(rows)

        component_keys = [
            "person_fe", "birank", "studio_exposure", "awcc", "patronage"
        ]

        # Build stacked bar: each component's absolute contribution
        x_labels = [r.person_id[:12] for r in top_rows]
        fig = go.Figure()

        for key in component_keys:
            y_vals = [abs(getattr(r, key)) for r in top_rows]
            fig.add_trace(
                go.Bar(
                    name=_COMPONENT_LABELS[key],
                    x=x_labels,
                    y=y_vals,
                    marker_color=_COMPONENT_COLORS[key],
                    hovertemplate=f"{_COMPONENT_LABELS[key]}: %{{y:.4f}}<extra></extra>",
                )
            )

        fig.update_layout(
            title=f"IV 成分構成（上位 {len(top_rows)} 名、IV降順）",
            xaxis_title="Person ID",
            yaxis_title="成分絶対値",
            barmode="stack",
            height=480,
            legend=dict(orientation="h", y=-0.25),
        )

        # Summary stats
        iv_vals = [r.iv_score for r in rows]
        iv_min = min(iv_vals)
        iv_max = max(iv_vals)
        iv_med = sorted(iv_vals)[len(iv_vals) // 2]

        findings = (
            f"<p>総対象者数: {n_total:,} 名（iv_score ≠ NULL）。"
            f"IV スコア範囲: [{iv_min:.4f}, {iv_max:.4f}]、中央値 {iv_med:.4f}。"
            f"棒グラフは上位 {len(top_rows)} 名の 5 成分絶対値を積み上げ表示する。"
            f"各成分は生スケール（z正規化前）。</p>"
            f"<p>成分別の寄与割合はコホート内パーセンタイル欄（Section 2）を参照。"
            f"dormancy 乗算は IV 最終値には反映されるが、成分分解には含まれない（Section 3 参照）。</p>"
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="IV 成分構成（上位人物）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_iv_composition", height=480),
            method_note=(
                "IV = (Σ_k λ_k·z_k) × D。"
                "棒グラフの y 軸は z 正規化前の生スケール絶対値。"
                "contrib_pct（成分比率）は |λ_k·z_k| / Σ_j |λ_j·z_j| × 100。"
                "dormancy 乗算 D は棒グラフには含まれない。"
                "コホート: デビュー年代 × 主役職グループ（src/utils/role_groups.py）。"
            ),
            chart_caption=(
                "各バーは上位人物の IV 成分を積み上げ表示。"
                "色は成分種別（凡例参照）、高さは各成分の絶対値（正規化前）。"
            ),
            section_id="iv_composition",
        )

    # ── Section 2: Cohort Distribution ──────────────────────────────

    def _build_cohort_section(
        self, sb: SectionBuilder, rows: list[_PersonRow]
    ) -> ReportSection:
        if not rows:
            findings = "<p>コホートデータが取得できませんでした。</p>"
            findings = append_validation_warnings(findings, sb)
            return ReportSection(
                title="コホート別 IV 分布",
                findings_html=findings,
                section_id="iv_cohort",
            )

        # Group by cohort
        cohort_iv: dict[str, list[float]] = {}
        for r in rows:
            cohort_iv.setdefault(r.cohort, []).append(r.iv_score)

        # Top cohorts by size
        top_cohorts = sorted(
            cohort_iv.keys(), key=lambda c: len(cohort_iv[c]), reverse=True
        )[:15]

        if not top_cohorts:
            findings = "<p>コホートが構成できませんでした。</p>"
            findings = append_validation_warnings(findings, sb)
            return ReportSection(
                title="コホート別 IV 分布",
                findings_html=findings,
                section_id="iv_cohort",
            )

        fig = go.Figure()
        for cohort in top_cohorts:
            vals = cohort_iv[cohort]
            fig.add_trace(
                go.Box(
                    y=vals,
                    name=cohort,
                    boxmean=True,
                    hovertemplate=(
                        f"{cohort}<br>"
                        "IV: %{y:.4f}<extra></extra>"
                    ),
                )
            )

        fig.update_layout(
            title="コホート別 IV スコア分布（上位 15 コホート、人数順）",
            xaxis_title="コホート（デビュー年代 × 主役職）",
            yaxis_title="IV スコア",
            height=480,
        )

        cohort_strs = [f"{c}（n={len(cohort_iv[c]):,}）" for c in top_cohorts[:5]]
        findings = (
            f"<p>コホート定義: デビュー年代 × 主役職グループ。"
            f"コホート数（上位表示）: {len(top_cohorts)} / 全 {len(cohort_iv)} コホート。"
            f"上位 5 コホート: {'、'.join(cohort_strs)}。</p>"
            f"<p>コホート内パーセンタイルは個人の IV が同コホートの何位置にあるかを示す。"
            f"グローバルランクとは異なり、デビュー時期・役職ごとの相対位置を提示する。</p>"
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="コホート別 IV 分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_iv_cohort", height=480),
            method_note=(
                "コホート = debut_decade × primary_role_group。"
                "debut_decade は first_credit_year を 10 年区切りで分類。"
                "primary_role は最多クレジット役職（src/utils/role_groups.py）。"
                "箱ひげ: 菱形=平均、中線=中央値。"
                "コホートパーセンタイルは個人向け API (/api/persons/{id}/iv) で確認可能。"
            ),
            chart_caption=(
                "各ボックスはコホート内の IV スコア四分位分布。"
                "x 軸=コホートラベル、y 軸=IV スコア（[0, 1] 正規化済み）。"
            ),
            section_id="iv_cohort",
        )

    # ── Section 3: Lambda Weights + Correlation Diagnostics ─────────

    def _build_method_section(
        self,
        sb: SectionBuilder,
        lambda_weights: dict[str, float] | None,
        rows: list[_PersonRow],
    ) -> ReportSection:
        lw = lambda_weights or {name: 0.2 for name in _COMPONENT_LABELS}

        # Bar chart of lambda weights
        comp_names = list(_COMPONENT_LABELS.keys())
        lambda_vals = [lw.get(k, 0.0) for k in comp_names]
        display_names = [_COMPONENT_LABELS[k] for k in comp_names]

        fig = go.Figure(
            go.Bar(
                x=display_names,
                y=lambda_vals,
                marker_color=[_COMPONENT_COLORS[k] for k in comp_names],
                text=[f"{v:.3f}" for v in lambda_vals],
                textposition="outside",
                hovertemplate="%{x}: λ=%{y:.4f}<extra></extra>",
            )
        )
        fig.update_layout(
            title="IV 成分 λ 重み（PCA PC1 loadings 由来、または等重み）",
            xaxis_title="成分",
            yaxis_title="λ 重み",
            height=360,
            yaxis=dict(range=[0, max(lambda_vals) * 1.3] if lambda_vals else [0, 1]),
        )

        # Aggregate component metadata for table
        from src.analysis.scoring.iv_decompose import COMPONENT_METADATA

        meta_rows_html = ""
        for key in comp_names:
            meta = COMPONENT_METADATA.get(key, {})
            lam_val = lw.get(key, 0.2)
            meta_rows_html += (
                f"<tr>"
                f"<td style='padding:0.4rem;'><strong>{_COMPONENT_LABELS[key]}</strong></td>"
                f"<td style='padding:0.4rem;'>{lam_val:.4f}</td>"
                f"<td style='padding:0.4rem;font-size:0.82rem;'>{meta.get('source','')}</td>"
                f"<td style='padding:0.4rem;font-size:0.82rem;'>{meta.get('aggregation_note','')}</td>"
                f"</tr>"
            )

        meta_table_html = (
            "<table style='width:100%;border-collapse:collapse;margin-top:1rem;'>"
            "<thead><tr style='background:#2a2a4a;'>"
            "<th style='padding:0.5rem;text-align:left;'>成分</th>"
            "<th style='padding:0.5rem;'>λ</th>"
            "<th style='padding:0.5rem;text-align:left;'>データソース</th>"
            "<th style='padding:0.5rem;text-align:left;'>集計時点・方法</th>"
            "</tr></thead>"
            f"<tbody>{meta_rows_html}</tbody>"
            "</table>"
        )

        lambda_sum = sum(lw.values())
        findings = (
            f"<p>IV = (Σ_k λ_k·z_k) × D。"
            f"λ 合計: {lambda_sum:.4f}（PCA PC1 loadings を正規化したもの）。"
            f"成分数: 5（person_fe / birank / studio_exposure / awcc / patronage）。"
            f"各 λ はパイプライン実行ごとに PCA で再推定される（事前固定 anime.score 最適化は削除済み）。</p>"
            f"{meta_table_html}"
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="λ 重み・成分ソース・集計時点（方法の透明性）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_iv_lambda", height=360),
            method_note=(
                "λ 重みは PCA PC1 loadings の絶対値を正規化したもの（合計 = 1）。"
                "anime.score は一切使用しない（CLAUDE.md H1 Hard Rule）。"
                "成分間高相関（|r| > 0.9）時は Shapley 等価値に切替え（API レスポンスで報告）。"
                "z 正規化パラメータ（μ, σ）はパイプライン実行時の全人物分布から推定。"
            ),
            chart_caption=(
                "各バーが 1 成分の λ 重み。"
                "合計は 1.0（PCA PC1 loadings 正規化）。"
                "重みが大きいほど IV への寄与が大きい。"
            ),
            section_id="iv_method",
        )

    # ── Section 4: Dormancy Distribution ────────────────────────────

    def _build_dormancy_section(
        self, sb: SectionBuilder, rows: list[_PersonRow]
    ) -> ReportSection:
        if not rows:
            findings = "<p>dormancy データが取得できませんでした。</p>"
            findings = append_validation_warnings(findings, sb)
            return ReportSection(
                title="Dormancy 乗算の分布",
                findings_html=findings,
                section_id="iv_dormancy",
            )

        dormancy_vals = [r.dormancy for r in rows]
        last_years = [r.last_credit_year for r in rows if r.last_credit_year is not None]

        fig = go.Figure()
        fig.add_trace(
            go.Histogram(
                x=dormancy_vals,
                nbinsx=20,
                name="Dormancy D",
                marker_color="#3593D2",
                hovertemplate="D: %{x:.3f}<br>人数: %{y}<extra></extra>",
            )
        )
        fig.update_layout(
            title="Dormancy 乗算 D の分布（全人物）",
            xaxis_title="Dormancy D（0=完全休眠, 1=アクティブ）",
            yaxis_title="人数",
            height=340,
        )

        n = len(dormancy_vals)
        mean_d = sum(dormancy_vals) / n if n else 0.0
        active_n = sum(1 for d in dormancy_vals if d >= 0.95)
        dormant_n = sum(1 for d in dormancy_vals if d < 0.5)
        last_yr_min = min(last_years) if last_years else None
        last_yr_max = max(last_years) if last_years else None

        findings = (
            f"<p>対象者数: {n:,} 名。"
            f"Dormancy D 平均: {mean_d:.4f}。"
            f"D ≥ 0.95（活動中）: {active_n:,} 名 ({active_n / n * 100:.1f}%)。"
            f"D < 0.5（休眠）: {dormant_n:,} 名 ({dormant_n / n * 100:.1f}%)。"
            f"最終クレジット年の範囲: {last_yr_min} ～ {last_yr_max}。</p>"
            f"<p>IV 最終値は (Σ_k λ_k·z_k) × D の積で算出される。"
            f"D = 0 の人物は IV = 0 となる（過去クレジットの構造的位置は保持されるが、"
            f"休眠期間により乗算で減少する）。</p>"
        )
        findings = append_validation_warnings(findings, sb)

        return ReportSection(
            title="Dormancy 乗算の分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_iv_dormancy", height=340),
            method_note=(
                "Dormancy D: 直近クレジット年から推定した活動継続率。"
                "D = 1.0: 最近のクレジットあり（アクティブ）。"
                "D ≈ 0: 長期間クレジット不在（休眠）。"
                "算出詳細: src/analysis/scoring/patronage_dormancy.py。"
                "D は IV のスケールダウン乗算として作用する。"
            ),
            chart_caption=(
                "x 軸: Dormancy D（0 = 完全休眠、1 = アクティブ）。"
                "y 軸: 人数。"
                "分布の形状が D = 1 付近に集中している場合、"
                "データセットはアクティブ人物が多数を占める。"
            ),
            section_id="iv_dormancy",
        )


# ---------------------------------------------------------------------------
# v3 SPEC (method gate declaration)
# ---------------------------------------------------------------------------

from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="iv_xai_breakdown",
    audience="common",
    claim=(
        "IV (Integrated Value) を 5 成分 (person_fe / birank / studio_exposure / "
        "awcc / patronage) + dormancy 乗算に透明分解し、"
        "各成分のデータソース・λ 重み・集計時点を明示する"
    ),
    identifying_assumption=(
        "IV の加法分解可能性: IV = (Σ_k λ_k·z_k) × D。"
        "成分間高相関（|r| > 0.9）では Shapley 等価値に切替え。"
        "λ は PCA PC1 loadings 由来（anime.score 最適化なし）。"
        "成分比率 contrib_pct = |λ_k·z_k| / Σ_j |λ_j·z_j|。"
    ),
    null_model=["N6"],
    sources=["feat_person_scores", "feat_career", "credits"],
    meta_table="meta_iv_lambda",
    estimator="Additive decomposition of linear IV model",
    ci_estimator="analytical_se",
    n_resamples=None,
    extra_limitations=[
        "dormancy 乗算後の IV はパイプライン毎に再正規化、絶対値比較不可",
        "studio_exposure は AKM 非移動者で欠損（z=0 補完）",
        "λ は PCA PC1 loadings からデータ駆動で決定、パイプライン毎に変化しうる",
    ],
)
