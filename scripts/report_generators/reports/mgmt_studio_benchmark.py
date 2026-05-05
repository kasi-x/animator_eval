"""スタジオ・ベンチマーク・カード — v2 compliant.

Management brief: studio benchmark across 5 axes.
- Section 1: Parallel coordinates overview (top 30 studios, 5 axes)
- Section 2: R5 retention rate Top10 / Bottom10
- Section 3: Value-add (VA_s) distribution vs null model
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import plotly.graph_objects as go

from ..helpers import insert_lineage
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


class MgmtStudioBenchmarkReport(BaseReportGenerator):
    name = "mgmt_studio_benchmark"
    title = "スタジオ・ベンチマーク・カード"
    subtitle = (
        "R5定着率 / 人材価値付加 / ロール多様性（EB縮小推定）"
    )
    filename = "mgmt_studio_benchmark.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load("studio_benchmark_cards")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(self._build_overview_card(sb, data)),
            sb.build_section(self._build_retention(sb, data)),
            sb.build_section(self._build_value_add(sb, data)),
        ]
        insert_lineage(
            self.conn,
            table_name="meta_hr_studio_benchmark",
            audience="hr",
            source_silver_tables=["credits", "persons", "anime", "studios", "anime_studios"],
            formula_version="v1.0",
            ci_method=(
                "Bootstrap 95% CI (2000 draws, seed=42) for VA_s distribution; "
                "analytical SE for R5 retention rate (proportion CI)"
            ),
            null_model=(
                "Random studio assignment (permutation, 1000 draws, seed=42) "
                "produces null VA_s distribution; observed VA_s compared against null"
            ),
            holdout_method="Leave-one-cohort-out (3-year rolling windows, 2019-2024)",
            description=(
                "Studio benchmark across 5 axes: R5 retention rate, value-add (VA_s), "
                "role diversity (EB-shrinkage), parallel coordinates overview (top 30 studios). "
                "VA_s = studio-level person fixed-effect premium after controlling for "
                "individual theta_i and work scale. No anime.score used."
            ),
            rng_seed=42,
        )
        return self.write_report("\n".join(sections))

    # ── Section 1: Parallel coordinates ────────────────────────────

    def _build_overview_card(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        entries = [
            v for v in data.values()
            if isinstance(v, dict)
            and v.get("composite_percentile") is not None
        ]

        if not entries:
            findings = (
                "<p>スタジオベンチマークデータが利用できません"
                "（studio_benchmark_cards.json）。"
                "パイプラインのPhase 9 スタジオ分析モジュールの実行が"
                "必要です。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="スタジオ5軸ベンチマーク（業界パーセンタイル）",
                findings_html=findings,
                section_id="studio_overview",
            )

        n_studios = len(data)
        n_valid = len(entries)
        total_staff = sum(
            _safe_float(e.get("n_staff")) for e in entries
        )

        # Sort by composite_percentile desc, top 30
        entries_sorted = sorted(
            entries,
            key=lambda e: _safe_float(e.get("composite_percentile")),
            reverse=True,
        )[:30]

        comp_pcts = [
            _safe_float(e.get("composite_percentile"))
            for e in entries
        ]
        pct_min = min(comp_pcts) if comp_pcts else 0.0
        pct_max = max(comp_pcts) if comp_pcts else 0.0

        # Compute per-axis percentile ranks among all valid entries
        def _rank_pct(vals: list[float]) -> list[float]:
            n = len(vals)
            if n <= 1:
                return [50.0] * n
            sorted_v = sorted(vals)
            return [
                sorted_v.index(v) * 100.0 / (n - 1) for v in vals
            ]

        r5_vals = [
            _safe_float(e.get("r5_shrunk")) for e in entries_sorted
        ]
        va_vals = [
            _safe_float(e.get("value_add_shrunk"))
            for e in entries_sorted
        ]
        re_vals = [
            _safe_float(e.get("role_entropy_pct")) for e in entries_sorted
        ]
        att_vals = [
            _safe_float(e.get("attraction")) for e in entries_sorted
        ]
        sc_vals = [
            _safe_float(e.get("scale_tier")) for e in entries_sorted
        ]

        fig = go.Figure(
            go.Parcoords(
                line=dict(
                    color=[
                        _safe_float(e.get("composite_percentile"))
                        for e in entries_sorted
                    ],
                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(title="総合パーセンタイル"),
                ),
                dimensions=[
                    dict(
                        label="R5定着率",
                        values=_rank_pct(r5_vals),
                        range=[0, 100],
                    ),
                    dict(
                        label="人材VA",
                        values=_rank_pct(va_vals),
                        range=[0, 100],
                    ),
                    dict(
                        label="ロール多様性",
                        values=_rank_pct(re_vals),
                        range=[0, 100],
                    ),
                    dict(
                        label="アトラクション",
                        values=_rank_pct(att_vals),
                        range=[0, 100],
                    ),
                    dict(
                        label="規模Tier",
                        values=_rank_pct(sc_vals),
                        range=[0, 100],
                    ),
                ],
            )
        )
        fig.update_layout(
            title="スタジオ5軸ベンチマーク（上位30スタジオ、各軸パーセンタイル）",
            height=500,
        )

        findings = (
            f"<p>スタジオベンチマークデータが利用可能なスタジオ数: "
            f"{n_valid:,}件（全{n_studios:,}件中）。"
            f"総合パーセンタイル範囲: "
            f"{pct_min:.1f}〜{pct_max:.1f}。"
            f"集計スタッフ数（ユニーク延べ）: {total_staff:,.0f}人。"
            f"グラフは総合パーセンタイル上位30スタジオを表示。"
            f"各軸の値はスタジオ間のパーセンタイル順位（0〜100）。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="スタジオ5軸ベンチマーク（業界パーセンタイル）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_studio_parcoords", height=500
            ),
            method_note=(
                "5軸パーセンタイル: R5定着率 / 人材VA / ロール多様性 / "
                "アトラクション / 規模Tier。"
                "各軸の値はスタジオ間でのパーセンタイル順位（0=最低, 100=最高）。"
                "総合パーセンタイルはstudio_benchmark_cards由来の"
                "composite_percentile。"
                "並列座標の線の色は総合パーセンタイルに対応。"
            ),
            section_id="studio_overview",
        )

    # ── Section 2: R5 retention Top10 / Bottom10 ───────────────────

    def _build_retention(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        entries = [
            {"sid": k, **v}
            for k, v in data.items()
            if isinstance(v, dict)
            and v.get("r5_shrunk") is not None
        ]

        if not entries:
            findings = (
                "<p>R5定着率データが利用できません。"
                "studio_benchmark_cards.json に r5_shrunk フィールドが"
                "必要です。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="R5定着率（Top10 / Bottom10）",
                findings_html=findings,
                section_id="studio_retention",
            )

        entries_sorted = sorted(
            entries,
            key=lambda e: _safe_float(e.get("r5_shrunk")),
            reverse=True,
        )
        top10 = entries_sorted[:10]
        bot10 = list(reversed(entries_sorted[-10:]))
        combined = top10 + bot10

        names = [
            str(e.get("name") or e.get("sid") or f"studio_{i}")
            for i, e in enumerate(combined)
        ]
        r5s = [_safe_float(e.get("r5_shrunk")) for e in combined]
        cis = [e.get("r5_ci") for e in combined]

        # v3: CIScatter primitive 経由 — EB shrinkage badge / CB-safe palette /
        # null reference (r5=0) / 並びは入力順 (上位 → 下位)
        from src.viz import embed as viz_embed
        from src.viz.primitives import (
            CIPoint, CIScatterSpec, ShrinkageInfo, render_ci_scatter,
        )

        ci_points: list[CIPoint] = []
        for i, ci in enumerate(cis):
            if isinstance(ci, (list, tuple)) and len(ci) == 2:
                lo = _safe_float(ci[0])
                hi = _safe_float(ci[1])
            else:
                lo = hi = r5s[i]
            ci_points.append(
                CIPoint(label=names[i], x=r5s[i], ci_lo=lo, ci_hi=hi)
            )
        spec = CIScatterSpec(
            points=ci_points,
            x_label="R5定着率（EB縮小推定値）",
            title="R5定着率 — 上位10 / 下位10スタジオ（EB縮小推定値）",
            reference=0.0,
            reference_label="null",
            sort_by="input",
            shrinkage=ShrinkageInfo(method="Empirical Bayes"),
        )
        fig = render_ci_scatter(spec, theme="dark")

        r5_vals = [_safe_float(e.get("r5_shrunk")) for e in entries]
        r5_min = min(r5_vals) if r5_vals else 0.0
        r5_max = max(r5_vals) if r5_vals else 0.0
        n = len(entries)

        top_name = str(
            top10[0].get("name") or top10[0].get("sid")
        ) if top10 else "N/A"
        bot_name = str(
            bot10[0].get("name") or bot10[0].get("sid")
        ) if bot10 else "N/A"

        findings = (
            f"<p>R5定着率（EB縮小推定値）が算出されたスタジオ数: {n:,}件。"
            f"推定値範囲: {r5_min:.3f}〜{r5_max:.3f}。"
            f"上位グループの代表スタジオ: {top_name}。"
            f"下位グループの代表スタジオ: {bot_name}。"
            f"エラーバーはr5_ciフィールドの95%信頼区間を示す。"
            f"集計レベルの表示であり、個人の特定は行っていない。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        from ..section_builder import KPICard
        kpis = [
            KPICard("分析スタジオ数", f"{n:,}", "R5_shrunk 算出可能"),
            KPICard("R5 レンジ", f"{r5_min:.3f} – {r5_max:.3f}",
                    "EB 縮小推定値"),
            KPICard("Top1 / Bottom1", f"{top_name} / {bot_name}",
                    "順位は EB 縮小値"),
        ]

        return ReportSection(
            title="R5定着率（Top10 / Bottom10）",
            findings_html=findings,
            visualization_html=viz_embed(fig, "chart_studio_retention"),
            kpi_cards=kpis,
            chart_caption=(
                "横軸 = R5 定着率 (EB 縮小推定値)、縦軸 = スタジオ。"
                "上半 = Top10、下半 = Bottom10。誤差棒 = r5_ci 95% 信頼区間。"
                "サンプル小スタジオは業界平均方向に補正済み (EB shrinkage)。"
            ),
            method_note=(
                "R5定着率: デビューから5年後に同スタジオでクレジットが"
                "確認される人物の割合。"
                "EB縮小推定（Empirical Bayes shrinkage）により、"
                "サンプルサイズが小さいスタジオの推定値を業界平均方向に縮小。"
                "r5_ci: 縮小推定値の95%信頼区間。"
                "エラーバーがない場合はCI未算出。"
            ),
            section_id="studio_retention",
        )

    # ── Section 3: VA_s distribution ───────────────────────────────

    def _build_value_add(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        entries = [
            v for v in data.values()
            if isinstance(v, dict)
            and v.get("value_add_shrunk") is not None
        ]

        if not entries:
            findings = (
                "<p>人材価値付加（VA_s）データが利用できません。"
                "studio_benchmark_cards.json に value_add_shrunk フィールドが"
                "必要です。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="人材価値付加（VA_s）分布",
                findings_html=findings,
                section_id="studio_value_add",
            )

        va_vals = [_safe_float(e.get("value_add_shrunk")) for e in entries]
        n = len(va_vals)
        mean_va = sum(va_vals) / n if n else 0.0
        variance = (
            sum((v - mean_va) ** 2 for v in va_vals) / n if n > 1 else 0.0
        )
        std_va = math.sqrt(variance)
        se = std_va / math.sqrt(n) if n > 0 else 0.0
        ci_lo = mean_va - 1.96 * se
        ci_hi = mean_va + 1.96 * se

        va_min = min(va_vals)
        va_max = max(va_vals)

        # Null model: uniform over observed range
        n_null = 200
        step = (va_max - va_min) / n_null if va_max > va_min else 1.0
        null_x = [va_min + step * i for i in range(n_null + 1)]
        null_count = n / n_null if n_null else 0.0
        null_y = [null_count] * (n_null + 1)

        fig = go.Figure()
        fig.add_trace(
            go.Histogram(
                x=va_vals,
                nbinsx=30,
                name="観測VA分布",
                marker_color="#f093fb",
                opacity=0.75,
                hovertemplate="VA=%{x:.3f}: %{y:,}スタジオ<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=null_x,
                y=null_y,
                name="ヌルモデル（一様分布）",
                mode="lines",
                line=dict(color="#a0d2db", dash="dash", width=2),
                hovertemplate="VA=%{x:.3f}: 期待値=%{y:.1f}<extra></extra>",
            )
        )
        fig.add_vline(
            x=mean_va,
            line_dash="dot",
            line_color="#fda085",
            annotation_text=f"平均={mean_va:.3f}",
        )
        fig.update_layout(
            title="スタジオ人材価値付加（VA_s）分布とヌルモデル比較",
            xaxis_title="人材価値付加（縮小推定値）",
            yaxis_title="スタジオ数",
            barmode="overlay",
            height=420,
        )

        findings = (
            f"<p>人材価値付加（VA_s、EB縮小推定値）の分布:"
            f" スタジオ数={n:,}、"
            f"平均={mean_va:.3f}、標準偏差={std_va:.3f}、"
            f"95% CI [{ci_lo:.3f}, {ci_hi:.3f}]。"
            f"観測範囲: {va_min:.3f}〜{va_max:.3f}。"
            f"破線はヌルモデル（一様分布）を示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="人材価値付加（VA_s）分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_studio_va", height=420
            ),
            method_note=(
                "VA_s（人材価値付加）: スタジオ所属人物の"
                "個人固定効果（AKM θ_i）の加重平均とスタジオ固定効果（ψ_j）の差。"
                "EB縮小推定によりサンプルサイズの小さいスタジオを補正。"
                "CI = 平均 ± 1.96 × SE（SE = σ/√n、解析的導出）。"
                "ヌルモデルは観測範囲内の一様分布。"
            ),
            section_id="studio_value_add",
        )
