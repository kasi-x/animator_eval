"""O2 Mid-Management Pipeline report — v2 compliant.

中堅枯渇 (mid-management pipeline depletion) analysis:
- Section 1: 役職進行年数 KM curve (cohort 層別, 95% CI)
- Section 2: スタジオ別パイプライン詰まり指標 (bootstrap CI, top/bottom 20)
- Section 3: 昇進ファネル (動画 → 原画 → 作監 → 監督)

Uses only structural data (credit records, role, year).  Viewer ratings are not used.
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

# Pipeline pairs for KM analysis
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

# v3: Okabe-Ito CB-safe palette (8 + 2 from extended palette for >8 cohorts)
from src.viz.palettes import OKABE_ITO_DARK  # noqa: E402

_COHORT_COLORS = list(OKABE_ITO_DARK) + ["#7CC8F2", "#E09BC2"]

# Pre-condition: minimum credits per role
_MIN_ROLE_COUNT = 1000


def _hex_to_rgb(hex_color: str) -> str:
    """Convert #RRGGBB to 'R,G,B' string for rgba() use."""
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"{r},{g},{b}"


class O2MidManagementReport(BaseReportGenerator):
    """O2: 中堅枯渇 — mid-management pipeline analysis."""

    name = "o2_mid_management"
    title = "中堅枯渇パイプライン分析"
    subtitle = (
        "動画 → 原画 → 作監 → 監督 進行年数 KM curve "
        "+ スタジオ別パイプライン詰まり指標"
    )
    filename = "o2_mid_management.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        from src.analysis.career.role_progression import (
            PIPELINE_ROLES,
            compute_progression_years,
            compute_role_counts,
            compute_studio_blockage,
            km_role_tenure,
            logrank_cohort_comparison,
        )

        sb = SectionBuilder()

        # Pre-condition check
        role_counts = compute_role_counts(self.conn)
        for role in PIPELINE_ROLES:
            cnt = role_counts.get(role, 0)
            if cnt < _MIN_ROLE_COUNT:
                # Stop-if: insufficient data — emit placeholder and return
                body = self._build_insufficient_data_body(sb, role, cnt)
                return self.write_report(body)

        # Build sections
        sections: list[str] = [
            sb.build_section(
                self._build_km_section(sb, compute_progression_years, km_role_tenure, logrank_cohort_comparison)
            ),
            sb.build_section(
                self._build_blockage_section(sb, compute_studio_blockage)
            ),
            sb.build_section(
                self._build_funnel_section(sb, role_counts)
            ),
        ]

        insert_lineage(
            self.conn,
            table_name="meta_o2_mid_management",
            audience="hr",
            source_silver_tables=["credits", "persons", "anime"],
            formula_version="v1.0",
            ci_method=(
                "Greenwood formula for KM survival curves (95% CI); "
                "bootstrap CI (n=1000 resamples, seed=42) for studio blockage score"
            ),
            null_model=(
                "Industry median as reference for studio blockage score; "
                "multivariate log-rank test for cohort comparison"
            ),
            holdout_method="Not applicable (descriptive survival analysis)",
            description=(
                "Mid-management pipeline analysis: Kaplan-Meier survival curves "
                "for role progression (in_between→key_animator→animation_director→director), "
                "stratified by 5-year debut cohort. "
                "Studio blockage score = studio_median(progression_years) - industry_median. "
                "Positive score = slower-than-industry progression. "
                "Credit visibility loss, not career exit, is the observed event."
            ),
            rng_seed=42,
        )

        return self.write_report("\n".join(sections))

    # ── Pre-condition failure ─────────────────────────────────────────────────

    def _build_insufficient_data_body(
        self, sb: SectionBuilder, missing_role: str, count: int
    ) -> str:
        label = _ROLE_LABELS.get(missing_role, missing_role)
        sec = ReportSection(
            title="データ不足 / Insufficient Data",
            findings_html=(
                f"<p>役職 <strong>{label} ({missing_role})</strong> の"
                f"クレジット件数が {count:,} 件で、分析閾値 {_MIN_ROLE_COUNT:,} 件を下回っています。"
                f"パイプライン分析を実行するには各役職の個人数が十分である必要があります。</p>"
                f"<p>Role <strong>{label} ({missing_role})</strong> has {count:,} credits, "
                f"below the analysis threshold of {_MIN_ROLE_COUNT:,}. "
                f"Pipeline analysis requires sufficient person counts per role.</p>"
            ),
            method_note=(
                "Pre-condition: each pipeline role must have >= 1,000 distinct persons. "
                f"Role '{missing_role}' has {count:,} — analysis aborted."
            ),
            section_id="insufficient_data",
        )
        return sb.build_section(sec)

    # ── Section 1: KM curves ──────────────────────────────────────────────────

    def _build_km_section(
        self, sb: SectionBuilder, compute_progression_years, km_role_tenure, logrank_cohort_comparison
    ) -> ReportSection:
        fig = go.Figure()
        findings_parts: list[str] = []

        for role_from, role_to, pair_label in _PIPELINE_PAIRS:
            records = compute_progression_years(self.conn, role_from, role_to)
            km_results = km_role_tenure(records)
            logrank = logrank_cohort_comparison(records)

            if not km_results:
                findings_parts.append(
                    f"<li><strong>{pair_label}</strong>: KMデータ不足</li>"
                )
                continue

            # Aggregate across cohorts for Findings text
            all_medians = [
                r.median_survival for r in km_results.values()
                if r.median_survival is not None
            ]
            overall_median_str = (
                f"{sum(all_medians)/len(all_medians):.1f}年"
                if all_medians else "算出不能"
            )
            lr_str = (
                f"多変量ログランク p={logrank['p_value']:.3f}"
                if logrank.get("p_value") is not None else ""
            )
            findings_parts.append(
                f"<li><strong>{pair_label}</strong>: "
                f"コホート別中央値平均 {overall_median_str}"
                f"{', ' + lr_str if lr_str else ''}, "
                f"コホート数={len(km_results)}</li>"
            )

            # Add traces per cohort
            for idx, (cohort_label, km) in enumerate(sorted(km_results.items())):
                color = _COHORT_COLORS[idx % len(_COHORT_COLORS)]
                trace_name = f"{pair_label} {cohort_label}"

                if km.ci_upper and km.ci_lower and len(km.ci_upper) == len(km.timeline):
                    x_fill = list(km.timeline) + list(reversed(km.timeline))
                    y_fill = list(km.ci_upper) + list(reversed(km.ci_lower))
                    fig.add_trace(go.Scatter(
                        x=x_fill, y=y_fill,
                        fill="toself",
                        fillcolor=f"rgba({_hex_to_rgb(color)},0.10)",
                        line=dict(width=0),
                        showlegend=False,
                        hoverinfo="skip",
                    ))

                fig.add_trace(go.Scatter(
                    x=km.timeline,
                    y=km.survival,
                    mode="lines",
                    name=trace_name,
                    line=dict(color=color, width=2),
                    hovertemplate=(
                        f"{trace_name}: t=%{{x:.1f}}年, S(t)=%{{y:.3f}}"
                        f"<br>n={km.n}, events={km.n_events}<extra></extra>"
                    ),
                ))

        fig.update_layout(
            title="役職進行年数 KM生存曲線（cohort 別, 95% CI）",
            xaxis_title="役職取得からの経過年数 / Years from role_from",
            yaxis_title="未昇進率 S(t)",
            yaxis=dict(range=[0, 1.05]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )

        findings_html = (
            "<p>各役職ペアの進行年数について、5年区切りデビューコホート別に"
            "カプラン–マイヤー推定を実施した（横軸: 進行年数, 縦軸: 未昇進率）:</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            "<p>中央生存時間は未進行の打切りを考慮した推定値。"
            "進行年数の長い role_pair はパイプラインの詰まりを示す。</p>"
        )

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += (
                f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="役職進行年数 Kaplan-Meier 生存曲線",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_km_role_tenure", height=500),
            method_note=(
                "Kaplan-Meier推定量（lifelines>=0.30）。"
                "イベント定義: person の role_to における最初のクレジット年が role_from より後。"
                "打切り: 観測期間内に role_to に未到達（右打切り, 25年上限）。"
                "Greenwood公式による95% CI（点線シェーディング）。"
                "コホート区分はデビュー年の5年区切り (cohort_5y = debut_year // 5 * 5)。"
                "多変量ログランク検定でコホート間差異を確認。"
                "進行年数はクレジット記録に基づく観察値であり、実際の契約・雇用形態を反映しない。"
            ),
            section_id="km_role_tenure",
        )

    # ── Section 2: Studio blockage ────────────────────────────────────────────

    def _build_blockage_section(
        self, sb: SectionBuilder, compute_studio_blockage
    ) -> ReportSection:
        rows = compute_studio_blockage(
            self.conn,
            role_from="in_between",
            role_to="key_animator",
            n_bootstrap=1000,
            rng_seed=42,
        )

        if not rows:
            return ReportSection(
                title="スタジオ別パイプライン詰まり指標",
                findings_html=(
                    "<p>スタジオ帰属データが不足しているか、"
                    "動画→原画の観測進行者が最小基準を下回っています。"
                    "anime.studio_id と credits の結合に十分なデータが必要です。</p>"
                ),
                method_note=(
                    "studio_blockage_score = studio_median(progression_years) - industry_median. "
                    "Bootstrap CI (n=1000, seed=42). "
                    "最低 5 名の観測進行者がいるスタジオのみ集計。"
                ),
                section_id="studio_blockage",
            )

        # Top 20 blockage / bottom 20
        top20 = rows[:20]
        bottom20 = list(reversed(rows[-20:]))
        display_rows = top20 + bottom20
        # Deduplicate
        seen: set[str] = set()
        dedup_rows = []
        for r in display_rows:
            if r.studio_id not in seen:
                seen.add(r.studio_id)
                dedup_rows.append(r)

        labels = [r.studio_id for r in dedup_rows]
        scores = [r.blockage_score for r in dedup_rows]
        ci_lo_err = [r.blockage_score - r.ci_low for r in dedup_rows]
        ci_hi_err = [r.ci_high - r.blockage_score for r in dedup_rows]
        n_list = [r.n_persons for r in dedup_rows]
        colors = [
            "#E07532" if s > 0 else "#3BC494" for s in scores
        ]

        industry_median = rows[0].industry_median if rows else 0.0
        positive_count = sum(1 for r in rows if r.blockage_score > 0)
        negative_count = sum(1 for r in rows if r.blockage_score <= 0)

        findings_html = (
            f"<p>スタジオ別動画→原画進行年数の産業中央値との差分"
            f"（blockage_score, bootstrap 95% CI）:"
            f"</p>"
            f"<ul>"
            f"<li>産業全体中央値: {industry_median:.1f}年</li>"
            f"<li>集計スタジオ数: {len(rows):,}</li>"
            f"<li>産業中央値より遅いスタジオ (score > 0): {positive_count:,}</li>"
            f"<li>産業中央値より早いスタジオ (score ≤ 0): {negative_count:,}</li>"
            f"</ul>"
            f"<p>正値はパイプラインの詰まり（産業平均より昇進に年数を要する）を示す。"
            f"負値は産業平均より早い昇進を示す。</p>"
        )

        # v3: CIScatter primitive — bootstrap 95% CI / null reference
        # (industry median = 0) / sort 入力順
        from src.viz import embed as viz_embed
        from src.viz.primitives import CIPoint, CIScatterSpec, render_ci_scatter

        ci_points = [
            CIPoint(
                label=dedup_rows[i].studio_id,
                x=scores[i],
                ci_lo=dedup_rows[i].ci_low,
                ci_hi=dedup_rows[i].ci_high,
                n=n_list[i],
            )
            for i in range(len(dedup_rows))
        ]
        spec = CIScatterSpec(
            points=ci_points,
            x_label="blockage_score (年) = studio_median - industry_median (95% CI)",
            title="スタジオ別パイプライン詰まり (blockage_score, 95% CI bootstrap)",
            reference=0.0,
            reference_label="産業中央値",
            sort_by="input",
        )
        fig = render_ci_scatter(spec, theme="dark")

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += (
                f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="スタジオ別パイプライン詰まり指標",
            findings_html=findings_html,
            visualization_html=viz_embed(fig, "chart_studio_blockage"),
            method_note=(
                "blockage_score[s] = median(progression_years: persons primarily affiliated with s) "
                "- industry_median(progression_years). "
                "主帰属スタジオ = credits × anime join で最多クレジット数のスタジオ。"
                "Bootstrap CI: n=1000 resamples per studio (seed=42), "
                "percentile法 (2.5th/97.5th)。"
                "集計対象: 観測進行者 >= 5名のスタジオ (動画→原画ペア)。"
                "打切り観測は blockage score 計算から除外（中央値は観測完了データのみ）。"
                "スタジオ帰属はクレジット記録上の推定であり、雇用契約形態を直接反映しない。"
            ),
            section_id="studio_blockage",
        )

    # ── Section 3: Promotion funnel ───────────────────────────────────────────

    def _build_funnel_section(
        self, sb: SectionBuilder, role_counts: dict[str, int]
    ) -> ReportSection:
        from src.analysis.career.role_progression import PIPELINE_ROLES

        labels = [_ROLE_LABELS.get(r, r) for r in PIPELINE_ROLES]
        values = [role_counts.get(r, 0) for r in PIPELINE_ROLES]

        # Funnel chart
        fig = go.Figure(go.Funnel(
            y=labels,
            x=values,
            textinfo="value+percent initial",
            marker=dict(color=["#3593D2", "#E09BC2", "#3BC494", "#F8EC6A"]),
            connector=dict(line=dict(color="#a0a0a0", width=1)),
            hovertemplate="%{label}: %{value:,}人<extra></extra>",
        ))
        fig.update_layout(
            title="役職別クレジット取得者数ファネル（動画 → 原画 → 作監 → 監督）",
            margin=dict(l=100),
        )

        findings_parts = []
        prev_val = None
        for role, label, val in zip(PIPELINE_ROLES, labels, values):
            if prev_val is not None and prev_val > 0:
                ratio = val / prev_val * 100
                findings_parts.append(
                    f"<li><strong>{label}</strong>: {val:,}人 "
                    f"(前役職比 {ratio:.1f}%)</li>"
                )
            else:
                findings_parts.append(f"<li><strong>{label}</strong>: {val:,}人</li>")
            prev_val = val

        findings_html = (
            "<p>各役職でクレジットが記録された個人の延べ人数（同一人物の複数役職は各役職でカウント）:</p>"
            f"<ul>{''.join(findings_parts)}</ul>"
            "<p>上位役職ほど人数が減少する構造は、各ステージへの到達者数の差を示す。</p>"
        )

        violations = sb.validate_findings(findings_html)
        if violations:
            findings_html += (
                f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="昇進ファネル — 役職別クレジット取得者数",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_role_funnel", height=400),
            method_note=(
                "各役職でクレジットが記録された個人の COUNT(DISTINCT person_id)。"
                "同一人物が複数役職を担当した場合は各役職でカウントされる（重複あり）。"
                "前役職比 = 当該役職人数 / 一段階低い役職人数 × 100%。"
                "ファネルは横断的（同一期間の全クレジット集計）であり、"
                "縦断的コホート追跡ではない点に注意。"
            ),
            section_id="role_funnel",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='o2_mid_management',
    audience='hr',
    claim='中堅枯渇パイプライン分析 に関する記述的指標 (subtitle: 動画 → 原画 → 作監 → 監督 進行年数 KM curve + スタジオ別パイプライン詰まり指標)',
    sources=["credits", "persons", "anime"],
    meta_table='meta_o2_mid_management',
)
