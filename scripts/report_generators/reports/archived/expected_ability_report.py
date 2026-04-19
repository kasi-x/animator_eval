"""Expected Ability report — v2 compliant.

Expected vs actual score analysis:
  1. Studio FE exposure vs actual IV score (scatter)
  2. Four-tier classification (excludes AKM-unmeasured persons)
  3. IV distribution by gender (violin)
  4. Conversion rate by career length
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}


class ExpectedAbilityReport(BaseReportGenerator):
    name = "expected_ability_report"
    title = "期待値・実績乖離分析"
    subtitle = "スタジオ環境（studio_fe_exposure）と実際IVスコアの4ティア分類"
    filename = "expected_ability_report.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_scatter_section(sb)))
        sections.append(sb.build_section(self._build_four_tier_section(sb)))
        sections.append(sb.build_section(self._build_gender_violin_section(sb)))
        sections.append(sb.build_section(self._build_conversion_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Shared: AKM-measured population filter ───────────────────
    # person_fe_n_obs > 0 ensures the person was in the AKM connected set.
    # studio_fe_exposure = 0 AND person_fe = 0 means "unmeasured", not "zero contribution".

    _AKM_FILTER = (
        "fps.person_fe_n_obs > 0 "
        "AND fps.studio_fe_exposure != 0"
    )

    # Stricter filter for percentile-based analyses (4-tier, conversion).
    # person_fe_n_obs < 5 yields unreliable estimates (42K persons have iv=0
    # with avg n_obs=2.3). Without this, p75 of iv_score ≈ 0.0002 — the
    # cutoff is indistinguishable from zero and the classification is noise.
    # With n_obs >= 5: p75 ≈ 0.047, giving a meaningful separation.
    _AKM_RELIABLE_FILTER = (
        "fps.person_fe_n_obs >= 5 "
        "AND fps.studio_fe_exposure != 0"
    )

    # ── Section 1: Scatter expected vs actual ─────────────────────

    def _build_scatter_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute(f"""
                SELECT
                    fps.person_id,
                    fps.iv_score,
                    fps.studio_fe_exposure,
                    modal_tier.scale_tier AS tier
                FROM feat_person_scores fps
                LEFT JOIN (
                    SELECT fcc.person_id, fwc.scale_tier,
                           ROW_NUMBER() OVER (
                               PARTITION BY fcc.person_id
                               ORDER BY COUNT(*) DESC
                           ) AS rn
                    FROM feat_credit_contribution fcc
                    JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                    WHERE fwc.scale_tier IS NOT NULL
                    GROUP BY fcc.person_id, fwc.scale_tier
                ) modal_tier ON fps.person_id = modal_tier.person_id AND modal_tier.rn = 1
                WHERE fps.iv_score IS NOT NULL
                  AND {self._AKM_FILTER}
                ORDER BY RANDOM()
                LIMIT 5000
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="studio_fe_exposure vs IVスコア散布図",
                findings_html="<p>期待値vs実績データが利用できません。</p>",
                section_id="scatter_ea",
            )

        findings = (
            f"<p>studio_fe_exposure vs iv_scoreの散布図"
            f"（n={len(rows):,}、AKM計測対象者のみ、ランダムサンプル）。"
            "studio_fe_exposure = クレジットのあるスタジオ固定効果の加重平均。"
            "person_fe_n_obs=0またはstudio_fe_exposure=0の人物"
            "（AKM連結集合外）は除外。</p>"
        )

        fig = go.Figure()
        tier_data: dict[int | str, list] = {}
        for r in rows:
            t = r["tier"] if r["tier"] is not None else "unknown"
            tier_data.setdefault(t, []).append(r)

        for t, td in sorted(tier_data.items(), key=lambda x: (x[0] == "unknown", x[0])):
            color = _TIER_COLORS.get(t, "#a0a0c0") if isinstance(t, int) else "#a0a0c0"
            label = f"Tier {t}" if isinstance(t, int) else "Unknown"
            fig.add_trace(go.Scattergl(
                x=[r["studio_fe_exposure"] for r in td],
                y=[r["iv_score"] for r in td],
                mode="markers",
                name=label,
                marker=dict(color=color, size=4, opacity=0.5),
                hovertemplate=f"{label}: sfe=%{{x:.3f}}, iv=%{{y:.3f}}<extra></extra>",
            ))
        fig.update_layout(
            title="Studio FE Exposure vs IVスコア（AKM計測対象のみ）",
            xaxis_title="Studio FE Exposure",
            yaxis_title="IVスコア",
        )

        return ReportSection(
            title="studio_fe_exposure vs IVスコア散布図",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_scatter_ea", height=480),
            method_note=(
                "studio_fe_exposureはfeat_person_scores由来。"
                "AKM連結集合外の人物を除外するため person_fe_n_obs > 0 "
                "かつ studio_fe_exposure != 0 でフィルタ。"
                "ランダムサンプルは5,000件に制限。"
            ),
            section_id="scatter_ea",
        )

    # ── Section 2: Four-tier classification ───────────────────────

    def _build_four_tier_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            # Count total population for the data statement
            total_pop = self.conn.execute(
                "SELECT COUNT(*) FROM feat_person_scores WHERE iv_score IS NOT NULL"
            ).fetchone()[0]
            # Use _AKM_RELIABLE_FILTER: person_fe_n_obs >= 5 ensures the AKM
            # estimate is based on enough observations to be meaningful.
            # Without this, 42K persons have iv=0 with avg n_obs=2.3 and the
            # p75 cutoff collapses to ~0.0002 (indistinguishable from zero).
            rows = self.conn.execute(f"""
                SELECT iv_score, studio_fe_exposure
                FROM feat_person_scores fps
                WHERE iv_score IS NOT NULL
                  AND {self._AKM_RELIABLE_FILTER}
            """).fetchall()
        except Exception:
            rows = []
            total_pop = 0

        if len(rows) < 100:
            return ReportSection(
                title="4ティア分類（期待×実績）",
                findings_html="<p>4ティアデータが利用できません（AKM計測対象者が不足）。</p>",
                section_id="four_tier",
            )

        # Use p75 (top 25%) on reliably-measured population
        high_pct = 75
        all_iv = sorted(r["iv_score"] for r in rows)
        all_se = sorted(r["studio_fe_exposure"] for r in rows)
        n = len(all_iv)
        iv_cutoff = all_iv[int(n * high_pct / 100)]
        se_cutoff = all_se[int(n * high_pct / 100)]

        tier_counts: dict[str, int] = {
            "高期待×高実績": 0, "高期待×低実績": 0,
            "低期待×高実績": 0, "低期待×低実績": 0,
        }
        for r in rows:
            high_iv = r["iv_score"] >= iv_cutoff
            high_se = r["studio_fe_exposure"] >= se_cutoff
            if high_iv and high_se:
                tier_counts["高期待×高実績"] += 1
            elif not high_iv and high_se:
                tier_counts["高期待×低実績"] += 1
            elif high_iv and not high_se:
                tier_counts["低期待×高実績"] += 1
            else:
                tier_counts["低期待×低実績"] += 1

        n_excluded = total_pop - n
        findings = (
            f"<p>4ティア分類（信頼性のある計測対象者: "
            f"person_fe_n_obs &ge; 5、n={n:,}人; "
            f"AKM観測値5未満またはAKM連結集合外の{n_excluded:,}人を除外）。"
            f"閾値 {high_pct}パーセンタイル: "
            f"iv_score={iv_cutoff:.4f}, studio_fe_exposure={se_cutoff:.4f}。</p><ul>"
        )
        for label, cnt in tier_counts.items():
            findings += f"<li><strong>{label}</strong>: {cnt:,} ({100*cnt/n:.1f}%)</li>"
        findings += "</ul>"

        fig = go.Figure(go.Bar(
            x=list(tier_counts.keys()),
            y=list(tier_counts.values()),
            marker_color=["#06D6A0", "#f5576c", "#FFD166", "#a0a0c0"],
            hovertemplate="%{x}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            title="4ティア分類（person_fe_n_obs ≥ 5）",
            yaxis_title="人数",
        )

        return ReportSection(
            title="4ティア分類（期待×実績）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_four_tier", height=380),
            method_note=(
                f"閾値 = person_fe_n_obs >= 5の人物に対する{high_pct}パーセンタイル。"
                "AKM観測値が5件未満の人物は、person_fe推定値が信頼できないため除外"
                "（4.2万人の iv=0 かつ平均 n_obs=2.3 — 安定推定には観測が不足）。"
                "studio_fe_exposureは機会代理変数であり、直接的な「期待能力」指標ではない。"
            ),
            interpretation_html=(
                "<p>「高期待×低実績」には、高FEスタジオに所属するキャリア初期または"
                "サポート役の人物が含まれる。「低期待×高実績」には、低FEスタジオで"
                "活動しながらも高IVスコアを築いた人物が含まれる — 過小評価されている"
                "可能性のある人材、あるいは低FEから高FEスタジオにキャリア後期で"
                "移籍した人物（studio_fe_exposureはキャリア平均であり現在値ではない）。"
                "代替案: キャリアステージ調整パーセンタイルを使用する。</p>"
            ),
            section_id="four_tier",
        )

    # ── Section 3: IV distribution by gender (violin) ─────────────

    def _build_gender_violin_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute(f"""
                SELECT fps.iv_score, p.gender
                FROM feat_person_scores fps
                JOIN persons p ON fps.person_id = p.id
                WHERE fps.iv_score IS NOT NULL
                  AND {self._AKM_FILTER}
                  AND p.gender IN ('Male', 'Female')
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="IV Score分布（性別）",
                findings_html="<p>性別別IVデータが利用できません。</p>",
                section_id="gender_violin",
            )

        gender_vals: dict[str, list[float]] = {}
        for r in rows:
            gender_vals.setdefault(r["gender"], []).append(r["iv_score"])

        findings = "<p>性別のIVスコア分布（AKM計測対象者のみ）:</p><ul>"
        for g, gv in sorted(gender_vals.items()):
            gs = distribution_summary(gv, label=g)
            findings += (
                f"<li><strong>{g}</strong> (n={gs['n']:,}): "
                f"{format_distribution_inline(gs)}, "
                f"{format_ci((gs['ci_lower'], gs['ci_upper']))}</li>"
            )
        findings += "</ul>"

        fig = go.Figure()
        gender_colors = {"Male": "#667eea", "Female": "#f5576c"}
        for g, gv in sorted(gender_vals.items()):
            sample = gv[:3000] if len(gv) > 3000 else gv
            fig.add_trace(go.Violin(
                y=sample, name=g,
                box_visible=True, meanline_visible=True,
                points=False,
                line_color=gender_colors.get(g, "#a0a0c0"),
            ))
        fig.update_layout(
            title="性別IVスコア分布（AKM計測対象）",
            yaxis_title="IVスコア",
        )

        return ReportSection(
            title="IV Score分布（性別）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_gender_violin", height=440),
            method_note=(
                "iv_scoreはfeat_person_scores由来。"
                "AKM計測対象（person_fe_n_obs > 0）でフィルタ。"
                "Violinは性別ごとに3,000サンプルで打ち切り。"
            ),
            section_id="gender_violin",
        )

    # ── Section 4: Conversion rate ────────────────────────────────

    def _build_conversion_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute(f"""
                SELECT fps.iv_score, fps.studio_fe_exposure, fc.active_years
                FROM feat_person_scores fps
                JOIN feat_career fc ON fps.person_id = fc.person_id
                WHERE fps.iv_score IS NOT NULL
                  AND fc.active_years IS NOT NULL
                  AND {self._AKM_RELIABLE_FILTER}
            """).fetchall()
        except Exception:
            rows = []

        if len(rows) < 100:
            return ReportSection(
                title="高期待→高実績 転換率",
                findings_html="<p>転換率データが利用できません。</p>",
                section_id="conversion",
            )

        n = len(rows)
        high_pct = 75
        all_iv = sorted(r["iv_score"] for r in rows)
        all_se = sorted(r["studio_fe_exposure"] for r in rows)
        iv_cutoff = all_iv[int(n * high_pct / 100)]
        se_cutoff = all_se[int(n * high_pct / 100)]

        buckets = [(0, 3), (4, 10), (11, 20), (21, 999)]

        conv_rates = []
        for lo, hi in buckets:
            eligible = [r for r in rows if r["studio_fe_exposure"] >= se_cutoff and lo <= r["active_years"] <= hi]
            converted = [r for r in eligible if r["iv_score"] >= iv_cutoff]
            rate = len(converted) / max(len(eligible), 1)
            conv_rates.append((f"{lo}–{hi}yr" if hi < 999 else f"{lo}+yr", len(eligible), len(converted), rate))

        findings = (
            f"<p>転換率（高studio_fe_exposure → 高iv_score）キャリア年数別"
            f"（person_fe_n_obs &ge; 5, n={n:,}人）。"
            f"閾値 p{high_pct}: sfe &ge; {se_cutoff:.4f}, iv &ge; {iv_cutoff:.4f}。</p><ul>"
        )
        for label, n_elig, n_conv, rate in conv_rates:
            findings += (
                f"<li><strong>{label}</strong>: "
                f"対象{n_elig:,}人 → 転換{n_conv:,}人（{100*rate:.1f}%）</li>"
            )
        findings += "</ul>"

        fig = go.Figure(go.Bar(
            x=[r[0] for r in conv_rates],
            y=[r[3] * 100 for r in conv_rates],
            marker_color=["#667eea", "#06D6A0", "#FFD166", "#f5576c"],
            hovertemplate="%{x}: %{y:.1f}%<extra></extra>",
        ))
        fig.update_layout(
            title="キャリア年数別 転換率（n_obs ≥ 5）",
            xaxis_title="キャリア年数", yaxis_title="転換率 (%)",
        )

        return ReportSection(
            title="高期待→高実績 転換率",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_conversion", height=380),
            method_note=(
                f"高閾値 = person_fe_n_obs >= 5の人物に対するp{high_pct}。"
                "active_yearsはfeat_career由来。"
                "転換率 = 高sfeの人物のうち高iv_scoreも持つ割合。"
                "0–3年バケットが小さくなるのは、AKM観測5件未満の人物が"
                "既に除外されており、キャリア初期の人物の多くがこの閾値を"
                "満たさないため。"
            ),
            section_id="conversion",
        )
