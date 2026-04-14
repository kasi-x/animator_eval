"""Structural Career report — v2 compliant.

Covers career pipeline structure:
- Section 1: Workforce stock by career stage (tier × era tabs)
- Section 2: Entry / exit flows by year
- Section 3: Career duration distributions
- Section 4: Cohort survival (years active)
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..html_templates import plotly_div_safe, stratification_tabs, strat_panel
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class StructuralCareerReport(BaseReportGenerator):
    name = "structural_career"
    title = "構造的キャリア分析"
    subtitle = "人材パイプラインの構造的特性と流入・流出パターン"
    filename = "structural_career.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_stock_section(sb)))
        sections.append(sb.build_section(self._build_entry_exit_section(sb)))
        sections.append(sb.build_section(self._build_career_duration_section(sb)))
        sections.append(sb.build_section(self._build_cohort_survival_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 1: Workforce stock by stage ──────────────────────

    def _build_stock_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fc.highest_stage,
                    fc.career_track,
                    COUNT(*) AS n
                FROM feat_career fc
                WHERE fc.highest_stage IS NOT NULL
                GROUP BY fc.highest_stage, fc.career_track
                ORDER BY fc.highest_stage, fc.career_track
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="現役人材ストック（キャリアステージ別）",
                findings_html="<p>feat_careerデータが利用できません。</p>",
                section_id="stock",
            )

        stage_totals: dict[int, int] = {}
        track_stage: dict[str, dict[int, int]] = {}
        for r in rows:
            s = r["highest_stage"]
            t = r["career_track"] or "unknown"
            stage_totals[s] = stage_totals.get(s, 0) + r["n"]
            if t not in track_stage:
                track_stage[t] = {}
            track_stage[t][s] = track_stage[t].get(s, 0) + r["n"]

        total = sum(stage_totals.values())
        findings = (
            f"<p>現役人材ストック（feat_careerのn={total:,}人）の到達キャリアステージ別分布:</p><ul>"
        )
        stage_labels = {0: "Unknown", 1: "Entry", 2: "Junior", 3: "Mid",
                        4: "Senior", 5: "Principal", 6: "Director/Lead"}
        for s in sorted(stage_totals):
            cnt = stage_totals[s]
            findings += (
                f"<li><strong>Stage {s} ({stage_labels.get(s, s)})</strong>: "
                f"{cnt:,} ({100*cnt/total:.1f}%)</li>"
            )
        findings += "</ul>"

        stages = sorted(stage_totals.keys())
        stage_lbls = [f"S{s}" for s in stages]
        fig = go.Figure()

        # Overall bar
        fig.add_trace(go.Bar(
            x=stage_lbls,
            y=[stage_totals[s] for s in stages],
            marker_color="#f093fb",
            name="全体",
            hovertemplate="%{x}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(
            title="キャリアステージ別 現役人材ストック",
            xaxis_title="最高到達ステージ", yaxis_title="人数",
        )

        # Tab: overall vs by career track
        tab_axes = {"overall": "全体", "by_track": "キャリアトラック別"}
        tabs_html = stratification_tabs("stock_tabs", tab_axes, active="overall")

        # By-track stacked bar
        fig2 = go.Figure()
        tracks = sorted(track_stage.keys())
        track_colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#667eea",
                        "#f5576c", "#fda085", "#8a94a0"]
        for i, track in enumerate(tracks):
            fig2.add_trace(go.Bar(
                x=stage_lbls,
                y=[track_stage[track].get(s, 0) for s in stages],
                name=track,
                marker_color=track_colors[i % len(track_colors)],
            ))
        fig2.update_layout(
            title="ステージ×キャリアトラック別 現役人材ストック",
            barmode="stack", xaxis_title="ステージ", yaxis_title="人数",
        )

        panel_overall = strat_panel("stock_tabs", "overall",
                                    plotly_div_safe(fig, "chart_stock_overall", height=380), active=True)
        panel_track = strat_panel("stock_tabs", "by_track",
                                  plotly_div_safe(fig2, "chart_stock_track", height=420))

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="現役人材ストック（キャリアステージ別）",
            findings_html=findings,
            visualization_html=tabs_html + panel_overall + panel_track,
            method_note=(
                "highest_stage は feat_career 由来（Phase 6 のルールベース分類）。"
                "ステージ値: 0=未分類、1〜5=段階的シニアリティ、6=監督/リード。"
                "career_track は feat_cluster_membership（キャリア特徴量に対するK-Means）より取得。"
                "ストック件数は直近活動の有無によらず feat_career 登録全員を含む。"
            ),
            section_id="stock",
        )

    # ── Section 2: Entry / exit flows by year ───────────────────

    def _build_entry_exit_section(self, sb: SectionBuilder) -> ReportSection:
        # Entry = debut year distribution; Exit = last active year
        try:
            entry_rows = self.conn.execute("""
                SELECT first_year AS yr, COUNT(*) AS n
                FROM feat_career
                WHERE first_year BETWEEN 1963 AND 2025
                GROUP BY first_year ORDER BY first_year
            """).fetchall()
            exit_rows = self.conn.execute("""
                SELECT latest_year AS yr, COUNT(*) AS n
                FROM feat_career
                WHERE latest_year BETWEEN 1963 AND 2020
                GROUP BY latest_year ORDER BY latest_year
            """).fetchall()
        except Exception:
            entry_rows, exit_rows = [], []

        if not entry_rows:
            return ReportSection(
                title="人材参入・離脱フロー（年次）",
                findings_html="<p>参入・離脱データが利用できません。</p>",
                section_id="entry_exit",
            )

        entry_by_yr = {r["yr"]: r["n"] for r in entry_rows}
        exit_by_yr = {r["yr"]: r["n"] for r in exit_rows}
        years = sorted(set(entry_by_yr) | set(exit_by_yr))
        entries = [entry_by_yr.get(y, 0) for y in years]
        exits = [exit_by_yr.get(y, 0) for y in years]

        total_entries = sum(entries)
        total_exits = sum(exits)
        findings = (
            f"<p>{len(years)}年間のキャリア参入・離脱フロー"
            f"（参入: 1963–2025年、離脱: 2020年まで — 5年間クレジットなしで離脱判定）。"
            f"デビュー総数: {total_entries:,}人。"
            f"2020年以前の最終クレジット人数: {total_exits:,}人。</p>"
            f"<p>離脱年 = feat_career.latest_year（最終クレジット年）。"
            f"確定的な引退ではなく「最後に記録された活動年」を指す。ブランク後に復帰する人物もいる。</p>"
        )

        fig = go.Figure()
        fig.add_trace(go.Bar(x=years, y=entries, name="参入（デビュー）",
                             marker_color="rgba(6,214,160,0.7)",
                             hovertemplate="%{x}: %{y:,}人デビュー<extra></extra>"))
        fig.add_trace(go.Bar(x=years, y=[-e for e in exits], name="離脱（最終クレジット）",
                             marker_color="rgba(245,87,108,0.7)",
                             hovertemplate="%{x}: %{y:,}人離脱<extra></extra>"))
        fig.update_layout(
            title="年次 参入・離脱フロー",
            barmode="overlay",
            xaxis_title="年", yaxis_title="人数（正=参入、負=離脱）",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="人材参入・離脱フロー（年次）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_entry_exit", height=420),
            method_note=(
                "参入 = feat_career.first_year（人物ごとの MIN credit_year）。"
                "離脱 = feat_career.latest_year（人物ごとの MAX credit_year）。"
                "離脱は2020年で打ち切り（離脱 = 5年以上クレジットなしと定義）。"
                "打ち切らない場合、直近の離脱数が見かけ上膨らむためである。"
                "これはストック・フロー会計視点であり、生存モデルではない。"
            ),
            interpretation_html=(
                "<p>2010年代を通じた参入数の増加傾向は、実際の産業成長と"
                "クレジット記録密度の向上の両方を反映している。"
                "2021年以前の離脱系列は、活動量を減らしたが完全には離脱していない人物を"
                "過小カウントしている可能性がある。離脱は5年以上クレジットがないことと定義されており、"
                "直近の分析用には「準離脱」閾値（3年以上）も利用可能。</p>"
            ),
            section_id="entry_exit",
        )

    # ── Section 3: Career duration distribution ─────────────────

    def _build_career_duration_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT active_years, career_track
                FROM feat_career
                WHERE active_years IS NOT NULL AND active_years >= 0
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="キャリア継続年数の分布",
                findings_html="<p>キャリア継続年数データが利用できません。</p>",
                section_id="career_duration",
            )

        all_vals = [r["active_years"] for r in rows]
        overall = distribution_summary(all_vals, label="全体")

        track_groups: dict[str, list[float]] = {}
        for r in rows:
            t = r["career_track"] or "unknown"
            track_groups.setdefault(t, []).append(r["active_years"])
        track_summs = {t: distribution_summary(v, label=t) for t, v in sorted(track_groups.items())}

        findings = (
            f"<p>活動年数の分布（n={overall['n']:,}人）: "
            f"{format_distribution_inline(overall)}, "
            f"{format_ci((overall['ci_lower'], overall['ci_upper']))}。</p>"
            f"<p>active_years = latest_year − first_year（ブランク年未調整）。"
            f"0はキャリアスパンが1年以内であることを示す。</p>"
        )
        if track_summs:
            findings += "<p>キャリアトラック別:</p><ul>"
            for t, s in track_summs.items():
                findings += (
                    f"<li><strong>{t}</strong> (n={s['n']:,}): "
                    f"{format_distribution_inline(s)}, {format_ci((s['ci_lower'], s['ci_upper']))}</li>"
                )
            findings += "</ul>"

        fig = go.Figure()
        for t in sorted(track_groups, key=lambda x: -len(track_groups[x]))[:8]:
            vals = track_groups[t]
            sample = vals[:1000] if len(vals) > 1000 else vals
            fig.add_trace(go.Box(y=sample, name=t[:20], boxpoints=False))
        fig.update_layout(
            title="キャリアトラック別 活動年数",
            xaxis_title="キャリアトラック", yaxis_title="活動年数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="キャリア継続年数の分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_career_dur", height=400),
            method_note=(
                "active_years = feat_career.latest_year − feat_career.first_year。"
                "ブランク年は差し引かず、総スパンを表す（連続活動ではない）。"
                "feat_credit_activity.active_years（四半期活動ベース）は"
                "より粒度の細かい指標だが本節では使用していない。"
                "上位8キャリアトラックを人数順に表示。"
            ),
            section_id="career_duration",
        )

    # ── Section 4: Cohort survival ───────────────────────────────

    def _build_cohort_survival_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    (first_year / 10) * 10 AS decade,
                    active_years
                FROM feat_career
                WHERE first_year BETWEEN 1960 AND 2019
                  AND active_years IS NOT NULL
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="デビューコホート別キャリア継続分布",
                findings_html="<p>コホートデータが利用できません。</p>",
                section_id="cohort_survival",
            )

        decade_vals: dict[int, list[float]] = {}
        for r in rows:
            decade_vals.setdefault(r["decade"], []).append(r["active_years"])

        findings = "<p>デビュー年代コホート別キャリアスパン分布:</p><ul>"
        for d in sorted(decade_vals):
            summ = distribution_summary(decade_vals[d], label=str(d))
            findings += (
                f"<li><strong>{d}年代コホート</strong> (n={summ['n']:,}): "
                f"{format_distribution_inline(summ)}, "
                f"{format_ci((summ['ci_lower'], summ['ci_upper']))}</li>"
            )
        findings += (
            "</ul><p>注意: 最近のコホートは右打ち切り（キャリア継続中）のため、"
            "active_yearsの中央値は実際のキャリア年数より低くなる。</p>"
        )

        fig = go.Figure()
        decades_sorted = sorted(decade_vals.keys())
        for d in decades_sorted:
            vals = decade_vals[d]
            sample = vals[:500] if len(vals) > 500 else vals
            fig.add_trace(go.Violin(
                x=[f"{d}年代"] * len(sample), y=sample,
                name=f"{d}年代", box_visible=True,
                meanline_visible=True, points=False,
            ))
        fig.update_layout(
            title="デビュー年代コホート別 キャリアスパン",
            xaxis_title="デビュー年代", yaxis_title="活動年数",
            violinmode="overlay",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="デビューコホート別キャリア継続分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cohort_survival", height=440),
            method_note=(
                "Cohort = (feat_career.first_year / 10) * 10. "
                "active_years = latest_year − first_year (right-censored for post-2010 cohorts). "
                "2020s cohort excluded (too few years of data). "
                "A proper survival analysis (Kaplan-Meier) would account for censoring; "
                "this visualization does not."
            ),
            interpretation_html=(
                "<p>最近のコホートほど active_years の中央値が低下するのは"
                "右打ち切りから予想される現象である: 2015年にデビューした人物は"
                "2025年時点で最大10年の active_years しか持ち得ない。"
                "打ち切り補正をせずにコホート間で中央値を比較すると、"
                "最近のコホートの期間を系統的に過小評価することになる。"
                "キャリアが完了したと見なせる人物のみ（latest_year ≤ 2020）を用いる代替分析は"
                "このバイアスを回避できるが、離脱による選択が生じる。</p>"
            ),
            section_id="cohort_survival",
        )
