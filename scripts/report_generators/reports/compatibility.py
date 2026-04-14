"""Compatibility report — v2 compliant.

Collaboration compatibility analysis computed from raw credit tables.
Uses credits + feat_person_scores + feat_career + feat_work_context + persons.

- Section 1: Co-credit pair statistics (shared_works distribution)
- Section 2: Compatibility score distribution by scale tier
- Section 3: Repeat collaboration patterns by primary_role
- Section 4: Top collaboration pairs
"""

from __future__ import annotations

import math
from collections import defaultdict
from pathlib import Path

import numpy as np
import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..helpers import add_distribution_stats, person_link
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}
_ROLE_COLORS = {
    "animator": "#f093fb",
    "director": "#667eea",
    "production": "#06D6A0",
    "designer": "#FFD166",
    "writing": "#a0d2db",
    "technical": "#EF476F",
    "other": "#9B59B6",
}
_TOP_N_PERSONS = 2000
_MIN_SHARED_WORKS = 3


class CompatibilityReport(BaseReportGenerator):
    name = "compatibility"
    title = "コラボレーション相性分析"
    subtitle = (
        "上位スコア人材間の共同クレジット統計・相性指標・リピート協業パターン "
        f"(feat_person_scores上位{_TOP_N_PERSONS:,}人対象)"
    )
    filename = "compatibility.html"

    def generate(self) -> Path | None:
        # Build temp tables once and reuse across all sections
        if not self._prepare_temp_tables():
            sb = SectionBuilder()
            fallback = sb.build_section(ReportSection(
                title="データ準備",
                findings_html="<p>共同クレジットペアの計算に必要なデータが不足しています。</p>",
                section_id="no_data",
            ))
            return self.write_report(fallback)

        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_cowork_stats_section(sb)))
        sections.append(sb.build_section(self._build_compat_score_section(sb)))
        sections.append(sb.build_section(self._build_repeat_section(sb)))
        sections.append(sb.build_section(self._build_top_pairs_section(sb)))
        self._cleanup_temp_tables()
        return self.write_report("\n".join(sections))

    # ── Temp table lifecycle ─────────────────────────────────────

    def _prepare_temp_tables(self) -> bool:
        """Create temp tables for pair analysis. Returns False if data insufficient."""
        try:
            # Step 1: Top persons by IV score
            self.conn.execute("DROP TABLE IF EXISTS _compat_top_persons")
            self.conn.execute(f"""
                CREATE TEMP TABLE _compat_top_persons AS
                SELECT person_id, iv_score FROM feat_person_scores
                ORDER BY iv_score DESC LIMIT {_TOP_N_PERSONS}
            """)
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS _idx_ctp "
                "ON _compat_top_persons(person_id)"
            )

            n_persons = self.conn.execute(
                "SELECT COUNT(*) FROM _compat_top_persons"
            ).fetchone()[0]
            if n_persons < 10:
                return False

            # Step 2: Distinct (person_id, anime_id) for these persons
            self.conn.execute("DROP TABLE IF EXISTS _compat_credits")
            self.conn.execute("""
                CREATE TEMP TABLE _compat_credits AS
                SELECT DISTINCT c.person_id, c.anime_id
                FROM credits c
                JOIN _compat_top_persons tp ON c.person_id = tp.person_id
            """)
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS _idx_cc_anime "
                "ON _compat_credits(anime_id)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS _idx_cc_person "
                "ON _compat_credits(person_id)"
            )

            # Step 3: Co-credit pairs (shared_works >= threshold)
            self.conn.execute("DROP TABLE IF EXISTS _compat_pairs")
            self.conn.execute(f"""
                CREATE TEMP TABLE _compat_pairs AS
                SELECT c1.person_id AS p1, c2.person_id AS p2,
                       COUNT(*) AS shared_works
                FROM _compat_credits c1
                JOIN _compat_credits c2
                    ON c1.anime_id = c2.anime_id
                    AND c1.person_id < c2.person_id
                GROUP BY c1.person_id, c2.person_id
                HAVING shared_works >= {_MIN_SHARED_WORKS}
            """)
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS _idx_cp_p1 ON _compat_pairs(p1)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS _idx_cp_p2 ON _compat_pairs(p2)"
            )

            n_pairs = self.conn.execute(
                "SELECT COUNT(*) FROM _compat_pairs"
            ).fetchone()[0]
            return n_pairs > 0

        except Exception:
            return False

    def _cleanup_temp_tables(self) -> None:
        for tbl in ("_compat_top_persons", "_compat_credits", "_compat_pairs"):
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {tbl}")
            except Exception:
                pass

    # ── Section 1: Co-credit pair statistics ─────────────────────

    def _build_cowork_stats_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            stats = self.conn.execute("""
                SELECT COUNT(*) AS n_pairs,
                       AVG(shared_works) AS avg_shared,
                       MAX(shared_works) AS max_shared,
                       MIN(shared_works) AS min_shared
                FROM _compat_pairs
            """).fetchone()

            dist_rows = self.conn.execute("""
                SELECT shared_works FROM _compat_pairs
                ORDER BY RANDOM() LIMIT 10000
            """).fetchall()
        except Exception:
            stats = None
            dist_rows = []

        if not stats or stats["n_pairs"] == 0:
            return ReportSection(
                title="共同クレジットペア統計",
                findings_html="<p>共同クレジットデータが取得できませんでした。</p>",
                section_id="cowork_stats",
            )

        vals = [r["shared_works"] for r in dist_rows]
        summ = distribution_summary(vals, label="shared_works")

        # Per-bucket counts for the bar chart
        bucket_rows = self.conn.execute("""
            SELECT shared_works, COUNT(*) AS n_pairs
            FROM _compat_pairs
            GROUP BY shared_works
            ORDER BY shared_works
        """).fetchall()

        # Unique persons involved
        n_unique = self.conn.execute("""
            SELECT COUNT(DISTINCT pid) FROM (
                SELECT p1 AS pid FROM _compat_pairs
                UNION ALL
                SELECT p2 AS pid FROM _compat_pairs
            )
        """).fetchone()[0]

        findings = (
            f"<p>feat_person_scores上位{_TOP_N_PERSONS:,}人を対象に、"
            f"同一作品にクレジットされたペアを集計した "
            f"(shared_works >= {_MIN_SHARED_WORKS})。</p>"
            f"<p>ペア数: {stats['n_pairs']:,}組。"
            f"関与人数: {n_unique:,}人。"
            f"shared_works分布（サンプルn={summ['n']:,}）: "
            f"{format_distribution_inline(summ)}, "
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。"
            f"最大shared_works: {stats['max_shared']}。</p>"
        )

        # Chart 1a: Histogram of shared_works
        fig_hist = go.Figure(go.Histogram(
            x=vals, nbinsx=40, marker_color="#a0d2db",
            hovertemplate="shared_works=%{x}: %{y:,}組<extra></extra>",
        ))
        fig_hist.update_layout(
            title="共同クレジット作品数の分布 (shared_works)",
            xaxis_title="shared_works",
            yaxis_title="ペア数",
        )
        add_distribution_stats(fig_hist, vals, axis="x")

        # Chart 1b: Cumulative distribution (log scale)
        bucket_sw = [r["shared_works"] for r in bucket_rows]
        bucket_n = [r["n_pairs"] for r in bucket_rows]
        total = sum(bucket_n)
        cumulative_pct = []
        running = 0
        for n in bucket_n:
            running += n
            cumulative_pct.append(100.0 * (1 - running / total))

        fig_cdf = go.Figure()
        fig_cdf.add_trace(go.Scatter(
            x=bucket_sw, y=cumulative_pct,
            mode="lines+markers",
            line=dict(color="#f093fb", width=2),
            marker=dict(size=4),
            hovertemplate="shared_works>=%{x}: %{y:.1f}%<extra></extra>",
        ))
        fig_cdf.update_layout(
            title="shared_works累積分布（上側）",
            xaxis_title="shared_works",
            yaxis_title="割合（%、この値以上）",
            yaxis_type="log",
        )

        viz_html = (
            plotly_div_safe(fig_hist, "chart_cowork_hist", height=380)
            + plotly_div_safe(fig_cdf, "chart_cowork_cdf", height=380)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="共同クレジットペア統計",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                f"feat_person_scores上位{_TOP_N_PERSONS:,}人（iv_score降順）を抽出し、"
                "creditsテーブルからDISTINCT (person_id, anime_id)を取得。"
                "同一anime_idに両者がクレジットされたペアを集計。"
                f"shared_works >= {_MIN_SHARED_WORKS}のペアのみを対象とする。"
                "計算量: O(sum_per_work(n_top_persons^2)) — "
                f"上位{_TOP_N_PERSONS:,}人に限定することで実用的な計算時間を実現。"
            ),
            section_id="cowork_stats",
        )

    # ── Section 2: Compatibility score by tier ───────────────────

    def _build_compat_score_section(self, sb: SectionBuilder) -> ReportSection:
        """Compatibility score = shared_works x sqrt(min(iv_p1, iv_p2)).

        Stratified by the modal scale_tier of the pair's shared works.
        """
        try:
            # Compute compat_score and identify the modal tier for each pair
            # Step 1: compat_score for all pairs
            pair_scores = self.conn.execute("""
                SELECT pr.p1, pr.p2, pr.shared_works,
                       tp1.iv_score AS iv1, tp2.iv_score AS iv2
                FROM _compat_pairs pr
                JOIN _compat_top_persons tp1 ON pr.p1 = tp1.person_id
                JOIN _compat_top_persons tp2 ON pr.p2 = tp2.person_id
            """).fetchall()

            # Step 2: For each pair, find the shared anime_ids and their tier
            # (sampling for efficiency — take max 50000 pairs)
            sampled_pairs = pair_scores[:50000]

            # Get per-pair tier info via a single aggregated query
            # Build lookup: (p1, p2) -> modal tier
            # Get all shared anime for sampled pairs
            p1_set = set()
            p2_set = set()
            for r in sampled_pairs:
                p1_set.add(r["p1"])
                p2_set.add(r["p2"])

            # Get the most common tier for each person
            person_tier_rows = self.conn.execute("""
                SELECT cc.person_id,
                       fwc.scale_tier,
                       COUNT(*) AS n
                FROM _compat_credits cc
                JOIN feat_work_context fwc ON cc.anime_id = fwc.anime_id
                WHERE fwc.scale_tier IS NOT NULL
                GROUP BY cc.person_id, fwc.scale_tier
            """).fetchall()

            # For each person, find modal tier
            person_tier_counts: dict[str, dict[int, int]] = defaultdict(
                lambda: defaultdict(int)
            )
            for r in person_tier_rows:
                person_tier_counts[r["person_id"]][r["scale_tier"]] += r["n"]

            def modal_tier(pid: str) -> int | None:
                tc = person_tier_counts.get(pid)
                if not tc:
                    return None
                return max(tc, key=lambda t: tc[t])

        except Exception:
            pair_scores = []

        if not pair_scores:
            return ReportSection(
                title="相性スコアのTier別分布",
                findings_html="<p>相性スコアデータが取得できませんでした。</p>",
                section_id="compat_score",
            )

        # Compute compat_score and group by pair's average modal tier
        tier_compat: dict[int, list[float]] = defaultdict(list)
        all_compat: list[float] = []
        for r in pair_scores:
            iv_min = min(r["iv1"], r["iv2"])
            if iv_min <= 0:
                continue
            compat = r["shared_works"] * math.sqrt(iv_min)
            all_compat.append(compat)
            # Use the modal tier of the lower-IV person (the "receiver")
            receiver = r["p1"] if r["iv1"] <= r["iv2"] else r["p2"]
            tier = modal_tier(receiver)
            if tier is not None:
                tier_compat[tier].append(compat)

        all_summ = distribution_summary(all_compat, label="compat_score")

        findings = (
            "<p>相性スコアを shared_works x sqrt(min(iv_score_p1, iv_score_p2)) と定義し、"
            "ペアの主活動Tier（低IV側人材のモーダルTier）別に分布を集計した。</p>"
            f"<p>全体分布（n={all_summ['n']:,}）: "
            f"{format_distribution_inline(all_summ)}, "
            f"{format_ci((all_summ['ci_lower'], all_summ['ci_upper']))}。</p>"
            "<p>Tier別:</p><ul>"
        )
        for t in sorted(tier_compat):
            ts = distribution_summary(tier_compat[t], label=f"tier{t}")
            findings += (
                f"<li><strong>Tier {t}</strong> (n={ts['n']:,}): "
                f"{format_distribution_inline(ts)}</li>"
            )
        findings += "</ul>"

        # Chart 2a: Box plot by tier
        fig_box = go.Figure()
        for t in sorted(tier_compat):
            fig_box.add_trace(go.Box(
                y=tier_compat[t][:5000],  # subsample for rendering
                name=f"Tier {t}",
                marker_color=_TIER_COLORS.get(t, "#a0a0c0"),
                boxpoints=False,
            ))
        fig_box.update_layout(
            title="相性スコアのTier別分布",
            yaxis_title="compat_score",
        )

        # Chart 2b: Scatter of shared_works vs iv_min, colored by compat_score
        # Subsample for scatter
        rng = np.random.default_rng(42)
        n_scatter = min(5000, len(pair_scores))
        scatter_idx = rng.choice(len(pair_scores), size=n_scatter, replace=False)
        scatter_sw = []
        scatter_iv_min = []
        scatter_compat = []
        for i in scatter_idx:
            r = pair_scores[i]
            iv_min = min(r["iv1"], r["iv2"])
            if iv_min <= 0:
                continue
            scatter_sw.append(r["shared_works"])
            scatter_iv_min.append(iv_min)
            scatter_compat.append(r["shared_works"] * math.sqrt(iv_min))

        fig_scatter = go.Figure(go.Scattergl(
            x=scatter_sw, y=scatter_iv_min,
            mode="markers",
            marker=dict(
                size=4, color=scatter_compat,
                colorscale="Viridis", showscale=True,
                colorbar=dict(title="compat"),
                opacity=0.5,
            ),
            hovertemplate=(
                "shared_works=%{x}<br>min(iv)=%{y:.3f}"
                "<br>compat=%{marker.color:.2f}<extra></extra>"
            ),
        ))
        fig_scatter.update_layout(
            title="shared_works vs min(iv_score) — 色: 相性スコア",
            xaxis_title="shared_works",
            yaxis_title="min(iv_score)",
        )

        viz_html = (
            plotly_div_safe(fig_box, "chart_compat_tier", height=420)
            + plotly_div_safe(fig_scatter, "chart_compat_scatter", height=450)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="相性スコアのTier別分布",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "compat_score = shared_works x sqrt(min(iv_score_p1, iv_score_p2)). "
                "shared_worksは協業頻度、min(iv_score)はペアの下限スコアを反映する。"
                "Tier分類: 低IV側人材のモーダルscale_tier（最頻出Tier）を使用。"
                "散布図は最大5,000ペアをランダムサンプリング。"
            ),
            section_id="compat_score",
        )

    # ── Section 3: Repeat collaboration patterns ─────────────────

    def _build_repeat_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            # By primary_role: count persons with 3+ and 5+ shared works pairs
            rows = self.conn.execute("""
                SELECT fc.primary_role,
                       COUNT(DISTINCT pr.p1) AS n_persons,
                       SUM(CASE WHEN pr.shared_works >= 5 THEN 1 ELSE 0 END)
                           AS strong_repeat,
                       SUM(CASE WHEN pr.shared_works >= 10 THEN 1 ELSE 0 END)
                           AS deep_repeat,
                       COUNT(*) AS total_pairs,
                       AVG(pr.shared_works) AS avg_shared
                FROM _compat_pairs pr
                JOIN feat_career fc ON pr.p1 = fc.person_id
                WHERE fc.primary_role IS NOT NULL
                GROUP BY fc.primary_role
                ORDER BY total_pairs DESC
            """).fetchall()

            # By highest_stage: same metrics
            stage_rows = self.conn.execute("""
                SELECT fc.highest_stage,
                       COUNT(DISTINCT pr.p1) AS n_persons,
                       SUM(CASE WHEN pr.shared_works >= 5 THEN 1 ELSE 0 END)
                           AS strong_repeat,
                       COUNT(*) AS total_pairs,
                       AVG(pr.shared_works) AS avg_shared
                FROM _compat_pairs pr
                JOIN feat_career fc ON pr.p1 = fc.person_id
                WHERE fc.highest_stage IS NOT NULL
                GROUP BY fc.highest_stage
                ORDER BY fc.highest_stage
            """).fetchall()
        except Exception:
            rows = []
            stage_rows = []

        if not rows:
            return ReportSection(
                title="リピートコラボレーションパターン",
                findings_html="<p>リピート協業データが取得できませんでした。</p>",
                section_id="repeat",
            )

        total_pairs = sum(r["total_pairs"] for r in rows)
        total_strong = sum(r["strong_repeat"] for r in rows)
        total_deep = sum(r["deep_repeat"] for r in rows)

        findings = (
            f"<p>全{total_pairs:,}ペア中、"
            f"shared_works >= 5のペア: {total_strong:,}"
            f"（{100*total_strong/max(total_pairs,1):.1f}%）、"
            f"shared_works >= 10のペア: {total_deep:,}"
            f"（{100*total_deep/max(total_pairs,1):.1f}%）。</p>"
            "<p>primary_role別:</p><ul>"
        )
        for r in rows:
            strong_pct = 100 * r["strong_repeat"] / max(r["total_pairs"], 1)
            findings += (
                f"<li><strong>{r['primary_role']}</strong>: "
                f"{r['total_pairs']:,}ペア、"
                f"平均shared_works={r['avg_shared']:.1f}、"
                f"5回以上リピート率={strong_pct:.1f}%</li>"
            )
        findings += "</ul>"

        # Chart 3a: Grouped bar by primary_role
        roles = [r["primary_role"] for r in rows]
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            x=roles,
            y=[r["total_pairs"] - r["strong_repeat"] for r in rows],
            name=f"{_MIN_SHARED_WORKS}-4回",
            marker_color="#a0d2db",
            hovertemplate="%{x}: %{y:,}ペア<extra></extra>",
        ))
        fig_bar.add_trace(go.Bar(
            x=roles,
            y=[r["strong_repeat"] - r["deep_repeat"] for r in rows],
            name="5-9回",
            marker_color="#06D6A0",
            hovertemplate="%{x}: %{y:,}ペア<extra></extra>",
        ))
        fig_bar.add_trace(go.Bar(
            x=roles,
            y=[r["deep_repeat"] for r in rows],
            name="10回以上",
            marker_color="#f5576c",
            hovertemplate="%{x}: %{y:,}ペア<extra></extra>",
        ))
        fig_bar.update_layout(
            title="職種別リピート協業ペア数",
            xaxis_title="primary_role",
            yaxis_title="ペア数",
            barmode="stack",
        )

        # Chart 3b: Stage-based repeat rate
        if stage_rows:
            stages = [f"Stage {r['highest_stage']}" for r in stage_rows]
            repeat_rates = [
                100 * r["strong_repeat"] / max(r["total_pairs"], 1)
                for r in stage_rows
            ]
            avg_shared = [r["avg_shared"] for r in stage_rows]

            fig_stage = go.Figure()
            fig_stage.add_trace(go.Bar(
                x=stages, y=repeat_rates,
                name="5回以上リピート率 (%)",
                marker_color="#667eea",
                yaxis="y",
                hovertemplate="%{x}: %{y:.1f}%<extra></extra>",
            ))
            fig_stage.add_trace(go.Scatter(
                x=stages, y=avg_shared,
                name="平均shared_works",
                mode="lines+markers",
                line=dict(color="#FFD166", width=2),
                marker=dict(size=8),
                yaxis="y2",
                hovertemplate="%{x}: %{y:.1f}<extra></extra>",
            ))
            fig_stage.update_layout(
                title="キャリアステージ別リピート協業",
                yaxis=dict(title="5回以上リピート率 (%)", side="left"),
                yaxis2=dict(
                    title="平均shared_works", side="right",
                    overlaying="y", showgrid=False,
                ),
                legend=dict(orientation="h", y=1.12),
            )
            viz_html = (
                plotly_div_safe(fig_bar, "chart_repeat_role", height=420)
                + plotly_div_safe(fig_stage, "chart_repeat_stage", height=400)
            )
        else:
            viz_html = plotly_div_safe(fig_bar, "chart_repeat_role", height=420)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="リピートコラボレーションパターン",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "ペアの一方(p1)のfeat_careerからprimary_roleおよびhighest_stageを取得。"
                "p1 < p2の制約により、各ペアは1回のみカウントされる。"
                "リピート閾値: 5回（strong）、10回（deep）。"
            ),
            section_id="repeat",
        )

    # ── Section 4: Top pairs ─────────────────────────────────────

    def _build_top_pairs_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    pr.p1, pr.p2, pr.shared_works,
                    COALESCE(NULLIF(pa.name_ja,''), pa.name_en, pr.p1) AS name_a,
                    COALESCE(NULLIF(pb.name_ja,''), pb.name_en, pr.p2) AS name_b,
                    tp1.iv_score AS iv1, tp2.iv_score AS iv2,
                    fc1.primary_role AS role_a,
                    fc2.primary_role AS role_b
                FROM _compat_pairs pr
                JOIN persons pa ON pr.p1 = pa.id
                JOIN persons pb ON pr.p2 = pb.id
                JOIN _compat_top_persons tp1 ON pr.p1 = tp1.person_id
                JOIN _compat_top_persons tp2 ON pr.p2 = tp2.person_id
                LEFT JOIN feat_career fc1 ON pr.p1 = fc1.person_id
                LEFT JOIN feat_career fc2 ON pr.p2 = fc2.person_id
                ORDER BY pr.shared_works DESC
                LIMIT 30
            """).fetchall()

            # Also get top 20 by compat_score for second chart
            all_pairs = self.conn.execute("""
                SELECT
                    pr.p1, pr.p2, pr.shared_works,
                    COALESCE(NULLIF(pa.name_ja,''), pa.name_en, pr.p1) AS name_a,
                    COALESCE(NULLIF(pb.name_ja,''), pb.name_en, pr.p2) AS name_b,
                    tp1.iv_score AS iv1, tp2.iv_score AS iv2
                FROM _compat_pairs pr
                JOIN persons pa ON pr.p1 = pa.id
                JOIN persons pb ON pr.p2 = pb.id
                JOIN _compat_top_persons tp1 ON pr.p1 = tp1.person_id
                JOIN _compat_top_persons tp2 ON pr.p2 = tp2.person_id
                ORDER BY pr.shared_works * pr.shared_works
                         * MIN(tp1.iv_score, tp2.iv_score) DESC
                LIMIT 30
            """).fetchall()

        except Exception:
            rows = []
            all_pairs = []

        if not rows:
            return ReportSection(
                title="高協業ペアTop 30",
                findings_html="<p>トップペアデータが取得できませんでした。</p>",
                section_id="top_pairs",
            )

        findings = (
            f"<p>shared_works上位30ペアを表示する。"
            f"最多ペアのshared_works: {rows[0]['shared_works']}。</p>"
        )

        # Table: top 30 by shared_works
        table_rows = ""
        for i, r in enumerate(rows, 1):
            iv_min = min(r["iv1"], r["iv2"])
            compat = r["shared_works"] * math.sqrt(max(iv_min, 0))
            name_a_html = person_link(r["name_a"], r["p1"])
            name_b_html = person_link(r["name_b"], r["p2"])
            role_a = r["role_a"] or "-"
            role_b = r["role_b"] or "-"
            table_rows += (
                f"<tr>"
                f"<td>{i}</td>"
                f"<td>{name_a_html}</td>"
                f"<td>{role_a}</td>"
                f"<td>{name_b_html}</td>"
                f"<td>{role_b}</td>"
                f"<td>{r['shared_works']:,}</td>"
                f"<td>{compat:.2f}</td>"
                f"</tr>"
            )

        table_html = (
            '<div style="overflow-x:auto;">'
            '<table style="width:100%;border-collapse:collapse;font-size:0.85rem;">'
            "<thead><tr>"
            '<th style="padding:0.4rem;">#</th>'
            '<th style="padding:0.4rem;">人物A</th>'
            '<th style="padding:0.4rem;">役職A</th>'
            '<th style="padding:0.4rem;">人物B</th>'
            '<th style="padding:0.4rem;">役職B</th>'
            '<th style="padding:0.4rem;">共同作品数</th>'
            '<th style="padding:0.4rem;">相性スコア</th>'
            "</tr></thead>"
            f"<tbody>{table_rows}</tbody></table></div>"
        )

        # Chart 4: Horizontal bar of top 20 by compat_score
        if all_pairs:
            labels = []
            compats = []
            sws = []
            for r in all_pairs[:20]:
                iv_min = min(r["iv1"], r["iv2"])
                compat = r["shared_works"] * math.sqrt(max(iv_min, 0))
                label = f"{r['name_a']} x {r['name_b']}"
                labels.append(label)
                compats.append(compat)
                sws.append(r["shared_works"])

            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                y=list(reversed(labels)),
                x=list(reversed(compats)),
                orientation="h",
                marker_color="#f093fb",
                text=[f"sw={sw}" for sw in reversed(sws)],
                textposition="auto",
                hovertemplate="%{y}: compat=%{x:.2f}<extra></extra>",
            ))
            fig_bar.update_layout(
                title="相性スコアTop 20ペア",
                xaxis_title="compat_score",
                height=max(400, len(labels) * 30 + 100),
                margin=dict(l=250),
            )
            viz_html = table_html + plotly_div_safe(
                fig_bar, "chart_top_compat", height=max(400, len(labels) * 30 + 100)
            )
        else:
            viz_html = table_html

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f'[v2: {"; ".join(violations)}]</p>'
            )

        return ReportSection(
            title="高協業ペアTop 30",
            findings_html=findings,
            visualization_html=viz_html,
            method_note=(
                "shared_works降順で上位30ペアを表示。"
                "compat_score = shared_works x sqrt(min(iv_score_p1, iv_score_p2))。"
                "棒グラフはcompat_score降順Top 20。"
                "人名はpersonsテーブルのname_ja（なければname_en）を使用。"
            ),
            section_id="top_pairs",
        )
