"""Studio Impact report — v2 compliant.

Rich visualization port from original generate_studio_impact_report():
- Section 1: Causal Effect Identification (Forest Plot with CIs)
- Section 2: Studio comparison (top 20 bars + role stacked bar)
- Section 3: Studio K-Means clustering (violin + scatter)
- Section 4: Studio FE distribution by tier
- Section 5: Studio staff composition and network metrics
- Section 6: Top studios by average person FE
- Section 7: Studio timeseries summary (production scale trend)
"""

from __future__ import annotations

from pathlib import Path
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..helpers import load_json
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_TIER_COLORS = {1: "#667eea", 2: "#a0d2db", 3: "#06D6A0", 4: "#FFD166", 5: "#f5576c"}
_CLUSTER_COLORS = ["#f093fb", "#f5576c", "#fda085", "#a0d2db", "#06D6A0", "#FFD166"]


class StudioImpactReport(BaseReportGenerator):
    name = "studio_impact"
    title = "スタジオインパクト分析"
    subtitle = "スタジオFE分布・スタッフ構成・Tier別効果推定"
    filename = "studio_impact.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []

        # Rich sections from JSON data
        sections.append(sb.build_section(self._build_causal_section(sb)))
        sections.append(sb.build_section(self._build_studio_comparison_section(sb)))
        sections.append(sb.build_section(self._build_kmeans_section(sb)))

        # SQL-based sections
        sections.append(sb.build_section(self._build_studio_fe_tier_section(sb)))
        sections.append(sb.build_section(self._build_studio_staff_section(sb)))
        sections.append(sb.build_section(self._build_top_studios_section(sb)))
        sections.append(sb.build_section(self._build_studio_trend_section(sb)))
        return self.write_report("\n".join(sections))

    # ── Section 0a: Causal Effect Identification ────────────────

    def _build_causal_section(self, sb: SectionBuilder) -> ReportSection:
        causal = load_json("causal_identification.json")
        if not causal:
            return ReportSection(
                title="因果効果推定",
                findings_html="<p>因果効果推定データが取得できませんでした (causal_identification.json)。</p>",
                section_id="causal_id",
            )

        estimates = causal.get("causal_estimates", {})
        conclusion = causal.get("conclusion", {})
        sample = causal.get("sample_statistics", {})

        findings = (
            f"<p>因果効果推定: "
            f"{sample.get('total_trajectories', 0):,}軌跡、"
            f"{sample.get('total_transitions', 0):,}遷移。"
            f"支配的効果: {conclusion.get('dominant_effect', 'N/A')}、"
            f"信頼度: {conclusion.get('confidence_level', 'N/A')}。</p>"
        )

        # Forest Plot
        forest_data = []
        for ename, edata in estimates.items():
            ci = edata.get("confidence_interval", [0, 0])
            forest_data.append({
                "name": ename.replace("_", " ").title(),
                "estimate": edata.get("estimate", 0),
                "ci_lower": ci[0] if len(ci) > 0 else 0,
                "ci_upper": ci[1] if len(ci) > 1 else 0,
            })

        if not forest_data:
            return ReportSection(
                title="因果効果推定",
                findings_html=findings,
                section_id="causal_id",
            )

        findings += "<p>効果推定値（95%信頼区間付き）:</p><ul>"
        for fd in forest_data:
            findings += (
                f"<li><strong>{fd['name']}</strong>: "
                f"estimate={fd['estimate']:.4f}, "
                f"95% CI [{fd['ci_lower']:.4f}, {fd['ci_upper']:.4f}]</li>"
            )
        findings += "</ul>"

        names = [fd["name"] for fd in forest_data]
        ests = [fd["estimate"] for fd in forest_data]
        ci_lo = [fd["ci_lower"] for fd in forest_data]
        ci_hi = [fd["ci_upper"] for fd in forest_data]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            y=names, x=ests, mode="markers",
            marker=dict(size=10, color="#f093fb"),
            error_x=dict(
                type="data", symmetric=False,
                array=[h - e for h, e in zip(ci_hi, ests)],
                arrayminus=[e - lo for e, lo in zip(ests, ci_lo)],
                color="#f093fb",
            ),
            hovertemplate="%{y}: %{x:.4f}<extra></extra>",
        ))
        fig.add_vline(x=0, line_dash="dash", line_color="#a0a0c0")
        fig.update_layout(
            title="因果効果推定値 — Forest Plot（95% CI付き）",
            xaxis_title="効果量",
            height=max(300, len(forest_data) * 60 + 100),
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="因果効果推定 (Forest Plot)",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_causal_forest", height=max(350, len(forest_data)*60+100)),
            method_note=(
                "因果推定値は causal_identification.json 由来（Phase 9 causal モジュール）。"
                "フォレストプロット: 点 = 推定値、水平バー = 95% CI。"
                "ゼロラインを跨ぐ効果は統計的に有意ではない。"
                "選抜 = 高品質スタッフの採用。"
                "処置 = スタジオの育成効果。"
                "ブランド = 評判インフレ。"
            ),
            interpretation_html=(
                "<p>支配的効果タイプ（選抜 vs 処置 vs ブランド）は、"
                "スタジオが主に人材を採用しているのか、育成しているのか、"
                "あるいは評判効果の恩恵を受けているのかを示す。"
                "ただし、識別は観測変数を条件としてスタジオ間の移動が外生的であるという"
                "仮定に依存しており、この仮定が完全に成立する可能性は低い。</p>"
            ),
            section_id="causal_id",
        )

    # ── Section 0b: Studio comparison ───────────────────────────

    def _build_studio_comparison_section(self, sb: SectionBuilder) -> ReportSection:
        studios = load_json("studios.json")
        if not studios:
            return ReportSection(
                title="スタジオ比較",
                findings_html="<p>スタジオデータが取得できませんでした (studios.json)。</p>",
                section_id="studio_compare",
            )

        studio_list = sorted(studios.items(), key=lambda x: x[1].get("credit_count", 0), reverse=True)
        top20 = studio_list[:20]

        findings = (
            f"<p>データセット内{len(studios):,}スタジオ。"
            f"クレジット数最多: {top20[0][0]}（{top20[0][1].get('credit_count', 0):,}クレジット）。</p>"
        )

        fig = make_subplots(
            rows=1, cols=3,
            subplot_titles=("クレジット数", "人数", "作品数"),
        )
        fig.add_trace(go.Bar(
            x=[s[0] for s in top20],
            y=[s[1].get("credit_count", 0) for s in top20],
            marker_color="#f093fb",
            hovertemplate="%{x}: %{y:,}<extra></extra>",
        ), row=1, col=1)
        fig.add_trace(go.Bar(
            x=[s[0] for s in top20],
            y=[s[1].get("person_count", 0) for s in top20],
            marker_color="#a0d2db",
            hovertemplate="%{x}: %{y:,}<extra></extra>",
        ), row=1, col=2)
        fig.add_trace(go.Bar(
            x=[s[0] for s in top20],
            y=[s[1].get("anime_count", 0) for s in top20],
            marker_color="#fda085",
            hovertemplate="%{x}: %{y:,}<extra></extra>",
        ), row=1, col=3)
        fig.update_layout(
            title="Top 20 スタジオ",
            showlegend=False,
            xaxis_tickangle=-45, xaxis2_tickangle=-45, xaxis3_tickangle=-45,
        )

        # Role stacked bar
        role_color_map = {
            "director": "#9b59b6", "character_designer": "#3498db",
            "animator": "#06D6A0", "background_art": "#fda085",
            "screenplay": "#f5576c", "sound_director": "#FFD166",
        }
        studio_role_data: dict[str, dict[str, int]] = {}
        for s_name, s_data in top20:
            role_counts_s: dict[str, int] = {}
            for tp in s_data.get("top_persons", []):
                role = tp.get("primary_role", tp.get("role", "other"))
                role_counts_s[role] = role_counts_s.get(role, 0) + 1
            studio_role_data[s_name] = role_counts_s

        all_roles_s = set()
        for rc in studio_role_data.values():
            all_roles_s.update(rc.keys())

        fig_sr = go.Figure()
        default_colors = ["#9b59b6", "#3498db", "#06D6A0", "#fda085", "#f5576c",
                          "#FFD166", "#667eea", "#a0d2db", "#EF476F", "#f093fb"]
        for idx, role in enumerate(sorted(all_roles_s)):
            fig_sr.add_trace(go.Bar(
                x=list(studio_role_data.keys()),
                y=[studio_role_data[s].get(role, 0) for s in studio_role_data],
                name=role,
                marker_color=role_color_map.get(role, default_colors[idx % len(default_colors)]),
            ))
        fig_sr.update_layout(
            barmode="stack",
            title="Top 20 スタジオ — 主要人物の役職内訳（Stacked）",
            xaxis_tickangle=-45, yaxis_title="人数",
        )

        viz = (
            plotly_div_safe(fig, "chart_studio_top20", height=500) +
            plotly_div_safe(fig_sr, "chart_studio_role_stacked", height=500)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオ比較 (Top 20)",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "スタジオデータは studios.json 由来（Phase 9 スタジオ分析）。"
                "クレジット数上位20件。"
                "役職積み上げ棒はスタジオ別の上位人物の primary_role を表示。"
            ),
            section_id="studio_compare",
        )

    # ── Section 0c: K-Means studio clustering ───────────────────

    def _build_kmeans_section(self, sb: SectionBuilder) -> ReportSection:
        studios = load_json("studios.json")
        if not studios or len(studios) < 6:
            return ReportSection(
                title="スタジオK-Meansクラスタリング",
                findings_html="<p>クラスタリングに必要なスタジオ数が不足しています。</p>",
                section_id="studio_kmeans",
            )

        try:
            from sklearn.cluster import KMeans
            from sklearn.preprocessing import StandardScaler
        except ImportError:
            return ReportSection(
                title="スタジオK-Meansクラスタリング",
                findings_html="<p>sklearnが利用できません。</p>",
                section_id="studio_kmeans",
            )

        studio_feats = []
        for s_name, s_data in studios.items():
            pc = s_data.get("person_count", 0)
            ac = s_data.get("anime_count", 0)
            cc = s_data.get("credit_count", 0)
            aps = s_data.get("avg_person_score")
            if aps is None:
                tp_list = s_data.get("top_persons", [])
                sc_list = [tp.get("score", tp.get("iv_score", 0)) for tp in tp_list]
                aps = sum(sc_list) / len(sc_list) if sc_list else 0
            cpp = cc / max(pc, 1)
            studio_feats.append({
                "name": s_name, "person_count": pc, "anime_count": ac,
                "avg_score": float(aps or 0), "credit_per_person": cpp,
            })

        X = np.array([
            [d["person_count"], d["anime_count"], d["avg_score"], d["credit_per_person"]]
            for d in studio_feats
        ], dtype=float)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        n_clusters = min(6, len(studio_feats))
        km = KMeans(n_clusters=n_clusters, random_state=42, n_init=20)
        labels = km.fit_predict(X_scaled)
        centers = scaler.inverse_transform(km.cluster_centers_)

        # Dynamic naming
        cluster_names = self._name_studio_clusters(centers)
        for i, d in enumerate(studio_feats):
            d["cluster"] = int(labels[i])
            d["cluster_name"] = cluster_names[int(labels[i])]

        cluster_groups: dict[int, list] = {}
        for d in studio_feats:
            cluster_groups.setdefault(d["cluster"], []).append(d)

        findings = f"<p>K-Meansクラスタリング（K={n_clusters}）、4特徴量: person_count, anime_count, avg_score, credit_per_person。</p><ul>"
        for cid in sorted(cluster_groups):
            mems = cluster_groups[cid]
            avg_sc = sum(m["avg_score"] for m in mems) / len(mems)
            avg_pc = sum(m["person_count"] for m in mems) / len(mems)
            findings += (
                f"<li><strong>{cluster_names[cid]}</strong> (n={len(mems):,}): "
                f"平均スコア={avg_sc:.2f}、平均人数={avg_pc:.0f}</li>"
            )
        findings += "</ul>"

        # Scatter: person_count vs avg_score colored by cluster
        fig = go.Figure()
        for cid in sorted(cluster_groups):
            mems = cluster_groups[cid]
            fig.add_trace(go.Scatter(
                x=[m["person_count"] for m in mems],
                y=[m["avg_score"] for m in mems],
                mode="markers",
                name=cluster_names[cid],
                marker=dict(
                    size=[max(6, min(20, m["anime_count"] / 5)) for m in mems],
                    color=_CLUSTER_COLORS[cid % len(_CLUSTER_COLORS)],
                    opacity=0.75,
                ),
                text=[m["name"] for m in mems],
                hovertemplate="%{text}<br>規模: %{x}人<br>平均スコア: %{y:.2f}<extra></extra>",
            ))
        fig.update_layout(
            title=f"スタジオクラスタ散布図 (K={n_clusters})",
            xaxis_title="人数 (person_count)",
            yaxis_title="平均スコア (avg_score)",
            xaxis_type="log",
        )

        # Violin: avg_score distribution per cluster
        fig_v = go.Figure()
        for cid in sorted(cluster_groups):
            mems = cluster_groups[cid]
            vals = [m["avg_score"] for m in mems if m["avg_score"] > 0]
            if len(vals) >= 3:
                fig_v.add_trace(go.Violin(
                    y=vals, name=f"{cluster_names[cid]} (n={len(mems)})",
                    box_visible=True, meanline_visible=True,
                    line_color=_CLUSTER_COLORS[cid % len(_CLUSTER_COLORS)],
                    points="outliers" if len(vals) > 40 else "all",
                ))
        fig_v.update_layout(
            title="クラスタ別 スタジオ平均スコア分布 (Violin)",
            yaxis_title="平均スコア",
            violinmode="group",
        )

        viz = (
            plotly_div_safe(fig, "chart_studio_kmeans_scatter", height=500) +
            plotly_div_safe(fig_v, "chart_studio_kmeans_violin", height=450)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title=f"スタジオK-Meansクラスタリング ({n_clusters}クラスタ)",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                f"K-Means（K={n_clusters}、n_init=20）を StandardScaler 正規化特徴量に適用: "
                "person_count、anime_count、avg_score、credit_per_person。"
                "クラスタ名は重心ランクから決定。"
                "散布図のマーカーサイズ = anime_count/5（6〜20でクランプ）。X軸はログスケール。"
                "バイオリンはクラスタ別の avg_score 分布を表示。"
            ),
            section_id="studio_kmeans",
        )

    @staticmethod
    def _name_studio_clusters(centers: np.ndarray) -> dict[int, str]:
        feat_labels = [
            ["大規模", "中規模", "小規模", "極小", "零細", "個人"],
            [],  # skip anime_count
            ["高品質", "中品質", "低活動", "未評価", "新興", "不明"],
            ["多産", "中産", "少産", "寡作", "極寡作", "新規"],
        ]
        names: dict[int, str] = {}
        # Primary name from person_count
        size_order = np.argsort(-centers[:, 0])
        score_order = np.argsort(-centers[:, 2])
        for rank, cid in enumerate(size_order):
            primary = feat_labels[0][min(rank, len(feat_labels[0]) - 1)]
            score_rank = int(np.where(score_order == cid)[0][0])
            secondary = feat_labels[2][min(score_rank, len(feat_labels[2]) - 1)]
            names[int(cid)] = f"{primary}×{secondary}"
        return names

    # ── Section 1: Studio FE by tier ─────────────────────────────

    def _build_studio_fe_tier_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fsa.studio_id,
                    COALESCE(fsa.studio_name, fsa.studio_id) AS studio_label,
                    AVG(fps.iv_score) AS avg_iv,
                    AVG(fwc.scale_tier) AS avg_tier,
                    COUNT(DISTINCT fsa.person_id) AS n_staff,
                    COUNT(DISTINCT fcc.anime_id) AS n_works
                FROM feat_studio_affiliation fsa
                JOIN feat_person_scores fps ON fsa.person_id = fps.person_id
                JOIN feat_credit_contribution fcc
                    ON fsa.person_id = fcc.person_id AND fsa.credit_year = fcc.credit_year
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fps.iv_score IS NOT NULL
                  AND fwc.scale_tier IS NOT NULL
                GROUP BY fsa.studio_id, studio_label
                HAVING n_staff >= 5
                ORDER BY avg_iv DESC
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スタジオ平均IVスコア × Tier",
                findings_html="<p>スタジオFE-Tierデータが取得できませんでした。</p>",
                section_id="studio_fe_tier",
            )

        iv_vals = [r["avg_iv"] for r in rows]
        summ = distribution_summary(iv_vals, label="avg_iv")

        findings = (
            f"<p>スタジオ別平均IVスコア vs 平均作品Tier（{summ['n']:,}スタジオ、最低5名以上）: "
            f"{format_distribution_inline(summ)}, "
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
            "<p>平均Tierはスタジオが制作した全作品のscale_tierの平均値。</p>"
        )

        filtered = [r for r in rows if r["avg_tier"] is not None]
        fig = go.Figure(go.Scatter(
            x=[r["avg_tier"] for r in filtered],
            y=[r["avg_iv"] for r in filtered],
            mode="markers",
            marker=dict(
                color=[r["n_works"] for r in filtered],
                colorscale="Viridis",
                size=8, opacity=0.7,
                colorbar=dict(title="作品数"),
            ),
            text=[r["studio_label"] for r in filtered],
            hovertemplate="%{text}<br>avg_tier=%{x:.2f}, avg_iv=%{y:.3f}<extra></extra>",
        ))
        fig.update_layout(
            title="スタジオ平均IVスコア vs 平均作品Tier",
            xaxis_title="平均Scale Tier", yaxis_title="平均IVスコア",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオ平均IVスコア × Tier",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_studio_fe_tier", height=440),
            method_note=(
                "feat_studio_affiliation × feat_person_scores × feat_credit_contribution × "
                "feat_work_context のJOINにより、スタジオ別平均IVスコアと平均Tierを算出。"
                "カラー=作品数。"
                "スタジオ平均IVスコアとscale_tierは制作規模を共有するため、"
                "正の相関は部分的に構造的。"
            ),
            section_id="studio_fe_tier",
        )

    # ── Section 2: Studio staff composition ──────────────────────

    def _build_studio_staff_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fsa.studio_id,
                    COUNT(DISTINCT fsa.person_id) AS n_staff,
                    AVG(fps.person_fe) AS avg_person_fe,
                    COUNT(CASE WHEN p.gender = 'Female' THEN 1 END) AS n_female,
                    COUNT(CASE WHEN p.gender = 'Male' THEN 1 END) AS n_male
                FROM feat_studio_affiliation fsa
                JOIN persons p ON fsa.person_id = p.id
                LEFT JOIN feat_person_scores fps ON fsa.person_id = fps.person_id
                GROUP BY fsa.studio_id
                HAVING n_staff >= 5
                ORDER BY n_staff DESC
                LIMIT 30
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="スタジオスタッフ構成",
                findings_html="<p>スタジオスタッフデータが取得できませんでした。</p>",
                section_id="studio_staff",
            )

        n_staff_vals = [r["n_staff"] for r in rows]
        fe_vals = [r["avg_person_fe"] for r in rows if r["avg_person_fe"] is not None]

        findings = (
            f"<p>スタジオスタッフ構成（上位30スタジオ、最低5名以上）。"
            f"スタッフ数範囲: {min(n_staff_vals):,}–{max(n_staff_vals):,}。"
            "avg_person_fe = スタジオ所属スタッフのAKM個人固定効果の平均値。</p>"
        )
        if fe_vals:
            fs = distribution_summary(fe_vals, label="avg_person_fe")
            findings += (
                f"<p>スタジオ間avg_person_fe分布: "
                f"{format_distribution_inline(fs)}, "
                f"{format_ci((fs['ci_lower'], fs['ci_upper']))}。</p>"
            )

        fig = go.Figure(go.Bar(
            x=[str(r["studio_id"])[:12] for r in rows],
            y=n_staff_vals,
            marker_color="#a0d2db",
            hovertemplate="Studio %{x}: %{y:,} staff<extra></extra>",
        ))
        fig.update_layout(title="スタッフ数上位30スタジオ", xaxis_title="スタジオID", yaxis_title="スタッフ数")

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオスタッフ構成",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_studio_staff", height=400),
            method_note=(
                "n_staff = distinct person_id in feat_studio_affiliation per studio. "
                "avg_person_fe = mean of feat_person_scores.person_fe for staff at studio. "
                "Top 30 studios by total staff shown. Persons not in AKM connected set "
                "have NULL person_fe and are excluded from avg_person_fe."
            ),
            section_id="studio_staff",
        )

    # ── Section 3: Top studios by avg person FE ───────────────────

    def _build_top_studios_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    fsa.studio_id,
                    s.name AS studio_name,
                    AVG(fps.person_fe) AS avg_person_fe,
                    COUNT(DISTINCT fsa.person_id) AS n_staff
                FROM feat_studio_affiliation fsa
                LEFT JOIN studios s ON fsa.studio_id = s.id
                JOIN feat_person_scores fps ON fsa.person_id = fps.person_id
                WHERE fps.person_fe IS NOT NULL
                GROUP BY fsa.studio_id, s.name
                HAVING n_staff >= 5
                ORDER BY avg_person_fe DESC
                LIMIT 20
            """).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="平均個人FE上位スタジオ",
                findings_html="<p>スタジオ別個人FEランキングデータが取得できませんでした。</p>",
                section_id="top_studios",
            )

        findings = (
            "<p>スタッフ平均person_fe上位20スタジオ（FE推定済みスタッフ5名以上）。"
            "person_feはAKMにより推定された個人貢献度であり、スタジオ効果を統制した値。</p>"
        )

        table_rows = "".join(
            f"<tr>"
            f"<td>{i}</td>"
            f"<td>{r['studio_name'] or r['studio_id']}</td>"
            f"<td>{r['n_staff']:,}</td>"
            f"<td>{r['avg_person_fe']:.4f}</td>"
            f"</tr>"
            for i, r in enumerate(rows, 1)
        )
        table_html = (
            '<div style="overflow-x:auto;"><table>'
            "<thead><tr><th>#</th><th>スタジオ</th><th>スタッフ数（FE推定済）</th>"
            "<th>平均 Person FE</th></tr></thead>"
            f"<tbody>{table_rows}</tbody></table></div>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="平均個人FE上位スタジオ",
            findings_html=findings,
            visualization_html=table_html,
            method_note=(
                "スタッフの avg(person_fe) で順位付け。"
                "person_fe が非 NULL のスタッフが最低5人必要。"
                "これはスタジオランキングではなく、avg person_fe の高いスタジオは"
                "高FEの人物を惹きつけ・育成した可能性もあれば、"
                "共同制作によって所属帰属にバイアスがかかっている可能性もある。"
            ),
            section_id="top_studios",
        )

    # ── Section 4: Studio production scale trend ──────────────────

    def _build_studio_trend_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT
                    asj.studio_id,
                    COALESCE(s.name, asj.studio_id) AS studio_label,
                    a.year,
                    COUNT(DISTINCT a.id) AS n_works,
                    AVG(fwc.scale_tier) AS avg_tier
                FROM anime_studios asj
                JOIN anime a ON asj.anime_id = a.id
                JOIN feat_work_context fwc ON fwc.anime_id = a.id
                LEFT JOIN studios s ON asj.studio_id = s.id
                WHERE a.year BETWEEN 1990 AND 2024
                  AND fwc.scale_tier IS NOT NULL
                GROUP BY asj.studio_id, studio_label, a.year
            """).fetchall()
            # Top 5 studios by total works
            studio_totals: dict[str, int] = {}
            studio_labels: dict[str, str] = {}
            for r in rows:
                sid = r["studio_id"]
                studio_totals[sid] = studio_totals.get(sid, 0) + r["n_works"]
                studio_labels[sid] = r["studio_label"]
            top_studios = sorted(studio_totals, key=lambda s: -studio_totals[s])[:5]
        except Exception:
            rows = []
            top_studios = []
            studio_labels = {}

        if not rows or not top_studios:
            return ReportSection(
                title="スタジオ制作規模トレンド",
                findings_html="<p>スタジオトレンドデータが取得できませんでした。</p>",
                section_id="studio_trend",
            )

        studio_year: dict[str, dict[int, float]] = {}
        for r in rows:
            if r["studio_id"] in top_studios:
                studio_year.setdefault(r["studio_id"], {})[r["year"]] = r["avg_tier"]

        years = list(range(1990, 2025))
        findings = (
            "<p>作品数上位5スタジオの平均制作規模Tier推移（1990–2024年）。"
            "各スタジオの制作規模がどう変化したかを示す。</p>"
        )

        fig = go.Figure()
        colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#667eea"]
        for i, sid in enumerate(top_studios):
            fig.add_trace(go.Scatter(
                x=years,
                y=[studio_year.get(sid, {}).get(y) for y in years],
                name=studio_labels.get(sid, str(sid))[:20],
                mode="lines+markers",
                line=dict(color=colors[i % len(colors)]),
                connectgaps=True,
            ))
        fig.update_layout(
            title="スタジオ別平均Scale Tier推移（上位5、1990–2024）",
            xaxis_title="年", yaxis_title="平均Scale Tier",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スタジオ制作規模トレンド",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_studio_trend", height=420),
            method_note=(
                "anime_studios × anime × feat_work_context のJOINにより、"
                "スタジオ別・年別の平均scale_tierを算出。"
                "作品数上位5スタジオを表示。線の途切れはクレジット作品なしの年を示す。"
            ),
            section_id="studio_trend",
        )
