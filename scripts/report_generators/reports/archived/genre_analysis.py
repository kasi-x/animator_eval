# DEPRECATED (Phase 3-5, 2026-04-19): merged into biz_genre_whitespace. ジャンル需給詳細章に吸収.
# This module is retained in archived/ for regeneration and audit only.
# It is NOT in V2_REPORT_CLASSES and will not run in default generation.
"""Genre Analysis report — v2 compliant.

Rich visualization port from original generate_genre_report():
- Section 1: Summary stats grid (specialist/generalist breakdown)
- Section 2: Staff K-Means clustering (k=6) with PCA 2D scatter
- Section 3: Cluster profile heatmap (z-score of centroids)
- Section 4: IV Score violin/raincloud per cluster
- Section 5: Cluster × era stacked bar
- Section 6: Cluster × role stacked bar
- Section 7: Career decade × cluster time series
- Section 8: Genre-era clustering heatmap + radar
- Section 9: Era distribution pie
"""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import plotly.graph_objects as go

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..helpers import get_feat_genre_affinity, get_feat_person_scores
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_CLUSTER_COLORS = [
    "#f093fb", "#f5576c", "#fda085", "#a0d2db",
    "#06D6A0", "#FFD166", "#667eea", "#43e97b",
]
_ERA_COLORS = {
    "modern": "#f093fb", "2010s": "#a0d2db",
    "2000s": "#fda085", "classic": "#667eea", "unknown": "#888",
}
_ERA_ORDER = ["modern", "2010s", "2000s", "classic"]
_ERA_JP = {
    "modern": "現代(2020+)", "2010s": "2010年代",
    "2000s": "2000年代", "classic": "クラシック(〜1999)",
    "unknown": "不明",
}

FEATURE_COLS = [
    "iv_score", "birank", "patronage", "person_fe",
    "degree", "betweenness", "eigenvector",
    "versatility", "active_years", "highest_stage",
    "hub_score", "collaborators",
    "total_credits", "modern_pct", "era_2010s_pct",
    "era_2000s_pct", "classic_pct", "tier_concentration",
]
FEAT_JP = {
    "iv_score": "IV Score", "birank": "BiRank",
    "patronage": "Patronage", "person_fe": "Person FE",
    "degree": "次数中心性", "betweenness": "媒介中心性",
    "eigenvector": "固有値中心性", "versatility": "多様性",
    "active_years": "活動年数", "highest_stage": "最高ステージ",
    "hub_score": "ハブスコア", "collaborators": "共同作業者数",
    "total_credits": "総クレジット数", "modern_pct": "現代割合",
    "era_2010s_pct": "2010年代割合", "era_2000s_pct": "2000年代割合",
    "classic_pct": "クラシック割合", "tier_concentration": "ティア集中度",
}


def _name_clusters_by_rank(
    centers: np.ndarray,
    feat_specs: list[tuple[int, list[str]]],
) -> dict[int, str]:
    """Dynamic cluster naming based on centroid rank per feature."""
    k = len(centers)
    parts: dict[int, list[str]] = {i: [] for i in range(k)}
    for feat_idx, labels in feat_specs:
        order = np.argsort(-centers[:, feat_idx])
        for rank, cid in enumerate(order):
            lbl = labels[min(rank, len(labels) - 1)]
            parts[int(cid)].append(lbl)
    return {cid: "×".join(ps[:2]) for cid, ps in parts.items()}


class GenreAnalysisReport(BaseReportGenerator):
    name = "genre_analysis"
    title = "ジャンル・スコア親和性分析"
    subtitle = "スタッフクラスタリング・ジャンル年代クラスタリング（K-Means + PCA）"
    filename = "genre_analysis.html"

    _EXTRA_CSS = """
<style>
.stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 1rem; margin: 1.5rem 0;
}
.stat-card {
    background: linear-gradient(135deg, rgba(240,147,251,0.15), rgba(245,87,108,0.1));
    border: 1px solid rgba(240,147,251,0.2);
    border-radius: 12px; padding: 1.2rem; text-align: center;
}
.stat-card .value {
    font-size: 1.8rem; font-weight: 800;
    background: linear-gradient(135deg, #f093fb, #f5576c);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
}
.stat-card .label { font-size: 0.82rem; color: #a0a0c0; margin-top: 0.2rem; }
</style>
"""

    def generate(self) -> Path | None:
        # Load data from JSON (DB tables not available for genre affinity)
        genre_data = get_feat_genre_affinity()
        scores_list = get_feat_person_scores()
        if not genre_data or not scores_list:
            return None

        scores_map = {p["person_id"]: p for p in scores_list if isinstance(p, dict)}

        # Build joint dataset
        persons = self._build_persons(genre_data, scores_map)
        if len(persons) < 10:
            return None

        sb = SectionBuilder()
        sections: list[str] = []

        sections.append(sb.build_section(self._build_summary_section(sb, persons)))

        # K-Means clustering
        cluster_result = self._run_staff_clustering(persons)
        if cluster_result:
            persons, cluster_names, n_clusters, pca_model, scaler, km = cluster_result
            sections.append(sb.build_section(
                self._build_pca_scatter_section(sb, persons, cluster_names, n_clusters, pca_model)))
            sections.append(sb.build_section(
                self._build_heatmap_section(sb, km, scaler, cluster_names, n_clusters)))
            sections.append(sb.build_section(
                self._build_cluster_violin_section(sb, persons, cluster_names, n_clusters)))
            sections.append(sb.build_section(
                self._build_cluster_era_section(sb, persons, cluster_names, n_clusters)))
            sections.append(sb.build_section(
                self._build_cluster_role_section(sb, persons, cluster_names, n_clusters)))
            sections.append(sb.build_section(
                self._build_decade_cluster_section(sb, persons, cluster_names, n_clusters)))

        # Genre-era clustering
        sections.append(sb.build_section(self._build_genre_era_cluster_section(sb, persons)))

        # Era pie
        sections.append(sb.build_section(self._build_era_pie_section(sb, persons)))

        body = self._EXTRA_CSS + "\n".join(sections)
        return self.write_report(body)

    # ── Data building ───────────────────────────────────────────

    @staticmethod
    def _build_persons(
        genre_data: dict, scores_map: dict[str, dict],
    ) -> list[dict[str, Any]]:
        persons: list[dict[str, Any]] = []
        for pid, gdata in genre_data.items():
            sdata = scores_map.get(pid, {})
            cent = sdata.get("centrality", {})
            vers = sdata.get("versatility", {})
            car = sdata.get("career", {})
            net = sdata.get("network", {})
            tiers = gdata.get("score_tiers", {})
            eras = gdata.get("eras", {})

            persons.append({
                "person_id": pid,
                "name": sdata.get("name", pid),
                "primary_role": sdata.get("primary_role", "unknown"),
                "iv_score": float(sdata.get("iv_score", 0) or 0),
                "birank": float(sdata.get("birank", 0) or 0),
                "patronage": float(sdata.get("patronage", 0) or 0),
                "person_fe": float(sdata.get("person_fe", 0) or 0),
                "degree": float(cent.get("degree", 0) or 0),
                "betweenness": float(cent.get("betweenness", 0) or 0),
                "eigenvector": float(cent.get("eigenvector", 0) or 0),
                "versatility": float(vers.get("score", 0) or 0),
                "active_years": float(car.get("active_years", 0) or 0),
                "highest_stage": float(car.get("highest_stage", 0) or 0),
                "hub_score": float(net.get("hub_score", 0) or 0),
                "collaborators": float(net.get("collaborators", 0) or 0),
                "first_year": car.get("first_year"),
                "primary_tier": gdata.get("primary_tier", "unknown"),
                "primary_era": gdata.get("primary_era", "unknown"),
                "total_credits": float(gdata.get("total_credits", 0) or 0),
                "modern_pct": float(eras.get("modern", 0) or 0),
                "era_2010s_pct": float(eras.get("2010s", 0) or 0),
                "era_2000s_pct": float(eras.get("2000s", 0) or 0),
                "classic_pct": float(eras.get("classic", 0) or 0),
                "tier_concentration": float(max(tiers.values()) if tiers else 0),
            })
        return persons

    # ── K-Means clustering ──────────────────────────────────────

    @staticmethod
    def _run_staff_clustering(persons: list[dict]) -> tuple | None:
        try:
            from sklearn.cluster import KMeans
            from sklearn.decomposition import PCA
            from sklearn.preprocessing import StandardScaler
        except ImportError:
            return None

        n_clusters = min(6, max(2, len(persons)))
        if len(persons) < n_clusters:
            return None

        X = np.array([[p[f] for f in FEATURE_COLS] for p in persons], dtype=float)
        scaler = StandardScaler()
        Xs = scaler.fit_transform(X)

        km = KMeans(n_clusters=n_clusters, n_init=20, random_state=42)
        labels = km.fit_predict(Xs)

        pca = PCA(n_components=2, random_state=42)
        Xpca = pca.fit_transform(Xs)

        centers_orig = scaler.inverse_transform(km.cluster_centers_)
        fidx = {f: i for i, f in enumerate(FEATURE_COLS)}
        cluster_names = _name_clusters_by_rank(
            centers_orig,
            [
                (fidx["iv_score"], ["高スコア", "中スコア", "低スコア"]),
                (fidx["total_credits"], ["多作", "少作"]),
                (fidx["modern_pct"], ["現代", "旧世代"]),
            ],
        )

        for i, p in enumerate(persons):
            p["staff_cluster"] = int(labels[i])
            p["staff_cluster_name"] = cluster_names[int(labels[i])]
            p["pca_x"] = float(Xpca[i, 0])
            p["pca_y"] = float(Xpca[i, 1])

        return persons, cluster_names, n_clusters, pca, scaler, km

    # ── Section 1: Summary stats ────────────────────────────────

    def _build_summary_section(self, sb: SectionBuilder, persons: list[dict]) -> ReportSection:
        total = len(persons)
        specialist_cnt = sum(1 for p in persons if p["tier_concentration"] >= 80)
        generalist_pct = (total - specialist_cnt) / max(total, 1) * 100
        all_primary_eras: Counter = Counter(p["primary_era"] for p in persons)

        findings = f"<p>ジャンル・スコア親和性分析の対象: {total:,}人。</p>"

        stats_html = '<div class="stats-grid">'
        for label, val in [
            ("対象人物数", f"{total:,}"),
            ("スペシャリスト（80%+集中）", f"{specialist_cnt:,}"),
            ("ジェネラリスト比率", f"{generalist_pct:.1f}%"),
            ("時代区分", f"{len(all_primary_eras)}"),
        ]:
            stats_html += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
        stats_html += "</div>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="ジャンル・スコア親和性 — 概要",
            findings_html=findings,
            visualization_html=stats_html,
            method_note=(
                "データ: genre_affinity.json（Phase 9 ジャンル親和性モジュール）+ "
                "scores.json（Phase 8 スコアリング）。"
                "スペシャリスト = tier_concentration ≥ 80%（単一スコアティア優勢）。"
                f"ジャンル分析対象人数: {total:,}名（ネットワーク位置上位の人物）。"
            ),
            section_id="genre_summary",
        )

    # ── Section 2: PCA scatter ──────────────────────────────────

    def _build_pca_scatter_section(
        self, sb: SectionBuilder, persons: list[dict],
        cluster_names: dict[int, str], n_clusters: int,
        pca_model: Any,
    ) -> ReportSection:
        cluster_groups: dict[int, list] = {}
        for p in persons:
            cluster_groups.setdefault(p["staff_cluster"], []).append(p)

        findings = (
            f"<p>K-Meansクラスタリング（K={n_clusters}）: {len(FEATURE_COLS)}特徴量、"
            f"{len(persons):,}人を対象。"
            f"PCA寄与率: PC1={pca_model.explained_variance_ratio_[0]*100:.1f}%、"
            f"PC2={pca_model.explained_variance_ratio_[1]*100:.1f}%。</p>"
            "<p>クラスタサイズ:</p><ul>"
        )
        for cid in sorted(cluster_groups):
            findings += f"<li><strong>{cluster_names[cid]}</strong>: n={len(cluster_groups[cid]):,}</li>"
        findings += "</ul>"

        fig = go.Figure()
        for cid in sorted(cluster_groups):
            mems = cluster_groups[cid]
            fig.add_trace(go.Scattergl(
                x=[m["pca_x"] for m in mems],
                y=[m["pca_y"] for m in mems],
                mode="markers",
                name=cluster_names[cid],
                marker=dict(size=4, color=_CLUSTER_COLORS[cid % len(_CLUSTER_COLORS)], opacity=0.6),
                text=[m["name"] for m in mems],
                hovertemplate="%{text}<br>PCA(%{x:.2f}, %{y:.2f})<extra></extra>",
            ))
        fig.update_layout(
            title="スタッフクラスタリング PCA 2D",
            xaxis_title=f"PC1 ({pca_model.explained_variance_ratio_[0]*100:.1f}%)",
            yaxis_title=f"PC2 ({pca_model.explained_variance_ratio_[1]*100:.1f}%)",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title=f"スタッフ K-Means クラスタリング（{n_clusters}クラスタ）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_pca_scatter", height=550),
            method_note=(
                f"K-Means (K={n_clusters}, n_init=20) をStandardScalerで正規化した"
                f"{len(FEATURE_COLS)}特徴量に適用: {', '.join(FEATURE_COLS[:6])}... "
                "可視化用にPCA 2D。クラスタ名はiv_score、total_credits、modern_pctの"
                "重心ランクから導出。"
            ),
            section_id="pca_scatter",
        )

    # ── Section 3: Cluster heatmap ──────────────────────────────

    def _build_heatmap_section(
        self, sb: SectionBuilder, km: Any, scaler: Any,
        cluster_names: dict[int, str], n_clusters: int,
    ) -> ReportSection:
        HEATMAP_FEATS = [
            "iv_score", "birank", "patronage", "person_fe",
            "betweenness", "versatility", "active_years",
            "total_credits", "modern_pct", "classic_pct",
            "tier_concentration",
        ]
        fidx_all = {f: i for i, f in enumerate(FEATURE_COLS)}
        centers_z = km.cluster_centers_
        z_vals = [
            [centers_z[cid, fidx_all[f]] for f in HEATMAP_FEATS]
            for cid in range(n_clusters)
        ]

        findings = (
            "<p>主要特徴量におけるクラスタ重心のz-score。"
            "赤 = 当該クラスタで母集団平均より高い特徴量、"
            "青 = 低い特徴量。</p>"
        )

        fig = go.Figure(go.Heatmap(
            z=z_vals,
            x=[FEAT_JP.get(f, f) for f in HEATMAP_FEATS],
            y=[cluster_names[c] for c in range(n_clusters)],
            colorscale="RdBu", zmid=0,
            colorbar=dict(title="z-score"),
            hovertemplate="クラスタ: %{y}<br>特徴: %{x}<br>z-score: %{z:.2f}<extra></extra>",
        ))
        fig.update_layout(
            title="クラスタプロファイル Heatmap（z-score）",
            xaxis_title="特徴量", yaxis_title="クラスタ",
            xaxis_tickangle=-35,
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="クラスタプロファイル Heatmap",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cluster_heatmap", height=500),
            method_note=(
                "Heatmapは各クラスタのz-score化された重心値を示す。"
                "RdBuカラースケール: 赤 = 平均より高い、青 = 低い。"
                "表示する特徴量はクラスタリングに用いた全特徴量の一部。"
            ),
            section_id="cluster_heatmap",
        )

    # ── Section 4: IV Score violin per cluster ──────────────────

    def _build_cluster_violin_section(
        self, sb: SectionBuilder, persons: list[dict],
        cluster_names: dict[int, str], n_clusters: int,
    ) -> ReportSection:
        cluster_groups: dict[int, list] = {}
        for p in persons:
            cluster_groups.setdefault(p["staff_cluster"], []).append(p)

        findings = "<p>クラスタ別のIV score分布:</p><ul>"
        for cid in sorted(cluster_groups):
            vals = [m["iv_score"] for m in cluster_groups[cid]]
            if vals:
                s = distribution_summary(vals, label=cluster_names[cid])
                findings += (
                    f"<li><strong>{cluster_names[cid]}</strong>（n={s['n']:,}）: "
                    f"{format_distribution_inline(s)}、"
                    f"{format_ci((s['ci_lower'], s['ci_upper']))}</li>"
                )
        findings += "</ul>"

        fig = go.Figure()
        for cid in sorted(cluster_groups):
            mems = cluster_groups[cid]
            vals = [m["iv_score"] for m in mems]
            if len(vals) >= 3:
                fig.add_trace(go.Violin(
                    y=vals,
                    name=cluster_names[cid],
                    box_visible=True, meanline_visible=True,
                    line_color=_CLUSTER_COLORS[cid % len(_CLUSTER_COLORS)],
                    fillcolor=f"rgba({','.join(str(int(_CLUSTER_COLORS[cid % len(_CLUSTER_COLORS)].lstrip('#')[i:i+2], 16)) for i in (0,2,4))},0.3)",
                    points="outliers" if len(vals) > 40 else "all",
                ))
        fig.update_layout(
            title="クラスタ別 IV Score分布 (Violin)",
            yaxis_title="IVスコア",
            violinmode="group",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="クラスタ別 IV Score分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cluster_violin", height=480),
            method_note=(
                "K-Meansクラスタごとのiv_scoreのViolinプロット。"
                "Box = IQR、線 = 中央値、破線 = 平均。"
                "小規模クラスタ（n<40）は全点表示、大規模は外れ値のみ。"
            ),
            section_id="cluster_violin",
        )

    # ── Section 5: Cluster × era ────────────────────────────────

    def _build_cluster_era_section(
        self, sb: SectionBuilder, persons: list[dict],
        cluster_names: dict[int, str], n_clusters: int,
    ) -> ReportSection:
        cluster_groups: dict[int, list] = {}
        for p in persons:
            cluster_groups.setdefault(p["staff_cluster"], []).append(p)

        findings = "<p>クラスタ別の主要活動時代構成:</p>"

        fig = go.Figure()
        for era in _ERA_ORDER + ["unknown"]:
            counts = [
                sum(1 for m in cluster_groups.get(cid, []) if m["primary_era"] == era)
                for cid in range(n_clusters)
            ]
            if any(c > 0 for c in counts):
                fig.add_trace(go.Bar(
                    name=_ERA_JP.get(era, era),
                    x=[cluster_names[c] for c in range(n_clusters)],
                    y=counts,
                    marker_color=_ERA_COLORS.get(era, "#888"),
                ))
        fig.update_layout(
            title="クラスタ × 主要活動時代（人数）",
            barmode="stack", xaxis_tickangle=-20, yaxis_title="人数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="クラスタ × 活動時代",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cluster_era", height=450),
            method_note=(
                "積み上げ棒: クラスタごとにgenre_affinity.jsonのprimary_eraを集計。"
                "primary_era = 各人物において最もクレジット比率が高い時代。"
            ),
            section_id="cluster_era",
        )

    # ── Section 6: Cluster × role ───────────────────────────────

    def _build_cluster_role_section(
        self, sb: SectionBuilder, persons: list[dict],
        cluster_names: dict[int, str], n_clusters: int,
    ) -> ReportSection:
        cluster_groups: dict[int, list] = {}
        for p in persons:
            cluster_groups.setdefault(p["staff_cluster"], []).append(p)

        role_counter: Counter = Counter(p["primary_role"] for p in persons)
        top_roles = [r for r, _ in role_counter.most_common(8)]

        findings = f"<p>クラスタ横断の上位8役職: {', '.join(top_roles)}。</p>"

        fig = go.Figure()
        role_colors = _CLUSTER_COLORS[:8]
        for ri, role in enumerate(top_roles):
            counts = [
                sum(1 for m in cluster_groups.get(cid, []) if m["primary_role"] == role)
                for cid in range(n_clusters)
            ]
            if any(c > 0 for c in counts):
                fig.add_trace(go.Bar(
                    name=role,
                    x=[cluster_names[c] for c in range(n_clusters)],
                    y=counts,
                    marker_color=role_colors[ri % len(role_colors)],
                ))
        fig.update_layout(
            title="クラスタ × 主要ロール（人数）",
            barmode="stack", xaxis_tickangle=-20, yaxis_title="人数",
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="クラスタ × ロール構成",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cluster_role", height=450),
            method_note=(
                "積み上げ棒: クラスタごとにscores.jsonのprimary_roleを集計。"
                "総件数上位8ロールを表示。"
                "primary_role = 各人物において最もクレジット数の多いロール。"
            ),
            section_id="cluster_role",
        )

    # ── Section 7: Decade × cluster ─────────────────────────────

    def _build_decade_cluster_section(
        self, sb: SectionBuilder, persons: list[dict],
        cluster_names: dict[int, str], n_clusters: int,
    ) -> ReportSection:
        decade_cluster: dict[str, dict[int, int]] = {}
        for p in persons:
            fy = p.get("first_year")
            if fy and isinstance(fy, (int, float)) and 1960 <= fy <= 2030:
                decade = f"{int(fy) // 10 * 10}年代"
            else:
                decade = "不明"
            if decade not in decade_cluster:
                decade_cluster[decade] = defaultdict(int)
            decade_cluster[decade][p.get("staff_cluster", 0)] += 1

        decades_sorted = sorted(d for d in decade_cluster if d != "不明")
        if len(decades_sorted) < 2:
            return ReportSection(
                title="キャリア開始年代別クラスタ構成",
                findings_html="<p>時系列分析に必要な年代データが不足しています。</p>",
                section_id="decade_cluster",
            )

        findings = (
            f"<p>キャリア開始年代別のクラスタ構成（{decades_sorted[0]}–{decades_sorted[-1]}）。</p>"
        )

        fig = go.Figure()
        for cid in range(n_clusters):
            counts = [decade_cluster.get(d, {}).get(cid, 0) for d in decades_sorted]
            if any(c > 0 for c in counts):
                fig.add_trace(go.Bar(
                    name=cluster_names[cid],
                    x=decades_sorted, y=counts,
                    marker_color=_CLUSTER_COLORS[cid % len(_CLUSTER_COLORS)],
                ))
        fig.update_layout(
            title="キャリア開始年代別 クラスタ構成",
            barmode="stack", yaxis_title="人数", xaxis_title="キャリア開始年代",
        )

        # Also build decade × cluster avg IV heatmap
        comp_matrix = []
        for cid in range(n_clusters):
            row = []
            for dec in decades_sorted:
                mems_dc = [
                    p for p in persons
                    if p.get("staff_cluster") == cid
                    and (
                        f"{int(p['first_year']) // 10 * 10}年代"
                        if p.get("first_year") and 1960 <= p["first_year"] <= 2030
                        else "不明"
                    ) == dec
                ]
                avg_c = (sum(m["iv_score"] for m in mems_dc) / len(mems_dc)) if mems_dc else 0
                row.append(round(avg_c, 1))
            comp_matrix.append(row)

        fig_ht = go.Figure(go.Heatmap(
            z=comp_matrix,
            x=decades_sorted,
            y=[cluster_names[c] for c in range(n_clusters)],
            colorscale="Viridis",
            colorbar=dict(title="平均IV"),
            hovertemplate="クラスタ: %{y}<br>年代: %{x}<br>平均IV: %{z:.1f}<extra></extra>",
        ))
        fig_ht.update_layout(
            title="年代 × クラスタ 平均IV Score Heatmap",
            xaxis_title="キャリア開始年代", yaxis_title="クラスタ",
        )

        viz = (
            plotly_div_safe(fig, "chart_decade_cluster_bar", height=450) +
            plotly_div_safe(fig_ht, "chart_decade_cluster_heatmap", height=450)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="キャリア開始年代別クラスタ構成",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "キャリア開始年代はscores.jsonのfirst_year由来。"
                "積み上げ棒: 年代×クラスタごとの人数。"
                "Heatmap: 年代×クラスタセルごとの平均IVスコア。"
            ),
            section_id="decade_cluster",
        )

    # ── Section 8: Genre-era cluster ────────────────────────────

    def _build_genre_era_cluster_section(
        self, sb: SectionBuilder, persons: list[dict],
    ) -> ReportSection:
        TIER_ORDER = ["high_rated", "mid_rated", "low_rated"]
        tier_jp = {
            "high_rated": "高評価(8+)", "mid_rated": "中評価(6.5-8)",
            "low_rated": "低評価(<6.5)",
        }

        ge_data: dict[tuple, list[dict]] = {}
        for p in persons:
            key = (p["primary_tier"], p["primary_era"])
            ge_data.setdefault(key, []).append(p)

        ge_cells = []
        ge_cell_keys = []
        for t in TIER_ORDER:
            for e in _ERA_ORDER:
                mems = ge_data.get((t, e), [])
                if mems:
                    ge_cells.append([
                        np.mean([m["iv_score"] for m in mems]),
                        len(mems),
                        np.mean([m["birank"] for m in mems]),
                        np.mean([m["active_years"] for m in mems]),
                        np.mean([m["versatility"] for m in mems]),
                    ])
                    ge_cell_keys.append((t, e))

        if len(ge_cells) < 4:
            return ReportSection(
                title="ジャンル×年代 クラスタリング",
                findings_html="<p>クラスタリングに必要なジャンル×年代セルが不足しています。</p>",
                section_id="genre_era_cluster",
            )

        try:
            from sklearn.cluster import KMeans
            from sklearn.preprocessing import StandardScaler
        except ImportError:
            return ReportSection(
                title="ジャンル×年代 クラスタリング",
                findings_html="<p>sklearnが利用できません。</p>",
                section_id="genre_era_cluster",
            )

        Xge = np.array(ge_cells, dtype=float)
        scaler_ge = StandardScaler()
        Xge_s = scaler_ge.fit_transform(Xge)
        k_ge = min(4, len(ge_cells))
        km_ge = KMeans(n_clusters=k_ge, n_init=20, random_state=42)
        ge_labels = list(km_ge.fit_predict(Xge_s))
        GE_COLORS = ["#f093fb", "#06D6A0", "#fda085", "#a0d2db"]

        findings = (
            f"<p>ジャンル×年代クラスタリング: {len(ge_cell_keys)}個のtier × eraセルを"
            f"平均IV score、人数、平均BiRank、平均活動年数、平均多様性に基づき"
            f"{k_ge}グループに分類。</p>"
        )

        # Heatmap
        z_grid = [[None] * len(_ERA_ORDER) for _ in TIER_ORDER]
        z_labels = [[""] * len(_ERA_ORDER) for _ in TIER_ORDER]
        for idx, (t, e) in enumerate(ge_cell_keys):
            ti = TIER_ORDER.index(t) if t in TIER_ORDER else -1
            ei = _ERA_ORDER.index(e) if e in _ERA_ORDER else -1
            if ti >= 0 and ei >= 0:
                cl = ge_labels[idx]
                n_c = len(ge_data.get((t, e), []))
                avg_c = ge_cells[idx][0]
                z_grid[ti][ei] = cl
                z_labels[ti][ei] = f"C{cl+1}<br>n={n_c}<br>avg={avg_c:.1f}"

        fig_ht = go.Figure(go.Heatmap(
            z=z_grid,
            x=[_ERA_JP.get(e, e) for e in _ERA_ORDER],
            y=[tier_jp.get(t, t) for t in TIER_ORDER],
            colorscale=[[i / max(k_ge - 1, 1), c] for i, c in enumerate(GE_COLORS[:k_ge])],
            showscale=False,
            text=z_labels, texttemplate="%{text}",
            hovertemplate="ティア: %{y}<br>時代: %{x}<br>クラスタ: %{z}<extra></extra>",
        ))
        fig_ht.update_layout(
            title="ジャンル×時代 クラスタ分布 (Heatmap)",
            xaxis_title="時代", yaxis_title="スコアティア",
        )

        # Radar chart
        radar_feats = ["iv_score", "birank", "active_years", "versatility", "total_credits"]
        radar_jp = ["IV Score", "BiRank", "活動年数", "多様性", "総クレジット数"]
        feat_max = {}
        for f in radar_feats:
            vals_f = [p[f] for p in persons if p[f] > 0]
            feat_max[f] = max(vals_f) if vals_f else 1

        ge_cluster_cells: dict[int, list] = defaultdict(list)
        for idx, (t, e) in enumerate(ge_cell_keys):
            cl = ge_labels[idx]
            ge_cluster_cells[cl].extend(ge_data.get((t, e), []))

        fig_radar = go.Figure()
        for cl in range(k_ge):
            mems_cl = ge_cluster_cells[cl]
            if not mems_cl:
                continue
            r_vals = [np.mean([m[f] for m in mems_cl]) / feat_max[f] for f in radar_feats]
            r_vals.append(r_vals[0])  # close polygon
            fig_radar.add_trace(go.Scatterpolar(
                r=r_vals, theta=radar_jp + [radar_jp[0]],
                fill="toself", name=f"ジャンルC{cl+1}",
                line_color=GE_COLORS[cl % len(GE_COLORS)],
                opacity=0.6,
            ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            title="ジャンル×年代クラスタ プロファイル比較 (Radar)",
        )

        viz = (
            plotly_div_safe(fig_ht, "chart_genre_era_heatmap", height=400) +
            plotly_div_safe(fig_radar, "chart_genre_era_radar", height=500)
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="ジャンル×年代 クラスタリング",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "12セル（3スコアティア × 4時代）。各セルは平均IV、人数、平均BiRank、"
                "平均active_years、平均versatilityで特徴づけられる。"
                "StandardScalerで正規化したセル特徴量にK-Means（k=4）を適用。"
                "Radarチャート: 特徴量は全人物にわたる最大値で[0, 1]に正規化。"
            ),
            section_id="genre_era_cluster",
        )

    # ── Section 9: Era pie ──────────────────────────────────────

    def _build_era_pie_section(self, sb: SectionBuilder, persons: list[dict]) -> ReportSection:
        era_counts: Counter = Counter(p["primary_era"] for p in persons)

        findings = "<p>主要活動時代の分布:</p><ul>"
        for era, count in era_counts.most_common():
            findings += f"<li><strong>{_ERA_JP.get(era, era)}</strong>: {count:,}人（{count/len(persons)*100:.1f}%）</li>"
        findings += "</ul>"

        fig = go.Figure(go.Pie(
            labels=[_ERA_JP.get(e, e) for e in era_counts],
            values=list(era_counts.values()),
            marker_colors=[_ERA_COLORS.get(e, "#888") for e in era_counts],
            hole=0.4, textinfo="label+percent",
        ))
        fig.update_layout(title="主要活動時代分布")

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="活動時代分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_era_pie", height=420),
            method_note=(
                "primary_era = genre_affinity.jsonにおいて最もクレジット比率が高い時代。"
                "ドーナツチャートは4時代区分＋不明にわたる分布を示す。"
            ),
            section_id="era_pie",
        )
