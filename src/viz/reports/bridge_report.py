"""ブリッジ分析レポート — ReportSpec 構築."""

from __future__ import annotations

from pathlib import Path

import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from src.viz.chart_spec import (
    AxisSpec,
    BarSpec,
    ChartSpec,
    ColorMapping,
    ExplanationMeta,
    HeatmapSpec,
    ScatterSpec,
    ViolinSpec,
)
from src.viz.data_providers.bridge import load_bridge_data
from src.viz.helpers.naming import name_clusters_by_rank
from src.viz.helpers.statistics import fmt_num
from src.viz.report_spec import ReportSpec, SectionSpec, StatCardSpec, TableSpec

_BR_COLORS = ("#f093fb", "#06D6A0", "#fda085", "#a0d2db", "#FFD166")


def build_bridge_report(json_dir: Path) -> ReportSpec | None:
    """bridges.json + scores.json → ReportSpec."""
    data = load_bridge_data(json_dir)
    if data is None:
        return None

    sections: list[SectionSpec] = []

    # ── Section 1: サマリー ──
    sections.append(SectionSpec(
        title="Bridge Detection Summary",
        description=(
            f"Of {fmt_num(data.total_persons)} persons across "
            f"{fmt_num(data.total_communities)} communities, "
            f"{fmt_num(data.bridge_person_count)} serve as bridges "
            "connecting different communities."
        ),
        stats=(
            StatCardSpec(label="Total Communities", value=fmt_num(data.total_communities)),
            StatCardSpec(label="Bridge Persons", value=fmt_num(data.bridge_person_count)),
            StatCardSpec(label="Cross-Community Edges", value=fmt_num(data.total_cross_edges)),
            StatCardSpec(label="Bridge Ratio", value=f"{data.bridge_ratio_pct:.1f}%"),
            StatCardSpec(label="Total Network Persons", value=fmt_num(data.total_persons)),
        ),
    ))

    # ── データ駆動 key_findings ──
    findings: list[str] = []
    if data.bridge_ivs and data.nonbridge_ivs:
        br_med = float(np.median(data.bridge_ivs))
        nb_med = float(np.median(data.nonbridge_ivs))
        diff_pct = (br_med - nb_med) / max(abs(nb_med), 0.01) * 100
        if diff_pct > 10:
            findings.append(
                f"ブリッジ人材のIV中央値 ({br_med:.1f}) は非ブリッジ ({nb_med:.1f}) より"
                f" {diff_pct:.0f}% 高い — コミュニティ間接続は高評価と関連"
            )
    if data.bridge_persons:
        top5_avg_conn = np.mean([bp.communities_connected for bp in data.bridge_persons[:5]])
        findings.append(
            f"上位5人のブリッジは平均 {top5_avg_conn:.1f} コミュニティを接続"
        )
    if data.bridge_role_counts:
        top_role = max(data.bridge_role_counts, key=data.bridge_role_counts.get)
        top_role_pct = data.bridge_role_counts[top_role] / max(sum(data.bridge_role_counts.values()), 1) * 100
        findings.append(f"ブリッジ人材の最多役職: {top_role} ({top_role_pct:.0f}%)")

    charts_dist: list[ChartSpec] = []

    # ── Chart 1: Bridge Score Violin ──
    if data.bridge_scores:
        charts_dist.append(ViolinSpec(
            chart_id="bridge_scores_violin",
            title=f"Bridge Score分布 — {fmt_num(len(data.bridge_scores))}人",
            groups={"Bridge Score": tuple(float(s) for s in data.bridge_scores)},
            raincloud=False,
            show_box=True,
            show_points="outliers",
            colors=ColorMapping(palette=("#f093fb",)),
            y_axis=AxisSpec(label="Bridge Score"),
            height=400,
            explanation=ExplanationMeta(
                question="ブリッジスコアの分布はどのような形状か？",
                reading_guide=(
                    "幅が広い帯域ほど該当者が多い。白線=中央値、箱=四分位範囲。"
                    "高スコアの外れ値が「超重要ブリッジ」。"
                ),
                key_findings=tuple(findings[:2]),
            ),
        ))

    # ── Chart 2: Bridge vs Non-Bridge IV ──
    if data.bridge_ivs and data.nonbridge_ivs:
        charts_dist.append(ViolinSpec(
            chart_id="bridge_nonbridge_iv",
            title="Bridge vs Non-Bridge — IV Score分布比較",
            groups={
                f"Bridge (n={len(data.bridge_ivs)})": data.bridge_ivs,
                f"Non-Bridge (n={len(data.nonbridge_ivs)})": data.nonbridge_ivs,
            },
            raincloud=False,
            show_box=True,
            colors=ColorMapping(palette=("#f093fb", "#667eea")),
            y_axis=AxisSpec(label="IV Score"),
            height=450,
            explanation=ExplanationMeta(
                question="ブリッジ人材は非ブリッジ人材より高いIV Scoreを持つか？",
                reading_guide=(
                    "ピンク=ブリッジ人材、青=非ブリッジ人材のIV Scoreを比較。"
                    "ブリッジ人材のIVが有意に高い場合、コミュニティ間の橋渡しは"
                    "高い業界評価と関連していることを示す。"
                ),
            ),
        ))

    # ── Chart 3: コミュニティ接続数別スコア (Raincloud) ──
    raincloud_groups: dict[str, tuple[float, ...]] = {}
    raincloud_colors: list[str] = []
    palette = ("#f093fb", "#a0d2db", "#f5576c", "#fda085", "#667eea", "#06D6A0", "#FFD166")
    for idx, cc in enumerate(sorted(data.scores_by_communities.keys())):
        scores = data.scores_by_communities[cc]
        if len(scores) >= 3:
            raincloud_groups[f"{cc}コミュニティ"] = scores
            raincloud_colors.append(palette[idx % len(palette)])

    if raincloud_groups:
        charts_dist.append(ViolinSpec(
            chart_id="bridge_violin",
            title="コミュニティ接続数別 ブリッジスコア分布 (Raincloud)",
            groups=raincloud_groups,
            raincloud=True,
            colors=ColorMapping(palette=tuple(raincloud_colors)),
            y_axis=AxisSpec(label="Bridge Score"),
            x_axis=AxisSpec(label="接続コミュニティ数"),
            height=500,
            explanation=ExplanationMeta(
                question="接続コミュニティ数が多いほどブリッジスコアは高いか？",
                reading_guide=(
                    "Violin plotで接続コミュニティ数ごとのブリッジスコアの分布形状を比較。"
                    "多くのコミュニティを接続する人物ほどスコアが高い傾向があるか確認。"
                ),
            ),
        ))

    if charts_dist:
        sections.append(SectionSpec(
            title="Bridge Score Distribution",
            charts=tuple(charts_dist),
        ))

    # ── Section: K-Means クラスタ分析 ──
    if len(data.bridge_features) >= 8:
        sections.append(_build_kmeans_section(data))

    # ── Chart: ブリッジ人材の役職分布 (Bar) ──
    if data.bridge_role_counts:
        sorted_roles = sorted(data.bridge_role_counts.items(), key=lambda x: x[1], reverse=True)
        sections.append(SectionSpec(
            title="ブリッジ人材の役職分布",
            charts=(BarSpec(
                chart_id="bridge_roles",
                title="ブリッジ人材の Primary Role 分布",
                categories=tuple(r for r, _ in sorted_roles),
                values=tuple(float(c) for _, c in sorted_roles),
                colors=ColorMapping(palette=("#f093fb",)),
                x_axis=AxisSpec(label="Role"),
                y_axis=AxisSpec(label="人数"),
                height=400,
                explanation=ExplanationMeta(
                    question="どの役職の人材がブリッジ人材として多いか？",
                    reading_guide="横軸=primary role、縦軸=人数。",
                    key_findings=tuple(findings[2:3]),
                ),
            ),),
        ))

    # ── Chart: Cross-Community Connectivity (Bar) ──
    if data.top_community_pairs:
        sections.append(SectionSpec(
            title="Cross-Community Connectivity",
            charts=(BarSpec(
                chart_id="cross_edges",
                title="Top 30 Community Pairs by Cross-Edges",
                categories=tuple(cp.label for cp in data.top_community_pairs),
                values=tuple(float(cp.edge_count) for cp in data.top_community_pairs),
                colors=ColorMapping(palette=("#f5576c",)),
                x_axis=AxisSpec(label="Community Pair"),
                y_axis=AxisSpec(label="Edge Count"),
                height=400,
                explanation=ExplanationMeta(
                    question="どのコミュニティペア間の接続が最も強いか？",
                    reading_guide="棒の高さ=クロスコミュニティエッジ数。最も交流が活発なペアがトップに。",
                ),
            ),),
        ))

    # ── Chart: コミュニティ間接続 Heatmap ──
    if data.community_matrix and len(data.community_matrix_labels) >= 3:
        sections.append(SectionSpec(
            title="コミュニティ間接続強度マトリクス",
            charts=(HeatmapSpec(
                chart_id="community_heatmap",
                title="Top 10 コミュニティ間エッジ数 Heatmap",
                z=data.community_matrix,
                x_labels=data.community_matrix_labels,
                y_labels=data.community_matrix_labels,
                show_text=len(data.community_matrix_labels) <= 10,
                colors=ColorMapping(colorscale="YlOrRd"),
                height=500,
                explanation=ExplanationMeta(
                    question="どのコミュニティ間の接続が特に密接か？",
                    reading_guide=(
                        "セルの色が濃いほどそのコミュニティペア間のクロスエッジ数が多い。"
                        "対角線はコミュニティ内エッジ（同一コミュニティのペア）。"
                    ),
                ),
            ),),
        ))

    # ── Section: Top 50 Bridge Persons Table ──
    top50 = data.bridge_persons[:50]
    if top50:
        rows = tuple(
            (
                str(i),
                bp.name,
                str(bp.bridge_score),
                str(bp.communities_connected),
                str(bp.cross_community_edges),
            )
            for i, bp in enumerate(top50, 1)
        )
        sections.append(SectionSpec(
            title="Top 50 Bridge Persons",
            description=(
                "ブリッジスコア順にランキング。より多くのコミュニティを"
                "より多くのクロスコミュニティエッジで接続する人物が高スコアを獲得。"
            ),
            tables=(TableSpec(
                headers=("#", "Person", "Bridge Score", "Communities", "Cross Edges"),
                rows=rows,
                sortable=True,
            ),),
        ))

    return ReportSpec(
        title="ネットワークブリッジ分析",
        subtitle=(
            f"コミュニティ間ブリッジ分析 — "
            f"{fmt_num(data.bridge_person_count)}人のブリッジ / "
            f"{fmt_num(data.total_communities)}コミュニティ"
        ),
        audience="スタジオ人事、タレントスカウト、ネットワーク研究者",
        description=(
            "ブリッジ人材とは、協業ネットワーク上で本来分離しているコミュニティ同士を"
            "接続する人物です。スタジオやジャンルの境界を越えた知識移転・スタイル伝播・"
            "人材発掘を促進します。"
        ),
        sections=tuple(sections),
        glossary={
            "ブリッジ人材 (Bridge Person)": (
                "協業グラフ上で2つ以上の異なるコミュニティに所属し、"
                "それらを接続する人物。"
            ),
            "ブリッジスコア (Bridge Score)": (
                "接続するコミュニティ数とクロスコミュニティ結合の強さを"
                "統合した0-99のスコア。"
            ),
            "K-Meansクラスタリング": (
                "ブリッジスコア・接続コミュニティ数・クロスエッジ数の3特徴量で"
                "ブリッジ人材をグループ分け。StandardScalerで標準化後に実行。"
            ),
        },
    )


def _build_kmeans_section(data) -> SectionSpec:
    """K-Means 4クラスタ散布図 + 要約テーブル."""
    feats = np.array(data.bridge_features, dtype=float)
    k = min(4, len(feats))
    scaler = StandardScaler()
    feats_s = scaler.fit_transform(feats)
    km = KMeans(n_clusters=k, n_init=20, random_state=42)
    labels = km.fit_predict(feats_s)
    centers = scaler.inverse_transform(km.cluster_centers_)

    cluster_names = name_clusters_by_rank(
        centers,
        [
            (0, ["超ブリッジ", "中堅ブリッジ", "周辺ブリッジ"]),
            (1, ["広域接続", "中域接続", "局所接続"]),
            (2, ["多クロスエッジ", "中クロスエッジ", "少クロスエッジ"]),
        ],
    )

    # Scatter: cross_edges (x, log) vs bridge_score (y), size=communities
    persons = data.bridge_persons
    x_vals = tuple(float(persons[i].cross_community_edges) for i in range(len(labels)))
    y_vals = tuple(float(persons[i].bridge_score) for i in range(len(labels)))
    cats = tuple(cluster_names[labels[i]] for i in range(len(labels)))
    names = tuple(persons[i].name for i in range(len(labels)))

    scatter = ScatterSpec(
        chart_id="bridge_cluster_scatter",
        title="ブリッジクラスタ散布図（クロスエッジ × スコア）",
        x=x_vals,
        y=y_vals,
        categories=cats,
        labels=names,
        max_points=5000,
        colors=ColorMapping(palette=_BR_COLORS[:k]),
        x_axis=AxisSpec(label="クロスコミュニティエッジ数", log_scale=True),
        y_axis=AxisSpec(label="Bridge Score"),
        height=500,
        explanation=ExplanationMeta(
            question="ブリッジ人材はどのようなクラスタに分かれるか？",
            reading_guide=(
                "色=K-Meansクラスタ。X=クロスエッジ数(対数)、Y=ブリッジスコア。"
                "右上ほど強力なブリッジ。クラスタ名は各特徴量のランクから自動命名。"
            ),
        ),
    )

    # Summary table
    rows: list[tuple[str, ...]] = []
    for cid in range(k):
        mask = labels == cid
        n = int(mask.sum())
        avg_score = float(feats[mask, 0].mean())
        avg_conn = float(feats[mask, 1].mean())
        avg_cross = float(feats[mask, 2].mean())
        rows.append((
            cluster_names[cid],
            fmt_num(n),
            f"{avg_score:.1f}",
            f"{avg_conn:.1f}",
            f"{avg_cross:.0f}",
        ))

    table = TableSpec(
        headers=("クラスタ", "人数", "平均スコア", "平均接続数", "平均クロスエッジ"),
        rows=tuple(rows),
        sortable=True,
    )

    return SectionSpec(
        title="ブリッジクラスタ分析 (K-Means)",
        description=(
            f"ブリッジスコア・接続コミュニティ数・クロスエッジ数の3特徴量で"
            f" K={k} クラスタに分類。"
        ),
        charts=(scatter,),
        tables=(table,),
    )
