"""ネットワーク進化レポート — ReportSpec 構築."""

from __future__ import annotations

from pathlib import Path

import numpy as np

from src.viz.chart_spec import (
    AxisSpec,
    BarSpec,
    ColorMapping,
    ExplanationMeta,
    LineSpec,
)
from src.viz.data_providers.network_evolution import (
    load_network_evolution_data,
)
from src.viz.helpers.statistics import fmt_num, moving_avg
from src.viz.report_spec import ReportSpec, SectionSpec, StatCardSpec


def build_network_evolution_report(json_dir: Path) -> ReportSpec | None:
    """network_evolution.json → ReportSpec."""
    data = load_network_evolution_data(json_dir)
    if data is None or not data.years:
        return None

    years = data.years
    year_span = f"{min(years)}-{max(years)}"

    sections: list[SectionSpec] = []

    # ── データ駆動 key_findings ──
    findings: list[str] = []
    final_persons = data.cumulative_persons[-1] if data.cumulative_persons else 0
    final_edges = data.cumulative_edges[-1] if data.cumulative_edges else 0

    if data.new_persons and len(data.new_persons) >= 2:
        peak_new_idx = int(np.argmax(data.new_persons))
        peak_new_year = years[peak_new_idx]
        peak_new_count = data.new_persons[peak_new_idx]
        findings.append(f"新規参入ピーク: {peak_new_year}年 ({fmt_num(peak_new_count)}人)")

    if data.density and len(data.density) >= 2:
        max_dens_idx = int(np.argmax(data.density))
        max_dens_year = years[max_dens_idx]
        findings.append(f"密度最大年: {max_dens_year}年 ({data.density[max_dens_idx]:.6f})")

    if data.node_growth_pct and len(data.node_growth_pct) >= 5:
        recent_5 = data.node_growth_pct[-5:]
        avg_recent = float(np.mean(recent_5))
        overall_avg = float(np.mean(data.node_growth_pct))
        if avg_recent < overall_avg * 0.7:
            findings.append("直近5年の成長率は全期間平均を30%以上下回る — 成長鈍化")
        elif avg_recent > overall_avg * 1.3:
            findings.append("直近5年の成長率は全期間平均を30%以上上回る — 成長加速")

    if data.avg_new_persons_per_year > 0:
        findings.append(f"年間平均新規参入: {data.avg_new_persons_per_year:.0f}人")

    # Edge-to-node ratio trend
    if final_persons > 0 and final_edges > 0:
        ratio = final_edges / final_persons
        findings.append(f"最終エッジ/ノード比: {ratio:.1f} (1人あたりの平均接続数)")

    # ── Section 1: サマリー ──
    stats = [
        StatCardSpec(label="追跡期間", value=f"{len(years)} 年"),
        StatCardSpec(label="累積人数", value=fmt_num(final_persons)),
        StatCardSpec(label="累積エッジ", value=fmt_num(final_edges)),
    ]
    if data.avg_new_persons_per_year > 0:
        stats.append(StatCardSpec(
            label="年間平均新規",
            value=f"{data.avg_new_persons_per_year:.0f}人",
        ))
    if data.avg_new_edges_per_year > 0:
        stats.append(StatCardSpec(
            label="年間平均新規エッジ",
            value=f"{data.avg_new_edges_per_year:.0f}",
        ))

    sections.append(SectionSpec(
        title="ネットワーク進化サマリー",
        description=(
            f"{len(years)} 年間（{year_span}）のネットワーク位相変化を追跡。"
            f"最終規模: {fmt_num(final_persons)}人, {fmt_num(final_edges)}エッジ"
        ),
        stats=tuple(stats),
    ))

    # ── Section 2: ネットワーク規模推移 ──
    scale_charts = []

    # Chart 1: 年代別アクティブ人数 (Stacked Area)
    scale_charts.append(LineSpec(
        chart_id="ne_active",
        title=f"年代別アクティブ人数推移 ({year_span})",
        stacked=True,
        x=years,
        stacked_series={
            "アクティブ人数": data.active_persons,
            "新規参入人数": data.new_persons,
        },
        colors=ColorMapping(palette=("#f093fb", "#a0d2db")),
        x_axis=AxisSpec(label="年"),
        y_axis=AxisSpec(label="人数"),
        height=450,
        explanation=ExplanationMeta(
            question="各年に何人が活動し、何人が新たに参入したか？",
            reading_guide=(
                "紫=その年にクレジットが1件以上ある人数。"
                "水色=その年に初登場した新規人数。"
            ),
            key_findings=tuple(findings[:2]),
        ),
    ))

    # Chart 2: ネットワーク密度時系列
    scale_charts.append(LineSpec(
        chart_id="ne_density",
        title="ネットワーク密度推移",
        series={"密度": tuple(zip(years, data.density))},
        colors=ColorMapping(palette=("#06D6A0",)),
        x_axis=AxisSpec(label="年"),
        y_axis=AxisSpec(label="密度"),
        height=400,
        explanation=ExplanationMeta(
            question="ネットワーク密度は時間とともにどう変化したか？",
            reading_guide=(
                "密度=実エッジ数/可能エッジ数。ネットワーク規模が拡大すると密度は"
                "自然に低下する傾向があるため、密度の上昇は協業の活性化を意味する。"
            ),
        ),
    ))

    # Chart 3: 累積人数・エッジ (dual Line)
    scale_charts.append(LineSpec(
        chart_id="ne_cumul",
        title="累積人数・エッジ数推移",
        series={
            "累積人数": tuple(zip(years, data.cumulative_persons)),
            "累積エッジ": tuple(zip(years, data.cumulative_edges)),
        },
        colors=ColorMapping(palette=("#667eea", "#fda085")),
        x_axis=AxisSpec(label="年"),
        y_axis=AxisSpec(label="累積人数", log_scale=True),
        height=450,
        explanation=ExplanationMeta(
            question="ネットワーク規模はどのペースで拡大してきたか？",
            reading_guide=(
                "青=累積参加人数。橙=累積エッジ数。"
                "対数スケールで規模拡大のペースを比較。"
            ),
        ),
    ))

    sections.append(SectionSpec(
        title="ネットワーク規模推移",
        charts=tuple(scale_charts),
    ))

    # ── Section 3: 年別エッジ分析 ──
    if data.year_edges and any(e > 0 for e in data.year_edges):
        edge_charts = []

        edge_charts.append(BarSpec(
            chart_id="ne_year_edges",
            title="年別エッジ数（当年のみ）",
            categories=tuple(str(y) for y in years),
            values=tuple(float(e) for e in data.year_edges),
            colors=ColorMapping(palette=("#fda085",)),
            x_axis=AxisSpec(label="年"),
            y_axis=AxisSpec(label="エッジ数"),
            height=400,
            explanation=ExplanationMeta(
                question="各年にどれだけの協業エッジが発生したか？",
                reading_guide=(
                    "累積ではなく当年のみのエッジ数。ピークは業界が最も活発だった年。"
                    "急減は不況や構造転換を示唆。"
                ),
            ),
        ))

        if data.new_edges and any(e > 0 for e in data.new_edges):
            edge_charts.append(LineSpec(
                chart_id="ne_new_vs_total_edges",
                title="新規エッジ vs 全エッジ推移",
                series={
                    "全エッジ(当年)": tuple(
                        (float(y), float(e)) for y, e in zip(years, data.year_edges)
                    ),
                    "新規エッジ": tuple(
                        (float(y), float(e)) for y, e in zip(years, data.new_edges)
                    ),
                },
                colors=ColorMapping(palette=("#fda085", "#06D6A0")),
                x_axis=AxisSpec(label="年"),
                y_axis=AxisSpec(label="エッジ数"),
                height=400,
                explanation=ExplanationMeta(
                    question="新規の協業エッジと既存ペアの再協業の比率はどう推移したか？",
                    reading_guide=(
                        "橙=当年の全エッジ、緑=初めて協業したペアのエッジ。"
                        "差分が大きいほど「リピート協業」が多い成熟したネットワーク。"
                    ),
                ),
            ))

        sections.append(SectionSpec(
            title="年別エッジ分析",
            description="累積ではなく各年の活動レベルで協業パターンを分析。",
            charts=tuple(edge_charts),
        ))

    # ── Section 4: 構造変化イベント ──
    if len(data.density_changes) >= 2:
        change_years = years[1:]
        max_change = max(data.density_changes)
        max_idx = data.density_changes.index(max_change)
        max_year = change_years[max_idx]

        sections.append(SectionSpec(
            title="構造変化イベント検出",
            charts=(BarSpec(
                chart_id="ne_change",
                title="年間密度変化量",
                categories=tuple(str(y) for y in change_years),
                values=data.density_changes,
                colors=ColorMapping(palette=("#f5576c",)),
                x_axis=AxisSpec(label="年"),
                y_axis=AxisSpec(label="|Δ密度|"),
                height=400,
                explanation=ExplanationMeta(
                    question="いつ業界構造に急激な変化が起きたか？",
                    reading_guide=(
                        "密度の年間変化量。急激な変化は業界構造の転換点を示唆。"
                        f"最大変化年: {max_year}。"
                    ),
                ),
            ),),
        ))

    # ── Section 5: 年間成長率 + 移動平均 ──
    if len(data.node_growth_pct) >= 2:
        growth_years = years[1:]
        node_ma = moving_avg(data.node_growth_pct)
        edge_ma = moving_avg(data.edge_growth_pct)

        growth_charts = []

        growth_charts.append(LineSpec(
            chart_id="ne_growth",
            title="年間成長率（前年比%）+ 5年移動平均",
            series={
                "ノード成長率(5年MA)": tuple(zip(growth_years, node_ma)),
                "エッジ成長率(5年MA)": tuple(zip(growth_years, edge_ma)),
            },
            colors=ColorMapping(palette=("#a0d2db", "#fda085")),
            x_axis=AxisSpec(label="年"),
            y_axis=AxisSpec(label="成長率(%)"),
            height=450,
            explanation=ExplanationMeta(
                question="ネットワーク成長は加速しているか減速しているか？",
                reading_guide=(
                    "5年移動平均の成長率。エッジ成長率>ノード成長率なら"
                    "既存人材間の協業が増加。"
                ),
                key_findings=tuple(findings[2:4]),
            ),
        ))

        # エッジ/ノード比の推移
        if data.cumulative_persons and data.cumulative_edges:
            en_ratio = []
            for p, e in zip(data.cumulative_persons, data.cumulative_edges):
                en_ratio.append(e / max(p, 1))
            en_ratio_ma = moving_avg(tuple(en_ratio))

            growth_charts.append(LineSpec(
                chart_id="ne_edge_node_ratio",
                title="エッジ/ノード比の推移（5年移動平均）",
                series={
                    "エッジ/ノード比": tuple(zip(years, en_ratio_ma)),
                },
                colors=ColorMapping(palette=("#FFD166",)),
                x_axis=AxisSpec(label="年"),
                y_axis=AxisSpec(label="エッジ/ノード比"),
                height=400,
                explanation=ExplanationMeta(
                    question="1人あたりの平均接続数はどう変化してきたか？",
                    reading_guide=(
                        "エッジ/ノード比=累積エッジ数/累積人数。"
                        "上昇傾向なら人材あたりの協業密度が増加中。"
                        "下降ならネットワークが希薄化（新規参入>新規接続）。"
                    ),
                    key_findings=tuple(findings[4:5]),
                ),
            ))

        sections.append(SectionSpec(
            title="年間成長率推移",
            charts=tuple(growth_charts),
        ))

    # ── Section 6: 四半期分解 ──
    if data.quarterly_labels and len(data.quarterly_labels) >= 4:
        quarterly_charts = []

        quarterly_charts.append(BarSpec(
            chart_id="ne_quarterly_active",
            title="四半期別アクティブ人数",
            categories=data.quarterly_labels,
            values=tuple(float(a) for a in data.quarterly_active),
            colors=ColorMapping(palette=("#f093fb",)),
            x_axis=AxisSpec(label="四半期"),
            y_axis=AxisSpec(label="人数"),
            height=400,
            explanation=ExplanationMeta(
                question="四半期ごとの活動量にはどの程度の季節性があるか？",
                reading_guide="各四半期にクレジットが1件以上ある人物の数。",
            ),
        ))

        if data.quarterly_credits and any(c > 0 for c in data.quarterly_credits):
            quarterly_charts.append(LineSpec(
                chart_id="ne_quarterly_credits",
                title="四半期別クレジット数推移",
                series={
                    "クレジット数": tuple(
                        (float(i), float(c))
                        for i, c in enumerate(data.quarterly_credits)
                    ),
                },
                colors=ColorMapping(palette=("#667eea",)),
                x_axis=AxisSpec(label="四半期インデックス"),
                y_axis=AxisSpec(label="クレジット数"),
                height=400,
                explanation=ExplanationMeta(
                    question="クレジット発生量の四半期推移はどのようなパターンか？",
                    reading_guide=(
                        "テレビアニメの放送クールに対応した季節パターンが見られる場合、"
                        "Q1(冬)/Q2(春)/Q3(夏)/Q4(秋)クールの制作スケジュールが反映。"
                    ),
                ),
            ))

        sections.append(SectionSpec(
            title="四半期分解",
            description="年間集計では見えない季節パターンを四半期粒度で分析。",
            charts=tuple(quarterly_charts),
        ))

    return ReportSpec(
        title="ネットワーク構造変化",
        subtitle=f"協業ネットワーク位相の時系列変化 ({year_span})",
        audience="ネットワーク研究者、業界アナリスト",
        description=(
            "アニメ協業ネットワークは構造的にどう変化してきたのか？ 本レポートでは"
            "主要なグラフ指標 — ノード数・エッジ数・密度 — "
            "を数十年の制作期間にわたって追跡します。"
            "年別エッジ分析と四半期分解で詳細な活動パターンを明らかにします。"
        ),
        sections=tuple(sections),
        glossary={
            "密度 (Density)": (
                "グラフ上の実際のエッジ数と可能なエッジ数の比率。"
                "密度が高いほどネットワーク規模に対して協業接続が多い。"
            ),
            "エッジ/ノード比": (
                "累積エッジ数/累積人数。1人あたりの平均接続強度の指標。"
                "ネットワーク密度とは異なり、規模増大の自然な希薄化に影響されにくい。"
            ),
            "新規エッジ": (
                "その年に初めて協業したペアの接続。全エッジ−新規エッジ＝リピート協業。"
            ),
            "連結成分 (Connected Component)": (
                "すべてのノードが何らかのパスで相互到達可能なグループ。"
                "複数の成分は孤立したサブネットワークの存在を意味する。"
            ),
        },
    )
