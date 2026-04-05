"""成長・スコア分析レポート — ReportSpec 構築."""

from __future__ import annotations

from pathlib import Path

import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from src.viz.chart_spec import (
    AxisSpec,
    BarSpec,
    ColorMapping,
    ExplanationMeta,
    HeatmapSpec,
    LineSpec,
    ScatterSpec,
    ViolinSpec,
)
from src.viz.data_providers.growth import load_growth_data
from src.viz.helpers.naming import name_clusters_by_rank
from src.viz.helpers.statistics import fmt_num
from src.viz.report_spec import ReportSpec, SectionSpec, StatCardSpec, TableSpec

TREND_COLORS = {
    "rising": "#06D6A0",
    "stable": "#FFD166",
    "declining": "#EF476F",
    "new": "#a0d2db",
    "inactive": "#666666",
}
TREND_ORDER = ("rising", "stable", "declining", "new", "inactive")

_GR_COLORS = ("#f093fb", "#667eea", "#06D6A0", "#EF476F", "#FFD166")


def build_growth_score_report(json_dir: Path) -> ReportSpec | None:
    """growth.json → ReportSpec."""
    data = load_growth_data(json_dir)
    if data is None:
        return None

    sections: list[SectionSpec] = []

    # ── データ駆動 key_findings ──
    findings: list[str] = []
    if data.trend_summary:
        top_trend = max(data.trend_summary, key=data.trend_summary.get)
        top_pct = data.trend_summary[top_trend] / max(data.total_persons, 1) * 100
        findings.append(f"最多トレンド: {top_trend} ({top_pct:.0f}%)")
    if data.trend_credits.get("rising") and data.trend_credits.get("stable"):
        rising_med = float(np.median(data.trend_credits["rising"]))
        stable_med = float(np.median(data.trend_credits["stable"]))
        findings.append(
            f"Rising人材のクレジット中央値 ({rising_med:.0f}) vs "
            f"Stable ({stable_med:.0f})"
        )
    if data.persons:
        top5 = sorted(data.persons, key=lambda p: p.total_credits, reverse=True)[:5]
        avg_span = np.mean([p.career_span for p in top5])
        findings.append(f"上位5人の平均キャリア期間: {avg_span:.1f}年")

    # ── Section 1: Overview Stats ──
    stat_cards = []
    for trend in sorted(data.trend_summary, key=lambda t: data.trend_summary[t], reverse=True):
        count = data.trend_summary[trend]
        pct = count / max(data.total_persons, 1) * 100
        stat_cards.append(StatCardSpec(
            label=f"{trend.title()} ({pct:.1f}%)",
            value=fmt_num(count),
        ))

    sections.append(SectionSpec(
        title="Growth Trends Overview",
        stats=tuple(stat_cards),
    ))

    charts_dist: list = []

    # ── Chart 1: Trend Stacked Bar ──
    trend_series: dict[str, tuple[float, ...]] = {}
    for t in TREND_ORDER:
        cnt = data.trend_summary.get(t, 0)
        if cnt > 0:
            trend_series[t.title()] = (float(cnt),)

    if trend_series:
        charts_dist.append(BarSpec(
            chart_id="trend_bar",
            title=f"成長トレンド構成比 — {fmt_num(data.total_persons)}人",
            categories=("全人物",),
            stacked_series=trend_series,
            bar_mode="stack",
            orientation="h",
            colors=ColorMapping(palette=tuple(
                TREND_COLORS.get(t, "#888") for t in TREND_ORDER if data.trend_summary.get(t, 0) > 0
            )),
            x_axis=AxisSpec(label="人数"),
            height=250,
            explanation=ExplanationMeta(
                question="人物の成長トレンドはどのように分布しているか？",
                reading_guide=(
                    "全人物の成長カテゴリ構成比を横積み上げ棒グラフで表示。"
                    "Rising=活動増加中、Stable=安定、Declining=活動減少中、"
                    "New=最近参入、Inactive=最近のクレジットなし。"
                ),
                key_findings=tuple(findings[:1]),
            ),
        ))

    # ── Chart 2: Trend Category vs Total Credits (Raincloud Violin) ──
    violin_groups: dict[str, tuple[float, ...]] = {}
    violin_colors: list[str] = []
    for t in TREND_ORDER:
        credits = data.trend_credits.get(t)
        if credits and len(credits) >= 3:
            violin_groups[t.title()] = tuple(float(c) for c in credits)
            violin_colors.append(TREND_COLORS.get(t, "#888"))

    if violin_groups:
        charts_dist.append(ViolinSpec(
            chart_id="trend_violin",
            title="トレンドカテゴリ別 総クレジット数分布 (Raincloud)",
            groups=violin_groups,
            raincloud=True,
            colors=ColorMapping(palette=tuple(violin_colors)),
            x_axis=AxisSpec(label="トレンドカテゴリ"),
            y_axis=AxisSpec(label="総クレジット数", log_scale=True),
            height=500,
            explanation=ExplanationMeta(
                question="各トレンドカテゴリの人物はどれだけのクレジットを持つか？",
                reading_guide=(
                    "Violin plotで各トレンドカテゴリの総クレジット数分布を比較。"
                    "Risingなのにクレジット数が少ない＝急成長新人、Stableで多い＝安定ベテラン。"
                ),
                key_findings=tuple(findings[1:2]),
            ),
        ))

    if charts_dist:
        sections.append(SectionSpec(
            title="Trend Distribution",
            charts=tuple(charts_dist),
        ))

    # ── Chart 3: 年別ローリングトレンド (Stacked Area via LineSpec) ──
    yearly = data.yearly_rolling_trends
    if yearly:
        sorted_yrs = tuple(yr for yr in sorted(yearly) if yr >= 1980)
        if sorted_yrs:
            area_series: dict[str, tuple[float, ...]] = {}
            area_colors: list[str] = []
            for t in ("rising", "stable", "declining", "new"):
                vals = tuple(
                    float(yearly.get(yr, {}).get(t, 0)) for yr in sorted_yrs
                )
                area_series[t.title()] = vals
                area_colors.append(TREND_COLORS.get(t, "#888"))

            sections.append(SectionSpec(
                title="年別ローリングトレンド推移（サンプル）",
                charts=(LineSpec(
                    chart_id="trend_yearly",
                    title="年別ローリングトレンド推移（上位200人サンプル・3年窓）",
                    stacked=True,
                    x=sorted_yrs,
                    stacked_series=area_series,
                    colors=ColorMapping(palette=tuple(area_colors)),
                    x_axis=AxisSpec(label="年"),
                    y_axis=AxisSpec(label="人数"),
                    height=450,
                    explanation=ExplanationMeta(
                        question="トレンド分布は年代によってどう変化してきたか？",
                        reading_guide=(
                            "上位200人について、各年ごとに3年窓のローリングトレンドを算出し積み上げ表示。"
                            "Rising=直近2年のクレジットが前2年より30%以上増加、"
                            "Declining=50%以上減少、Stable=その中間、New=デビュー3年以内。"
                        ),
                        caveats=(
                            "growth.jsonは上位200人のみの詳細データを含みます。この時系列は業界全体ではなく"
                            "サンプルの推移です。各年のトレンドは3年ローリング窓で分類しており、"
                            "最終トレンドの遡及適用ではありません。",
                        ),
                    ),
                ),),
            ))

    # ── Section: K-Means キャリアクラスタ分析 ──
    if len(data.career_features) >= 10:
        sections.append(_build_career_cluster_section(data))

    # ── Section: キャリア生存・定着曲線 ──
    if data.career_durations and len(data.career_durations) >= 10:
        sections.append(_build_survival_section(data))

    # ── Section: Rising Stars Table ──
    rising = sorted(
        [p for p in data.persons if p.trend == "rising"],
        key=lambda p: p.total_credits,
        reverse=True,
    )
    if rising:
        rows = tuple(
            (
                str(i),
                p.name,
                str(p.total_credits),
                f"{p.career_span} yrs",
                f"{p.activity_ratio:.2f}",
            )
            for i, p in enumerate(rising[:30], 1)
        )
        sections.append(SectionSpec(
            title=f"Rising Stars ({len(rising)} persons)",
            description=(
                "「上昇中」に分類されたプロフェッショナル — クレジット数と活動が増加中。"
                "総クレジット数順にソートし、最も活発な人材を強調。"
            ),
            tables=(TableSpec(
                headers=("#", "Person", "Total Credits", "Career Span", "Activity Ratio"),
                rows=rows,
                sortable=True,
            ),),
        ))

    return ReportSpec(
        title="成長・スコア分析",
        subtitle=f"Growth & Score Analysis — {fmt_num(data.total_persons)}人",
        audience="キャリア分析担当者、スタジオ人事",
        description=(
            "業界人材の成長トレンドを分析し、Rising Stars（急成長人材）、"
            "安定したベテラン、活動が減少している人材を特定します。"
            "K-Meansクラスタリングでキャリアパターンを類型化し、"
            "生存曲線で業界定着率を可視化します。"
        ),
        sections=tuple(sections),
        glossary={
            "成長トレンド (Growth Trend)": (
                "3年ローリング窓での活動量変化。Rising=30%以上増加、"
                "Declining=50%以上減少、Stable=その中間。"
            ),
            "活動率 (Activity Ratio)": (
                "実活動年数 / キャリア期間。1.0=途切れなく活動、"
                "0.5=キャリア期間の半分だけ活動。"
            ),
            "キャリア生存率": (
                "業界参入からN年後にまだ1件以上のクレジットがある人物の割合。"
                "全人物のactive_years分布から算出。"
            ),
        },
    )


def _build_career_cluster_section(data) -> SectionSpec:
    """K-Means 5クラスタ散布図 + z-scoreヒートマップ + クラスタ×トレンド棒."""
    feats = np.array(data.career_features, dtype=float)
    k = min(5, len(feats))
    scaler = StandardScaler()
    feats_s = scaler.fit_transform(feats)
    km = KMeans(n_clusters=k, n_init=20, random_state=42)
    labels = km.fit_predict(feats_s)
    centers = scaler.inverse_transform(km.cluster_centers_)

    cluster_names = name_clusters_by_rank(
        centers,
        [
            (0, ["多作型", "中堅型", "寡作型"]),
            (2, ["長期キャリア", "中期キャリア", "短期キャリア"]),
            (3, ["高活動率", "中活動率", "低活動率"]),
        ],
    )

    # ── 1. 散布図: credits_per_year (x) vs activity_ratio (y) ──
    # feats: (total_credits, credits_per_year, career_span, activity_ratio, debut_year)
    persons_with_features = [p for p in data.persons if p.debut_year > 0]
    x_vals = tuple(float(feats[i, 1]) for i in range(len(labels)))  # credits_per_year
    y_vals = tuple(float(feats[i, 3]) for i in range(len(labels)))  # activity_ratio
    cats = tuple(cluster_names[labels[i]] for i in range(len(labels)))
    names = tuple(
        persons_with_features[i].name if i < len(persons_with_features) else ""
        for i in range(len(labels))
    )

    scatter = ScatterSpec(
        chart_id="career_cluster_scatter",
        title="キャリアクラスタ散布図（年間クレジット × 活動率）",
        x=x_vals,
        y=y_vals,
        categories=cats,
        labels=names,
        max_points=5000,
        colors=ColorMapping(palette=_GR_COLORS[:k]),
        x_axis=AxisSpec(label="年間平均クレジット数", log_scale=True),
        y_axis=AxisSpec(label="活動率"),
        height=500,
        explanation=ExplanationMeta(
            question="キャリアパターンはどのようなクラスタに分かれるか？",
            reading_guide=(
                "色=K-Meansクラスタ。X=年間クレジット(対数)、Y=活動率。"
                "右上ほど活発で継続的に活動。クラスタ名は特徴量ランクから自動命名。"
            ),
        ),
    )

    # ── 2. z-scoreヒートマップ ──
    feat_names = list(data.career_feature_names)
    mean = centers.mean(axis=0)
    std = centers.std(axis=0)
    std[std < 1e-10] = 1.0
    z_scores = (centers - mean) / std

    heatmap = HeatmapSpec(
        chart_id="career_cluster_zscore",
        title="キャリアクラスタ z-score プロファイル",
        z=tuple(tuple(float(v) for v in row) for row in z_scores),
        x_labels=tuple(feat_names),
        y_labels=tuple(cluster_names),
        show_text=True,
        colors=ColorMapping(colorscale="RdBu"),
        height=400,
        explanation=ExplanationMeta(
            question="各クラスタは5つの特徴量でどう特徴付けられるか？",
            reading_guide=(
                "セルの色=全クラスタ平均からの偏差(z-score)。"
                "赤=平均より高い、青=平均より低い。"
                "各クラスタの「個性」が一目で判別可能。"
            ),
        ),
    )

    # ── 3. クラスタ×トレンド クロス集計 (Stacked Bar) ──
    # persons_with_features と labels の対応で集計
    cluster_trend_counts: dict[str, dict[str, int]] = {}
    for i, lbl in enumerate(labels):
        cn = cluster_names[lbl]
        if i < len(persons_with_features):
            trend = persons_with_features[i].trend
        else:
            trend = "unknown"
        cluster_trend_counts.setdefault(cn, {}).setdefault(trend, 0)
        cluster_trend_counts[cn][trend] += 1

    if cluster_trend_counts:
        bar_categories = tuple(cluster_names)
        trend_stacked: dict[str, tuple[float, ...]] = {}
        trend_bar_colors: list[str] = []
        for t in TREND_ORDER:
            vals = tuple(
                float(cluster_trend_counts.get(cn, {}).get(t, 0))
                for cn in cluster_names
            )
            if any(v > 0 for v in vals):
                trend_stacked[t.title()] = vals
                trend_bar_colors.append(TREND_COLORS.get(t, "#888"))

        cross_tab_bar = BarSpec(
            chart_id="career_cluster_trend",
            title="キャリアクラスタ × 成長トレンド クロス集計",
            categories=bar_categories,
            stacked_series=trend_stacked,
            bar_mode="stack",
            colors=ColorMapping(palette=tuple(trend_bar_colors)),
            x_axis=AxisSpec(label="キャリアクラスタ"),
            y_axis=AxisSpec(label="人数"),
            height=450,
            explanation=ExplanationMeta(
                question="キャリアクラスタごとに成長トレンドの構成は異なるか？",
                reading_guide=(
                    "各クラスタ内のRising/Stable/Declining/Newの人数を積み上げ表示。"
                    "長期キャリア型にDecliningが多い場合、ベテラン層の活動縮小を示唆。"
                    "短期型にRisingが多い場合、新人の急成長パターンを示唆。"
                ),
            ),
        )
    else:
        cross_tab_bar = None

    # ── Summary table ──
    rows: list[tuple[str, ...]] = []
    for cid in range(k):
        mask = labels == cid
        n = int(mask.sum())
        avg_credits = float(feats[mask, 0].mean())
        avg_cpy = float(feats[mask, 1].mean())
        avg_span = float(feats[mask, 2].mean())
        avg_ar = float(feats[mask, 3].mean())
        avg_debut = float(feats[mask, 4].mean())
        rows.append((
            cluster_names[cid],
            fmt_num(n),
            f"{avg_credits:.0f}",
            f"{avg_cpy:.1f}",
            f"{avg_span:.1f}年",
            f"{avg_ar:.2f}",
            f"{avg_debut:.0f}",
        ))

    table = TableSpec(
        headers=("クラスタ", "人数", "平均総クレジット", "年間クレジット",
                 "キャリア期間", "活動率", "平均デビュー年"),
        rows=tuple(rows),
        sortable=True,
    )

    charts = [scatter, heatmap]
    if cross_tab_bar is not None:
        charts.append(cross_tab_bar)

    return SectionSpec(
        title="キャリアクラスタ分析 (K-Means)",
        description=(
            f"総クレジット・年間クレジット・キャリア期間・活動率・デビュー年の"
            f"5特徴量で K={k} クラスタに分類。StandardScalerで標準化後に実行。"
        ),
        charts=tuple(charts),
        tables=(table,),
    )


def _build_survival_section(data) -> SectionSpec:
    """キャリア生存・定着曲線 (Bar + Line dual representation)."""
    durations = sorted(data.career_durations)
    total_n = len(durations)

    # 年数ごとの生存人数と生存率を算出
    max_dur = min(int(np.percentile(durations, 99)), 40)  # 40年でキャップ
    years_range = list(range(1, max_dur + 1))

    surviving = []
    survival_rate = []
    for yr in years_range:
        n_survived = sum(1 for d in durations if d >= yr)
        surviving.append(n_survived)
        survival_rate.append(n_survived / total_n * 100)

    # 5年・10年・20年の定着率を findings に
    findings: list[str] = []
    for milestone in (5, 10, 20):
        if milestone <= max_dur:
            rate = sum(1 for d in durations if d >= milestone) / total_n * 100
            findings.append(f"{milestone}年定着率: {rate:.1f}%")

    # 中央値生存年数
    median_dur = float(np.median(durations))
    findings.append(f"キャリア年数中央値: {median_dur:.1f}年")

    # Bar: 生存人数
    bar = BarSpec(
        chart_id="career_survival_bar",
        title="キャリア年数別 残存人数",
        categories=tuple(f"{y}年" for y in years_range),
        values=tuple(float(s) for s in surviving),
        colors=ColorMapping(palette=("#a0d2db",)),
        x_axis=AxisSpec(label="キャリア年数"),
        y_axis=AxisSpec(label="残存人数"),
        height=400,
        explanation=ExplanationMeta(
            question="業界参入から何年で何人が残っているか？",
            reading_guide=(
                "各年数時点で「少なくともN年以上のアクティブ年数」がある人物の数。"
                "急激に減少する年数帯が離脱のピーク。"
            ),
            key_findings=tuple(findings),
        ),
    )

    # Line: 生存率%
    line = LineSpec(
        chart_id="career_survival_rate",
        title="キャリア生存率曲線（Kaplan-Meier近似）",
        series={
            "生存率": tuple((float(y), r) for y, r in zip(years_range, survival_rate)),
        },
        colors=ColorMapping(palette=("#f093fb",)),
        x_axis=AxisSpec(label="キャリア年数"),
        y_axis=AxisSpec(label="生存率 (%)"),
        height=400,
        explanation=ExplanationMeta(
            question="業界定着率は年数とともにどう推移するか？",
            reading_guide=(
                "Y軸=業界参入からN年後にまだ活動している割合(%)。"
                "急降下する箇所が「離脱のクリティカルポイント」。"
                "カーブが緩やかになる年数以降は定着が安定。"
            ),
            caveats=(
                "active_yearsは累積活動年数であり、最終活動年からの経過年数ではありません。"
                "近年デビューした人材はまだキャリア年数が短いため、右側のデータ点は"
                "生存者バイアスの影響を受けています。",
            ),
        ),
    )

    return SectionSpec(
        title="キャリア生存・定着曲線",
        description=(
            f"全{fmt_num(total_n)}人のアクティブ年数分布から業界定着率を可視化。"
            f"キャリア年数中央値: {median_dur:.1f}年。"
        ),
        charts=(bar, line),
    )
