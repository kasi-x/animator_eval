#!/usr/bin/env python3
"""全レポート・ビジュアライゼーション一括生成スクリプト.

パイプライン結果から以下を生成:
- 13のインタラクティブHTML分析レポート (Plotly embedded)
- インデックスHTML (全レポート一覧)
- matplotlib静的グラフ (PNG)
- Plotlyインタラクティブグラフ (HTML)

Usage:
    pixi run python scripts/generate_all_reports.py
"""

import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

JSON_DIR = Path("result/json")
REPORTS_DIR = Path("result/reports")
GRAPHS_DIR = Path("result/graphs")


def load_json(name: str) -> dict | list | None:
    """JSONファイルを安全にロード."""
    path = JSON_DIR / name
    if not path.exists():
        print(f"  [SKIP] {name} not found")
        return None
    with open(path) as f:
        return json.load(f)


# ============================================================
# Shared HTML template helpers
# ============================================================

COMMON_CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Noto Sans CJK JP', sans-serif;
    line-height: 1.6; color: #1a1a2e; min-height: 100vh;
}
.page-bg {
    background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
    min-height: 100vh; padding: 2rem;
}
.container { max-width: 1400px; margin: 0 auto; }
header {
    text-align: center; padding: 3rem 2rem 2rem;
    color: white;
}
header h1 {
    font-size: 2.8rem; font-weight: 800;
    background: linear-gradient(135deg, #f093fb, #f5576c, #fda085);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text; margin-bottom: 0.5rem;
}
header .subtitle { font-size: 1.1rem; opacity: 0.7; color: #a0a0c0; }
header .timestamp { font-size: 0.85rem; opacity: 0.5; color: #808090; margin-top: 0.5rem; }

.card {
    background: rgba(255,255,255,0.05);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 16px; padding: 2rem;
    margin-bottom: 1.5rem; color: #e0e0e0;
}
.card h2 {
    font-size: 1.6rem; margin-bottom: 1rem;
    color: #f093fb; font-weight: 700;
}
.card h3 { font-size: 1.2rem; color: #a0d2db; margin: 1.5rem 0 0.8rem; }

.stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem; margin: 1.5rem 0;
}
.stat-card {
    background: linear-gradient(135deg, rgba(240,147,251,0.15), rgba(245,87,108,0.1));
    border: 1px solid rgba(240,147,251,0.2);
    border-radius: 12px; padding: 1.5rem; text-align: center;
}
.stat-card .value {
    font-size: 2.2rem; font-weight: 800;
    background: linear-gradient(135deg, #f093fb, #f5576c);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
}
.stat-card .label { font-size: 0.85rem; color: #a0a0c0; margin-top: 0.3rem; }

table {
    width: 100%; border-collapse: collapse;
    margin: 1rem 0; font-size: 0.9rem;
}
thead { background: rgba(240,147,251,0.15); }
thead th {
    padding: 0.8rem 1rem; text-align: left;
    color: #f093fb; font-weight: 600;
    border-bottom: 2px solid rgba(240,147,251,0.3);
}
tbody tr { border-bottom: 1px solid rgba(255,255,255,0.05); }
tbody tr:hover { background: rgba(255,255,255,0.05); }
tbody td { padding: 0.6rem 1rem; color: #c0c0d0; }
tbody td:first-child { color: #a0d2db; font-weight: 600; }

.badge {
    display: inline-block; padding: 0.2rem 0.6rem;
    border-radius: 12px; font-size: 0.75rem; font-weight: 600;
}
.badge-high { background: rgba(6,214,160,0.2); color: #06D6A0; }
.badge-mid { background: rgba(255,209,102,0.2); color: #FFD166; }
.badge-low { background: rgba(239,71,111,0.2); color: #EF476F; }

.chart-container {
    background: rgba(0,0,0,0.2); border-radius: 12px;
    padding: 1rem; margin: 1rem 0;
}

.insight-box {
    background: linear-gradient(135deg, rgba(160,210,219,0.1), rgba(240,147,251,0.05));
    border-left: 3px solid #a0d2db;
    border-radius: 0 12px 12px 0;
    padding: 1.2rem 1.5rem; margin: 1rem 0;
}
.insight-box strong { color: #a0d2db; }

footer {
    text-align: center; padding: 2rem;
    color: rgba(255,255,255,0.3); font-size: 0.8rem;
}

.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }
@media (max-width: 900px) { .two-col { grid-template-columns: 1fr; } }

.report-intro {
    background: linear-gradient(135deg, rgba(240,147,251,0.08), rgba(160,210,219,0.08));
    border: 1px solid rgba(240,147,251,0.15);
    border-radius: 16px; padding: 2rem;
    margin-bottom: 1.5rem; color: #c0c0d0;
}
.report-intro h2 { color: #f093fb; font-size: 1.4rem; margin-bottom: 0.8rem; }
.report-intro p { margin-bottom: 0.6rem; line-height: 1.7; }
.report-intro .audience { font-size: 0.85rem; color: #a0a0c0; font-style: italic; margin-top: 0.5rem; }

.chart-guide {
    border-left: 3px solid #667eea;
    background: rgba(102,126,234,0.06);
    border-radius: 0 8px 8px 0;
    padding: 0.8rem 1.2rem; margin: 0.8rem 0;
    font-size: 0.88rem; color: #b0b0c0;
}
.chart-guide strong { color: #667eea; }

.section-desc {
    color: #a0a0c0; font-size: 0.9rem;
    margin: 0.5rem 0 1rem; line-height: 1.6;
}

.disclaimer-block {
    border: 1px solid rgba(255,209,102,0.3);
    background: rgba(255,209,102,0.05);
    border-radius: 12px; padding: 1.5rem;
    margin: 2rem 0; color: #c0c0d0;
}
.disclaimer-block h3 { color: #FFD166; font-size: 1.1rem; margin-bottom: 0.8rem; }
.disclaimer-block p { font-size: 0.88rem; line-height: 1.7; margin-bottom: 0.5rem; }

details.glossary-toggle {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px; padding: 1.2rem;
    margin: 1.5rem 0; color: #c0c0d0;
}
details.glossary-toggle summary {
    cursor: pointer; font-weight: 600; color: #a0d2db;
    font-size: 1.1rem; padding: 0.3rem 0;
}
details.glossary-toggle dl { margin-top: 1rem; }
details.glossary-toggle dt {
    font-weight: 600; color: #f093fb;
    margin-top: 0.8rem; font-size: 0.95rem;
}
details.glossary-toggle dd {
    margin-left: 1.2rem; color: #b0b0c0;
    font-size: 0.88rem; line-height: 1.6;
}

.methodology {
    font-size: 0.82rem; color: rgba(255,255,255,0.4);
    max-width: 800px; margin: 0 auto;
    line-height: 1.6; text-align: left;
}

.toc { margin: 1rem 0 1.5rem; }
.toc a {
    color: #a0d2db; text-decoration: none;
    font-size: 0.9rem; display: inline-block;
    margin-right: 1.5rem; margin-bottom: 0.4rem;
}
.toc a:hover { text-decoration: underline; }
"""


# ============================================================
# 共通テキスト定数・ヘルパー関数
# ============================================================

DISCLAIMER = (
    "本スコアは公開クレジットデータに基づくネットワーク上の位置・協業密度の定量指標であり、"
    "個人の能力・技量・芸術性を評価・測定・示唆するものではありません。"
    "低スコアはデータセット上のネットワーク可視性が限定的であることを意味し、"
    "実力の不足を意味するものではありません。"
    "本データを雇用・報酬・人事評価の唯一の根拠として使用することは推奨されません。"
)

METHODOLOGY_SUMMARY = (
    "評価は3軸で構成されます：(1) Authority（権威）— 重み付きPageRankによる"
    "著名監督・作品への近接性、(2) Trust（信頼）— 同一監督からの継続起用による"
    "累積エッジ重み、(3) Skill（技能）— OpenSkill (Plackett-Luce) モデルに基づく"
    "直近プロジェクト貢献度。総合スコアは3軸の重み付き統合値で、0-100に正規化されます。"
)

COMMON_GLOSSARY_TERMS: dict[str, str] = {
    "Authority（権威スコア）": (
        "PageRankベースの中心性指標。著名な監督や高評価作品との協業ネットワーク上の"
        "近さを測定します。値が高いほど業界の中心的な位置にいることを示します。"
    ),
    "Trust（信頼スコア）": (
        "継続的な協業から蓄積されるエッジ重み。同じ監督やプロデューサーから"
        "繰り返し起用されることを反映し、職業的な信頼の指標です。"
    ),
    "Skill（技能スコア）": (
        "OpenSkill (Plackett-Luce) モデルに基づくレーティング。直近のプロジェクト"
        "貢献度と成長軌道を反映します。Authorityと異なり、最近の活動を重視します。"
    ),
    "Composite（総合スコア）": (
        "Authority・Trust・Skillの重み付き統合値。0-100に正規化された"
        "主要ランキング指標です。"
    ),
    "PageRank（ページランク）": (
        "Web検索用に開発されたグラフ中心性アルゴリズム。本システムでは"
        "アニメ協業ネットワーク上での人物の中心性を測定するために使用します。"
    ),
}


def report_intro(title: str, description: str, audience: str) -> str:
    """レポート冒頭の説明ブロックを生成."""
    return (
        f'<div class="report-intro">'
        f"<h2>{title}</h2>"
        f"<p>{description}</p>"
        f'<p class="audience">対象読者: {audience}</p>'
        f"</div>"
    )


def chart_guide(text: str) -> str:
    """チャート読み方ガイドを生成."""
    return f'<div class="chart-guide"><strong>チャートの見方:</strong> {text}</div>'


def key_findings(items: list[str]) -> str:
    """主要な知見ブロックを生成."""
    if not items:
        return ""
    lis = "".join(f"<li>{item}</li>" for item in items)
    return (
        '<div class="insight-box">'
        "<strong>主要な知見 (Key Findings)</strong>"
        f"<ul style='margin:0.5rem 0 0 1.2rem;line-height:1.8'>{lis}</ul>"
        "</div>"
    )


def section_desc(text: str) -> str:
    """セクション説明テキストを生成."""
    return f'<p class="section-desc">{text}</p>'


def build_glossary(terms: dict[str, str]) -> str:
    """折りたたみ用語集を生成."""
    if not terms:
        return ""
    dl = ""
    for term, defn in terms.items():
        dl += f"<dt>{term}</dt><dd>{defn}</dd>"
    return (
        '<details class="glossary-toggle">'
        "<summary>用語集 (Glossary)</summary>"
        f"<dl>{dl}</dl>"
        "</details>"
    )


def wrap_html(title: str, subtitle: str, body: str, *, intro_html: str = "",
              glossary_terms: dict[str, str] | None = None) -> str:
    """共通HTMLテンプレート."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    glossary_html = build_glossary(glossary_terms) if glossary_terms else ""
    disclaimer_html = (
        '<div class="disclaimer-block">'
        "<h3>免責事項 (Disclaimer)</h3>"
        f"<p>{DISCLAIMER}</p>"
        "</div>"
    )
    methodology_html = f'<div class="methodology"><p><strong>評価方法:</strong> {METHODOLOGY_SUMMARY}</p></div>'
    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>{COMMON_CSS}</style>
</head>
<body>
<div class="page-bg">
<div class="container">
<header>
    <h1>{title}</h1>
    <p class="subtitle">{subtitle}</p>
    <p class="timestamp">生成日時: {ts}</p>
</header>
{intro_html}
{body}
{glossary_html}
{disclaimer_html}
<footer>
    <p>Animetor Eval パイプライン分析により自動生成</p>
    <p>データ: 125,419人 / 60,091作品 / 994,854クレジット</p>
    {methodology_html}
</footer>
</div>
</div>
</body>
</html>"""


def plotly_div(fig: go.Figure, div_id: str, height: int = 500) -> str:
    """Plotlyチャートをdivとして埋め込み."""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0.2)",
        font=dict(color="#c0c0d0"),
        height=height,
        margin=dict(l=60, r=30, t=50, b=50),
    )
    chart_json = fig.to_json()
    return f"""<div class="chart-container">
<div id="{div_id}"></div>
<script>
Plotly.newPlot("{div_id}", ...JSON.parse('{chart_json.replace(chr(39), chr(92)+chr(39))}').data,
    JSON.parse('{chart_json.replace(chr(39), chr(92)+chr(39))}').layout,
    {{responsive: true, displayModeBar: true}});
</script>
</div>"""


def plotly_div_safe(fig: go.Figure, div_id: str, height: int = 500) -> str:
    """Plotlyチャートを安全に埋め込み (JSON escaping)."""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0.2)",
        font=dict(color="#c0c0d0"),
        height=height,
        margin=dict(l=60, r=30, t=50, b=50),
    )
    chart_json = fig.to_json()
    # Use base64 encoding for safety
    import base64
    encoded = base64.b64encode(chart_json.encode()).decode()
    return f"""<div class="chart-container">
<div id="{div_id}"></div>
<script>
(function() {{
    var data = JSON.parse(atob("{encoded}"));
    Plotly.newPlot("{div_id}", data.data, data.layout, {{responsive: true, displayModeBar: true}});
}})();
</script>
</div>"""


def fmt_num(n: int | float) -> str:
    """数値をフォーマット."""
    if isinstance(n, float):
        if n >= 1000:
            return f"{n:,.0f}"
        return f"{n:.2f}"
    return f"{n:,}"


# ============================================================
# Report 1: Industry Overview Dashboard
# ============================================================

def generate_industry_overview():
    """業界概観ダッシュボード."""
    print("  Generating Industry Overview Dashboard...")
    summary = load_json("summary.json")
    time_series = load_json("time_series.json")
    decades = load_json("decades.json")
    seasonal = load_json("seasonal.json")
    insights = load_json("insights_report.json")
    growth = load_json("growth.json")

    if not summary:
        return

    d = summary.get("data", {})
    g = summary.get("graph", {})
    s = summary.get("scores", {})

    body = ""

    # Summary stats
    body += '<div class="card">'
    body += "<h2>Pipeline Summary</h2>"
    body += section_desc(
        "パイプライン実行結果の主要指標。グラフのノードには人物とアニメ作品の両方が含まれ、"
        "エッジは共同クレジット関係を表します。"
    )
    body += '<div class="stats-grid">'
    for label, val in [
        ("Total Persons", fmt_num(d.get("persons", 0))),
        ("Total Anime", fmt_num(d.get("anime", 0))),
        ("Total Credits", fmt_num(d.get("credits", 0))),
        ("Graph Nodes", fmt_num(g.get("nodes", 0))),
        ("Graph Edges", fmt_num(g.get("edges", 0))),
        ("Graph Density", f"{g.get('density', 0):.4f}"),
        ("Avg Degree", f"{g.get('avg_degree', 0):.1f}"),
        ("Components", fmt_num(g.get("components", 0))),
        ("Top Composite", f"{s.get('top_composite', 0):.2f}"),
        ("Median Composite", f"{s.get('median_composite', 0):.2f}"),
        ("Largest Component", fmt_num(g.get("largest_component_size", 0))),
        ("Elapsed", f"{summary.get('elapsed_seconds', 0):.0f}s"),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div></div>"

    # Time series chart
    if time_series:
        years = time_series["years"]
        series = time_series["series"]

        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Credits per Year", "Active Persons", "New Entrants", "Unique Anime"),
            vertical_spacing=0.12, horizontal_spacing=0.08,
        )

        fill_colors = {
            "#f093fb": "rgba(240,147,251,0.1)",
            "#a0d2db": "rgba(160,210,219,0.1)",
            "#f5576c": "rgba(245,87,108,0.1)",
            "#fda085": "rgba(253,160,133,0.1)",
        }
        for row, col, key, color in [
            (1, 1, "credit_count", "#f093fb"),
            (1, 2, "active_persons", "#a0d2db"),
            (2, 1, "new_entrants", "#f5576c"),
            (2, 2, "unique_anime", "#fda085"),
        ]:
            vals = series.get(key, {})
            y_vals = [vals.get(str(yr), 0) for yr in years]
            fig.add_trace(go.Scatter(
                x=years, y=y_vals, mode="lines",
                line=dict(color=color, width=2),
                fill="tozeroy", fillcolor=fill_colors[color],
                name=key.replace("_", " ").title(),
                hovertemplate="%{x}: %{y:,.0f}<extra></extra>",
            ), row=row, col=col)

        fig.update_layout(title="Industry Time Series (1917-2027)", showlegend=False)
        body += '<div class="card">'
        body += "<h2>Time Series</h2>"
        body += chart_guide(
            "各サブプロットはアニメ制作史全体の異なる指標を追跡しています。"
            "任意のポイントにホバーすると正確な値が表示されます。上昇傾向は業界の成長を示し、"
            "横ばいや減少は初期の年代におけるデータカバレッジの差を反映している可能性があります。"
        )
        body += plotly_div_safe(fig, "timeseries", 600)
        body += "</div>"

    # Decades comparison
    if decades:
        dec_data = decades.get("decades", decades)
        dec_names = sorted(dec_data.keys())
        credits = [dec_data[d].get("credit_count", 0) for d in dec_names]
        persons = [dec_data[d].get("unique_persons", 0) for d in dec_names]
        anime = [dec_data[d].get("unique_anime", 0) for d in dec_names]

        fig = go.Figure()
        fig.add_trace(go.Bar(x=dec_names, y=credits, name="Credits", marker_color="#f093fb"))
        fig.add_trace(go.Bar(x=dec_names, y=persons, name="Persons", marker_color="#a0d2db"))
        fig.add_trace(go.Bar(x=dec_names, y=anime, name="Anime", marker_color="#fda085"))
        fig.update_layout(barmode="group", title="Decade Comparison")
        body += '<div class="card">'
        body += "<h2>Decade Comparison</h2>"
        body += chart_guide(
            "グループ化された棒グラフで、年代別のクレジット数・人数・作品数を比較します。"
            "どの年代に最も制作活動が活発だったかを把握できます。"
        )
        body += plotly_div_safe(fig, "decades", 450)
        body += "</div>"

    # Seasonal
    if seasonal:
        seasons = seasonal.get("by_season", {})
        season_names = ["winter", "spring", "summer", "fall"]
        season_labels = ["Winter (1-3)", "Spring (4-6)", "Summer (7-9)", "Fall (10-12)"]

        fig = make_subplots(rows=1, cols=2, subplot_titles=("Anime Count by Season", "Credit Count by Season"))
        anime_counts = [seasons.get(s, {}).get("anime_count", 0) for s in season_names]
        credit_counts = [seasons.get(s, {}).get("credit_count", 0) for s in season_names]

        colors = ["#667eea", "#f093fb", "#fda085", "#f5576c"]
        fig.add_trace(go.Bar(x=season_labels, y=anime_counts, marker_color=colors, name="Anime"), row=1, col=1)
        fig.add_trace(go.Bar(x=season_labels, y=credit_counts, marker_color=colors, name="Credits"), row=1, col=2)
        fig.update_layout(title="Seasonal Patterns", showlegend=False)

        body += '<div class="card">'
        body += "<h2>Seasonal Patterns</h2>"
        body += chart_guide(
            "アニメ制作は季節放送サイクルに従います。冬（1-3月）・春（4-6月）・"
            "夏（7-9月）・秋（10-12月）それぞれで制作量と平均品質スコアが異なります。"
        )
        body += plotly_div_safe(fig, "seasonal", 400)

        body += '<div class="stats-grid">'
        for s, label in zip(season_names, season_labels):
            sd = seasons.get(s, {})
            body += f'''<div class="stat-card">
                <div class="value">{fmt_num(sd.get("anime_count", 0))}</div>
                <div class="label">{label}<br>
                    {fmt_num(sd.get("credit_count", 0))} credits /
                    {fmt_num(sd.get("person_count", 0))} persons<br>
                    Avg Score: {sd.get("avg_anime_score", 0):.2f}
                </div>
            </div>'''
        body += "</div></div>"

    # Growth trends
    if growth:
        trend_summary = growth.get("trend_summary", {})
        fig = go.Figure(go.Pie(
            labels=list(trend_summary.keys()),
            values=list(trend_summary.values()),
            marker_colors=["#666", "#a0d2db", "#06D6A0", "#EF476F", "#FFD166"],
            hole=0.4,
            textinfo="label+percent",
            hovertemplate="%{label}: %{value:,}<extra></extra>",
        ))
        fig.update_layout(title=f"Growth Trends ({fmt_num(growth.get('total_persons', 0))} persons)")
        body += '<div class="card">'
        body += "<h2>Growth Trends</h2>"
        body += plotly_div_safe(fig, "growth", 450)
        body += "</div>"

    # Key findings from insights
    if insights:
        body += '<div class="card">'
        body += "<h2>Key Findings</h2>"
        for finding in insights.get("key_findings", []):
            body += f'<div class="insight-box">{finding}</div>'
        body += "<h3>Recommendations</h3>"
        for rec in insights.get("recommendations", []):
            body += f'<div class="insight-box">{rec}</div>'
        body += "</div>"

    body += key_findings([
        "アニメ業界の協業ネットワークは100年以上にわたり拡大を続けており、"
        "特に2000年代以降の成長が顕著",
        "クレジット数の増加率が人数の増加率を上回っており、一人あたりの参加作品数が増加傾向",
        "季節ごとの制作量にはばらつきがあり、放送枠の需給バランスが反映されている",
        "成長トレンド分類では「安定」「上昇」が多数を占め、業界全体として成熟と拡大が共存",
    ])

    html = wrap_html(
        "業界俯瞰ダッシュボード",
        "アニメ業界の包括的分析 — 125,419人 / 60,091作品 / 994,854クレジット",
        body,
        intro_html=report_intro(
            "業界俯瞰レポート",
            "アニメ業界の協業ネットワークをマクロ視点で分析します。100年以上にわたる"
            "制作量の推移、季節放送パターン、年代比較、人材の成長軌道を網羅します。",
            "スタジオ経営者、業界研究者、政策立案者",
        ),
        glossary_terms=COMMON_GLOSSARY_TERMS,
    )
    out = REPORTS_DIR / "industry_overview.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 2: Network & Bridge Analysis
# ============================================================

def generate_bridge_report():
    """ネットワークブリッジ分析レポート."""
    print("  Generating Bridge Analysis Report...")
    bridges = load_json("bridges.json")
    if not bridges:
        return

    stats = bridges.get("stats", {})
    bridge_persons = bridges.get("bridge_persons", [])
    cross_edges = bridges.get("cross_community_edges", [])
    connectivity = bridges.get("community_connectivity", {})

    body = ""

    # Stats
    body += '<div class="card">'
    body += "<h2>Bridge Detection Summary</h2>"
    body += '<div class="stats-grid">'
    for label, val in [
        ("Total Communities", fmt_num(stats.get("total_communities", 0))),
        ("Bridge Persons", fmt_num(stats.get("bridge_person_count", 0))),
        ("Cross-Community Edges", fmt_num(stats.get("total_cross_edges", 0))),
        ("Community Pairs", fmt_num(len(connectivity))),
        ("Total Network Persons", fmt_num(stats.get("total_persons", 0))),
        ("Bridge Ratio", f"{stats.get('bridge_person_count', 0) / max(stats.get('total_persons', 1), 1) * 100:.1f}%"),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div>"
    body += '<div class="insight-box"><strong>Interpretation:</strong> '
    body += f"Of {fmt_num(stats.get('total_persons', 0))} persons across {fmt_num(stats.get('total_communities', 0))} communities, "
    body += f"{fmt_num(stats.get('bridge_person_count', 0))} serve as bridges connecting different communities. "
    body += "These individuals facilitate knowledge transfer and collaboration across otherwise separate groups.</div>"
    body += "</div>"

    # Bridge score distribution
    score_dist = Counter()
    for bp in bridge_persons:
        score_dist[bp["bridge_score"]] += 1

    scores_sorted = sorted(score_dist.keys())
    fig = go.Figure(go.Bar(
        x=[str(s) for s in scores_sorted],
        y=[score_dist[s] for s in scores_sorted],
        marker_color=[f"rgba(240,147,251,{0.3 + 0.7 * s / 100})" for s in scores_sorted],
        hovertemplate="Score %{x}: %{y} persons<extra></extra>",
    ))
    fig.update_layout(title="Bridge Score Distribution", xaxis_title="Bridge Score", yaxis_title="Person Count")

    body += '<div class="card">'
    body += "<h2>Bridge Score Distribution</h2>"
    body += chart_guide(
        "ブリッジスコアは0〜100の範囲です。高スコアほど、より多くのコミュニティを"
        "より強い結びつきで接続していることを示します。棒の色の濃さはスコア値を反映します。"
    )
    body += plotly_div_safe(fig, "bridge_scores", 400)
    body += "</div>"

    # Communities connected distribution
    comm_dist = Counter()
    for bp in bridge_persons:
        comm_dist[bp["communities_connected"]] += 1

    comm_sorted = sorted(comm_dist.keys())
    fig = go.Figure(go.Bar(
        x=[str(c) for c in comm_sorted],
        y=[comm_dist[c] for c in comm_sorted],
        marker_color="#a0d2db",
        hovertemplate="%{x} communities: %{y} persons<extra></extra>",
    ))
    fig.update_layout(title="Communities Connected per Bridge Person", xaxis_title="Communities Connected", yaxis_title="Person Count")

    body += '<div class="card">'
    body += "<h2>Communities Connected</h2>"
    body += plotly_div_safe(fig, "communities_connected", 400)
    body += "</div>"

    # Cross-community edge analysis
    if cross_edges:
        comm_pair_counts = Counter()
        for edge in cross_edges:
            pair = tuple(sorted([edge["community_a"], edge["community_b"]]))
            comm_pair_counts[pair] += 1

        top_pairs = comm_pair_counts.most_common(30)
        pair_labels = [f"C{a}-C{b}" for (a, b), _ in top_pairs]
        pair_counts = [c for _, c in top_pairs]

        fig = go.Figure(go.Bar(
            x=pair_labels, y=pair_counts,
            marker_color="#f5576c",
            hovertemplate="%{x}: %{y} edges<extra></extra>",
        ))
        fig.update_layout(title="Top 30 Community Pairs by Cross-Edges", xaxis_title="Community Pair", yaxis_title="Edge Count")

        body += '<div class="card">'
        body += "<h2>Cross-Community Connectivity</h2>"
        body += plotly_div_safe(fig, "cross_edges", 400)
        body += "</div>"

    # Top bridge persons table
    body += '<div class="card">'
    body += "<h2>Top 50 Bridge Persons</h2>"
    body += section_desc(
        "ブリッジスコア順にランキング。より多くのコミュニティをより多くの"
        "クロスコミュニティエッジで接続する人物が高スコアを獲得します。"
    )
    body += "<table><thead><tr>"
    body += "<th>#</th><th>Person</th><th>Bridge Score</th><th>Communities</th><th>Cross Edges</th>"
    body += "</tr></thead><tbody>"
    for i, bp in enumerate(bridge_persons[:50], 1):
        score = bp["bridge_score"]
        badge = "badge-high" if score >= 80 else "badge-mid" if score >= 50 else "badge-low"
        name = bp.get("name", bp["person_id"])
        body += f"<tr><td>{i}</td><td>{name}</td>"
        body += f'<td><span class="badge {badge}">{score}</span></td>'
        body += f"<td>{bp['communities_connected']}</td><td>{bp['cross_community_edges']}</td></tr>"
    body += "</tbody></table></div>"

    # Bridge score vs cross edges scatter
    fig = go.Figure(go.Scatter(
        x=[bp["cross_community_edges"] for bp in bridge_persons],
        y=[bp["bridge_score"] for bp in bridge_persons],
        mode="markers",
        marker=dict(
            size=6, color=[bp["communities_connected"] for bp in bridge_persons],
            colorscale="Viridis", showscale=True,
            colorbar=dict(title="Communities"),
        ),
        hovertemplate="Cross Edges: %{x}<br>Score: %{y}<br>Communities: %{marker.color}<extra></extra>",
    ))
    fig.update_layout(title="Bridge Score vs Cross-Community Edges", xaxis_title="Cross-Community Edges", yaxis_title="Bridge Score")

    body += '<div class="card">'
    body += "<h2>Score vs Connectivity</h2>"
    body += chart_guide(
        "各ドットは1人のブリッジ人材を表します。X軸=クロスコミュニティエッジ数、"
        "Y軸=ブリッジスコア、色=接続コミュニティ数。右上の外れ値が最も影響力の大きい"
        "ブリッジです。"
    )
    body += plotly_div_safe(fig, "bridge_scatter", 500)
    body += "</div>"

    body += key_findings([
        "全人物のうちブリッジ人材は少数派だが、コミュニティ間の知識移転と"
        "スタイル伝播に不可欠な役割を担う",
        "上位ブリッジ人材は複数のスタジオ・ジャンル圏を横断的に結びつけ、"
        "人材発掘のハブとして機能",
        "ブリッジスコアとクロスコミュニティエッジ数には正の相関があり、"
        "活発な越境コラボレーターほど高スコア",
    ])

    html = wrap_html(
        "ネットワークブリッジ分析",
        f"コミュニティ間ブリッジ分析 — {fmt_num(stats.get('bridge_person_count', 0))}人のブリッジ / {fmt_num(stats.get('total_communities', 0))}コミュニティ",
        body,
        intro_html=report_intro(
            "ブリッジ分析",
            "ブリッジ人材とは、協業ネットワーク上で本来分離しているコミュニティ同士を"
            "接続する人物です。スタジオやジャンルの境界を越えた知識移転・スタイル伝播・"
            "人材発掘を促進します。本レポートでは最も影響力の大きいブリッジと"
            "コミュニティ間接続パターンを特定します。",
            "スタジオ人事、タレントスカウト、ネットワーク研究者",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "ブリッジ人材 (Bridge Person)": (
                "協業グラフ上で2つ以上の異なるコミュニティに所属し、"
                "それらを接続する人物。"
            ),
            "ブリッジスコア (Bridge Score)": (
                "接続するコミュニティ数とクロスコミュニティ結合の強さを"
                "反映した0-100の複合指標。"
            ),
            "コミュニティ (Community)": (
                "協業ネットワーク上の密に接続されたクラスタ。"
                "グラフ分割アルゴリズムにより検出。"
            ),
        },
    )
    out = REPORTS_DIR / "bridge_analysis.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 3: Team Composition Analysis
# ============================================================

def generate_team_report():
    """チーム構成分析レポート."""
    print("  Generating Team Composition Report...")
    teams = load_json("teams.json")
    if not teams:
        return

    high_score_teams = teams.get("high_score_teams", [])
    role_combos = teams.get("role_combinations", [])
    rec_pairs = teams.get("recommended_pairs", [])
    size_stats = teams.get("team_size_stats", {})

    body = ""

    # Summary stats
    body += '<div class="card">'
    body += "<h2>Team Analysis Summary</h2>"
    body += '<div class="stats-grid">'
    for label, val in [
        ("High-Score Works", fmt_num(teams.get("total_high_score", 0))),
        ("Top 50 Teams Listed", fmt_num(len(high_score_teams))),
        ("Role Combinations", fmt_num(len(role_combos))),
        ("Recommended Pairs", fmt_num(len(rec_pairs))),
        ("Min Team Size", fmt_num(size_stats.get("min", 0))),
        ("Max Team Size", fmt_num(size_stats.get("max", 0))),
        ("Avg Team Size", f"{size_stats.get('avg', 0):.1f}"),
        ("High-Score Avg Size", f"{size_stats.get('high_score_avg', 0):.1f}"),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div></div>"

    # Top teams table
    body += '<div class="card">'
    body += "<h2>Top 50 High-Scoring Teams</h2>"
    body += "<table><thead><tr>"
    body += "<th>#</th><th>Title</th><th>Year</th><th>Score</th><th>Team Size</th><th>Core Roles</th>"
    body += "</tr></thead><tbody>"
    for i, team in enumerate(high_score_teams[:50], 1):
        score = team.get("anime_score", 0)
        badge = "badge-high" if score >= 8.5 else "badge-mid" if score >= 8.0 else "badge-low"
        title = team.get("title", team.get("anime_id", ""))
        body += f"<tr><td>{i}</td><td>{title}</td><td>{team.get('year', '')}</td>"
        body += f'<td><span class="badge {badge}">{score:.1f}</span></td>'
        body += f"<td>{team.get('team_size', 0)}</td><td>{team.get('core_roles', 0)}</td></tr>"
    body += "</tbody></table></div>"

    # Team score vs size scatter
    if high_score_teams:
        fig = go.Figure(go.Scatter(
            x=[t.get("team_size", 0) for t in high_score_teams],
            y=[t.get("anime_score", 0) for t in high_score_teams],
            mode="markers",
            marker=dict(
                size=10, color=[t.get("year", 2020) for t in high_score_teams],
                colorscale="Viridis", showscale=True,
                colorbar=dict(title="Year"),
            ),
            text=[t.get("title", "") for t in high_score_teams],
            hovertemplate="%{text}<br>Size: %{x}<br>Score: %{y:.1f}<br>Year: %{marker.color}<extra></extra>",
        ))
        fig.update_layout(title="Team Size vs Anime Score", xaxis_title="Team Size", yaxis_title="Anime Score")

        body += '<div class="card">'
        body += "<h2>Team Size vs Quality</h2>"
        body += chart_guide(
            "各ドットは1作品。X軸=チーム規模（スタッフ数）、Y軸=作品スコア、"
            "色=放送年。チーム規模と品質の相関を確認できます。"
        )
        body += plotly_div_safe(fig, "team_scatter", 500)
        body += "</div>"

    # Role combinations
    if role_combos:
        fig = go.Figure(go.Bar(
            x=[rc["roles"] for rc in role_combos[:20]],
            y=[rc["count"] for rc in role_combos[:20]],
            marker_color="#a0d2db",
            hovertemplate="%{x}: %{y}<extra></extra>",
        ))
        fig.update_layout(title="Top 20 Role Combinations in High-Score Works", xaxis_tickangle=-45)

        body += '<div class="card">'
        body += "<h2>Role Combinations</h2>"
        body += chart_guide(
            "高評価作品に頻出する役職の組み合わせを棒グラフで表示。"
            "どのスタッフ構成が高品質アニメを生み出しやすいかのパターンが分かります。"
        )
        body += plotly_div_safe(fig, "role_combos", 450)
        body += "</div>"

    # Recommended pairs
    if rec_pairs:
        body += '<div class="card">'
        body += "<h2>Top 30 Recommended Collaboration Pairs</h2>"
        body += section_desc(
            "高評価作品で繰り返し共同クレジットされた人物ペア。"
            "共有数が多いほど、創造的相乗効果が高いことを示唆します。"
        )
        body += "<table><thead><tr>"
        body += "<th>#</th><th>Person A</th><th>Person B</th><th>Shared High-Score Works</th>"
        body += "</tr></thead><tbody>"
        for i, pair in enumerate(rec_pairs[:30], 1):
            name_a = pair.get("name_a") or pair["person_a"]
            name_b = pair.get("name_b") or pair["person_b"]
            body += f"<tr><td>{i}</td><td>{name_a}</td><td>{name_b}</td>"
            body += f'<td><span class="badge badge-high">{pair["shared_high_score_works"]}</span></td></tr>'
        body += "</tbody></table></div>"

    # Year distribution of high-score works
    if high_score_teams:
        year_counts = Counter(t.get("year", 0) for t in high_score_teams if t.get("year"))
        years_sorted = sorted(year_counts.keys())
        fig = go.Figure(go.Bar(
            x=years_sorted, y=[year_counts[y] for y in years_sorted],
            marker_color="#f093fb",
            hovertemplate="%{x}: %{y} works<extra></extra>",
        ))
        fig.update_layout(title="High-Score Works by Year", xaxis_title="Year", yaxis_title="Count")
        body += '<div class="card">'
        body += "<h2>High-Score Works Timeline</h2>"
        body += plotly_div_safe(fig, "team_years", 400)
        body += "</div>"

    body += key_findings([
        "チーム規模と作品品質には一定の相関があるが、規模だけでなく"
        "コアロールの充実度が重要",
        "頻出する役職組み合わせパターンが存在し、成功作品に共通するスタッフ構成がある",
        "繰り返し共演するコラボペアは安定した品質を生み出す傾向",
    ])

    html = wrap_html(
        "チーム構成分析",
        f"チーム構成パターン分析 — {fmt_num(teams.get('total_high_score', 0))}本の高評価作品を分析",
        body,
        intro_html=report_intro(
            "チーム構成分析",
            "どのようなチーム構成が優れたアニメを生み出すのか？ 本レポートでは高評価作品の"
            "スタッフ構成を分析し、最適なチーム規模・成功しやすい役職組み合わせ・"
            "実績のあるコラボレーションペアを明らかにします。",
            "プロデューサー、制作デスク、制作進行",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "高評価作品 (High-Score Work)": "平均以上の視聴者/評論家スコアを持つアニメ作品。",
            "コアロール (Core Roles)": (
                "監督・作画監督・キャラクターデザインなど、作品に配置された"
                "主要制作役職の種類数。"
            ),
        },
    )
    out = REPORTS_DIR / "team_analysis.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 4: Career Transitions
# ============================================================

def generate_career_report():
    """キャリア遷移レポート."""
    print("  Generating Career Transitions Report...")
    transitions = load_json("transitions.json")
    role_flow = load_json("role_flow.json")

    if not transitions and not role_flow:
        return

    body = ""

    if transitions:
        trans = transitions.get("transitions", [])
        paths = transitions.get("career_paths", [])
        avg_time = transitions.get("avg_time_to_stage", {})
        total_analyzed = transitions.get("total_persons_analyzed", 0)

        # Summary
        body += '<div class="card">'
        body += "<h2>Career Transition Analysis</h2>"
        body += '<div class="stats-grid">'
        for label, val in [
            ("Persons Analyzed", fmt_num(total_analyzed)),
            ("Transition Types", fmt_num(len(trans))),
            ("Career Paths", fmt_num(len(paths))),
            ("Career Stages", fmt_num(len(avg_time))),
        ]:
            body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
        body += "</div></div>"

        # Transition matrix
        if trans:
            stages = sorted(set(
                [t["from_label"] for t in trans] + [t["to_label"] for t in trans]
            ))
            matrix = {}
            for t in trans:
                matrix[(t["from_label"], t["to_label"])] = t["count"]

            z = [[matrix.get((s1, s2), 0) for s2 in stages] for s1 in stages]

            fig = go.Figure(go.Heatmap(
                z=z, x=stages, y=stages,
                colorscale="Magma", text=[[str(v) if v > 0 else "" for v in row] for row in z],
                texttemplate="%{text}", hovertemplate="%{y} -> %{x}: %{z}<extra></extra>",
            ))
            fig.update_layout(title="Career Stage Transition Matrix", xaxis_title="To Stage", yaxis_title="From Stage")

            body += '<div class="card">'
            body += "<h2>Transition Matrix</h2>"
            body += chart_guide(
                "行=出発ステージ、列=到達ステージ。明るいセルほど遷移が頻繁であることを示します。"
                "行を横に読むと、そのステージの人々が次にどこへ進むかが分かります。"
            )
            body += plotly_div_safe(fig, "transition_matrix", 500)
            body += "</div>"

        # Average time to each stage
        if avg_time:
            stage_labels = []
            avg_years = []
            median_years = []
            sample_sizes = []
            for stage_id in sorted(avg_time.keys(), key=int):
                sd = avg_time[stage_id]
                stage_labels.append(sd.get("label", f"Stage {stage_id}"))
                avg_years.append(sd.get("avg_years", 0))
                median_years.append(sd.get("median_years", 0))
                sample_sizes.append(sd.get("sample_size", 0))

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=stage_labels, y=avg_years, name="Average",
                marker_color="#f093fb",
                hovertemplate="%{x}<br>Avg: %{y:.1f} years<extra></extra>",
            ))
            fig.add_trace(go.Bar(
                x=stage_labels, y=median_years, name="Median",
                marker_color="#a0d2db",
                hovertemplate="%{x}<br>Median: %{y:.1f} years<extra></extra>",
            ))
            fig.update_layout(title="Average Time to Reach Each Career Stage", barmode="group",
                            xaxis_title="Career Stage", yaxis_title="Years")

            body += '<div class="card">'
            body += "<h2>Time to Stage</h2>"
            body += chart_guide(
                "グループ棒グラフで各キャリアステージ到達までの平均年数と中央値を表示。"
                "平均と中央値の差が大きい場合、遅咲きの外れ値が存在する歪んだ分布を示唆します。"
            )
            body += plotly_div_safe(fig, "time_to_stage", 400)

            # Table
            body += "<table><thead><tr><th>Stage</th><th>Avg Years</th><th>Median Years</th><th>Sample Size</th></tr></thead><tbody>"
            for lbl, avg, med, n in zip(stage_labels, avg_years, median_years, sample_sizes):
                body += f"<tr><td>{lbl}</td><td>{avg:.1f}</td><td>{med:.0f}</td><td>{fmt_num(n)}</td></tr>"
            body += "</tbody></table></div>"

        # Top career paths
        if paths:
            fig = go.Figure(go.Bar(
                x=[" → ".join(p["path_labels"]) for p in paths[:15]],
                y=[p["count"] for p in paths[:15]],
                marker_color="#fda085",
                hovertemplate="%{x}: %{y}<extra></extra>",
            ))
            fig.update_layout(title="Top 15 Career Paths", xaxis_tickangle=-45)

            body += '<div class="card">'
            body += "<h2>Most Common Career Paths</h2>"
            body += plotly_div_safe(fig, "career_paths", 450)
            body += "</div>"

    # Sankey diagram from role_flow
    if role_flow:
        nodes = role_flow.get("nodes", [])
        links = role_flow.get("links", [])

        if nodes and links:
            node_labels = [n["label"] for n in nodes]
            node_map = {n["id"]: i for i, n in enumerate(nodes)}

            # Filter to top transitions for readability
            sorted_links = sorted(links, key=lambda x: x["value"], reverse=True)[:40]

            valid = [lk for lk in sorted_links if lk["source"] in node_map and lk["target"] in node_map]
            src_indices = [node_map[lk["source"]] for lk in valid]
            tgt_indices = [node_map[lk["target"]] for lk in valid]
            values = [lk["value"] for lk in valid]

            node_colors = px.colors.qualitative.Pastel
            link_colors = []
            for s in src_indices:
                c = node_colors[s % len(node_colors)]
                # Convert rgb() to rgba() with transparency
                if c.startswith("rgb("):
                    link_colors.append(c.replace("rgb(", "rgba(").replace(")", ",0.3)"))
                else:
                    link_colors.append("rgba(180,180,200,0.3)")

            fig = go.Figure(go.Sankey(
                node=dict(
                    pad=15, thickness=20,
                    label=node_labels,
                    color=[node_colors[i % len(node_colors)] for i in range(len(node_labels))],
                ),
                link=dict(
                    source=src_indices, target=tgt_indices, value=values,
                    color=link_colors,
                ),
            ))
            fig.update_layout(title=f"Career Role Flow (Top 40 transitions, {fmt_num(role_flow.get('total_transitions', 0))} total)")

            body += '<div class="card">'
            body += "<h2>Role Flow (Sankey Diagram)</h2>"
            body += chart_guide(
                "サンキーダイアグラムは役職間のフローを表示します。帯が太いほど、"
                "その遷移を行う人数が多いことを意味します。左から右へ帯を追うと"
                "キャリアパスを辿れます。"
            )
            body += plotly_div_safe(fig, "sankey", 600)
            body += "</div>"

    body += key_findings([
        "キャリアステージ間の遷移には典型的なパターンがあり、"
        "多くの人材が段階的にステージを上がる正規ルートを辿る",
        "各ステージ到達までの平均年数は個人差が大きく、"
        "中央値との乖離が遅咲き人材の存在を示す",
        "最も一般的なキャリアパスは少数のルートに集中しており、"
        "業界標準のキャリア進行が存在する",
    ])

    html = wrap_html(
        "キャリア遷移分析",
        f"キャリアステージ遷移分析 — {fmt_num(transitions.get('total_persons_analyzed', 0) if transitions else 0)}人を分析",
        body,
        intro_html=report_intro(
            "キャリア遷移分析",
            "アニメ業界のプロフェッショナルはどのようにキャリアステージを進むのか？"
            "本レポートでは役職間の遷移パターンを可視化し、各キャリアステージ到達までの"
            "平均期間を示し、最も一般的なキャリアパスをサンキーフローで表現します。",
            "キャリアアドバイザー、アニメーター志望者、人事部門",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "キャリアステージ (Career Stage)": (
                "クレジット履歴の期間と役職の進行に基づく人物の段階分類"
                "（新人・中堅・ベテラン・マスターなど）。"
            ),
            "遷移行列 (Transition Matrix)": (
                "キャリアステージ間の遷移頻度を示すグリッド。"
                "各セル(行,列)=行のステージから列のステージへ移った人数。"
            ),
            "サンキーダイアグラム (Sankey Diagram)": (
                "帯の幅が役職間の遷移量を表すフロー可視化。"
                "主要なキャリアパスを一目で把握できます。"
            ),
        },
    )
    out = REPORTS_DIR / "career_transitions.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 5: Temporal Authority & Foresight
# ============================================================

def generate_temporal_report():
    """時系列権威・先見レポート."""
    print("  Generating Temporal Authority & Foresight Report...")
    tp = load_json("temporal_pagerank.json")
    if not tp:
        return

    timelines = tp.get("authority_timelines", {})
    foresight = tp.get("foresight_scores", {})
    promotions = tp.get("promotion_credits", {})
    years_computed = tp.get("years_computed", [])

    body = ""

    # Summary stats
    body += '<div class="card">'
    body += "<h2>Temporal PageRank Summary</h2>"
    body += '<div class="stats-grid">'
    for label, val in [
        ("Total Persons", fmt_num(tp.get("total_persons", 0))),
        ("Years Computed", fmt_num(len(years_computed))),
        ("Foresight Persons", fmt_num(len(foresight))),
        ("Promotion Persons", fmt_num(len(promotions))),
        ("Computation Time", f"{tp.get('computation_time_seconds', 0):.1f}s"),
        ("Year Range", f"{min(years_computed)}-{max(years_computed)}" if years_computed else "N/A"),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div></div>"

    # Top persons by peak authority — sample 20 with highest peak
    persons_by_peak = []
    for pid, data in timelines.items():
        persons_by_peak.append((pid, data.get("peak_authority", 0), data.get("peak_year", 0), data))
    persons_by_peak.sort(key=lambda x: x[1], reverse=True)

    # Authority timeline for top 10
    if persons_by_peak:
        fig = go.Figure()
        for pid, peak_auth, peak_yr, data in persons_by_peak[:10]:
            snapshots = data.get("snapshots", [])
            if snapshots:
                fig.add_trace(go.Scatter(
                    x=[s["year"] for s in snapshots],
                    y=[s["authority"] for s in snapshots],
                    mode="lines+markers",
                    name=f"{data.get('name', pid)[:20]} (peak={peak_auth:.2f})",
                    hovertemplate="%{x}: %{y:.4f}<extra></extra>",
                    line=dict(width=2),
                    marker=dict(size=5),
                ))
        fig.update_layout(title="Authority Evolution — Top 10 Persons", xaxis_title="Year", yaxis_title="Authority Score")

        body += '<div class="card">'
        body += "<h2>Authority Evolution (Top 10)</h2>"
        body += chart_guide(
            "各線は1人のAuthority（PageRank）の時系列推移。上昇線はネットワーク影響力の"
            "増大、ピーク後の下降はキャリアフェーズの転換を示唆します。"
            "ホバーで年次の正確な値を確認できます。"
        )
        body += plotly_div_safe(fig, "authority_timeline", 550)
        body += "</div>"

    # Top persons table
    body += '<div class="card">'
    body += "<h2>Top 30 by Peak Authority</h2>"
    body += "<table><thead><tr>"
    body += "<th>#</th><th>Person</th><th>Peak Authority</th><th>Peak Year</th><th>Career Start</th><th>Trajectory</th>"
    body += "</tr></thead><tbody>"
    for i, (pid, peak, peak_yr, data) in enumerate(persons_by_peak[:30], 1):
        traj = data.get("trajectory", "unknown")
        badge = "badge-high" if traj == "rising" else "badge-mid" if traj == "stable" else "badge-low"
        t_name = data.get("name", pid)
        body += f"<tr><td>{i}</td><td>{t_name}</td><td>{peak:.4f}</td><td>{peak_yr}</td>"
        body += f"<td>{data.get('career_start_year', '')}</td>"
        body += f'<td><span class="badge {badge}">{traj}</span></td></tr>'
    body += "</tbody></table></div>"

    # Foresight scores
    if foresight:
        foresight_list = sorted(foresight.values(), key=lambda x: x.get("foresight_normalized", 0), reverse=True)

        body += '<div class="card">'
        body += "<h2>Foresight Scores (Early Adopter Detection)</h2>"
        body += '<div class="insight-box"><strong>Foresight</strong> measures the ability to identify talent early. '
        body += f"{len(foresight_list)} persons demonstrated significant foresight in collaborator selection.</div>"

        # Foresight distribution
        fig = go.Figure(go.Histogram(
            x=[f["foresight_normalized"] for f in foresight_list],
            nbinsx=30, marker_color="#f093fb",
            hovertemplate="Score: %{x:.1f}<br>Count: %{y}<extra></extra>",
        ))
        fig.update_layout(title="Foresight Score Distribution", xaxis_title="Foresight (Normalized)", yaxis_title="Count")
        body += chart_guide(
            "高い先見スコアは、将来のスターがまだ無名の時期に協業した人物を示します。"
            "分布は右に偏っており、大多数は低スコアで少数の優秀なタレントスカウトが存在します。"
        )
        body += plotly_div_safe(fig, "foresight_dist", 400)

        # Top foresight table
        body += "<h3>Top 30 Foresight Persons</h3>"
        body += "<table><thead><tr>"
        body += "<th>#</th><th>Person</th><th>Foresight (Norm)</th><th>Foresight (Raw)</th><th>Discoveries</th><th>Confidence</th>"
        body += "</tr></thead><tbody>"
        for i, f in enumerate(foresight_list[:30], 1):
            norm = f.get("foresight_normalized", 0)
            badge = "badge-high" if norm >= 70 else "badge-mid" if norm >= 30 else "badge-low"
            conf = f"[{f.get('confidence_lower', 0):.1f}, {f.get('confidence_upper', 0):.1f}]"
            f_name = f.get("name", f["person_id"])
            body += f"<tr><td>{i}</td><td>{f_name}</td>"
            body += f'<td><span class="badge {badge}">{norm:.1f}</span></td>'
            body += f"<td>{f.get('foresight_raw', 0):.4f}</td>"
            body += f"<td>{f.get('n_discoveries', 0)}</td><td>{conf}</td></tr>"
        body += "</tbody></table></div>"

    # Promotion analysis
    if promotions:
        promo_list = sorted(promotions.values(), key=lambda x: x.get("promotion_count", 0), reverse=True)

        body += '<div class="card">'
        body += "<h2>Promotion Credit Analysis</h2>"
        body += f'<div class="insight-box"><strong>{len(promo_list)}</strong> persons with promotion track records. '
        body += "Promotion credits measure how effectively a person nurtures talent that later achieves prominence.</div>"

        # Success rate distribution
        fig = go.Figure(go.Histogram(
            x=[p.get("promotion_success_rate", 0) * 100 for p in promo_list],
            nbinsx=20, marker_color="#a0d2db",
            hovertemplate="Rate: %{x:.0f}%<br>Count: %{y}<extra></extra>",
        ))
        fig.update_layout(title="Promotion Success Rate Distribution", xaxis_title="Success Rate (%)", yaxis_title="Count")
        body += plotly_div_safe(fig, "promo_dist", 400)

        # Top promoters table
        body += "<h3>Top 20 Talent Promoters</h3>"
        body += "<table><thead><tr>"
        body += "<th>#</th><th>Person</th><th>Promotions</th><th>Successful</th><th>Success Rate</th><th>vs Baseline</th>"
        body += "</tr></thead><tbody>"
        for i, p in enumerate(promo_list[:20], 1):
            rate = p.get("promotion_success_rate", 0) * 100
            badge = "badge-high" if rate >= 50 else "badge-mid" if rate >= 25 else "badge-low"
            p_name = p.get("name", p["person_id"])
            body += f"<tr><td>{i}</td><td>{p_name}</td>"
            body += f"<td>{p.get('promotion_count', 0)}</td><td>{p.get('successful_promotions', 0)}</td>"
            body += f'<td><span class="badge {badge}">{rate:.0f}%</span></td>'
            body += f"<td>{p.get('vs_cohort_baseline', 0):.2f}x</td></tr>"
        body += "</tbody></table></div>"

    body += key_findings([
        "Authority（権威）は静的ではなく、キャリアの進行とともに動的に変化する",
        "高い先見スコアを持つ人物は、将来著名になる人材と早期に協業しており、"
        "タレントスカウトとしての能力を示す",
        "昇進クレジットが高い人物は、後に頭角を現す人材の育成に効果的に貢献している",
    ])

    html = wrap_html(
        "時系列権威・先見スコア分析",
        f"時系列権威推定・先見スコア分析 — {fmt_num(tp.get('total_persons', 0))}人 / {len(years_computed)}年間",
        body,
        intro_html=report_intro(
            "時系列権威・先見分析",
            "Authorityは静的ではなく、キャリアの進行とともに変化します。本レポートでは"
            "各人物のネットワーク中心性（PageRank）の時系列変化を追跡し、将来の人材を"
            "先見的に発見する個人を特定し、育成効果を測定します。",
            "タレントスカウト、スタジオ経営層、キャリア研究者",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "時系列PageRank (Temporal PageRank)": (
                "協業グラフの年次スナップショットに対して計算されたPageRank。"
                "キャリアを通じたネットワーク中心性の変化を示します。"
            ),
            "先見スコア (Foresight Score)": (
                "後に高ランクとなる人物と早期に協業した頻度を測定。"
                "人材を認知される前に発見する能力の指標。"
            ),
            "昇進クレジット (Promotion Credit)": (
                "後に著名になる人材をどれだけ効果的に育成したかを追跡。"
                "高い値は優れたメンタリング実績を意味します。"
            ),
        },
    )
    out = REPORTS_DIR / "temporal_foresight.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 6: Network Evolution
# ============================================================

def generate_network_evolution_report():
    """ネットワーク進化レポート."""
    print("  Generating Network Evolution Report...")
    net_evo = load_json("network_evolution.json")
    if not net_evo:
        return

    body = ""

    years = net_evo.get("years", [])

    if isinstance(net_evo, dict) and "years" in net_evo:
        body += '<div class="card">'
        body += "<h2>Network Evolution Over Time</h2>"
        body += f'<div class="insight-box">Tracking network topology changes across {len(years)} years ({min(years) if years else "?"}-{max(years) if years else "?"}).</div>'

        # Plot each available metric
        available_series = {}
        for k in net_evo:
            if k == "years":
                continue
            v = net_evo[k]
            if isinstance(v, dict):
                # Year-indexed dict
                available_series[k] = v
            elif isinstance(v, list) and len(v) == len(years):
                available_series[k] = dict(zip([str(y) for y in years], v))

        if available_series:
            n_metrics = len(available_series)
            rows = (n_metrics + 1) // 2
            fig = make_subplots(
                rows=rows, cols=2,
                subplot_titles=[k.replace("_", " ").title() for k in available_series],
            )
            colors = ["#f093fb", "#a0d2db", "#f5576c", "#fda085", "#667eea", "#06D6A0"]
            for idx, (key, vals) in enumerate(available_series.items()):
                row = idx // 2 + 1
                col = idx % 2 + 1
                if isinstance(vals, dict):
                    x = sorted(vals.keys(), key=lambda x: int(x) if x.lstrip("-").isdigit() else 0)
                    y = [vals[k] for k in x]
                else:
                    x = years
                    y = vals
                fig.add_trace(go.Scatter(
                    x=x, y=y, mode="lines",
                    line=dict(color=colors[idx % len(colors)], width=2),
                    name=key.replace("_", " ").title(),
                ), row=row, col=col)
            fig.update_layout(title="Network Metrics Over Time", showlegend=False)
            body += chart_guide(
                "各サブプロットはネットワーク位相指標の時系列推移。ノード/エッジ数の増加は"
                "業界の成長を示し、密度やクラスタリングの変化は協業パターンの変遷を明らかにします。"
            )
            body += plotly_div_safe(fig, "net_evolution", 200 + 250 * rows)
        body += "</div>"

    body += key_findings([
        "協業ネットワークは数十年にわたり構造的に変化し続けており、"
        "ノード数・エッジ数ともに増加傾向",
        "ネットワーク密度とクラスタリング係数の推移から、"
        "協業パターンの時代的変遷が読み取れる",
    ])

    html = wrap_html(
        "ネットワーク構造変化",
        "協業ネットワーク位相の時系列変化",
        body,
        intro_html=report_intro(
            "ネットワーク構造変化",
            "アニメ協業ネットワークは構造的にどう変化してきたのか？ 本レポートでは"
            "主要なグラフ指標 — ノード数・エッジ数・密度・クラスタリング係数・"
            "連結成分 — を数十年の制作期間にわたって追跡します。",
            "ネットワーク研究者、業界アナリスト",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "密度 (Density)": (
                "グラフ上の実際のエッジ数と可能なエッジ数の比率。"
                "密度が高いほどネットワーク規模に対して協業接続が多い。"
            ),
            "クラスタリング係数 (Clustering Coefficient)": (
                "ノードがどの程度クラスタを形成する傾向にあるかの指標。"
                "高い値は密接な協業グループの存在を示す。"
            ),
            "連結成分 (Connected Component)": (
                "すべてのノードが何らかのパスで相互到達可能なグループ。"
                "複数の成分は孤立したサブネットワークの存在を意味する。"
            ),
        },
    )
    out = REPORTS_DIR / "network_evolution.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 7: Growth & Score Analysis
# ============================================================

def generate_growth_score_report():
    """成長・スコア分析レポート."""
    print("  Generating Growth & Score Report...")
    growth = load_json("growth.json")
    insights = load_json("insights_report.json")

    body = ""

    if growth:
        trend_summary = growth.get("trend_summary", {})
        total = growth.get("total_persons", 0)

        body += '<div class="card">'
        body += "<h2>Growth Trends Overview</h2>"
        body += '<div class="stats-grid">'
        trend_colors = {"inactive": "#666", "new": "#a0d2db", "rising": "#06D6A0", "declining": "#EF476F", "stable": "#FFD166"}
        for trend, count in sorted(trend_summary.items(), key=lambda x: x[1], reverse=True):
            pct = count / max(total, 1) * 100
            body += f'<div class="stat-card"><div class="value">{fmt_num(count)}</div>'
            body += f'<div class="label">{trend.title()} ({pct:.1f}%)</div></div>'
        body += "</div></div>"

        # Trend pie chart
        fig = go.Figure(go.Pie(
            labels=[t.title() for t in trend_summary],
            values=list(trend_summary.values()),
            marker_colors=[trend_colors.get(t, "#888") for t in trend_summary],
            hole=0.45, textinfo="label+percent",
        ))
        fig.update_layout(title=f"Growth Trend Distribution ({fmt_num(total)} persons)")
        body += '<div class="card">'
        body += "<h2>Trend Distribution</h2>"
        body += chart_guide(
            "ドーナツチャートは全プロフェッショナルの成長カテゴリ分布を表示。"
            "Rising=活動増加中、Declining=活動減少中、Stable=安定、"
            "New=最近参入、Inactive=最近のクレジットなし。"
        )
        body += plotly_div_safe(fig, "trend_pie", 450)
        body += "</div>"

        # Sample rising stars
        persons = growth.get("persons", {})
        rising = [(pid, p) for pid, p in persons.items() if p.get("trend") == "rising"]
        rising.sort(key=lambda x: x[1].get("total_credits", 0), reverse=True)

        if rising:
            body += '<div class="card">'
            body += f"<h2>Rising Stars ({len(rising)} persons)</h2>"
            body += section_desc(
                "「上昇中」に分類されたプロフェッショナル — クレジット数と活動が増加中。"
                "総クレジット数順にソートし、最も活発な人材を強調します。"
            )
            body += "<table><thead><tr>"
            body += "<th>#</th><th>Person</th><th>Total Credits</th><th>Career Span</th><th>Activity Ratio</th><th>Recent Avg Score</th>"
            body += "</tr></thead><tbody>"
            for i, (pid, p) in enumerate(rising[:30], 1):
                g_name = p.get("name", pid)
                body += f"<tr><td>{i}</td><td>{g_name}</td><td>{p.get('total_credits', 0)}</td>"
                body += f"<td>{p.get('career_span', 0)} yrs</td>"
                body += f"<td>{p.get('activity_ratio', 0):.2f}</td>"
                score = p.get("recent_avg_anime_score")
                body += f"<td>{score:.1f if score else 'N/A'}</td></tr>"
            body += "</tbody></table></div>"

    # Insights: undervaluation alerts
    if insights:
        alerts = insights.get("undervaluation_alerts", [])
        if alerts:
            body += '<div class="card">'
            body += f"<h2>Undervaluation Alerts ({len(alerts)} persons)</h2>"
            body += '<div class="insight-box"><strong>Warning:</strong> These persons may be undervalued based on bias-corrected analysis. '
            body += "Studio size bias correction reveals hidden contributions.</div>"
            body += "<table><thead><tr>"
            body += "<th>#</th><th>Person</th><th>Current Composite</th><th>Debiased Authority</th><th>Gap</th><th>Category</th>"
            body += "</tr></thead><tbody>"
            for i, a in enumerate(alerts[:20], 1):
                gap = a.get("authority_gap", 0)
                badge = "badge-high" if gap >= 5 else "badge-mid" if gap >= 2 else "badge-low"
                name = a.get("name", a.get("person_id", ""))
                body += f"<tr><td>{i}</td><td>{name}</td>"
                body += f"<td>{a.get('current_composite', 0):.2f}</td>"
                body += f"<td>{a.get('debiased_authority', 0):.2f}</td>"
                body += f'<td><span class="badge {badge}">+{gap:.2f}</span></td>'
                body += f"<td>{a.get('category', '')}</td></tr>"
            body += "</tbody></table></div>"

        # PageRank analysis
        pa = insights.get("pagerank_analysis", {})
        if pa:
            body += '<div class="card">'
            body += "<h2>PageRank Analysis</h2>"
            body += '<div class="stats-grid">'
            for label, val in [
                ("Top 1% Share", f"{pa.get('top_percentile_share', 0):.1f}%"),
                ("Concentration Ratio", f"{pa.get('concentration_ratio', 0):.3f}"),
                ("Avg Score", f"{pa.get('avg_score', 0):.2f}"),
                ("Median Score", f"{pa.get('median_score', 0):.2f}"),
            ]:
                body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
            body += "</div>"
            body += f'<div class="insight-box"><strong>Score concentration:</strong> The top 1% of persons hold {pa.get("top_percentile_share", 0):.1f}% of total authority. '
            body += f'Average score is {pa.get("avg_score", 0):.2f}, median is {pa.get("median_score", 0):.2f} — '
            body += "indicating a highly skewed distribution typical of scale-free networks.</div>"
            body += "</div>"

    body += key_findings([
        "成長トレンドの分布から、業界の人材動態のバランスが把握できる",
        "「上昇中」の人材はクレジット数が加速的に増加しており、将来の主力候補",
        "バイアス補正後のPageRank分析により、過小評価されている人材が特定される",
        "スコア集中度は上位1%に偏っており、スケールフリーネットワーク特有の分布を示す",
    ])

    html = wrap_html(
        "成長トレンド・スコア分析",
        "成長トレンドとスコア分析",
        body,
        intro_html=report_intro(
            "成長・スコア分析",
            "誰が上昇中で、誰が衰退中で、誰が過小評価されているのか？ 本レポートでは"
            "全プロフェッショナルを成長軌道で分類し、キャリアが加速中のライジングスターを"
            "強調し、バイアス補正PageRank分析に基づく過小評価の可能性を検出します。",
            "タレントスカウト、業界アナリスト、スタジオ人事",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "上昇中 (Rising)": "直近年のクレジット活動が増加傾向にある成長トレンド。",
            "衰退中 (Declining)": "クレジット活動が減少傾向にある成長トレンド。",
            "安定 (Stable)": "直近年にわたって一貫した活動レベル。",
            "非活動 (Inactive)": "直近の分析期間にクレジット記録がない状態。",
            "集中度 (Concentration Ratio)": (
                "上位パーセンタイルが保持する総Authority量の割合。"
                "高い集中度はスケールフリーネットワークの典型。"
            ),
        },
    )
    out = REPORTS_DIR / "growth_scores.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 8: Person Ranking & Score Analysis
# ============================================================

def generate_person_ranking_report():
    """人物ランキング・スコア分析レポート."""
    print("  Generating Person Ranking Report...")
    scores = load_json("scores.json")
    profiles = load_json("individual_profiles.json")

    if not scores:
        return

    # Sort by composite descending
    persons = sorted(scores, key=lambda x: x.get("composite", 0), reverse=True)

    body = ""

    # Summary stats
    composites = [p.get("composite", 0) for p in persons]
    authorities = [p.get("authority", 0) for p in persons]
    trusts = [p.get("trust", 0) for p in persons]
    skills = [p.get("skill", 0) for p in persons]

    body += '<div class="card">'
    body += "<h2>Score Summary</h2>"
    body += '<div class="stats-grid">'
    for label, val in [
        ("Total Persons", fmt_num(len(persons))),
        ("Avg Composite", f"{sum(composites) / max(len(composites), 1):.2f}"),
        ("Top Composite", f"{max(composites):.2f}" if composites else "N/A"),
        ("Median Composite", f"{sorted(composites)[len(composites) // 2]:.2f}" if composites else "N/A"),
        ("Avg Authority", f"{sum(authorities) / max(len(authorities), 1):.2f}"),
        ("Avg Trust", f"{sum(trusts) / max(len(trusts), 1):.2f}"),
        ("Avg Skill", f"{sum(skills) / max(len(skills), 1):.2f}"),
        ("Profiles", fmt_num(len(profiles.get("profiles", {}))) if profiles else "N/A"),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div></div>"

    # Score distribution histograms (2x2)
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Composite Score", "Authority", "Trust", "Skill"),
        vertical_spacing=0.12, horizontal_spacing=0.08,
    )
    for row, col, vals, color, name in [
        (1, 1, composites, "#f093fb", "Composite"),
        (1, 2, authorities, "#a0d2db", "Authority"),
        (2, 1, trusts, "#f5576c", "Trust"),
        (2, 2, skills, "#fda085", "Skill"),
    ]:
        fig.add_trace(go.Histogram(
            x=vals, nbinsx=30, marker_color=color, name=name,
            hovertemplate="%{x:.1f}: %{y}<extra></extra>",
        ), row=row, col=col)
    fig.update_layout(title="Score Distributions", showlegend=False)
    body += '<div class="card">'
    body += "<h2>Score Distributions</h2>"
    body += chart_guide(
        "4つのヒストグラムで各スコア軸の分布を表示。大多数が低スコアに集中し、"
        "長い右裾を持つ — 協業ネットワークに典型的なべき乗則分布です。"
    )
    body += plotly_div_safe(fig, "score_dist", 600)
    body += "</div>"

    # Radar chart for top 10
    top10 = persons[:10]
    fig = go.Figure()
    categories = ["Authority", "Trust", "Skill"]
    for p in top10:
        vals = [p.get("authority", 0), p.get("trust", 0), p.get("skill", 0)]
        fig.add_trace(go.Scatterpolar(
            r=vals + [vals[0]], theta=categories + [categories[0]],
            name=p.get("name", p["person_id"])[:20],
            fill="toself", opacity=0.6,
        ))
    fig.update_layout(
        title="Top 10 — Authority / Trust / Skill Radar",
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
    )
    body += '<div class="card">'
    body += "<h2>Top 10 Radar</h2>"
    body += chart_guide(
        "各ポリゴンは1人の3軸スコアプロファイル。均等な三角形は全領域で均衡していることを"
        "示し、偏った形状は特定の軸に特化していることを示します。"
    )
    body += plotly_div_safe(fig, "radar_top10", 550)
    body += "</div>"

    # Authority vs Trust scatter
    fig = go.Figure(go.Scatter(
        x=authorities, y=trusts, mode="markers",
        marker=dict(size=5, color=composites, colorscale="Viridis", showscale=True,
                    colorbar=dict(title="Composite")),
        text=[p.get("name", "") for p in persons],
        hovertemplate="%{text}<br>Authority: %{x:.1f}<br>Trust: %{y:.1f}<extra></extra>",
    ))
    fig.update_layout(title="Authority vs Trust", xaxis_title="Authority", yaxis_title="Trust")
    body += '<div class="card">'
    body += "<h2>Authority vs Trust</h2>"
    body += chart_guide(
        "各ドットは1人の人物。X=Authority、Y=Trust、色=Composite。"
        "右上象限の人物はネットワーク中心性と継続起用の両方が高い。"
        "ホバーで名前を確認できます。"
    )
    body += plotly_div_safe(fig, "auth_trust_scatter", 500)
    body += "</div>"

    # Role distribution
    role_counts = Counter(p.get("primary_role", "unknown") for p in persons)
    fig = go.Figure(go.Pie(
        labels=list(role_counts.keys()),
        values=list(role_counts.values()),
        hole=0.4, textinfo="label+percent",
    ))
    fig.update_layout(title="Primary Role Distribution")
    body += '<div class="card">'
    body += "<h2>Role Distribution</h2>"
    body += plotly_div_safe(fig, "role_pie", 450)
    body += "</div>"

    # Top 50 table
    body += '<div class="card">'
    body += "<h2>Top 50 Persons by Composite Score</h2>"
    body += section_desc(
        "総合スコア（Authority・Trust・Skillの重み付き統合値）順にランキング。"
        "バッジ色: 緑=上位層(70+)、黄=中間層(40-69)、赤=下位層。"
    )
    body += "<table><thead><tr>"
    body += "<th>#</th><th>Name</th><th>Role</th><th>Composite</th>"
    body += "<th>Authority</th><th>Trust</th><th>Skill</th><th>Credits</th>"
    body += "</tr></thead><tbody>"
    for i, p in enumerate(persons[:50], 1):
        comp = p.get("composite", 0)
        badge = "badge-high" if comp >= 70 else "badge-mid" if comp >= 40 else "badge-low"
        name = p.get("name", p["person_id"])
        body += f"<tr><td>{i}</td><td>{name}</td><td>{p.get('primary_role', '')}</td>"
        body += f'<td><span class="badge {badge}">{comp:.2f}</span></td>'
        body += f"<td>{p.get('authority', 0):.1f}</td><td>{p.get('trust', 0):.1f}</td>"
        body += f"<td>{p.get('skill', 0):.1f}</td><td>{p.get('total_credits', 0)}</td></tr>"
    body += "</tbody></table></div>"

    body += key_findings([
        "スコア分布はべき乗則に従い、少数の上位人材が圧倒的に高いスコアを持つ",
        "Authority・Trust・Skillの3軸プロファイルは人物ごとに大きく異なり、"
        "得意領域の違いが明確に表れる",
        "AuthorityとTrustには正の相関があるが、Skillは独立した軸として機能",
    ])

    html = wrap_html(
        "人物ランキング・スコア分析",
        f"人物ランキング・スコア分析 — {fmt_num(len(persons))}人を評価",
        body,
        intro_html=report_intro(
            "人物ランキング・スコア分析",
            "総合スコアによるアニメ業界プロフェッショナルの決定版ランキング。"
            "3つの評価軸（Authority・Trust・Skill）ごとのスコア分布を分解し、"
            "トップ10のレーダープロファイル比較と、全人口にわたるAuthority-Trust相関を"
            "表示します。",
            "スタジオ経営者、プロデューサー、業界研究者",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "Composite算出方法": (
                "Composite = Authority・Trust・Skillの重み付き合計。各軸は0-100に"
                "正規化。デフォルトではAuthorityとTrustをSkillより重視。"
            ),
        },
    )
    out = REPORTS_DIR / "person_ranking.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 9: Compensation Fairness
# ============================================================

def generate_compensation_report():
    """公正報酬・貢献分析レポート."""
    print("  Generating Compensation Fairness Report...")
    fair = load_json("fair_compensation.json")
    anime_values = load_json("anime_values.json")

    if not fair:
        return

    analyses = fair.get("analyses", [])
    summary = fair.get("summary", {})

    body = ""

    # Summary stats
    body += '<div class="card">'
    body += "<h2>Compensation Fairness Summary</h2>"
    body += '<div class="stats-grid">'
    for label, val in [
        ("Total Anime Analyzed", fmt_num(fair.get("total_anime", 0))),
        ("Avg Gini Coefficient", f"{summary.get('avg_gini_coefficient', 0):.3f}"),
        ("Avg Shapley Correlation", f"{summary.get('avg_shapley_correlation', 0):.3f}"),
        ("Type Distribution", ", ".join(f"{k}: {v}" for k, v in fair.get("anime_type_distribution", {}).items())),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div>"
    body += '<div class="insight-box"><strong>Interpretation:</strong> '
    body += "Gini coefficient measures compensation inequality (0 = perfect equality, 1 = maximum inequality). "
    body += "Shapley correlation measures how well compensation aligns with marginal contribution.</div>"
    body += "</div>"

    # Gini coefficient distribution
    if analyses:
        gini_values = [a["fairness"]["gini_coefficient"] for a in analyses if "fairness" in a]
        fig = go.Figure(go.Histogram(
            x=gini_values, nbinsx=20, marker_color="#f093fb",
            hovertemplate="Gini: %{x:.3f}<br>Count: %{y}<extra></extra>",
        ))
        fig.update_layout(title="Gini Coefficient Distribution", xaxis_title="Gini Coefficient", yaxis_title="Anime Count")
        body += '<div class="card">'
        body += "<h2>Gini Coefficient Distribution</h2>"
        body += chart_guide(
            "分析対象全作品のGini係数分布。0に近いほどスタッフ間のクレジット配分が均等、"
            "1に近いほど少数の人物がクレジット配分を独占していることを示します。"
        )
        body += plotly_div_safe(fig, "gini_dist", 400)
        body += "</div>"

        # Shapley allocation per anime (top 20)
        sorted_analyses = sorted(analyses, key=lambda a: a.get("staff_count", 0), reverse=True)
        for a in sorted_analyses[:5]:
            allocs = a.get("allocations", [])
            if not allocs:
                continue
            names = [al.get("name", al["person_id"]) for al in allocs]
            allocations = [al.get("allocation", 0) for al in allocs]
            roles = [al.get("role", "") for al in allocs]

            fig = go.Figure(go.Bar(
                x=names, y=allocations,
                marker_color=["#f093fb", "#a0d2db", "#f5576c", "#fda085", "#667eea",
                               "#06D6A0", "#FFD166", "#EF476F"][:len(names)],
                text=[f"{r}<br>{v:.1f}%" for r, v in zip(roles, allocations)],
                textposition="auto",
                hovertemplate="%{x}<br>Allocation: %{y:.1f}%<br>%{text}<extra></extra>",
            ))
            gini = a.get("fairness", {}).get("gini_coefficient", 0)
            fig.update_layout(
                title=f"{a.get('anime_title', a['anime_id'])} (Gini: {gini:.3f})",
                xaxis_title="Staff", yaxis_title="Allocation (%)",
            )
            body += '<div class="card">'
            body += f"<h3>{a.get('anime_title', a['anime_id'])}</h3>"
            body += plotly_div_safe(fig, f"shapley_{a['anime_id']}", 400)
            body += "</div>"

    # Anime value multi-axis (radar)
    if anime_values:
        av_list = sorted(anime_values.values(), key=lambda x: x.get("composite_value", 0), reverse=True)
        value_axes = ["commercial_value", "critical_value", "creative_value", "cultural_value", "technical_value"]
        axis_labels = ["Commercial", "Critical", "Creative", "Cultural", "Technical"]

        fig = go.Figure()
        for av in av_list[:8]:
            vals = [av.get(k, 0) for k in value_axes]
            # Normalize to 0-100 range for radar readability
            max_val = max(vals) if max(vals) > 0 else 1
            norm_vals = [v / max_val * 100 for v in vals]
            fig.add_trace(go.Scatterpolar(
                r=norm_vals + [norm_vals[0]], theta=axis_labels + [axis_labels[0]],
                name=f"{av.get('title', av['anime_id'])}",
                fill="toself", opacity=0.5,
            ))
        fig.update_layout(
            title="Anime Value Profiles (Top 8, Normalized)",
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        )
        body += '<div class="card">'
        body += "<h2>Anime Value Profiles</h2>"
        body += plotly_div_safe(fig, "anime_value_radar", 550)

        # Value table
        body += "<table><thead><tr>"
        body += "<th>#</th><th>Title</th><th>Year</th><th>Composite</th>"
        body += "<th>Commercial</th><th>Critical</th><th>Creative</th><th>Technical</th><th>Cultural</th>"
        body += "</tr></thead><tbody>"
        for i, av in enumerate(av_list[:30], 1):
            body += f"<tr><td>{i}</td><td>{av.get('title', av['anime_id'])}</td>"
            body += f"<td>{av.get('year', '')}</td><td>{av.get('composite_value', 0):.1f}</td>"
            body += f"<td>{av.get('commercial_value', 0):.2f}</td>"
            body += f"<td>{av.get('critical_value', 0):.2f}</td>"
            body += f"<td>{av.get('creative_value', 0):.2f}</td>"
            body += f"<td>{av.get('technical_value', 0):.2f}</td>"
            body += f"<td>{av.get('cultural_value', 0):.2f}</td></tr>"
        body += "</tbody></table></div>"

    body += key_findings([
        "Gini係数の分布からアニメごとのクレジット配分の不均等度が把握でき、"
        "作品間で大きなばらつきがある",
        "Shapley値による配分は限界貢献度に基づき、役職の重要度が反映される",
        "5軸の作品価値プロファイル（商業・批評・創造・文化・技術）により、"
        "作品の多面的な価値が可視化される",
    ])

    html = wrap_html(
        "報酬公平性分析",
        f"公正報酬・貢献分析 — {fmt_num(fair.get('total_anime', 0))}作品を分析",
        body,
        intro_html=report_intro(
            "報酬公平性分析",
            "クレジットはスタッフ間で公平に配分されているか？ 本レポートではShapley値を用いて"
            "各人物の限界貢献度を推定し、Gini係数で不均等度を測定します。また、商業・批評・"
            "創造・文化・技術の5次元で作品の価値プロファイルを作成します。",
            "プロデューサー、労働組合関係者、報酬分析担当者",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "Gini係数 (Gini Coefficient)": (
                "不均等度の指標。0=完全平等、1=最大不平等。"
                "各アニメのスタッフ間クレジット配分に適用。"
            ),
            "Shapley値 (Shapley Value)": (
                "ゲーム理論の概念。各参加者の限界貢献度に基づいて全体の価値を公平に配分。"
                "各スタッフの作品成功への寄与度を推定するために使用。"
            ),
        },
    )
    out = REPORTS_DIR / "compensation_fairness.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 10: Bias Detection
# ============================================================

def generate_bias_report():
    """バイアス検出レポート."""
    print("  Generating Bias Detection Report...")
    bias = load_json("bias_report.json")
    insights = load_json("insights_report.json")

    if not bias and not insights:
        return

    body = ""

    if bias:
        summary = bias.get("summary", {})
        body += '<div class="card">'
        body += "<h2>Bias Detection Summary</h2>"
        body += '<div class="stats-grid">'
        for label, val in [
            ("Total Biases Detected", fmt_num(summary.get("total_biases_detected", 0))),
            ("Severe", fmt_num(summary.get("severe_biases", 0))),
            ("Moderate", fmt_num(summary.get("moderate_biases", 0))),
            ("Mild", fmt_num(summary.get("mild_biases", 0))),
        ]:
            body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
        body += "</div>"

        total = summary.get("total_biases_detected", 0)
        if total == 0:
            body += '<div class="insight-box"><strong>No significant biases detected.</strong> '
            body += "The scoring system shows no systematic bias by role, studio, or career stage in current data.</div>"
        body += "</div>"

        # Severity distribution pie chart
        severity_data = {
            "Severe": summary.get("severe_biases", 0),
            "Moderate": summary.get("moderate_biases", 0),
            "Mild": summary.get("mild_biases", 0),
        }
        severity_data = {k: v for k, v in severity_data.items() if v > 0}
        if severity_data:
            fig = go.Figure(go.Pie(
                labels=list(severity_data.keys()),
                values=list(severity_data.values()),
                marker_colors=["#EF476F", "#FFD166", "#a0d2db"],
                hole=0.4, textinfo="label+value",
            ))
            fig.update_layout(title="Bias Severity Distribution")
            body += '<div class="card">'
            body += "<h2>Severity Distribution</h2>"
            body += chart_guide(
                "検出されたバイアスの重大度別内訳。重度のバイアスは大きな系統的歪みであり"
                "補正が必要。軽度のバイアスは許容範囲内の可能性があります。"
            )
            body += plotly_div_safe(fig, "bias_severity", 400)
            body += "</div>"

        # Role biases table
        role_biases = bias.get("role_biases", [])
        if role_biases:
            body += '<div class="card">'
            body += "<h2>Role-Based Biases</h2>"
            body += "<table><thead><tr>"
            body += "<th>Role</th><th>Bias Type</th><th>Severity</th><th>Expected</th><th>Actual</th><th>Gap</th>"
            body += "</tr></thead><tbody>"
            for rb in role_biases[:20]:
                sev = rb.get("severity", "mild")
                badge = "badge-high" if sev == "severe" else "badge-mid" if sev == "moderate" else "badge-low"
                body += f"<tr><td>{rb.get('role', '')}</td><td>{rb.get('bias_type', '')}</td>"
                body += f'<td><span class="badge {badge}">{sev}</span></td>'
                body += f"<td>{rb.get('expected', 0):.2f}</td><td>{rb.get('actual', 0):.2f}</td>"
                body += f"<td>{rb.get('gap', 0):.2f}</td></tr>"
            body += "</tbody></table></div>"

        # Studio biases
        studio_biases = bias.get("studio_biases", [])
        if studio_biases:
            body += '<div class="card">'
            body += "<h2>Studio-Based Biases</h2>"
            body += "<table><thead><tr>"
            body += "<th>Studio</th><th>Bias Type</th><th>Severity</th><th>Effect Size</th>"
            body += "</tr></thead><tbody>"
            for sb in studio_biases[:20]:
                sev = sb.get("severity", "mild")
                badge = "badge-high" if sev == "severe" else "badge-mid" if sev == "moderate" else "badge-low"
                body += f"<tr><td>{sb.get('studio', '')}</td><td>{sb.get('bias_type', '')}</td>"
                body += f'<td><span class="badge {badge}">{sev}</span></td>'
                body += f"<td>{sb.get('effect_size', 0):.3f}</td></tr>"
            body += "</tbody></table></div>"

        # Recommendations
        recommendations = bias.get("recommendations", [])
        if recommendations:
            body += '<div class="card">'
            body += "<h2>Recommendations</h2>"
            for rec in recommendations:
                body += f'<div class="insight-box">{rec}</div>'
            body += "</div>"

    # Insights: bias correction analysis
    if insights:
        bca = insights.get("bias_correction_analysis", {})
        if bca:
            body += '<div class="card">'
            body += "<h2>Bias Correction Analysis</h2>"
            body += section_desc(
                "バイアス特定後、スコアが調整されます。「上昇者」は補正後にスコアが"
                "上がった人物（従来過小評価）。「下降者」は下がった人物"
                "（スタジオや役職の優位性による従来の過大評価）。"
            )
            body += '<div class="stats-grid">'
            for label, val in [
                ("Persons Affected", fmt_num(bca.get("total_persons_affected", 0))),
                ("Avg Correction", f"{bca.get('avg_correction', 0):.3f}"),
            ]:
                body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
            body += "</div>"

            # Top gainers/losers
            gainers = bca.get("top_gainers", [])
            losers = bca.get("top_losers", [])
            if gainers or losers:
                body += '<div class="two-col">'
                if gainers:
                    body += "<div><h3>Top Gainers (Undervalued)</h3>"
                    body += "<table><thead><tr><th>Person</th><th>Correction</th></tr></thead><tbody>"
                    for g in gainers[:10]:
                        body += f"<tr><td>{g.get('name', g.get('person_id', ''))}</td>"
                        body += f'<td><span class="badge badge-high">+{g.get("correction", 0):.3f}</span></td></tr>'
                    body += "</tbody></table></div>"
                if losers:
                    body += "<div><h3>Top Losers (Overvalued)</h3>"
                    body += "<table><thead><tr><th>Person</th><th>Correction</th></tr></thead><tbody>"
                    for lo in losers[:10]:
                        body += f"<tr><td>{lo.get('name', lo.get('person_id', ''))}</td>"
                        body += f'<td><span class="badge badge-low">{lo.get("correction", 0):.3f}</span></td></tr>'
                    body += "</tbody></table></div>"
                body += "</div>"
            body += "</div>"

        # Undervaluation alerts
        alerts = insights.get("undervaluation_alerts", [])
        if alerts:
            body += '<div class="card">'
            body += f"<h2>Undervaluation Alerts ({len(alerts)} persons)</h2>"
            gaps = [a.get("authority_gap", 0) for a in alerts]
            fig = go.Figure(go.Histogram(
                x=gaps, nbinsx=20, marker_color="#EF476F",
                hovertemplate="Gap: %{x:.2f}<br>Count: %{y}<extra></extra>",
            ))
            fig.update_layout(title="Undervaluation Gap Distribution", xaxis_title="Authority Gap", yaxis_title="Count")
            body += plotly_div_safe(fig, "underval_gaps", 400)

            body += "<table><thead><tr>"
            body += "<th>#</th><th>Person</th><th>Current</th><th>Debiased</th><th>Gap</th><th>Category</th>"
            body += "</tr></thead><tbody>"
            for i, a in enumerate(alerts[:20], 1):
                gap = a.get("authority_gap", 0)
                badge = "badge-high" if gap >= 5 else "badge-mid" if gap >= 2 else "badge-low"
                body += f"<tr><td>{i}</td><td>{a.get('name', a.get('person_id', ''))}</td>"
                body += f"<td>{a.get('current_composite', 0):.2f}</td>"
                body += f"<td>{a.get('debiased_authority', 0):.2f}</td>"
                body += f'<td><span class="badge {badge}">+{gap:.2f}</span></td>'
                body += f"<td>{a.get('category', '')}</td></tr>"
            body += "</tbody></table></div>"

    body += key_findings([
        "役職・スタジオ・キャリアステージに起因する系統的バイアスの有無と程度を検証",
        "バイアス補正後のスコアと生スコアの差分から、過小評価・過大評価の人物を特定",
        "補正により、スタジオ所属の優位性に隠れていた実力者が浮かび上がる",
    ])

    html = wrap_html(
        "バイアス検出レポート",
        "バイアス検出・是正分析レポート",
        body,
        intro_html=report_intro(
            "バイアス検出",
            "スコアはスタジオ所属・役職タイプ・キャリアステージによって系統的に"
            "歪められていないか？ 本レポートではバイアスの検定・重大度の定量化を行い、"
            "補正後スコアと生スコアの差分を示して、誰が過小評価され誰が過大評価されているかを"
            "明らかにします。",
            "監査担当者、公平性研究者、人事ポリシー策定者",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "スタジオバイアス (Studio Bias)": (
                "個人の実力ではなくスタジオ所属に起因する"
                "スコアの系統的な膨張または縮小。"
            ),
            "キャリアステージバイアス (Career Stage Bias)": (
                "キャリアステージに基づくスコア上の系統的な有利・不利"
                "（新人が不利、ベテランが有利など）。"
            ),
            "効果量 (Effect Size)": (
                "検出されたバイアスの大きさ。大きいほど"
                "系統的歪みが有意であることを示す。"
            ),
        },
    )
    out = REPORTS_DIR / "bias_detection.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 11: Genre Analysis
# ============================================================

def generate_genre_report():
    """ジャンル親和性分析レポート."""
    print("  Generating Genre Analysis Report...")
    genre = load_json("genre_affinity.json")

    if not genre:
        return

    body = ""

    # Aggregate tier & era distribution across all persons
    all_tiers = Counter()
    all_eras = Counter()
    all_primary_tiers = Counter()
    all_primary_eras = Counter()
    avg_scores = []

    for pid, data in genre.items():
        tiers = data.get("score_tiers", {})
        for tier, pct in tiers.items():
            all_tiers[tier] += pct
        eras = data.get("eras", {})
        for era, pct in eras.items():
            all_eras[era] += pct
        all_primary_tiers[data.get("primary_tier", "unknown")] += 1
        all_primary_eras[data.get("primary_era", "unknown")] += 1
        if data.get("avg_anime_score"):
            avg_scores.append(data["avg_anime_score"])

    total_persons = len(genre)
    body += '<div class="card">'
    body += "<h2>Genre Affinity Overview</h2>"
    body += '<div class="stats-grid">'
    for label, val in [
        ("Total Persons", fmt_num(total_persons)),
        ("Avg Anime Score", f"{sum(avg_scores) / max(len(avg_scores), 1):.2f}"),
        ("Score Tier Types", fmt_num(len(all_tiers))),
        ("Era Types", fmt_num(len(all_eras))),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div></div>"

    # Primary tier distribution
    fig = go.Figure(go.Pie(
        labels=list(all_primary_tiers.keys()),
        values=list(all_primary_tiers.values()),
        marker_colors=["#06D6A0", "#FFD166", "#EF476F", "#a0d2db", "#f093fb"],
        hole=0.4, textinfo="label+percent",
    ))
    fig.update_layout(title="Primary Score Tier Distribution")
    body += '<div class="card">'
    body += "<h2>Score Tier Distribution</h2>"
    body += chart_guide(
        "各人物の主要スコアティア（最も多くのクレジットを持つ品質帯）を表示。"
        "主に高評価アニメに参加する人物は上位ティアに集中します。"
    )
    body += plotly_div_safe(fig, "tier_pie", 450)
    body += "</div>"

    # Era distribution
    fig = go.Figure(go.Pie(
        labels=list(all_primary_eras.keys()),
        values=list(all_primary_eras.values()),
        marker_colors=["#f093fb", "#a0d2db", "#fda085", "#667eea", "#f5576c"],
        hole=0.4, textinfo="label+percent",
    ))
    fig.update_layout(title="Primary Era Distribution")
    body += '<div class="card">'
    body += "<h2>Era Distribution</h2>"
    body += plotly_div_safe(fig, "era_pie", 450)
    body += "</div>"

    # Avg anime score distribution (specialist vs generalist)
    if avg_scores:
        fig = go.Figure(go.Histogram(
            x=avg_scores, nbinsx=30, marker_color="#a0d2db",
            hovertemplate="Score: %{x:.1f}<br>Count: %{y}<extra></extra>",
        ))
        fig.update_layout(title="Average Anime Score Distribution", xaxis_title="Average Anime Score", yaxis_title="Person Count")
        body += '<div class="card">'
        body += "<h2>Anime Score Specialization</h2>"
        body += plotly_div_safe(fig, "score_hist", 400)
        body += "</div>"

    # Specialization analysis — persons with high concentration in one tier
    specialists = []
    for pid, data in genre.items():
        tiers = data.get("score_tiers", {})
        if tiers:
            max_pct = max(tiers.values())
            specialists.append({
                "person_id": pid,
                "primary_tier": data.get("primary_tier", ""),
                "concentration": max_pct,
                "total_credits": data.get("total_credits", 0),
                "avg_score": data.get("avg_anime_score", 0),
            })
    specialists.sort(key=lambda x: x["concentration"], reverse=True)

    # Concentration histogram
    concentrations = [s["concentration"] for s in specialists]
    fig = go.Figure(go.Histogram(
        x=concentrations, nbinsx=20, marker_color="#f5576c",
        hovertemplate="Concentration: %{x:.0f}%<br>Count: %{y}<extra></extra>",
    ))
    fig.update_layout(title="Tier Concentration (Specialist vs Generalist)",
                      xaxis_title="Max Tier Concentration (%)", yaxis_title="Count")
    body += '<div class="card">'
    body += "<h2>Specialist vs Generalist</h2>"
    body += '<div class="insight-box"><strong>Interpretation:</strong> '
    body += "Higher concentration means the person works predominantly in one score tier (specialist). "
    body += "Lower concentration means diverse participation across tiers (generalist).</div>"
    body += plotly_div_safe(fig, "concentration_hist", 400)
    body += "</div>"

    body += key_findings([
        "プロフェッショナルの大多数は特定のスコアティアに偏って活動している（スペシャリスト傾向）",
        "時代分布から、特定の年代に活動が集中する人物と幅広い年代で活躍する人物が区別される",
        "ティア集中度の分布は連続的であり、純粋なスペシャリストからジェネラリストまで段階的",
    ])

    html = wrap_html(
        "ジャンル・スコア親和性分析",
        f"ジャンル・スコア親和性分析 — {fmt_num(total_persons)}人",
        body,
        intro_html=report_intro(
            "ジャンル・スコア親和性",
            "プロフェッショナルは高品質作品に特化しているのか、それとも幅広い品質帯で"
            "活動しているのか？ 本レポートではスコアティア親和性・時代分布・"
            "スペシャリストvsジェネラリストのスペクトラムを分析します。",
            "キャリアアドバイザー、スタジオ企画担当、ジャンル研究者",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "スペシャリスト (Specialist)": (
                "クレジットが単一のスコアティアまたは時代に集中している人物。"
                "ティア集中度が高い（例：90%以上が同一ティア）。"
            ),
            "ジェネラリスト (Generalist)": (
                "複数のスコアティアと時代にわたってクレジットが分散している人物。"
                "ティア集中度が低い。"
            ),
            "スコアティア (Score Tier)": (
                "視聴者/評論家スコアに基づく品質帯"
                "（上位ティア・中間ティア・下位ティアなど）。"
            ),
        },
    )
    out = REPORTS_DIR / "genre_analysis.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 12: Studio Impact Analysis
# ============================================================

def generate_studio_impact_report():
    """スタジオ影響分析レポート."""
    print("  Generating Studio Impact Report...")
    causal = load_json("causal_identification.json")
    structural = load_json("structural_estimation.json")
    studios = load_json("studios.json")

    if not causal and not studios:
        return

    body = ""

    # Causal identification
    if causal:
        estimates = causal.get("causal_estimates", {})
        conclusion = causal.get("conclusion", {})
        sample = causal.get("sample_statistics", {})

        body += '<div class="card">'
        body += "<h2>Causal Effect Identification</h2>"
        body += '<div class="stats-grid">'
        for label, val in [
            ("Total Trajectories", fmt_num(sample.get("total_trajectories", 0))),
            ("Total Transitions", fmt_num(sample.get("total_transitions", 0))),
            ("Dominant Effect", conclusion.get("dominant_effect", "N/A")),
            ("Confidence", conclusion.get("confidence_level", "N/A")),
        ]:
            body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
        body += "</div>"

        # Effect sizes bar chart
        effect_names = []
        effect_values = []
        effect_colors = []
        effect_ci_lower = []
        effect_ci_upper = []
        color_map = {"selection_effect": "#f093fb", "treatment_effect": "#a0d2db", "brand_effect": "#f5576c"}
        for ename, edata in estimates.items():
            effect_names.append(ename.replace("_", " ").title())
            effect_values.append(edata.get("estimate", 0))
            ci = edata.get("confidence_interval", [0, 0])
            effect_ci_lower.append(abs(edata.get("estimate", 0) - ci[0]))
            effect_ci_upper.append(abs(ci[1] - edata.get("estimate", 0)))
            effect_colors.append(color_map.get(ename, "#888"))

        fig = go.Figure(go.Bar(
            x=effect_names, y=effect_values,
            marker_color=effect_colors,
            error_y=dict(type="data", symmetric=False, array=effect_ci_upper, arrayminus=effect_ci_lower),
            hovertemplate="%{x}: %{y:.3f}<extra></extra>",
        ))
        fig.update_layout(title="Causal Effect Estimates (with 95% CI)", yaxis_title="Effect Size")
        body += chart_guide(
            "各棒は推定因果効果と95%信頼区間（誤差棒）を表示。正の値は効果がスコアを"
            "上昇させることを意味し、負の値は低下させることを意味します。"
            "信頼区間がゼロを跨ぐ場合、その効果は統計的に有意ではありません。"
        )
        body += plotly_div_safe(fig, "causal_effects", 400)

        # Interpretations
        for ename, edata in estimates.items():
            interp = edata.get("interpretation", "")
            if interp:
                body += f'<div class="insight-box"><strong>{ename.replace("_", " ").title()}:</strong> {interp}</div>'
        body += "</div>"

    # Structural estimation
    if structural:
        body += '<div class="card">'
        body += "<h2>Structural Estimation</h2>"

        # Fixed effects vs DID comparison
        fe = structural.get("fixed_effects", {})
        did = structural.get("difference_in_differences", {})

        body += '<div class="two-col"><div>'
        body += "<h3>Fixed Effects</h3>"
        body += '<div class="stats-grid">'
        for label, val in [
            ("Beta", f"{fe.get('beta', 0):.4f}"),
            ("p-value", f"{fe.get('p_value', 1):.4f}"),
            ("N obs", fmt_num(fe.get("n_obs", 0))),
            ("R²", f"{fe.get('r_squared', 0):.4f}"),
        ]:
            body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
        body += "</div></div><div>"

        body += "<h3>Difference-in-Differences</h3>"
        body += '<div class="stats-grid">'
        for label, val in [
            ("Beta", f"{did.get('beta', 0):.4f}"),
            ("p-value", f"{did.get('p_value', 1):.4f}"),
            ("N treated", fmt_num(did.get("n_treated", 0))),
            ("N control", fmt_num(did.get("n_control", 0))),
        ]:
            body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
        body += "</div></div></div>"

        # Preferred estimate
        pref = structural.get("preferred_estimate", {})
        if pref:
            body += f'<div class="insight-box"><strong>Preferred Estimate ({pref.get("method", "")}):</strong> '
            body += f'{pref.get("interpretation", "")}</div>'

        # Summary
        summ = structural.get("summary", "")
        if summ:
            body += f'<div class="insight-box">{summ[:500]}</div>'
        body += "</div>"

    # Studio comparison
    if studios:
        # Sort by credit count
        studio_list = [(name, data) for name, data in studios.items()]
        studio_list.sort(key=lambda x: x[1].get("credit_count", 0), reverse=True)

        body += '<div class="card">'
        body += f"<h2>Studio Comparison ({len(studio_list)} studios)</h2>"

        # Top studios bar chart
        top_studios = studio_list[:20]
        fig = make_subplots(rows=1, cols=3, subplot_titles=("Credits", "Persons", "Anime"))
        fig.add_trace(go.Bar(
            x=[s[0] for s in top_studios], y=[s[1].get("credit_count", 0) for s in top_studios],
            marker_color="#f093fb", hovertemplate="%{x}: %{y:,}<extra></extra>",
        ), row=1, col=1)
        fig.add_trace(go.Bar(
            x=[s[0] for s in top_studios], y=[s[1].get("person_count", 0) for s in top_studios],
            marker_color="#a0d2db", hovertemplate="%{x}: %{y:,}<extra></extra>",
        ), row=1, col=2)
        fig.add_trace(go.Bar(
            x=[s[0] for s in top_studios], y=[s[1].get("anime_count", 0) for s in top_studios],
            marker_color="#fda085", hovertemplate="%{x}: %{y:,}<extra></extra>",
        ), row=1, col=3)
        fig.update_layout(title="Top 20 Studios", showlegend=False, xaxis_tickangle=-45,
                          xaxis2_tickangle=-45, xaxis3_tickangle=-45)
        body += plotly_div_safe(fig, "studio_bars", 500)

        # Studio table
        body += "<table><thead><tr>"
        body += "<th>#</th><th>Studio</th><th>Anime</th><th>Persons</th><th>Credits</th>"
        body += "</tr></thead><tbody>"
        for i, (name, data) in enumerate(studio_list[:30], 1):
            body += f"<tr><td>{i}</td><td>{name}</td>"
            body += f"<td>{fmt_num(data.get('anime_count', 0))}</td>"
            body += f"<td>{fmt_num(data.get('person_count', 0))}</td>"
            body += f"<td>{fmt_num(data.get('credit_count', 0))}</td></tr>"
        body += "</tbody></table></div>"

    body += key_findings([
        "選抜効果・処置効果・ブランド効果の3つの因果メカニズムが分離して推定され、"
        "どの効果が支配的かが明らかになる",
        "固定効果推定とDID（差分の差分）推定の比較により、結果の頑健性を確認",
        "スタジオ規模とスタッフ数・クレジット数には正の相関があるが、"
        "品質指標との関係はより複雑",
    ])

    html = wrap_html(
        "スタジオ影響分析",
        "スタジオ影響・因果効果分析",
        body,
        intro_html=report_intro(
            "スタジオ影響分析",
            "有名スタジオで働くことは本当にスコアを向上させるのか、それとも優秀な人材が"
            "集まるだけなのか？ 本レポートでは3つの因果メカニズム — 選抜効果（誰が採用されるか）・"
            "処置効果（スタジオが人材をどう育てるか）・ブランド効果（名声の波及）"
            "— を分離して分析します。",
            "スタジオ経営者、エコノミスト、業界研究者",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "選抜効果 (Selection Effect)": (
                "スタジオが既に優秀な人材を採用することで説明されるスコア差。"
                "育成ではなく採用による差。"
            ),
            "処置効果 (Treatment Effect)": (
                "スタジオでの勤務がスコアに与える純粋な因果的影響。"
                "スタジオの人材育成への貢献度。"
            ),
            "ブランド効果 (Brand Effect)": (
                "名門スタジオへの所属がもたらすスコア膨張。"
                "実際のスキル向上とは独立した名声効果。"
            ),
        },
    )
    out = REPORTS_DIR / "studio_impact.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Report 13: Credit Statistics
# ============================================================

def generate_credit_statistics_report():
    """クレジット統計レポート."""
    print("  Generating Credit Statistics Report...")
    credit_stats = load_json("credit_stats.json")
    role_flow = load_json("role_flow.json")
    productivity = load_json("productivity.json")

    if not credit_stats:
        return

    body = ""
    summary = credit_stats.get("summary", {})

    # Summary stats
    body += '<div class="card">'
    body += "<h2>Credit Statistics Summary</h2>"
    body += '<div class="stats-grid">'
    for label, val in [
        ("Total Credits", fmt_num(summary.get("total_credits", 0))),
        ("Unique Persons", fmt_num(summary.get("unique_persons", 0))),
        ("Unique Anime", fmt_num(summary.get("unique_anime", 0))),
        ("Avg Credits/Person", f"{summary.get('avg_credits_per_person', 0):.1f}"),
        ("Avg Staff/Anime", f"{summary.get('avg_staff_per_anime', 0):.1f}"),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div></div>"

    # Role distribution
    role_dist = credit_stats.get("role_distribution", {})
    if role_dist:
        sorted_roles = sorted(role_dist.items(), key=lambda x: x[1], reverse=True)
        fig = go.Figure(go.Bar(
            x=[r[0] for r in sorted_roles[:20]],
            y=[r[1] for r in sorted_roles[:20]],
            marker_color="#f093fb",
            hovertemplate="%{x}: %{y:,}<extra></extra>",
        ))
        fig.update_layout(title="Role Distribution (Top 20)", xaxis_title="Role", yaxis_title="Credit Count",
                          xaxis_tickangle=-45)
        body += '<div class="card">'
        body += "<h2>Role Distribution</h2>"
        body += plotly_div_safe(fig, "role_dist", 450)
        body += "</div>"

    # Timeline stats
    timeline = credit_stats.get("timeline_stats", {})
    by_year = timeline.get("by_year", [])
    if by_year:
        by_year_sorted = sorted(by_year, key=lambda x: x.get("year", 0))
        years = [y["year"] for y in by_year_sorted]
        credits_by_yr = [y.get("credits", 0) for y in by_year_sorted]
        persons_by_yr = [y.get("person_count", 0) for y in by_year_sorted]
        anime_by_yr = [y.get("anime_count", 0) for y in by_year_sorted]

        fig = make_subplots(rows=1, cols=3, subplot_titles=("Credits/Year", "Persons/Year", "Anime/Year"))
        fig.add_trace(go.Bar(x=years, y=credits_by_yr, marker_color="#f093fb"), row=1, col=1)
        fig.add_trace(go.Bar(x=years, y=persons_by_yr, marker_color="#a0d2db"), row=1, col=2)
        fig.add_trace(go.Bar(x=years, y=anime_by_yr, marker_color="#fda085"), row=1, col=3)
        fig.update_layout(title=f"Credits Timeline ({timeline.get('year_range', '')})", showlegend=False)
        body += '<div class="card">'
        body += "<h2>Credits Timeline</h2>"
        body += chart_guide(
            "3つの並列タイムラインで年間クレジット数・アクティブ人数・作品数を表示。"
            "前年比の傾向から業界の拡大期と縮小期が読み取れます。"
        )
        body += plotly_div_safe(fig, "credit_timeline", 400)
        body += "</div>"

    # Collaboration stats
    collab = credit_stats.get("collaboration_stats", {})
    if collab:
        body += '<div class="card">'
        body += "<h2>Collaboration Statistics</h2>"
        body += '<div class="stats-grid">'
        body += f'<div class="stat-card"><div class="value">{fmt_num(collab.get("total_pair_instances", 0))}</div>'
        body += '<div class="label">Collaboration Pair Instances</div></div>'
        body += "</div></div>"

    # Person stats
    person_stats = credit_stats.get("person_id_stats", {})
    if person_stats:
        body += '<div class="card">'
        body += "<h2>Person Credit Distribution</h2>"
        body += '<div class="stats-grid">'
        for label, val in [
            ("Avg Roles/Person", f"{person_stats.get('avg_roles_per_person', 0):.1f}"),
            ("Max Roles (Single Person)", fmt_num(person_stats.get("max_roles_single_person", 0))),
        ]:
            body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
        body += "</div>"

        # Role diversity distribution
        diversity = person_stats.get("role_diversity_distribution", {})
        if diversity:
            sorted_div = sorted(diversity.items(), key=lambda x: int(x[0]))
            fig = go.Figure(go.Bar(
                x=[f"{d[0]} roles" for d in sorted_div],
                y=[d[1] for d in sorted_div],
                marker_color="#a0d2db",
                hovertemplate="%{x}: %{y} persons<extra></extra>",
            ))
            fig.update_layout(title="Role Diversity Distribution", xaxis_title="Number of Distinct Roles", yaxis_title="Person Count")
            body += plotly_div_safe(fig, "role_diversity", 400)
        body += "</div>"

    # Sankey from role_flow
    if role_flow:
        nodes = role_flow.get("nodes", [])
        links = role_flow.get("links", [])
        if nodes and links:
            node_labels = [n["label"] for n in nodes]
            node_map = {n["id"]: i for i, n in enumerate(nodes)}
            sorted_links = sorted(links, key=lambda x: x["value"], reverse=True)[:40]
            valid_links = [lk for lk in sorted_links if lk["source"] in node_map and lk["target"] in node_map]

            if valid_links:
                node_colors = px.colors.qualitative.Pastel
                fig = go.Figure(go.Sankey(
                    node=dict(pad=15, thickness=20, label=node_labels,
                              color=[node_colors[i % len(node_colors)] for i in range(len(node_labels))]),
                    link=dict(
                        source=[node_map[lk["source"]] for lk in valid_links],
                        target=[node_map[lk["target"]] for lk in valid_links],
                        value=[lk["value"] for lk in valid_links],
                    ),
                ))
                fig.update_layout(title=f"Role Flow (Top 40 of {fmt_num(role_flow.get('total_transitions', 0))} transitions)")
                body += '<div class="card">'
                body += "<h2>Role Flow (Sankey)</h2>"
                body += plotly_div_safe(fig, "credit_sankey", 600)
                body += "</div>"

    # Productivity
    if productivity:
        prod_list = sorted(productivity.items(), key=lambda x: x[1].get("credits_per_year", 0), reverse=True)
        cpy = [p[1].get("credits_per_year", 0) for p in prod_list]
        consistency = [p[1].get("consistency_score", 0) for p in prod_list]

        fig = go.Figure(go.Scatter(
            x=cpy, y=consistency, mode="markers",
            marker=dict(size=6, color="#f5576c", opacity=0.6),
            hovertemplate="Credits/Year: %{x:.1f}<br>Consistency: %{y:.2f}<extra></extra>",
        ))
        fig.update_layout(title="Productivity vs Consistency", xaxis_title="Credits per Year", yaxis_title="Consistency Score")
        body += '<div class="card">'
        body += "<h2>Productivity vs Consistency</h2>"
        body += chart_guide(
            "各ドットは1人の人物。X=活動年あたりのクレジット数、Y=一貫性スコア"
            "（アウトプットの安定度）。右上=多作で安定。左上=安定だが選択的。"
        )
        body += plotly_div_safe(fig, "productivity_scatter", 450)
        body += "</div>"

    body += key_findings([
        "クレジット数の年次推移から業界の成長率と制作量の変遷が定量的に把握できる",
        "役職分布の偏りから、特定の職種に人材が集中している構造が見える",
        "コラボレーションペアの数は業界の協業密度を示す重要な指標",
        "生産性と一貫性のバランスは個人によって大きく異なる",
    ])

    html = wrap_html(
        "クレジット統計レポート",
        f"クレジット統計 — {fmt_num(summary.get('total_credits', 0))}クレジット / "
        f"{fmt_num(summary.get('unique_persons', 0))}人 / {fmt_num(summary.get('unique_anime', 0))}作品",
        body,
        intro_html=report_intro(
            "クレジット統計",
            "すべての分析の基盤となる生データ統計。総クレジット数・役職分布・年次タイムライン・"
            "コラボレーションペア数・生産性指標を網羅します。他のすべての分析が依拠する"
            "統計的ベースラインを提供します。",
            "データアナリスト、パイプラインエンジニア、QAレビュアー",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "クレジット (Credit)": (
                "人物-役職-作品の単一レコード。1人が1作品で複数の役職を担当する場合、"
                "複数のクレジットが発生。"
            ),
            "コラボレーションペア (Collaboration Pair)": (
                "同一アニメ作品で少なくとも1つのクレジットを共有する2人の人物。"
            ),
        },
    )
    out = REPORTS_DIR / "credit_statistics.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Index Page
# ============================================================

REPORT_CATALOG = [
    {
        "file": "industry_overview.html",
        "title": "業界俯瞰ダッシュボード",
        "subtitle": "100年以上のアニメ制作のマクロトレンド",
        "desc": "時系列推移、季節パターン、年代比較、成長分析。",
        "sources": "summary, time_series, decades, seasonal, growth",
    },
    {
        "file": "bridge_analysis.html",
        "title": "ネットワークブリッジ分析",
        "subtitle": "コミュニティ間ブリッジ人材と接続性",
        "desc": "分離したコミュニティを接続する人物。ブリッジスコア、クロスコミュニティエッジ、接続パターン。",
        "sources": "bridges",
    },
    {
        "file": "team_analysis.html",
        "title": "チーム構成分析",
        "subtitle": "高評価アニメのスタッフ構成パターン",
        "desc": "チーム構造、役職組み合わせ、推薦コラボペア、チーム規模vs品質。",
        "sources": "teams",
    },
    {
        "file": "career_transitions.html",
        "title": "キャリア遷移分析",
        "subtitle": "キャリアステージの進行と役職フロー",
        "desc": "プロフェッショナルのキャリアステージ進行。遷移行列、サンキーダイアグラム、一般的なキャリアパス。",
        "sources": "transitions, role_flow",
    },
    {
        "file": "temporal_foresight.html",
        "title": "時系列権威・先見スコア",
        "subtitle": "権威の時系列変化と人材早期発見",
        "desc": "Authorityの時系列推移、先見スコアによる早期人材発見、昇進クレジット分析。",
        "sources": "temporal_pagerank",
    },
    {
        "file": "network_evolution.html",
        "title": "ネットワーク構造変化",
        "subtitle": "協業ネットワーク位相の時系列変化",
        "desc": "協業ネットワークの構造的変化。ノード/エッジ数、密度、クラスタリングの推移。",
        "sources": "network_evolution",
    },
    {
        "file": "growth_scores.html",
        "title": "成長トレンド・スコア分析",
        "subtitle": "成長傾向、ライジングスター、過小評価",
        "desc": "成長トレンド分布、ライジングスター、過小評価アラート、PageRank集中度分析。",
        "sources": "growth, insights_report",
    },
    {
        "file": "person_ranking.html",
        "title": "人物ランキング・スコア分析",
        "subtitle": "総合スコアによる人物ランキング",
        "desc": "総合スコア順の上位人物。スコア分布、レーダーチャート、Authority/Trust/Skill散布図。",
        "sources": "scores, individual_profiles",
    },
    {
        "file": "compensation_fairness.html",
        "title": "報酬公平性分析",
        "subtitle": "Shapley配分とGini分析",
        "desc": "Shapleyベースの公正配分、作品別Gini係数、5軸の作品価値プロファイル（商業/批評/創造）。",
        "sources": "fair_compensation, anime_values",
    },
    {
        "file": "bias_detection.html",
        "title": "バイアス検出レポート",
        "subtitle": "系統的バイアスの検出と補正",
        "desc": "役職・スタジオ・キャリアステージ別の系統的バイアス。過小評価アラートと補正推奨。",
        "sources": "bias_report, credit_stats, insights_report",
    },
    {
        "file": "genre_analysis.html",
        "title": "ジャンル・スコア親和性",
        "subtitle": "品質帯・時代別の親和性分析",
        "desc": "品質帯と時代によるプロフェッショナルのクラスタリング。スペシャリストvsジェネラリスト。",
        "sources": "genre_affinity, anime_stats",
    },
    {
        "file": "studio_impact.html",
        "title": "スタジオ影響分析",
        "subtitle": "スタジオ所属の因果効果",
        "desc": "スタジオ所属の因果効果（選抜/処置/ブランド）、構造推定、スタジオ比較。",
        "sources": "causal_identification, structural_estimation, studios",
    },
    {
        "file": "credit_statistics.html",
        "title": "クレジット統計",
        "subtitle": "クレジット集計、役職分布、生産性",
        "desc": "クレジット数、役職分布、年次タイムライン、役職フロー（サンキー）、生産性指標。",
        "sources": "credit_stats, role_flow, productivity",
    },
]


def generate_index_page():
    """全レポートインデックスHTMLを生成."""
    print("  Generating Index Page...")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Check which reports exist
    existing = {r["file"]: (REPORTS_DIR / r["file"]).exists() for r in REPORT_CATALOG}

    cards_html = ""
    for i, r in enumerate(REPORT_CATALOG, 1):
        exists = existing.get(r["file"], False)
        opacity = "1" if exists else "0.4"
        link_attr = f'href="{r["file"]}"' if exists else 'style="pointer-events:none"'
        status_badge = '<span class="badge badge-high">Ready</span>' if exists else '<span class="badge badge-low">Missing</span>'
        cards_html += f"""
        <a {link_attr} style="text-decoration:none; opacity:{opacity}">
        <div class="report-card">
            <div class="report-num">{i:02d}</div>
            <h3>{r["title"]}</h3>
            <p class="report-subtitle">{r["subtitle"]}</p>
            <p class="report-desc">{r["desc"]}</p>
            <div class="report-meta">
                <span class="report-sources">{r["sources"]}</span>
                {status_badge}
            </div>
        </div>
        </a>"""

    # Count reports
    ready_count = sum(1 for v in existing.values() if v)
    total_count = len(REPORT_CATALOG)

    index_css = """
    .report-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
        gap: 1.5rem; margin: 2rem 0;
    }
    .report-card {
        background: rgba(255,255,255,0.05);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 16px; padding: 1.8rem;
        transition: transform 0.2s, border-color 0.2s;
        cursor: pointer; position: relative;
        min-height: 200px;
    }
    .report-card:hover {
        transform: translateY(-4px);
        border-color: rgba(240,147,251,0.4);
    }
    .report-num {
        position: absolute; top: 1rem; right: 1.2rem;
        font-size: 2.5rem; font-weight: 900; opacity: 0.1;
        color: #f093fb;
    }
    .report-card h3 {
        font-size: 1.3rem; color: #f093fb; margin-bottom: 0.3rem;
        font-weight: 700;
    }
    .report-subtitle {
        font-size: 0.9rem; color: #a0d2db; margin-bottom: 0.8rem;
    }
    .report-desc {
        font-size: 0.85rem; color: #b0b0c0; line-height: 1.5;
        margin-bottom: 1rem;
    }
    .report-meta {
        display: flex; justify-content: space-between; align-items: center;
        font-size: 0.75rem; color: #808090;
        border-top: 1px solid rgba(255,255,255,0.05);
        padding-top: 0.8rem;
    }
    .report-sources { font-style: italic; }
    .summary-bar {
        display: flex; justify-content: center; gap: 3rem;
        margin: 1.5rem 0; flex-wrap: wrap;
    }
    .summary-item {
        text-align: center; color: #c0c0d0;
    }
    .summary-item .value {
        font-size: 2rem; font-weight: 800;
        background: linear-gradient(135deg, #f093fb, #f5576c);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .summary-item .label { font-size: 0.85rem; color: #a0a0c0; }
    @media (max-width: 800px) { .report-grid { grid-template-columns: 1fr; } }
    """

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Animetor Eval — Report Index</title>
<style>{COMMON_CSS}
{index_css}
</style>
</head>
<body>
<div class="page-bg">
<div class="container">
<header>
    <h1>Animetor Eval Reports</h1>
    <p class="subtitle">アニメ業界評価パイプライン — 全分析レポートインデックス</p>
    <p class="timestamp">Generated: {ts}</p>
</header>

<div class="summary-bar">
    <div class="summary-item"><div class="value">{ready_count}/{total_count}</div><div class="label">Reports Ready</div></div>
    <div class="summary-item"><div class="value">13</div><div class="label">Analysis Categories</div></div>
    <div class="summary-item"><div class="value">30+</div><div class="label">JSON Data Sources</div></div>
</div>

<div class="report-grid">
{cards_html}
</div>

<footer>
    <p>Generated by Animetor Eval Pipeline Analysis</p>
    <p style="margin-top:0.5rem">
        <a href="../graphs/" style="color:#a0d2db; text-decoration:none">Interactive Charts</a>
        &nbsp;|&nbsp;
        <a href="../" style="color:#a0d2db; text-decoration:none">Static Charts (PNG)</a>
    </p>
</footer>
</div>
</div>
</body>
</html>"""

    out = REPORTS_DIR / "index.html"
    out.write_text(html, encoding="utf-8")
    print(f"    -> {out}")


# ============================================================
# Run existing matplotlib + Plotly visualizations
# ============================================================

def run_matplotlib_visualizations():
    """既存のmatplotlib可視化を実行."""
    print("  Generating matplotlib static charts...")
    try:
        from src.analysis.visualize import (
            plot_score_distribution,
            plot_top_persons_radar,
            plot_time_series,
            plot_growth_trends,
            plot_decade_comparison,
            plot_seasonal_trends,
            plot_bridge_analysis,
            plot_transition_heatmap,
            plot_role_flow_sankey,
            plot_studio_comparison,
        )
    except ImportError as e:
        print(f"    [SKIP] Could not import visualize: {e}")
        return

    viz_dir = Path("result")
    funcs = [
        ("score_distribution", plot_score_distribution, "scores.json"),
        ("top_radar", plot_top_persons_radar, "scores.json"),
        ("time_series", plot_time_series, "time_series.json"),
        ("growth_trends", plot_growth_trends, "growth.json"),
        ("decade_comparison", plot_decade_comparison, "decades.json"),
        ("seasonal_trends", plot_seasonal_trends, "seasonal.json"),
        ("bridge_analysis", plot_bridge_analysis, "bridges.json"),
        ("transition_heatmap", plot_transition_heatmap, "transitions.json"),
        ("role_flow_sankey", plot_role_flow_sankey, "role_flow.json"),
        ("studio_comparison", plot_studio_comparison, "studios.json"),
    ]

    for name, func, data_file in funcs:
        try:
            data_path = JSON_DIR / data_file
            if not data_path.exists():
                print(f"    [SKIP] {name} (missing {data_file})")
                continue

            with open(data_path) as f:
                data = json.load(f)

            out_path = viz_dir / f"{name}.png"
            func(data, output_path=out_path)
            print(f"    -> {out_path}")
        except Exception as e:
            print(f"    [ERROR] {name}: {type(e).__name__}: {e}")


def run_interactive_visualizations():
    """Plotlyインタラクティブ可視化を実行."""
    print("  Generating Plotly interactive charts...")
    GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        from src.analysis.visualize_interactive import (
            plot_interactive_score_distribution,
            plot_interactive_radar,
            plot_interactive_scatter,
            plot_interactive_timeline,
            plot_interactive_network,
        )
    except ImportError as e:
        print(f"    [SKIP] Could not import visualize_interactive: {e}")
        return

    # Load data
    scores_data = load_json("scores.json")
    time_series = load_json("time_series.json")
    collabs = load_json("collaborations.json")

    if scores_data and isinstance(scores_data, list):
        try:
            plot_interactive_score_distribution(scores_data, output_path=GRAPHS_DIR / "score_distribution.html")
            print(f"    -> {GRAPHS_DIR / 'score_distribution.html'}")
        except Exception as e:
            print(f"    [ERROR] score_distribution: {e}")

        try:
            plot_interactive_radar(scores_data, top_n=15, output_path=GRAPHS_DIR / "radar_top15.html")
            print(f"    -> {GRAPHS_DIR / 'radar_top15.html'}")
        except Exception as e:
            print(f"    [ERROR] radar: {e}")

        for x, y in [("authority", "trust"), ("authority", "skill"), ("trust", "skill")]:
            try:
                plot_interactive_scatter(scores_data, x, y, output_path=GRAPHS_DIR / f"scatter_{x}_{y}.html")
                print(f"    -> {GRAPHS_DIR / f'scatter_{x}_{y}.html'}")
            except Exception as e:
                print(f"    [ERROR] scatter_{x}_{y}: {e}")

    if time_series:
        try:
            plot_interactive_timeline(time_series, output_path=GRAPHS_DIR / "timeline.html")
            print(f"    -> {GRAPHS_DIR / 'timeline.html'}")
        except Exception as e:
            print(f"    [ERROR] timeline: {e}")

    if collabs and isinstance(collabs, list):
        try:
            plot_interactive_network(collabs, top_n=80, output_path=GRAPHS_DIR / "network.html")
            print(f"    -> {GRAPHS_DIR / 'network.html'}")
        except Exception as e:
            print(f"    [ERROR] network: {e}")


# ============================================================
# Main
# ============================================================

def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Animetor Eval — Full Report & Visualization Generator")
    print("=" * 60)
    print()

    # Phase 1: HTML Analysis Reports (13 reports)
    print("[Phase 1] Generating HTML Analysis Reports...")
    generate_industry_overview()
    generate_bridge_report()
    generate_team_report()
    generate_career_report()
    generate_temporal_report()
    generate_network_evolution_report()
    generate_growth_score_report()
    generate_person_ranking_report()
    generate_compensation_report()
    generate_bias_report()
    generate_genre_report()
    generate_studio_impact_report()
    generate_credit_statistics_report()
    generate_index_page()
    print()

    # Phase 2: matplotlib static charts
    print("[Phase 2] Generating matplotlib static charts...")
    run_matplotlib_visualizations()
    print()

    # Phase 3: Plotly interactive charts
    print("[Phase 3] Generating Plotly interactive charts...")
    run_interactive_visualizations()
    print()

    # Summary
    reports = list(REPORTS_DIR.glob("*.html"))
    graphs = list(GRAPHS_DIR.glob("*.html"))
    pngs = list(Path("result").glob("*.png"))

    print("=" * 60)
    print("Generation Complete!")
    print(f"  HTML Reports:      {len(reports)} files in {REPORTS_DIR}/")
    print(f"  Interactive Charts: {len(graphs)} files in {GRAPHS_DIR}/")
    print(f"  Static Charts:     {len(pngs)} files in result/")
    print(f"  Total:             {len(reports) + len(graphs) + len(pngs)} files")
    print("=" * 60)


if __name__ == "__main__":
    main()
