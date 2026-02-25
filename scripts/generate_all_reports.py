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
"""


def wrap_html(title: str, subtitle: str, body: str) -> str:
    """共通HTMLテンプレート."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
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
    <p class="timestamp">Generated: {ts}</p>
</header>
{body}
<footer>
    <p>Generated by Animetor Eval Pipeline Analysis</p>
    <p>Data: 125,419 persons / 60,091 anime / 994,854 credits</p>
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

    html = wrap_html(
        "Industry Overview Dashboard",
        "アニメ業界の包括的分析レポート — 125,419人 / 60,091作品 / 994,854クレジット",
        body,
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
    body += plotly_div_safe(fig, "bridge_scatter", 500)
    body += "</div>"

    html = wrap_html(
        "Network Bridge Analysis",
        f"コミュニティ間ブリッジ分析 — {fmt_num(stats.get('bridge_person_count', 0))} bridge persons / {fmt_num(stats.get('total_communities', 0))} communities",
        body,
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
        body += plotly_div_safe(fig, "role_combos", 450)
        body += "</div>"

    # Recommended pairs
    if rec_pairs:
        body += '<div class="card">'
        body += "<h2>Top 30 Recommended Collaboration Pairs</h2>"
        body += "<table><thead><tr>"
        body += "<th>#</th><th>Person A</th><th>Person B</th><th>Shared High-Score Works</th>"
        body += "</tr></thead><tbody>"
        for i, pair in enumerate(rec_pairs[:30], 1):
            body += f"<tr><td>{i}</td><td>{pair['person_a']}</td><td>{pair['person_b']}</td>"
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

    html = wrap_html(
        "Team Composition Analysis",
        f"チーム構成パターン分析 — {fmt_num(teams.get('total_high_score', 0))} high-score works analyzed",
        body,
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
            body += plotly_div_safe(fig, "sankey", 600)
            body += "</div>"

    html = wrap_html(
        "Career Transitions Analysis",
        f"キャリアステージ遷移分析 — {fmt_num(transitions.get('total_persons_analyzed', 0) if transitions else 0)} persons analyzed",
        body,
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

    html = wrap_html(
        "Temporal Authority & Foresight",
        f"時系列権威推定・先見スコア分析 — {fmt_num(tp.get('total_persons', 0))} persons / {len(years_computed)} years",
        body,
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
            body += plotly_div_safe(fig, "net_evolution", 200 + 250 * rows)
        body += "</div>"

    html = wrap_html(
        "Network Evolution",
        "ネットワーク構造の時系列変化",
        body,
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
        body += plotly_div_safe(fig, "trend_pie", 450)
        body += "</div>"

        # Sample rising stars
        persons = growth.get("persons", {})
        rising = [(pid, p) for pid, p in persons.items() if p.get("trend") == "rising"]
        rising.sort(key=lambda x: x[1].get("total_credits", 0), reverse=True)

        if rising:
            body += '<div class="card">'
            body += f"<h2>Rising Stars ({len(rising)} persons)</h2>"
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

    html = wrap_html(
        "Growth & Score Analysis",
        "成長トレンドとスコア分析",
        body,
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

    html = wrap_html(
        "Person Ranking & Score Analysis",
        f"人物ランキング・スコア分析 — {fmt_num(len(persons))} persons evaluated",
        body,
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

    html = wrap_html(
        "Compensation Fairness Analysis",
        f"公正報酬・貢献分析 — {fmt_num(fair.get('total_anime', 0))} anime analyzed",
        body,
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

    html = wrap_html(
        "Bias Detection Report",
        "バイアス検出・是正分析レポート",
        body,
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

    html = wrap_html(
        "Genre & Score Affinity Analysis",
        f"ジャンル・スコア親和性分析 — {fmt_num(total_persons)} persons",
        body,
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

    html = wrap_html(
        "Studio Impact Analysis",
        "スタジオ影響・因果効果分析",
        body,
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
        body += plotly_div_safe(fig, "productivity_scatter", 450)
        body += "</div>"

    html = wrap_html(
        "Credit Statistics Report",
        f"クレジット統計 — {fmt_num(summary.get('total_credits', 0))} credits / "
        f"{fmt_num(summary.get('unique_persons', 0))} persons / {fmt_num(summary.get('unique_anime', 0))} anime",
        body,
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
        "title": "Industry Overview Dashboard",
        "subtitle": "業界概観ダッシュボード",
        "desc": "Macro-level trends across 100+ years of anime production. Time series, seasonal patterns, decade comparisons, and growth analysis.",
        "sources": "summary, time_series, decades, seasonal, growth",
    },
    {
        "file": "bridge_analysis.html",
        "title": "Network Bridge Analysis",
        "subtitle": "ネットワーク橋渡し分析",
        "desc": "Individuals who connect otherwise separate communities. Bridge scores, cross-community edges, and connectivity patterns.",
        "sources": "bridges",
    },
    {
        "file": "team_analysis.html",
        "title": "Team Composition Analysis",
        "subtitle": "チーム構成パターン分析",
        "desc": "Team structures behind high-scoring anime. Role combinations, recommended collaboration pairs, and team size vs quality.",
        "sources": "teams",
    },
    {
        "file": "career_transitions.html",
        "title": "Career Transitions Analysis",
        "subtitle": "キャリアステージ遷移分析",
        "desc": "How professionals move through career stages. Transition matrices, Sankey diagrams, and common career paths.",
        "sources": "transitions, role_flow",
    },
    {
        "file": "temporal_foresight.html",
        "title": "Temporal Authority & Foresight",
        "subtitle": "時系列権威・先見スコア分析",
        "desc": "Authority evolution over time, foresight scores for early talent detection, and promotion credit analysis.",
        "sources": "temporal_pagerank",
    },
    {
        "file": "network_evolution.html",
        "title": "Network Evolution",
        "subtitle": "ネットワーク構造の時系列変化",
        "desc": "How the collaboration network topology has changed over time. Node/edge counts, density, and clustering trends.",
        "sources": "network_evolution",
    },
    {
        "file": "growth_scores.html",
        "title": "Growth & Score Analysis",
        "subtitle": "成長トレンドとスコア分析",
        "desc": "Growth trend distribution, rising stars, undervaluation alerts, and PageRank concentration analysis.",
        "sources": "growth, insights_report",
    },
    {
        "file": "person_ranking.html",
        "title": "Person Ranking & Scores",
        "subtitle": "人物ランキング・スコア分析",
        "desc": "Top-ranked professionals by composite score. Score distributions, radar charts, and Authority/Trust/Skill scatter plots.",
        "sources": "scores, individual_profiles",
    },
    {
        "file": "compensation_fairness.html",
        "title": "Compensation Fairness",
        "subtitle": "公正報酬・貢献分析",
        "desc": "Shapley-based fair allocation, Gini coefficients per anime, and multi-axis anime value profiles (commercial/critical/creative).",
        "sources": "fair_compensation, anime_values",
    },
    {
        "file": "bias_detection.html",
        "title": "Bias Detection Report",
        "subtitle": "バイアス検出・是正分析",
        "desc": "Systematic biases by role, studio, and career stage. Undervaluation alerts and correction recommendations.",
        "sources": "bias_report, credit_stats, insights_report",
    },
    {
        "file": "genre_analysis.html",
        "title": "Genre & Score Affinity",
        "subtitle": "ジャンル・スコア親和性分析",
        "desc": "How professionals cluster by anime quality tier and era. Specialist vs generalist concentration analysis.",
        "sources": "genre_affinity, anime_stats",
    },
    {
        "file": "studio_impact.html",
        "title": "Studio Impact Analysis",
        "subtitle": "スタジオ影響・因果効果分析",
        "desc": "Causal effects of studio affiliation (selection/treatment/brand), structural estimation, and studio comparisons.",
        "sources": "causal_identification, structural_estimation, studios",
    },
    {
        "file": "credit_statistics.html",
        "title": "Credit Statistics",
        "subtitle": "クレジット統計レポート",
        "desc": "Credit counts, role distribution, year-by-year timelines, role flow (Sankey), and productivity metrics.",
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
