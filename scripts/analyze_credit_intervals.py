#!/usr/bin/env python3
"""クレジット登場間隔分析スクリプト.

職能別（role category）・キャリア年数別に、クレジットの登場間隔を計算し
Plotlyレポートを生成する。

Usage:
    pixi run python scripts/analyze_credit_intervals.py
"""

import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from math import log1p
from pathlib import Path
from statistics import mean, median, stdev

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.database import get_connection
from src.models import Role

JSON_DIR = Path("result/json")
REPORTS_DIR = Path("result/reports")

# ── Role category mapping (matches src/utils/role_groups.py) ──

ROLE_CATEGORY: dict[str, str] = {
    "director": "direction",
    "episode_director": "direction",
    "animation_director": "animation_supervision",
    "key_animator": "animation",
    "in_between": "animation",
    "layout": "animation",
    "character_designer": "design",
    "cgi_director": "technical",
    "photography_director": "technical",
    "background_art": "art",
    "finishing": "art",
    "sound_director": "sound",
    "music": "sound",
    "screenplay": "writing",
    "original_creator": "writing",
    "producer": "production",
    "production_manager": "production",
    "editing": "technical",
    "settings": "design",
}

# Exclude non-production roles
NON_PRODUCTION_ROLES = {"voice_actor", "theme_song", "adr", "special"}

CATEGORY_LABELS: dict[str, tuple[str, str]] = {
    "direction": ("演出・監督", "#f093fb"),
    "animation_supervision": ("作画監督", "#a0d2db"),
    "animation": ("動画・原画", "#06D6A0"),
    "design": ("デザイン", "#FFD166"),
    "technical": ("技術・撮影", "#667eea"),
    "art": ("美術", "#fda085"),
    "sound": ("音響", "#EF476F"),
    "writing": ("脚本・原作", "#78C4D4"),
    "production": ("制作", "#F72585"),
}

CAREER_YEAR_BINS = [
    (1, 3, "1-3年目"),
    (4, 6, "4-6年目"),
    (7, 10, "7-10年目"),
    (11, 15, "11-15年目"),
    (16, 20, "16-20年目"),
    (21, 30, "21-30年目"),
    (31, 999, "31年目+"),
]


# ── Shared HTML helpers (from generate_all_reports.py) ──

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

.chart-guide {
    background: rgba(160,210,219,0.08);
    border-left: 3px solid rgba(160,210,219,0.4);
    border-radius: 0 8px 8px 0;
    padding: 0.8rem 1.2rem; margin: 0.5rem 0 1rem;
    font-size: 0.85rem; color: #a0a0c0;
}

.disclaimer-block {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px; padding: 1.5rem;
    margin-top: 2rem; font-size: 0.85rem; color: #808090;
}
.disclaimer-block h3 { color: #a0a0c0; margin-bottom: 0.5rem; }

footer {
    text-align: center; padding: 2rem;
    color: rgba(255,255,255,0.3); font-size: 0.8rem;
}

.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }
@media (max-width: 900px) { .two-col { grid-template-columns: 1fr; } }
"""

DISCLAIMER = (
    "本レポートのスコアは公開クレジットデータに基づくネットワーク上の位置を定量化したものであり、"
    "個人の能力や技量を評価するものではありません。数値の低さは「能力不足」を意味しません。"
    " / Scores quantify network position from public credit data and do not assess individual "
    "ability. A low score does not imply lack of skill."
)


def plotly_div_safe(fig: go.Figure, div_id: str, height: int = 500) -> str:
    """Plotlyチャートを安全に埋め込み."""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0.2)",
        font=dict(color="#c0c0d0"),
        height=height,
        margin=dict(l=60, r=30, t=50, b=50),
    )
    chart_json = fig.to_json()
    return (
        f'<div class="chart-container" id="{div_id}" style="height:{height}px;"></div>\n'
        f"<script>Plotly.newPlot('{div_id}', "
        f"...function(){{ var d={chart_json}; return [d.data, d.layout]; }}()"
        f");</script>\n"
    )


def plotly_div(fig: go.Figure, div_id: str, height: int = 500) -> str:
    """Plotlyチャートをdivとして埋め込み (simple version)."""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0.2)",
        font=dict(color="#c0c0d0"),
        height=height,
        margin=dict(l=60, r=30, t=50, b=50),
    )
    return fig.to_html(full_html=False, include_plotlyjs=False, div_id=div_id)


def chart_guide(text: str) -> str:
    """チャート読み方ガイド."""
    return f'<div class="chart-guide"><strong>チャートの見方:</strong> {text}</div>'


def wrap_html(title: str, subtitle: str, body: str) -> str:
    """共通HTMLテンプレート."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    disclaimer_html = (
        '<div class="disclaimer-block">'
        "<h3>免責事項 (Disclaimer)</h3>"
        f"<p>{DISCLAIMER}</p>"
        "</div>"
    )
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
{body}
{disclaimer_html}
<footer>
    <p>Animetor Eval パイプライン分析により自動生成</p>
</footer>
</div>
</div>
</body>
</html>"""


def fmt(n: float, digits: int = 2) -> str:
    """数値フォーマット."""
    if isinstance(n, int):
        return f"{n:,}"
    return f"{n:.{digits}f}"


def _career_year_bin(cy: int) -> str:
    """キャリア年数をビンに振り分け."""
    for lo, hi, label in CAREER_YEAR_BINS:
        if lo <= cy <= hi:
            return label
    return "31年目+"


# ============================================================
# Data loading
# ============================================================


def load_credit_data(conn: sqlite3.Connection) -> list[dict]:
    """クレジットデータをアニメ年度付きでロード."""
    rows = conn.execute(
        """
        SELECT c.person_id, c.anime_id, c.role, a.year
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE a.year IS NOT NULL
          AND c.role NOT IN ('voice_actor', 'theme_song', 'adr', 'special')
        ORDER BY c.person_id, a.year
        """
    ).fetchall()
    return [dict(r) for r in rows]


def load_person_names(conn: sqlite3.Connection) -> dict[str, str]:
    """person_id → name_ja mapping."""
    rows = conn.execute("SELECT id, name_ja, name_en FROM persons").fetchall()
    return {r["id"]: (r["name_ja"] or r["name_en"] or r["id"]) for r in rows}


# ============================================================
# Interval computation
# ============================================================


def compute_intervals(credits: list[dict]) -> dict:
    """クレジット登場間隔を計算.

    Returns:
        {
            "overall": {person_id: [intervals]},
            "by_category": {category: {person_id: [intervals]}},
            "person_first_year": {person_id: first_year},
            "person_years": {person_id: sorted_list_of_active_years},
            "person_categories": {person_id: set_of_categories},
            "person_role_years": {person_id: {category: sorted_years}},
        }
    """
    # Group credits by person
    person_credits: dict[str, list[dict]] = defaultdict(list)
    for c in credits:
        person_credits[c["person_id"]].append(c)

    overall_intervals: dict[str, list[int]] = {}
    category_intervals: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    person_first_year: dict[str, int] = {}
    person_years: dict[str, list[int]] = {}
    person_categories: dict[str, set] = defaultdict(set)
    person_role_years: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))

    for pid, creds in person_credits.items():
        # Overall intervals: unique years for this person
        years = sorted({c["year"] for c in creds})
        if len(years) < 2:
            continue

        person_first_year[pid] = years[0]
        person_years[pid] = years

        # Overall intervals
        intervals = [years[i + 1] - years[i] for i in range(len(years) - 1)]
        overall_intervals[pid] = intervals

        # Per-category intervals
        cat_years: dict[str, set[int]] = defaultdict(set)
        for c in creds:
            cat = ROLE_CATEGORY.get(c["role"], "other")
            if cat == "other":
                continue
            cat_years[cat].add(c["year"])
            person_categories[pid].add(cat)

        for cat, yrs in cat_years.items():
            syrs = sorted(yrs)
            person_role_years[pid][cat] = syrs
            if len(syrs) >= 2:
                cat_ivs = [syrs[i + 1] - syrs[i] for i in range(len(syrs) - 1)]
                category_intervals[cat][pid] = cat_ivs

    return {
        "overall": overall_intervals,
        "by_category": dict(category_intervals),
        "person_first_year": person_first_year,
        "person_years": person_years,
        "person_categories": dict(person_categories),
        "person_role_years": dict(person_role_years),
    }


def compute_career_year_intervals(
    overall: dict[str, list[int]],
    person_years: dict[str, list[int]],
    person_first_year: dict[str, int],
) -> dict[str, list[int]]:
    """キャリア年数ビン別の間隔を計算.

    Returns:
        {bin_label: [all intervals in that career year range]}
    """
    bin_intervals: dict[str, list[int]] = {label: [] for _, _, label in CAREER_YEAR_BINS}

    for pid, years in person_years.items():
        fy = person_first_year[pid]
        for i in range(len(years) - 1):
            interval = years[i + 1] - years[i]
            career_year = years[i] - fy + 1  # 1-indexed
            bin_label = _career_year_bin(career_year)
            bin_intervals[bin_label].append(interval)

    return bin_intervals


def compute_category_career_matrix(
    category_intervals: dict[str, dict[str, list[int]]],
    person_role_years: dict[str, dict[str, list[int]]],
    person_first_year: dict[str, int],
) -> dict[str, dict[str, list[int]]]:
    """職能 × キャリア年数のマトリクス.

    Returns:
        {category: {bin_label: [intervals]}}
    """
    matrix: dict[str, dict[str, list[int]]] = {}

    for cat in CATEGORY_LABELS:
        matrix[cat] = {label: [] for _, _, label in CAREER_YEAR_BINS}

    for cat, pid_intervals in category_intervals.items():
        if cat not in matrix:
            continue
        for pid, intervals in pid_intervals.items():
            fy = person_first_year.get(pid)
            if fy is None:
                continue
            role_years = person_role_years.get(pid, {}).get(cat, [])
            for i, iv in enumerate(intervals):
                if i < len(role_years):
                    cy = role_years[i] - fy + 1
                    bl = _career_year_bin(cy)
                    matrix[cat][bl].append(iv)

    return matrix


# ============================================================
# Report generation
# ============================================================


def generate_report(conn: sqlite3.Connection) -> None:
    """メインレポート生成."""
    print("Loading credit data...")
    credits = load_credit_data(conn)
    print(f"  Loaded {len(credits):,} credits")

    print("Computing intervals...")
    result = compute_intervals(credits)
    overall = result["overall"]
    by_category = result["by_category"]
    person_first_year = result["person_first_year"]
    person_years = result["person_years"]
    person_role_years = result["person_role_years"]

    career_intervals = compute_career_year_intervals(
        overall, person_years, person_first_year
    )
    cat_career_matrix = compute_category_career_matrix(
        by_category, person_role_years, person_first_year
    )

    # Flatten all intervals for overall stats
    all_intervals = [iv for ivs in overall.values() for iv in ivs]
    print(f"  {len(overall):,} persons with 2+ active years")
    print(f"  {len(all_intervals):,} total intervals")

    body = ""

    # ================================================================
    # Section 1: Overview statistics
    # ================================================================
    body += '<div class="card">\n'
    body += "<h2>1. 概要統計</h2>\n"
    body += "<p>クレジットの登場間隔＝ある人物の連続する活動年の差（年）。"
    body += "間隔が短い＝安定して毎年クレジットに登場。間隔が長い＝活動休止期間あり。</p>\n"

    body += '<div class="stats-grid">\n'
    stats = [
        (f"{len(overall):,}", "分析対象者数"),
        (f"{len(all_intervals):,}", "間隔データ数"),
        (f"{fmt(mean(all_intervals))}年", "平均間隔"),
        (f"{fmt(median(all_intervals))}年", "中央値間隔"),
        (f"{fmt(stdev(all_intervals))}年", "標準偏差"),
        (f"{sum(1 for iv in all_intervals if iv == 1):,}", "連続年活動 (1年間隔)"),
    ]
    for val, lbl in stats:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{lbl}</div></div>\n'
    body += "</div>\n"

    # Overall distribution histogram
    fig1 = go.Figure()
    # Cap at 15 for readability
    capped = [min(iv, 15) for iv in all_intervals]
    fig1.add_trace(go.Histogram(
        x=capped, nbinsx=15,
        marker_color="#f093fb",
        hovertemplate="間隔 %{x}年: %{y:,}件<extra></extra>",
    ))
    fig1.update_layout(
        title="Chart 1. クレジット登場間隔の全体分布",
        xaxis_title="間隔（年）",
        yaxis_title="件数",
        xaxis=dict(dtick=1),
        bargap=0.1,
    )
    body += plotly_div(fig1, "chart_overall_hist", 400)
    body += chart_guide(
        "横軸は連続するクレジット間の年数。1年＝毎年活動。"
        "15年以上は「15+」にまとめています。ピークが1年に集中するほど業界定着率が高い。"
    )
    body += "</div>\n"

    # ================================================================
    # Section 2: By role category (box plot)
    # ================================================================
    body += '<div class="card">\n'
    body += "<h2>2. 職能別の登場間隔</h2>\n"
    body += "<p>職能カテゴリごとの間隔分布。演出・監督は作品単位での関与が多く間隔が長い傾向、"
    body += "動画・原画は毎クール参加できるため間隔が短い傾向があります。</p>\n"

    # Summary table
    body += "<table><thead><tr>"
    body += "<th>職能</th><th>人数</th><th>間隔数</th><th>平均(年)</th><th>中央値(年)</th><th>標準偏差</th><th>連続率</th>"
    body += "</tr></thead><tbody>\n"

    cat_order = []
    for cat in CATEGORY_LABELS:
        if cat not in by_category:
            continue
        ivs = [iv for person_ivs in by_category[cat].values() for iv in person_ivs]
        if len(ivs) < 5:
            continue
        cat_order.append(cat)
        label, color = CATEGORY_LABELS[cat]
        n_persons = len(by_category[cat])
        n_ivs = len(ivs)
        m = mean(ivs)
        med = median(ivs)
        sd = stdev(ivs) if len(ivs) > 1 else 0.0
        consec_rate = sum(1 for iv in ivs if iv == 1) / n_ivs * 100
        body += f"<tr><td>{label}</td><td>{n_persons:,}</td><td>{n_ivs:,}</td>"
        body += f"<td>{fmt(m)}</td><td>{fmt(med)}</td><td>{fmt(sd)}</td>"
        body += f"<td>{fmt(consec_rate, 1)}%</td></tr>\n"
    body += "</tbody></table>\n"

    # Box plot
    fig2 = go.Figure()
    for cat in cat_order:
        ivs = [iv for person_ivs in by_category[cat].values() for iv in person_ivs]
        label, color = CATEGORY_LABELS[cat]
        fig2.add_trace(go.Box(
            y=ivs, name=label,
            marker_color=color,
            boxmean="sd",
            hovertemplate=f"{label}<br>間隔: %{{y}}年<extra></extra>",
        ))
    fig2.update_layout(
        title="Chart 2. 職能別クレジット登場間隔（箱ひげ図）",
        yaxis_title="間隔（年）",
        yaxis=dict(range=[0, 15]),
        showlegend=False,
    )
    body += plotly_div(fig2, "chart_cat_box", 500)
    body += chart_guide(
        "箱の中央線＝中央値、箱＝四分位範囲(IQR)、ひげ＝1.5×IQR、ダイヤ＝平均値。"
        "箱が低い位置にあるほど安定した連続活動を示します。"
    )

    # Violin plot
    fig2b = go.Figure()
    for cat in cat_order:
        ivs = [min(iv, 15) for person_ivs in by_category[cat].values() for iv in person_ivs]
        label, color = CATEGORY_LABELS[cat]
        fig2b.add_trace(go.Violin(
            y=ivs, name=label,
            marker_color=color,
            box_visible=True,
            meanline_visible=True,
            hovertemplate=f"{label}<br>間隔: %{{y}}年<extra></extra>",
        ))
    fig2b.update_layout(
        title="Chart 3. 職能別クレジット登場間隔（バイオリン図）",
        yaxis_title="間隔（年）",
        showlegend=False,
    )
    body += plotly_div(fig2b, "chart_cat_violin", 500)
    body += chart_guide(
        "バイオリンの幅＝そのY値での密度。中央の箱＝四分位範囲。白い線＝平均値。"
        "裾野が広いほどばらつきが大きく、不安定な活動パターンを示します。"
    )
    body += "</div>\n"

    # ================================================================
    # Section 3: By career year
    # ================================================================
    body += '<div class="card">\n'
    body += "<h2>3. キャリア年数別の登場間隔</h2>\n"
    body += "<p>デビュー年からの経過年数で分類した間隔の変化。"
    body += "キャリア初期は連続して仕事を得る（低間隔）か、業界を離れる（高間隔）に二極化。"
    body += "中堅以降は安定する傾向があります。</p>\n"

    # Summary table
    body += "<table><thead><tr>"
    body += "<th>キャリア年数</th><th>間隔数</th><th>平均(年)</th><th>中央値(年)</th><th>連続率</th>"
    body += "</tr></thead><tbody>\n"

    bin_labels_ordered = [label for _, _, label in CAREER_YEAR_BINS]
    for bl in bin_labels_ordered:
        ivs = career_intervals.get(bl, [])
        if not ivs:
            continue
        m = mean(ivs)
        med = median(ivs)
        cr = sum(1 for iv in ivs if iv == 1) / len(ivs) * 100
        body += f"<tr><td>{bl}</td><td>{len(ivs):,}</td>"
        body += f"<td>{fmt(m)}</td><td>{fmt(med)}</td>"
        body += f"<td>{fmt(cr, 1)}%</td></tr>\n"
    body += "</tbody></table>\n"

    # Box plot by career year
    fig3 = go.Figure()
    colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#667eea", "#fda085", "#EF476F"]
    for i, bl in enumerate(bin_labels_ordered):
        ivs = career_intervals.get(bl, [])
        if not ivs:
            continue
        fig3.add_trace(go.Box(
            y=ivs, name=bl,
            marker_color=colors[i % len(colors)],
            boxmean="sd",
        ))
    fig3.update_layout(
        title="Chart 4. キャリア年数別の登場間隔",
        yaxis_title="間隔（年）",
        yaxis=dict(range=[0, 15]),
        showlegend=False,
    )
    body += plotly_div(fig3, "chart_career_box", 450)
    body += chart_guide(
        "キャリア年数が進むにつれて間隔の分布がどう変わるか。"
        "中堅以降で中央値が安定する（＝定着した人は安定して仕事がある）一方、"
        "外れ値（長期休止）も増える傾向が見られます。"
    )

    # Mean interval trend line
    fig3b = go.Figure()
    means = []
    medians = []
    xs = []
    for bl in bin_labels_ordered:
        ivs = career_intervals.get(bl, [])
        if not ivs:
            continue
        xs.append(bl)
        means.append(mean(ivs))
        medians.append(median(ivs))

    fig3b.add_trace(go.Scatter(
        x=xs, y=means, mode="lines+markers", name="平均",
        line=dict(color="#f093fb", width=3),
        marker=dict(size=10),
    ))
    fig3b.add_trace(go.Scatter(
        x=xs, y=medians, mode="lines+markers", name="中央値",
        line=dict(color="#a0d2db", width=3, dash="dash"),
        marker=dict(size=10),
    ))
    fig3b.update_layout(
        title="Chart 5. キャリア年数別の平均・中央値間隔の推移",
        yaxis_title="間隔（年）",
        legend=dict(orientation="h", y=1.1),
    )
    body += plotly_div(fig3b, "chart_career_trend", 400)
    body += chart_guide(
        "平均値と中央値の乖離が大きいほど、分布が歪んでいる（一部の人が長期休止している）ことを示します。"
    )
    body += "</div>\n"

    # ================================================================
    # Section 4: Category × Career Year heatmap
    # ================================================================
    body += '<div class="card">\n'
    body += "<h2>4. 職能 × キャリア年数の間隔ヒートマップ</h2>\n"
    body += "<p>職能別・キャリア年数別のクレジット登場間隔の中央値をヒートマップで表示。"
    body += "色が濃いほど間隔が長い（活動が不安定）。</p>\n"

    # Build matrix
    cats_with_data = [c for c in CATEGORY_LABELS if c in by_category]
    z_matrix = []
    y_labels = []
    for cat in cats_with_data:
        row = []
        label, _ = CATEGORY_LABELS[cat]
        y_labels.append(label)
        for bl in bin_labels_ordered:
            ivs = cat_career_matrix.get(cat, {}).get(bl, [])
            if ivs:
                row.append(round(median(ivs), 2))
            else:
                row.append(None)
        z_matrix.append(row)

    fig4 = go.Figure(data=go.Heatmap(
        z=z_matrix,
        x=bin_labels_ordered,
        y=y_labels,
        colorscale=[
            [0, "#06D6A0"],
            [0.3, "#FFD166"],
            [0.6, "#f093fb"],
            [1.0, "#EF476F"],
        ],
        colorbar=dict(title="中央値(年)"),
        hovertemplate="%{y} / %{x}<br>中央値間隔: %{z}年<extra></extra>",
    ))
    fig4.update_layout(
        title="Chart 6. 職能 × キャリア年数 間隔ヒートマップ（中央値）",
        xaxis_title="キャリア年数",
        yaxis=dict(autorange="reversed"),
    )
    body += plotly_div(fig4, "chart_heatmap", 450)
    body += chart_guide(
        "緑＝間隔が短い（安定）。赤＝間隔が長い（不安定）。"
        "空白セル＝データ不足。"
        "動画・原画は全キャリアを通じて安定傾向、演出・監督はキャリア初期に間隔が長い傾向があります。"
    )
    body += "</div>\n"

    # ================================================================
    # Section 5: Hiatus analysis (intervals >= 3 years)
    # ================================================================
    body += '<div class="card">\n'
    body += "<h2>5. 活動休止（ブランク）分析</h2>\n"
    body += "<p>3年以上のクレジット空白期間を「休止」と定義し、その発生パターンを分析。</p>\n"

    hiatus_threshold = 3
    hiatus_intervals = [iv for iv in all_intervals if iv >= hiatus_threshold]
    no_hiatus = [iv for iv in all_intervals if iv < hiatus_threshold]
    persons_with_hiatus = set()
    for pid, ivs in overall.items():
        if any(iv >= hiatus_threshold for iv in ivs):
            persons_with_hiatus.add(pid)

    body += '<div class="stats-grid">\n'
    h_stats = [
        (f"{len(hiatus_intervals):,}", f"休止回数（{hiatus_threshold}年+）"),
        (f"{fmt(len(hiatus_intervals)/len(all_intervals)*100, 1)}%", "全間隔に占める割合"),
        (f"{len(persons_with_hiatus):,}", "休止経験者数"),
        (f"{fmt(len(persons_with_hiatus)/len(overall)*100, 1)}%", "全対象者に占める割合"),
    ]
    for val, lbl in h_stats:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{lbl}</div></div>\n'
    body += "</div>\n"

    # Hiatus by category
    body += "<h3>職能別の休止率</h3>\n"
    body += "<table><thead><tr>"
    body += "<th>職能</th><th>休止回数</th><th>全間隔</th><th>休止率</th><th>平均休止期間</th>"
    body += "</tr></thead><tbody>\n"
    for cat in cat_order:
        ivs = [iv for person_ivs in by_category[cat].values() for iv in person_ivs]
        h_ivs = [iv for iv in ivs if iv >= hiatus_threshold]
        if not ivs:
            continue
        label, _ = CATEGORY_LABELS[cat]
        rate = len(h_ivs) / len(ivs) * 100
        avg_h = mean(h_ivs) if h_ivs else 0
        body += f"<tr><td>{label}</td><td>{len(h_ivs):,}</td><td>{len(ivs):,}</td>"
        body += f"<td>{fmt(rate, 1)}%</td><td>{fmt(avg_h)}年</td></tr>\n"
    body += "</tbody></table>\n"

    # Hiatus timing by career year
    fig5 = go.Figure()
    hiatus_by_bin: dict[str, int] = defaultdict(int)
    total_by_bin: dict[str, int] = defaultdict(int)
    for pid, years in person_years.items():
        fy = person_first_year[pid]
        for i in range(len(years) - 1):
            iv = years[i + 1] - years[i]
            cy = years[i] - fy + 1
            bl = _career_year_bin(cy)
            total_by_bin[bl] += 1
            if iv >= hiatus_threshold:
                hiatus_by_bin[bl] += 1

    h_rates = []
    h_xs = []
    for bl in bin_labels_ordered:
        if total_by_bin.get(bl, 0) > 0:
            h_xs.append(bl)
            h_rates.append(hiatus_by_bin.get(bl, 0) / total_by_bin[bl] * 100)

    fig5.add_trace(go.Bar(
        x=h_xs, y=h_rates,
        marker_color="#EF476F",
        hovertemplate="%{x}<br>休止率: %{y:.1f}%<extra></extra>",
    ))
    fig5.update_layout(
        title="Chart 7. キャリア年数別の休止発生率（3年以上のブランク）",
        yaxis_title="休止率（%）",
        bargap=0.3,
    )
    body += plotly_div(fig5, "chart_hiatus_career", 400)
    body += chart_guide(
        "各キャリア年数帯で、間隔が3年以上になる確率。"
        "初期（1-3年目）の休止率が高い場合、早期離職が多いことを示唆します。"
    )
    body += "</div>\n"

    # ================================================================
    # Section 6: Activity density (credits per active year)
    # ================================================================
    body += '<div class="card">\n'
    body += "<h2>6. 活動密度分析</h2>\n"
    body += "<p>活動密度＝クレジット数 ÷ キャリアスパン年数。密度が高い＝休止なく毎年活動。</p>\n"

    # Compute density per person per category
    person_density: dict[str, float] = {}
    for pid, years in person_years.items():
        span = years[-1] - years[0] + 1
        if span > 0:
            person_density[pid] = len(years) / span  # active years / span

    # Density by category
    fig6 = go.Figure()
    for cat in cat_order:
        persons_in_cat = list(by_category[cat].keys())
        densities = []
        for pid in persons_in_cat:
            role_yrs = person_role_years.get(pid, {}).get(cat, [])
            if len(role_yrs) >= 2:
                span = role_yrs[-1] - role_yrs[0] + 1
                if span > 0:
                    densities.append(len(role_yrs) / span)
        if not densities:
            continue
        label, color = CATEGORY_LABELS[cat]
        fig6.add_trace(go.Violin(
            y=densities, name=label,
            marker_color=color,
            box_visible=True,
            meanline_visible=True,
        ))
    fig6.update_layout(
        title="Chart 8. 職能別の活動密度（活動年/スパン年）",
        yaxis_title="活動密度 (0-1)",
        yaxis=dict(range=[0, 1.05]),
        showlegend=False,
    )
    body += plotly_div(fig6, "chart_density_cat", 450)
    body += chart_guide(
        "密度1.0＝スパン中毎年活動。0.5＝2年に1回。"
        "動画・原画は密度が高い（毎年仕事がある）傾向。演出・監督は密度にばらつきがある傾向。"
    )

    # Density vs career span scatter
    fig6b = go.Figure()
    for cat in cat_order:
        persons_in_cat = list(by_category[cat].keys())
        spans = []
        densities = []
        for pid in persons_in_cat:
            role_yrs = person_role_years.get(pid, {}).get(cat, [])
            if len(role_yrs) >= 2:
                span = role_yrs[-1] - role_yrs[0] + 1
                if span > 0:
                    spans.append(span)
                    densities.append(len(role_yrs) / span)
        if not spans:
            continue
        label, color = CATEGORY_LABELS[cat]
        fig6b.add_trace(go.Scatter(
            x=spans, y=densities,
            mode="markers",
            name=label,
            marker=dict(color=color, size=4, opacity=0.5),
            hovertemplate=f"{label}<br>スパン: %{{x}}年<br>密度: %{{y:.2f}}<extra></extra>",
        ))
    fig6b.update_layout(
        title="Chart 9. キャリアスパン vs 活動密度",
        xaxis_title="キャリアスパン（年）",
        yaxis_title="活動密度",
        yaxis=dict(range=[0, 1.05]),
        legend=dict(orientation="h", y=1.1),
    )
    body += plotly_div(fig6b, "chart_span_density", 450)
    body += chart_guide(
        "右上＝長期かつ毎年活動（安定したベテラン）。左上＝短期だが毎年活動（集中期）。"
        "右下＝長期だが活動がまばら（不安定または兼業）。"
    )
    body += "</div>\n"

    # ================================================================
    # Section 7: Year-over-year intervals (temporal trend)
    # ================================================================
    body += '<div class="card">\n'
    body += "<h2>7. 時代別の登場間隔トレンド</h2>\n"
    body += "<p>各年に発生した間隔の平均値の推移。業界全体の雇用安定性のトレンドが見えます。</p>\n"

    year_intervals: dict[int, list[int]] = defaultdict(list)
    for pid, years in person_years.items():
        for i in range(len(years) - 1):
            iv = years[i + 1] - years[i]
            year_intervals[years[i]].append(iv)

    trend_years = sorted(y for y in year_intervals if 1985 <= y <= 2025)
    trend_means = [mean(year_intervals[y]) for y in trend_years]
    trend_medians = [median(year_intervals[y]) for y in trend_years]
    trend_counts = [len(year_intervals[y]) for y in trend_years]

    fig7 = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.08,
    )

    fig7.add_trace(go.Scatter(
        x=trend_years, y=trend_means,
        mode="lines", name="平均間隔",
        line=dict(color="#f093fb", width=2),
    ), row=1, col=1)
    fig7.add_trace(go.Scatter(
        x=trend_years, y=trend_medians,
        mode="lines", name="中央値間隔",
        line=dict(color="#a0d2db", width=2, dash="dash"),
    ), row=1, col=1)
    fig7.add_trace(go.Bar(
        x=trend_years, y=trend_counts,
        name="データ数",
        marker_color="rgba(240,147,251,0.3)",
    ), row=2, col=1)

    fig7.update_layout(
        title="Chart 10. 年別の平均クレジット登場間隔と件数",
        legend=dict(orientation="h", y=1.08),
    )
    fig7.update_yaxes(title_text="間隔（年）", row=1, col=1)
    fig7.update_yaxes(title_text="件数", row=2, col=1)
    body += plotly_div(fig7, "chart_year_trend", 550)
    body += chart_guide(
        "上段: 各年における間隔の平均・中央値の推移。下段: データ件数。"
        "近年の平均間隔が低下傾向であれば、業界が人材を安定して確保できていることを示します。"
    )
    body += "</div>\n"

    # ================================================================
    # Section 8: Individual role detail (per-role histograms)
    # ================================================================
    body += '<div class="card">\n'
    body += "<h2>8. 個別職種の間隔分布</h2>\n"
    body += "<p>主要な個別職種ごとの間隔分布をヒストグラムで比較。</p>\n"

    # Compute per-role intervals (not per-category, but per-role)
    role_intervals: dict[str, list[int]] = defaultdict(list)
    person_role_years_raw: dict[str, dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))
    for c in credits:
        role = c["role"]
        if role in NON_PRODUCTION_ROLES:
            continue
        person_role_years_raw[c["person_id"]][role].add(c["year"])

    for pid, role_yrs in person_role_years_raw.items():
        for role, yrs in role_yrs.items():
            syrs = sorted(yrs)
            for i in range(len(syrs) - 1):
                role_intervals[role].append(syrs[i + 1] - syrs[i])

    # Show top roles by data volume
    ROLE_JA: dict[str, str] = {
        "key_animator": "原画",
        "animation_director": "作画監督",
        "in_between": "動画",
        "episode_director": "演出",
        "director": "監督",
        "character_designer": "キャラクターデザイン",
        "layout": "レイアウト",
        "photography_director": "撮影監督",
        "screenplay": "脚本",
        "sound_director": "音響監督",
        "background_art": "背景美術",
        "finishing": "仕上げ",
        "cgi_director": "CGI監督",
        "producer": "プロデューサー",
        "production_manager": "制作進行",
        "music": "音楽",
        "original_creator": "原作",
        "editing": "編集",
        "settings": "設定",
        "special": "特殊",
        "voice_actor": "声優",
    }

    top_roles = sorted(
        [(r, ivs) for r, ivs in role_intervals.items() if len(ivs) >= 50],
        key=lambda x: -len(x[1])
    )[:12]

    if top_roles:
        n_cols = 3
        n_rows = (len(top_roles) + n_cols - 1) // n_cols
        fig8 = make_subplots(
            rows=n_rows, cols=n_cols,
            subplot_titles=[ROLE_JA.get(r, r) for r, _ in top_roles],
            vertical_spacing=0.08,
            horizontal_spacing=0.06,
        )
        palette = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166", "#667eea",
                    "#fda085", "#EF476F", "#78C4D4", "#F72585", "#4CC9F0",
                    "#7209B7", "#3A0CA3"]
        for idx, (role, ivs) in enumerate(top_roles):
            r = idx // n_cols + 1
            c_idx = idx % n_cols + 1
            capped_ivs = [min(iv, 10) for iv in ivs]
            fig8.add_trace(go.Histogram(
                x=capped_ivs, nbinsx=10,
                marker_color=palette[idx % len(palette)],
                hovertemplate="%{x}年: %{y:,}件<extra></extra>",
                showlegend=False,
            ), row=r, col=c_idx)

        fig8.update_layout(
            title="Chart 11. 個別職種別のクレジット登場間隔分布",
            height=250 * n_rows,
        )
        body += plotly_div(fig8, "chart_role_histograms", 250 * n_rows)
        body += chart_guide(
            "各職種の間隔分布。1年にピークがある＝毎年コンスタントに参加。"
            "2-3年にもう一つのピークがある＝プロジェクト単位での参加パターン。"
        )

    body += "</div>\n"

    # ================================================================
    # Export JSON
    # ================================================================
    json_output = {
        "overall_stats": {
            "n_persons": len(overall),
            "n_intervals": len(all_intervals),
            "mean_interval": round(mean(all_intervals), 3),
            "median_interval": round(median(all_intervals), 3),
            "stdev_interval": round(stdev(all_intervals), 3),
            "consecutive_rate": round(sum(1 for iv in all_intervals if iv == 1) / len(all_intervals), 4),
        },
        "by_category": {},
        "by_career_year": {},
        "by_role": {},
        "temporal_trend": {
            str(y): {"mean": round(mean(year_intervals[y]), 3),
                      "median": round(median(year_intervals[y]), 3),
                      "count": len(year_intervals[y])}
            for y in trend_years
        },
    }
    for cat in cat_order:
        ivs = [iv for person_ivs in by_category[cat].values() for iv in person_ivs]
        label, _ = CATEGORY_LABELS[cat]
        json_output["by_category"][cat] = {
            "label": label,
            "n_persons": len(by_category[cat]),
            "n_intervals": len(ivs),
            "mean": round(mean(ivs), 3),
            "median": round(median(ivs), 3),
            "stdev": round(stdev(ivs), 3) if len(ivs) > 1 else 0,
            "consecutive_rate": round(sum(1 for iv in ivs if iv == 1) / len(ivs), 4),
            "hiatus_rate": round(sum(1 for iv in ivs if iv >= 3) / len(ivs), 4),
        }
    for bl in bin_labels_ordered:
        ivs = career_intervals.get(bl, [])
        if ivs:
            json_output["by_career_year"][bl] = {
                "n_intervals": len(ivs),
                "mean": round(mean(ivs), 3),
                "median": round(median(ivs), 3),
                "consecutive_rate": round(sum(1 for iv in ivs if iv == 1) / len(ivs), 4),
            }
    for role, ivs in sorted(role_intervals.items(), key=lambda x: -len(x[1])):
        if len(ivs) >= 10:
            json_output["by_role"][role] = {
                "label": ROLE_JA.get(role, role),
                "n_intervals": len(ivs),
                "mean": round(mean(ivs), 3),
                "median": round(median(ivs), 3),
            }

    JSON_DIR.mkdir(parents=True, exist_ok=True)
    json_path = JSON_DIR / "credit_intervals.json"
    with open(json_path, "w") as f:
        json.dump(json_output, f, ensure_ascii=False, indent=2)
    print(f"  -> {json_path}")

    # ================================================================
    # Write HTML
    # ================================================================
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    html = wrap_html(
        "クレジット登場間隔分析",
        "職能別・キャリア年数別の活動間隔パターン",
        body,
    )
    out_path = REPORTS_DIR / "credit_intervals.html"
    with open(out_path, "w") as f:
        f.write(html)
    print(f"  -> {out_path}")


def main():
    """エントリーポイント."""
    print("=" * 60)
    print("Credit Interval Analysis — クレジット登場間隔分析")
    print("=" * 60)

    conn = get_connection()
    try:
        generate_report(conn)
    finally:
        conn.close()

    print("\nDone!")


if __name__ == "__main__":
    main()
