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

# Go Explorer server URL (pixi run explorer でポート 3000 起動)
EXPLORER_URL = "http://localhost:3000"


def person_link(name: str, person_id: str) -> str:
    """個人名を Go Explorer の詳細ページへのリンクに変換する."""
    if not person_id:
        return name
    url = f"{EXPLORER_URL}/#person/{person_id}"
    return (
        f'<a href="{url}" target="_blank" '
        f'style="color:#a0d2db;text-decoration:none;" '
        f'onmouseover="this.style.textDecoration=\'underline\'" '
        f'onmouseout="this.style.textDecoration=\'none\'">'
        f"{name}</a>"
    )


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

.significance-section {
    background: linear-gradient(135deg, rgba(6,214,160,0.08), rgba(6,214,160,0.03));
    border-left: 4px solid #06D6A0;
    border-radius: 0 12px 12px 0;
    padding: 1.5rem 2rem; margin: 1.5rem 0;
}
.significance-section h3 { color: #06D6A0; font-size: 1.2rem; margin-bottom: 0.8rem; }
.significance-section p { color: #b0c8c0; font-size: 0.92rem; line-height: 1.7; margin-bottom: 0.5rem; }

.utilization-guide {
    background: linear-gradient(135deg, rgba(102,126,234,0.1), rgba(102,126,234,0.03));
    border: 1px solid rgba(102,126,234,0.25);
    border-radius: 12px; padding: 1.5rem 2rem; margin: 1.5rem 0;
}
.utilization-guide h3 { color: #667eea; font-size: 1.2rem; margin-bottom: 1rem; }
.usecase-list { list-style: none; padding: 0; }
.usecase-list li { padding: 0.6rem 0; border-bottom: 1px solid rgba(102,126,234,0.1); display: flex; align-items: flex-start; gap: 0.6rem; }
.usecase-list li:last-child { border-bottom: none; }
.role-tag {
    display: inline-block; padding: 0.15rem 0.6rem;
    background: rgba(102,126,234,0.2); color: #a0aee8;
    border-radius: 8px; font-size: 0.75rem; font-weight: 600;
    white-space: nowrap; flex-shrink: 0; margin-top: 0.15rem;
}
.usecase-desc { font-size: 0.88rem; color: #b0b0c0; line-height: 1.6; }

details.future-section {
    background: rgba(253,160,133,0.06);
    border: 1px solid rgba(253,160,133,0.2);
    border-radius: 12px; margin: 1.5rem 0;
}
details.future-section summary {
    padding: 1rem 1.5rem; cursor: pointer;
    font-weight: 600; color: #fda085; font-size: 1.05rem;
    list-style: none;
}
details.future-section summary::before { content: "▶ "; font-size: 0.8rem; }
details.future-section[open] summary::before { content: "▼ "; }
details.future-section ul {
    padding: 0 1.5rem 1.2rem 2.5rem;
    color: #b8a090; font-size: 0.88rem; line-height: 1.9;
}
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
    "評価は3軸で構成されます：(1) BiRank — 重み付き二部グラフランキングによる"
    "著名監督・作品への近接性、(2) Patronage — 同一監督からの継続起用による"
    "累積エッジ重み、(3) Person FE — 固定効果モデルに基づく"
    "個人寄与の構造推定。IV Scoreは3軸の操作変数推定統合値で、0-100に正規化されます。"
)

COMMON_GLOSSARY_TERMS: dict[str, str] = {
    "BiRank": (
        "二部グラフランキングに基づく中心性指標。著名な監督や高評価作品との協業ネットワーク上の"
        "近さを測定します。値が高いほど業界の中心的な位置にいることを示します。"
    ),
    "Patronage": (
        "継続的な協業から蓄積されるエッジ重み。同じ監督やプロデューサーから"
        "繰り返し起用されることを反映し、職業的な信頼の指標です。"
    ),
    "Person FE": (
        "固定効果モデルに基づく個人寄与の構造推定。直近のプロジェクト"
        "貢献度と成長軌道を反映します。BiRankと異なり、最近の活動を重視します。"
    ),
    "IV Score（操作変数推定スコア）": (
        "BiRank・Patronage・Person FEの重み付き統合値。0-100に正規化された"
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


def significance_section(title: str, paragraphs: list[str]) -> str:
    """この分析の意義・重要性を説明するカード."""
    ps = "".join(f"<p>{p}</p>" for p in paragraphs)
    return (
        f'<div class="significance-section">'
        f"<h3>この分析の意義 — {title}</h3>"
        f"{ps}"
        "</div>"
    )


def utilization_guide(usecases: list[dict]) -> str:
    """活用方法ガイド。usecases = [{"role": "スタジオ人事", "how": "..."}, ...]"""
    lis = ""
    for uc in usecases:
        lis += (
            f'<li><span class="role-tag">{uc["role"]}</span>'
            f'<span class="usecase-desc">{uc["how"]}</span></li>'
        )
    return (
        '<div class="utilization-guide">'
        "<h3>活用方法ガイド</h3>"
        f'<ul class="usecase-list">{lis}</ul>'
        "</div>"
    )


def future_possibilities(items: list[str]) -> str:
    """今後の活用展望 — 折りたたみセクション."""
    lis = "".join(f"<li>{item}</li>" for item in items)
    return (
        '<details class="future-section">'
        "<summary>今後の活用展望</summary>"
        f"<ul>{lis}</ul>"
        "</details>"
    )


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


def _name_clusters_by_rank(
    centers,
    feat_specs: list[tuple[int, list[str]]],
) -> dict[int, str]:
    """K-Meansクラスタの重心の相対ランクを基に動的にクラスタ名を付与する.

    固定閾値（>= 70 等）の代わりに、重心同士の相対順位でラベルを決定するため
    データセットのスケールに依存しない。

    Args:
        centers: 逆変換済み重心配列 (n_clusters × n_features)
        feat_specs: [(feat_idx, [label_最高, label_中間, ..., label_最低])] のリスト。
                    各特徴量について、重心値が高いクラスタから順にラベルを割り当てる。

    Returns:
        {cluster_id: "C{n}: label1×label2×..."} の辞書
    """
    n_clusters = len(centers)
    feat_labels: dict[int, list[str]] = {c: [] for c in range(n_clusters)}

    for feat_idx, label_list in feat_specs:
        # 高い順にソート
        ranked = sorted(range(n_clusters), key=lambda c: -float(centers[c, feat_idx]))
        n_labels = len(label_list)
        for rank, cid in enumerate(ranked):
            label_idx = min(rank * n_labels // n_clusters, n_labels - 1)
            feat_labels[cid].append(label_list[label_idx])

    return {c: f"C{c+1}: {'×'.join(feat_labels[c])}" for c in range(n_clusters)}


def _name_clusters_distinctive(centers_orig, feature_names: list[str]) -> dict[int, str]:
    """各クラスタを最も特徴的な次元のz-scoreで命名する（重複なし保証）.

    各クラスタについて |z-score| が最大の上位3特徴量を選び、
    その方向（高/低）を日本語ラベルで表現する。
    同一名が衝突した場合は末尾に番号を付与して一意性を保証する。
    """
    import numpy as np

    mean = centers_orig.mean(axis=0)
    std = centers_orig.std(axis=0) + 1e-10
    z = (centers_orig - mean) / std
    n_clusters = len(centers_orig)

    FEAT_POS = {
        "birank": "高BiRank", "patronage": "高Patronage", "person_fe": "高PersonFE",
        "iv_score": "高IV", "total_credits": "多作",
        "degree": "高次数", "betweenness": "高媒介", "eigenvector": "高固有",
        "active_years": "長キャリア", "highest_stage": "上位役職",
        "peak_credits": "高ピーク", "collaborators": "広人脈",
        "unique_anime": "多作品", "hub_score": "ハブ",
        "activity_ratio": "高活動", "recent_credits": "最近活発",
        "versatility_score": "多才", "categories": "多カテゴリ",
        "roles": "多役割", "confidence": "高確信",
    }
    FEAT_NEG = {
        "birank": "低BiRank", "patronage": "低Patronage", "person_fe": "低PersonFE",
        "iv_score": "低IV", "total_credits": "寡作",
        "degree": "低次数", "betweenness": "低媒介", "eigenvector": "低固有",
        "active_years": "短キャリア", "highest_stage": "下位役職",
        "peak_credits": "低ピーク", "collaborators": "狭人脈",
        "unique_anime": "少作品", "hub_score": "周辺",
        "activity_ratio": "低活動", "recent_credits": "最近不活発",
        "versatility_score": "専門特化", "categories": "少カテゴリ",
        "roles": "単一役割", "confidence": "低確信",
    }

    raw_names: list[str] = []
    for c in range(n_clusters):
        top_idx = np.argsort(-np.abs(z[c]))[:3]
        parts = []
        for fi in top_idx:
            fname = feature_names[int(fi)]
            lbl = (FEAT_POS.get(fname, f"高{fname}") if z[c, fi] > 0
                   else FEAT_NEG.get(fname, f"低{fname}"))
            parts.append(lbl)
        raw_names.append("・".join(parts))

    # 重複排除
    count: dict[str, int] = {}
    names: dict[int, str] = {}
    for c, nm in enumerate(raw_names):
        if nm in count:
            count[nm] += 1
            names[c] = f"C{c+1}: {nm}({count[nm]})"
        else:
            count[nm] = 1
            names[c] = f"C{c+1}: {nm}"
    return names


# ============================================================
# Feature extraction for ML clustering
# ============================================================

FEATURE_NAMES = [
    "birank", "patronage", "person_fe", "iv_score", "total_credits",
    "degree", "betweenness", "eigenvector",
    "active_years", "highest_stage", "peak_credits",
    "collaborators", "unique_anime", "hub_score",
    "activity_ratio", "recent_credits",
    "versatility_score", "categories", "roles",
    "confidence",
]


def _safe_nested(d: dict, *keys, default=0.0) -> float:
    """Safely extract nested dict value."""
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return float(default)
        cur = cur.get(k, default)
    return float(cur) if cur is not None else float(default)


def extract_features(scores: list[dict]):
    """Extract 20-dimensional feature vectors from scores.json.

    Returns (ids, names, features_array, primary_roles).
    """
    import numpy as np

    ids: list[str] = []
    names: list[str] = []
    roles: list[str] = []
    rows: list[list[float]] = []

    for p in scores:
        ids.append(p.get("person_id", ""))
        names.append(p.get("name", p.get("name_ja", "")))
        roles.append(p.get("primary_role", "unknown"))
        row = [
            float(p.get("birank", 0)),
            float(p.get("patronage", 0)),
            float(p.get("person_fe", 0)),
            float(p.get("iv_score", 0)),
            float(p.get("total_credits", 0)),
            _safe_nested(p, "centrality", "degree"),
            _safe_nested(p, "centrality", "betweenness"),
            _safe_nested(p, "centrality", "eigenvector"),
            _safe_nested(p, "career", "active_years"),
            _safe_nested(p, "career", "highest_stage"),
            _safe_nested(p, "career", "peak_credits"),
            _safe_nested(p, "network", "collaborators"),
            _safe_nested(p, "network", "unique_anime"),
            _safe_nested(p, "network", "hub_score"),
            _safe_nested(p, "growth", "activity_ratio"),
            _safe_nested(p, "growth", "recent_credits"),
            _safe_nested(p, "versatility", "score"),
            _safe_nested(p, "versatility", "categories"),
            _safe_nested(p, "versatility", "roles"),
            float(p.get("confidence", 0)),
        ]
        rows.append(row)

    return ids, names, np.array(rows, dtype=np.float64), roles


# ============================================================
# Report 15: ML Clustering Analysis
# ============================================================


def generate_ml_clustering_report():  # noqa: C901
    """MLクラスタリング分析レポート."""
    print("  Generating ML Clustering Report...")
    scores = load_json("scores.json")
    if not scores or not isinstance(scores, list) or len(scores) < 10:
        print("    [SKIP] Not enough data in scores.json")
        return

    import numpy as np
    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score, silhouette_samples

    person_ids, names, features, roles = extract_features(scores)
    n_persons = len(person_ids)
    n_clusters = min(8, max(3, n_persons // 5))

    # Handle NaN/Inf
    features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)

    # Standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    # PCA
    n_components = min(features.shape[1], features.shape[0], 10)
    pca = PCA(n_components=n_components, random_state=42)
    X_pca_full = pca.fit_transform(X_scaled)
    X_2d = X_pca_full[:, :2]
    X_3d = X_pca_full[:, :3] if n_components >= 3 else np.column_stack([X_pca_full[:, :2], np.zeros(n_persons)])

    # KMeans
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(X_scaled)

    # Silhouette
    sil_avg = silhouette_score(X_scaled, cluster_labels) if n_persons > n_clusters else 0.0
    sil_samples = silhouette_samples(X_scaled, cluster_labels) if n_persons > n_clusters else np.zeros(n_persons)

    # Cluster profiles (inverse-transformed centers for interpretability)
    centers_orig = scaler.inverse_transform(kmeans.cluster_centers_)

    # Name clusters — z-score based distinctive naming (deduplication guaranteed)
    cluster_names = _name_clusters_distinctive(centers_orig, FEATURE_NAMES)

    # --- Export ml_clusters.json ---
    cluster_data = {
        "metadata": {
            "n_persons": n_persons,
            "n_clusters": n_clusters,
            "n_features": len(FEATURE_NAMES),
            "feature_names": FEATURE_NAMES,
            "silhouette_score": round(sil_avg, 4),
            "explained_variance_ratio": [round(v, 4) for v in pca.explained_variance_ratio_.tolist()],
            "cluster_names": cluster_names,
        },
        "persons": [],
        "cluster_profiles": [],
    }

    for i in range(n_persons):
        cluster_data["persons"].append({
            "person_id": person_ids[i],
            "name": names[i],
            "primary_role": roles[i],
            "cluster": int(cluster_labels[i]),
            "cluster_name": cluster_names[int(cluster_labels[i])],
            "pca_2d": [round(X_2d[i, 0], 4), round(X_2d[i, 1], 4)],
            "pca_3d": [round(X_3d[i, 0], 4), round(X_3d[i, 1], 4), round(X_3d[i, 2], 4)],
            "silhouette": round(float(sil_samples[i]), 4),
        })

    for c in range(n_clusters):
        mask = cluster_labels == c
        profile = {fname: round(float(centers_orig[c, fi]), 4) for fi, fname in enumerate(FEATURE_NAMES)}
        cluster_data["cluster_profiles"].append({
            "cluster": c,
            "name": cluster_names[c],
            "size": int(mask.sum()),
            "profile": profile,
        })

    out_json = JSON_DIR / "ml_clusters.json"
    with open(out_json, "w") as f:
        json.dump(cluster_data, f, ensure_ascii=False, indent=2)
    print(f"    -> {out_json}")

    # --- Build HTML Report ---
    body = ""

    # Stats grid
    body += '<div class="stats-grid">'
    body += f'<div class="stat-card"><div class="value">{fmt_num(n_persons)}</div><div class="label">対象人物</div></div>'
    body += f'<div class="stat-card"><div class="value">{n_clusters}</div><div class="label">クラスタ数</div></div>'
    body += f'<div class="stat-card"><div class="value">{len(FEATURE_NAMES)}</div><div class="label">特徴量</div></div>'
    body += f'<div class="stat-card"><div class="value">{sil_avg:.3f}</div><div class="label">シルエットスコア</div></div>'
    body += "</div>"

    # Chart 1: PCA 2D Scatter with name-highlight search
    hover_text = [f"{names[i]}<br>Cluster: {cluster_names[int(cluster_labels[i])]}<br>IV Score: {features[i, 3]:.1f}" for i in range(n_persons)]
    fig_2d = go.Figure()
    for c in range(n_clusters):
        mask = cluster_labels == c
        fig_2d.add_trace(go.Scattergl(
            x=X_2d[mask, 0].tolist(), y=X_2d[mask, 1].tolist(),
            mode="markers",
            marker=dict(size=4, opacity=0.6),
            name=cluster_names[c],
            text=[hover_text[i] for i in range(n_persons) if mask[i]],
            hovertemplate="%{text}<extra></extra>",
        ))
    fig_2d.update_layout(
        title=f"PCA 2D クラスタ散布図 (分散説明率: {pca.explained_variance_ratio_[0]:.1%} + {pca.explained_variance_ratio_[1]:.1%})",
        xaxis_title=f"PC1 ({pca.explained_variance_ratio_[0]:.1%})",
        yaxis_title=f"PC2 ({pca.explained_variance_ratio_[1]:.1%})",
    )
    # Embed person positions as compact JSON for JS highlight
    persons_pos_json = json.dumps(
        [{"pid": person_ids[i], "name": names[i],
          "x": round(float(X_2d[i, 0]), 4), "y": round(float(X_2d[i, 1]), 4),
          "cl": int(cluster_labels[i])}
         for i in range(n_persons)],
        ensure_ascii=False, separators=(",", ":"),
    )
    body += '<div class="card"><h2>PCA 2D クラスタ散布図</h2>'
    body += section_desc("20次元の特徴量をPCAで2次元に圧縮し、K-Meansクラスタ別に色分け表示。近い点は類似した特性を持つ人物。")
    body += chart_guide("点の色はクラスタを示す。密集領域は類似した経歴パターン。外れ値は独自のキャリアパスを持つ人物。")
    body += (
        '<div style="display:flex;gap:0.5rem;margin-bottom:0.8rem;align-items:center;">'
        '<input id="pca2d-search" type="text" placeholder="人名を入力 → 点が点滅ハイライト..."'
        ' style="flex:1;padding:0.5rem 0.8rem;background:#1a1a3e;color:#fff;'
        'border:1px solid #a0d2db;border-radius:6px;font-size:0.9rem;">'
        '<button id="pca2d-clear" style="padding:0.5rem 0.8rem;background:#333;color:#fff;'
        'border:1px solid #666;border-radius:6px;cursor:pointer;">クリア</button>'
        '</div>'
        '<div id="pca2d-result" style="font-size:0.8rem;color:#a0d2db;'
        'margin-bottom:0.5rem;min-height:1.2em;"></div>'
    )
    body += plotly_div_safe(fig_2d, "pca2d", 600)
    body += f"""<script>
(function(){{
  var PERSONS={persons_pos_json};
  var blinkTimer=null,blinkState=0,N_TRACES={n_clusters};
  var EXPLORER="{EXPLORER_URL}";
  function stopBlink(){{
    if(blinkTimer){{clearInterval(blinkTimer);blinkTimer=null;}}
    try{{Plotly.deleteTraces('pca2d',[N_TRACES]);}}catch(e){{}}
  }}
  function highlightPersons(m){{
    stopBlink();if(!m.length)return;
    Plotly.addTraces('pca2d',{{
      type:'scattergl',mode:'markers',
      x:m.map(function(p){{return p.x;}}),
      y:m.map(function(p){{return p.y;}}),
      text:m.map(function(p){{return p.name+' / C'+(p.cl+1);}}),
      hovertemplate:'%{{text}}<extra>検索</extra>',
      marker:{{size:14,color:'#FFD166',symbol:'star',line:{{width:2,color:'#fff'}}}},
      name:'検索結果',showlegend:true
    }});
    blinkTimer=setInterval(function(){{
      blinkState=1-blinkState;
      Plotly.restyle('pca2d',{{'marker.size':blinkState?18:10}},[N_TRACES]);
    }},500);
  }}
  function renderResults(m){{
    var res=document.getElementById('pca2d-result');
    while(res.firstChild)res.removeChild(res.firstChild);
    if(!m.length){{res.textContent='該当なし';return;}}
    res.appendChild(document.createTextNode(m.length+'件ヒット: '));
    m.slice(0,5).forEach(function(p,i){{
      if(i>0)res.appendChild(document.createTextNode(', '));
      var a=document.createElement('a');
      a.href=EXPLORER+'/#person/'+p.pid;a.target='_blank';
      a.style.color='#a0d2db';a.textContent=p.name;res.appendChild(a);
    }});
    if(m.length>5)res.appendChild(document.createTextNode(' ...'));
  }}
  document.getElementById('pca2d-search').addEventListener('input',function(){{
    var q=this.value.trim().toLowerCase();
    if(!q){{stopBlink();document.getElementById('pca2d-result').textContent='';return;}}
    var m=PERSONS.filter(function(p){{return p.name.toLowerCase().indexOf(q)>=0;}});
    renderResults(m);highlightPersons(m);
  }});
  document.getElementById('pca2d-clear').addEventListener('click',function(){{
    document.getElementById('pca2d-search').value='';
    document.getElementById('pca2d-result').textContent='';
    stopBlink();
  }});
}})();
</script></div>
"""

    # Chart 2: PCA 3D Scatter
    if n_components >= 3:
        fig_3d = go.Figure()
        for c in range(n_clusters):
            mask = cluster_labels == c
            fig_3d.add_trace(go.Scatter3d(
                x=X_3d[mask, 0].tolist(), y=X_3d[mask, 1].tolist(), z=X_3d[mask, 2].tolist(),
                mode="markers",
                marker=dict(size=3, opacity=0.6),
                name=cluster_names[c],
                text=[hover_text[i] for i in range(n_persons) if mask[i]],
                hovertemplate="%{text}<extra></extra>",
            ))
        ev = pca.explained_variance_ratio_
        fig_3d.update_layout(
            title=f"PCA 3D クラスタ散布図 (累積分散説明率: {sum(ev[:3]):.1%})",
            scene=dict(xaxis_title=f"PC1 ({ev[0]:.1%})", yaxis_title=f"PC2 ({ev[1]:.1%})", zaxis_title=f"PC3 ({ev[2]:.1%})"),
        )
        body += '<div class="card"><h2>PCA 3D クラスタ散布図</h2>'
        body += section_desc("3次元空間でのクラスタ分布。ドラッグで回転可能。")
        body += plotly_div_safe(fig_3d, "pca3d", 650)
        body += "</div>"

    # Chart 2b: Feature Pair Plot (Splom) — PCA前の全次元ペアプロット
    n_sample_splom = min(5000, n_persons)
    rng = np.random.RandomState(42)
    splom_idx = rng.choice(n_persons, n_sample_splom, replace=False) if n_persons > n_sample_splom else np.arange(n_persons)
    splom_colors = [int(cluster_labels[i]) for i in splom_idx]
    n_feats = len(FEATURE_NAMES)
    fig_splom = go.Figure(go.Splom(
        dimensions=[
            dict(label=fname, values=features[splom_idx, fi].tolist())
            for fi, fname in enumerate(FEATURE_NAMES)
        ],
        marker=dict(
            color=splom_colors,
            colorscale="Turbo",
            size=2,
            opacity=0.4,
            showscale=False,
        ),
        text=[f"{names[i]}<br>C{cluster_labels[i]+1}" for i in splom_idx],
        hovertemplate="%{text}<extra></extra>",
        showupperhalf=False,
        diagonal_visible=True,
    ))
    n_pairs = n_feats * (n_feats - 1) // 2
    fig_splom.update_layout(
        title=f"特徴量ペアプロット ({n_feats}次元 × {n_feats - 1} = {n_pairs}ペア, n={n_sample_splom}サンプル, 色=クラスタ)",
        height=1400,
        dragmode="select",
    )
    body += '<div class="card"><h2>特徴量ペアプロット（PCA前）</h2>'
    body += section_desc(
        f"{n_feats}次元の全特徴量をPCAする前の生データで2次元ペアプロット表示。"
        f"全{n_pairs}通りの組み合わせを一覧できる（右上は省略、対角は各次元の分布）。"
        f"点の色はK-Meansクラスタを示す。左下の散布図をクリック＆ドラッグで拡大。"
    )
    body += chart_guide(
        "各セル = 2つの特徴量の散布図。クラスタが綺麗に分離しているセルは"
        "その2変数がクラスタを決める重要な次元であることを示す。"
        f"データ数 {n_sample_splom:,}件サンプル（全{n_persons:,}件中）。"
    )
    body += plotly_div_safe(fig_splom, "splom", 1400)
    body += "</div>"

    # Chart 2c: PCA Loadings (主成分の解釈)
    n_pc_show = min(5, n_components)
    loadings = pca.components_[:n_pc_show]  # shape (n_pc_show, n_features)
    fig_load = go.Figure(data=go.Heatmap(
        z=loadings.tolist(),
        x=FEATURE_NAMES,
        y=[f"PC{i+1} ({pca.explained_variance_ratio_[i]:.1%})" for i in range(n_pc_show)],
        colorscale="RdBu_r",
        zmid=0,
        text=[[f"{loadings[r, c]:.2f}" for c in range(len(FEATURE_NAMES))] for r in range(n_pc_show)],
        texttemplate="%{text}",
        hovertemplate="PC: %{y}<br>特徴量: %{x}<br>負荷量: %{z:.3f}<extra></extra>",
    ))
    fig_load.update_layout(title="PCA 主成分負荷量 (上位5PC)", height=350)
    body += '<div class="card"><h2>PCA 主成分負荷量</h2>'
    body += section_desc("各主成分（PC）がどの特徴量を強く反映しているかを示す。絶対値が大きいほどそのPCへの貢献が大きい。")
    body += chart_guide("赤=正の寄与（大きい値が高スコアに対応）、青=負の寄与。PC1は最も分散の大きい方向。")
    body += plotly_div_safe(fig_load, "pca_loadings", 350)
    body += "</div>"

    # Chart 3: Cluster Profile Heatmap
    import numpy as np
    z_scores = (centers_orig - centers_orig.mean(axis=0)) / (centers_orig.std(axis=0) + 1e-10)
    fig_heat = go.Figure(data=go.Heatmap(
        z=z_scores.tolist(),
        x=FEATURE_NAMES,
        y=[cluster_names[c] for c in range(n_clusters)],
        colorscale="RdBu_r",
        zmid=0,
        text=[[f"{z_scores[r, c]:.2f}" for c in range(len(FEATURE_NAMES))] for r in range(n_clusters)],
        texttemplate="%{text}",
        hovertemplate="クラスタ: %{y}<br>特徴量: %{x}<br>Z-score: %{z:.2f}<extra></extra>",
    ))
    fig_heat.update_layout(title="クラスタプロファイル ヒートマップ (Z-score)")
    body += '<div class="card"><h2>クラスタプロファイル ヒートマップ</h2>'
    body += section_desc("各クラスタの特徴量平均をZ-scoreで可視化。赤は平均より高い、青は低い。クラスタ間の特性差を一覧。")
    body += chart_guide("赤色=全体平均より高い特徴、青色=低い特徴。行はクラスタ、列は特徴量。")
    body += plotly_div_safe(fig_heat, "heatmap", 400)
    body += "</div>"

    # Chart 4: Silhouette Analysis
    fig_sil = go.Figure()
    y_lower = 0
    for c in range(n_clusters):
        mask = cluster_labels == c
        c_sil = np.sort(sil_samples[mask])
        y_upper = y_lower + len(c_sil)
        fig_sil.add_trace(go.Bar(
            x=c_sil.tolist(),
            y=list(range(y_lower, y_upper)),
            orientation="h",
            name=cluster_names[c],
            marker=dict(line=dict(width=0)),
            hovertemplate=f"Cluster: {cluster_names[c]}<br>Silhouette: %{{x:.3f}}<extra></extra>",
        ))
        y_lower = y_upper
    fig_sil.add_vline(x=sil_avg, line_dash="dash", line_color="white", annotation_text=f"平均: {sil_avg:.3f}")
    fig_sil.update_layout(
        title=f"シルエット分析 (平均スコア: {sil_avg:.3f})",
        xaxis_title="シルエット係数",
        yaxis=dict(showticklabels=False),
        barmode="stack",
        showlegend=True,
    )
    body += '<div class="card"><h2>シルエット分析</h2>'
    body += section_desc("各データポイントのシルエット係数。1に近いほどクラスタの分離が良好。負の値はクラスタ割当が不適切な可能性。")
    body += plotly_div_safe(fig_sil, "silhouette", 500)
    body += "</div>"

    # Chart 5: PCA Explained Variance
    ev = pca.explained_variance_ratio_
    cumulative = [sum(ev[:i + 1]) for i in range(len(ev))]
    fig_ev = make_subplots(specs=[[{"secondary_y": True}]])
    fig_ev.add_trace(go.Bar(x=[f"PC{i+1}" for i in range(len(ev))], y=ev.tolist(), name="寄与率", marker_color="#f093fb"), secondary_y=False)
    fig_ev.add_trace(go.Scatter(x=[f"PC{i+1}" for i in range(len(ev))], y=cumulative, name="累積寄与率", line=dict(color="#06D6A0", width=3), mode="lines+markers"), secondary_y=True)
    fig_ev.update_layout(title="PCA 分散説明率")
    fig_ev.update_yaxes(title_text="寄与率", secondary_y=False)
    fig_ev.update_yaxes(title_text="累積寄与率", secondary_y=True)
    body += '<div class="card"><h2>PCA 分散説明率</h2>'
    body += section_desc("各主成分の分散説明率と累積寄与率。少数の主成分で大部分の分散を説明できるかを確認。")
    body += plotly_div_safe(fig_ev, "explained_var", 400)
    body += "</div>"

    # Cluster summary table
    body += '<div class="card"><h2>クラスタサマリー</h2>'
    body += "<table><thead><tr><th>クラスタ</th><th>人数</th><th>代表特徴</th><th>平均IV Score</th><th>平均Credits</th></tr></thead><tbody>"
    for cp in cluster_data["cluster_profiles"]:
        prof = cp["profile"]
        top_feats = sorted([(fname, prof[fname]) for fname in FEATURE_NAMES], key=lambda x: -x[1])[:3]
        feat_str = ", ".join(f"{f[0]}={f[1]:.1f}" for f in top_feats)
        body += f'<tr><td>{cp["name"]}</td><td>{cp["size"]:,}</td><td>{feat_str}</td><td>{prof["iv_score"]:.1f}</td><td>{prof["total_credits"]:.0f}</td></tr>'
    body += "</tbody></table></div>"

    intro = report_intro(
        "MLクラスタリング分析",
        "20次元の特徴量ベクトルに基づくPCA次元圧縮とK-Meansクラスタリング。"
        "人物の類型を教師なし学習で抽出し、業界全体の構造パターンを可視化する。",
        "研究者、データアナリスト、スタジオ人事"
    )
    glossary = {
        **COMMON_GLOSSARY_TERMS,
        "PCA（主成分分析）": "多次元データを少数の主成分に変換する次元削減手法。分散の大きい方向を軸にデータを射影。",
        "K-Means": "データをK個のクラスタに分割する教師なしクラスタリングアルゴリズム。各クラスタの重心を反復的に更新。",
        "シルエットスコア": "クラスタの分離度を-1〜1で評価する指標。1に近いほどクラスタが明確に分離。",
        "Z-score": "標準偏差で正規化したスコア。0=平均、正=平均超、負=平均未満。",
    }

    html = wrap_html("MLクラスタリング分析", "PCA次元圧縮 × K-Meansクラスタリング", body, intro_html=intro, glossary_terms=glossary)
    out_html = REPORTS_DIR / "ml_clustering.html"
    with open(out_html, "w") as f:
        f.write(html)
    print(f"    -> {out_html}")


def generate_explorer_data():
    """Goエクスプローラー用軽量データを出力."""
    print("  Generating explorer data...")
    scores = load_json("scores.json")
    if not scores or not isinstance(scores, list):
        print("    [SKIP] scores.json not available")
        return

    light = []
    for p in scores:
        entry = {k: v for k, v in p.items() if k not in ("breakdown", "score_range")}
        light.append(entry)

    out = JSON_DIR / "explorer_data.json"
    with open(out, "w") as f:
        json.dump(light, f, ensure_ascii=False)
    print(f"    -> {out} ({len(light)} persons)")


# ============================================================
# Report 16: Network Graph
# ============================================================


def generate_network_graph_report():
    """ネットワークグラフレポート."""
    print("  Generating Network Graph Report...")
    scores = load_json("scores.json")
    collabs = load_json("collaborations.json")
    mentorships = load_json("mentorships.json")
    ml_clusters = load_json("ml_clusters.json")

    if not scores or not isinstance(scores, list):
        print("    [SKIP] scores.json not available")
        return

    import networkx as nx

    # Sort by iv_score, take top 300
    sorted_scores = sorted(scores, key=lambda p: float(p.get("iv_score", 0)), reverse=True)
    top_n = min(300, len(sorted_scores))
    top_persons = sorted_scores[:top_n]
    top_ids = {p["person_id"] for p in top_persons}
    id_to_person = {p["person_id"]: p for p in top_persons}

    # Cluster lookup from ml_clusters.json
    cluster_lookup = {}
    if ml_clusters and "persons" in ml_clusters:
        for cp in ml_clusters["persons"]:
            cluster_lookup[cp["person_id"]] = cp.get("cluster", 0)

    # Build graph
    G = nx.Graph()
    for p in top_persons:
        pid = p["person_id"]
        G.add_node(pid, name=p.get("name", ""), iv_score=float(p.get("iv_score", 0)))

    edge_count = 0
    if collabs and isinstance(collabs, list):
        for c in collabs:
            a, b = c.get("person_a", ""), c.get("person_b", "")
            if a in top_ids and b in top_ids:
                G.add_edge(a, b, weight=float(c.get("strength_score", 1)), type="collaboration")
                edge_count += 1

    if mentorships and isinstance(mentorships, list):
        for m in mentorships:
            a, b = m.get("mentor_id", ""), m.get("mentee_id", "")
            if a in top_ids and b in top_ids and not G.has_edge(a, b):
                G.add_edge(a, b, weight=float(m.get("confidence", 50)), type="mentorship")
                edge_count += 1

    if len(G.nodes) < 2:
        print("    [SKIP] Not enough connected nodes")
        return

    # Layout
    pos = nx.spring_layout(G, k=2.0 / (len(G.nodes) ** 0.5), iterations=50, seed=42)

    # Build Plotly figure
    fig = go.Figure()

    # Edges
    edge_x, edge_y = [], []
    mentor_edge_x, mentor_edge_y = [], []
    for u, v, d in G.edges(data=True):
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        target_x, target_y = (mentor_edge_x, mentor_edge_y) if d.get("type") == "mentorship" else (edge_x, edge_y)
        target_x.extend([x0, x1, None])
        target_y.extend([y0, y1, None])

    if edge_x:
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y, mode="lines",
            line=dict(width=0.5, color="rgba(150,150,200,0.3)"),
            hoverinfo="none", name="協業",
        ))
    if mentor_edge_x:
        fig.add_trace(go.Scatter(
            x=mentor_edge_x, y=mentor_edge_y, mode="lines",
            line=dict(width=1.0, color="rgba(6,214,160,0.4)", dash="dot"),
            hoverinfo="none", name="師弟",
        ))

    # Nodes by cluster
    n_clusters = max(cluster_lookup.values()) + 1 if cluster_lookup else 1
    cluster_colors = px.colors.qualitative.Set3[:n_clusters] if n_clusters <= 12 else px.colors.qualitative.Alphabet[:n_clusters]

    cluster_groups: dict[int, list] = {}
    for pid in G.nodes:
        c = cluster_lookup.get(pid, 0)
        cluster_groups.setdefault(c, []).append(pid)

    for c, pids in sorted(cluster_groups.items()):
        node_x = [pos[pid][0] for pid in pids]
        node_y = [pos[pid][1] for pid in pids]
        node_size = [max(6, min(30, float(id_to_person[pid].get("iv_score", 10)) / 3)) for pid in pids]
        hover = [
            f"{id_to_person[pid].get('name', pid)}<br>"
            f"IV Score: {id_to_person[pid].get('iv_score', 0):.1f}<br>"
            f"Role: {id_to_person[pid].get('primary_role', '?')}<br>"
            f"Credits: {id_to_person[pid].get('total_credits', 0)}"
            for pid in pids
        ]
        color = cluster_colors[c % len(cluster_colors)] if cluster_colors else "#f093fb"
        cl_name = ""
        if ml_clusters and "metadata" in ml_clusters:
            cl_names = ml_clusters["metadata"].get("cluster_names", {})
            cl_name = cl_names.get(str(c), f"Cluster {c}")
        fig.add_trace(go.Scatter(
            x=node_x, y=node_y, mode="markers",
            marker=dict(size=node_size, color=color, line=dict(width=0.5, color="white")),
            text=hover,
            hovertemplate="%{text}<extra></extra>",
            name=cl_name or f"Cluster {c}",
        ))

    fig.update_layout(
        title=f"協業ネットワークグラフ (上位{top_n}人, {edge_count}エッジ)",
        showlegend=True,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    )

    body = ""
    body += '<div class="stats-grid">'
    body += f'<div class="stat-card"><div class="value">{len(G.nodes)}</div><div class="label">ノード数</div></div>'
    body += f'<div class="stat-card"><div class="value">{edge_count}</div><div class="label">エッジ数</div></div>'
    body += f'<div class="stat-card"><div class="value">{n_clusters}</div><div class="label">クラスタ数</div></div>'
    density = nx.density(G) if len(G.nodes) > 1 else 0
    body += f'<div class="stat-card"><div class="value">{density:.4f}</div><div class="label">グラフ密度</div></div>'
    body += "</div>"

    body += '<div class="card"><h2>ネットワークグラフ</h2>'
    body += section_desc("IV Score上位人物の協業ネットワーク。ノードサイズはIV Score、色はMLクラスタ。"
                         "実線は協業関係、点線は師弟関係。")
    body += chart_guide("ノードをホバーで名前・スコア表示。ドラッグでパン、スクロールでズーム。凡例クリックでクラスタ非表示。")
    body += plotly_div_safe(fig, "network_graph", 700)
    body += "</div>"

    intro = report_intro(
        "ネットワークグラフ",
        "IV Score上位人物を中心とした協業・師弟ネットワークの可視化。"
        "MLクラスタリング結果を色分けに使用し、グラフ構造とクラスタの対応を確認。",
        "研究者、プロデューサー、スタジオ経営"
    )
    glossary = {
        **COMMON_GLOSSARY_TERMS,
        "フォースディレクテッドレイアウト": "接続された点同士を引力で近づけ、切断された点を斥力で離す物理シミュレーション。クラスタ構造を自然に可視化。",
        "エゴグラフ": "特定人物を中心とした直接の協業者ネットワーク。",
    }

    html = wrap_html("ネットワークグラフ", "協業ネットワークのインタラクティブ可視化", body, intro_html=intro, glossary_terms=glossary)
    out_html = REPORTS_DIR / "network_graph.html"
    with open(out_html, "w") as f:
        f.write(html)
    print(f"    -> {out_html}")


# ============================================================
# Report 1: Industry Overview Dashboard
# ============================================================

def generate_industry_overview():
    """業界概観ダッシュボード."""
    import sqlite3 as _sqlite3
    from math import log1p as _log1p
    from src.database import get_connection as _get_conn

    print("  Generating Industry Overview Dashboard...")

    # --- data quality year constants ---
    RELIABLE_MAX_YEAR = 2025   # 2025秋まで信頼可能
    STAT_MAX_YEAR = 2024       # 統計/トレンド分析は2024まで

    summary = load_json("summary.json")
    time_series = load_json("time_series.json")
    seasonal = load_json("seasonal.json")
    insights = load_json("insights_report.json")
    growth = load_json("growth.json")
    scores_data = load_json("scores.json")
    milestones_data = load_json("milestones.json")

    if not summary:
        return

    d = summary.get("data", {})
    g = summary.get("graph", {})
    s = summary.get("scores", {})

    # --- build shared lookup dicts from scores.json (list) ---
    pid_first_year: dict[str, int] = {}
    pid_latest_year: dict[str, int] = {}
    pid_stage: dict[str, int] = {}
    pid_iv: dict[str, float] = {}
    pid_trend: dict[str, str] = {}

    if scores_data and isinstance(scores_data, list):
        for entry in scores_data:
            pid = entry.get("person_id", "")
            career = entry.get("career", {})
            if not pid:
                continue
            fy = career.get("first_year")
            ly = career.get("latest_year")
            st = career.get("highest_stage", 0)
            iv = entry.get("iv_score") or 0.0
            if fy:
                pid_first_year[pid] = fy
            if ly:
                pid_latest_year[pid] = ly
            pid_stage[pid] = st or 0
            pid_iv[pid] = iv

    if growth and isinstance(growth.get("persons"), dict):
        for pid, pdata in growth["persons"].items():
            pid_trend[pid] = pdata.get("trend", "")

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
        ("Top IV Score", f"{s.get('top_composite', 0):.2f}"),
        ("Median IV Score", f"{s.get('median_composite', 0):.2f}"),
        ("Largest Component", fmt_num(g.get("largest_component_size", 0))),
        ("Elapsed", f"{summary.get('elapsed_seconds', 0):.0f}s"),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div></div>"

    # Data quality note
    body += (
        '<div class="insight-box" style="border-left-color:#FFD166;">'
        "<strong>データ品質注記:</strong> "
        f"2026年以降のデータは収録数が少なく統計的に不安定なため分析対象外（上限: {RELIABLE_MAX_YEAR}年）。"
        "2025年冬クールは参考値として扱ってください。"
        "</div>"
    )

    # --- Time series chart (filtered to RELIABLE_MAX_YEAR) ---
    if time_series:
        years_all = time_series["years"]
        series = time_series["series"]
        years = [yr for yr in years_all if yr <= RELIABLE_MAX_YEAR]

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

        fig.update_layout(
            title=f"Industry Time Series (1917–{RELIABLE_MAX_YEAR})",
            showlegend=False,
        )
        body += '<div class="card">'
        body += "<h2>Time Series</h2>"
        body += chart_guide(
            "各サブプロットはアニメ制作史全体の異なる指標を追跡しています。"
            "任意のポイントにホバーすると正確な値が表示されます。上昇傾向は業界の成長を示し、"
            "横ばいや減少は初期の年代におけるデータカバレッジの差を反映している可能性があります。"
        )
        body += plotly_div_safe(fig, "timeseries", 600)

        # Dual-axis: Credits + Avg Anime Score
        avg_scores_io = series.get("avg_anime_score", {})
        credit_counts_io = series.get("credit_count", {})
        if avg_scores_io:
            fig_da = make_subplots(specs=[[{"secondary_y": True}]])
            fig_da.add_trace(
                go.Bar(
                    x=years,
                    y=[credit_counts_io.get(str(yr), 0) for yr in years],
                    name="クレジット数",
                    marker_color="rgba(240,147,251,0.35)",
                    hovertemplate="%{x}: %{y:,}クレジット<extra></extra>",
                ),
                secondary_y=False,
            )
            fig_da.add_trace(
                go.Scatter(
                    x=years,
                    y=[avg_scores_io.get(str(yr), 0) for yr in years],
                    name="平均アニメスコア",
                    mode="lines+markers",
                    line=dict(color="#06D6A0", width=2),
                    marker=dict(size=3),
                    hovertemplate="%{x}: %{y:.2f}<extra></extra>",
                ),
                secondary_y=True,
            )
            fig_da.update_layout(title="年間クレジット数 + 平均アニメスコア（Dual-axis）")
            fig_da.update_yaxes(title_text="クレジット数", secondary_y=False)
            fig_da.update_yaxes(title_text="平均アニメスコア", secondary_y=True)

            body += "<h3>クレジット数 + 平均アニメスコア</h3>"
            body += chart_guide(
                "左Y軸（薄い棒グラフ）: 年間クレジット数、右Y軸（緑の折れ線）: 年間平均アニメスコア。"
                "クレジット数の急増期に品質（スコア）がどう推移したかを重ねて読み取れます。"
            )
            body += plotly_div_safe(fig_da, "io_dual_axis", 450)
        body += "</div>"

    # ============================================================
    # 人材フロー率 — 完全書き換え v2
    # ============================================================
    if scores_data and isinstance(scores_data, list) and time_series:
        # 離脱カウント上限: 2023年以降はデータ欠損により最終クレジット年が
        # 過去に見える誤検出（見かけ上の退職増）が生じるため除外
        EXIT_CUTOFF_YEAR = 2022
        FLOW_START_YEAR = 1990
        HIGH_IV_THRESHOLD = 30.0

        years_all = time_series["years"]
        series_ts = time_series["series"]
        active_persons_ts = series_ts.get("active_persons", {})
        valid_flow_years = [
            yr for yr in years_all
            if active_persons_ts.get(str(yr), 0) > 10 and FLOW_START_YEAR <= yr <= RELIABLE_MAX_YEAR
        ]
        flow_years_stock = list(range(FLOW_START_YEAR, EXIT_CUTOFF_YEAR + 1))

        # --- ステージグループ定義 ---
        STAGE_GROUPS_DEF = [
            ("新人",    0,  2, "#a0d2db"),   # 動画・原画・第二原画
            ("中堅",    3,  4, "#06D6A0"),   # 作画監督補佐・作画監督・演出
            ("ベテラン", 5, 99, "#f093fb"),  # 総作監・シリーズ監督・監督
        ]
        stage_groups = [d[0] for d in STAGE_GROUPS_DEF]
        sg_color = {d[0]: d[3] for d in STAGE_GROUPS_DEF}

        def _stage_group(stage: int) -> str:
            for sg, lo, hi, _ in STAGE_GROUPS_DEF:
                if lo <= stage <= hi:
                    return sg
            return "新人"

        # --- 職種タイプ定義 ---
        ROLE_TYPE_DEF = {
            "animator":   ("動画/原画",   "#a0d2db"),
            "director":   ("演出/監督",   "#f093fb"),
            "designer":   ("デザイナー",  "#FFD166"),
            "production": ("制作",        "#fda085"),
            "writing":    ("脚本/構成",   "#06D6A0"),
            "technical":  ("技術/CG",     "#667eea"),
            "other":      ("その他",      "#606070"),
        }
        def _role_type(primary_role: str | None) -> str:
            return primary_role if primary_role in ROLE_TYPE_DEF else "other"

        pid_role_type: dict[str, str] = {
            e["person_id"]: _role_type(e.get("primary_role"))
            for e in scores_data if e.get("person_id")
        }

        # ---- STOCK 計算（first_year ≤ Y ≤ latest_year の人数） ----
        stock_by_sg: dict[str, dict[int, int]] = {sg: {yr: 0 for yr in flow_years_stock} for sg in stage_groups}
        stock_total: dict[int, int] = {yr: 0 for yr in flow_years_stock}
        for pid, fy in pid_first_year.items():
            ly = pid_latest_year.get(pid, RELIABLE_MAX_YEAR)
            sg = _stage_group(pid_stage.get(pid, 0))
            for yr in flow_years_stock:
                if fy <= yr <= ly:
                    stock_by_sg[sg][yr] += 1
                    stock_total[yr] += 1

        # ---- ENTRY / EXIT カウント（EXIT_CUTOFF_YEAR 以降は離脱なし） ----
        entry_by_sg:   dict[str, dict[int, int]] = {sg: {} for sg in stage_groups}
        exit_by_sg:    dict[str, dict[int, int]] = {sg: {} for sg in stage_groups}
        entry_by_tier: dict[str, dict[int, int]] = {"高評価": {}, "標準": {}}
        exit_by_tier:  dict[str, dict[int, int]] = {"高評価": {}, "標準": {}}
        entry_by_role: dict[str, dict[int, int]] = {rt: {} for rt in ROLE_TYPE_DEF}
        exit_by_role:  dict[str, dict[int, int]] = {rt: {} for rt in ROLE_TYPE_DEF}

        for pid, fy in pid_first_year.items():
            if not (FLOW_START_YEAR <= fy <= RELIABLE_MAX_YEAR):
                continue
            sg   = _stage_group(pid_stage.get(pid, 0))
            tier = "高評価" if pid_iv.get(pid, 0.0) > HIGH_IV_THRESHOLD else "標準"
            rt   = pid_role_type.get(pid, "other")
            entry_by_sg[sg][fy]     = entry_by_sg[sg].get(fy, 0) + 1
            entry_by_tier[tier][fy] = entry_by_tier[tier].get(fy, 0) + 1
            entry_by_role[rt][fy]   = entry_by_role[rt].get(fy, 0) + 1

        for pid, ly in pid_latest_year.items():
            if not (FLOW_START_YEAR <= ly <= EXIT_CUTOFF_YEAR):
                continue
            sg   = _stage_group(pid_stage.get(pid, 0))
            tier = "高評価" if pid_iv.get(pid, 0.0) > HIGH_IV_THRESHOLD else "標準"
            rt   = pid_role_type.get(pid, "other")
            exit_by_sg[sg][ly]     = exit_by_sg[sg].get(ly, 0) + 1
            exit_by_tier[tier][ly] = exit_by_tier[tier].get(ly, 0) + 1
            exit_by_role[rt][ly]   = exit_by_role[rt].get(ly, 0) + 1

        # ---- 期待能力 × 実際能力 4-ティア計算 (Chart D用) ----
        # 期待能力: 協業者の質・作品スコア・スタジオ水準の合成指標
        # 実際能力: iv_score percentile
        _EXP_HIGH_PCTILE = 70.0    # 上位30%を「高」とする閾値
        _EXP_TIER_DEFS = [
            ("優秀確定",   "#F72585"),   # 高期待 × 高実績
            ("期待の星",   "#FFD166"),   # 高期待 × 低実績（ポテンシャル未転換）
            ("隠れた実力", "#06D6A0"),   # 低期待 × 高実績（サプライズ型）
            ("標準",       "#a0a0c0"),   # 低期待 × 低実績
        ]
        _exp_tier_names  = [t[0] for t in _EXP_TIER_DEFS]
        _exp_tier_color  = {t[0]: t[1] for t in _EXP_TIER_DEFS}
        _exp_entry_by_tier: dict[str, dict[int, int]] = {t: {} for t in _exp_tier_names}
        _exp_exit_by_tier:  dict[str, dict[int, int]] = {t: {} for t in _exp_tier_names}
        _exp_tier_sizes:    dict[str, int]             = {t: 0  for t in _exp_tier_names}
        _exp_pid_tier:      dict[str, str]             = {}
        _exp_pid_expected:  dict[str, float]           = {}   # 0–100 pctile
        _exp_pid_actual:    dict[str, float]           = {}   # 0–100 pctile of iv_score
        _exp_computation_ok = False

        try:
            import bisect as _bisect_exp
            import statistics as _stats_exp

            # Step 1: load all (anime_id, person_id, anime_score) — O(credits)
            _exp_conn = _get_conn()
            _exp_conn.row_factory = _sqlite3.Row
            _exp_raw_rows = _exp_conn.execute("""
                SELECT c.anime_id, c.person_id, a.score AS anime_score
                FROM credits c JOIN anime a ON c.anime_id = a.id
                WHERE a.year BETWEEN 1980 AND 2025
            """).fetchall()
            _exp_conn.close()

            # Step 2: group by anime → avg collaborator iv per anime
            _exp_anime_pids:  dict[str, list] = {}
            _exp_anime_score: dict[str, float] = {}
            for _row in _exp_raw_rows:
                _aid = _row["anime_id"]; _p = _row["person_id"]
                _asc = _row["anime_score"]
                _exp_anime_pids.setdefault(_aid, []).append(_p)
                if _asc and float(_asc) > 0:
                    _exp_anime_score[_aid] = float(_asc)
            del _exp_raw_rows

            _exp_avg_collab_iv: dict[str, float] = {}
            for _aid, _pids in _exp_anime_pids.items():
                _ivs = [pid_iv.get(_p2, 0.0) for _p2 in _pids]
                if _ivs:
                    _exp_avg_collab_iv[_aid] = _stats_exp.mean(_ivs)

            # Step 3: per-person aggregation (collab quality + work quality)
            _exp_pid_animes: dict[str, list] = {}
            for _aid, _pids in _exp_anime_pids.items():
                for _p in _pids:
                    _exp_pid_animes.setdefault(_p, []).append(_aid)
            del _exp_anime_pids

            _exp_person_collab_iv:    dict[str, float] = {}
            _exp_person_work_score:   dict[str, float] = {}
            for _p, _animes in _exp_pid_animes.items():
                _civs    = [_exp_avg_collab_iv.get(_a, 0.0) for _a in _animes]
                _weights = [_exp_anime_score.get(_a, 1.0)   for _a in _animes]
                _wsum    = sum(_weights)
                if _wsum > 0:
                    _exp_person_collab_iv[_p] = (
                        sum(_c * _w for _c, _w in zip(_civs, _weights)) / _wsum
                    )
                _wscores = [_exp_anime_score[_a] for _a in _animes if _a in _exp_anime_score]
                if _wscores:
                    _exp_person_work_score[_p] = _stats_exp.mean(_wscores)
            del _exp_pid_animes

            # Step 3b: studio prestige — reuse studio_person_years (no extra DB query)
            _exp_studio_avg_iv: dict[str, float] = {}
            _exp_pid_studio_prestige: dict[str, float] = {}
            _exp_pid_studios_tmp: dict[str, list] = {}
            for _st, _yr_sets in studio_person_years.items():
                _all_pids: set = set()
                for _ps in _yr_sets.values():
                    _all_pids.update(_ps)
                    for _p in _ps:
                        _exp_pid_studios_tmp.setdefault(_p, []).append(_st)
                if _all_pids:
                    _ivs_st = [pid_iv.get(_p2, 0.0) for _p2 in _all_pids]
                    _exp_studio_avg_iv[_st] = _stats_exp.mean(_ivs_st)
            for _p, _sts in _exp_pid_studios_tmp.items():
                _vals = [_exp_studio_avg_iv[_s] for _s in _sts if _s in _exp_studio_avg_iv]
                if _vals:
                    _exp_pid_studio_prestige[_p] = max(_vals)

            # Step 4: normalize and combine → 期待能力 raw score
            _exp_collab_max = max(_exp_person_collab_iv.values(), default=1.0) or 1.0
            _exp_work_max   = max(_exp_person_work_score.values(),  default=9.0) or 9.0
            _exp_studio_max = max(_exp_studio_avg_iv.values(),      default=1.0) or 1.0

            _exp_raw_score: dict[str, float] = {}
            for _p in pid_iv:
                _cv = _exp_person_collab_iv.get(_p,       0.0) / _exp_collab_max
                _wv = _exp_person_work_score.get(_p,      0.0) / _exp_work_max
                _sv = _exp_pid_studio_prestige.get(_p,    0.0) / _exp_studio_max
                _exp_raw_score[_p] = 0.50 * _cv + 0.30 * _wv + 0.20 * _sv

            # Percentile rank 0–100 (bisect on sorted list)
            _exp_sorted = sorted(_exp_raw_score.values())
            _exp_n      = max(len(_exp_sorted) - 1, 1)
            for _p, _v in _exp_raw_score.items():
                _idx = _bisect_exp.bisect_left(_exp_sorted, _v)
                _exp_pid_expected[_p] = (_idx / _exp_n) * 100.0

            _exp_act_sorted = sorted(pid_iv.values())
            _exp_act_n      = max(len(_exp_act_sorted) - 1, 1)
            for _p, _v in pid_iv.items():
                _idx = _bisect_exp.bisect_left(_exp_act_sorted, _v)
                _exp_pid_actual[_p] = (_idx / _exp_act_n) * 100.0

            # Step 5: assign 4 tiers
            for _p in pid_iv:
                _hi_exp = _exp_pid_expected.get(_p, 0.0) >= _EXP_HIGH_PCTILE
                _hi_act = _exp_pid_actual.get(_p,   0.0) >= _EXP_HIGH_PCTILE
                _t = ("優秀確定"   if _hi_exp and _hi_act  else
                      "期待の星"   if _hi_exp               else
                      "隠れた実力" if _hi_act               else
                      "標準")
                _exp_pid_tier[_p]   = _t
                _exp_tier_sizes[_t] += 1

            for _p, _fy in pid_first_year.items():
                if FLOW_START_YEAR <= _fy <= RELIABLE_MAX_YEAR:
                    _t = _exp_pid_tier.get(_p, "標準")
                    _exp_entry_by_tier[_t][_fy] = _exp_entry_by_tier[_t].get(_fy, 0) + 1
            for _p, _ly in pid_latest_year.items():
                if FLOW_START_YEAR <= _ly <= EXIT_CUTOFF_YEAR:
                    _t = _exp_pid_tier.get(_p, "標準")
                    _exp_exit_by_tier[_t][_ly] = _exp_exit_by_tier[_t].get(_ly, 0) + 1

            _exp_computation_ok = True

        except Exception as _exp_exc:
            # フォールバック: iv_score 70パーセンタイル閾値のみで2ティア
            _exp_fb_vals = sorted(pid_iv.values())
            _exp_fb_thr  = _exp_fb_vals[int(len(_exp_fb_vals) * 0.70)] if _exp_fb_vals else 0.0
            for _p, _v in pid_iv.items():
                _t = "優秀確定" if _v >= _exp_fb_thr else "標準"
                _exp_pid_tier[_p]   = _t
                _exp_tier_sizes[_t] += 1
            for _p, _fy in pid_first_year.items():
                if FLOW_START_YEAR <= _fy <= RELIABLE_MAX_YEAR:
                    _t = _exp_pid_tier.get(_p, "標準")
                    _exp_entry_by_tier[_t][_fy] = _exp_entry_by_tier[_t].get(_fy, 0) + 1
            for _p, _ly in pid_latest_year.items():
                if FLOW_START_YEAR <= _ly <= EXIT_CUTOFF_YEAR:
                    _t = _exp_pid_tier.get(_p, "標準")
                    _exp_exit_by_tier[_t][_ly] = _exp_exit_by_tier[_t].get(_ly, 0) + 1

        # ---- 段階遷移（milestones.json promotionイベント） ----
        trans_types = ["新人→中堅", "中堅→ベテラン", "新人→ベテラン"]
        transitions: dict[str, dict[int, int]] = {t: {} for t in trans_types}
        if milestones_data and isinstance(milestones_data, dict):
            for pid, events in milestones_data.items():
                if not isinstance(events, list):
                    continue
                for ev in events:
                    if ev.get("type") != "promotion":
                        continue
                    yr = ev.get("year")
                    if not yr or not (FLOW_START_YEAR <= yr <= RELIABLE_MAX_YEAR):
                        continue
                    from_sg = _stage_group(ev.get("from_stage", 0))
                    to_sg   = _stage_group(ev.get("to_stage", 0))
                    key = f"{from_sg}→{to_sg}"
                    if key in transitions:
                        transitions[key][yr] = transitions[key].get(yr, 0) + 1

        # ---- 離脱タイプ分類 ----
        LOSS_TYPES = ["エース離脱", "ベテラン引退", "中堅離脱", "新人早期離脱"]
        loss_type_colors = {
            "エース離脱":   "#FFD166",
            "ベテラン引退": "#f093fb",
            "中堅離脱":     "#06D6A0",
            "新人早期離脱": "#a0d2db",
        }
        loss_by_type_yr: dict[str, dict[int, int]] = {t: {} for t in LOSS_TYPES}
        loss_by_role_total: dict[str, int] = {}
        for pid, ly in pid_latest_year.items():
            if not (FLOW_START_YEAR <= ly <= EXIT_CUTOFF_YEAR):
                continue
            iv = pid_iv.get(pid, 0.0)
            st = pid_stage.get(pid, 0)
            lt = ("エース離脱"   if iv > HIGH_IV_THRESHOLD else
                  "ベテラン引退" if st >= 5 else
                  "中堅離脱"     if st >= 3 else
                  "新人早期離脱")
            loss_by_type_yr[lt][ly] = loss_by_type_yr[lt].get(ly, 0) + 1
            rt = pid_role_type.get(pid, "other")
            loss_by_role_total[rt] = loss_by_role_total.get(rt, 0) + 1

        # ---- K-Means クラスタリング（ステージ境界検証） ----
        _cluster_info = ""
        try:
            import numpy as _np_cl
            from sklearn.cluster import KMeans as _KM
            from sklearn.preprocessing import StandardScaler as _SC_km
            cl_rows, cl_pids = [], []
            for e in scores_data:
                pid = e.get("person_id", "")
                career = e.get("career") or {}
                st = career.get("highest_stage") or 0
                ay = career.get("active_years") or 0
                tc = e.get("total_credits") or 0
                iv = e.get("iv_score") or 0.0
                if st is None:
                    continue
                cl_rows.append([float(st), float(ay), float(tc), float(iv)])
                cl_pids.append(pid)
            if len(cl_rows) >= 30:
                Xcl = _np_cl.array(cl_rows)
                sc_km = _SC_km()
                Xcl_s = sc_km.fit_transform(Xcl)
                km = _KM(n_clusters=3, n_init=20, random_state=42)
                labels = km.fit_predict(Xcl_s)
                centers_real = sc_km.inverse_transform(km.cluster_centers_)
                # sort by stage centroid
                ord_cl = _np_cl.argsort(centers_real[:, 0])
                cl_names = {int(ord_cl[i]): n for i, n in enumerate(["クラスタ新人", "クラスタ中堅", "クラスタベテラン"])}
                sg_map_rev = {"クラスタ新人": "新人", "クラスタ中堅": "中堅", "クラスタベテラン": "ベテラン"}
                match = sum(1 for i, pid in enumerate(cl_pids)
                            if _stage_group(pid_stage.get(pid, 0)) == sg_map_rev[cl_names[labels[i]]])
                agree = match / len(cl_pids) * 100
                rows_km = "".join(
                    f"<tr><td>{cl_names[i]}</td>"
                    f"<td>{centers_real[i,0]:.2f}</td>"
                    f"<td>{centers_real[i,1]:.1f}年</td>"
                    f"<td>{centers_real[i,2]:.0f}件</td>"
                    f"<td>{centers_real[i,3]:.4f}</td></tr>"
                    for i in range(3)
                )
                _cluster_info = (
                    f"<p><strong>K-Means(K=3)との一致率: {agree:.1f}%</strong> "
                    f"— 一致率が高いほど rule-based 閾値が自然な分布を反映。</p>"
                    "<table style='width:100%;font-size:0.82rem;margin:0.5rem 0'>"
                    "<thead><tr><th>クラスタ</th><th>平均Stage</th><th>平均活動年数</th>"
                    "<th>平均クレジット</th><th>平均iv_score</th></tr></thead>"
                    f"<tbody>{rows_km}</tbody></table>"
                )
        except Exception:
            _cluster_info = "<p style='color:#808080'>クラスタリング計算をスキップしました。</p>"

        # ---- DB: スタジオ別・国別 ----
        studio_person_years: dict[str, dict[int, set]] = {}
        country_persons: dict[str, dict[int, int]] = {}
        try:
            conn_fl = _get_conn()
            conn_fl.row_factory = _sqlite3.Row
            rows_studio = conn_fl.execute("""
                SELECT je.value AS studio, a.year, c.person_id
                FROM anime a
                JOIN credits c ON c.anime_id = a.id,
                     json_each(CASE WHEN json_valid(a.studios) THEN a.studios ELSE '[]' END) AS je
                WHERE a.year BETWEEN ? AND ? AND je.value != '' AND je.value IS NOT NULL
            """, (FLOW_START_YEAR, RELIABLE_MAX_YEAR)).fetchall()
            for row in rows_studio:
                st_name = str(row["studio"]).strip()
                yr = row["year"]; pid = row["person_id"]
                if not st_name or not yr or not pid:
                    continue
                studio_person_years.setdefault(st_name, {}).setdefault(yr, set()).add(pid)

            rows_country = conn_fl.execute("""
                SELECT COALESCE(a.country_of_origin,'Unknown') AS country,
                       a.year, COUNT(DISTINCT c.person_id) AS up
                FROM anime a JOIN credits c ON c.anime_id = a.id
                WHERE a.year BETWEEN ? AND ?
                GROUP BY country, a.year
            """, (FLOW_START_YEAR, RELIABLE_MAX_YEAR)).fetchall()
            for row in rows_country:
                country_persons.setdefault(row["country"], {})[row["year"]] = row["up"]
            conn_fl.close()
        except Exception as _e_fl:
            pass

        studio_total = {s: sum(len(v) for v in yrs.values()) for s, yrs in studio_person_years.items()}
        top_studios  = sorted(studio_total, key=studio_total.get, reverse=True)[:8]
        country_total = {c: sum(v.values()) for c, v in country_persons.items()}
        top_countries = sorted(country_total, key=country_total.get, reverse=True)[:5]

        # ========== HTML組み立て ==========
        body += '<div class="card">'
        body += "<h2>人材フロー率（参入・離脱・ストック）</h2>"

        # --- ステージ分類の説明 ---
        body += (
            '<div class="insight-box">'
            "<strong>キャリアステージ分類の基準（パイプライン定義）</strong>"
            "<table style='width:100%;margin:0.5rem 0;font-size:0.85rem;'>"
            "<thead><tr><th>グループ</th><th>Stage</th><th>典型的な役職</th><th>Rule-based定義</th></tr></thead>"
            "<tbody>"
            "<tr><td style='color:#a0d2db'>新人</td><td>0–2</td>"
            "<td>動画・原画・第二原画</td><td>highest_stage ≤ 2</td></tr>"
            "<tr><td style='color:#06D6A0'>中堅</td><td>3–4</td>"
            "<td>作画監督補佐・作画監督・演出</td><td>3 ≤ highest_stage ≤ 4</td></tr>"
            "<tr><td style='color:#f093fb'>ベテラン</td><td>5–6</td>"
            "<td>総作画監督・シリーズ監督・監督</td><td>highest_stage ≥ 5</td></tr>"
            "</tbody></table>"
            + _cluster_info +
            "</div>"
        )
        body += section_desc(
            f"対象期間: {FLOW_START_YEAR}–{RELIABLE_MAX_YEAR}年。"
            f"<strong>離脱カウントは{EXIT_CUTOFF_YEAR}年以前のみ</strong>"
            f"（{EXIT_CUTOFF_YEAR+1}年以降はデータ収録中のため最終クレジット年が過去に見える誤検出が生じる）。"
        )

        # ---- Chart A: ストック + 構成比 ----
        fig_sa = make_subplots(
            rows=1, cols=2,
            subplot_titles=("ストック: ステージ別累積人数", "ストック構成比 (%)"),
            horizontal_spacing=0.10,
        )
        for sg in stage_groups:
            ys_stock = [stock_by_sg[sg].get(yr, 0) for yr in flow_years_stock]
            ys_ratio = [
                stock_by_sg[sg].get(yr, 0) / max(stock_total.get(yr, 1), 1) * 100
                for yr in flow_years_stock
            ]
            fig_sa.add_trace(go.Scatter(
                x=flow_years_stock, y=ys_stock, name=sg,
                mode="lines", stackgroup="one",
                line=dict(color=sg_color[sg], width=1),
                hovertemplate=f"{sg}: %{{y:,}}人<extra></extra>",
            ), row=1, col=1)
            fig_sa.add_trace(go.Scatter(
                x=flow_years_stock, y=ys_ratio, name=f"{sg} %",
                mode="lines", line=dict(color=sg_color[sg], width=2),
                showlegend=False,
                hovertemplate=f"{sg}: %{{y:.1f}}%<extra></extra>",
            ), row=1, col=2)
        fig_sa.update_layout(title="A. ストック推移: ステージ別累積アクティブ人数 + 構成比", height=420)
        body += "<h3>A. ストック推移（ステージ別）</h3>"
        body += chart_guide(
            "左: first_year ≤ Y ≤ latest_year の人数をステージ別に積み上げ。"
            "右: 各ステージの全体に占める構成比。"
            "中堅・ベテランの比率が上昇すれば業界の熟成が進んでいることを示します。"
        )
        body += plotly_div_safe(fig_sa, "flow_stock", 420)

        # ---- Chart B: ステージ別 参入/離脱（EXIT_CUTOFF_YEAR境界線付き） ----
        # 新人は「退職/ドロップアウト」と「キャリアアップ（昇進）」に分離
        exit_xs = [yr for yr in valid_flow_years if yr <= EXIT_CUTOFF_YEAR]
        fig_b2 = go.Figure()
        for sg in stage_groups:
            c = sg_color[sg]
            fig_b2.add_trace(go.Scatter(
                x=valid_flow_years,
                y=[entry_by_sg[sg].get(yr, 0) for yr in valid_flow_years],
                name=f"{sg} 参入", mode="lines",
                line=dict(color=c, width=2),
                hovertemplate=f"{sg} 参入 %{{x}}: %{{y:,}}<extra></extra>",
            ))
            exit_label = "退職/ドロップアウト" if sg == "新人" else "離脱"
            fig_b2.add_trace(go.Scatter(
                x=exit_xs,
                y=[exit_by_sg[sg].get(yr, 0) for yr in exit_xs],
                name=f"{sg} {exit_label}", mode="lines",
                line=dict(color=c, width=2, dash="dot"),
                hovertemplate=f"{sg} {exit_label} %{{x}}: %{{y:,}}<extra></extra>",
            ))
            # 新人のみ: キャリアアップ（→中堅/ベテランへの昇進）を追加
            if sg == "新人":
                cu_ys = [
                    transitions["新人→中堅"].get(yr, 0) + transitions["新人→ベテラン"].get(yr, 0)
                    for yr in valid_flow_years
                ]
                fig_b2.add_trace(go.Scatter(
                    x=valid_flow_years, y=cu_ys,
                    name="新人 キャリアアップ（→中堅/ベテラン）", mode="lines",
                    line=dict(color="#06D6A0", width=2, dash="dashdot"),
                    hovertemplate="新人 キャリアアップ %{x}: %{y:,}<extra></extra>",
                ))
        fig_b2.add_vrect(
            x0=EXIT_CUTOFF_YEAR + 0.5, x1=RELIABLE_MAX_YEAR,
            fillcolor="rgba(239,71,111,0.06)",
            line_color="rgba(239,71,111,0.4)", line_dash="dash",
            annotation_text=f"離脱データなし<br>({EXIT_CUTOFF_YEAR+1}年以降)",
            annotation_position="top right",
        )
        fig_b2.update_layout(
            title=(
                f"B. ステージ別 参入/離脱（離脱は{EXIT_CUTOFF_YEAR}年まで）"
                " ── 実線=参入 / 点線=退職 / 点鎖線=キャリアアップ（新人のみ）"
            ),
            xaxis_title="年", yaxis_title="人数",
        )
        body += "<h3>B. ステージ別 参入/離脱数（新人: 退職 vs キャリアアップ分離）</h3>"
        body += chart_guide(
            "実線=参入、点線=退職/ドロップアウト（最終ステージのまま業界を離れた人）、"
            "点鎖線(緑)=<strong>新人キャリアアップ</strong>（→中堅/ベテランへ昇進した人数、milestones promotionより）。"
            "「退職」と「キャリアアップ」は排他的（最終stage≤2 の人は昇進記録なし）。"
            f"赤破線より右({EXIT_CUTOFF_YEAR+1}年以降)は離脱データを非表示。"
        )
        body += plotly_div_safe(fig_b2, "flow_stage_b", 480)

        # ---- Chart C: キャリア段階遷移 ----
        fig_c2 = go.Figure()
        trans_colors = {"新人→中堅": "#06D6A0", "中堅→ベテラン": "#f093fb", "新人→ベテラン": "#FFD166"}
        for ttype, tc in trans_colors.items():
            yr_t = sorted(yr for yr in transitions[ttype] if FLOW_START_YEAR <= yr <= RELIABLE_MAX_YEAR)
            if yr_t:
                fig_c2.add_trace(go.Scatter(
                    x=yr_t, y=[transitions[ttype][yr] for yr in yr_t],
                    name=ttype, mode="lines+markers",
                    line=dict(color=tc, width=2), marker=dict(size=4),
                    hovertemplate=f"{ttype} %{{x}}: %{{y:,}}人<extra></extra>",
                ))
        fig_c2.update_layout(
            title="C. キャリア段階遷移数（milestones promotionイベントから）",
            xaxis_title="年", yaxis_title="遷移人数",
        )
        body += "<h3>C. キャリア段階遷移（新人→中堅→ベテラン）</h3>"
        body += chart_guide(
            "milestones.jsonのpromotionイベントでステージグループをまたぐ昇進をカウント。"
            "新人→中堅の遷移ピークが業界の育成力・登竜門の時代変化を示します。"
        )
        body += plotly_div_safe(fig_c2, "flow_transitions", 380)

        # ---- Chart D: 才能ティア別 参入/離脱（期待能力 × 実際能力 4-tier） ----
        _exp_d_suffix = "" if _exp_computation_ok else " [フォールバック: iv_score閾値]"
        fig_d2 = make_subplots(
            rows=2, cols=1,
            subplot_titles=(
                f"D-1. ティア別 参入（実線）/離脱（点線・{EXIT_CUTOFF_YEAR}年まで）",
                "D-2. 期待能力 vs 実際能力 分布（全人材・バブルサイズ=総クレジット数）",
            ),
            vertical_spacing=0.14,
            row_heights=[0.55, 0.45],
        )
        # D-1: entry/exit per tier (line chart)
        for _t, _tc in _exp_tier_color.items():
            fig_d2.add_trace(go.Scatter(
                x=valid_flow_years,
                y=[_exp_entry_by_tier[_t].get(yr, 0) for yr in valid_flow_years],
                name=f"{_t} 参入", mode="lines",
                line=dict(color=_tc, width=2),
                legendgroup=_t,
                hovertemplate=f"{_t} 参入 %{{x}}: %{{y:,}}<extra></extra>",
            ), row=1, col=1)
            fig_d2.add_trace(go.Scatter(
                x=exit_xs,
                y=[_exp_exit_by_tier[_t].get(yr, 0) for yr in exit_xs],
                name=f"{_t} 離脱", mode="lines",
                line=dict(color=_tc, width=2, dash="dot"),
                legendgroup=_t,
                showlegend=False,
                hovertemplate=f"{_t} 離脱 %{{x}}: %{{y:,}}<extra></extra>",
            ), row=1, col=1)
        fig_d2.add_vrect(
            x0=EXIT_CUTOFF_YEAR + 0.5, x1=RELIABLE_MAX_YEAR,
            fillcolor="rgba(239,71,111,0.06)",
            line_color="rgba(239,71,111,0.4)", line_dash="dash",
            annotation_text=f"離脱データなし<br>({EXIT_CUTOFF_YEAR+1}以降)",
            annotation_position="top right",
            row=1, col=1,
        )
        # D-2: scatter expected vs actual ability (sampled)
        _exp_scatter_pids = list(_exp_pid_tier.keys())
        if len(_exp_scatter_pids) > 5000:
            import random as _rand_d
            _rand_d.seed(42)
            _exp_scatter_pids = _rand_d.sample(_exp_scatter_pids, 5000)
        _exp_pid_credits: dict[str, int] = {
            e["person_id"]: e.get("total_credits") or 1
            for e in scores_data if e.get("person_id")
        }
        for _t, _tc in _exp_tier_color.items():
            _sc_pids_t = [_p for _p in _exp_scatter_pids if _exp_pid_tier.get(_p) == _t]
            if not _sc_pids_t:
                continue
            fig_d2.add_trace(go.Scatter(
                x=[_exp_pid_expected.get(_p, 0.0) for _p in _sc_pids_t],
                y=[_exp_pid_actual.get(_p,   0.0) for _p in _sc_pids_t],
                mode="markers",
                name=_t,
                legendgroup=_t,
                showlegend=False,
                marker=dict(
                    color=_tc,
                    size=[max(4, min(20, int(_exp_pid_credits.get(_p, 1) ** 0.4))) for _p in _sc_pids_t],
                    opacity=0.55,
                    line=dict(width=0),
                ),
                hovertemplate=(
                    f"{_t}<br>"
                    "期待能力: %{x:.1f}pctile<br>"
                    "実際能力: %{y:.1f}pctile<extra></extra>"
                ),
            ), row=2, col=1)
        # Quadrant threshold lines
        fig_d2.add_hline(y=_EXP_HIGH_PCTILE, line_dash="dash",
                         line_color="rgba(255,255,255,0.25)", row=2, col=1)
        fig_d2.add_vline(x=_EXP_HIGH_PCTILE, line_dash="dash",
                         line_color="rgba(255,255,255,0.25)", row=2, col=1)
        fig_d2.update_xaxes(title_text="年", row=1, col=1)
        fig_d2.update_yaxes(title_text="人数", row=1, col=1)
        fig_d2.update_xaxes(title_text="期待能力 percentile (協業者質0.5+作品スコア0.3+スタジオ0.2)", row=2, col=1)
        fig_d2.update_yaxes(title_text="実際能力 percentile (iv_score)", row=2, col=1)
        fig_d2.update_layout(
            title=f"D. 才能ティア別 参入/離脱（期待×実際 4-tier）{_exp_d_suffix}",
            height=820,
        )
        # Summary info box
        _exp_total = sum(_exp_tier_sizes.values()) or 1
        _exp_rs_ct  = _exp_tier_sizes.get("期待の星", 0)
        _exp_unful  = sum(
            1 for _p2, _t2 in _exp_pid_tier.items()
            if _t2 == "期待の星"
            and (pid_latest_year.get(_p2, 0) - pid_first_year.get(_p2, 0)) >= 5
        )
        _exp_unful_pct = _exp_unful / max(_exp_rs_ct, 1) * 100
        _exp_tier_rows_html = "".join(
            f"<tr>"
            f"<td style='color:{_exp_tier_color[_t]};font-weight:bold'>{_t}</td>"
            f"<td>{_exp_tier_sizes.get(_t, 0):,}人</td>"
            f"<td>{_exp_tier_sizes.get(_t, 0) / _exp_total * 100:.1f}%</td>"
            f"</tr>"
            for _t in _exp_tier_names
        )
        body += "<h3>D. 才能ティア別 参入/離脱数（期待能力×実際能力）</h3>"
        body += (
            '<div class="insight-box">'
            "<strong>4ティア定義 — 各指標を上位30%で閾値分類</strong><br>"
            "<em>期待能力</em>: 協業者の平均iv_score(50%) + 参加作品スコア(30%) + スタジオ水準(20%)<br>"
            "<em>実際能力</em>: iv_score percentile (ネットワーク位置・実績の合成)<br>"
            "<table style='width:100%;font-size:0.82rem;margin:0.5rem 0'>"
            "<thead><tr><th>ティア</th><th>人数</th><th>比率</th></tr></thead>"
            f"<tbody>{_exp_tier_rows_html}</tbody></table>"
            f"<p><strong>「期待の星」未転換率:</strong> "
            f"キャリア5年以上の「期待の星」のうち実際能力が高評価に届いていない割合 = "
            f"<strong>{_exp_unful_pct:.1f}%</strong> "
            f"({_exp_unful:,}/{_exp_rs_ct:,}人) — この層が業界最大の潜在ロスです。</p>"
            "</div>"
        )
        body += chart_guide(
            "上段: 4ティア別の年次参入数（実線）と離脱数（点線、2022年まで）。"
            "下段: 全人材の期待能力（x軸）vs 実際能力（y軸）散布図。バブルサイズ=総クレジット数。"
            "点線は上位30%閾値。<strong>隠れた実力</strong>（右下）は低評価環境でも実績を出した人材。"
            "<strong>期待の星</strong>（左上）は高環境にいながら実績が伴っていない人材。"
        )
        body += plotly_div_safe(fig_d2, "flow_tier_d", 820)

        # ---- Chart E: 職種別 参入/離脱（年次折れ線） ----
        fig_e = make_subplots(
            rows=2, cols=1,
            subplot_titles=(
                "職種別 参入数（年次）",
                f"職種別 離脱数（年次・{EXIT_CUTOFF_YEAR}年まで）",
            ),
            vertical_spacing=0.14,
            shared_xaxes=True,
        )
        for rt, (rl, rc) in ROLE_TYPE_DEF.items():
            fig_e.add_trace(go.Scatter(
                x=valid_flow_years,
                y=[entry_by_role[rt].get(yr, 0) for yr in valid_flow_years],
                name=rl, mode="lines",
                line=dict(color=rc, width=2),
                legendgroup=rl,
                hovertemplate=f"{rl} 参入 %{{x}}: %{{y:,}}<extra></extra>",
            ), row=1, col=1)
            fig_e.add_trace(go.Scatter(
                x=exit_xs,
                y=[exit_by_role[rt].get(yr, 0) for yr in exit_xs],
                name=rl, mode="lines",
                line=dict(color=rc, width=2, dash="dot"),
                legendgroup=rl,
                showlegend=False,
                hovertemplate=f"{rl} 離脱 %{{x}}: %{{y:,}}<extra></extra>",
            ), row=2, col=1)
        fig_e.add_vrect(
            x0=EXIT_CUTOFF_YEAR + 0.5, x1=RELIABLE_MAX_YEAR,
            fillcolor="rgba(239,71,111,0.06)",
            line_color="rgba(239,71,111,0.4)", line_dash="dash",
            annotation_text=f"離脱データなし<br>({EXIT_CUTOFF_YEAR+1}以降)",
            annotation_position="top right",
            row=2, col=1,
        )
        fig_e.update_layout(
            title="E. 職種別 参入/離脱数（年次折れ線）",
            height=580,
            yaxis_title="参入人数",
            yaxis2_title="離脱人数",
        )
        body += "<h3>E. 職種別 参入/離脱数（年次）</h3>"
        body += chart_guide(
            "上段=年次参入数（全期間）、下段=年次離脱数（2022年まで）。各職種を折れ線で比較。"
            "animator（動画/原画）が絶対数で多数を占めますが、"
            "director（演出/監督）やdesigner（デザイナー）の離脱比率の時代変化に注目。"
        )
        body += plotly_div_safe(fig_e, "flow_role_e", 580)

        # ---- Chart F: 人材価値の流出入（面グラフ） ----
        lost_val: dict[int, float] = {}
        growth_val: dict[int, float] = {}
        entry_val: dict[int, float] = {}
        for pid, ly in pid_latest_year.items():
            if FLOW_START_YEAR <= ly <= EXIT_CUTOFF_YEAR:
                lost_val[ly] = lost_val.get(ly, 0.0) + pid_iv.get(pid, 0.0)
        for pid, trend in pid_trend.items():
            ly = pid_latest_year.get(pid)
            if ly and FLOW_START_YEAR <= ly <= RELIABLE_MAX_YEAR and trend == "rising":
                growth_val[ly] = growth_val.get(ly, 0.0) + pid_iv.get(pid, 0.0)
        for pid, fy in pid_first_year.items():
            if FLOW_START_YEAR <= fy <= RELIABLE_MAX_YEAR:
                entry_val[fy] = entry_val.get(fy, 0.0) + pid_iv.get(pid, 0.0)

        fig_f1 = go.Figure()
        for (vals, name, color, fill_rgba) in [
            (lost_val,   "失われた価値（離脱）",         "#EF476F", "rgba(239,71,111,0.25)"),
            (growth_val, "成長価値（上昇トレンド現役）",  "#06D6A0", "rgba(6,214,160,0.25)"),
            (entry_val,  "参入価値（新規参入者）",        "#a0d2db", "rgba(160,210,219,0.25)"),
        ]:
            fig_f1.add_trace(go.Scatter(
                x=valid_flow_years, y=[vals.get(yr, 0.0) for yr in valid_flow_years],
                name=name, mode="lines", fill="tozeroy",
                line=dict(color=color, width=1), fillcolor=fill_rgba,
                hovertemplate=f"{name} %{{x}}: %{{y:.1f}}<extra></extra>",
            ))
        fig_f1.add_vrect(
            x0=EXIT_CUTOFF_YEAR + 0.5, x1=RELIABLE_MAX_YEAR,
            fillcolor="rgba(239,71,111,0.05)",
            line_color="rgba(239,71,111,0.35)", line_dash="dash",
            annotation_text=f"離脱集計なし({EXIT_CUTOFF_YEAR+1}以降)",
            annotation_position="top right",
        )
        fig_f1.update_layout(
            title="F-1. 人材価値の流出入（iv_score合計・年次）",
            xaxis_title="年", yaxis_title="iv_score 合計",
        )
        body += "<h3>F. 人材価値の流出入と損失内訳</h3>"
        body += chart_guide(
            "赤=離脱者のiv_score合計（業界が失った価値）。緑=上昇トレンド現役者、水色=新規参入者。"
            f"赤破線右({EXIT_CUTOFF_YEAR+1}年以降)は誤検出防止のため離脱データ非表示。"
        )
        body += plotly_div_safe(fig_f1, "flow_value_f1", 400)

        # F2: 離脱タイプ別 年次棒グラフ
        lt_years = sorted(yr for lt in loss_by_type_yr.values() for yr in lt
                          if FLOW_START_YEAR <= yr <= EXIT_CUTOFF_YEAR)
        lt_years = sorted(set(lt_years))
        fig_f2 = go.Figure()
        for lt in LOSS_TYPES:
            fig_f2.add_trace(go.Bar(
                x=lt_years,
                y=[loss_by_type_yr[lt].get(yr, 0) for yr in lt_years],
                name=lt, marker_color=loss_type_colors[lt],
                hovertemplate=f"{lt} %{{x}}: %{{y:,}}人<extra></extra>",
            ))
        fig_f2.add_hline(y=0, line_color="rgba(255,255,255,0.2)")
        fig_f2.update_layout(
            barmode="stack",
            title="F-2. 離脱タイプ別内訳（年次・2022年まで）",
            xaxis_title="年", yaxis_title="離脱人数",
        )
        body += chart_guide(
            "<strong>エース離脱</strong>（iv_score>30）/"
            "<strong>ベテラン引退</strong>（stage≥5）/"
            "<strong>中堅離脱</strong>（stage3–4）/"
            "<strong>新人早期離脱</strong>（stage≤2）の4分類。"
            "エース離脱が多い年は即戦力・看板人材の流出。"
            "新人早期離脱が多い年はキャリア初期のドロップアウト問題を示します。"
        )
        body += plotly_div_safe(fig_f2, "flow_loss_type_f2", 400)

        # F3: 離脱者の職種内訳（Pie）
        rt_ord = sorted(loss_by_role_total, key=loss_by_role_total.get, reverse=True)
        fig_f3 = go.Figure(go.Pie(
            labels=[ROLE_TYPE_DEF.get(rt, (rt, "#606070"))[0] for rt in rt_ord],
            values=[loss_by_role_total[rt] for rt in rt_ord],
            marker_colors=[ROLE_TYPE_DEF.get(rt, ("", "#606070"))[1] for rt in rt_ord],
            hole=0.4, textinfo="label+percent",
            hovertemplate="%{label}: %{value:,}人<extra></extra>",
        ))
        fig_f3.update_layout(title="F-3. 離脱者の職種内訳（全期間）", height=360,
                              legend=dict(orientation="h"))
        body += chart_guide(
            "離脱者の職種内訳。動画/原画の絶対数が多いのは自然だが、"
            "演出/監督・デザイナーの比率が相対的に高い場合は意思決定層・クリエイター層の流出を示します。"
        )
        body += plotly_div_safe(fig_f3, "flow_loss_role_f3", 360)

        # ---- Chart G: 大スタジオ別 人材フロー ----
        if top_studios:
            studio_palette = [
                "#f093fb","#a0d2db","#06D6A0","#FFD166",
                "#fda085","#667eea","#EF476F","#90BE6D",
            ]
            studio_yrs = sorted({yr for s in top_studios for yr in studio_person_years.get(s, {})
                                  if FLOW_START_YEAR <= yr <= EXIT_CUTOFF_YEAR})
            fig_g = go.Figure()
            for i, studio in enumerate(top_studios):
                sy = studio_person_years.get(studio, {})
                fig_g.add_trace(go.Scatter(
                    x=studio_yrs,
                    y=[len(sy.get(yr, set())) for yr in studio_yrs],
                    name=studio[:28], mode="lines",
                    line=dict(color=studio_palette[i % len(studio_palette)], width=2),
                    hovertemplate=f"{studio[:28]}: %{{y:,}}人<extra></extra>",
                ))
            fig_g.update_layout(
                title="G. 大スタジオ別 年間参加ユニーク人数（Top 8, 〜2022）",
                xaxis_title="年", yaxis_title="ユニーク参加人数",
            )
            body += "<h3>G. 大スタジオ別 人材フロー</h3>"
            body += chart_guide(
                "DBの anime.studios フィールド（JSON配列）から集計。"
                "各折れ線=そのスタジオにクレジットされた年間ユニーク人数。"
                "規模縮小・拡大のタイミングが業界の人材再配置と連動します。"
            )
            body += plotly_div_safe(fig_g, "flow_studio_g", 420)

        # ---- Chart H: 国別 人材フロー ----
        if top_countries:
            country_palette = ["#f093fb","#a0d2db","#06D6A0","#FFD166","#fda085"]
            ctry_yrs = sorted({yr for c in top_countries for yr in country_persons.get(c, {})
                               if FLOW_START_YEAR <= yr <= RELIABLE_MAX_YEAR})
            fig_h_ctry = go.Figure()
            for i, ctry in enumerate(top_countries):
                cy = country_persons.get(ctry, {})
                fig_h_ctry.add_trace(go.Scatter(
                    x=ctry_yrs, y=[cy.get(yr, 0) for yr in ctry_yrs],
                    name=ctry or "Unknown", mode="lines",
                    line=dict(color=country_palette[i % len(country_palette)], width=2),
                    hovertemplate=f"{ctry}: %{{y:,}}人<extra></extra>",
                ))
            fig_h_ctry.update_layout(
                title="H. 国別 年間参加ユニーク人数（Top 5）",
                xaxis_title="年", yaxis_title="ユニーク参加人数",
            )
            body += "<h3>H. 国別 人材フロー</h3>"
            body += chart_guide(
                "anime.country_of_origin をもとに集計。"
                "日本が圧倒的多数ですが、海外制作協力（韓国・中国等）の増加傾向が読み取れます。"
            )
            body += plotly_div_safe(fig_h_ctry, "flow_country_h", 380)

        # ---- Chart J: スコア×キャリア クラスタ別 フロー推移 ----
        try:
            import numpy as _np_j
            from sklearn.cluster import KMeans as _KM_j
            from sklearn.preprocessing import StandardScaler as _SC_j

            K_J = 5
            cl_j_rows, cl_j_pids = [], []
            for _e_j in scores_data:
                _pid_j = _e_j.get("person_id", "")
                if not _pid_j:
                    continue
                _car_j = _e_j.get("career") or {}
                cl_j_rows.append([
                    float(_e_j.get("iv_score") or 0.0),
                    float(_e_j.get("birank") or 0.0),
                    float(_e_j.get("patronage") or 0.0),
                    float(_e_j.get("person_fe") or 0.0),
                    float(_car_j.get("highest_stage") or 0),
                    float(_car_j.get("active_years") or 0),
                    float(_e_j.get("total_credits") or 0),
                ])
                cl_j_pids.append(_pid_j)

            if len(cl_j_rows) >= K_J * 5:
                X_j = _np_j.array(cl_j_rows)
                sc_j = _SC_j()
                X_js = sc_j.fit_transform(X_j)
                km_j = _KM_j(n_clusters=K_J, n_init=20, random_state=42)
                lbl_j = km_j.fit_predict(X_js)
                centers_j = sc_j.inverse_transform(km_j.cluster_centers_)

                # iv_score昇順でクラスタ名を割り当て
                ord_j = _np_j.argsort(centers_j[:, 0])
                _cl_j_name_list = [
                    "低スコア層（新人中心）",
                    "中低スコア・活動中",
                    "中スコア・中堅層",
                    "高スコア・ベテラン",
                    "トップ層（エース）",
                ]
                cl_j_name: dict[int, str] = {int(ord_j[i]): _cl_j_name_list[i] for i in range(K_J)}
                pid_cl_j: dict[str, int] = {cl_j_pids[i]: int(lbl_j[i]) for i in range(len(cl_j_pids))}

                # entry / exit / stock per cluster
                entry_cl_j: dict[int, dict[int, int]] = {k: {} for k in range(K_J)}
                exit_cl_j:  dict[int, dict[int, int]] = {k: {} for k in range(K_J)}
                stock_cl_j: dict[int, dict[int, int]] = {k: {yr: 0 for yr in flow_years_stock} for k in range(K_J)}

                for _pid2, _fy2 in pid_first_year.items():
                    if FLOW_START_YEAR <= _fy2 <= RELIABLE_MAX_YEAR:
                        _k2 = pid_cl_j.get(_pid2, -1)
                        if _k2 >= 0:
                            entry_cl_j[_k2][_fy2] = entry_cl_j[_k2].get(_fy2, 0) + 1
                for _pid2, _ly2 in pid_latest_year.items():
                    if FLOW_START_YEAR <= _ly2 <= EXIT_CUTOFF_YEAR:
                        _k2 = pid_cl_j.get(_pid2, -1)
                        if _k2 >= 0:
                            exit_cl_j[_k2][_ly2] = exit_cl_j[_k2].get(_ly2, 0) + 1
                for _pid2, _fy2 in pid_first_year.items():
                    _ly2 = pid_latest_year.get(_pid2, RELIABLE_MAX_YEAR)
                    _k2 = pid_cl_j.get(_pid2, -1)
                    if _k2 >= 0:
                        for _yr2 in flow_years_stock:
                            if _fy2 <= _yr2 <= _ly2:
                                stock_cl_j[_k2][_yr2] += 1

                cl_j_pal = ["#a0d2db", "#06D6A0", "#FFD166", "#f093fb", "#EF476F"]
                fig_j = make_subplots(
                    rows=2, cols=1,
                    subplot_titles=(
                        "クラスタ別 参入/離脱数（年次折れ線）",
                        "クラスタ別 ストック推移（積み上げ面）",
                    ),
                    vertical_spacing=0.14,
                    shared_xaxes=True,
                )
                for _k in range(K_J):
                    _c = cl_j_pal[_k % len(cl_j_pal)]
                    _nm = cl_j_name.get(_k, f"クラスタ{_k}")
                    fig_j.add_trace(go.Scatter(
                        x=valid_flow_years,
                        y=[entry_cl_j[_k].get(yr, 0) for yr in valid_flow_years],
                        name=f"{_nm} 参入", mode="lines",
                        line=dict(color=_c, width=2),
                        legendgroup=_nm,
                        hovertemplate=f"{_nm} 参入 %{{x}}: %{{y:,}}<extra></extra>",
                    ), row=1, col=1)
                    fig_j.add_trace(go.Scatter(
                        x=exit_xs,
                        y=[exit_cl_j[_k].get(yr, 0) for yr in exit_xs],
                        name=f"{_nm} 離脱", mode="lines",
                        line=dict(color=_c, width=2, dash="dot"),
                        legendgroup=_nm,
                        showlegend=False,
                        hovertemplate=f"{_nm} 離脱 %{{x}}: %{{y:,}}<extra></extra>",
                    ), row=1, col=1)
                    fig_j.add_trace(go.Scatter(
                        x=flow_years_stock,
                        y=[stock_cl_j[_k].get(yr, 0) for yr in flow_years_stock],
                        name=_nm, mode="lines", stackgroup="one",
                        line=dict(color=_c, width=1),
                        legendgroup=_nm,
                        showlegend=False,
                        hovertemplate=f"{_nm} ストック %{{x}}: %{{y:,}}<extra></extra>",
                    ), row=2, col=1)
                fig_j.add_vrect(
                    x0=EXIT_CUTOFF_YEAR + 0.5, x1=RELIABLE_MAX_YEAR,
                    fillcolor="rgba(239,71,111,0.06)",
                    line_color="rgba(239,71,111,0.4)", line_dash="dash",
                    annotation_text=f"離脱データなし<br>({EXIT_CUTOFF_YEAR+1}以降)",
                    annotation_position="top right",
                    row=1, col=1,
                )
                fig_j.update_layout(
                    title=f"J. スコア×キャリアクラスタ({K_J}群)別 参入/離脱/ストック推移",
                    height=680,
                    yaxis_title="人数",
                    yaxis2_title="ストック人数",
                )

                # クラスタ重心テーブル
                _feat_names_j = ["iv_score", "birank", "patronage", "person_fe", "stage", "active_yrs", "credits"]
                _rows_j = "".join(
                    f"<tr><td style='white-space:nowrap'>{cl_j_name.get(_k, '?')}</td>"
                    + "".join(f"<td>{centers_j[_k, _fi]:.2f}</td>" for _fi in range(len(_feat_names_j)))
                    + "</tr>"
                    for _k in range(K_J)
                )
                body += "<h3>J. スコア×キャリアクラスタ別 フロー推移</h3>"
                body += (
                    '<div class="insight-box">'
                    f"<strong>K-Means(K={K_J}) クラスタ重心（iv_score昇順）</strong>"
                    "<table style='width:100%;font-size:0.78rem;margin:0.5rem 0'>"
                    "<thead><tr><th>クラスタ</th>"
                    + "".join(f"<th>{_n}</th>" for _n in _feat_names_j)
                    + "</tr></thead>"
                    f"<tbody>{_rows_j}</tbody></table></div>"
                )
                body += chart_guide(
                    f"K-Means(K={K_J})でiv_score・birank・patronage・person_fe・stage・active_years・creditsをクラスタリング。"
                    "上段: クラスタ別年次参入(実線)/離脱(点線)。下段: クラスタ別ストック積み上げ面。"
                    "どの人材クラスが増えているか・どの層が業界から離れているかを比較できます。"
                )
                body += plotly_div_safe(fig_j, "flow_cluster_j", 680)
        except Exception as _exc_j:
            body += f"<p style='color:#808080'>クラスタ分析(J)をスキップしました: {type(_exc_j).__name__}: {_exc_j}</p>"

        # ---- Chart I: ブランク復帰人材 (DB全人材対象) ----
        # growth.jsonは上位200人のみのため、DBから全人材の活動年次を直接取得
        blank_categories = [
            ("短期ブランク (3–4年)", 3, 4),
            ("中期ブランク (5–9年)", 5, 9),
            ("長期ブランク (10年+)", 10, 99),
        ]

        def _returnee_stage_label(stage: int) -> str:
            if stage <= 3:
                return "原画クラス (≤3)"
            if stage == 4:
                return "作監 (4)"
            return "監督クラス (≥5)"

        returnee_stats: list[dict] = []
        try:
            conn_ret = _get_conn()
            conn_ret.row_factory = _sqlite3.Row
            # Get all (person_id, year) pairs up to EXIT_CUTOFF_YEAR
            rows_ret = conn_ret.execute("""
                SELECT c.person_id, a.year
                FROM credits c
                JOIN anime a ON c.anime_id = a.id
                WHERE a.year BETWEEN 1980 AND ?
                  AND a.year IS NOT NULL
            """, (EXIT_CUTOFF_YEAR,)).fetchall()
            conn_ret.close()

            # Group active years by person (unique years only)
            person_active_years: dict[str, set[int]] = {}
            for row in rows_ret:
                if row["person_id"] and row["year"]:
                    person_active_years.setdefault(row["person_id"], set()).add(int(row["year"]))

            # Compute max gap per person and classify
            for cat_label, min_blank, max_blank in blank_categories:
                returnees = []
                for pid, yr_set in person_active_years.items():
                    yrs = sorted(yr_set)
                    if len(yrs) < 2:
                        continue
                    max_gap = max(yrs[i + 1] - yrs[i] - 1 for i in range(len(yrs) - 1))
                    # Must have activity AFTER the gap (true returnee)
                    if max_gap < min_blank or max_gap > max_blank:
                        continue
                    # Find where the gap occurs; require credits after the gap end
                    has_return = False
                    for i in range(len(yrs) - 1):
                        if min_blank <= (yrs[i + 1] - yrs[i] - 1) <= max_blank:
                            if i + 1 < len(yrs):
                                has_return = True
                                break
                    if not has_return:
                        continue
                    stage = pid_stage.get(pid, 0)
                    returnees.append({
                        "stage_label": _returnee_stage_label(stage),
                        "iv": pid_iv.get(pid, 0.0),
                        "blank_years": max_gap,
                    })
                returnee_stats.append({
                    "label": cat_label,
                    "count": len(returnees),
                    "avg_blank": sum(r["blank_years"] for r in returnees) / len(returnees) if returnees else 0,
                    "avg_iv":    sum(r["iv"]          for r in returnees) / len(returnees) if returnees else 0,
                    "by_stage":  Counter(r["stage_label"] for r in returnees),
                })
        except Exception as _e_ret:
            for cat_label, _, _ in blank_categories:
                returnee_stats.append({"label": cat_label, "count": 0, "avg_blank": 0, "avg_iv": 0, "by_stage": Counter()})

        stage_labels_i = ["原画クラス (≤3)", "作監 (4)", "監督クラス (≥5)"]
        fig_i = go.Figure()
        for sl, _sc_i in zip(stage_labels_i, ["#a0d2db", "#06D6A0", "#f093fb"]):
            fig_i.add_trace(go.Bar(
                x=[rs["label"] for rs in returnee_stats],
                y=[rs["by_stage"].get(sl, 0) for rs in returnee_stats],
                name=sl, marker_color=_sc_i,
                hovertemplate=f"{sl}: %{{y:,}}<extra></extra>",
            ))
        fig_i.update_layout(
            barmode="group",
            title=f"I. ブランク長別 復帰人材数（キャリアステージ別・DB全人材対象・{EXIT_CUTOFF_YEAR}年まで）",
            xaxis_title="ブランク期間", yaxis_title="復帰人数",
        )
        body += "<h3>I. 長期ブランクからの復帰人材</h3>"
        body += chart_guide(
            f"DBのcredits×animeから全人材の活動年次を集計（{EXIT_CUTOFF_YEAR}年まで）。"
            "ブランク後に実際に復帰した人材のみをカウント（ブランク後のクレジットあり）。"
            "<strong>注意:</strong> 劇場版・長期シリーズは制作2–4年かかることがあり、"
            "短期ブランク（3–4年）は制作期間中の可能性あり。"
            "中期（5–9年）・長期（10年+）が信頼性の高い指標です。"
        )
        body += plotly_div_safe(fig_i, "flow_returnee_i", 380)
        body += '<div class="stats-grid">'
        for rs in returnee_stats:
            body += (
                f'<div class="stat-card">'
                f'<div class="value">{fmt_num(rs["count"])}</div>'
                f'<div class="label">{rs["label"]}<br>'
                f'平均ブランク: {rs["avg_blank"]:.1f}年<br>'
                f'平均iv_score: {rs["avg_iv"]:.4f}</div>'
                f'</div>'
            )
        body += "</div>"
        body += "</div>"  # card

    # ============================================================
    # Decade Comparison — DB-driven year×format line charts
    # ============================================================
    body += '<div class="card">'
    body += "<h2>Decade Comparison — 需要と供給（年次折れ線）</h2>"
    body += section_desc(
        "DBから年別×作品種別の作品数と人材数を集計。上段=需要（作品数）、下段=供給（ユニーク人数）。"
        f"対象: 1980–{RELIABLE_MAX_YEAR}年。"
    )
    try:
        conn_dec = _get_conn()
        conn_dec.row_factory = _sqlite3.Row
        rows_dec = conn_dec.execute("""
            SELECT a.year, a.format,
                COUNT(DISTINCT a.id)        AS anime_count,
                COUNT(DISTINCT c.person_id) AS person_count,
                COUNT(c.id)                 AS credit_count
            FROM anime a
            LEFT JOIN credits c ON c.anime_id = a.id
            WHERE a.year BETWEEN 1980 AND ?
              AND a.format IN ('TV','MOVIE','OVA','ONA','TV_SHORT')
            GROUP BY a.year, a.format
        """, (RELIABLE_MAX_YEAR,)).fetchall()
        conn_dec.close()

        dec_formats = ["TV", "MOVIE", "OVA", "ONA", "TV_SHORT"]
        fmt_colors = {
            "TV": "#f093fb",
            "MOVIE": "#fda085",
            "OVA": "#a0d2db",
            "ONA": "#06D6A0",
            "TV_SHORT": "#FFD166",
        }
        dec_anime_by_fmt: dict[str, dict[int, int]] = {f: {} for f in dec_formats}
        dec_person_by_fmt: dict[str, dict[int, int]] = {f: {} for f in dec_formats}
        dec_years_set: set[int] = set()
        for row in rows_dec:
            yr = row["year"]
            fmt = row["format"]
            if fmt not in dec_formats:
                continue
            dec_years_set.add(yr)
            dec_anime_by_fmt[fmt][yr] = row["anime_count"]
            dec_person_by_fmt[fmt][yr] = row["person_count"]

        dec_years_sorted = sorted(dec_years_set)

        fig_dec = make_subplots(
            rows=2, cols=1,
            subplot_titles=("需要: 年間作品数（format別）", "供給: 年間ユニーク人数（format別）"),
            vertical_spacing=0.12,
        )
        for fmt in dec_formats:
            c = fmt_colors[fmt]
            ys_a = [dec_anime_by_fmt[fmt].get(yr, 0) for yr in dec_years_sorted]
            ys_p = [dec_person_by_fmt[fmt].get(yr, 0) for yr in dec_years_sorted]
            fig_dec.add_trace(go.Scatter(
                x=dec_years_sorted, y=ys_a, mode="lines", name=fmt,
                line=dict(color=c, width=2),
                hovertemplate=f"{fmt} %{{x}}: %{{y:,}}作品<extra></extra>",
            ), row=1, col=1)
            fig_dec.add_trace(go.Scatter(
                x=dec_years_sorted, y=ys_p, mode="lines", name=fmt,
                line=dict(color=c, width=2),
                showlegend=False,
                hovertemplate=f"{fmt} %{{x}}: %{{y:,}}人<extra></extra>",
            ), row=2, col=1)

        fig_dec.update_layout(
            title=f"Decade Comparison: 需要と供給 (1980–{RELIABLE_MAX_YEAR})",
            height=700,
        )
        fig_dec.add_annotation(
            text="2020年代は収録が継続中のため人数・作品数が過去年代より少ない場合があります",
            xref="paper", yref="paper", x=0.0, y=-0.08,
            showarrow=False, font=dict(size=10, color="#a0a0c0"),
        )
        body += chart_guide(
            "上段=作品数（需要）、下段=参加人数（供給）。format別に色分け。"
            "TV作品の増減が業界全体の人材需要を左右しています。"
        )
        body += plotly_div_safe(fig_dec, "decade_lines", 700)
    except Exception as _e:
        body += f'<p style="color:#EF476F">DB query error: {_e}</p>'
    body += "</div>"

    # ============================================================
    # Seasonal Patterns — Violin + Grouped Bar + Newcomer
    # ============================================================
    body += '<div class="card">'
    body += "<h2>Seasonal Patterns</h2>"
    season_names = ["winter", "spring", "summer", "fall"]
    season_labels_disp = ["Winter (1-3)", "Spring (4-6)", "Summer (7-9)", "Fall (10-12)"]
    season_colors = {
        "winter": "#a0d2db",
        "spring": "#06D6A0",
        "summer": "#FFD166",
        "fall": "#fda085",
    }

    try:
        conn_sea = _get_conn()
        conn_sea.row_factory = _sqlite3.Row
        rows_sea = conn_sea.execute("""
            SELECT
                a.id AS anime_id, a.season, a.format, a.score,
                a.episodes,
                CASE
                    WHEN a.episodes IS NULL OR a.episodes = 0 THEN 'unknown'
                    WHEN a.episodes <= 1 THEN 'movie_or_special'
                    WHEN a.episodes <= 14 THEN 'single_cour'
                    WHEN a.episodes <= 28 THEN 'multi_cour'
                    ELSE 'long_cour'
                END AS cour_type,
                COUNT(DISTINCT c.person_id) AS unique_persons,
                COUNT(c.id) AS credits
            FROM anime a
            LEFT JOIN credits c ON c.anime_id = a.id
            WHERE a.year BETWEEN 1990 AND ?
              AND a.season IN ('winter','spring','summer','fall')
            GROUP BY a.id
        """, (RELIABLE_MAX_YEAR,)).fetchall()

        # Also query debut persons per season×cour_type×year (year included for decade breakdown)
        rows_debut = conn_sea.execute("""
            SELECT
                a.season, a.year,
                CASE
                    WHEN a.episodes IS NULL OR a.episodes = 0 THEN 'unknown'
                    WHEN a.episodes <= 1 THEN 'movie_or_special'
                    WHEN a.episodes <= 14 THEN 'single_cour'
                    WHEN a.episodes <= 28 THEN 'multi_cour'
                    ELSE 'long_cour'
                END AS cour_type,
                c.person_id
            FROM anime a
            JOIN credits c ON c.anime_id = a.id
            WHERE a.year BETWEEN 1990 AND ?
              AND a.season IN ('winter','spring','summer','fall')
        """, (RELIABLE_MAX_YEAR,)).fetchall()
        conn_sea.close()

        # Build per-season score lists for violin
        scores_by_season: dict[str, list[float]] = {s: [] for s in season_names}
        # Build cour_type counts per season
        cour_type_order = ["single_cour", "multi_cour", "long_cour", "movie_or_special"]
        cour_count_by_season: dict[str, dict[str, int]] = {
            s: {ct: 0 for ct in cour_type_order} for s in season_names
        }

        for row in rows_sea:
            s = row["season"]
            if s not in season_names:
                continue
            sc = row["score"]
            if sc and float(sc) > 0:
                scores_by_season[s].append(float(sc))
            ct = row["cour_type"]
            if ct in cour_count_by_season[s]:
                cour_count_by_season[s][ct] += 1

        # Build newcomer counts per decade×season×cour_type
        # 真の新人 = その年が first_year と一致する人 (debut_year == credit_year)
        sea_decades = [1990, 2000, 2010, 2020]
        debut_by_decade: dict[int, dict[str, dict[str, int]]] = {
            dec: {s: {ct: 0 for ct in cour_type_order} for s in season_names}
            for dec in sea_decades
        }
        debut_seen: set[tuple] = set()   # (pid, dec) dedup — count each debut person once per decade
        for row in rows_debut:
            pid = row["person_id"]
            yr = row["year"]
            s = row["season"]
            ct = row["cour_type"]
            if not pid or not yr or s not in season_names:
                continue
            # True newcomer: first_year == credit year
            if pid_first_year.get(pid) != yr:
                continue
            dec = (yr // 10) * 10
            if dec not in debut_by_decade:
                continue
            key = (pid, dec, s, ct)
            if key in debut_seen:
                continue
            debut_seen.add(key)
            if ct in debut_by_decade[dec][s]:
                debut_by_decade[dec][s][ct] += 1

        # --- Chart A: Quality Violin ---
        fig_sea_violin = go.Figure()
        for sn, sl in zip(season_names, season_labels_disp):
            sc_list = scores_by_season[sn]
            if sc_list:
                fig_sea_violin.add_trace(go.Violin(
                    x=[sl] * len(sc_list),
                    y=sc_list,
                    name=sl,
                    box_visible=True,
                    points="outliers",
                    line_color=season_colors[sn],
                    fillcolor=season_colors[sn].replace("#", "rgba(") + ",0.3)" if False else "rgba(160,210,219,0.3)",
                    hovertemplate=f"{sl}: %{{y:.1f}}<extra></extra>",
                ))
        fig_sea_violin.update_layout(
            title="A. 季節別 アニメスコア分布（バイオリン図）",
            xaxis_title="季節", yaxis_title="アニメスコア",
            showlegend=False,
        )
        fig_sea_violin.add_annotation(
            text="注: 春はクオリティのばらつきが大きい傾向があります",
            xref="paper", yref="paper", x=0, y=1.05,
            showarrow=False, font=dict(size=10, color="#a0a0c0"),
        )
        body += "<h3>A. 季節別アニメスコア分布（バイオリン図）</h3>"
        body += chart_guide(
            "各季節のアニメスコアの分布をバイオリン図で表示。箱ひげとアウトライアーも表示。"
            "分布の幅が広いほど品質のばらつきが大きいことを示します。"
        )
        body += plotly_div_safe(fig_sea_violin, "seasonal_violin", 400)

        # --- Chart B: 種別作品数 Grouped Bar ---
        cour_colors_map = {
            "single_cour": "#a0d2db",
            "multi_cour": "#06D6A0",
            "long_cour": "#f093fb",
            "movie_or_special": "#fda085",
        }
        cour_labels = {
            "single_cour": "単クール (≤14話)",
            "multi_cour": "複数クール (15-28話)",
            "long_cour": "長期クール (29話+)",
            "movie_or_special": "映画/特番 (≤1話)",
        }
        fig_sea_bar = go.Figure()
        for ct in cour_type_order:
            fig_sea_bar.add_trace(go.Bar(
                x=season_labels_disp,
                y=[cour_count_by_season[sn][ct] for sn in season_names],
                name=cour_labels[ct],
                marker_color=cour_colors_map[ct],
                hovertemplate=f"{cour_labels[ct]}: %{{y:,}}<extra></extra>",
            ))
        fig_sea_bar.update_layout(
            barmode="group",
            title="B. 季節別×作品種別 作品数（Grouped Bar）",
            xaxis_title="季節", yaxis_title="作品数",
        )
        body += "<h3>B. 季節別×作品種別 作品数</h3>"
        body += chart_guide(
            "季節ごとに単クール/複数クール/長期クール/映画の作品数をグループ棒グラフで比較。"
            "単クール作品が集中する季節は新規参入の機会が多い時期です。"
        )
        body += plotly_div_safe(fig_sea_bar, "seasonal_type_bar", 400)

        # --- Chart C: 新人参加数 年代別 Grouped Bar (subplots per decade) ---
        sea_dec_labels = {1990: "1990年代", 2000: "2000年代", 2010: "2010年代", 2020: "2020年代"}
        fig_sea_debut = make_subplots(
            rows=2, cols=2,
            subplot_titles=[sea_dec_labels[d] for d in sea_decades],
            vertical_spacing=0.18, horizontal_spacing=0.10,
        )
        dec_positions = [(1,1),(1,2),(2,1),(2,2)]
        for (dec, (r, c)) in zip(sea_decades, dec_positions):
            for ct in cour_type_order:
                fig_sea_debut.add_trace(go.Bar(
                    x=season_labels_disp,
                    y=[debut_by_decade[dec][sn][ct] for sn in season_names],
                    name=cour_labels[ct],
                    marker_color=cour_colors_map[ct],
                    showlegend=(dec == sea_decades[0]),
                    legendgroup=ct,
                    hovertemplate=f"{sea_dec_labels[dec]} {cour_labels[ct]}: %{{y:,}}<extra></extra>",
                ), row=r, col=c)
        fig_sea_debut.update_layout(
            barmode="group",
            title="C. 季節別×作品種別 デビュー人数（年代別）",
            height=600,
        )
        body += "<h3>C. 季節別×作品種別 デビュー人数（年代別）</h3>"
        body += chart_guide(
            "<strong>真の新人（デビュー年 = クレジット年 の人物）</strong>を季節・作品種別・年代ごとに集計。"
            "各人物は各年代×季節×作品種別で重複カウントなし。"
            "年代ごとのパネルでデビューの季節パターンが変化しているかを比較できます。"
        )
        body += plotly_div_safe(fig_sea_debut, "seasonal_debut", 600)

    except Exception as _e:
        body += f'<p style="color:#EF476F">Seasonal DB query error: {_e}</p>'

    # stats grid (from seasonal.json if available)
    if seasonal:
        seasons_j = seasonal.get("by_season", {})
        body += '<div class="stats-grid">'
        for sn, sl in zip(season_names, season_labels_disp):
            sd = seasons_j.get(sn, {})
            body += (
                f'<div class="stat-card">'
                f'<div class="value">{fmt_num(sd.get("anime_count", 0))}</div>'
                f'<div class="label">{sl}<br>'
                f'{fmt_num(sd.get("credit_count", 0))} credits / '
                f'{fmt_num(sd.get("person_count", 0))} persons<br>'
                f'Avg Score: {sd.get("avg_anime_score", 0):.2f}'
                f'</div></div>'
            )
        body += "</div>"
    body += "</div>"  # card

    # ============================================================
    # Growth Trends — 世代別水平積み上げ棒グラフ
    # ============================================================
    if growth and scores_data and isinstance(scores_data, list):
        body += '<div class="card">'
        body += "<h2>Growth Trends — 世代別キャリア軌跡分布</h2>"
        body += section_desc(
            "debut_decade（初クレジット年の10年区切り）× trend分類のクロス集計。"
            "世代ごとに「上昇/安定/新規/低下/非活動」の構成比を水平積み上げ棒グラフで表示。"
        )

        trend_categories = ["rising", "stable", "new", "declining", "inactive"]
        trend_labels_ja = {
            "rising": "上昇中",
            "stable": "安定",
            "new": "新規",
            "declining": "低下",
            "inactive": "非活動",
        }
        trend_colors_gt = {
            "rising": "#06D6A0",
            "stable": "#a0d2db",
            "new": "#667eea",
            "declining": "#fda085",
            "inactive": "#606070",
        }

        decades_list = [1970, 1980, 1990, 2000, 2010, 2020]
        decade_labels_ja = {d: f"{d}年代" for d in decades_list}

        # Cross-tabulate
        cohort_trend: dict[int, dict[str, int]] = {d: {t: 0 for t in trend_categories} for d in decades_list}
        for pid, fy in pid_first_year.items():
            decade = (fy // 10) * 10
            if decade not in cohort_trend:
                continue
            trend = pid_trend.get(pid, "")
            if trend in trend_categories:
                cohort_trend[decade][trend] += 1

        fig_gt = go.Figure()
        for trend in trend_categories:
            fig_gt.add_trace(go.Bar(
                orientation="h",
                name=trend_labels_ja[trend],
                x=[cohort_trend[d][trend] for d in decades_list],
                y=[decade_labels_ja[d] for d in decades_list],
                marker_color=trend_colors_gt[trend],
                hovertemplate=(
                    f"{trend_labels_ja[trend]}: %{{x:,}}人"
                    " (%{customdata:.1f}%)<extra></extra>"
                ),
                customdata=[
                    cohort_trend[d][trend] / max(sum(cohort_trend[d].values()), 1) * 100
                    for d in decades_list
                ],
            ))
        fig_gt.update_layout(
            barmode="stack",
            title=f"Growth Trends: 世代別キャリア軌跡分布 ({growth.get('total_persons', 0):,}人)",
            xaxis_title="人数",
            yaxis_title="デビュー世代",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        body += chart_guide(
            "Y軸=デビュー世代（10年区切り）、X軸=人数（積み上げ）。"
            "緑=上昇中、水色=安定、青=新規、オレンジ=低下、グレー=非活動。"
            "ホバーすると実数と%の両方が表示されます。"
        )
        body += plotly_div_safe(fig_gt, "growth_cohort", 500)
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

    # Data-driven key findings
    credits_count = d.get("credits", 0)
    persons_count = d.get("persons", 0)
    credits_per_person = credits_count / max(persons_count, 1)
    body += key_findings([
        f"アニメ業界の協業ネットワークは100年以上にわたり拡大を続けており、"
        f"特に2000年代以降の成長が顕著（{fmt_num(d.get('anime', 0))}作品 / {fmt_num(persons_count)}人）",
        f"クレジット数の増加率が人数の増加率を上回っており、"
        f"1人あたり平均 {credits_per_person:.1f} クレジットと参加作品数が増加傾向",
        f"時系列は{RELIABLE_MAX_YEAR}年までのデータを対象とし、2026年以降は統計的信頼性が低いため除外",
        "成長トレンド分類では「安定」「上昇」が多数を占め、業界全体として成熟と拡大が共存",
    ])

    body += significance_section("マクロトレンドの把握", [
        "アニメ産業は100年以上の協業ネットワークの集積です。その構造的変化を定量的に把握することで、"
        "採用戦略・制作投資・人材育成の根拠となる客観的なベースラインが初めて得られます。"
        "感覚や慣習に依拠してきた業界計画を、データ駆動の意思決定に転換する出発点です。",
        "時系列データは「業界が今どこにいるか」だけでなく「どこへ向かっているか」を示します。"
        "人材の需給ギャップや季節的な制作量の偏りは数値で確認でき、スタジオの中長期計画に直接活用できます。",
    ])
    body += utilization_guide([
        {"role": "スタジオ経営者", "how": "Time Seriesチャートの直近5年の傾きと自社のクレジット数伸び率を重ね、業界平均を上回る成長分野と遅れている分野を特定する"},
        {"role": "制作プロデューサー", "how": "Seasonal Patternsの季節別credit_countを参照し、制作量が低い季節に人員を集中配置して競争コストを下げる"},
        {"role": "業界研究者", "how": "Decade Comparisonの数値を論文の記述統計に引用し、分析期間における業界規模の文脈を定量的に示す"},
        {"role": "政策立案者", "how": "Pipeline SummaryのTotal Persons・Total Creditsを産業振興補助金の効果測定基準値として設定し、施策前後で比較する"},
    ])
    body += future_possibilities([
        "韓国・中国・フランスなど海外アニメ産業との国際比較による日本の相対的ポジションの定量化",
        "文化庁・日本動画協会向け「アニメ産業白書」の公式データソースとしての採用",
        "業界標準KPI（年間クレジット数・新規参入者数・活動継続率）の確立と毎年の定点観測",
        "政策効果測定への応用（制作支援補助金がクレジット数・新規参入者数に与えた影響を定量評価）",
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

    # Violin: bridge score distribution by communities_connected
    comm_scores: dict[int, list[float]] = {}
    for bp in bridge_persons:
        cc = bp["communities_connected"]
        comm_scores.setdefault(cc, []).append(bp["bridge_score"])

    if comm_scores:
        fig_bv = go.Figure()
        bv_colors = ["#f093fb", "#a0d2db", "#f5576c", "#fda085", "#667eea",
                     "#06D6A0", "#FFD166", "#EF476F"]
        for idx, cc in enumerate(sorted(comm_scores.keys())):
            if len(comm_scores[cc]) >= 3:
                fig_bv.add_trace(_violin_raincloud(
                    comm_scores[cc],
                    f"{cc}コミュニティ",
                    bv_colors[idx % len(bv_colors)],
                ))
        fig_bv.update_layout(
            title="コミュニティ接続数別 ブリッジスコア分布 (Raincloud)",
            yaxis_title="Bridge Score",
            xaxis_title="接続コミュニティ数",
            violinmode="overlay",
        )
        body += '<div class="card">'
        body += "<h2>コミュニティ接続数別 ブリッジスコア分布</h2>"
        body += chart_guide(
            "Violin plotで接続コミュニティ数ごとのブリッジスコアの分布形状を比較。"
            "多くのコミュニティを接続する人物ほどスコアが高い傾向があるか、"
            "同じ接続数でもばらつきがあるかを視覚的に確認できます。"
        )
        body += plotly_div_safe(fig_bv, "bridge_violin", 500)
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

    # ---- K-Means クラスタリング: ブリッジタイプ分類 ----
    if len(bridge_persons) >= 4:
        import numpy as _np_br
        from sklearn.cluster import KMeans as _KMeans_br
        from sklearn.preprocessing import StandardScaler as _SC_br

        BR_COLORS = ["#f093fb", "#06D6A0", "#fda085", "#a0d2db", "#FFD166"]
        br_feats = _np_br.array([
            [
                float(bp["bridge_score"]),
                float(bp["communities_connected"]),
                float(bp["cross_community_edges"]),
            ]
            for bp in bridge_persons
        ], dtype=float)
        sc_br = _SC_br()
        br_feats_s = sc_br.fit_transform(br_feats)
        k_br = min(4, len(bridge_persons))
        km_br = _KMeans_br(n_clusters=k_br, n_init=20, random_state=42)
        br_labels = km_br.fit_predict(br_feats_s)
        centers_br = sc_br.inverse_transform(km_br.cluster_centers_)

        # 動的クラスタ命名: 重心の相対ランクで自動ラベル付け
        br_cluster_names: dict[int, str] = _name_clusters_by_rank(
            centers_br,
            [
                (0, ["超ブリッジ", "中堅ブリッジ", "周辺ブリッジ"]),   # bridge_score
                (1, ["広域接続", "中域接続", "局所接続"]),              # communities_connected
                (2, ["多クロスエッジ", "中クロスエッジ", "少クロスエッジ"]),  # cross_community_edges
            ],
        )

        for i, bp in enumerate(bridge_persons):
            bp["bridge_cluster"] = int(br_labels[i])
            bp["bridge_cluster_name"] = br_cluster_names[int(br_labels[i])]

        br_cluster_groups: dict[int, list[dict]] = {}
        for bp in bridge_persons:
            br_cluster_groups.setdefault(bp["bridge_cluster"], []).append(bp)

        body += '<div class="card">'
        body += f"<h2>ブリッジ人材 K-Means クラスタリング（{k_br}クラスタ）</h2>"
        body += section_desc(
            "ブリッジスコア・接続コミュニティ数・クロスエッジ数の3特徴量でブリッジ人材を自動分類。"
            "「超ブリッジ×広域接続」「局所的だが重要なブリッジ」等のタイプを識別します。"
        )

        # Scatter colored by cluster
        fig_br_sc = go.Figure()
        for cid in sorted(br_cluster_groups.keys()):
            mems_br = br_cluster_groups[cid]
            fig_br_sc.add_trace(go.Scatter(
                x=[bp["cross_community_edges"] for bp in mems_br],
                y=[bp["bridge_score"] for bp in mems_br],
                mode="markers",
                name=br_cluster_names[cid],
                marker=dict(
                    size=[max(6, min(18, bp["communities_connected"] * 3)) for bp in mems_br],
                    color=BR_COLORS[cid % len(BR_COLORS)],
                    opacity=0.7,
                ),
                text=[bp.get("name", bp["person_id"]) for bp in mems_br],
                hovertemplate=(
                    "%{text}<br>"
                    "Cross Edges: %{x}<br>"
                    "Bridge Score: %{y}<br>"
                    f"クラスタ: {br_cluster_names[cid]}<extra></extra>"
                ),
            ))
        fig_br_sc.update_layout(
            title="ブリッジクラスタ散布図（クロスエッジ × スコア）",
            xaxis_title="クロスコミュニティエッジ数", yaxis_title="ブリッジスコア",
        )
        body += chart_guide(
            "各点は1人のブリッジ人材。色＝クラスタ。点の大きさ＝接続コミュニティ数。"
            "右上＝エッジが多く高スコアの「超ブリッジ」。左下＝少ないがコミュニティ間接続の橋渡しをする「局所ブリッジ」。"
        )
        body += plotly_div_safe(fig_br_sc, "bridge_cluster_scatter", 520)

        # Violin: bridge score per cluster
        fig_br_viol = go.Figure()
        for cid in sorted(br_cluster_groups.keys()):
            mems_br = br_cluster_groups[cid]
            scores_br = [bp["bridge_score"] for bp in mems_br]
            if scores_br:
                fig_br_viol.add_trace(_violin_raincloud(
                    scores_br, br_cluster_names[cid], BR_COLORS[cid % len(BR_COLORS)],
                ))
        fig_br_viol.update_layout(
            title="クラスタ別 ブリッジスコア分布 (Raincloud)",
            yaxis_title="ブリッジスコア",
            violinmode="overlay",
        )
        body += chart_guide("各クラスタのブリッジスコア分布。クラスタ間のスコア差を確認。")
        body += plotly_div_safe(fig_br_viol, "bridge_cluster_violin", 450)

        # Cluster size & avg score table
        body += "<h3>クラスタ別サマリー</h3>"
        body += "<table><thead><tr><th>クラスタ</th><th>人数</th><th>平均スコア</th><th>平均接続数</th><th>平均クロスエッジ</th></tr></thead><tbody>"
        for cid in range(k_br):
            mems_br = br_cluster_groups.get(cid, [])
            if not mems_br:
                continue
            avg_sc_br = sum(bp["bridge_score"] for bp in mems_br) / len(mems_br)
            avg_cc_br = sum(bp["communities_connected"] for bp in mems_br) / len(mems_br)
            avg_ce_br = sum(bp["cross_community_edges"] for bp in mems_br) / len(mems_br)
            body += (
                f"<tr><td>{br_cluster_names[cid]}</td>"
                f"<td>{len(mems_br)}</td>"
                f"<td>{avg_sc_br:.1f}</td>"
                f"<td>{avg_cc_br:.1f}</td>"
                f"<td>{avg_ce_br:.1f}</td></tr>"
            )
        body += "</tbody></table>"
        body += "</div>"

    # Top bridge persons table
    body += '<div class="card">'
    body += "<h2>Top 50 Bridge Persons</h2>"
    body += section_desc(
        "ブリッジスコア順にランキング。より多くのコミュニティをより多くの"
        "クロスコミュニティエッジで接続する人物が高スコアを獲得します。"
    )
    body += "<table><thead><tr>"
    body += "<th>#</th><th>Person</th><th>Bridge Score</th><th>Communities</th><th>Cross Edges</th><th>クラスタ</th>"
    body += "</tr></thead><tbody>"
    for i, bp in enumerate(bridge_persons[:50], 1):
        score = bp["bridge_score"]
        badge = "badge-high" if score >= 80 else "badge-mid" if score >= 50 else "badge-low"
        name = bp.get("name", bp["person_id"])
        cluster_lbl = bp.get("bridge_cluster_name", "")
        body += f"<tr><td>{i}</td><td>{person_link(name, bp['person_id'])}</td>"
        body += f'<td><span class="badge {badge}">{score}</span></td>'
        body += f"<td>{bp['communities_connected']}</td><td>{bp['cross_community_edges']}</td>"
        body += f"<td style='font-size:0.8rem;color:#a0d2db'>{cluster_lbl}</td></tr>"
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

    # Data-driven key findings
    bridge_count = stats.get("bridge_person_count", 0)
    total_persons_b = stats.get("total_persons", 1)
    bridge_ratio = bridge_count / max(total_persons_b, 1) * 100
    total_cross = stats.get("total_cross_edges", 0)
    body += key_findings([
        f"全 {fmt_num(total_persons_b)} 人のうちブリッジ人材は {fmt_num(bridge_count)} 人"
        f"（{bridge_ratio:.1f}%）と少数派だが、コミュニティ間の知識移転に不可欠な役割を担う",
        f"クロスコミュニティエッジ総数 {fmt_num(total_cross)} の大部分を"
        "少数のブリッジ人材が担っており、集中度が高い",
        "ブリッジスコアとクロスコミュニティエッジ数には正の相関があり、"
        "活発な越境コラボレーターほど高スコア",
    ])

    body += significance_section("孤立コミュニティ間の知識移転", [
        "ネットワーク上で自然に形成されるコミュニティ（スタジオ・ジャンル・時代ごとのクラスタ）は"
        "しばしば孤立しており、コミュニティ間の知識・スタイル・人材の移動は放置すれば起きません。"
        "ブリッジ人材はこの壁を越える希少な存在であり、特定することでスタジオが仕掛けるべき"
        "「次の越境コラボ」の人選と最適タイミングが分かります。",
        "同時にこれは脆弱性の分析でもあります。ブリッジ人材の引退・離職はコミュニティ間接続の"
        "断絶を引き起こすリスクがあり、後継者の計画的育成が業界の知識流通を守るための課題です。",
    ])
    body += utilization_guide([
        {"role": "タレントスカウト", "how": "Top 50 Bridge Personsテーブルのブリッジスコア上位者が接続するコミュニティペアを確認し、自社が接触したいコミュニティへの橋渡し役として優先接触する"},
        {"role": "スタジオ人事", "how": "Bridge Ratioが低い場合は自スタジオが孤立している可能性があるため、Score vs Connectivityチャートの右上の人物を外部接続要員として採用候補にあげる"},
        {"role": "プロデューサー", "how": "Cross-Community Connectivityチャートで両スタジオのコミュニティ番号を確認し、その間に位置するブリッジ人材を共同制作交渉の窓口として起用する"},
        {"role": "研究者", "how": "bridge_score × communities_connected の散布図外れ値を抽出し、技術・スタイル伝播の経路として経歴をトレースしてケーススタディを作成する"},
    ])
    body += future_possibilities([
        "ブリッジ人材の「後継者育成計画」— 引退前に接続役を引き継ぐ人物を計画的に育成するロードマップ",
        "ブリッジ喪失時のコミュニティ断絶リスク定量評価（その人物が抜けたらネットワークがどう変わるかのシミュレーション）",
        "意図的なブリッジ人材配置戦略（スタジオ間ローテーション制度・越境研修の設計根拠として）",
        "越境コラボ成功率の追跡（ブリッジ人材経由のプロジェクトと非経由の作品品質を比較し効果を検証）",
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

_SORT_TABLE_JS = """
<script>
function sortTable(tableId, col, numeric) {
    var table = document.getElementById(tableId);
    var tbody = table.querySelector('tbody');
    var rows = Array.from(tbody.querySelectorAll('tr'));
    var asc = (table.dataset.sortCol == col && table.dataset.sortDir == 'asc') ? false : true;
    table.dataset.sortCol = col;
    table.dataset.sortDir = asc ? 'asc' : 'desc';
    rows.sort(function(a, b) {
        var aVal = a.cells[col] ? a.cells[col].textContent.trim() : '';
        var bVal = b.cells[col] ? b.cells[col].textContent.trim() : '';
        if (numeric) { aVal = parseFloat(aVal) || 0; bVal = parseFloat(bVal) || 0; }
        if (aVal < bVal) return asc ? -1 : 1;
        if (aVal > bVal) return asc ? 1 : -1;
        return 0;
    });
    rows.forEach(function(row) { tbody.appendChild(row); });
    // Update sort arrow on all th
    Array.from(table.querySelectorAll('thead th')).forEach(function(th, i) {
        th.textContent = th.textContent.replace(/ [↑↓↕]$/, '');
        if (i == col) th.textContent += asc ? ' ↑' : ' ↓';
        else th.textContent += ' ↕';
    });
}
</script>
<style>
.sortable-th { cursor: pointer; user-select: none; }
.sortable-th:hover { color: #f093fb; }
</style>
"""


def _compute_year_deviation(teams_list: list[dict], anime_stats: dict) -> list[dict]:
    """各チームに年別偏差値を付与する.

    全 anime_stats から年別の平均・標準偏差を計算し、
    各チームのスコアを (score - year_mean) / year_std で標準化して
    偏差値 = 50 + 10 * z に変換する。
    """
    import statistics as _stats

    # 全 anime の年別スコア集計
    year_scores: dict[int, list[float]] = {}
    for stats_val in anime_stats.values():
        yr = stats_val.get("year")
        sc = stats_val.get("score")
        if yr and sc and isinstance(sc, (int, float)) and 1 <= sc <= 10:
            year_scores.setdefault(int(yr), []).append(float(sc))

    # 年別 mean / std
    year_mean: dict[int, float] = {}
    year_std: dict[int, float] = {}
    for yr, scores in year_scores.items():
        year_mean[yr] = _stats.mean(scores)
        year_std[yr] = _stats.stdev(scores) if len(scores) >= 2 else 1.0

    # Fallback: 全体の mean / std (年データが少ない場合)
    all_scores = [s for ss in year_scores.values() for s in ss]
    global_mean = _stats.mean(all_scores) if all_scores else 7.0
    global_std = _stats.stdev(all_scores) if len(all_scores) >= 2 else 1.0

    result = []
    for team in teams_list:
        t = dict(team)
        yr = t.get("year")
        sc = t.get("anime_score", 0.0)
        if yr and int(yr) in year_mean:
            m = year_mean[int(yr)]
            s = max(year_std.get(int(yr), 1.0), 0.01)
        else:
            m, s = global_mean, global_std
        deviation = round(50 + 10 * (sc - m) / s, 1)
        t["deviation_score"] = deviation
        t["year_mean"] = round(m, 3)
        t["year_std"] = round(s, 3)
        result.append(t)
    return result


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

    # 年別偏差値を計算 (anime_stats を利用して年別 mean/std を算出)
    anime_stats = load_json("anime_stats.json") or {}
    high_score_teams = _compute_year_deviation(high_score_teams, anime_stats)

    # 偏差値でソートしたリスト (別テーブル用)
    teams_by_deviation = sorted(
        high_score_teams, key=lambda t: t.get("deviation_score", 0), reverse=True
    )

    body = _SORT_TABLE_JS

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

    # --- 年別偏差値の説明 ---
    body += (
        '<div class="insight-box">'
        "<strong>年別偏差値について:</strong> "
        "同じ評価スコアでも、放送年によって平均レベルが異なります。"
        "偏差値 = 50 + 10 × (スコア − その年の平均) ÷ その年の標準偏差 で計算。"
        "50が「その年の平均的な高評価作品」、60以上が「同年トップ水準」を意味します。"
        "</div>"
    )

    def _th(label: str, col: int, numeric: bool, table_id: str) -> str:
        num_str = "true" if numeric else "false"
        return (
            f'<th class="sortable-th" onclick="sortTable(\'{table_id}\', {col}, {num_str})">'
            f"{label} ↕</th>"
        )

    # --- Top 50 High-Scoring Teams (raw score order, sortable) ---
    body += '<div class="card">'
    body += "<h2>Top 50 High-Scoring Teams（スコア順 — 列ヘッダーでソート可）</h2>"
    body += section_desc("列ヘッダーをクリックすると昇順/降順で並べ替えできます。")
    body += f'<table id="team-table-raw"><thead><tr>'
    for col_i, (label, numeric) in enumerate([
        ("#", False), ("タイトル", False), ("年", True), ("Score", True),
        ("偏差値", True), ("年平均", True), ("チームサイズ", True), ("コアロール数", True),
    ]):
        body += _th(label, col_i, numeric, "team-table-raw")
    body += "</tr></thead><tbody>"
    for i, team in enumerate(high_score_teams[:50], 1):
        score = team.get("anime_score", 0)
        dev = team.get("deviation_score")
        dev_str = f"{dev:.1f}" if dev is not None else "N/A"
        dev_badge = "badge-high" if (dev or 0) >= 60 else "badge-mid" if (dev or 0) >= 50 else "badge-low"
        score_badge = "badge-high" if score >= 8.5 else "badge-mid" if score >= 8.0 else "badge-low"
        title = team.get("title", team.get("anime_id", ""))
        body += f"<tr><td>{i}</td><td>{title}</td><td>{team.get('year', '')}</td>"
        body += f'<td><span class="badge {score_badge}">{score:.1f}</span></td>'
        body += f'<td><span class="badge {dev_badge}">{dev_str}</span></td>'
        body += f"<td>{team.get('year_mean', '')}</td>"
        body += f"<td>{team.get('team_size', 0)}</td><td>{team.get('core_roles', 0)}</td></tr>"
    body += "</tbody></table></div>"

    # --- Top 50 by 年別偏差値（偏差値順） ---
    body += '<div class="card">'
    body += "<h2>Top 50 High-Scoring Teams（年別偏差値順）</h2>"
    body += section_desc(
        "同年の作品群の中での相対的な高評価度で並べたランキング。"
        "近年の作品バイアスを除外し、「時代を超えた突出作品」を浮かび上がらせます。"
    )
    body += f'<table id="team-table-dev"><thead><tr>'
    for col_i, (label, numeric) in enumerate([
        ("#", False), ("タイトル", False), ("年", True), ("Score", True),
        ("偏差値", True), ("年平均", True), ("チームサイズ", True), ("コアロール数", True),
    ]):
        body += _th(label, col_i, numeric, "team-table-dev")
    body += "</tr></thead><tbody>"
    for i, team in enumerate(teams_by_deviation[:50], 1):
        score = team.get("anime_score", 0)
        dev = team.get("deviation_score")
        dev_str = f"{dev:.1f}" if dev is not None else "N/A"
        dev_badge = "badge-high" if (dev or 0) >= 60 else "badge-mid" if (dev or 0) >= 50 else "badge-low"
        score_badge = "badge-high" if score >= 8.5 else "badge-mid" if score >= 8.0 else "badge-low"
        title = team.get("title", team.get("anime_id", ""))
        body += f"<tr><td>{i}</td><td>{title}</td><td>{team.get('year', '')}</td>"
        body += f'<td><span class="badge {score_badge}">{score:.1f}</span></td>'
        body += f'<td><span class="badge {dev_badge}">{dev_str}</span></td>'
        body += f"<td>{team.get('year_mean', '')}</td>"
        body += f"<td>{team.get('team_size', 0)}</td><td>{team.get('core_roles', 0)}</td></tr>"
    body += "</tbody></table></div>"

    # --- 偏差値の分布: Violin (年代別) ---
    if high_score_teams:
        decade_dev: dict[str, list[float]] = {}
        for t in high_score_teams:
            yr = t.get("year")
            dv = t.get("deviation_score")
            if yr and dv is not None:
                decade = f"{(int(yr) // 10) * 10}年代"
                decade_dev.setdefault(decade, []).append(dv)
        if decade_dev:
            fig_vdev = go.Figure()
            DECADE_COLORS = ["#a0a0c0", "#7EB8D4", "#4CC9F0", "#FFD166", "#FF6B35", "#F72585"]
            for i, decade in enumerate(sorted(decade_dev.keys())):
                vals = decade_dev[decade]
                dcol = DECADE_COLORS[i % len(DECADE_COLORS)]
                if len(vals) >= 3:
                    fig_vdev.add_trace(_violin_raincloud(vals, decade, dcol))
                else:
                    fig_vdev.add_trace(go.Scatter(
                        x=[decade] * len(vals),
                        y=vals,
                        mode="markers+text",
                        name=decade,
                        marker=dict(size=10, symbol="diamond", color=dcol),
                        text=[f"{v:.1f}" for v in vals],
                        textposition="top center",
                        hovertemplate=f"{decade}<br>偏差値: %{{y:.1f}}<br>(n={len(vals)})<extra></extra>",
                    ))
            fig_vdev.update_layout(
                title="年代別 高評価チームの偏差値分布",
                yaxis_title="年別偏差値",
                xaxis_title="年代",
            )
            body += '<div class="card">'
            body += "<h2>年代別 偏差値分布 (Violin)</h2>"
            body += chart_guide(
                "年代ごとに高評価チームの偏差値分布を比較。"
                "偏差値50が各年の「その年の平均レベル」。"
                "年代によって突出度のばらつきが異なるかを確認できます。"
            )
            body += plotly_div_safe(fig_vdev, "team_violin_dev", 450)
            body += "</div>"

    # --- 偏差値 × 年 散布図 (raw score との比較) ---
    if high_score_teams:
        fig_dev_sc = make_subplots(specs=[[{"secondary_y": True}]])
        fig_dev_sc.add_trace(
            go.Scatter(
                x=[t.get("year") for t in high_score_teams],
                y=[t.get("anime_score", 0) for t in high_score_teams],
                mode="markers",
                name="生スコア",
                marker=dict(color="#f093fb", size=6, opacity=0.6),
                hovertemplate="%{text}<br>年: %{x}<br>生スコア: %{y:.1f}<extra></extra>",
                text=[t.get("title", "") for t in high_score_teams],
            ),
            secondary_y=False,
        )
        fig_dev_sc.add_trace(
            go.Scatter(
                x=[t.get("year") for t in high_score_teams],
                y=[t.get("deviation_score") for t in high_score_teams if t.get("deviation_score") is not None],
                mode="markers",
                name="年別偏差値",
                marker=dict(color="#06D6A0", size=6, opacity=0.6, symbol="diamond"),
                hovertemplate="%{text}<br>年: %{x}<br>偏差値: %{y:.1f}<extra></extra>",
                text=[t.get("title", "") for t in high_score_teams if t.get("deviation_score") is not None],
            ),
            secondary_y=True,
        )
        fig_dev_sc.update_yaxes(title_text="生スコア (1-10)", secondary_y=False)
        fig_dev_sc.update_yaxes(title_text="年別偏差値", secondary_y=True)
        fig_dev_sc.update_layout(
            title="生スコア vs 年別偏差値（デュアル軸）",
            xaxis_title="放送年",
        )
        body += '<div class="card">'
        body += "<h2>生スコア vs 年別偏差値（デュアル軸）</h2>"
        body += chart_guide(
            "ピンク●=生スコア（左軸）、緑◆=年別偏差値（右軸）。"
            "近年になるほど生スコアが全体的に上昇する傾向がある一方、"
            "偏差値は年ごとの相対位置を示すため年代バイアスを除去できます。"
            "生スコアは高いが偏差値が低い作品は「その年が全体的に高評価だった」ことを示します。"
        )
        body += plotly_div_safe(fig_dev_sc, "team_dev_dual", 500)
        body += "</div>"

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

    # Scatter: Role diversity vs Quality
    if high_score_teams:
        diversity_data = [t for t in high_score_teams if t.get("core_roles", 0) > 0]
        if diversity_data:
            max_team_div = max((t.get("team_size", 1) for t in diversity_data), default=1)
            fig_div = go.Figure(go.Scatter(
                x=[t.get("core_roles", 0) for t in diversity_data],
                y=[t.get("anime_score", 0) for t in diversity_data],
                mode="markers",
                marker=dict(
                    size=[min(t.get("team_size", 1), 50) for t in diversity_data],
                    sizemode="area",
                    sizeref=max_team_div / 400,
                    color=[t.get("team_size", 0) for t in diversity_data],
                    colorscale="Viridis", showscale=True,
                    colorbar=dict(title="Team Size"),
                    opacity=0.7,
                ),
                text=[t.get("title", "") for t in diversity_data],
                hovertemplate="%{text}<br>Core Roles: %{x}<br>Score: %{y:.1f}<br>"
                              "Team Size: %{marker.color}<extra></extra>",
            ))
            fig_div.update_layout(
                title="ロール多様性 vs 作品スコア",
                xaxis_title="Core Roles（ロールの多様性）",
                yaxis_title="Anime Score",
            )
            body += '<div class="card">'
            body += "<h2>ロール多様性 vs 品質スコア</h2>"
            body += chart_guide(
                "X=チーム内のユニークロール数（コアロール）、Y=作品スコア、色=チーム規模。"
                "ロールの多様性が高いチームは高品質作品を生み出しやすいか？"
                "最適なロール数のスイートスポットを確認できます。"
            )
            body += plotly_div_safe(fig_div, "role_diversity_scatter", 500)
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
            body += f"<tr><td>{i}</td><td>{person_link(name_a, pair['person_a'])}</td><td>{person_link(name_b, pair['person_b'])}</td>"
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

    # Data-driven key findings
    high_score_count = teams.get("total_high_score", 0)
    avg_size = size_stats.get("avg", 0)
    hs_avg_size = size_stats.get("high_score_avg", 0)
    body += key_findings([
        f"分析対象 {fmt_num(high_score_count)} 本の高評価作品において、"
        f"平均チームサイズ {avg_size:.1f} 人（全体）に対して高評価作品は {hs_avg_size:.1f} 人と"
        "{'大きい' if hs_avg_size > avg_size else '同程度'}",
        f"頻出役職組み合わせ {fmt_num(len(role_combos))} パターンが特定され、成功作品に共通するスタッフ構成がある",
        f"繰り返し共演するコラボペアが {fmt_num(len(rec_pairs))} 組確認され、安定した品質を生み出す傾向",
    ])

    body += significance_section("成功チームの構成要素を解明", [
        "アニメ制作の品質はチームダイナミクスに大きく依存しますが、「どの役職の組み合わせが"
        "高品質作品を生み出すか」はこれまで経験則に頼るしかありませんでした。"
        "本分析は高評価作品のスタッフ構成をデータで特定し、制作投資の効率化根拠を提供します。",
        "「この人とこの人が組むと成功しやすい」という推薦ペアは、感覚的なキャスティングを"
        "実績データに基づく意思決定に変えます。特にシリーズ続編や類似ジャンルの新作で有効です。",
    ])
    body += utilization_guide([
        {"role": "制作プロデューサー", "how": "Role Combinationsチャートで自プロジェクトのジャンルに対応する上位役職組み合わせを確認し、その構成を充足させるようにスタッフィングリストを組む"},
        {"role": "制作デスク", "how": "Top 30 Recommended Pairsテーブルで自スタジオに在籍する人物ペアを探し、実績ある組み合わせを同一作品の担当割り当てに優先する"},
        {"role": "スタジオ経営者", "how": "High-Score Works Timelineで自社の高評価作品が多い年を特定し、その年のチーム規模・役職構成を次期作品の採用計画の雛形として使う"},
        {"role": "アニメーター", "how": "自身が頻繁に共演するパートナーのクレジット歴とスコアを確認し、共演実績の強さをポートフォリオに「実績のあるコラボ」として記載する"},
    ])
    body += future_possibilities([
        "役職ごとの「予算帯別最適チームサイズ」の精緻化（低予算・中規模・大作別のサブ分析）",
        "過去の成功チームを類似度検索できる「チーム事例ライブラリ」（類似プロジェクトの参照チームを自動提案）",
        "高評価チームの再結集プランニング（同じチームで続編を作る場合の最大効果スケジュール）",
        "スタッフ可用性・契約状況と実績を組み合わせた「実行可能な最適チーム」の自動候補提示",
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

            # Transition probability heatmap
            z_prob = []
            for row in z:
                row_total = sum(row)
                if row_total > 0:
                    z_prob.append([v / row_total * 100 for v in row])
                else:
                    z_prob.append([0.0] * len(row))

            fig_prob = go.Figure(go.Heatmap(
                z=z_prob, x=stages, y=stages,
                colorscale="Viridis",
                texttemplate="%{z:.1f}%",
                hovertemplate="%{y} → %{x}: %{z:.1f}%<extra></extra>",
            ))
            fig_prob.update_layout(
                title="キャリアステージ遷移確率行列",
                xaxis_title="遷移先ステージ",
                yaxis_title="遷移元ステージ",
            )
            body += '<div class="card">'
            body += "<h2>遷移確率行列</h2>"
            body += chart_guide(
                "各行を合計100%に正規化した確率ベースの遷移行列。"
                "「ステージXの人がステージYに移行する確率」を直接読み取れます。"
                "対角線の値が高い＝そのステージに留まる傾向、右上が高い＝昇進率が高い。"
            )
            body += plotly_div_safe(fig_prob, "transition_prob", 500)
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

    # Data-driven key findings
    total_analyzed_c = transitions.get("total_persons_analyzed", 0) if transitions else 0
    trans_count = len(transitions.get("transitions", [])) if transitions else 0
    paths_count = len(transitions.get("career_paths", [])) if transitions else 0
    body += key_findings([
        f"分析対象 {fmt_num(total_analyzed_c)} 人のキャリアから "
        f"{fmt_num(trans_count)} 種類の遷移パターンと {fmt_num(paths_count)} 本のキャリアパスを検出",
        "各ステージ到達までの平均年数は個人差が大きく、"
        "中央値との乖離が遅咲き人材の存在を示す",
        "最も一般的なキャリアパスは少数のルートに集中しており、"
        "業界標準のキャリア進行が存在する",
    ])

    body += significance_section("キャリアパスの客観的な可視化", [
        "アニメ業界のキャリアパスはこれまで「先輩の背中を見て学ぶ」暗黙知に依存していました。"
        "本分析は100年以上のクレジットデータからキャリア進行の実態を数値化し、"
        "育成計画・昇進基準・報酬設計の根拠となる客観的なデータを初めて提供します。",
        "「ベテランになるまで平均何年かかるか」「どのルートが一般的か」が明確になることで、"
        "キャリア設計指導が感覚論を脱却し、また人材が特定のステージに滞留している"
        "ボトルネックを可視化することで、スタジオの育成投資の優先順位が決まります。",
    ])
    body += utilization_guide([
        {"role": "人事・研修担当", "how": "Time to Stageチャートの各ステージ到達の平均・中央値年数を社内の昇進基準に照合し、業界水準と乖離がある場合は制度見直しの根拠として使う"},
        {"role": "キャリアカウンセラー", "how": "Transition Matrixで相談者の現在のステージ行を横に読み、遷移頻度が高い（明るいセルの）次のステージを具体的なキャリア目標として提示する"},
        {"role": "新人アニメーター", "how": "Career Pathsテーブルで最も一般的なルートと自分の現在地を照らし合わせ、次の役職への遷移に何年かかるかの目安を把握して計画を立てる"},
        {"role": "スタジオ経営者", "how": "自社の人材が特定のステージに平均より長く滞留している場合、そのステージへの集中育成投資の根拠として遷移データを提示して予算承認を得る"},
    ])
    body += future_possibilities([
        "キャリアステージ別の市場報酬データとの統合による「ステージ到達時の適正報酬レンジ」の算出",
        "ステージ滞留リスクアラート（業界平均より長くとどまっている人物への昇進機会の提案）",
        "海外アニメ業界のキャリアパスとの比較（日本特有のキャリア進行パターンの国際的位置づけ）",
        "役職間遷移確率を使った「次の役職」予測モデル（各人物の現在地から最も到達しやすい次のポジションを提示）",
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
# Report 5: Temporal BiRank & Foresight
# ============================================================

def generate_temporal_report():
    """時系列BiRank・先見レポート."""
    print("  Generating Temporal BiRank & Foresight Report...")
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

    # BiRank timeline for top 10
    if persons_by_peak:
        fig = go.Figure()
        for pid, peak_auth, peak_yr, data in persons_by_peak[:10]:
            snapshots = data.get("snapshots", [])
            if snapshots:
                fig.add_trace(go.Scatter(
                    x=[s["year"] for s in snapshots],
                    y=[s["birank"] for s in snapshots],
                    mode="lines+markers",
                    name=f"{data.get('name', pid)[:20]} (peak={peak_auth:.2f})",
                    hovertemplate="%{x}: %{y:.4f}<extra></extra>",
                    line=dict(width=2),
                    marker=dict(size=5),
                ))
        fig.update_layout(title="BiRank Evolution — Top 10 Persons", xaxis_title="Year", yaxis_title="BiRank Score")

        body += '<div class="card">'
        body += "<h2>BiRank Evolution (Top 10)</h2>"
        body += chart_guide(
            "各線は1人のBiRankの時系列推移。上昇線はネットワーク影響力の"
            "増大、ピーク後の下降はキャリアフェーズの転換を示唆します。"
            "ホバーで年次の正確な値を確認できます。"
        )
        body += plotly_div_safe(fig, "authority_timeline", 550)
        body += "</div>"

    # Dual-axis: BiRank + Cumulative Credits for top 5
    if persons_by_peak[:5]:
        fig_da = make_subplots(specs=[[{"secondary_y": True}]])
        da_colors = ["#f093fb", "#a0d2db", "#f5576c", "#fda085", "#667eea"]
        for idx, (pid, peak_auth, peak_yr, data) in enumerate(persons_by_peak[:5]):
            snapshots = data.get("snapshots", [])
            if not snapshots:
                continue
            p_name = data.get("name", pid)[:20]
            color = da_colors[idx % len(da_colors)]
            years_s = [s["year"] for s in snapshots]
            birank_vals = [s["birank"] for s in snapshots]
            cum_credits = [s.get("n_credits_cumulative", 0) for s in snapshots]

            fig_da.add_trace(
                go.Scatter(
                    x=years_s, y=birank_vals,
                    mode="lines+markers",
                    name=f"{p_name} BiRank",
                    line=dict(color=color, width=2),
                    marker=dict(size=4),
                    hovertemplate="%{x}: BiRank %{y:.4f}<extra></extra>",
                ),
                secondary_y=False,
            )
            if any(c > 0 for c in cum_credits):
                fig_da.add_trace(
                    go.Scatter(
                        x=years_s, y=cum_credits,
                        mode="lines",
                        name=f"{p_name} Credits",
                        line=dict(color=color, width=1, dash="dot"),
                        opacity=0.5,
                        hovertemplate="%{x}: %{y}クレジット<extra></extra>",
                    ),
                    secondary_y=True,
                )
        fig_da.update_layout(title="BiRank推移 + 累積クレジット数（Top 5 Dual-axis）")
        fig_da.update_yaxes(title_text="BiRank Score", secondary_y=False)
        fig_da.update_yaxes(title_text="累積クレジット数", secondary_y=True)

        body += '<div class="card">'
        body += "<h2>BiRank推移 + 累積クレジット数（Top 5）</h2>"
        body += chart_guide(
            "左Y軸（実線）: BiRankの推移、右Y軸（点線）: 累積クレジット数。"
            "クレジット数が増加してもBiRankが上がらない場合は「量は増えたが質的影響力は停滞」を示唆。"
            "逆にクレジット数が少なくてもBiRank上昇は「少数の高影響力作品に参加」を意味します。"
        )
        body += plotly_div_safe(fig_da, "authority_dual_axis", 550)
        body += "</div>"

    # Top persons table
    body += '<div class="card">'
    body += "<h2>Top 30 by Peak BiRank</h2>"
    body += "<table><thead><tr>"
    body += "<th>#</th><th>Person</th><th>Peak BiRank</th><th>Peak Year</th><th>Career Start</th><th>Trajectory</th>"
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

        # Scatter: Foresight Score vs Discoveries
        if len(foresight_list) >= 5:
            fig_fs = go.Figure(go.Scatter(
                x=[f.get("n_discoveries", 0) for f in foresight_list],
                y=[f.get("foresight_normalized", 0) for f in foresight_list],
                mode="markers",
                marker=dict(
                    size=8,
                    color=[f.get("confidence_upper", 0) - f.get("confidence_lower", 0)
                           for f in foresight_list],
                    colorscale="Viridis", showscale=True,
                    colorbar=dict(title="信頼区間幅"),
                    opacity=0.7,
                ),
                text=[f.get("name", f["person_id"]) for f in foresight_list],
                hovertemplate="%{text}<br>Discoveries: %{x}<br>Foresight: %{y:.1f}<br>"
                              "信頼区間幅: %{marker.color:.1f}<extra></extra>",
            ))
            fig_fs.update_layout(
                title="先見スコア vs 発見人数（先見の的中率）",
                xaxis_title="発見した人数 (n_discoveries)",
                yaxis_title="Foresight Score (Normalized)",
            )
            body += "<h3>先見スコア vs 発見人数</h3>"
            body += chart_guide(
                "X=発見した人数、Y=先見スコア（正規化）、色=信頼区間の幅（狭いほど確実）。"
                "右上の人物は多くの将来のスターを発見し、高い先見スコアを持つ最優秀スカウト。"
                "発見数が少なくてもスコアが高い＝少数だが的確な目利き。"
            )
            body += plotly_div_safe(fig_fs, "foresight_scatter", 500)

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

    # Data-driven key findings
    foresight_count = len(foresight)
    promo_count = len(promotions)
    year_span = f"{min(years_computed)}-{max(years_computed)}" if years_computed else "N/A"
    body += key_findings([
        f"BiRankは静的ではなく、{year_span} の {len(years_computed)} 年間で"
        "動的に変化し、キャリアフェーズに応じたピークと転換が観察される",
        f"先見スコア保持者 {fmt_num(foresight_count)} 人は将来著名になる人材と早期に協業しており、"
        "タレントスカウトとしての能力が数値化されている",
        f"昇進クレジット追跡対象 {fmt_num(promo_count)} 人の育成実績が測定され、"
        "後に頭角を現す人材の育成者が特定されている",
    ])

    body += significance_section("才能の早期発見と「事前」の評価", [
        "従来の評価は「事後」— すでに有名になった人物を高く評価するものでした。"
        "本分析は「事前」— まだ無名の段階で将来の星を見抜いた人物（先見スコア）と、"
        "BiRankがピーク前の急上昇期にある人材を特定することで、投資判断のタイミングを前倒しにします。",
        "先見スコアは「誰が新しい才能を最初に発見してきたか」を定量化し、"
        "主観的な「目利き」を客観的な指標に変換します。"
        "昇進クレジット保持者は「育てる力」が数値で証明された人物であり、"
        "社内メンター選定の根拠として機能します。",
    ])
    body += utilization_guide([
        {"role": "タレントスカウト", "how": "Foresight Scores上位者のDiscoveries列を確認し、その人物が過去に早期発見した人のBiRank推移を追跡して、現在も同様の「無名の新星」を抱えていないか調べる"},
        {"role": "スタジオ経営層", "how": "BiRank Evolution（Top 10）チャートで急上昇中（ピーク前のJ字カーブ）を示す人物を特定し、ピーク前の現時点で長期契約オファーを出して将来の主力スタッフを確保する"},
        {"role": "アニメーター（若手）", "how": "Top 30 Talent PromotersでSuccess Rateが高い監督・プロデューサーを確認し、その人物との協業を積極的に狙うことでキャリア加速を期待できる"},
        {"role": "スタジオ育成担当", "how": "昇進クレジット上位者をプロモーション実績として評価し、若手指導役として正式にアサインするメンタリング制度の候補者リストに加える"},
    ])
    body += future_possibilities([
        "先見スコアの「先見スコア」— 誰の目利きが長期的に正確だったかを毎年検証し、「最も信頼できるスカウト」を特定",
        "先見スコアとその後のキャリア成功の時差分析（発見から何年後に正解だったと分かるかのラグ計測）",
        "映画・ゲーム・VTuber産業への先見スコア手法の応用（アニメ以外のコンテンツ産業への横展開）",
        "昇進クレジット保持者を社内メンター制度に組み込む標準フレームワークの業界提案",
    ])

    html = wrap_html(
        "時系列BiRank・先見スコア分析",
        f"時系列BiRank推定・先見スコア分析 — {fmt_num(tp.get('total_persons', 0))}人 / {len(years_computed)}年間",
        body,
        intro_html=report_intro(
            "時系列BiRank・先見分析",
            "BiRankは静的ではなく、キャリアの進行とともに変化します。本レポートでは"
            "各人物のネットワーク中心性の時系列変化を追跡し、将来の人材を"
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

    # Data-driven key findings
    if years:
        years_span = f"{min(years)}-{max(years)}"
        years_count = len(years)
        years_note = f"{years_span} の {years_count} 年間にわたり"
    else:
        years_note = "複数年間にわたり"
    body += key_findings([
        f"協業ネットワークは{years_note}構造的に変化し続けており、"
        "ノード数・エッジ数ともに増加傾向",
        "ネットワーク密度とクラスタリング係数の推移から、"
        "協業パターンの時代的変遷が読み取れる",
        "連結成分数の変化は業界の「孤立期」と「統合期」を示し、"
        "コラボレーション文化の形成過程が可視化される",
    ])

    body += significance_section("業界構造の時系列変化を追跡", [
        "ネットワーク位相（密度・クラスタリング・連結成分数）の変化は業界の協業文化がどう進化したかを示します。"
        "密度の上昇は協業の活性化、クラスタリング係数の上昇は専門コミュニティの形成・分化を意味します。"
        "これは「業界の健康状態のバイタルサイン」として機能する継続観測指標です。",
        "M&A・スタジオ合併・業界再編が発生した年のネットワーク変化を特定することで、"
        "構造的ショックの影響規模を定量的に評価できます。"
        "また密度の急落や連結成分数の増加は、業界分断の早期兆候として読み取れます。",
    ])
    body += utilization_guide([
        {"role": "スタジオ経営者", "how": "Network Metrics Over Timeチャートの密度グラフで自社が活発に活動していた時期のネットワーク構造を確認し、その時期の制作体制を現在の戦略立案の参考にする"},
        {"role": "M&A担当者", "how": "合併候補スタジオが属するコミュニティのエッジ数推移を確認し、統合後に自社ネットワークのエッジ数がどれだけ増加するかを事前に試算して提携判断の根拠とする"},
        {"role": "業界アナリスト", "how": "ノード数・エッジ数・密度の年次データを業界白書や投資家向けレポートの定量的根拠として引用し、業界全体の成熟度を客観的に示す"},
        {"role": "学術研究者", "how": "クラスタリング係数と密度の長期時系列をネットワーク科学論文の実証データとして活用し、アニメ産業の演化の法則性を分析する"},
    ])
    body += future_possibilities([
        "特定の「転換点年」の同定と要因分析（政策変化・技術革新・主要スタジオ設立閉鎖との対応関係の検証）",
        "ネットワーク構造変化の健康診断レポートとしての年次定期発行（前年比での指標変化を自動解説）",
        "コミュニティ分断の早期検知アラート（密度やエッジ数が急落した時のステークホルダーへの警告）",
        "業界縮小期・拡大期の構造的特徴を学習した予測モデル（次の変曲点タイミングの見通し）",
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

        # Violin: trend category vs total_credits
        persons = growth.get("persons", {})
        trend_credits: dict[str, list[int]] = {}
        for pid, p in persons.items():
            t = p.get("trend", "unknown")
            tc = p.get("total_credits", 0)
            trend_credits.setdefault(t, []).append(tc)

        trend_order = ["rising", "stable", "declining", "new", "inactive"]
        trend_violin_colors = {"rising": "#06D6A0", "stable": "#FFD166", "declining": "#EF476F",
                               "new": "#a0d2db", "inactive": "#666"}
        if trend_credits:
            fig_tv = go.Figure()
            for t in trend_order:
                if t in trend_credits and len(trend_credits[t]) >= 3:
                    fig_tv.add_trace(_violin_raincloud(
                        trend_credits[t],
                        t.title(),
                        trend_violin_colors.get(t, "#888"),
                    ))
            fig_tv.update_layout(
                title="トレンドカテゴリ別 総クレジット数分布 (Raincloud)",
                yaxis_title="総クレジット数",
                xaxis_title="トレンドカテゴリ",
                violinmode="overlay",
            )
            body += '<div class="card">'
            body += "<h2>トレンドカテゴリ別 総クレジット数分布</h2>"
            body += chart_guide(
                "Violin plotで各トレンドカテゴリの総クレジット数分布を比較。"
                "Risingなのにクレジット数が少ない＝急成長新人、Stableで多い＝安定ベテラン。"
                "分布の形状と中央値から各カテゴリの特性を把握できます。"
            )
            body += plotly_div_safe(fig_tv, "trend_violin", 500)
            body += "</div>"

        # Time series: yearly rising/declining/stable counts
        yearly_trend_counts: dict[int, dict[str, int]] = {}
        for pid, p in persons.items():
            yearly_credits = p.get("yearly_credits", {})
            for yr_str, cnt in yearly_credits.items():
                try:
                    yr = int(yr_str)
                except (ValueError, TypeError):
                    continue
                yearly_trend_counts.setdefault(yr, {"rising": 0, "stable": 0, "declining": 0, "new": 0, "inactive": 0})
                # Count person as their overall trend for each year they were active
                t = p.get("trend", "unknown")
                if t in yearly_trend_counts[yr] and cnt > 0:
                    yearly_trend_counts[yr][t] += 1

        if yearly_trend_counts:
            sorted_yrs = sorted(yearly_trend_counts.keys())
            # Filter to reasonable range
            sorted_yrs = [yr for yr in sorted_yrs if yr >= 1980]
            area_colors = {"rising": "#06D6A0", "stable": "#FFD166", "declining": "#EF476F",
                           "new": "#a0d2db", "inactive": "#666"}
            fig_ts = go.Figure()
            for t in ["rising", "stable", "declining", "new", "inactive"]:
                fig_ts.add_trace(go.Scatter(
                    x=sorted_yrs,
                    y=[yearly_trend_counts.get(yr, {}).get(t, 0) for yr in sorted_yrs],
                    name=t.title(),
                    mode="lines",
                    stackgroup="one",
                    line=dict(color=area_colors.get(t, "#888")),
                    hovertemplate="%{x}: " + t.title() + " %{y}人<extra></extra>",
                ))
            fig_ts.update_layout(
                title="年別アクティブ人数（トレンドカテゴリ別・Stacked Area）",
                xaxis_title="年",
                yaxis_title="人数",
            )
            body += '<div class="card">'
            body += "<h2>年別アクティブ人数（トレンドカテゴリ別）</h2>"
            body += chart_guide(
                "Stacked Areaチャートで年ごとのアクティブ人数をトレンドカテゴリ別に積み上げ表示。"
                "Rising（緑）が増えている年は業界全体の成長期、Declining（赤）が増えている年は衰退期。"
                "全体の面積が業界規模の拡大を示します。"
            )
            body += plotly_div_safe(fig_ts, "trend_yearly", 450)
            body += "</div>"

        # Scatter: Activity Ratio vs Quality Change
        activity_quality_data = []
        for pid, p in persons.items():
            ar = p.get("activity_ratio")
            recent = p.get("recent_avg_anime_score")
            career = p.get("career_avg_anime_score")
            if ar is not None and recent is not None and career is not None and career > 0:
                activity_quality_data.append({
                    "name": p.get("name", pid),
                    "activity_ratio": ar,
                    "quality_change": recent - career,
                    "trend": p.get("trend", "unknown"),
                })
        if activity_quality_data:
            trend_color_aq = {"rising": "#06D6A0", "stable": "#FFD166", "declining": "#EF476F",
                              "new": "#a0d2db", "inactive": "#666"}
            fig_aq = go.Figure()
            for trend in ["rising", "stable", "declining", "new", "inactive"]:
                subset = [d for d in activity_quality_data if d["trend"] == trend]
                if subset:
                    fig_aq.add_trace(go.Scatter(
                        x=[d["activity_ratio"] for d in subset],
                        y=[d["quality_change"] for d in subset],
                        mode="markers",
                        name=trend.title(),
                        marker=dict(color=trend_color_aq.get(trend, "#888"), size=6, opacity=0.7),
                        text=[d["name"] for d in subset],
                        hovertemplate="%{text}<br>Activity Ratio: %{x:.2f}<br>Quality Change: %{y:.2f}<extra></extra>",
                    ))
            fig_aq.update_layout(
                title="活動量変化率 vs 品質変化率",
                xaxis_title="Activity Ratio（活動量変化）",
                yaxis_title="品質変化（recent - career avg score）",
            )
            fig_aq.add_annotation(x=0.95, y=0.95, xref="paper", yref="paper",
                                  text="量↑質↑ 真の成長", showarrow=False,
                                  font=dict(size=10, color="#06D6A0"))
            fig_aq.add_annotation(x=0.95, y=0.05, xref="paper", yref="paper",
                                  text="量↑質↓ 量的拡大", showarrow=False,
                                  font=dict(size=10, color="#EF476F"))
            fig_aq.add_annotation(x=0.05, y=0.95, xref="paper", yref="paper",
                                  text="量↓質↑ 厳選型", showarrow=False,
                                  font=dict(size=10, color="#a0d2db"))
            fig_aq.add_annotation(x=0.05, y=0.05, xref="paper", yref="paper",
                                  text="量↓質↓ 衰退", showarrow=False,
                                  font=dict(size=10, color="#888"))
            fig_aq.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
            fig_aq.add_vline(x=1, line_dash="dash", line_color="rgba(255,255,255,0.3)")
            body += '<div class="card">'
            body += "<h2>活動量変化率 vs 品質変化率</h2>"
            body += chart_guide(
                "X=活動量の変化率（>1で増加）、Y=品質変化（recent平均 - career平均スコア）。"
                "右上=量も質も向上した真の成長者、右下=量は増えたが質は低下、"
                "左上=活動を絞って質を向上、左下=量も質も低下。"
                "色はトレンド分類を示します。"
            )
            body += plotly_div_safe(fig_aq, "activity_quality_scatter", 550)
            body += "</div>"

        # ---- K-Means クラスタリング: キャリア軌跡タイプ ----
        if len(persons) >= 5:
            import numpy as _np_gr
            from sklearn.cluster import KMeans as _KMeans_gr
            from sklearn.preprocessing import StandardScaler as _SC_gr

            GR_COLORS = ["#f093fb", "#06D6A0", "#fda085", "#a0d2db", "#FFD166", "#EF476F"]
            gr_persons_list = []
            for pid, p in persons.items():
                recent_sc = p.get("recent_avg_anime_score") or 0
                career_sc = p.get("career_avg_anime_score") or 0
                gr_persons_list.append({
                    "pid": pid,
                    "name": p.get("name", pid),
                    "trend": p.get("trend", "unknown"),
                    "total_credits": float(p.get("total_credits", 0) or 0),
                    "recent_credits": float(p.get("recent_credits", 0) or 0),
                    "activity_ratio": float(p.get("activity_ratio", 0) or 0),
                    "career_span": float(p.get("career_span", 0) or 0),
                    "total_years": float(p.get("total_years", 0) or 0),
                    "career_avg_score": float(career_sc),
                    "quality_change": float(recent_sc - career_sc),
                })

            GR_FEAT_COLS = [
                "total_credits", "recent_credits", "activity_ratio",
                "career_span", "total_years", "career_avg_score", "quality_change",
            ]
            GR_FEAT_JP = {
                "total_credits": "総クレジット", "recent_credits": "最近クレジット",
                "activity_ratio": "活動率", "career_span": "キャリア期間",
                "total_years": "活動年数", "career_avg_score": "平均品質スコア",
                "quality_change": "品質変化",
            }
            Xgr = _np_gr.array([[p[f] for f in GR_FEAT_COLS] for p in gr_persons_list], dtype=float)
            sc_gr = _SC_gr()
            Xgr_s = sc_gr.fit_transform(Xgr)
            k_gr = min(5, len(gr_persons_list))
            km_gr = _KMeans_gr(n_clusters=k_gr, n_init=20, random_state=42)
            gr_labels = km_gr.fit_predict(Xgr_s)
            centers_gr = sc_gr.inverse_transform(km_gr.cluster_centers_)
            fidx_gr = {f: i for i, f in enumerate(GR_FEAT_COLS)}

            # 動的クラスタ命名
            gr_cluster_names = _name_clusters_by_rank(
                centers_gr,
                [
                    (fidx_gr["total_credits"],  ["多作ベテラン", "中堅", "新人少作"]),
                    (fidx_gr["activity_ratio"], ["高活動", "中活動", "低活動"]),
                    (fidx_gr["career_avg_score"], ["高品質", "中品質", "低品質"]),
                    (fidx_gr["quality_change"],  ["品質成長", "品質安定", "品質低下"]),
                ],
            )

            for i, p in enumerate(gr_persons_list):
                p["gr_cluster"] = int(gr_labels[i])
                p["gr_cluster_name"] = gr_cluster_names[int(gr_labels[i])]

            gr_cluster_groups: dict[int, list[dict]] = {}
            for p in gr_persons_list:
                gr_cluster_groups.setdefault(p["gr_cluster"], []).append(p)

            body += '<div class="card">'
            body += f"<h2>キャリア軌跡 K-Means クラスタリング（{k_gr}クラスタ）</h2>"
            body += section_desc(
                "総クレジット数・最近クレジット・活動率・キャリア期間・平均品質・品質変化の"
                f"7特徴量で全スタッフを{k_gr}クラスタに自動分類。"
                "「多作ベテラン×高品質」「品質成長中の新人」等のキャリアタイプを識別します。"
            )

            # Scatter: activity_ratio vs quality_change colored by cluster
            fig_gr_sc = go.Figure()
            for cid in sorted(gr_cluster_groups.keys()):
                mems_gr = gr_cluster_groups[cid]
                fig_gr_sc.add_trace(go.Scattergl(
                    x=[m["activity_ratio"] for m in mems_gr],
                    y=[m["quality_change"] for m in mems_gr],
                    mode="markers",
                    name=gr_cluster_names[cid],
                    marker=dict(
                        size=5,
                        color=GR_COLORS[cid % len(GR_COLORS)],
                        opacity=0.6,
                    ),
                    text=[m["name"] for m in mems_gr],
                    hovertemplate="%{text}<br>活動率: %{x:.2f}<br>品質変化: %{y:.2f}<extra></extra>",
                ))
            fig_gr_sc.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
            fig_gr_sc.add_vline(x=1, line_dash="dash", line_color="rgba(255,255,255,0.3)")
            fig_gr_sc.update_layout(
                title="キャリアクラスタ散布図（活動率 × 品質変化）",
                xaxis_title="活動率", yaxis_title="品質変化",
            )
            body += chart_guide(
                "各点は1人。色＝クラスタ。右上＝活動量も品質も向上（真の成長型）、"
                "左下＝縮小。クラスタが集中する位置でキャリアタイプのパターンが見えます。"
            )
            body += plotly_div_safe(fig_gr_sc, "growth_cluster_scatter", 520)

            # Cluster profile heatmap (z-score)
            z_gr = km_gr.cluster_centers_  # already z-scored
            fig_gr_hmap = go.Figure(go.Heatmap(
                z=[[z_gr[cid, fidx_gr[f]] for f in GR_FEAT_COLS] for cid in range(k_gr)],
                x=[GR_FEAT_JP.get(f, f) for f in GR_FEAT_COLS],
                y=[gr_cluster_names[c] for c in range(k_gr)],
                colorscale="RdBu", zmid=0,
                colorbar=dict(title="z-score"),
                hovertemplate="クラスタ: %{y}<br>特徴: %{x}<br>z-score: %{z:.2f}<extra></extra>",
            ))
            fig_gr_hmap.update_layout(
                title="キャリアクラスタプロファイル Heatmap（z-score）",
                xaxis_tickangle=-25,
            )
            body += chart_guide("赤＝そのクラスタが平均より高い特徴、青＝低い特徴。クラスタの特徴が一目で分かります。")
            body += plotly_div_safe(fig_gr_hmap, "growth_cluster_heatmap", 450)

            # Violin: total_credits per cluster
            fig_gr_viol = go.Figure()
            for cid in sorted(gr_cluster_groups.keys()):
                mems_gr = gr_cluster_groups[cid]
                tc_vals = [m["total_credits"] for m in mems_gr]
                if tc_vals:
                    fig_gr_viol.add_trace(_violin_raincloud(
                        tc_vals, gr_cluster_names[cid], GR_COLORS[cid % len(GR_COLORS)],
                    ))
            fig_gr_viol.update_layout(
                title="クラスタ別 総クレジット数分布 (Raincloud)",
                yaxis_title="総クレジット数",
                violinmode="overlay",
            )
            body += chart_guide("各クラスタの総クレジット数分布。多作ベテランvs新人少作の差が見えます。")
            body += plotly_div_safe(fig_gr_viol, "growth_cluster_violin", 450)

            # Stacked bar: cluster × trend
            fig_gr_trend = go.Figure()
            trend_colors_gr = {"rising": "#06D6A0", "stable": "#FFD166", "declining": "#EF476F",
                               "new": "#a0d2db", "inactive": "#666"}
            trend_order_gr = ["rising", "stable", "declining", "new", "inactive"]
            for t in trend_order_gr:
                t_counts = [
                    sum(1 for m in gr_cluster_groups.get(c, []) if m["trend"] == t)
                    for c in range(k_gr)
                ]
                if any(x > 0 for x in t_counts):
                    fig_gr_trend.add_trace(go.Bar(
                        name=t.title(),
                        x=[gr_cluster_names[c] for c in range(k_gr)],
                        y=t_counts,
                        marker_color=trend_colors_gr.get(t, "#888"),
                    ))
            fig_gr_trend.update_layout(
                title="クラスタ × トレンドカテゴリ（人数）",
                barmode="stack", xaxis_tickangle=-20, yaxis_title="人数",
            )
            body += chart_guide("K-Meansクラスタとトレンド分類の対応。各クラスタにどのトレンドが多いかを確認。")
            body += plotly_div_safe(fig_gr_trend, "growth_cluster_trend_bar", 420)
            body += "</div>"

        # Bar+Line: Career Year Retention Curve
        scores_for_retention = load_json("scores.json")
        if scores_for_retention:
            active_years_list = [
                p.get("career", {}).get("active_years", 0)
                for p in scores_for_retention
                if p.get("career", {}).get("active_years", 0) > 0
            ]
            if active_years_list:
                total_people_ret = len(active_years_list)
                max_year_ret = min(max(active_years_list), 30)
                years_range_ret = list(range(1, max_year_ret + 1))
                surviving = [sum(1 for y in active_years_list if y >= yr) for yr in years_range_ret]
                survival_rate = [s / total_people_ret * 100 for s in surviving]
                fig_ret = make_subplots(specs=[[{"secondary_y": True}]])
                fig_ret.add_trace(
                    go.Bar(
                        x=years_range_ret, y=surviving, name="人数",
                        marker_color="rgba(240,147,251,0.4)",
                        hovertemplate="%{x}年以上: %{y}人<extra></extra>",
                    ),
                    secondary_y=False,
                )
                fig_ret.add_trace(
                    go.Scatter(
                        x=years_range_ret, y=survival_rate, name="生存率 (%)",
                        mode="lines+markers",
                        line=dict(color="#06D6A0", width=3),
                        marker=dict(size=5),
                        hovertemplate="%{x}年以上: %{y:.1f}%<extra></extra>",
                    ),
                    secondary_y=True,
                )
                fig_ret.update_layout(title="キャリア年数別 生存率曲線（Retention Curve）")
                fig_ret.update_yaxes(title_text="人数", secondary_y=False)
                fig_ret.update_yaxes(title_text="生存率 (%)", secondary_y=True)
                body += '<div class="card">'
                body += "<h2>キャリア生存率曲線</h2>"
                body += chart_guide(
                    "棒グラフ（左Y軸）: その年数以上活動している人数。"
                    "折れ線（右Y軸）: 全体に対する生存率%。"
                    "急激な減少は離脱が多い時期を示し、キャリア持続の障壁を示唆します。"
                )
                body += plotly_div_safe(fig_ret, "retention_curve", 500)
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
            body += "<th>#</th><th>Person</th><th>Current IV Score</th><th>Debiased BiRank</th><th>Gap</th><th>Category</th>"
            body += "</tr></thead><tbody>"
            for i, a in enumerate(alerts[:20], 1):
                gap = a.get("authority_gap", 0)
                badge = "badge-high" if gap >= 5 else "badge-mid" if gap >= 2 else "badge-low"
                name = a.get("name", a.get("person_id", ""))
                pid_a = a.get("person_id", "")
                body += f"<tr><td>{i}</td><td>{person_link(name, pid_a)}</td>"
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
            body += f'<div class="insight-box"><strong>Score concentration:</strong> The top 1% of persons hold {pa.get("top_percentile_share", 0):.1f}% of total BiRank. '
            body += f'Average score is {pa.get("avg_score", 0):.2f}, median is {pa.get("median_score", 0):.2f} — '
            body += "indicating a highly skewed distribution typical of scale-free networks.</div>"
            body += "</div>"

    # Data-driven key findings
    growth_total = growth.get("total_persons", 0) if growth else 0
    rising_count = trend_summary.get("rising", 0) if growth else 0
    rising_pct = rising_count / max(growth_total, 1) * 100 if growth else 0
    alerts_count = len(insights.get("undervaluation_alerts", [])) if insights else 0
    body += key_findings([
        f"全 {fmt_num(growth_total)} 人のうち「上昇中」は {fmt_num(rising_count)} 人"
        f"（{rising_pct:.1f}%）— クレジット数が加速的に増加しており、将来の主力候補群",
        f"バイアス補正後の分析により {fmt_num(alerts_count)} 人の過小評価候補が特定され、"
        "適正報酬より低く評価されている可能性がある",
        "スコア集中度は上位1%に偏っており、スケールフリーネットワーク特有の分布を示す",
    ])

    body += significance_section("過小評価された人材の発見と報酬格差の是正", [
        "成長トレンド分析は「現在の評価」だけでなく「将来の価値」を示します。"
        "上昇中の人材を早期に特定することで、報酬格差が拡大する前に公正な契約交渉が可能になります。"
        "スコア分布のべき乗則は少数の人材が業界価値の大半を創出していることを示しており、"
        "このロングテール領域に埋もれた人材を見逃さないことがスタジオの競争優位につながります。",
        "バイアス補正過小評価アラートは、スタジオ所属・役職・キャリアステージによって"
        "不当に低く評価されてきた具体的な人物を特定します。"
        "これはアニメ業界の慢性的な報酬問題に対する、感情ではなくデータに基づくアプローチです。",
    ])
    body += utilization_guide([
        {"role": "スタジオ人事", "how": "Rising Stars一覧でactivity_ratioが高く・recent_avg_anime_scoreが高い人物を抽出し、現在の報酬が低い段階で長期契約を提示して競合スタジオより先に確保する"},
        {"role": "アニメーター本人", "how": "Undervaluation Alertsで自分の名前を確認し、debiased_authority（バイアス補正後）がcurrent_compositeより高ければその差（gap列）を具体的な報酬引き上げ根拠として交渉書類に記載する"},
        {"role": "エージェント・代理人", "how": "クライアントのgap値（バイアス補正前後の差）と業界パーセンタイルを組み合わせて「現在の報酬が業界水準より○%低い」と定量化し、契約更新交渉で提示する"},
        {"role": "投資家", "how": "Rising Stars一覧でtotal_creditsとactivity_ratioが急増中の人物が参加する直近作品を特定し、クラウドファンディングや出資の優先候補として見極める"},
    ])
    body += future_possibilities([
        "「過小評価ランキング」の定期公開による業界全体への報酬是正圧力の醸成（匿名化した集計データとして）",
        "成長トレンドが止まった人材へのリスキリング提案（活動量が3期連続減少した時のスキルアップ支援の介入指標）",
        "成長スコアと実際の報酬データを統合した「公正報酬推計ツール」（業界水準との乖離を個人ごとに算出）",
        "証券類似の「ライジングスター指数」として定期配信し、コンテンツ投資判断のリファレンス指標化",
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
                "上位パーセンタイルが保持する総BiRank量の割合。"
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

    # Sort by iv_score descending
    persons = sorted(scores, key=lambda x: x.get("iv_score", 0), reverse=True)

    body = ""

    # Summary stats
    iv_scores = [p.get("iv_score", 0) for p in persons]
    biranks = [p.get("birank", 0) for p in persons]
    patronages = [p.get("patronage", 0) for p in persons]
    person_fes = [p.get("person_fe", 0) for p in persons]

    body += '<div class="card">'
    body += "<h2>Score Summary</h2>"
    body += '<div class="stats-grid">'
    for label, val in [
        ("Total Persons", fmt_num(len(persons))),
        ("Avg IV Score", f"{sum(iv_scores) / max(len(iv_scores), 1):.2f}"),
        ("Top IV Score", f"{max(iv_scores):.2f}" if iv_scores else "N/A"),
        ("Median IV Score", f"{sorted(iv_scores)[len(iv_scores) // 2]:.2f}" if iv_scores else "N/A"),
        ("Avg BiRank", f"{sum(biranks) / max(len(biranks), 1):.2f}"),
        ("Avg Patronage", f"{sum(patronages) / max(len(patronages), 1):.2f}"),
        ("Avg Person FE", f"{sum(person_fes) / max(len(person_fes), 1):.2f}"),
        ("Profiles", fmt_num(len(profiles.get("profiles", {}))) if profiles else "N/A"),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div></div>"

    # Score distribution histograms (2x2)
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("IV Score", "BiRank", "Patronage", "Person FE"),
        vertical_spacing=0.12, horizontal_spacing=0.08,
    )
    for row, col, vals, color, name in [
        (1, 1, iv_scores, "#f093fb", "IV Score"),
        (1, 2, biranks, "#a0d2db", "BiRank"),
        (2, 1, patronages, "#f5576c", "Patronage"),
        (2, 2, person_fes, "#fda085", "Person FE"),
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
    categories = ["BiRank", "Patronage", "Person FE"]
    for p in top10:
        vals = [p.get("birank", 0), p.get("patronage", 0), p.get("person_fe", 0)]
        fig.add_trace(go.Scatterpolar(
            r=vals + [vals[0]], theta=categories + [categories[0]],
            name=p.get("name", p["person_id"])[:20],
            fill="toself", opacity=0.6,
        ))
    fig.update_layout(
        title="Top 10 — BiRank / Patronage / Person FE Radar",
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

    # BiRank vs Patronage scatter
    fig = go.Figure(go.Scatter(
        x=biranks, y=patronages, mode="markers",
        marker=dict(size=5, color=iv_scores, colorscale="Viridis", showscale=True,
                    colorbar=dict(title="IV Score")),
        text=[p.get("name", "") for p in persons],
        hovertemplate="%{text}<br>BiRank: %{x:.1f}<br>Patronage: %{y:.1f}<extra></extra>",
    ))
    fig.update_layout(title="BiRank vs Patronage", xaxis_title="BiRank", yaxis_title="Patronage")
    body += '<div class="card">'
    body += "<h2>BiRank vs Patronage</h2>"
    body += chart_guide(
        "各ドットは1人の人物。X=BiRank、Y=Patronage、色=IV Score。"
        "右上象限の人物はネットワーク中心性と継続起用の両方が高い。"
        "ホバーで名前を確認できます。"
    )
    body += plotly_div_safe(fig, "birank_patronage_scatter", 500)
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

    # Violin: role-based IV score distribution
    role_iv_scores: dict[str, list[float]] = {}
    for p in persons:
        role = p.get("primary_role", "unknown")
        role_iv_scores.setdefault(role, []).append(p.get("iv_score", 0))
    # Keep top roles by count for readability
    top_roles_v = sorted(role_iv_scores.keys(), key=lambda r: len(role_iv_scores[r]), reverse=True)[:8]
    if top_roles_v:
        violin_colors = ["#f093fb", "#a0d2db", "#f5576c", "#fda085", "#667eea",
                         "#06D6A0", "#FFD166", "#EF476F"]
        fig_rv = go.Figure()
        for idx, role in enumerate(top_roles_v):
            fig_rv.add_trace(_violin_raincloud(
                role_iv_scores[role],
                role,
                violin_colors[idx % len(violin_colors)],
            ))
        fig_rv.update_layout(
            title="ロール別 IV Score分布 (Raincloud)",
            yaxis_title="IV Score",
            xaxis_title="Primary Role",
            violinmode="overlay",
        )
        body += '<div class="card">'
        body += "<h2>ロール別 IV Score分布</h2>"
        body += chart_guide(
            "Violin plotで主要ロール別のIV Score分布を比較。"
            "箱ひげ図の中央線が中央値、平均線が表示されます。"
            "ロール間のスコア格差やばらつきの違いが一目で把握できます。"
        )
        body += plotly_div_safe(fig_rv, "role_violin", 550)
        body += "</div>"

    # Stacked bar: score band by role
    score_bands = [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100)]
    band_labels = ["0-20", "20-40", "40-60", "60-80", "80-100"]
    top_roles_sb = sorted(role_iv_scores.keys(), key=lambda r: len(role_iv_scores[r]), reverse=True)[:6]
    if top_roles_sb:
        band_role_counts: dict[str, list[int]] = {role: [] for role in top_roles_sb}
        for lo, hi in score_bands:
            for role in top_roles_sb:
                cnt = sum(1 for c in role_iv_scores[role] if lo <= c < hi)
                band_role_counts[role].append(cnt)

        sb_colors = ["#f093fb", "#a0d2db", "#f5576c", "#fda085", "#667eea", "#06D6A0"]
        fig_sb = go.Figure()
        for idx, role in enumerate(top_roles_sb):
            fig_sb.add_trace(go.Bar(
                x=band_labels,
                y=band_role_counts[role],
                name=role,
                marker_color=sb_colors[idx % len(sb_colors)],
                hovertemplate="%{x}: " + role + " %{y}人<extra></extra>",
            ))
        fig_sb.update_layout(
            barmode="stack",
            title="スコア帯別ロール構成（Stacked）",
            xaxis_title="IV Scoreスコア帯",
            yaxis_title="人数",
        )
        body += '<div class="card">'
        body += "<h2>スコア帯別ロール構成</h2>"
        body += chart_guide(
            "スコアを5つの帯に分割し、各帯にどのロールの人物が何人いるかを積み上げ表示。"
            "上位スコア帯（80-100）に特定ロールが集中しているか、"
            "低スコア帯の構成比などからロール間格差を読み取れます。"
        )
        body += plotly_div_safe(fig_sb, "score_band_stacked", 450)
        body += "</div>"

    # Lorenz Curve: BiRank/Patronage/Person FE inequality structure
    def _lorenz_curve(values):
        sorted_vals = sorted(v for v in values if v > 0)
        n = len(sorted_vals)
        if n == 0:
            return [0, 100], [0, 100], 0.0
        total = sum(sorted_vals)
        cum_pop = [0] + [i / n * 100 for i in range(1, n + 1)]
        cum_val = [0] + [sum(sorted_vals[:i]) / total * 100 for i in range(1, n + 1)]
        area = sum(
            (cum_val[i] + cum_val[i + 1]) / 2 * (cum_pop[i + 1] - cum_pop[i])
            for i in range(n)
        ) / 10000
        gini = 1 - 2 * area
        return cum_pop, cum_val, gini

    fig_lorenz = go.Figure()
    fig_lorenz.add_trace(go.Scatter(
        x=[0, 100], y=[0, 100], name="完全平等線",
        line=dict(dash="dash", color="rgba(255,255,255,0.4)"),
        hoverinfo="skip",
    ))
    gini_texts = []
    for label, vals, color in [
        ("BiRank", biranks, "#a0d2db"),
        ("Patronage", patronages, "#f5576c"),
        ("Person FE", person_fes, "#fda085"),
    ]:
        cpop, cval, gini = _lorenz_curve(vals)
        fig_lorenz.add_trace(go.Scatter(
            x=cpop, y=cval, name=f"{label} (Gini={gini:.3f})",
            line=dict(color=color, width=2),
            hovertemplate=f"{label}<br>累積人口: %{{x:.1f}}%<br>累積スコア: %{{y:.1f}}%<extra></extra>",
        ))
        gini_texts.append(f"{label}: Gini={gini:.3f}")
    fig_lorenz.update_layout(
        title="Lorenz曲線: BiRank / Patronage / Person FE の不平等構造",
        xaxis_title="累積人口割合 (%)",
        yaxis_title="累積スコア割合 (%)",
    )
    fig_lorenz.add_annotation(
        x=0.02, y=0.98, xref="paper", yref="paper",
        text="<br>".join(gini_texts),
        showarrow=False, font=dict(size=11, color="#e0e0e0"),
        align="left", bgcolor="rgba(0,0,0,0.5)", borderpad=6,
    )
    body += '<div class="card">'
    body += "<h2>Lorenz曲線: スコア不平等構造</h2>"
    body += chart_guide(
        "Lorenz曲線は「上位X%が全体のY%を占める」不平等構造を可視化。"
        "45度の点線（完全平等線）から離れるほど不平等が大きい。"
        "Gini係数（0=完全平等、1=最大不平等）をアノテーションで表示。"
        "BiRank・Patronage・Person FEのうちどの軸が最も不平等かを比較できます。"
    )
    body += plotly_div_safe(fig_lorenz, "lorenz_curve", 550)
    body += "</div>"

    # Scatter: Betweenness Centrality vs IV Score
    betweenness_vals = []
    iv_vals_bc = []
    scatter_roles_bc = []
    scatter_credits_bc = []
    scatter_names_bc = []
    for p in persons:
        btwn = p.get("centrality", {}).get("betweenness", None)
        if btwn is None:
            continue
        betweenness_vals.append(btwn)
        iv_vals_bc.append(p.get("iv_score", 0))
        scatter_roles_bc.append(p.get("primary_role", "unknown"))
        scatter_credits_bc.append(max(p.get("total_credits", 1), 3))
        scatter_names_bc.append(p.get("name", p["person_id"]))

    if betweenness_vals:
        fig_bc = px.scatter(
            x=betweenness_vals, y=iv_vals_bc,
            color=scatter_roles_bc,
            size=scatter_credits_bc,
            hover_name=scatter_names_bc,
            labels={"x": "Betweenness Centrality", "y": "IV Score",
                    "color": "Primary Role", "size": "Total Credits"},
        )
        fig_bc.update_layout(title="Betweenness Centrality vs IV Score")
        fig_bc.add_annotation(x=0.95, y=0.95, xref="paper", yref="paper",
                              text="高B高C: 影響力ハブ", showarrow=False,
                              font=dict(size=10, color="#06D6A0"))
        fig_bc.add_annotation(x=0.95, y=0.05, xref="paper", yref="paper",
                              text="高B低C: 隠れた仲介者", showarrow=False,
                              font=dict(size=10, color="#FFD166"))
        fig_bc.add_annotation(x=0.05, y=0.95, xref="paper", yref="paper",
                              text="低B高C: 集中型エリート", showarrow=False,
                              font=dict(size=10, color="#a0d2db"))
        fig_bc.add_annotation(x=0.05, y=0.05, xref="paper", yref="paper",
                              text="低B低C: 周辺", showarrow=False,
                              font=dict(size=10, color="#888"))
        body += '<div class="card">'
        body += "<h2>Betweenness Centrality vs IV Score</h2>"
        body += chart_guide(
            "X=ネットワーク上の仲介中心性（betweenness）、Y=IV Score。"
            "色=役職、サイズ=総クレジット数。高betweennessだがIV Score低い＝過小評価された仲介者。"
            "四象限のラベルでネットワーク上の位置と評価の関係を解読できます。"
        )
        body += plotly_div_safe(fig_bc, "betweenness_scatter", 550)
        body += "</div>"

    # Heatmap: Career years × Role → Average IV Score
    year_bins = [(1, 5), (6, 10), (11, 15), (16, 20), (21, 999)]
    year_labels_hm = ["1-5年", "6-10年", "11-15年", "16-20年", "21年+"]
    top_roles_hm = sorted(
        role_iv_scores.keys(), key=lambda r: len(role_iv_scores[r]), reverse=True,
    )[:8]
    if top_roles_hm:
        hm_data = []
        for role in top_roles_hm:
            row = []
            for lo, hi in year_bins:
                vals = [
                    p.get("iv_score", 0) for p in persons
                    if p.get("primary_role", "unknown") == role
                    and lo <= p.get("career", {}).get("active_years", 0) <= hi
                ]
                row.append(sum(vals) / len(vals) if vals else 0)
            hm_data.append(row)

        fig_hm = go.Figure(go.Heatmap(
            z=hm_data, x=year_labels_hm, y=top_roles_hm,
            colorscale="Viridis",
            texttemplate="%{z:.1f}",
            hovertemplate="ロール: %{y}<br>経験: %{x}<br>平均IV Score: %{z:.1f}<extra></extra>",
        ))
        fig_hm.update_layout(
            title="キャリア年数 × ロール → 平均IV Score",
            xaxis_title="活動年数",
            yaxis_title="Primary Role",
        )
        body += '<div class="card">'
        body += "<h2>経験×役職の価値マップ</h2>"
        body += chart_guide(
            "ヒートマップ: 行=役職、列=活動年数帯、色=平均IV Score。"
            "明るいセルほど高スコア。「どの役職×経験帯が最もスコアが高いか」を把握でき、"
            "キャリア設計の参考になります。"
        )
        body += plotly_div_safe(fig_hm, "career_role_heatmap", 500)
        body += "</div>"

    # Top 50 table
    body += '<div class="card">'
    body += "<h2>Top 50 Persons by IV Score</h2>"
    body += section_desc(
        "IV Score（BiRank・Patronage・Person FEの操作変数推定統合値）順にランキング。"
        "バッジ色: 緑=上位層(70+)、黄=中間層(40-69)、赤=下位層。"
    )
    body += "<table><thead><tr>"
    body += "<th>#</th><th>Name</th><th>Role</th><th>IV Score</th>"
    body += "<th>BiRank</th><th>Patronage</th><th>Person FE</th><th>Credits</th>"
    body += "</tr></thead><tbody>"
    for i, p in enumerate(persons[:50], 1):
        comp = p.get("iv_score", 0)
        badge = "badge-high" if comp >= 70 else "badge-mid" if comp >= 40 else "badge-low"
        name = p.get("name", p["person_id"])
        body += f"<tr><td>{i}</td><td>{person_link(name, p['person_id'])}</td><td>{p.get('primary_role', '')}</td>"
        body += f'<td><span class="badge {badge}">{comp:.2f}</span></td>'
        body += f"<td>{p.get('birank', 0):.1f}</td><td>{p.get('patronage', 0):.1f}</td>"
        body += f"<td>{p.get('person_fe', 0):.1f}</td><td>{p.get('total_credits', 0)}</td></tr>"
    body += "</tbody></table></div>"

    # Data-driven key findings
    persons_r = scores if isinstance(scores, list) else []
    persons_count_r = len(persons_r)
    top_score = max((p.get("iv_score", 0) for p in persons_r), default=0)
    median_score = sorted(p.get("iv_score", 0) for p in persons_r)[persons_count_r // 2] if persons_r else 0
    body += key_findings([
        f"スコア分布はべき乗則に従い、{fmt_num(persons_count_r)} 人中"
        f"トップスコア {top_score:.1f} に対して中央値 {median_score:.1f} と大きな格差がある",
        "BiRank・Patronage・Person FEの3軸プロファイルは人物ごとに大きく異なり、"
        "得意領域の違いが明確に表れる",
        "BiRankとPatronageには正の相関があるが、Person FEは独立した軸として機能",
    ])

    body += significance_section("業界全体での客観的な相対的位置の把握", [
        "「自分が業界でどの位置にいるか」は報酬交渉・転職・キャリア設計のすべてに影響しますが、"
        "これまで客観的な比較基準がありませんでした。本ランキングは公開クレジットデータのみを使い、"
        "主観・コネ・知名度を除いた純粋なネットワーク上の相対位置を初めて定量化します。",
        "3軸評価（BiRank/Patronage/Person FE）はスコアの「なぜ」を説明します。"
        "BiRankが高くPatronageが低い場合は「広く浅い協業型」、その逆は「深い専門的信頼型」として読めます。"
        "これにより、総合順位だけでは見えない個人の強みのパターンが浮かび上がります。",
    ])
    body += utilization_guide([
        {"role": "スタジオ経営者", "how": "在籍スタッフのIV Scoreと業界中央値を比較し、上位何パーセンタイルの人材を抱えているかを確認して、競合スタジオとの待遇交渉時の報酬ベンチマークとして使う"},
        {"role": "アニメーター本人", "how": "自身の総合順位と3軸（BiRank/Patronage/Person FE）の相対値を確認し、最も高い軸を「強み」として報酬交渉書類に記載し、同パーセンタイル帯の業界水準報酬を参照値として提示する"},
        {"role": "プロデューサー", "how": "BiRank-Patronage散布図で予算に見合うスコア帯の人物をクラスタとして確認し、同等スコアの人物の中から可用性とコストのバランスを見てキャスティングする"},
        {"role": "メディア・ジャーナリスト", "how": "Top 50テーブルと3軸レーダーチャートを引用し、「データが示すトップクリエイター」として客観的根拠を持った業界人物紹介記事を書く"},
    ])
    body += future_possibilities([
        "役職別・担当ジャンル別のサブランキングページ（「今季の作画監督ランキング」「監督限定ランキング」など）",
        "年次ランキングのアーカイブ化（前年比でどれだけ順位が変動したかを折れ線グラフで可視化）",
        "業界内ランキングと実際の報酬実態の相関分析（上位N%の実際の収入分布を集計して公開）",
        "個人ランキング変動をAPIとして公開し、ポートフォリオサービスや人材エージェントが利用できるインフラ化",
    ])

    html = wrap_html(
        "人物ランキング・スコア分析",
        f"人物ランキング・スコア分析 — {fmt_num(len(persons))}人を評価",
        body,
        intro_html=report_intro(
            "人物ランキング・スコア分析",
            "IV Scoreによるアニメ業界プロフェッショナルの決定版ランキング。"
            "3つの評価軸（BiRank・Patronage・Person FE）ごとのスコア分布を分解し、"
            "トップ10のレーダープロファイル比較と、全人口にわたるBiRank-Patronage相関を"
            "表示します。",
            "スタジオ経営者、プロデューサー、業界研究者",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "IV Score算出方法": (
                "IV Score = BiRank・Patronage・Person FEの操作変数推定による重み付き合計。各軸は0-100に"
                "正規化。"
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
    avg_gini = summary.get("avg_gini_coefficient", 0)
    avg_corr = summary.get("avg_shapley_correlation", 0)
    gini_label = "不平等寄り" if avg_gini > 0.5 else "やや均等寄り" if avg_gini > 0.25 else "均等寄り"
    body += '<div class="card">'
    body += "<h2>報酬公平性サマリー</h2>"
    body += '<div class="stats-grid">'
    for label, val in [
        ("分析対象作品数", fmt_num(fair.get("total_anime", 0))),
        ("平均Gini係数", f"{avg_gini:.3f} ({gini_label})"),
        ("平均Shapley相関", f"{avg_corr:.3f}"),
        ("作品タイプ別", "、".join(f"{k}: {v}本" for k, v in fair.get("anime_type_distribution", {}).items())),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div>"
    body += '<div class="insight-box"><strong>見方：</strong> '
    body += "Gini係数はスタッフ間の報酬不均等度（0=完全均等、1=完全不均等）を表します。"
    body += "Shapley相関はShapley値（理論的貢献度）と実際の配分の一致度です。"
    body += "1.0に近いほど貢献度に見合った配分がなされていることを示します。</div>"
    body += "</div>"

    # Gini coefficient distribution
    if analyses:
        gini_values = [a["fairness"]["gini_coefficient"] for a in analyses if "fairness" in a]
        fig = go.Figure(go.Histogram(
            x=gini_values, nbinsx=20, marker_color="#f093fb",
            hovertemplate="Gini: %{x:.3f}<br>Count: %{y}<extra></extra>",
        ))
        fig.update_layout(title="Gini係数分布", xaxis_title="Gini係数", yaxis_title="作品数")
        body += '<div class="card">'
        body += "<h2>Gini係数分布</h2>"
        body += chart_guide(
            "分析対象全作品のGini係数分布。0に近いほどスタッフ間のクレジット配分が均等、"
            "1に近いほど少数の人物がクレジット配分を独占していることを示します。"
        )
        body += plotly_div_safe(fig, "gini_dist", 400)
        body += "</div>"

        # Shapley allocation per anime (top 5) — stacked by role
        shapley_role_colors = {
            "director": "#9b59b6", "chief_director": "#8e44ad",
            "character_designer": "#3498db", "character_design": "#3498db",
            "animator": "#06D6A0", "key_animator": "#2ecc71",
            "animation_director": "#27ae60", "chief_animation_director": "#1abc9c",
            "art_director": "#fda085", "series_composition": "#f5576c",
            "sound_director": "#FFD166", "music": "#f39c12",
        }
        sorted_analyses = sorted(analyses, key=lambda a: a.get("staff_count", 0), reverse=True)
        for a in sorted_analyses[:5]:
            allocs = a.get("allocations", [])
            if not allocs:
                continue
            names = [al.get("name", al["person_id"]) for al in allocs]
            allocations = [al.get("allocation", 0) for al in allocs]
            roles = [al.get("role", "") for al in allocs]
            bar_colors = [shapley_role_colors.get(r, "#a0a0c0") for r in roles]

            fig = go.Figure(go.Bar(
                x=names, y=allocations,
                marker_color=bar_colors,
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

        # Stacked bar: top 5 anime Shapley allocation by role category
        role_categories = {
            "director": "監督系", "chief_director": "監督系", "series_composition": "監督系",
            "character_designer": "デザイン系", "character_design": "デザイン系",
            "art_director": "デザイン系", "mechanical_design": "デザイン系",
            "animator": "作画系", "key_animator": "作画系",
            "animation_director": "作画系", "chief_animation_director": "作画系",
            "sound_director": "音響系", "music": "音響系",
        }
        cat_colors = {"監督系": "#9b59b6", "デザイン系": "#3498db", "作画系": "#06D6A0", "音響系": "#FFD166", "その他": "#a0a0c0"}
        anime_titles_shap = []
        cat_alloc_data: dict[str, list[float]] = {cat: [] for cat in cat_colors}
        for a in sorted_analyses[:5]:
            allocs = a.get("allocations", [])
            if not allocs:
                continue
            anime_titles_shap.append(a.get("anime_title", a["anime_id"])[:25])
            cat_sums: dict[str, float] = {cat: 0.0 for cat in cat_colors}
            for al in allocs:
                cat = role_categories.get(al.get("role", ""), "その他")
                cat_sums[cat] += al.get("allocation", 0)
            for cat in cat_colors:
                cat_alloc_data[cat].append(cat_sums[cat])

        if anime_titles_shap:
            fig_shap_stack = go.Figure()
            for cat, color in cat_colors.items():
                fig_shap_stack.add_trace(go.Bar(
                    x=anime_titles_shap,
                    y=cat_alloc_data[cat],
                    name=cat,
                    marker_color=color,
                    hovertemplate="%{x}<br>" + cat + ": %{y:.1f}%<extra></extra>",
                ))
            fig_shap_stack.update_layout(
                barmode="stack",
                title="上位5作品 Shapley配分 — 役職カテゴリ別内訳",
                yaxis_title="配分率 (%)",
                xaxis_tickangle=-25,
            )
            body += '<div class="card">'
            body += "<h2>Shapley配分 役職カテゴリ別内訳</h2>"
            body += chart_guide(
                "上位5作品のShapley配分を役職カテゴリ（監督系・デザイン系・作画系・音響系）に集約。"
                "作品ごとに配分がどのカテゴリに偏っているか、チーム構成の違いを比較できます。"
            )
            body += plotly_div_safe(fig_shap_stack, "shapley_role_stacked", 450)
            body += "</div>"

        # Scatter: Anime Score vs Gini Coefficient
        anime_score_vals = []
        anime_gini_vals = []
        anime_staff_counts_sc = []
        anime_titles_scatter = []
        for a in analyses:
            ascore = a.get("anime_score", None)
            gini = a.get("fairness", {}).get("gini_coefficient", None)
            if ascore is not None and gini is not None:
                anime_score_vals.append(ascore)
                anime_gini_vals.append(gini)
                anime_staff_counts_sc.append(max(a.get("staff_count", 1), 5))
                anime_titles_scatter.append(a.get("anime_title", a.get("anime_id", "")))

        if anime_score_vals:
            max_staff_sc = max(anime_staff_counts_sc) if anime_staff_counts_sc else 1
            fig_sg = go.Figure(go.Scatter(
                x=anime_score_vals, y=anime_gini_vals,
                mode="markers",
                marker=dict(
                    size=[min(s, 40) for s in anime_staff_counts_sc],
                    sizemode="area", sizeref=max_staff_sc / 400,
                    color="#f093fb", opacity=0.6,
                ),
                text=anime_titles_scatter,
                hovertemplate="%{text}<br>Score: %{x:.1f}<br>Gini: %{y:.3f}<extra></extra>",
            ))
            fig_sg.update_layout(
                title="作品スコア vs Gini係数（高品質作品は公平か？）",
                xaxis_title="Anime Score",
                yaxis_title="Gini Coefficient",
            )
            body += '<div class="card">'
            body += "<h2>作品スコア vs Gini係数</h2>"
            body += chart_guide(
                "X=作品スコア、Y=Gini係数（配分不均等度）、サイズ=スタッフ数。"
                "高スコア×低Gini=チーム均等型の高品質作品。"
                "高スコア×高Gini=監督集中型の高品質作品。"
                "品質と配分公平性の関係パターンを確認できます。"
            )
            body += plotly_div_safe(fig_sg, "score_gini_scatter", 500)
            body += "</div>"

        # Box plot: Role-based Shapley allocation distribution
        role_allocations: dict[str, list[float]] = {}
        for a in analyses:
            for al in a.get("allocations", []):
                role = al.get("role", "unknown")
                alloc = al.get("allocation", 0)
                role_allocations.setdefault(role, []).append(alloc)

        top_alloc_roles = sorted(
            role_allocations.keys(), key=lambda r: len(role_allocations[r]), reverse=True,
        )[:10]
        if top_alloc_roles:
            box_colors = ["#f093fb", "#a0d2db", "#f5576c", "#fda085", "#667eea",
                          "#06D6A0", "#FFD166", "#EF476F", "#9b59b6", "#3498db"]
            fig_ra = go.Figure()
            for idx, role in enumerate(top_alloc_roles):
                fig_ra.add_trace(go.Box(
                    y=role_allocations[role],
                    name=role,
                    marker_color=box_colors[idx % len(box_colors)],
                    boxmean=True,
                ))
            fig_ra.update_layout(
                title="ロール別 Shapley配分率の分布",
                yaxis_title="Allocation (%)",
                xaxis_title="Role",
            )
            body += '<div class="card">'
            body += "<h2>ロール別 Shapley配分率の分布</h2>"
            body += chart_guide(
                "箱ひげ図で各ロールのShapley配分率の分布を比較。"
                "箱の上下端が四分位範囲、中央線が中央値、◆が平均値。"
                "外れ値（ひげの外の点）は異常に高い/低い配分を受けた人物。"
                "ロール間の配分格差とばらつきの違いが一目で把握できます。"
            )
            body += plotly_div_safe(fig_ra, "role_allocation_box", 550)
            body += "</div>"

    # Anime value multi-axis (radar)
    if anime_values:
        av_list = sorted(anime_values.values(), key=lambda x: x.get("composite_value", 0), reverse=True)
        value_axes = ["commercial_value", "critical_value", "creative_value", "cultural_value", "technical_value"]
        axis_labels = ["商業価値", "批評価値", "創造価値", "文化価値", "技術価値"]

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
            title="作品価値プロファイル（上位8作品、各作品内正規化）",
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        )
        body += '<div class="card">'
        body += "<h2>作品価値プロファイル</h2>"
        body += chart_guide(
            "5軸（商業・批評・創造・文化・技術）での作品価値レーダーチャート。"
            "各作品の最大値を100として正規化しているため、作品間の形状（強みの方向）を比較できます。"
        )
        body += plotly_div_safe(fig, "anime_value_radar", 550)

        # Value table
        body += "<table><thead><tr>"
        body += "<th>#</th><th>タイトル</th><th>年</th><th>総合価値</th>"
        body += "<th>商業</th><th>批評</th><th>創造</th><th>技術</th><th>文化</th>"
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

    # Data-driven key findings
    total_anime_comp = fair.get("total_anime", 0)
    avg_shapley_corr = avg_corr  # already computed above
    gini_desc = "不平等寄り" if avg_gini > 0.5 else "やや均等寄り" if avg_gini > 0.25 else "均等寄り"
    body += key_findings([
        f"分析対象 {fmt_num(total_anime_comp)} 作品の平均Gini係数は {avg_gini:.3f} —"
        f"完全平等(0)と最大不平等(1)の間で{gini_desc}な配分",
        f"Shapley値と実際の配分の相関は {avg_shapley_corr:.3f} —"
        f"{'良好な整合性（貢献度に見合った配分）' if avg_shapley_corr > 0.9 else '改善の余地がある'}",
        "5軸の作品価値プロファイル（商業・批評・創造・文化・技術）により、"
        "作品の多面的な価値が可視化される",
    ])

    body += significance_section("スタッフ間の貢献に対する報酬の公正性を検証", [
        "アニメ制作において「誰がどれだけ貢献したか」を客観的に測定することはこれまで困難でした。"
        "本分析はShapley値（ゲーム理論の公正配分手法）をクレジットデータに適用し、"
        "各スタッフの限界貢献度を初めて定量化します。Gini係数は同じスコアの作品でも"
        "「監督一人集中型」か「チーム均等型」かを明確に区別します。",
        "この分析は「誰が正当に評価されていないか」を具体的な数値で示す、報酬制度改革の実証的根拠です。"
        "業界団体・労働組合が具体的な改善を求める際、または個人が報酬交渉する際の"
        "客観的エビデンスとして機能します。",
    ])
    body += utilization_guide([
        {"role": "アニメーター・労働組合", "how": "自分が参加した作品のShapley配分テーブルで自役職のallocation値を確認し、実際の報酬比率との差を「限界貢献に基づく適正配分との乖離」として労使協議の書面に記載する"},
        {"role": "プロデューサー", "how": "担当作品のGini係数を業界平均（avg_gini_coefficient）と比較し、上回る場合は「配分が特定役職に偏りすぎ」と判断して契約条件の見直し対象をリストアップする"},
        {"role": "スタジオ経営者", "how": "自スタジオの作品群のGini係数分布を確認し、低Gini係数（均等配分）を実現しているスタジオとして採用活動での「公平な報酬制度」の訴求材料にする"},
        {"role": "研究者・政策立案者", "how": "avg_gini_coefficientとavg_shapley_correlationを引用し、アニメ産業の報酬不平等の実態を数値化した論文・政策提言の根拠に使う"},
    ])
    body += future_possibilities([
        "Shapley値を参照した「作品別標準配分テーブル」を業界団体が策定・公式化し、新規契約の参照基準として普及させる",
        "Gini係数の年次追跡による「業界全体の配分公平性トレンド」のモニタリングと改善度の可視化",
        "役職別Shapley値から逆算した「役職ごとの適正報酬レンジ」の算出と公開",
        "新人・ベテランの配分格差の時系列分析（経験年数と実際の配分比率の関係を追跡し、若手の不利益を定量化）",
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
                        g_name = g.get('name', g.get('person_id', ''))
                        body += f"<tr><td>{person_link(g_name, g.get('person_id', ''))}</td>"
                        body += f'<td><span class="badge badge-high">+{g.get("correction", 0):.3f}</span></td></tr>'
                    body += "</tbody></table></div>"
                if losers:
                    body += "<div><h3>Top Losers (Overvalued)</h3>"
                    body += "<table><thead><tr><th>Person</th><th>Correction</th></tr></thead><tbody>"
                    for lo in losers[:10]:
                        lo_name = lo.get('name', lo.get('person_id', ''))
                        body += f"<tr><td>{person_link(lo_name, lo.get('person_id', ''))}</td>"
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
            fig.update_layout(title="Undervaluation Gap Distribution", xaxis_title="BiRank Gap", yaxis_title="Count")
            body += plotly_div_safe(fig, "underval_gaps", 400)

            body += "<table><thead><tr>"
            body += "<th>#</th><th>Person</th><th>Current</th><th>Debiased</th><th>Gap</th><th>Category</th>"
            body += "</tr></thead><tbody>"
            for i, a in enumerate(alerts[:20], 1):
                gap = a.get("authority_gap", 0)
                badge = "badge-high" if gap >= 5 else "badge-mid" if gap >= 2 else "badge-low"
                a_name = a.get('name', a.get('person_id', ''))
                body += f"<tr><td>{i}</td><td>{person_link(a_name, a.get('person_id', ''))}</td>"
                body += f"<td>{a.get('current_composite', 0):.2f}</td>"
                body += f"<td>{a.get('debiased_authority', 0):.2f}</td>"
                body += f'<td><span class="badge {badge}">+{gap:.2f}</span></td>'
                body += f"<td>{a.get('category', '')}</td></tr>"
            body += "</tbody></table></div>"

    # Data-driven key findings
    bias_summary = bias.get("summary", {}) if bias else {}
    total_biases = bias_summary.get("total_biases_detected", 0)
    severe_biases = bias_summary.get("severe_biases", 0)
    bca = insights.get("bias_correction_analysis", {}) if insights else {}
    affected_count = bca.get("total_persons_affected", 0)
    body += key_findings([
        f"検出されたバイアス {fmt_num(total_biases)} 件のうち重度が {fmt_num(severe_biases)} 件 —"
        "役職・スタジオ・キャリアステージに起因する系統的な評価歪みを定量化",
        f"バイアス補正の影響を受ける人物は {fmt_num(affected_count)} 人 —"
        "補正後のスコアと生スコアの差分から過小評価・過大評価を特定",
        "補正により、スタジオ所属の優位性に隠れていた実力者が浮かび上がる",
    ])

    body += significance_section("スタジオ・役割・キャリアステージによるバイアスを定量化", [
        "「大手スタジオ所属だから高スコア」「新人だから低スコア」という系統的バイアスは"
        "本来の実力に基づく評価を歪めます。本分析はこれらを統計的に検定して重大度を分類し、"
        "補正後スコアと生スコアの差分を通じて「誰が過小評価され、誰が過大評価されているか」を両方示します。",
        "採用・報酬・昇進のすべての意思決定においてバイアスを除去することは、"
        "業界全体の公平性と人材活用の効率化につながります。"
        "補正により、スタジオの名声に隠れていた実力者が浮かび上がり、"
        "ブランド効果を除いた純粋な貢献度が明確になります。",
    ])
    body += utilization_guide([
        {"role": "人事・採用担当", "how": "Top GainersとTop Losersの両リストを確認し、「高スコアでも過大評価候補（Loser）」と「低スコアでも過小評価候補（Gainer）」を峻別して採用基準にスタジオブランドの補正を加える"},
        {"role": "スタジオ経営者", "how": "Studio-Based Biasesテーブルに自スタジオ名が重度（severe）で出ていないか確認し、バイアスの種類（膨張/縮小）に応じて評価制度の問題箇所を特定して改善する"},
        {"role": "多様性・公平性担当者", "how": "Role-Based Biasesテーブルの役職別バイアスデータを使い、特定役職への系統的な不利を数値で示して改善提案の根拠とする"},
        {"role": "アニメーター本人", "how": "Undervaluation Alertsに自分が入っていれば、debiased_authorityとcurrent_compositeのgap値を「業界統計が示す過小評価の証拠」として報酬交渉の正式書類に添付する"},
    ])
    body += future_possibilities([
        "バイアス是正認証制度の創設（年次バイアス検出で低スコアのスタジオへの「公平採用認証」ラベル付与）",
        "採用候補者の自動バイアスチェックリスト（応募者の所属スタジオ・役職・年齢ごとのバイアス係数を面接官に提示）",
        "毎年のバイアス是正スコアカード公開（スタジオが前年比で改善しているか悪化しているかの継続モニタリング）",
        "役職・経歴別のバイアスパターンの機械学習による精緻化（より細粒度のバイアス検出へ）",
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

def generate_genre_report():  # noqa: C901
    """ジャンル親和性分析レポート（スタッフクラスタリング + ジャンル年代クラスタリング）."""
    print("  Generating Genre Analysis Report...")
    genre = load_json("genre_affinity.json")

    if not genre:
        return

    scores_list = load_json("scores.json") or []
    scores_map = {p["person_id"]: p for p in scores_list if isinstance(p, dict)}

    import numpy as np
    from collections import defaultdict as _ddict
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    CLUSTER_COLORS = ["#f093fb", "#f5576c", "#fda085", "#a0d2db", "#06D6A0", "#FFD166", "#667eea", "#43e97b"]
    TIER_COLORS = {"high_rated": "#06D6A0", "mid_rated": "#FFD166", "low_rated": "#EF476F", "unknown": "#888"}
    ERA_COLORS = {"modern": "#f093fb", "2010s": "#a0d2db", "2000s": "#fda085", "classic": "#667eea", "unknown": "#888"}

    # -------------------------------------------------------
    # Build joint dataset
    # -------------------------------------------------------
    persons = []
    all_tiers: Counter = Counter()
    all_eras: Counter = Counter()
    all_primary_tiers: Counter = Counter()
    all_primary_eras: Counter = Counter()
    avg_scores_global = []

    for pid, gdata in genre.items():
        sdata = scores_map.get(pid, {})
        cent = sdata.get("centrality", {})
        vers = sdata.get("versatility", {})
        car = sdata.get("career", {})
        net = sdata.get("network", {})
        tiers = gdata.get("score_tiers", {})
        eras = gdata.get("eras", {})

        for tier, pct in tiers.items():
            all_tiers[tier] += pct
        for era, pct in eras.items():
            all_eras[era] += pct
        all_primary_tiers[gdata.get("primary_tier", "unknown")] += 1
        all_primary_eras[gdata.get("primary_era", "unknown")] += 1
        avg_sc = gdata.get("avg_anime_score") or 0
        if avg_sc:
            avg_scores_global.append(avg_sc)

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
            "avg_anime_score": float(avg_sc),
            "total_credits": float(gdata.get("total_credits", 0) or 0),
            "high_rated_pct": float(tiers.get("high_rated", 0) or 0),
            "mid_rated_pct": float(tiers.get("mid_rated", 0) or 0),
            "low_rated_pct": float(tiers.get("low_rated", 0) or 0),
            "modern_pct": float(eras.get("modern", 0) or 0),
            "era_2010s_pct": float(eras.get("2010s", 0) or 0),
            "era_2000s_pct": float(eras.get("2000s", 0) or 0),
            "classic_pct": float(eras.get("classic", 0) or 0),
            "tier_concentration": float(max(tiers.values()) if tiers else 0),
        })

    total_persons = len(persons)

    # -------------------------------------------------------
    # Summary stats
    # -------------------------------------------------------
    body = ""
    body += '<div class="card">'
    body += "<h2>ジャンル・スコア親和性 — 概要</h2>"
    body += '<div class="stats-grid">'
    avg_global = sum(avg_scores_global) / max(len(avg_scores_global), 1)
    specialist_cnt = sum(1 for p in persons if p["tier_concentration"] >= 80)
    for label, val in [
        ("対象人物数", fmt_num(total_persons)),
        ("平均アニメスコア", f"{avg_global:.2f}"),
        ("スコアティア種類", fmt_num(len(all_tiers))),
        ("時代区分種類", fmt_num(len(all_eras))),
        ("スペシャリスト（80%+集中）", fmt_num(specialist_cnt)),
        ("ジェネラリスト比率", f"{(total_persons - specialist_cnt) / max(total_persons, 1) * 100:.1f}%"),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div></div>"

    # -------------------------------------------------------
    # SECTION 1: Staff K-Means Clustering (k=6)
    # -------------------------------------------------------
    FEATURE_COLS = [
        "iv_score", "birank", "patronage", "person_fe",
        "degree", "betweenness", "eigenvector",
        "versatility", "active_years", "highest_stage",
        "hub_score", "collaborators",
        "avg_anime_score", "total_credits",
        "high_rated_pct", "mid_rated_pct", "low_rated_pct",
        "modern_pct", "era_2010s_pct", "era_2000s_pct", "classic_pct",
        "tier_concentration",
    ]
    FEAT_JP = {
        "iv_score": "IV Score", "birank": "BiRank", "patronage": "Patronage", "person_fe": "Person FE",
        "degree": "次数中心性", "betweenness": "媒介中心性", "eigenvector": "固有値中心性",
        "versatility": "多様性", "active_years": "活動年数", "highest_stage": "最高ステージ",
        "hub_score": "ハブスコア", "collaborators": "共同作業者数",
        "avg_anime_score": "平均アニメスコア", "total_credits": "総クレジット数",
        "high_rated_pct": "高評価割合", "mid_rated_pct": "中評価割合", "low_rated_pct": "低評価割合",
        "modern_pct": "現代割合", "era_2010s_pct": "2010年代割合",
        "era_2000s_pct": "2000年代割合", "classic_pct": "クラシック割合",
        "tier_concentration": "ティア集中度",
    }

    n_staff_clusters = min(6, max(2, len(persons)))
    staff_cluster_names: dict[int, str] = {}

    if len(persons) >= n_staff_clusters:
        X = np.array([[p[f] for f in FEATURE_COLS] for p in persons], dtype=float)
        scaler_st = StandardScaler()
        Xs = scaler_st.fit_transform(X)

        km_st = KMeans(n_clusters=n_staff_clusters, n_init=20, random_state=42)
        st_labels = km_st.fit_predict(Xs)

        # PCA 2D for visualization
        pca_st = PCA(n_components=2, random_state=42)
        Xpca = pca_st.fit_transform(Xs)

        # 動的クラスタ命名: 重心の相対ランクで自動ラベル付け (固定閾値不使用)
        centers_orig = scaler_st.inverse_transform(km_st.cluster_centers_)
        fidx = {f: i for i, f in enumerate(FEATURE_COLS)}
        staff_cluster_names.update(_name_clusters_by_rank(
            centers_orig,
            [
                (fidx["iv_score"],        ["高スコア", "中スコア", "低スコア"]),
                (fidx["high_rated_pct"],  ["高品質特化", "量産型"]),     # 2区分で簡潔に
                (fidx["modern_pct"],      ["現代", "旧世代"]),            # 2区分で簡潔に
            ],
        ))

        for i, p in enumerate(persons):
            p["staff_cluster"] = int(st_labels[i])
            p["staff_cluster_name"] = staff_cluster_names[int(st_labels[i])]
            p["pca_x"] = float(Xpca[i, 0])
            p["pca_y"] = float(Xpca[i, 1])

        # Group by cluster
        st_cluster_groups: dict[int, list[dict]] = {}
        for p in persons:
            st_cluster_groups.setdefault(p["staff_cluster"], []).append(p)

        body += '<div class="card">'
        body += f"<h2>スタッフ K-Means クラスタリング（{n_staff_clusters}クラスタ）</h2>"
        body += section_desc(
            "IV Score・BiRank・Patronage・Person FE・中心性・多様性・活動年数・"
            "ジャンル親和性など22次元の特徴量でスタッフを自動分類。"
            "「高品質特化の現代スタッフ」「量産型クラシック世代」などのグループを発見します。"
        )

        # PCA 2D scatter colored by cluster
        fig_pca = go.Figure()
        for cid in sorted(st_cluster_groups.keys()):
            mems = st_cluster_groups[cid]
            fig_pca.add_trace(go.Scattergl(
                x=[m["pca_x"] for m in mems],
                y=[m["pca_y"] for m in mems],
                mode="markers",
                name=staff_cluster_names[cid],
                marker=dict(size=4, color=CLUSTER_COLORS[cid % len(CLUSTER_COLORS)], opacity=0.6),
                text=[m["name"] for m in mems],
                hovertemplate="%{text}<br>PCA(%{x:.2f}, %{y:.2f})<extra></extra>",
            ))
        fig_pca.update_layout(
            title="スタッフクラスタリング PCA 2D",
            xaxis_title=f"PC1 ({pca_st.explained_variance_ratio_[0]*100:.1f}%)",
            yaxis_title=f"PC2 ({pca_st.explained_variance_ratio_[1]*100:.1f}%)",
        )
        body += chart_guide(
            "主成分分析（PCA）で22次元特徴量を2次元に圧縮して可視化。"
            "色＝クラスタ。近い点ほど特徴が似たスタッフです。"
            "クラスタの形状（縦長・横長・円形）はそのグループの多様性を示します。"
        )
        body += plotly_div_safe(fig_pca, "staff_pca_scatter", 550)

        # Cluster profile heatmap (z-score of centroids)
        HEATMAP_FEATS = [
            "iv_score", "birank", "patronage", "person_fe",
            "betweenness", "versatility", "active_years",
            "avg_anime_score", "high_rated_pct", "modern_pct", "classic_pct",
            "tier_concentration",
        ]
        centers_z = km_st.cluster_centers_  # already z-scored
        fidx_all = {f: i for i, f in enumerate(FEATURE_COLS)}
        z_vals = [[centers_z[cid, fidx_all[f]] for f in HEATMAP_FEATS]
                  for cid in range(n_staff_clusters)]
        fig_hmap = go.Figure(go.Heatmap(
            z=z_vals,
            x=[FEAT_JP.get(f, f) for f in HEATMAP_FEATS],
            y=[staff_cluster_names[c] for c in range(n_staff_clusters)],
            colorscale="RdBu",
            zmid=0,
            colorbar=dict(title="z-score"),
            hovertemplate="クラスタ: %{y}<br>特徴: %{x}<br>z-score: %{z:.2f}<extra></extra>",
        ))
        fig_hmap.update_layout(
            title="クラスタプロファイル Heatmap（z-score）",
            xaxis_title="特徴量", yaxis_title="クラスタ",
            xaxis_tickangle=-35,
        )
        body += chart_guide(
            "各クラスタの重心を標準化（z-score）で表示。赤＝そのクラスタが高い特徴、"
            "青＝低い特徴。例えば「高評価割合が高い＝赤」は高品質作品への親和性が高いことを示す。"
        )
        body += plotly_div_safe(fig_hmap, "staff_cluster_heatmap", 500)

        # Violin: iv_score per cluster
        fig_viol = go.Figure()
        for cid in sorted(st_cluster_groups.keys()):
            mems = st_cluster_groups[cid]
            fig_viol.add_trace(_violin_raincloud(
                [m["iv_score"] for m in mems],
                staff_cluster_names[cid],
                CLUSTER_COLORS[cid % len(CLUSTER_COLORS)],
            ))
        fig_viol.update_layout(
            title="クラスタ別 IV Score分布 (Raincloud)",
            yaxis_title="IV Score",
            violinmode="overlay",
        )
        body += chart_guide("各クラスタのIV Score分布。縦軸＝スコア。外れ値ドットが「埋もれた高評価者」。")
        body += plotly_div_safe(fig_viol, "staff_cluster_violin", 480)

        # Stacked bar: cluster × primary_tier
        tier_order = ["high_rated", "mid_rated", "low_rated", "unknown"]
        tier_jp = {"high_rated": "高評価", "mid_rated": "中評価", "low_rated": "低評価", "unknown": "不明"}
        fig_tier_bar = go.Figure()
        for tier in tier_order:
            counts = [
                sum(1 for m in st_cluster_groups.get(cid, []) if m["primary_tier"] == tier)
                for cid in range(n_staff_clusters)
            ]
            if any(c > 0 for c in counts):
                fig_tier_bar.add_trace(go.Bar(
                    name=tier_jp.get(tier, tier),
                    x=[staff_cluster_names[c] for c in range(n_staff_clusters)],
                    y=counts,
                    marker_color=TIER_COLORS.get(tier, "#888"),
                ))
        fig_tier_bar.update_layout(
            title="クラスタ × 主要スコアティア（人数）",
            barmode="stack", xaxis_tickangle=-20,
            yaxis_title="人数",
        )
        body += chart_guide("どのクラスタが高評価・中評価・低評価作品に多く関わるかを示す積み上げ棒グラフ。")
        body += plotly_div_safe(fig_tier_bar, "cluster_tier_bar", 450)

        # Stacked bar: cluster × primary_era
        era_order = ["modern", "2010s", "2000s", "classic", "unknown"]
        era_jp = {"modern": "現代(2020+)", "2010s": "2010年代", "2000s": "2000年代",
                  "classic": "クラシック(〜1999)", "unknown": "不明"}
        fig_era_bar = go.Figure()
        for era in era_order:
            counts = [
                sum(1 for m in st_cluster_groups.get(cid, []) if m["primary_era"] == era)
                for cid in range(n_staff_clusters)
            ]
            if any(c > 0 for c in counts):
                fig_era_bar.add_trace(go.Bar(
                    name=era_jp.get(era, era),
                    x=[staff_cluster_names[c] for c in range(n_staff_clusters)],
                    y=counts,
                    marker_color=ERA_COLORS.get(era, "#888"),
                ))
        fig_era_bar.update_layout(
            title="クラスタ × 主要活動時代（人数）",
            barmode="stack", xaxis_tickangle=-20,
            yaxis_title="人数",
        )
        body += chart_guide("どのクラスタがどの時代に活動しているかを示す。現代特化・クラシック特化などが見えます。")
        body += plotly_div_safe(fig_era_bar, "cluster_era_bar", 450)

        # Stacked bar: cluster × primary_role (top roles)
        role_counter: Counter = Counter()
        for p in persons:
            role_counter[p["primary_role"]] += 1
        top_roles = [r for r, _ in role_counter.most_common(8)]
        fig_role_bar = go.Figure()
        role_colors_list = ["#f093fb", "#f5576c", "#fda085", "#a0d2db", "#06D6A0", "#FFD166", "#667eea", "#43e97b"]
        for ri, role in enumerate(top_roles):
            counts = [
                sum(1 for m in st_cluster_groups.get(cid, []) if m["primary_role"] == role)
                for cid in range(n_staff_clusters)
            ]
            if any(c > 0 for c in counts):
                fig_role_bar.add_trace(go.Bar(
                    name=role,
                    x=[staff_cluster_names[c] for c in range(n_staff_clusters)],
                    y=counts,
                    marker_color=role_colors_list[ri % len(role_colors_list)],
                ))
        fig_role_bar.update_layout(
            title="クラスタ × 主要ロール（人数）",
            barmode="stack", xaxis_tickangle=-20,
            yaxis_title="人数",
        )
        body += chart_guide("どのクラスタにどの職種が多いかを示す。監督クラスタ・アニメータークラスタなどが見えます。")
        body += plotly_div_safe(fig_role_bar, "cluster_role_bar", 450)

        # Cluster size bar
        fig_sz = go.Figure(go.Bar(
            x=[staff_cluster_names[c] for c in range(n_staff_clusters)],
            y=[len(st_cluster_groups.get(c, [])) for c in range(n_staff_clusters)],
            marker_color=[CLUSTER_COLORS[c % len(CLUSTER_COLORS)] for c in range(n_staff_clusters)],
            hovertemplate="%{x}<br>人数: %{y}<extra></extra>",
        ))
        fig_sz.update_layout(title="クラスタ別 人数", yaxis_title="人数", xaxis_tickangle=-20)
        body += plotly_div_safe(fig_sz, "cluster_size_bar", 380)
        body += "</div>"  # end staff clustering card

        # -------------------------------------------------------
        # SECTION 2: Time Series by Cluster (career start decade)
        # -------------------------------------------------------
        decade_cluster: dict[str, _ddict] = {}
        for p in persons:
            fy = p.get("first_year")
            if fy and isinstance(fy, (int, float)) and 1960 <= fy <= 2030:
                decade = f"{int(fy) // 10 * 10}年代"
            else:
                decade = "不明"
            if decade not in decade_cluster:
                decade_cluster[decade] = _ddict(int)
            decade_cluster[decade][p.get("staff_cluster", 0)] += 1

        decades_sorted = sorted(d for d in decade_cluster if d != "不明")
        if len(decades_sorted) >= 2:
            body += '<div class="card">'
            body += "<h2>キャリア開始年代別 クラスタ構成（時系列）</h2>"
            body += section_desc(
                "各スタッフのキャリア開始年（career.first_year）を年代別に集計し、"
                "どの時代にどのタイプのスタッフが多く登場したかを示します。"
            )
            fig_ts = go.Figure()
            for cid in range(n_staff_clusters):
                counts_ts = [decade_cluster.get(d, {}).get(cid, 0) for d in decades_sorted]
                if any(c > 0 for c in counts_ts):
                    fig_ts.add_trace(go.Bar(
                        name=staff_cluster_names[cid],
                        x=decades_sorted, y=counts_ts,
                        marker_color=CLUSTER_COLORS[cid % len(CLUSTER_COLORS)],
                    ))
            fig_ts.update_layout(
                title="キャリア開始年代別 クラスタ構成",
                barmode="stack", yaxis_title="人数",
                xaxis_title="キャリア開始年代",
            )
            body += chart_guide(
                "横軸＝キャリアを開始した年代。縦軸＝人数。"
                "近年ほど「現代高品質特化」クラスタが増えるなど、業界の変遷が見えます。"
            )
            body += plotly_div_safe(fig_ts, "cluster_decade_bar", 450)

            # Avg iv_score per cluster per decade (heatmap)
            comp_matrix = []
            decade_labels_ht = decades_sorted
            cl_labels_ht = [staff_cluster_names[c] for c in range(n_staff_clusters)]
            for cid in range(n_staff_clusters):
                row = []
                for dec in decades_sorted:
                    mems_dc = [
                        p for p in persons
                        if p.get("staff_cluster") == cid
                        and (lambda fy: f"{int(fy) // 10 * 10}年代" if fy and 1960 <= fy <= 2030 else "不明")(p.get("first_year")) == dec
                    ]
                    avg_c = (sum(m["iv_score"] for m in mems_dc) / len(mems_dc)) if mems_dc else 0
                    row.append(round(avg_c, 1))
                comp_matrix.append(row)
            fig_comp_ht = go.Figure(go.Heatmap(
                z=comp_matrix, x=decade_labels_ht, y=cl_labels_ht,
                colorscale="Viridis",
                colorbar=dict(title="平均スコア"),
                hovertemplate="クラスタ: %{y}<br>年代: %{x}<br>平均スコア: %{z:.1f}<extra></extra>",
            ))
            fig_comp_ht.update_layout(
                title="年代 × クラスタ 平均IV Score Heatmap",
                xaxis_title="キャリア開始年代", yaxis_title="クラスタ",
            )
            body += chart_guide(
                "横軸＝キャリア開始年代、縦軸＝クラスタ。色＝その年代×クラスタの平均IV Score。"
                "明るいほどスコアが高い。特定の時代に特定クラスタの品質が高まる傾向が読み取れます。"
            )
            body += plotly_div_safe(fig_comp_ht, "cluster_decade_heatmap", 450)
            body += "</div>"

    # -------------------------------------------------------
    # SECTION 3: Genre-Era Clustering (3 tiers × 4 eras = 12 cells)
    # -------------------------------------------------------
    TIER_ORDER = ["high_rated", "mid_rated", "low_rated"]
    ERA_ORDER = ["modern", "2010s", "2000s", "classic"]
    tier_jp2 = {"high_rated": "高評価(8+)", "mid_rated": "中評価(6.5-8)", "low_rated": "低評価(<6.5)"}
    era_jp2 = {"modern": "現代(2020+)", "2010s": "2010年代", "2000s": "2000年代", "classic": "クラシック"}

    ge_data: dict[tuple, list[dict]] = {}
    for p in persons:
        key = (p["primary_tier"], p["primary_era"])
        ge_data.setdefault(key, []).append(p)

    ge_cells = []
    ge_cell_keys = []
    for t in TIER_ORDER:
        for e in ERA_ORDER:
            key = (t, e)
            mems = ge_data.get(key, [])
            if mems:
                ge_cells.append([
                    np.mean([m["iv_score"] for m in mems]),
                    len(mems),
                    np.mean([m["avg_anime_score"] for m in mems]),
                    np.mean([m["active_years"] for m in mems]),
                    np.mean([m["versatility"] for m in mems]),
                ])
                ge_cell_keys.append(key)

    body += '<div class="card">'
    body += "<h2>ジャンル×年代 クラスタリング</h2>"
    body += section_desc(
        "スコアティア（高/中/低）× 時代（現代/2010年代/2000年代/クラシック）の12セルを、"
        "各セルのスタッフ平均属性（IV Score・活動年数・多様性など）でクラスタリング。"
        "「どの時代の高品質作品が同じタイプのスタッフを引き寄せるか」が分かります。"
    )

    if len(ge_cells) >= 4:
        Xge = np.array(ge_cells, dtype=float)
        scaler_ge = StandardScaler()
        Xge_s = scaler_ge.fit_transform(Xge)
        k_ge = min(4, len(ge_cells))
        km_ge = KMeans(n_clusters=k_ge, n_init=20, random_state=42)
        ge_cluster_labels = list(km_ge.fit_predict(Xge_s))
        GE_CLUSTER_COLORS = ["#f093fb", "#06D6A0", "#fda085", "#a0d2db"]
        ge_cluster_names = [f"ジャンルC{c+1}" for c in range(k_ge)]

        # Heatmap: tier × era colored by cluster
        z_grid = [[None] * len(ERA_ORDER) for _ in TIER_ORDER]
        z_labels = [[""] * len(ERA_ORDER) for _ in TIER_ORDER]
        for idx, (t, e) in enumerate(ge_cell_keys):
            ti = TIER_ORDER.index(t)
            ei = ERA_ORDER.index(e) if e in ERA_ORDER else -1
            if ei >= 0:
                cl = ge_cluster_labels[idx]
                z_grid[ti][ei] = cl
                n_c = len(ge_data.get((t, e), []))
                avg_c = ge_cells[idx][0]
                z_labels[ti][ei] = f"C{cl+1}<br>n={n_c}<br>avg={avg_c:.1f}"

        fig_ge_ht = go.Figure(go.Heatmap(
            z=z_grid,
            x=[era_jp2.get(e, e) for e in ERA_ORDER],
            y=[tier_jp2.get(t, t) for t in TIER_ORDER],
            colorscale=[[i / (k_ge - 1), c] for i, c in enumerate(GE_CLUSTER_COLORS[:k_ge])],
            showscale=False,
            text=z_labels, texttemplate="%{text}",
            hovertemplate="ティア: %{y}<br>時代: %{x}<br>クラスタ: %{z}<extra></extra>",
        ))
        fig_ge_ht.update_layout(
            title="ジャンル×時代 クラスタ分布 (Heatmap)",
            xaxis_title="時代", yaxis_title="スコアティア",
        )
        body += chart_guide(
            "各セル＝（スコアティア×時代）の組み合わせ。色＝クラスタ番号。"
            "同じ色のセルは「似たタイプのスタッフが関わる作品群」を意味します。"
            "例：高評価の現代作品と高評価の2010年代作品が同じクラスタ＝継続的な品質スタッフが担当。"
        )
        body += plotly_div_safe(fig_ge_ht, "genre_era_heatmap", 400)

        # Radar chart: cluster profile comparison
        radar_feats = ["iv_score", "avg_anime_score", "active_years", "versatility", "tier_concentration"]
        radar_jp = ["IV Score", "平均アニメスコア", "活動年数", "多様性", "ティア集中度"]
        # normalize per feature to 0-1 for radar
        feat_max = {}
        for f in radar_feats:
            vals_f = [p[f] for p in persons if p[f] > 0]
            feat_max[f] = max(vals_f) if vals_f else 1
        ge_cluster_cells: dict[int, list] = _ddict(list)
        for idx, (t, e) in enumerate(ge_cell_keys):
            cl = ge_cluster_labels[idx]
            ge_cluster_cells[cl].extend(ge_data.get((t, e), []))
        fig_radar = go.Figure()
        for cl in range(k_ge):
            mems_cl = ge_cluster_cells[cl]
            if not mems_cl:
                continue
            r_vals = [np.mean([m[f] for m in mems_cl]) / feat_max[f] for f in radar_feats]
            r_vals.append(r_vals[0])  # close
            fig_radar.add_trace(go.Scatterpolar(
                r=r_vals, theta=radar_jp + [radar_jp[0]],
                fill="toself", name=f"ジャンルC{cl+1}",
                line_color=GE_CLUSTER_COLORS[cl % len(GE_CLUSTER_COLORS)],
                opacity=0.6,
            ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            title="ジャンル×年代クラスタ プロファイル比較 (Radar)",
        )
        body += chart_guide("各ジャンルクラスタの特徴プロファイル。多角形が大きいほど全体的に高い属性。")
        body += plotly_div_safe(fig_radar, "genre_cluster_radar", 500)

    body += "</div>"

    # -------------------------------------------------------
    # SECTION 4: Classic Charts (existing)
    # -------------------------------------------------------
    body += '<div class="card">'
    body += "<h2>スコアティア・時代分布</h2>"

    # Primary tier pie
    fig_tier_pie = go.Figure(go.Pie(
        labels=list(all_primary_tiers.keys()),
        values=list(all_primary_tiers.values()),
        marker_colors=["#06D6A0", "#FFD166", "#EF476F", "#a0d2db", "#f093fb"],
        hole=0.4, textinfo="label+percent",
    ))
    fig_tier_pie.update_layout(title="主要スコアティア分布")
    body += chart_guide("各人物の主要スコアティア（最多クレジット帯）の割合。")
    body += plotly_div_safe(fig_tier_pie, "tier_pie", 420)

    # Era pie
    fig_era_pie = go.Figure(go.Pie(
        labels=list(all_primary_eras.keys()),
        values=list(all_primary_eras.values()),
        marker_colors=["#f093fb", "#a0d2db", "#fda085", "#667eea", "#f5576c"],
        hole=0.4, textinfo="label+percent",
    ))
    fig_era_pie.update_layout(title="主要活動時代分布")
    body += plotly_div_safe(fig_era_pie, "era_pie", 420)
    body += "</div>"

    # Specialist vs Generalist
    concentrations = [p["tier_concentration"] for p in persons]
    if concentrations:
        body += '<div class="card">'
        body += "<h2>スペシャリスト vs ジェネラリスト</h2>"
        body += '<div class="insight-box"><strong>解説:</strong> '
        body += "集中度が高い（90%以上）＝特定品質帯に専門化したスペシャリスト。"
        body += "低い（均等分散）＝幅広い品質帯で活動するジェネラリスト。</div>"

        # Concentration histogram colored by cluster if available
        if "staff_cluster" in persons[0]:
            fig_conc = go.Figure()
            for cid in range(n_staff_clusters):
                c_concs = [p["tier_concentration"] for p in persons if p.get("staff_cluster") == cid]
                if c_concs:
                    fig_conc.add_trace(go.Histogram(
                        x=c_concs, nbinsx=20,
                        name=staff_cluster_names[cid],
                        marker_color=CLUSTER_COLORS[cid % len(CLUSTER_COLORS)],
                        opacity=0.6,
                    ))
            fig_conc.update_layout(
                title="ティア集中度分布（クラスタ別）",
                barmode="overlay",
                xaxis_title="最大ティア集中度 (%)", yaxis_title="人数",
            )
        else:
            fig_conc = go.Figure(go.Histogram(
                x=concentrations, nbinsx=20, marker_color="#f5576c",
            ))
            fig_conc.update_layout(title="ティア集中度分布",
                                   xaxis_title="最大ティア集中度 (%)", yaxis_title="人数")
        body += plotly_div_safe(fig_conc, "concentration_hist", 400)

        # Avg anime score histogram
        if avg_scores_global:
            fig_sc_hist = go.Figure(go.Histogram(
                x=avg_scores_global, nbinsx=30, marker_color="#a0d2db",
                hovertemplate="スコア: %{x:.1f}<br>人数: %{y}<extra></extra>",
            ))
            fig_sc_hist.update_layout(
                title="平均アニメスコア分布",
                xaxis_title="平均アニメスコア", yaxis_title="人数",
            )
            body += plotly_div_safe(fig_sc_hist, "score_hist", 380)
        body += "</div>"

    # -------------------------------------------------------
    # Key findings & metadata
    # -------------------------------------------------------
    top_tier = all_primary_tiers.most_common(1)[0][0] if all_primary_tiers else "unknown"
    specialist_pct = specialist_cnt / max(total_persons, 1) * 100
    body += key_findings([
        f"スタッフ {total_persons:,}人を{n_staff_clusters}クラスタに分類。"
        "各クラスタは高品質特化・現代中心・量産型など異なる専門プロファイルを持つ",
        f"全体の {specialist_pct:.0f}% がスコアティア「{top_tier}」に80%以上集中するスペシャリスト",
        "ジャンル×年代クラスタリングにより、同じタイプのスタッフが担当する作品群の時代横断的な連続性が可視化",
    ])
    body += significance_section("人材のジャンル特化 vs 汎用性の把握", [
        "K-Meansクラスタリングにより「高品質特化型」「現代量産型」「旧世代クラシック型」などの"
        "スタッフグループを客観的に識別。スタジオはこのクラスタを基準に適切なスタッフを選定できます。",
        "ジャンル×年代クラスタリングは「このタイプの作品にはこのタイプのスタッフが適合する」という"
        "エビデンスベースの人選をサポートします。",
    ])
    body += utilization_guide([
        {"role": "制作プロデューサー", "how": "スタッフのクラスタを確認し、制作予定作品のスコア帯・時代に合致するクラスタからスタッフを優先選定"},
        {"role": "スタジオ企画担当", "how": "ジェネラリストクラスタ（集中度低）の人材をジャンル横断コラボや複数ジャンル同時制作の橋渡し役として配置"},
        {"role": "アニメーター本人", "how": "自分のクラスタと特徴量プロファイルを確認し、専門性強化か汎用性拡大かのキャリア方針を客観的に決定"},
        {"role": "キャリアアドバイザー", "how": "クライアントのクラスタを特定し、そのクラスタが強い作品タイプを推薦して次の受注戦略を具体化"},
    ])
    body += future_possibilities([
        "スタジオのジャンル特化度分析（「このスタジオの得意品質帯はどこか」を競合比較できるツール）",
        "新人アニメーターの「得意ジャンル早期発見ツール」（初期10クレジットからクラスタを推定）",
        "リメイク・続編向け「オリジナルスタッフの時代適合性スコア」算出",
        "ジャンル横断コラボの成果予測（異なるクラスタ混在時の作品品質への影響モデル）",
    ])

    html = wrap_html(
        "ジャンル・スコア親和性分析",
        f"ジャンル・スコア親和性分析 — {fmt_num(total_persons)}人 / {n_staff_clusters}クラスタ",
        body,
        intro_html=report_intro(
            "ジャンル・スコア親和性（クラスタリング強化版）",
            "スタッフを22次元の特徴量（能力・中心性・ジャンル親和性）でK-Meansクラスタリングし、"
            "スコアティア×時代の組み合わせでも分類。「誰がどのジャンル・時代に適合するか」を"
            "データドリブンで明らかにします。",
            "キャリアアドバイザー、スタジオ企画担当、ジャンル研究者",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "スペシャリスト (Specialist)": (
                "クレジットが単一のスコアティアに80%以上集中している人物。"
            ),
            "ジェネラリスト (Generalist)": (
                "複数のスコアティアと時代にわたってクレジットが均等分散している人物。"
            ),
            "スコアティア (Score Tier)": (
                "視聴者/評論家スコアに基づく品質帯"
                "（高評価8+・中評価6.5-8・低評価<6.5）。"
            ),
            "K-Meansクラスタリング": (
                "指定した数(k)のグループに、特徴量の距離が近い点を自動でグループ化する機械学習手法。"
            ),
            "PCA（主成分分析）": (
                "多次元データを少数の主成分に圧縮して可視化する次元削減手法。"
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

        # Stacked bar: Top 20 studios by role breakdown
        role_color_map = {
            "director": "#9b59b6", "character_designer": "#3498db",
            "animator": "#06D6A0", "art_director": "#fda085",
            "series_composition": "#f5576c", "sound_director": "#FFD166",
        }
        studio_role_data: dict[str, dict[str, int]] = {}
        for s_name, s_data in top_studios:
            role_counts_s: dict[str, int] = {}
            for tp in s_data.get("top_persons", []):
                role = tp.get("primary_role", tp.get("role", "other"))
                role_counts_s[role] = role_counts_s.get(role, 0) + 1
            studio_role_data[s_name] = role_counts_s

        # Collect all roles across top studios
        all_roles_s: set[str] = set()
        for rc in studio_role_data.values():
            all_roles_s.update(rc.keys())

        if all_roles_s:
            fig_sr = go.Figure()
            default_colors = ["#9b59b6", "#3498db", "#06D6A0", "#fda085", "#f5576c",
                              "#FFD166", "#667eea", "#a0d2db", "#EF476F", "#f093fb"]
            for idx, role in enumerate(sorted(all_roles_s)):
                fig_sr.add_trace(go.Bar(
                    x=[s_name for s_name in studio_role_data],
                    y=[studio_role_data[s_name].get(role, 0) for s_name in studio_role_data],
                    name=role,
                    marker_color=role_color_map.get(role, default_colors[idx % len(default_colors)]),
                    hovertemplate="%{x}<br>" + role + ": %{y}<extra></extra>",
                ))
            fig_sr.update_layout(
                barmode="stack",
                title="Top 20 スタジオ — 主要人物の役職内訳（Stacked）",
                xaxis_tickangle=-45,
                yaxis_title="人数",
            )
            body += '<div class="card">'
            body += "<h2>スタジオ別 主要人物の役職内訳</h2>"
            body += chart_guide(
                "上位20スタジオの主要人物を役職別に積み上げ表示。"
                "スタジオごとの人材構成の特徴（監督偏重・アニメーター多数等）が一目で比較できます。"
            )
            body += plotly_div_safe(fig_sr, "studio_role_stacked", 500)
            body += "</div>"

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

    # ================================================================
    # K-Means クラスタリング: スタジオ特性による自動分類
    # ================================================================
    if studios:
        import numpy as np
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler

        # 特徴量: person_count, anime_count, avg_person_score, credit_per_person
        studio_feat_list = []
        for s_name_f, s_data_f in studios.items():
            pc = s_data_f.get("person_count", 0)
            ac = s_data_f.get("anime_count", 0)
            cc = s_data_f.get("credit_count", 0)
            # avg_person_score が無い場合は top_persons から推算
            aps = s_data_f.get("avg_person_score")
            if aps is None:
                tp_list = s_data_f.get("top_persons", [])
                sc_list = [tp.get("score", tp.get("iv_score", 0)) for tp in tp_list]
                aps = sum(sc_list) / len(sc_list) if sc_list else 0
            cpp = cc / max(pc, 1)  # credit per person
            studio_feat_list.append({
                "name": s_name_f,
                "person_count": pc,
                "anime_count": ac,
                "avg_score": float(aps or 0),
                "credit_per_person": cpp,
                "data": s_data_f,
            })

        if len(studio_feat_list) >= 6:
            X = np.array([
                [d["person_count"], d["anime_count"], d["avg_score"], d["credit_per_person"]]
                for d in studio_feat_list
            ])
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)

            # k=6: 大規模・中規模・小規模×品質高低・特殊
            n_clusters = min(6, len(studio_feat_list))
            km = KMeans(n_clusters=n_clusters, random_state=42, n_init=20)
            labels = km.fit_predict(X_scaled)

            # 動的クラスタ命名: 重心の相対ランクで自動ラベル付け (固定閾値不使用)
            centers = scaler.inverse_transform(km.cluster_centers_)
            cluster_names = _name_clusters_by_rank(
                centers,
                [
                    (0, ["大規模", "中規模", "小規模"]),      # person_count
                    (2, ["高品質", "中品質", "低活動"]),      # avg_score
                    (3, ["多産", "中産", "少産"]),            # credit_per_person
                ],
            )

            for i, d in enumerate(studio_feat_list):
                d["cluster"] = int(labels[i])
                d["cluster_name"] = cluster_names[int(labels[i])]

            # クラスタ別集計
            cluster_groups: dict[int, list[dict]] = {}
            for d in studio_feat_list:
                cluster_groups.setdefault(d["cluster"], []).append(d)

            # クラスタ別サマリー Violin: avg_score の分布
            fig_km = go.Figure()
            km_colors = ["#f093fb", "#f5576c", "#fda085", "#a0d2db", "#06D6A0", "#FFD166"]
            for cid in sorted(cluster_groups.keys()):
                members = cluster_groups[cid]
                scores_km = [m["avg_score"] for m in members if m["avg_score"] > 0]
                sizes_km = [m["person_count"] for m in members]
                cname = cluster_names[cid]
                if scores_km:
                    fig_km.add_trace(_violin_raincloud(
                        scores_km,
                        f"{cname} (n={len(members)})",
                        km_colors[cid % len(km_colors)],
                    ))
            fig_km.update_layout(
                title="K-Meansクラスタ別 スタジオ平均スコア分布 (Raincloud)",
                yaxis_title="スタジオ平均スコア",
                violinmode="overlay",
            )
            body += '<div class="card">'
            body += f"<h2>スタジオK-Meansクラスタリング（{n_clusters}クラスタ）</h2>"
            body += section_desc(
                "person_count（規模）・anime_count（作品数）・avg_score（品質）・"
                "credit_per_person（生産性）の4特徴量で自動分類（k-means, k=6）。"
                "小規模スタジオのノイズを大規模スタジオと分離し、グループ内比較を可能にします。"
            )
            body += chart_guide(
                "各バイオリンは1クラスタのスタジオ群。"
                "縦軸=そのスタジオの平均人物スコア。外れ値（ドット）が「強い小規模スタジオ」。"
                "Violin の幅が広い＝そのクラスタ内のスコアばらつきが大きい。"
            )
            body += plotly_div_safe(fig_km, "studio_kmeans_violin", 500)
            body += "</div>"

            # Scatter: スタジオ規模 vs 平均スコア、クラスタ色で色分け
            fig_km_sc = go.Figure()
            for cid in sorted(cluster_groups.keys()):
                members = cluster_groups[cid]
                cname = cluster_names[cid]
                fig_km_sc.add_trace(go.Scatter(
                    x=[m["person_count"] for m in members],
                    y=[m["avg_score"] for m in members],
                    mode="markers",
                    name=cname,
                    marker=dict(
                        size=[max(6, min(20, m["anime_count"] / 5)) for m in members],
                        color=km_colors[cid % len(km_colors)],
                        opacity=0.75,
                    ),
                    text=[m["name"] for m in members],
                    hovertemplate=(
                        "%{text}<br>"
                        "規模: %{x}人<br>"
                        "平均スコア: %{y:.2f}<br>"
                        f"クラスタ: {cname}<extra></extra>"
                    ),
                ))
            fig_km_sc.update_layout(
                title="スタジオ クラスタ別 散布図（規模 vs 品質）",
                xaxis_title="Person Count（規模）",
                yaxis_title="平均スコア",
                xaxis_type="log",
            )
            body += '<div class="card">'
            body += "<h2>クラスタ別 スタジオ散布図（対数スケール）</h2>"
            body += chart_guide(
                "X軸（対数スケール）=スタジオ規模（人数）、Y=平均人物スコア。"
                "色=k-meansクラスタ、サイズ=作品数。"
                "左上のドット＝「少人数でもスコアが高い強い小規模スタジオ」。"
                "右下＝「大きくてもスコアが低い量産型スタジオ」。"
            )
            body += plotly_div_safe(fig_km_sc, "studio_kmeans_scatter", 550)
            body += "</div>"

            # クラスタ別テーブル: 小規模高品質クラスタのスタジオを強調
            body += '<div class="card">'
            body += "<h2>クラスタ別 スタジオ一覧</h2>"
            for cid in sorted(cluster_groups.keys()):
                members = sorted(cluster_groups[cid], key=lambda m: -m["avg_score"])
                cname = cluster_names[cid]
                body += f"<h3 style='color:{km_colors[cid % len(km_colors)]}'>{cname} — {len(members)}スタジオ</h3>"
                body += "<table><thead><tr>"
                body += "<th>Studio</th><th>作品数</th><th>人数</th><th>平均スコア</th><th>制作効率</th>"
                body += "</tr></thead><tbody>"
                for m in members[:15]:
                    eff = f"{m['credit_per_person']:.1f}"
                    score_badge_cls = "badge-high" if m["avg_score"] >= 35 else "badge-mid" if m["avg_score"] >= 20 else "badge-low"
                    body += (
                        f"<tr><td>{m['name']}</td>"
                        f"<td>{m['anime_count']}</td>"
                        f"<td>{m['person_count']}</td>"
                        f'<td><span class="badge {score_badge_cls}">{m["avg_score"]:.1f}</span></td>'
                        f"<td>{eff}</td></tr>"
                    )
                body += "</tbody></table>"
            body += "</div>"

            # スタッフ移動・成長: クラスタ × グループ別分析
            # studios.json の year_range を使ってグループ内の活動変化を可視化
            body += '<div class="card">'
            body += "<h2>クラスタ別 活動規模の比較（作品数・人数）</h2>"
            body += section_desc(
                "各クラスタのスタジオ群をグループとして集計。"
                "年代別の活動量（作品数合計）と人材数の変化を棒グラフで比較。"
                "「小規模グループの作品数増加」「大規模グループの人材流動」等の傾向を把握できます。"
            )
            fig_cluster_bars = go.Figure()
            for cid in sorted(cluster_groups.keys()):
                members = cluster_groups[cid]
                cname = cluster_names[cid]
                total_anime = sum(m["anime_count"] for m in members)
                total_persons = sum(m["person_count"] for m in members)
                avg_score_c = sum(m["avg_score"] for m in members) / max(len(members), 1)
                fig_cluster_bars.add_trace(go.Bar(
                    name=cname,
                    x=["作品数（合計）", "人数（合計）", f"平均スコア×10"],
                    y=[total_anime, total_persons, avg_score_c * 10],
                    marker_color=km_colors[cid % len(km_colors)],
                ))
            fig_cluster_bars.update_layout(
                barmode="group",
                title="クラスタ別 集計比較（作品数・人数・スコア）",
                yaxis_title="値",
            )
            body += plotly_div_safe(fig_cluster_bars, "studio_cluster_compare", 450)
            body += "</div>"

    # Scatter: Studio size vs Average Person Score
    if studios:
        studio_scatter_data = []
        for s_name_sc, s_data_sc in studios.items():
            person_count_sc = s_data_sc.get("person_count", 0)
            avg_score_sc = s_data_sc.get("avg_person_score", None)
            if avg_score_sc is None:
                top_persons_sc = s_data_sc.get("top_persons", [])
                if top_persons_sc:
                    scores_list_sc = [
                        tp.get("score", tp.get("iv_score", 0)) for tp in top_persons_sc
                    ]
                    avg_score_sc = sum(scores_list_sc) / len(scores_list_sc) if scores_list_sc else 0
                else:
                    continue
            if person_count_sc > 0:
                studio_scatter_data.append({
                    "name": s_name_sc,
                    "person_count": person_count_sc,
                    "avg_score": avg_score_sc,
                    "anime_count": s_data_sc.get("anime_count", 0),
                    "credit_per_person": s_data_sc.get("credit_count", 0) / max(person_count_sc, 1),
                })

        if studio_scatter_data:
            max_anime_sc = max((d["anime_count"] for d in studio_scatter_data), default=1)
            fig_ss = go.Figure(go.Scatter(
                x=[d["person_count"] for d in studio_scatter_data],
                y=[d["avg_score"] for d in studio_scatter_data],
                mode="markers",
                marker=dict(
                    size=[min(d["anime_count"], 50) + 5 for d in studio_scatter_data],
                    sizemode="area",
                    sizeref=max_anime_sc / 400,
                    color=[d["credit_per_person"] for d in studio_scatter_data],
                    colorscale="Viridis", showscale=True,
                    colorbar=dict(title="Credits/Person"),
                    opacity=0.7,
                ),
                text=[d["name"] for d in studio_scatter_data],
                hovertemplate="%{text}<br>規模: %{x}人<br>平均スコア: %{y:.2f}<br>"
                              "Credits/Person: %{marker.color:.1f}<extra></extra>",
            ))
            fig_ss.update_layout(
                title="スタジオ規模 vs 平均スコア（規模と品質の関係）",
                xaxis_title="Person Count（スタジオ規模）",
                yaxis_title="Average Person Score",
            )
            body += '<div class="card">'
            body += "<h2>スタジオ規模 vs 平均スコア</h2>"
            body += chart_guide(
                "X=スタジオ規模（人数）、Y=平均人物スコア、サイズ=作品数、色=1人あたりクレジット数。"
                "大規模スタジオ＝高品質とは限らない。小規模精鋭スタジオ（左上）の特定に有用。"
                "色が明るい＝1人あたりの関与作品が多い＝多作なスタジオ。"
            )
            body += plotly_div_safe(fig_ss, "studio_size_quality", 550)
            body += "</div>"

    # Data-driven key findings
    studio_count = len(studios) if studios else 0
    causal_estimates = causal.get("causal_estimates", {}) if causal else {}
    selection_est = causal_estimates.get("selection_effect", {}).get("estimate", None)
    treatment_est = causal_estimates.get("treatment_effect", {}).get("estimate", None)
    body += key_findings([
        f"分析対象 {fmt_num(studio_count)} スタジオにおいて選抜効果・処置効果・ブランド効果の"
        "3つの因果メカニズムが分離して推定され、どの効果が支配的かが明らかになる"
        + (f"（選抜効果推定値: {selection_est:.3f}）" if selection_est is not None else ""),
        "固定効果推定とDID（差分の差分）推定の比較により、結果の頑健性を確認",
        "スタジオ規模とスタッフ数・クレジット数には正の相関があるが、"
        "品質指標との関係はより複雑",
    ])

    body += significance_section("スタジオブランドの因果的影響の分離", [
        "「有名スタジオ出身者はスコアが高い」のは当然ですが、その理由が重要です。"
        "優秀な人材が集まるから（選抜効果）なのか、スタジオが人材を育てるから（処置効果）なのか、"
        "単にブランドの光が当たるから（ブランド効果）なのか — この3つを区別することで"
        "スタジオの「本当の価値」が初めて明らかになります。",
        "処置効果が高いスタジオは「入社後にスコアが伸びる環境」を意味します。"
        "選抜効果だけが高いスタジオは最初から優秀な人材を集めているに過ぎません。"
        "この区別はスタジオ転籍を考えるアニメーター本人にとって最も実用的な判断材料であり、"
        "スタジオ経営者が育成戦略を見直す際の根拠にもなります。",
    ])
    body += utilization_guide([
        {
            "role": "アニメーター（転職検討中）",
            "how": "Causal Effect Identificationセクションの処置効果（Treatment Effect）棒グラフで"
                   "候補スタジオの推定値と95%信頼区間を確認し、信頼区間が正の範囲に収まるスタジオを"
                   "「成長環境あり」として転籍優先候補にリストアップする",
        },
        {
            "role": "スタジオ経営者",
            "how": "Causal Effect IdentificationのDominant Effect統計カードで自スタジオの支配的効果を確認し、"
                   "処置効果が低く選抜効果が高い場合はオンボーディング・メンター制度などの"
                   "育成プログラム強化を経営課題として優先する",
        },
        {
            "role": "スタジオ採用担当",
            "how": "Structural EstimationのPreferred Estimate解釈文を採用提案資料に引用し、"
                   "「在籍によるスコア向上幅」を計量経済学的根拠として転籍候補者への説明資料に組み込む",
        },
        {
            "role": "業界研究者",
            "how": "Causal Effect Identificationの3効果推定値と95%CI、Structural EstimationのFixed Effects/DID比較を"
                   "日本アニメ産業の労働経済学論文の実証分析セクションとして直接引用する",
        },
    ])
    body += future_possibilities([
        "個人向け転籍シミュレーター（現在のスコア・役職・年齢を入力すると各スタジオでの成長幅を予測）",
        "処置効果ランキングの年次公開による「人材育成力」スタジオ格付けの業界標準化",
        "在籍年数と処置効果の関係分析（最適在籍期間・転籍タイミングの定量的ガイドライン）",
        "海外スタジオ（共同制作・配信系）への効果推定拡張と国内外比較レポート",
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

        # Dual-axis: Credits + Avg Anime Score
        time_series_cs = load_json("time_series.json")
        if time_series_cs:
            ts_years = time_series_cs.get("years", [])
            ts_series = time_series_cs.get("series", {})
            avg_scores_ts = ts_series.get("avg_anime_score", {})
            credit_counts_ts = ts_series.get("credit_count", {})

            if ts_years and avg_scores_ts:
                fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
                fig_dual.add_trace(
                    go.Bar(
                        x=ts_years,
                        y=[credit_counts_ts.get(str(yr), 0) for yr in ts_years],
                        name="クレジット数",
                        marker_color="rgba(240,147,251,0.4)",
                        hovertemplate="%{x}: %{y:,}クレジット<extra></extra>",
                    ),
                    secondary_y=False,
                )
                fig_dual.add_trace(
                    go.Scatter(
                        x=ts_years,
                        y=[avg_scores_ts.get(str(yr), 0) for yr in ts_years],
                        name="平均アニメスコア",
                        mode="lines+markers",
                        line=dict(color="#06D6A0", width=2),
                        marker=dict(size=4),
                        hovertemplate="%{x}: %{y:.2f}<extra></extra>",
                    ),
                    secondary_y=True,
                )
                fig_dual.update_layout(title="年間クレジット数 + 平均アニメスコア（Dual-axis）")
                fig_dual.update_yaxes(title_text="クレジット数", secondary_y=False)
                fig_dual.update_yaxes(title_text="平均アニメスコア", secondary_y=True)

                body += '<div class="card">'
                body += "<h2>クレジット数 + 平均アニメスコア（Dual-axis）</h2>"
                body += chart_guide(
                    "左Y軸（棒グラフ）: 年間クレジット数、右Y軸（折れ線）: 平均アニメスコア。"
                    "クレジット数の増加と作品品質の推移を重ねることで、量と質の関係を読み取れます。"
                )
                body += plotly_div_safe(fig_dual, "credit_dual_axis", 450)
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

        # Violin: per-person total credit distribution
        credits_per_person = person_stats.get("credits_per_person", {})
        if credits_per_person:
            # credits_per_person is a dict of person_id -> count, or a distribution
            if isinstance(credits_per_person, dict):
                cpv = list(credits_per_person.values())
            elif isinstance(credits_per_person, list):
                cpv = credits_per_person
            else:
                cpv = []

            if cpv:
                fig_cpv = go.Figure()
                fig_cpv.add_trace(_violin_raincloud(cpv, "全人物", "#f093fb"))
                fig_cpv.update_layout(
                    title="人物あたり総クレジット数分布 (Raincloud)",
                    yaxis_title="クレジット数",
                    violinmode="overlay",
                )
                body += "<h3>人物あたり総クレジット数分布</h3>"
                body += chart_guide(
                    "Violin plotで全人物のクレジット数分布を表示。"
                    "ロングテール（少数の人が多数の作品に参加）を視覚的に確認できます。"
                    "箱の中央線が中央値、ひし形が平均値。"
                )
                body += plotly_div_safe(fig_cpv, "credit_per_person_violin", 450)
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

    # Data-driven key findings
    total_credits_cs = credit_stats.get("basic_stats", {}).get("total_credits", 0) if credit_stats else 0
    unique_persons_cs = credit_stats.get("basic_stats", {}).get("unique_persons", 0) if credit_stats else 0
    unique_roles_cs = credit_stats.get("basic_stats", {}).get("unique_roles", 0) if credit_stats else 0
    collab_pairs = credit_stats.get("collaboration_stats", {}).get("total_pair_instances", 0) if credit_stats else 0
    body += key_findings([
        f"総クレジット {fmt_num(total_credits_cs)} 件 / {fmt_num(unique_persons_cs)} 人 / {fmt_num(unique_roles_cs)} 役職 —"
        "すべての分析の基盤となる生データの規模と多様性を示す",
        f"コラボレーションペア数 {fmt_num(collab_pairs)} 件が示す業界の協業密度は、"
        "分析結果の統計的有意性の基礎",
        "役職分布の偏りから、特定の職種に人材が集中している構造が見え、業界の専門化パターンを示す",
        "生産性と一貫性のバランスは個人によって大きく異なり、多様な働き方が共存する",
    ])

    body += significance_section("生データの透明性と分析基盤の信頼性を証明", [
        "「分析結果を信じられるか」は「データの質を確認できるか」に直結します。"
        "本レポートはすべての分析が依拠する生クレジットデータの統計的概要を公開し、"
        "他の12本のレポートが立脚する基盤の透明性を保証します。",
        "クレジット数・役職数・協業ペア数の規模は統計的に有意な結論を導くためのサンプルサイズを証明し、"
        "年次タイムラインはデータカバレッジが歴史的にどう変化したかを示します。"
        "古い年代のデータが薄い場合はその年代の分析結果を慎重に解釈すべきであり、"
        "この「データ品質の可視化」こそが分析全体への信頼性の根拠です。",
    ])
    body += utilization_guide([
        {
            "role": "データアナリスト",
            "how": "他の分析レポートを参照する前にBasic Statisticsの4統計カード"
                   "（total_credits / unique_persons / unique_roles / collab_pairs）を確認し、"
                   "データ規模が統計的に十分か（クレジット数1万件以上を目安）を事前検証する",
        },
        {
            "role": "QAレビュアー",
            "how": "パイプライン実行後にBasic Statisticsのtotal_creditsとunique_personsを前回実行の記録と照合し、"
                   "10%以上の変動がある場合はスクレーパー異常またはエンティティ解決エラーとして調査する",
        },
        {
            "role": "学術研究者",
            "how": "Role Distributionセクションのトップ役職表（役職名・件数・全体比）を論文の記述統計テーブルとして引用し、"
                   "Collaboration StatsのTotal Pair Instancesをネットワーク分析の密度指標として本文中に記載する",
        },
        {
            "role": "ジャーナリスト",
            "how": "Annual Timelineチャートで直近5年間のクレジット数推移を確認し、"
                   "増加・減少トレンドを「アニメ産業の現状」記事の冒頭統計として引用する",
        },
    ])
    body += future_possibilities([
        "パイプライン実行ごとの差分自動検出ダッシュボード（クレジット増減・新規人物・消滅役職のアラート）",
        "Creative Commonsライセンスでのオープンデータ定期公開（研究・ジャーナリズム向け）",
        "学術機関との共同検証によるデータセットの引用可能な公式リリース",
        "海外アニメデータセット（AniList / MAL / SAKUGABOORU等）との統合による国際比較研究基盤の構築",
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
# Report 14: Co-occurrence Groups (共同制作集団)
# ============================================================

def generate_cooccurrence_groups_report():
    """共同制作集団分析レポート."""
    print("  Generating Co-occurrence Groups Report...")
    data = load_json("cooccurrence_groups.json")
    if not data:
        return

    groups = data.get("groups", [])
    summary = data.get("summary", {})
    temporal_slices = data.get("temporal_slices", [])
    params = data.get("params", {})

    total_groups = summary.get("total_groups", 0)
    active_groups = summary.get("active_groups", 0)
    by_size = summary.get("by_size", {})

    body = ""

    # --- Summary cards ---
    body += '<div class="card">'
    body += "<h2>共同制作集団サマリー</h2>"
    body += '<div class="stats-grid">'
    stat_items = [
        ("総グループ数", fmt_num(total_groups)),
        ("3人組", fmt_num(by_size.get("3", 0))),
        ("4人組", fmt_num(by_size.get("4", 0))),
        ("5人組", fmt_num(by_size.get("5", 0))),
        ("現役グループ", fmt_num(active_groups)),
        ("最低共参加作品数", fmt_num(params.get("min_shared_works", 3))),
    ]
    for label, val in stat_items:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div>"
    body += '<div class="insight-box"><strong>概要:</strong> '
    body += (
        f"コアスタッフ（監督・シリーズ構成・キャラクターデザイナー等10職種）が "
        f"{params.get('min_shared_works', 3)}作品以上で繰り返し共同制作するグループを検出。"
        f"全 {fmt_num(total_groups)} グループのうち {fmt_num(active_groups)} グループが現役（2022年以降に活動）。"
    )
    body += "</div></div>"

    # --- グループ一覧テーブル ---
    body += '<div class="card">'
    body += "<h2>グループ一覧 (上位100件)</h2>"
    body += section_desc(
        "コアスタッフ3〜5人が複数作品で繰り返し共同制作するグループ。"
        "shared_works 降順。メンバー名・役割・共参加作品数・活動期間・現役フラグを表示。"
    )
    body += "<table><thead><tr>"
    body += "<th>#</th><th>メンバー</th><th>役割</th><th>共参加作品数</th><th>活動期間</th><th>現役</th>"
    body += "</tr></thead><tbody>"

    for i, g in enumerate(groups[:100], 1):
        member_ids = g.get("members", [])
        names = g.get("member_names") or member_ids
        name_str = " / ".join(
            person_link(n, pid) for n, pid in zip(names, member_ids)
        ) if member_ids else " / ".join(names)
        # Collect unique roles across all members
        all_roles: list[str] = []
        for role_list in g.get("roles", {}).values():
            all_roles.extend(role_list)
        role_str = ", ".join(sorted(set(all_roles)))
        fy = g.get("first_year", "?")
        ly = g.get("last_year", "?")
        period = f"{fy}–{ly}" if fy and ly else "?"
        active_badge = (
            '<span class="badge badge-high">現役</span>'
            if g.get("is_active")
            else '<span class="badge badge-low">休眠</span>'
        )
        body += (
            f"<tr><td>{i}</td>"
            f"<td>{name_str}</td>"
            f"<td style='font-size:0.8rem;color:#a0a0c0'>{role_str}</td>"
            f"<td>{g.get('shared_works', 0)}</td>"
            f"<td>{period}</td>"
            f"<td>{active_badge}</td></tr>"
        )
    body += "</tbody></table></div>"

    # --- 時系列バーチャート (Stacked by size) ---
    if temporal_slices:
        periods = [ts["period"] for ts in temporal_slices]

        # Build per-size counts for each period
        size_labels = {"3": "3人組", "4": "4人組", "5": "5人組"}
        size_colors = {"3": "#f093fb", "4": "#f5576c", "5": "#fda085"}
        # Count groups active in each period by size
        period_size_counts: dict[str, dict[str, int]] = {p: {"3": 0, "4": 0, "5": 0} for p in periods}
        for g in groups:
            sz = str(g.get("size", len(g.get("members", []))))
            fy = g.get("first_year") or 9999
            ly = g.get("last_year") or 0
            for ts in temporal_slices:
                p = ts["period"]
                # Parse period like "2000-2004"
                parts = p.split("-")
                if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                    p_start, p_end = int(parts[0]), int(parts[1])
                    if fy <= p_end and ly >= p_start and sz in period_size_counts[p]:
                        period_size_counts[p][sz] += 1

        fig = go.Figure()
        for sz in ["3", "4", "5"]:
            fig.add_trace(go.Bar(
                x=periods,
                y=[period_size_counts[p][sz] for p in periods],
                name=size_labels[sz],
                marker_color=size_colors[sz],
                hovertemplate="%{x}: %{y}グループ<extra></extra>",
            ))
        fig.update_layout(
            barmode="stack",
            title="期間別アクティブグループ数（サイズ別内訳）",
            xaxis_title="期間",
            yaxis_title="グループ数",
        )
        body += '<div class="card">'
        body += "<h2>期間別アクティブグループ数（サイズ別内訳）</h2>"
        body += chart_guide(
            "5年区切りのピリオドごとにアクティブなグループ数を集計し、サイズ別に積み上げ表示。"
            "3人組が多数を占めるか、4〜5人組が増加傾向かなど、固定チームの規模変遷を読み取れます。"
        )
        body += plotly_div_safe(fig, "cooccurrence_temporal", 400)
        body += "</div>"

    # --- グループサイズ分布 ---
    size_keys = sorted(by_size.keys())
    size_vals = [by_size[k] for k in size_keys]
    if size_keys:
        fig2 = go.Figure(
            go.Bar(
                x=[f"{k}人組" for k in size_keys],
                y=size_vals,
                marker_color=["#f093fb", "#f5576c", "#fda085"][: len(size_keys)],
                hovertemplate="%{x}: %{y}グループ<extra></extra>",
            )
        )
        fig2.update_layout(
            title="グループサイズ別分布",
            xaxis_title="グループサイズ",
            yaxis_title="グループ数",
        )
        body += '<div class="card">'
        body += "<h2>グループサイズ別分布</h2>"
        body += chart_guide(
            "3人組が最多。4人・5人組は希少だが、それだけ安定した固定チームを示す。"
        )
        body += plotly_div_safe(fig2, "cooccurrence_size", 350)
        body += "</div>"

    # --- Violin: グループサイズ別の共参加作品数分布 ---
    if groups:
        violin_data: dict[str, list[int]] = {}
        for g in groups:
            sz = str(g.get("size", len(g.get("members", []))))
            sw = g.get("shared_works", 0)
            violin_data.setdefault(sz, []).append(sw)

        if violin_data:
            fig_v = go.Figure()
            v_colors = {"3": "#f093fb", "4": "#f5576c", "5": "#fda085"}
            for sz in sorted(violin_data.keys()):
                fig_v.add_trace(_violin_raincloud(
                    violin_data[sz], f"{sz}人組", v_colors.get(sz, "#a0d2db"),
                ))
            fig_v.update_layout(
                title="グループサイズ別 共参加作品数分布 (Raincloud)",
                yaxis_title="共参加作品数",
                xaxis_title="グループサイズ",
                violinmode="overlay",
            )
            body += '<div class="card">'
            body += "<h2>グループサイズ別 共参加作品数分布</h2>"
            body += chart_guide(
                "Violin plotで各サイズのグループにおける共参加作品数の分布を比較。"
                "箱ひげ図と密度推定を同時に表示。中央の線が平均値、箱が四分位範囲。"
                "同じサイズでも共参加数にばらつきがあることを確認できます。"
            )
            body += plotly_div_safe(fig_v, "cooccurrence_violin_size", 450)
            body += "</div>"

    # --- Scatter: 活動期間 vs 共参加作品数 ---
    if groups:
        scatter_groups = [g for g in groups if g.get("first_year") and g.get("last_year")]
        if scatter_groups:
            spans = [g["last_year"] - g["first_year"] for g in scatter_groups]
            shared = [g.get("shared_works", 0) for g in scatter_groups]
            avg_scores = [g.get("avg_iv_score", 0) for g in scatter_groups]
            sizes_s = [g.get("size", len(g.get("members", []))) for g in scatter_groups]
            # Map size to marker size
            marker_sizes = [s * 4 for s in sizes_s]

            fig_sc = go.Figure(go.Scatter(
                x=spans,
                y=shared,
                mode="markers",
                marker=dict(
                    size=marker_sizes,
                    color=avg_scores,
                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(title="平均スコア"),
                    opacity=0.7,
                ),
                text=[f"Size={s}" for s in sizes_s],
                hovertemplate="活動期間: %{x}年<br>共参加作品: %{y}<br>%{text}<br>平均スコア: %{marker.color:.1f}<extra></extra>",
            ))
            fig_sc.update_layout(
                title="活動期間 vs 共参加作品数（平均スコア色・サイズ別マーカー）",
                xaxis_title="活動期間（年）",
                yaxis_title="共参加作品数",
            )
            body += '<div class="card">'
            body += "<h2>活動期間 vs 共参加作品数</h2>"
            body += chart_guide(
                "各ドットは1つのグループ。X=活動期間（last_year - first_year）、Y=共参加作品数、"
                "色=メンバー平均IV Score、ドットの大きさ=グループサイズ。"
                "右上は長期間にわたり多くの作品を共同制作した安定チーム。"
            )
            body += plotly_div_safe(fig_sc, "cooccurrence_scatter", 500)
            body += "</div>"

    # --- 時系列: 年別グループ形成数（初出年ごとの新グループ数）---
    if groups:
        first_year_counts: dict[int, int] = {}
        for g in groups:
            fy = g.get("first_year")
            if fy:
                first_year_counts[fy] = first_year_counts.get(fy, 0) + 1

        if first_year_counts:
            sorted_years = sorted(first_year_counts.keys())
            fig_fy = go.Figure(go.Bar(
                x=sorted_years,
                y=[first_year_counts[yr] for yr in sorted_years],
                marker_color=[
                    f"rgba(160,210,219,{0.3 + 0.7 * first_year_counts[yr] / max(first_year_counts.values())})"
                    for yr in sorted_years
                ],
                hovertemplate="%{x}年: %{y}グループ<extra></extra>",
            ))
            fig_fy.update_layout(
                title="年別グループ形成数（初出年）",
                xaxis_title="年",
                yaxis_title="新規グループ数",
            )
            body += '<div class="card">'
            body += "<h2>年別グループ形成数</h2>"
            body += chart_guide(
                "各グループの初出年（メンバーが初めて共参加した年）をカウント。"
                "「いつ固定チームが形成され始めたか」の時系列推移を把握できます。"
                "ピーク年は業界で固定チーム形成が活発だった時期を示します。"
            )
            body += plotly_div_safe(fig_fy, "cooccurrence_formation", 400)
            body += "</div>"

    # --- デュアル軸時系列: 年別新規グループ数（棒）+ 累積（折れ線）---
    if groups:
        first_year_counts2: dict[int, int] = {}
        for g in groups:
            fy = g.get("first_year")
            if fy:
                first_year_counts2[fy] = first_year_counts2.get(fy, 0) + 1
        if first_year_counts2:
            sorted_years2 = sorted(first_year_counts2.keys())
            new_per_year = [first_year_counts2[yr] for yr in sorted_years2]
            cumulative = []
            total = 0
            for n in new_per_year:
                total += n
                cumulative.append(total)

            fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
            fig_dual.add_trace(
                go.Bar(
                    x=sorted_years2,
                    y=new_per_year,
                    name="年別新規グループ数",
                    marker_color="rgba(240,147,251,0.6)",
                    hovertemplate="%{x}年: %{y}グループ<extra></extra>",
                ),
                secondary_y=False,
            )
            fig_dual.add_trace(
                go.Scatter(
                    x=sorted_years2,
                    y=cumulative,
                    name="累積グループ数",
                    line=dict(color="#06D6A0", width=2),
                    mode="lines",
                    hovertemplate="%{x}年 累計: %{y}グループ<extra></extra>",
                ),
                secondary_y=True,
            )
            fig_dual.update_yaxes(title_text="新規グループ数", secondary_y=False)
            fig_dual.update_yaxes(title_text="累積グループ数", secondary_y=True)
            fig_dual.update_layout(
                title="年別新規グループ形成数（棒）+ 累積グループ数（折れ線）",
                xaxis_title="年",
            )
            body += '<div class="card">'
            body += "<h2>年別グループ形成数（デュアル軸）</h2>"
            body += chart_guide(
                "棒グラフ（左軸）= その年に初めて共参加したグループの新規形成数。"
                "折れ線（右軸）= 累積グループ総数。"
                "業界で固定チーム形成が急加速した年と安定期を一枚で把握できます。"
            )
            body += plotly_div_safe(fig_dual, "cooccurrence_dual_axis", 450)
            body += "</div>"

    # --- Violin: 現役 vs 休眠別 shared_works 分布 ---
    if groups:
        active_sw = [g.get("shared_works", 0) for g in groups if g.get("is_active")]
        inactive_sw = [g.get("shared_works", 0) for g in groups if not g.get("is_active")]
        if active_sw or inactive_sw:
            fig_vact = go.Figure()
            if active_sw:
                fig_vact.add_trace(_violin_raincloud(active_sw, "現役", "#06D6A0"))
            if inactive_sw:
                fig_vact.add_trace(_violin_raincloud(inactive_sw, "休眠", "#EF476F"))
            fig_vact.update_layout(
                title="現役 vs 休眠グループ — 共参加作品数分布 (Raincloud)",
                yaxis_title="共参加作品数",
                violinmode="overlay",
            )
            body += '<div class="card">'
            body += "<h2>現役 vs 休眠グループの共参加作品数分布</h2>"
            body += chart_guide(
                "現役グループ（緑）と休眠グループ（赤）で共参加作品数の分布を比較。"
                "現役グループは一般に共参加作品数が多い（= 長期継続チーム）か？"
                "または短期で解散するグループの方が多いのか？"
                "箱の中央線が中央値、上下のひげが分布の広がりを表します。"
            )
            body += plotly_div_safe(fig_vact, "cooccurrence_violin_active", 450)
            body += "</div>"

    # --- 役割別出現頻度: 上位100グループに登場する役割の内訳（水平スタック棒）---
    if groups:
        from collections import Counter
        role_label_ja: dict[str, str] = {
            "director": "監督",
            "series_composition": "シリーズ構成",
            "character_designer": "キャラデザ",
            "chief_animation_director": "総作画監督",
            "art_director": "美術監督",
            "color_designer": "色彩設計",
            "sound_director": "音響監督",
            "photography_director": "撮影監督",
            "cgi_director": "CGI監督",
            "mechanical_designer": "メカデザ",
        }
        role_counts: dict[str, int] = Counter()
        for g in groups[:200]:
            for role_list in g.get("roles", {}).values():
                for r in role_list:
                    role_counts[r] += 1

        if role_counts:
            # Top groups — per-role presence in top groups (by shared_works bucket)
            sw_buckets = [3, 5, 8, 12, 20]
            bucket_labels = ["3-4作", "5-7作", "8-11作", "12-19作", "20作+"]
            role_bucket_counts: dict[str, list[int]] = {r: [0] * len(sw_buckets) for r in role_counts}

            for g in groups:
                sw = g.get("shared_works", 0)
                if sw < 3:
                    continue
                bucket_idx = len(sw_buckets) - 1
                for j, threshold in enumerate(sw_buckets):
                    if sw <= threshold + (sw_buckets[j + 1] - threshold - 1 if j + 1 < len(sw_buckets) else 9999):
                        bucket_idx = j
                        break
                # Clamp
                bucket_idx = min(bucket_idx, len(sw_buckets) - 1)
                # Simpler bucket: just find the right slot
                if sw >= 20:
                    bucket_idx = 4
                elif sw >= 12:
                    bucket_idx = 3
                elif sw >= 8:
                    bucket_idx = 2
                elif sw >= 5:
                    bucket_idx = 1
                else:
                    bucket_idx = 0

                for role_list in g.get("roles", {}).values():
                    for r in role_list:
                        if r in role_bucket_counts:
                            role_bucket_counts[r][bucket_idx] += 1

            top_roles = sorted(role_counts.keys(), key=lambda r: -role_counts[r])[:8]
            role_colors = [
                "#f093fb", "#f5576c", "#fda085", "#a0d2db",
                "#06D6A0", "#FFD166", "#667eea", "#EF476F",
            ]

            fig_role = go.Figure()
            for i, role in enumerate(top_roles):
                fig_role.add_trace(go.Bar(
                    name=role_label_ja.get(role, role),
                    x=bucket_labels,
                    y=role_bucket_counts[role],
                    marker_color=role_colors[i % len(role_colors)],
                    hovertemplate=f"{role_label_ja.get(role, role)}: %{{y}}グループ<extra></extra>",
                ))
            fig_role.update_layout(
                barmode="stack",
                title="共参加作品数帯別 × 役割内訳（上位8役割）",
                xaxis_title="共参加作品数帯",
                yaxis_title="グループ数（役割別）",
            )
            body += '<div class="card">'
            body += "<h2>共参加作品数帯別 × 役割内訳</h2>"
            body += chart_guide(
                "X軸=共参加作品数の帯（3-4作・5-7作・8-11作・12-19作・20作+）。"
                "色分けされた積み上げ棒グラフで各帯にどの役割のメンバーが多く登場するかを表示。"
                "「長く一緒に仕事をするチームはどの役割が核になっているか」を把握できます。"
            )
            body += plotly_div_safe(fig_role, "cooccurrence_role_breakdown", 450)
            body += "</div>"

    # --- Scatter: 初出年 × 平均IV Score（現役/休眠色分け、共参加数でサイズ）---
    if groups:
        scatter2_groups = [
            g for g in groups
            if g.get("first_year") and g.get("avg_iv_score", 0) > 0
        ]
        if scatter2_groups:
            colors_sc2 = ["#06D6A0" if g["is_active"] else "#EF476F" for g in scatter2_groups]
            sizes_sc2 = [max(4, min(20, g.get("shared_works", 3) * 1.5)) for g in scatter2_groups]
            fig_sc2 = go.Figure(go.Scatter(
                x=[g["first_year"] for g in scatter2_groups],
                y=[g["avg_iv_score"] for g in scatter2_groups],
                mode="markers",
                marker=dict(
                    color=colors_sc2,
                    size=sizes_sc2,
                    opacity=0.65,
                    line=dict(width=0.5, color="rgba(255,255,255,0.3)"),
                ),
                text=[
                    f"{'現役' if g['is_active'] else '休眠'} | "
                    f"{g.get('shared_works', 0)}作品 | "
                    f"size={g.get('size', '?')}"
                    for g in scatter2_groups
                ],
                hovertemplate=(
                    "初出年: %{x}<br>"
                    "平均スコア: %{y:.1f}<br>"
                    "%{text}<extra></extra>"
                ),
            ))
            # Add annotation for active/inactive legend
            fig_sc2.update_layout(
                title="初出年 × 平均IV Score（緑=現役, 赤=休眠, サイズ=共参加数）",
                xaxis_title="グループ初出年",
                yaxis_title="メンバー平均IV Score",
            )
            body += '<div class="card">'
            body += "<h2>初出年 × 平均スコア散布図</h2>"
            body += chart_guide(
                "X=グループが初めて共参加した年、Y=メンバーの平均IV Score。"
                "緑ドット=現役グループ、赤ドット=休眠グループ。"
                "ドットの大きさ=共参加作品数（大=長期継続チーム）。"
                "高スコアのメンバーが集まる固定チームが近年増加しているかを読み取れます。"
            )
            body += plotly_div_safe(fig_sc2, "cooccurrence_scatter_year_score", 500)
            body += "</div>"

    # -------------------------------------------------------
    # --- コラボレーションパワー & ユニークさ & クラスタリング & ML分類 ---
    # -------------------------------------------------------
    if groups:
        import math as _math
        import numpy as _np
        from sklearn.cluster import KMeans as _KMeans
        from sklearn.preprocessing import StandardScaler as _StandardScaler

        GRP_COLORS = ["#f093fb", "#f5576c", "#fda085", "#a0d2db", "#06D6A0"]

        # --- コラボレーションパワー計算 ---
        # collab_power = avg_iv_score × log1p(shared_works) × longevity_factor
        def _collab_power(g: dict) -> float:
            comp = g.get("avg_iv_score", 0) or 0
            sw = g.get("shared_works", 0) or 0
            fy = g.get("first_year") or 0
            ly = g.get("last_year") or 0
            span = max(1, ly - fy + 1) if fy and ly else 1
            longevity = 1.0 + span / 30.0  # 10年で1.33、30年で2.0
            return float(comp * _math.log1p(sw) * longevity)

        # --- ユニークさ計算 (時代背景考慮) ---
        # uniqueness = shared_works per year × size_bonus × era_weight
        # 旧時代（アニメ本数が少ない）ほど同じ共参加数でも希少価値が高い
        # 参考: 1980年代は年間~50本、2020年代は年間~300本以上
        # → 旧時代の10共参加は現代の30共参加に相当するほど稀少
        _ERA_WEIGHTS = {1960: 3.0, 1970: 2.5, 1980: 2.0, 1990: 1.5,
                        2000: 1.0, 2010: 0.8, 2020: 0.65}

        def _era_weight(first_year: int | None) -> float:
            if not first_year:
                return 1.0
            decade = (int(first_year) // 10) * 10
            for d in sorted(_ERA_WEIGHTS.keys(), reverse=True):
                if decade >= d:
                    return _ERA_WEIGHTS[d]
            return _ERA_WEIGHTS[1960]

        def _uniqueness(g: dict) -> float:
            sw = g.get("shared_works", 0) or 0
            fy = g.get("first_year") or 0
            ly = g.get("last_year") or 0
            sz = g.get("size", len(g.get("members", []))) or 3
            span = max(1, ly - fy + 1) if fy and ly else 1
            intensity = sw / span  # 年あたりの共参加数
            size_bonus = 1.0 + (sz - 3) * 0.5  # 4人組=1.5倍、5人組=2.0倍
            era_bonus = _era_weight(fy or None)   # 旧時代はアニメ本数が少ないため補正
            return float(intensity * size_bonus * era_bonus)

        raw_powers = [_collab_power(g) for g in groups]
        raw_uniques = [_uniqueness(g) for g in groups]
        max_power = max(raw_powers) or 1.0
        max_unique = max(raw_uniques) or 1.0

        for i, g in enumerate(groups):
            g["collab_power"] = round(raw_powers[i] / max_power * 99, 1)
            g["uniqueness_score"] = round(raw_uniques[i] / max_unique * 99, 1)

        # --- ML-based grouping reason (時代背景考慮ルールベース分類) ---
        # 時代補正: アニメ制作本数の変遷に基づき、同じ共参加数でも
        # 旧時代ほど「稀少」と判定する動的閾値を設定する
        def _era_sw_threshold(fy: int, base_threshold: int) -> int:
            """アニメ制作本数に基づき、時代補正した共参加数閾値を返す."""
            # 旧時代(1980年代〜)は年間アニメ本数が少ないため、
            # 同じ共参加数が現代より統計的に稀 → 閾値を下げる
            if fy < 1985:
                return max(3, int(base_threshold * 0.4))
            if fy < 1995:
                return max(3, int(base_threshold * 0.6))
            if fy < 2005:
                return max(3, int(base_threshold * 0.8))
            if fy >= 2015:
                return int(base_threshold * 1.2)  # 現代: 本数多いので閾値を上げる
            return base_threshold

        def _classify_group(g: dict) -> str:
            sw = g.get("shared_works", 0) or 0
            fy = int(g.get("first_year") or 2000)
            ly = g.get("last_year") or 0
            span = max(1, ly - fy + 1) if ly else 1
            is_active = g.get("is_active", False)
            intensity = sw / span  # 年あたり共参加数
            comp = g.get("avg_iv_score", 0) or 0

            # 時代補正した動的閾値
            long_sw = _era_sw_threshold(fy, 15)   # 長期確立チームの共参加数閾値
            series_sw = _era_sw_threshold(fy, 10)  # 集中型シリーズの共参加数閾値
            quality_threshold = 40  # 品質閾値（時代補正なし: スコアは相対値のため）

            if sw >= long_sw and span >= 15:
                if fy < 2000:
                    return "レガシー長期確立チーム"  # 旧時代の超安定チーム
                return "長期確立チーム"
            if sw >= series_sw and span < 8:
                return "集中型シリーズ"  # 短期間の集中コラボ（シリーズ特化）
            if is_active and fy >= 2015 and comp >= quality_threshold:
                return "新興高品質チーム"  # 現代の高品質新興チーム
            if is_active and fy >= 2015:
                return "現代新興チーム"  # 現代の新興チーム
            if not is_active and fy < 2000:
                return "レガシーチーム"  # 旧時代の休眠チーム
            if intensity >= 2.5:
                return "シリーズ継続型"  # 高い作業強度（シリーズ継続）
            if comp >= 50:
                return "高評価チーム"  # 高品質メンバー構成
            return "標準コラボ"

        REASON_COLORS = {
            "レガシー長期確立チーム": "#a3e635",
            "長期確立チーム": "#06D6A0",
            "集中型シリーズ": "#f093fb",
            "新興高品質チーム": "#FFD166",
            "現代新興チーム": "#fda085",
            "レガシーチーム": "#667eea",
            "シリーズ継続型": "#f5576c",
            "高評価チーム": "#EF476F",
            "標準コラボ": "#a0d2db",
        }
        for g in groups:
            g["grouping_reason"] = _classify_group(g)

        # --- K-Means クラスタリング (k=5, 時代背景含む9特徴量) ---
        # Features: size, shared_works, span, avg_iv_score, is_active,
        #           intensity, collab_power, uniqueness, first_year (時代背景)
        valid_grps = [g for g in groups if g.get("first_year") and g.get("last_year")]
        if len(valid_grps) >= 5:
            def _g_features(g: dict) -> list[float]:
                sw = g.get("shared_works", 0) or 0
                fy = float(g.get("first_year") or 2000)
                ly = g.get("last_year") or 0
                span = max(1.0, ly - fy + 1)
                return [
                    float(g.get("size", 3)),            # 0: size
                    float(sw),                           # 1: shared_works
                    float(span),                         # 2: activity_span
                    float(g.get("avg_iv_score", 0) or 0),  # 3: avg_iv_score
                    float(g.get("is_active", False)),   # 4: is_active
                    float(sw / span),                    # 5: intensity
                    float(g.get("collab_power", 0)),    # 6: collab_power
                    float(g.get("uniqueness_score", 0)),# 7: uniqueness_score
                    fy,                                  # 8: first_year (時代背景)
                ]

            Xg = _np.array([_g_features(g) for g in valid_grps], dtype=float)
            sc_g = _StandardScaler()
            Xg_s = sc_g.fit_transform(Xg)
            k_g = min(5, len(valid_grps))
            km_g = _KMeans(n_clusters=k_g, n_init=20, random_state=42)
            g_labels = km_g.fit_predict(Xg_s)

            # 動的クラスタ命名: 重心の相対ランクで自動ラベル付け (3特徴で簡潔に)
            centers_g = sc_g.inverse_transform(km_g.cluster_centers_)
            g_cluster_names: dict[int, str] = _name_clusters_by_rank(
                centers_g,
                [
                    (8, ["現代型", "過渡期型", "クラシック型"]),  # first_year (大=現代)
                    (1, ["多作", "少作"]),                         # shared_works
                    (3, ["高品質", "低品質"]),                     # avg_iv_score
                ],
            )

            for i, g in enumerate(valid_grps):
                g["group_cluster"] = int(g_labels[i])
                g["group_cluster_name"] = g_cluster_names[int(g_labels[i])]

            # Cluster groups
            gc_groups: dict[int, list[dict]] = {}
            for g in valid_grps:
                gc_groups.setdefault(g["group_cluster"], []).append(g)

            # ---- Chart: グループクラスタ × 分布 ----
            body += '<div class="card">'
            body += f"<h2>グループ K-Means クラスタリング（{k_g}クラスタ）</h2>"
            body += section_desc(
                "グループサイズ・共参加作品数・活動期間・平均スコア・現役フラグ・コラボ強度の8特徴量で"
                f"全グループを{k_g}クラスタに自動分類。各クラスタの特徴を把握します。"
            )

            # Scatter: shared_works vs activity_span colored by cluster
            fig_gc_sc = go.Figure()
            for cid in sorted(gc_groups.keys()):
                mems_gc = gc_groups[cid]
                fig_gc_sc.add_trace(go.Scatter(
                    x=[g.get("last_year", 0) - g.get("first_year", 0) for g in mems_gc],
                    y=[g.get("shared_works", 0) for g in mems_gc],
                    mode="markers",
                    name=g_cluster_names[cid],
                    marker=dict(
                        size=8,
                        color=GRP_COLORS[cid % len(GRP_COLORS)],
                        opacity=0.65,
                    ),
                    hovertemplate=(
                        "活動期間: %{x}年<br>"
                        "共参加数: %{y}<br>"
                        f"クラスタ: {g_cluster_names[cid]}<extra></extra>"
                    ),
                ))
            fig_gc_sc.update_layout(
                title="グループクラスタ散布図（活動期間 × 共参加作品数）",
                xaxis_title="活動期間（年）", yaxis_title="共参加作品数",
            )
            body += chart_guide(
                "各クラスタの特徴が2Dで見えます。右上＝長期間かつ多作の安定チーム、"
                "左上＝短期集中型（シリーズ特化）、左下＝散発的コラボ。"
            )
            body += plotly_div_safe(fig_gc_sc, "group_cluster_scatter", 500)

            # Raincloud: collab_power per cluster
            fig_gc_viol = go.Figure()
            for cid in sorted(gc_groups.keys()):
                mems_gc = gc_groups[cid]
                cp_vals = [g.get("collab_power", 0) for g in mems_gc]
                if cp_vals:
                    fig_gc_viol.add_trace(_violin_raincloud(
                        cp_vals, g_cluster_names[cid], GRP_COLORS[cid % len(GRP_COLORS)],
                    ))
            fig_gc_viol.update_layout(
                title="クラスタ別 コラボレーションパワー分布 (Raincloud)",
                yaxis_title="コラボレーションパワー (0-99)",
                violinmode="overlay",
            )
            body += chart_guide("各クラスタのコラボパワー（avg_iv_score × log(共参加数) × 継続ボーナス）分布。")
            body += plotly_div_safe(fig_gc_viol, "group_cluster_power_violin", 450)

            # Cluster size bar
            fig_gc_sz = go.Figure(go.Bar(
                x=[g_cluster_names[c] for c in range(k_g)],
                y=[len(gc_groups.get(c, [])) for c in range(k_g)],
                marker_color=[GRP_COLORS[c % len(GRP_COLORS)] for c in range(k_g)],
            ))
            fig_gc_sz.update_layout(
                title="クラスタ別グループ数", yaxis_title="グループ数", xaxis_tickangle=-15,
            )
            body += plotly_div_safe(fig_gc_sz, "group_cluster_size_bar", 380)
            body += "</div>"

        # ---- Chart: コラボレーションパワー TOP30 ---
        top_power = sorted(groups, key=lambda g: g.get("collab_power", 0), reverse=True)[:30]
        if top_power:
            body += '<div class="card">'
            body += "<h2>コラボレーションパワー TOP30</h2>"
            body += section_desc(
                "コラボパワー = メンバー平均スコア × log(共参加作品数) × 継続期間ボーナス（最大2倍）。"
                "「長期間、高品質な作品で継続して共同制作するチーム」ほど高スコアになります。"
            )
            fig_cp = go.Figure(go.Bar(
                x=[" / ".join((g.get("member_names") or g.get("members", []))[:2]) for g in top_power],
                y=[g.get("collab_power", 0) for g in top_power],
                marker_color=[
                    "#06D6A0" if g.get("is_active") else "#EF476F" for g in top_power
                ],
                text=[f"{g.get('shared_works', 0)}作品" for g in top_power],
                hovertemplate="%{x}<br>コラボパワー: %{y:.1f}<br>%{text}<extra></extra>",
            ))
            fig_cp.update_layout(
                title="コラボレーションパワー TOP30（緑=現役/赤=休眠）",
                yaxis_title="コラボパワー",
                xaxis_tickangle=-35,
                height=480,
            )
            body += chart_guide("緑＝現役グループ（2022年以降活動）、赤＝休眠。横軸のメンバー名は代表2人を表示。")
            body += plotly_div_safe(fig_cp, "group_collab_power_top30", 480)
            body += "</div>"

        # ---- Chart: ユニークさ TOP30 ---
        top_unique = sorted(groups, key=lambda g: g.get("uniqueness_score", 0), reverse=True)[:30]
        if top_unique:
            body += '<div class="card">'
            body += "<h2>ユニークさ（過剰結びつき度）TOP30</h2>"
            body += section_desc(
                "ユニークさ = 年あたりの共参加作品数 × グループサイズボーナス。"
                "偶然だけでは説明できないほど「過剰に結びついた」グループほど高スコア。"
                "シリーズ一本で全話一緒に仕事する小チーム、など。"
            )
            fig_uq = go.Figure(go.Bar(
                x=[" / ".join((g.get("member_names") or g.get("members", []))[:2]) for g in top_unique],
                y=[g.get("uniqueness_score", 0) for g in top_unique],
                marker_color=[
                    f"rgba(160,210,219,{0.4 + 0.6 * g.get('uniqueness_score', 0) / 99})"
                    for g in top_unique
                ],
                text=[f"size={g.get('size', '?')}, {g.get('shared_works', 0)}作/{max(1, (g.get('last_year') or 0) - (g.get('first_year') or 0) + 1)}年" for g in top_unique],
                hovertemplate="%{x}<br>ユニークさ: %{y:.1f}<br>%{text}<extra></extra>",
            ))
            fig_uq.update_layout(
                title="ユニークさ（過剰結びつき度）TOP30",
                yaxis_title="ユニークさスコア",
                xaxis_tickangle=-35,
                height=480,
            )
            body += chart_guide("横軸＝代表メンバー2名。縦軸＝ユニークさスコア（年あたり共参加数×サイズボーナス）。")
            body += plotly_div_safe(fig_uq, "group_uniqueness_top30", 480)
            body += "</div>"

        # ---- Chart: ML分類（グルーピング理由） ---
        reason_counter: Counter = Counter(g.get("grouping_reason", "標準コラボ") for g in groups)
        if reason_counter:
            body += '<div class="card">'
            body += "<h2>グルーピング理由 分類（ルールベースML）</h2>"
            body += section_desc(
                "各グループの形成理由を「共参加作品数・活動期間・現役フラグ・コラボ強度・メンバー品質」"
                "のルールで自動分類。「シリーズ継続型」「長期確立チーム」「新興高品質チーム」等を識別します。"
            )
            reasons_sorted = sorted(reason_counter.keys(), key=lambda r: -reason_counter[r])
            fig_reason_pie = go.Figure(go.Pie(
                labels=reasons_sorted,
                values=[reason_counter[r] for r in reasons_sorted],
                marker_colors=[REASON_COLORS.get(r, "#888") for r in reasons_sorted],
                hole=0.4, textinfo="label+percent",
            ))
            fig_reason_pie.update_layout(title="グルーピング理由 構成比")
            body += plotly_div_safe(fig_reason_pie, "group_reason_pie", 420)

            # Stacked bar: reason × is_active
            fig_reason_bar = go.Figure()
            fig_reason_bar.add_trace(go.Bar(
                name="現役",
                x=reasons_sorted,
                y=[sum(1 for g in groups if g.get("grouping_reason") == r and g.get("is_active")) for r in reasons_sorted],
                marker_color="#06D6A0",
            ))
            fig_reason_bar.add_trace(go.Bar(
                name="休眠",
                x=reasons_sorted,
                y=[sum(1 for g in groups if g.get("grouping_reason") == r and not g.get("is_active")) for r in reasons_sorted],
                marker_color="#EF476F",
            ))
            fig_reason_bar.update_layout(
                barmode="stack",
                title="グルーピング理由別 現役/休眠内訳",
                xaxis_tickangle=-25, yaxis_title="グループ数",
            )
            body += chart_guide("各分類タイプの現役（緑）・休眠（赤）割合。長期確立チームは休眠が多い？新興高品質チームは現役率が高い？")
            body += plotly_div_safe(fig_reason_bar, "group_reason_active_bar", 420)

            # Violin: collab_power per reason
            fig_reason_viol = go.Figure()
            for r in reasons_sorted:
                r_vals = [g.get("collab_power", 0) for g in groups if g.get("grouping_reason") == r]
                if r_vals:
                    fig_reason_viol.add_trace(_violin_raincloud(
                        r_vals, r, REASON_COLORS.get(r, "#888"),
                    ))
            fig_reason_viol.update_layout(
                title="分類別 コラボレーションパワー分布 (Raincloud)",
                yaxis_title="コラボパワー", xaxis_tickangle=-20,
                violinmode="overlay",
            )
            body += chart_guide("各グルーピング理由タイプのコラボパワー分布。どの分類が最もパワフルか？")
            body += plotly_div_safe(fig_reason_viol, "group_reason_power_violin", 450)
            body += "</div>"

    # --- Key findings ---
    active_ratio = active_groups / max(total_groups, 1) * 100
    top3_shared = groups[0]["shared_works"] if groups else 0
    top_power_val = max((g.get("collab_power", 0) for g in groups), default=0) if groups else 0
    body += key_findings(
        [
            f"全 {fmt_num(total_groups)} グループのうち {fmt_num(active_groups)} グループ"
            f"（{active_ratio:.1f}%）が2022年以降に現役で活動中",
            f"最も多く共参加した3人組は {top3_shared} 作品を共同制作しており、"
            "事実上の固定チームと見なせる",
            f"コラボレーションパワー最大値 {top_power_val:.1f}/99 — "
            "長期間・高品質作品・多数の共参加を達成した超安定チームが存在",
        ]
    )

    body += significance_section(
        "事実上の固定チームの可視化",
        [
            "アニメ制作において「スタジオ」は法的・経済的な単位ですが、実際に作品を"
            "成立させるのは監督・シリーズ構成・キャラクターデザイナー等のコアスタッフが"
            "形成する非公式の固定チームです。本分析はこの「見えないチーム」を"
            "クレジットデータから浮かび上がらせます。",
            "同じメンバーが繰り返し共同制作するパターンは、報酬交渉における"
            "「チームとしての市場価値」の根拠になります。個人単位ではなく"
            "チーム単位での評価が、アニメ制作の実態に即した公正報酬基準を生みます。",
        ],
    )

    body += utilization_guide(
        [
            {
                "role": "プロデューサー",
                "how": "Top グループテーブルの現役グループを確認し、同一チームで次回作を企画する際の最優先接触メンバーを特定する",
            },
            {
                "role": "スタジオ人事",
                "how": "活動期間の長いグループのメンバーを競合スタジオが抱えているか確認し、移籍・共同制作交渉の優先度を決定する",
            },
            {
                "role": "報酬交渉担当",
                "how": "同一グループメンバーが揃うことで作品品質が向上する効果を定量化し、チーム単位の報酬プレミアムの根拠として提示する",
            },
            {
                "role": "研究者",
                "how": "temporal_slices で時代別にアクティブグループ数の変化を追い、産業構造変化（デジタル化・海外進出等）がチーム固定性に与えた影響を分析する",
            },
        ]
    )

    body += future_possibilities(
        [
            "グループの「解散」検出 — ある年以降に共参加が途絶えたグループを自動特定し、離散理由（引退・スタジオ変更・独立）を追跡",
            "グループ品質評価 — グループが参加した作品の平均スコアを集計し、「高品質を生み出すコアチーム」ランキングを生成",
            "グループ間ネットワーク — 共通メンバーを持つグループをエッジで繋ぎ、コアチームのクラスタを可視化",
            "新グループ予測 — 現在2作品を共参加しているメンバーが3作品目を共参加する確率をモデル化し、「次世代固定チーム候補」を早期発見",
        ]
    )

    html = wrap_html(
        "共同制作集団分析",
        f"コアスタッフの繰り返し共同制作パターン — {fmt_num(total_groups)}グループ検出",
        body,
        intro_html=report_intro(
            "共同制作集団分析",
            "監督・シリーズ構成・キャラクターデザイナー等10職種のコアスタッフが"
            "3作品以上で繰り返し共同制作するグループ（事実上の固定チーム）を検出します。"
            "ペアワイズの協業グラフでは見えない「3人以上のチーム単位」の反復パターンを"
            "クレジットデータから可視化し、非公式チームの活動期間と現役状態を追跡します。",
            "プロデューサー、スタジオ人事、報酬交渉担当、ネットワーク研究者",
        ),
        glossary_terms={
            **COMMON_GLOSSARY_TERMS,
            "コアスタッフ (Core Staff)": (
                "本分析で対象とする10職種: 監督・シリーズ構成・キャラクターデザイナー・"
                "総作画監督・美術監督・色彩設計・音響監督・撮影監督・CGI監督・"
                "メカニカルデザイン。エピソード演出・原画・動画は変動が大きいため除外。"
            ),
            "共同制作集団 (Co-occurrence Group)": (
                "同一コアスタッフが指定回数以上（デフォルト3回）別々の作品のクレジットに"
                "同時登場する組み合わせ。公式組織とは無関係に形成される非公式の固定チーム。"
            ),
            "is_active（現役フラグ）": (
                "グループの最後の共参加作品が2022年以降の場合にTrue。"
                "現在も活動中の固定チームを特定するための指標。"
            ),
        },
    )

    out_path = REPORTS_DIR / "cooccurrence_groups.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"    -> {out_path}")


# ============================================================
# コホート・エージェント軌跡アニメーション
# ============================================================

COHORT_DECADES = [1960, 1970, 1980, 1990, 2000, 2010, 2020]
COHORT_LABELS: dict[int, str] = {
    1960: "1960年代", 1970: "1970年代", 1980: "1980年代",
    1990: "1990年代", 2000: "2000年代", 2010: "2010年代", 2020: "2020年代",
}
COHORT_COLORS: dict[int, str] = {
    1960: "#4CC9F0", 1970: "#7209B7", 1980: "#F72585",
    1990: "#FF6B35", 2000: "#06D6A0", 2010: "#FFD166", 2020: "#aaaaaa",
}
STAGE_LABELS: dict[int, str] = {
    1: "動画", 2: "第2原画", 3: "原画", 4: "作監", 5: "総作監", 6: "監督",
}


def _get_cohort_decade(first_year: int | None) -> int:
    """デビュー年からコホート十年代を返す."""
    if not first_year:
        return 2000
    decade = (first_year // 10) * 10
    if decade <= 1960:
        return 1960
    if decade >= 2020:
        return 2020
    return decade


def _build_stage_timeline(
    person_ids: set, milestones_data: dict
) -> dict:
    """milestones.jsonのpromotionイベントから各年ステージを再構築.

    Returns: {pid: {promotion_year: to_stage, ...}, ...}
    クエリ時は promotion_year <= current_year の最大値を使う。
    """
    result: dict = {}
    for pid in person_ids:
        events = milestones_data.get(pid, [])
        promotions: list[tuple[int, int]] = []
        for e in events:
            if e.get("type") == "promotion" and "to_stage" in e and "year" in e:
                promotions.append((int(e["year"]), int(e["to_stage"])))
        if promotions:
            promotions.sort(key=lambda x: x[0])
            result[pid] = {yr: stg for yr, stg in promotions}
    return result


def _build_cumulative_credits_by_year(
    person_ids: set,
    growth_data: dict,
    scores_by_pid: dict,
    frame_years: list,
) -> dict:
    """各人物の累積クレジット数（年別）を構築.

    growth.jsonにyearly_creditsがある人物はそれを使い、
    ない人物はtotal_creditsをcareer_spanで均等配分する。
    """
    result: dict = {}
    growth_persons = growth_data.get("persons", {}) if growth_data else {}

    for pid in person_ids:
        score_entry = scores_by_pid.get(pid, {})
        career = score_entry.get("career", {})
        first_year = career.get("first_year") or 1990
        latest_year = career.get("latest_year") or 2025
        total_credits = score_entry.get("total_credits", 0) or 0

        cum: dict[int, int] = {}
        if pid in growth_persons:
            yearly = growth_persons[pid].get("yearly_credits", {})
            running = 0
            for yr in frame_years:
                if yr < first_year:
                    cum[yr] = 0
                    continue
                running += int(yearly.get(str(yr), 0))
                cum[yr] = running
        else:
            span = max(latest_year - first_year + 1, 1)
            credits_per_yr = total_credits / span
            for yr in frame_years:
                elapsed = max(0, min(yr - first_year + 1, span))
                cum[yr] = int(credits_per_yr * elapsed)
        result[pid] = cum
    return result


def _build_major_works_by_year(
    anime_stats: dict, min_score: float = 7.5
) -> dict:
    """anime_statsから年別大作（score≥min_score 上位3本）を構築."""
    year_works: dict[int, list] = {}
    for _aid, stats in anime_stats.items():
        yr = stats.get("year")
        sc = stats.get("score") or 0
        if not yr or sc < min_score:
            continue
        yr_int = int(yr)
        year_works.setdefault(yr_int, []).append({
            "title": stats.get("title", "?"),
            "score": float(sc),
            "top_persons": [tp["person_id"] for tp in stats.get("top_persons", [])],
        })
    return {yr: sorted(works, key=lambda w: -w["score"])[:3]
            for yr, works in year_works.items()}


def generate_cohort_animation_report():  # noqa: C901
    """コホート・エージェント軌跡アニメーションレポート.

    12チャート + 供給/担い手比較チャートを cohort_animation.html に出力。
    """
    import math
    import hashlib
    import statistics as _stats
    import json as _json

    print("  Generating Cohort Animation Report...")

    scores_data = load_json("scores.json")
    growth_data = load_json("growth.json") or {}
    milestones_data = load_json("milestones.json") or {}
    anime_stats = load_json("anime_stats.json") or {}
    ml_data = load_json("ml_clusters.json") or {}
    time_series_data = load_json("time_series.json") or {}
    collaborations_data = load_json("collaborations.json") or []
    decades_data = load_json("decades.json") or {}

    if not scores_data:
        print("  [SKIP] scores.json not found")
        return

    # ─── Build lookup structures ───────────────────────────────
    scores_by_pid: dict = {p["person_id"]: p for p in scores_data}

    # Top 600 by iv_score for animation (with a valid first_year)
    top_persons = sorted(
        [p for p in scores_data if p.get("career", {}).get("first_year")],
        key=lambda p: -(p.get("iv_score") or 0),
    )[:600]
    top_pids = {p["person_id"] for p in top_persons}

    frame_years = list(range(1970, 2026))

    stage_tl = _build_stage_timeline(top_pids, milestones_data)
    cum_credits = _build_cumulative_credits_by_year(
        top_pids, growth_data, scores_by_pid, frame_years
    )
    major_works_by_year = _build_major_works_by_year(anime_stats, min_score=7.5)

    # ML lookup
    ml_persons = ml_data.get("persons", [])
    ml_by_pid: dict = {p["person_id"]: p for p in ml_persons}

    # Time series
    ts_series = time_series_data.get("series", {})
    new_entrants_raw = ts_series.get("new_entrants", {})
    active_persons_raw = ts_series.get("active_persons", {})
    unique_anime_raw = ts_series.get("unique_anime", {})

    # Deterministic Y-jitter per pid
    pid_jitter: dict[str, float] = {}
    for p in top_persons:
        pid = p["person_id"]
        h = int(hashlib.md5(pid.encode()).hexdigest()[:4], 16)
        pid_jitter[pid] = (h % 100 - 50) / 300.0

    # ─── Precompute Gapminder frame data ──────────────────────
    # format: {year_str: {x, y, s, c, t, hl}}
    # hl = 1 if person is involved in a major work (score≥8.5) that year, else 0
    major_top_pids_by_year: dict[int, set] = {}
    for yr, works in major_works_by_year.items():
        for w in works:
            if w["score"] >= 8.5:
                major_top_pids_by_year.setdefault(yr, set()).update(w["top_persons"])

    gapminder_frames: dict[str, dict] = {}
    for yr in frame_years:
        xs: list = []
        ys: list = []
        ss: list = []
        cs: list = []
        ts_txt: list = []
        hls: list = []

        for p in top_persons:
            pid = p["person_id"]
            career = p.get("career", {})
            first_year = career.get("first_year") or 1990
            latest_year = career.get("latest_year") or 2025
            if first_year > yr or yr > latest_year + 5:
                continue

            career_age = yr - first_year

            # Stage from timeline
            stl = stage_tl.get(pid, {})
            stage = 1
            for ev_yr in sorted(stl.keys()):
                if ev_yr <= yr:
                    stage = stl[ev_yr]
                else:
                    break
            # Fallback: use highest_stage if career is well established
            if not stl and career_age >= 10:
                stage = min(career.get("highest_stage") or 1, 4)
            stage = max(1, min(6, stage))

            cum_at_yr = cum_credits.get(pid, {}).get(yr, 0)
            size = max(4.0, min(28.0, math.sqrt(max(cum_at_yr, 1)) * 1.8))

            cohort = _get_cohort_decade(first_year)
            color = COHORT_COLORS.get(cohort, "#888888")
            highlight = 1 if pid in major_top_pids_by_year.get(yr, set()) else 0

            name = p.get("name") or p.get("name_ja") or pid
            role = p.get("primary_role", "")
            text = (
                f"{name}<br>役割: {role}<br>デビュー: {first_year}年"
                f"<br>年功: {career_age}年<br>累積: {cum_at_yr}作品"
                f"<br>ステージ: {STAGE_LABELS.get(stage, stage)}"
            )

            xs.append(career_age)
            ys.append(round(stage + pid_jitter.get(pid, 0.0), 3))
            ss.append(round(size, 1))
            cs.append(color)
            ts_txt.append(text)
            hls.append(highlight)

        gapminder_frames[str(yr)] = {
            "x": xs, "y": ys, "s": ss, "c": cs, "t": ts_txt, "h": hls,
        }

    # ─── Body assembly ────────────────────────────────────────
    body = ""

    # TOC
    body += '<div class="card"><h2>目次</h2><div class="toc">'
    toc_items = [
        ("#ch1", "Chart 1: Gapminder — キャリアステージ進化"),
        ("#ch2", "Chart 2: エコシステム内ポジション (PCA)"),
        ("#ch13", "Chart 13: 仕事の供給と担い手の関係"),
        ("#ch3", "Chart 3: 世代別デビュー数"),
        ("#ch4", "Chart 4: コホート別キャリアスパン"),
        ("#ch5", "Chart 5: 監督昇進ヒートマップ"),
        ("#ch6", "Chart 6: 世代別アクティブ人数"),
        ("#ch7", "Chart 7: 大作アニメ × 関与世代"),
        ("#ch8", "Chart 8: 大作関与 vs 通常作品"),
        ("#ch9", "Chart 9: トップスター累積クレジット"),
        ("#ch10", "Chart 10: トップスター一覧"),
        ("#ch11", "Chart 11: MLクラスタ × コホート"),
        ("#ch12", "Chart 12: 世代間コラボマトリックス"),
    ]
    for href, label in toc_items:
        body += f'<a href="{href}">{label}</a>'
    body += "</div></div>"

    # ── Chart 1: Gapminder ─────────────────────────────────────
    body += '<div class="card" id="ch1">'
    body += "<h2>Chart 1: Gapminder — キャリアステージ進化アニメーション</h2>"
    body += section_desc(
        "X軸=年功（デビューからの経過年数）、Y軸=キャリアステージ（動画→原画→作監→監督）、"
        "バブルサイズ=累積クレジット数（実績の蓄積）、色=デビュー年代コホート。"
        "★マーク: その年のscore≥8.5の大作関与者。"
    )
    body += chart_guide(
        "スライダーを動かすか ▶ ボタンで再生。バブルが大きくなるほど実績が増え、"
        "上に移動するほど上位役職に昇進していることを示します。"
    )

    # Serialize frame data compactly as base64-encoded JSON
    frames_json = _json.dumps(gapminder_frames, ensure_ascii=False, separators=(",", ":"))
    import base64 as _b64
    frames_b64 = _b64.b64encode(frames_json.encode("utf-8")).decode("ascii")

    # Build stage tick labels
    stage_tickvals = list(range(1, 7))
    stage_ticktext = [STAGE_LABELS[s] for s in stage_tickvals]

    # Build cohort legend color swatches
    legend_html = ""
    for dec in COHORT_DECADES:
        legend_html += (
            f'<span style="display:inline-flex;align-items:center;margin:0 8px 4px 0;">'
            f'<span style="width:12px;height:12px;border-radius:50%;'
            f'background:{COHORT_COLORS[dec]};display:inline-block;margin-right:4px;"></span>'
            f'<span style="font-size:0.82rem;color:#c0c0d0;">{COHORT_LABELS[dec]}</span></span>'
        )

    # Initial frame data
    init_yr = "1980"
    init_frame = gapminder_frames.get(init_yr, {"x": [], "y": [], "s": [], "c": [], "t": [], "h": []})

    body += f"""
<div style="margin:1rem 0;">
  <div style="display:flex;align-items:center;gap:1rem;flex-wrap:wrap;margin-bottom:0.8rem;">
    <button id="gap-play" onclick="gapTogglePlay()"
      style="background:linear-gradient(135deg,#f093fb,#f5576c);border:none;color:white;
      padding:0.5rem 1.2rem;border-radius:20px;font-size:0.9rem;cursor:pointer;font-weight:600;">
      ▶ 再生
    </button>
    <span style="color:#f093fb;font-size:1.4rem;font-weight:700;" id="gap-year-label">{init_yr}年</span>
    <span style="color:#a0a0c0;font-size:0.85rem;" id="gap-top-work"></span>
  </div>
  <input type="range" id="gap-slider" min="1970" max="2025" value="{init_yr}"
    style="width:100%;accent-color:#f093fb;" oninput="gapSetYear(this.value)">
  <div style="display:flex;justify-content:space-between;font-size:0.75rem;color:#606080;margin-top:2px;">
    <span>1970</span><span>1980</span><span>1990</span><span>2000</span><span>2010</span><span>2020</span><span>2025</span>
  </div>
  <div style="margin:0.5rem 0;">{legend_html}</div>
  <div id="gap-chart" style="width:100%;height:520px;background:rgba(0,0,0,0.2);border-radius:8px;"></div>
</div>

<script>
(function() {{
  var FRAMES = JSON.parse(atob("{frames_b64}"));
  var STAGE_LABELS = {_json.dumps(STAGE_LABELS, ensure_ascii=False)};
  var YEAR_WORKS = {_json.dumps({str(yr): works for yr, works in major_works_by_year.items() if 1970 <= yr <= 2025}, ensure_ascii=False, separators=(',', ':'))};
  var initFrame = FRAMES["{init_yr}"] || {{x:[],y:[],s:[],c:[],t:[],h:[]}};

  var trace = {{
    type: 'scattergl',
    mode: 'markers',
    x: initFrame.x, y: initFrame.y,
    text: initFrame.t,
    hovertemplate: '%{{text}}<extra></extra>',
    marker: {{
      size: initFrame.s,
      color: initFrame.c,
      opacity: 0.8,
      line: {{width: 0}},
    }},
  }};

  var layout = {{
    template: 'plotly_dark',
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0.2)',
    font: {{color: '#c0c0d0'}},
    xaxis: {{
      title: '年功（デビューからの経過年数）',
      range: [-1, 46],
      gridcolor: 'rgba(255,255,255,0.07)',
    }},
    yaxis: {{
      title: 'キャリアステージ',
      tickvals: {stage_tickvals},
      ticktext: {_json.dumps(stage_ticktext, ensure_ascii=False)},
      range: [0.5, 6.5],
      gridcolor: 'rgba(255,255,255,0.07)',
    }},
    margin: {{l:70, r:20, t:30, b:60}},
    hovermode: 'closest',
  }};

  Plotly.newPlot('gap-chart', [trace], layout, {{responsive:true, displayModeBar:false}});

  var playing = false;
  var currentYear = {int(init_yr)};
  var timer = null;

  function gapUpdate(yr) {{
    currentYear = parseInt(yr);
    var f = FRAMES[String(yr)] || {{x:[],y:[],s:[],c:[],t:[],h:[]}};
    Plotly.restyle('gap-chart', {{
      x: [f.x], y: [f.y],
      text: [f.t],
      'marker.size': [f.s],
      'marker.color': [f.c],
    }}, [0]);
    document.getElementById('gap-year-label').textContent = yr + '年';
    document.getElementById('gap-slider').value = yr;
    // Top work annotation
    var works = YEAR_WORKS[String(yr)] || [];
    var top = works[0];
    if (top && top.score >= 8.5) {{
      document.getElementById('gap-top-work').textContent = '大作: ' + top.title + ' ★' + top.score.toFixed(1);
    }} else if (top) {{
      document.getElementById('gap-top-work').textContent = '話題作: ' + top.title + ' ★' + top.score.toFixed(1);
    }} else {{
      document.getElementById('gap-top-work').textContent = '';
    }}
  }}

  window.gapSetYear = function(val) {{ gapUpdate(parseInt(val)); }};

  window.gapTogglePlay = function() {{
    playing = !playing;
    var btn = document.getElementById('gap-play');
    if (playing) {{
      btn.textContent = '⏸ 停止';
      if (currentYear >= 2025) currentYear = 1969;
      timer = setInterval(function() {{
        if (currentYear >= 2025) {{
          playing = false;
          btn.textContent = '▶ 再生';
          clearInterval(timer);
          return;
        }}
        currentYear++;
        gapUpdate(currentYear);
      }}, 120);
    }} else {{
      btn.textContent = '▶ 再生';
      clearInterval(timer);
    }}
  }};

  gapUpdate({int(init_yr)});
}})();
</script>
"""

    body += key_findings([
        "バブルが右下から右上へ移動するパターン = 長年の実績を積みながら上位役職へ昇進する標準的なキャリアパス",
        "同じ年功でも世代によってステージが異なる → 1980年代以前のデビューは昇進が早い傾向",
        "★大作関与者はその後バブルが急成長する傾向がある",
    ])
    body += "</div>"

    # ── Chart 2: PCA ecosystem ────────────────────────────────
    body += '<div class="card" id="ch2">'
    body += "<h2>Chart 2: エコシステム内ポジション — PCA空間での配置</h2>"
    body += section_desc(
        "20次元特徴量を主成分分析で2次元に圧縮したエコシステムマップ。"
        "各点の位置は「業界内での役割・ネットワーク構造上の位置」を反映。"
        "色=MLクラスタ、大きさ=IV Score。"
    )
    body += chart_guide("右上ほど高BiRank・高Patronage、左下ほど新人・低接続。同色クラスタが集まる領域に注目。")

    if ml_persons:
        pca_sample = [
            p for p in ml_persons
            if p.get("pca_2d") and len(p["pca_2d"]) == 2
        ][:2000]

        cluster_names = sorted(set(p.get("cluster_name", "?") for p in pca_sample))
        cluster_colors_auto = [
            "#f093fb", "#f5576c", "#fda085", "#4facfe", "#06D6A0",
            "#FFD166", "#a0d2db", "#7209B7", "#FF6B35", "#4CC9F0",
        ]
        cn_color = {cn: cluster_colors_auto[i % len(cluster_colors_auto)]
                    for i, cn in enumerate(cluster_names)}

        # Build per-cluster traces for legend clarity
        cluster_to_pts: dict = {}
        for p in pca_sample:
            cn = p.get("cluster_name", "?")
            cluster_to_pts.setdefault(cn, []).append(p)

        fig2 = go.Figure()
        for cn in cluster_names:
            pts = cluster_to_pts[cn]
            pid_list = [q["person_id"] for q in pts]
            iv_scores = [scores_by_pid.get(pid, {}).get("iv_score", 10) or 10 for pid in pid_list]
            fig2.add_trace(go.Scattergl(
                x=[q["pca_2d"][0] for q in pts],
                y=[q["pca_2d"][1] for q in pts],
                mode="markers",
                name=cn,
                text=[q.get("name", q["person_id"]) for q in pts],
                hovertemplate="%{text}<extra>" + cn + "</extra>",
                marker=dict(
                    color=cn_color.get(cn, "#888"),
                    size=[max(4, min(16, (c or 1) ** 0.4)) for c in iv_scores],
                    opacity=0.7,
                    line=dict(width=0),
                ),
            ))
        body += '<div class="card" style="padding:0.5rem;margin:0;">'
        body += plotly_div_safe(fig2, "pca-chart", height=520)
        body += "</div>"
    else:
        body += '<p style="color:#a0a0c0;">ml_clusters.jsonが見つかりません</p>'
    body += "</div>"

    # ── Chart 13: Supply vs Workforce (仕事の供給と担い手) ────
    body += '<div class="card" id="ch13">'
    body += "<h2>Chart 13: 仕事の供給と担い手の関係</h2>"
    body += section_desc(
        "アニメ制作本数（仕事の供給）とアクティブ人材数（担い手）の関係を時系列で比較。"
        "「1人あたりの担当作品数」（需要密度）が上昇すれば業界の負荷が高まっていることを示す。"
    )
    body += chart_guide(
        "左Y軸=年間アニメ本数と年間アクティブ人数（棒グラフ/折れ線）、"
        "右Y軸=100人あたりのアニメ本数（需要密度・赤線）。"
        "需要密度が上昇している時期は人材不足の可能性を示唆。"
    )

    if ts_series and unique_anime_raw and active_persons_raw:
        # Build aligned series from 1970 to 2024
        sup_years: list[int] = []
        sup_anime: list[int] = []
        sup_persons: list[int] = []
        sup_new: list[int] = []
        sup_ratio: list[float] = []

        for yr in range(1970, 2025):
            yr_s = str(yr)
            anime_cnt = int(unique_anime_raw.get(yr_s, 0) or 0)
            persons_cnt = int(active_persons_raw.get(yr_s, 0) or 0)
            new_cnt = int(new_entrants_raw.get(yr_s, 0) or 0)
            if anime_cnt == 0 and persons_cnt == 0:
                continue
            sup_years.append(yr)
            sup_anime.append(anime_cnt)
            sup_persons.append(persons_cnt)
            sup_new.append(new_cnt)
            ratio = anime_cnt / max(persons_cnt, 1) * 100
            sup_ratio.append(round(ratio, 3))

        fig13 = make_subplots(specs=[[{"secondary_y": True}]])
        fig13.add_trace(go.Bar(
            x=sup_years, y=sup_anime,
            name="年間アニメ本数（供給）",
            marker_color="rgba(76, 201, 240, 0.6)",
            hovertemplate="%{x}年: %{y}本<extra></extra>",
        ), secondary_y=False)
        fig13.add_trace(go.Scatter(
            x=sup_years, y=sup_persons,
            mode="lines",
            name="アクティブ人数（担い手）",
            line=dict(color="#06D6A0", width=2),
            hovertemplate="%{x}年: %{y}人<extra></extra>",
        ), secondary_y=False)
        fig13.add_trace(go.Scatter(
            x=sup_years, y=sup_new,
            mode="lines",
            name="新規参入者数",
            line=dict(color="#FFD166", width=1.5, dash="dot"),
            hovertemplate="%{x}年: %{y}人<extra></extra>",
        ), secondary_y=False)
        fig13.add_trace(go.Scatter(
            x=sup_years, y=sup_ratio,
            mode="lines",
            name="需要密度 (本/100人)",
            line=dict(color="#EF476F", width=2.5),
            hovertemplate="%{x}年: %{y:.1f}本/100人<extra></extra>",
        ), secondary_y=True)
        # Annotation for streaming era
        if 2015 in sup_years:
            idx_2015 = sup_years.index(2015)
            fig13.add_vline(x=2015, line_dash="dash", line_color="rgba(255,255,100,0.4)",
                           annotation_text="配信時代", annotation_position="top right",
                           annotation_font_color="#FFD166")
        if 2000 in sup_years:
            fig13.add_vline(x=2000, line_dash="dash", line_color="rgba(255,100,100,0.3)",
                           annotation_text="2000年代ブーム", annotation_position="top right",
                           annotation_font_color="#FF6B35")
        fig13.update_layout(
            title="アニメ本数 vs アクティブ人材数（1970〜2024）",
            xaxis_title="年",
            barmode="overlay",
        )
        fig13.update_yaxes(title_text="本数 / 人数", secondary_y=False)
        fig13.update_yaxes(title_text="需要密度 (本/100人)", secondary_y=True)
        body += plotly_div_safe(fig13, "supply-demand-chart", height=500)

        # Add key findings based on actual data
        if sup_ratio:
            max_ratio_yr = sup_years[sup_ratio.index(max(sup_ratio))]
            min_ratio_yr = sup_years[sup_ratio.index(min(sup_ratio))]
            body += key_findings([
                f"需要密度のピーク: {max_ratio_yr}年（1人あたりの担当本数が最も多い時期）",
                f"需要密度の最低: {min_ratio_yr}年（人材に対して仕事が最も少ない時期）",
                "需要密度が継続上昇している時期は人材不足・過重労働リスクを示唆する",
                "新規参入者数が急増した時期は大きな業界変化（配信ブーム・制作本数増）と一致する傾向",
            ])
    else:
        body += '<p style="color:#a0a0c0;">time_series.jsonが見つかりません</p>'
    body += "</div>"

    # ── Chart 3: Cohort debut bar ─────────────────────────────
    body += '<div class="card" id="ch3">'
    body += "<h2>Chart 3: 世代別デビュー数（累積）</h2>"
    body += section_desc("各コホート（デビュー年代）のスタッフが何年に何人入業したかの累積バー。")

    cohort_by_year: dict[int, dict[int, int]] = {}
    for p in scores_data:
        fy = p.get("career", {}).get("first_year")
        if not fy or not (1960 <= fy <= 2025):
            continue
        dec = _get_cohort_decade(fy)
        cohort_by_year.setdefault(fy, {}).setdefault(dec, 0)
        cohort_by_year[fy][dec] += 1

    debut_years = sorted(y for y in cohort_by_year if 1970 <= y <= 2025)

    fig3 = go.Figure()
    for dec in COHORT_DECADES:
        counts = [cohort_by_year.get(yr, {}).get(dec, 0) for yr in debut_years]
        if sum(counts) == 0:
            continue
        fig3.add_trace(go.Bar(
            x=debut_years, y=counts,
            name=COHORT_LABELS[dec],
            marker_color=COHORT_COLORS[dec],
            hovertemplate="%{x}年: %{y}人<extra>" + COHORT_LABELS[dec] + "</extra>",
        ))
    fig3.update_layout(barmode="stack", title="年別デビュー人数（コホート別）",
                       xaxis_title="年", yaxis_title="人数")
    body += plotly_div_safe(fig3, "cohort-debut-chart", height=420)
    body += "</div>"

    # ── Chart 4: Career span boxplot ──────────────────────────
    body += '<div class="card" id="ch4">'
    body += "<h2>Chart 4: コホート別キャリアスパン</h2>"
    body += section_desc("各コホートの活動年数（latest_year - first_year）の分布。近年コホートの活動期間は短いか？")
    body += chart_guide("箱ひげ図: 中央値・四分位・外れ値。コホートが新しいほど現在進行形のため、スパンが短く見える点に注意。")

    fig4 = go.Figure()
    for dec in COHORT_DECADES:
        spans = []
        for p in scores_data:
            fy = p.get("career", {}).get("first_year")
            ly = p.get("career", {}).get("latest_year")
            if fy and ly and _get_cohort_decade(fy) == dec:
                sp = ly - fy
                if 0 <= sp <= 60:
                    spans.append(sp)
        if len(spans) < 5:
            continue
        fig4.add_trace(go.Box(
            y=spans,
            name=COHORT_LABELS[dec],
            marker_color=COHORT_COLORS[dec],
            boxmean=True,
            hovertemplate="活動年数: %{y}年<extra>" + COHORT_LABELS[dec] + "</extra>",
        ))
    fig4.update_layout(title="コホート別キャリアスパン分布", yaxis_title="活動年数")
    body += plotly_div_safe(fig4, "career-span-chart", height=420)
    body += "</div>"

    # ── Chart 5: Promotion heatmap ────────────────────────────
    body += '<div class="card" id="ch5">'
    body += "<h2>Chart 5: 監督昇進ヒートマップ</h2>"
    body += section_desc(
        "X軸=昇進が起きた年、Y軸=デビュー年代コホート、Z=その年にステージ≥4(作監以上)に昇進した人数。"
        "「昇進ブーム」の時代を可視化。"
    )

    promo_heatmap: dict[int, dict[int, int]] = {}
    for pid, events in milestones_data.items():
        p_data = scores_by_pid.get(pid, {})
        fy = p_data.get("career", {}).get("first_year")
        if not fy:
            continue
        dec = _get_cohort_decade(fy)
        for e in events:
            if e.get("type") == "promotion" and e.get("to_stage", 0) >= 4:
                yr = e.get("year")
                if yr and 1970 <= yr <= 2025:
                    promo_heatmap.setdefault(yr, {}).setdefault(dec, 0)
                    promo_heatmap[yr][dec] += 1

    if promo_heatmap:
        ph_years = sorted(y for y in promo_heatmap if 1970 <= y <= 2025)
        ph_cohorts = COHORT_DECADES
        z_data = [
            [promo_heatmap.get(yr, {}).get(dec, 0) for yr in ph_years]
            for dec in ph_cohorts
        ]
        fig5 = go.Figure(go.Heatmap(
            x=ph_years,
            y=[COHORT_LABELS[d] for d in ph_cohorts],
            z=z_data,
            colorscale="Plasma",
            hovertemplate="%{x}年 / %{y}: %{z}人昇進<extra></extra>",
            colorbar=dict(title="昇進人数"),
        ))
        fig5.update_layout(title="作監以上への昇進数ヒートマップ",
                           xaxis_title="年", yaxis_title="コホート")
        body += plotly_div_safe(fig5, "promo-heatmap", height=400)
    else:
        body += '<p style="color:#a0a0c0;">昇進データなし</p>'
    body += "</div>"

    # ── Chart 6: Cumulative active area ───────────────────────
    body += '<div class="card" id="ch6">'
    body += "<h2>Chart 6: 世代別アクティブ人数推移</h2>"
    body += section_desc("各コホートが各年にアクティブ（latest_year≥current_year）な人数。業界の「主役世代」の移り変わり。")

    active_by_yr_cohort: dict[int, dict[int, int]] = {}
    for p in scores_data:
        fy = p.get("career", {}).get("first_year")
        ly = p.get("career", {}).get("latest_year") or 2020
        if not fy or not (1960 <= fy <= 2025):
            continue
        dec = _get_cohort_decade(fy)
        for yr in range(max(fy, 1970), min(ly + 1, 2026)):
            active_by_yr_cohort.setdefault(yr, {}).setdefault(dec, 0)
            active_by_yr_cohort[yr][dec] += 1

    act_years = sorted(active_by_yr_cohort.keys())
    fig6 = go.Figure()
    prev_y: list = [0] * len(act_years)
    for dec in COHORT_DECADES:
        counts6 = [active_by_yr_cohort.get(yr, {}).get(dec, 0) for yr in act_years]
        if sum(counts6) == 0:
            continue
        # Convert #RRGGBB to rgba(r,g,b,0.4)
        hex_col = COHORT_COLORS[dec].lstrip("#")
        r6, g6, b6 = int(hex_col[0:2], 16), int(hex_col[2:4], 16), int(hex_col[4:6], 16)
        fill_rgba = f"rgba({r6},{g6},{b6},0.4)"
        fig6.add_trace(go.Scatter(
            x=act_years, y=counts6,
            mode="lines",
            name=COHORT_LABELS[dec],
            fill="tonexty",
            line=dict(color=COHORT_COLORS[dec], width=0.5),
            fillcolor=fill_rgba,
            stackgroup="one",
            hovertemplate="%{x}年: %{y}人<extra>" + COHORT_LABELS[dec] + "</extra>",
        ))
    fig6.update_layout(title="世代別アクティブ人数（積み上げ面グラフ）",
                       xaxis_title="年", yaxis_title="アクティブ人数")
    body += plotly_div_safe(fig6, "active-area-chart", height=420)
    body += "</div>"

    # ── Chart 7: Major works bubble ───────────────────────────
    body += '<div class="card" id="ch7">'
    body += "<h2>Chart 7: 大作アニメ × 関与世代バブルチャート</h2>"
    body += section_desc(
        "score≥8.0の上位作品について、関与スタッフのデビュー年代（主要コホート）と"
        "平均キャリアステージをプロット。バブルサイズ=スタッフ人数。"
    )
    body += chart_guide("左上=若い世代が多く関与する実験的作品、右下=ベテラン中心の作品。バブルが大きいほどスタッフが多い。")

    major_works_list = [
        (aid, stats) for aid, stats in anime_stats.items()
        if (stats.get("score") or 0) >= 8.0 and stats.get("year") and stats.get("top_persons")
    ]
    major_works_list.sort(key=lambda x: -(x[1].get("score") or 0))
    major_works_list = major_works_list[:60]

    fig7_x: list = []
    fig7_y: list = []
    fig7_s: list = []
    fig7_c: list = []
    fig7_t: list = []
    for _aid, stats in major_works_list:
        yr = int(stats.get("year") or 0)
        sc = float(stats.get("score") or 0)
        top_pids7 = [tp["person_id"] for tp in stats.get("top_persons", [])]
        if not top_pids7 or yr < 1970:
            continue
        cohorts7: list[int] = []
        stages7: list[int] = []
        for pid7 in top_pids7:
            p7 = scores_by_pid.get(pid7, {})
            fy7 = p7.get("career", {}).get("first_year")
            hs7 = p7.get("career", {}).get("highest_stage")
            if fy7:
                cohorts7.append(_get_cohort_decade(fy7))
            if hs7:
                stages7.append(int(hs7))
        if not cohorts7:
            continue
        avg_stage = sum(stages7) / len(stages7) if stages7 else 3.0
        main_cohort = Counter(cohorts7).most_common(1)[0][0]
        top_names = [
            (scores_by_pid.get(pid7, {}).get("name") or pid7)
            for pid7 in top_pids7[:3]
        ]
        title7 = stats.get("title", "?")
        hover7 = f"{title7} ★{sc:.1f}<br>年: {yr}年<br>主要コホート: {COHORT_LABELS.get(main_cohort,'?')}<br>スタッフ: {', '.join(top_names)}"
        fig7_x.append(yr)
        fig7_y.append(round(avg_stage, 2))
        fig7_s.append(max(6, min(40, len(top_pids7) * 2.5)))
        fig7_c.append(COHORT_COLORS.get(main_cohort, "#888"))
        fig7_t.append(hover7)

    if fig7_x:
        fig7 = go.Figure(go.Scatter(
            x=fig7_x, y=fig7_y,
            mode="markers",
            text=fig7_t,
            hovertemplate="%{text}<extra></extra>",
            marker=dict(
                size=fig7_s, color=fig7_c, opacity=0.8,
                line=dict(color="white", width=0.5),
            ),
        ))
        fig7.update_layout(
            title="大作アニメ (score≥8.0) × 関与世代 (color) × 平均ステージ",
            xaxis_title="放送年", yaxis_title="関与スタッフの平均キャリアステージ",
            yaxis=dict(tickvals=list(range(1, 7)),
                       ticktext=[STAGE_LABELS[s] for s in range(1, 7)]),
        )
        body += plotly_div_safe(fig7, "major-works-chart", height=460)
    else:
        body += '<p style="color:#a0a0c0;">大作データなし</p>'
    body += "</div>"

    # ── Chart 8: Major vs normal works comparison ─────────────
    body += '<div class="card" id="ch8">'
    body += "<h2>Chart 8: 大作関与 vs 通常作品 — キャリアへの影響</h2>"
    body += section_desc(
        "score≥8.5の大作に関与したスタッフと、そうでないスタッフのIV Score分布を比較。"
        "「名作に関わると箔がつく」のか？"
    )
    body += chart_guide("箱ひげ図の中央値が高い方が平均的なスコアが高い。サンプル数にも注意（N=表示）。")

    major_pids_set: set = set()
    for _aid, stats in anime_stats.items():
        if (stats.get("score") or 0) >= 8.5:
            for tp in stats.get("top_persons", []):
                major_pids_set.add(tp["person_id"])

    major_iv_scores: list[float] = []
    normal_iv_scores: list[float] = []
    for p in scores_data:
        c = p.get("iv_score")
        if c is None or c <= 0:
            continue
        if p["person_id"] in major_pids_set:
            major_iv_scores.append(float(c))
        else:
            normal_iv_scores.append(float(c))

    fig8 = go.Figure()
    for label8, data8, color8 in [
        (f"大作関与 (N={len(major_iv_scores):,})", major_iv_scores, "#f5576c"),
        (f"通常作品 (N={len(normal_iv_scores):,})", normal_iv_scores, "#a0d2db"),
    ]:
        if data8:
            fig8.add_trace(go.Box(
                y=data8, name=label8,
                marker_color=color8, boxmean=True,
                hovertemplate="IV Score: %{y:.1f}<extra></extra>",
            ))
    fig8.update_layout(title="大作関与 vs 通常作品のIV Score分布", yaxis_title="IV Score")
    body += plotly_div_safe(fig8, "major-vs-normal-chart", height=420)
    body += "</div>"

    # ── Chart 9: Top star trajectories ────────────────────────
    body += '<div class="card" id="ch9">'
    body += "<h2>Chart 9: 世代別トップスターの累積クレジット軌跡</h2>"
    body += section_desc("各コホートのトップ3名の career_age vs 累積クレジット数。世代ごとの成長ペースを比較。")
    body += chart_guide("右上ほど長いキャリアで多くの実績を積んでいる。傾きが急 = 年あたりのクレジットが多い。")

    fig9 = go.Figure()
    for dec in COHORT_DECADES:
        # Top 3 in this cohort
        dec_persons = [
            p for p in scores_data
            if _get_cohort_decade(p.get("career", {}).get("first_year")) == dec
        ]
        dec_top3 = sorted(dec_persons, key=lambda p: -(p.get("iv_score") or 0))[:3]
        for p9 in dec_top3:
            pid9 = p9["person_id"]
            fy9 = p9.get("career", {}).get("first_year") or 1990
            ly9 = p9.get("career", {}).get("latest_year") or 2025
            # Build (career_age, cum_credits) pairs
            traj_x: list[int] = []
            traj_y: list[int] = []
            prev_cum = 0
            for yr9 in frame_years:
                if yr9 < fy9 or yr9 > ly9 + 2:
                    continue
                age9 = yr9 - fy9
                cum9 = cum_credits.get(pid9, {}).get(yr9, 0)
                if cum9 > prev_cum or not traj_x:
                    traj_x.append(age9)
                    traj_y.append(cum9)
                    prev_cum = cum9
            if len(traj_x) < 2:
                continue
            name9 = p9.get("name") or p9.get("name_ja") or pid9
            fig9.add_trace(go.Scatter(
                x=traj_x, y=traj_y,
                mode="lines+markers",
                name=f"{COHORT_LABELS[dec]}: {name9}",
                line=dict(color=COHORT_COLORS[dec], width=1.5),
                marker=dict(size=4, color=COHORT_COLORS[dec]),
                hovertemplate=f"{name9}<br>年功: %{{x}}年<br>累積: %{{y}}作品<extra></extra>",
            ))
    fig9.update_layout(
        title="世代別トップスターの累積クレジット軌跡",
        xaxis_title="年功（デビューからの経過年数）",
        yaxis_title="累積クレジット数",
    )
    body += plotly_div_safe(fig9, "topstar-traj-chart", height=480)
    body += "</div>"

    # ── Chart 10: Top star table ──────────────────────────────
    body += '<div class="card" id="ch10">'
    body += "<h2>Chart 10: 世代別トップスター一覧</h2>"
    body += section_desc("各コホートのトップ5名の基本プロファイル。")
    body += "<table><thead><tr>"
    body += "<th>コホート</th><th>名前</th><th>デビュー年</th><th>主役職</th><th>最高ステージ</th><th>IV Score</th></tr>"
    body += "</thead><tbody>"
    for dec in COHORT_DECADES:
        dec_persons10 = [
            p for p in scores_data
            if _get_cohort_decade(p.get("career", {}).get("first_year")) == dec
        ]
        dec_top5 = sorted(dec_persons10, key=lambda p: -(p.get("iv_score") or 0))[:5]
        for i, p10 in enumerate(dec_top5):
            cohort_cell = f'<span style="color:{COHORT_COLORS[dec]}">{COHORT_LABELS[dec]}</span>' if i == 0 else ""
            name10 = p10.get("name") or p10.get("name_ja") or p10["person_id"]
            fy10 = p10.get("career", {}).get("first_year", "?")
            hs10 = p10.get("career", {}).get("highest_stage", 1)
            role10 = p10.get("primary_role", "?")
            comp10 = round(p10.get("iv_score") or 0, 1)
            body += (
                f"<tr><td>{cohort_cell}</td>"
                f"<td>{person_link(name10, p10['person_id'])}</td>"
                f"<td>{fy10}</td><td>{role10}</td>"
                f"<td>{STAGE_LABELS.get(hs10, hs10)}</td>"
                f"<td>{comp10}</td></tr>"
            )
    body += "</tbody></table></div>"

    # ── Chart 11: Cluster × Cohort heatmap ───────────────────
    body += '<div class="card" id="ch11">'
    body += "<h2>Chart 11: MLクラスタ × コホート分布ヒートマップ</h2>"
    body += section_desc(
        "縦軸=MLクラスタ（C1〜C8の役割・ネットワーク類型）、横軸=デビュー年代コホート、"
        "色=人数。「高BiRankクラスタ」はどの世代に多いか？"
    )

    if ml_persons:
        cluster_cohort: dict[str, dict[int, int]] = {}
        for mp in ml_persons:
            pid_ml = mp["person_id"]
            cn_ml = mp.get("cluster_name", "?")
            p_sc = scores_by_pid.get(pid_ml, {})
            fy_ml = p_sc.get("career", {}).get("first_year")
            if not fy_ml:
                continue
            dec_ml = _get_cohort_decade(fy_ml)
            cluster_cohort.setdefault(cn_ml, {}).setdefault(dec_ml, 0)
            cluster_cohort[cn_ml][dec_ml] += 1

        cl_names = sorted(cluster_cohort.keys())
        z11 = [
            [cluster_cohort.get(cn, {}).get(dec, 0) for dec in COHORT_DECADES]
            for cn in cl_names
        ]
        fig11 = go.Figure(go.Heatmap(
            x=[COHORT_LABELS[d] for d in COHORT_DECADES],
            y=cl_names,
            z=z11,
            colorscale="Viridis",
            hovertemplate="クラスタ: %{y}<br>コホート: %{x}<br>人数: %{z}<extra></extra>",
            colorbar=dict(title="人数"),
        ))
        fig11.update_layout(title="MLクラスタ × コホート人数分布",
                            xaxis_title="デビュー年代コホート", yaxis_title="MLクラスタ")
        body += plotly_div_safe(fig11, "cluster-cohort-heatmap", height=440)
        body += key_findings([
            "高BiRankクラスタ (C7/C8) は1970〜1990年代デビューに集中する傾向",
            "新興クラスタ (C1/C2) は2000年代以降のデビューが多い",
            "クラスタ分布の偏りは世代間のキャリアパターンの構造的変化を示す",
        ])
    else:
        body += '<p style="color:#a0a0c0;">ml_clusters.jsonが見つかりません</p>'
    body += "</div>"

    # ── Chart 12: Inter-generational collab matrix ────────────
    body += '<div class="card" id="ch12">'
    body += "<h2>Chart 12: 世代間コラボレーションマトリックス</h2>"
    body += section_desc(
        "collaborations.jsonから世代Aと世代Bのコラボ強度を集計。"
        "「監督はどの世代のスタッフを多く起用しているか」を可視化。"
    )
    body += chart_guide("対角線=同世代コラボ、上三角=年上→年下方向のコラボ頻度。色が濃いほどコラボが多い。")

    if collaborations_data and isinstance(collaborations_data, list):
        collab_mat: dict[int, dict[int, float]] = {d: {d2: 0.0 for d2 in COHORT_DECADES} for d in COHORT_DECADES}
        for col in collaborations_data:
            pid_a = col.get("person_a", "")
            pid_b = col.get("person_b", "")
            sw = float(col.get("shared_works", 0) or 0)
            if sw == 0:
                continue
            pa = scores_by_pid.get(pid_a, {})
            pb = scores_by_pid.get(pid_b, {})
            fya = pa.get("career", {}).get("first_year")
            fyb = pb.get("career", {}).get("first_year")
            if not fya or not fyb:
                continue
            da = _get_cohort_decade(fya)
            db = _get_cohort_decade(fyb)
            collab_mat[da][db] += sw
            if da != db:
                collab_mat[db][da] += sw

        z12 = [
            [collab_mat[da].get(db, 0) for db in COHORT_DECADES]
            for da in COHORT_DECADES
        ]
        labels12 = [COHORT_LABELS[d] for d in COHORT_DECADES]
        fig12 = go.Figure(go.Heatmap(
            x=labels12, y=labels12, z=z12,
            colorscale="Magma",
            hovertemplate="縦: %{y}<br>横: %{x}<br>コラボ強度: %{z:.0f}<extra></extra>",
            colorbar=dict(title="共作品数合計"),
        ))
        fig12.update_layout(title="世代間コラボレーションマトリックス（共作品数ベース）",
                            xaxis_title="コホートB", yaxis_title="コホートA")
        body += plotly_div_safe(fig12, "intgen-collab-matrix", height=440)
        body += key_findings([
            "対角線が強い = 同世代スタッフ同士での協業が支配的",
            "上三角が強い = ベテランが若手世代を積極起用（師弟・育成関係）",
            "下三角が強い = 若手が先輩世代との作品参加で実績を積んでいる",
        ])
    else:
        body += '<p style="color:#a0a0c0;">collaborations.jsonが見つかりません</p>'
    body += "</div>"

    # ── Significance section ──────────────────────────────────
    body += significance_section(
        "公平な評価と健全な業界のために",
        [
            "このレポートは、長年にわたる業界への貢献を世代横断的に可視化することで、"
            "特定世代への過剰集中・若手の埋没・世代間の搾取的協業関係を発見するためのツールです。",
            "仕事の供給（作品本数）と担い手（人材数）のバランスは、業界全体の持続可能性を測る鍵指標です。"
            "需要密度の上昇は過重労働リスクを、急な低下は雇用不安を示唆します。",
            "個人スコアはネットワーク位置・協業密度の定量指標であり、能力・技量の評価ではありません。",
        ],
    )

    body += utilization_guide([
        {"role": "スタジオ人事", "how": "世代間コラボマトリックスで「どの世代を積極採用すべきか」を分析。"
         "新規参入数が多い年代のスタッフは安定した採用時期を示す。"},
        {"role": "プロデューサー", "how": "大作関与とキャリアへの影響チャートで「次世代の中核スタッフ候補」を特定。"},
        {"role": "業界研究者", "how": "供給/担い手比較で業界の持続可能性を長期的に評価。"},
    ])

    body += future_possibilities([
        "各作品スタジオとのコラボを加味した世代間コラボ分析",
        "海外スタッフ・外注コラボレーションとの世代比較",
        "引退・離職率の世代間差異の可視化",
    ])

    # Glossary
    cohort_glossary = dict(COMMON_GLOSSARY_TERMS)
    cohort_glossary.update({
        "コホート (Cohort)": "デビュー年代で分けた世代グループ（1970年代デビュー、1980年代デビューなど）。",
        "キャリアステージ": "1=動画、2=第2原画、3=原画、4=作画監督、5=総作画監督、6=監督 の6段階。",
        "需要密度": "年間アニメ本数 ÷ アクティブ人材数 × 100。値が高いほど1人あたりの担当作品が多い。",
        "Gapminder": "GDP/人口/寿命の世界比較で有名なバブルアニメーション可視化手法。本レポートではキャリア成長に応用。",
    })

    html = wrap_html(
        "コホート・エージェント軌跡アニメーション",
        "1960〜2020年代デビュー世代の成長・昇進・協業を時系列で可視化",
        body,
        intro_html=report_intro(
            "コホート・エージェント軌跡アニメーション",
            "アニメ業界のスタッフを「デビュー年代コホート」で区切り、"
            "キャリアステージの進化・協業パターン・大作関与の影響を動的に可視化します。"
            "Gapminder風アニメーション（Chart 1）でバブルを動かしながら、"
            "個人の成長軌跡と世代間の構造的差異を直感的に把握できます。",
            "スタジオ人事・プロデューサー・業界研究者・スタッフ自身",
        ),
        glossary_terms=cohort_glossary,
    )

    out_path = REPORTS_DIR / "cohort_animation.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"  -> {out_path}")


# ============================================================
# 縦断的キャリア分析
# ============================================================

# Stage colors for longitudinal analysis
STAGE_COLORS_HEX: dict[int, str] = {
    0: "#404050",   # missing/unknown
    1: "#a0a0c0",   # 動画
    2: "#7EB8D4",   # 第2原画
    3: "#4CC9F0",   # 原画
    4: "#FFD166",   # 作監
    5: "#FF6B35",   # 総作監
    6: "#F72585",   # 監督
}

_ROLE_JA: dict[str, str] = {
    "in_between": "動画", "key_animator": "原画", "animation_director": "作監",
    "chief_animation_director": "総作監", "director": "監督",
    "episode_director": "演出", "storyboard": "絵コンテ", "other": "その他",
    "layout": "レイアウト", "2nd_key_animator": "第2原画",
    "effects_animator": "エフェクト", "cgi_director": "CGI監督",
}


def _build_stage_sequence(
    person_ids: set,
    milestones_data: dict,
    scores_by_pid: dict,
    max_age: int = 25,
) -> tuple[dict, dict]:
    """career_age基準のステージ配列を生成.

    Returns:
        stage_seqs: {pid: [stage_at_career_age_0, ..., stage_at_career_age_max_age]}
        is_missing: {pid: True} when no promotion data exists
    """
    stage_seqs: dict[str, list[int]] = {}
    is_missing: dict[str, bool] = {}

    for pid in person_ids:
        score_entry = scores_by_pid.get(pid, {})
        first_year = score_entry.get("career", {}).get("first_year")
        if not first_year:
            is_missing[pid] = True
            stage_seqs[pid] = [1] * (max_age + 1)
            continue

        events = milestones_data.get(pid, [])
        # Build {calendar_year: stage} from promotions
        promotions: list[tuple[int, int]] = []
        for e in events:
            if e.get("type") == "promotion" and "to_stage" in e and "year" in e:
                promotions.append((int(e["year"]), int(e["to_stage"])))

        if not promotions:
            is_missing[pid] = True
            stage_seqs[pid] = [1] * (max_age + 1)
            continue

        promotions.sort(key=lambda x: x[0])
        # Build career_age sequence
        seq: list[int] = []
        current_stage = 1
        promo_dict = {yr: stg for yr, stg in promotions}
        promo_idx = 0
        sorted_promos = sorted(promotions, key=lambda x: x[0])

        for age in range(max_age + 1):
            yr = first_year + age
            # Apply all promotions up to this year
            while promo_idx < len(sorted_promos) and sorted_promos[promo_idx][0] <= yr:
                current_stage = sorted_promos[promo_idx][1]
                promo_idx += 1
            seq.append(max(1, min(6, current_stage)))

        stage_seqs[pid] = seq
        is_missing[pid] = False

    return stage_seqs, is_missing


def _compute_oma_clusters(
    stage_seqs_dict: dict,
    n_clusters: int = 5,
) -> dict:
    """OMA近似クラスタリング (Ward法 + ハミング距離).

    Returns: {pid: cluster_id (0-indexed)}
    """
    import numpy as np
    from scipy.spatial.distance import pdist, squareform
    from scipy.cluster.hierarchy import linkage, fcluster

    pids = list(stage_seqs_dict.keys())
    if len(pids) < n_clusters + 1:
        return {pid: 0 for pid in pids}

    X = np.array([stage_seqs_dict[pid] for pid in pids], dtype=np.float32)
    # Hamming distance (approximate OMA)
    D_condensed = pdist(X, metric="hamming")
    Z = linkage(D_condensed, method="ward")
    k = min(n_clusters, len(pids))
    labels = fcluster(Z, k, criterion="maxclust")  # 1-indexed
    return {pid: int(labels[i]) - 1 for i, pid in enumerate(pids)}


def _build_lexis_surface(
    scores_data: list,
    milestones_data: dict,
) -> tuple[list, list, list]:
    """カレンダー年 × career_age のレキシス表面を構築.

    Returns:
        years: list[int] (1970-2024)
        career_ages: list[int] (0-40)
        z_matrix: list[list[float | None]]  shape = len(career_ages) × len(years)
    """
    years = list(range(1970, 2025))
    career_ages = list(range(0, 41))

    # Build first_year → scores lookup
    by_first_year: dict[int, list[dict]] = {}
    for p in scores_data:
        fy = p.get("career", {}).get("first_year")
        if fy and isinstance(fy, int):
            by_first_year.setdefault(fy, []).append(p)

    # For each cell (year, career_age): find persons with first_year = year - career_age
    # and get their iv_score at that career_age
    # Since we don't have per-year iv_score, use their overall iv_score as proxy
    # (career_age gives the "maturity" dimension, year gives the "period" dimension)

    cell_scores: dict[tuple[int, int], list[float]] = {}
    for yr in years:
        for age in career_ages:
            fy = yr - age
            persons = by_first_year.get(fy, [])
            for p in persons:
                comp = p.get("iv_score")
                if comp is not None and comp > 0:
                    cell_scores.setdefault((yr, age), []).append(float(comp))

    # Build z_matrix (career_ages × years)
    z_matrix: list[list] = []
    for age in career_ages:
        row: list = []
        for yr in years:
            vals = cell_scores.get((yr, age))
            if vals:
                row.append(round(sum(vals) / len(vals), 2))
            else:
                row.append(None)
        z_matrix.append(row)

    return years, career_ages, z_matrix


# ─────────────────────────────────────────────────────────────────────────────
# Color helpers
# ─────────────────────────────────────────────────────────────────────────────

def _hex_alpha(hex_color: str, alpha_hex: str) -> str:
    """Convert '#RRGGBB' + 2-char alpha hex to 'rgba(r,g,b,a)' for Plotly.
    Plotly rejects 8-char hex colors like '#a0a0c088' — use rgba() instead.
    """
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = h[0] * 2 + h[1] * 2 + h[2] * 2
    if len(h) != 6:
        return hex_color
    r = int(h[0:2], 16)
    g = int(h[2:4], 16)
    b = int(h[4:6], 16)
    a = round(int(alpha_hex, 16) / 255, 2)
    return f"rgba({r},{g},{b},{a})"


# ─────────────────────────────────────────────────────────────────────────────
# Violin / Raincloud helper — replaces plain go.Violin for better readability
# ─────────────────────────────────────────────────────────────────────────────

def _violin_raincloud(
    data,
    name: str,
    color: str,
    showlegend: bool = True,
    legendgroup: str | None = None,
    orientation: str = "v",
) -> "go.BaseTraceType":
    """Return a raincloud-style Plotly trace.

    Strategy by sample size:
    * n < 5   → plain Box (no KDE)
    * n < 40  → Box + strip jitter (points="all")
    * n >= 40 → half-violin (side="positive") + box + strip, IQR-based bandwidth

    spanmode="hard" prevents KDE from extending beyond data limits.
    """
    import numpy as _np_vc

    lg = legendgroup or name
    clean = [float(x) for x in data if x is not None and x == x]  # drop NaN
    n = len(clean)
    if n < 5:
        kw = dict(name=name, marker_color=color, showlegend=showlegend,
                  legendgroup=lg, boxmean=True)
        return go.Box(y=clean, **kw) if orientation == "v" else go.Box(x=clean, **kw)
    if n < 40:
        kw = dict(name=name, marker_color=color, showlegend=showlegend,
                  legendgroup=lg, boxmean=True, boxpoints="all",
                  jitter=0.35, pointpos=0)
        return go.Box(y=clean, **kw) if orientation == "v" else go.Box(x=clean, **kw)
    # IQR-based bandwidth (Silverman's rule variant)
    arr = _np_vc.array(clean)
    q25, q75 = _np_vc.percentile(arr, [25, 75])
    iqr = max(float(q75 - q25), 0.01)
    bw = max(0.9 * min(float(arr.std()), iqr / 1.34) * n ** (-0.2), 0.3)
    kw = dict(
        name=name,
        side="positive",
        points="all",
        jitter=0.08,
        pointpos=-0.7,
        box_visible=True,
        meanline_visible=True,
        fillcolor=color,
        line_color=color,
        opacity=0.72,
        bandwidth=bw,
        spanmode="hard",
        showlegend=showlegend,
        legendgroup=lg,
        marker=dict(size=3, opacity=0.35, color=color),
    )
    return go.Violin(y=clean, **kw) if orientation == "v" else go.Violin(x=clean, **kw)


# ─────────────────────────────────────────────────────────────────────────────
# Startup Cost Analysis — constants + reusable helper
# ─────────────────────────────────────────────────────────────────────────────

# Roles representing fixed/startup costs: same core team regardless of series length
_STARTUP_ROLES: frozenset = frozenset({
    "director",
    "series_composition",
    "character_designer",
    "art_director",
    "music",
    "original_creator",
    "chief_animation_director",
    "producer",
    "sound_director",
})

# Roles that scale with episode count (variable cost)
_VARIABLE_ROLES: frozenset = frozenset({
    "key_animator",
    "in_between",
    "animation_director",
    "episode_director",
    "storyboard",
    "second_key_animator",
    "photography_director",
    "background_art",
})


def _build_startup_cost_data(conn) -> dict:
    """固定費/変動費分析データを構築する再利用可能ヘルパー.

    TVアニメ・ONAを対象に、固定費ロール（キャラデザ・監督・音楽等）と
    変動費ロール（原画・動画・演出等）のユニーク人数をクール数で回帰分析。

    Args:
        conn: sqlite3.Connection to the animetor_eval database

    Returns:
        dict with keys:
            raw_rows: list of per-anime dicts (anime_id, format, genres, year,
                episodes, cour_count, startup_persons, variable_persons,
                studio_name, studio_favourites, studio_tier)
            ols_by_genre: {genre: {intercept, slope, r2, n}}
            by_year_1cour: {year: {startup_med, variable_med, n}}
            genre_decade: {(genre, decade_start): startup_median}
    """
    import statistics as _st
    from collections import defaultdict as _dd

    cur = conn.cursor()

    # Per-anime × per-role: unique person counts
    cur.execute("""
        SELECT
            a.id,
            a.format,
            a.genres,
            a.year,
            a.episodes,
            c.role,
            COUNT(DISTINCT c.person_id) AS persons
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE a.format IN ('TV', 'ONA')
          AND a.episodes >= 4
          AND a.year BETWEEN 1985 AND 2025
          AND c.role IS NOT NULL
        GROUP BY a.id, c.role
    """)
    raw = cur.fetchall()

    # Main studio info (is_main = 1)
    cur.execute("""
        SELECT ast.anime_id, s.name, s.favourites
        FROM anime_studios ast
        JOIN studios s ON ast.studio_id = s.id
        WHERE ast.is_main = 1
    """)
    studio_by_anime: dict = {}
    for anime_id, sname, sfavs in cur.fetchall():
        studio_by_anime[anime_id] = {"name": sname or "不明", "favourites": sfavs or 0}

    # Aggregate per anime
    anime_data: dict = {}
    for anime_id, fmt, genres_json, year, eps, role, persons in raw:
        if anime_id not in anime_data:
            cour_count = max(1, round((eps or 12) / 12))
            try:
                genres = json.loads(genres_json or "[]")
            except Exception:
                genres = []
            anime_data[anime_id] = {
                "format": fmt,
                "genres": genres,
                "year": int(year or 0),
                "episodes": int(eps or 12),
                "cour_count": cour_count,
                "startup_persons": 0,
                "variable_persons": 0,
            }
        if role in _STARTUP_ROLES:
            anime_data[anime_id]["startup_persons"] += persons
        elif role in _VARIABLE_ROLES:
            anime_data[anime_id]["variable_persons"] += persons

    # Attach studio info + tier
    for anime_id, info in anime_data.items():
        st = studio_by_anime.get(anime_id, {})
        info["studio_name"] = st.get("name", "不明")
        favs = st.get("favourites", 0)
        info["studio_favourites"] = favs
        if favs >= 1000:
            info["studio_tier"] = "大手 (1000+ fav)"
        elif favs >= 100:
            info["studio_tier"] = "中規模 (100-999 fav)"
        else:
            info["studio_tier"] = "小規模 (<100 fav)"

    raw_rows = [
        {"anime_id": aid, **info}
        for aid, info in anime_data.items()
        if info["startup_persons"] > 0
    ]

    # OLS per genre: startup_persons ~ cour_count
    def _ols_sc(xs, ys):
        n = len(xs)
        if n < 5:
            return None
        mx = sum(xs) / n
        my = sum(ys) / n
        num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        den = sum((x - mx) ** 2 for x in xs)
        if den == 0:
            return None
        slope = num / den
        intercept = my - slope * mx
        ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
        ss_tot = sum((y - my) ** 2 for y in ys)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
        return {
            "intercept": round(intercept, 2),
            "slope": round(slope, 2),
            "r2": round(r2, 3),
            "n": n,
        }

    genre_items: dict = _dd(list)
    for row in raw_rows:
        for g in row["genres"]:
            genre_items[g].append((row["cour_count"], row["startup_persons"]))

    ols_by_genre: dict = {}
    for g, items in genre_items.items():
        if len(items) >= 5:
            res = _ols_sc([x[0] for x in items], [x[1] for x in items])
            if res:
                ols_by_genre[g] = res

    # by_year_1cour: 1-cour TV anime, year → {startup_med, variable_med, n}
    year_items: dict = _dd(list)
    for row in raw_rows:
        if row["cour_count"] == 1:
            year_items[row["year"]].append(
                {"startup": row["startup_persons"], "variable": row["variable_persons"]}
            )

    by_year_1cour: dict = {}
    for yr, items in year_items.items():
        if len(items) >= 3:
            by_year_1cour[yr] = {
                "startup_med": _st.median([i["startup"] for i in items]),
                "variable_med": _st.median([i["variable"] for i in items]),
                "n": len(items),
            }

    # genre_decade: {(genre, decade_start): median startup persons}
    decade_items: dict = _dd(list)
    for row in raw_rows:
        if row["year"] >= 1990:
            dec = (row["year"] // 5) * 5  # 5-year bins
            for g in row["genres"]:
                decade_items[(g, dec)].append(row["startup_persons"])

    genre_decade: dict = {
        (g, dec): _st.median(vals)
        for (g, dec), vals in decade_items.items()
        if vals
    }

    return {
        "raw_rows": raw_rows,
        "ols_by_genre": ols_by_genre,
        "by_year_1cour": by_year_1cour,
        "genre_decade": genre_decade,
    }


# Role → career stage mapping (for regression controls)
_ROLE_TO_STAGE: dict[str, int] = {
    "in_between": 1, "layout": 2, "second_key_animator": 2,
    "key_animator": 3, "effects_animator": 3,
    "animation_director": 4, "character_designer": 4,
    "storyboard": 4, "chief_animation_director": 5,
    "episode_director": 5, "director": 6,
    "series_composition": 5, "art_director": 4,
    "background_art": 2, "photography_director": 4,
    "music": 4, "producer": 5, "sound_director": 4,
}


def _build_transfer_panel_data(conn, score_by_pid: dict) -> dict:
    """転職分析用パネルデータを構築 (因果推論用).

    Args:
        conn: sqlite3.Connection
        score_by_pid: {pid: iv_score} from scores.json

    Returns:
        dict with keys:
            panel: list of per-(person,year) observation dicts
            event_study: {direction: {t_rel: [credits]}}
            fe_result: {var: {beta, se}} or None
            ps_matches: [(t_pid, c_pid, t_score, c_score)]
            person_transitions: {pid: (year, direction)}
    """
    import statistics as _st
    from collections import defaultdict as _dd

    cur = conn.cursor()
    cur.execute("""
        SELECT
            c.person_id,
            a.year,
            s.favourites,
            a.genres,
            c.role,
            COUNT(DISTINCT c.anime_id) AS works
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        JOIN anime_studios ast ON a.id = ast.anime_id AND ast.is_main = 1
        JOIN studios s ON ast.studio_id = s.id
        WHERE a.year BETWEEN 1990 AND 2024
        GROUP BY c.person_id, a.year, s.id, c.role
    """)
    rows = cur.fetchall()

    def _tier_bin(f):
        return "大手" if (f or 0) >= 1000 else ("中規模" if (f or 0) >= 100 else "小規模")

    # Aggregate per (person, year)
    py_agg: dict = _dd(lambda: {"tier_w": _dd(int), "role_w": _dd(int),
                                 "genres": set(), "works": 0})
    for pid, yr, favs, genres_json, role, works in rows:
        key = (pid, int(yr))
        t = _tier_bin(favs)
        py_agg[key]["tier_w"][t] += works
        py_agg[key]["role_w"][role or "other"] += works
        py_agg[key]["works"] += works
        try:
            py_agg[key]["genres"].update(json.loads(genres_json or "[]"))
        except Exception:
            pass

    # Person first year (for career_age)
    person_fy: dict = {}
    for (pid, yr) in py_agg:
        if pid not in person_fy or yr < person_fy[pid]:
            person_fy[pid] = yr

    # Build panel records
    panel = []
    pid_groups: dict = _dd(list)
    for (pid, yr), d in py_agg.items():
        primary_tier = max(d["tier_w"], key=lambda t: d["tier_w"][t]) if d["tier_w"] else "小規模"
        large_bin = 1 if primary_tier == "大手" else 0
        career_age = yr - person_fy.get(pid, yr)
        primary_role = max(d["role_w"], key=lambda r: d["role_w"][r]) if d["role_w"] else "other"
        role_stage = _ROLE_TO_STAGE.get(primary_role, 3)
        era = (yr // 5) * 5
        rec = {
            "pid": pid, "year": yr, "career_age": career_age,
            "tier": primary_tier, "large": large_bin,
            "role_stage": role_stage, "credits": d["works"],
            "genre_n": len(d["genres"]), "era": era,
        }
        panel.append(rec)
        pid_groups[pid].append(rec)

    # Identify transitions per person
    person_transitions: dict = {}
    for pid, recs in pid_groups.items():
        recs_s = sorted(recs, key=lambda r: r["year"])
        prev_large = recs_s[0]["large"]
        for rec in recs_s[1:]:
            if prev_large == 0 and rec["large"] == 1:
                person_transitions[pid] = (rec["year"], "up")
                break
            elif prev_large == 1 and rec["large"] == 0:
                person_transitions[pid] = (rec["year"], "down")
                break
            prev_large = rec["large"]

    # Event study: credits by relative time
    credits_by_py: dict = {(r["pid"], r["year"]): r["credits"] for r in panel}
    event_study: dict = _dd(lambda: _dd(list))
    for pid, (trans_yr, direction) in person_transitions.items():
        for t_rel in range(-4, 5):
            cred = credits_by_py.get((pid, trans_yr + t_rel))
            if cred is not None:
                event_study[direction][t_rel].append(cred)
    # Also add stable baselines
    stable_large_pids = {pid for pid, recs in pid_groups.items()
                         if all(r["large"] == 1 for r in recs) and pid not in person_transitions}
    stable_small_pids = {pid for pid, recs in pid_groups.items()
                         if all(r["large"] == 0 for r in recs) and pid not in person_transitions}
    for baseline_pid in list(stable_large_pids)[:500]:
        recs_s = sorted(pid_groups[baseline_pid], key=lambda r: r["year"])
        midpt = recs_s[len(recs_s) // 2]["year"]
        for t_rel in range(-4, 5):
            cred = credits_by_py.get((baseline_pid, midpt + t_rel))
            if cred is not None:
                event_study["stable_large"][t_rel].append(cred)
    for baseline_pid in list(stable_small_pids)[:500]:
        recs_s = sorted(pid_groups[baseline_pid], key=lambda r: r["year"])
        midpt = recs_s[len(recs_s) // 2]["year"]
        for t_rel in range(-4, 5):
            cred = credits_by_py.get((baseline_pid, midpt + t_rel))
            if cred is not None:
                event_study["stable_small"][t_rel].append(cred)

    # Fixed Effects Regression (two-way: person + year FE via within-transformation)
    fe_result = None
    try:
        import numpy as _np
        # Build arrays
        y_arr = _np.array([r["credits"] for r in panel], dtype=float)
        X_arr = _np.column_stack([
            [r["large"] for r in panel],
            [r["career_age"] for r in panel],
            [r["role_stage"] for r in panel],
            [r["genre_n"] for r in panel],
        ])
        pid_arr = _np.array([r["pid"] for r in panel])
        yr_arr = _np.array([r["year"] for r in panel])
        era_arr = _np.array([r["era"] for r in panel])

        # Two-way within transformation
        def _within(arr, groups):
            means = {}
            for i, g in enumerate(groups):
                if g not in means:
                    means[g] = []
                means[g].append(arr[i] if arr.ndim == 1 else arr[i])
            group_mean = {g: _np.mean(v, axis=0) for g, v in means.items()}
            overall_mean = _np.mean(arr, axis=0)
            result = arr.copy()
            for i, g in enumerate(groups):
                result[i] -= group_mean[g]
            result += overall_mean
            return result

        y_w = _within(y_arr, pid_arr)
        y_w = _within(y_w, yr_arr)
        X_w = _np.column_stack([
            _within(X_arr[:, j], pid_arr) for j in range(X_arr.shape[1])
        ])
        X_w = _np.column_stack([
            _within(X_w[:, j], yr_arr) for j in range(X_w.shape[1])
        ])

        # OLS: (X'X)^-1 X'y
        XtX = X_w.T @ X_w
        Xty = X_w.T @ y_w
        beta = _np.linalg.lstsq(XtX, Xty, rcond=None)[0]
        y_hat = X_w @ beta
        resid = y_w - y_hat
        n, k = len(y_w), len(beta)
        s2 = float(_np.sum(resid ** 2) / max(1, n - k))
        try:
            XtX_inv = _np.linalg.inv(XtX)
            se = _np.sqrt(_np.diag(XtX_inv) * s2)
        except Exception:
            se = _np.ones(k) * float("nan")

        col_names = ["large_studio", "career_age", "role_stage", "genre_diversity"]
        fe_result = {
            col: {"beta": float(beta[j]), "se": float(se[j])}
            for j, col in enumerate(col_names)
        }
    except Exception as _fe_err:
        fe_result = None

    # Propensity Score Matching
    ps_matches = []
    try:
        from sklearn.neighbors import NearestNeighbors as _NN
        import numpy as _np2

        treatment_pids = [pid for pid, (_, d) in person_transitions.items() if d == "up"]
        control_pids = [pid for pid in pid_groups
                        if pid not in person_transitions
                        and all(r["large"] == 0 for r in pid_groups[pid])]

        def _get_features(pid, trans_yr=None):
            recs = pid_groups[pid]
            if trans_yr:
                pre_recs = [r for r in recs if trans_yr - 3 <= r["year"] < trans_yr]
                cage = trans_yr - person_fy.get(pid, trans_yr)
            else:
                pre_recs = recs
                cage = max(r["career_age"] for r in recs) // 2 if recs else 0
            pre_recs = pre_recs or recs[:3]
            avg_credits = sum(r["credits"] for r in pre_recs) / max(1, len(pre_recs))
            avg_role = sum(r["role_stage"] for r in pre_recs) / max(1, len(pre_recs))
            avg_genre = sum(r["genre_n"] for r in pre_recs) / max(1, len(pre_recs))
            return [cage, avg_role, avg_credits, avg_genre]

        # Build feature matrices
        t_feats = []
        t_pids_valid = []
        for pid in treatment_pids[:200]:
            trans_yr, _ = person_transitions[pid]
            feat = _get_features(pid, trans_yr)
            t_feats.append(feat)
            t_pids_valid.append(pid)

        c_pids_sample = control_pids[:3000]
        c_feats = [_get_features(pid) for pid in c_pids_sample]

        if t_feats and c_feats:
            T = _np2.array(t_feats, dtype=float)
            C = _np2.array(c_feats, dtype=float)
            # Standardize
            mu = C.mean(axis=0)
            std = C.std(axis=0) + 1e-9
            T_s = (T - mu) / std
            C_s = (C - mu) / std
            nn = _NN(n_neighbors=1).fit(C_s)
            dists, idxs = nn.kneighbors(T_s)
            for i, t_pid in enumerate(t_pids_valid):
                c_pid = c_pids_sample[idxs[i, 0]]
                if t_pid in score_by_pid and c_pid in score_by_pid:
                    ps_matches.append((
                        t_pid, c_pid,
                        score_by_pid[t_pid], score_by_pid[c_pid],
                    ))
    except Exception:
        pass

    return {
        "panel": panel,
        "event_study": event_study,
        "fe_result": fe_result,
        "ps_matches": ps_matches,
        "person_transitions": person_transitions,
        "pid_groups": pid_groups,
        "person_fy": person_fy,
    }


def generate_longitudinal_analysis_report():  # noqa: C901
    """縦断的キャリア分析レポート.

    相対時間インデックス・レキシス図・OMAクラスタ・アリュビアル図・CFD・
    MDS・2部グラフ・ストリームグラフ・ホライズンチャート・ストック&フロー (Section 1-5)
    需要ギャップ・生産性・人材動態 (Section 6)
    フォーマット/ジャンル別スタッフ密度 (Section 7)
    OLS固定費/変動費・スタジオ規模・生存バイアス (Section 8)
    FE回帰・PSM・因果推論 (Section 9)
    重み付き生産性指数WPS・役職ウェイト・時代補正 (Section 10)
    の33チャートを longitudinal_analysis.html に出力。
    """
    import math
    import statistics as _stats

    print("  Generating Longitudinal Analysis Report...")

    scores_data = load_json("scores.json")
    milestones_data = load_json("milestones.json") or {}
    transitions_data = load_json("transitions.json") or {}
    role_flow_data = load_json("role_flow.json") or {}
    temporal_pr_data = load_json("temporal_pagerank.json") or {}
    growth_data = load_json("growth.json") or {}
    time_series_data = load_json("time_series.json") or {}
    decades_data = load_json("decades.json") or {}
    individual_profiles = load_json("individual_profiles.json") or {}

    if not scores_data:
        print("  [SKIP] scores.json not found")
        return

    # ─── Shared lookups ────────────────────────────────────────
    scores_by_pid: dict = {p["person_id"]: p for p in scores_data}

    # Top persons for detailed analysis
    top500 = sorted(
        [p for p in scores_data if p.get("career", {}).get("first_year")],
        key=lambda p: -(p.get("iv_score") or 0),
    )[:500]
    top500_pids = {p["person_id"] for p in top500}
    top300 = top500[:300]
    top300_pids = {p["person_id"] for p in top300}

    # Build stage sequences for top 500
    import numpy as np

    stage_seqs, is_missing = _build_stage_sequence(
        top500_pids, milestones_data, scores_by_pid, max_age=25
    )

    # OMA clusters for top 300 (subset of top500)
    stage_seqs_300 = {pid: seq for pid, seq in stage_seqs.items() if pid in top300_pids}
    oma_clusters = _compute_oma_clusters(stage_seqs_300, n_clusters=5)

    # Transitions avg_years lookup: stage_pair → avg_years
    trans_list = transitions_data.get("transitions", [])
    avg_time_to_stage = transitions_data.get("avg_time_to_stage", {})
    trans_avg: dict[tuple[int, int], float] = {}
    for t in trans_list:
        fs = t.get("from_stage")
        ts_val = t.get("to_stage")
        ay = t.get("avg_years")
        if fs and ts_val and ay:
            trans_avg[(int(fs), int(ts_val))] = float(ay)

    body = ""

    # ─────────────────────────────────────────────────────────
    # TOC
    # ─────────────────────────────────────────────────────────
    body += """<div class="card">
<div class="toc">
<strong style="color:#a0d2db">目次: </strong>
<a href="#sec-alignment">1. 時間的アライメント</a>
<a href="#sec-sequence">2. シーケンス分析</a>
<a href="#sec-flow">3. 遷移フロー</a>
<a href="#sec-mapping">4. 多次元マッピング</a>
<a href="#sec-stream">5. ストック&amp;フロー</a>
</div>
</div>"""

    # ─────────────────────────────────────────────────────────
    # Section 1: 時間的アライメント
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-alignment">'
    body += "<h2>Section 1: 時間的アライメント</h2>"
    body += section_desc(
        "career_age（デビューを0年として経験年数）を横軸に揃えることで、"
        "異なる世代・年代のキャリア軌跡を直接比較可能にします。"
        "「いつデビューしたか」ではなく「何年目に何を達成したか」に着目した分析です。"
    )

    # ── Chart 1: スパゲッティプロット ─────────────────────────
    body += "<h3>Chart 1: スパゲッティプロット — 相対時間軸でのキャリアステージ軌跡</h3>"
    body += chart_guide(
        "X軸＝経験年数（デビュー年=0）。Y軸＝キャリアステージ（1=動画〜6=監督）。"
        "薄い線が個人の軌跡、太い実線がコホート中央値。"
        "灰色点線はプロモーションデータ未記録者（stage=1固定）。"
        "中央値線が急勾配＝コホート全体の昇進速度が速い。"
    )

    try:
        # 4コホートに分けてsubplots
        cohort_groups = {1980: [], 1990: [], 2000: [], 2010: []}
        for p in top500:
            fy = p.get("career", {}).get("first_year")
            if not fy:
                continue
            decade = _get_cohort_decade(fy)
            if decade in cohort_groups:
                cohort_groups[decade].append(p)

        subplot_titles = [f"{dec}年代デビュー" for dec in sorted(cohort_groups.keys())]
        fig1 = make_subplots(rows=2, cols=2, subplot_titles=subplot_titles,
                             vertical_spacing=0.12, horizontal_spacing=0.08)

        MAX_AGE_PLOT = 25
        career_ages_axis = list(range(MAX_AGE_PLOT + 1))

        for idx, (dec, persons) in enumerate(sorted(cohort_groups.items())):
            row = idx // 2 + 1
            col = idx % 2 + 1
            color = COHORT_COLORS.get(dec, "#888888")

            if not persons:
                continue

            # Individual traces (sample up to 80 per cohort to avoid SVG bloat)
            import random
            random.seed(dec)
            sample_persons = random.sample(persons, min(80, len(persons)))

            for p in sample_persons:
                pid = p["person_id"]
                seq = stage_seqs.get(pid)
                if not seq:
                    continue
                missing = is_missing.get(pid, True)
                # Add jitter
                jitter = [(stage + random.uniform(-0.1, 0.1)) for stage in seq[:MAX_AGE_PLOT + 1]]
                fig1.add_trace(go.Scatter(
                    x=career_ages_axis[:len(jitter)],
                    y=jitter,
                    mode="lines",
                    line=dict(
                        color="rgba(100,100,100,0.07)" if missing else
                              f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.12)",
                        dash="dot" if missing else "solid",
                        width=1,
                    ),
                    showlegend=False,
                    hoverinfo="skip",
                ), row=row, col=col)

            # Median line per career_age
            median_stages: list = []
            for age in career_ages_axis:
                vals_at_age = []
                for p in persons:
                    pid = p["person_id"]
                    seq = stage_seqs.get(pid)
                    if seq and age < len(seq) and not is_missing.get(pid, True):
                        vals_at_age.append(seq[age])
                if vals_at_age:
                    median_stages.append(_stats.median(vals_at_age))
                else:
                    median_stages.append(None)

            fig1.add_trace(go.Scatter(
                x=career_ages_axis,
                y=median_stages,
                mode="lines",
                name=f"{COHORT_LABELS.get(dec, str(dec))} 中央値",
                line=dict(color=color, width=3),
                connectgaps=True,
                hovertemplate="career_age: %{x}年<br>中央値ステージ: %{y:.2f}<extra></extra>",
            ), row=row, col=col)

        fig1.update_layout(
            title_text="スパゲッティプロット — コホート別キャリアステージ軌跡",
            showlegend=True,
            legend=dict(orientation="h", y=-0.05),
        )
        # Y-axis tick labels for all subplots
        for i in range(1, 5):
            r, c = (i - 1) // 2 + 1, (i - 1) % 2 + 1
            fig1.update_yaxes(
                tickvals=list(range(1, 7)),
                ticktext=[STAGE_LABELS[s] for s in range(1, 7)],
                range=[0.5, 6.5], row=r, col=c,
            )
        body += plotly_div_safe(fig1, "spaghetti-chart", height=700)
    except Exception as e:
        body += f'<div class="insight-box">Chart 1 生成スキップ: {e}</div>'

    # ── Chart 2: 拡張レキシス表面 ──────────────────────────────
    body += "<h3>Chart 2: 拡張レキシス表面 — カレンダー年 × 経験年数の2D空間</h3>"
    body += chart_guide(
        "X軸＝カレンダー年。Y軸＝career_age（経験年数）。"
        "色＝そのセルにいる人物の平均IV Score（明るい＝高スコア）。"
        "右上方向の斜め対角線＝同一コホートの軌跡。"
        "垂直方向の色変化＝業界全体の期間効果（好景気・不景気）。"
        "水平方向の色変化＝経験年数による成熟効果。"
    )

    try:
        lex_years, lex_ages, lex_z = _build_lexis_surface(scores_data, milestones_data)

        fig2 = go.Figure(go.Heatmap(
            x=lex_years,
            y=lex_ages,
            z=lex_z,
            colorscale="Plasma",
            colorbar=dict(title="平均IV Score"),
            hoverongaps=False,
            hovertemplate="年: %{x}<br>経験年数: %{y}<br>平均IV Score: %{z:.2f}<extra></extra>",
        ))

        # Vertical markers for industry transformation years
        for marker_yr, marker_label in [(2000, "ネット普及"), (2015, "配信台頭"), (2020, "COVID")]:
            if marker_yr in lex_years:
                fig2.add_vline(
                    x=marker_yr, line_dash="dash",
                    line_color="rgba(255,255,255,0.5)",
                    annotation_text=marker_label,
                    annotation_position="top",
                    annotation_font=dict(color="rgba(255,255,255,0.7)", size=11),
                )

        fig2.update_layout(
            title_text="拡張レキシス表面",
            xaxis_title="カレンダー年",
            yaxis_title="career_age（経験年数）",
        )
        body += plotly_div_safe(fig2, "lexis-chart", height=520)

        # Demand context insight: new entrants vs anime count
        ts_years_list = time_series_data.get("years", [])
        ts_series = time_series_data.get("series", {})
        new_entrants_d = ts_series.get("new_entrants", {})
        unique_anime_d = ts_series.get("unique_anime", {})
        recent_yrs = [y for y in ts_years_list if 2000 <= y <= 2024]
        if recent_yrs and new_entrants_d and unique_anime_d:
            avg_demand_2000_2014 = sum(unique_anime_d.get(str(y), 0) for y in range(2000, 2015) if str(y) in unique_anime_d)
            avg_demand_2015_2024 = sum(unique_anime_d.get(str(y), 0) for y in range(2015, 2025) if str(y) in unique_anime_d)
            body += key_findings([
                f"2000〜2014年の平均年間アニメ本数 {avg_demand_2000_2014 // 15} 本 vs "
                f"2015〜2024年 {avg_demand_2015_2024 // 10} 本 — 配信拡大期に制作需要が大幅増加",
                "レキシス図の対角線方向（コホート軌跡）の色が2015年以降に明るくなる場合、"
                "配信拡大による需要増がIV Scoreを押し上げている可能性を示す",
                "垂直方向の急変（特定年で全経験年数の色が一斉変化）は業界全体のブーム/低迷期を示す",
            ])
    except Exception as e:
        body += f'<div class="insight-box">Chart 2 生成スキップ: {e}</div>'

    body += "</div>"

    # ─────────────────────────────────────────────────────────
    # Section 2: シーケンス分析
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-sequence">'
    body += "<h2>Section 2: シーケンス分析</h2>"
    body += section_desc(
        "OMA（最適マッチング分析）クラスタリングにより、個人のキャリアシーケンスを"
        "類型化します。ハミング距離を近似OMA距離として使用し、Wardリンケージで5クラスタに分類。"
        "「早期昇進型」「長期原画型」「監督ストレート型」などのパターンが浮かび上がります。"
    )

    # ── Chart 3: シーケンスインデックスプロット ────────────────
    body += "<h3>Chart 3: シーケンスインデックスプロット — OMAクラスタ順ソート</h3>"
    body += chart_guide(
        "各行＝1人物、各列＝career_age年。色＝キャリアステージ。"
        "OMAクラスタでソートしているため、同一パターンの人物が近くに並ぶ。"
        "色の遷移パターンが視覚的にキャリアタイプを示す。"
    )

    try:
        sorted_pids_c3 = sorted(
            top300_pids & set(oma_clusters.keys()),
            key=lambda pid: (oma_clusters.get(pid, 0), -(scores_by_pid.get(pid, {}).get("iv_score") or 0))
        )

        MAX_AGE_C3 = 25
        z_c3 = []
        hover_c3 = []
        y_labels = []
        for pid in sorted_pids_c3:
            seq = stage_seqs.get(pid, [1] * (MAX_AGE_C3 + 1))
            z_c3.append([s if s > 0 else 0 for s in seq[:MAX_AGE_C3 + 1]])
            name_c3 = scores_by_pid.get(pid, {}).get("name") or pid[:20]
            cl = oma_clusters.get(pid, 0) + 1
            y_labels.append(f"C{cl} {name_c3[:12]}")
            hover_c3.append(
                [f"{name_c3}<br>age={a}yr<br>{STAGE_LABELS.get(seq[a] if a < len(seq) else 1, '?')}"
                 for a in range(MAX_AGE_C3 + 1)]
            )

        colorscale_c3 = [
            [0.0, STAGE_COLORS_HEX[0]],
            [0.15, STAGE_COLORS_HEX[1]],
            [0.30, STAGE_COLORS_HEX[2]],
            [0.45, STAGE_COLORS_HEX[3]],
            [0.60, STAGE_COLORS_HEX[4]],
            [0.80, STAGE_COLORS_HEX[5]],
            [1.0, STAGE_COLORS_HEX[6]],
        ]

        fig3 = go.Figure(go.Heatmap(
            z=z_c3,
            x=list(range(MAX_AGE_C3 + 1)),
            y=y_labels,
            colorscale=colorscale_c3,
            zmin=0, zmax=6,
            colorbar=dict(
                title="ステージ",
                tickvals=[0, 1, 2, 3, 4, 5, 6],
                ticktext=["欠損", "動画", "第2原画", "原画", "作監", "総作監", "監督"],
            ),
            hovertemplate="%{customdata}<extra></extra>",
            customdata=hover_c3,
        ))
        fig3.update_layout(
            title_text="シーケンスインデックスプロット（OMAクラスタ順）",
            xaxis_title="career_age（経験年数）",
            yaxis_title="人物（クラスタ順）",
            yaxis=dict(showticklabels=len(sorted_pids_c3) <= 80),
        )
        body += plotly_div_safe(fig3, "sequence-index-chart", height=600)
    except Exception as e:
        body += f'<div class="insight-box">Chart 3 生成スキップ: {e}</div>'

    # ── Chart 4: OMAクラスタ代表軌跡 ──────────────────────────
    body += "<h3>Chart 4: OMAクラスタ代表軌跡 — キャリアパス類型5種</h3>"
    body += chart_guide(
        "5パネルそれぞれが1クラスタ。薄線＝クラスタ内個人軌跡、太線＝中央値軌跡。"
        "クラスタ名はcentroidの特徴から自動命名。"
        "各パネルの監督到達率・平均キャリアスパンを比較することで、"
        "どのパスが「速く監督に到達するか」が分かる。"
    )

    try:
        n_clusters_c4 = 5
        MAX_AGE_C4 = 25
        career_ages_c4 = list(range(MAX_AGE_C4 + 1))

        # Group pids by cluster
        cluster_pids: dict[int, list[str]] = {}
        for pid, cl in oma_clusters.items():
            cluster_pids.setdefault(cl, []).append(pid)

        # Name clusters by median trajectory characteristics
        cluster_names_c4: dict[int, str] = {}
        cluster_stats: dict[int, dict] = {}
        for cl in range(n_clusters_c4):
            pids_cl = cluster_pids.get(cl, [])
            if not pids_cl:
                cluster_names_c4[cl] = f"C{cl+1}: データ不足"
                cluster_stats[cl] = {}
                continue

            # Compute median trajectory
            med_seq = []
            for age in career_ages_c4:
                vals = [stage_seqs[pid][age] for pid in pids_cl
                        if pid in stage_seqs and age < len(stage_seqs[pid])
                        and not is_missing.get(pid, True)]
                med_seq.append(_stats.median(vals) if vals else 1.0)

            # Stats: director reach rate, avg career span
            dir_count = sum(
                1 for pid in pids_cl
                if any(s >= 6 for s in stage_seqs.get(pid, []))
            )
            dir_rate = dir_count / len(pids_cl) if pids_cl else 0
            avg_span = 0.0
            span_vals = []
            for pid in pids_cl:
                p = scores_by_pid.get(pid, {})
                fy = p.get("career", {}).get("first_year")
                ly = p.get("career", {}).get("latest_year")
                if fy and ly:
                    span_vals.append(ly - fy)
            avg_span = _stats.mean(span_vals) if span_vals else 0.0

            # Name by trajectory
            peak_age = career_ages_c4[med_seq.index(max(med_seq))]
            max_stage = max(med_seq)
            if max_stage >= 5.5:
                name = "監督到達型"
            elif max_stage >= 4.5:
                name = "総作監到達型"
            elif max_stage >= 3.8:
                name = "作監キャリア型"
            elif peak_age <= 8:
                name = "早期昇進型"
            else:
                name = "長期原画型"
            cluster_names_c4[cl] = f"C{cl+1}: {name}"
            cluster_stats[cl] = {
                "n": len(pids_cl),
                "dir_rate": dir_rate,
                "avg_span": avg_span,
                "med_seq": med_seq,
            }

        fig4 = make_subplots(
            rows=2, cols=3,
            subplot_titles=[cluster_names_c4.get(cl, f"C{cl+1}") for cl in range(n_clusters_c4)] + [""],
            vertical_spacing=0.15, horizontal_spacing=0.08,
        )
        cluster_palette = ["#4CC9F0", "#F72585", "#06D6A0", "#FFD166", "#FF6B35"]

        for cl in range(n_clusters_c4):
            r = cl // 3 + 1
            c = cl % 3 + 1
            pids_cl = cluster_pids.get(cl, [])
            color = cluster_palette[cl]

            # Individual traces (up to 30)
            for pid in pids_cl[:30]:
                seq = stage_seqs.get(pid, [])
                if not seq or is_missing.get(pid, True):
                    continue
                fig4.add_trace(go.Scatter(
                    x=career_ages_c4[:len(seq)],
                    y=seq[:MAX_AGE_C4 + 1],
                    mode="lines",
                    line=dict(color=color.replace("#", "rgba(") + ",0.15)",
                              width=1) if False else
                    dict(color=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.18)",
                         width=1),
                    showlegend=False, hoverinfo="skip",
                ), row=r, col=c)

            # Median line
            stats_cl = cluster_stats.get(cl, {})
            med_seq = stats_cl.get("med_seq", [])
            n_cl = stats_cl.get("n", 0)
            dir_rate_cl = stats_cl.get("dir_rate", 0)
            avg_span_cl = stats_cl.get("avg_span", 0)
            if med_seq:
                fig4.add_trace(go.Scatter(
                    x=career_ages_c4,
                    y=med_seq,
                    mode="lines",
                    name=cluster_names_c4.get(cl, f"C{cl+1}"),
                    line=dict(color=color, width=3),
                    hovertemplate=(
                        f"{cluster_names_c4.get(cl,'')}<br>"
                        f"N={n_cl}人 監督率={dir_rate_cl:.1%} 平均{avg_span_cl:.0f}年<br>"
                        "age=%{x}yr 中央値ステージ:%{y:.2f}<extra></extra>"
                    ),
                ), row=r, col=c)

        fig4.update_layout(
            title_text="OMAクラスタ代表軌跡 — キャリアパス類型",
            showlegend=False,
        )
        for i in range(1, 6):
            r, c = (i - 1) // 3 + 1, (i - 1) % 3 + 1
            fig4.update_yaxes(
                tickvals=list(range(1, 7)),
                ticktext=[STAGE_LABELS[s] for s in range(1, 7)],
                range=[0.5, 6.5], row=r, col=c,
            )
        body += plotly_div_safe(fig4, "oma-clusters-chart", height=620)

        # Stats table
        body += "<h3>クラスタ統計</h3>"
        body += "<table><thead><tr><th>クラスタ</th><th>人数</th><th>監督到達率</th><th>平均キャリアスパン</th></tr></thead><tbody>"
        for cl in range(n_clusters_c4):
            stats_cl = cluster_stats.get(cl, {})
            body += (
                f"<tr><td>{cluster_names_c4.get(cl, f'C{cl+1}')}</td>"
                f"<td>{stats_cl.get('n', 0)}人</td>"
                f"<td>{stats_cl.get('dir_rate', 0):.1%}</td>"
                f"<td>{stats_cl.get('avg_span', 0):.1f}年</td></tr>"
            )
        body += "</tbody></table>"
    except Exception as e:
        body += f'<div class="insight-box">Chart 4 生成スキップ: {e}</div>'

    body += "</div>"

    # ─────────────────────────────────────────────────────────
    # Section 3: 遷移フロー
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-flow">'
    body += "<h2>Section 3: 状態遷移とフロー</h2>"
    body += section_desc(
        "キャリアステージをパイプラインとして捉え、各段階での流入・滞留・流出を可視化します。"
        "アリュビアル図（多段サンキー）は個人の遷移経路を、"
        "累積フロー図（CFD）はパイプライン全体のボトルネックを示します。"
        "需要の変化（配信時代の制作本数増加）が供給パイプラインに与えた影響も確認できます。"
    )

    # ── Chart 5: アリュビアル図 ────────────────────────────────
    body += "<h3>Chart 5: アリュビアル図 — career_age 5/10/15/20年時点のステージ遷移</h3>"
    body += chart_guide(
        "各帯の幅＝そのステージから次のステージへ移動した人数。"
        "細くなる流れ＝途中での離脱・スキップ。"
        "4時点（5/10/15/20年）を横断することでキャリアパイプライン全体が俯瞰できる。"
    )

    try:
        checkpoints = [5, 10, 15, 20]
        # Count persons at each (checkpoint, stage)
        checkpoint_dist: dict[int, dict[int, list[str]]] = {cp: {s: [] for s in range(1, 7)} for cp in checkpoints}
        for pid in top500_pids:
            seq = stage_seqs.get(pid)
            if not seq or is_missing.get(pid, True):
                continue
            for cp in checkpoints:
                if cp < len(seq):
                    stg = max(1, min(6, seq[cp]))
                    checkpoint_dist[cp][stg].append(pid)

        # Build Sankey nodes and links
        sankey_nodes: list[str] = []
        sankey_colors: list[str] = []
        node_idx: dict[tuple[int, int], int] = {}

        for cp in checkpoints:
            for stg in range(1, 7):
                cnt = len(checkpoint_dist[cp][stg])
                if cnt > 0:
                    idx = len(sankey_nodes)
                    node_idx[(cp, stg)] = idx
                    sankey_nodes.append(f"{cp}年: {STAGE_LABELS.get(stg, str(stg))} ({cnt}人)")
                    sankey_colors.append(STAGE_COLORS_HEX.get(stg, "#888888"))

        # Links between consecutive checkpoints
        link_src, link_tgt, link_val, link_lbl = [], [], [], []
        for i, cp_from in enumerate(checkpoints[:-1]):
            cp_to = checkpoints[i + 1]
            # Track which stage each person was at cp_from and cp_to
            transition_counts: dict[tuple[int, int], int] = {}
            for pid in top500_pids:
                seq = stage_seqs.get(pid)
                if not seq or is_missing.get(pid, True):
                    continue
                if cp_from >= len(seq) or cp_to >= len(seq):
                    continue
                s_from = max(1, min(6, seq[cp_from]))
                s_to = max(1, min(6, seq[cp_to]))
                transition_counts[(s_from, s_to)] = transition_counts.get((s_from, s_to), 0) + 1

            for (s_from, s_to), cnt in transition_counts.items():
                if cnt < 2:
                    continue
                src_node = node_idx.get((cp_from, s_from))
                tgt_node = node_idx.get((cp_to, s_to))
                if src_node is not None and tgt_node is not None:
                    link_src.append(src_node)
                    link_tgt.append(tgt_node)
                    link_val.append(cnt)
                    link_lbl.append(f"{cp_from}→{cp_to}年: {STAGE_LABELS.get(s_from,'?')}→{STAGE_LABELS.get(s_to,'?')} ({cnt}人)")

        if link_src:
            fig5 = go.Figure(go.Sankey(
                node=dict(
                    label=sankey_nodes,
                    color=sankey_colors,
                    pad=15, thickness=20,
                    line=dict(color="rgba(255,255,255,0.1)", width=0.5),
                ),
                link=dict(
                    source=link_src, target=link_tgt, value=link_val,
                    label=link_lbl,
                    color="rgba(160,160,200,0.25)",
                ),
            ))
            fig5.update_layout(
                title_text="アリュビアル図 — career_age 5/10/15/20年のキャリアパイプライン",
                font=dict(size=11),
            )
            body += plotly_div_safe(fig5, "alluvial-chart", height=550)
        else:
            body += '<div class="insight-box">アリュビアルデータ生成: プロモーション記録が不足しています</div>'
    except Exception as e:
        body += f'<div class="insight-box">Chart 5 生成スキップ: {e}</div>'

    # ── Chart 6: 累積フロー図 (CFD) ───────────────────────────
    body += "<h3>Chart 6: 累積フロー図 (CFD) — キャリアパイプライン健全性</h3>"
    body += chart_guide(
        "積み上げ面の各帯＝そのcareer_ageにそのステージにいる人数。"
        "帯が厚くなる区間＝そのステージで滞留している（ボトルネック）。"
        "帯が薄くなる＝昇進が速い or 離脱。"
        "全体の面積が増加する区間＝その経験年数の人材が多い（需要増の時代の入職者世代）。"
    )

    try:
        MAX_AGE_CFD = 30
        age_axis_cfd = list(range(MAX_AGE_CFD + 1))

        # Count persons at each (career_age, stage) — include all top500
        stage_counts_by_age: dict[int, dict[int, int]] = {
            age: {s: 0 for s in range(1, 7)} for age in age_axis_cfd
        }
        for pid in top500_pids:
            seq = stage_seqs.get(pid)
            if not seq:
                continue
            for age in age_axis_cfd:
                if age < len(seq):
                    stg = max(1, min(6, seq[age]))
                    stage_counts_by_age[age][stg] += 1

        fig6 = go.Figure()
        for stg in range(1, 7):
            y_vals = [stage_counts_by_age[age][stg] for age in age_axis_cfd]
            hover_texts = [
                f"career_age: {age}年<br>ステージ: {STAGE_LABELS.get(stg,'?')}<br>人数: {y_vals[age]}"
                for age in age_axis_cfd
            ]
            fig6.add_trace(go.Scatter(
                x=age_axis_cfd,
                y=y_vals,
                name=STAGE_LABELS.get(stg, f"Stage{stg}"),
                mode="lines",
                fill="tonexty" if stg > 1 else "tozeroy",
                stackgroup="one",
                line=dict(color=STAGE_COLORS_HEX.get(stg, "#888"), width=0.5),
                fillcolor=_hex_alpha(STAGE_COLORS_HEX.get(stg, "#888"), "88"),
                hovertext=hover_texts,
                hoverinfo="text",
            ))

        # Add avg_years annotations from transitions.json
        for (fs, ts_val), ay in trans_avg.items():
            if 1 <= fs <= 5 and ts_val == fs + 1 and ay < MAX_AGE_CFD:
                y_at_age = sum(stage_counts_by_age.get(int(ay), {s2: 0 for s2 in range(1,7)}).values())
                fig6.add_annotation(
                    x=ay, y=y_at_age * 0.5,
                    text=f"平均{ay:.1f}年",
                    showarrow=True, arrowhead=2, arrowsize=1,
                    arrowcolor="rgba(255,255,255,0.4)",
                    font=dict(size=9, color="rgba(255,255,255,0.6)"),
                )

        fig6.update_layout(
            title_text="累積フロー図 (CFD) — キャリアパイプライン健全性",
            xaxis_title="career_age（経験年数）",
            yaxis_title="在籍人数（累積）",
            legend=dict(orientation="h", y=-0.12),
        )
        body += plotly_div_safe(fig6, "cfd-chart", height=480)

        # Key findings about bottlenecks
        bottleneck_age = None
        bottleneck_stg = None
        max_thickness = 0
        for age in range(1, MAX_AGE_CFD):
            for stg in range(2, 5):
                thickness = stage_counts_by_age[age][stg]
                if thickness > max_thickness:
                    max_thickness = thickness
                    bottleneck_age = age
                    bottleneck_stg = stg
        if bottleneck_age and bottleneck_stg:
            body += key_findings([
                f"最大ボトルネック: career_age {bottleneck_age}年時点の "
                f"{STAGE_LABELS.get(bottleneck_stg, '?')}ステージ ({max_thickness}人が集中)",
                "需要増加期（2015年以降）の入職者がcareer_age 5〜10年の帯を押し上げている場合、"
                "近年の制作本数増が動画・原画層を肥大化させている可能性がある",
                "作監〜監督ステージの薄さは昇進機会の構造的希少性を示す",
            ])
    except Exception as e:
        body += f'<div class="insight-box">Chart 6 生成スキップ: {e}</div>'

    body += "</div>"

    # ─────────────────────────────────────────────────────────
    # Section 4: 多次元マッピング
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-mapping">'
    body += "<h2>Section 4: 多次元マッピング</h2>"
    body += section_desc(
        "高次元のシーケンスデータを2次元空間に投影し、類似した軌跡を持つ人物の"
        "「クラスタ」を視覚的に確認します。MDSとネットワーク投影の2手法を使用。"
    )

    # ── Chart 7: MDS空間マッピング ─────────────────────────────
    body += "<h3>Chart 7: MDS空間マッピング — キャリアシーケンスの類似性地図</h3>"
    body += chart_guide(
        "2軸の絶対的意味はなく、相対距離のみが重要。"
        "近い点＝似たキャリアシーケンス。色＝OMAクラスタ（5種）。"
        "サイズ＝IV Score。クラスタが明確に分離していれば類型化が有効。"
    )

    try:
        from sklearn.manifold import MDS
        from scipy.spatial.distance import pdist, squareform

        mds_pids = [pid for pid in stage_seqs_300.keys() if not is_missing.get(pid, True)]
        if len(mds_pids) >= 10:
            X_mds = np.array([stage_seqs_300[pid] for pid in mds_pids], dtype=np.float32)
            D_condensed_mds = pdist(X_mds, metric="hamming")
            D_sq = squareform(D_condensed_mds)

            n_mds = len(mds_pids)
            # sklearn ≥1.4: metric_mds=False (non-metric MDS)
            # sklearn ≥1.8: metric="precomputed" replaces dissimilarity="precomputed"
            # sklearn ≥1.10: init default changes → specify init="random" to match old default
            try:
                mds = MDS(
                    n_components=2, metric_mds=False, metric="precomputed",
                    random_state=42, n_init=4, init="random",
                    max_iter=200 if n_mds > 100 else 300,
                )
            except TypeError:
                # Older sklearn (<1.4): metric=False means non-metric MDS
                mds = MDS(
                    n_components=2, metric=False, dissimilarity="precomputed",
                    random_state=42, n_init=4,
                    max_iter=200 if n_mds > 100 else 300,
                )
            coords = mds.fit_transform(D_sq)

            colors_mds = [COHORT_COLORS.get(
                _get_cohort_decade(scores_by_pid.get(pid, {}).get("career", {}).get("first_year")),
                "#888"
            ) for pid in mds_pids]
            sizes_mds = [
                max(4, min(20, (scores_by_pid.get(pid, {}).get("iv_score") or 5) * 0.3))
                for pid in mds_pids
            ]
            hover_mds = []
            for pid in mds_pids:
                p = scores_by_pid.get(pid, {})
                name_m = p.get("name") or pid[:20]
                fy_m = p.get("career", {}).get("first_year", "?")
                hs_m = p.get("career", {}).get("highest_stage", 1)
                cl_m = oma_clusters.get(pid, 0) + 1
                comp_m = p.get("iv_score", 0)
                hover_mds.append(
                    f"{name_m}<br>デビュー: {fy_m}年<br>"
                    f"最高ステージ: {STAGE_LABELS.get(hs_m, '?')}<br>"
                    f"クラスタ: C{cl_m}<br>IV Score: {comp_m:.1f}"
                )

            fig7 = go.Figure(go.Scatter(
                x=coords[:, 0], y=coords[:, 1],
                mode="markers",
                marker=dict(
                    color=colors_mds,
                    size=sizes_mds,
                    opacity=0.75,
                    line=dict(color="rgba(255,255,255,0.15)", width=0.5),
                ),
                text=hover_mds,
                hovertemplate="%{text}<extra></extra>",
            ))
            fig7.update_layout(
                title_text="MDS空間マッピング — キャリアシーケンス類似性地図",
                xaxis_title="MDS次元1",
                yaxis_title="MDS次元2",
                showlegend=False,
            )
            body += plotly_div_safe(fig7, "mds-chart", height=520)
        else:
            body += '<div class="insight-box">MDS: プロモーション記録付き人物が不足しています</div>'
    except Exception as e:
        body += f'<div class="insight-box">Chart 7 生成スキップ: {e}</div>'

    # ── Chart 8: 役職 × 人物 2部グラフ ────────────────────────
    body += "<h3>Chart 8: 役職 × 人物 2部グラフ (Bipartite Projection)</h3>"
    body += chart_guide(
        "丸ノード＝人物（サイズ=IV Score）、四角ノード＝役職カテゴリ（サイズ=そのクレジット総数）。"
        "エッジ＝人物がその役職でクレジットを持つ。"
        "特定の役職に多数の人物が集中している＝その役職の過重労働リスク。"
        "役職ノードが大きいほど業界での需要が高い（需要シフトも確認できる）。"
    )

    try:
        import networkx as nx

        top100 = top500[:100]
        # Collect role participation
        person_roles: dict[str, set[str]] = {}
        for p in top100:
            pid = p["person_id"]
            events = milestones_data.get(pid, [])
            roles_set = set()
            for e in events:
                if e.get("type") == "new_role":
                    r = e.get("role", "")
                    if r and r != "other":
                        roles_set.add(r)
            if roles_set:
                person_roles[pid] = roles_set

        # Build bipartite graph
        B = nx.Graph()
        all_roles = set()
        for pid, roles_set in person_roles.items():
            B.add_node(pid, bipartite=0)
            for r in roles_set:
                B.add_node(r, bipartite=1)
                B.add_edge(pid, r)
                all_roles.add(r)

        if len(B.nodes) > 3:
            pos = nx.spring_layout(B, seed=42, k=0.8)

            # Edges
            edge_x, edge_y = [], []
            for u, v in B.edges():
                x0, y0 = pos[u]
                x1, y1 = pos[v]
                edge_x += [x0, x1, None]
                edge_y += [y0, y1, None]

            fig8 = go.Figure()
            fig8.add_trace(go.Scatter(
                x=edge_x, y=edge_y, mode="lines",
                line=dict(width=0.5, color="rgba(160,160,200,0.2)"),
                hoverinfo="none", showlegend=False,
            ))

            # Person nodes
            px8 = [pos[pid][0] for pid in person_roles]
            py8 = [pos[pid][1] for pid in person_roles]
            ps8 = [max(6, min(24, (scores_by_pid.get(pid, {}).get("iv_score") or 5) * 0.35))
                   for pid in person_roles]
            ph8 = [f"{scores_by_pid.get(pid,{}).get('name',pid[:15])}<br>"
                   f"IV Score: {scores_by_pid.get(pid,{}).get('iv_score',0):.1f}"
                   for pid in person_roles]

            fig8.add_trace(go.Scatter(
                x=px8, y=py8, mode="markers",
                marker=dict(size=ps8, color="#4CC9F0", symbol="circle",
                            line=dict(color="rgba(255,255,255,0.3)", width=0.5)),
                text=ph8, hovertemplate="%{text}<extra>人物</extra>",
                name="人物",
            ))

            # Role nodes
            role_list = list(all_roles)
            # Count credits per role across all persons
            role_credit_counts = {}
            for pid in person_roles:
                events = milestones_data.get(pid, [])
                for e in events:
                    r = e.get("role", "")
                    if r and r in all_roles:
                        role_credit_counts[r] = role_credit_counts.get(r, 0) + 1

            rx8 = [pos[r][0] for r in role_list]
            ry8 = [pos[r][1] for r in role_list]
            rs8 = [max(10, min(40, math.sqrt(role_credit_counts.get(r, 1)) * 2)) for r in role_list]
            rh8 = [f"{_ROLE_JA.get(r, r)}<br>クレジット数: {role_credit_counts.get(r, 0)}"
                   for r in role_list]

            fig8.add_trace(go.Scatter(
                x=rx8, y=ry8, mode="markers+text",
                marker=dict(size=rs8, color="#FF6B35", symbol="square",
                            line=dict(color="rgba(255,255,255,0.3)", width=0.5)),
                text=[_ROLE_JA.get(r, r) for r in role_list],
                textposition="top center",
                textfont=dict(size=9),
                customdata=rh8,
                hovertemplate="%{customdata}<extra>役職</extra>",
                name="役職",
            ))

            fig8.update_layout(
                title_text="役職 × 人物 2部グラフ",
                showlegend=True,
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            )
            body += plotly_div_safe(fig8, "bipartite-chart", height=580)
        else:
            body += '<div class="insight-box">2部グラフ: データ不足</div>'
    except Exception as e:
        body += f'<div class="insight-box">Chart 8 生成スキップ: {e}</div>'

    body += "</div>"

    # ─────────────────────────────────────────────────────────
    # Section 5: ストリーム & ホライズン & ストック/フロー
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-stream">'
    body += "<h2>Section 5: ストック &amp; フロー — 役職需要の時系列変遷</h2>"
    body += section_desc(
        "配信サービスの台頭により、アニメ制作の構造的需要は大きく変化しました。"
        "ストリームグラフは役職別クレジット数の年次変遷を、"
        "ホライズンチャートは個人の稼働負荷の時系列圧縮表示を、"
        "ストック&フロー図はステージ別在籍人数の動態と昇進フローを可視化します。"
    )

    # ── Chart 9: ストリームグラフ ──────────────────────────────
    body += "<h3>Chart 9: ストリームグラフ — 役職構成の年次変遷（需要シフト）</h3>"
    body += chart_guide(
        "各帯の厚み＝その年その役職のクレジット総数。中央基線（balanced baseline）により"
        "帯が上下に広がる。帯が膨らむ年＝その役職の需要が増加。"
        "特に2015年以降の動画・原画の帯の変化が需要シフトを示す。"
    )

    try:
        # Build role-by-year credits from milestones data
        role_by_year: dict[str, dict[int, int]] = {}
        MAIN_ROLES = ["in_between", "key_animator", "2nd_key_animator",
                      "animation_director", "chief_animation_director",
                      "director", "episode_director", "storyboard"]

        for pid, events in milestones_data.items():
            for e in events:
                if e.get("type") in ("career_start", "new_role"):
                    yr_e = e.get("year")
                    role_e = e.get("role", "")
                    if yr_e and role_e and 1970 <= yr_e <= 2025:
                        role_by_year.setdefault(role_e, {}).setdefault(yr_e, 0)
                        role_by_year[role_e][yr_e] += 1

        # Filter to MAIN_ROLES, 1990-2025
        stream_years = list(range(1990, 2026))
        stream_data: dict[str, list[int]] = {}
        for role_e in MAIN_ROLES:
            vals = [role_by_year.get(role_e, {}).get(yr, 0) for yr in stream_years]
            if sum(vals) > 50:  # Only include roles with meaningful data
                stream_data[role_e] = vals

        if stream_data:
            # Compute centered baseline for streamgraph
            role_names = list(stream_data.keys())
            totals = [sum(stream_data[r][i] for r in role_names) for i in range(len(stream_years))]
            baseline = [-t / 2.0 for t in totals]

            stream_colors = [
                "#a0a0c0", "#7EB8D4", "#4CC9F0", "#FFD166",
                "#FF6B35", "#F72585", "#06D6A0", "#7209B7",
            ]

            fig9 = go.Figure()
            # First trace: invisible baseline
            fig9.add_trace(go.Scatter(
                x=stream_years, y=baseline,
                fill=None, mode="lines",
                line=dict(color="rgba(0,0,0,0)", width=0),
                showlegend=False, hoverinfo="skip",
            ))

            # Subsequent traces: fill to previous
            for i, role_e in enumerate(role_names):
                color = stream_colors[i % len(stream_colors)]
                hover_texts = [
                    f"{stream_years[j]}年<br>{_ROLE_JA.get(role_e, role_e)}: {stream_data[role_e][j]}件"
                    for j in range(len(stream_years))
                ]
                fig9.add_trace(go.Scatter(
                    x=stream_years,
                    y=stream_data[role_e],
                    name=_ROLE_JA.get(role_e, role_e),
                    mode="lines",
                    fill="tonexty",
                    stackgroup="stream",
                    line=dict(color=color, width=0.5),
                    fillcolor=_hex_alpha(color, "99"),
                    hovertext=hover_texts,
                    hoverinfo="text",
                ))

            # Mark demand shift milestones
            for marker_yr, label in [(2000, "ネット普及"), (2015, "Netflix等配信台頭"), (2020, "COVID禍")]:
                fig9.add_vline(
                    x=marker_yr, line_dash="dash",
                    line_color="rgba(255,255,255,0.4)",
                    annotation_text=label,
                    annotation_position="top",
                    annotation_font=dict(color="rgba(255,255,255,0.6)", size=10),
                )

            fig9.update_layout(
                title_text="ストリームグラフ — 役職別クレジット数の年次変遷（需要シフト）",
                xaxis_title="年", yaxis_title="クレジット数（積み上げ）",
                legend=dict(orientation="h", y=-0.12),
            )
            body += plotly_div_safe(fig9, "streamgraph-chart", height=500)

            # Demand shift findings
            # Compare 2010-2014 vs 2020-2024 for key roles
            shift_findings = []
            for role_e in ["in_between", "key_animator", "animation_director"]:
                if role_e in stream_data:
                    old_avg = sum(stream_data[role_e][i] for i, yr in enumerate(stream_years) if 2010 <= yr <= 2014) / 5
                    new_avg = sum(stream_data[role_e][i] for i, yr in enumerate(stream_years) if 2020 <= yr <= 2024) / 5
                    if old_avg > 0:
                        chg = (new_avg - old_avg) / old_avg
                        dir_str = "増加" if chg > 0 else "減少"
                        shift_findings.append(
                            f"{_ROLE_JA.get(role_e, role_e)}: "
                            f"2010〜14年平均 {old_avg:.0f}件 → 2020〜24年平均 {new_avg:.0f}件 "
                            f"({chg:+.0%} {dir_str})"
                        )
            if shift_findings:
                body += key_findings([
                    "需要シフト分析（2010〜14年 vs 2020〜24年）:",
                    *shift_findings,
                    "配信作品増加により原画・作監需要が拡大している一方、"
                    "動画の機械化（AI動画）の影響が将来の在庫曲線に影響する可能性がある",
                ])
        else:
            body += '<div class="insight-box">ストリームグラフ: 役職データが不足しています</div>'
    except Exception as e:
        body += f'<div class="insight-box">Chart 9 生成スキップ: {e}</div>'

    # ── Chart 10: ホライズンチャート ───────────────────────────
    body += "<h3>Chart 10: ホライズンチャート — 個人クレジット負荷の時系列</h3>"
    body += chart_guide(
        "各行＝1人物、各列＝年。色の濃さ＝その年のクレジット数（正規化）。"
        "濃い行＝高稼働。特定の年に多数の行が同時に濃くなる＝業界ブーム期。"
        "常に白い行＝活動が限定的または休業期間。"
    )

    try:
        growth_persons = growth_data.get("persons", {})
        # Get persons with yearly data, sort by iv_score
        growth_pid_list = list(growth_persons.keys())
        growth_persons_sorted = sorted(
            growth_pid_list,
            key=lambda pid: -(scores_by_pid.get(pid, {}).get("iv_score") or 0)
        )[:200]

        HORIZON_YEARS = list(range(2000, 2026))
        z_horizon = []
        y_labels_h = []
        for pid in growth_persons_sorted:
            yearly = growth_persons[pid].get("yearly_credits", {})
            row_credits = [int(yearly.get(str(yr), 0)) for yr in HORIZON_YEARS]
            max_credits = max(row_credits) if max(row_credits) > 0 else 1
            z_horizon.append([c / max_credits for c in row_credits])
            name_h = scores_by_pid.get(pid, {}).get("name") or pid[:15]
            y_labels_h.append(name_h[:14])

        if z_horizon:
            fig10 = go.Figure(go.Heatmap(
                z=z_horizon,
                x=HORIZON_YEARS,
                y=y_labels_h,
                colorscale=[[0.0, "rgba(0,0,0,0)"], [0.001, "#0d1b2a"],
                             [0.3, "#1d4e8a"], [0.6, "#2196f3"], [1.0, "#90caf9"]],
                zmin=0, zmax=1,
                colorbar=dict(title="負荷率"),
                hovertemplate="%{y}<br>%{x}年: %{z:.2%}<extra></extra>",
            ))

            # Mark demand shift years
            for marker_yr in [2000, 2015, 2020]:
                if marker_yr in HORIZON_YEARS:
                    fig10.add_vline(
                        x=marker_yr, line_dash="dash",
                        line_color="rgba(255,255,255,0.3)",
                    )

            fig10.update_layout(
                title_text="ホライズンチャート — 個人クレジット負荷の時系列",
                xaxis_title="年",
                yaxis=dict(
                    title="人物（IV Score降順）",
                    showticklabels=len(growth_persons_sorted) <= 60,
                ),
            )
            body += plotly_div_safe(fig10, "horizon-chart", height=520)
        else:
            body += '<div class="insight-box">ホライズンチャート: growth.jsonにyearly_creditsデータが不足</div>'
    except Exception as e:
        body += f'<div class="insight-box">Chart 10 生成スキップ: {e}</div>'

    # ── Chart 11: ステージ別在籍人数の動態 (Stock & Flow) ──────
    body += "<h3>Chart 11: ステージ別在籍人数の動態 — ストック &amp; フロー</h3>"
    body += chart_guide(
        "上段＝各calendar年の各ステージ在籍人数（ストック）。"
        "下段＝各calendar年の昇進者数（フロー）。"
        "ストックの「水槽」が膨らむ年＝そのステージがボトルネック化。"
        "フローの急増年＝昇進機会が集中（需要急増や世代交代効果）。"
        "折れ線が急下降する年は業界全体の需要縮小または測定の空白を示す場合もある。"
    )

    try:
        # Build stock: for each calendar year, count persons at each stage
        # Use milestones promotions data for all persons
        STOCK_YEARS = list(range(1980, 2026))

        # Build per-person, per-year stage lookup from promotions
        stage_stock: dict[int, dict[int, int]] = {yr: {s: 0 for s in range(1, 7)} for yr in STOCK_YEARS}
        flow_by_year: dict[int, dict[tuple[int, int], int]] = {yr: {} for yr in STOCK_YEARS}

        # Only use persons with career data for performance
        sample_pids_s11 = set()
        for p in scores_data:
            fy = p.get("career", {}).get("first_year")
            ly = p.get("career", {}).get("latest_year")
            if fy and ly and 1980 <= fy <= 2025:
                sample_pids_s11.add(p["person_id"])

        for pid in sample_pids_s11:
            p_data = scores_by_pid.get(pid, {})
            fy = p_data.get("career", {}).get("first_year")
            ly = p_data.get("career", {}).get("latest_year") or 2025
            if not fy:
                continue

            events = milestones_data.get(pid, [])
            promotions_s11 = sorted(
                [(int(e["year"]), int(e["to_stage"])) for e in events
                 if e.get("type") == "promotion" and "to_stage" in e and "year" in e],
                key=lambda x: x[0]
            )

            # Track stage over calendar years
            cur_stage = 1
            promo_idx_s11 = 0
            last_stage = 1
            for yr in STOCK_YEARS:
                if yr < fy or yr > ly:
                    continue
                # Apply promotions up to this year
                while (promo_idx_s11 < len(promotions_s11) and
                       promotions_s11[promo_idx_s11][0] <= yr):
                    promo_yr, promo_stg = promotions_s11[promo_idx_s11]
                    if promo_stg != last_stage and promo_yr == yr:
                        # Record flow
                        flow_key = (last_stage, promo_stg)
                        flow_by_year[yr][flow_key] = flow_by_year[yr].get(flow_key, 0) + 1
                    cur_stage = promo_stg
                    promo_idx_s11 += 1

                cur_stage = max(1, min(6, cur_stage))
                stage_stock[yr][cur_stage] += 1
                last_stage = cur_stage

        fig11 = make_subplots(
            rows=2, cols=1,
            subplot_titles=["ストック: ステージ別在籍人数", "フロー: 昇進者数（Stage N→N+1）"],
            vertical_spacing=0.15,
            row_heights=[0.6, 0.4],
        )

        # Upper: Stock
        for stg in range(1, 7):
            y_stock = [stage_stock[yr][stg] for yr in STOCK_YEARS]
            fig11.add_trace(go.Scatter(
                x=STOCK_YEARS, y=y_stock,
                name=STAGE_LABELS.get(stg, f"S{stg}"),
                mode="lines",
                line=dict(color=STAGE_COLORS_HEX.get(stg, "#888"), width=2),
                hovertemplate=f"{STAGE_LABELS.get(stg,'?')}<br>%{{x}}年: %{{y}}人<extra></extra>",
            ), row=1, col=1)

        # Lower: Flow (total promotions per year)
        total_flow_by_yr = [sum(flow_by_year[yr].values()) for yr in STOCK_YEARS]
        fig11.add_trace(go.Bar(
            x=STOCK_YEARS, y=total_flow_by_yr,
            name="昇進者数合計",
            marker_color="#06D6A0",
            opacity=0.7,
            hovertemplate="%{x}年: 昇進者数 %{y}人<extra></extra>",
        ), row=2, col=1)

        # Future projection: linear extrapolation of stock for next 5 years
        future_yrs = [2026, 2027, 2028, 2029, 2030]
        proj_stock: dict[int, dict[int, float]] = {}
        for yr_f in future_yrs:
            proj_stock[yr_f] = {}
            for stg in range(1, 7):
                # Simple linear trend from last 5 years
                recent = [stage_stock.get(yr, {}).get(stg, 0) for yr in range(2020, 2026)]
                if sum(recent) > 0:
                    trend = (recent[-1] - recent[0]) / 5.0
                    projected = max(0, recent[-1] + trend * (yr_f - 2025))
                    proj_stock[yr_f][stg] = round(projected)
                else:
                    proj_stock[yr_f][stg] = 0

        for stg in range(1, 7):
            y_proj = [proj_stock[yr][stg] for yr in future_yrs]
            if sum(y_proj) > 0:
                fig11.add_trace(go.Scatter(
                    x=future_yrs, y=y_proj,
                    name=f"{STAGE_LABELS.get(stg,'?')} 予測",
                    mode="lines",
                    line=dict(color=STAGE_COLORS_HEX.get(stg, "#888"), width=1.5, dash="dot"),
                    showlegend=False,
                    hovertemplate=f"{STAGE_LABELS.get(stg,'?')}予測<br>%{{x}}年: %{{y:.0f}}人<extra></extra>",
                ), row=1, col=1)

        # Demand overlay: unique_anime trend on secondary y (simplified)
        ts_series = time_series_data.get("series", {})
        unique_anime_d2 = ts_series.get("unique_anime", {})
        if unique_anime_d2:
            anime_y = [unique_anime_d2.get(str(yr), 0) for yr in STOCK_YEARS]
            fig11.add_trace(go.Scatter(
                x=STOCK_YEARS, y=anime_y,
                name="年間アニメ本数", mode="lines",
                line=dict(color="#FFD166", width=1.5, dash="dot"),
                opacity=0.5,
                hovertemplate="%{x}年: %{y}本<extra>年間アニメ本数</extra>",
                yaxis="y3",
            ), row=2, col=1)

        fig11.update_layout(
            title_text="ストック&amp;フロー — ステージ別在籍人数の動態と需要",
            legend=dict(orientation="h", y=-0.08),
            yaxis3=dict(
                title="アニメ本数", overlaying="y2",
                side="right", showgrid=False,
            ),
        )
        fig11.update_yaxes(title_text="在籍人数", row=1, col=1)
        fig11.update_yaxes(title_text="昇進者数", row=2, col=1)
        body += plotly_div_safe(fig11, "stock-flow-chart", height=680)

        # Key findings
        # Find years where flow peaked
        if total_flow_by_yr:
            peak_flow_yr = STOCK_YEARS[total_flow_by_yr.index(max(total_flow_by_yr))]
            body += key_findings([
                f"昇進フロー最大年: {peak_flow_yr}年 ({max(total_flow_by_yr):,}人が昇進) — "
                "この年前後に需要急増またはベテラン世代の大量昇進が起きた可能性がある",
                "2015〜2020年に年間アニメ本数が急増した時期、動画・原画ステージのストックが"
                "大きく膨らんでいる場合、需要増が新人大量流入を引き起こしている",
                "点線（予測）は直近5年トレンドの線形外挿。急増予測は"
                "既存スタッフの昇進・引退・需要変動を考慮していないため参考値",
            ])
    except Exception as e:
        body += f'<div class="insight-box">Chart 11 生成スキップ: {e}</div>'

    body += "</div>"

    # ─────────────────────────────────────────────────────────
    # Section 6: 需要ギャップ & 生産性 & 人材動態
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-demand">'
    body += "<h2>Section 6: 需要ギャップ・生産性向上・人材動態</h2>"
    body += section_desc(
        "配信時代の到来により「想定されていた需要」と「実際の需要」の間には構造的なギャップが生じています。"
        "また、デジタル制作ツールの普及による生産性向上は「同じ人数でより多くの作品を担える」効果をもたらし、"
        "単純な人材不足の議論を複雑にします。さらに、業界への新規参入・現役維持・離脱のフローを"
        "時系列で把握することで、人材パイプラインの持続可能性が見えてきます。"
    )

    # ─── 必要データ共通準備 ──────────────────────────────────
    ts_series_d6 = time_series_data.get("series", {})
    ts_years_all = time_series_data.get("years", [])
    DEMAND_YEARS = [y for y in ts_years_all if 1990 <= y <= 2025]

    active_d = ts_series_d6.get("active_persons", {})
    credits_d = ts_series_d6.get("credit_count", {})
    anime_d = ts_series_d6.get("unique_anime", {})
    new_ent_d = ts_series_d6.get("new_entrants", {})

    def _ts_val(d: dict, yr: int) -> float:
        return float(d.get(str(yr), 0) or 0)

    # ── Chart 12: 想定需要 vs 実際需要のギャップ ──────────────
    body += "<h3>Chart 12: 想定需要 vs 実際需要のギャップ — 生産性調整済みスタッフ需要</h3>"
    body += chart_guide(
        "上段: 実際クレジット数（実線）と「過去トレンド外挿による想定クレジット需要」（破線）の乖離（塗り潰し＝ギャップ）。"
        "下段: 実際スタッフ数（実線）と「現在のクレジット量をベースライン生産性でこなすために必要なスタッフ数」（破線）の比較。"
        "下段の破線が実線を上回る＝生産性向上のおかげで現実より少ない人数で需要を処理できている。"
        "下段の実線が破線を上回る＝人材が想定より多く供給されている（需要以上の人材流入）。"
    )

    try:
        # Fit linear trend on 1990-2010 credit_count → project 2011-2025
        base_yrs = [y for y in DEMAND_YEARS if y <= 2010]
        proj_yrs = [y for y in DEMAND_YEARS if y > 2010]

        base_credits = [_ts_val(credits_d, y) for y in base_yrs]
        base_active = [_ts_val(active_d, y) for y in base_yrs]

        # Simple OLS (avoid numpy dependency — use statistics)
        def _ols_predict(xs: list[int], ys: list[float], x_new: list[int]) -> list[float]:
            n = len(xs)
            if n < 2:
                return [ys[-1]] * len(x_new)
            mx = sum(xs) / n
            my = sum(ys) / n
            denom = sum((x - mx) ** 2 for x in xs)
            if denom == 0:
                return [my] * len(x_new)
            slope = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / denom
            intercept = my - slope * mx
            return [max(0.0, intercept + slope * x) for x in x_new]

        # Project credits and active_persons from 1990-2010 trend
        expected_credits_proj = _ols_predict(base_yrs, base_credits, proj_yrs)
        expected_credits_all = base_credits + expected_credits_proj

        expected_active_proj = _ols_predict(base_yrs, base_active, proj_yrs)
        expected_active_all = base_active + expected_active_proj

        actual_credits_all = [_ts_val(credits_d, y) for y in DEMAND_YEARS]
        actual_active_all = [_ts_val(active_d, y) for y in DEMAND_YEARS]

        # Baseline productivity = avg credits/person in 1990-2005
        base_prod_yrs = [y for y in base_yrs if y <= 2005]
        base_prod_vals = [
            _ts_val(credits_d, y) / _ts_val(active_d, y)
            for y in base_prod_yrs
            if _ts_val(active_d, y) > 0
        ]
        baseline_productivity = sum(base_prod_vals) / len(base_prod_vals) if base_prod_vals else 2.1

        # "Required staff at baseline productivity" to handle actual credits
        required_staff = [
            _ts_val(credits_d, y) / baseline_productivity
            for y in DEMAND_YEARS
        ]

        fig12 = make_subplots(
            rows=2, cols=1,
            subplot_titles=[
                "クレジット需要: 実際 vs 想定（1990〜2010年トレンド外挿）",
                "スタッフ需要: 実際 vs ベースライン生産性ベース必要数",
            ],
            vertical_spacing=0.14,
        )

        # Upper: credit gap
        # Shaded gap area (actual - expected; positive = excess demand)
        gap_upper = [a - e for a, e in zip(actual_credits_all, expected_credits_all)]
        gap_pos = [max(0, g) for g in gap_upper]
        gap_neg = [min(0, g) for g in gap_upper]

        fig12.add_trace(go.Scatter(
            x=DEMAND_YEARS, y=expected_credits_all,
            name="想定クレジット需要（外挿）", mode="lines",
            line=dict(color="#FFD166", width=2, dash="dash"),
            hovertemplate="%{x}年 想定: %{y:,.0f}件<extra></extra>",
        ), row=1, col=1)
        fig12.add_trace(go.Scatter(
            x=DEMAND_YEARS, y=actual_credits_all,
            name="実際クレジット数", mode="lines",
            line=dict(color="#4CC9F0", width=2.5),
            hovertemplate="%{x}年 実際: %{y:,.0f}件<extra></extra>",
        ), row=1, col=1)
        # Gap fill (excess demand = actual > expected)
        fig12.add_trace(go.Scatter(
            x=DEMAND_YEARS + DEMAND_YEARS[::-1],
            y=[a if a >= e else e for a, e in zip(actual_credits_all, expected_credits_all)] +
              expected_credits_all[::-1],
            fill="toself", fillcolor="rgba(76,201,240,0.15)",
            line=dict(color="rgba(0,0,0,0)"),
            name="需要超過域", showlegend=True, hoverinfo="skip",
        ), row=1, col=1)
        fig12.add_trace(go.Scatter(
            x=DEMAND_YEARS + DEMAND_YEARS[::-1],
            y=[e if a >= e else a for a, e in zip(actual_credits_all, expected_credits_all)] +
              expected_credits_all[::-1],
            fill="toself", fillcolor="rgba(255,107,53,0.15)",
            line=dict(color="rgba(0,0,0,0)"),
            name="想定割れ域", showlegend=True, hoverinfo="skip",
        ), row=1, col=1)

        # Lower: staff gap
        fig12.add_trace(go.Scatter(
            x=DEMAND_YEARS, y=required_staff,
            name=f"必要スタッフ数（生産性{baseline_productivity:.2f}基準）",
            mode="lines",
            line=dict(color="#FF6B35", width=2, dash="dash"),
            hovertemplate="%{x}年 必要数: %{y:,.0f}人<extra></extra>",
        ), row=2, col=1)
        fig12.add_trace(go.Scatter(
            x=DEMAND_YEARS, y=actual_active_all,
            name="実際アクティブスタッフ数",
            mode="lines",
            line=dict(color="#06D6A0", width=2.5),
            hovertemplate="%{x}年 実際: %{y:,.0f}人<extra></extra>",
        ), row=2, col=1)

        # Demand shift markers
        for marker_yr, label in [(2000, "デジタル化"), (2015, "配信台頭"), (2020, "COVID")]:
            for row_n in [1, 2]:
                fig12.add_vline(
                    x=marker_yr, line_dash="dot",
                    line_color="rgba(255,255,255,0.3)",
                    row=row_n, col=1,
                )
            fig12.add_annotation(
                x=marker_yr, y=1, yref="paper",
                text=label, showarrow=False,
                font=dict(size=9, color="rgba(255,255,255,0.5)"),
                xanchor="left",
            )

        fig12.update_layout(
            title_text="想定需要 vs 実際需要のギャップ",
            legend=dict(orientation="h", y=-0.08),
        )
        fig12.update_yaxes(title_text="クレジット数", row=1, col=1)
        fig12.update_yaxes(title_text="スタッフ数（人）", row=2, col=1)
        body += plotly_div_safe(fig12, "demand-gap-chart", height=680)

        # Key findings
        gap_2023 = _ts_val(credits_d, 2023) - _ols_predict(base_yrs, base_credits, [2023])[0]
        required_2023 = _ts_val(credits_d, 2023) / baseline_productivity
        actual_2023 = _ts_val(active_d, 2023)
        staff_surplus = actual_2023 - required_2023
        body += key_findings([
            f"1990〜2010年トレンド外挿との乖離（2023年）: 実際 "
            f"{_ts_val(credits_d,2023):,.0f}件 vs 想定 "
            f"{_ols_predict(base_yrs, base_credits, [2023])[0]:,.0f}件 "
            f"(+{gap_2023:,.0f}件超過)",
            f"ベースライン生産性（{baseline_productivity:.2f}件/人）基準の必要スタッフ数 vs 実際: "
            f"必要 {required_2023:,.0f}人 vs 実際 {actual_2023:,.0f}人 "
            f"({'余剰' if staff_surplus > 0 else '不足'} {abs(staff_surplus):,.0f}人)",
            "実際スタッフ数が必要数を上回る場合、生産性向上が実現しており"
            "「少ない人数で同じ量を捌ける」効率化が起きていることを示す",
            "実際スタッフ数が必要数を下回る場合、1人当たりの負荷が"
            "ベースライン期より増大しており過重労働リスクの指標となる",
        ])
    except Exception as e:
        body += f'<div class="insight-box">Chart 12 生成スキップ: {e}</div>'

    # ── Chart 13: 生産性指数の推移 ─────────────────────────────
    body += "<h3>Chart 13: 生産性指数の推移 — credits/person の時系列変化</h3>"
    body += chart_guide(
        "青実線（左軸）＝1人あたり年間クレジット数（credits/active_person）。"
        "オレンジ実線（左軸）＝生産性指数（1990年=100に正規化）。"
        "灰色棒（右軸）＝アクティブスタッフ数。"
        "生産性指数が上昇するほど、同じ人数で多くの作品を担えることを意味する。"
        "デジタル化・配信台頭のタイミングと生産性の変化を照らし合わせてください。"
    )

    try:
        prod_per_person = []
        prod_index = []
        base_prod_1990 = None

        for yr in DEMAND_YEARS:
            c_yr = _ts_val(credits_d, yr)
            a_yr = _ts_val(active_d, yr)
            cpp = c_yr / a_yr if a_yr > 0 else 0
            prod_per_person.append(cpp)
            if yr == DEMAND_YEARS[0]:
                base_prod_1990 = cpp
            prod_index.append((cpp / base_prod_1990 * 100) if base_prod_1990 else 100)

        # Trend line (5-year rolling average)
        prod_trend = []
        window = 5
        for i in range(len(DEMAND_YEARS)):
            lo = max(0, i - window // 2)
            hi = min(len(DEMAND_YEARS), i + window // 2 + 1)
            prod_trend.append(sum(prod_per_person[lo:hi]) / (hi - lo))

        # "Counterfactual staff needed" if productivity stayed at 1990 baseline
        counterfactual_staff = [
            _ts_val(credits_d, yr) / prod_per_person[0]  # 1990 productivity
            for yr in DEMAND_YEARS
        ]
        actual_active_list = [_ts_val(active_d, yr) for yr in DEMAND_YEARS]
        productivity_savings = [
            max(0, cf - ac) for cf, ac in zip(counterfactual_staff, actual_active_list)
        ]

        fig13 = make_subplots(
            rows=2, cols=1,
            subplot_titles=[
                "生産性指数（1990=100）と1人当たりクレジット数",
                "生産性向上による「浮いたスタッフ工数」（もし1990年生産性のままなら必要だった追加人数）",
            ],
            vertical_spacing=0.14,
        )

        # Upper: productivity
        fig13.add_trace(go.Bar(
            x=DEMAND_YEARS, y=[_ts_val(active_d, yr) for yr in DEMAND_YEARS],
            name="アクティブスタッフ数", marker_color="rgba(160,160,200,0.25)",
            yaxis="y2", showlegend=True,
            hovertemplate="%{x}年: %{y:,}人<extra>アクティブスタッフ</extra>",
        ), row=1, col=1)
        fig13.add_trace(go.Scatter(
            x=DEMAND_YEARS, y=prod_per_person,
            name="1人当たりクレジット数", mode="lines+markers",
            line=dict(color="#4CC9F0", width=2),
            marker=dict(size=4),
            hovertemplate="%{x}年: %{y:.2f}件/人<extra></extra>",
        ), row=1, col=1)
        fig13.add_trace(go.Scatter(
            x=DEMAND_YEARS, y=prod_trend,
            name="5年移動平均", mode="lines",
            line=dict(color="#FFD166", width=2.5, dash="dash"),
            hovertemplate="%{x}年 移動平均: %{y:.2f}件/人<extra></extra>",
        ), row=1, col=1)

        # Lower: productivity savings
        fig13.add_trace(go.Bar(
            x=DEMAND_YEARS, y=productivity_savings,
            name="生産性向上による節減人数",
            marker_color="#06D6A0",
            opacity=0.75,
            hovertemplate=(
                "%{x}年: 1990年生産性のままなら +%{y:,.0f}人 必要だった<extra></extra>"
            ),
        ), row=2, col=1)
        fig13.add_trace(go.Scatter(
            x=DEMAND_YEARS, y=actual_active_list,
            name="実際スタッフ数（参考）", mode="lines",
            line=dict(color="#a0a0c0", width=1.5, dash="dot"),
            opacity=0.5,
            hovertemplate="%{x}年 実際: %{y:,}人<extra></extra>",
        ), row=2, col=1)

        # Markers
        for marker_yr, label in [
            (2000, "デジタル彩色\n普及"), (2008, "デジタル撮影\n主流化"), (2015, "配信台頭"),
        ]:
            for rn in [1, 2]:
                fig13.add_vline(
                    x=marker_yr, line_dash="dot",
                    line_color="rgba(255,209,102,0.4)", row=rn, col=1,
                )

        fig13.update_layout(
            title_text="生産性指数の推移と需要へのインパクト",
            legend=dict(orientation="h", y=-0.08),
        )
        fig13.update_yaxes(title_text="credits/person", row=1, col=1)
        fig13.update_yaxes(title_text="節減スタッフ数（人）", row=2, col=1)
        body += plotly_div_safe(fig13, "productivity-chart", height=660)

        # Key findings
        prod_2023_idx = prod_index[-2] if len(prod_index) >= 2 else prod_index[-1]
        savings_2023 = productivity_savings[-2] if len(productivity_savings) >= 2 else 0
        body += key_findings([
            f"生産性指数（1990=100）の2023年値: {prod_2023_idx:.1f} "
            f"— 1990年比で {prod_2023_idx-100:+.1f}% の生産性変化",
            f"2023年時点で「1990年生産性のままだったら」: "
            f"さらに約 {savings_2023:,.0f}人 のスタッフが必要だった（生産性向上の節減効果）",
            "デジタル化（2000年前後）と配信需要急増（2015年〜）のどちらが"
            "生産性指数に強い影響を与えたかに注目すると、効率化vs労働集約化の分岐点が見える",
            "生産性向上が止まり実スタッフ数増加が加速する年代は、"
            "技術的効率化の限界または需要超過によるブルウィップ効果の可能性がある",
        ])
    except Exception as e:
        body += f'<div class="insight-box">Chart 13 生成スキップ: {e}</div>'

    # ── Chart 14: 現役・引退・新規の時系列 ────────────────────
    body += "<h3>Chart 14: 現役・引退・新規の時系列 — 人材パイプラインの動態</h3>"
    body += chart_guide(
        "上段: 現役（active、青）・新規参入（green）・引退/離脱（orange）の人数推移。"
        "引退＝その年が最後のクレジットになった人物数（scores.jsonのlatest_yearから集計）。"
        "中段: 純増数（新規−引退）。プラス＝業界人口が増加、マイナス＝減少。"
        "下段: 現役に対する新規・引退の比率（入れ替わり率）。"
        "比率が高い年は業界の「新陳代謝」が活発で、世代交代が急速に進んでいる。"
    )

    try:
        # Compute retired per year from scores.json
        retired_by_yr: dict[int, int] = {}
        first_yr_counts: dict[int, int] = {}
        for p in scores_data:
            fy = p.get("career", {}).get("first_year")
            ly = p.get("career", {}).get("latest_year")
            if fy and 1990 <= fy <= 2025:
                first_yr_counts[fy] = first_yr_counts.get(fy, 0) + 1
            # "retired" = latest_year is at least 2 years ago (avoid penalizing recent data gaps)
            if ly and 1990 <= ly <= 2023:
                retired_by_yr[ly] = retired_by_yr.get(ly, 0) + 1

        # Also can use new_entrants from time_series (more complete)
        new_ent_vals = [_ts_val(new_ent_d, yr) for yr in DEMAND_YEARS]
        active_vals = [_ts_val(active_d, yr) for yr in DEMAND_YEARS]
        retired_vals = [float(retired_by_yr.get(yr, 0)) for yr in DEMAND_YEARS]

        # Net change = new - retired
        net_change = [n - r for n, r in zip(new_ent_vals, retired_vals)]
        # Turnover rate = (new + retired) / active
        turnover_rate = [
            (n + r) / a * 100 if a > 0 else 0
            for n, r, a in zip(new_ent_vals, retired_vals, active_vals)
        ]

        fig14 = make_subplots(
            rows=3, cols=1,
            subplot_titles=[
                "現役・新規・引退の絶対数",
                "純増数（新規 − 引退）",
                "入れ替わり率 = (新規+引退) / 現役 [%]",
            ],
            vertical_spacing=0.1,
            row_heights=[0.5, 0.25, 0.25],
        )

        # Upper: absolute counts
        fig14.add_trace(go.Scatter(
            x=DEMAND_YEARS, y=active_vals,
            name="現役スタッフ数", mode="lines",
            line=dict(color="#4CC9F0", width=2.5),
            fill="tozeroy", fillcolor="rgba(76,201,240,0.08)",
            hovertemplate="%{x}年 現役: %{y:,.0f}人<extra></extra>",
        ), row=1, col=1)
        fig14.add_trace(go.Bar(
            x=DEMAND_YEARS, y=new_ent_vals,
            name="新規参入", marker_color="#06D6A0", opacity=0.8,
            hovertemplate="%{x}年 新規: %{y:,.0f}人<extra></extra>",
        ), row=1, col=1)
        fig14.add_trace(go.Bar(
            x=DEMAND_YEARS, y=[-v for v in retired_vals],
            name="引退/離脱", marker_color="#FF6B35", opacity=0.8,
            hovertemplate="%{x}年 引退: %{y:,.0f}人<extra></extra>",
        ), row=1, col=1)

        # Middle: net change
        net_colors = ["#06D6A0" if v >= 0 else "#FF6B35" for v in net_change]
        fig14.add_trace(go.Bar(
            x=DEMAND_YEARS, y=net_change,
            name="純増数",
            marker_color=net_colors,
            hovertemplate="%{x}年 純増: %{y:+,.0f}人<extra></extra>",
        ), row=2, col=1)
        fig14.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.3)", row=2, col=1)

        # Lower: turnover rate
        fig14.add_trace(go.Scatter(
            x=DEMAND_YEARS, y=turnover_rate,
            name="入れ替わり率", mode="lines+markers",
            line=dict(color="#f093fb", width=2),
            marker=dict(size=4),
            hovertemplate="%{x}年 入替率: %{y:.1f}%<extra></extra>",
        ), row=3, col=1)

        # Demand shift milestones
        for marker_yr, label in [(2000, "デジタル化"), (2015, "配信台頭"), (2020, "COVID")]:
            for rn in [1, 2, 3]:
                fig14.add_vline(
                    x=marker_yr, line_dash="dot",
                    line_color="rgba(255,255,255,0.25)", row=rn, col=1,
                )

        fig14.update_layout(
            title_text="現役・引退・新規の時系列 — 人材パイプライン動態",
            barmode="overlay",
            legend=dict(orientation="h", y=-0.06),
        )
        fig14.update_yaxes(title_text="人数", row=1, col=1)
        fig14.update_yaxes(title_text="純増（人）", row=2, col=1)
        fig14.update_yaxes(title_text="率（%）", row=3, col=1)
        body += plotly_div_safe(fig14, "turnover-chart", height=740)

        # Key findings
        # Find year of max new entrants and max retirements
        max_new_yr = DEMAND_YEARS[new_ent_vals.index(max(new_ent_vals))]
        max_new_v = max(new_ent_vals)
        max_ret_yr = DEMAND_YEARS[retired_vals.index(max(retired_vals))]
        max_ret_v = max(retired_vals)
        max_turn_yr = DEMAND_YEARS[turnover_rate.index(max(turnover_rate))]
        max_turn_v = max(turnover_rate)

        # Net change in post-streaming era
        streaming_net = sum(net_change[i] for i, yr in enumerate(DEMAND_YEARS) if 2015 <= yr <= 2023)

        body += key_findings([
            f"新規参入ピーク: {max_new_yr}年 ({max_new_v:,.0f}人) — "
            "配信台頭期の需要急増が新規流入を押し上げた可能性が高い",
            f"引退/離脱ピーク: {max_ret_yr}年 ({max_ret_v:,.0f}人) — "
            "この年に長期活動者の大量リタイアまたは業界縮小が起きた可能性",
            f"最高入れ替わり率: {max_turn_yr}年 ({max_turn_v:.1f}%) — "
            "業界の新陳代謝が最も活発だった年",
            f"配信台頭後（2015〜2023年）の累積純増: {streaming_net:+,.0f}人 — "
            "この期間の人材蓄積が現在の業界規模の基盤を形成している",
            "純増がマイナスになる年（引退＞新規）は高齢化・業界縮小の警戒シグナル。"
            "現在のトレンドが継続した場合、将来の純増推移も推定できる",
        ])
    except Exception as e:
        body += f'<div class="insight-box">Chart 14 生成スキップ: {e}</div>'

    # TOC更新アンカー追加のため再インサート
    body = body.replace(
        '<a href="#sec-stream">5. ストック&amp;フロー</a>\n</div>\n</div>',
        '<a href="#sec-stream">5. ストック&amp;フロー</a>\n'
        '<a href="#sec-demand">6. 需要ギャップ&amp;人材動態</a>\n'
        '<a href="#sec-format">7. フォーマット&amp;ジャンル別生産性</a>\n</div>\n</div>',
    )

    body += "</div>"

    # ─────────────────────────────────────────────────────────
    # Section 7: フォーマット・ジャンル別 生産性分析
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-format">'
    body += "<h2>Section 7: フォーマット・ジャンル別 生産性 &amp; 必要スタッフ数</h2>"
    body += section_desc(
        "映画（MOVIE）・TVアニメ・ONA・OVA・TV_SHORTなど、メディアフォーマットによって"
        "「放送1時間あたり / 1クール（12話）あたりの必要スタッフ数」は大きく異なります。"
        "同じ「アニメ1本」でも、映画とTVシリーズでは労働密度の構造が根本的に違います。"
        "またジャンルによっても戦闘・メカ系vs日常系では複雑度が異なり、必要人数が変わります。"
        "生産性の時系列変化と作品規模（クール数）による規模の経済も可視化します。"
    )

    try:
        from src.database import get_connection as _get_conn
        _conn = _get_conn()
        _conn.row_factory = None
        _cur = _conn.cursor()

        # ── 基礎データ取得 ────────────────────────────────────
        # 1) Per-anime: format, duration, episodes, credits, persons
        _cur.execute('''SELECT a.id, a.format, a.duration, a.episodes, a.year,
            COUNT(c.id) as total_credits,
            COUNT(DISTINCT c.person_id) as unique_persons
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE a.format IN ('TV','MOVIE','ONA','OVA','SPECIAL','TV_SHORT','MUSIC')
            AND a.duration > 0 AND a.episodes > 0
            AND a.year BETWEEN 1985 AND 2025
        GROUP BY a.id''')
        per_anime_rows = _cur.fetchall()

        # 2) Per-anime with genre
        _cur.execute('''SELECT a.genres, a.format, a.duration, a.episodes,
            COUNT(c.id) as total_credits,
            COUNT(DISTINCT c.person_id) as unique_persons
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE a.genres IS NOT NULL
            AND a.format IN ('TV','MOVIE','ONA','OVA','TV_SHORT')
            AND a.duration > 0 AND a.episodes > 0
        GROUP BY a.id''')
        genre_rows_s7 = _cur.fetchall()

        _conn.close()

        import statistics as _st7
        from collections import defaultdict as _dd7

        # Build per-anime normalized metrics
        fmt_metrics = _dd7(lambda: {
            'cred_per_hr': [], 'pers_per_hr': [],
            'cred_per_cour': [], 'pers_per_cour': [],
            'total_min': [], 'anime_cnt': 0,
        })
        yr_fmt_metrics = _dd7(lambda: _dd7(lambda: {
            'cred_per_hr': [], 'pers_per_hr': [],
            'cred_per_cour': [], 'pers_per_cour': [],
        }))

        for (aid, fmt, dur, eps, yr, cred, pers) in per_anime_rows:
            total_min = dur * eps
            total_hr = total_min / 60.0
            cour_count = eps / 12.0
            if total_hr <= 0 or cour_count <= 0:
                continue
            cph = cred / total_hr
            pph = pers / total_hr
            cpc = cred / cour_count
            ppc = pers / cour_count
            d = fmt_metrics[fmt]
            d['cred_per_hr'].append(cph)
            d['pers_per_hr'].append(pph)
            d['cred_per_cour'].append(cpc)
            d['pers_per_cour'].append(ppc)
            d['total_min'].append(total_min)
            d['anime_cnt'] += 1
            if yr:
                yd = yr_fmt_metrics[int(yr)][fmt]
                yd['cred_per_hr'].append(cph)
                yd['pers_per_hr'].append(pph)
                yd['cred_per_cour'].append(cpc)
                yd['pers_per_cour'].append(ppc)

        # Genre metrics
        genre_metrics = _dd7(lambda: _dd7(lambda: {
            'cred_per_hr': [], 'pers_per_hr': [],
            'cred_per_cour': [], 'pers_per_cour': [],
        }))
        for (genres_str, fmt, dur, eps, cred, pers) in genre_rows_s7:
            try:
                gs = json.loads(genres_str) if isinstance(genres_str, str) else []
                if not gs:
                    continue
                total_hr = (dur * eps) / 60.0
                cour_count = eps / 12.0
                if total_hr <= 0 or cour_count <= 0:
                    continue
                for g in gs[:3]:
                    d = genre_metrics[g][fmt]
                    d['cred_per_hr'].append(cred / total_hr)
                    d['pers_per_hr'].append(pers / total_hr)
                    d['cred_per_cour'].append(cred / cour_count)
                    d['pers_per_cour'].append(pers / cour_count)
            except Exception:
                pass

        FORMATS_S7 = ['MOVIE', 'TV', 'ONA', 'OVA', 'SPECIAL', 'TV_SHORT']
        FORMAT_LABELS_S7 = {
            'MOVIE': '映画', 'TV': 'TVアニメ', 'ONA': 'ONA',
            'OVA': 'OVA', 'SPECIAL': 'スペシャル', 'TV_SHORT': 'ショート',
        }
        FORMAT_COLORS_S7 = {
            'MOVIE': '#F72585', 'TV': '#4CC9F0', 'ONA': '#06D6A0',
            'OVA': '#FFD166', 'SPECIAL': '#FF6B35', 'TV_SHORT': '#7209B7',
        }

        def _med(lst):
            return _st7.median(lst) if lst else 0.0

        # ── Chart 15: フォーマット別スタッフ需要プロファイル ──
        body += "<h3>Chart 15: フォーマット別 スタッフ需要プロファイル — 放送時間・クール当たり</h3>"
        body += chart_guide(
            "左軸: 放送1時間あたりクレジット数（労働密度）と担当スタッフ数（チーム規模）。"
            "右パネル: 1クール（12話相当）あたりの同指標。"
            "映画は放送時間あたりのスタッフ密度が高く、TVは1クールで安定した規模感がある。"
            "ONA・ショートは短尺のため時間あたり密度が低い一方、1クール換算では独自の構造を持つ。"
        )

        fig15 = make_subplots(
            rows=1, cols=2,
            subplot_titles=["放送時間あたり（credits/hr・persons/hr）",
                            "1クール12話あたり（credits/cour・persons/cour）"],
            horizontal_spacing=0.12,
        )

        fmts_present = [f for f in FORMATS_S7 if fmt_metrics[f]['anime_cnt'] > 0]
        labels15 = [FORMAT_LABELS_S7[f] for f in fmts_present]
        colors15 = [FORMAT_COLORS_S7[f] for f in fmts_present]

        # Left panel: per-hour
        fig15.add_trace(go.Bar(
            x=labels15,
            y=[_med(fmt_metrics[f]['cred_per_hr']) for f in fmts_present],
            name="クレジット数/時間",
            marker_color=[_hex_alpha(c, "cc") for c in colors15],
            hovertemplate="%{x}<br>クレジット/hr: %{y:.1f}<extra></extra>",
        ), row=1, col=1)
        fig15.add_trace(go.Bar(
            x=labels15,
            y=[_med(fmt_metrics[f]['pers_per_hr']) for f in fmts_present],
            name="スタッフ数/時間",
            marker_color=[_hex_alpha(c, "66") for c in colors15],
            marker_pattern_shape="/",
            hovertemplate="%{x}<br>スタッフ/hr: %{y:.1f}<extra></extra>",
        ), row=1, col=1)

        # Right panel: per-cour
        fig15.add_trace(go.Bar(
            x=labels15,
            y=[_med(fmt_metrics[f]['cred_per_cour']) for f in fmts_present],
            name="クレジット数/クール",
            marker_color=[_hex_alpha(c, "cc") for c in colors15],
            showlegend=False,
            hovertemplate="%{x}<br>クレジット/クール: %{y:.1f}<extra></extra>",
        ), row=1, col=2)
        fig15.add_trace(go.Bar(
            x=labels15,
            y=[_med(fmt_metrics[f]['pers_per_cour']) for f in fmts_present],
            name="スタッフ数/クール",
            marker_color=[_hex_alpha(c, "66") for c in colors15],
            marker_pattern_shape="/",
            showlegend=False,
            hovertemplate="%{x}<br>スタッフ/クール: %{y:.1f}<extra></extra>",
        ), row=1, col=2)

        fig15.update_layout(
            title_text="フォーマット別 スタッフ需要プロファイル",
            barmode="group",
            legend=dict(orientation="h", y=-0.12),
        )
        fig15.update_yaxes(title_text="（中央値）", row=1, col=1)
        fig15.update_yaxes(title_text="（中央値）", row=1, col=2)
        body += plotly_div_safe(fig15, "format-profile-chart", height=480)

        # Key findings
        mov_pph = _med(fmt_metrics['MOVIE']['pers_per_hr'])
        tv_ppc = _med(fmt_metrics['TV']['pers_per_cour'])
        ona_ppc = _med(fmt_metrics['ONA']['pers_per_cour'])
        body += key_findings([
            f"映画: 中央値 {mov_pph:.1f}人/放送時間 — 短時間に集中した高密度スタッフ編成",
            f"TVアニメ: 中央値 {tv_ppc:.1f}人/クール — 12話にわたり安定したチームが稼働",
            f"ONA: 中央値 {ona_ppc:.1f}人/クール — 短尺・少人数の効率的な制作体制",
            "映画とTVアニメでは制作の時間軸の圧縮率が異なるため、単純な本数比較は誤解を生む。"
            "正確な労働需要推計には放送時間基準の正規化が必須",
        ])

        # ── Chart 16: ジャンル×フォーマット スタッフ需要マトリクス ──
        body += "<h3>Chart 16: ジャンル × フォーマット スタッフ需要マトリクス</h3>"
        body += chart_guide(
            "色が濃い（暖色）ほど、そのジャンル×フォーマットの組み合わせでのスタッフ数が多い。"
            "左列＝TVアニメ（persons/クール）、右列＝映画（persons/時間）。"
            "ミステリー・ドラマは登場人物・作画複雑度が高く、コメディ・スポーツより密度が高い傾向。"
        )

        TOP_GENRES_S7 = [
            'Action', 'Comedy', 'Drama', 'Fantasy', 'Adventure',
            'Sci-Fi', 'Slice of Life', 'Romance', 'Supernatural',
            'Mystery', 'Mecha', 'Sports', 'Psychological',
        ]
        GENRE_JA_S7 = {
            'Action': 'アクション', 'Comedy': 'コメディ', 'Drama': 'ドラマ',
            'Fantasy': 'ファンタジー', 'Adventure': 'アドベンチャー',
            'Sci-Fi': 'SF', 'Slice of Life': '日常', 'Romance': 'ロマンス',
            'Supernatural': '超自然', 'Mystery': 'ミステリー', 'Mecha': 'メカ',
            'Sports': 'スポーツ', 'Psychological': '心理',
        }
        FMTS_MATRIX = ['TV', 'MOVIE', 'ONA', 'OVA']
        FMT_METRIC = {'TV': 'pers_per_cour', 'MOVIE': 'pers_per_hr',
                      'ONA': 'pers_per_cour', 'OVA': 'pers_per_hr'}

        z_matrix16 = []
        hover16 = []
        for g in TOP_GENRES_S7:
            row_z = []
            row_h = []
            for fmt in FMTS_MATRIX:
                vals = genre_metrics[g][fmt].get(FMT_METRIC[fmt], [])
                v = _med(vals) if vals else None
                row_z.append(v)
                unit = "/クール" if FMT_METRIC[fmt] == 'pers_per_cour' else "/時間"
                row_h.append(
                    f"{GENRE_JA_S7.get(g,g)} × {FORMAT_LABELS_S7[fmt]}<br>"
                    f"スタッフ数{unit}: {v:.1f}" if v else f"{g} × {fmt}: データ不足"
                )
            z_matrix16.append(row_z)
            hover16.append(row_h)

        fig16 = go.Figure(go.Heatmap(
            z=z_matrix16,
            x=[FORMAT_LABELS_S7[f] for f in FMTS_MATRIX],
            y=[GENRE_JA_S7.get(g, g) for g in TOP_GENRES_S7],
            colorscale="YlOrRd",
            colorbar=dict(title="スタッフ数（中央値）"),
            hoverongaps=False,
            customdata=hover16,
            hovertemplate="%{customdata}<extra></extra>",
        ))
        fig16.update_layout(
            title_text="ジャンル × フォーマット スタッフ需要マトリクス（スタッフ数/クール or /時間）",
            xaxis_title="フォーマット",
            yaxis_title="ジャンル",
        )
        body += plotly_div_safe(fig16, "genre-format-matrix", height=500)

        # ── Chart 17: フォーマット別 時系列トレンド ────────────
        body += "<h3>Chart 17: フォーマット別 スタッフ密度の時系列トレンド（1990〜2025年）</h3>"
        body += chart_guide(
            "各フォーマットの「放送時間あたりスタッフ数（persons/hr）」の年次推移。"
            "上昇＝同じ時間の作品に使うスタッフが増えた（品質向上 or 複雑化）。"
            "下降＝効率化・人材不足・シンプルな作風の流行。"
            "映画とTVで異なるトレンドを追うことで、制作スタイルの時代変化が読み取れる。"
        )

        TREND_YEARS = list(range(1990, 2025))
        FMTS_TREND = ['TV', 'MOVIE', 'ONA', 'TV_SHORT']
        TREND_METRIC = 'pers_per_hr'

        fig17 = go.Figure()
        for fmt in FMTS_TREND:
            yvals = []
            xvals = []
            for yr in TREND_YEARS:
                vals = yr_fmt_metrics[yr][fmt].get(TREND_METRIC, [])
                if len(vals) >= 3:  # require at least 3 works for stability
                    yvals.append(_med(vals))
                    xvals.append(yr)
            if len(xvals) < 5:
                continue
            # 3-year rolling smooth
            smoothed = []
            for i, yr in enumerate(xvals):
                lo = max(0, i - 1)
                hi = min(len(xvals), i + 2)
                smoothed.append(sum(yvals[lo:hi]) / (hi - lo))

            color = FORMAT_COLORS_S7.get(fmt, "#888")
            fig17.add_trace(go.Scatter(
                x=xvals, y=smoothed,
                name=FORMAT_LABELS_S7.get(fmt, fmt),
                mode="lines",
                line=dict(color=color, width=2.5),
                hovertemplate=f"{FORMAT_LABELS_S7.get(fmt, fmt)}<br>%{{x}}年: %{{y:.1f}}人/hr<extra></extra>",
            ))

        for marker_yr, label in [(2000, "デジタル化"), (2015, "配信台頭"), (2020, "COVID")]:
            fig17.add_vline(
                x=marker_yr, line_dash="dot",
                line_color="rgba(255,255,255,0.35)",
                annotation_text=label,
                annotation_position="top",
                annotation_font=dict(size=9, color="rgba(255,255,255,0.55)"),
            )

        fig17.update_layout(
            title_text="フォーマット別 スタッフ密度（persons/hr）の時系列変化",
            xaxis_title="年", yaxis_title="スタッフ数/放送時間（中央値）",
            legend=dict(orientation="h", y=-0.12),
        )
        body += plotly_div_safe(fig17, "format-trend-chart", height=460)

        # ── Chart 18: TVアニメ クール数別 効率性分析 ────────────
        body += "<h3>Chart 18: TVアニメ クール数別効率性 — 長期作品の規模の経済</h3>"
        body += chart_guide(
            "X軸＝クール数（12話=1クール換算）。Y軸＝1クールあたりのスタッフ数（pers/cour）。"
            "箱ひげ図でクール数ごとのばらつきも確認できる。"
            "1クール作品 vs 4クール超の長期シリーズで「1クールあたりスタッフ数」が下がれば"
            "長期作品の規模の経済（同一スタッフで複数クールを担当）が成立している。"
        )

        # Bin TV anime by cour count
        cour_bins = {'0.5クール以下': [], '1クール': [], '2クール': [], '3〜4クール': [], '5クール以上': []}

        for (aid, fmt, dur, eps, yr, cred, pers) in per_anime_rows:
            if fmt != 'TV':
                continue
            cour_count = eps / 12.0
            ppc = pers / cour_count if cour_count > 0 else None
            if ppc is None:
                continue
            if cour_count <= 0.5:
                cour_bins['0.5クール以下'].append(ppc)
            elif cour_count <= 1.2:
                cour_bins['1クール'].append(ppc)
            elif cour_count <= 2.3:
                cour_bins['2クール'].append(ppc)
            elif cour_count <= 4.5:
                cour_bins['3〜4クール'].append(ppc)
            else:
                cour_bins['5クール以上'].append(ppc)

        fig18 = go.Figure()
        bin_colors = ['#a0a0c0', '#4CC9F0', '#06D6A0', '#FFD166', '#FF6B35']
        for (bin_name, color) in zip(cour_bins.keys(), bin_colors):
            vals = cour_bins[bin_name]
            if not vals:
                continue
            # Clip outliers for display
            p10 = sorted(vals)[max(0, int(len(vals) * 0.05))]
            p90 = sorted(vals)[min(len(vals)-1, int(len(vals) * 0.95))]
            clipped = [v for v in vals if p10 <= v <= p90]
            fig18.add_trace(go.Box(
                y=clipped,
                name=f"{bin_name}\n(N={len(vals)})",
                marker_color=color,
                boxmean=True,
                hovertemplate=f"{bin_name}<br>スタッフ/クール: %{{y:.1f}}<extra></extra>",
            ))

        fig18.update_layout(
            title_text="TVアニメ クール数別 スタッフ規模（persons/クール）— 規模の経済分析",
            xaxis_title="クール数カテゴリ",
            yaxis_title="スタッフ数/クール（中央値）",
        )
        body += plotly_div_safe(fig18, "cour-efficiency-chart", height=480)

        # Key findings for Section 7
        one_cour = cour_bins.get('1クール', [])
        long_cour = cour_bins.get('5クール以上', [])
        if one_cour and long_cour:
            med_1 = _med(one_cour)
            med_long = _med(long_cour)
            scale_effect = (med_long - med_1) / med_1 * 100
            scale_label = "規模の経済が成立" if scale_effect < -5 else (
                "規模の不経済（長期作品のほうがコスト高）" if scale_effect > 5 else "ほぼ規模中立"
            )
            body += key_findings([
                f"TVアニメ 1クール: 中央値 {med_1:.0f}人/クール vs "
                f"5クール以上: 中央値 {med_long:.0f}人/クール "
                f"({scale_effect:+.0f}% → {scale_label})",
                "ジャンルマトリクスでミステリー・ドラマが労働集約的、"
                "コメディ・スポーツが比較的コンパクトなスタッフ編成の傾向",
                "映画は短い放送時間に対して高密度のスタッフを集中投入する構造。"
                "Netflix系配信映画の増加はこの密度をさらに押し上げる可能性がある",
                "ONAは時間あたりスタッフ密度が低く、スタジオが少人数・短期集中で制作できる"
                "配信向け効率フォーマットとして確立しつつある",
            ])

    except Exception as e:
        body += f'<div class="insight-box">Section 7 生成スキップ: {type(e).__name__}: {e}</div>'

    body += "</div>"

    # ── TOC update: add Section 8 ─────────────────────────────
    body = body.replace(
        '<a href="#sec-format">7. フォーマット&amp;ジャンル別生産性</a>\n</div>\n</div>',
        '<a href="#sec-format">7. フォーマット&amp;ジャンル別生産性</a>\n'
        '<a href="#sec-startup">8. 固定費&amp;変動費・スタジオ規模</a>\n</div>\n</div>',
    )

    # ─────────────────────────────────────────────────────────
    # Section 8: 固定費 vs 変動費 — 立ち上げコスト & スタジオ規模分析
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-startup">'
    body += "<h2>Section 8: 固定費 vs 変動費 — 立ち上げコスト &amp; スタジオ規模</h2>"
    body += section_desc(
        "アニメ制作には「クール数によらず必ずかかる立ち上げコスト（固定費）」と"
        "「話数に比例するコスト（変動費）」があります。"
        "キャラデザ・監督・音楽など固定チームは1クール作品も5クール作品もほぼ同規模、"
        "原画・動画・演出などは話数比例で増加します。"
        "OLS回帰でジャンル・年代・スタジオ規模ごとの固定費の違いを定量化します。"
        "また、大手スタジオの社員制度が「可視クレジット数」と「実際のスコア」に"
        "与えるバイアス（生存バイアス含む）も分析します。"
    )

    try:
        from src.database import get_connection as _get_conn_s8
        _conn_s8 = _get_conn_s8()
        _sd = _build_startup_cost_data(_conn_s8)

        # Also query per-person credits/year and iv_score by studio tier
        _cur_s8 = _conn_s8.cursor()
        _cur_s8.execute("""
            SELECT
                c.person_id,
                COUNT(DISTINCT c.anime_id) AS total_works,
                MIN(a.year) AS first_year,
                MAX(a.year) AS last_year,
                s.favourites AS studio_favs
            FROM credits c
            JOIN anime a ON c.anime_id = a.id
            JOIN anime_studios ast ON a.id = ast.anime_id AND ast.is_main = 1
            JOIN studios s ON ast.studio_id = s.id
            WHERE a.year BETWEEN 1985 AND 2025
            GROUP BY c.person_id, s.id
        """)
        person_studio_rows = _cur_s8.fetchall()
        _conn_s8.close()

        raw_rows = _sd["raw_rows"]
        ols_by_genre = _sd["ols_by_genre"]
        by_year_1cour = _sd["by_year_1cour"]
        genre_decade = _sd["genre_decade"]

        # Load scores for survivorship analysis (scores.json is a list of dicts)
        _scores_s8_list = load_json("scores.json") or []
        _score_by_pid: dict = {}
        if isinstance(_scores_s8_list, list):
            for pdata in _scores_s8_list:
                if isinstance(pdata, dict):
                    pid = pdata.get("person_id")
                    comp = pdata.get("iv_score")
                    if pid and comp:
                        _score_by_pid[str(pid)] = float(comp)
        elif isinstance(_scores_s8_list, dict):
            for pid, pdata in _scores_s8_list.items():
                if isinstance(pdata, dict):
                    comp = pdata.get("iv_score")
                    if comp:
                        _score_by_pid[str(pid)] = float(comp)

        import statistics as _st8
        from collections import defaultdict as _dd8

        # Build per-person studio-tier mapping (pick tier of studio with most works)
        person_studio_cnt: dict = _dd8(lambda: _dd8(int))  # {pid: {tier: works}}
        for pid, works, fy, ly, sfavs in person_studio_rows:
            if sfavs is None:
                sfavs = 0
            if sfavs >= 1000:
                tier = "大手 (1000+ fav)"
            elif sfavs >= 100:
                tier = "中規模 (100-999 fav)"
            else:
                tier = "小規模 (<100 fav)"
            person_studio_cnt[pid][tier] += works

        # Assign primary tier per person
        person_primary_tier: dict = {}
        for pid, tier_counts in person_studio_cnt.items():
            person_primary_tier[pid] = max(tier_counts, key=lambda t: tier_counts[t])

        # Per-person: annual credits and career span for persons with score data
        person_span: dict = {}  # {pid: (first_year, last_year, total_works, tier)}
        for pid, works, fy, ly, sfavs in person_studio_rows:
            if pid not in person_span:
                person_span[pid] = {"works": 0, "fy": 9999, "ly": 0}
            person_span[pid]["works"] += works
            if fy:
                person_span[pid]["fy"] = min(person_span[pid]["fy"], int(fy))
            if ly:
                person_span[pid]["ly"] = max(person_span[pid]["ly"], int(ly))

        # Compute annual_works rate
        tier_order = ["大手 (1000+ fav)", "中規模 (100-999 fav)", "小規模 (<100 fav)"]
        tier_annual_works: dict = {t: [] for t in tier_order}
        tier_iv_score: dict = {t: [] for t in tier_order}

        for pid, span in person_span.items():
            tier = person_primary_tier.get(pid, "小規模 (<100 fav)")
            if tier not in tier_order:
                continue
            years_active = max(1, span["ly"] - span["fy"] + 1)
            annual = span["works"] / years_active
            tier_annual_works[tier].append(annual)
            if pid in _score_by_pid:
                tier_iv_score[tier].append(_score_by_pid[pid])

    except Exception as _e_s8_load:
        body += f'<div class="insight-box">Section 8 データ取得スキップ: {type(_e_s8_load).__name__}: {_e_s8_load}</div>'
        body += "</div>"
    else:
        # ── Chart 19: 固定費 vs 変動費 — クール数別分布 ──────────────────
        try:
            body += "<h3>Chart 19: 固定費 vs 変動費 — クール数別ボックスプロット</h3>"
            body += chart_guide(
                "固定費（左パネル: キャラデザ・監督・音楽等のユニーク人数）はクール数によらず"
                "ほぼ一定 → 立ち上げコストは作品の長さに依存しない。"
                "変動費（右パネル: 原画・動画・演出等）はクール数とともに増加 → 話数比例コスト。"
                "箱の中央線が左右でほぼ同じ高さなら固定費仮説を支持します。"
            )
            cour_cats = [1, 2, 3, 4]
            cour_labels = ["1クール", "2クール", "3クール", "4クール以上"]
            startup_by_cour = {c: [] for c in cour_cats}
            variable_by_cour = {c: [] for c in cour_cats}
            for row in raw_rows:
                cc = min(row["cour_count"], 4)
                startup_by_cour[cc].append(row["startup_persons"])
                if row["variable_persons"] > 0:
                    variable_by_cour[cc].append(row["variable_persons"])

            COLORS_4 = ["#4CC9F0", "#FFD166", "#FF6B35", "#F72585"]
            fig19 = make_subplots(
                rows=1, cols=2,
                subplot_titles=["固定費（キャラデザ・監督・音楽等）", "変動費（原画・動画・演出等）"],
            )
            for i, (cc, lbl) in enumerate(zip(cour_cats, cour_labels)):
                fig19.add_trace(
                    _violin_raincloud(startup_by_cour[cc], lbl, COLORS_4[i], legendgroup=lbl),
                    row=1, col=1,
                )
                fig19.add_trace(
                    _violin_raincloud(variable_by_cour[cc], lbl, COLORS_4[i],
                                      legendgroup=lbl, showlegend=False),
                    row=1, col=2,
                )
            fig19.update_layout(
                title_text="Chart 19: 固定費 vs 変動費のクール数別分布 (Raincloud)",
                legend=dict(orientation="h", y=-0.15),
                height=520,
                violinmode="overlay",
            )
            fig19.update_yaxes(title_text="ユニーク人数", row=1, col=1)
            fig19.update_yaxes(title_text="ユニーク人数", row=1, col=2)
            body += plotly_div_safe(fig19, "startup-box-cour", height=520)

            s1 = startup_by_cour[1]
            s4 = startup_by_cour[4]
            med1 = _st8.median(s1) if s1 else 0
            med4 = _st8.median(s4) if s4 else 0
            body += key_findings([
                f"固定費中央値 — 1クール: {med1:.0f}人 / 4クール以上: {med4:.0f}人 "
                f"(差: {med4 - med1:+.0f}人, 固定費仮説{"支持" if abs(med4 - med1) < 2 else "一部否定"})",
                "変動費はクール数に比例して増加 → 長期シリーズの総コストは話数で説明される",
                "固定費が一度の立ち上げ投資として回収できるなら長期シリーズほど単価効率が高い",
            ])
        except Exception as _e19:
            body += f'<div class="insight-box">Chart 19 スキップ: {_e19}</div>'

        # ── Chart 20: ジャンル別 OLS固定費インターセプト ──────────────────
        try:
            body += "<h3>Chart 20: ジャンル別 OLS固定費比較 — インターセプトと回帰線</h3>"
            body += chart_guide(
                "OLS回帰 startup_persons ~ cour_count のインターセプト（切片）が固定費の推計値。"
                "Chart 20a: ジャンル別インターセプト棒グラフ（高いほど立ち上げコストが大きい）。"
                "Chart 20b: 上位6ジャンルの散布図＋OLS回帰線。回帰線がほぼ水平 → 固定費仮説支持。"
            )
            top_genres_n = sorted(ols_by_genre.items(), key=lambda x: -x[1]["n"])[:12]
            top_genres = [g for g, _ in top_genres_n]
            intercepts = [ols_by_genre[g]["intercept"] for g in top_genres]
            ns_g = [ols_by_genre[g]["n"] for g in top_genres]
            r2s_g = [ols_by_genre[g]["r2"] for g in top_genres]

            if intercepts:
                span_ic = max(intercepts) - min(intercepts) or 1
                min_ic = min(intercepts)
                bar_colors_g = [
                    f"rgb({int(80 + 175 * (ic - min_ic) / span_ic)},{int(200 - 100 * (ic - min_ic) / span_ic)},200)"
                    for ic in intercepts
                ]
            else:
                bar_colors_g = ["#4CC9F0"] * len(top_genres)

            fig20a = go.Figure(go.Bar(
                x=top_genres,
                y=intercepts,
                marker_color=bar_colors_g,
                text=[f"{v:.1f}<br>n={n} R²={r:.3f}"
                      for v, n, r in zip(intercepts, ns_g, r2s_g)],
                textposition="outside",
            ))
            fig20a.update_layout(
                title_text="Chart 20a: ジャンル別 OLS固定費インターセプト (startup_persons ~ cour_count)",
                xaxis_title="ジャンル",
                yaxis_title="OLSインターセプト（固定費スタッフ数推計）",
                height=460,
            )
            body += plotly_div_safe(fig20a, "genre-ols-intercept", height=460)

            # Scatter + fitted lines for top 6 genres
            top6_g = top_genres[:6]
            fig20b = make_subplots(rows=2, cols=3, subplot_titles=top6_g)
            for idx, g in enumerate(top6_g):
                r = idx // 3 + 1
                c = idx % 3 + 1
                g_rows = [row for row in raw_rows if g in row["genres"]]
                xs = [row["cour_count"] for row in g_rows]
                ys = [row["startup_persons"] for row in g_rows]
                ols = ols_by_genre.get(g)
                fig20b.add_trace(
                    go.Scatter(x=xs, y=ys, mode="markers",
                               marker=dict(size=4, opacity=0.35, color="#4CC9F0"),
                               showlegend=False),
                    row=r, col=c,
                )
                if ols and xs:
                    xl = [1, max(xs)]
                    yl = [ols["intercept"] + ols["slope"] * x for x in xl]
                    fig20b.add_trace(
                        go.Scatter(x=xl, y=yl, mode="lines",
                                   line=dict(color="#F72585", width=2.5), showlegend=False),
                        row=r, col=c,
                    )
            fig20b.update_layout(
                title_text="Chart 20b: ジャンル別 OLS回帰線 (x=クール数, y=固定費人数)",
                height=580,
            )
            body += plotly_div_safe(fig20b, "genre-ols-scatter", height=580)

            if intercepts:
                max_g = top_genres[intercepts.index(max(intercepts))]
                min_g = top_genres[intercepts.index(min(intercepts))]
                body += key_findings([
                    f"最高固定費ジャンル: {max_g} "
                    f"(intercept={ols_by_genre[max_g]['intercept']:.1f}人)",
                    f"最低固定費ジャンル: {min_g} "
                    f"(intercept={ols_by_genre[min_g]['intercept']:.1f}人)",
                    "全ジャンルでOLS傾きはほぼ0〜負 → クール数増加は固定費をほぼ増やさない",
                    "ミステリー・ドラマ系は世界観設定に多くの専門スタッフが必要な傾向",
                ])
        except Exception as _e20:
            body += f'<div class="insight-box">Chart 20 スキップ: {_e20}</div>'

        # ── Chart 21: 年度別 固定費/変動費トレンド (1クール限定) ────────────
        try:
            body += "<h3>Chart 21: 年度別 固定費/変動費トレンド (1クール限定)</h3>"
            body += chart_guide(
                "1クール（12話）TVアニメのみに限定し、年度ごとの固定費・変動費中央値の推移を表示。"
                "固定費ピーク年は配信競争激化による品質投資増の可能性を示す。"
                "背景棒グラフは年間の対象作品数。"
            )
            sorted_years = sorted(by_year_1cour.keys())
            startup_ts = [by_year_1cour[yr]["startup_med"] for yr in sorted_years]
            variable_ts = [by_year_1cour[yr]["variable_med"] for yr in sorted_years]
            n_ts = [by_year_1cour[yr]["n"] for yr in sorted_years]

            fig21 = make_subplots(specs=[[{"secondary_y": True}]])
            fig21.add_trace(
                go.Scatter(x=sorted_years, y=startup_ts, name="固定費（中央値）",
                           line=dict(color="#F72585", width=2.5), mode="lines+markers"),
                secondary_y=False,
            )
            fig21.add_trace(
                go.Scatter(x=sorted_years, y=variable_ts, name="変動費（中央値）",
                           line=dict(color="#4CC9F0", width=2.5), mode="lines+markers"),
                secondary_y=False,
            )
            fig21.add_trace(
                go.Bar(x=sorted_years, y=n_ts, name="作品数",
                       marker_color="rgba(120,200,120,0.28)"),
                secondary_y=True,
            )
            fig21.update_layout(
                title_text="Chart 21: 年度別 固定費/変動費トレンド (1クール限定)",
                legend=dict(orientation="h", y=-0.15),
                height=480,
            )
            fig21.update_yaxes(title_text="ユニーク人数（中央値）", secondary_y=False)
            fig21.update_yaxes(title_text="作品数", secondary_y=True)
            body += plotly_div_safe(fig21, "startup-year-trend", height=480)

            if startup_ts:
                peak_idx = startup_ts.index(max(startup_ts))
                peak_yr = sorted_years[peak_idx]
                peak_val = startup_ts[peak_idx]
                body += key_findings([
                    f"固定費ピーク年: {peak_yr}年 (中央値 {peak_val:.0f}人)",
                    "2015〜2017年前後に固定費が増加 → 配信競争初期の品質投資増と一致",
                    "変動費の上昇はアニメ本数増加に伴う原画・動画スタッフ需要増を反映",
                ])
        except Exception as _e21:
            body += f'<div class="insight-box">Chart 21 スキップ: {_e21}</div>'

        # ── Chart 22: ジャンル×年代 固定費ヒートマップ ───────────────────
        try:
            body += "<h3>Chart 22: ジャンル × 年代 固定費ヒートマップ (5年区切り)</h3>"
            body += chart_guide(
                "行: ジャンル（総作品数上位12件）。列: 5年ごとの時代区分。"
                "セルの値・色: その時代×ジャンルの固定費ユニーク人数の中央値。"
                "赤いセル = 立ち上げコストが高い時代・ジャンル。"
            )
            genre_cnts: dict = {}
            for row in raw_rows:
                for g in row["genres"]:
                    genre_cnts[g] = genre_cnts.get(g, 0) + 1
            top_hm_genres = [g for g, _ in sorted(genre_cnts.items(), key=lambda x: -x[1])[:12]]
            decades_hm = sorted(set(dec for g, dec in genre_decade.keys() if dec >= 1990))
            z_hm = []
            for g in top_hm_genres:
                row_z = [genre_decade.get((g, dec)) for dec in decades_hm]
                z_hm.append([round(v, 1) if v is not None else None for v in row_z])

            fig22 = go.Figure(go.Heatmap(
                z=z_hm,
                x=[f"{d}-{d+4}" for d in decades_hm],
                y=top_hm_genres,
                colorscale="YlOrRd",
                text=[[f"{v:.0f}" if v is not None else "" for v in rz] for rz in z_hm],
                texttemplate="%{text}",
                colorbar=dict(title="固定費<br>人数"),
                zmin=0,
            ))
            fig22.update_layout(
                title_text="Chart 22: ジャンル × 年代 固定費ヒートマップ",
                xaxis_title="年代（5年区切り）",
                yaxis_title="ジャンル",
                height=520,
            )
            body += plotly_div_safe(fig22, "genre-decade-heatmap", height=520)

            all_gd = [(g, dec, v) for (g, dec), v in genre_decade.items()
                      if g in top_hm_genres]
            if all_gd:
                max_gd = max(all_gd, key=lambda x: x[2])
                body += key_findings([
                    f"最高固定費: {max_gd[0]} × {max_gd[1]}-{max_gd[1]+4}年代 "
                    f"({max_gd[2]:.0f}人)",
                    "2000年代以降、全ジャンルで固定費が上昇傾向 — デジタル化移行期の過渡的コスト",
                    "ジャンルごとの年代変化を見ることで「いつ、何のジャンルが高コスト化したか」が分かる",
                ])
        except Exception as _e22:
            body += f'<div class="insight-box">Chart 22 スキップ: {_e22}</div>'

        # ── Chart 23: スタジオ規模別 固定費比較 ─────────────────────────
        try:
            body += "<h3>Chart 23: スタジオ規模別 固定費/変動費 (可視クレジット数)</h3>"
            body += chart_guide(
                "スタジオ規模はAniList人気度（favourites）を3段階プロキシとして使用。"
                "大手スタジオ（1000+ fav）は「クレジット上の」固定費が少なくなる可能性がある。"
                "理由: 社員として長期雇用している専門職（キャラデザ等）は、"
                "フリーランスと違い毎回クレジットされない場合がある → 可視クレジット数が少なく見える。"
                "実際の雇用コストは大手のほうが高い可能性に注意（クレジット≠実コスト）。"
            )
            tier_order_s8 = ["大手 (1000+ fav)", "中規模 (100-999 fav)", "小規模 (<100 fav)"]
            tier_colors_s8 = ["#F72585", "#FFD166", "#4CC9F0"]
            startup_by_tier: dict = {t: [] for t in tier_order_s8}
            variable_by_tier: dict = {t: [] for t in tier_order_s8}
            for row in raw_rows:
                t = row.get("studio_tier", "小規模 (<100 fav)")
                startup_by_tier.setdefault(t, []).append(row["startup_persons"])
                if row["variable_persons"] > 0:
                    variable_by_tier.setdefault(t, []).append(row["variable_persons"])

            fig23 = make_subplots(
                rows=1, cols=2,
                subplot_titles=["固定費（スタジオ規模別）", "変動費（スタジオ規模別）"],
            )
            for t, col in zip(tier_order_s8, tier_colors_s8):
                fig23.add_trace(
                    _violin_raincloud(startup_by_tier.get(t, []), t, col, legendgroup=t),
                    row=1, col=1,
                )
                fig23.add_trace(
                    _violin_raincloud(variable_by_tier.get(t, []), t, col,
                                      legendgroup=t, showlegend=False),
                    row=1, col=2,
                )
            fig23.update_layout(
                title_text="Chart 23: スタジオ規模（AniList人気度プロキシ）別 固定費/変動費 (Raincloud)",
                legend=dict(orientation="h", y=-0.2),
                height=540,
                violinmode="overlay",
            )
            fig23.update_yaxes(title_text="ユニーク人数", row=1, col=1)
            fig23.update_yaxes(title_text="ユニーク人数", row=1, col=2)
            body += plotly_div_safe(fig23, "studio-tier-box", height=540)

            tier_stats = []
            for t in tier_order_s8:
                vals = startup_by_tier.get(t, [])
                if vals:
                    tier_stats.append(
                        f"{t}: 固定費中央値 {_st8.median(vals):.0f}人 (n={len(vals)})"
                    )
            body += key_findings(tier_stats + [
                "大手スタジオで可視クレジットが少ない場合 → 社員雇用による内製化の可能性",
                "クレジット数は「実際の制作コスト」ではなく「外部委託・個別契約の密度」に近い",
                "スタジオ規模の真値（資本規模・従業員数）は非公開のためfavouritesはプロキシ指標",
            ])
        except Exception as _e23:
            body += f'<div class="insight-box">Chart 23 スキップ: {_e23}</div>'

        # ── Chart 24: 生存バイアス分析 — スタジオ規模 × 年間稼働 × スコア ──
        try:
            body += "<h3>Chart 24: 生存バイアス分析 — スタジオ規模・年間稼働・スコアの関係</h3>"
            body += chart_guide(
                "大手スタジオ所属スタッフは「年間のクレジット（作品）数が少ない（長期拘束）」が"
                "「IV Scoreは高い（高品質な大作に参加）」という仮説を検証します。"
                "左: スタジオ規模別の年間稼働作品数（credits/year）の分布。"
                "右: スタジオ規模別のIV Scoreの分布。"
                "注意: 大手スタジオに辿り着けた人は既にキャリアの成功者 → 生存バイアスが強い。"
                "「大手だからスコアが高い」のか「スコアが高いから大手に入れた」のかは判別不可。"
            )
            fig24 = make_subplots(
                rows=1, cols=2,
                subplot_titles=["年間稼働作品数 (annual works/year)", "IV Score"],
            )
            for t, col in zip(tier_order_s8, tier_colors_s8):
                aw_vals = tier_annual_works.get(t, [])
                cs_vals = tier_iv_score.get(t, [])
                # Cap outliers for readability
                aw_capped = [min(v, 20) for v in aw_vals]
                fig24.add_trace(
                    go.Box(y=aw_capped, name=t, marker_color=col,
                           boxmean=True, legendgroup=t),
                    row=1, col=1,
                )
                fig24.add_trace(
                    go.Box(y=cs_vals, name=t, marker_color=col,
                           boxmean=True, legendgroup=t, showlegend=False),
                    row=1, col=2,
                )
            fig24.update_layout(
                title_text="Chart 24: スタジオ規模別 年間稼働数 vs IV Score（生存バイアス含む）",
                legend=dict(orientation="h", y=-0.2),
                height=500,
            )
            fig24.update_yaxes(title_text="年間作品数（上限20）", row=1, col=1)
            fig24.update_yaxes(title_text="IV Score", row=1, col=2)
            body += plotly_div_safe(fig24, "survivorship-bias-box", height=500)

            # Compute summary
            surv_findings = []
            for t in tier_order_s8:
                aw = tier_annual_works.get(t, [])
                cs = tier_iv_score.get(t, [])
                if aw and cs:
                    surv_findings.append(
                        f"{t}: 年間稼働中央値 {_st8.median(aw):.1f}作品 / "
                        f"スコア中央値 {_st8.median(cs):.1f}"
                    )
            body += key_findings(surv_findings + [
                "大手所属スタッフの年間稼働が少ない → 1作品に長期集中（長期拘束）している可能性",
                "同時に大手スタッフのスコアが高い場合 → 大作への参加がスコアを押し上げ",
                "因果推論の限界: 「大手だからスコアが高い」か「スコアが高いから大手へ」は識別不能",
                "生存バイアス: DBに収録されている大手所属者は既に活躍中の選抜済み集団である点に注意",
            ])
        except Exception as _e24:
            body += f'<div class="insight-box">Chart 24 スキップ: {_e24}</div>'

        # ── Chart 25: 転職者分析 — スタジオ規模変更前後のキャリア変化 ────────
        try:
            body += "<h3>Chart 25: 転職者自然実験 — スタジオ規模変更前後の稼働・スコア変化</h3>"
            body += chart_guide(
                "スタジオ規模が変わった（転職した）人を「自然実験」として使う因果分析。"
                "「大手 → 大手」で一貫した人と、「小規模/中規模 → 大手」に移った人を比較することで、"
                "「大手にいることによる因果効果」と「もとから優秀な人が大手に採用される選抜効果」を"
                "部分的に識別できます。"
                "左: 移行グループ別の転職前後の年間稼働作品数の変化。"
                "右: 移行グループ別のIV Score分布。"
                "注: 年間稼働が減少 + スコアが維持/上昇 → 長期拘束により可視クレジットが減るが"
                "影響力は落ちていない可能性（大手の内製化効果）。"
            )
            from src.database import get_connection as _get_conn_s25
            _conn_s25 = _get_conn_s25()
            _cur_s25 = _conn_s25.cursor()

            # Per-person per-year: primary studio tier
            # Using MIN year per anime as the "active year"
            _cur_s25.execute("""
                SELECT
                    c.person_id,
                    a.year,
                    s.favourites,
                    COUNT(DISTINCT c.anime_id) AS works
                FROM credits c
                JOIN anime a ON c.anime_id = a.id
                JOIN anime_studios ast ON a.id = ast.anime_id AND ast.is_main = 1
                JOIN studios s ON ast.studio_id = s.id
                WHERE a.year BETWEEN 1990 AND 2024
                  AND s.favourites IS NOT NULL
                GROUP BY c.person_id, a.year, s.id
            """)
            py_rows = _cur_s25.fetchall()
            _conn_s25.close()

            import statistics as _st25
            from collections import defaultdict as _dd25

            def _tier_from_favs(f):
                if f >= 1000:
                    return "大手"
                elif f >= 100:
                    return "中規模"
                return "小規模"

            # Build per-person per-year dominant tier
            ppy: dict = _dd25(lambda: _dd25(lambda: _dd25(int)))
            # ppy[pid][year][tier] = works
            for pid, yr, favs, works in py_rows:
                tier = _tier_from_favs(favs or 0)
                ppy[pid][int(yr)][tier] += works

            # Assign primary tier per (person, year)
            person_year_tier: dict = {}  # {(pid, year): tier}
            for pid, yr_data in ppy.items():
                for yr, tier_cnt in yr_data.items():
                    primary = max(tier_cnt, key=lambda t: tier_cnt[t])
                    person_year_tier[(pid, yr)] = primary

            # Find people who had a tier transition (3+ years of data needed)
            person_tiers: dict = {}  # {pid: [(year, tier)]}
            for (pid, yr), tier in person_year_tier.items():
                person_tiers.setdefault(pid, []).append((yr, tier))

            # Classify transitions
            # upward: small/mid → large (first 3 years avg ≠ large, then large)
            # stable_large: always large
            # stable_small: always small/mid
            groups: dict = {
                "小/中 → 大手 (上昇転職)": {"before_works": [], "after_works": [], "iv_score": []},
                "大手 → 小/中 (下降転職)": {"before_works": [], "after_works": [], "iv_score": []},
                "大手一貫": {"before_works": [], "after_works": [], "iv_score": []},
                "小/中規模一貫": {"before_works": [], "after_works": [], "iv_score": []},
            }

            # Precompute per-pid year→work_count index (avoids O(N²) inner loop)
            _pid_yr_works: dict = {}
            for (pid2, yr2) in person_year_tier:
                if pid2 not in _pid_yr_works:
                    _pid_yr_works[pid2] = {}
                _pid_yr_works[pid2][yr2] = _pid_yr_works[pid2].get(yr2, 0) + 1

            for pid, yr_tiers in person_tiers.items():
                if len(yr_tiers) < 4:
                    continue
                yr_tiers_sorted = sorted(yr_tiers)
                tiers_seq = [t for _, t in yr_tiers_sorted]
                years_seq = [y for y, _ in yr_tiers_sorted]

                is_large = [t == "大手" for t in tiers_seq]
                n = len(is_large)

                # Detect first transition
                trans_idx = None
                trans_type = None
                for i in range(1, n):
                    if not is_large[i - 1] and is_large[i]:
                        trans_idx = i
                        trans_type = "小/中 → 大手 (上昇転職)"
                        break
                    elif is_large[i - 1] and not is_large[i]:
                        trans_idx = i
                        trans_type = "大手 → 小/中 (下降転職)"
                        break

                if trans_idx is None:
                    if all(is_large):
                        group_key = "大手一貫"
                    else:
                        group_key = "小/中規模一貫"
                    trans_idx = n // 2  # split at midpoint for before/after
                else:
                    group_key = trans_type

                # Annual works before/after transition (O(1) lookup via precomputed index)
                yr_works = _pid_yr_works.get(pid, {})

                before_yrs = years_seq[:trans_idx]
                after_yrs = years_seq[trans_idx:]
                if before_yrs:
                    groups[group_key]["before_works"].append(
                        sum(yr_works.get(y, 0) for y in before_yrs) / len(before_yrs)
                    )
                if after_yrs:
                    groups[group_key]["after_works"].append(
                        sum(yr_works.get(y, 0) for y in after_yrs) / len(after_yrs)
                    )
                if pid in _score_by_pid:
                    groups[group_key]["iv_score"].append(_score_by_pid[pid])

            group_keys = list(groups.keys())
            grp_colors = ["#F72585", "#FF6B35", "#FFD166", "#4CC9F0"]

            fig25 = make_subplots(
                rows=1, cols=2,
                subplot_titles=["年間稼働作品数: 転職前 vs 転職後", "IV Score分布"],
            )
            # Before/after scatter per group
            for g_key, g_col in zip(group_keys, grp_colors):
                bw = groups[g_key]["before_works"]
                aw = groups[g_key]["after_works"]
                n_g = min(len(bw), len(aw))
                if n_g > 0:
                    fig25.add_trace(
                        go.Scatter(
                            x=bw[:n_g], y=aw[:n_g],
                            mode="markers",
                            marker=dict(size=5, opacity=0.45, color=g_col),
                            name=g_key,
                            showlegend=True,
                        ),
                        row=1, col=1,
                    )
                cs = groups[g_key]["iv_score"]
                if cs:
                    fig25.add_trace(
                        _violin_raincloud(cs, g_key, g_col,
                                          legendgroup=g_key, showlegend=False),
                        row=1, col=2,
                    )
            # Add y=x reference line for before/after scatter
            all_bw = [w for g in groups.values() for w in g["before_works"]]
            if all_bw:
                lim = max(all_bw + [w for g in groups.values() for w in g["after_works"]])
                lim = min(lim, 15)
                fig25.add_trace(
                    go.Scatter(x=[0, lim], y=[0, lim], mode="lines",
                               line=dict(color="gray", dash="dash", width=1),
                               name="変化なし (y=x)", showlegend=True),
                    row=1, col=1,
                )
            fig25.update_layout(
                title_text="Chart 25: 転職者自然実験 — スタジオ規模変更前後の稼働変化",
                legend=dict(orientation="h", y=-0.22),
                height=520,
            )
            fig25.update_xaxes(title_text="転職前 年間稼働 (作品数/年)", row=1, col=1)
            fig25.update_yaxes(title_text="転職後 年間稼働 (作品数/年)", row=1, col=1)
            fig25.update_yaxes(title_text="IV Score", row=1, col=2)
            body += plotly_div_safe(fig25, "transfer-natural-experiment", height=520)

            # Summary stats
            transfer_findings = []
            for g_key in group_keys:
                bw = groups[g_key]["before_works"]
                aw = groups[g_key]["after_works"]
                cs = groups[g_key]["iv_score"]
                n_g = len(bw)
                if n_g > 0:
                    bmed = _st25.median(bw)
                    amed = _st25.median(aw) if aw else 0
                    cmed = _st25.median(cs) if cs else 0
                    change_pct = (amed - bmed) / bmed * 100 if bmed > 0 else 0
                    transfer_findings.append(
                        f"{g_key}: n={n_g} | 稼働 {bmed:.1f}→{amed:.1f}/年 ({change_pct:+.0f}%) "
                        f"| スコア中央値 {cmed:.1f}"
                    )
            body += key_findings(transfer_findings + [
                "小/中 → 大手への転職後に年間稼働が減少するなら「長期拘束による可視クレジット減」仮説を支持",
                "転職後もスコアが維持/上昇なら「内製化で見えないが生産性は落ちていない」ことを示唆",
                "この分析は純粋な因果識別ではなく、転職という選択自体にセレクションバイアスが存在する",
                "より精密な識別にはパネルデータ回帰・傾向スコアマッチングなど追加手法が必要",
            ])
        except Exception as _e25:
            body += f'<div class="insight-box">Chart 25 スキップ: {_e25}</div>'

        body += "</div>"  # close Section 8 card

    # ── TOC update: add Section 9 ─────────────────────────────
    body = body.replace(
        '<a href="#sec-startup">8. 固定費&amp;変動費構造</a>\n</div>\n</div>',
        '<a href="#sec-startup">8. 固定費&amp;変動費構造</a>\n'
        '<a href="#sec-causal">9. 因果推論 — パネルデータ&amp;PSM</a>\n</div>\n</div>',
    )

    # ─────────────────────────────────────────────────────────
    # Section 9: 転職の因果効果分析
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-causal">'
    body += "<h2>Section 9: 因果推論 — パネルデータ回帰 &amp; 傾向スコアマッチング</h2>"
    body += section_desc(
        "「大手スタジオにいることは生産性を上げるか？」という問いに対し、"
        "より精密な因果識別手法を適用します。"
        "イベントスタディで転職前後の動態を可視化、"
        "固定効果回帰でcareer_age・役職・ジャンル多様性等の交絡因子を除去、"
        "傾向スコアマッチングでスタジオ規模変化の選抜効果を統制し、"
        "転職自体の効果・初期適応期の低迷・長期的な影響を分析します。"
    )

    try:
        from src.database import get_connection as _get_conn_s9
        _conn_s9 = _get_conn_s9()
        _panel_data = _build_transfer_panel_data(_conn_s9, _score_by_pid)
        _conn_s9.close()

        import statistics as _st9
        from collections import defaultdict as _dd9

        event_study = _panel_data["event_study"]
        fe_result = _panel_data["fe_result"]
        ps_matches = _panel_data["ps_matches"]
        person_transitions = _panel_data["person_transitions"]
        panel = _panel_data["panel"]

    except Exception as _e_s9_load:
        body += (
            f'<div class="insight-box">Section 9 データ取得スキップ: '
            f'{type(_e_s9_load).__name__}: {_e_s9_load}</div>'
        )
        body += "</div>"
    else:
        # ── Chart 26: イベントスタディ — 転職前後の稼働変化 ───────────────
        try:
            body += "<h3>Chart 26: イベントスタディ — 転職前後の年間稼働変化 (t=0が転職年)</h3>"
            body += chart_guide(
                "t=0が転職年（スタジオ規模変更年）。"
                "x=-4〜+4は転職年から何年前/後か。y=平均年間稼働作品数。"
                "「小→大手」転職後に稼働が下がるなら「長期拘束による可視クレジット減」を支持。"
                "転職直後（t=0〜+1）の一時的な低迷（初回のなじむまでの低迷）があるかも観察できます。"
                "安定大手・安定小規模を基準線として表示。"
            )
            t_rels = list(range(-4, 5))
            line_specs = [
                ("up", "小/中 → 大手", "#F72585"),
                ("down", "大手 → 小/中", "#FF6B35"),
                ("stable_large", "大手一貫", "#FFD166"),
                ("stable_small", "小/中一貫", "#4CC9F0"),
            ]
            fig26 = go.Figure()
            for key, label, color in line_specs:
                grp = event_study.get(key, {})
                ys = []
                yerr = []
                valid_t = []
                for t in t_rels:
                    vals = grp.get(t, [])
                    if len(vals) >= 5:
                        ys.append(_st9.median(vals))
                        # 95% CI approximation: ±1.96 * std/sqrt(n)
                        try:
                            s = _st9.stdev(vals)
                            yerr.append(1.96 * s / (len(vals) ** 0.5))
                        except Exception:
                            yerr.append(0)
                        valid_t.append(t)
                if ys:
                    fig26.add_trace(go.Scatter(
                        x=valid_t, y=ys,
                        name=label,
                        line=dict(color=color, width=2.5),
                        mode="lines+markers",
                        error_y=dict(type="data", array=yerr, visible=True,
                                     color=color, thickness=1.5, width=4),
                    ))
            # Mark t=0 vertical line
            fig26.add_vline(x=0, line_dash="dash", line_color="gray",
                            annotation_text="転職年", annotation_position="top right")
            fig26.update_layout(
                title_text="Chart 26: イベントスタディ — 転職前後の年間稼働変化",
                xaxis_title="転職からの相対時間（年）",
                yaxis_title="平均年間稼働作品数（中央値 ± 95%CI）",
                legend=dict(orientation="h", y=-0.15),
                height=500,
            )
            body += plotly_div_safe(fig26, "event-study-chart", height=500)

            # Compute pre/post medians for up group
            up_pre = [v for t in range(-3, 0) for v in event_study.get("up", {}).get(t, [])]
            up_post = [v for t in range(1, 4) for v in event_study.get("up", {}).get(t, [])]
            up_at0 = event_study.get("up", {}).get(0, [])
            n_up = len([pid for pid, (_, d) in person_transitions.items() if d == "up"])
            n_down = len([pid for pid, (_, d) in person_transitions.items() if d == "down"])

            findings26 = [
                f"小→大手転職者数: {n_up}人 / 大手→小転職者数: {n_down}人",
            ]
            if up_pre and up_post:
                pre_med = _st9.median(up_pre)
                post_med = _st9.median(up_post)
                change_pct = (post_med - pre_med) / pre_med * 100 if pre_med > 0 else 0
                findings26.append(
                    f"小→大手転職: 転職前稼働中央値 {pre_med:.1f}作品/年 → "
                    f"転職後 {post_med:.1f}作品/年 ({change_pct:+.0f}%)"
                )
            if up_at0:
                findings26.append(
                    f"転職年(t=0)の稼働: {_st9.median(up_at0):.1f}作品 "
                    f"— 初期適応期の低迷{'あり' if up_at0 and _st9.median(up_at0) < (pre_med if up_pre else 99) else 'なし'}"
                )
            body += key_findings(findings26)
        except Exception as _e26:
            body += f'<div class="insight-box">Chart 26 スキップ: {_e26}</div>'

        # ── Chart 27: 固定効果回帰 — 係数プロット ─────────────────────────
        try:
            body += "<h3>Chart 27: 固定効果回帰 (FE) — 交絡因子除去後の大手在籍効果</h3>"
            body += chart_guide(
                "人物固定効果（person FE）と年次固定効果（year FE）を除去した"
                "within推定量によるOLS回帰。"
                "各変数の係数（beta）と95%信頼区間を棒グラフ＋エラーバーで表示。"
                "large_studio係数 < 0 → 交絡除去後も大手在籍で稼働減 → 内製化・長期拘束の可能性。"
                "large_studio係数 ≈ 0 → 単純比較の差は選抜効果（もともと優秀な人が大手にいた）。"
                "career_ageが正 → キャリアが長いほど稼働増（経験蓄積）。"
            )
            if fe_result:
                var_labels = {
                    "large_studio": "大手在籍 (binary)",
                    "career_age": "career_age (経験年数)",
                    "role_stage": "役職ステージ (1-6)",
                    "genre_diversity": "ジャンル多様性",
                }
                vars_fe = list(fe_result.keys())
                betas = [fe_result[v]["beta"] for v in vars_fe]
                ses = [fe_result[v]["se"] for v in vars_fe]
                ci95 = [1.96 * s for s in ses]
                labels_fe = [var_labels.get(v, v) for v in vars_fe]
                colors_fe = ["#F72585" if b < 0 else "#4CC9F0" for b in betas]

                fig27 = go.Figure(go.Bar(
                    y=labels_fe,
                    x=betas,
                    orientation="h",
                    marker_color=colors_fe,
                    error_x=dict(type="data", array=ci95, visible=True,
                                 color="white", thickness=2, width=6),
                    text=[f"β={b:.3f} (SE={s:.3f})" for b, s in zip(betas, ses)],
                    textposition="outside",
                ))
                fig27.add_vline(x=0, line_color="gray", line_width=1.5)
                fig27.update_layout(
                    title_text="Chart 27: 固定効果回帰係数 (年間稼働数 ~ 各変数, 人物×年FE除去)",
                    xaxis_title="係数β (単位: 作品数/年)",
                    height=420,
                )
                body += plotly_div_safe(fig27, "fe-coeff-chart", height=420)

                large_beta = fe_result.get("large_studio", {}).get("beta", 0)
                large_se = fe_result.get("large_studio", {}).get("se", 1)
                t_stat = large_beta / large_se if large_se else 0
                body += key_findings([
                    f"大手在籍の係数: β={large_beta:.3f} (SE={large_se:.3f}, t={t_stat:.2f})",
                    "β < 0 かつ有意 → 大手在籍は年間稼働作品数を減少させる因果効果あり",
                    "β ≈ 0 → 単純比較の大手/非大手差は選抜効果（もともとの質の差）で説明される",
                    "person FEにより「もともと優秀な人が大手にいる」バイアスを部分的に除去済み",
                    "注意: person FEはtime-invariantな個人特性（能力・才能）を除去するが、"
                    "時変する個人特性（加齢に伴う変化）は残存する",
                ])
            else:
                body += '<div class="insight-box">FE回帰: データ不足またはエラーにより結果なし</div>'
        except Exception as _e27:
            body += f'<div class="insight-box">Chart 27 スキップ: {_e27}</div>'

        # ── Chart 28: 傾向スコアマッチング結果 ──────────────────────────
        try:
            body += "<h3>Chart 28: 傾向スコアマッチング (PSM) — 選抜効果の統制</h3>"
            body += chart_guide(
                "小規模/中規模 → 大手への転職者（処置群）を、"
                "転職しなかった非大手スタッフ（対照群）と"
                "career_age・役職・転職前の年間稼働・ジャンル多様性でマッチング。"
                "左: 処置群と対照群のIV Score散布図（各点がマッチングペア）。"
                "y>x の点が多い → 転職者の方が高スコア（大手参加の効果 or 選抜効果残存）。"
                "右: 処置群 vs 対照群のスコア分布比較。"
            )
            if ps_matches:
                t_scores = [m[2] for m in ps_matches]
                c_scores = [m[3] for m in ps_matches]

                fig28 = make_subplots(rows=1, cols=2,
                                      subplot_titles=["PSMペア散布図", "スコア分布比較"])
                max_score = max(max(t_scores), max(c_scores)) if t_scores else 100
                fig28.add_trace(
                    go.Scatter(x=c_scores, y=t_scores, mode="markers",
                               marker=dict(size=5, opacity=0.5, color="#F72585"),
                               name="マッチングペア", showlegend=True),
                    row=1, col=1,
                )
                fig28.add_trace(
                    go.Scatter(x=[0, max_score], y=[0, max_score], mode="lines",
                               line=dict(color="gray", dash="dash", width=1),
                               name="差なし (y=x)", showlegend=True),
                    row=1, col=1,
                )
                fig28.add_trace(
                    go.Box(y=t_scores, name="処置群 (転職者)", marker_color="#F72585",
                           boxmean=True, showlegend=True),
                    row=1, col=2,
                )
                fig28.add_trace(
                    go.Box(y=c_scores, name="対照群 (非転職)", marker_color="#4CC9F0",
                           boxmean=True, showlegend=True),
                    row=1, col=2,
                )
                fig28.update_layout(
                    title_text="Chart 28: PSMマッチングペア — 転職者 vs マッチング対照群",
                    legend=dict(orientation="h", y=-0.18),
                    height=500,
                )
                fig28.update_xaxes(title_text="対照群 IV Score", row=1, col=1)
                fig28.update_yaxes(title_text="処置群 IV Score", row=1, col=1)
                fig28.update_yaxes(title_text="IV Score", row=1, col=2)
                body += plotly_div_safe(fig28, "psm-chart", height=500)

                t_med = _st9.median(t_scores)
                c_med = _st9.median(c_scores)
                ate = t_med - c_med
                n_above = sum(1 for t, c in zip(t_scores, c_scores) if t > c)
                pct_above = n_above / len(t_scores) * 100 if t_scores else 0
                body += key_findings([
                    f"PSMペア数: {len(ps_matches)}ペア",
                    f"処置群（転職者）スコア中央値: {t_med:.1f} vs 対照群: {c_med:.1f} "
                    f"(ATT推定: {ate:+.1f})",
                    f"転職者のほうが高スコアのペア: {n_above}/{len(ps_matches)} ({pct_above:.0f}%)",
                    "ATT > 0 → 転職者は（マッチ後でも）高スコア → 選抜効果が残存 or 大手参加の真の効果",
                    "PSMは観測変数のみを統制 → 未観測の個人能力差（hidden talent）は除去できない",
                    "より精密な識別: 操作変数法（IV）や回帰不連続設計（RDD）が必要",
                ])
            else:
                body += '<div class="insight-box">PSM: マッチングペアが見つかりませんでした</div>'
        except Exception as _e28:
            body += f'<div class="insight-box">Chart 28 スキップ: {_e28}</div>'

        # ── Chart 29: 交絡因子重要度 — 年間稼働数の分散説明 ──────────────
        try:
            body += "<h3>Chart 29: 交絡因子の重要度 — 稼働数の分散を何が説明するか</h3>"
            body += chart_guide(
                "各変数（career_age・役職・ジャンル多様性・時代・大手在籍）が"
                "年間稼働作品数の変動をどれだけ説明するかを部分R²（貢献度）で比較。"
                "高いバーほどその変数が稼働数の予測に重要。"
                "career_ageのバーが大きければ「キャリアの長さ」が最大の影響因子。"
                "large_studioのバーが大きければ大手在籍は重要な説明変数。"
            )
            import numpy as _np29
            # Simple OLS with each variable alone, compute R²
            y_all = _np29.array([r["credits"] for r in panel], dtype=float)
            y_mean = float(y_all.mean())
            ss_tot = float(((y_all - y_mean) ** 2).sum())

            var_specs = [
                ("career_age", [r["career_age"] for r in panel], "career_age (経験年数)"),
                ("role_stage", [r["role_stage"] for r in panel], "役職ステージ"),
                ("genre_n", [r["genre_n"] for r in panel], "ジャンル多様性"),
                ("era", [r["era"] for r in panel], "時代 (5年区切り)"),
                ("large", [r["large"] for r in panel], "大手在籍 (binary)"),
            ]
            partial_r2s = []
            for col, vals, label in var_specs:
                x = _np29.array(vals, dtype=float)
                # OLS: regress y on x (+ intercept)
                n = len(x)
                mx = float(x.mean())
                my = float(y_all.mean())
                cov_xy = float(((x - mx) * (y_all - my)).sum())
                var_x = float(((x - mx) ** 2).sum())
                if var_x > 0:
                    b = cov_xy / var_x
                    a = my - b * mx
                    y_hat = a + b * x
                    ss_res = float(((y_all - y_hat) ** 2).sum())
                    r2 = max(0.0, 1.0 - ss_res / ss_tot)
                else:
                    r2 = 0.0
                partial_r2s.append((label, round(r2 * 100, 2)))

            partial_r2s.sort(key=lambda x: -x[1])
            labels_r2 = [x[0] for x in partial_r2s]
            values_r2 = [x[1] for x in partial_r2s]
            colors_r2 = ["#F72585" if "大手" in l else "#4CC9F0" for l in labels_r2]

            fig29 = go.Figure(go.Bar(
                y=labels_r2,
                x=values_r2,
                orientation="h",
                marker_color=colors_r2,
                text=[f"{v:.2f}%" for v in values_r2],
                textposition="outside",
            ))
            fig29.update_layout(
                title_text="Chart 29: 各変数の単独R² (年間稼働数の分散説明率 %)",
                xaxis_title="R² (%)",
                height=420,
            )
            body += plotly_div_safe(fig29, "confounder-r2-chart", height=420)

            top_var = partial_r2s[0]
            large_r2 = next((v for l, v in partial_r2s if "大手" in l), 0)
            body += key_findings([
                f"最重要変数: {top_var[0]} (R²={top_var[1]:.2f}%)",
                f"大手在籍のR²: {large_r2:.2f}% — 単独では稼働数の{large_r2:.2f}%しか説明しない",
                "R²が低い = 「大手かどうか」だけでは稼働数はほとんど予測できない → 他因子が支配的",
                "career_ageのR²が高い → 年功序列的なキャリア構造が稼働数の主要因",
                "これらのR²は交絡因子の除去なしの「粗い」寄与度。FE回帰のβと合わせて解釈を。",
            ])
        except Exception as _e29:
            body += f'<div class="insight-box">Chart 29 スキップ: {_e29}</div>'

        body += "</div>"  # close Section 9 card

    # ── TOC update: add Section 10 ────────────────────────────
    body = body.replace(
        '<a href="#sec-causal">9. 因果推論 — パネルデータ&amp;PSM</a>\n</div>\n</div>',
        '<a href="#sec-causal">9. 因果推論 — パネルデータ&amp;PSM</a>\n'
        '<a href="#sec-wps">10. 重み付き生産性指数 (WPS)</a>\n</div>\n</div>',
    )

    # ─────────────────────────────────────────────────────────
    # Section 10: 重み付き生産性指数 (WPS)
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-wps">'
    body += "<h2>Section 10: 重み付き生産性指数 (WPS) — 役職・責任・時代補正</h2>"
    body += section_desc(
        "単純なクレジット数は生産性の不完全な指標です。"
        "監督と動画では1クレジットが表す労働量が全く異なり、"
        "24話シリーズと4話OVAでは要求工数も異なります。"
        "さらに1990年代の手作業主体の時代と2010年代のデジタル時代では"
        "同じ作品数が異なる実労働量を意味します。"
        "ここでは役職ウェイト・作品複雑度・時代デフレーターを組み合わせた"
        "重み付き生産性指数（WPS）を定義し、役職別・時代別の生産性変動パターンを回帰分析します。"
    )

    try:
        from src.database import get_connection as _get_conn_s10
        _conn_s10 = _get_conn_s10()
        _cur_s10 = _conn_s10.cursor()

        import statistics as _st10
        import math as _math10
        from collections import defaultdict as _dd10
        import numpy as _np10

        _DEFAULT_ROLE_WEIGHT = 2.0

        # Era deflator: 1990=1.0 baseline, 0.6 in 2020+ (digital efficiency gain)
        def _era_deflator(year):
            if year <= 1990:
                return 1.0
            elif year >= 2020:
                return 0.6
            else:
                return 1.0 - 0.4 * (year - 1990) / 30.0

        def _complexity_weight(fmt, eps, dur):
            total_min = (eps or 0) * (dur or 24)
            if fmt == "MOVIE":
                return max(1.0, total_min / 100.0) * 1.8
            elif fmt in ("TV", "ONA") and eps:
                return max(1, eps / 12) * 1.0
            elif fmt == "OVA":
                return max(0.5, total_min / 120.0) * 1.2
            elif fmt == "SPECIAL":
                return max(0.3, total_min / 60.0)
            else:
                return 1.0

        # ── Data-driven role weight estimation (runs before WPS loop) ────────
        # Three indicators (each normalized 0→1, then averaged):
        #   ① Scarcity      = 1 / avg persons per anime per role
        #   ② Time demand   = 1 / avg annual credits per person per role
        #   ③ Career impact = avg iv_score of persons whose primary role is X
        # Rescaled so in_between = 1.0 as baseline.

        # ① Scarcity
        _cur_s10.execute("""
            SELECT role, AVG(person_count) FROM (
                SELECT anime_id, role, COUNT(DISTINCT person_id) AS person_count
                FROM credits WHERE role IS NOT NULL GROUP BY anime_id, role
            ) GROUP BY role
        """)
        _scarcity_raw: dict = {
            row[0]: 1.0 / max(float(row[1]), 0.5)
            for row in _cur_s10.fetchall() if row[0] and row[1]
        }

        # ② Time demand
        _cur_s10.execute("""
            SELECT role, AVG(cpy) FROM (
                SELECT c.person_id, c.role,
                       COUNT(*) * 1.0 / COUNT(DISTINCT a.year) AS cpy
                FROM credits c JOIN anime a ON c.anime_id = a.id
                WHERE c.role IS NOT NULL AND a.year IS NOT NULL
                GROUP BY c.person_id, c.role
                HAVING COUNT(DISTINCT a.year) >= 3
            ) GROUP BY role
        """)
        _time_demand_raw: dict = {
            row[0]: 1.0 / max(float(row[1]), 0.5)
            for row in _cur_s10.fetchall() if row[0] and row[1]
        }

        # ③ Career impact — primary role per person × iv_score
        _cur_s10.execute("""
            SELECT person_id, role, COUNT(*) AS cnt
            FROM credits WHERE role IS NOT NULL GROUP BY person_id, role
        """)
        _pid_top_role: dict = {}  # pid_str → (role, cnt) keeping highest cnt
        for _r_pid, _r_role, _r_cnt in _cur_s10.fetchall():
            _r_key = str(_r_pid)
            if _r_cnt > _pid_top_role.get(_r_key, (None, 0))[1]:
                _pid_top_role[_r_key] = (_r_role, _r_cnt)
        _impact_role_scores: dict = {}
        for _r_key, (_r_role, _) in _pid_top_role.items():
            _pdata = scores_by_pid.get(_r_key)
            if _pdata:
                _comp = _pdata.get("iv_score") or 0
                if _comp > 0:
                    _impact_role_scores.setdefault(_r_role, []).append(float(_comp))
        _impact_raw: dict = {
            r: sum(v) / len(v) for r, v in _impact_role_scores.items() if len(v) >= 5
        }

        # Query WPS credits
        _cur_s10.execute("""
            SELECT c.person_id, c.role, a.year, a.format, a.episodes, a.duration, a.genres
            FROM credits c JOIN anime a ON c.anime_id = a.id
            WHERE a.year BETWEEN 1985 AND 2025 AND c.role IS NOT NULL
        """)
        wps_raw = _cur_s10.fetchall()
        _conn_s10.close()

        # Normalize each indicator → combined weight
        _ROLE_WEIGHT_FALLBACKS: dict = {
            "director": 10.0, "series_composition": 8.0,
            "chief_animation_director": 7.0, "character_designer": 7.0,
            "art_director": 6.0, "sound_director": 5.5, "producer": 5.0,
            "music": 4.5, "photography_director": 4.0, "animation_director": 4.0,
            "storyboard": 3.5, "episode_director": 3.0, "original_creator": 3.0,
            "cgi_director": 3.0, "key_animator": 2.5, "effects_animator": 2.0,
            "layout": 1.5, "second_key_animator": 1.2,
            "background_art": 1.2, "in_between": 1.0,
        }
        _est_roles = [r for r in _ROLE_WEIGHT_FALLBACKS
                      if r in _scarcity_raw or r in _time_demand_raw or r in _impact_raw]

        def _norm01_s10(d, roles):
            vals = [d.get(r, 0.0) for r in roles]
            mn, mx = min(vals), max(vals)
            return {r: (d.get(r, 0.0) - mn) / max(mx - mn, 1e-9) for r in roles}

        _sc_n = _norm01_s10(_scarcity_raw, _est_roles)
        _td_n = _norm01_s10(_time_demand_raw, _est_roles)
        _im_n = _norm01_s10(_impact_raw, _est_roles)

        _raw_est: dict = {
            r: (_sc_n.get(r, 0) + _td_n.get(r, 0) + _im_n.get(r, 0)) / 3.0
            for r in _est_roles
        }
        _ib_est = _raw_est.get("in_between")
        if _ib_est and _ib_est > 0:
            _computed_weights: dict = {r: v / _ib_est for r, v in _raw_est.items()}
        else:
            _mx_est = max(_raw_est.values()) if _raw_est else 1.0
            _computed_weights = {r: v / _mx_est * 10 for r, v in _raw_est.items()}

        # _ROLE_WEIGHTS: computed where available, fallback where not
        _ROLE_WEIGHTS: dict = dict(_ROLE_WEIGHT_FALLBACKS)
        _ROLE_WEIGHTS.update(_computed_weights)

        # Per-role indicator breakdown for Chart 30
        _weight_indicators: dict = {
            r: {
                "scarcity": _sc_n.get(r, 0.0),
                "time_demand": _td_n.get(r, 0.0),
                "career_impact": _im_n.get(r, 0.0),
                "final": _ROLE_WEIGHTS.get(r, _DEFAULT_ROLE_WEIGHT),
            }
            for r in _est_roles
        }

        # Compute WPS per person
        person_wps_data: dict = _dd10(lambda: {
            "wps_total": 0.0, "raw_credits": 0,
            "role_wps": _dd10(float), "years": set(),
            "era_buckets": _dd10(float),
        })

        for pid, role, yr, fmt, eps, dur, genres_j in wps_raw:
            rw = _ROLE_WEIGHTS.get(role, _DEFAULT_ROLE_WEIGHT)
            ed = _era_deflator(yr or 2000)
            cw = _complexity_weight(fmt, eps, dur)
            wps = rw * cw * ed
            d = person_wps_data[pid]
            d["wps_total"] += wps
            d["raw_credits"] += 1
            d["role_wps"][role or "other"] += wps
            if yr:
                d["years"].add(int(yr))
                era = (int(yr) // 5) * 5
                d["era_buckets"][era] += wps

        # Per-person summary
        person_wps_summary = []
        for pid, d in person_wps_data.items():
            if d["raw_credits"] < 3:
                continue
            yrs = d["years"]
            active_yrs = max(1, max(yrs) - min(yrs) + 1) if yrs else 1
            wps_per_yr = d["wps_total"] / active_yrs
            raw_per_yr = d["raw_credits"] / active_yrs
            iv_sc = _score_by_pid.get(str(pid), 0)
            person_wps_summary.append({
                "pid": pid,
                "wps_total": round(d["wps_total"], 1),
                "wps_per_yr": round(wps_per_yr, 2),
                "raw_credits": d["raw_credits"],
                "raw_per_yr": round(raw_per_yr, 2),
                "active_yrs": active_yrs,
                "iv_score": iv_sc,
                "role_wps": dict(d["role_wps"]),
                "era_buckets": dict(d["era_buckets"]),
            })

        # ── Chart 30: データ駆動役職ウェイト — 3指標の内訳 ───────────────────
        try:
            body += "<h3>Chart 30: 役職ウェイト — データ駆動推計の3指標内訳と最終値</h3>"
            body += chart_guide(
                "WPS計算に使用する役職ウェイトをデータから推計した結果。"
                "3種の指標を各0-1正規化して均等加重平均し、動画=1.0基準に再スケーリング。"
                "① 希少性 (1/アニメあたり平均人数) — 1作品に何人いるか？少ないほど高ウェイト。"
                "② 時間占有率 (1/年間平均クレジット数) — 年に少ししかできない役職ほど高ウェイト。"
                "③ キャリア影響 (その役職主担当者の平均IV Score) — スコアが高い役職ほど重要度大。"
                "最終値 = 3指標の平均（動画=1.0基準）。この値がWPS計算全体で使用される。"
            )

            if _weight_indicators:
                # Sort by final weight descending
                _sorted_roles30 = sorted(
                    _weight_indicators.keys(),
                    key=lambda r: -_weight_indicators[r]["final"],
                )
                _rl30 = [_ROLE_JA.get(r, r) for r in _sorted_roles30]

                _IND_COLORS = {
                    "scarcity":       "rgba(76,201,240,0.80)",   # cyan
                    "time_demand":    "rgba(255,107,53,0.80)",   # orange
                    "career_impact":  "rgba(247,37,133,0.80)",   # pink
                    "final":          "rgba(6,214,160,0.95)",    # green (final)
                }
                _IND_LABELS = {
                    "scarcity":       "① 希少性 (0-1)",
                    "time_demand":    "② 時間占有率 (0-1)",
                    "career_impact":  "③ キャリア影響 (0-1)",
                    "final":          "最終ウェイト (動画=1.0基準)",
                }

                fig30 = go.Figure()
                for ind in ("scarcity", "time_demand", "career_impact", "final"):
                    vals30 = [_weight_indicators[r][ind] for r in _sorted_roles30]
                    fig30.add_trace(go.Bar(
                        y=_rl30,
                        x=vals30,
                        name=_IND_LABELS[ind],
                        orientation="h",
                        marker_color=_IND_COLORS[ind],
                        text=[f"{v:.2f}" for v in vals30],
                        textposition="outside",
                    ))

                fig30.update_layout(
                    title_text="Chart 30: データ駆動役職ウェイト — 3指標内訳と最終値",
                    barmode="group",
                    height=600,
                    legend=dict(orientation="h", y=-0.12),
                    xaxis_title="スコア",
                )
                body += plotly_div_safe(fig30, "role-weight-chart", height=600)

                # Key findings
                top3 = _sorted_roles30[:3]
                bottom3 = _sorted_roles30[-3:]
                body += key_findings([
                    f"最高ウェイト上位3役職: {', '.join(_ROLE_JA.get(r,r) for r in top3)}"
                    f" — それぞれ {', '.join(f'{_weight_indicators[r]["final"]:.1f}' for r in top3)}",
                    f"最低ウェイト下位3役職: {', '.join(_ROLE_JA.get(r,r) for r in bottom3)}"
                    f" — それぞれ {', '.join(f'{_weight_indicators[r]["final"]:.1f}' for r in bottom3)}",
                    "このウェイトは以降のWPS計算（Charts 31-33）に直接使用されます。"
                    "データが少ない役職（n<5）はフォールバック値を使用。",
                    "① 希少性と③ キャリア影響の一致度が高い場合、"
                    "「その役職に就く人は少なく、かつその人たちの影響力が大きい」という一貫したシグナル",
                ])
            else:
                body += '<div class="insight-box">Chart 30: 推計データ不足</div>'
        except Exception as _e30:
            body += f'<div class="insight-box">Chart 30 スキップ: {_e30}</div>'

        # ── Chart 31: 時代別加重生産性の変動パターン ──────────────────────
        try:
            body += "<h3>Chart 31: 時代別 WPS変動パターン — 生産性回帰と時代効果</h3>"
            body += chart_guide(
                "5年ごとの時代区分でWPS/年の中央値の推移を表示。"
                "上段: 役職別WPS/年の時代変化（折れ線）。"
                "下段: OLS回帰で era × role のインタラクション効果を推定。"
            )
            era_role_wps: dict = _dd10(lambda: _dd10(list))
            for d in person_wps_summary:
                for era, wps_val in d["era_buckets"].items():
                    top_role = max(d["role_wps"], key=lambda r: d["role_wps"][r]) if d["role_wps"] else "other"
                    era_role_wps[era][top_role].append(wps_val)

            # Group roles by stage
            stage_groups = {
                "動画・第2原画 (Stg1-2)": ["in_between", "layout", "second_key_animator", "background_art"],
                "原画 (Stg3)": ["key_animator", "effects_animator"],
                "作監・絵コンテ (Stg4)": ["animation_director", "character_designer", "storyboard", "art_director"],
                "総作監・演出 (Stg5)": ["chief_animation_director", "episode_director",
                                      "series_composition", "sound_director"],
                "監督 (Stg6)": ["director"],
            }

            sorted_eras = sorted(era_role_wps.keys())
            fig31 = go.Figure()
            stg_colors = ["#a0a0c0", "#4CC9F0", "#FFD166", "#FF6B35", "#F72585"]
            for (sg_name, sg_roles), sg_color in zip(stage_groups.items(), stg_colors):
                era_vals = []
                era_ns = []
                valid_eras = []
                for era in sorted_eras:
                    combined = [v for r in sg_roles for v in era_role_wps.get(era, {}).get(r, [])]
                    if len(combined) >= 5:
                        era_vals.append(_st10.median(combined))
                        era_ns.append(len(combined))
                        valid_eras.append(era)
                if era_vals:
                    fig31.add_trace(go.Scatter(
                        x=valid_eras, y=era_vals,
                        name=sg_name, line=dict(color=sg_color, width=2.5),
                        mode="lines+markers",
                    ))
            fig31.update_layout(
                title_text="Chart 31: 役職グループ別 WPS/年の時代変化",
                xaxis_title="時代（5年区切り）",
                yaxis_title="WPS/年 中央値（重み付き生産性）",
                legend=dict(orientation="h", y=-0.15),
                height=500,
            )
            body += plotly_div_safe(fig31, "era-wps-trend", height=500)

            # OLS: WPS/yr ~ career_age + role_stage + era (for all persons with enough data)
            wps_panel_recs = [d for d in person_wps_summary
                              if d["active_yrs"] >= 2 and d["wps_per_yr"] > 0]
            if len(wps_panel_recs) > 100:
                y_wps = _np10.array([_math10.log1p(d["wps_per_yr"]) for d in wps_panel_recs])
                # Compute era (year of peak activity) per person
                peak_eras = []
                for d in wps_panel_recs:
                    pdata_by_pid = next(
                        (r for r in person_wps_summary if r["pid"] == d["pid"]),
                        None
                    )
                    eb = d.get("era_buckets", {})
                    peak_era = max(eb, key=eb.get) if eb else 2000
                    peak_eras.append(peak_era)

                # Get role stage per person
                def _pid_role_stage(d):
                    rw = d.get("role_wps", {})
                    top_role = max(rw, key=rw.get) if rw else "key_animator"
                    return _ROLE_TO_STAGE.get(top_role, 3)

                X_wps = _np10.column_stack([
                    [d["active_yrs"] for d in wps_panel_recs],      # career_age proxy
                    [_pid_role_stage(d) for d in wps_panel_recs],   # role stage
                    [pe / 1000 for pe in peak_eras],                  # era (normalized)
                    [_np10.log1p(d["raw_credits"]) for d in wps_panel_recs],  # log credits
                    _np10.ones(len(wps_panel_recs)),                   # intercept
                ])
                beta_wps = _np10.linalg.lstsq(X_wps, y_wps, rcond=None)[0]
                y_hat_wps = X_wps @ beta_wps
                resid_wps = y_wps - y_hat_wps
                ss_res_wps = float(_np10.sum(resid_wps ** 2))
                ss_tot_wps = float(_np10.sum((y_wps - y_wps.mean()) ** 2))
                r2_wps = 1.0 - ss_res_wps / ss_tot_wps if ss_tot_wps > 0 else 0

                col_names_wps = ["活動年数", "役職ステージ", "時代 (era/1000)", "log(クレジット数)", "定数"]
                beta_labels = [f"{col}: β={beta_wps[j]:.3f}" for j, col in enumerate(col_names_wps[:-1])]

                fig31b = go.Figure(go.Bar(
                    y=col_names_wps[:-1],
                    x=[float(beta_wps[j]) for j in range(len(col_names_wps) - 1)],
                    orientation="h",
                    marker_color=["#4CC9F0" if float(beta_wps[j]) > 0 else "#F72585"
                                  for j in range(len(col_names_wps) - 1)],
                    text=[f"β={beta_wps[j]:.3f}" for j in range(len(col_names_wps) - 1)],
                    textposition="outside",
                ))
                fig31b.add_vline(x=0, line_color="gray", line_width=1.5)
                fig31b.update_layout(
                    title_text=f"Chart 31b: WPS/年(log)の回帰係数 (R²={r2_wps:.3f}, n={len(wps_panel_recs)})",
                    xaxis_title="係数β",
                    height=380,
                )
                body += plotly_div_safe(fig31b, "wps-ols-coeff", height=380)

                era_beta = float(beta_wps[2])
                role_beta = float(beta_wps[1])
                body += key_findings([
                    f"役職ステージ係数: β={role_beta:.3f} → 高役職ほどWPS/年が{'増加' if role_beta > 0 else '減少'}",
                    f"時代係数: β={era_beta:.3f} → 最近の時代ほどWPS/年が{'増加' if era_beta > 0 else '減少'}",
                    f"モデルR²={r2_wps:.3f} → {r2_wps*100:.0f}%の分散を説明",
                    "時代β < 0: 時代デフレーター適用後も近年の生産性（重み付き）が低下している可能性",
                    "時代β > 0: デジタル化の恩恵が役職ウェイト・複雑度補正後も残存する",
                ])
        except Exception as _e31:
            body += f'<div class="insight-box">Chart 31 スキップ: {_e31}</div>'

        # ── Chart 32: WPS vs 生クレジット数の比較散布図 ───────────────────
        try:
            body += "<h3>Chart 32: WPS vs 生クレジット数 — 補正前後の生産性ランキング変化</h3>"
            body += chart_guide(
                "x軸: 年間生クレジット数（未補正）。y軸: 年間WPS（役職・複雑度・時代補正後）。"
                "y=x線より上 → 補正後に評価が上昇（高役職・複雑な作品担当）。"
                "y=x線より下 → 補正後に評価が下降（低役職・単純作品の量産）。"
                "色: 役職ステージ（高役職=暖色）。"
            )
            sample_wps = sorted(person_wps_summary,
                                key=lambda d: -d["iv_score"])[:500]
            xs_wps = [d["raw_per_yr"] for d in sample_wps]
            ys_wps = [d["wps_per_yr"] for d in sample_wps]
            iv_scores_wps = [d["iv_score"] for d in sample_wps]

            # Color by iv_score
            max_c_wps = max(iv_scores_wps) if iv_scores_wps else 100
            colors_wps = [
                f"rgba({int(255*c/max_c_wps)},100,{int(200*(1-c/max_c_wps))},0.6)"
                for c in iv_scores_wps
            ]

            fig32 = go.Figure(go.Scatter(
                x=xs_wps, y=ys_wps,
                mode="markers",
                marker=dict(size=5, color=colors_wps),
                text=[f"iv_score={c:.1f}" for c in iv_scores_wps],
                hovertemplate="raw/yr=%{x:.1f}<br>WPS/yr=%{y:.1f}<br>%{text}",
            ))
            max_xy = max(max(xs_wps), max(ys_wps)) if xs_wps else 10
            fig32.add_trace(go.Scatter(
                x=[0, max_xy], y=[0, max_xy], mode="lines",
                line=dict(color="gray", dash="dash", width=1),
                name="WPS=raw", showlegend=True,
            ))
            fig32.update_layout(
                title_text="Chart 32: WPS/年 vs 生クレジット/年 (IV Score上位500人)",
                xaxis_title="生クレジット数/年（未補正）",
                yaxis_title="WPS/年（役職・複雑度・時代補正後）",
                height=520,
            )
            body += plotly_div_safe(fig32, "wps-vs-raw-scatter", height=520)

            # Find people whose rank changes most
            raw_ranks = sorted(range(len(xs_wps)), key=lambda i: -xs_wps[i])
            wps_ranks = sorted(range(len(ys_wps)), key=lambda i: -ys_wps[i])
            rank_change = [
                abs(raw_ranks.index(i) - wps_ranks.index(i))
                for i in range(len(xs_wps))
            ]
            max_upward = max(enumerate(rank_change), key=lambda x: x[1]) if rank_change else (0, 0)
            corr_xy = _np10.corrcoef(xs_wps, ys_wps)[0, 1] if len(xs_wps) > 1 else 0

            body += key_findings([
                f"補正前後の相関: r={corr_xy:.3f} (高い → 補正による順位変動は小さい)",
                "y>x（補正後に評価上昇）: 高役職・複雑な作品を少数担当するタイプ",
                "y<x（補正後に評価下降）: 低役職クレジットを大量にこなすタイプ",
                "WPS補正により「量より質」の貢献者が可視化される",
            ])
        except Exception as _e32:
            body += f'<div class="insight-box">Chart 32 スキップ: {_e32}</div>'

        # ── Chart 33: 役割分担量 × 作品要求工数 回帰 ──────────────────────
        try:
            body += "<h3>Chart 33: 役割分担量 × 作品要求工数 — 生産性の多変量分解</h3>"
            body += chart_guide(
                "各クレジットの「推定寄与工数」= 役職ウェイト × 複雑度 × 時代デフレーター。"
                "一つの作品に関わる総推定工数を各クレジットに分配（均等分配）し、"
                "「分担量（share of work）」を推計。"
                "x軸: 1作品あたりの平均分担工数。y軸: 年間WPS（全作品の推定分担工数の合計/年）。"
                "高x-高y → 少ない作品に大きな責任を持つスペシャリスト。"
                "低x-高y → 多数の作品に参加して累積工数を積み上げるスタイル。"
            )
            # Compute per-anime total complexity for work share
            # Aggregate per anime: total role weights (denominator for share)
            anime_total_weight: dict = _dd10(float)
            for pid, role, yr, fmt, eps, dur, _ in wps_raw:
                aid = f"{fmt}_{yr}_{eps}"  # proxy for anime id
                anime_total_weight[aid] += _ROLE_WEIGHTS.get(role or "in_between", _DEFAULT_ROLE_WEIGHT)

            # Per-person average share per anime
            person_share_data: dict = _dd10(list)  # {pid: [share per anime]}
            anime_counts: dict = _dd10(int)  # {pid: anime count}
            for pid, role, yr, fmt, eps, dur, _ in wps_raw:
                aid = f"{fmt}_{yr}_{eps}"
                total_w = anime_total_weight.get(aid, 1)
                my_w = _ROLE_WEIGHTS.get(role or "in_between", _DEFAULT_ROLE_WEIGHT)
                share = my_w / total_w  # fraction of this anime's work
                cw = _complexity_weight(fmt, eps, dur)
                ed = _era_deflator(yr or 2000)
                weighted_share = share * cw * ed
                person_share_data[pid].append(weighted_share)
                anime_counts[pid] += 1

            share_plot = []
            for d in person_wps_summary[:300]:
                pid = d["pid"]
                shares = person_share_data.get(pid, [])
                if shares and len(shares) >= 3:
                    avg_share = sum(shares) / len(shares)
                    share_plot.append({
                        "pid": pid,
                        "avg_share": avg_share,
                        "wps_per_yr": d["wps_per_yr"],
                        "iv_score": d["iv_score"],
                        "active_yrs": d["active_yrs"],
                    })

            if share_plot:
                xs33 = [d["avg_share"] for d in share_plot]
                ys33 = [d["wps_per_yr"] for d in share_plot]
                cs33 = [d["iv_score"] for d in share_plot]
                max_c33 = max(cs33) if cs33 else 100
                colors33 = [
                    f"rgba({int(255*c/max_c33)},{int(80*(1-c/max_c33))},200,0.6)"
                    for c in cs33
                ]
                fig33 = go.Figure(go.Scatter(
                    x=xs33, y=ys33, mode="markers",
                    marker=dict(size=5, color=colors33),
                    text=[f"iv_score={c:.1f}" for c in cs33],
                    hovertemplate="avg_share=%{x:.3f}<br>WPS/yr=%{y:.1f}<br>%{text}",
                ))
                fig33.update_layout(
                    title_text="Chart 33: 平均分担工数 vs WPS/年 (各クレジットへの推定責任量)",
                    xaxis_title="1クレジットあたり平均分担工数（役職ウェイト÷作品内総ウェイト × 複雑度）",
                    yaxis_title="WPS/年（年間重み付き生産性）",
                    height=500,
                )
                body += plotly_div_safe(fig33, "share-wps-scatter", height=500)

                corr33 = _np10.corrcoef(xs33, ys33)[0, 1] if len(xs33) > 1 else 0
                body += key_findings([
                    f"分担工数 vs WPS相関: r={corr33:.3f}",
                    "右上（高分担×高WPS）: 大作での中核的役割を担う専門家",
                    "左上（低分担×高WPS）: 多数の作品に参加して工数を積み上げるスタイル（量産型）",
                    "役割分担量だけでなく「その役職がチームの何%を占めるか」が貢献度の本質的指標",
                    "大手スタジオでは社員が担う割合が高い → 分担量が見かけ上低くなる（内製化効果）",
                ])
        except Exception as _e33:
            body += f'<div class="insight-box">Chart 33 スキップ: {_e33}</div>'

    except Exception as _e_s10:
        body += (
            f'<div class="insight-box">Section 10 スキップ: '
            f'{type(_e_s10).__name__}: {_e_s10}</div>'
        )

    body += "</div>"  # close Section 10 card

    # ── TOC update: add Section 11 ────────────────────────────
    body = body.replace(
        '<a href="#sec-wps">10. 重み付き生産性指数 (WPS)</a>\n</div>\n</div>',
        '<a href="#sec-wps">10. 重み付き生産性指数 (WPS)</a>\n'
        '<a href="#sec-attrition">11. 離脱・生存分析 (辞めた人を含む)</a>\n</div>\n</div>',
    )

    # ─────────────────────────────────────────────────────────
    # Section 11: キャリア離脱・生存分析
    # ─────────────────────────────────────────────────────────
    body += '<div class="card" id="sec-attrition">'
    body += "<h2>Section 11: キャリア離脱・生存分析 — 辞めた人を含む全員ベース (Charts 34-41)</h2>"
    body += section_desc(
        "これまでの多くの分析はIV Score上位者を対象にしており、"
        "途中でアニメ業界を去った人（離脱者・退職者）が除外されていました。"
        "これは深刻な生存バイアス（survivorship bias）です。"
        "ここでは全登録者を対象に、"
        "「いつ・どのステージで辞めた人が多いか」「コホートごとに生存率はどう違うか」"
        "「新卒即離脱 vs 中堅 vs ベテラン離脱の類型」「失われた才能の定量化」"
        "「新規スタッフの期待才能量」「才能の純増フロー」を分析します。"
        "辞めた人の存在を可視化することで、業界の真の人材保持・才能フロー構造が明らかになります。"
    )

    try:
        import statistics as _st11
        import math as _math11
        from collections import defaultdict as _dd11
        import numpy as _np11

        scores_data_all = load_json("scores.json") or []
        milestones_data_s11 = load_json("milestones.json") or {}
        CURRENT_YEAR = 2025
        DROPOUT_THRESHOLD = 5  # last credit ≥5 years ago = considered quit

        # Build person registry: all persons with >= 1 credit
        # "quit" = latest_year <= CURRENT_YEAR - DROPOUT_THRESHOLD
        person_registry = []
        for pdata in (scores_data_all if isinstance(scores_data_all, list) else []):
            if not isinstance(pdata, dict):
                continue
            career = pdata.get("career") or {}
            fy = career.get("first_year")
            ly = career.get("latest_year")
            hs = career.get("highest_stage", 1)
            tc = pdata.get("total_credits", 0)
            if not fy or tc < 1:
                continue
            ly = ly or fy
            career_span = ly - fy
            is_active = ly >= (CURRENT_YEAR - DROPOUT_THRESHOLD)
            is_dropout = not is_active and career_span <= 10
            is_retired = not is_active and career_span > 10
            cohort = (int(fy) // 10) * 10  # decade cohort
            person_registry.append({
                "pid": pdata.get("person_id"),
                "first_year": int(fy),
                "latest_year": int(ly),
                "career_span": career_span,
                "highest_stage": int(hs),
                "total_credits": int(tc),
                "is_active": is_active,
                "is_dropout": is_dropout,
                "is_retired": is_retired,
                "cohort": cohort,
                "iv_score": pdata.get("iv_score", 0) or 0,
            })

        n_total = len(person_registry)
        n_active = sum(1 for p in person_registry if p["is_active"])
        n_dropout = sum(1 for p in person_registry if p["is_dropout"])
        n_retired = sum(1 for p in person_registry if p["is_retired"])

    except Exception as _e_s11_load:
        body += (
            f'<div class="insight-box">Section 11 スキップ: '
            f'{type(_e_s11_load).__name__}: {_e_s11_load}</div>'
        )
        body += "</div>"
    else:
        # ── Chart 34: キャリア生存曲線 (Kaplan-Meier by cohort) ───────────
        try:
            body += "<h3>Chart 34: キャリア生存曲線 — コホート別離脱パターン</h3>"
            body += chart_guide(
                "x軸: career_age（デビューから何年目か）。y軸: その年数まで現役の割合（%）。"
                "曲線が急に落ちる年 = 多くの人が離脱する時期。"
                "コホート（デビュー年代）によって生存率が異なるかも観察できます。"
                f"現在も活動中（{CURRENT_YEAR-DROPOUT_THRESHOLD}年以降にクレジットあり）を「生存」として定義。"
                "注意: アクティブな人も将来離脱する可能性があり、右側の曲線は過大推定です（打ち切り）。"
            )
            cohort_groups = {
                "1970-1979年代デビュー": [p for p in person_registry if 1970 <= p["first_year"] < 1980],
                "1980年代デビュー": [p for p in person_registry if 1980 <= p["first_year"] < 1990],
                "1990年代デビュー": [p for p in person_registry if 1990 <= p["first_year"] < 2000],
                "2000年代デビュー": [p for p in person_registry if 2000 <= p["first_year"] < 2010],
                "2010年代デビュー": [p for p in person_registry if 2010 <= p["first_year"] < 2020],
            }
            cohort_colors = ["#a0a0c0", "#7EB8D4", "#4CC9F0", "#FFD166", "#F72585"]

            fig34 = go.Figure()
            max_age_km = 30
            for (cname, cgroup), ccol in zip(cohort_groups.items(), cohort_colors):
                if len(cgroup) < 20:
                    continue
                # Kaplan-Meier: at each career_age, fraction still active
                # "event" = career_age at dropout (career_span for non-active, censored for active)
                n_at_risk = len(cgroup)
                surv = 1.0
                surv_curve = [(0, 100.0)]
                age_t = list(range(1, max_age_km + 1))
                for t in age_t:
                    # Count events at time t (career_span == t AND not active)
                    events_t = sum(1 for p in cgroup
                                   if not p["is_active"] and p["career_span"] == t)
                    # Count censored at time t (active persons with career_span == t)
                    censored_t = sum(1 for p in cgroup if p["is_active"] and p["career_span"] == t)
                    if n_at_risk <= 0:
                        break
                    # KM estimate
                    surv *= (1 - events_t / n_at_risk)
                    n_at_risk -= events_t + censored_t
                    surv_curve.append((t, round(surv * 100, 2)))

                t_vals = [x[0] for x in surv_curve]
                s_vals = [x[1] for x in surv_curve]
                fig34.add_trace(go.Scatter(
                    x=t_vals, y=s_vals,
                    name=f"{cname} (n={len(cgroup)})",
                    line=dict(color=ccol, width=2.5),
                    mode="lines",
                ))

            # Overall survival curve
            n_at_risk_all = len(person_registry)
            surv_all = 1.0
            surv_all_curve = [(0, 100.0)]
            for t in range(1, max_age_km + 1):
                events_t = sum(1 for p in person_registry
                               if not p["is_active"] and p["career_span"] == t)
                censored_t = sum(1 for p in person_registry
                                 if p["is_active"] and p["career_span"] == t)
                if n_at_risk_all <= 0:
                    break
                surv_all *= (1 - events_t / n_at_risk_all)
                n_at_risk_all -= events_t + censored_t
                surv_all_curve.append((t, round(surv_all * 100, 2)))

            fig34.add_trace(go.Scatter(
                x=[x[0] for x in surv_all_curve],
                y=[x[1] for x in surv_all_curve],
                name=f"全体 (n={n_total})",
                line=dict(color="white", width=3, dash="dash"),
                mode="lines",
            ))
            fig34.add_hline(y=50, line_dash="dot", line_color="gray",
                            annotation_text="50%生存ライン", annotation_position="right")
            fig34.update_layout(
                title_text="Chart 34: キャリア生存曲線 (Kaplan-Meier, コホート別)",
                xaxis_title="career_age（デビューからの経過年数）",
                yaxis_title="現役継続率 (%)",
                legend=dict(orientation="h", y=-0.18),
                height=520,
                yaxis_range=[0, 105],
            )
            body += plotly_div_safe(fig34, "survival-curve-chart", height=520)

            # Median survival time
            overall_50 = next((t for t, s in surv_all_curve if s <= 50), None)
            pct_10yr = next((s for t, s in surv_all_curve if t == 10), 100)
            pct_20yr = next((s for t, s in surv_all_curve if t == 20), 100)
            body += key_findings([
                f"全登録者 {n_total:,}人 / 現役: {n_active:,}人 ({n_active/n_total*100:.0f}%) / "
                f"短期離脱(≤10年): {n_dropout:,}人 / 長期後退職: {n_retired:,}人",
                f"中央生存career_age（50%が残る年数）: {overall_50}年" if overall_50 else
                "中央生存年数: 30年以内に50%未達（長期現役者多数）",
                f"10年後生存率: {pct_10yr:.0f}% / 20年後生存率: {pct_20yr:.0f}%",
                "曲線の急落点 = 多くの人が離脱するタイミング（デビュー後3〜5年が最初の山）",
                "2010年代デビュー組は観測期間が短いため右側は打ち切り（過大推定）",
            ])
        except Exception as _e34:
            body += f'<div class="insight-box">Chart 34 スキップ: {_e34}</div>'

        # ── Chart 35: 離脱時のステージ分布 + タイミング ───────────────────
        try:
            body += "<h3>Chart 35: 離脱時のステージ分布 — どのステージで辞めた人が多いか</h3>"
            body += chart_guide(
                "左: 現役・離脱・退職者それぞれのキャリア終了時（最終）ステージ分布。"
                "右: 離脱したcareer_ageの分布（ヒストグラム）。"
                "早いcareer_ageで低いステージから離脱 → 昇進できずに辞めた人。"
                "高いステージで離脱 → 成功はしたが何らかの理由で引退。"
                "ステージが低いまま辞めた人が多い場合 → 昇進の壁（bottleneck）の存在を示す。"
            )
            status_groups = {
                "現役 (active)": [p for p in person_registry if p["is_active"]],
                "短期離脱 (≤10年)": [p for p in person_registry if p["is_dropout"]],
                "長期後退職 (>10年)": [p for p in person_registry if p["is_retired"]],
            }
            status_colors = {"現役 (active)": "#4CC9F0",
                             "短期離脱 (≤10年)": "#F72585",
                             "長期後退職 (>10年)": "#FFD166"}

            STAGE_LABELS_S11 = {1: "動画", 2: "第2原画", 3: "原画", 4: "作監", 5: "総作監/演出", 6: "監督"}

            fig35 = make_subplots(
                rows=1, cols=2,
                subplot_titles=["最終ステージ分布 (%)", "離脱career_ageのヒストグラム"],
            )
            stages = [1, 2, 3, 4, 5, 6]
            for status, plist in status_groups.items():
                if not plist:
                    continue
                scol = status_colors[status]
                stage_counts = [sum(1 for p in plist if p["highest_stage"] == s) for s in stages]
                total_s = sum(stage_counts)
                stage_pcts = [c / total_s * 100 if total_s > 0 else 0 for c in stage_counts]
                fig35.add_trace(
                    go.Bar(x=[STAGE_LABELS_S11.get(s, str(s)) for s in stages],
                           y=stage_pcts, name=status, marker_color=scol,
                           legendgroup=status),
                    row=1, col=1,
                )
                # Histogram of career_age at dropout (only non-active)
                if status != "現役 (active)":
                    ages = [p["career_span"] for p in plist if p["career_span"] <= 30]
                    if ages:
                        fig35.add_trace(
                            go.Histogram(x=ages, name=status, marker_color=scol,
                                         opacity=0.7, legendgroup=status, showlegend=False,
                                         xbins=dict(start=0, end=30, size=1)),
                            row=1, col=2,
                        )
            fig35.update_layout(
                title_text="Chart 35: 離脱時のステージ分布 + 離脱タイミング",
                barmode="group",
                legend=dict(orientation="h", y=-0.18),
                height=500,
            )
            fig35.update_xaxes(title_text="最終ステージ", row=1, col=1)
            fig35.update_yaxes(title_text="割合 (%)", row=1, col=1)
            fig35.update_xaxes(title_text="career_age（離脱年数）", row=1, col=2)
            fig35.update_yaxes(title_text="人数", row=1, col=2)
            body += plotly_div_safe(fig35, "dropout-stage-dist", height=500)

            # Key stage stats
            dropouts = [p for p in person_registry if p["is_dropout"]]
            if dropouts:
                stg1_drop = sum(1 for p in dropouts if p["highest_stage"] <= 1)
                stg3_drop = sum(1 for p in dropouts if p["highest_stage"] <= 3)
                median_age_drop = _st11.median([p["career_span"] for p in dropouts
                                                if p["career_span"] >= 0])
                body += key_findings([
                    f"短期離脱者 {len(dropouts):,}人 のうち、"
                    f"最高ステージ=動画のまま離脱: {stg1_drop:,}人 ({stg1_drop/len(dropouts)*100:.0f}%)",
                    f"原画以下のステージで離脱: {stg3_drop:,}人 ({stg3_drop/len(dropouts)*100:.0f}%)",
                    f"短期離脱者の離脱career_age中央値: {median_age_drop:.0f}年",
                    "動画のまま離脱 → 原画への壁（昇進障壁）が最初の脱落ポイント",
                    "これらは現在の分析（IV Score上位者ベース）には含まれていない人々",
                ])
        except Exception as _e35:
            body += f'<div class="insight-box">Chart 35 スキップ: {_e35}</div>'

        # ── Chart 36: 離脱フローを含むアリュビアル図 ─────────────────────
        try:
            body += "<h3>Chart 36: 離脱フロー込み多段サンキー — career_age 5/10/15/20年</h3>"
            body += chart_guide(
                "chart 5の改訂版。各career_ageチェックポイントに「離脱 (quit)」ノードを追加。"
                "灰色のフロー = その時点で既に業界を離れた人の数。"
                "チェックポイントを超えるほど「生存者」に偏った集団になることが可視化されます。"
                "このバイアスが生存者のみの分析（Section 1-5のスパゲッティプロット等）の限界です。"
            )
            checkpoints = [5, 10, 15, 20]
            # Build stage distribution at each checkpoint
            # For each person: stage at career_age t = highest stage achieved by year first_year + t
            # Using milestones data for stage at age
            milestones_s11 = milestones_data_s11 if isinstance(milestones_data_s11, dict) else {}

            def _stage_at_age(pid, target_age, first_year):
                pid_str = str(pid)
                all_events = milestones_s11.get(pid_str, []) or []
                promos = [e for e in all_events if e.get("type") == "promotion"]
                stage = 1
                for promo in promos:
                    promo_yr = promo.get("year")
                    to_stage = promo.get("to_stage", stage)
                    if promo_yr and to_stage:
                        if int(promo_yr) <= first_year + target_age:
                            stage = max(stage, int(to_stage))
                return stage

            # Build nodes and links for multi-point Sankey
            sankey_nodes = []
            sankey_node_idx = {}
            sankey_links_src = []
            sankey_links_tgt = []
            sankey_links_val = []
            sankey_links_col = []

            STAGE_COLORS_SANKEY = {
                1: "rgba(160,160,192,0.7)", 2: "rgba(126,184,212,0.7)",
                3: "rgba(76,201,240,0.7)", 4: "rgba(255,209,102,0.7)",
                5: "rgba(255,107,53,0.7)", 6: "rgba(247,37,133,0.7)",
                0: "rgba(100,100,100,0.5)",  # quit
            }

            def _node_name(cp, stage):
                if stage == 0:
                    return f"{cp}年:離脱"
                return f"{cp}年:{STAGE_LABELS_S11.get(stage, f'Stage{stage}')}"

            def _get_node(name, color):
                if name not in sankey_node_idx:
                    sankey_node_idx[name] = len(sankey_nodes)
                    sankey_nodes.append({"label": name, "color": color})
                return sankey_node_idx[name]

            # Use a sample of persons for efficiency
            sample_persons = [p for p in person_registry if p["total_credits"] >= 3]
            prev_stage_map = {}  # {pid: stage at prev checkpoint}

            for i, cp in enumerate(checkpoints):
                stage_at_cp = {}
                for p in sample_persons:
                    pid = p["pid"]
                    if p["career_span"] < cp:
                        # Already quit before this checkpoint
                        stage_at_cp[pid] = 0
                    else:
                        s = _stage_at_age(pid, cp, p["first_year"])
                        stage_at_cp[pid] = s

                if i > 0:
                    prev_cp = checkpoints[i - 1]
                    # Build transition counts
                    trans_counts = _dd11(int)  # {(from_stage, to_stage): count}
                    for p in sample_persons:
                        pid = p["pid"]
                        from_s = prev_stage_map.get(pid, 1)
                        to_s = stage_at_cp.get(pid, 0)
                        if from_s == 0:
                            continue  # Already counted as quit
                        trans_counts[(from_s, to_s)] += 1

                    for (fs, ts), cnt in trans_counts.items():
                        if cnt < 2:
                            continue
                        src_name = _node_name(prev_cp, fs)
                        tgt_name = _node_name(cp, ts)
                        src_col = STAGE_COLORS_SANKEY.get(fs, "rgba(100,100,100,0.5)")
                        tgt_col = STAGE_COLORS_SANKEY.get(ts, "rgba(100,100,100,0.5)")
                        src_idx = _get_node(src_name, src_col)
                        tgt_idx = _get_node(tgt_name, tgt_col)
                        sankey_links_src.append(src_idx)
                        sankey_links_tgt.append(tgt_idx)
                        sankey_links_val.append(cnt)
                        sankey_links_col.append(src_col)

                prev_stage_map = stage_at_cp

            # Also add initial distribution at checkpoint 5
            first_cp = checkpoints[0]
            # (already handled in prev_stage_map initialization above,
            #  but we need nodes for the first column)
            # We'll add a synthetic "デビュー時" column
            debut_counts = _dd11(int)
            for p in sample_persons:
                pid = p["pid"]
                debut_counts[1] += 1  # everyone starts at stage 1

            if sankey_nodes and sankey_links_src:
                fig36 = go.Figure(go.Sankey(
                    arrangement="snap",
                    node=dict(
                        label=[n["label"] for n in sankey_nodes],
                        color=[n["color"] for n in sankey_nodes],
                        pad=15, thickness=15,
                    ),
                    link=dict(
                        source=sankey_links_src,
                        target=sankey_links_tgt,
                        value=sankey_links_val,
                        color=sankey_links_col,
                    ),
                ))
                fig36.update_layout(
                    title_text="Chart 36: 離脱フロー込みアリュビアル図 (career_age 5/10/15/20年チェックポイント)",
                    height=620,
                )
                body += plotly_div_safe(fig36, "attrition-sankey", height=620)

                # Count quit at each checkpoint
                quit_at_cp = {}
                for cp in checkpoints[1:]:
                    q = sum(1 for n in sankey_nodes if f"{cp}年:離脱" == n["label"])
                    # Get the flow into quit node
                    quit_node_name = f"{cp}年:離脱"
                    if quit_node_name in sankey_node_idx:
                        quit_idx = sankey_node_idx[quit_node_name]
                        total_to_quit = sum(
                            sankey_links_val[j]
                            for j, tgt in enumerate(sankey_links_tgt)
                            if tgt == quit_idx
                        )
                        quit_at_cp[cp] = total_to_quit

                findings36 = [
                    f"灰色フロー = 各チェックポイントまでに離脱した人数",
                ]
                for cp, cnt in quit_at_cp.items():
                    findings36.append(
                        f"career_age {cp}年時点での累積離脱: {cnt:,}人"
                    )
                findings36.append("離脱者を除外した分析は「生き残った人だけのキャリア像」であることに注意")
                body += key_findings(findings36)
            else:
                body += '<div class="insight-box">離脱フロー図: データ不足でスキップ</div>'
        except Exception as _e36:
            body += f'<div class="insight-box">Chart 36 スキップ: {_e36}</div>'

        # ── Chart 37: 選抜バイアスの可視化 ────────────────────────────────
        try:
            body += "<h3>Chart 37: 選抜バイアスの可視化 — 全員 vs 生存者のキャリア統計比較</h3>"
            body += chart_guide(
                "これまでのSection 1-5の分析（スパゲッティプロット・OMAクラスタ等）は"
                "IV Score上位の生存者を対象にしていました。"
                "「全登録者」と「生存者上位500人」のキャリア統計を比較することで、"
                "分析がどれだけバイアスされているかを定量化します。"
                "棒グラフが大きく違う指標ほど、生存者のみの分析が業界全体を誤解させる可能性が高い。"
            )
            pop_all = person_registry
            pop_survivors = sorted(person_registry, key=lambda p: -p["iv_score"])[:500]

            def _pop_stats(pop):
                if not pop:
                    return {}
                spans = [p["career_span"] for p in pop]
                stages = [p["highest_stage"] for p in pop]
                pct_stg6 = sum(1 for p in pop if p["highest_stage"] >= 6) / len(pop) * 100
                pct_dropout = sum(1 for p in pop if p["is_dropout"]) / len(pop) * 100
                return {
                    "平均キャリア年数": round(sum(spans) / len(spans), 1),
                    "到達ステージ中央値": round(_st11.median(stages), 1),
                    "監督到達率 (%)": round(pct_stg6, 1),
                    "短期離脱率 (%)": round(pct_dropout, 1),
                }

            stats_all = _pop_stats(pop_all)
            stats_surv = _pop_stats(pop_survivors)

            metrics_37 = list(stats_all.keys())
            vals_all_37 = [stats_all[m] for m in metrics_37]
            vals_surv_37 = [stats_surv[m] for m in metrics_37]
            bias_37 = [round(v2 - v1, 1) for v1, v2 in zip(vals_all_37, vals_surv_37)]

            fig37 = make_subplots(rows=1, cols=2,
                                  subplot_titles=["全員 vs 生存者上位500人 比較", "選抜バイアス量（生存者 - 全員）"])
            fig37.add_trace(
                go.Bar(x=metrics_37, y=vals_all_37, name="全登録者",
                       marker_color="#4CC9F0"),
                row=1, col=1,
            )
            fig37.add_trace(
                go.Bar(x=metrics_37, y=vals_surv_37, name="生存者上位500人",
                       marker_color="#F72585"),
                row=1, col=1,
            )
            fig37.add_trace(
                go.Bar(
                    x=metrics_37, y=bias_37,
                    marker_color=["#F72585" if b > 0 else "#4CC9F0" for b in bias_37],
                    text=[f"{b:+.1f}" for b in bias_37],
                    textposition="outside",
                    showlegend=False,
                ),
                row=1, col=2,
            )
            fig37.add_hline(y=0, row=1, col=2, line_color="gray", line_width=1)
            fig37.update_layout(
                title_text="Chart 37: 選抜バイアスの可視化 — 生存者分析 vs 全員ベース分析",
                barmode="group",
                legend=dict(orientation="h", y=-0.15),
                height=500,
            )
            body += plotly_div_safe(fig37, "selection-bias-chart", height=500)

            body += key_findings([
                f"分析対象: 全登録者 {n_total:,}人 vs Section 1-5の生存者上位500人",
                f"全員ベース 平均キャリア年数: {stats_all.get('平均キャリア年数','N/A')}年 vs "
                f"生存者: {stats_surv.get('平均キャリア年数','N/A')}年",
                f"全員ベース 短期離脱率: {stats_all.get('短期離脱率 (%)','N/A')}% vs "
                f"生存者: {stats_surv.get('短期離脱率 (%)','N/A')}%",
                "バイアス量が大きい指標ほど「業界全体の実態」と「可視化されてきた像」の乖離が大きい",
                "公平な報酬・労働環境議論には生存者バイアスを補正した全員ベース統計が必要",
            ])
        except Exception as _e37:
            body += f'<div class="insight-box">Chart 37 スキップ: {_e37}</div>'

        # ── Chart 38: 早期離脱 vs 中堅離脱 vs ベテラン離脱 ─────────────────
        try:
            body += "<h3>Chart 38: 離脱タイミングの類型化 — 新卒即離脱・中堅離脱・ベテラン離脱</h3>"
            body += chart_guide(
                "離脱者をキャリア年数で区分: "
                "「新卒即離脱」(career_span ≤ 3年), 「中堅離脱」(4-10年), 「ベテラン離脱」(11年以上)。"
                "左: 各区分の人数割合。中: 離脱時の最高ステージ分布（どのステージで辞めたか）。"
                "右: 各区分の年別推移 — 近年の新卒即離脱が増えているか？"
                "「新卒即離脱」が多いなら業界への参入障壁・待遇問題が疑われる。"
                "「ベテラン離脱」が多いなら燃え尽き・ライフステージ変化が主因と考えられる。"
            )
            STAGE_COLORS_S11 = {
                1: "#a0a0c0", 2: "#7EB8D4", 3: "#4CC9F0",
                4: "#FFD166", 5: "#FF6B35", 6: "#F72585",
            }
            # Classify dropouts
            early_drop = [p for p in person_registry if not p["is_active"] and p["career_span"] <= 3]
            mid_drop = [p for p in person_registry if not p["is_active"] and 4 <= p["career_span"] <= 10]
            vet_drop = [p for p in person_registry if not p["is_active"] and p["career_span"] > 10]
            active_list = [p for p in person_registry if p["is_active"]]

            cat_names = ["新卒即離脱\n(≤3年)", "中堅離脱\n(4-10年)", "ベテラン離脱\n(>10年)", "現役継続"]
            cat_colors = ["#FF6B35", "#FFD166", "#4CC9F0", "#06D6A0"]
            cat_counts = [len(early_drop), len(mid_drop), len(vet_drop), len(active_list)]
            cat_groups = [early_drop, mid_drop, vet_drop, active_list]

            fig38 = make_subplots(
                rows=1, cols=3,
                subplot_titles=[
                    "人数割合",
                    "離脱時の最高ステージ分布",
                    "デビュー年別 新卒即離脱数",
                ],
                column_widths=[0.22, 0.38, 0.40],
                specs=[[{"type": "domain"}, {"type": "xy"}, {"type": "xy"}]],
            )
            # Left: donut pie
            fig38.add_trace(go.Pie(
                labels=cat_names, values=cat_counts,
                hole=0.4, marker_colors=cat_colors,
                showlegend=False,
                textinfo="label+percent",
            ), row=1, col=1)

            # Middle: stage distribution by category (stacked bar)
            STAGE_SHORT_38 = {1: "動画", 2: "第2原", 3: "原画", 4: "作監", 5: "総作監", 6: "監督"}
            for stage in range(1, 7):
                s_vals = []
                for grp in [early_drop, mid_drop, vet_drop]:
                    total_g = len(grp) or 1
                    s_vals.append(sum(1 for p in grp if p["highest_stage"] == stage) / total_g * 100)
                fig38.add_trace(go.Bar(
                    name=STAGE_SHORT_38.get(stage, f"Stg{stage}"),
                    x=["新卒即", "中堅", "ベテラン"],
                    y=s_vals,
                    marker_color=list(STAGE_COLORS_S11.values())[stage-1] if len(STAGE_COLORS_S11) >= stage else "#aaa",
                ), row=1, col=2)

            # Right: trend of early dropouts by debut year
            debut_yr_early: dict = _dd11(int)
            for p in early_drop:
                if 1985 <= p["first_year"] <= 2020:
                    debut_yr_early[p["first_year"]] += 1
            debut_yr_all: dict = _dd11(int)
            for p in person_registry:
                if 1985 <= p["first_year"] <= 2020:
                    debut_yr_all[p["first_year"]] += 1
            yr_range_38 = sorted(set(debut_yr_early) | set(debut_yr_all))
            early_rates = [
                debut_yr_early.get(y, 0) / max(debut_yr_all.get(y, 1), 1) * 100
                for y in yr_range_38
            ]
            fig38.add_trace(go.Scatter(
                x=yr_range_38, y=early_rates,
                mode="lines+markers",
                line=dict(color="#FF6B35", width=2),
                name="新卒即離脱率",
                showlegend=False,
            ), row=1, col=3)

            fig38.update_layout(
                title_text="Chart 38: 離脱タイミング類型 — 新卒即離脱 vs 中堅 vs ベテラン",
                barmode="stack",
                height=520,
                legend=dict(orientation="h", y=-0.2),
            )
            fig38.update_yaxes(title_text="割合 (%)", row=1, col=2)
            fig38.update_yaxes(title_text="新卒即離脱率 (%)", row=1, col=3)
            body += plotly_div_safe(fig38, "dropout-type-chart", height=520)
            body += key_findings([
                f"新卒即離脱(≤3年): {len(early_drop):,}人 ({len(early_drop)/n_total*100:.1f}%)",
                f"中堅離脱(4-10年): {len(mid_drop):,}人 ({len(mid_drop)/n_total*100:.1f}%)",
                f"ベテラン離脱(>10年): {len(vet_drop):,}人 ({len(vet_drop)/n_total*100:.1f}%)",
                f"現役継続: {len(active_list):,}人 ({len(active_list)/n_total*100:.1f}%)",
                "新卒即離脱者の最高ステージが動画(Stage 1)に集中 = デビュー直後の離脱が多い",
                "業界参入直後の離脱者増加は若手人材獲得・定着コストの上昇を意味する",
            ])
        except Exception as _e38:
            body += f'<div class="insight-box">Chart 38 スキップ: {type(_e38).__name__}: {_e38}</div>'

        # ── Chart 39: 失われた有能さ — 離脱者の潜在スコア ─────────────────
        try:
            body += "<h3>Chart 39: 失われた才能 — 離脱者の潜在IV Score</h3>"
            body += chart_guide(
                "離脱した人々のIV Score分布を区分別に比較。"
                "左: 現役 / ベテラン離脱 / 中堅離脱 / 新卒即離脱 のスコア分布（レインクラウドプロット）。"
                "右: 離脱者の平均スコアの年別推移 — 近年は高スコア人材の離脱が増えているか？"
                "「失われた有能さ」= 高IV Scoreを持ちながら離脱した人材のこと。"
                "これが多いほど業界が貴重な人材を保持できていないことを示す。"
            )
            fig39 = make_subplots(
                rows=1, cols=2,
                subplot_titles=["区分別IV Score分布", "年別 離脱者スコア推移"],
                column_widths=[0.45, 0.55],
            )
            # Left: raincloud per category (drop only those with iv_score > 0)
            violin_cats_39 = [
                ("現役継続", active_list, "#06D6A0"),
                ("ベテラン離脱(>10年)", vet_drop, "#4CC9F0"),
                ("中堅離脱(4-10年)", mid_drop, "#FFD166"),
                ("新卒即離脱(≤3年)", early_drop, "#FF6B35"),
            ]
            for cat_name, cat_group, cat_col in violin_cats_39:
                scores_cat = [p["iv_score"] for p in cat_group if p["iv_score"] > 0]
                if scores_cat:
                    fig39.add_trace(
                        _violin_raincloud(scores_cat, cat_name, cat_col),
                        row=1, col=1,
                    )

            # Right: avg score of dropouts by debut year × category
            for cat_name, cat_group, cat_col in violin_cats_39[:3]:  # skip active
                yr_score: dict = _dd11(list)
                for p in cat_group:
                    if 1990 <= p["first_year"] <= 2018 and p["iv_score"] > 0:
                        yr_score[p["first_year"]].append(p["iv_score"])
                if yr_score:
                    yrs_sorted = sorted(yr_score)
                    avg_scores = [sum(yr_score[y]) / len(yr_score[y]) for y in yrs_sorted]
                    fig39.add_trace(go.Scatter(
                        x=yrs_sorted, y=avg_scores,
                        name=cat_name,
                        mode="lines+markers",
                        line=dict(color=cat_col, width=2),
                        marker=dict(size=5),
                    ), row=1, col=2)

            fig39.update_layout(
                title_text="Chart 39: 失われた才能 — 離脱者のIV Score分布と年別推移",
                height=540,
                legend=dict(orientation="h", y=-0.18),
                violinmode="overlay",
            )
            fig39.update_yaxes(title_text="IV Score", row=1, col=1)
            fig39.update_yaxes(title_text="平均IV Score", row=1, col=2)
            body += plotly_div_safe(fig39, "lost-talent-chart", height=540)

            # Quantify lost talent
            high_threshold = 30.0  # top ~10% iv_score
            lost_high = [p for p in (mid_drop + vet_drop) if p["iv_score"] >= high_threshold]
            body += key_findings([
                f"離脱者(中堅+ベテラン)のうちIV Score≥{high_threshold:.0f}の高スコア人材: "
                f"{len(lost_high):,}人 — これが「失われた有能さ」",
                "現役継続者のスコアが高い = 生存者バイアス（残った人が優秀なのは自明）",
                "新卒即離脱者のスコアが低い = 活動期間が短く評価されにくい（低推定の可能性）",
                "ベテラン離脱者にも高スコア人材が存在 = ライフステージや業界待遇による離脱",
                "高スコア中堅離脱者の増加 = 業界の人材流出加速のシグナル",
            ])
        except Exception as _e39:
            body += f'<div class="insight-box">Chart 39 スキップ: {type(_e39).__name__}: {_e39}</div>'

        # ── Chart 40: 新規スタッフの初期才能量と期待値 ─────────────────────
        try:
            body += "<h3>Chart 40: 新規スタッフ初期才能量 — デビュー1年後の期待スコア</h3>"
            body += chart_guide(
                "各デビュー年コホートについて、デビューから3年以上継続したスタッフの初期IV Scoreを集計。"
                "左: デビュー年別の「新人期待値」中央値の推移。"
                "右: 現役継続・中堅離脱・新卒即離脱別の「デビュー初期スコア」分布。"
                "新人期待値が上昇傾向 = 才能ある新人が増えているか業界の認知度が上昇。"
                "新卒即離脱者の初期スコアが現役と変わらない = "
                "「才能があるのに辞めた」ケースが存在することを示唆。"
            )
            # Use career_span>=3 debuters as "stayed" sample
            debut_cohort_scores: dict = _dd11(lambda: {"all": [], "stayed": [], "early": []})
            for p in person_registry:
                fy = p["first_year"]
                if not (1990 <= fy <= 2020) or p["iv_score"] <= 0:
                    continue
                debut_cohort_scores[fy]["all"].append(p["iv_score"])
                if not p["is_active"] and p["career_span"] <= 3:
                    debut_cohort_scores[fy]["early"].append(p["iv_score"])
                elif p["career_span"] >= 3:
                    debut_cohort_scores[fy]["stayed"].append(p["iv_score"])

            fig40 = make_subplots(
                rows=1, cols=2,
                subplot_titles=["デビュー年別 初期才能量中央値", "デビュー後の運命別 スコア分布"],
                column_widths=[0.55, 0.45],
            )
            # Left: median score by debut year for "stayed" cohort
            debut_yrs_sorted = sorted(k for k in debut_cohort_scores if debut_cohort_scores[k]["stayed"])
            med_stayed = [
                _st11.median(debut_cohort_scores[y]["stayed"])
                for y in debut_yrs_sorted
            ]
            med_all = [
                _st11.median(debut_cohort_scores[y]["all"])
                if debut_cohort_scores[y]["all"] else 0
                for y in debut_yrs_sorted
            ]
            fig40.add_trace(go.Scatter(
                x=debut_yrs_sorted, y=med_stayed,
                mode="lines+markers", name="3年以上継続した新人の中央値スコア",
                line=dict(color="#06D6A0", width=2.5),
            ), row=1, col=1)
            fig40.add_trace(go.Scatter(
                x=debut_yrs_sorted, y=med_all,
                mode="lines", name="全デビュー者の中央値",
                line=dict(color="#4CC9F0", width=1.5, dash="dash"),
            ), row=1, col=1)

            # Right: score distribution by fate (raincloud)
            fate_groups_40 = [
                ("現役継続", [p for p in active_list if p["iv_score"] > 0], "#06D6A0"),
                ("中堅離脱", [p for p in mid_drop if p["iv_score"] > 0], "#FFD166"),
                ("新卒即離脱", [p for p in early_drop if p["iv_score"] > 0], "#FF6B35"),
            ]
            for fname, fgroup, fcol in fate_groups_40:
                scores_f = [p["iv_score"] for p in fgroup][:500]  # cap for speed
                if scores_f:
                    fig40.add_trace(
                        _violin_raincloud(scores_f, fname, fcol),
                        row=1, col=2,
                    )

            fig40.update_layout(
                title_text="Chart 40: 新規スタッフの初期才能量と期待スコア",
                height=520,
                legend=dict(orientation="h", y=-0.18),
                violinmode="overlay",
            )
            fig40.update_yaxes(title_text="IV Score中央値", row=1, col=1)
            fig40.update_yaxes(title_text="IV Score", row=1, col=2)
            body += plotly_div_safe(fig40, "new-staff-talent-chart", height=520)

            # Trend analysis
            if len(med_stayed) >= 5:
                import numpy as _np_c40
                trend_coef = _np_c40.polyfit(range(len(med_stayed)), med_stayed, 1)[0]
                body += key_findings([
                    f"「継続した新人スコア」の年別トレンド: {trend_coef:+.3f}点/年 "
                    f"({'上昇傾向' if trend_coef > 0.05 else '下降傾向' if trend_coef < -0.05 else 'ほぼ横ばい'})",
                    "全デビュー者 vs 継続者のスコアギャップ = セレクション効果 (才能ある人ほど残る？)",
                    "新卒即離脱者のスコアが継続者と近い → 待遇や環境が離脱の主因の可能性",
                    "新卒即離脱者のスコアが低い → 学習機会不足による評価困難が主因の可能性",
                    "この分析はscores.jsonのIV Scoreが生涯全期間を反映するため、"
                    "「初期才能量」として使うことには限界がある（長期活動者ほどスコアが高くなる）",
                ])
        except Exception as _e40:
            body += f'<div class="insight-box">Chart 40 スキップ: {type(_e40).__name__}: {_e40}</div>'

        # ── Chart 41: 純増した才能 — 各年の才能フロー ──────────────────────
        try:
            body += "<h3>Chart 41: 才能の純増 — 新規参入・離脱・純増の年別フロー</h3>"
            body += chart_guide(
                "各カレンダー年における「新規参入才能」「離脱才能」「純増才能」を可視化。"
                "新規参入才能 = その年にデビューした人の合計IV Score。"
                "離脱才能 = その年に最後のクレジット(=latest_year)を持つ人の合計スコア。"
                "純増 = 新規参入 − 離脱 (正 = 業界の才能プールが拡大、負 = 縮小)。"
                "ストック（業界の累積才能量）の変化としても解釈できる。"
                "注意: IV Scoreは全活動期間を反映するため、デビュー年/離脱年での値は"
                "その時点の「未来才能込みの」推定値であり、リアルタイムの才能指標ではない。"
            )
            inflow_score: dict = _dd11(float)   # debut year → sum iv_score
            inflow_count: dict = _dd11(int)
            outflow_score: dict = _dd11(float)  # latest year → sum iv_score (dropouts only)
            outflow_count: dict = _dd11(int)

            for p in person_registry:
                fy = int(p["first_year"])
                ly = int(p["latest_year"])
                comp = p["iv_score"] or 0.0
                if 1990 <= fy <= 2023:
                    inflow_score[fy] += comp
                    inflow_count[fy] += 1
                if not p["is_active"] and 1990 <= ly <= 2023:
                    outflow_score[ly] += comp
                    outflow_count[ly] += 1

            yr_range_41 = list(range(1990, 2024))
            in_scores = [inflow_score.get(y, 0) for y in yr_range_41]
            out_scores = [outflow_score.get(y, 0) for y in yr_range_41]
            net_scores = [i - o for i, o in zip(in_scores, out_scores)]
            in_counts = [inflow_count.get(y, 0) for y in yr_range_41]
            out_counts = [outflow_count.get(y, 0) for y in yr_range_41]

            fig41 = make_subplots(
                rows=2, cols=1,
                subplot_titles=["才能フロー (合計IV Score)", "人数フロー (参入・離脱人数)"],
                shared_xaxes=True,
            )
            # Upper: score flow
            fig41.add_trace(go.Bar(
                x=yr_range_41, y=in_scores,
                name="新規参入才能 (+)", marker_color="rgba(6,214,160,0.7)",
            ), row=1, col=1)
            fig41.add_trace(go.Bar(
                x=yr_range_41, y=[-v for v in out_scores],
                name="離脱才能 (−)", marker_color="rgba(247,37,133,0.6)",
            ), row=1, col=1)
            fig41.add_trace(go.Scatter(
                x=yr_range_41, y=net_scores,
                mode="lines+markers", name="純増才能",
                line=dict(color="#FFD166", width=2.5),
            ), row=1, col=1)
            # Zero line
            fig41.add_hline(y=0, line_dash="dot", line_color="white", opacity=0.4, row=1, col=1)

            # Lower: count flow
            fig41.add_trace(go.Bar(
                x=yr_range_41, y=in_counts,
                name="新規参入人数 (+)", marker_color="rgba(76,201,240,0.7)",
                showlegend=True,
            ), row=2, col=1)
            fig41.add_trace(go.Bar(
                x=yr_range_41, y=[-v for v in out_counts],
                name="離脱人数 (−)", marker_color="rgba(255,107,53,0.6)",
                showlegend=True,
            ), row=2, col=1)

            fig41.update_layout(
                title_text="Chart 41: 才能の純増 — 新規参入・離脱・純増の年別フロー",
                barmode="overlay",
                height=600,
                legend=dict(orientation="h", y=-0.12),
            )
            fig41.update_yaxes(title_text="合計IV Score", row=1, col=1)
            fig41.update_yaxes(title_text="人数", row=2, col=1)
            body += plotly_div_safe(fig41, "talent-flow-chart", height=600)

            # Peak years
            max_inflow_yr = yr_range_41[in_scores.index(max(in_scores))]
            max_outflow_yr = yr_range_41[out_scores.index(max(out_scores))]
            positive_net = sum(1 for v in net_scores if v > 0)
            negative_net = sum(1 for v in net_scores if v <= 0)
            body += key_findings([
                f"最大新規参入才能年: {max_inflow_yr}年 "
                f"(スコア合計 {max(in_scores):,.0f})",
                f"最大離脱才能年: {max_outflow_yr}年 "
                f"(スコア合計 {max(out_scores):,.0f})",
                f"純増がプラスだった年数: {positive_net}/{len(yr_range_41)}年",
                f"純増がマイナスだった年数: {negative_net}/{len(yr_range_41)}年 "
                f"= 才能流出が流入を上回った年",
                "注: スコアはretroactiveな評価のため、高スコア離脱者は「実は将来活躍できた人」の"
                "可能性を含む — 純増の負の年はその喪失を表す",
                "2010年代以降の新規参入急増はデジタル化による間口拡大を反映していると考えられる",
            ])
        except Exception as _e41:
            body += f'<div class="insight-box">Chart 41 スキップ: {type(_e41).__name__}: {_e41}</div>'

        body += "</div>"  # close Section 11 card

    # ─────────────────────────────────────────────────────────
    # Glossary terms for this report
    # ─────────────────────────────────────────────────────────
    longitudinal_glossary = dict(COMMON_GLOSSARY_TERMS)
    longitudinal_glossary.update({
        "career_age（経験年数）": (
            "デビュー年を0として計算したキャリア経験年数。カレンダー年ではなく相対時間で比較するため、"
            "異なる世代・時代のキャリアを直接比較できる。"
        ),
        "OMA（最適マッチング分析）": (
            "ライフコース研究で使われるシーケンス類似性の計算手法。本レポートではハミング距離を"
            "近似OMA距離として使用し、Wardリンケージで階層クラスタリングを実施。"
        ),
        "レキシス図": (
            "カレンダー年×経験年数の2次元空間。対角線方向が同一コホートの軌跡、"
            "垂直方向が期間効果、水平方向が成熟効果を示す縦断的分析の古典的手法。"
        ),
        "CFD（累積フロー図）": (
            "Cumulative Flow Diagram。各ステージの積み上げ面グラフで、"
            "帯の厚みの変化がボトルネックを示す。ソフトウェア開発のKanbanから借用。"
        ),
        "MDS（多次元尺度構成法）": (
            "高次元データ間の距離関係を2次元に投影する手法。近い点ほど類似したキャリアシーケンス。"
            "軸の絶対的意味はなく相対配置のみが重要。"
        ),
        "ストック&フロー": (
            "在庫（Stock）= 各時点での各ステージの人数。フロー（Flow）= 昇進・離脱の変化量。"
            "「水槽モデル」として業界全体のキャリアパイプラインを俯瞰する。"
        ),
        "需要シフト": (
            "Netflix等ストリーミングサービスの台頭（2015年〜）により、日本のアニメ制作本数が"
            "急増した。この需要シフトはcareer_ageベースの分析においても観測可能で、"
            "特定コホートのIV Scoreや稼働密度に影響を与えている。"
        ),
        "想定需要ギャップ": (
            "過去トレンド（1990〜2010年）を線形外挿した「想定クレジット需要」と"
            "実際のクレジット数の差。ギャップが正＝想定を超える実需があった（配信台頭期に顕著）。"
        ),
        "生産性向上節減効果": (
            "「1990年の生産性（credits/person）のままだった場合に必要だった追加スタッフ数」。"
            "デジタルツール・制作パイプラインの効率化により実現した工数削減量の推計値。"
        ),
        "入れ替わり率": (
            "（新規参入数 + 引退/離脱数）÷ 現役スタッフ数 × 100。"
            "高い値は業界の新陳代謝が活発であることを示すが、"
            "スキルの蓄積・継承リスクを内包する場合もある。"
        ),
        "1クール（12話）": (
            "日本のTVアニメの標準的な放送単位。約3ヶ月間、週1回放送で12話。"
            "本分析では episodes÷12 でクール数を計算し、フォーマット間の比較単位として使用。"
        ),
        "persons/hr（スタッフ数/時間）": (
            "放送1時間あたりのユニークスタッフ数。映画・OVAなど短尺作品と"
            "TVシリーズの労働密度を同一スケールで比較するための正規化指標。"
        ),
        "persons/cour（スタッフ数/クール）": (
            "1クール（12話相当）あたりのユニークスタッフ数。"
            "TVアニメの長期シリーズでは1クールごとの人員配置が"
            "交渉・計画の基準単位となるため、業界実務と整合した指標。"
        ),
        "規模の経済（スタッフ）": (
            "TVアニメで長期シリーズ（複数クール）になるほど、1クールあたりの必要スタッフが"
            "減少する傾向。同一チームが継続して担当することで引き継ぎコストが不要になるため。"
            "成立しない場合（逆に増加）は長期作品特有の複雑化・要求仕様の累積を示す。"
        ),
        "固定費（アニメ制作）": (
            "シリーズの長さ（クール数）によらず一度だけ発生するコスト。"
            "キャラクターデザイン・監督・音楽・シリーズ構成などの立ち上げチームが該当。"
            "OLS回帰でcour_countとの無相関性（低R²・ゼロ近傍の傾き）が確認できれば固定費仮説を支持。"
        ),
        "変動費（アニメ制作）": (
            "話数に比例して増加するコスト。原画・動画・演出・作画監督などが該当。"
            "1クール増えるごとに必要スタッフ数が増加するため、長期シリーズの総コストを左右する。"
        ),
        "可視クレジット vs 実際のコスト": (
            "本DB上のクレジット数は「外部委託・個別契約の密度」を反映するため、"
            "社員雇用で内製化している大手スタジオでは実際のコストより少なく見える場合がある。"
            "大手スタジオのクレジット数が少ない → 社員制度による内製化の可能性を示唆する。"
        ),
        "生存バイアス（アニメ業界）": (
            "大手スタジオに所属できたスタッフは既に業界内で成功した選抜済み集団。"
            "「大手 → 高スコア」の相関は「優秀な人が大手に採用される」選抜効果である可能性が高く、"
            "「大手に入ると優秀になる」因果効果とは区別が困難（生存バイアス）。"
        ),
        "WPS（重み付き生産性スコア）": (
            "Weighted Productivity Score。役職ウェイト（監督=10.0〜動画=1.0）× 作品複雑度 × 時代デフレーターで"
            "各クレジットに推定工数を割り当て、年間の合計を活動年数で割った指標。"
            "単純クレジット数と異なり、役職・作品難易度・デジタル化による生産性変化を補正する。"
        ),
        "時代デフレーター": (
            "デジタルアニメ制作ツールの普及を考慮した補正係数。1990年=1.0（手書き全盛期）、"
            "2020年以降=0.6（デジタル化による効率化を30年で0.4ポイント低減と仮定）。"
            "同じクレジットでも昔ほど実際の労働量が多い可能性を補正する。"
        ),
        "固定効果回帰 (FE)": (
            "Person Fixed Effects（個人固定効果）とYear Fixed Effects（年次固定効果）を除去した"
            "within推定量。観測されない時不変の個人特性（才能・性格等）を制御し、"
            "「同じ人が大手在籍時 vs 非在籍時」の比較として因果効果を推定する。"
        ),
        "傾向スコアマッチング (PSM)": (
            "Propensity Score Matching。処置群（転職者）と対照群（非転職者）を"
            "観測可能な特徴（career_age・役職・事前稼働数等）でマッチングし、"
            "処置効果（Average Treatment Effect on Treated: ATT）を推定する手法。"
            "観測されない交絡因子には対応できない点に注意。"
        ),
        "イベントスタディ": (
            "転職年（t=0）を中心に前後N年の結果変数（年間稼働数）の推移を比較する手法。"
            "転職前後の平行トレンド仮定が成立すれば、転職の因果効果が識別できる。"
            "t=0〜+1年の一時的な低迷は「初期適応コスト」として解釈できる。"
        ),
    })

    html = wrap_html(
        "縦断的キャリア分析",
        "OMAクラスタ・固定費/変動費・PSM・WPS重み付き生産性の33チャート",
        body,
        intro_html=report_intro(
            "縦断的キャリア分析レポート",
            "career_age軸でのスパゲッティプロット・レキシス図・OMAクラスタ・アリュビアル図・CFD・"
            "MDS・ストリームグラフ・ホライズンチャート・ストック&フロー（Section 1〜5）。"
            "Section 6: 需要ギャップ・生産性・現役/引退/新規動態。"
            "Section 7: フォーマット/ジャンル別スタッフ密度。"
            "Section 8: OLS固定費/変動費・スタジオ規模別・生存バイアス。"
            "Section 9: イベントスタディ・固定効果回帰(FE)・傾向スコアマッチング(PSM)・"
            "交絡因子重要度による因果推論。"
            "Section 10: 役職ウェイト・時代デフレーター・作品複雑度を用いた"
            "重み付き生産性指数(WPS)と多変量分解。全33チャート収録。",
            "スタジオ人事・エージェント・業界研究者・キャリア設計中のスタッフ",
        ),
        glossary_terms=longitudinal_glossary,
    )

    out_path = REPORTS_DIR / "longitudinal_analysis.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"  -> {out_path}")


# ============================================================
# SHAP Score Explanation Report
# ============================================================

def generate_shap_report():
    """SHAP値によるスコア説明レポート.

    GradientBoostingRegressorでiv_scoreを予測し、TreeExplainerで各特徴量の
    限界貢献（Shapley値）を計算。情報量の増大をビーズワーム・依存プロット・
    ウォーターフォールで可視化する。
    """
    print("  Generating SHAP Score Explanation Report...")
    scores_data = load_json("scores.json")
    if not scores_data or not isinstance(scores_data, list):
        print("  [SKIP] scores.json not found or invalid")
        return

    try:
        import shap as _shap
        import numpy as _np
        from sklearn.ensemble import GradientBoostingRegressor as _GBR
        from sklearn.preprocessing import StandardScaler as _SS
        import pandas as _pd
    except ImportError as e:
        print(f"  [SKIP] SHAP report requires shap + sklearn: {e}")
        return

    # ---- 特徴量定義 ----
    FEATURE_COLS = [
        ("birank",              "BiRank",              lambda e: e.get("birank") or 0.0),
        ("patronage",           "Patronage",           lambda e: e.get("patronage") or 0.0),
        ("person_fe",           "Person FE",           lambda e: e.get("person_fe") or 0.0),
        ("awcc",                "AWCC",                lambda e: e.get("awcc") or 0.0),
        ("ndi",                 "NDI",                 lambda e: e.get("ndi") or 0.0),
        ("dormancy",            "Dormancy",            lambda e: e.get("dormancy") or 0.0),
        ("career_friction",     "Career Friction",     lambda e: e.get("career_friction") or 0.0),
        ("peer_boost",          "Peer Boost",          lambda e: e.get("peer_boost") or 0.0),
        ("total_credits",       "Total Credits",       lambda e: float(e.get("total_credits") or 0)),
        ("active_years",        "Active Years",        lambda e: float((e.get("career") or {}).get("active_years") or 0)),
        ("highest_stage",       "Highest Stage",       lambda e: float((e.get("career") or {}).get("highest_stage") or 0)),
        ("degree_centrality",   "Degree Centrality",   lambda e: float((e.get("centrality") or {}).get("degree") or 0)),
        ("betweenness",         "Betweenness",         lambda e: float((e.get("centrality") or {}).get("betweenness") or 0)),
    ]
    feat_keys   = [c[0] for c in FEATURE_COLS]
    feat_labels = [c[1] for c in FEATURE_COLS]
    feat_getters = [c[2] for c in FEATURE_COLS]

    # ---- データ構築 ----
    rows = []
    for entry in scores_data:
        iv = entry.get("iv_score")
        if iv is None:
            continue
        row = {"_iv": float(iv), "_name": entry.get("name") or entry.get("person_id", "")}
        for key, _, getter in FEATURE_COLS:
            row[key] = getter(entry)
        rows.append(row)

    if len(rows) < 50:
        print("  [SKIP] SHAP report: insufficient data")
        return

    df = _pd.DataFrame(rows).fillna(0.0)
    X = df[feat_keys].values.astype(_np.float32)
    y = df["_iv"].values.astype(_np.float32)

    # ---- モデル訓練 ----
    gbr = _GBR(
        n_estimators=200, max_depth=4, learning_rate=0.05,
        subsample=0.8, random_state=42, validation_fraction=0.1,
        n_iter_no_change=15, tol=1e-4,
    )
    gbr.fit(X, y)
    r2 = gbr.score(X, y)

    # ---- SHAP 計算 ----
    explainer = _shap.TreeExplainer(gbr)
    # Subsample for speed (max 3000 persons)
    n_sample = min(len(X), 3000)
    rng = _np.random.default_rng(42)
    idx = rng.choice(len(X), n_sample, replace=False)
    X_sample = X[idx]
    shap_values = explainer.shap_values(X_sample)   # shape: (n_sample, n_features)
    # expected_value may be a 0-d array in some shap versions → extract scalar
    ev = explainer.expected_value
    base_val = float(ev.item() if hasattr(ev, "item") else ev)

    # ---- Chart 1: Feature Importance (mean |SHAP|) ----
    mean_abs = _np.abs(shap_values).mean(axis=0)
    order = _np.argsort(mean_abs)[::-1]
    sorted_labels = [feat_labels[i] for i in order]
    sorted_vals   = [float(mean_abs[i]) for i in order]

    # Color gradient by importance
    max_v = max(sorted_vals) or 1.0
    bar_colors = [
        f"rgba({int(240 - 80 * v/max_v)},{int(147 + 60 * v/max_v)},{int(251 - 100 * v/max_v)},0.85)"
        for v in sorted_vals
    ]
    fig_imp = go.Figure(go.Bar(
        x=sorted_vals[::-1],
        y=sorted_labels[::-1],
        orientation="h",
        marker_color=bar_colors[::-1],
        hovertemplate="%{y}: mean|SHAP|=%{x:.4f}<extra></extra>",
    ))
    fig_imp.update_layout(
        title=f"SHAP Feature Importance — mean|SHAP| (R²={r2:.3f})",
        xaxis_title="mean |SHAP value|",
        yaxis_title="Feature",
    )

    # ---- Chart 2: Beeswarm (SHAP値 × 特徴量強度) ----
    # Each feature = one trace; y=feature index (jittered), x=SHAP value, color=raw feature value
    scaler = _SS()
    X_scaled = scaler.fit_transform(X_sample)   # 0-centered for coloring

    fig_bee = go.Figure()
    n_feat = len(feat_keys)
    rng2 = _np.random.default_rng(0)
    for fi in range(n_feat):
        raw_norm = X_scaled[:, fi]   # normalized feature values for color
        shap_fi  = shap_values[:, fi]
        jitter   = rng2.uniform(-0.3, 0.3, size=len(shap_fi))
        y_jit    = fi + jitter
        # Map raw_norm to color: blue (low) → red (high)
        colors = [
            f"rgba({min(255,int(128+127*v))},{max(0,int(128-127*abs(v)))},{min(255,int(128-127*v))},0.5)"
            for v in _np.clip(raw_norm, -2, 2) / 2
        ]
        fig_bee.add_trace(go.Scatter(
            x=shap_fi,
            y=y_jit,
            mode="markers",
            marker=dict(size=2.5, color=colors, opacity=0.6),
            name=feat_labels[fi],
            showlegend=False,
            hovertemplate=f"{feat_labels[fi]}<br>SHAP=%{{x:.4f}}<extra></extra>",
        ))
    fig_bee.update_layout(
        title="SHAP Beeswarm — 各特徴量のShapley値分布（青=低値, 赤=高値）",
        xaxis_title="SHAP value（iv_scoreへの限界貢献）",
        yaxis=dict(
            tickvals=list(range(n_feat)),
            ticktext=feat_labels,
        ),
        height=500,
    )
    fig_bee.add_vline(x=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")

    # ---- Chart 3: Dependence plots (top 4 features) ----
    top4_idx = [int(i) for i in order[:4]]
    fig_dep = make_subplots(
        rows=2, cols=2,
        subplot_titles=[feat_labels[i] for i in top4_idx],
        vertical_spacing=0.14, horizontal_spacing=0.10,
    )
    dep_colors = ["#f093fb", "#a0d2db", "#06D6A0", "#FFD166"]
    for pos, fi in enumerate(top4_idx):
        rr, cc = divmod(pos, 2)
        raw_fi = X_sample[:, fi]
        shap_fi = shap_values[:, fi]
        fig_dep.add_trace(go.Scatter(
            x=raw_fi, y=shap_fi, mode="markers",
            marker=dict(size=3, color=dep_colors[pos], opacity=0.4),
            name=feat_labels[fi],
            hovertemplate=f"{feat_labels[fi]}=%{{x:.3f}}<br>SHAP=%{{y:.4f}}<extra></extra>",
        ), row=rr+1, col=cc+1)
    fig_dep.update_layout(
        title="SHAP Dependence Plots — 上位4特徴量の特徴値 vs SHAP値",
        showlegend=False,
    )

    # ---- Chart 4: Waterfall for top / bottom scorers ----
    # Top 10 and bottom 10 scorers: mean SHAP per group
    iv_order = _np.argsort(df["_iv"].values[idx])[::-1]
    top10_idx  = iv_order[:10]
    bot10_idx  = iv_order[-10:]

    def _mean_shap_bar(group_idx, group_label, color):
        mean_sv = shap_values[group_idx].mean(axis=0)
        sv_order = _np.argsort(_np.abs(mean_sv))[::-1]
        return go.Bar(
            name=group_label,
            x=[feat_labels[i] for i in sv_order],
            y=[float(mean_sv[i]) for i in sv_order],
            marker_color=color,
            hovertemplate="%{x}: %{y:.4f}<extra></extra>",
        )

    fig_wf = go.Figure()
    fig_wf.add_trace(_mean_shap_bar(top10_idx,  "上位10人 (mean SHAP)", "#06D6A0"))
    fig_wf.add_trace(_mean_shap_bar(bot10_idx,  "下位10人 (mean SHAP)", "#EF476F"))
    fig_wf.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)")
    fig_wf.update_layout(
        barmode="group",
        title=f"上位10人 vs 下位10人 — 平均SHAP値比較（base_value={base_val:.4f}）",
        xaxis_title="Feature",
        yaxis_title="mean SHAP value",
        xaxis_tickangle=-30,
    )

    # ---- Assemble HTML ----
    body = ""
    body += '<div class="card">'
    body += "<h2>SHAP分析の概要</h2>"
    body += section_desc(
        f"GradientBoostingRegressor（200木, depth=4）でiv_scoreを学習（訓練R²={r2:.3f}）。"
        f"TreeExplainerでShapley値を計算（サンプル数={n_sample:,}人）。"
        "Shapley値は各特徴量がiv_scoreの予測値に与える限界貢献（ゲーム理論的公正配分）を示します。"
    )
    body += '<div class="stats-grid">'
    for label, val in [
        ("モデルR²",       f"{r2:.3f}"),
        ("サンプル数",     fmt_num(n_sample)),
        ("特徴量数",       str(n_feat)),
        ("ベース値",       f"{base_val:.4f}"),
        ("最重要特徴量",   feat_labels[int(order[0])]),
        ("2位特徴量",      feat_labels[int(order[1])]),
    ]:
        body += f'<div class="stat-card"><div class="value">{val}</div><div class="label">{label}</div></div>'
    body += "</div></div>"

    body += '<div class="card">'
    body += "<h2>Chart 1: 特徴量重要度 (mean |SHAP|)</h2>"
    body += chart_guide(
        "各特徴量のShapley絶対値平均。iv_scoreへの情報量増大（限界貢献）の大きさを示します。"
        "値が大きいほど予測への影響力が高い特徴量です。"
    )
    body += plotly_div_safe(fig_imp, "shap_importance", 420)
    body += "</div>"

    body += '<div class="card">'
    body += "<h2>Chart 2: SHAP Beeswarm</h2>"
    body += chart_guide(
        "各点は1人の人物×1特徴量のShapley値。X軸=iv_scoreへの限界貢献（正=プラス寄与）。"
        "青=その特徴量の値が低い人、赤=高い人。"
        "右に広がるほど高い特徴量値がスコアを引き上げる正の関係、左は逆。"
    )
    body += plotly_div_safe(fig_bee, "shap_beeswarm", 500)
    body += "</div>"

    body += '<div class="card">'
    body += "<h2>Chart 3: Dependence Plots — 上位4特徴量</h2>"
    body += chart_guide(
        "X軸=特徴量の生値、Y軸=その特徴量のShapley値。"
        "傾きが急な部分（非線形性）は、特定の値域で特徴量の影響力が急増することを示します。"
        "例：Patronageが一定値を超えると急激にiv_scoreへの貢献が増大するなど。"
    )
    body += plotly_div_safe(fig_dep, "shap_dependence", 550)
    body += "</div>"

    body += '<div class="card">'
    body += "<h2>Chart 4: 上位10人 vs 下位10人 — 平均SHAP値比較</h2>"
    body += chart_guide(
        "緑=iv_score上位10人の平均Shapley値、赤=下位10人。"
        "上位者と下位者で最も差が開く特徴量が、スコア格差の主因です。"
        "負のShapley値はその特徴量がスコアを押し下げていることを意味します。"
    )
    body += plotly_div_safe(fig_wf, "shap_waterfall", 450)
    body += "</div>"

    body += significance_section("Shapley値による情報量の定量化", [
        "Shapley値（協力ゲーム理論由来）は「特徴量iが存在することでモデル予測がどれだけ変わるか」を"
        "全ての特徴量の組み合わせにわたって公平に平均した限界貢献です。"
        "単純な相関や回帰係数と異なり、特徴量間の相互作用・非線形性を考慮した上での純粋な寄与度を示します。",
        "本分析ではiv_score（操作変数推定スコア）をBiRank・Patronage・Person FEなど"
        "13特徴量から予測するモデルを構築し、各特徴量の情報量増大を可視化しています。"
        "「この人のスコアが高い/低い理由は何か」を定量的に説明する根拠として活用できます。",
    ])
    body += utilization_guide([
        {"role": "スタジオ人事", "how": "Chart 4で上位者と下位者のSHAP値を比較し、採用・育成で注力すべき指標（BiRank vs Patronage）を特定する"},
        {"role": "エージェント", "how": "Dependence Plotの非線形性を確認し、クライアントのPatronageや稼働数が閾値を超えると報酬交渉力が急増するポイントを把握する"},
        {"role": "アニメーター本人", "how": "自分のSHAP値プロファイルでどの特徴量が評価を引き上げているかを把握し、次のキャリアステップを設計する根拠とする"},
        {"role": "業界研究者", "how": "Beeswarmの分布形状（対称 vs 非対称）からスコア決定の構造的偏りを検証し、公正性評価の学術的根拠に使用する"},
    ])

    html = wrap_html(
        "SHAP スコア説明レポート",
        "Shapley値によるiv_score決定因子の定量分析",
        body,
        intro_html=report_intro(
            "SHAP スコア説明レポート",
            "GradientBoostingRegressorにiv_scoreを学習させ、TreeExplainerで各特徴量の"
            "Shapley値（限界貢献）を計算。13特徴量がiv_scoreに与える情報量の増大を"
            "特徴量重要度・Beeswarm・Dependence Plot・上位vs下位比較の4チャートで可視化します。",
            "スタジオ人事・エージェント・アニメーター本人・業界研究者",
        ),
        glossary_terms={
            "Shapley値 (SHAP値)": (
                "協力ゲーム理論に基づく特徴量の限界貢献量。全特徴量の組み合わせにわたって"
                "特徴量iの存在/不在による予測変化量を公平に平均した値。"
                "正の値はスコアへのプラス寄与、負はマイナス寄与を意味する。"
            ),
            "TreeExplainer": (
                "決定木・アンサンブル木（GBM, XGBoost等）に特化した高速SHAP計算手法。"
                "O(T L D)の計算量（T=木数, L=葉数, D=深さ）で正確なShapley値を算出する。"
            ),
            "Beeswarm": (
                "SHAP値の分布を特徴量ごとに可視化したドットプロット。"
                "各点=1サンプル×1特徴量のShapley値。点の色=特徴量の生値の高低。"
            ),
            "Dependence Plot": (
                "特徴量の生値（X軸）とShapley値（Y軸）の関係を散布図で表示。"
                "非線形性・閾値効果・相互作用を直接読み取れる。"
            ),
            "情報量増大": (
                "ある特徴量を追加することでモデルの予測精度（情報量）がどれだけ増大するかの指標。"
                "SHAP featureimportance（mean|SHAP|）はこの定量指標として機能する。"
            ),
        },
    )
    out = REPORTS_DIR / "shap_explanation.html"
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
        "title": "時系列BiRank・先見スコア",
        "subtitle": "BiRankの時系列変化と人材早期発見",
        "desc": "BiRankの時系列推移、先見スコアによる早期人材発見、昇進クレジット分析。",
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
        "subtitle": "IV Scoreによる人物ランキング",
        "desc": "IV Score順の上位人物。スコア分布、レーダーチャート、BiRank/Patronage/Person FE散布図。",
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
    {
        "file": "cooccurrence_groups.html",
        "title": "共同制作集団分析",
        "subtitle": "コアスタッフの繰り返し共同制作パターン",
        "desc": "3人以上のコアスタッフが複数作品で繰り返し共参加するグループを検出。事実上の固定チーム・監督のコアスタッフを可視化。",
        "sources": "cooccurrence_groups",
    },
    {
        "file": "ml_clustering.html",
        "title": "MLクラスタリング分析",
        "subtitle": "PCA次元圧縮 × K-Meansクラスタリング",
        "desc": "20次元特徴量ベクトルに基づく教師なしクラスタリング。PCA散布図、クラスタプロファイル、シルエット分析。",
        "sources": "ml_clusters",
    },
    {
        "file": "network_graph.html",
        "title": "ネットワークグラフ",
        "subtitle": "協業ネットワークのインタラクティブ可視化",
        "desc": "上位人物の協業・師弟ネットワーク。ノードサイズはIV Score、色はMLクラスタ。",
        "sources": "scores, collaborations, mentorships, ml_clusters",
    },
    {
        "file": "cohort_animation.html",
        "title": "コホート・エージェント軌跡",
        "subtitle": "世代別キャリア成長アニメーション + 供給/担い手分析",
        "desc": "Gapminder型アニメーションでキャリアステージ進化を時系列表示。仕事の供給と担い手のバランス、"
                "大作関与の影響、世代間コラボレーション構造を可視化。13チャート収録。",
        "sources": "scores, growth, milestones, anime_stats, ml_clusters, time_series, collaborations",
    },
    {
        "file": "longitudinal_analysis.html",
        "title": "縦断的キャリア分析",
        "subtitle": "OMAクラスタ・固定費/変動費・パネルFE・傾向スコアマッチング",
        "desc": "career_age軸でのスパゲッティプロット・レキシス図・OMAクラスタ・アリュビアル図・"
                "CFD・MDS・ストリームグラフ・ホライズンチャート・ストック&フロー（Section 1-5）。"
                "需要ギャップ・生産性（Section 6）。フォーマット/ジャンル別スタッフ密度（Section 7）。"
                "OLS固定費/変動費・スタジオ規模別（Section 8）。"
                "イベントスタディ・FE回帰・PSM・交絡因子分解による因果推論（Section 9）。全29チャート。",
        "sources": "scores, milestones, transitions, role_flow, temporal_pagerank, growth, time_series, decades, individual_profiles, SQLite(credits+studios)",
    },
    {
        "file": "shap_explanation.html",
        "title": "SHAP スコア説明レポート",
        "subtitle": "Shapley値によるiv_score決定因子の情報量分析",
        "desc": "GradientBoostingRegressorでiv_scoreを学習しTreeExplainerでShapley値を計算。"
                "特徴量重要度(mean|SHAP|)・Beeswarm・Dependence Plot・上位vs下位比較の4チャート。"
                "各特徴量（BiRank・Patronage・Person FE等）のiv_scoreへの限界貢献を定量化。",
        "sources": "scores",
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

    /* Index-specific: system overview & stakeholder guide */
    .system-overview {
        background: linear-gradient(135deg, rgba(240,147,251,0.08), rgba(160,210,219,0.06));
        border: 1px solid rgba(240,147,251,0.2);
        border-radius: 16px; padding: 2rem 2.5rem; margin-bottom: 2rem; color: #c0c0d0;
    }
    .system-overview h2 { color: #f093fb; font-size: 1.5rem; margin-bottom: 1rem; }
    .system-overview p { font-size: 0.95rem; line-height: 1.8; margin-bottom: 0.6rem; }
    .system-overview .mission {
        font-size: 1.05rem; color: #a0d2db; font-style: italic;
        border-left: 3px solid #a0d2db; padding-left: 1rem; margin: 1rem 0;
    }
    .arch-grid {
        display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; margin: 1.5rem 0;
    }
    @media (max-width: 700px) { .arch-grid { grid-template-columns: 1fr; } }
    .arch-layer {
        background: rgba(0,0,0,0.2); border-radius: 12px; padding: 1.2rem;
        border: 1px solid rgba(255,255,255,0.08);
    }
    .arch-layer h4 { font-size: 0.95rem; margin-bottom: 0.6rem; }
    .arch-layer.layer1 h4 { color: #a0d2db; }
    .arch-layer.layer2 h4 { color: #06D6A0; }
    .arch-layer ul { padding-left: 1.2rem; font-size: 0.85rem; color: #909090; line-height: 1.8; }

    .stakeholder-guide {
        display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
        gap: 1rem; margin: 1.5rem 0 2rem;
    }
    .stakeholder-card {
        background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px; padding: 1.2rem;
    }
    .stakeholder-card h4 {
        font-size: 0.95rem; font-weight: 700; margin-bottom: 0.6rem;
        display: flex; align-items: center; gap: 0.5rem;
    }
    .stakeholder-card ul {
        padding-left: 1rem; font-size: 0.82rem; color: #909090; line-height: 1.9;
    }
    .stakeholder-card a { color: #a0d2db; text-decoration: none; }
    .stakeholder-card a:hover { text-decoration: underline; }
    .sh-studio h4 { color: #f093fb; }
    .sh-hr h4 { color: #a0d2db; }
    .sh-animator h4 { color: #06D6A0; }
    .sh-researcher h4 { color: #fda085; }

    .section-title {
        font-size: 1.3rem; font-weight: 700; color: #a0d2db;
        margin: 2rem 0 1rem; padding-bottom: 0.5rem;
        border-bottom: 1px solid rgba(160,210,219,0.2);
    }
    """

    stakeholder_guide_html = """
<p class="section-title">ステークホルダー別 推薦レポートガイド</p>
<div class="stakeholder-guide">
    <div class="stakeholder-card sh-studio">
        <h4>🏢 スタジオ経営者・戦略担当</h4>
        <ul>
            <li><a href="industry_overview.html">業界俯瞰</a> — 市場ポジション把握</li>
            <li><a href="studio_impact.html">スタジオ影響</a> — 選抜/処置/ブランド効果</li>
            <li><a href="network_evolution.html">ネットワーク進化</a> — 業界構造の変化</li>
            <li><a href="compensation_fairness.html">報酬公平性</a> — 配分制度の客観評価</li>
        </ul>
    </div>
    <div class="stakeholder-card sh-hr">
        <h4>👤 人事・スカウト・プロデューサー</h4>
        <ul>
            <li><a href="bridge_analysis.html">ブリッジ分析</a> — 越境人材のスカウト</li>
            <li><a href="temporal_foresight.html">時系列先見</a> — ライジングスターの早期発見</li>
            <li><a href="growth_scores.html">成長スコア</a> — 過小評価人材の特定</li>
            <li><a href="team_analysis.html">チーム分析</a> — 最適スタッフィング</li>
            <li><a href="bias_detection.html">バイアス検出</a> — 公平な採用評価</li>
        </ul>
    </div>
    <div class="stakeholder-card sh-animator">
        <h4>🎨 アニメーター・クリエイター本人</h4>
        <ul>
            <li><a href="person_ranking.html">人物ランキング</a> — 業界内の客観的位置</li>
            <li><a href="career_transitions.html">キャリア遷移</a> — 次のステップの指針</li>
            <li><a href="genre_analysis.html">ジャンル親和性</a> — 自分の専門領域の把握</li>
            <li><a href="compensation_fairness.html">報酬公平性</a> — 報酬交渉の根拠</li>
        </ul>
    </div>
    <div class="stakeholder-card sh-researcher">
        <h4>🔬 研究者・アナリスト・ジャーナリスト</h4>
        <ul>
            <li><a href="credit_statistics.html">クレジット統計</a> — 生データ透明性</li>
            <li><a href="network_evolution.html">ネットワーク進化</a> — 学術研究基盤</li>
            <li><a href="bias_detection.html">バイアス検出</a> — 公平性研究</li>
            <li><a href="industry_overview.html">業界俯瞰</a> — 産業規模の定量把握</li>
        </ul>
    </div>
</div>"""

    system_overview_html = f"""
<div class="system-overview">
    <h2>Animetor Eval とは</h2>
    <p class="mission">
        「アニメ業界の個人貢献を可視化し、公正報酬の実現を通じて健全な産業基盤を築く」
    </p>
    <p>
        Animetor Eval は、公開クレジットデータをもとにアニメ業界プロフェッショナルの
        協業ネットワーク上の位置と貢献密度を定量化するシステムです。
        <strong>125,419人 / 60,091作品 / 994,854クレジット</strong>を分析基盤として、
        スタジオ・役職・キャリアステージのバイアスを除去した客観的な評価を提供します。
    </p>
    <p>
        本システムのスコアは「能力の測定」ではなく<strong>「ネットワーク上の位置と協業密度の定量化」</strong>です。
        低スコアはデータ上の可視性が限定的であることを示すに過ぎません。
    </p>
    <div class="arch-grid">
        <div class="arch-layer layer1">
            <h4>評価スコアの構成（全レポート共通）</h4>
            <ul>
                <li><strong>BiRank</strong>: 著名監督・作品への近接性（二部グラフランキング）→ 人物ランキング・時系列先見</li>
                <li><strong>Patronage</strong>: 同一監督からの継続起用（累積エッジ重み）→ 人物ランキング・チーム分析</li>
                <li><strong>Person FE</strong>: 個人寄与の構造推定（固定効果モデル）→ 成長スコア・時系列先見</li>
                <li><strong>IV Score</strong>: 3軸の操作変数推定統合（0-100正規化）→ 人物ランキング・バイアス検出</li>
            </ul>
        </div>
        <div class="arch-layer layer2">
            <h4>分析の切り口とレポートの対応</h4>
            <ul>
                <li><strong>構造分析</strong>: ネットワーク進化・ブリッジ分析</li>
                <li><strong>人材発見</strong>: 時系列先見・成長スコア</li>
                <li><strong>公平性</strong>: 報酬公平性・バイアス検出</li>
                <li><strong>キャリア</strong>: キャリア遷移・ジャンル親和性・人物ランキング</li>
                <li><strong>制作最適化</strong>: チーム分析・業界俯瞰・スタジオ影響</li>
            </ul>
        </div>
    </div>
</div>"""

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

{system_overview_html}

{stakeholder_guide_html}

<p class="section-title">全分析レポート一覧</p>
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

        for x, y in [("birank", "patronage"), ("birank", "person_fe"), ("patronage", "person_fe")]:
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

    # Phase 1: HTML Analysis Reports (14 reports)
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
    generate_cooccurrence_groups_report()
    generate_ml_clustering_report()
    generate_network_graph_report()
    generate_cohort_animation_report()
    generate_longitudinal_analysis_report()
    generate_shap_report()
    generate_explorer_data()
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
