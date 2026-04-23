#!/usr/bin/env python3
"""HTML template functions and styling for report generation.

Functions for building common HTML report components:
- layout wrappers (wrap_html)
- section headers (report_intro, chart_guide, etc.)
- Plotly chart embedding (plotly_div, plotly_div_safe)
- Glossaries, disclaimers, and utility boxes
"""

import base64
from datetime import datetime
from pathlib import Path

import plotly.graph_objects as go

from . import helpers

# Global constants (must be configured by main script)
JSON_DIR = Path("result/json")
REPORTS_DIR = Path("result/reports")

# Common CSS for all reports
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
/* Make SVG text selectable/copyable in Plotly charts */
.js-plotly-plot svg text,
.js-plotly-plot .legend text,
.js-plotly-plot .g-gtitle text,
.js-plotly-plot .xtitle text,
.js-plotly-plot .ytitle text {
    user-select: text !important;
    -webkit-user-select: text !important;
    -moz-user-select: text !important;
    cursor: text;
    pointer-events: all !important;
}
.copy-toast {
    position: fixed; bottom: 2rem; right: 2rem; z-index: 9999;
    background: rgba(6,214,160,0.95); color: #1a1a2e;
    padding: 0.6rem 1.2rem; border-radius: 8px;
    font-size: 0.85rem; font-weight: 600;
    opacity: 0; transition: opacity 0.3s;
    pointer-events: none; max-width: 400px;
    word-break: break-all;
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

details.section-accordion {
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 16px; margin-bottom: 1rem;
}
details.section-accordion > summary {
    padding: 1rem 1.5rem; cursor: pointer;
    font-weight: 700; color: #a0d2db; font-size: 1.1rem;
    list-style: none; user-select: none;
    border-radius: 16px;
}
details.section-accordion > summary:hover { background: rgba(255,255,255,0.04); }
details.section-accordion > summary::before { content: "▶ "; font-size: 0.85rem; color: #f093fb; }
details.section-accordion[open] > summary::before { content: "▼ "; }
details.section-accordion[open] > summary {
    border-bottom: 1px solid rgba(255,255,255,0.08);
    border-radius: 16px 16px 0 0;
}
details.section-accordion > .accordion-body { padding: 1rem; }

.caveat-box {
    border-left: 3px solid #E6A23C; background: rgba(230,162,60,0.06);
    padding: 0.8rem 1rem; margin: 0.8rem 0; font-size: 0.9rem;
    color: #d4c4a0; line-height: 1.7; border-radius: 0 8px 8px 0;
}
.competing-interp {
    border-left: 3px solid #667eea; background: rgba(102,126,234,0.04);
    padding: 1rem 1.2rem; margin: 0.8rem 0; border-radius: 0 8px 8px 0;
}
.competing-interp .ci-claim { color: #a0d2db; font-size: 0.95rem; margin-bottom: 0.5rem; }
.competing-interp .ci-alts { color: #b0b8c8; font-size: 0.88rem; }
.competing-interp ol { margin: 0.3rem 0 0 1.5rem; line-height: 1.8; }
"""

# Content constants
DISCLAIMER = (
    "本スコアは公開クレジットデータに基づくネットワーク上の位置・協業密度の定量指標であり、"
    "個人の能力・技量・芸術性を評価・測定・示唆するものではありません。"
    "低スコアはデータセット上のネットワーク可視性が限定的であることを意味し、"
    "実力の不足を意味するものではありません。"
    "本データを雇用・報酬・人事評価の唯一の根拠として使用することは推奨されません。"
)

METHODOLOGY_SUMMARY = (
    "評価はIntegrated Value (IV) Scoreに基づきます。5つのコンポーネント — "
    "(1) Person FE (θ): AKM固定効果モデルによる個人寄与推定、"
    "(2) BiRank: 二部グラフランキングによるネットワーク中心性、"
    "(3) Studio Exposure: スタジオ固定効果への累積露出、"
    "(4) AWCC: 能力加重協業中心性、"
    "(5) Patronage: 監督からの継続起用プレミアム — を"
    "z正規化した後、PCA第1主成分の負荷量で重み付けし、"
    "休眠ペナルティ(Dormancy)で乗算した統合指標です。"
)

COMMON_GLOSSARY_TERMS: dict[str, str] = {
    "BiRank": (
        "二部グラフ（人物-作品）上のランキングアルゴリズム。大規模・高評価スタッフが"
        "集まる作品に参加するほどスコアが上昇する。log1p(score × 10000)で"
        "べき乗分布を正規化。"
    ),
    "Patronage（パトロネージ）": (
        "同一監督・演出家からの継続起用を定量化する信頼指標。"
        "繰り返し起用 = 職業的信頼の累積。edge weight × repeat bonus で計算。"
    ),
    "Person FE（個人固定効果 θ）": (
        "AKM (Abowd-Kramarz-Margolis) モデルで推定する個人寄与。"
        "outcome = log(staff_count × episodes × duration_mult) から"
        "スタジオ効果 ψ を除去した個人の構造的貢献度。"
    ),
    "Studio Exposure（スタジオ環境指標）": (
        "所属スタジオの固定効果 ψ の加重和。高品質なスタジオ環境への"
        "累積露出度を反映。個人の実力ではなく環境要因の指標。"
    ),
    "AWCC（能力加重協業中心性）": (
        "協業者のIV Scoreで重み付けした次数中心性。"
        "強い協業者が多いほど高くなる。ネットワーク品質の指標。"
    ),
    "Dormancy（休眠ペナルティ）": (
        "最近の活動頻度に基づく乗数 (0〜1)。直近2年間のクレジットが"
        "少ないほどペナルティが大きい。キャリア初期は保護される。"
    ),
    "IV Score（統合評価スコア）": (
        "5成分(Person FE, BiRank, Patronage, Studio Exposure, AWCC)を"
        "z正規化 → PCA第1主成分の負荷量で重み付け → Dormancyで乗算した"
        "統合指標。ネットワーク上の位置と協業密度を反映。"
    ),
    "PageRank（ページランク）": (
        "グラフ中心性アルゴリズム。本システムではBiRankに置き換えられたが、"
        "一部の分析で参考指標として使用。"
    ),
    "Career Friction（キャリア摩擦）": (
        "キャリア進行の障壁を定量化した指標。ステージ停滞期間、"
        "ブランク期間、昇進速度の偏差を反映。"
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


def caveat_box(text: str) -> str:
    """解釈上の注意・制約を強調する黄橙ボックス."""
    return f'<div class="caveat-box">&#9888; <strong>解釈上の注意:</strong> {text}</div>'


def competing_interpretations(claim: str, alternatives: list[str]) -> str:
    """主張と競合する代替解釈を構造化して提示."""
    alts = "".join(f"<li>{a}</li>" for a in alternatives)
    return (
        '<div class="competing-interp">'
        f'<div class="ci-claim"><strong>主張:</strong> {claim}</div>'
        f'<div class="ci-alts"><strong>競合解釈:</strong><ol>{alts}</ol></div>'
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


def _build_base_scripts_html() -> str:
    """Copy-toast + plot-queue JS (shared by v1 and v2 wrappers)."""
    return """function showCopyToast(txt) {
    var t = document.getElementById('copy-toast');
    t.textContent = 'Copied: ' + txt;
    t.style.opacity = '1';
    setTimeout(function() { t.style.opacity = '0'; }, 1500);
}
document.addEventListener('dblclick', function(e) {
    var el = e.target;
    if (el.tagName === 'text' || (el.tagName === 'tspan' && el.closest && el.closest('text'))) {
        var textEl = el.tagName === 'tspan' ? el.closest('text') : el;
        if (textEl && textEl.closest('.js-plotly-plot')) {
            var txt = textEl.textContent.trim();
            if (txt) {
                navigator.clipboard.writeText(txt).then(function() {
                    showCopyToast(txt);
                });
            }
        }
    }
});
var _plotQueue = [];
var _plotBusy = false;
function queuePlot(fn) {
    _plotQueue.push(fn);
    _drainPlotQueue();
}
function _drainPlotQueue() {
    if (_plotBusy || _plotQueue.length === 0) return;
    _plotBusy = true;
    var fn = _plotQueue.shift();
    fn().then(function() {
        _plotBusy = false;
        setTimeout(_drainPlotQueue, 50);
    }).catch(function() {
        _plotBusy = false;
        setTimeout(_drainPlotQueue, 50);
    });
}"""


def _build_v1_head_html(title: str) -> str:
    return (
        "<!DOCTYPE html>\n<html lang=\"ja\">\n<head>\n"
        "<meta charset=\"UTF-8\">\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"
        f"<title>{title}</title>\n"
        "<script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>\n"
        f"<style>{COMMON_CSS}</style>\n"
        "</head>"
    )


def _build_v1_footer_html(footer_stats: str, methodology_html: str) -> str:
    return (
        "<footer>\n"
        f"    <p>Animetor Eval パイプライン分析により自動生成</p>\n"
        f"    <p>データ: {footer_stats}</p>\n"
        f"    {methodology_html}\n"
        "</footer>"
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
    footer_stats = helpers.get_footer_stats()
    head = _build_v1_head_html(title)
    scripts = _build_base_scripts_html()
    footer = _build_v1_footer_html(footer_stats, methodology_html)
    return f"""{head}
<body>
<div id="copy-toast" class="copy-toast"></div>
<script>
{scripts}
</script>
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
{footer}
</div>
</div>
</body>
</html>"""


# =========================================================================
# v2 Report Philosophy components
# =========================================================================

# Additional CSS for v2 components (appended to COMMON_CSS in wrap_html_v2)
V2_CSS = """
/* Stratification tabs */
.strat-tabs {
    display: flex; flex-wrap: wrap; gap: 0.4rem;
    margin: 1rem 0; padding: 0.5rem 0;
    border-bottom: 1px solid rgba(255,255,255,0.1);
}
.strat-tab {
    padding: 0.4rem 1rem; border-radius: 8px;
    font-size: 0.82rem; font-weight: 600;
    cursor: pointer; border: 1px solid rgba(255,255,255,0.1);
    background: rgba(255,255,255,0.03); color: #a0a0c0;
    transition: all 0.2s;
}
.strat-tab:hover { background: rgba(240,147,251,0.1); color: #e0e0e0; }
.strat-tab.active {
    background: rgba(240,147,251,0.2); color: #f093fb;
    border-color: rgba(240,147,251,0.4);
}
.strat-panel { display: none; }
.strat-panel.active { display: block; }

/* Method note */
.method-note {
    margin: 0.8rem 0; padding: 0;
}
.method-note details summary {
    cursor: pointer; color: #7b8794;
    font-size: 0.85rem; user-select: none;
}
.method-note .note-body {
    margin-top: 0.5rem; font-size: 0.82rem;
    color: #8a94a0; line-height: 1.5;
}

/* v2 Interpretation section */
.v2-interpretation {
    border-top: 1px solid #3a3a5c;
    margin-top: 1.5rem; padding-top: 1rem;
}
.v2-interpretation h3 {
    color: #c0a0d0; font-size: 1rem;
    margin-bottom: 0.8rem;
}
.v2-interpretation .interp-claim {
    color: #d0d0e0; font-size: 0.92rem;
    line-height: 1.7; margin-bottom: 0.8rem;
}
.v2-interpretation .interp-alternatives {
    border-left: 2px solid #667eea;
    padding-left: 1rem; margin: 0.8rem 0;
}
.v2-interpretation .interp-alternatives h4 {
    color: #667eea; font-size: 0.88rem; margin-bottom: 0.4rem;
}
.v2-interpretation .interp-alternatives ol {
    margin: 0.3rem 0 0 1.2rem;
    color: #b0b8c8; font-size: 0.85rem; line-height: 1.8;
}
.v2-interpretation .interp-premises {
    font-size: 0.82rem; color: #8a94a0;
    margin-top: 0.8rem; font-style: italic;
}

/* Null findings */
.null-findings {
    border-left: 3px solid #5a5a8a;
    background: rgba(90,90,138,0.06);
    padding: 1rem 1.2rem; margin: 1rem 0;
    border-radius: 0 8px 8px 0;
}
.null-findings strong { color: #8080b0; }
.null-findings p { font-size: 0.9rem; color: #a0a0c0; line-height: 1.7; }

/* Data statement */
.data-statement {
    border-left: 3px solid #5a5a8a;
    margin-top: 2rem;
}
.data-statement h2 { color: #a0a0c0; font-size: 1.3rem; }
.data-statement table { font-size: 0.85rem; }
.data-statement td { padding: 0.5rem; }
.data-statement .ds-label {
    color: #9a9ab0; vertical-align: top; width: 25%;
    font-weight: 600;
}
"""

# Tab switching JavaScript (embedded in wrap_html_v2)
_TAB_SWITCH_JS = """
function switchTab(groupId, tabKey) {
    var group = document.getElementById(groupId);
    if (!group) return;
    var tabs = group.querySelectorAll('.strat-tab');
    tabs.forEach(function(t) {
        t.classList.toggle('active', t.getAttribute('data-key') === tabKey);
    });
    var panels = document.querySelectorAll('.strat-panel[data-group="' + groupId + '"]');
    panels.forEach(function(p) {
        p.classList.toggle('active', p.getAttribute('data-key') === tabKey);
    });
    // Force render lazy-loaded Plotly charts that are now visible
    panels.forEach(function(p) {
        if (p.getAttribute('data-key') !== tabKey) return;
        var charts = p.querySelectorAll('[data-b64]');
        charts.forEach(function(el) {
            var b64 = el.getAttribute('data-b64');
            if (!b64) return;
            el.removeAttribute('data-b64');
            var d = JSON.parse(atob(b64));
            if (typeof queuePlot === 'function') {
                queuePlot(function() {
                    return Plotly.newPlot(el.id, d.data, d.layout,
                        {responsive: true, displayModeBar: true});
                });
            } else {
                Plotly.newPlot(el.id, d.data, d.layout,
                    {responsive: true, displayModeBar: true});
            }
        });
        // Resize already-rendered Plotly charts (they may have wrong dimensions)
        var rendered = p.querySelectorAll('.js-plotly-plot');
        rendered.forEach(function(el) { Plotly.Plots.resize(el); });
    });
}
"""


def stratification_tabs(
    group_id: str,
    axes: dict[str, str],
    active: str = "",
) -> str:
    """Generate tab buttons for stratification axes.

    Args:
        group_id: unique HTML id for this tab group
        axes: {tab_key: display_label} e.g. {"all": "全体", "tier": "Tier別", ...}
        active: key of the initially active tab (defaults to first)

    Returns:
        HTML string with tab buttons. Pair with strat_panel() for content.
    """
    if not active:
        active = next(iter(axes))
    tabs = []
    for key, label in axes.items():
        cls = "strat-tab active" if key == active else "strat-tab"
        tabs.append(
            f'<div class="{cls}" data-key="{key}" '
            f"""onclick="switchTab('{group_id}','{key}')">{label}</div>"""
        )
    return (
        f'<div class="strat-tabs" id="{group_id}">'
        + "".join(tabs)
        + "</div>"
    )


def strat_panel(group_id: str, key: str, content: str, active: bool = False) -> str:
    """Wrap content in a stratification panel that shows/hides with tabs.

    Args:
        group_id: must match the group_id used in stratification_tabs()
        key: tab key this panel corresponds to
        content: HTML content for this panel
        active: whether this panel is initially visible
    """
    cls = "strat-panel active" if active else "strat-panel"
    return f'<div class="{cls}" data-group="{group_id}" data-key="{key}">{content}</div>'


def method_note_block(text: str) -> str:
    """Collapsible method note per v2 structure."""
    return (
        '<div class="method-note">'
        "<details>"
        '<summary style="cursor:pointer;color:#7b8794;font-size:0.85rem;">'
        "Method Note / 方法論</summary>"
        f'<div class="note-body">{text}</div>'
        "</details>"
        "</div>"
    )


def interpretation_block(
    claim: str,
    alternatives: list[str],
    premises: str = "",
    author: str = "Animetor Eval analysis system",
) -> str:
    """v2 Interpretation section: labeled, authored, with alternatives.

    Per v2 Section 5: interpretation must be structurally separated from
    findings, explicitly labeled, state authorship, present at least 1
    alternative interpretation, and disclose premises.
    """
    alts_html = "".join(f"<li>{a}</li>" for a in alternatives)
    premises_html = (
        f'<p class="interp-premises"><strong>前提 / Premises:</strong> {premises}</p>'
        if premises else ""
    )
    return (
        '<div class="v2-interpretation">'
        '<h3>Interpretation / 解釈</h3>'
        f'<p style="font-size:0.78rem;color:#8a94a0;margin-bottom:0.5rem;">'
        f"Author: {author}</p>"
        f'<div class="interp-claim">{claim}</div>'
        '<div class="interp-alternatives">'
        "<h4>Alternative Interpretations / 代替解釈</h4>"
        f"<ol>{alts_html}</ol>"
        "</div>"
        f"{premises_html}"
        "</div>"
    )


def null_findings_block(text: str) -> str:
    """v2 Section 3.5: Null results are products, not failures.

    Used when analysis finds no pattern / no significant difference.
    """
    return (
        '<div class="null-findings">'
        "<strong>Null Finding / 帰無結果</strong>"
        f"<p>{text}</p>"
        "</div>"
    )


_DOC_LABEL: dict[str, str] = {
    "main": "",
    "brief": " [Executive Brief]",
    "appendix": " [Technical Appendix]",
}


def _resolve_v2_disclaimer(disclaimer_html: str) -> str:
    if disclaimer_html:
        return disclaimer_html
    return (
        '<div class="disclaimer-block">'
        "<h3>免責事項 (Disclaimer)</h3>"
        f"<p>{DISCLAIMER}</p>"
        "</div>"
    )


def _build_v2_head_html(title: str, doc_type: str) -> str:
    label = _DOC_LABEL.get(doc_type, "")
    return (
        "<!DOCTYPE html>\n<html lang=\"ja\">\n<head>\n"
        "<meta charset=\"UTF-8\">\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"
        f"<title>{title}{label}</title>\n"
        "<script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>\n"
        f"<style>{COMMON_CSS}\n{V2_CSS}</style>\n"
        "</head>"
    )


def _build_v2_header_block(title: str, subtitle: str, ts: str) -> str:
    return (
        "<header>\n"
        f"    <h1>{title}</h1>\n"
        f"    <p class=\"subtitle\">{subtitle}</p>\n"
        f"    <p class=\"timestamp\">生成日時: {ts}</p>\n"
        "</header>"
    )


def wrap_html_v2(
    title: str,
    subtitle: str,
    body: str,
    *,
    doc_type: str = "main",
    intro_html: str = "",
    glossary_terms: dict[str, str] | None = None,
    data_statement_html: str = "",
    disclaimer_html: str = "",
) -> str:
    """v2-compliant HTML wrapper.

    Args:
        doc_type: 'main', 'brief', or 'appendix' — controls header styling
        data_statement_html: pre-rendered data statement (from SectionBuilder)
        disclaimer_html: pre-rendered v2 disclaimer (from SectionBuilder)
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    glossary_html = build_glossary(glossary_terms) if glossary_terms else ""
    resolved_disclaimer = _resolve_v2_disclaimer(disclaimer_html)
    footer_stats = helpers.get_footer_stats()
    head = _build_v2_head_html(title, doc_type)
    scripts = _build_base_scripts_html()
    header = _build_v2_header_block(title, subtitle, ts)
    return f"""{head}
<body>
<div id="copy-toast" class="copy-toast"></div>
<script>
{scripts}
{_TAB_SWITCH_JS}
</script>
<div class="page-bg">
<div class="container">
{header}
{intro_html}
{body}
{glossary_html}
{data_statement_html}
{resolved_disclaimer}
<footer>
    <p>Animetor Eval — 自動生成レポート (v2)</p>
    <p>データ: {footer_stats}</p>
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


def _apply_dark_theme(fig: go.Figure, height: int) -> None:
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0.2)",
        font=dict(color="#c0c0d0"),
        height=height,
        margin=dict(l=60, r=30, t=50, b=50),
    )


def _subsample_box_violin_traces(fig: go.Figure) -> None:
    """Reduce data volume for box/violin traces to ≤200 points (x/y in sync)."""
    import numpy as _np_pds

    for trace in fig.data:
        if getattr(trace, "type", "") not in ("box", "violin"):
            continue
        y_vals = trace.y
        if y_vals is None or not hasattr(y_vals, "__len__") or len(y_vals) <= 200:
            continue
        n = len(y_vals)
        rng = _np_pds.random.default_rng(42)
        sel = rng.choice(n, size=min(200, n), replace=False)
        trace.y = _np_pds.asarray(y_vals)[sel].tolist()
        x_vals = trace.x
        if x_vals is not None and hasattr(x_vals, "__len__") and len(x_vals) == n:
            trace.x = _np_pds.asarray(x_vals)[sel].tolist()


def _encode_fig_base64(fig: go.Figure) -> str:
    return base64.b64encode(fig.to_json().encode()).decode()


def plotly_div_safe(fig: go.Figure, div_id: str, height: int = 500) -> str:
    """Plotlyチャートを安全に埋め込み (JSON escaping)."""
    _apply_dark_theme(fig, height)
    _subsample_box_violin_traces(fig)
    encoded = _encode_fig_base64(fig)
    return f"""<div class="chart-container">
<div id="{div_id}" data-b64="{encoded}" style="min-height:{height}px;"></div>
<script>
(function() {{
    var el = document.getElementById("{div_id}");
    var done = false;
    function doRender() {{
        if (done) return;
        var b64 = el.getAttribute("data-b64");
        if (!b64) {{ done = true; return; }}
        el.removeAttribute("data-b64");
        done = true;
        var d = JSON.parse(atob(b64));
        return Plotly.newPlot("{div_id}", d.data, d.layout,
                       {{responsive: true, displayModeBar: true}});
    }}
    if (typeof IntersectionObserver !== "undefined") {{
        var obs = new IntersectionObserver(function(entries) {{
            if (done || !entries[0].isIntersecting) return;
            obs.disconnect();
            if (typeof queuePlot === "function") {{
                queuePlot(doRender);
            }} else {{
                doRender();
            }}
        }}, {{rootMargin: "200px"}});
        obs.observe(el);
    }} else {{
        doRender();
    }}
}})();
</script>
</div>"""
