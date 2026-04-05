"""HTML定数テンプレート — CSS, JS, wrap_html.

scripts/generate_all_reports.py から抽出した共通HTML基盤。
"""

from __future__ import annotations

from datetime import datetime

# ── CSS ──

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

.sortable-th { cursor: pointer; user-select: none; }
.sortable-th:hover { color: #f093fb; }
"""

# ── JS ──

QUEUE_PLOT_JS = """\
function showCopyToast(txt) {
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
}
"""

SORT_TABLE_JS = """\
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
    Array.from(table.querySelectorAll('thead th')).forEach(function(th, i) {
        th.textContent = th.textContent.replace(/ [↑↓↕]$/, '');
        if (i == col) th.textContent += asc ? ' ↑' : ' ↓';
        else th.textContent += ' ↕';
    });
}
"""

# ── テキスト定数 ──

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
}


# ── wrap_html ──

def wrap_html(
    title: str,
    subtitle: str,
    body: str,
    *,
    intro_html: str = "",
    glossary_terms: dict[str, str] | None = None,
    footer_stats: str = "",
) -> str:
    """共通HTMLテンプレート — 完全なHTMLページを生成."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    glossary_block = ""
    if glossary_terms:
        defs = ""
        for term, definition in sorted(glossary_terms.items()):
            defs += f"<dt>{term}</dt><dd>{definition}</dd>"
        glossary_block = (
            '<details class="glossary-toggle">'
            "<summary>用語集 (Glossary)</summary>"
            f"<dl>{defs}</dl>"
            "</details>"
        )

    disclaimer_html = (
        '<div class="disclaimer-block">'
        "<h3>免責事項 (Disclaimer)</h3>"
        f"<p>{DISCLAIMER}</p>"
        "</div>"
    )
    methodology_html = (
        '<div class="methodology">'
        f"<p><strong>評価方法:</strong> {METHODOLOGY_SUMMARY}</p>"
        "</div>"
    )

    footer_data = f"<p>データ: {footer_stats}</p>" if footer_stats else ""

    return f"""\
<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>{COMMON_CSS}</style>
</head>
<body>
<div id="copy-toast" class="copy-toast"></div>
<script>
{QUEUE_PLOT_JS}
{SORT_TABLE_JS}
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
{glossary_block}
{disclaimer_html}
<footer>
    <p>Animetor Eval パイプライン分析により自動生成</p>
    {footer_data}
    {methodology_html}
</footer>
</div>
</div>
</body>
</html>"""
