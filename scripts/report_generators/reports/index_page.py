"""Index page report — full report catalog with tier/cluster metadata.

v2 changes from the monolith version:
- Uses BaseReportGenerator for consistent wrapping
- DB-backed stats (live counts from feat_ tables)
- Tier and cluster distribution badges on the catalog
- v2 data statement + disclaimer
"""

from __future__ import annotations

from pathlib import Path

from ..html_templates import COMMON_CSS
from ._base import BaseReportGenerator


# -------------------------------------------------------------------------
# Static catalog (matches generate_all_reports.py REPORT_CATALOG)
# -------------------------------------------------------------------------

_CATALOG: list[dict] = [
    {"file": "industry_overview.html",     "title": "業界概観ダッシュボード",       "subtitle": "業界構造・人材フロー・退職分析", "desc": "業界規模、人材ストック、参入/退出フロー、期待能力分析。", "category": "overview", "sources": "summary, MADB, feat_career, credits"},
    {"file": "industry_analysis.html",    "title": "業界分析ダッシュボード",       "subtitle": "100年以上のアニメ制作マクロトレンド", "desc": "時系列推移、季節パターン、年代比較。", "category": "overview", "sources": "summary, time_series"},
    {"file": "structural_career.html",    "title": "構造的キャリア分析",          "subtitle": "キャリア遷移と人材パイプライン", "desc": "キャリアSankey、人材育成コスト、離職分析。", "category": "career", "sources": "transitions, milestones"},
    {"file": "network_analysis.html",     "title": "ネットワーク分析",            "subtitle": "Burt構造的空隙 / AKMバイアス",    "desc": "Bridge Score検証、役職別4レイヤー分析。", "category": "network", "sources": "bridges, DB"},
    {"file": "bridge_analysis.html",      "title": "ブリッジ分析",                "subtitle": "コミュニティ間ブリッジ人材",       "desc": "コミュニティを接続する人物の特定。",        "category": "network", "sources": "bridges"},
    {"file": "team_analysis.html",        "title": "チーム構成分析",              "subtitle": "高評価アニメのスタッフ構成",       "desc": "チーム構造、役職組み合わせ。",              "category": "production", "sources": "teams"},
    {"file": "career_transitions.html",   "title": "キャリア遷移分析",            "subtitle": "キャリアステージの進行と役職フロー", "desc": "遷移行列、サンキー、キャリアパス。", "category": "career", "sources": "transitions, role_flow"},
    {"file": "temporal_foresight.html",   "title": "時系列BiRank・先見スコア",    "subtitle": "BiRank時系列と人材早期発見",      "desc": "BiRank推移、先見スコアによる人材発見。", "category": "scoring", "sources": "temporal_pagerank"},
    {"file": "network_evolution.html",    "title": "ネットワーク構造変化",        "subtitle": "協業ネットワーク位相の時系列変化", "desc": "ノード/エッジ数、密度、クラスタリング推移。", "category": "network", "sources": "network_evolution"},
    {"file": "growth_scores.html",        "title": "成長トレンド・スコア分析",    "subtitle": "成長傾向・ライジングスター",       "desc": "成長トレンド分布、過小評価アラート。", "category": "scoring", "sources": "growth"},
    {"file": "person_ranking.html",       "title": "人物ランキング",              "subtitle": "IV Scoreによる人物ランキング",     "desc": "IV Score順上位人物、スコア分布。",          "category": "scoring", "sources": "scores"},
    {"file": "compensation_fairness.html","title": "報酬公平性分析",              "subtitle": "Shapley配分とGini分析",           "desc": "公正配分、作品別Gini係数。",                "category": "fairness", "sources": "scores"},
    {"file": "bias_detection.html",       "title": "バイアス検出レポート",        "subtitle": "系統的バイアスの検出と補正",      "desc": "役職・スタジオ別バイアス。",                "category": "fairness", "sources": "scores"},
    {"file": "genre_analysis.html",       "title": "ジャンル・スコア親和性",      "subtitle": "品質帯・時代別の親和性分析",      "desc": "スペシャリストvsジェネラリスト。",           "category": "career", "sources": "genre_affinity"},
    {"file": "studio_impact.html",        "title": "スタジオ影響分析",            "subtitle": "スタジオ所属の因果効果",          "desc": "選抜/処置/ブランド効果、構造推定。",         "category": "production", "sources": "studios"},
    {"file": "credit_statistics.html",    "title": "クレジット統計",              "subtitle": "クレジット集計、役職分布",        "desc": "クレジット数、役職分布、生産性指標。",       "category": "overview", "sources": "credits, role_flow"},
    {"file": "cooccurrence_groups.html",  "title": "共同制作集団分析",            "subtitle": "コアスタッフ繰り返し共同制作",    "desc": "固定チーム・監督コアスタッフの可視化。",    "category": "network", "sources": "cooccurrence_groups"},
    {"file": "ml_clustering.html",        "title": "MLクラスタリング分析",        "subtitle": "PCA × K-Meansクラスタリング",    "desc": "20次元特徴量ベクトル教師なしクラスタリング。", "category": "scoring", "sources": "ml_clusters"},
    {"file": "network_graph.html",        "title": "ネットワークグラフ",          "subtitle": "協業ネットワーク可視化",          "desc": "ノードサイズ=IV Score、色=MLクラスタ。",    "category": "network", "sources": "scores, collaborations"},
    {"file": "cohort_animation.html",     "title": "コホート・エージェント軌跡",  "subtitle": "世代別キャリア成長アニメーション", "desc": "Gapminder型アニメーション、13チャート。", "category": "career", "sources": "scores, growth, milestones"},
    {"file": "longitudinal_analysis.html","title": "縦断的キャリア分析",          "subtitle": "OMAクラスタ・パネルFE・PSM",      "desc": "固定費/変動費、イベントスタディ、PSM。",    "category": "career", "sources": "scores, milestones"},
    {"file": "shap_explanation.html",     "title": "SHAP スコア説明",             "subtitle": "Shapley値によるスコア決定因子",   "desc": "特徴量重要度、Beeswarm、Dependence Plot。", "category": "scoring", "sources": "scores"},
    {"file": "knowledge_network.html",    "title": "知識架橋分析",               "subtitle": "AWCC・NDIに基づく知識スパナー",   "desc": "知識橋渡し人材の特定、10チャート。",        "category": "network", "sources": "scores"},
    {"file": "akm_diagnostics_report.html","title": "AKMモデル診断",              "subtitle": "固定効果推定の診断",              "desc": "R²・ムーバー数・時代固定効果。",             "category": "technical", "sources": "DB"},
    {"file": "expected_ability_report.html","title": "期待能力・タレントギャップ", "subtitle": "期待vs実績のギャップ分析",        "desc": "過達成者・未達成者の特定。",                "category": "scoring", "sources": "scores"},
    {"file": "anime_value_report.html",   "title": "アニメ経済価値分析",          "subtitle": "5軸価値分解と貢献帰属",           "desc": "商業/批評/創造/文化/技術価値の分解。",      "category": "production", "sources": "anime_values"},
    {"file": "career_friction_report.html","title": "キャリア摩擦分析",           "subtitle": "キャリア変動指数の分布と相関",    "desc": "摩擦指数の分布、IV・AWCC との相関。",       "category": "career", "sources": "scores"},
    {"file": "studio_timeseries.html",    "title": "スタジオ時系列分析",          "subtitle": "スタジオ別人材動向の時系列",      "desc": "スタジオ別スコア推移、クラスタ比較。",      "category": "production", "sources": "studios, scores"},
    {"file": "compatibility.html",        "title": "コンパティビリティ分析",      "subtitle": "スタッフ間の協業適性",            "desc": "ペア適性スコア、ネットワーク相性。",         "category": "production", "sources": "scores, collaborations"},
    {"file": "score_layers_analysis.html","title": "スコア層別分析",              "subtitle": "3層スコア構造の分解",             "desc": "Person FE・Studio FE・Networkの層別分析。", "category": "scoring", "sources": "scores"},
    {"file": "career_dynamics.html",      "title": "キャリアダイナミクス",        "subtitle": "個人プロファイルとCI分析",        "desc": "CIバンド付きプロファイルカード。",           "category": "career", "sources": "scores, milestones"},
    {"file": "madb_coverage.html",        "title": "MADB カバレッジ分析",         "subtitle": "文化庁メディア芸術DBカバレッジ",  "desc": "MADBデータカバレッジと品質分析。",           "category": "technical", "sources": "MADB"},
    {"file": "dml_report.html",           "title": "DML 因果推論",               "subtitle": "Double/Debiased ML",              "desc": "OLS vs DML 2パターン比較。",                "category": "technical", "sources": "scores, DB"},
    {"file": "derived_params_report.html","title": "派生パラメータ分析",          "subtitle": "各種パラメータの透明性",          "desc": "Role weights, era deflator, WPS分析。",     "category": "technical", "sources": "DB"},
    {"file": "exit_analysis.html",       "title": "退職・休止期間分析",         "subtitle": "退職判定の信頼性と復帰パターン",   "desc": "年単位/四半期単位の休止分析、復帰率。",      "category": "exit", "sources": "feat_career_gaps, feat_credit_activity"},
]

_CATEGORY_LABELS = {
    "overview": ("業界概要", "#a0d2db"),
    "career": ("キャリア", "#f093fb"),
    "network": ("ネットワーク", "#06D6A0"),
    "scoring": ("スコアリング", "#fda085"),
    "fairness": ("公平性", "#FFD166"),
    "production": ("制作分析", "#667eea"),
    "technical": ("技術・診断", "#8a9ab0"),
}

_INDEX_CSS = """
.report-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
    gap: 1.2rem; margin: 1.5rem 0;
}
.report-card {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 14px; padding: 1.5rem;
    transition: transform 0.18s, border-color 0.18s;
    cursor: pointer; position: relative;
    display: flex; flex-direction: column; gap: 0.4rem;
}
.report-card:hover { transform: translateY(-3px); border-color: rgba(240,147,251,0.35); }
.report-card.missing { opacity: 0.45; pointer-events: none; }
.report-num {
    position: absolute; top: 1rem; right: 1.2rem;
    font-size: 2rem; font-weight: 900; opacity: 0.08; color: #f093fb;
}
.report-card h3 { font-size: 1.1rem; color: #f093fb; font-weight: 700; margin: 0; }
.report-subtitle { font-size: 0.82rem; color: #a0d2db; }
.report-desc { font-size: 0.82rem; color: #b0b0c0; line-height: 1.5; flex: 1; }
.report-meta {
    display: flex; justify-content: space-between; align-items: center;
    font-size: 0.75rem; margin-top: 0.5rem;
    border-top: 1px solid rgba(255,255,255,0.05); padding-top: 0.6rem;
}
.cat-badge {
    display: inline-block; padding: 0.15rem 0.55rem;
    border-radius: 8px; font-size: 0.72rem; font-weight: 600;
    background: rgba(160,210,219,0.15); color: #a0d2db;
}

.summary-bar {
    display: flex; justify-content: center; gap: 3rem;
    margin: 1.5rem 0; flex-wrap: wrap;
}
.summary-item { text-align: center; }
.summary-item .value {
    font-size: 2rem; font-weight: 800;
    background: linear-gradient(135deg, #f093fb, #f5576c);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
}
.summary-item .label { font-size: 0.82rem; color: #a0a0c0; }

.filter-bar {
    display: flex; flex-wrap: wrap; gap: 0.4rem;
    margin: 1rem 0 1.5rem;
}
.filter-btn {
    padding: 0.35rem 0.85rem; border-radius: 20px;
    border: 1px solid rgba(255,255,255,0.12);
    background: rgba(255,255,255,0.03);
    color: #a0a0c0; font-size: 0.8rem; cursor: pointer;
    transition: all 0.15s;
}
.filter-btn:hover, .filter-btn.active {
    background: rgba(240,147,251,0.18); color: #f093fb;
    border-color: rgba(240,147,251,0.35);
}

.system-intro {
    background: linear-gradient(135deg, rgba(240,147,251,0.07), rgba(160,210,219,0.05));
    border: 1px solid rgba(240,147,251,0.18);
    border-radius: 14px; padding: 1.8rem; margin-bottom: 1.5rem;
    color: #c0c0d0;
}
.system-intro .mission {
    font-size: 1rem; color: #a0d2db; font-style: italic;
    border-left: 3px solid #a0d2db; padding-left: 1rem; margin: 0.8rem 0;
}
.system-intro h2 { color: #f093fb; font-size: 1.4rem; margin-bottom: 0.8rem; }
.system-intro p { font-size: 0.9rem; line-height: 1.8; margin-bottom: 0.4rem; }

.db-stats {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 0.8rem; margin: 1rem 0;
}
.db-stat {
    background: rgba(0,0,0,0.2); border-radius: 10px;
    padding: 0.8rem 1rem; text-align: center;
}
.db-stat .v {
    font-size: 1.4rem; font-weight: 800;
    background: linear-gradient(135deg, #a0d2db, #667eea);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
}
.db-stat .l { font-size: 0.75rem; color: #8a94a0; margin-top: 0.2rem; }

@media (max-width: 750px) { .report-grid { grid-template-columns: 1fr; } }
"""

_FILTER_JS = """
function filterReports(cat) {
    var btns = document.querySelectorAll('.filter-btn');
    btns.forEach(function(b) {
        b.classList.toggle('active', b.getAttribute('data-cat') === cat);
    });
    var cards = document.querySelectorAll('.report-card-wrap');
    cards.forEach(function(c) {
        var match = cat === 'all' || c.getAttribute('data-cat') === cat;
        c.style.display = match ? '' : 'none';
    });
}
"""


class IndexPageReport(BaseReportGenerator):
    """Full report catalog index page."""

    name = "index"
    title = "Animetor Eval Reports"
    subtitle = "アニメ業界評価パイプライン — 全分析レポートインデックス"
    filename = "index.html"

    def generate(self) -> Path | None:
        catalog = _CATALOG

        # ── Live DB stats ──────────────────────────────────────────────
        stats = self._load_db_stats()

        # ── Card availability ──────────────────────────────────────────
        existing = {r["file"]: (self.output_dir / r["file"]).exists() for r in catalog}
        ready_count = sum(1 for v in existing.values() if v)

        # ── Filter buttons ─────────────────────────────────────────────
        categories = {"all": ("全て", "#f093fb")}
        categories.update({k: (v, c) for k, (v, c) in _CATEGORY_LABELS.items()})

        filter_btns = "".join(
            f'<button class="filter-btn{" active" if k == "all" else ""}" '
            f'data-cat="{k}" onclick="filterReports(\'{k}\')">'
            f'<span style="color:{col}">■</span> {label}</button>'
            for k, (label, col) in categories.items()
        )

        # ── Report cards ───────────────────────────────────────────────
        cards_html = ""
        for i, r in enumerate(catalog, 1):
            exists = existing.get(r["file"], False)
            cat = r.get("category", "overview")
            cat_label, cat_color = _CATEGORY_LABELS.get(cat, ("", "#a0a0c0"))
            status_badge = (
                '<span class="badge badge-high" style="font-size:0.72rem;">公開中</span>'
                if exists else
                '<span class="badge badge-low" style="font-size:0.72rem;">準備中</span>'
            )
            link_start = f'<a href="{r["file"]}" style="text-decoration:none">' if exists else "<div>"
            link_end = "</a>" if exists else "</div>"
            card_cls = "report-card" + ("" if exists else " missing")

            cards_html += (
                f'<div class="report-card-wrap" data-cat="{cat}">'
                f"{link_start}"
                f'<div class="{card_cls}">'
                f'<div class="report-num">{i:02d}</div>'
                f"<h3>{r['title']}</h3>"
                f'<p class="report-subtitle">{r["subtitle"]}</p>'
                f'<p class="report-desc">{r["desc"]}</p>'
                f'<div class="report-meta">'
                f'<span class="cat-badge" style="background:rgba(0,0,0,0.25);color:{cat_color};">'
                f'{cat_label}</span>'
                f"{status_badge}"
                f"</div>"
                f"</div>"
                f"{link_end}"
                f"</div>"
            )

        # ── DB stats panel ─────────────────────────────────────────────
        stat_items = [
            (f"{stats.get('persons', 0):,}", "登録人物"),
            (f"{stats.get('anime', 0):,}", "作品数"),
            (f"{stats.get('credits', 0):,}", "クレジット数"),
            (f"{stats.get('scored', 0):,}", "スコア算出済み"),
            (f"{stats.get('tier5', 0):,}", "Tier 5作品"),
            (f"{stats.get('schema_version', '?')}", "スキーマ版"),
        ]
        db_stats_html = (
            '<div class="db-stats">'
            + "".join(
                f'<div class="db-stat"><div class="v">{v}</div>'
                f'<div class="l">{lbl}</div></div>'
                for v, lbl in stat_items
            )
            + "</div>"
        )

        body = f"""
<div class="summary-bar">
    <div class="summary-item">
        <div class="value">{ready_count}/{len(catalog)}</div>
        <div class="label">公開レポート数</div>
    </div>
    <div class="summary-item">
        <div class="value">{len(_CATEGORY_LABELS)}</div>
        <div class="label">分析カテゴリ数</div>
    </div>
</div>

<div class="system-intro">
    <h2>Animetor Eval とは</h2>
    <p class="mission">アニメ業界の個人貢献を可視化し、公正報酬の実現を通じて健全な産業基盤を築く</p>
    <p>公開クレジットデータをもとにアニメ業界プロフェッショナルの協業ネットワーク上の位置と
    貢献密度を定量化するシステムです。スタジオ・役職・キャリアステージのバイアスを除去した
    記述的指標を提供します。</p>
    <p><strong>本スコアは「能力の測定」ではなく「ネットワーク上の位置と協業密度の定量化」です。</strong>
    低スコアはデータ上の可視性が限定的であることを示すに過ぎません。</p>
    {db_stats_html}
</div>

<div style="margin:1.5rem 0 0.5rem;color:#a0d2db;font-weight:700;font-size:1rem;">
    全分析レポート一覧
</div>
<div class="filter-bar">{filter_btns}</div>
<div class="report-grid">{cards_html}</div>
<script>{_FILTER_JS}</script>
"""
        # Write using custom HTML (not wrap_html_v2, since index is special)
        from datetime import datetime
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Animetor Eval — レポートインデックス</title>
<style>{COMMON_CSS}
{_INDEX_CSS}</style>
</head>
<body>
<div class="page-bg"><div class="container">
<header>
    <h1>Animetor Eval Reports</h1>
    <p class="subtitle">アニメ業界評価パイプライン — 全分析レポートインデックス</p>
    <p class="timestamp">生成日時: {ts}</p>
</header>
{body}
<footer>
    <p>Animetor Eval パイプライン分析により自動生成</p>
</footer>
</div></div>
</body>
</html>"""
        out = self.output_dir / self.filename
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(html, encoding="utf-8")
        return out

    def _load_db_stats(self) -> dict:
        """Load live statistics from the database."""
        stats: dict = {}
        try:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM persons"
            ).fetchone()
            stats["persons"] = row["n"] if row else 0
        except Exception:
            stats["persons"] = 0

        try:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM anime"
            ).fetchone()
            stats["anime"] = row["n"] if row else 0
        except Exception:
            stats["anime"] = 0

        try:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM credits"
            ).fetchone()
            stats["credits"] = row["n"] if row else 0
        except Exception:
            stats["credits"] = 0

        try:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM feat_person_scores"
            ).fetchone()
            stats["scored"] = row["n"] if row else 0
        except Exception:
            stats["scored"] = 0

        try:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM feat_work_context WHERE scale_tier = 5"
            ).fetchone()
            stats["tier5"] = row["n"] if row else 0
        except Exception:
            stats["tier5"] = 0

        try:
            from src.database import SCHEMA_VERSION
            stats["schema_version"] = SCHEMA_VERSION
        except Exception:
            stats["schema_version"] = "?"

        return stats
