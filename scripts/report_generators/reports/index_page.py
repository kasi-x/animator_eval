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
    # ── common / landing ──
    {"file": "industry_overview.html",     "title": "業界概観ダッシュボード",       "subtitle": "業界構造・人材フロー・翌年クレジット可視性喪失分析", "desc": "業界規模、人材ストック、参入/退出フロー、協業者 IV 平均 × 個人 IV パーセンタイルの 4 群分布、クレジット統計（統合）。", "category": "overview", "sources": "summary, MADB, feat_career, credits"},
    {"file": "person_parameter_card.html", "title": "個人パラメータカード",         "subtitle": "全 audience 共通基盤",              "desc": "個人ごとのパラメータ・CI・キャリア段階。",    "category": "overview", "sources": "scores, feat_person_scores"},
    {"file": "bias_detection.html",        "title": "スコア差異分析",               "subtitle": "系統的差異の検出と補正",            "desc": "役職・スタジオ・時代別の差異パターン。",      "category": "fairness", "sources": "scores"},
    # ── audience brief entry points ──
    {"file": "policy_brief_index.html",    "title": "政策提言 Brief",               "subtitle": "政府・業界団体向けサマリー",        "desc": "翌年クレジット可視性喪失・市場集中度・ジェンダー動態。", "category": "brief", "sources": "meta_policy_*"},
    {"file": "hr_brief_index.html",        "title": "現場 Workflow 分析 Brief",      "subtitle": "スタジオ HR・制作デスク向け",        "desc": "配置適合度・育成実績・後継者プロファイル・チーム組成。",  "category": "brief", "sources": "meta_hr_*"},
    {"file": "biz_brief_index.html",       "title": "新たな試み提案 Brief",         "subtitle": "投資家・新規企画者向け",            "desc": "ジャンル空白地・信頼ネット参入・独立ユニット形成。",       "category": "brief", "sources": "meta_biz_*"},
    # ── policy brief reports ──
    {"file": "policy_attrition.html",      "title": "翌年クレジット可視性喪失率分析","subtitle": "DML 推定 / KM / Cox（旧 exit/friction 統合）","desc": "cohort × treatment ATE、Hazard Ratio、離職・復職章を統合。","category": "policy", "sources": "meta_policy_attrition"},
    {"file": "policy_monopsony.html",      "title": "労働市場集中度分析",           "subtitle": "スタジオ HHI と logit(stay)",        "desc": "年 × スタジオ別 HHI・HHI*、monopsony 係数。",  "category": "policy", "sources": "meta_policy_monopsony"},
    {"file": "policy_gender_bottleneck.html","title":"職種遷移とジェンダー生存分析","subtitle": "transition stage × cohort 別",       "desc": "生存確率と log-rank 検定、役職ごとのボトルネック。","category": "policy", "sources": "meta_policy_gender"},
    {"file": "policy_generational_health.html","title":"世代別キャリア生存曲線",  "subtitle": "debut decade × career year bin",    "desc": "S(k) 曲線、キャリア遷移統合章、ピラミッド。",     "category": "policy", "sources": "meta_policy_generation"},
    {"file": "compensation_fairness.html", "title": "報酬格差記述統計",             "subtitle": "役割 × スタジオ × 年代別生産スケール","desc": "Shapley 配分・Gini 分析、公平性可視化。",       "category": "policy", "sources": "scores"},
    # ── hr brief reports ──
    {"file": "mgmt_studio_benchmark.html", "title": "スタジオ配置効率ベンチマーク",  "subtitle": "studio × year（旧 studio_impact/timeseries 統合）", "desc": "R5 定着率・Value-Added・H_s スコア、時系列章を含む。","category": "hr", "sources": "meta_hr_studio_benchmark"},
    {"file": "mgmt_director_mentor.html",  "title": "監督育成実績プロファイル",     "subtitle": "M̂_d EB 縮小推定 + Null モデル",     "desc": "監督ごとのメンティー輩出実績（記述的プロファイル）。","category": "hr", "sources": "meta_hr_mentor_card"},
    {"file": "mgmt_attrition_risk.html",   "title": "翌年クレジット可視性喪失リスクプロファイル","subtitle":"新人コホート別（認証要）","desc": "予測リスク + SHAP 上位特徴量、C-index ゲート。", "category": "hr", "sources": "meta_hr_attrition_risk"},
    {"file": "mgmt_succession.html",       "title": "後継者候補プロファイル",        "subtitle": "ベテラン × 候補者",                  "desc": "successor score の aggregate 公開版。",         "category": "hr", "sources": "meta_hr_succession"},
    {"file": "mgmt_team_chemistry.html",   "title": "チーム適合度プロファイル",      "subtitle": "team_analysis / compatibility 統合", "desc": "チーム構成 × 過去共演パターン適合スコア。",      "category": "hr", "sources": "meta_hr_team_chemistry"},
    {"file": "growth_scores.html",         "title": "キャリア成長軌跡",              "subtitle": "structural_career / career_dynamics 統合","desc": "役職ステージ遷移速度と成長クラスタ分布、個人CIカード。","category": "hr", "sources": "scores, growth, milestones"},
    # ── biz brief reports ──
    {"file": "biz_genre_whitespace.html",  "title": "ジャンル空白地図",             "subtitle": "genre × year（genre_analysis 統合）","desc": "CAGR・penetration・W_g スコア、ジャンル需給詳細章。","category": "biz", "sources": "meta_biz_whitespace"},
    {"file": "biz_undervalued_talent.html","title": "露出機会ギャップ人材プール",   "subtitle": "U_p スコア分布 / アーキタイプ",      "desc": "ネットワーク到達範囲に対し露出が少ない人物群の記述。","category": "biz", "sources": "meta_biz_undervalued"},
    {"file": "biz_trust_entry.html",       "title": "信頼ネットワーク参入分析",     "subtitle": "bridge_analysis 概要を統合",         "desc": "ゲートキーパー G_p・Reach_p と新規参入速度。",  "category": "biz", "sources": "meta_biz_trust_entry"},
    {"file": "biz_team_template.html",     "title": "チームテンプレート提案",        "subtitle": "cluster × tier",                     "desc": "テンプレートと silhouette スコア。",            "category": "biz", "sources": "meta_biz_team_template"},
    {"file": "biz_independent_unit.html",  "title": "独立制作ユニット分析",          "subtitle": "community 別 coverage × density",    "desc": "V_G スコア、実行可能性指標。",                  "category": "biz", "sources": "meta_biz_independent_unit"},
    # ── technical appendix ──
    {"file": "akm_diagnostics_report.html","title": "AKMモデル診断",                 "subtitle": "固定効果推定の診断",                 "desc": "R²・ムーバー数・時代固定効果。",                 "category": "technical", "sources": "DB"},
    {"file": "dml_report.html",            "title": "DML 因果推論",                  "subtitle": "Double/Debiased ML",                 "desc": "OLS vs DML 2 パターン比較。",                    "category": "technical", "sources": "scores, DB"},
    {"file": "score_layers_analysis.html", "title": "スコア層別分析",                "subtitle": "3層スコア構造の分解",                "desc": "Person FE・Studio FE・Network の層別分析。",    "category": "technical", "sources": "scores"},
    {"file": "shap_explanation.html",      "title": "SHAP スコア説明",               "subtitle": "Shapley 値によるスコア決定因子",     "desc": "特徴量重要度、Beeswarm、Dependence Plot。",     "category": "technical", "sources": "scores"},
    {"file": "longitudinal_analysis.html", "title": "縦断的キャリア分析",            "subtitle": "OMAクラスタ・パネルFE・PSM",         "desc": "固定費/変動費、イベントスタディ、PSM。",        "category": "technical", "sources": "scores, milestones"},
    {"file": "ml_clustering.html",         "title": "MLクラスタリング分析",          "subtitle": "PCA × K-Means クラスタリング",       "desc": "20 次元特徴量ベクトル教師なしクラスタリング。", "category": "technical", "sources": "ml_clusters"},
    {"file": "network_analysis.html",      "title": "ネットワーク分析",              "subtitle": "Burt 構造的空隙 / AKM バイアス",     "desc": "Bridge Score 検証、役職別 4 レイヤー分析。",    "category": "technical", "sources": "bridges, DB"},
    {"file": "network_graph.html",         "title": "ネットワークグラフ",            "subtitle": "協業ネットワーク可視化",              "desc": "ノードサイズ=IV Score、色=ML クラスタ。",      "category": "technical", "sources": "scores, collaborations"},
    {"file": "network_evolution.html",     "title": "ネットワーク構造変化",          "subtitle": "協業ネットワーク位相の時系列変化",    "desc": "ノード/エッジ数、密度、クラスタリング推移。",  "category": "technical", "sources": "network_evolution"},
    {"file": "cooccurrence_groups.html",   "title": "共同制作集団分析",              "subtitle": "コアスタッフ繰り返し共同制作",        "desc": "固定チーム・監督コアスタッフの可視化。",        "category": "technical", "sources": "cooccurrence_groups"},
    {"file": "madb_coverage.html",         "title": "MADB カバレッジ分析",           "subtitle": "文化庁メディア芸術 DB カバレッジ",    "desc": "MADB データカバレッジと品質分析。",             "category": "technical", "sources": "MADB"},
    {"file": "derived_params_report.html", "title": "派生パラメータ分析",            "subtitle": "各種パラメータの透明性",              "desc": "Role weights, era deflator, WPS 分析。",        "category": "technical", "sources": "DB"},
    {"file": "cohort_animation.html",      "title": "コホート・エージェント軌跡",    "subtitle": "世代別キャリア成長アニメーション",    "desc": "Gapminder 型アニメーション、13 チャート。",     "category": "technical", "sources": "scores, growth, milestones"},
    {"file": "knowledge_network.html",     "title": "知識架橋分析",                  "subtitle": "AWCC・NDI に基づく知識スパナー",      "desc": "知識橋渡し人材の特定、10 チャート。",           "category": "technical", "sources": "scores"},
    {"file": "temporal_foresight.html",    "title": "時系列 BiRank・活動パターン記述",    "subtitle": "BiRank 時系列と活動パターン記述",       "desc": "BiRank 推移と活動比率の回顧的分析、初期指標・成長傾向の記述。","category": "technical", "sources": "temporal_pagerank"},
    {"file": "bridge_analysis.html",       "title": "ネットワークブリッジ詳細",      "subtitle": "概要は biz_trust_entry に統合",       "desc": "コミュニティ間接続人物の詳細（監査用）。",      "category": "technical", "sources": "bridges"},
]

_CATEGORY_LABELS = {
    "overview": ("業界概要", "#a0d2db"),
    "brief": ("audience brief", "#f093fb"),
    "policy": ("政策提言", "#FFD166"),
    "hr": ("現場 Workflow", "#06D6A0"),
    "biz": ("新たな試み", "#fda085"),
    "fairness": ("差異検出", "#FFD166"),
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
    <p><strong>本スコアは「ネットワーク位置と協業密度の定量化」です。スコアは定量指標であり、個人評価ではありません。</strong>
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
            from src.db import SCHEMA_VERSION
            stats["schema_version"] = SCHEMA_VERSION
        except Exception:
            stats["schema_version"] = "?"

        return stats
