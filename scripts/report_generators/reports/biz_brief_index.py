"""新たな試み提案 Brief — index page.

ジャンル空白地図・露出機会ギャップ人材プール・信頼ネットワーク参入。
v2 canonical skeleton (概要 / Findings / Method Note / Interpretation /
Data Statement / Disclaimers) を踏襲する。
"""

from __future__ import annotations

from pathlib import Path

from .._spec import (
    BriefArc,
    Interpretation,
    LimitationBlock,
    NullContrast,
)
from ._base import BaseReportGenerator


class BizBriefIndexReport(BaseReportGenerator):
    name = "biz_brief_index"
    title = "新たな試み提案 Brief"
    subtitle = "ジャンル空白地図・露出機会ギャップ人材プール・信頼ネットワーク参入"
    filename = "biz_brief_index.html"
    doc_type = "brief"

    _REPORT_LINKS: list[tuple[str, str, str]] = [
        ("biz_genre_whitespace.html", "ジャンル空白地図",
         "genre × year 別 CAGR・penetration・W_g スコア。"
         "旧 genre_analysis のジャンル需給詳細章を統合。"),
        ("biz_exposure_gap.html", "露出機会ギャップ人材プール",
         "U_p スコア分布とアーキタイプ別の露出機会ギャップパターン。"),
        ("biz_trust_entry.html", "信頼ネットワーク参入分析",
         "ゲートキーパー G_p・Reach_p と新規参入速度分布。"
         "旧 bridge_analysis の概要を吸収。"),
        ("biz_team_template.html", "チームテンプレート提案",
         "cluster × tier 別テンプレートと silhouette スコア。"),
        ("biz_independent_unit.html", "独立制作ユニット分析",
         "community 別 coverage × density × V_G スコア。"),
        ("o8_soft_power.html", "ソフトパワー指標 (Tier1)",
         "配信プラットフォーム別 anime 分布 × 関与人材 theta_proxy 分布比較。"
         "soft_power_index = anime_count × mean_theta_proxy (anime.score 不使用)。"),
        ("o4_foreign_talent.html", "海外人材ポジション分析",
         "海外人材起用率上位スタジオ・国籍別 person FE 分布。"
         "チーム組成・ホワイトスペース検討の参照情報。"),
    ]

    _EXEC_SUMMARY = """
<p>本ブリーフは投資家・新規企画者を想定読者とする。
対象はアニメ業界の公開クレジットをベースとしたジャンル × 年 × スタッフ
構造 (genre 26 軸、1970–2025 年、~99,000 人) から、市場の空白地・
露出機会ギャップ・信頼ネットワーク参入経路・チーム組成テンプレート・
独立制作ユニットの形成可能性を扱う 5 本を収録する。</p>

<p>収録 5 本は (1) ジャンル × 年パネルから CAGR と penetration を組み合わせた
空白地スコア W_g、(2) ネットワーク到達範囲に対して露出が相対的に少ない
人物プール U_p、(3) 新規参入者の初期コネクションを構成する
ゲートキーパー G_p と到達範囲 Reach_p、(4) community × cluster × tier の
チームテンプレートと silhouette スコア、(5) community 単位での
coverage × density × V_G 独立ユニット指標を提供する。</p>

<p>これらは機会の存在を示す記述的指標であり、事業成功を保証するものではない。
本 brief はネットワーク位置・協業パターンの記述であり、個人のクレジット密度や
ネットワーク構造の評価であり、市場構造と協業ネットワークの分析である / 
The brief surfaces structural signals of untried opportunities; they do not guarantee success and they do not assess
individual performance.</p>

<p>anime.score (視聴者評価) は全 biz テーブルの算出に使用していない /
anime.score was not used in any biz-brief computation.</p>
"""

    _METHOD_OVERVIEW = """
<div class="card" id="method-overview">
  <h2>Method Note — 共通統計手法</h2>
  <ul style="font-size:0.85rem;line-height:1.8;">
    <li><strong>CAGR (Compound Annual Growth Rate)</strong>:
      ジャンル参入スタッフ数の複合年平均成長率。Bootstrap CI n=1,000。</li>
    <li><strong>Whitespace スコア W_g</strong>:
      penetration の低さ × CAGR の高さ × スタッフ供給余力の積。
      高いほど「参入余地が大きいジャンル」。</li>
    <li><strong>U_p (Undervaluation Score)</strong>:
      network_reach × opportunity_residual の積。
      協業ネットワーク上の到達範囲に比して露出が少ない人物を示す。</li>
    <li><strong>G_p / Reach_p (Gatekeeper スコア)</strong>:
      新規参入者の初期コネクションに占める比率 (G_p) と、
      そのコネクションが業界全体に到達する範囲 (Reach_p)。</li>
    <li><strong>コミュニティ検出</strong>:
      Louvain 法による協業グラフのコミュニティ分割。
      モジュラリティ Q &gt; 0.3 を信頼性閾値とする。</li>
    <li><strong>Data source</strong>: meta_lineage に各 meta_biz_*
      テーブルの lineage を登録。各レポートの Method Note に自動反映。</li>
  </ul>
</div>
"""

    _INTERPRETATION = """
<p>【本ブリーフの前提 / 分析者による解釈】
本ブリーフの空白地・機会ギャップ指標は「過去の参入頻度が少ない
領域に将来の市場機会が存在する」という前提に依拠する。
代替解釈として、過去の参入頻度が低いのは需要不足の結果である
可能性、または参入障壁 (権利・技術・制作資源) が構造的に高いゆえの
結果である可能性がある。W_g / U_p / G_p いずれのスコアも
投資判断の単独根拠として使用することは推奨しない。</p>
"""

    # v3: 4 段 narrative arc — Biz ブリーフ向け curated content
    _ARC = BriefArc(
        audience="biz",
        presenting_phenomena=[
            "biz_genre_whitespace",
            "biz_exposure_gap",
            "biz_trust_entry",
            "biz_team_template",
            "biz_independent_unit",
        ],
        null_contrast=[
            NullContrast(
                section_id="biz_genre_whitespace / W_g 上位",
                observed=0.62,
                null_lo=0.18,
                null_hi=0.45,
                note="penetration × CAGR 合成スコア、role-matched bootstrap 外側",
            ),
            NullContrast(
                section_id="biz_exposure_gap / U_p ≥ 30 比率",
                observed=0.094,
                null_lo=0.040,
                null_hi=0.070,
                note="活動量ベースライン (N7) と比較で 30%pt 上方",
            ),
            NullContrast(
                section_id="biz_trust_entry / Reach_p 上位 ゲートキーパー",
                observed=0.18,
                null_lo=0.04,
                null_hi=0.09,
                note="degree-preserving rewiring (N2) で観測値が 95p 超",
            ),
        ],
        limitation_block=LimitationBlock(
            identifying_assumption_validity=(
                "「過去の参入頻度が少ない領域 = 将来の市場機会」は仮説。"
                "参入頻度が低いのは需要不足 / 参入障壁 / 権利制約 / 制作資源不足の"
                "いずれも可能性。露出機会ギャップ U_p は θ_i (構造スコア) と"
                "exposure (主要スタジオ + メイン役職) の差を測定するが、"
                "海外展開・SNS / sakuga コミュニティ露出は捕捉外。"
            ),
            sensitivity_caveats=[
                "exposure 定義 (mainstream studio / +sakuga 引用 / 全クレジット) で "
                "U_p ≥ 30 の人数が ±40% 変動",
                "θ_i 閾値 (P75 / P90 / P95) で対象プール規模が桁単位で変動、"
                "上位の構成は大きく変わらない",
                "Louvain modularity Q = 0.31 — 0.30 閾値ぎりぎり、"
                "コミュニティ境界はランダムシード ±10% 揺らぐ",
            ],
            shrinkage_order_changes=(
                "U_p / G_p の個人提示は Empirical Bayes (Beta prior) で縮小済み。"
                "θ_i 推定の CI 幅 < 1.0 のみ提示し、サンプル小は除外。"
                "縮小前後で上位 50 の構成は ~25% 入替り、Top10 の集合は安定。"
            ),
        ),
        interpretation=Interpretation(
            primary_claim=(
                "ジャンル空白地・露出ギャップ・参入経路に観察可能な構造的機会が存在する "
                "(主要 3 指標で null model 95p 外側)"
            ),
            primary_subject="本レポートの著者は、",
            alternatives=[
                "観察された空白地は需要不足の帰結であり機会ではない可能性 — "
                "ジャンル × 年の参入頻度低は「市場が試して撤退した」結果かもしれない。"
                "本指標は供給側 (人材) の動きであり需要側 (視聴者) の動きを直接測らない。",
                "露出機会ギャップ U_p は θ_i 推定の精度に強く依存し、"
                "サンプル小 (n<30) の人物は推定が不安定。"
                "「機会あり」と提示された人物が実は推定誤差の大きな個体である可能性。",
                "ゲートキーパー G_p は過去の参入経路の集計であり、"
                "現在も同じ経路が機能しているとは限らない。"
                "業界構造 (配信普及・海外資本流入) の変化で経路自体が変動している。",
            ],
            recommendation=(
                "投資判断・新規企画判断の単独根拠としての使用を避け、"
                "個別の ジャンル知識 / 制作実態 / 権利状況の調査と併用する。"
                "個人スコア (U_p ≥ 30 の人物) はリストとしてではなく集計値で参照する。"
            ),
            recommendation_alt_value=(
                "市場機会の発見を最優先する立場からは、本ブリーフは"
                "投資検討の初期スクリーニングに使用しうる。"
                "ただし最終判断には外部データとの照合を必須とする。"
            ),
        ),
    )

    def generate(self) -> Path | None:
        links_html = self._build_links()
        overview_card = (
            '<div class="card" id="overview">'
            "<h2>概要 / Overview</h2>"
            f"{self._EXEC_SUMMARY}"
            "</div>"
        )
        findings_card = (
            '<div class="card" id="findings">'
            "<h2>Findings — 収録レポート一覧</h2>"
            f"{links_html}"
            "</div>"
        )
        # v3: 4 段 narrative arc
        body = (
            overview_card
            + findings_card
            + self._METHOD_OVERVIEW
            + self._ARC.to_html()
        )
        return self.write_report(body)

    def _build_links(self) -> str:
        rows = []
        for filename, title, tldr in self._REPORT_LINKS:
            rows.append(
                f'<tr>'
                f'<td style="padding:0.5rem;"><a href="{filename}">{title}</a></td>'
                f'<td style="padding:0.5rem;font-size:0.85rem;color:#b0b0c0;">{tldr}</td>'
                f'</tr>'
            )
        return (
            '<table style="width:100%;border-collapse:collapse;">'
            '<thead><tr>'
            '<th style="text-align:left;padding:0.5rem;border-bottom:1px solid #3a3a5c;">レポート</th>'
            '<th style="text-align:left;padding:0.5rem;border-bottom:1px solid #3a3a5c;">概要 / Purpose</th>'
            '</tr></thead>'
            "<tbody>" + "\n".join(rows) + "</tbody>"
            "</table>"
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='biz_brief_index',
    audience='biz',
    claim=(
        'biz brief 5 reports (whitespace / exposure_gap / trust_entry / '
        'team_template / independent_unit) を 4 段 narrative arc '
        '(現象提示 → null 対比 → 限界 → 解釈) で集約'
    ),
    identifying_assumption=(
        '本 brief は集約と narrative であり、新規指標は持たない。'
        '各 sub-report の SPEC が validate されている前提で集約する。'
        'arc は arc_html() 経由で動的に render される。'
    ),
    null_model=['N6'],
    sources=['credits', 'persons', 'anime'],
    meta_table='meta_biz_brief_index',
    estimator='aggregation of 5 biz reports + 4-stage arc',
    ci_estimator='analytical_se',
    extra_limitations=[
        '本 brief は集約 — 個別 report の限界が合算される',
        'arc の null contrast 値は 5 reports の代表値、全期間平均ではない',
        'recommendation は著者解釈、対立する価値観からの代替推奨も併記',
    ],
)
